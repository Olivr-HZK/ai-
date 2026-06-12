"""Generate the VE daily filter review dashboard.

The dashboard is a local HTML review surface for the two filters humans usually
want to inspect visually:

1. cover dedupe: ahash/url fingerprint plus CLIP cross-day matches;
2. play one-liner dedupe: intraday, old-play, and high-confidence embedding
   duplicate filters.

It is intentionally file-based so cron/pipeline runs can refresh it without any
extra service.
"""

from __future__ import annotations

import argparse
import copy
import contextlib
import io
import json
import os
import sqlite3
import subprocess
import warnings
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from html import escape
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests

from ua_workflows.shared.config import DATA_DIR, REPORTS_DIR, load_project_env
from ua_workflows.shared.db.video_enhancer import _coarse_play_cluster_key
from ua_workflows.shared.media.resolve import normalize_video_url_for_consumption
from ua_workflows.shared.llm.client import bytes_to_embedding
from ua_workflows.video_enhancer.cover_dedupe import (
    _cluster_clip_dedupe,
    _cover_history_hard_dedupe_days,
    _cover_visual_threshold,
    _history_reference_dates,
)
from ua_workflows.video_enhancer.play_assets import (
    legacy_play_library_enabled,
    load_play_assets,
    match_play_asset,
)
from ua_workflows.video_enhancer.play_asset_doc_sync import maybe_pull_play_asset_doc
from ua_workflows.video_enhancer.play_asset_report import build_daily_asset_variant_report

load_project_env()


def _output_prefix(target_date: str) -> str:
    return f"workflow_video_enhancer_{target_date}"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _env_enabled(name: str, default: str = "1") -> bool:
    value = (os.getenv(name) or default).strip().lower()
    return value not in {"", "0", "false", "no", "off"}


def _h(value: Any) -> str:
    return escape("" if value is None else str(value), quote=True)


def _short_key(value: Any, n: int = 12) -> str:
    text = str(value or "")
    return text[:n] + ("..." if len(text) > n else "")


def _format_material_time(value: Any) -> str:
    if value in (None, "", 0, "0"):
        return "-"
    text = str(value).strip()
    try:
        timestamp = float(text)
    except (TypeError, ValueError):
        return text
    if abs(timestamp) > 10_000_000_000:
        timestamp = timestamp / 1000
    try:
        return datetime.fromtimestamp(timestamp, timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M")
    except (OSError, OverflowError, ValueError):
        return text


def _cover_filter_scope(row: dict[str, Any], target_date: str, matched_date: Any = "") -> str:
    reason = str(row.get("reason") or "").strip()
    if reason == "cover_style_cluster":
        return "今日同样素材筛选"
    if reason == "cover_style_cluster_vs_yesterday":
        return "历史筛选"
    if matched_date and str(matched_date) == target_date:
        return "今日同样素材筛选"
    return "历史筛选"


def _cover_group_scope_label(scope_counts: Counter[str]) -> str:
    if scope_counts.get("今日同样素材筛选") and not scope_counts.get("历史筛选"):
        return "今日同样素材筛选"
    if scope_counts.get("历史筛选") and not scope_counts.get("今日同样素材筛选"):
        return "历史筛选"
    return "混合筛选"


def _first_text(row: dict[str, Any]) -> str:
    for key in ("effect_one_liner", "play_fingerprint", "ad_one_liner", "title", "body"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return "素材"


def _play_family(row: dict[str, Any], evidence: dict[str, Any] | None = None) -> str:
    evidence = evidence or {}
    play_text = (
        row.get("play_fingerprint")
        or evidence.get("play_fingerprint")
        or evidence.get("matched_play_fingerprint")
        or ""
    )
    display_text = (
        row.get("effect_one_liner")
        or evidence.get("effect_one_liner")
        or evidence.get("matched_effect_one_liner")
        or ""
    )
    return _coarse_play_cluster_key(play_text, display_text) or str(play_text or display_text or "-")


def _row_value(row: dict[str, Any], key: str) -> Any:
    value = row.get(key)
    return "" if value is None else value


def _media_url(row: dict[str, Any]) -> str:
    video_url = normalize_video_url_for_consumption(str(row.get("video_url") or "").strip())
    return str(video_url or row.get("preview_img_url") or row.get("image_url") or "").strip()


def _image_url(row: dict[str, Any]) -> str:
    return str(row.get("preview_img_url") or row.get("image_url") or row.get("cover_url") or "").strip()


def _ext_from_bytes(data: bytes) -> str:
    if data.startswith(b"\xff\xd8\xff"):
        return ".jpg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return ".png"
    if data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return ".gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return ".webp"
    return ".jpg"


def _asset_dir(target_date: str) -> Path:
    path = REPORTS_DIR / "assets" / f"ve_filter_review_{target_date}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _local_image_path(target_date: str, ad_key: str) -> str:
    files = sorted(_asset_dir(target_date).glob(f"{ad_key}.*"))
    if files:
        return f"assets/ve_filter_review_{target_date}/{files[0].name}"
    legacy_dir = REPORTS_DIR / "assets" / f"ve_cover_dedupe_{target_date}"
    legacy_files = sorted(legacy_dir.glob(f"{ad_key}.*"))
    if legacy_files:
        return f"assets/ve_cover_dedupe_{target_date}/{legacy_files[0].name}"
    return ""


def _cache_image(target_date: str, ad_key: str, url: str) -> str:
    if not ad_key:
        return ""
    existing = _local_image_path(target_date, ad_key)
    if existing:
        return existing
    if not url:
        return ""
    enabled = (os.getenv("VE_REVIEW_DASHBOARD_DOWNLOAD_IMAGES") or "1").strip().lower()
    if enabled in ("0", "false", "no", "off"):
        return ""
    try:
        timeout = max(3, min(30, int(os.getenv("VE_REVIEW_DASHBOARD_IMAGE_TIMEOUT_SEC") or "12")))
    except ValueError:
        timeout = 12
    res = subprocess.run(
        ["curl", "-L", "-sS", "--fail", "--max-time", str(timeout), url],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if res.returncode != 0 or not res.stdout:
        return ""
    ext = _ext_from_bytes(res.stdout)
    path = _asset_dir(target_date) / f"{ad_key}{ext}"
    try:
        path.write_bytes(res.stdout)
    except OSError:
        return ""
    return f"assets/ve_filter_review_{target_date}/{path.name}"


def _raw_today_rows(raw_payload: dict[str, Any], product_by_appid: dict[str, str]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for item in raw_payload.get("items") or []:
        if not isinstance(item, dict):
            continue
        creative = item.get("creative") or {}
        if not isinstance(creative, dict):
            continue
        ad_key = str(creative.get("ad_key") or "").strip()
        if not ad_key:
            continue
        appid = str(item.get("appid") or creative.get("appid") or "").strip()
        row = {
            "ad_key": ad_key,
            "appid": appid,
            "product": str(item.get("product") or product_by_appid.get(appid) or ""),
            "all_exposure_value": int(creative.get("all_exposure_value") or 0),
            "preview_img_url": str(creative.get("preview_img_url") or ""),
            "image_url": "",
            "video_url": "",
            "title": str(creative.get("title") or ""),
            "body": str(creative.get("body") or ""),
            "first_seen": creative.get("first_seen") or item.get("first_seen") or "",
            "created_at": creative.get("created_at") or item.get("created_at") or "",
        }
        for res in creative.get("resource_urls") or []:
            if isinstance(res, dict):
                row["image_url"] = row["image_url"] or str(res.get("image_url") or "")
                row["video_url"] = row["video_url"] or normalize_video_url_for_consumption(str(res.get("video_url") or ""))
        rows[ad_key] = row
    return rows


def _analysis_rows(analysis_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in analysis_payload.get("results") or []:
        if not isinstance(row, dict):
            continue
        ad_key = str(row.get("ad_key") or "").strip()
        if ad_key:
            rows[ad_key] = row
    return rows


def _analysis_exclusion_is_hard(row: dict[str, Any]) -> bool:
    if not row.get("exclude_from_bitable"):
        return False
    if row.get("adult_content_filter_match") or row.get("human_photo_effect_filter_match") or row.get("launched_effect_match"):
        return True
    soft_play_keys = (
        "intraday_effect_match",
        "old_effect_match",
        "effect_embedding_duplicate_match",
    )
    if any(row.get(key) for key in soft_play_keys) and _env_enabled("BITABLE_SYNC_INCLUDE_PLAY_DUPLICATE_EXCLUDES", "1"):
        return False
    return True


def _business_reason(row: dict[str, Any]) -> str:
    if row.get("adult_content_filter_match"):
        return "成人/色情风险"
    if row.get("human_photo_effect_filter_match"):
        match = row.get("human_photo_effect_filter_match") or {}
        if isinstance(match, dict):
            reason = str(match.get("reason") or "").strip()
            if reason:
                return reason
        return "非人物照片加工特效"
    if row.get("launched_effect_match"):
        return "我方已投放"
    if row.get("intraday_effect_match"):
        return "日内玩法重复"
    if row.get("old_effect_match"):
        return "历史玩法重复"
    if row.get("effect_embedding_duplicate_match"):
        return "玩法 embedding 重复"
    if row.get("exclude_from_bitable"):
        return "入表前硬拦"
    return ""


def _item_product(item: dict[str, Any]) -> str:
    creative = item.get("creative") if isinstance(item, dict) else {}
    if not isinstance(creative, dict):
        creative = {}
    return str(
        item.get("product")
        or creative.get("product")
        or creative.get("advertiser_name")
        or item.get("keyword")
        or "未知产品"
    ).strip() or "未知产品"


def _review_row_from_raw_item(
    item: dict[str, Any],
    analysis_by_ad: dict[str, dict[str, Any]],
    *,
    status: str = "应入多维表",
) -> dict[str, Any]:
    creative = item.get("creative") if isinstance(item, dict) else {}
    if not isinstance(creative, dict):
        creative = {}
    ad_key = str(item.get("ad_key") or creative.get("ad_key") or "").strip()
    row = dict(analysis_by_ad.get(ad_key) or {})
    row["ad_key"] = ad_key
    row["_review_status"] = status
    for key in (
        "product",
        "appid",
        "platform",
        "creative_type",
        "advertiser_name",
        "page_name",
        "title",
        "body",
        "preview_img_url",
        "image_url",
        "video_url",
        "video_duration",
        "all_exposure_value",
    ):
        value = item.get(key)
        if value in (None, ""):
            value = creative.get(key)
        if value not in (None, ""):
            row[key] = value
    row["product"] = row.get("product") or _item_product(item)
    return row


def _simulate_business_gate(
    target_date: str,
    raw_payload: dict[str, Any],
    analysis_payload: dict[str, Any],
) -> dict[str, Any]:
    """Mirror the Bitable pre-write gate without creating Feishu records."""
    analysis_copy = copy.deepcopy(analysis_payload)
    raw_copy = copy.deepcopy(raw_payload)
    results = [row for row in analysis_copy.get("results") or [] if isinstance(row, dict)]
    successful_rows = [
        row
        for row in results
        if str(row.get("ad_key") or "").strip()
        and str(row.get("analysis") or "").strip()
        and not str(row.get("analysis") or "").startswith("[ERROR]")
    ]
    out: dict[str, Any] = {
        "successful_rows": successful_rows,
        "hard_excluded_rows": [],
        "after_hard_items": [],
        "template_skipped_rows": [],
        "template_kept_items": [],
        "error": "",
    }
    try:
        warnings.filterwarnings("ignore", message="pkg_resources is deprecated as an API.*")
        from ua_workflows.video_enhancer.play_asset_report import annotate_daily_play_asset_novelty
        from ua_workflows.video_enhancer.sync import (
            apply_adult_content_filter,
            apply_human_photo_effect_filter,
            apply_template_dedup_for_bitable,
            raw_items_with_successful_analysis,
        )

        with contextlib.redirect_stdout(io.StringIO()):
            apply_adult_content_filter(results)
            apply_human_photo_effect_filter(results)
            annotate_daily_play_asset_novelty(results, target_date)

        hard_rows = [row for row in successful_rows if _analysis_exclusion_is_hard(row)]
        out["hard_excluded_rows"] = hard_rows

        play_asset_by_ad: dict[str, dict[str, Any]] = {}
        effect_by_ad: dict[str, str] = {}
        play_fingerprint_by_ad: dict[str, str] = {}
        template_fingerprint_by_ad: dict[str, str] = {}
        for row in results:
            ad_key = str(row.get("ad_key") or "").strip()
            if not ad_key:
                continue
            play_asset_by_ad[ad_key] = {
                "play_asset_name": str(row.get("play_asset_name") or ""),
                "play_asset_id": str(row.get("play_asset_id") or ""),
                "template_fingerprint": str(row.get("template_fingerprint") or ""),
                "play_asset_match_source": str(row.get("play_asset_match_source") or ""),
                "play_asset_classification_reason": str(row.get("play_asset_classification_reason") or ""),
            }
            effect_by_ad[ad_key] = str(row.get("effect_one_liner") or "")
            play_fingerprint_by_ad[ad_key] = str(row.get("play_fingerprint") or "")
            template_fingerprint_by_ad[ad_key] = str(row.get("template_fingerprint") or "")

        after_hard_items = raw_items_with_successful_analysis(raw_copy, analysis_copy)
        out["after_hard_items"] = after_hard_items
        with contextlib.redirect_stdout(io.StringIO()):
            template_kept, template_skipped = apply_template_dedup_for_bitable(
                after_hard_items,
                play_asset_by_ad=play_asset_by_ad,
                effect_by_ad=effect_by_ad,
                play_fingerprint_by_ad=play_fingerprint_by_ad,
                template_fingerprint_by_ad=template_fingerprint_by_ad,
            )
        out["template_kept_items"] = template_kept
        out["template_skipped_rows"] = template_skipped
    except Exception as exc:
        out["hard_excluded_rows"] = [row for row in successful_rows if _analysis_exclusion_is_hard(row)]
        out["error"] = str(exc)
    return out


def _bitable_counts_for_date(target_date: str) -> dict[str, Any]:
    out: dict[str, Any] = {
        "count": None,
        "by_product": {},
        "scanned": 0,
        "error": "",
    }
    if not _env_enabled("VE_REVIEW_DASHBOARD_BITABLE_COUNT", "1"):
        out["error"] = "disabled"
        return out
    bitable_url = (os.getenv("VIDEO_ENHANCER_BITABLE_URL") or "").strip()
    app_id = (os.getenv("FEISHU_APP_ID") or "").strip()
    app_secret = (os.getenv("FEISHU_APP_SECRET") or "").strip()
    if not bitable_url or not app_id or not app_secret:
        out["error"] = "missing_env"
        return out
    try:
        parsed = urlparse(bitable_url)
        parts = [part for part in parsed.path.split("/") if part]
        app_token = parts[1] if len(parts) >= 2 and parts[0] == "base" else ""
        table_id = (parse_qs(parsed.query or "").get("table") or [""])[0]
        if not app_token or not table_id:
            out["error"] = "bad_bitable_url"
            return out
        resp = requests.post(
            "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": app_id, "app_secret": app_secret},
            timeout=10,
        )
        resp.raise_for_status()
        token_payload = resp.json()
        if token_payload.get("code") != 0:
            out["error"] = f"token_failed:{token_payload.get('code')}"
            return out
        token = str(token_payload.get("tenant_access_token") or "")
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}
        api = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
        rows: list[dict[str, Any]] = []
        page_token = ""
        while True:
            params: dict[str, Any] = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            resp2 = requests.get(api, headers=headers, params=params, timeout=20)
            resp2.raise_for_status()
            payload = resp2.json()
            if payload.get("code") != 0:
                out["error"] = f"list_failed:{payload.get('code')}"
                return out
            data = payload.get("data") or {}
            items = data.get("items") or data.get("records") or []
            rows.extend(row for row in items if isinstance(row, dict))
            if not data.get("has_more"):
                break
            page_token = str(data.get("page_token") or "")
            if not page_token or len(rows) > 20000:
                break

        def _date_value_to_ymd(value: Any) -> str:
            if isinstance(value, (int, float)):
                return datetime.fromtimestamp(int(value) / 1000, timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
            return str(value or "")[:10]

        by_product: Counter[str] = Counter()
        for row in rows:
            fields = row.get("fields") or {}
            if not isinstance(fields, dict):
                continue
            if _date_value_to_ymd(fields.get("抓取日期")) != target_date:
                continue
            by_product[str(fields.get("产品") or "未知产品")] += 1
        out["scanned"] = len(rows)
        out["count"] = int(sum(by_product.values()))
        out["by_product"] = {key: int(value) for key, value in by_product.most_common()}
        return out
    except Exception as exc:
        out["error"] = str(exc)
        return out


def _query_creative_meta(ad_keys: set[str]) -> dict[str, dict[str, Any]]:
    keys = sorted(k for k in ad_keys if k)
    if not keys:
        return {}
    conn = sqlite3.connect(DATA_DIR / "video_enhancer_pipeline.db")
    conn.row_factory = sqlite3.Row
    out: dict[str, dict[str, Any]] = {}
    try:
        cur = conn.cursor()
        for i in range(0, len(keys), 400):
            chunk = keys[i : i + 400]
            ph = ",".join("?" for _ in chunk)
            cur.execute(
                f"""
                SELECT ad_key, product, appid, platform, video_duration, title, body,
                       video_url, image_url, preview_img_url, best_heat, best_impression,
                       best_all_exposure_value, first_target_date, last_target_date,
                       effect_one_liner, play_fingerprint, ad_one_liner
                FROM creative_library
                WHERE ad_key IN ({ph})
                """,
                chunk,
            )
            for row in cur.fetchall():
                out[str(row["ad_key"])] = dict(row)
            cur.execute(
                f"""
                SELECT ad_key, MIN(NULLIF(first_seen, 0)) AS first_seen,
                       MIN(NULLIF(created_at, 0)) AS created_at
                FROM daily_creative_insights
                WHERE ad_key IN ({ph})
                GROUP BY ad_key
                """,
                chunk,
            )
            for row in cur.fetchall():
                current = out.setdefault(str(row["ad_key"]), {"ad_key": str(row["ad_key"])})
                if row["first_seen"] not in (None, ""):
                    current["first_seen"] = row["first_seen"]
                if row["created_at"] not in (None, ""):
                    current["created_at"] = row["created_at"]
        return out
    finally:
        conn.close()


def _merge_meta(*sources: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for source in sources:
        for ad_key, row in source.items():
            cur = out.setdefault(ad_key, {})
            for key, value in row.items():
                if value not in (None, ""):
                    cur[key] = value
    return out


def _history_cover_rows(target_date: str, history_dates: list[str]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, bytes]]:
    if not history_dates:
        return {}, {}
    conn = sqlite3.connect(DATA_DIR / "video_enhancer_pipeline.db")
    conn.row_factory = sqlite3.Row
    by_app: dict[str, list[dict[str, Any]]] = defaultdict(list)
    blobs: dict[str, bytes] = {}
    try:
        cur = conn.cursor()
        ph = ",".join("?" for _ in history_dates)
        cur.execute(
            f"""
            SELECT d.target_date, d.ad_key, d.appid, d.product, COALESCE(d.all_exposure_value, 0) AS exp,
                   c.cover_embedding, c.preview_img_url, c.image_url, c.video_url, c.title, c.body,
                   c.effect_one_liner, c.play_fingerprint, c.ad_one_liner, c.first_target_date
            FROM daily_creative_insights d
            JOIN creative_library c ON c.ad_key = d.ad_key
            WHERE d.target_date IN ({ph})
              AND COALESCE(TRIM(d.insight_cover_style), '') <> ''
              AND c.cover_embedding IS NOT NULL
            """,
            history_dates,
        )
        for row in cur.fetchall():
            item = dict(row)
            item["exposure"] = int(item.get("exp") or 0)
            by_app[str(item.get("appid") or "")].append(item)
            blobs[str(item.get("ad_key") or "")] = bytes(item["cover_embedding"])
        return dict(by_app), blobs
    finally:
        conn.close()


def _today_cover_embeddings(ad_keys: list[str]) -> dict[str, list[float]]:
    keys = [k for k in ad_keys if k]
    if not keys:
        return {}
    conn = sqlite3.connect(DATA_DIR / "video_enhancer_pipeline.db")
    conn.row_factory = sqlite3.Row
    out: dict[str, list[float]] = {}
    try:
        cur = conn.cursor()
        for i in range(0, len(keys), 400):
            chunk = keys[i : i + 400]
            ph = ",".join("?" for _ in chunk)
            cur.execute(
                f"SELECT ad_key, cover_embedding FROM creative_library WHERE ad_key IN ({ph}) AND cover_embedding IS NOT NULL",
                chunk,
            )
            for row in cur.fetchall():
                out[str(row["ad_key"])] = bytes_to_embedding(bytes(row["cover_embedding"]))
        return out
    finally:
        conn.close()


def _cosine_similarity(a: list[float] | None, b: list[float] | None) -> float | None:
    if not a or not b:
        return None
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(y * y for y in b) ** 0.5
    if not na or not nb:
        return None
    return dot / (na * nb)


def _cover_report_clip_removed(report: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for per_app in report.get("per_appid") or []:
        if not isinstance(per_app, dict):
            continue
        appid = str(per_app.get("appid") or "")
        product = str(per_app.get("product") or "")
        for row in per_app.get("removed") or []:
            if not isinstance(row, dict):
                continue
            if str(row.get("reason") or "").startswith("cover_style_cluster"):
                item = dict(row)
                item.setdefault("appid", appid)
                item.setdefault("product", product)
                out.append(item)
    return out


def _compute_clip_crossday_preview(
    target_date: str,
    today_rows: dict[str, dict[str, Any]],
    product_by_appid: dict[str, str],
    threshold: float,
    history_dates: list[str],
) -> list[dict[str, Any]]:
    """Recompute current-rule CLIP cross-day matches for review/backfill pages."""
    if not today_rows:
        return []
    today_vecs = _today_cover_embeddings(list(today_rows))
    history_by_app, history_blobs = _history_cover_rows(target_date, history_dates)
    by_app: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in today_rows.values():
        appid = str(row.get("appid") or "")
        if appid and row.get("ad_key") in today_vecs:
            by_app[appid].append(row)
    out: list[dict[str, Any]] = []
    for appid, rows in by_app.items():
        today_input = [
            {
                "ad_key": str(row.get("ad_key") or ""),
                "exposure": int(row.get("all_exposure_value") or row.get("best_all_exposure_value") or 0),
                "product": str(row.get("product") or product_by_appid.get(appid) or ""),
                "cover_url": _image_url(row),
                "vec": today_vecs[str(row.get("ad_key") or "")],
            }
            for row in rows
            if str(row.get("ad_key") or "") in today_vecs
        ]
        history_hist = [
            {
                "ad_key": str(row.get("ad_key") or ""),
                "target_date": str(row.get("target_date") or ""),
                "exposure": int(row.get("exposure") or 0),
            }
            for row in history_by_app.get(appid, [])
        ]
        history_emb = {
            str(row.get("ad_key") or ""): history_blobs[str(row.get("ad_key") or "")]
            for row in history_by_app.get(appid, [])
            if str(row.get("ad_key") or "") in history_blobs
        }
        _kept, removed, _refresh = _cluster_clip_dedupe(
            threshold=threshold,
            today_rows=today_input,
            history_hist=history_hist,
            history_emb=history_emb,
            target_date=target_date,
            hard_dedupe_days=_cover_history_hard_dedupe_days(),
        )
        for item in removed:
            if item.get("reason") != "cover_style_cluster_vs_yesterday":
                continue
            item = dict(item)
            item.setdefault("appid", appid)
            item.setdefault("product", product_by_appid.get(appid) or (rows[0].get("product") if rows else appid))
            item.setdefault("source", "computed_current_rule")
            out.append(item)
    return out


def _merge_cover_clip_rows(*lists: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    for rows in lists:
        for row in rows:
            key = (
                str(row.get("ad_key") or ""),
                str(row.get("kept_ad_key") or ""),
                str(row.get("reason") or ""),
            )
            if not key[0]:
                continue
            cur = merged.setdefault(key, {})
            cur.update({k: v for k, v in row.items() if v not in (None, "")})
    return list(merged.values())


def _cover_fingerprint_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in report.get("cross_day_fingerprint_removed") or []:
        if isinstance(row, dict):
            item = dict(row)
            item["kept_ad_key"] = str(item.get("matched_ad_key") or "")
            out.append(item)
    return out


def _one_liner_removed_rows(analysis_rows: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for ad_key, row in analysis_rows.items():
        if not bool(row.get("exclude_from_bitable") or row.get("exclude_from_cluster")):
            continue
        effect = str(row.get("play_fingerprint") or row.get("effect_one_liner") or "").strip()
        if not effect:
            continue
        base = {
            "ad_key": ad_key,
            "appid": str(row.get("appid") or ""),
            "product": str(row.get("product") or ""),
            "effect_one_liner": str(row.get("effect_one_liner") or ""),
            "play_fingerprint": str(row.get("play_fingerprint") or ""),
        }
        intraday = row.get("intraday_effect_match")
        if isinstance(intraday, dict):
            out.append(
                {
                    **base,
                    "kind": "日内玩法重复",
                    "kept_ad_key": str(intraday.get("kept_ad_key") or ""),
                    "matched_effect_one_liner": str(intraday.get("kept_effect_one_liner") or ""),
                    "matched_play_fingerprint": str(intraday.get("kept_play_fingerprint") or ""),
                    "similarity": intraday.get("similarity"),
                    "threshold": intraday.get("threshold"),
                }
            )
        old = row.get("old_effect_match")
        if isinstance(old, dict):
            out.append(
                {
                    **base,
                    "kind": "老玩法重复",
                    "kept_ad_key": "",
                    "matched_ad_key": str(old.get("matched_ad_key") or ""),
                    "matched_effect_one_liner": str(old.get("matched_effect_one_liner") or ""),
                    "matched_play_fingerprint": str(old.get("matched_play_fingerprint") or ""),
                    "matched_date": str(old.get("first_seen_date") or ""),
                    "similarity": old.get("similarity"),
                    "threshold": old.get("threshold"),
                    "history_count": old.get("history_count"),
                }
            )
        emb = row.get("effect_embedding_duplicate_match")
        if isinstance(emb, dict):
            out.append(
                {
                    **base,
                    "kind": "embedding玩法重复",
                    "kept_ad_key": str(emb.get("matched_ad_key") or ""),
                    "matched_ad_key": str(emb.get("matched_ad_key") or ""),
                    "matched_effect_one_liner": str(emb.get("matched_effect_one_liner") or ""),
                    "matched_play_fingerprint": str(emb.get("matched_play_fingerprint") or ""),
                    "matched_date": str(emb.get("matched_date") or emb.get("matched_first_seen_date") or ""),
                    "source": str(emb.get("source") or ""),
                    "similarity": emb.get("similarity"),
                    "threshold": emb.get("threshold"),
                }
            )
        if row.get("semantic_dedup_matched"):
            out.append(
                {
                    **base,
                    "kind": "分析语义重复",
                    "kept_ad_key": str(row.get("semantic_dedup_matched") or ""),
                    "matched_ad_key": str(row.get("semantic_dedup_matched") or ""),
                    "matched_effect_one_liner": "",
                    "matched_play_fingerprint": "",
                    "similarity": row.get("semantic_dedup_similarity"),
                    "threshold": "",
                }
            )
    return out


def _reference_ad_key(row: dict[str, Any]) -> str:
    for key in ("kept_ad_key", "matched_ad_key"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _resolve_one_liner_reference_keys(rows: list[dict[str, Any]], target_date: str) -> None:
    """Fill matched_ad_key for old-play rows that only stored matched text."""
    unresolved = [row for row in rows if not _reference_ad_key(row)]
    if not unresolved:
        return
    conn = sqlite3.connect(DATA_DIR / "video_enhancer_pipeline.db")
    conn.row_factory = sqlite3.Row
    cache: dict[tuple[str, str], tuple[str, str]] = {}
    try:
        cur = conn.cursor()
        for row in unresolved:
            appid = str(row.get("appid") or "").strip()
            if not appid:
                continue
            texts: list[str] = []
            for field in ("matched_play_fingerprint", "matched_effect_one_liner"):
                text = str(row.get(field) or "").strip()
                if text and text not in texts:
                    texts.append(text)
            for text in texts:
                cache_key = (appid, text)
                if cache_key not in cache:
                    cur.execute(
                        """
                        SELECT ad_key, first_target_date
                        FROM creative_library
                        WHERE appid = ?
                          AND first_target_date < ?
                          AND (
                            COALESCE(NULLIF(TRIM(play_fingerprint), ''), TRIM(effect_one_liner)) = ?
                            OR TRIM(play_fingerprint) = ?
                            OR TRIM(effect_one_liner) = ?
                          )
                        ORDER BY COALESCE(best_all_exposure_value, 0) DESC,
                                 COALESCE(best_impression, 0) DESC,
                                 COALESCE(last_target_date, '') DESC
                        LIMIT 1
                        """,
                        (appid, target_date, text, text, text),
                    )
                    hit = cur.fetchone()
                    cache[cache_key] = (
                        str(hit["ad_key"] or "") if hit else "",
                        str(hit["first_target_date"] or "") if hit else "",
                    )
                matched_key, matched_date = cache[cache_key]
                if matched_key:
                    row["matched_ad_key"] = matched_key
                    row["kept_ad_key"] = row.get("kept_ad_key") or matched_key
                    if not row.get("matched_date"):
                        row["matched_date"] = matched_date
                    break
    finally:
        conn.close()


def _annotate_one_liner_cover_similarity(rows: list[dict[str, Any]], threshold: float) -> None:
    keys: set[str] = set()
    for row in rows:
        ad_key = str(row.get("ad_key") or "")
        ref_key = _reference_ad_key(row)
        if ad_key:
            keys.add(ad_key)
        if ref_key:
            keys.add(ref_key)
    vecs = _today_cover_embeddings(sorted(keys))
    for row in rows:
        ad_key = str(row.get("ad_key") or "")
        ref_key = _reference_ad_key(row)
        sim = _cosine_similarity(vecs.get(ad_key), vecs.get(ref_key)) if ad_key and ref_key else None
        if sim is None:
            note = "缺少封面向量，未进入CLIP对比"
        elif sim >= threshold:
            note = f"CLIP {sim:.3f} >= 阈值 {threshold:.2f}，理论应由封面命中"
        else:
            note = f"CLIP {sim:.3f} < 阈值 {threshold:.2f}，所以封面未拦"
        row["cover_similarity"] = round(sim, 3) if sim is not None else ""
        row["cover_similarity_note"] = note


def _group_by_kept(rows: list[dict[str, Any]], *, key_field: str = "kept_ad_key") -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = str(row.get(key_field) or row.get("matched_ad_key") or row.get("matched_play_fingerprint") or row.get("matched_effect_one_liner") or "")
        if not key:
            key = "未定位代表"
        grouped[key].append(row)
    out: list[dict[str, Any]] = []
    for key, members in grouped.items():
        out.append({"key": key, "members": members})
    out.sort(key=lambda x: (-len(x["members"]), str(x["key"])))
    return out


def _css() -> str:
    return """
:root{color-scheme:light;--bg:#f6f7f9;--ink:#17191f;--muted:#6c7280;--line:#d9dde5;--panel:#fff;--accent:#0f766e;--accent-soft:#e0f2ef;--danger:#b42318;--danger-soft:#fee4e2;--keep:#2563eb;--keep-soft:#dbeafe;--warn:#9a6700;--warn-soft:#fff6d6}
*{box-sizing:border-box}body{margin:0;font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;color:var(--ink);background:var(--bg)}header{background:var(--bg);border-bottom:1px solid var(--line)}.wrap{max-width:1440px;margin:0 auto;padding:22px 28px}h1{margin:0 0 4px;font-size:26px;line-height:1.15}.sub{color:var(--muted);display:flex;flex-wrap:wrap;gap:10px 18px}.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-top:18px}.stat{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:14px 16px}.stat strong{display:block;font-size:26px;line-height:1;margin-bottom:8px}.stat span{color:var(--muted)}.toolbar{display:flex;gap:12px;align-items:center;margin-top:16px}input[type=search]{width:min(620px,100%);padding:10px 12px;border:1px solid var(--line);border-radius:8px;font:inherit;background:#fff}main.wrap{padding-top:18px}.note{color:var(--muted);background:#fff;border:1px solid var(--line);border-radius:8px;padding:12px 14px;margin-bottom:18px}.chips{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:18px}.chip{border:1px solid var(--line);background:#fff;padding:7px 10px;border-radius:999px;color:#2a2f3a}.section-title{font-size:20px;margin:24px 0 12px}.cluster{background:var(--panel);border:1px solid var(--line);border-radius:8px;margin:0 0 18px;overflow:hidden}.cluster.covered-play{border-color:#bdd7ff}.cluster-head{display:flex;justify-content:space-between;gap:14px;padding:14px 16px;border-bottom:1px solid var(--line);background:#fbfcfd}.covered-play .cluster-head{background:#f5f9ff}.cluster-title{font-weight:700;font-size:16px}.cluster-meta{color:var(--muted);margin-top:3px}.badges{display:flex;flex-wrap:wrap;justify-content:flex-end;gap:6px}.badge{display:inline-flex;align-items:center;min-height:24px;padding:3px 8px;border-radius:999px;background:#edf0f5;color:#384152;font-size:12px;white-space:nowrap}.badge.keep{color:var(--keep);background:var(--keep-soft)}.badge.drop{color:var(--danger);background:var(--danger-soft)}.badge.reason{color:var(--accent);background:var(--accent-soft)}.badge.warn{color:var(--warn);background:var(--warn-soft)}.cards{display:grid;grid-template-columns:280px repeat(auto-fill,minmax(220px,1fr));gap:12px;padding:14px;align-items:start}.play-pairs{display:grid;grid-template-columns:repeat(auto-fit,minmax(460px,1fr));gap:14px;padding:14px;border-bottom:1px solid var(--line)}.dupe-pair{display:grid;grid-template-columns:minmax(0,1fr) 28px minmax(0,1fr);gap:8px;align-items:start}.pair-label{font-size:12px;color:var(--muted);font-weight:700;margin:0 0 6px}.pair-arrow{align-self:center;text-align:center;color:var(--muted);font-weight:700}.card{border:1px solid var(--line);border-radius:8px;overflow:hidden;background:#fff;min-width:0}.card.rep{border-color:#9bbcf8}.thumb{position:relative;background:#eef1f5;aspect-ratio:9/16;display:grid;place-items:center;overflow:hidden}.thumb img{width:100%;height:100%;object-fit:cover;display:block}.thumb .missing{color:var(--muted);padding:12px;text-align:center}.ribbon{position:absolute;left:8px;top:8px;border-radius:999px;padding:4px 8px;font-size:12px;font-weight:700;background:rgba(255,255,255,.92);border:1px solid rgba(0,0,0,.08)}.ribbon.keep{color:var(--keep)}.ribbon.drop{color:var(--danger)}.card-body{padding:10px 11px 12px}.card-title{font-weight:700;margin-bottom:6px;overflow-wrap:anywhere}.card-text{color:#3f4652;min-height:42px;overflow-wrap:anywhere}.kv{display:grid;grid-template-columns:72px 1fr;gap:4px 8px;margin-top:10px;color:var(--muted);font-size:12px}.kv div:nth-child(even){color:#323842;overflow-wrap:anywhere}.play-list{padding:0 14px 14px}.play-row{display:grid;grid-template-columns:minmax(220px,1.2fr) minmax(220px,1fr) 120px 120px;gap:12px;border-top:1px solid var(--line);padding:12px 0}.play-row:first-child{border-top:0}.play-main{font-weight:700}.play-sub{color:var(--muted);font-size:12px;margin-top:4px;overflow-wrap:anywhere}.empty{display:none;background:#fff;border:1px dashed var(--line);border-radius:8px;padding:24px;color:var(--muted);text-align:center}a{color:#0f5fb8;text-decoration:none}a:hover{text-decoration:underline}@media(max-width:900px){.stats{grid-template-columns:repeat(2,minmax(140px,1fr))}.cards{grid-template-columns:1fr}.play-pairs{grid-template-columns:1fr}.dupe-pair{grid-template-columns:1fr}.pair-arrow{display:none}.cluster-head{display:block}.badges{justify-content:flex-start;margin-top:10px}.play-row{grid-template-columns:1fr}}
.asset-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(290px,1fr));gap:12px;margin-bottom:18px}.asset-card{background:#fff;border:1px solid var(--line);border-radius:8px;padding:14px}.asset-card h3{font-size:15px;line-height:1.25;margin:0 0 8px}.asset-card p{margin:0 0 10px;color:#394150}.asset-meta{color:var(--muted);font-size:12px;overflow-wrap:anywhere}.asset-tags{display:flex;flex-wrap:wrap;gap:6px;margin-top:10px}.asset-tag{border-radius:999px;background:#edf0f5;color:#384152;padding:3px 8px;font-size:12px}
.funnel-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin:0 0 16px}.funnel-card{background:#fff;border:1px solid var(--line);border-radius:8px;padding:16px}.funnel-title{font-weight:800;font-size:15px;margin-bottom:10px}.funnel-count{display:flex;align-items:baseline;gap:10px;margin-bottom:12px}.funnel-count span,.funnel-count strong{font-size:30px;line-height:1}.funnel-count strong{color:var(--keep)}.funnel-count em{font-style:normal;color:var(--muted)}.funnel-reasons{display:flex;flex-wrap:wrap;gap:6px}.funnel-reason{display:inline-flex;gap:5px;align-items:center;border-radius:999px;background:#f0f2f6;color:#404855;padding:4px 8px;font-size:12px}.funnel-reason b{color:#17191f}.funnel-table-wrap{overflow:auto;background:#fff;border:1px solid var(--line);border-radius:8px;margin-bottom:20px}.funnel-table{width:100%;border-collapse:collapse;min-width:980px}.funnel-table th,.funnel-table td{padding:9px 10px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap}.funnel-table th:first-child,.funnel-table td:first-child{text-align:left;position:sticky;left:0;background:#fff;z-index:1}.funnel-table th{font-size:12px;color:var(--muted);background:#fbfcfd}.funnel-table th:first-child{background:#fbfcfd}.warn-note{border-color:#f1d488;background:#fffaf0}
"""


def _card_html(
    target_date: str,
    ad_key: str,
    row: dict[str, Any],
    ribbon: str,
    ribbon_cls: str,
    subtitle: str,
    extra: list[tuple[str, Any]],
) -> str:
    img = _local_image_path(target_date, ad_key)
    if not img:
        img = _cache_image(target_date, ad_key, _image_url(row))
    img_html = f'<img src="{_h(img)}" loading="lazy" alt="封面">' if img else '<div class="missing">无本地封面</div>'
    link = _media_url(row)
    open_a = f'<a href="{_h(link)}" target="_blank" rel="noreferrer">' if link else ""
    close_a = "</a>" if link else ""
    kv = [
        ("ad_key", ad_key),
        ("产品", row.get("product") or ""),
        ("First seen", _format_material_time(row.get("first_seen"))),
        ("Created at", _format_material_time(row.get("created_at"))),
        *extra,
    ]
    kv_html = "".join(f"<div>{_h(k)}</div><div>{_h(v)}</div>" for k, v in kv)
    return (
        f'<article class="card"><div class="thumb">{open_a}{img_html}{close_a}'
        f'<span class="ribbon {ribbon_cls}">{_h(ribbon)}</span></div>'
        f'<div class="card-body"><div class="card-title">{_h(_first_text(row))}</div>'
        f'<div class="card-text">{_h(subtitle)}</div><div class="kv">{kv_html}</div></div></article>'
    )


def _render_cover_section(
    *,
    title: str,
    groups: list[dict[str, Any]],
    mode: str,
    target_date: str,
    meta: dict[str, dict[str, Any]],
    today_meta: dict[str, dict[str, Any]],
) -> str:
    parts = [
        f'<h2 class="section-title">{_h(title)}</h2>',
        '<div class="note">筛选类型说明：今日同样素材筛选 = 目标日内部相似素材合并，只保留当日代表；历史筛选 = 命中目标日前历史素材或历史指纹。</div>',
    ]
    for idx, group in enumerate(groups, 1):
        kept_key = str(group["key"])
        members = group["members"]
        hist = meta.get(kept_key, {})
        appid = str(hist.get("appid") or members[0].get("appid") or "")
        product = str(hist.get("product") or members[0].get("product") or "")
        dates = sorted({str(m.get("matched_date") or m.get("first_seen_date") or hist.get("first_target_date") or "") for m in members})
        reasons = sorted({str(m.get("reason") or mode) for m in members})
        scope_counts: Counter[str] = Counter(
            _cover_filter_scope(m, target_date, m.get("matched_date") or m.get("first_seen_date") or hist.get("first_target_date") or "")
            for m in members
        )
        scope_label = _cover_group_scope_label(scope_counts)
        representative_ribbon = "今日代表" if scope_label == "今日同样素材筛选" else ("历史命中" if scope_label == "历史筛选" else "命中代表")
        representative_subtitle = "今日同样素材代表" if scope_label == "今日同样素材筛选" else ("历史筛选代表" if scope_label == "历史筛选" else "混合筛选代表")
        search_blob = " ".join(
            [
                product,
                appid,
                kept_key,
                _first_text(hist),
                " ".join(str(m.get("ad_key") or "") for m in members),
                " ".join(reasons),
                " ".join(scope_counts),
            ]
        ).lower()
        cards = [
            _card_html(
                target_date,
                kept_key,
                hist,
                representative_ribbon,
                "keep",
                representative_subtitle,
                [("筛选类型", scope_label), ("代表日期", hist.get("first_target_date") or ", ".join(dates)), ("展示", hist.get("best_all_exposure_value") or hist.get("exposure") or "-")],
            )
        ]
        for member in members:
            ad_key = str(member.get("ad_key") or "")
            row = dict(meta.get(ad_key) or today_meta.get(ad_key) or {})
            row.setdefault("ad_key", ad_key)
            row["product"] = row.get("product") or member.get("product") or product
            row["preview_img_url"] = row.get("preview_img_url") or member.get("preview_img_url") or member.get("cover_url") or ""
            row["image_url"] = row.get("image_url") or member.get("image_url") or ""
            row["video_url"] = normalize_video_url_for_consumption(str(row.get("video_url") or member.get("video_url") or ""))
            row["title"] = row.get("title") or member.get("title") or ""
            row["body"] = row.get("body") or member.get("body") or ""
            row["all_exposure_value"] = row.get("all_exposure_value") or member.get("all_exposure_value") or ""
            if not _image_url(row) and hist:
                row["preview_img_url"] = hist.get("preview_img_url") or hist.get("cover_url") or ""
                row["image_url"] = hist.get("image_url") or ""
                row["video_url"] = normalize_video_url_for_consumption(str(hist.get("video_url") or ""))
                row["title"] = row.get("title") or "同簇代表封面"
            today_date = row.get("target_date") or member.get("target_date") or target_date
            matched_date = member.get("matched_date") or member.get("first_seen_date") or hist.get("first_target_date") or ""
            filter_scope = _cover_filter_scope(member, target_date, matched_date)
            match_date_label = "今日代表日期" if filter_scope == "今日同样素材筛选" else "历史命中日期"
            extra = [
                ("筛选类型", filter_scope),
                ("今日素材日期", today_date),
                (match_date_label, matched_date),
                ("命中", kept_key),
                ("原因", member.get("reason") or ""),
            ]
            if member.get("similarity") not in (None, ""):
                extra.append(("相似度", member.get("similarity")))
            cards.append(_card_html(target_date, ad_key, row, "今日剔除", "drop", filter_scope, extra))
        parts.append(
            f'<section class="cluster" data-search="{_h(search_blob)}">'
            f'<div class="cluster-head"><div><div class="cluster-title">{_h(mode)} 簇 {idx:02d} · {_h(scope_label)} · {_h(product)}</div>'
            f'<div class="cluster-meta">appid: {_h(appid)} · 证据日期: {_h(", ".join(d for d in dates if d) or "-")}</div></div>'
            f'<div class="badges"><span class="badge keep">{_h(representative_ribbon)} {_h(_short_key(kept_key))}</span>'
            f'<span class="badge warn">今日同样素材筛选 {scope_counts.get("今日同样素材筛选", 0)} 条</span>'
            f'<span class="badge reason">历史筛选 {scope_counts.get("历史筛选", 0)} 条</span>'
            f'<span class="badge drop">今日剔除 {len(members)} 条</span>'
            f'<span class="badge reason">{_h(", ".join(reasons))}</span></div></div>'
            f'<div class="cards">{"".join(cards)}</div></section>'
        )
    return "".join(parts)


def _render_one_liner_section(
    groups: list[dict[str, Any]],
    target_date: str,
    meta: dict[str, dict[str, Any]],
    today_meta: dict[str, dict[str, Any]],
    cover_hit_by_ad: dict[str, str],
) -> str:
    parts = ['<h2 class="section-title">一句话 / 玩法筛选剔除</h2>']
    for idx, group in enumerate(groups, 1):
        key = str(group["key"])
        members = group["members"]
        kinds = sorted({str(m.get("kind") or "") for m in members})
        unique_keys = {str(m.get("ad_key") or "") for m in members if m.get("ad_key")}
        unique_materials = len(unique_keys)
        covered_keys = {ad_key for ad_key in unique_keys if ad_key in cover_hit_by_ad}
        fallback_keys = unique_keys - covered_keys
        cluster_cls = "cluster covered-play" if covered_keys and not fallback_keys else "cluster"
        search_blob = " ".join(
            [
                key,
                " ".join(kinds),
                " ".join(str(m.get("ad_key") or "") for m in members),
                " ".join(str(_reference_ad_key(m)) for m in members),
                " ".join(str(m.get("effect_one_liner") or "") for m in members),
                " ".join(str(m.get("matched_effect_one_liner") or "") for m in members),
                " ".join(str(m.get("_play_asset_name") or "") for m in members),
            ]
        ).lower()
        pair_html = []
        card_keys: set[str] = set()
        for member in members:
            ad_key = str(member.get("ad_key") or "")
            if not ad_key or ad_key in card_keys:
                continue
            card_keys.add(ad_key)
            row = dict(meta.get(ad_key) or today_meta.get(ad_key) or {})
            row.setdefault("ad_key", ad_key)
            row["product"] = row.get("product") or member.get("product") or ""
            row["effect_one_liner"] = row.get("effect_one_liner") or member.get("effect_one_liner") or ""
            row["play_fingerprint"] = row.get("play_fingerprint") or member.get("play_fingerprint") or ""
            evidences = [m for m in members if str(m.get("ad_key") or "") == ad_key]
            evidence_kinds = sorted({str(m.get("kind") or "") for m in evidences if m.get("kind")})
            similarities = [
                str(m.get("similarity"))
                for m in evidences
                if m.get("similarity") not in (None, "")
            ]
            cover_hit = cover_hit_by_ad.get(ad_key, "")
            status = f"封面已覆盖：{cover_hit}" if cover_hit else "玩法兜底"
            family = _play_family(row, member)
            ref_key = _reference_ad_key(member)
            extra = [
                ("命中", ref_key or key),
                ("玩法资产", member.get("_play_asset_name") or "待沉淀"),
                ("子标签", member.get("_play_asset_subtags") or "-"),
                ("玩法族", family),
                ("类型", ", ".join(evidence_kinds) or "-"),
                ("当前口径", status),
                ("封面CLIP", member.get("cover_similarity_note") or "-"),
            ]
            if similarities:
                extra.append(("相似度", ", ".join(similarities[:3])))
            drop_card = _card_html(
                target_date,
                ad_key,
                row,
                "封面已覆盖" if cover_hit else "一句话剔除",
                "keep" if cover_hit else "drop",
                " / ".join(evidence_kinds) or "玩法重复",
                extra,
            )
            if ref_key:
                ref_row = dict(meta.get(ref_key) or today_meta.get(ref_key) or {})
                ref_row.setdefault("ad_key", ref_key)
                ref_row["product"] = ref_row.get("product") or row.get("product") or member.get("product") or ""
                ref_row["effect_one_liner"] = ref_row.get("effect_one_liner") or member.get("matched_effect_one_liner") or ""
                ref_row["play_fingerprint"] = ref_row.get("play_fingerprint") or member.get("matched_play_fingerprint") or ""
                ref_extra = [
                    ("命中素材", ad_key),
                    ("玩法资产", member.get("_play_asset_name") or "待沉淀"),
                    ("子标签", member.get("_play_asset_subtags") or "-"),
                    ("日期", member.get("matched_date") or ref_row.get("first_target_date") or target_date),
                    ("类型", ", ".join(evidence_kinds) or "-"),
                    ("封面CLIP", member.get("cover_similarity_note") or "-"),
                ]
                if similarities:
                    ref_extra.append(("相似度", ", ".join(similarities[:3])))
                ref_card = _card_html(
                    target_date,
                    ref_key,
                    ref_row,
                    "对比对象",
                    "keep",
                    "去重命中的参照素材",
                    ref_extra,
                )
            else:
                matched_text = member.get("matched_play_fingerprint") or member.get("matched_effect_one_liner") or key
                ref_card = (
                    '<article class="card"><div class="thumb"><div class="missing">无参照封面</div>'
                    '<span class="ribbon keep">对比对象</span></div><div class="card-body">'
                    f'<div class="card-title">{_h(matched_text)}</div>'
                    '<div class="card-text">未定位到参照素材 ad_key</div>'
                    f'<div class="kv"><div>命中素材</div><div>{_h(ad_key)}</div>'
                    f'<div>类型</div><div>{_h(", ".join(evidence_kinds) or "-")}</div></div>'
                    '</div></article>'
                )
            pair_html.append(
                '<div class="dupe-pair">'
                '<div><div class="pair-label">被剔除素材</div>'
                f'{drop_card}</div>'
                '<div class="pair-arrow">vs</div>'
                '<div><div class="pair-label">对比对象</div>'
                f'{ref_card}</div></div>'
            )
        rows_html = []
        for member in members:
            ad_key = str(member.get("ad_key") or "")
            row = meta.get(ad_key) or today_meta.get(ad_key) or {}
            effect = member.get("play_fingerprint") or member.get("effect_one_liner") or _first_text(row)
            matched = member.get("matched_play_fingerprint") or member.get("matched_effect_one_liner") or key
            cover_hit = cover_hit_by_ad.get(ad_key, "")
            family = _play_family(row, member)
            route_badge = (
                '<span class="badge keep">封面已覆盖</span>'
                if cover_hit
                else '<span class="badge warn">玩法兜底</span>'
            )
            rows_html.append(
                '<div class="play-row">'
                f'<div><div class="play-main">{_h(effect)}</div><div class="play-sub">{_h(ad_key)} · {_h(member.get("product") or row.get("product") or "")} · 资产：{_h(member.get("_play_asset_name") or "待沉淀")} · 玩法族：{_h(family)}</div></div>'
                f'<div><div class="play-main">{_h(matched)}</div><div class="play-sub">命中：{_h(_reference_ad_key(member) or key)}</div></div>'
                f'<div><span class="badge drop">{_h(member.get("kind") or "")}</span>'
                f'{route_badge}</div>'
                f'<div><div>{_h(member.get("similarity") or "-")}</div><div class="play-sub">阈值 {_h(member.get("threshold") or "-")}</div><div class="play-sub">{_h(member.get("cover_similarity_note") or "")}</div></div>'
                '</div>'
            )
        parts.append(
            f'<section class="{cluster_cls}" data-search="{_h(search_blob)}">'
            f'<div class="cluster-head"><div><div class="cluster-title">玩法簇 {idx:02d} · {_h(key)}</div>'
            f'<div class="cluster-meta">类型：{_h(", ".join(kinds))}</div></div>'
            f'<div class="badges"><span class="badge drop">剔除 {unique_materials or len(members)} 条</span>'
            f'<span class="badge keep">封面覆盖 {len(covered_keys)} 条</span>'
            f'<span class="badge warn">玩法兜底 {len(fallback_keys)} 条</span>'
            f'<span class="badge reason">证据 {len(members)} 条</span>'
            f'<span class="badge warn">一句话筛选</span></div></div>'
            f'<div class="play-pairs">{"".join(pair_html)}</div>'
            f'<div class="play-list">{"".join(rows_html)}</div></section>'
        )
    return "".join(parts)


def _merge_item_meta(item: dict[str, Any], meta: dict[str, dict[str, Any]], today_meta: dict[str, dict[str, Any]]) -> dict[str, Any]:
    ad_key = str(item.get("ad_key") or "")
    row = dict(meta.get(ad_key) or today_meta.get(ad_key) or {})
    row.setdefault("ad_key", ad_key)
    for key in (
        "product",
        "appid",
        "platform",
        "creative_type",
        "advertiser_name",
        "page_name",
        "title",
        "body",
        "preview_img_url",
        "image_url",
        "video_url",
        "video_duration",
        "effect_one_liner",
        "play_fingerprint",
        "ad_one_liner",
        "best_all_exposure_value",
        "best_impression",
        "all_exposure_value",
        "first_seen",
        "created_at",
    ):
        value = item.get(key)
        if value not in (None, ""):
            row[key] = value
    return row


def _render_material_section(
    *,
    title: str,
    rows: list[dict[str, Any]],
    target_date: str,
    meta: dict[str, dict[str, Any]],
    today_meta: dict[str, dict[str, Any]],
    ribbon: str,
    ribbon_cls: str,
    empty_text: str,
    extra_builder: Any | None = None,
) -> str:
    parts = [f'<h2 class="section-title">{_h(title)}</h2>']
    if not rows:
        parts.append(f'<div class="note">{_h(empty_text)}</div>')
        return "".join(parts)
    by_product: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in rows:
        row = _merge_item_meta(item, meta, today_meta)
        product = str(row.get("product") or item.get("product") or "未知")
        by_product[product].append({**item, "_row": row})
    for product, items in sorted(by_product.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        search_blob = " ".join(
            [
                product,
                " ".join(str(item.get("ad_key") or "") for item in items),
                " ".join(str(item.get("_play_asset_name") or "") for item in items),
                " ".join(str((item.get("_row") or {}).get("effect_one_liner") or "") for item in items),
            ]
        ).lower()
        cards: list[str] = []
        for item in items:
            row = item.get("_row") or {}
            ad_key = str(row.get("ad_key") or item.get("ad_key") or "")
            extra = extra_builder(item, row) if extra_builder else []
            cards.append(_card_html(target_date, ad_key, row, ribbon, ribbon_cls, _first_text(row), extra))
        parts.append(
            f'<section class="cluster" data-search="{_h(search_blob)}">'
            f'<div class="cluster-head"><div><div class="cluster-title">{_h(product)}</div>'
            f'<div class="cluster-meta">{len(items)} 条素材</div></div>'
            f'<div class="badges"><span class="badge {ribbon_cls}">{_h(ribbon)}</span></div></div>'
            f'<div class="cards">{"".join(cards)}</div></section>'
        )
    return "".join(parts)


def _render_advertiser_grouped_material_section(
    *,
    title: str,
    rows: list[dict[str, Any]],
    target_date: str,
    meta: dict[str, dict[str, Any]],
    today_meta: dict[str, dict[str, Any]],
    ribbon: str,
    ribbon_cls: str,
    empty_text: str,
) -> str:
    parts = [f'<h2 class="section-title">{_h(title)}</h2>']
    if not rows:
        parts.append(f'<div class="note">{_h(empty_text)}</div>')
        return "".join(parts)

    by_advertiser: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in rows:
        row = _merge_item_meta(item, meta, today_meta)
        advertiser = str(
            row.get("advertiser_name")
            or item.get("advertiser_name")
            or row.get("page_name")
            or item.get("page_name")
            or "未知广告主"
        ).strip() or "未知广告主"
        by_advertiser[advertiser].append({**item, "_row": row})

    for advertiser, items in sorted(by_advertiser.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        products = sorted({str((item.get("_row") or {}).get("product") or item.get("product") or "未知") for item in items})
        search_blob = " ".join(
            [
                advertiser,
                " ".join(products),
                " ".join(str(item.get("ad_key") or "") for item in items),
                " ".join(str((item.get("_row") or {}).get("effect_one_liner") or "") for item in items),
                " ".join(str((item.get("_row") or {}).get("play_fingerprint") or "") for item in items),
            ]
        ).lower()
        cards: list[str] = []
        for item in items:
            row = item.get("_row") or {}
            ad_key = str(row.get("ad_key") or item.get("ad_key") or "")
            extra = [
                ("状态", item.get("_review_status") or ribbon),
                ("玩法资产", item.get("_play_asset_name") or "待沉淀"),
                ("子标签", item.get("_play_asset_subtags") or "-"),
                ("玩法族", _play_family(row, item)),
                ("展示", row.get("best_all_exposure_value") or row.get("all_exposure_value") or row.get("best_impression") or "-"),
            ]
            cards.append(_card_html(target_date, ad_key, row, ribbon, ribbon_cls, _first_text(row), extra))
        parts.append(
            f'<section class="cluster" data-search="{_h(search_blob)}">'
            f'<div class="cluster-head"><div><div class="cluster-title">{_h(advertiser)}</div>'
            f'<div class="cluster-meta">{len(items)} 条素材 · 产品：{_h(", ".join(products))}</div></div>'
            f'<div class="badges"><span class="badge {ribbon_cls}">{_h(ribbon)} {len(items)} 条</span></div></div>'
            f'<div class="cards">{"".join(cards)}</div></section>'
        )
    return "".join(parts)


def _render_three_layer_funnel_section(
    *,
    target_date: str,
    crawl_report: dict[str, Any],
    business_gate: dict[str, Any],
    bitable_counts: dict[str, Any],
) -> str:
    summary = crawl_report.get("summary") or {}
    crawl_removed = summary.get("removed_reasons") or {}
    after_crawl = int(summary.get("kept_after_crawl_filter") or 0)
    after_cover = int(summary.get("kept_after_cover_filter") or 0)
    cover_fingerprint = int(crawl_removed.get("cover_crossday_fingerprint") or 0)
    cover_clip_crossday = int(crawl_removed.get("cover_clip_crossday") or 0)
    cover_clip_intraday = int(crawl_removed.get("cover_clip_intraday") or 0)
    hard_rows = business_gate.get("hard_excluded_rows") or []
    template_skipped = business_gate.get("template_skipped_rows") or []
    after_hard = len(business_gate.get("after_hard_items") or [])
    would_enter = len(business_gate.get("template_kept_items") or [])
    if not would_enter and business_gate.get("after_hard_items") and not template_skipped:
        would_enter = len(business_gate.get("after_hard_items") or [])
    actual_count = bitable_counts.get("count")
    actual_label = str(actual_count) if actual_count is not None else "未读取"
    bitable_note = ""
    if actual_count is None:
        bitable_note = f"多维表读取失败：{bitable_counts.get('error') or 'unknown'}"
    elif int(actual_count) != int(would_enter):
        bitable_note = "多维表实际数和本地应入表数不一致，通常表示今天还没同步或同步中断。"
    else:
        bitable_note = "多维表实际数和本地应入表数一致。"

    focus_cards = [
        (
            "封面图去重",
            after_crawl,
            after_cover,
            [
                ("跨日指纹", cover_fingerprint),
                ("跨日 CLIP", cover_clip_crossday),
                ("日内 CLIP", cover_clip_intraday),
            ],
        ),
        (
            "同玩法同模板去重",
            after_hard,
            would_enter,
            [
                ("同玩法同模板", len(template_skipped)),
                ("封面阈值", os.getenv("BITABLE_TEMPLATE_DEDUP_CLIP_THRESHOLD") or "0.70"),
                ("文本阈值", "关闭" if not _env_enabled("BITABLE_TEMPLATE_DEDUP_TEXT_SIMILARITY_ENABLED", "0") else os.getenv("BITABLE_TEMPLATE_DEDUP_TEXT_SIMILARITY_THRESHOLD") or "0.78"),
            ],
        ),
        (
            "多维表对账",
            would_enter,
            actual_label,
            [
                ("业务硬拦", len(hard_rows)),
                ("扫描记录", bitable_counts.get("scanned") or 0),
                ("差值", (would_enter - int(actual_count)) if actual_count is not None else "-"),
            ],
        ),
    ]
    cards_html = ""
    for title, start, kept, reasons in focus_cards:
        reason_html = "".join(
            f'<span class="funnel-reason"><b>{_h(value)}</b>{_h(label)}</span>' for label, value in reasons
        )
        cards_html += (
            f'<article class="funnel-card">'
            f'<div class="funnel-title">{_h(title)}</div>'
            f'<div class="funnel-count"><span>{_h(start)}</span><em>→</em><strong>{_h(kept)}</strong></div>'
            f'<div class="funnel-reasons">{reason_html}</div>'
            f'</article>'
        )

    business_by_product: dict[str, Counter[str]] = defaultdict(Counter)
    for row in hard_rows:
        business_by_product[str(row.get("product") or "未知产品")]["hard"] += 1
    for row in template_skipped:
        business_by_product[str(row.get("product") or "未知产品")]["template"] += 1
    for item in business_gate.get("template_kept_items") or []:
        business_by_product[_item_product(item)]["would_enter"] += 1
    for row in business_gate.get("successful_rows") or []:
        business_by_product[str(row.get("product") or "未知产品")]["analyzed"] += 1

    actual_by_product = bitable_counts.get("by_product") or {}
    products = {
        str(row.get("product") or "未知产品")
        for row in crawl_report.get("per_product") or []
        if isinstance(row, dict)
    }
    products.update(business_by_product)
    products.update(str(k) for k in actual_by_product)

    product_rows = []
    by_product_report = {
        str(row.get("product") or "未知产品"): row
        for row in crawl_report.get("per_product") or []
        if isinstance(row, dict)
    }
    for product in sorted(products):
        row = by_product_report.get(product) or {}
        removed = row.get("removed_reasons") or {}
        cover_removed = row.get("cover_removed_reasons") or {}
        product_rows.append(
            "<tr>"
            f"<td>{_h(product)}</td>"
            f"<td>{_h(row.get('kept_after_crawl_filter') or 0)}</td>"
            f"<td>{_h(cover_removed.get('cover_crossday_fingerprint') or removed.get('cover_crossday_fingerprint') or 0)}</td>"
            f"<td>{_h(cover_removed.get('cover_clip_crossday') or removed.get('cover_clip_crossday') or 0)}</td>"
            f"<td>{_h(cover_removed.get('cover_clip_intraday') or removed.get('cover_clip_intraday') or 0)}</td>"
            f"<td>{_h(row.get('kept_after_cover_filter') or 0)}</td>"
            f"<td>{_h(business_by_product.get(product, Counter()).get('hard') or 0)}</td>"
            f"<td>{_h(business_by_product.get(product, Counter()).get('template') or 0)}</td>"
            f"<td>{_h(business_by_product.get(product, Counter()).get('would_enter') or 0)}</td>"
            f"<td>{_h(actual_by_product.get(product, 0) if actual_count is not None else '-')}</td>"
            "</tr>"
        )
    product_table = (
        '<div class="funnel-table-wrap"><table class="funnel-table">'
        "<thead><tr>"
        "<th>产品</th><th>封面前</th><th>指纹</th><th>跨日CLIP</th><th>日内CLIP</th>"
        "<th>第二层后</th><th>业务硬拦</th><th>同模板</th><th>应入表</th><th>表内实际</th>"
        "</tr></thead><tbody>"
        + "".join(product_rows)
        + "</tbody></table></div>"
    )
    error_note = ""
    if business_gate.get("error"):
        error_note = f'<div class="note warn-note">入表前业务层模拟不完整：{_h(business_gate.get("error"))}</div>'
    return (
        '<h2 class="section-title">核心筛选看板（封面图 + 同玩法同模板）</h2>'
        f'<div class="note">口径日期：{_h(target_date)}。第一层爬取资格已收起；表里的“封面前”指已经通过广告主、日期和重投过滤的素材。{_h(bitable_note)}</div>'
        f'<div class="funnel-grid">{cards_html}</div>'
        f"{error_note}{product_table}"
    )


def _annotate_play_assets(
    rows: list[dict[str, Any]],
    *,
    meta: dict[str, dict[str, Any]],
    today_meta: dict[str, dict[str, Any]],
    assets: list[dict[str, Any]],
) -> None:
    asset_by_id = {str(asset.get("asset_id") or ""): asset for asset in assets if isinstance(asset, dict)}

    def _copy_ai_play_fields(item: dict[str, Any], row: dict[str, Any]) -> bool:
        asset_id = str(item.get("play_asset_id") or row.get("play_asset_id") or "").strip()
        asset_name = str(item.get("play_asset_name") or row.get("play_asset_name") or "").strip()
        if not asset_id or asset_id == "new_play":
            if asset_name:
                item["_play_asset_id"] = ""
                item["_play_asset_name"] = asset_name
                item["_play_asset_confidence"] = str(
                    item.get("play_asset_confidence")
                    or row.get("play_asset_confidence")
                    or item.get("play_asset_match_source")
                    or row.get("play_asset_match_source")
                    or "AI"
                )
                item["_play_asset_keywords"] = str(
                    item.get("play_asset_matched_keywords")
                    or row.get("play_asset_matched_keywords")
                    or ""
                )
                item["_play_asset_subtags"] = str(
                    item.get("play_asset_subtag_names")
                    or row.get("play_asset_subtag_names")
                    or ""
                )
                return True
            return False
        asset = asset_by_id.get(asset_id)
        if not asset and not asset_name:
            return False
        item["_play_asset_id"] = asset_id if asset else ""
        item["_play_asset_name"] = asset_name or str(asset.get("name") or "待沉淀")
        item["_play_asset_confidence"] = str(
            item.get("play_asset_confidence")
            or row.get("play_asset_confidence")
            or item.get("play_asset_match_source")
            or row.get("play_asset_match_source")
            or "AI"
        )
        item["_play_asset_keywords"] = str(
            item.get("play_asset_matched_keywords")
            or row.get("play_asset_matched_keywords")
            or ""
        )
        item["_play_asset_subtags"] = str(
            item.get("play_asset_subtag_names")
            or row.get("play_asset_subtag_names")
            or ""
        )
        return True

    for item in rows:
        row = _merge_item_meta(item, meta, today_meta)
        if _copy_ai_play_fields(item, row):
            continue
        match = match_play_asset(row, assets=assets, evidence=item)
        if not match:
            item["_play_asset_id"] = ""
            item["_play_asset_name"] = "待沉淀"
            item["_play_asset_confidence"] = ""
            item["_play_asset_keywords"] = ""
            item["_play_asset_subtags"] = ""
            continue
        item["_play_asset_id"] = match.get("asset_id") or ""
        item["_play_asset_name"] = match.get("name") or "待沉淀"
        item["_play_asset_confidence"] = match.get("confidence") or ""
        item["_play_asset_score"] = match.get("score") or ""
        item["_play_asset_keywords"] = ", ".join(str(x) for x in match.get("matched_keywords") or [])
        item["_play_asset_subtags"] = ", ".join(
            str(x.get("name") or "")
            for x in match.get("matched_subtags") or []
            if isinstance(x, dict) and x.get("name")
        )
        if item.get("play_asset_name"):
            item["_play_asset_name"] = item.get("play_asset_name")
        if item.get("play_asset_confidence"):
            item["_play_asset_confidence"] = item.get("play_asset_confidence")
        if item.get("play_asset_matched_keywords"):
            item["_play_asset_keywords"] = item.get("play_asset_matched_keywords")
        if item.get("play_asset_subtag_names"):
            item["_play_asset_subtags"] = item.get("play_asset_subtag_names")


def _render_play_asset_library_section(assets: list[dict[str, Any]]) -> str:
    source_title = "旧 JSON 玩法资产库" if legacy_play_library_enabled() else f'多维表玩法标签库（字段：{os.getenv("VE_PLAY_LABEL_FIELD_NAME") or "玩法"}）'
    parts = [f'<h2 class="section-title">{_h(source_title)}</h2>']
    if not assets:
        parts.append('<div class="note">还没有读取到玩法标签。</div>')
        return "".join(parts)
    if not legacy_play_library_enabled():
        parts.append('<div class="note">这里的标签来自多维表格「玩法」字段；看板和分析阶段都会优先使用这些标签，旧 JSON 玩法库不作为默认来源。</div>')
    cards: list[str] = []
    for asset in assets:
        tags = [
            *[str(x) for x in asset.get("aliases") or []][:3],
            *[str(x) for x in asset.get("variant_dimensions") or []][:3],
            *[str(x.get("name") or "") for x in asset.get("subtags") or [] if isinstance(x, dict)][:5],
        ]
        tags_html = "".join(f'<span class="asset-tag">{_h(tag)}</span>' for tag in tags if tag)
        reps = ", ".join(_short_key(x, 8) for x in asset.get("representative_ad_keys") or [])
        search_blob = " ".join(
            [
                str(asset.get("name") or ""),
                str(asset.get("definition") or ""),
                " ".join(str(x) for x in asset.get("aliases") or []),
                " ".join(str(x) for x in asset.get("include_keywords") or []),
                " ".join(str(x.get("name") or "") for x in asset.get("subtags") or [] if isinstance(x, dict)),
                " ".join(" ".join(str(y) for y in (x.get("keywords") or [])) for x in asset.get("subtags") or [] if isinstance(x, dict)),
                " ".join(str(x) for x in asset.get("representative_ad_keys") or []),
            ]
        ).lower()
        cards.append(
            f'<article class="asset-card" data-search="{_h(search_blob)}">'
            f'<h3>{_h(asset.get("name") or "")}</h3>'
            f'<p>{_h(asset.get("definition") or "")}</p>'
            f'<div class="asset-meta">代表素材：{_h(reps or "-")}</div>'
            f'<div class="asset-meta">沉淀日期：{_h(", ".join(str(x) for x in asset.get("source_dates") or []) or "-")}</div>'
            f'<div class="asset-tags">{tags_html}</div></article>'
        )
    return "".join(parts) + f'<div class="asset-grid">{"".join(cards)}</div>'


def _render_label_table_section(
    *,
    title: str,
    rows: list[dict[str, Any]],
    meta: dict[str, dict[str, Any]],
    today_meta: dict[str, dict[str, Any]],
    empty_text: str,
) -> str:
    if not rows:
        return f'<h2 class="section-title">{_h(title)}</h2><div class="note">{_h(empty_text)}</div>'
    sorted_rows = sorted(
        rows,
        key=lambda item: (
            str(item.get("_play_asset_name") if item.get("_play_asset_id") else "待沉淀"),
            str(item.get("product") or ""),
            str(item.get("ad_key") or ""),
        ),
    )
    label_counts = Counter(
        str(item.get("_play_asset_name") if item.get("_play_asset_id") else "待沉淀")
        for item in sorted_rows
    )
    chips = "".join(
        f'<span class="chip">{_h(label)}：{_h(count)}</span>'
        for label, count in sorted(label_counts.items(), key=lambda kv: (kv[0] == "待沉淀", -kv[1], kv[0]))
    )
    trs: list[str] = []
    search_parts: list[str] = []
    for item in sorted_rows:
        row = _merge_item_meta(item, meta, today_meta)
        label = str(item.get("_play_asset_name") if item.get("_play_asset_id") else "待沉淀")
        ai_suggestion = "" if item.get("_play_asset_id") else str(item.get("_play_asset_name") or "")
        if ai_suggestion == "待沉淀":
            ai_suggestion = ""
        product = str(row.get("product") or item.get("product") or "未知")
        ad_key = str(item.get("ad_key") or row.get("ad_key") or "")
        effect = str(row.get("effect_one_liner") or item.get("effect_one_liner") or "")
        play_fp = str(row.get("play_fingerprint") or item.get("play_fingerprint") or "")
        template = str(row.get("template_fingerprint") or item.get("template_fingerprint") or "")
        source = "多维表标签" if item.get("_play_asset_id") else "待沉淀/new_play"
        search_parts.extend([label, ai_suggestion, product, ad_key, effect, play_fp, template])
        trs.append(
            "<tr>"
            f"<td>{_h(label)}</td>"
            f"<td>{_h(ai_suggestion or '-')}</td>"
            f"<td>{_h(product)}</td>"
            f"<td>{_h(ad_key)}</td>"
            f"<td>{_h(effect or '-')}</td>"
            f"<td>{_h(play_fp or '-')}</td>"
            f"<td>{_h(template or '-')}</td>"
            f"<td>{_h(source)}</td>"
            "</tr>"
        )
    return (
        f'<h2 class="section-title">{_h(title)}</h2>'
        f'<div class="chips">{chips}</div>'
        f'<section class="cluster" data-search="{_h(" ".join(search_parts).lower())}">'
        '<div class="funnel-table-wrap"><table class="funnel-table">'
        '<thead><tr><th>玩法标签</th><th>AI建议玩法</th><th>产品</th><th>广告ID</th><th>核心卖点</th><th>玩法指纹</th><th>模板指纹</th><th>来源</th></tr></thead>'
        f'<tbody>{"".join(trs)}</tbody></table></div></section>'
    )


def _render_asset_grouped_material_section(
    *,
    title: str,
    rows: list[dict[str, Any]],
    target_date: str,
    meta: dict[str, dict[str, Any]],
    today_meta: dict[str, dict[str, Any]],
    ribbon: str,
    ribbon_cls: str,
    empty_text: str,
) -> str:
    parts = [f'<h2 class="section-title">{_h(title)}</h2>']
    if not rows:
        parts.append(f'<div class="note">{_h(empty_text)}</div>')
        return "".join(parts)
    by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in rows:
        row = _merge_item_meta(item, meta, today_meta)
        asset_name = str(item.get("_play_asset_name") or "待沉淀")
        by_asset[asset_name].append({**item, "_row": row})
    for asset_name, items in sorted(by_asset.items(), key=lambda kv: (kv[0] == "待沉淀", -len(kv[1]), kv[0])):
        products = sorted({str((item.get("_row") or {}).get("product") or item.get("product") or "未知") for item in items})
        search_blob = " ".join(
            [
                asset_name,
                " ".join(products),
                " ".join(str(item.get("ad_key") or "") for item in items),
                " ".join(str(item.get("_play_asset_name") or "") for item in items),
                " ".join(str(item.get("_play_asset_subtags") or "") for item in items),
                " ".join(str((item.get("_row") or {}).get("effect_one_liner") or "") for item in items),
                " ".join(str((item.get("_row") or {}).get("play_fingerprint") or "") for item in items),
            ]
        ).lower()
        cards: list[str] = []
        for item in items:
            row = item.get("_row") or {}
            ad_key = str(row.get("ad_key") or item.get("ad_key") or "")
            extra = [
                ("玩法资产", asset_name),
                ("子标签", item.get("_play_asset_subtags") or "-"),
                ("置信", item.get("_play_asset_confidence") or "-"),
                ("命中词", item.get("_play_asset_keywords") or "-"),
                ("玩法族", _play_family(row, item)),
                ("展示", row.get("best_all_exposure_value") or row.get("all_exposure_value") or row.get("best_impression") or "-"),
            ]
            cards.append(_card_html(target_date, ad_key, row, ribbon, ribbon_cls, _first_text(row), extra))
        badge_cls = "warn" if asset_name == "待沉淀" else "reason"
        parts.append(
            f'<section class="cluster" data-search="{_h(search_blob)}">'
            f'<div class="cluster-head"><div><div class="cluster-title">{_h(asset_name)}</div>'
            f'<div class="cluster-meta">{len(items)} 条素材 · 产品：{_h(", ".join(products))}</div></div>'
            f'<div class="badges"><span class="badge {badge_cls}">{_h(asset_name)}</span>'
            f'<span class="badge {ribbon_cls}">{_h(ribbon)} {len(items)} 条</span></div></div>'
            f'<div class="cards">{"".join(cards)}</div></section>'
        )
    return "".join(parts)


def _db_exclude_flags(target_date: str) -> dict[str, dict[str, Any]]:
    conn = sqlite3.connect(DATA_DIR / "video_enhancer_pipeline.db")
    conn.row_factory = sqlite3.Row
    out: dict[str, dict[str, Any]] = {}
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ad_key, exclude_from_bitable, exclude_from_cluster, material_tags
            FROM daily_creative_insights
            WHERE target_date = ?
            """,
            (target_date,),
        )
        for row in cur.fetchall():
            out[str(row["ad_key"] or "")] = dict(row)
        return out
    finally:
        conn.close()


def write_filter_review_dashboard(
    target_date: str,
    *,
    output_path: Path | None = None,
    write_legacy_cover_path: bool = True,
) -> Path:
    if legacy_play_library_enabled():
        maybe_pull_play_asset_doc()
    prefix = _output_prefix(target_date)
    cover_report = _read_json(DATA_DIR / f"{prefix}_cover_style_intraday.json")
    raw_payload = _read_json(DATA_DIR / f"{prefix}_raw.json")
    analysis_payload = _read_json(DATA_DIR / f"video_analysis_{prefix}_raw.json")
    crawl_report = _read_json(DATA_DIR / f"{prefix}_crawl_product_retention.json")

    product_by_appid = {
        str(row.get("appid") or ""): str(row.get("product") or "")
        for row in cover_report.get("per_appid") or []
        if isinstance(row, dict)
    }
    today_meta = _raw_today_rows(raw_payload, product_by_appid)
    analysis_by_ad = _analysis_rows(analysis_payload)

    threshold = _cover_visual_threshold()
    lookback = int(cover_report.get("cross_day_history_lookback_days") or os.getenv("COVER_STYLE_HISTORY_LOOKBACK_DAYS") or 7)
    history_dates = cover_report.get("cross_day_history_dates") or _history_reference_dates(target_date, lookback)
    clip_rows = _merge_cover_clip_rows(
        _cover_report_clip_removed(cover_report),
        _compute_clip_crossday_preview(target_date, today_meta, product_by_appid, threshold, list(history_dates)),
    )
    fingerprint_rows = _cover_fingerprint_rows(cover_report)
    one_liner_rows = _one_liner_removed_rows(analysis_by_ad)
    _resolve_one_liner_reference_keys(one_liner_rows, target_date)
    _annotate_one_liner_cover_similarity(one_liner_rows, threshold)
    daily_report = build_daily_asset_variant_report(target_date, lookback_days=lookback)
    daily_summary = daily_report.get("summary") or {}
    business_gate = _simulate_business_gate(target_date, raw_payload, analysis_payload)
    bitable_counts = _bitable_counts_for_date(target_date)

    needed_keys: set[str] = set(today_meta) | set(analysis_by_ad)
    report_meta: dict[str, dict[str, Any]] = {}
    for rows in (clip_rows, fingerprint_rows, one_liner_rows):
        for row in rows:
            ad_key = str(row.get("ad_key") or "")
            if ad_key:
                report_meta[ad_key] = row
            for key in ("ad_key", "kept_ad_key", "matched_ad_key"):
                value = str(row.get(key) or "")
                if value and len(value) >= 12:
                    needed_keys.add(value)
    for key in (
        "candidate_items",
        "new_items",
        "new_play_items",
        "old_play_items",
        "unknown_play_items",
        "asset_variant_items",
        "new_asset_variant_items",
    ):
        for item in daily_report.get(key) or []:
            ad_key = str(item.get("ad_key") or "")
            if ad_key:
                needed_keys.add(ad_key)
    for item in business_gate.get("hard_excluded_rows") or []:
        ad_key = str(item.get("ad_key") or "")
        if ad_key:
            needed_keys.add(ad_key)
    for item in business_gate.get("template_kept_items") or []:
        if not isinstance(item, dict):
            continue
        creative = item.get("creative") or {}
        ad_key = str(item.get("ad_key") or (creative.get("ad_key") if isinstance(creative, dict) else "") or "")
        if ad_key:
            needed_keys.add(ad_key)
    for item in business_gate.get("template_skipped_rows") or []:
        for key in ("ad_key", "kept_ad_key", "match_ad_key"):
            ad_key = str(item.get(key) or "")
            if ad_key:
                needed_keys.add(ad_key)
    meta = _merge_meta(today_meta, report_meta, _query_creative_meta(needed_keys), analysis_by_ad)

    clip_groups = _group_by_kept(clip_rows)
    fingerprint_groups = _group_by_kept(fingerprint_rows)
    one_liner_groups = _group_by_kept(one_liner_rows)
    cover_hit_by_ad: dict[str, str] = {}
    for row in fingerprint_rows:
        ad_key = str(row.get("ad_key") or "")
        if ad_key:
            cover_hit_by_ad[ad_key] = "指纹"
    for row in clip_rows:
        ad_key = str(row.get("ad_key") or "")
        if ad_key:
            cover_hit_by_ad[ad_key] = "CLIP"

    product_counts: dict[str, dict[str, int]] = defaultdict(lambda: {"clip": 0, "fingerprint": 0, "one_liner": 0})
    for row in clip_rows:
        product_counts[str(row.get("product") or meta.get(str(row.get("ad_key") or ""), {}).get("product") or "未知")]["clip"] += 1
    for row in fingerprint_rows:
        appid = str(row.get("appid") or "")
        product_counts[str(row.get("product") or product_by_appid.get(appid) or "未知")]["fingerprint"] += 1
    one_liner_by_product: dict[str, set[str]] = defaultdict(set)
    for row in one_liner_rows:
        product = str(row.get("product") or meta.get(str(row.get("ad_key") or ""), {}).get("product") or "未知")
        one_liner_by_product[product].add(str(row.get("ad_key") or ""))
    for product, ad_keys in one_liner_by_product.items():
        product_counts[product]["one_liner"] = len({x for x in ad_keys if x})

    one_liner_material_count = len({str(row.get("ad_key") or "") for row in one_liner_rows if row.get("ad_key")})
    one_liner_cover_covered = len(
        {
            str(row.get("ad_key") or "")
            for row in one_liner_rows
            if row.get("ad_key") and str(row.get("ad_key") or "") in cover_hit_by_ad
        }
    )
    one_liner_fallback_count = max(0, one_liner_material_count - one_liner_cover_covered)
    clip_crossday_count = sum(1 for row in clip_rows if row.get("reason") == "cover_style_cluster_vs_yesterday")
    clip_intraday_count = max(0, len(clip_rows) - clip_crossday_count)
    cover_removed_keys = {str(row.get("ad_key") or "") for row in [*clip_rows, *fingerprint_rows] if row.get("ad_key")}
    one_liner_removed_keys = {str(row.get("ad_key") or "") for row in one_liner_rows if row.get("ad_key")}
    kept_rows = [
        {"ad_key": ad_key, "_review_status": "未命中封面/一句话硬拦"}
        for ad_key in sorted(set(today_meta) - cover_removed_keys - one_liner_removed_keys)
    ]
    db_flags = _db_exclude_flags(target_date)
    cluster_only_keys = {
        ad_key
        for ad_key, row in analysis_by_ad.items()
        if row.get("exclude_from_cluster") and not row.get("exclude_from_bitable")
    }
    db_bitable_excluded_keys = {
        ad_key
        for ad_key, row in db_flags.items()
        if int(row.get("exclude_from_bitable") or 0) == 1
    }
    cluster_only_rows = [{"ad_key": ad_key, **(analysis_by_ad.get(ad_key) or {})} for ad_key in sorted(cluster_only_keys)]
    business_hard_rows = [
        {**row, "_review_status": _business_reason(row) or "入表前硬拦"}
        for row in business_gate.get("hard_excluded_rows") or []
        if isinstance(row, dict)
    ]
    template_dedup_rows = [
        {**row, "_review_status": "同玩法同模板"}
        for row in business_gate.get("template_skipped_rows") or []
        if isinstance(row, dict)
    ]
    final_bitable_rows = [
        _review_row_from_raw_item(item, analysis_by_ad, status="应入多维表")
        for item in business_gate.get("template_kept_items") or []
        if isinstance(item, dict)
    ]
    new_push_rows = list(daily_report.get("new_asset_variant_items") or [])
    new_push_keys = {str(item.get("ad_key") or "") for item in new_push_rows if item.get("ad_key")}
    asset_variant_candidates = [item for item in daily_report.get("asset_variant_items") or [] if isinstance(item, dict)]
    asset_variant_filtered_rows = [
        {
            **item,
            "_review_status": (
                "狭义新非代表"
                if str(item.get("narrow_novelty_label") or item.get("play_asset_novelty_label") or "") in ("新玩法", "老玩法新迭代")
                else "老玩法换皮/已沉淀"
            ),
        }
        for item in asset_variant_candidates
        if str(item.get("ad_key") or "") not in new_push_keys
    ]
    play_assets = load_play_assets()
    for rows in (
        kept_rows,
        final_bitable_rows,
        new_push_rows,
        asset_variant_filtered_rows,
        cluster_only_rows,
        one_liner_rows,
        business_hard_rows,
        template_dedup_rows,
    ):
        _annotate_play_assets(rows, meta=meta, today_meta=today_meta, assets=play_assets)
    kept_asset_matched_count = len([item for item in kept_rows if item.get("_play_asset_id")])
    kept_asset_pending_count = max(0, len(kept_rows) - kept_asset_matched_count)
    would_enter_bitable_count = len(business_gate.get("template_kept_items") or [])
    bitable_actual_count = bitable_counts.get("count")
    stats = [
        ("CLIP 封面命中", len(clip_rows), f"{len(clip_groups)} 簇 / 跨日 {clip_crossday_count} / 日内 {clip_intraday_count}"),
        ("指纹跨日命中", len(fingerprint_rows), f"{len(fingerprint_groups)} 簇"),
        ("一句话命中素材", one_liner_material_count, f"{len(one_liner_groups)} 簇 / {len(one_liner_rows)} 条证据"),
        ("其中封面已覆盖", one_liner_cover_covered, "按当前封面规则会提前剔除"),
        ("玩法兜底素材", one_liner_fallback_count, "封面未命中，才依赖玩法筛选"),
        ("看板未筛掉", len(kept_rows), "封面+一句话后仍保留"),
        ("玩法资产命中", kept_asset_matched_count, f"保留素材待沉淀 {kept_asset_pending_count} / 资产 {len(play_assets)} 个"),
        ("日报候选", daily_summary.get("candidate_material_count", 0), f"DB硬拦 {len(db_bitable_excluded_keys)} / cluster-only {len(cluster_only_keys)}"),
        ("应入多维表", would_enter_bitable_count, f"业务硬拦 {len(business_hard_rows)} / 同模板 {len(template_dedup_rows)}"),
        ("多维表实际", bitable_actual_count if bitable_actual_count is not None else "未读取", f"扫描 {bitable_counts.get('scanned') or 0} 条"),
        ("日报狭义新", daily_summary.get("narrow_new_play_count", 0), f"老玩法新迭代 {daily_summary.get('old_play_new_iteration_count', 0)} / 推送代表 {len(new_push_rows)}"),
        ("日报未推送", len(asset_variant_filtered_rows), "狭义新非代表、老玩法换皮或已沉淀"),
    ]
    stats_html = "".join(f'<div class="stat"><strong>{_h(value)}</strong><span>{_h(label)} · {_h(note)}</span></div>' for label, value, note in stats)
    chips_html = "".join(
        f'<span class="chip">{_h(product)}：CLIP {vals["clip"]} / 指纹 {vals["fingerprint"]} / 一句话 {vals["one_liner"]}</span>'
        for product, vals in sorted(product_counts.items(), key=lambda kv: (-(kv[1]["clip"] + kv[1]["fingerprint"] + kv[1]["one_liner"]), kv[0]))
    )

    def push_extra(item: dict[str, Any], row: dict[str, Any]) -> list[tuple[str, Any]]:
        return [
            ("推送段", item.get("narrow_novelty_label") or item.get("play_asset_novelty_label") or "狭义新"),
            ("玩法资产", item.get("_play_asset_name") or "待沉淀"),
            ("子标签", item.get("_play_asset_subtags") or "-"),
            ("同狭义新素材", item.get("cluster_material_count") or item.get("daily_play_cluster_size") or 1),
            ("玩法族", _play_family(row, item)),
        ]

    def asset_variant_filtered_extra(item: dict[str, Any], row: dict[str, Any]) -> list[tuple[str, Any]]:
        return [
            ("未推送原因", item.get("_review_status") or "-"),
            ("玩法新旧", item.get("play_asset_novelty_label") or "-"),
            ("狭义理由", item.get("narrow_novelty_reason") or "-"),
            ("玩法资产", item.get("_play_asset_name") or item.get("play_asset_name") or "待沉淀"),
            ("子标签", item.get("_play_asset_subtags") or item.get("play_asset_subtag_names") or "-"),
            ("玩法变种ID", item.get("play_asset_variant_key") or "-"),
            ("玩法族", _play_family(row, item)),
            ("展示", row.get("best_all_exposure_value") or row.get("all_exposure_value") or row.get("best_impression") or "-"),
        ]

    def cluster_only_extra(item: dict[str, Any], row: dict[str, Any]) -> list[tuple[str, Any]]:
        return [
            ("口径差异", "exclude_from_cluster=1 / bitable=0"),
            ("玩法资产", item.get("_play_asset_name") or "待沉淀"),
            ("子标签", item.get("_play_asset_subtags") or "-"),
            ("玩法族", _play_family(row, item)),
            ("DB硬拦", "否"),
        ]

    def business_hard_extra(item: dict[str, Any], row: dict[str, Any]) -> list[tuple[str, Any]]:
        return [
            ("第三层模块", "业务硬拦"),
            ("不入表原因", item.get("_review_status") or _business_reason(item) or "-"),
            ("玩法资产", item.get("_play_asset_name") or "待沉淀"),
            ("玩法族", _play_family(row, item)),
            ("广告ID", item.get("ad_key") or row.get("ad_key") or "-"),
        ]

    def template_dedup_extra(item: dict[str, Any], row: dict[str, Any]) -> list[tuple[str, Any]]:
        return [
            ("第三层模块", "同玩法同模板"),
            ("保留代表", item.get("kept_ad_key") or item.get("match_ad_key") or "-"),
            ("封面相似度", item.get("cover_clip_similarity") or "-"),
            ("文本相似度", item.get("template_similarity") or "-"),
            ("原因", item.get("match_reason") or "-"),
            ("玩法资产", item.get("_play_asset_name") or "待沉淀"),
            ("模板", item.get("template_key") or row.get("template_fingerprint") or "-"),
        ]

    daily_funnel_note = (
        f"日报新口径：今日 DB 候选 {daily_summary.get('candidate_material_count', 0)} 条 "
        f"→ 新玩法 {daily_summary.get('narrow_new_play_count', 0)} 个 "
        f"→ 老玩法新迭代 {daily_summary.get('old_play_new_iteration_count', 0)} 个 "
        f"→ 推送代表 {len(new_push_rows)} 条；"
        f"未推送 {len(asset_variant_filtered_rows)} 条会在下方展示，其中包含狭义新非代表、老玩法换皮和已沉淀玩法。"
    )
    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>VE {target_date} 筛选复核看板</title>
  <style>{_css()}</style>
</head>
<body>
  <header>
    <div class="wrap">
      <h1>VE {target_date} 筛选复核看板</h1>
      <div class="sub">
        <span>封面：ahash/url + CLIP</span>
        <span>历史窗口：{_h(", ".join(str(x) for x in history_dates))}</span>
        <span>生成：{_h(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}</span>
      </div>
      <div class="stats">{stats_html}</div>
      <div class="toolbar"><input id="search" type="search" placeholder="搜索产品、appid、ad_key、玩法资产、玩法文案或去重原因"></div>
    </div>
  </header>
  <main class="wrap">
    <div class="note">这个页面按三层主筛选口径展示：爬取资格、封面/指纹、入表前业务筛选。日报区只解释推送代表，不作为入多维表的新增筛选层。</div>
    <div class="chips">{chips_html}</div>
    <div id="empty" class="empty">没有匹配的簇</div>
    {_render_three_layer_funnel_section(target_date=target_date, crawl_report=crawl_report, business_gate=business_gate, bitable_counts=bitable_counts)}
    {_render_label_table_section(title="筛选后素材 · 每条玩法标签（应入多维表）", rows=final_bitable_rows, meta=meta, today_meta=today_meta, empty_text="没有最终应入多维表的素材")}
    {_render_cover_section(title="封面图去重 · CLIP 命中（跨日 / 日内）", groups=clip_groups, mode="CLIP", target_date=target_date, meta=meta, today_meta=today_meta)}
    {_render_cover_section(title="封面图去重 · ahash / URL 指纹跨日命中", groups=fingerprint_groups, mode="指纹", target_date=target_date, meta=meta, today_meta=today_meta)}
    {_render_material_section(title="第三层 · 同玩法同模板（不进多维表）", rows=template_dedup_rows, target_date=target_date, meta=meta, today_meta=today_meta, ribbon="同模板", ribbon_cls="warn", empty_text="第三层没有同玩法同模板素材", extra_builder=template_dedup_extra)}
    {_render_material_section(title="第三层 · 业务硬拦（不进多维表）", rows=business_hard_rows, target_date=target_date, meta=meta, today_meta=today_meta, ribbon="不入表", ribbon_cls="drop", empty_text="第三层没有业务硬拦素材", extra_builder=business_hard_extra)}
    {_render_play_asset_library_section(play_assets)}
    {_render_one_liner_section(one_liner_groups, target_date, meta, today_meta, cover_hit_by_ad)}
    <h2 class="section-title">日报推送筛选逻辑</h2>
    <div class="note">{_h(daily_funnel_note)}</div>
    {_render_material_section(title="日报新玩法 / 老玩法新迭代推送段", rows=new_push_rows, target_date=target_date, meta=meta, today_meta=today_meta, ribbon="进入推送", ribbon_cls="keep", empty_text="没有日报狭义新素材", extra_builder=push_extra)}
    {_render_material_section(title="日报新口径未推送素材", rows=asset_variant_filtered_rows, target_date=target_date, meta=meta, today_meta=today_meta, ribbon="未推送", ribbon_cls="warn", empty_text="没有被新日报口径过滤掉的素材", extra_builder=asset_variant_filtered_extra)}
    {_render_material_section(title="口径差异：只从聚类剔除、未从日报候选硬拦", rows=cluster_only_rows, target_date=target_date, meta=meta, today_meta=today_meta, ribbon="cluster-only", ribbon_cls="warn", empty_text="没有 cluster-only 差异素材", extra_builder=cluster_only_extra)}
    {_render_advertiser_grouped_material_section(title="今天同步多维表素材 · 按广告主", rows=final_bitable_rows, target_date=target_date, meta=meta, today_meta=today_meta, ribbon="已同步多维表", ribbon_cls="keep", empty_text="没有同步到多维表的素材")}
  </main>
  <script>
    const search = document.querySelector('#search');
    const clusters = [...document.querySelectorAll('.cluster, .asset-card')];
    const empty = document.querySelector('#empty');
    function applyFilter() {{
      const q = search.value.trim().toLowerCase();
      let visible = 0;
      for (const cluster of clusters) {{
        const hit = !q || cluster.dataset.search.includes(q);
        cluster.style.display = hit ? '' : 'none';
        if (hit) visible += 1;
      }}
      empty.style.display = visible ? 'none' : 'block';
    }}
    search.addEventListener('input', applyFilter);
  </script>
</body>
</html>
"""
    output = output_path or (REPORTS_DIR / f"ve_filter_review_{target_date}.html")
    output.write_text(html, encoding="utf-8")
    if write_legacy_cover_path:
        legacy = REPORTS_DIR / f"ve_cover_dedupe_clusters_{target_date}.html"
        legacy.write_text(html, encoding="utf-8")
    return output


def _default_date() -> str:
    return (date.today() - timedelta(days=1)).isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(description="生成 VE 筛选复核看板 HTML")
    parser.add_argument("--date", default=_default_date(), help="目标日期 YYYY-MM-DD，默认昨天")
    parser.add_argument("--output", default="", help="可选输出 HTML 路径")
    parser.add_argument("--no-legacy-cover-path", action="store_true", help="不同时写 ve_cover_dedupe_clusters_日期.html")
    args = parser.parse_args()
    path = write_filter_review_dashboard(
        args.date,
        output_path=Path(args.output) if args.output else None,
        write_legacy_cover_path=not args.no_legacy_cover_path,
    )
    print(f"[review-dashboard] 已写 {path}")


if __name__ == "__main__":
    main()
