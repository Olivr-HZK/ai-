"""
Arrow2 竞品工作流专用 SQLite（与 Video Enhancer 主库分离）。
- arrow2_creative_library：跨日指纹 + cover_embedding（封面 CLIP）
- arrow2_daily_insights：按 target_date 存素材、封面占位、灵感分析、标签
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from path_util import DATA_DIR, PROJECT_ROOT

AHASH_HAMMING_THRESHOLD = 8

from video_enhancer_pipeline_db import (  # noqa: E402
    _ahash_hamming,
    _pick_image_url_from_raw,
    _pick_video_url_from_raw,
)
from tiktok_video_resolve import is_playable_ads_creative, pick_playable_html_url  # noqa: E402

# 人读：「展示估值」= exposure_top10；「最新创意」= latest_yesterday
ARROW2_CRAWL_LABEL_EXPOSURE = "展示估值"
ARROW2_CRAWL_LABEL_LATEST = "最新创意"


def _pull_id_to_crawl_workflow_label(pull_id: str) -> str:
    pid = (pull_id or "").strip()
    if pid == "exposure_top10" or pid == "exposure":
        return ARROW2_CRAWL_LABEL_EXPOSURE
    if pid == "latest_yesterday":
        return ARROW2_CRAWL_LABEL_LATEST
    if pid in (ARROW2_CRAWL_LABEL_EXPOSURE, ARROW2_CRAWL_LABEL_LATEST):
        return pid
    return ARROW2_CRAWL_LABEL_EXPOSURE


def derive_crawl_workflow_from_item(item: Dict[str, Any]) -> str:
    if not isinstance(item, dict):
        return ARROW2_CRAWL_LABEL_EXPOSURE
    ids: List[str] = []
    seen = item.get("seen_in_runs")
    if isinstance(seen, list):
        for s in seen:
            if not isinstance(s, dict):
                continue
            p = str(s.get("pull_id") or s.get("id") or "").strip()
            if p and p not in ids:
                ids.append(p)
    if not ids:
        p2 = str(item.get("pull_id") or "").strip()
        if p2:
            ids.append(p2)
    if not ids:
        return ARROW2_CRAWL_LABEL_EXPOSURE
    labels = [_pull_id_to_crawl_workflow_label(x) for x in sorted(ids)]
    out: List[str] = []
    for lb in labels:
        if lb not in out:
            out.append(lb)
    if len(out) == 1:
        return out[0]
    return ",".join(out)


def arrow2_creative_ad_key(creative: Dict[str, Any]) -> str:
    return str(
        creative.get("ad_key")
        or creative.get("creative_id")
        or creative.get("id")
        or creative.get("creativeId")
        or ""
    ).strip()


def dedupe_arrow2_raw_items_by_ad_key(
    raw_items: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    rows_in = len(raw_items)
    buckets: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []

    for idx, row in enumerate(raw_items):
        if not isinstance(row, dict):
            continue
        product = str(row.get("product") or "")
        keyword = str(row.get("keyword") or "")
        pid = str(row.get("pull_id") or "")
        c = row.get("creative")
        if not isinstance(c, dict):
            c = {}
        ak = arrow2_creative_ad_key(c)
        if not ak:
            ak = f"__no_ad_key__:{idx}"

        key = ak
        run = {
            "day_span": row.get("day_span"),
            "order_by": str(row.get("order_by") or ""),
            "pull_id": pid,
            "product": product,
            "keyword": keyword,
        }
        run_sig = (product, keyword, str(run["day_span"]), run["order_by"], pid)

        if key not in buckets:
            buckets[key] = {
                "product": product,
                "keyword": keyword,
                "appid": str(row.get("appid") or ""),
                "pull_id": pid,
                "pull_spec": row.get("pull_spec"),
                "ad_key": ak if not str(ak).startswith("__no_ad_key__") else "",
                "creative": c,
                "day_span": row.get("day_span"),
                "order_by": str(row.get("order_by") or ""),
                "seen_in_runs": [run],
                "_merge_count": 1,
                "_run_sigs": {run_sig},
            }
            order.append(key)
            continue

        b = buckets[key]
        b["_merge_count"] = int(b.get("_merge_count") or 1) + 1
        sigs: Any = b.setdefault("_run_sigs", set())
        if isinstance(sigs, set) and run_sig not in sigs:
            sigs.add(run_sig)
            b["seen_in_runs"].append(run)

    out: List[Dict[str, Any]] = []
    for key in order:
        b = buckets[key]
        merge_count = int(b.pop("_merge_count", 1))
        b.pop("_run_sigs", None)
        seen = b.get("seen_in_runs") or []
        b["cross_check"] = {
            "pull_hit_count": merge_count,
            "distinct_runs": len(seen),
        }
        out.append(b)

    rows_out = len(out)
    stats: Dict[str, Any] = {
        "rows_before_dedupe": rows_in,
        "rows_after_dedupe": rows_out,
        "duplicate_rows_merged": max(0, rows_in - rows_out),
        "unique_ad_key_rows": rows_out,
    }
    return out, stats


def get_arrow2_pipeline_items_from_raw_payload(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return []
    if "items_deduped_by_ad_key" in raw:
        v = raw.get("items_deduped_by_ad_key")
        return list(v) if isinstance(v, list) else []
    und = raw.get("items") or []
    if not isinstance(und, list) or not und:
        return []
    ded, _st = dedupe_arrow2_raw_items_by_ad_key(und)
    return ded


def _pick_arrow2_media_url(creative: Dict[str, Any]) -> str:
    """Arrow2：试玩 ads_type=7 时优先 HTML 试玩链，否则沿用 mp4/图逻辑。"""
    if is_playable_ads_creative(creative):
        u = pick_playable_html_url(creative)
        if u:
            return u
    return _pick_video_url_from_raw(creative)


def _db_path() -> Path:
    raw = (os.getenv("ARROW2_SQLITE_PATH") or "data/arrow2_pipeline.db").strip()
    p = Path(raw)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    return p


def _conn() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(_db_path())
    c.row_factory = sqlite3.Row
    return c


def init_db() -> None:
    p = _db_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS arrow2_creative_library (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ad_key TEXT NOT NULL UNIQUE,
              dedup_group_id TEXT,
              canonical_ad_key TEXT,
              is_canonical INTEGER DEFAULT 1,
              image_ahash_md5 TEXT,
              text_fingerprint TEXT,
              category TEXT,
              product TEXT,
              appid TEXT,
              platform TEXT,
              creative_type TEXT,
              video_duration INTEGER,
              title TEXT,
              body TEXT,
              video_url TEXT,
              image_url TEXT,
              preview_img_url TEXT,
              best_heat INTEGER DEFAULT 0,
              best_impression INTEGER DEFAULT 0,
              best_all_exposure_value INTEGER DEFAULT 0,
              first_target_date TEXT,
              last_target_date TEXT,
              appearance_count INTEGER DEFAULT 1,
              insight_analysis TEXT,
              insight_ua_suggestion TEXT,
              insight_cover_style TEXT,
              cover_embedding BLOB,
              dedup_reason TEXT,
              created_at_local TEXT DEFAULT (datetime('now','localtime')),
              updated_at_local TEXT DEFAULT (datetime('now','localtime'))
            );
            CREATE INDEX IF NOT EXISTS idx_a2cl_appid ON arrow2_creative_library(appid);
            CREATE INDEX IF NOT EXISTS idx_a2cl_first_td ON arrow2_creative_library(first_target_date);

            CREATE TABLE IF NOT EXISTS arrow2_daily_insights (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              crawl_date TEXT,
              target_date TEXT NOT NULL,
              category TEXT,
              product TEXT,
              appid TEXT NOT NULL,
              ad_key TEXT NOT NULL,
              platform TEXT,
              video_url TEXT,
              preview_img_url TEXT,
              video_duration INTEGER,
              first_seen INTEGER,
              created_at INTEGER,
              last_seen INTEGER,
              days_count INTEGER,
              heat INTEGER,
              all_exposure_value INTEGER,
              impression INTEGER,
              raw_json TEXT,
              insight_analysis TEXT,
              insight_ua_suggestion TEXT,
              insight_cover_style TEXT,
              material_tags TEXT,
              created_at_local TEXT DEFAULT (datetime('now','localtime')),
              updated_at_local TEXT DEFAULT (datetime('now','localtime')),
              UNIQUE(target_date, ad_key)
            );
            CREATE INDEX IF NOT EXISTS idx_a2di_td ON arrow2_daily_insights(target_date);
            """
        )
        conn.commit()
    finally:
        conn.close()
    _migrate_arrow2_daily_insights_schema()
    _migrate_arrow2_crawl_workflow_column()


def _migrate_arrow2_crawl_workflow_column() -> None:
    conn = _conn()
    try:
        cur = conn.cursor()
        for tbl in ("arrow2_creative_library", "arrow2_daily_insights"):
            cur.execute(f"PRAGMA table_info({tbl})")
            cols = {str(r[1]) for r in cur.fetchall()}
            if "crawl_workflow" not in cols:
                cur.execute(f"ALTER TABLE {tbl} ADD COLUMN crawl_workflow TEXT")
        conn.commit()
    finally:
        conn.close()


def _migrate_arrow2_daily_insights_schema() -> None:
    """为已有库补齐 Arrow2 日表字段（insight_material_category / ad_one_liner）。"""
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(arrow2_daily_insights)")
        cols = {str(r[1]) for r in cur.fetchall()}
        if "insight_material_category" not in cols:
            cur.execute("ALTER TABLE arrow2_daily_insights ADD COLUMN insight_material_category TEXT")
        if "ad_one_liner" not in cols:
            cur.execute("ALTER TABLE arrow2_daily_insights ADD COLUMN ad_one_liner TEXT")
        conn.commit()
    finally:
        conn.close()


def delete_arrow2_daily_insights_for_beijing_date(ymd: str) -> int:
    """删除 arrow2_daily_insights 中 target_date 或 crawl_date 等于给定日期（YYYY-MM-DD）的行。"""
    ymd = (ymd or "").strip()
    if not ymd:
        return 0
    init_db()
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM arrow2_daily_insights WHERE target_date = ? OR crawl_date = ?",
            (ymd, ymd),
        )
        n = cur.rowcount
        conn.commit()
        return int(n)
    finally:
        conn.close()


def delete_arrow2_daily_insights_beijing_today() -> int:
    """删除东八区「今天」在 target_date 或 crawl_date 上的行。"""
    from datetime import datetime, timedelta, timezone

    tz = timezone(timedelta(hours=8))
    today = datetime.now(tz).date().isoformat()
    return delete_arrow2_daily_insights_for_beijing_date(today)


def wipe_arrow2_sqlite_all_rows() -> dict[str, int]:
    """
    清空 arrow2_daily_insights 与 arrow2_creative_library 全部行（全库重置，慎用）。
    返回各表删除行数。
    """
    init_db()
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM arrow2_daily_insights")
        n_di = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
        cur.execute("DELETE FROM arrow2_creative_library")
        n_cl = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
        conn.commit()
        return {"arrow2_daily_insights": int(n_di), "arrow2_creative_library": int(n_cl)}
    finally:
        conn.close()


def prune_arrow2_creative_library_not_in_daily_insights(*, dry_run: bool = False) -> Dict[str, int]:
    """
    删除 arrow2_creative_library 中在 arrow2_daily_insights 从未出现过的 ad_key（孤儿行）。

    仅作用于 ARROW2_SQLITE_PATH（默认 data/arrow2_pipeline.db），与 Video Enhancer 的 creative_library 无关。
    """
    init_db()
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM arrow2_creative_library")
        total_before = int(cur.fetchone()[0])
        cur.execute(
            """
            SELECT COUNT(*) FROM arrow2_creative_library c
            WHERE NOT EXISTS (
              SELECT 1 FROM arrow2_daily_insights d
              WHERE d.ad_key = c.ad_key
                AND COALESCE(TRIM(d.ad_key), '') != ''
            )
            """
        )
        orphan = int(cur.fetchone()[0])
        kept = total_before - orphan
        if dry_run:
            return {
                "db_path": str(_db_path()),
                "total_before": total_before,
                "would_delete": orphan,
                "kept": kept,
                "dry_run": 1,
            }
        cur.execute(
            """
            DELETE FROM arrow2_creative_library
            WHERE NOT EXISTS (
              SELECT 1 FROM arrow2_daily_insights d
              WHERE d.ad_key = arrow2_creative_library.ad_key
                AND COALESCE(TRIM(d.ad_key), '') != ''
            )
            """
        )
        deleted = cur.rowcount if cur.rowcount is not None else 0
        conn.commit()
        return {
            "db_path": str(_db_path()),
            "total_before": total_before,
            "deleted": int(deleted),
            "kept": total_before - int(deleted),
            "dry_run": 0,
        }
    finally:
        conn.close()


def _text_fingerprint(title: str, body: str) -> str:
    t = f"{(title or '').strip()}\n{(body or '').strip()}"
    return hashlib.sha1(t.encode("utf-8")).hexdigest()


def upsert_arrow2_creative_library_batch(target_date: str, items: List[Dict[str, Any]]) -> int:
    """
    将当日 items 写入 arrow2_creative_library（供跨日指纹与 cover_embedding）。
    简化版：与 VE 主库类似的去重新插入/更新，仅作用于 Arrow2 表。
    """
    init_db()
    n = 0
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT ad_key, image_ahash_md5, dedup_group_id FROM arrow2_creative_library "
            "WHERE COALESCE(image_ahash_md5,'') <> ''"
        )
        existing_ahash_rows: List[Dict[str, Any]] = [
            {"ad_key": str(r["ad_key"]), "image_ahash_md5": str(r["image_ahash_md5"] or ""), "dedup_group_id": str(r["dedup_group_id"] or "")}
            for r in cur.fetchall()
        ]

        for item in items:
            if not isinstance(item, dict):
                continue
            c = item.get("creative") or {}
            if not isinstance(c, dict):
                continue
            ad_key = str(c.get("ad_key") or "").strip()
            if not ad_key:
                continue
            cw = derive_crawl_workflow_from_item(item)
            ahash = str(c.get("image_ahash_md5") or "").strip()
            title = str(c.get("title") or "")
            body = str(c.get("body") or "")
            text_fp = _text_fingerprint(title, body)
            vurl = _pick_arrow2_media_url(c)
            iurl = _pick_image_url_from_raw(c) if not vurl else ""
            creative_type = "video" if vurl else ("image" if iurl else "video")
            heat = int(c.get("heat") or 0)
            impression = int(c.get("impression") or 0)
            all_exp = int(c.get("all_exposure_value") or 0)
            preview = str(c.get("preview_img_url") or "").strip()
            appid = str(item.get("appid") or "").strip()
            product = str(item.get("product") or "").strip()

            cur.execute("SELECT ad_key, first_target_date, best_impression FROM arrow2_creative_library WHERE ad_key = ?", (ad_key,))
            ex = cur.fetchone()
            if ex:
                cur.execute(
                    """
                    UPDATE arrow2_creative_library SET
                      last_target_date = ?, best_heat = MAX(best_heat, ?), best_impression = MAX(best_impression, ?),
                      best_all_exposure_value = MAX(best_all_exposure_value, ?),
                      preview_img_url = COALESCE(NULLIF(?, ''), preview_img_url),
                      video_url = COALESCE(NULLIF(?, ''), video_url),
                      image_url = COALESCE(NULLIF(?, ''), image_url),
                      title = COALESCE(NULLIF(?, ''), title),
                      body = COALESCE(NULLIF(?, ''), body),
                      image_ahash_md5 = COALESCE(NULLIF(?, ''), image_ahash_md5),
                      crawl_workflow = ?,
                      updated_at_local = datetime('now','localtime')
                    WHERE ad_key = ?
                    """,
                    (target_date, heat, impression, all_exp, preview, vurl, iurl, title, body, ahash, cw, ad_key),
                )
                n += 1
                continue

            dedup_group_id = ""
            canonical = ad_key
            dedup_reason = "new"
            if ahash:
                best_dist = 999
                for row in existing_ahash_rows:
                    dist = _ahash_hamming(ahash, str(row["image_ahash_md5"] or ""))
                    if dist <= AHASH_HAMMING_THRESHOLD and dist < best_dist:
                        best_dist = dist
                        dedup_group_id = str(row["dedup_group_id"] or "") or f"ahash_{ahash[:8]}"
                        dedup_reason = f"ahash(dist={best_dist})"
                if dedup_group_id:
                    cur.execute(
                        "SELECT ad_key, best_impression FROM arrow2_creative_library WHERE dedup_group_id = ? ORDER BY best_impression DESC LIMIT 1",
                        (dedup_group_id,),
                    )
                    top = cur.fetchone()
                    canonical = str(top["ad_key"]) if top and int(top["best_impression"] or 0) >= impression else ad_key

            if not dedup_group_id and text_fp and appid:
                cur.execute(
                    "SELECT ad_key, dedup_group_id, best_impression FROM arrow2_creative_library WHERE text_fingerprint = ? AND appid = ? LIMIT 1",
                    (text_fp, appid),
                )
                tm = cur.fetchone()
                if tm:
                    dedup_group_id = str(tm["dedup_group_id"] or f"text_{text_fp[:8]}")
                    canonical = str(tm["ad_key"]) if int(tm["best_impression"] or 0) >= impression else ad_key
                    dedup_reason = "text"

            if not dedup_group_id:
                dedup_group_id = f"adkey_{ad_key[:8]}"
                canonical = ad_key
                dedup_reason = "new"

            is_canonical = 1 if canonical == ad_key else 0
            if canonical == ad_key and dedup_reason != "new":
                cur.execute(
                    "UPDATE arrow2_creative_library SET is_canonical = 0 WHERE dedup_group_id = ? AND is_canonical = 1",
                    (dedup_group_id,),
                )
                cur.execute(
                    "UPDATE arrow2_creative_library SET canonical_ad_key = ? WHERE dedup_group_id = ?",
                    (ad_key, dedup_group_id),
                )

            cur.execute(
                """INSERT INTO arrow2_creative_library (
                  ad_key, dedup_group_id, canonical_ad_key, is_canonical,
                  image_ahash_md5, text_fingerprint, category, product, appid, platform,
                  creative_type, video_duration, title, body, video_url, image_url, preview_img_url,
                  best_heat, best_impression, best_all_exposure_value,
                  first_target_date, last_target_date, appearance_count, dedup_reason, crawl_workflow
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?)""",
                (
                    ad_key, dedup_group_id, canonical, is_canonical,
                    ahash, text_fp, item.get("category"), product, appid, str(c.get("platform") or ""),
                    creative_type, int(c.get("video_duration") or 0), title, body, vurl, iurl, preview,
                    heat, impression, all_exp,
                    target_date, target_date, dedup_reason, cw,
                ),
            )
            if ahash:
                existing_ahash_rows.append({"ad_key": ad_key, "image_ahash_md5": ahash, "dedup_group_id": dedup_group_id})
            n += 1
        conn.commit()
    finally:
        conn.close()
    return n


def crossday_filter_arrow2_items(
    target_date: str,
    items: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """与 VE crossday_filter_items_against_creative_library 相同逻辑，表换为 arrow2_creative_library。"""
    init_db()
    deduped_items: List[Dict[str, Any]] = []
    crossday_removed: List[Dict[str, Any]] = []

    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ad_key, appid, image_ahash_md5, video_url, image_url, preview_img_url, first_target_date
            FROM arrow2_creative_library
            WHERE first_target_date < ? AND first_target_date IS NOT NULL
            """,
            (target_date,),
        )
        hist_rows = cur.fetchall()

        hist_adkey_date: Dict[str, Dict[str, str]] = defaultdict(dict)
        hist_urls: Dict[str, Dict[str, Tuple[str, str]]] = defaultdict(dict)
        hist_preview_urls: Dict[str, Dict[str, Tuple[str, str]]] = defaultdict(dict)
        hist_ahash: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list)

        for r in hist_rows:
            aid = str(r["appid"] or "").strip()
            ak = str(r["ad_key"] or "").strip()
            fdt = str(r["first_target_date"] or "")
            if ak:
                hist_adkey_date[aid][ak] = fdt
            vurl = str(r["video_url"] or "").strip()
            iurl = str(r["image_url"] or "").strip()
            media = vurl or iurl
            if media:
                hist_urls[aid][media] = (ak, fdt)
            purl = str(r["preview_img_url"] or "").strip()
            if purl:
                hist_preview_urls[aid][purl] = (ak, fdt)
            h = str(r["image_ahash_md5"] or "").strip()
            if h:
                hist_ahash[aid].append((h, ak, fdt))

        for item in items:
            if not isinstance(item, dict):
                continue
            c = item.get("creative") or {}
            if not isinstance(c, dict):
                continue
            ad_key = str(c.get("ad_key") or "").strip()
            ahash = str(c.get("image_ahash_md5") or "").strip()
            vurl = _pick_arrow2_media_url(c)
            iurl = _pick_image_url_from_raw(c) if not vurl else ""
            media = (vurl or iurl).strip()
            preview_u = str(c.get("preview_img_url") or "").strip()
            appid = str(item.get("appid") or "").strip()

            hit_ak = ""
            hit_date = ""
            reason_b = ""

            if appid and ad_key and ad_key in hist_adkey_date.get(appid, {}):
                hit_ak = ad_key
                hit_date = hist_adkey_date[appid][ad_key]
                reason_b = "ad_key"

            if not hit_ak and appid and media and media in hist_urls.get(appid, {}):
                hit_ak, hit_date = hist_urls[appid][media]
                reason_b = "url"

            if not hit_ak and appid and preview_u and preview_u in hist_preview_urls.get(appid, {}):
                hit_ak, hit_date = hist_preview_urls[appid][preview_u]
                reason_b = "preview_img_url"

            if not hit_ak and appid and ahash:
                best_dist = 999
                for (h, ak, dt) in hist_ahash.get(appid, []):
                    d = _ahash_hamming(ahash, h)
                    if d <= AHASH_HAMMING_THRESHOLD and d < best_dist:
                        best_dist, hit_ak, hit_date = d, ak, dt
                if hit_ak:
                    reason_b = f"ahash(dist={best_dist})"

            if hit_ak:
                crossday_removed.append(
                    {
                        "ad_key": ad_key,
                        "reason": reason_b,
                        "matched_ad_key": hit_ak,
                        "matched_date": hit_date,
                        "appid": appid,
                    }
                )
            else:
                deduped_items.append(item)
    finally:
        conn.close()

    return deduped_items, crossday_removed


def load_arrow2_cover_embedding_blob_map_by_ad_keys(ad_keys: List[str]) -> Dict[str, bytes]:
    keys = [str(k).strip() for k in ad_keys if str(k).strip()]
    if not keys:
        return {}
    init_db()
    conn = _conn()
    try:
        cur = conn.cursor()
        out: Dict[str, bytes] = {}
        for i in range(0, len(keys), 400):
            chunk = keys[i : i + 400]
            ph = ",".join(["?"] * len(chunk))
            cur.execute(
                f"SELECT ad_key, cover_embedding FROM arrow2_creative_library WHERE ad_key IN ({ph}) AND cover_embedding IS NOT NULL",
                tuple(chunk),
            )
            for r in cur.fetchall():
                b = r["cover_embedding"]
                if b:
                    out[str(r["ad_key"])] = bytes(b)
        return out
    finally:
        conn.close()


def upsert_arrow2_cover_embedding(ad_key: str, embedding_blob: bytes) -> bool:
    init_db()
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE arrow2_creative_library SET cover_embedding = ?, updated_at_local = datetime('now','localtime') WHERE ad_key = ?",
            (embedding_blob, ad_key),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def load_arrow2_cover_style_rows_for_dates_grouped_by_appid(
    target_dates: List[str],
) -> Dict[str, List[Dict[str, Any]]]:
    dates = sorted({(d or "").strip() for d in (target_dates or []) if (d or "").strip()})
    if not dates:
        return {}
    init_db()
    conn = _conn()
    try:
        cur = conn.cursor()
        ph = ",".join(["?"] * len(dates))
        cur.execute(
            f"""
            SELECT ad_key, appid, insight_cover_style, COALESCE(all_exposure_value, 0) AS exp
            FROM arrow2_daily_insights
            WHERE target_date IN ({ph})
              AND COALESCE(TRIM(insight_cover_style), '') <> ''
              AND COALESCE(TRIM(ad_key), '') <> ''
              AND COALESCE(TRIM(appid), '') <> ''
            """,
            tuple(dates),
        )
        merged: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
        for row in cur.fetchall():
            ak = str(row["ad_key"] or "").strip()
            aid = str(row["appid"] or "").strip()
            raw = str(row["insight_cover_style"] or "").strip()
            if not ak or not aid or not raw:
                continue
            try:
                obj = json.loads(raw)
                if not isinstance(obj, dict):
                    continue
            except Exception:
                continue
            exp = int(row["exp"] or 0)
            prev = merged[aid].get(ak)
            if prev is None or exp > prev["exposure"]:
                merged[aid][ak] = {"ad_key": ak, "style_json": obj, "exposure": exp}
        return {aid: list(rows.values()) for aid, rows in merged.items()}
    finally:
        conn.close()


def upsert_arrow2_single_cover_style_insight(
    target_date: str,
    crawl_date: Any,
    item: Dict[str, Any],
) -> bool:
    """仅写入 insight_cover_style，不覆盖分析正文。"""
    init_db()
    c = item.get("creative") or {}
    if not isinstance(c, dict):
        return False
    ad_key = str(c.get("ad_key") or "").strip()
    if not ad_key:
        return False
    cs = item.get("cover_style")
    if isinstance(cs, dict):
        cover_style_str = json.dumps(cs, ensure_ascii=False)
    elif cs is not None and str(cs).strip():
        cover_style_str = str(cs).strip()
    else:
        cover_style_str = ""

    raw_json = json.dumps(c, ensure_ascii=False)
    crawl_date_s = str(crawl_date) if crawl_date is not None else ""
    product = str(item.get("product") or "")
    appid = str(item.get("appid") or "")
    platform = str(c.get("platform") or "")
    cw = derive_crawl_workflow_from_item(item)
    video_url = _pick_arrow2_media_url(c)
    preview = str(c.get("preview_img_url") or "").strip()
    vd = int(c.get("video_duration") or 0)
    fs = c.get("first_seen")
    cr = c.get("created_at")
    ls = c.get("last_seen")
    heat = int(c.get("heat") or 0)
    exp = int(c.get("all_exposure_value") or 0)
    imp = int(c.get("impression") or 0)
    days_count = int(c.get("days_count") or 0)

    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO arrow2_daily_insights (
              crawl_date, target_date, category, product, appid, ad_key, platform,
              video_url, preview_img_url, video_duration, first_seen, created_at, last_seen,
              days_count, heat, all_exposure_value, impression, raw_json, insight_analysis, insight_ua_suggestion,
              insight_cover_style, material_tags, crawl_workflow
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(target_date, ad_key) DO UPDATE SET
              crawl_date=excluded.crawl_date,
              insight_cover_style=excluded.insight_cover_style,
              raw_json=excluded.raw_json,
              preview_img_url=COALESCE(NULLIF(excluded.preview_img_url,''), arrow2_daily_insights.preview_img_url),
              heat=excluded.heat,
              all_exposure_value=excluded.all_exposure_value,
              impression=excluded.impression,
              days_count=excluded.days_count,
              crawl_workflow=excluded.crawl_workflow,
              updated_at_local=datetime('now','localtime')
            """,
            (
                crawl_date_s,
                target_date,
                str(item.get("category") or ""),
                product,
                appid,
                ad_key,
                platform,
                video_url,
                preview,
                vd,
                int(fs) if fs is not None else None,
                int(cr) if cr is not None else None,
                int(ls) if ls is not None else None,
                days_count,
                heat,
                exp,
                imp,
                raw_json,
                "",
                "",
                cover_style_str,
                "",
                cw,
            ),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def upsert_arrow2_daily_insight_full(
    target_date: str,
    crawl_date: Any,
    item: Dict[str, Any],
    analysis_raw: Dict[str, Any],
) -> bool:
    init_db()
    c = item.get("creative") or {}
    if not isinstance(c, dict):
        return False
    ad_key = str(c.get("ad_key") or "").strip()
    if not ad_key:
        return False
    analysis = str(analysis_raw.get("analysis") or "")
    ua_single = str(analysis_raw.get("ua_suggestion_single") or "")
    mt = analysis_raw.get("material_tags")
    material_tags = json.dumps(mt, ensure_ascii=False) if isinstance(mt, list) else str(mt or "")
    mat_cat = str(analysis_raw.get("arrow2_material_category") or "").strip()[:500]
    one_liner = str(analysis_raw.get("ad_one_liner") or "").strip()[:120]

    cs = item.get("cover_style")
    if isinstance(cs, dict):
        cover_style_str = json.dumps(cs, ensure_ascii=False)
    elif cs is not None and str(cs).strip():
        cover_style_str = str(cs).strip()
    else:
        cover_style_str = ""

    raw_json = json.dumps(c, ensure_ascii=False)
    crawl_date_s = str(crawl_date) if crawl_date is not None else ""
    product = str(item.get("product") or "")
    appid = str(item.get("appid") or "")
    platform = str(c.get("platform") or "")
    cw = derive_crawl_workflow_from_item(item)
    video_url = _pick_arrow2_media_url(c)
    preview = str(c.get("preview_img_url") or "").strip()
    vd = int(c.get("video_duration") or 0)
    fs = c.get("first_seen")
    cr = c.get("created_at")
    ls = c.get("last_seen")
    heat = int(c.get("heat") or 0)
    exp = int(c.get("all_exposure_value") or 0)
    imp = int(c.get("impression") or 0)
    days_count = int(c.get("days_count") or 0)

    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO arrow2_daily_insights (
              crawl_date, target_date, category, product, appid, ad_key, platform,
              video_url, preview_img_url, video_duration, first_seen, created_at, last_seen,
              days_count, heat, all_exposure_value, impression, raw_json, insight_analysis, insight_ua_suggestion,
              insight_cover_style, material_tags, insight_material_category, ad_one_liner, crawl_workflow
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(target_date, ad_key) DO UPDATE SET
              crawl_date=excluded.crawl_date,
              insight_analysis=excluded.insight_analysis,
              insight_ua_suggestion=excluded.insight_ua_suggestion,
              material_tags=excluded.material_tags,
              insight_material_category=excluded.insight_material_category,
              ad_one_liner=excluded.ad_one_liner,
              insight_cover_style=COALESCE(NULLIF(excluded.insight_cover_style,''), arrow2_daily_insights.insight_cover_style),
              raw_json=excluded.raw_json,
              heat=excluded.heat,
              all_exposure_value=excluded.all_exposure_value,
              impression=excluded.impression,
              days_count=excluded.days_count,
              crawl_workflow=excluded.crawl_workflow,
              updated_at_local=datetime('now','localtime')
            """,
            (
                crawl_date_s,
                target_date,
                str(item.get("category") or ""),
                product,
                appid,
                ad_key,
                platform,
                video_url,
                preview,
                vd,
                int(fs) if fs is not None else None,
                int(cr) if cr is not None else None,
                int(ls) if ls is not None else None,
                days_count,
                heat,
                exp,
                imp,
                raw_json,
                analysis,
                ua_single,
                cover_style_str,
                material_tags,
                mat_cat,
                one_liner,
                cw,
            ),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def load_arrow2_daily_insights_for_country_backfill(
    *,
    target_date: Optional[str] = None,
    scan_limit: int = 2000,
) -> List[Dict[str, Any]]:
    """
    读取 raw_json 解析为 creative；由调用方判断是否仍缺地区字段。
    target_date 非空时只扫该业务日；scan_limit<=0 表示不限制扫描行数（慎用）。
    """
    init_db()
    conn = _conn()
    out: List[Dict[str, Any]] = []
    try:
        cur = conn.cursor()
        td = (target_date or "").strip()
        if td:
            cur.execute(
                """
                SELECT target_date, ad_key, raw_json FROM arrow2_daily_insights
                WHERE target_date = ? AND raw_json IS NOT NULL AND TRIM(raw_json) != ''
                ORDER BY ad_key
                """,
                (td,),
            )
        else:
            cur.execute(
                """
                SELECT target_date, ad_key, raw_json FROM arrow2_daily_insights
                WHERE raw_json IS NOT NULL AND TRIM(raw_json) != ''
                ORDER BY target_date DESC, ad_key
                """
            )
        for r in cur.fetchall():
            raw = str(r["raw_json"] or "").strip()
            if not raw:
                continue
            try:
                c = json.loads(raw)
            except Exception:
                continue
            if not isinstance(c, dict):
                continue
            out.append(
                {
                    "target_date": str(r["target_date"] or ""),
                    "ad_key": str(r["ad_key"] or ""),
                    "creative": c,
                }
            )
            if scan_limit > 0 and len(out) >= scan_limit:
                break
    finally:
        conn.close()
    return out


def update_arrow2_daily_insights_raw_json(target_date: str, ad_key: str, raw_obj: Dict[str, Any]) -> bool:
    """
    仅更新 arrow2_daily_insights.raw_json（及 updated_at_local），用于国家等字段离线回填。
    """
    init_db()
    td = (target_date or "").strip()
    ak = (ad_key or "").strip()
    if not td or not ak:
        return False
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE arrow2_daily_insights
            SET raw_json = ?, updated_at_local = datetime('now','localtime')
            WHERE target_date = ? AND ad_key = ?
            """,
            (json.dumps(raw_obj, ensure_ascii=False), td, ak),
        )
        conn.commit()
        return (cur.rowcount or 0) > 0
    finally:
        conn.close()


def prune_arrow2_daily_insights_not_in_raw(target_date: str, raw_payload: Dict[str, Any]) -> int:
    items = get_arrow2_pipeline_items_from_raw_payload(raw_payload)
    keep: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        c = item.get("creative") or {}
        if isinstance(c, dict):
            ak = str(c.get("ad_key") or "").strip()
            if ak:
                keep.add(ak)
    if not keep:
        return 0
    init_db()
    conn = _conn()
    try:
        cur = conn.cursor()
        ph = ",".join(["?"] * len(keep))
        cur.execute(
            f"DELETE FROM arrow2_daily_insights WHERE target_date = ? AND ad_key NOT IN ({ph})",
            (target_date, *tuple(keep)),
        )
        n = cur.rowcount
        conn.commit()
        return n
    finally:
        conn.close()
