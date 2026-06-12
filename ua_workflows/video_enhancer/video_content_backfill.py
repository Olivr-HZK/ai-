from __future__ import annotations

import argparse
import asyncio
import copy
import html
import json
import os
import sqlite3
import sys
import re
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from ua_workflows.shared.config import DATA_DIR, REPORTS_DIR, load_project_env
from ua_workflows.shared.db import video_enhancer as ve_db
from ua_workflows.shared.llm.client import call_embedding, call_vision, cosine_similarity


DEFAULT_KEYS_JSON = DATA_DIR / "ve_bitable_ai_video_synced_keys_2026-06-08_2026-06-10.json"
DEFAULT_REMOTE_RAW_DIR = DATA_DIR / "remote_snapshots" / "ve" / "data"
DEFAULT_OUTPUT_JSON = DATA_DIR / "ve_ai_video_video_content_embedding_dedupe_2026-06-08_2026-06-10.json"
DEFAULT_BACKFILL_JSON = DATA_DIR / "ve_ai_video_guangdada_video_content_backfill_2026-06-08_2026-06-10.json"
DEFAULT_REPORT_HTML = REPORTS_DIR / "ve_ai_video_video_content_embedding_dedupe_2026-06-08_2026-06-10.html"


@dataclass(frozen=True)
class KeyScope:
    product: str
    dates: dict[str, list[str]]

    @property
    def ad_keys(self) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for date in sorted(self.dates):
            for key in self.dates[date]:
                if key and key not in seen:
                    seen.add(key)
                    out.append(key)
        return out

    @property
    def date_range(self) -> tuple[str, str] | None:
        dates = sorted(self.dates)
        if not dates:
            return None
        return dates[0], dates[-1]


def _dedupe_preserve_order(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            seen.add(text)
            out.append(text)
    return out


def parse_adkeys(values: list[str] | None) -> list[str]:
    if not values:
        return []
    tokens: list[str] = []
    for value in values:
        if not value:
            continue
        for token in re.split(r"[\s,，]+", str(value).strip()):
            token = str(token).strip()
            if token:
                tokens.append(token)
    return _dedupe_preserve_order(tokens)


def load_key_scope(path: Path) -> KeyScope:
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw_dates = payload.get("dates") or {}
    if not isinstance(raw_dates, dict):
        raise ValueError(f"{path} missing dates object")
    dates: dict[str, list[str]] = {}
    for date, keys in raw_dates.items():
        if isinstance(keys, list):
            deduped = _dedupe_preserve_order(keys)
            if deduped:
                dates[str(date)] = deduped
    if not dates:
        raise ValueError(f"{path} has no date-specific ad keys")
    product = str(payload.get("product") or "").strip()
    return KeyScope(product=product, dates=dates)


def _raw_path_for_date(raw_dir: Path, target_date: str) -> Path:
    return raw_dir / f"workflow_video_enhancer_{target_date}_raw.json"


def _load_raw_payload(raw_dir: Path, target_date: str) -> dict[str, Any]:
    path = _raw_path_for_date(raw_dir, target_date)
    if not path.exists():
        return {"target_date": target_date, "items": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid raw json: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"raw payload is not an object: {path}")
    return payload


def material_video_content_from_creative(creative: dict[str, Any]) -> tuple[str, str]:
    text = str(creative.get("guangdada_video_content") or "").strip()
    if text:
        return text, "guangdada_video_content"

    analysis = creative.get("material_script_analysis")
    if isinstance(analysis, str) and analysis.strip():
        try:
            parsed = json.loads(analysis)
        except json.JSONDecodeError:
            parsed = None
        analysis = parsed if isinstance(parsed, dict) else analysis
    if isinstance(analysis, dict):
        text = str(analysis.get("video_content") or "").strip()
        if text:
            return text, "raw_material_script_analysis"
        script = analysis.get("script_analysis")
        if isinstance(script, dict):
            text = str(script.get("video_content") or script.get("video_summary") or "").strip()
            if text:
                return text, "raw_material_script_analysis"

    detail = creative.get("guangdada_detail_analysis")
    if isinstance(detail, dict):
        script = detail.get("script_analysis")
        if isinstance(script, dict):
            text = str(script.get("video_content") or "").strip()
            if text:
                return text, "guangdada_detail_analysis"
    return "", ""


def _video_url_from_creative(creative: dict[str, Any]) -> str:
    try:
        return ve_db._pick_video_url_from_raw(creative)  # noqa: SLF001 - reuse local parser
    except Exception:
        return str(creative.get("video_url") or "").strip()


def _image_url_from_creative(creative: dict[str, Any]) -> str:
    try:
        return ve_db._pick_image_url_from_raw(creative)  # noqa: SLF001 - reuse local parser
    except Exception:
        return str(creative.get("preview_img_url") or "").strip()


def _record_from_item(target_date: str, ad_key: str, item: dict[str, Any] | None) -> dict[str, Any]:
    if item is None:
        return {
            "target_date": target_date,
            "ad_key": ad_key,
            "product": "",
            "appid": "",
            "video_content": "",
            "content_source": "",
            "raw_found": False,
            "raw_item": None,
        }
    creative = item.get("creative") if isinstance(item.get("creative"), dict) else {}
    video_content, content_source = material_video_content_from_creative(creative)
    video_url = _video_url_from_creative(creative)
    image_url = _image_url_from_creative(creative)
    preview_img_url = str(creative.get("preview_img_url") or image_url or "").strip()
    return {
        "target_date": target_date,
        "ad_key": ad_key,
        "product": str(item.get("product") or creative.get("app_name") or "").strip(),
        "appid": str(item.get("appid") or creative.get("appid") or "").strip(),
        "video_content": video_content,
        "content_source": content_source,
        "raw_found": True,
        "preview_img_url": preview_img_url,
        "video_url": video_url,
        "image_url": image_url,
        "heat": creative.get("heat") or 0,
        "impression": creative.get("impression") or 0,
        "all_exposure_value": creative.get("all_exposure_value") or 0,
        "raw_item": item,
    }


def _best_item(existing: dict[str, Any] | None, candidate: dict[str, Any]) -> dict[str, Any]:
    if existing is None:
        return candidate
    existing_creative = existing.get("creative") if isinstance(existing.get("creative"), dict) else {}
    candidate_creative = candidate.get("creative") if isinstance(candidate.get("creative"), dict) else {}
    existing_content, _ = material_video_content_from_creative(existing_creative)
    candidate_content, _ = material_video_content_from_creative(candidate_creative)
    if candidate_content and not existing_content:
        return candidate
    existing_exp = int(existing_creative.get("all_exposure_value") or existing_creative.get("impression") or 0)
    candidate_exp = int(candidate_creative.get("all_exposure_value") or candidate_creative.get("impression") or 0)
    return candidate if candidate_exp > existing_exp else existing


def collect_raw_records(scope: KeyScope, raw_dir: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for target_date in sorted(scope.dates):
        payload = _load_raw_payload(raw_dir, target_date)
        wanted = set(scope.dates[target_date])
        by_key: dict[str, dict[str, Any]] = {}
        for item in payload.get("items") or []:
            if not isinstance(item, dict):
                continue
            creative = item.get("creative")
            if not isinstance(creative, dict):
                continue
            ad_key = str(creative.get("ad_key") or "").strip()
            if ad_key not in wanted:
                continue
            if scope.product and str(item.get("product") or "").strip() != scope.product:
                continue
            by_key[ad_key] = _best_item(by_key.get(ad_key), item)
        for ad_key in scope.dates[target_date]:
            records.append(_record_from_item(target_date, ad_key, by_key.get(ad_key)))
    return records


def overlay_existing_video_content(
    records: list[dict[str, Any]],
    existing: dict[tuple[str, str], str],
) -> int:
    updated = 0
    for record in records:
        if str(record.get("video_content") or "").strip():
            continue
        key = (str(record.get("target_date") or ""), str(record.get("ad_key") or ""))
        text = str(existing.get(key) or "").strip()
        if not text:
            continue
        record["video_content"] = text
        record["content_source"] = "db_existing_guangdada_video_content"
        updated += 1
    return updated


def load_existing_video_content_map(scope: KeyScope) -> dict[tuple[str, str], str]:
    ve_db.init_db()
    conn = sqlite3.connect(ve_db.DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        out: dict[tuple[str, str], str] = {}
        for target_date in sorted(scope.dates):
            keys = scope.dates[target_date]
            if not keys:
                continue
            placeholders = ",".join("?" for _ in keys)
            sql = (
                "SELECT target_date, ad_key, guangdada_video_content "
                "FROM daily_creative_insights "
                f"WHERE target_date = ? AND ad_key IN ({placeholders}) "
                "AND COALESCE(TRIM(guangdada_video_content), '') <> ''"
            )
            for row in conn.execute(sql, [target_date, *keys]):
                out[(str(row["target_date"]), str(row["ad_key"]))] = str(row["guangdada_video_content"] or "")
        return out
    finally:
        conn.close()


def _creatives_from_search_results(results: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for result in results or []:
        for creative in result.get("all_creatives") or []:
            if not isinstance(creative, dict):
                continue
            ad_key = str(creative.get("ad_key") or "").strip()
            if not ad_key:
                continue
            by_key[ad_key] = creative
    return by_key


def _flatten_adkeys(adkeys: list[str]) -> list[str]:
    return parse_adkeys(adkeys)


def _safe_query_placeholders(count: int) -> str:
    count = max(0, int(count))
    return ",".join("?" for _ in range(count))


def _material_script_video_content(analysis: dict[str, Any]) -> str:
    if not isinstance(analysis, dict):
        return ""
    script = analysis.get("script_analysis")
    if isinstance(script, dict):
        text = str(script.get("video_content") or script.get("video_summary") or "").strip()
        if text:
            return text
    return str(analysis.get("video_content") or "").strip()


def _apply_video_content_to_records(
    records: list[dict[str, Any]],
    ad_key: str,
    video_content: str,
    *,
    content_source: str,
    fallback_source: str | None = None,
    creative: dict[str, Any] | None = None,
    material_script_analysis: dict[str, Any] | None = None,
) -> int:
    updated = 0
    for record in records:
        if str(record.get("ad_key") or "") != ad_key:
            continue
        if str(record.get("video_content") or "").strip():
            continue
        record["video_content"] = video_content
        record["content_source"] = content_source
        if fallback_source:
            record["fallback_source"] = fallback_source
        raw_item = record.get("raw_item")
        if not isinstance(raw_item, dict):
            raw_item = {
                "target_date": record.get("target_date", ""),
                "product": record.get("product", ""),
                "appid": record.get("appid", ""),
                "creative": {
                    "ad_key": ad_key,
                },
            }
        else:
            raw_item = copy.deepcopy(raw_item)
        creative_payload = creative
        if not isinstance(creative_payload, dict):
            creative_payload = {}
        creative_payload = copy.deepcopy(creative_payload)
        creative_payload.setdefault("ad_key", ad_key)
        creative_payload["guangdada_video_content"] = video_content
        if isinstance(material_script_analysis, dict) and material_script_analysis:
            creative_payload["material_script_analysis"] = copy.deepcopy(material_script_analysis)
        raw_item["creative"] = creative_payload
        record["raw_item"] = raw_item
        updated += 1
    return updated


def merge_fetched_creatives(records: list[dict[str, Any]], creatives: dict[str, dict[str, Any]]) -> int:
    updated = 0
    for record in records:
        if str(record.get("video_content") or "").strip():
            continue
        creative = creatives.get(str(record.get("ad_key") or ""))
        if not creative:
            continue
        video_content, source = material_video_content_from_creative(creative)
        if not video_content:
            continue
        record["video_content"] = video_content
        record["content_source"] = "guangdada_search_card" if source else ""
        record["fallback_source"] = source
        record["fetched_creative"] = creative
        if not record.get("raw_item"):
            record["raw_item"] = {
                "target_date": record.get("target_date"),
                "product": record.get("product") or "",
                "appid": record.get("appid") or creative.get("appid") or "",
                "creative": creative,
            }
        else:
            raw_item = copy.deepcopy(record["raw_item"])
            raw_creative = raw_item.get("creative") if isinstance(raw_item.get("creative"), dict) else {}
            raw_creative["guangdada_video_content"] = video_content
            raw_item["creative"] = raw_creative
            record["raw_item"] = raw_item
        updated += 1
    return updated


def _video_content_from_analysis(analysis: dict[str, Any]) -> str:
    if not isinstance(analysis, dict):
        return ""
    direct = str(analysis.get("video_content") or "").strip()
    if direct:
        return direct
    script = analysis.get("script_analysis")
    if isinstance(script, dict):
        return str(script.get("video_content") or "").strip()
    return ""


def _extract_json_object(text: str) -> dict[str, Any] | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    fenced = re.search(r"```(?:json)?\s*(.*?)```", raw, re.S | re.I)
    if fenced:
        raw = fenced.group(1).strip()
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        pass
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(raw[start : end + 1])
            return parsed if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            return None
    return None


def build_llm_video_content_analysis(model_output: str | dict[str, Any]) -> dict[str, Any]:
    """Normalize LLM fallback output into the Guangdada-like script_analysis shape."""
    payload = model_output if isinstance(model_output, dict) else _extract_json_object(str(model_output or ""))
    if not isinstance(payload, dict):
        payload = {"video_content": str(model_output or "").strip()}

    script = payload.get("script_analysis")
    if not isinstance(script, dict):
        script = {}
    video_content = str(
        script.get("video_content")
        or payload.get("video_content")
        or payload.get("video_summary")
        or ""
    ).strip()
    timeline = script.get("timeline")
    if not isinstance(timeline, list):
        timeline = payload.get("timeline") if isinstance(payload.get("timeline"), list) else []

    out: dict[str, Any] = {"source": "llm_video_content_fallback"}
    if video_content or timeline:
        out["script_analysis"] = {}
        if video_content:
            out["script_analysis"]["video_content"] = video_content
        if timeline:
            out["script_analysis"]["timeline"] = timeline
    return out


def _llm_video_content_system() -> str:
    return (
        "你是广告素材视频内容结构化助手。只描述画面、动作、字幕、口播和生成结果，"
        "不要输出投放建议、去重判断或业务筛选结论。输出必须是 JSON 对象。"
    )


def _llm_video_content_text_fallback_system(media_kind: str) -> str:
    word = "视频" if media_kind == "video" else "封面图"
    return (
        f"你是广告素材内容结构化助手。即使无法直接解析{word}，也只能根据给定标题、文案、链接和元信息"
        "输出保守的视频内容描述；不确定的地方不要编造。输出必须是 JSON 对象。"
    )


def _build_llm_video_content_prompt(item: dict[str, Any], creative: dict[str, Any], media_type: str) -> str:
    media_desc = "视频" if media_type == "video" else "封面图"
    title = str(creative.get("title") or creative.get("name") or item.get("title") or "").strip()
    desc = str(
        creative.get("desc")
        or creative.get("description")
        or creative.get("content")
        or creative.get("body")
        or ""
    ).strip()
    return f"""
请根据这条 UA 广告素材的{media_desc}，生成一份与广大大「素材脚本分析」兼容的视频内容结构。

素材信息：
- 产品: {item.get('product') or creative.get('app_name') or ''}
- AppID: {item.get('appid') or creative.get('appid') or ''}
- ad_key: {creative.get('ad_key') or ''}
- 标题: {title or '无'}
- 正文/描述: {desc or '无'}

输出要求：
- 只输出 JSON，不要 Markdown 代码围栏。
- `script_analysis.video_content` 用一段中文描述素材实际画面、动作、字幕/口播和生成结果。
- 如果能分段观察，给 `script_analysis.timeline`，每项可含 `time`、`label`。
- 如果只能看到封面图，就明确写「根据封面可见/推断」，不要假装完整看过视频。

JSON 格式：
{{
  "script_analysis": {{
    "video_content": "……",
    "timeline": [
      {{"time": "0:00", "label": "……"}}
    ]
  }}
}}
""".strip()


def generate_llm_video_content_analysis(item: dict[str, Any], creative: dict[str, Any]) -> dict[str, Any]:
    video_url = _video_url_from_creative(creative)
    image_url = _image_url_from_creative(creative)
    if video_url:
        media_url = video_url
        media_type = "video"
    elif image_url:
        media_url = image_url
        media_type = "image"
    else:
        return {}
    prompt = _build_llm_video_content_prompt(item, creative, media_type)
    output = call_vision(
        _llm_video_content_system(),
        prompt,
        media_url,
        media_type,  # type: ignore[arg-type]
        text_fallback_system=_llm_video_content_text_fallback_system(media_type),
        quiet=True,
    )
    return build_llm_video_content_analysis(output)


def apply_llm_video_content_fallback_to_missing(
    records: list[dict[str, Any]],
    *,
    generate_fn: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]] = generate_llm_video_content_analysis,
    limit: int | None = None,
) -> dict[str, int]:
    requested = 0
    updated = 0
    skipped_no_media = 0
    failed = 0
    max_count = None if limit is None or int(limit) <= 0 else int(limit)
    for record in records:
        if str(record.get("video_content") or "").strip():
            continue
        if max_count is not None and requested >= max_count:
            break
        raw_item = record.get("raw_item")
        if not isinstance(raw_item, dict):
            raw_item = {
                "target_date": record.get("target_date", ""),
                "product": record.get("product", ""),
                "appid": record.get("appid", ""),
                "creative": {"ad_key": record.get("ad_key", "")},
            }
        creative = raw_item.get("creative")
        if not isinstance(creative, dict):
            creative = {"ad_key": record.get("ad_key", "")}
        if not _video_url_from_creative(creative) and not _image_url_from_creative(creative):
            skipped_no_media += 1
            continue
        requested += 1
        try:
            analysis = build_llm_video_content_analysis(generate_fn(raw_item, creative))
        except Exception:
            failed += 1
            continue
        content = _video_content_from_analysis(analysis)
        if not content:
            failed += 1
            continue
        updated += _apply_video_content_to_records(
            records,
            str(record.get("ad_key") or creative.get("ad_key") or "").strip(),
            content,
            content_source="llm_video_content_fallback",
            fallback_source="llm_video_content_fallback",
            creative=creative,
            material_script_analysis=analysis,
        )
    return {"requested": requested, "updated": updated, "skipped_no_media": skipped_no_media, "failed": failed}


def apply_video_content_records_to_raw_payload(raw_payload: dict[str, Any], records: list[dict[str, Any]]) -> int:
    by_key: dict[str, dict[str, Any]] = {}
    for record in records:
        ad_key = str(record.get("ad_key") or "").strip()
        content = str(record.get("video_content") or "").strip()
        if ad_key and content:
            by_key[ad_key] = record
    updated = 0
    for item in raw_payload.get("items") or []:
        if not isinstance(item, dict):
            continue
        creative = item.get("creative")
        if not isinstance(creative, dict):
            continue
        ad_key = str(creative.get("ad_key") or "").strip()
        record = by_key.get(ad_key)
        if not record:
            continue
        content = str(record.get("video_content") or "").strip()
        if not content:
            continue
        changed = False
        if str(creative.get("guangdada_video_content") or "").strip() != content:
            creative["guangdada_video_content"] = content
            changed = True
        raw_item = record.get("raw_item")
        if isinstance(raw_item, dict):
            raw_creative = raw_item.get("creative")
            if isinstance(raw_creative, dict) and isinstance(raw_creative.get("material_script_analysis"), dict):
                material_script_analysis = copy.deepcopy(raw_creative["material_script_analysis"])
                if creative.get("material_script_analysis") != material_script_analysis:
                    creative["material_script_analysis"] = material_script_analysis
                    changed = True
        item["creative"] = creative
        if changed:
            updated += 1
    return updated


async def apply_direct_analysis_to_missing(
    records: list[dict[str, Any]],
    fetch_fn: Callable[[dict[str, Any]], Any],
) -> dict[str, int]:
    requested = 0
    updated = 0
    for record in records:
        if str(record.get("video_content") or "").strip():
            continue
        raw_item = record.get("raw_item")
        if not isinstance(raw_item, dict):
            continue
        creative = raw_item.get("creative")
        if not isinstance(creative, dict):
            continue
        if not str(creative.get("ad_key") or "").strip():
            continue
        requested += 1
        try:
            analysis = await fetch_fn(creative)
        except Exception:
            continue
        content = _video_content_from_analysis(analysis)
        if not content:
            continue
        record["video_content"] = content
        record["content_source"] = "guangdada_direct_material_script_analysis"
        new_item = copy.deepcopy(raw_item)
        new_creative = new_item.get("creative") if isinstance(new_item.get("creative"), dict) else {}
        new_creative["guangdada_video_content"] = content
        new_item["creative"] = new_creative
        record["raw_item"] = new_item
        updated += 1
    return {"requested": requested, "updated": updated}


async def fetch_direct_missing_video_content(
    records: list[dict[str, Any]],
    *,
    debug: bool = False,
) -> dict[str, int]:
    if not any(
        not str(record.get("video_content") or "").strip() and isinstance(record.get("raw_item"), dict)
        for record in records
    ):
        return {"requested": 0, "updated": 0}
    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not email or not password:
        raise RuntimeError("请在 .env 中设置 GUANGDADA_EMAIL 和 GUANGDADA_PASSWORD")

    from playwright.async_api import async_playwright

    from ua_workflows.shared.guangdada.proxy import prepare_playwright_proxy_for_crawl
    from ua_workflows.shared.guangdada.search import (
        _await_post_login_shell,
        _fetch_material_script_analysis_from_page,
        _login_or_handle_human_check,
    )

    playwright_proxy = prepare_playwright_proxy_for_crawl()
    async with async_playwright() as p:
        launch_kw: dict[str, Any] = {"headless": not debug}
        if playwright_proxy:
            launch_kw["proxy"] = playwright_proxy
        browser = await p.chromium.launch(**launch_kw)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = await context.new_page()
        try:
            print("[direct] 正在登录广大大并准备素材脚本分析接口...", flush=True)
            login_ok = await _login_or_handle_human_check(page, email, password)
            if not login_ok:
                raise RuntimeError("广大大登录失败")
            await _await_post_login_shell(page)
            print("[direct] 登录态可用，开始按 raw detail 参数请求脚本分析...", flush=True)

            async def fetch_one(creative: dict[str, Any]) -> dict[str, Any]:
                return await _fetch_material_script_analysis_from_page(page, creative)

            return await apply_direct_analysis_to_missing(records, fetch_one)
        finally:
            await browser.close()


async def fetch_missing_video_content(
    records: list[dict[str, Any]],
    *,
    date_range: tuple[str, str] | None = None,
    limit: int | None = None,
    debug: bool = False,
    max_scroll_rounds: int = 1,
    direct_first: bool = True,
) -> dict[str, Any]:
    direct_summary = {"requested": 0, "updated": 0}
    if direct_first:
        direct_summary = await fetch_direct_missing_video_content(records, debug=debug)

    missing = _dedupe_preserve_order(
        [record.get("ad_key") for record in records if not str(record.get("video_content") or "").strip()]
    )
    if limit is not None:
        missing = missing[: max(0, int(limit))]
    if not missing:
        return {"direct": direct_summary, "search": {"requested": 0, "updated": 0, "found_creatives": 0, "keywords": []}}

    os.environ.setdefault("VIDEO_ENHANCER_DOM_CLICK_MAX_CARDS", "1")
    os.environ.setdefault("VIDEO_ENHANCER_DOM_CLICK_MAX_PAGES", "1")
    os.environ.setdefault("GUANGDADA_DETAIL_ANALYSIS_WAIT_MS", "20000")

    from ua_workflows.shared.guangdada.search import run_batch

    results = await run_batch(
        keywords=missing,
        debug=debug,
        is_tool=True,
        order_by="latest",
        detail_click_primary=True,
        date_range=date_range,
        max_scroll_rounds=max_scroll_rounds,
        extract_detail_dom_analysis=True,
    )
    creatives = _creatives_from_search_results(results)
    updated = merge_fetched_creatives(records, creatives)
    return {
        "direct": direct_summary,
        "search": {
        "requested": len(missing),
        "updated": updated,
        "found_creatives": len(creatives),
        "keywords": missing,
        },
    }


def _payload_items_from_records(records: list[dict[str, Any]], target_date: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for record in records:
        if record.get("target_date") != target_date:
            continue
        raw_item = record.get("raw_item")
        if not isinstance(raw_item, dict):
            continue
        item = copy.deepcopy(raw_item)
        creative = item.get("creative") if isinstance(item.get("creative"), dict) else {}
        content = str(record.get("video_content") or "").strip()
        if content:
            creative["guangdada_video_content"] = content
        item["creative"] = creative
        out.append(item)
    return out


def upsert_video_content_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {
        "daily_upserted": 0,
        "library_upserted": 0,
        "library_grouped": 0,
        "daily_updated": 0,
        "library_updated": 0,
    }
    ve_db.init_db()
    conn = sqlite3.connect(ve_db.DB_PATH)
    try:
        for record in records:
            target_date = str(record.get("target_date") or "").strip()
            ad_key = str(record.get("ad_key") or "").strip()
            content = str(record.get("video_content") or "").strip()
            if not target_date or not ad_key or not content:
                continue
            cur = conn.execute(
                """
                UPDATE daily_creative_insights
                   SET guangdada_video_content = ?
                 WHERE target_date = ?
                   AND ad_key = ?
                """,
                (content, target_date, ad_key),
            )
            summary["daily_updated"] += int(cur.rowcount or 0)
            try:
                cur = conn.execute(
                    """
                    UPDATE creative_library
                       SET guangdada_video_content = ?
                     WHERE ad_key = ?
                    """,
                    (content, ad_key),
                )
                summary["library_updated"] += int(cur.rowcount or 0)
            except sqlite3.OperationalError:
                pass
        conn.commit()
    finally:
        conn.close()
    return summary


def build_similarity_payload(
    records: list[dict[str, Any]],
    *,
    embed_fn: Callable[[str], list[float]] = call_embedding,
    top_k: int = 3,
    min_similarity: float = 0.86,
) -> dict[str, Any]:
    work: list[dict[str, Any]] = []
    vectors: dict[str, list[float]] = {}
    empty_count = 0
    for record in records:
        item = {
            key: value
            for key, value in record.items()
            if key not in {"raw_item", "fetched_creative"}
        }
        text = str(record.get("video_content") or "").strip()
        if not text:
            empty_count += 1
            item["top_matches"] = []
            work.append(item)
            continue
        vectors[str(record["ad_key"])] = embed_fn(text)
        item["top_matches"] = []
        work.append(item)

    by_key = {str(row["ad_key"]): row for row in work}
    pairs: list[dict[str, Any]] = []
    keys = list(vectors)
    for i, left_key in enumerate(keys):
        for right_key in keys[i + 1 :]:
            sim = float(cosine_similarity(vectors[left_key], vectors[right_key]))
            left = by_key[left_key]
            right = by_key[right_key]
            left_date = str(left.get("target_date") or "")
            right_date = str(right.get("target_date") or "")
            if left_date == right_date:
                subject_key, subject = left_key, left
                candidate_key, candidate = right_key, right
                add_reverse_same_day = True
            elif left_date > right_date:
                subject_key, subject = left_key, left
                candidate_key, candidate = right_key, right
                add_reverse_same_day = False
            else:
                subject_key, subject = right_key, right
                candidate_key, candidate = left_key, left
                add_reverse_same_day = False
            pair = {
                "left_ad_key": subject_key,
                "right_ad_key": candidate_key,
                "left_date": subject.get("target_date"),
                "right_date": candidate.get("target_date"),
                "similarity": round(sim, 6),
                "cross_day": subject.get("target_date") != candidate.get("target_date"),
            }
            if sim >= min_similarity:
                pairs.append(pair)
            subject["top_matches"].append(
                {
                    "ad_key": candidate_key,
                    "target_date": candidate.get("target_date"),
                    "similarity": round(sim, 6),
                    "cross_day": subject.get("target_date") != candidate.get("target_date"),
                }
            )
            if add_reverse_same_day:
                candidate["top_matches"].append(
                    {
                        "ad_key": subject_key,
                        "target_date": subject.get("target_date"),
                        "similarity": round(sim, 6),
                        "cross_day": False,
                    }
                )
    for row in work:
        row["top_matches"] = sorted(
            row.get("top_matches") or [],
            key=lambda item: float(item.get("similarity") or 0.0),
            reverse=True,
        )[:top_k]

    pairs.sort(key=lambda item: float(item.get("similarity") or 0.0), reverse=True)
    return {
        "summary": {
            "records": len(records),
            "embedded_records": len(vectors),
            "empty_video_content": empty_count,
            "min_similarity": min_similarity,
            "top_k": top_k,
            "candidate_pairs": len(pairs),
        },
        "records": work,
        "pairs": pairs,
    }


def _proxy_url(url: str) -> str:
    if not url:
        return ""
    parsed = urllib.parse.urlparse(url)
    if parsed.netloc == "sp2cdn-idea-global.zingfront.com":
        return "/__media_proxy?u=" + urllib.parse.quote(url, safe="")
    return url


def _media_html(record: dict[str, Any]) -> str:
    video_url = _proxy_url(str(record.get("video_url") or ""))
    poster = _proxy_url(str(record.get("preview_img_url") or record.get("image_url") or ""))
    if video_url:
        video = (
            f'<video controls preload="metadata" poster="{html.escape(poster)}" '
            f'src="{html.escape(video_url)}"></video>'
        )
        if poster:
            video += f'<img class="cover-fallback" src="{html.escape(poster)}" alt="">'
        return video
    if poster:
        return f'<img src="{html.escape(poster)}" alt="">'
    return '<div class="empty-media">No media</div>'


def _cover_url(record: dict[str, Any]) -> str:
    return _proxy_url(str(record.get("preview_img_url") or record.get("image_url") or ""))


def _cover_img(record: dict[str, Any], class_name: str, alt: str = "") -> str:
    url = _cover_url(record)
    if not url:
        return f'<div class="{html.escape(class_name)} empty-cover"></div>'
    return f'<img class="{html.escape(class_name)}" src="{html.escape(url)}" alt="{html.escape(alt)}">'


def render_dashboard_html(payload: dict[str, Any], *, title: str, generated_at: str = "") -> str:
    summary = payload.get("summary") or {}
    records = payload.get("records") or []
    by_key = {str(r.get("ad_key") or ""): r for r in records}
    pairs = payload.get("pairs") or []

    def esc(value: Any) -> str:
        return html.escape(str(value if value is not None else ""))

    top_pair_rows = []
    for idx, pair in enumerate(pairs[:80], 1):
        left_key = str(pair.get("left_ad_key") or "")
        right_key = str(pair.get("right_ad_key") or "")
        left = by_key.get(left_key, {})
        right = by_key.get(right_key, {})
        top_pair_rows.append(
            "<tr>"
            f"<td>{idx}</td><td>{esc(pair.get('similarity'))}</td>"
            f'<td><div class="pair-cell">{_cover_img(left, "pair-cover", left_key)}'
            f'<div>{esc(pair.get("left_date"))}<br><code>{esc(left_key)}</code></div></div></td>'
            f'<td><div class="pair-cell">{_cover_img(right, "pair-cover", right_key)}'
            f'<div>{esc(pair.get("right_date"))}<br><code>{esc(right_key)}</code></div></div></td>'
            f"<td>{'跨日' if pair.get('cross_day') else '同日'}</td>"
            "</tr>"
        )
    top_pairs = "\n".join(top_pair_rows)
    if not top_pairs:
        top_pairs = '<tr><td colspan="5">没有达到阈值的候选对。</td></tr>'

    cards: list[str] = []
    for record in sorted(records, key=lambda r: (str(r.get("target_date") or ""), str(r.get("ad_key") or ""))):
        matches = record.get("top_matches") or []
        match_rows = []
        for match in matches:
            other = by_key.get(str(match.get("ad_key") or ""), {})
            match_rows.append(
                '<div class="match">'
                f'{_cover_img(other, "match-cover", str(match.get("ad_key") or ""))}'
                '<div class="match-body">'
                f'<b>{esc(match.get("similarity"))}</b> '
                f'{esc(match.get("target_date"))} '
                f'<code>{esc(match.get("ad_key"))}</code>'
                f'<p>{esc(str(other.get("video_content") or "")[:180])}</p>'
                '</div>'
                '</div>'
            )
        if not match_rows:
            match_rows.append('<div class="match muted">暂无可比对文本</div>')
        cards.append(
            '<article class="card">'
            f'<div class="media">{_media_html(record)}</div>'
            '<div class="body">'
            f'<div class="meta"><span>{esc(record.get("target_date"))}</span>'
            f'<code>{esc(record.get("ad_key"))}</code>'
            f'<span>{esc(record.get("content_source") or "missing")}</span></div>'
            f'<p class="content">{esc(record.get("video_content") or "无视频内容")}</p>'
            '<h3>相似度前三</h3>'
            + "".join(match_rows)
            + '</div></article>'
        )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(title)}</title>
<style>
body {{ margin:0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; color:#171717; background:#f7f8fb; }}
header {{ position:sticky; top:0; z-index:2; background:#ffffff; border-bottom:1px solid #e5e7eb; padding:18px 28px; }}
h1 {{ margin:0 0 10px; font-size:22px; }}
.summary {{ display:flex; flex-wrap:wrap; gap:10px; }}
.pill {{ background:#edf2ff; color:#1d4ed8; border:1px solid #c7d2fe; border-radius:6px; padding:6px 10px; font-size:13px; }}
main {{ padding:24px 28px 48px; }}
section {{ margin-bottom:28px; }}
h2 {{ font-size:18px; margin:0 0 12px; }}
table {{ width:100%; border-collapse:collapse; background:#fff; border:1px solid #e5e7eb; }}
th,td {{ border-bottom:1px solid #e5e7eb; padding:10px; text-align:left; vertical-align:top; font-size:13px; }}
th {{ background:#f1f5f9; }}
code {{ font-size:12px; background:#f3f4f6; padding:2px 4px; border-radius:4px; word-break:break-all; }}
.grid {{ display:grid; grid-template-columns:repeat(auto-fill,minmax(520px,1fr)); gap:14px; }}
.card {{ display:grid; grid-template-columns:180px minmax(0,1fr); gap:14px; background:#fff; border:1px solid #e5e7eb; border-radius:8px; padding:12px; }}
.media video,.media img {{ width:180px; aspect-ratio:9/16; object-fit:cover; border-radius:6px; background:#111827; }}
.media .cover-fallback {{ display:block; margin-top:8px; }}
.pair-cell {{ display:flex; align-items:flex-start; gap:10px; }}
.pair-cover {{ width:88px; aspect-ratio:9/16; object-fit:cover; border-radius:6px; background:#111827; flex:0 0 auto; }}
.match {{ display:flex; gap:10px; align-items:flex-start; }}
.match-cover {{ width:64px; aspect-ratio:9/16; object-fit:cover; border-radius:5px; background:#111827; flex:0 0 auto; }}
.empty-cover {{ display:inline-block; background:#eef2f7; }}
.match-body {{ min-width:0; }}
.empty-media {{ width:180px; aspect-ratio:9/16; display:grid; place-items:center; color:#6b7280; background:#eef2f7; border-radius:6px; }}
.meta {{ display:flex; flex-wrap:wrap; gap:8px; align-items:center; color:#4b5563; font-size:12px; margin-bottom:8px; }}
.content {{ margin:0 0 10px; line-height:1.55; }}
h3 {{ margin:10px 0 8px; font-size:14px; }}
.match {{ border-top:1px solid #eef2f7; padding:8px 0; font-size:13px; }}
.match p {{ color:#4b5563; margin:4px 0 0; line-height:1.45; }}
.muted {{ color:#6b7280; }}
@media (max-width: 720px) {{
  header, main {{ padding-left:14px; padding-right:14px; }}
  .grid {{ grid-template-columns:1fr; }}
  .card {{ grid-template-columns:1fr; }}
  .media video,.media img,.empty-media {{ width:100%; max-height:360px; }}
}}
</style>
</head>
<body>
<header>
<h1>{esc(title)}</h1>
<div class="summary">
<span class="pill">记录 {esc(summary.get("records"))}</span>
<span class="pill">有视频内容 {esc(summary.get("embedded_records"))}</span>
<span class="pill">空视频内容 {esc(summary.get("empty_video_content"))}</span>
<span class="pill">候选对 {esc(summary.get("candidate_pairs"))}</span>
<span class="pill">阈值 {esc(summary.get("min_similarity"))}</span>
<span class="pill">生成 {esc(generated_at)}</span>
</div>
</header>
<main>
<section>
<h2>相似候选对</h2>
<table>
<thead><tr><th>#</th><th>相似度</th><th>待判素材</th><th>历史/同日候选</th><th>类型</th></tr></thead>
<tbody>{top_pairs}</tbody>
</table>
</section>
<section>
<h2>逐条素材 · 相似度前三</h2>
<div class="grid">{''.join(cards)}</div>
</section>
</main>
</body>
</html>"""


def write_backfill_report(path: Path, records: list[dict[str, Any]], db_summary: dict[str, Any]) -> dict[str, Any]:
    sanitized = [
        {key: value for key, value in record.items() if key not in {"raw_item", "fetched_creative"}}
        for record in records
    ]
    summary = {
        "records": len(records),
        "raw_found": sum(1 for r in records if r.get("raw_found")),
        "with_video_content": sum(1 for r in records if str(r.get("video_content") or "").strip()),
        "missing_video_content": sum(1 for r in records if not str(r.get("video_content") or "").strip()),
        "content_sources": {},
        "db": db_summary,
    }
    sources: dict[str, int] = {}
    for record in records:
        source = str(record.get("content_source") or "missing")
        sources[source] = sources.get(source, 0) + 1
    summary["content_sources"] = sources
    payload = {"summary": summary, "records": sanitized}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def read_video_content_records_from_db(scope: KeyScope) -> list[dict[str, Any]]:
    ve_db.init_db()
    conn = sqlite3.connect(ve_db.DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows: list[dict[str, Any]] = []
        for target_date in sorted(scope.dates):
            placeholders = ",".join("?" for _ in scope.dates[target_date])
            sql = (
                "SELECT target_date, product, appid, ad_key, video_url, preview_img_url, "
                "guangdada_video_content, heat, impression, all_exposure_value "
                "FROM daily_creative_insights "
                f"WHERE target_date = ? AND ad_key IN ({placeholders})"
            )
            args = [target_date, *scope.dates[target_date]]
            by_key = {str(row["ad_key"]): dict(row) for row in conn.execute(sql, args)}
            for ad_key in scope.dates[target_date]:
                row = by_key.get(ad_key)
                if not row:
                    rows.append({"target_date": target_date, "ad_key": ad_key, "video_content": ""})
                    continue
                rows.append(
                    {
                        "target_date": target_date,
                        "product": row.get("product") or "",
                        "appid": row.get("appid") or "",
                        "ad_key": ad_key,
                        "video_url": row.get("video_url") or "",
                        "preview_img_url": row.get("preview_img_url") or "",
                        "video_content": row.get("guangdada_video_content") or "",
                        "content_source": "db_guangdada_video_content" if row.get("guangdada_video_content") else "",
                        "heat": row.get("heat") or 0,
                        "impression": row.get("impression") or 0,
                        "all_exposure_value": row.get("all_exposure_value") or 0,
                    }
                )
        return rows
    finally:
        conn.close()


def read_video_content_records_for_adkeys(
    adkeys: list[str],
    target_dates: list[str] | None = None,
    product: str = "",
) -> list[dict[str, Any]]:
    normalized_dates = _dedupe_preserve_order([str(date or "").strip() for date in (target_dates or [])])
    normalized_adkeys = _flatten_adkeys(adkeys)
    if not normalized_adkeys:
        return []
    ve_db.init_db()
    conn = sqlite3.connect(ve_db.DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows: list[dict[str, Any]] = []
        where_base = [f"ad_key IN ({_safe_query_placeholders(len(normalized_adkeys))})"]
        args: list[Any] = [*normalized_adkeys]
        if normalized_dates:
            where_base.append(f"target_date IN ({_safe_query_placeholders(len(normalized_dates))})")
            args.extend(normalized_dates)
        if product:
            where_base.append("product = ?")
            args.append(product)
        where_clause = " AND ".join(where_base)
        sql = (
            "SELECT target_date, product, appid, ad_key, video_url, preview_img_url, "
            "guangdada_video_content, heat, impression, all_exposure_value "
            f"FROM daily_creative_insights WHERE {where_clause} "
            "ORDER BY target_date ASC, appid, ad_key ASC"
        )
        # appid 列不存在时回退到无该列排序。
        try:
            cur_rows = conn.execute(sql, args).fetchall()
        except sqlite3.OperationalError:
            sql_fallback = (
                "SELECT target_date, product, appid, ad_key, video_url, preview_img_url, "
                "guangdada_video_content, heat, impression, all_exposure_value "
                f"FROM daily_creative_insights WHERE {where_clause} "
                "ORDER BY target_date ASC, ad_key ASC"
            )
            cur_rows = conn.execute(sql_fallback, args).fetchall()
        for row in cur_rows:
            target_date = str(row["target_date"] or "")
            ad_key = str(row["ad_key"] or "")
            if not target_date or not ad_key:
                continue
            record = dict(row)
            rows.append(
                {
                    "target_date": target_date,
                    "ad_key": ad_key,
                    "product": record.get("product") or "",
                    "appid": record.get("appid") or "",
                    "video_url": record.get("video_url") or "",
                    "preview_img_url": record.get("preview_img_url") or "",
                    "video_content": record.get("guangdada_video_content") or "",
                    "content_source": "db_guangdada_video_content"
                    if record.get("guangdada_video_content")
                    else "",
                    "heat": record.get("heat") or 0,
                    "impression": record.get("impression") or 0,
                    "all_exposure_value": record.get("all_exposure_value") or 0,
                    "raw_item": {
                        "creative": {
                            "ad_key": ad_key,
                            "app_type": record.get("app_type"),
                            "video_url": record.get("video_url") or "",
                            "preview_img_url": record.get("preview_img_url") or "",
                        }
                    },
                }
            )
        return rows
    finally:
        conn.close()


async def fetch_missing_video_content_by_adkeys(
    records: list[dict[str, Any]],
    *,
    retries: int = 3,
    debug: bool = False,
    setup_search: bool = True,
    max_scroll_rounds: int = 1,
    use_direct: bool = True,
    date_range: tuple[str, str] | None = None,
    skip_time_filter: bool = False,
    per_key_timeout_sec: float | None = None,
    persist_each_update: bool = False,
) -> dict[str, Any]:
    if retries < 1:
        retries = 1

    adkey_to_records: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        ad_key = str(record.get("ad_key") or "").strip()
        if not ad_key:
            continue
        if str(record.get("video_content") or "").strip():
            continue
        adkey_to_records.setdefault(ad_key, []).append(record)

    if not adkey_to_records:
        return {
            "direct": {"requested": 0, "updated": 0, "attempts": 0},
            "search": {"requested": 0, "updated": 0, "attempts": 0},
        }

    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not email or not password:
        raise RuntimeError("请在 .env 中设置 GUANGDADA_EMAIL 和 GUANGDADA_PASSWORD")

    from playwright.async_api import async_playwright

    from ua_workflows.shared.guangdada.proxy import prepare_playwright_proxy_for_crawl
    from ua_workflows.shared.guangdada.search import (
        _await_post_login_shell,
        _collect_keyword_crawl_result_latest_dom_detail,
        _do_setup,
        _fetch_material_script_analysis_from_page,
        _login_or_handle_human_check,
    )

    direct_summary = {"requested": 0, "updated": 0, "attempts": 0}
    search_summary = {
        "requested": 0,
        "updated": 0,
        "attempts": 0,
        "timeouts": 0,
        "errors": 0,
        "persist_errors": 0,
    }

    playwright_proxy = prepare_playwright_proxy_for_crawl()
    async with async_playwright() as p:
        launch_kw: dict[str, Any] = {"headless": not debug}
        if playwright_proxy:
            launch_kw["proxy"] = playwright_proxy
        browser = await p.chromium.launch(**launch_kw)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = await context.new_page()
        try:
            print("[backfill] 正在登录广大大...", flush=True)
            login_ok = await _login_or_handle_human_check(page, email, password)
            if not login_ok:
                raise RuntimeError("广大大登录失败")
            await _await_post_login_shell(page)

            if setup_search:
                print("[backfill] 一次性设置筛选（工具/素材/最新创意）...", flush=True)
                if not await _do_setup(
                    page,
                    is_tool=True,
                    log_prefix="  ",
                    order_by="latest",
                    date_range=date_range,
                    skip_time_filter=bool(skip_time_filter),
                ):
                    print("[失败] 筛选条件未就绪，终止抓取。", file=sys.stderr)
                    return {"direct": direct_summary, "search": search_summary}

            if use_direct:
                for ad_key, record_group in adkey_to_records.items():
                    if not record_group:
                        continue
                    creative = {"ad_key": ad_key}
                    raw_item = record_group[0].get("raw_item")
                    if isinstance(raw_item, dict):
                        creative_candidate = raw_item.get("creative")
                        if isinstance(creative_candidate, dict):
                            creative.update(creative_candidate)

                    for attempt in range(1, retries + 1):
                        direct_summary["attempts"] += 1
                        direct_summary["requested"] += 1
                        try:
                            analysis = await _fetch_material_script_analysis_from_page(page, creative)
                        except Exception:
                            analysis = {}
                        content = _material_script_video_content(analysis)
                        if not content:
                            continue
                        updated = _apply_video_content_to_records(
                            record_group,
                            ad_key,
                            content,
                            content_source="guangdada_direct_material_script_analysis",
                        )
                        if updated:
                            direct_summary["updated"] += updated
                            if persist_each_update:
                                _persist_adkey_video_content(records, ad_key, search_summary)
                            adkey_to_records[ad_key] = [
                                r for r in adkey_to_records[ad_key] if str(r.get("video_content") or "").strip()
                            ]
                            break

            if setup_search:
                for attempt in range(1, retries + 1):
                    missing = [
                        ad_key
                        for ad_key, recs in adkey_to_records.items()
                        if any(not str(r.get("video_content") or "").strip() for r in recs)
                    ]
                    if not missing:
                        break

                    for index, ad_key in enumerate(missing, 1):
                        creative_rows = adkey_to_records.get(ad_key, [])
                        if not any(not str(r.get("video_content") or "").strip() for r in creative_rows):
                            continue
                        search_summary["attempts"] += 1
                        search_summary["requested"] += 1
                        print(
                            f"[backfill] 搜索回填 round={attempt}/{retries} key={index}/{len(missing)} "
                            f"ad_key={ad_key[:12]}...",
                            flush=True,
                        )
                        batches_ref: list[Any] = []
                        capture_state: dict[str, Any] = {"enabled": False}
                        try:
                            result, timed_out = await _collect_adkey_search_result_with_timeout(
                                _collect_keyword_crawl_result_latest_dom_detail,
                                page,
                                ad_key,
                                batches_ref,
                                capture_state,
                                max_scroll_rounds=max_scroll_rounds,
                                debug=debug,
                                timeout_sec=per_key_timeout_sec,
                            )
                            if timed_out:
                                search_summary["timeouts"] += 1
                                print(f"[backfill] ad_key={ad_key[:12]} 单条搜索超时，跳过本轮。", flush=True)
                                continue
                        except Exception:
                            search_summary["errors"] += 1
                            continue
                        if not result:
                            continue
                        creatives = _creatives_from_search_results([result])
                        creative = creatives.get(ad_key)
                        if not creative:
                            continue
                        content = material_video_content_from_creative(creative)[0]
                        if not content:
                            continue
                        updated = _apply_video_content_to_records(
                            records,
                            ad_key,
                            content,
                            content_source="guangdada_search_card",
                            fallback_source="guangdada_search_card",
                            creative=creative,
                        )
                        if updated:
                            search_summary["updated"] += updated
                            if persist_each_update:
                                _persist_adkey_video_content(records, ad_key, search_summary)
                            print(f"[backfill] ad_key={ad_key[:12]} 已获取素材脚本分析并写入内存。", flush=True)

            return {"direct": direct_summary, "search": search_summary}
        finally:
            await browser.close()


async def _collect_adkey_search_result_with_timeout(
    collect_fn: Callable[..., Any],
    page: Any,
    ad_key: str,
    batches_ref: list[Any],
    capture_state: dict[str, Any],
    *,
    max_scroll_rounds: int,
    debug: bool,
    timeout_sec: float | None = None,
) -> tuple[dict[str, Any] | None, bool]:
    coro = collect_fn(
        page,
        ad_key,
        batches_ref,
        capture_state,
        log_prefix="  ",
        max_scroll_rounds=max_scroll_rounds,
        log_quiet=not debug,
        extract_detail_dom_analysis=True,
    )
    if timeout_sec and timeout_sec > 0:
        try:
            return await asyncio.wait_for(coro, timeout=timeout_sec), False
        except asyncio.TimeoutError:
            return None, True
    return await coro, False


def _persist_adkey_video_content(
    records: list[dict[str, Any]],
    ad_key: str,
    summary: dict[str, Any],
) -> None:
    changed = [
        record
        for record in records
        if str(record.get("ad_key") or "").strip() == ad_key and str(record.get("video_content") or "").strip()
    ]
    if not changed:
        return
    try:
        persisted = upsert_video_content_records(changed)
    except Exception as exc:  # pragma: no cover - defensive runtime reporting
        summary["persist_errors"] = int(summary.get("persist_errors") or 0) + 1
        print(f"[backfill] ad_key={ad_key[:12]} 即时写库失败: {exc}", flush=True)
        return
    summary["persist_daily_updated"] = int(summary.get("persist_daily_updated") or 0) + int(
        persisted.get("daily_updated") or 0
    )
    summary["persist_library_updated"] = int(summary.get("persist_library_updated") or 0) + int(
        persisted.get("library_updated") or 0
    )
    print(f"[backfill] ad_key={ad_key[:12]} 已即时写库。", flush=True)


def run(args: argparse.Namespace) -> dict[str, Any]:
    load_project_env(override=True)
    scope = load_key_scope(Path(args.keys_json))
    records = collect_raw_records(scope, Path(args.raw_dir))
    resumed = overlay_existing_video_content(records, load_existing_video_content_map(scope))
    fetch_summary: dict[str, Any] = {"requested": 0, "updated": 0}
    if not args.skip_browser:
        fetch_summary = asyncio.run(
            fetch_missing_video_content(
                records,
                date_range=scope.date_range,
                limit=args.fallback_limit,
                debug=args.debug,
                max_scroll_rounds=args.max_scroll_rounds,
                direct_first=not args.no_direct,
            )
        )
    db_summary = upsert_video_content_records(records)
    backfill_payload = write_backfill_report(
        Path(args.backfill_json),
        records,
        {**db_summary, "fetch": fetch_summary, "resumed_from_db": resumed},
    )

    db_records = read_video_content_records_from_db(scope)
    payload = build_similarity_payload(
        db_records,
        top_k=args.top_k,
        min_similarity=args.min_similarity,
    )
    payload["scope"] = {"product": scope.product, "dates": scope.dates, "ad_keys": scope.ad_keys}
    Path(args.output_json).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output_json).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    from datetime import datetime

    title = f"{scope.product} 视频内容 embedding 去重 · {scope.date_range[0]}~{scope.date_range[1]}"
    html_text = render_dashboard_html(payload, title=title, generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    Path(args.report_html).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report_html).write_text(html_text, encoding="utf-8")
    return {
        "scope": {"product": scope.product, "dates": scope.dates},
        "backfill": backfill_payload["summary"],
        "similarity": payload["summary"],
        "output_json": str(args.output_json),
        "backfill_json": str(args.backfill_json),
        "report_html": str(args.report_html),
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill VE Guangdada video-content text and build embedding dedupe dashboard.")
    parser.add_argument("--keys-json", default=str(DEFAULT_KEYS_JSON))
    parser.add_argument("--raw-dir", default=str(DEFAULT_REMOTE_RAW_DIR))
    parser.add_argument("--output-json", default=str(DEFAULT_OUTPUT_JSON))
    parser.add_argument("--backfill-json", default=str(DEFAULT_BACKFILL_JSON))
    parser.add_argument("--report-html", default=str(DEFAULT_REPORT_HTML))
    parser.add_argument("--skip-browser", action="store_true", help="Only use raw material_script_analysis; do not search Guangdada for missing adkeys.")
    parser.add_argument("--no-direct", action="store_true", help="Skip direct material-script-analysis API fetch and go straight to adkey search fallback.")
    parser.add_argument("--fallback-limit", type=int, default=None, help="Maximum missing adkeys to fetch from Guangdada search.")
    parser.add_argument("--debug", action="store_true", help="Run Guangdada browser headed.")
    parser.add_argument("--max-scroll-rounds", type=int, default=1)
    parser.add_argument("--min-similarity", type=float, default=0.86)
    parser.add_argument("--top-k", type=int, default=3)
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    summary = run(args)
    json.dump(summary, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
