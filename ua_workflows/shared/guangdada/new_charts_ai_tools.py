"""Collect Guangdada new creative chart AI tool videos as VE-style raw JSON."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from ua_workflows.shared.config import DATA_DIR, load_project_env
from ua_workflows.shared.guangdada.login import login
from ua_workflows.shared.guangdada.proxy import prepare_playwright_proxy_for_crawl
from ua_workflows.shared.media.resolve import normalize_video_url_for_consumption

NEW_CHARTS_URL = "https://www.guangdada.net/modules/creative/charts/new-charts"
VE_RAW_CATEGORY = "ai_tools_new_charts"
AUTH_STATE_PATH = DATA_DIR / "guangdada_new_charts_auth.json"

REQUIRED_FIELDS = [
    "ad_key",
    "advertiser_name",
    "title",
    "body",
    "platform",
    "video_url",
    "preview_img_url",
    "first_seen",
    "last_seen",
    "days_count",
    "heat",
    "impression",
    "all_exposure_value",
    "video_duration",
]

INTERRUPTION_TEXTS = [
    "您已在其他设备登录",
    "当前登录被强制退出",
    "请完成安全验证",
    "请完成验证",
    "人机验证",
]


def _debug_print(enabled: bool, message: str) -> None:
    if enabled:
        print(f"[new-charts][debug] {message}")

_CREATIVE_ID_KEYS = ("ad_key", "creative_id", "creativeId")
_CREATIVE_SHAPE_KEYS = (
    "advertiser_name",
    "resource_urls",
    "preview_img_url",
    "first_seen",
    "last_seen",
    "platform",
    "impression",
    "all_exposure_value",
)


@dataclass(frozen=True)
class AiToolCategory:
    label: str
    slug: str


_CATEGORY_ALIASES = {
    "ai图像生成": AiToolCategory(label="AI工具/AI图像", slug="ai_image"),
    "ai图片生成": AiToolCategory(label="AI工具/AI图像", slug="ai_image"),
    "ai图像": AiToolCategory(label="AI工具/AI图像", slug="ai_image"),
    "ai图片": AiToolCategory(label="AI工具/AI图像", slug="ai_image"),
    "ai image": AiToolCategory(label="AI工具/AI图像", slug="ai_image"),
    "ai视频": AiToolCategory(label="AI工具/AI视频生成", slug="ai_video_generation"),
    "ai视频生成": AiToolCategory(label="AI工具/AI视频生成", slug="ai_video_generation"),
    "ai video": AiToolCategory(label="AI工具/AI视频生成", slug="ai_video_generation"),
}


def _today_utc8() -> str:
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")


def _alias_key(raw: str) -> str:
    return re.sub(r"\s+", "", str(raw or "").strip().lower())


def resolve_ai_tool_category(raw: str) -> AiToolCategory:
    """Resolve a user-facing category alias to the Guangdada UI label."""
    key = _alias_key(raw)
    if key in _CATEGORY_ALIASES:
        return _CATEGORY_ALIASES[key]
    if "/" in raw:
        slug = re.sub(r"[^a-z0-9]+", "_", raw.lower()).strip("_") or "ai_tool"
        return AiToolCategory(label=raw.strip(), slug=slug)
    raise ValueError(f"未知 AI 工具类目: {raw!r}")


def category_ui_label(category: AiToolCategory) -> str:
    """Return the label visible in Guangdada's second-level tool category panel."""
    return category.label.split("/")[-1].strip() or category.label


def extract_creative_lists(obj: Any) -> list[list[dict[str, Any]]]:
    """Recursively find creative-list shaped arrays in Guangdada NAPI JSON."""
    results: list[list[dict[str, Any]]] = []

    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            keys = obj[0].keys()
            has_creative_id = any(k in keys for k in _CREATIVE_ID_KEYS)
            has_creative_shape = any(k in keys for k in _CREATIVE_SHAPE_KEYS)
            if has_creative_id or (("id" in keys) and has_creative_shape):
                results.append(obj)
        for item in obj:
            results.extend(extract_creative_lists(item))
    elif isinstance(obj, dict):
        if "__meta__" in obj:
            return results
        for value in obj.values():
            results.extend(extract_creative_lists(value))
    return results


def _iter_resource_dicts(creative: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for key in ("resource_urls", "resourceUrls", "resources", "materials", "creative_resources"):
        value = creative.get(key)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    yield item


def _first_string(creative: dict[str, Any], keys: Iterable[str]) -> str:
    for key in keys:
        value = creative.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def pick_video_url(creative: dict[str, Any]) -> str:
    """Return the most stable video URL from a creative row."""
    direct = _first_string(
        creative,
        (
            "video_url",
            "videoUrl",
            "video",
            "play_url",
            "playUrl",
            "download_url",
            "downloadUrl",
        ),
    )
    if direct:
        return normalize_video_url_for_consumption(direct)
    for resource in _iter_resource_dicts(creative):
        url = _first_string(resource, ("video_url", "videoUrl", "url", "play_url", "playUrl"))
        if url and (".mp4" in url or ".mov" in url or resource.get("type") in (2, "2", "video")):
            return normalize_video_url_for_consumption(url)
    return ""


def pick_preview_img_url(creative: dict[str, Any]) -> str:
    direct = _first_string(
        creative,
        (
            "preview_img_url",
            "previewImgUrl",
            "cover_url",
            "coverUrl",
            "image_url",
            "imageUrl",
            "thumbnail_url",
            "thumbnailUrl",
        ),
    )
    if direct:
        return direct
    for resource in _iter_resource_dicts(creative):
        url = _first_string(resource, ("image_url", "imageUrl", "cover_url", "coverUrl", "url"))
        if url and not (".mp4" in url or ".mov" in url):
            return url
    return ""


def _creative_ad_key(creative: dict[str, Any]) -> str:
    return _first_string(creative, ("ad_key", "creative_id", "creativeId", "id", "ad_id", "adId"))


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    if isinstance(value, (list, dict)) and not value:
        return True
    return False


def _normalize_creative_for_ve_raw(
    creative: dict[str, Any],
    *,
    rank: int,
    video_rank: int,
    category: AiToolCategory,
) -> dict[str, Any] | None:
    video_url = pick_video_url(creative)
    if not video_url:
        return None

    normalized = dict(creative)
    ad_key = _creative_ad_key(normalized)
    if ad_key:
        normalized["ad_key"] = ad_key
    normalized["video_url"] = video_url

    preview = pick_preview_img_url(normalized)
    if preview:
        normalized["preview_img_url"] = preview

    normalized["new_charts_rank"] = rank
    normalized["new_charts_video_rank"] = video_rank
    normalized["new_charts_category_label"] = category.label
    normalized["new_charts_category_slug"] = category.slug
    normalized["new_charts_source_url"] = NEW_CHARTS_URL
    normalized.setdefault("creative_type", "video")
    return normalized


def build_ve_raw_payload(
    *,
    target_date: str,
    per_category_raw: dict[AiToolCategory, list[dict[str, Any]]],
    source_url: str = NEW_CHARTS_URL,
) -> dict[str, Any]:
    """Build a VE-compatible raw payload from per-category chart rows."""
    items: list[dict[str, Any]] = []
    competitors: list[str] = []
    filter_report: dict[str, Any] = {
        "source": "guangdada_new_charts_ai_tools",
        "source_url": source_url,
        "per_category": {},
    }

    for category, rows in per_category_raw.items():
        if category.label not in competitors:
            competitors.append(category.label)
        seen: set[str] = set()
        kept = 0
        skipped_no_video = 0
        skipped_duplicate = 0
        skipped_no_video_samples: list[dict[str, Any]] = []
        for raw_rank, raw in enumerate(rows, start=1):
            normalized = _normalize_creative_for_ve_raw(raw, rank=raw_rank, video_rank=kept + 1, category=category)
            if not normalized:
                skipped_no_video += 1
                if len(skipped_no_video_samples) < 10:
                    skipped_no_video_samples.append(
                        {
                            "rank": raw_rank,
                            "ad_key": _creative_ad_key(raw),
                            "title": raw.get("title"),
                            "advertiser_name": raw.get("advertiser_name"),
                            "preview_img_url": pick_preview_img_url(raw),
                            "video_duration": raw.get("video_duration"),
                            "resource_urls_count": len(raw.get("resource_urls") or []),
                        }
                    )
                continue
            dedupe_key = normalized.get("ad_key") or normalized.get("video_url") or json.dumps(normalized, ensure_ascii=False, sort_keys=True)
            if dedupe_key in seen:
                skipped_duplicate += 1
                continue
            seen.add(str(dedupe_key))
            kept += 1
            items.append(
                {
                    "category": VE_RAW_CATEGORY,
                    "product": category.label,
                    "appid": category.slug,
                    "keyword": category.label,
                    "creative": normalized,
                }
            )

        filter_report["per_category"][category.label] = {
            "raw": len(rows),
            "kept_video": kept,
            "skipped_no_video": skipped_no_video,
            "skipped_duplicate": skipped_duplicate,
            "skipped_no_video_samples": skipped_no_video_samples,
        }

    payload = {
        "target_date": target_date,
        "crawl_date": target_date,
        "total": len(items),
        "competitors": competitors,
        "items": items,
        "filter_report": filter_report,
    }
    payload["completeness_report"] = build_completeness_report(payload)
    return payload


def build_completeness_report(payload: dict[str, Any]) -> dict[str, Any]:
    items = payload.get("items") or []
    field_counts = {field: 0 for field in REQUIRED_FIELDS}
    missing_counts = {field: 0 for field in REQUIRED_FIELDS}
    per_category: dict[str, Any] = {}

    for item in items:
        creative = item.get("creative") if isinstance(item, dict) else {}
        if not isinstance(creative, dict):
            creative = {}
        category = str(item.get("product") or "unknown")
        category_report = per_category.setdefault(
            category,
            {
                "total": 0,
                "field_counts": {field: 0 for field in REQUIRED_FIELDS},
                "missing_counts": {field: 0 for field in REQUIRED_FIELDS},
            },
        )
        category_report["total"] += 1
        for field in REQUIRED_FIELDS:
            if _is_missing(creative.get(field)):
                missing_counts[field] += 1
                category_report["missing_counts"][field] += 1
            else:
                field_counts[field] += 1
                category_report["field_counts"][field] += 1

    return {
        "total": len(items),
        "required_fields": REQUIRED_FIELDS,
        "field_counts": field_counts,
        "missing_counts": missing_counts,
        "per_category": per_category,
    }


async def _click_first(page: Any, selectors: Iterable[str], *, timeout: int = 4000, debug: bool = False) -> bool:
    for selector in selectors:
        try:
            loc = page.locator(selector)
            if await loc.count() > 0:
                await loc.first.scroll_into_view_if_needed(timeout=timeout)
                await loc.first.click(timeout=timeout)
                _debug_print(debug, f"click selector success: {selector}")
                return True
        except Exception:
            _debug_print(debug, f"click selector failed: {selector}")
            continue
    _debug_print(debug, f"click selector all failed: {list(selectors)}")
    return False


async def _click_text(page: Any, texts: Iterable[str], *, exact: bool = True, timeout: int = 4000, debug: bool = False) -> bool:
    for text in texts:
        try:
            loc = page.get_by_text(text, exact=exact)
            if await loc.count() > 0:
                await loc.first.scroll_into_view_if_needed(timeout=timeout)
                await loc.first.click(timeout=timeout)
                _debug_print(debug, f"click text success: {text}")
                return True
        except Exception:
            _debug_print(debug, f"click text by get_by_text failed: {text}")
            continue
        try:
            loc = page.locator(f"text={text}")
            if await loc.count() > 0:
                await loc.first.scroll_into_view_if_needed(timeout=timeout)
                await loc.first.click(timeout=timeout)
                _debug_print(debug, f"click text locator success: {text}")
                return True
        except Exception:
            _debug_print(debug, f"click text locator failed: {text}")
            continue
    _debug_print(debug, f"click text all failed: {list(texts)}")
    return False


async def _page_text(page: Any) -> str:
    try:
        return await page.locator("body").inner_text(timeout=5000)
    except Exception:
        return ""


async def _raise_if_interrupted(page: Any, *, step: str) -> None:
    text = await _page_text(page)
    for marker in INTERRUPTION_TEXTS:
        if marker in text:
            raise RuntimeError(f"广大大页面中断: {step}: {marker}")


async def _select_tool_tab(page: Any, *, debug: bool = False) -> None:
    try:
        await page.get_by_role("tab", name="工具", exact=True).click(timeout=8000)
    except Exception:
        ok = await _click_first(
            page,
            [
                '[role="tab"]:has-text("工具")',
                'button:has-text("工具")',
            ],
            timeout=6000,
            debug=debug,
        )
        if not ok:
            await _click_text(page, ["工具"], exact=True, timeout=6000, debug=debug)
    _debug_print(debug, "selected 工具 tab")
    await page.wait_for_timeout(1200)


async def _select_video_only(page: Any, *, debug: bool = False) -> None:
    opened = False
    try:
        loc = page.locator("#creative_charts_filter_ads_type")
        if await loc.count() > 0:
            await loc.first.click(timeout=5000, force=True)
            opened = True
            _debug_print(debug, "open ad type select via #creative_charts_filter_ads_type")
    except Exception:
        opened = False
    if not opened:
        opened = await _click_first(
            page,
            [
                '.ant-select-selector:has-text("图片&视频")',
                'div:has-text("图片&视频")',
                'span:has-text("图片&视频")',
                'button:has-text("图片&视频")',
            ],
            timeout=5000,
            debug=debug,
        )
    if not opened:
        return
    await page.wait_for_timeout(500)
    clicked = await _click_first(
        page,
        [
            '.ant-select-dropdown:not(.ant-select-dropdown-hidden) .ant-select-item-option[title="视频"]',
            '.ant-select-dropdown:not(.ant-select-dropdown-hidden) .ant-select-item-option:has-text("视频")',
        ],
        timeout=5000,
        debug=debug,
    )
    if not clicked:
        await _click_text(page, ["视频"], exact=True, timeout=5000, debug=debug)
    _debug_print(debug, "selected material type: 视频")
    await page.wait_for_timeout(1500)


async def _select_category(page: Any, category: AiToolCategory, *, debug: bool = False) -> None:
    ui_label = category_ui_label(category)
    search_selectors = [
        "#rc_select_1",
        'input[aria-controls="rc_select_1_list"]',
        'input[aria-owns="rc_select_1_list"]',
    ]
    search_input = None
    for selector in search_selectors:
        loc = page.locator(selector)
        try:
            if await loc.count() > 0:
                search_input = loc.first
                break
        except Exception:
            continue
    if search_input is None:
        raise RuntimeError("未能找到工具分类快速检索输入框")

    await search_input.click(timeout=5000, force=True)
    await search_input.fill("")
    await search_input.fill(ui_label)
    await page.wait_for_timeout(1000)

    option_selectors = [
        f'.ant-select-dropdown:not(.ant-select-dropdown-hidden) .ant-select-item-option[title="{category.label}"]',
        f'.ant-select-dropdown:not(.ant-select-dropdown-hidden) .ant-select-item-option:has-text("{category.label}")',
        f'.ant-select-dropdown:not(.ant-select-dropdown-hidden) .ant-select-item-option:has-text("{ui_label}")',
    ]
    clicked = await _click_first(page, option_selectors, timeout=8000)
    if not clicked:
        raise RuntimeError(f"未能在广大大快速检索中选择类目: {category.label} ({ui_label})")
    _debug_print(debug, f"selected category: {category.label}")
    await page.wait_for_timeout(2000)


async def _select_categories(page: Any, categories: Iterable[AiToolCategory], *, debug: bool = False) -> None:
    categories = [c for c in categories if isinstance(c, AiToolCategory)]
    if not categories:
        raise RuntimeError("未提供任何工具类目")

    if len(categories) == 1:
        await _select_category(page, categories[0], debug=debug)
        return

    search_selectors = [
        "#rc_select_1",
        'input[aria-controls="rc_select_1_list"]',
        'input[aria-owns="rc_select_1_list"]',
    ]
    search_input = None
    for selector in search_selectors:
        loc = page.locator(selector)
        try:
            if await loc.count() > 0:
                search_input = loc.first
                break
        except Exception:
            continue
    if search_input is None:
        raise RuntimeError("未能找到工具分类快速检索输入框")

    for idx, category in enumerate(categories):
        ui_label = category_ui_label(category)
        await search_input.click(timeout=5000, force=True)
        if idx > 0:
            await search_input.click(timeout=5000, force=True)
        try:
            await search_input.fill("")
        except Exception:
            pass
        try:
            await search_input.press("Control+A")
            await search_input.press("Backspace")
            await search_input.type(ui_label, delay=20)
        except Exception:
            await search_input.fill(ui_label)

        await page.wait_for_timeout(500)
        option_selectors = [
            '.ant-select-dropdown:not(.ant-select-dropdown-hidden) .ant-select-item-option[title="{{value}}"]'.replace(
                "{{value}}", category.label
            ),
            '.ant-select-dropdown:not(.ant-select-dropdown-hidden) .ant-select-item-option:has-text("'
            + category.label
            + '")',
            '.ant-select-dropdown:not(.ant-select-dropdown-hidden) .ant-select-item-option:has-text("'
            + ui_label
            + '")',
        ]
        clicked = await _click_first(page, option_selectors, timeout=8000)
        if not clicked:
            raise RuntimeError(f"未能在广大大快速检索中选择类目: {category.label} ({ui_label})")
        await page.wait_for_timeout(600)

        _debug_print(debug, f"selected multi category[{idx}]: {category.label}")

    await page.keyboard.press("Escape")
    await page.wait_for_timeout(300)


async def _click_search_if_present(page: Any, *, debug: bool = False) -> None:
    await _click_first(
        page,
        [
            'button:has-text("搜索")',
            'button:has-text("查询")',
            '[role="button"]:has-text("搜索")',
            '[role="button"]:has-text("查询")',
        ],
        timeout=2500,
        debug=debug,
    )
    _debug_print(debug, "search clicked")
    await page.wait_for_timeout(1500)


def _dedupe_rows(rows: Iterable[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        key = _creative_ad_key(row) or pick_video_url(row) or json.dumps(row, ensure_ascii=False, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
        if len(result) >= limit:
            break
    return result


async def _visible_result_count(page: Any) -> int | None:
    text = await _page_text(page)
    match = re.search(r"共找到\s*([\d,]+)\s*个?结果", text)
    if not match:
        return None
    try:
        return int(match.group(1).replace(",", ""))
    except ValueError:
        return None


def _choose_rank_rows(
    batches: list[list[dict[str, Any]]],
    *,
    expected_count: int | None,
    limit: int,
) -> list[dict[str, Any]]:
    target = min(limit, expected_count) if expected_count else limit
    normalized_batches = [[row for row in batch if isinstance(row, dict)] for batch in batches]
    normalized_batches = [batch for batch in normalized_batches if batch]

    for batch in reversed(normalized_batches):
        deduped = _dedupe_rows(batch, limit=limit)
        if len(deduped) == target:
            return deduped[:target]

    if expected_count:
        for batch in reversed(normalized_batches):
            deduped = _dedupe_rows(batch, limit=limit)
            if len(deduped) <= target:
                return deduped[:target]

    rows: list[dict[str, Any]] = []
    for batch in reversed(normalized_batches):
        rows.extend(batch)
    return _dedupe_rows(rows, limit=target)


async def _collect_one_category(
    page: Any,
    category: AiToolCategory,
    *,
    limit: int,
    scroll_rounds: int,
    wait_after_filter_ms: int,
    debug: bool = False,
) -> list[dict[str, Any]]:
    return await _collect_categories(
        page,
        [category],
        limit=limit,
        scroll_rounds=scroll_rounds,
        wait_after_filter_ms=wait_after_filter_ms,
        debug=debug,
    )


async def _collect_categories(
    page: Any,
    categories: list[AiToolCategory],
    *,
    limit: int,
    scroll_rounds: int,
    wait_after_filter_ms: int,
    debug: bool = False,
) -> list[dict[str, Any]]:
    captured_batches: list[list[dict[str, Any]]] = []
    capture_state = {"enabled": False}

    async def on_response(response: Any) -> None:
        if not capture_state["enabled"]:
            return
        if "guangdada.net/napi" not in response.url or response.status != 200:
            return
        try:
            body = await response.json()
        except Exception:
            return
        for creative_list in extract_creative_lists(body):
            if creative_list:
                captured_batches.append(creative_list)
                _debug_print(debug, f"captured batch size={len(creative_list)} total_batches={len(captured_batches)}")
        if len(captured_batches) > 120:
            del captured_batches[:20]

    page.on("response", on_response)
    try:
        await page.goto(NEW_CHARTS_URL, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(2500)
        await _raise_if_interrupted(page, step="打开新创意榜")
        await _select_tool_tab(page, debug=debug)
        await _select_video_only(page, debug=debug)
        captured_batches.clear()
        capture_state["enabled"] = True
        # 同时支持一次性勾选多个类目并抓取下方同一排行榜前N条。  
        # 例如：AI图像+AI视频时，不再按类目分开取，每个类目共用同一次榜单结果。
        await _select_categories(page, categories, debug=debug)
        await _click_search_if_present(page, debug=debug)
        await page.wait_for_timeout(wait_after_filter_ms)
        expected_count = await _visible_result_count(page)
        _debug_print(debug, f"initial expected_count={expected_count}")
        target_count = min(limit, expected_count) if expected_count else limit

        for _ in range(scroll_rounds):
            rows = _choose_rank_rows(captured_batches, expected_count=expected_count, limit=limit)
            if len(rows) >= target_count:
                break
            await page.mouse.wheel(0, 1800)
            await page.wait_for_timeout(1200)
            clicked_more = await _click_text(page, ["加载更多", "查看更多"], exact=True, timeout=800, debug=debug)
            if debug:
                _debug_print(True, f"scroll loop rows={len(rows)} clicked_more={clicked_more}")
        await _raise_if_interrupted(page, step=f"滚动采集 {'/'.join([c.label for c in categories])}")
    finally:
        capture_state["enabled"] = False
        try:
            page.remove_listener("response", on_response)
        except Exception:
            pass

    expected_count = await _visible_result_count(page)
    rows = _choose_rank_rows(captured_batches, expected_count=expected_count, limit=limit)
    _debug_print(debug, f"final choose rows={len(rows)} expected_count={expected_count} limit={limit}")
    return rows


async def collect_new_charts_ai_tools(
    *,
    categories: list[AiToolCategory],
    category_mode: str,
    limit: int,
    auth_mode: str,
    debug: bool,
    headless: bool,
    scroll_rounds: int,
    wait_after_filter_ms: int,
) -> dict[AiToolCategory, list[dict[str, Any]]]:
    """Collect raw Guangdada creative rows for selected AI tool categories."""
    from playwright.async_api import async_playwright

    load_project_env(override=True)
    playwright_proxy = prepare_playwright_proxy_for_crawl()
    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")

    async with async_playwright() as playwright:
        launch_kw: dict[str, Any] = {"headless": headless}
        if debug:
            launch_kw["slow_mo"] = 220
        if playwright_proxy:
            launch_kw["proxy"] = playwright_proxy
        browser = await playwright.chromium.launch(**launch_kw)
        context_kw: dict[str, Any] = {
            "viewport": {"width": 1440, "height": 960},
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        }
        if auth_mode == "auth-state" and AUTH_STATE_PATH.exists():
            context_kw["storage_state"] = str(AUTH_STATE_PATH)
        context = await browser.new_context(**context_kw)
        page = await context.new_page()
        try:
            if auth_mode == "env-login" or not AUTH_STATE_PATH.exists():
                if not email or not password:
                    raise RuntimeError("请在 .env 设置 GUANGDADA_EMAIL/GUANGDADA_USERNAME 和 GUANGDADA_PASSWORD")
                ok = await login(page, email, password)
                if not ok:
                    raise RuntimeError("广大大邮箱登录失败")
                AUTH_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
                await context.storage_state(path=str(AUTH_STATE_PATH))
            elif auth_mode == "manual":
                await page.goto(NEW_CHARTS_URL, wait_until="domcontentloaded", timeout=90000)
                print("请在打开的浏览器中完成登录/验证后回到终端按回车继续。", file=sys.stderr)
                await asyncio.to_thread(sys.stdin.readline)
                await context.storage_state(path=str(AUTH_STATE_PATH))

            per_category: dict[AiToolCategory, list[dict[str, Any]]] = {}
            if category_mode == "combined":
                synthetic_label = "AI工具/" + "+".join(category.label.split("/", 1)[-1] for category in categories)
                synthetic_slug = "ai_tools_combined"
                synthetic_category = AiToolCategory(label=synthetic_label, slug=synthetic_slug)
                rows = await _collect_categories(
                    page,
                    categories,
                    limit=limit,
                    scroll_rounds=scroll_rounds,
                    wait_after_filter_ms=wait_after_filter_ms,
                    debug=debug,
                )
                per_category[synthetic_category] = rows
                print(f"[new-charts] 联合采集 {synthetic_label}，目标最多 {limit} 条视频素材", flush=True)
                print(f"[new-charts] {synthetic_label}: 捕获 {len(rows)} 条原始创意", flush=True)
            else:
                for category in categories:
                    print(f"[new-charts] 采集 {category.label}，目标最多 {limit} 条视频素材", flush=True)
                    per_category[category] = await _collect_one_category(
                        page,
                        category,
                        limit=limit,
                        scroll_rounds=scroll_rounds,
                        wait_after_filter_ms=wait_after_filter_ms,
                        debug=debug,
                    )
                    print(f"[new-charts] {category.label}: 捕获 {len(per_category[category])} 条原始创意", flush=True)
            return per_category
        finally:
            await context.close()
            await browser.close()


def write_payload_files(payload: dict[str, Any], *, output_prefix: str | None = None) -> tuple[Path, Path]:
    target_date = str(payload.get("target_date") or _today_utc8())
    prefix = output_prefix or f"guangdada_new_charts_ai_tools_{target_date}"
    raw_path = DATA_DIR / f"{prefix}_raw.json"
    completeness_path = DATA_DIR / f"{prefix}_completeness.json"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    completeness_path.write_text(
        json.dumps(payload.get("completeness_report") or build_completeness_report(payload), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return raw_path, completeness_path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="采集广大大新创意榜 AI 工具视频素材并落 VE raw 兼容 JSON")
    parser.add_argument("--date", default=_today_utc8(), help="写入 raw JSON 的 target_date，默认今天（UTC+8）")
    parser.add_argument("--limit", type=int, default=100, help="合并模式为榜单总上限，分开模式为每类上限")
    parser.add_argument(
        "--category-mode",
        choices=["combined", "separate"],
        default="combined",
        help="combined: 两类一起选同一榜单一次采集；separate: 维持逐类分别采集",
    )
    parser.add_argument(
        "--category",
        action="append",
        dest="categories",
        default=None,
        help="AI 工具类目别名，可重复传；默认 AI图像生成 + AI视频",
    )
    parser.add_argument(
        "--auth-mode",
        choices=["env-login", "auth-state", "manual"],
        default="env-login",
        help="登录方式：env-login 使用 .env 账号密码，auth-state 复用上次保存态，manual 人工登录",
    )
    parser.add_argument("--headed", action="store_true", help="打开有头浏览器便于人工观察")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="输出详细操作日志；自动启用慢速模式便于确认步骤",
    )
    parser.add_argument("--scroll-rounds", type=int, default=32, help="每个类目的滚动加载轮数")
    parser.add_argument("--wait-after-filter-ms", type=int, default=3000, help="筛选后等待接口返回的毫秒数")
    parser.add_argument("--output-prefix", default=None, help="自定义输出文件名前缀（不含 _raw.json）")
    return parser.parse_args(argv)


async def run_async(args: argparse.Namespace) -> tuple[Path, Path, dict[str, Any]]:
    category_names = args.categories or ["AI图像生成", "AI视频"]
    categories = [resolve_ai_tool_category(name) for name in category_names]
    per_category_raw = await collect_new_charts_ai_tools(
        categories=categories,
        category_mode=args.category_mode,
        limit=max(1, int(args.limit)),
        auth_mode=args.auth_mode,
        debug=args.debug,
        headless=not (args.headed or args.debug),
        scroll_rounds=max(1, int(args.scroll_rounds)),
        wait_after_filter_ms=max(500, int(args.wait_after_filter_ms)),
    )
    payload = build_ve_raw_payload(target_date=args.date, per_category_raw=per_category_raw)
    raw_path, completeness_path = write_payload_files(payload, output_prefix=args.output_prefix)
    return raw_path, completeness_path, payload


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        raw_path, completeness_path, payload = asyncio.run(run_async(args))
    except Exception as exc:
        print(f"[new-charts] 失败: {exc}", file=sys.stderr)
        return 1

    print(f"[new-charts] raw: {raw_path}")
    print(f"[new-charts] completeness: {completeness_path}")
    print(f"[new-charts] total video items: {payload.get('total')}")
    missing = (payload.get("completeness_report") or {}).get("missing_counts") or {}
    if missing:
        summary = ", ".join(f"{field}={count}" for field, count in missing.items() if count)
        print(f"[new-charts] missing fields: {summary or 'none'}")
    if args.debug:
        print("[new-charts][debug] 已完成采集并落本地，按回车后退出（核对本地 JSON）")
        try:
            input()
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
