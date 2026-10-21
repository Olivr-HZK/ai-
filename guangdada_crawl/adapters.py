"""Adapters from the installable CLI to the existing Guangdada crawl core."""

from __future__ import annotations

import argparse
import asyncio
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from guangdada_crawl.jobs import CrawlJob, JobStore
from guangdada_crawl.media import disabled_video_artifacts, materialize_video_artifacts


def _utc8_ymd_from_unix(ts: object) -> str | None:
    try:
        value = int(ts)  # type: ignore[arg-type]
    except Exception:
        return None
    return datetime.fromtimestamp(value, tz=timezone(timedelta(hours=8))).date().isoformat()


def _extract_ad_key(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        parsed = urlparse(raw)
        query = parse_qs(parsed.query)
        return (query.get("id") or query.get("ad_key") or [""])[0].strip()
    return raw


def _query_value(url: str, key: str) -> str:
    raw = (url or "").strip()
    if not raw.startswith(("http://", "https://")):
        return ""
    values = parse_qs(urlparse(raw).query).get(key) or []
    return str(values[0]).strip() if values else ""


def _int_or_none(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)  # type: ignore[arg-type]
    except Exception:
        return None


def _normalize_time_window(value: object) -> tuple[str, str]:
    raw = str(value or "7d").strip().lower().replace(" ", "")
    aliases = {
        "": ("7d", "7"),
        "7": ("7d", "7"),
        "7d": ("7d", "7"),
        "7天": ("7d", "7"),
        "30": ("30d", "30"),
        "30d": ("30d", "30"),
        "30天": ("30d", "30"),
        "90": ("90d", "90"),
        "90d": ("90d", "90"),
        "90天": ("90d", "90"),
        "365": ("1y", "365"),
        "365d": ("1y", "365"),
        "365天": ("1y", "365"),
        "1y": ("1y", "365"),
        "1年": ("1y", "365"),
        "近1年": ("1y", "365"),
        "一年": ("1y", "365"),
        "近一年": ("1y", "365"),
    }
    if raw not in aliases:
        raise ValueError("time_window must be one of 7d, 30d, 90d, or 1y")
    return aliases[raw]


def _date_range_includes(date_range_text: object, target_date: str) -> bool | None:
    dates = re.findall(r"\d{4}-\d{2}-\d{2}", str(date_range_text or ""))
    if len(dates) >= 2:
        return dates[0] <= target_date <= dates[-1]
    if len(dates) == 1:
        return dates[0] == target_date
    return None


def _creative_active_on_date(creative: dict[str, Any], target_date: str) -> bool:
    range_hit = _date_range_includes(creative.get("date_range_text"), target_date)
    if range_hit is not None:
        return range_hit
    first_seen = _utc8_ymd_from_unix(creative.get("first_seen"))
    last_seen = _utc8_ymd_from_unix(creative.get("last_seen")) or first_seen
    if first_seen and last_seen:
        return first_seen <= target_date <= last_seen
    return first_seen == target_date if first_seen else False


def _matches_target_advertiser(creative: dict[str, Any], *, keyword: str, label: str) -> bool:
    advertiser_id = str(creative.get("advertiser_id") or "").strip()
    if advertiser_id and keyword and advertiser_id == keyword:
        return True
    advertiser_name = str(creative.get("advertiser_name") or creative.get("page_name") or "").strip()
    target_label = str(label or keyword).strip()
    if advertiser_name and target_label:
        from ua_workflows.shared.guangdada.competitor_utils import advertiser_matches_product

        return advertiser_matches_product(advertiser_name, target_label)
    if advertiser_id and keyword and "." in keyword:
        return False
    return True


def _cover_dedupe_enabled(params: dict[str, Any]) -> bool:
    if "cover_dedupe" not in params:
        raise ValueError("cover_dedupe must be explicitly set to true or false before chart/advertiser crawl")
    return bool(params.get("cover_dedupe"))


def _skipped_cover_dedupe_report(reason: str, item_count: int, target_date: str) -> dict[str, Any]:
    return {
        "skipped": True,
        "reason": reason,
        "target_date": target_date or None,
        "input_count": int(item_count),
        "output_count": int(item_count),
        "removed_total": 0,
    }


def _apply_cover_dedupe_to_payload(
    payload: dict[str, Any],
    *,
    target_date: str,
    crawl_date: Any = None,
    enabled: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    items = [item for item in (payload.get("items") or []) if isinstance(item, dict)]
    input_count = len(items)
    if not enabled:
        report = _skipped_cover_dedupe_report("disabled_by_param", input_count, target_date)
        payload["cover_style_intraday_report"] = report
        payload["total_before_cover_dedupe"] = input_count
        payload["total"] = input_count
        return payload, report
    if not target_date:
        report = _skipped_cover_dedupe_report("missing_target_date", input_count, target_date)
        payload["cover_style_intraday_report"] = report
        payload["total_before_cover_dedupe"] = input_count
        payload["total"] = input_count
        return payload, report

    try:
        from ua_workflows.video_enhancer.cover_dedupe import apply_intraday_cover_style_dedupe
    except Exception as exc:
        report = _skipped_cover_dedupe_report("clip_cover_dedupe_unavailable", input_count, target_date)
        report["error"] = str(exc)
        payload["cover_style_intraday_report"] = report
        payload["total_before_cover_dedupe"] = input_count
        payload["total"] = input_count
        return payload, report

    deduped_items, report = apply_intraday_cover_style_dedupe(items, target_date, crawl_date)
    payload["items"] = deduped_items
    payload["cover_style_intraday_report"] = report
    payload["total_before_cover_dedupe"] = input_count
    payload["total"] = len(deduped_items)
    return payload, report


async def fetch_detail_v2_payload(
    *,
    ad_key: str,
    search_flag: int,
    app_type: int,
    auth_state_path: str | Path | None = None,
    headed: bool = False,
) -> dict[str, Any] | None:
    from playwright.async_api import async_playwright

    from ua_workflows.shared.guangdada.proxy import prepare_playwright_proxy_for_crawl

    playwright_proxy = prepare_playwright_proxy_for_crawl()
    async with async_playwright() as playwright:
        launch_kw: dict[str, Any] = {"headless": not headed}
        if playwright_proxy:
            launch_kw["proxy"] = playwright_proxy
        browser = await playwright.chromium.launch(**launch_kw)
        context_kw: dict[str, Any] = {
            "viewport": {"width": 1440, "height": 960},
            "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        }
        state_path = Path(auth_state_path).expanduser() if auth_state_path else None
        if state_path and state_path.exists():
            context_kw["storage_state"] = str(state_path)
        context = await browser.new_context(**context_kw)
        page = await context.new_page()
        try:
            await page.goto("https://www.guangdada.net/", wait_until="domcontentloaded", timeout=90000)
            url = f"/napi/v1/creative/detail-v2?ad_key={ad_key}&app_type={int(app_type)}&search_flag={int(search_flag)}"
            body = await page.evaluate(
                """
async ({ url }) => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 10000);
  const resp = await fetch(url, {
    method: 'GET',
    credentials: 'include',
    signal: controller.signal,
    headers: { 'accept': 'application/json, text/plain, */*' }
  });
  clearTimeout(timer);
  let data = null;
  try {
    data = await resp.json();
  } catch (e) {
    data = null;
  }
  return { ok: resp.ok, status: resp.status, url: resp.url, body: data };
}
""",
                {"url": url},
            )
            return body if isinstance(body, dict) else None
        finally:
            await context.close()
            await browser.close()


async def run_chart_crawl(job: CrawlJob, store: JobStore, params: dict[str, Any]) -> dict[str, object]:
    from ua_workflows.shared.guangdada import new_charts_ai_tools as chart_mod

    output_prefix = str(job.path / "chart")
    auth_state_path = params.get("auth_state_path")
    if auth_state_path:
        chart_mod.AUTH_STATE_PATH = Path(str(auth_state_path)).expanduser()
    args = argparse.Namespace(
        date=params["date"],
        chart_type=params["chart"],
        limit=int(params["limit"]),
        category_mode=params["category_mode"],
        categories=list(params["categories"]),
        auth_mode=params["auth_mode"],
        headed=bool(params["headed"]),
        debug=bool(params["debug"]),
        scroll_rounds=int(params["scroll_rounds"]),
        wait_after_filter_ms=int(params["wait_after_filter_ms"]),
        output_prefix=output_prefix,
    )
    _raw_path, completeness_path, payload = await chart_mod.run_async(args)
    target_date = str(params.get("date") or payload.get("target_date") or "")
    payload, cover_report = _apply_cover_dedupe_to_payload(
        payload,
        target_date=target_date,
        crawl_date=params.get("date") or payload.get("crawl_date") or target_date or None,
        enabled=_cover_dedupe_enabled(params),
    )
    media_summary = (
        materialize_video_artifacts(job, payload)
        if bool(params.get("download_videos", True))
        else disabled_video_artifacts(job)
    )
    store.write_raw(job.job_id, payload)
    completeness_target = job.path / "completeness.json"
    if Path(completeness_path).exists():
        completeness_target.write_text(Path(completeness_path).read_text(encoding="utf-8"), encoding="utf-8")
    summary = {
        "status": "completed",
        "kind": "chart",
        "total": payload.get("total", 0),
        "total_before_cover_dedupe": payload.get("total_before_cover_dedupe", payload.get("total", 0)),
        "cover_dedupe_removed_total": int(cover_report.get("removed_total") or 0),
        "cover_dedupe_skipped": bool(cover_report.get("skipped")),
        "chart": params["chart"],
        "categories": params["categories"],
        "raw_path": str(job.path / "raw.json"),
        "completeness_path": str(completeness_target),
        **media_summary,
    }
    store.write_summary(job.job_id, summary)
    return summary


async def run_advertiser_crawl(job: CrawlJob, store: JobStore, params: dict[str, Any]) -> dict[str, object]:
    from ua_workflows.shared.guangdada.search import run_batch

    keyword = str(params["keyword"])
    date = str(params.get("date") or "").strip()
    label = str(params.get("label") or keyword)
    date_mode = str(params.get("date_mode") or "first-seen").strip() or "first-seen"
    if date_mode not in {"active", "first-seen"}:
        raise ValueError("date_mode must be 'active' or 'first-seen'")
    time_window, day_span = _normalize_time_window(params.get("time_window"))
    results = await run_batch(
        keywords=[keyword],
        debug=bool(params.get("headed")),
        is_tool=bool(params.get("is_tool", False)),
        order_by=str(params.get("order_by") or "latest"),
        use_popularity_top1=False,
        detail_click_primary=bool(params.get("detail_click_primary", True)),
        first_seen_target_ymd=date if date and date_mode == "first-seen" else None,
        day_span=day_span,
        # A target date must drive Guangdada's custom date selector at the
        # source. Keep the later date-mode filtering as a defense-in-depth
        # check for incomplete or inconsistent detail metadata.
        date_range=date or None,
        keyword_labels={keyword: label},
        max_scroll_rounds=int(params.get("max_scroll_rounds") or 48),
        extract_detail_dom_analysis=bool(params.get("extract_detail_dom_analysis", True)),
    )
    rows: list[dict[str, Any]] = []
    for result in results:
        for creative in result.get("all_creatives") or []:
            if not isinstance(creative, dict):
                continue
            if date:
                if date_mode == "first-seen" and _utc8_ymd_from_unix(creative.get("first_seen")) != date:
                    continue
                if date_mode == "active" and not _creative_active_on_date(creative, date):
                    continue
            if not _matches_target_advertiser(creative, keyword=keyword, label=label):
                continue
            rows.append(
                {
                    "category": "advertiser",
                    "product": label,
                    "appid": keyword,
                    "keyword": keyword,
                    "creative": creative,
                }
            )
    payload = {
        "source": "guangdada_crawl_advertiser",
        "target_date": date or None,
        "date_mode": date_mode,
        "ui_date_range": date or None,
        "time_window": time_window,
        "total": len(rows),
        "competitors": [{"product": label, "appid": keyword}],
        "items": rows,
        "raw_results": results,
    }
    payload, cover_report = _apply_cover_dedupe_to_payload(
        payload,
        target_date=date,
        crawl_date=date or None,
        enabled=_cover_dedupe_enabled(params),
    )
    media_summary = (
        materialize_video_artifacts(job, payload)
        if bool(params.get("download_videos", True))
        else disabled_video_artifacts(job)
    )
    store.write_raw(job.job_id, payload)
    summary = {
        "status": "completed",
        "kind": "advertiser",
        "keyword": keyword,
        "label": label,
        "date": date or None,
        "date_mode": date_mode,
        "ui_date_range": date or None,
        "time_window": time_window,
        "total": payload.get("total", len(rows)),
        "total_before_cover_dedupe": payload.get("total_before_cover_dedupe", len(rows)),
        "cover_dedupe_removed_total": int(cover_report.get("removed_total") or 0),
        "cover_dedupe_skipped": bool(cover_report.get("skipped")),
        "raw_path": str(job.path / "raw.json"),
        **media_summary,
    }
    store.write_summary(job.job_id, summary)
    return summary


async def run_ad_crawl(job: CrawlJob, store: JobStore, params: dict[str, Any]) -> dict[str, object]:
    from ua_workflows.shared.guangdada.detail_url import try_build_url_spa

    raw_url = str(params.get("url") or "")
    ad_key = _extract_ad_key(str(params.get("ad_key") or raw_url or ""))
    if not ad_key:
        raise ValueError("ad crawl requires --ad-key or --url containing id/ad_key")
    creative_type = _int_or_none(params.get("type")) or _int_or_none(_query_value(raw_url, "type")) or 2
    search_flag = _int_or_none(params.get("search_flag")) or _int_or_none(_query_value(raw_url, "search_flag"))
    app_type = _int_or_none(params.get("app_type")) or creative_type
    created_at = _int_or_none(params.get("created_at")) or _int_or_none(_query_value(raw_url, "created_at"))
    creative: dict[str, Any] = {"ad_key": ad_key}
    if search_flag is not None:
        creative["search_flag"] = search_flag
    if created_at is not None:
        creative["created_at"] = created_at
    detail_url = try_build_url_spa(creative, creative_type=creative_type)
    detail_v2: dict[str, Any] | None = None
    detail_v2_status = "skipped_missing_search_flag"
    if search_flag is not None and bool(params.get("fetch_detail", True)):
        detail_v2 = await fetch_detail_v2_payload(
            ad_key=ad_key,
            search_flag=search_flag,
            app_type=app_type,
            auth_state_path=params.get("auth_state_path"),
            headed=bool(params.get("headed")),
        )
        detail_v2_status = "fetched" if isinstance(detail_v2, dict) and detail_v2.get("ok") else "fetch_failed"
    elif search_flag is not None:
        detail_v2_status = "skipped_by_flag"
    item: dict[str, Any] = {
        "ad_key": ad_key,
        "detail_url": detail_url,
        "type": creative_type,
        "app_type": app_type,
        "search_flag": search_flag,
        "detail_v2_status": detail_v2_status,
    }
    if detail_v2 is not None:
        item["detail_v2"] = detail_v2
    payload = {
        "source": "guangdada_crawl_ad",
        "total": 1,
        "items": [item],
    }
    media_summary = (
        materialize_video_artifacts(job, payload)
        if bool(params.get("download_videos", True))
        else disabled_video_artifacts(job)
    )
    store.write_raw(job.job_id, payload)
    summary = {
        "status": "completed",
        "kind": "ad",
        "ad_key": ad_key,
        "detail_url": detail_url,
        "app_type": app_type,
        "search_flag": search_flag,
        "detail_v2_status": detail_v2_status,
        "raw_path": str(job.path / "raw.json"),
        **media_summary,
    }
    store.write_summary(job.job_id, summary)
    return summary


def run_async(coro: object) -> object:
    return asyncio.run(coro)  # pragma: no cover
