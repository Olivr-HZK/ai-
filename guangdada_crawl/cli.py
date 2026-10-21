"""Command line interface for the Guangdada crawler package."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from guangdada_crawl.adapters import run_ad_crawl, run_advertiser_crawl, run_chart_crawl
from guangdada_crawl.auth import AuthStore, default_home_dir
from guangdada_crawl.doctor import build_doctor_report
from guangdada_crawl.examples import build_examples
from guangdada_crawl.jobs import JobStore, default_jobs_dir


def _print_json(payload: dict[str, object]) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _base_response(store: JobStore, job_id: str, status: str) -> dict[str, object]:
    manifest = store.read_manifest(job_id)
    response: dict[str, object] = {
        "job_id": job_id,
        "status": status,
        "job_dir": str(Path(str(manifest["paths"]["job_dir"]))),
        "manifest_path": str(manifest["paths"]["manifest"]),
        "summary_path": str(manifest["paths"]["summary"]),
        "raw_path": str(manifest["paths"]["raw"]),
        "log_path": str(manifest["paths"]["log"]),
    }
    summary = store.read_summary(job_id)
    for key in (
        "media_manifest_path",
        "media_zip_path",
        "downloaded_video_count",
        "video_download_failed_count",
        "skipped_without_video_count",
    ):
        if key in summary:
            response[key] = summary[key]
    return response


def _finish_dry_run(store: JobStore, job_id: str, kind: str, params: dict[str, object]) -> dict[str, object]:
    store.write_raw(job_id, {"source": "guangdada_crawl_dry_run", "kind": kind, "items": [], "params": params})
    store.write_summary(job_id, {"status": "dry_run", "kind": kind, "total": 0, "params": params})
    store.finish_job(job_id, status="dry_run")
    return _base_response(store, job_id, "dry_run")


async def _run_chart(args: argparse.Namespace, store: JobStore) -> dict[str, object]:
    auth_store = AuthStore.from_value(args.home)
    params = {
        "date": args.date,
        "chart": args.chart,
        "categories": args.categories or ["AI图像生成", "AI视频"],
        "limit": args.limit,
        "category_mode": args.category_mode,
        "auth_mode": args.auth_mode,
        "headed": args.headed,
        "debug": args.debug,
        "scroll_rounds": args.scroll_rounds,
        "wait_after_filter_ms": args.wait_after_filter_ms,
        "auth_state_path": str(auth_store.auth_state_path),
        "cover_dedupe": bool(args.cover_dedupe),
        "download_videos": not args.no_download_videos,
    }
    job = store.create_job(kind="chart", params=params, dry_run=args.dry_run)
    if args.dry_run:
        return _finish_dry_run(store, job.job_id, "chart", params)
    store.finish_job(job.job_id, status="running")
    await run_chart_crawl(job, store, params)
    store.finish_job(job.job_id, status="completed")
    return _base_response(store, job.job_id, "completed")


async def _run_advertiser(args: argparse.Namespace, store: JobStore) -> dict[str, object]:
    params = {
        "keyword": args.keyword,
        "label": args.label or args.keyword,
        "date": args.date,
        "date_mode": args.date_mode,
        "time_window": args.time_window,
        "order_by": args.order_by,
        "headed": args.headed,
        "is_tool": args.is_tool,
        "detail_click_primary": not args.napi_list,
        "extract_detail_dom_analysis": not args.no_detail_analysis,
        "max_scroll_rounds": args.max_scroll_rounds,
        "cover_dedupe": bool(args.cover_dedupe),
        "download_videos": not args.no_download_videos,
    }
    job = store.create_job(kind="advertiser", params=params, dry_run=args.dry_run)
    if args.dry_run:
        return _finish_dry_run(store, job.job_id, "advertiser", params)
    store.finish_job(job.job_id, status="running")
    await run_advertiser_crawl(job, store, params)
    store.finish_job(job.job_id, status="completed")
    return _base_response(store, job.job_id, "completed")


async def _run_ad(args: argparse.Namespace, store: JobStore) -> dict[str, object]:
    auth_store = AuthStore.from_value(args.home)
    params = {
        "ad_key": args.ad_key,
        "url": args.url,
        "type": args.type,
        "app_type": args.app_type,
        "search_flag": args.search_flag,
        "created_at": args.created_at,
        "fetch_detail": not args.no_fetch_detail,
        "auth_state_path": str(auth_store.auth_state_path),
        "headed": args.headed,
        "download_videos": not args.no_download_videos,
    }
    job = store.create_job(kind="ad", params=params, dry_run=args.dry_run)
    if args.dry_run:
        return _finish_dry_run(store, job.job_id, "ad", params)
    store.finish_job(job.job_id, status="running")
    await run_ad_crawl(job, store, params)
    store.finish_job(job.job_id, status="completed")
    return _base_response(store, job.job_id, "completed")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="guangdada", description="Lightweight Guangdada crawl CLI")
    parser.add_argument("--json", action="store_true", help="Emit JSON output; currently all commands emit JSON")
    parser.add_argument("--home", default=str(default_home_dir()), help="Home directory for auth state and default jobs")
    parser.add_argument("--jobs-dir", default=None, help="Directory for crawl job artifacts")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("doctor", help="Check crawler setup, auth, and runtime dependencies")
    sub.add_parser("examples", help="Print machine-readable usage examples and safety boundaries")

    auth = sub.add_parser("auth", help="Manage Guangdada auth state")
    auth_sub = auth.add_subparsers(dest="auth_command", required=True)
    auth_sub.add_parser("status", help="Show auth state and credential availability")
    login = auth_sub.add_parser("login", help="Create a reusable Guangdada browser auth state")
    login.add_argument("--auth-mode", choices=["env-login", "manual"], default="env-login")
    login.add_argument("--headed", action="store_true")
    login.add_argument("--dry-run", action="store_true")

    crawl = sub.add_parser("crawl", help="Start crawl jobs")
    crawl_sub = crawl.add_subparsers(dest="crawl_kind", required=True)

    chart = crawl_sub.add_parser("chart", help="Crawl Guangdada creative charts")
    chart.add_argument("--date", default="", help="Target date label for output, YYYY-MM-DD")
    chart.add_argument("--chart", choices=["new", "hot", "surge"], default="new")
    chart.add_argument("--category", action="append", dest="categories", default=None)
    chart.add_argument("--limit", type=int, default=100)
    chart.add_argument("--category-mode", choices=["combined", "separate"], default="combined")
    chart.add_argument("--auth-mode", choices=["env-login", "auth-state", "manual"], default="env-login")
    chart.add_argument("--headed", action="store_true")
    chart.add_argument("--debug", action="store_true")
    chart.add_argument("--scroll-rounds", type=int, default=32)
    chart.add_argument("--wait-after-filter-ms", type=int, default=3000)
    chart_cover = chart.add_mutually_exclusive_group(required=True)
    chart_cover.add_argument("--cover-dedupe", action="store_true", help="Run VE-style CLIP cover dedupe on crawled items")
    chart_cover.add_argument("--no-cover-dedupe", action="store_true", help="Skip VE-style CLIP cover dedupe on crawled items")
    chart.add_argument("--no-download-videos", action="store_true", help="Do not download video files or create videos.zip")
    chart.add_argument("--dry-run", action="store_true")

    advertiser = crawl_sub.add_parser("advertiser", help="Crawl creatives for one advertiser/app keyword")
    advertiser.add_argument("--keyword", required=True, help="Guangdada search keyword, usually appid/package name")
    advertiser.add_argument("--label", default="", help="Human-readable advertiser/product label")
    advertiser.add_argument("--date", default="", help="Target UTC+8 date")
    advertiser.add_argument(
        "--date-mode",
        choices=["active", "first-seen"],
        default="first-seen",
        help="active=ads active on --date; first-seen=ads first seen on --date",
    )
    advertiser.add_argument(
        "--time-window",
        choices=["7d", "30d", "90d", "1y"],
        default="7d",
        help="Guangdada shortcut time button to click before result filtering",
    )
    advertiser.add_argument("--order-by", choices=["latest", "exposure"], default="latest")
    advertiser.add_argument("--headed", action="store_true")
    advertiser.add_argument("--is-tool", action="store_true", help="Switch from the default game tab to Guangdada's tool tab")
    advertiser.add_argument("--napi-list", action="store_true", help="Use old NAPI list path instead of detail clicking")
    advertiser.add_argument("--no-detail-analysis", action="store_true")
    advertiser.add_argument("--max-scroll-rounds", type=int, default=48)
    advertiser_cover = advertiser.add_mutually_exclusive_group(required=True)
    advertiser_cover.add_argument("--cover-dedupe", action="store_true", help="Run VE-style CLIP cover dedupe on crawled items")
    advertiser_cover.add_argument("--no-cover-dedupe", action="store_true", help="Skip VE-style CLIP cover dedupe on crawled items")
    advertiser.add_argument("--no-download-videos", action="store_true", help="Do not download video files or create videos.zip")
    advertiser.add_argument("--dry-run", action="store_true")

    ad = crawl_sub.add_parser("ad", help="Create/fetch a single Guangdada ad detail seed")
    ad.add_argument("--ad-key", default="")
    ad.add_argument("--url", default="")
    ad.add_argument("--type", type=int, default=2, help="Guangdada SPA detail type, 2 for VE-style")
    ad.add_argument("--app-type", type=int, default=None, help="detail-v2 app_type; defaults to --type")
    ad.add_argument("--search-flag", type=int, default=None, help="detail-v2 search_flag; parsed from --url when present")
    ad.add_argument("--created-at", type=int, default=None, help="Optional created_at for the detail URL")
    ad.add_argument("--no-fetch-detail", action="store_true", help="Only write the detail URL seed")
    ad.add_argument("--no-download-videos", action="store_true", help="Do not download video files or create videos.zip")
    ad.add_argument("--headed", action="store_true")
    ad.add_argument("--dry-run", action="store_true")

    job = sub.add_parser("job", help="Inspect crawl jobs")
    job_sub = job.add_subparsers(dest="job_command", required=True)
    show = job_sub.add_parser("show", help="Show a job manifest and summary")
    show.add_argument("job_id")
    list_jobs = job_sub.add_parser("list", help="List recent crawl jobs")
    list_jobs.add_argument("--limit", type=int, default=20)
    artifacts = job_sub.add_parser("artifacts", help="Show artifact paths for a job")
    artifacts.add_argument("job_id")
    return parser


async def _run(args: argparse.Namespace) -> dict[str, object]:
    home = Path(args.home).expanduser()
    jobs_dir = Path(args.jobs_dir).expanduser() if args.jobs_dir else home / "jobs"
    store = JobStore(jobs_dir)
    auth_store = AuthStore.from_value(home)
    if args.command == "doctor":
        return build_doctor_report(home=home, jobs_dir=jobs_dir)
    if args.command == "examples":
        return build_examples()
    if args.command == "auth" and args.auth_command == "status":
        return auth_store.status()
    if args.command == "auth" and args.auth_command == "login":
        if args.dry_run:
            payload = auth_store.status()
            payload["status"] = "dry_run"
            payload["auth_mode"] = args.auth_mode
            payload["headed"] = bool(args.headed)
            return payload
        return await auth_store.login(headed=bool(args.headed), auth_mode=str(args.auth_mode))
    if args.command == "crawl" and args.crawl_kind == "chart":
        return await _run_chart(args, store)
    if args.command == "crawl" and args.crawl_kind == "advertiser":
        return await _run_advertiser(args, store)
    if args.command == "crawl" and args.crawl_kind == "ad":
        return await _run_ad(args, store)
    if args.command == "job" and args.job_command == "show":
        return {
            "job_id": args.job_id,
            "manifest": store.read_manifest(args.job_id),
            "summary": store.read_summary(args.job_id),
        }
    if args.command == "job" and args.job_command == "list":
        jobs = store.list_jobs(limit=int(args.limit))
        return {"total": len(jobs), "jobs": jobs}
    if args.command == "job" and args.job_command == "artifacts":
        return {"job_id": args.job_id, "artifacts": store.artifacts(args.job_id)}
    raise SystemExit("Unsupported command")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        payload = asyncio.run(_run(args))
    except Exception as exc:
        _print_json({"status": "failed", "error": str(exc)})
        return 1
    _print_json(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
