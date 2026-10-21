"""Thin MCP-facing helpers that delegate to the Guangdada CLI."""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Callable, Sequence
from typing import Any


Runner = Callable[..., subprocess.CompletedProcess[str]]


def _decode_json_object_from_stdout(stdout: str) -> dict[str, Any]:
    text = stdout or "{}"
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            decoded, end = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if not isinstance(decoded, dict):
            continue
        if text[index + end :].strip():
            continue
        return decoded
    raise json.JSONDecodeError("No trailing JSON object found", text, 0)


def _append_optional(args: list[str], flag: str, value: object | None) -> None:
    if value is None or value == "":
        return
    args.extend([flag, str(value)])


def _append_bool(args: list[str], flag: str, value: bool | None) -> None:
    if value:
        args.append(flag)


def _append_cover_dedupe_choice(args: list[str], value: object | None) -> None:
    if value is True:
        args.append("--cover-dedupe")
    elif value is False:
        args.append("--no-cover-dedupe")


def _append_download_videos_choice(args: list[str], value: object | None) -> None:
    if value is False:
        args.append("--no-download-videos")


def _as_list(value: object | None) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence):
        return [str(item) for item in value if str(item)]
    return [str(value)]


def build_cli_args(action: str, params: dict[str, Any] | None = None) -> list[str]:
    params = params or {}
    if action == "doctor":
        return ["doctor"]

    if action == "examples":
        return ["examples"]

    if action == "auth_status":
        return ["auth", "status"]

    if action == "start_chart_crawl":
        args = ["crawl", "chart", "--chart", str(params.get("chart") or "new")]
        for category in _as_list(params.get("categories")):
            args.extend(["--category", category])
        _append_optional(args, "--limit", params.get("limit"))
        _append_optional(args, "--date", params.get("date"))
        _append_optional(args, "--category-mode", params.get("category_mode"))
        _append_optional(args, "--auth-mode", params.get("auth_mode"))
        _append_optional(args, "--scroll-rounds", params.get("scroll_rounds"))
        _append_optional(args, "--wait-after-filter-ms", params.get("wait_after_filter_ms"))
        _append_bool(args, "--headed", bool(params.get("headed")))
        _append_bool(args, "--debug", bool(params.get("debug")))
        _append_cover_dedupe_choice(args, params.get("cover_dedupe"))
        _append_download_videos_choice(args, params.get("download_videos"))
        _append_bool(args, "--dry-run", bool(params.get("dry_run")))
        return args

    if action == "start_advertiser_crawl":
        args = ["crawl", "advertiser", "--keyword", str(params.get("keyword") or "")]
        _append_optional(args, "--label", params.get("label"))
        _append_optional(args, "--date", params.get("date"))
        _append_optional(args, "--date-mode", params.get("date_mode"))
        _append_optional(args, "--time-window", params.get("time_window"))
        _append_optional(args, "--order-by", params.get("order_by"))
        _append_optional(args, "--max-scroll-rounds", params.get("max_scroll_rounds"))
        _append_bool(args, "--headed", bool(params.get("headed")))
        _append_bool(args, "--is-tool", bool(params.get("is_tool")))
        _append_bool(args, "--napi-list", bool(params.get("napi_list")))
        _append_bool(args, "--no-detail-analysis", bool(params.get("no_detail_analysis")))
        _append_cover_dedupe_choice(args, params.get("cover_dedupe"))
        _append_download_videos_choice(args, params.get("download_videos"))
        _append_bool(args, "--dry-run", bool(params.get("dry_run")))
        return args

    if action == "start_ad_crawl":
        args = ["crawl", "ad"]
        _append_optional(args, "--ad-key", params.get("ad_key"))
        _append_optional(args, "--url", params.get("url"))
        _append_optional(args, "--type", params.get("type"))
        _append_optional(args, "--app-type", params.get("app_type"))
        _append_optional(args, "--search-flag", params.get("search_flag"))
        _append_optional(args, "--created-at", params.get("created_at"))
        _append_bool(args, "--no-fetch-detail", bool(params.get("no_fetch_detail")))
        _append_download_videos_choice(args, params.get("download_videos"))
        _append_bool(args, "--headed", bool(params.get("headed")))
        _append_bool(args, "--dry-run", bool(params.get("dry_run")))
        return args

    if action == "get_job_status":
        return ["job", "show", str(params.get("job_id") or "")]

    if action == "get_job_artifacts":
        return ["job", "artifacts", str(params.get("job_id") or "")]

    if action == "list_recent_jobs":
        args = ["job", "list"]
        _append_optional(args, "--limit", params.get("limit"))
        return args

    raise ValueError(f"Unsupported MCP action: {action}")


def call_cli_json(
    cli_args: Sequence[str],
    *,
    home: str | None = None,
    jobs_dir: str | None = None,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    cmd = [sys.executable, "-m", "guangdada_crawl.cli"]
    _append_optional(cmd, "--home", home)
    _append_optional(cmd, "--jobs-dir", jobs_dir)
    cmd.extend(str(arg) for arg in cli_args)

    proc = runner(cmd, text=True, capture_output=True, check=False)
    payload: dict[str, Any]
    try:
        decoded = _decode_json_object_from_stdout(proc.stdout or "{}")
        payload = decoded if isinstance(decoded, dict) else {"status": "failed", "error": "CLI JSON was not an object"}
    except json.JSONDecodeError as exc:
        payload = {"status": "failed", "error": f"CLI returned non-JSON stdout: {exc}", "stdout": proc.stdout}

    if proc.returncode != 0:
        payload.setdefault("status", "failed")
        if payload.get("status") != "failed":
            payload["status"] = "failed"
        payload.setdefault("error", proc.stderr.strip() or proc.stdout.strip() or "CLI command failed")
        payload["returncode"] = proc.returncode
        payload["cmd"] = cmd
    return payload


def auth_status(
    *,
    home: str | None = None,
    jobs_dir: str | None = None,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    return call_cli_json(build_cli_args("auth_status"), home=home, jobs_dir=jobs_dir, runner=runner)


def doctor(
    *,
    home: str | None = None,
    jobs_dir: str | None = None,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    return call_cli_json(build_cli_args("doctor"), home=home, jobs_dir=jobs_dir, runner=runner)


def examples(
    *,
    home: str | None = None,
    jobs_dir: str | None = None,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    return call_cli_json(build_cli_args("examples"), home=home, jobs_dir=jobs_dir, runner=runner)


def start_chart_crawl(
    *,
    chart: str = "new",
    categories: Sequence[str] | str | None = None,
    limit: int = 100,
    date: str = "",
    category_mode: str = "",
    auth_mode: str = "",
    headed: bool = False,
    debug: bool = False,
    scroll_rounds: int | None = None,
    wait_after_filter_ms: int | None = None,
    cover_dedupe: bool | None = None,
    download_videos: bool | None = None,
    dry_run: bool = False,
    home: str | None = None,
    jobs_dir: str | None = None,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    cli_args = build_cli_args(
        "start_chart_crawl",
        {
            "chart": chart,
            "categories": categories,
            "limit": limit,
            "date": date,
            "category_mode": category_mode,
            "auth_mode": auth_mode,
            "headed": headed,
            "debug": debug,
            "scroll_rounds": scroll_rounds,
            "wait_after_filter_ms": wait_after_filter_ms,
            "cover_dedupe": cover_dedupe,
            "download_videos": download_videos,
            "dry_run": dry_run,
        },
    )
    return call_cli_json(cli_args, home=home, jobs_dir=jobs_dir, runner=runner)


def start_advertiser_crawl(
    *,
    keyword: str,
    label: str = "",
    date: str = "",
    date_mode: str = "first-seen",
    time_window: str = "7d",
    order_by: str = "",
    headed: bool = False,
    is_tool: bool = False,
    napi_list: bool = False,
    no_detail_analysis: bool = False,
    max_scroll_rounds: int | None = None,
    cover_dedupe: bool | None = None,
    download_videos: bool | None = None,
    dry_run: bool = False,
    home: str | None = None,
    jobs_dir: str | None = None,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    cli_args = build_cli_args(
        "start_advertiser_crawl",
        {
            "keyword": keyword,
            "label": label,
            "date": date,
            "date_mode": date_mode,
            "time_window": time_window,
            "order_by": order_by,
            "headed": headed,
            "is_tool": is_tool,
            "napi_list": napi_list,
            "no_detail_analysis": no_detail_analysis,
            "max_scroll_rounds": max_scroll_rounds,
            "cover_dedupe": cover_dedupe,
            "download_videos": download_videos,
            "dry_run": dry_run,
        },
    )
    return call_cli_json(cli_args, home=home, jobs_dir=jobs_dir, runner=runner)


def start_ad_crawl(
    *,
    ad_key: str = "",
    url: str = "",
    type: int = 2,
    app_type: int | None = None,
    search_flag: int | None = None,
    created_at: int | None = None,
    no_fetch_detail: bool = False,
    download_videos: bool | None = None,
    headed: bool = False,
    dry_run: bool = False,
    home: str | None = None,
    jobs_dir: str | None = None,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    cli_args = build_cli_args(
        "start_ad_crawl",
        {
            "ad_key": ad_key,
            "url": url,
            "type": type,
            "app_type": app_type,
            "search_flag": search_flag,
            "created_at": created_at,
            "no_fetch_detail": no_fetch_detail,
            "download_videos": download_videos,
            "headed": headed,
            "dry_run": dry_run,
        },
    )
    return call_cli_json(cli_args, home=home, jobs_dir=jobs_dir, runner=runner)


def get_job_status(
    job_id: str,
    *,
    home: str | None = None,
    jobs_dir: str | None = None,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    return call_cli_json(build_cli_args("get_job_status", {"job_id": job_id}), home=home, jobs_dir=jobs_dir, runner=runner)


def get_job_artifacts(
    job_id: str,
    *,
    home: str | None = None,
    jobs_dir: str | None = None,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    return call_cli_json(build_cli_args("get_job_artifacts", {"job_id": job_id}), home=home, jobs_dir=jobs_dir, runner=runner)


def list_recent_jobs(
    *,
    limit: int = 20,
    home: str | None = None,
    jobs_dir: str | None = None,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    return call_cli_json(build_cli_args("list_recent_jobs", {"limit": limit}), home=home, jobs_dir=jobs_dir, runner=runner)
