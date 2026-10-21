"""Cold-start entrypoint for newly added VE competitors.

The default mode prints a plan only. Real crawling/syncing requires --execute.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable

from ua_workflows.shared.config import DATA_DIR, PROJECT_ROOT, load_project_env
from ua_workflows.shared.db.video_enhancer import (
    init_db as init_pipeline_db,
    upsert_creative_library,
    upsert_daily_creative_insights,
    upsert_daily_video_enhancer_filter_log,
)
from ua_workflows.shared.media.resolve import merge_inspiration_filter_stats
from ua_workflows.video_enhancer.cover_dedupe import apply_intraday_cover_style_dedupe, is_cover_style_intraday_enabled
from ua_workflows.video_enhancer.crawl_similarity import merge_cover_similarity_counts
from ua_workflows.video_enhancer.filter_reports import write_cover_filter_step_json, write_cover_filter_step_json_skipped
from ua_workflows.video_enhancer.pipeline import _build_crawl_product_retention_report

DEFAULT_HISTORY_DAYS = 4


CommandRunner = Callable[[list[str]], None]
ShallowRunner = Callable[[str, str], None]


def _today_shanghai() -> dt.date:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).date()


def _parse_date(value: str | dt.date | None) -> dt.date:
    if isinstance(value, dt.date):
        return value
    if value:
        return dt.date.fromisoformat(str(value))
    return _today_shanghai()


def _python_bin(value: str | None = None) -> str:
    return value or sys.executable


def _date_window(run_date: dt.date, history_days: int) -> list[dt.date]:
    if history_days < 1:
        raise ValueError("history_days must be >= 1")
    start = run_date - dt.timedelta(days=history_days)
    end = run_date - dt.timedelta(days=1)
    return [start + dt.timedelta(days=offset) for offset in range((end - start).days + 1)]


def build_cold_start_plan(
    *,
    products: str,
    run_date: str | dt.date | None = None,
    history_days: int = DEFAULT_HISTORY_DAYS,
    python_bin: str | None = None,
) -> dict[str, Any]:
    product_text = ",".join(part.strip() for part in str(products or "").split(",") if part.strip())
    if not product_text:
        raise ValueError("products is required")
    day = _parse_date(run_date)
    py = _python_bin(python_bin)
    dates = _date_window(day, int(history_days))
    steps: list[dict[str, Any]] = []
    for target_day in dates:
        target = target_day.isoformat()
        if target_day == dates[-1]:
            command = [
                py,
                "scripts/run_video_enhancer.py",
                "--date",
                target,
                "--products",
                product_text,
            ]
            mode = "full"
        else:
            command = [
                py,
                "scripts/run_ve_new_competitor_cold_start.py",
                "--shallow-date",
                target,
                "--products",
                product_text,
            ]
            mode = "shallow"
        steps.append({"date": target, "mode": mode, "command": command})
    return {
        "run_date": day.isoformat(),
        "history_days": int(history_days),
        "products": product_text,
        "steps": steps,
    }


def _subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    return env


def run_command(command: list[str]) -> None:
    print("$ " + " ".join(command))
    subprocess.run(command, check=True, cwd=str(PROJECT_ROOT), env=_subprocess_env())


def _product_from_item(item: dict[str, Any]) -> str:
    creative = item.get("creative") if isinstance(item.get("creative"), dict) else {}
    return str(item.get("product") or creative.get("advertiser_name") or item.get("keyword") or "未知产品").strip() or "未知产品"


def run_shallow_crawl_and_cover(target_date: str, products: str) -> dict[str, Any]:
    """Run crawl + cover dedupe + raw/library storage for one target date."""
    load_project_env()
    output_prefix = f"workflow_video_enhancer_{target_date}"
    raw_path = DATA_DIR / f"{output_prefix}_raw.json"
    crawl_cmd = [
        sys.executable,
        "-m",
        "ua_workflows.video_enhancer.crawl",
        "--target-date",
        target_date,
        "--output-prefix",
        output_prefix,
        "--products",
        products,
    ]
    run_command(crawl_cmd)

    raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
    if is_cover_style_intraday_enabled():
        items = raw_payload.get("items") or []
        items2, cover_report = apply_intraday_cover_style_dedupe(items, target_date, raw_payload.get("crawl_date"))
        raw_payload["items"] = items2
        raw_payload["cover_style_intraday_report"] = cover_report
        cover_report_path = DATA_DIR / f"{output_prefix}_cover_style_intraday.json"
        cover_report_path.write_text(json.dumps(cover_report, ensure_ascii=False, indent=2), encoding="utf-8")
        write_cover_filter_step_json(DATA_DIR, output_prefix, target_date, cover_report)
    else:
        write_cover_filter_step_json_skipped(DATA_DIR, output_prefix, target_date, "COVER_STYLE_INTRADAY_DISABLED")

    merge_cover_similarity_counts(raw_payload)
    raw_payload, _total, _eligible, _skipped = merge_inspiration_filter_stats(raw_payload)
    retention = _build_crawl_product_retention_report(raw_payload)
    retention["target_date"] = target_date
    raw_payload["crawl_product_retention_report"] = retention
    raw_path.write_text(json.dumps(raw_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    retention_path = DATA_DIR / f"{output_prefix}_crawl_product_retention.json"
    retention_path.write_text(json.dumps(retention, ensure_ascii=False, indent=2), encoding="utf-8")

    init_pipeline_db()
    insights = upsert_daily_creative_insights(target_date, raw_payload, {})
    library, grouped = upsert_creative_library(target_date, raw_payload, {})
    filter_logs = upsert_daily_video_enhancer_filter_log(
        target_date,
        raw_payload.get("filter_report") if isinstance(raw_payload, dict) else None,
    )
    counts: Counter[str] = Counter()
    for item in raw_payload.get("items") or []:
        if isinstance(item, dict):
            counts[_product_from_item(item)] += 1
    return {
        "target_date": target_date,
        "mode": "shallow",
        "products": products,
        "raw_path": str(raw_path),
        "retention_path": str(retention_path),
        "raw_items_after_cover": len(raw_payload.get("items") or []),
        "per_product_after_cover": dict(counts),
        "db_upserts": {
            "daily_creative_insights": int(insights),
            "creative_library": int(library),
            "creative_library_grouped": int(grouped),
            "filter_log": int(filter_logs),
        },
    }


def execute_plan(
    plan: dict[str, Any],
    *,
    execute: bool = False,
    runner: CommandRunner = run_command,
    shallow_runner: ShallowRunner = run_shallow_crawl_and_cover,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for step in plan.get("steps") or []:
        if not isinstance(step, dict):
            continue
        print(f"[cold-start] {step.get('date')} {step.get('mode')}: {' '.join(step.get('command') or [])}")
        if not execute:
            continue
        if step.get("mode") == "shallow":
            result = shallow_runner(str(step.get("date") or ""), str(plan.get("products") or ""))
            if isinstance(result, dict):
                results.append(result)
        else:
            runner([str(part) for part in (step.get("command") or [])])
            results.append({"target_date": step.get("date"), "mode": "full"})
    return results


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="VE 新竞品冷启动入口（默认只打印计划，不真实执行）")
    parser.add_argument("--products", required=True, help="新增竞品产品名，逗号分隔，需与 config/ai_product.json 完全一致")
    parser.add_argument("--run-date", default="", help="运行日期 YYYY-MM-DD，默认今天 UTC+8")
    parser.add_argument("--history-days", type=int, default=DEFAULT_HISTORY_DAYS, help="冷启动总天数，默认 4")
    parser.add_argument("--execute", action="store_true", help="真实执行冷启动；不传时只打印计划")
    parser.add_argument("--plan-json", default="", help="可选：将计划写入指定 JSON 文件")
    parser.add_argument("--shallow-date", default="", help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    load_project_env()
    if args.shallow_date:
        if not args.execute:
            print(
                json.dumps(
                    {
                        "target_date": args.shallow_date,
                        "mode": "shallow",
                        "products": args.products,
                        "execute": False,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0
        result = run_shallow_crawl_and_cover(args.shallow_date, args.products)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    plan = build_cold_start_plan(
        products=args.products,
        run_date=args.run_date or None,
        history_days=args.history_days,
        python_bin=sys.executable,
    )
    if args.plan_json:
        path = Path(args.plan_json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"[cold-start] 已写计划 {path}")
    print(json.dumps(plan, ensure_ascii=False, indent=2))
    execute_plan(plan, execute=bool(args.execute))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
