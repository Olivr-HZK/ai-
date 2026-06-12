#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ua_workflows.video_enhancer.video_content_backfill import (
    DEFAULT_BACKFILL_JSON,
    DEFAULT_OUTPUT_JSON,
    DEFAULT_REPORT_HTML,
    parse_adkeys,
    fetch_missing_video_content_by_adkeys,
    load_project_env,
    read_video_content_records_for_adkeys,
    render_dashboard_html,
    upsert_video_content_records,
    write_backfill_report,
    build_similarity_payload,
)


def _load_adkeys_from_file(path: str | None) -> list[str]:
    if not path:
        return []
    file_path = Path(path)
    if not file_path.exists():
        raise SystemExit(f"[输入错误] 找不到 adkeys 文件: {path}")
    text = file_path.read_text(encoding="utf-8")
    return parse_adkeys([text])


def build_arg_parser() -> argparse.ArgumentParser:
    today = date.today().isoformat()
    output_default = str(DEFAULT_OUTPUT_JSON.with_name(f"ve_ai_video_video_content_backfill_adkeys_{today}.json"))
    backfill_default = str(DEFAULT_BACKFILL_JSON.with_name(f"ve_ai_video_video_content_backfill_by_adkeys_{today}.json"))
    report_default = str(DEFAULT_REPORT_HTML.with_name(f"ve_ai_video_video_content_backfill_adkeys_{today}.html"))

    parser = argparse.ArgumentParser(description="按 ad_key 批量回填 Guangdada 视频素材脚本内容。")
    parser.add_argument("--adkey", action="append", default=[], help="单个 ad_key，支持重复传多个")
    parser.add_argument("--adkeys-file", default="", help="每行或逗号分隔的 ad_key 文件")
    parser.add_argument("--product", default="", help="可选，限制产品名，默认为全部")
    parser.add_argument(
        "--target-date",
        action="append",
        default=[],
        help="可选，限制 ad_key 对应的 target_date（可重复），不传则回填全部命中行。",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="每条 ad_key 的最大重试次数（默认 3）",
    )
    parser.add_argument("--debug", action="store_true", help="以有头浏览器运行，便于观察")
    parser.add_argument(
        "--max-scroll-rounds",
        type=int,
        default=1,
        help="回退到关键词搜索时的单页滚动上限（默认 1）",
    )
    parser.add_argument(
        "--per-key-timeout-sec",
        type=float,
        default=float(__import__("os").getenv("VE_VIDEO_CONTENT_BACKFILL_PER_KEY_TIMEOUT_SEC", "90")),
        help="每次 ad_key 搜索/点卡/等待素材脚本分析的超时时间（默认 90 秒）",
    )
    parser.add_argument("--no-direct", action="store_true", help="禁用 material-script-analysis 接口，直接走 keyword 搜索回退")
    parser.add_argument(
        "--skip-search",
        action="store_true",
        help="仅走 API 接口，不再进行关键词搜索回退",
    )
    parser.add_argument("--top-k", type=int, default=3, help="去重看板每条保留的 TopK（默认 3）")
    parser.add_argument(
        "--min-similarity",
        type=float,
        default=0.85,
        help="去重看板阈值（默认 0.85）",
    )
    parser.add_argument(
        "--build-dashboard",
        action="store_true",
        help="写入相似度看板 HTML，默认关闭",
    )
    parser.add_argument("--output-json", default=output_default, help="相似度 payload 输出路径")
    parser.add_argument("--backfill-json", default=backfill_default, help="回填结果报告输出路径")
    parser.add_argument("--report-html", default=report_default, help="看板 HTML 输出路径")
    return parser


def run(args: argparse.Namespace) -> dict[str, Any]:
    load_project_env(override=True)
    adkeys = parse_adkeys(args.adkey) + _load_adkeys_from_file(args.adkeys_file)
    # dedupe keep order
    merged = []
    seen = set()
    for key in adkeys:
        if key and key not in seen:
            seen.add(key)
            merged.append(key)
    adkeys = merged

    if not adkeys:
        raise SystemExit("[输入错误] 请至少提供一个 --adkey 或 --adkeys-file")

    print(f"[准备] 读取 {len(adkeys)} 个 ad_key（去重后）")
    records = read_video_content_records_for_adkeys(adkeys, target_dates=args.target_date, product=args.product)
    if not records:
        raise SystemExit("[结果] 未匹配到可回填记录（DB 内不存在该 ad_key）。")

    print(f"[准备] 命中 {len(records)} 条记录，开始回填...")
    fetch_summary = asyncio_run(
        fetch_missing_video_content_by_adkeys(
            records,
            retries=max(1, args.retries),
            debug=args.debug,
            max_scroll_rounds=max(1, args.max_scroll_rounds),
            use_direct=not args.no_direct,
            setup_search=not args.skip_search,
            date_range=None,
            skip_time_filter=True,
            per_key_timeout_sec=getattr(args, "per_key_timeout_sec", 90),
            persist_each_update=True,
        )
    )

    upsert_summary = upsert_video_content_records(records)
    db_summary = {"upsert": upsert_summary, "fetch": fetch_summary}
    write_backfill_report(Path(args.backfill_json), records, db_summary)

    dashboard_path = ""
    if args.build_dashboard:
        payload = build_similarity_payload(records, top_k=args.top_k, min_similarity=args.min_similarity)
        payload["scope"] = {
            "ad_keys": adkeys,
            "product": args.product,
            "target_dates": args.target_date,
        }
        Path(args.output_json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output_json).write_text(
            __import__("json").dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        title_parts = ["素材脚本回填结果"]
        if args.product:
            title_parts.append(args.product)
        if args.target_date:
            title_parts.append(",".join(args.target_date))
        html_text = render_dashboard_html(
            payload,
            title=" / ".join(title_parts),
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        Path(args.report_html).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report_html).write_text(html_text, encoding="utf-8")
        dashboard_path = str(args.report_html)

    return {
        "records": len(records),
        "fetch": fetch_summary,
        "upsert": upsert_summary,
        "with_content": sum(1 for r in records if str(r.get("video_content") or "").strip()),
        "backfill_json": str(args.backfill_json),
        "output_json": str(args.output_json) if args.build_dashboard else "",
        "report_html": dashboard_path,
    }


def asyncio_run(coro):
    import asyncio

    return asyncio.run(coro)


def main() -> None:
    summary = run(build_arg_parser().parse_args())

    json.dump(summary, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
