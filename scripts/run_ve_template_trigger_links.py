#!/usr/bin/env python3
"""Populate clickable VE template-copy trigger links in the Bitable."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ua_workflows.shared.config import load_project_env
from ua_workflows.video_enhancer.template_recognition import yesterday_shanghai
from ua_workflows.video_enhancer.template_trigger import update_template_trigger_links


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="给 VE 素材写入可点击模板复刻触发链接")
    parser.add_argument("--date", default=yesterday_shanghai(), help="目标抓取日期 YYYY-MM-DD，默认昨天")
    parser.add_argument("--bitable-url", default="", help="默认 VIDEO_ENHANCER_BITABLE_URL")
    parser.add_argument(
        "--trigger-url",
        default=os.getenv("VE_TEMPLATE_TRIGGER_URL", "http://127.0.0.1:8765/trigger"),
        help="点击后访问的 /trigger URL；本机点击默认 http://127.0.0.1:8765/trigger",
    )
    parser.add_argument("--reviewer", default="haopeng", choices=["haopeng", "weilan", "浩鹏", "尉蓝"])
    parser.add_argument("--include-legacy", action="store_true", help="评分为空时纳入旧采纳/接受记录")
    parser.add_argument(
        "--include-all-records",
        action="store_true",
        help="给目标日期所有记录预写触发链接；点击时服务端仍会校验是否满 3 星",
    )
    parser.add_argument(
        "--link-only",
        action="store_true",
        help="只写触发链接，不改模板复刻状态/任务ID；适合给已有表补入口",
    )
    parser.add_argument("--token", default=os.getenv("VE_TEMPLATE_TRIGGER_TOKEN", ""))
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_project_env()
    args = parse_args(argv)
    bitable_url = (args.bitable_url or os.getenv("VIDEO_ENHANCER_BITABLE_URL") or "").strip()
    if not bitable_url:
        raise SystemExit("请配置 VIDEO_ENHANCER_BITABLE_URL 或传入 --bitable-url")
    result = update_template_trigger_links(
        bitable_url=bitable_url,
        trigger_url=args.trigger_url,
        target_date=args.date,
        reviewer=args.reviewer,
        include_legacy=bool(args.include_legacy),
        token=args.token,
        include_all_records=bool(args.include_all_records),
        link_only=bool(args.link_only),
        dry_run=bool(args.dry_run),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
