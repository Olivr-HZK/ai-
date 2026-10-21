#!/usr/bin/env python3
"""Run the social/chat app daily creative workflow using the VE pipeline."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config" / "social_chat_apps.json"
DEFAULT_DB = ROOT / "data" / "afizzy_competitor_pipeline.db"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="社交/聊天 AI 应用素材日工作流")
    parser.add_argument("--date", default="", metavar="YYYY-MM-DD", help="目标日期（UTC+8），默认昨天")
    parser.add_argument("--bitable-url", default="", help="同步目标多维表链接；默认 SOCIAL_CHAT_BITABLE_URL 或 VE 链接")
    parser.add_argument("--skip-cover-dedupe", action="store_true")
    parser.add_argument("--no-bitable-sync", action="store_true")
    parser.add_argument("--db-path", default="", help="独立 SQLite 路径，默认 data/afizzy_competitor_pipeline.db")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    target_date = (args.date or (date.today() - timedelta(days=1)).isoformat()).strip()
    bitable_url = (args.bitable_url or os.getenv("SOCIAL_CHAT_BITABLE_URL") or os.getenv("VIDEO_ENHANCER_BITABLE_URL") or "").strip()
    if not bitable_url and not args.no_bitable_sync:
        raise SystemExit("未配置多维表链接：请传 --bitable-url 或设置 SOCIAL_CHAT_BITABLE_URL")

    db_path = Path(args.db_path or os.getenv("AFIZZY_COMPETITOR_DB_PATH") or DEFAULT_DB).expanduser()
    child_env = os.environ.copy()
    child_env["VIDEO_ENHANCER_DB_PATH"] = str(db_path)
    cmd = [
        sys.executable,
        "-m",
        "ua_workflows.video_enhancer.pipeline",
        "--date",
        target_date,
        "--workflow-name",
        "social_chat",
        "--crawl-config-file",
        str(CONFIG),
        "--crawl-categories",
        "social_chat",
        "--skip-advertiser-filter",
        "--skip-competitor-config-sync",
        "--no-card",
        "--no-wecom",
        "--no-sheet",
        "--no-topn-push",
        "--per-advertiser-daily-limit",
        "10",
    ]
    if args.no_bitable_sync:
        cmd.append("--no-bitable-sync")
    elif bitable_url:
        cmd.extend(["--bitable-url", bitable_url])
    if args.skip_cover_dedupe:
        cmd.append("--skip-cover-dedupe")
    print("$ " + " ".join(cmd), flush=True)
    completed = subprocess.run(cmd, cwd=ROOT, env=child_env)
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
