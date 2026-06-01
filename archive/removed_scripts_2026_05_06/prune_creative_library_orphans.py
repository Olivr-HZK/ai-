#!/usr/bin/env python3
"""
⚠️ 仅 Video Enhancer：data/video_enhancer_pipeline.db 的 creative_library（不是 Arrow2）。

Arrow2 请用：scripts/prune_arrow2_creative_library_orphans.py

删除 creative_library 中在 daily_creative_insights 从未出现过的 ad_key。
默认仅 dry-run；加 --execute 才真正 DELETE。

用法（项目根目录，建议 .venv）：
  .venv/bin/python scripts/prune_creative_library_orphans.py
  .venv/bin/python scripts/prune_creative_library_orphans.py --execute
"""

from __future__ import annotations

import argparse
import sys

from video_enhancer_pipeline_db import prune_creative_library_not_in_daily_insights


def main() -> None:
    p = argparse.ArgumentParser(description="按 daily_creative_insights 裁剪 creative_library 孤儿行")
    p.add_argument(
        "--execute",
        action="store_true",
        help="执行删除（默认仅统计将要删除的行数）",
    )
    args = p.parse_args()
    dry = not args.execute
    r = prune_creative_library_not_in_daily_insights(dry_run=dry)
    if dry:
        print(
            f"[dry-run] creative_library 共 {r['total_before']} 行，"
            f"将删除 {r['would_delete']} 行（daily 中无此 ad_key），保留 {r['kept']} 行。"
        )
        print("若确认执行：加 --execute")
        return
    print(
        f"[done] 已删除 {r['deleted']} 行，保留 {r['kept']} 行（原先共 {r['total_before']} 行）。"
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
