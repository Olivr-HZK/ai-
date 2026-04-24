#!/usr/bin/env python3
"""
清空 Arrow2 专用 SQLite 中 arrow2_daily_insights、arrow2_creative_library 全部行。
库路径由 ARROW2_SQLITE_PATH 决定（默认 data/arrow2_pipeline.db）。

用法（项目根目录）：
  .venv/bin/python3 scripts/arrow2_wipe_sqlite.py --yes
"""
from __future__ import annotations

import argparse
import os
import sys

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

from arrow2_pipeline_db import wipe_arrow2_sqlite_all_rows  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser(description="清空 Arrow2 SQLite 两表全部行")
    p.add_argument(
        "--yes",
        action="store_true",
        help="确认执行（防止误删）",
    )
    args = p.parse_args()
    if not args.yes:
        print("请追加 --yes 确认清空当前 ARROW2_SQLITE_PATH 指向的库。", file=sys.stderr)
        sys.exit(2)
    path_hint = (os.getenv("ARROW2_SQLITE_PATH") or "data/arrow2_pipeline.db").strip()
    print(f"[arrow2-wipe] ARROW2_SQLITE_PATH={path_hint!r}")
    out = wipe_arrow2_sqlite_all_rows()
    print(f"[arrow2-wipe] 已删除行数: {out}")


if __name__ == "__main__":
    main()
