"""
兼容包装器：旧入口仍可用，但底层统一转调 `test_arrow2_competitors.py`。

等价于：
  TARGET_DATE=YYYY-MM-DD PYTHONUNBUFFERED=1 .venv/bin/python scripts/test_arrow2_competitors.py \
    --all-products --pull-only latest_yesterday --debug --pause-per-product
"""

from __future__ import annotations

import os
import subprocess
import sys

from dotenv import load_dotenv

from path_util import PROJECT_ROOT

load_dotenv(PROJECT_ROOT / ".env")


def main() -> int:
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    os.environ.setdefault("ARROW2_DEBUG_PAUSE_AT_END", "0")
    os.environ.setdefault("ARROW2_DEBUG_PAUSE", "0")
    os.environ.setdefault("ARROW2_DEBUG_PAUSE_AFTER_GEO", "0")
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "test_arrow2_competitors.py"),
        "--all-products",
        "--pull-only",
        "latest_yesterday",
        "--debug",
        "--pause-per-product",
    ]
    print("[兼容入口] 已统一转调 test_arrow2_competitors.py：")
    print(" " + " ".join(cmd))
    return subprocess.run(cmd, cwd=str(PROJECT_ROOT)).returncode


if __name__ == "__main__":
    raise SystemExit(main())
