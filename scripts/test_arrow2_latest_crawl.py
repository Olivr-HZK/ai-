from __future__ import annotations

import argparse
import asyncio
import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ua_workflows.arrow2.crawl import CONFIG_FILE, _load_entries, main as crawl_main  # noqa: E402
from ua_workflows.shared.crawl_smoke import build_arrow2_crawl_args, update_process_browser_mode  # noqa: E402


def _default_date() -> str:
    return (datetime.now(timezone(timedelta(hours=8))).date() - timedelta(days=1)).isoformat()


def _default_product() -> str:
    cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    entries = _load_entries(cfg)
    if not entries:
        raise SystemExit("未找到 Arrow2 可测试产品，请检查 config/arrow2_competitor.json")
    return entries[0]["match"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Arrow2 latest_yesterday 爬取冒烟测试：只跑一个产品，只落 raw JSON，不入库/不同步。",
        epilog=(
            "示例:\n"
            "  .venv/bin/python scripts/test_arrow2_latest_crawl.py --headless\n"
            "  .venv/bin/python scripts/test_arrow2_latest_crawl.py --headed --product \"Arrows - Puzzle Escape\""
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--date", default=_default_date(), help="目标日期 YYYY-MM-DD，默认昨日 UTC+8")
    parser.add_argument("--product", default="", help="只验证一个产品；默认 config 中第一个 Arrow2 产品")
    parser.add_argument("--output-prefix", default="", help="输出前缀，默认 smoke_arrow2_latest_<date>")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--headed", action="store_true", help="有头浏览器运行")
    mode.add_argument("--headless", action="store_true", help="无头浏览器运行（默认）")
    return parser.parse_args()


@contextmanager
def _argv(args: list[str]) -> Iterator[None]:
    old = sys.argv[:]
    sys.argv = ["ua_workflows.arrow2.crawl", *args]
    try:
        yield
    finally:
        sys.argv = old


def main() -> None:
    args = parse_args()
    date = args.date.strip()
    product = (args.product or "").strip() or _default_product()
    output_prefix = (args.output_prefix or "").strip() or f"smoke_arrow2_latest_{date}"
    update_process_browser_mode(headed=bool(args.headed), headless=True if not args.headed else bool(args.headless))

    forward_args = build_arrow2_crawl_args(
        date=date,
        product=product,
        pull_only="latest_yesterday",
        output_prefix=output_prefix,
    )
    print(f"[smoke] Arrow2 latest crawl-only product={product!r} date={date} output_prefix={output_prefix}")
    with _argv(forward_args):
        asyncio.run(crawl_main())


if __name__ == "__main__":
    main()
