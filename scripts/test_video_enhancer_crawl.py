from __future__ import annotations

import argparse
import asyncio
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ua_workflows.shared.crawl_smoke import (  # noqa: E402
    build_video_enhancer_crawl_args,
    update_process_browser_mode,
)
from ua_workflows.video_enhancer.crawl import _load_workflow_competitors, main as crawl_main  # noqa: E402


def _default_date() -> str:
    return (datetime.now(timezone(timedelta(hours=8))).date() - timedelta(days=1)).isoformat()


def _default_product() -> str:
    competitors = _load_workflow_competitors()
    if not competitors:
        raise SystemExit("未找到 Video Enhancer 可测试产品，请检查 config/ai_product.json")
    return competitors[0].product


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Video Enhancer 爬取冒烟测试：只落 raw JSON，不入库/不同步。",
        epilog=(
            "示例:\n"
            "  .venv/bin/python scripts/test_video_enhancer_crawl.py --headless\n"
            "  .venv/bin/python scripts/test_video_enhancer_crawl.py --headed --product \"Remini - AI Photo Enhancer\"\n"
            "  .venv/bin/python scripts/test_video_enhancer_crawl.py --headed --all-products --pause-per-product"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--date", default=_default_date(), help="目标日期 YYYY-MM-DD，默认昨日 UTC+8")
    parser.add_argument("--product", default="", help="只验证一个产品；默认 config 中第一个 VE 产品")
    parser.add_argument("--all-products", action="store_true", help="验证 config 中全部 VE 产品（仍排除 Hula）")
    parser.add_argument("--pause-per-product", action="store_true", help="每个产品爬完后暂停，供人工检查浏览器页面")
    parser.add_argument("--keep-browser-open", action="store_true", help="爬完并写出文件后保持浏览器打开，回车后才关闭")
    parser.add_argument("--output-prefix", default="", help="输出前缀，默认 smoke_video_enhancer_<date>")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--headed", action="store_true", help="有头浏览器运行")
    mode.add_argument("--headless", action="store_true", help="无头浏览器运行（默认）")
    return parser.parse_args()


@contextmanager
def _argv(args: list[str]) -> Iterator[None]:
    old = sys.argv[:]
    sys.argv = ["ua_workflows.video_enhancer.crawl", *args]
    try:
        yield
    finally:
        sys.argv = old


def main() -> None:
    args = parse_args()
    date = args.date.strip()
    product = "" if args.all_products else ((args.product or "").strip() or _default_product())
    output_prefix = (args.output_prefix or "").strip() or f"smoke_video_enhancer_{date}"
    update_process_browser_mode(headed=bool(args.headed), headless=True if not args.headed else bool(args.headless))

    if args.all_products:
        forward_args = ["--target-date", date]
        if output_prefix:
            forward_args.extend(["--output-prefix", output_prefix])
    else:
        forward_args = build_video_enhancer_crawl_args(
            date=date,
            product=product,
            output_prefix=output_prefix,
        )
    if args.pause_per_product:
        forward_args.append("--pause-per-product")
    if args.keep_browser_open:
        forward_args.append("--keep-browser-open")
    target_desc = "ALL_PRODUCTS" if args.all_products else repr(product)
    print(f"[smoke] Video Enhancer crawl-only product={target_desc} date={date} output_prefix={output_prefix}")
    with _argv(forward_args):
        asyncio.run(crawl_main())


if __name__ == "__main__":
    main()
