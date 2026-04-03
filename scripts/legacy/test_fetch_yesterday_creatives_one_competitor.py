"""
测试：只爬取一个竞品，看“7天窗口界面可见的全部素材”抓取效果。

用法：
  DEBUG=1 python scripts/test_fetch_yesterday_creatives_one_competitor.py
  python scripts/test_fetch_yesterday_creatives_one_competitor.py --product "ChatOn - AI Chat Bot Assistant"
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date, timedelta

from path_util import CONFIG_DIR, DATA_DIR
from run_search_workflow import run_batch


def parse_args():
    p = argparse.ArgumentParser(description="测试：单竞品抓取前一天上线素材")
    p.add_argument(
        "--product",
        default="",
        help="指定竞品名称（必须与 config/ai_product.json 的 key 一致）；不传则取第一个",
    )
    p.add_argument(
        "--date",
        default=None,
        metavar="YYYY-MM-DD",
        help="抓取日期（默认今天），实际会抓取该日期的前一天上线素材",
    )
    args = p.parse_args()
    crawl_date = date.fromisoformat(args.date) if args.date else date.today()
    args.crawl_date = crawl_date.isoformat()
    args.target_date = (crawl_date - timedelta(days=1)).isoformat()
    return args


def _pick_one_product(name: str) -> str:
    data = json.load(open(CONFIG_DIR / "ai_product.json", encoding="utf-8"))
    all_products: list[str] = []
    if isinstance(data, dict):
        for _, items in data.items():
            if isinstance(items, dict):
                all_products.extend([str(k) for k in items.keys() if k])
    if not all_products:
        raise RuntimeError("ai_product.json 中无竞品")
    if name and name in all_products:
        return name
    return all_products[0]


async def main():
    args = parse_args()
    product = _pick_one_product(args.product)
    print(f"[测试] product={product}，抓取 7天窗口界面可见的全部素材（工具→7天→素材）")

    results = await run_batch(
        keywords=[product],
        debug=True,   # 测试默认开界面，方便你观察点击是否成功
        is_tool=True,
        order_by="latest",
        use_popularity_top1=False,
    )
    r = results[0] if results else {}
    all_creatives = r.get("all_creatives") or []
    if not isinstance(all_creatives, list):
        all_creatives = []

    from datetime import datetime

    y = [c for c in all_creatives if isinstance(c, dict)]
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out = DATA_DIR / f"test_7d_all_creatives_{product}_{args.crawl_date}.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(
            {
                "tested_at": datetime.now().isoformat(timespec="seconds"),
                "product": product,
                "crawl_date": args.crawl_date,
                "total_captured_7d": len(all_creatives),
                "total_dumped": len(y),
                "creatives": y,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )
    print(f"[测试] 7天窗口素材={len(all_creatives)}，已保存：{out.name}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("中断。", file=sys.stderr)

