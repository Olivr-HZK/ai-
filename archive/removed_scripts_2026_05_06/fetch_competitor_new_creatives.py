"""
根据 config/ai_product_us.json 中的竞品列表，抓取最近 7 天的 UA 素材（整页结果），
按天写入 SQLite 表 competitor_ua_creatives_daily，并与前一日对比写入
competitor_ua_new_creatives，便于查看「今天新增的竞品素材」。

用法（在项目根目录）：
  python scripts/fetch_competitor_new_creatives.py
  python scripts/fetch_competitor_new_creatives.py --date 2026-03-11
  python scripts/fetch_competitor_new_creatives.py --date 2026-03-11 --prev-date 2026-03-10
  DEBUG=1 python scripts/fetch_competitor_new_creatives.py --debug
"""

import argparse
import asyncio
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

from path_util import CONFIG_DIR

from run_search_workflow import run_batch
from competitor_ua_db import insert_competitor_creatives, compute_competitor_new_creatives

INPUT_FILE = CONFIG_DIR / "ai_product_us.json"


def _load_competitors() -> List[Dict[str, Any]]:
    """
    从 ai_product_us.json 解析竞品列表。
    返回元素形如：
      {
        "category": "video enhancer",
        "product": "AI Photo Enhancer - Evoke",
        "ios_appid": "...",
        "android_appid": "...",
      }
    """
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"未找到配置文件: {INPUT_FILE}")
    with open(INPUT_FILE, encoding="utf-8") as f:
        data = json.load(f)
    competitors: List[Dict[str, Any]] = []
    for category, products in data.items():
        if not isinstance(products, dict):
            continue
        for name, meta in products.items():
            if not name:
                continue
            meta = meta or {}
            competitors.append(
                {
                    "category": category,
                    "product": name,
                    "ios_appid": meta.get("ios_appid"),
                    "android_appid": meta.get("android_appid"),
                }
            )
    return competitors


def _parse_dates(
    date_str: str | None,
    prev_date_str: str | None,
) -> Tuple[str, str]:
    """解析爬取日期和对比日期。"""
    if date_str:
        crawl_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        crawl_date = datetime.today().date()
    if prev_date_str:
        prev_date = datetime.strptime(prev_date_str, "%Y-%m-%d").date()
    else:
        prev_date = crawl_date - timedelta(days=1)
    return crawl_date.strftime("%Y-%m-%d"), prev_date.strftime("%Y-%m-%d")


async def main(
    date: str | None = None,
    prev_date: str | None = None,
    debug: bool = False,
) -> None:
    crawl_date, prev_date_resolved = _parse_dates(date, prev_date)
    competitors = _load_competitors()
    if not competitors:
        print("配置中未解析到任何竞品。")
        return

    print(f"爬取日期: {crawl_date}，对比基准日: {prev_date_resolved}")
    print(f"共 {len(competitors)} 个竞品：")
    for c in competitors:
        print(
            f"  - [{c['category']}] {c['product']} "
            f"(iOS={c.get('ios_appid')}, Android={c.get('android_appid')})"
        )

    keywords = [c["product"] for c in competitors]

    print("\n[1/3] 调用搜索工作流，按竞品抓取最近 7 天 UA 素材（整页结果）...")
    # 竞品工作流：排序选择「最新创意」，不按展示估值排序
    batch_results = await run_batch(
        keywords,
        debug=debug,
        is_tool=False,
        order_by="latest",
    )

    print("[2/3] 汇总并写入 SQLite 表 competitor_ua_creatives_daily ...")
    items: List[Dict[str, Any]] = []
    total_raw = 0
    for meta, result in zip(competitors, batch_results):
        creatives = result.get("all_creatives") or []
        if not creatives:
            print(
                f"  - 无素材：[{meta['category']}] {meta['product']} "
                f"(keyword={result.get('keyword')})"
            )
            continue
        print(
            f"  - [{meta['category']}] {meta['product']} "
            f"→ 捕获 {len(creatives)} 条素材"
        )
        total_raw += len(creatives)
        for c in creatives:
            items.append(
                {
                    "category": meta["category"],
                    "product": meta["product"],
                    "ios_appid": meta.get("ios_appid"),
                    "android_appid": meta.get("android_appid"),
                    "creative": c,
                }
            )

    inserted = insert_competitor_creatives(crawl_date, items)
    print(
        f"[2/3] 已写入 competitor_ua_creatives_daily 表 {inserted} 条 "
        f"(原始素材 {total_raw} 条，按 ad_key 去重后可能略少)。"
    )

    print("[3/3] 计算相较前一日的新增竞品素材...")
    new_count = compute_competitor_new_creatives(crawl_date, prev_date_resolved)
    print(
        f"[3/3] 与 {prev_date_resolved} 对比，今日新增竞品素材 {new_count} 条，"
        "结果已写入 competitor_ua_new_creatives 表。"
    )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="按竞品列表抓取最近 7 天 UA 素材并计算每日新增。"
    )
    p.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        default=None,
        help="爬取日期（默认今天）",
    )
    p.add_argument(
        "--prev-date",
        metavar="YYYY-MM-DD",
        default=None,
        help="对比日期（默认等于 date 的前一天）",
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help="显示浏览器窗口",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    asyncio.run(main(date=args.date, prev_date=args.prev_date, debug=args.debug))

