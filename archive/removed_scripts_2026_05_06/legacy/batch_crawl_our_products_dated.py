"""
根据 ai_product_us.json 批量爬取我方产品在广大大的 UA 素材，写入同一数据库，并标记 is_our_product=1。

- 使用 app id（Android 包名 / iOS 应用 ID）在广大大直接搜索，不再用产品名关键词
- 配置结构：category -> product_name -> { ios_appid, android_appid }
- 每个产品有 1～2 个 appid 则各搜一次，结果按产品合并后取热度前三
- 写入 data/ai_products_ua.db 表 ai_products_crawl，is_our_product=1
- 同时保留 JSON 备份：data/ai_products_ua_ours_YYYYMMDD.json

用法: python scripts/batch_crawl_our_products_dated.py [--debug]
"""
import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, List, Tuple

from path_util import CONFIG_DIR, DATA_DIR

INPUT_FILE = CONFIG_DIR / "ai_product_us.json"


def _to_appid_list(v: Any) -> List[str]:
    """从 ai_product_us 的 { ios_appid, android_appid } 转为非空 appid 列表"""
    if not v or not isinstance(v, dict):
        return []
    out = []
    for key in ("ios_appid", "android_appid"):
        val = v.get(key)
        if val is not None and str(val).strip():
            out.append(str(val).strip())
    return out


def extract_products(data: dict) -> List[Tuple[str, str, list]]:
    """返回 [(category, product_name, appid_list), ...]，仅包含至少有一个 appid 的产品"""
    out = []
    for category, products_dict in data.items():
        if not isinstance(products_dict, dict):
            continue
        for product_name, appid_val in products_dict.items():
            if not (product_name and str(product_name).strip()):
                continue
            appids = _to_appid_list(appid_val)
            if not appids:
                continue
            out.append((category.strip(), str(product_name).strip(), appids))
    return out


def build_search_items(
    product_list: List[Tuple[str, str, list]],
) -> List[Tuple[str, str, list, str]]:
    """
    每个产品的每个 appid 生成一条搜索项：(category, product_name, appid_list, search_term)。
    search_term 即用于广大大搜索的 app id（包名或 iOS id）。
    """
    items = []
    for category, product_name, appids in product_list:
        for appid in appids:
            items.append((category, product_name, appids, appid))
    return items


def merge_top_creatives(
    result_list: List[dict],
    max_per_product: int = 3,
) -> List[dict]:
    """多个 run_batch 结果（同一产品多 appid）合并，按 heat 去重后取前 max_per_product 条"""
    seen_keys = set()
    merged = []
    for r in result_list:
        for c in r.get("top_creatives") or ([] if not r.get("selected") else [r["selected"]]):
            key = c.get("ad_key") or id(c)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            merged.append(c)
    merged.sort(key=lambda c: (-(c.get("heat") or 0), c.get("days_count") or 999999))
    return merged[:max_per_product]


async def main(debug: bool = False):
    if not INPUT_FILE.exists():
        print(f"[错误] 未找到 {INPUT_FILE.name}", file=sys.stderr)
        sys.exit(1)

    with open(INPUT_FILE, encoding="utf-8") as f:
        data = json.load(f)

    product_list = extract_products(data)
    if not product_list:
        print("[错误] 未解析到任何我方产品（或均无 ios_appid/android_appid）", file=sys.stderr)
        sys.exit(1)

    search_items = build_search_items(product_list)
    crawl_date = datetime.now().strftime("%Y-%m-%d")
    crawl_date_short = datetime.now().strftime("%Y%m%d")
    out_file = DATA_DIR / f"ai_products_ua_ours_{crawl_date_short}.json"

    print(f"爬取日期: {crawl_date}")
    print(f"输出文件: {out_file.name}")
    print(f"我方产品: {len(product_list)} 个，按 app id 共 {len(search_items)} 次搜索:")
    for category, name, appids in product_list:
        print(f"  - [{category}] {name}  appid={appids}")

    from run_search_workflow import run_batch

    keywords = [item[3] for item in search_items]
    batch_results = await run_batch(keywords, debug=debug, is_tool=True)

    # 按产品合并：同一 (category, product_name) 的多次搜索结果合并后取热度前三
    from collections import defaultdict
    by_product: dict = defaultdict(list)
    product_appids: dict = {}
    for (category, product_name, appids, _), r in zip(search_items, batch_results):
        key = (category, product_name)
        by_product[key].append(r)
        product_appids[key] = appids

    results = []
    for (category, product_name), result_list in by_product.items():
        appids = product_appids[(category, product_name)]
        top_list = merge_top_creatives(result_list, max_per_product=3)
        if not top_list:
            print(f"[失败] {product_name} (appid={list(appids)}): 未拿到任何素材结果", file=sys.stderr)
            continue
        total_captured = sum(r.get("total_captured", 0) for r in result_list)
        for rank, sel in enumerate(top_list, 1):
            results.append({
                "category": category,
                "product": product_name,
                "appid": list(appids),
                "crawl_date": crawl_date,
                "keyword": list(appids)[0] if appids else "",
                "selected": sel,
                "total_captured": total_captured,
                "rank": rank,
            })

    from ua_crawl_db import insert_crawl_results

    n = insert_crawl_results(crawl_date, results, is_our_product=1)
    print(f"\n[完成] 已写入数据库 data/ai_products_ua.db（我方素材），共 {n} 条")

    payload = {
        "crawl_date": crawl_date,
        "is_our_product": True,
        "products": results,
    }
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[备份] JSON 已保存 {out_file.name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="显示浏览器")
    args = parser.parse_args()
    asyncio.run(main(debug=args.debug))
