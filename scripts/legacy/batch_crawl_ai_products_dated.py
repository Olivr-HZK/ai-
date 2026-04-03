"""
根据 ai_product.json 做关键词批量爬取，结果带日期和 appid，写入 data 下 SQLite 数据库。

- 使用产品名作为搜索关键词
- 配置中的 value 视为 appid（字符串或列表均转为 list 存储）
- 数据库：data/ai_products_ua.db，表 ai_products_crawl
- 同时保留 JSON 备份：data/ai_products_ua_YYYYMMDD.json

用法: python scripts/batch_crawl_ai_products_dated.py [--debug]
"""
import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, List, Tuple

from path_util import CONFIG_DIR, DATA_DIR

INPUT_FILE = CONFIG_DIR / "ai_product.json"


def _normalize_appid(v: Any) -> list:
    """配置中的 appid 可能是字符串或列表，统一为 list"""
    if v is None or v == "":
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    return [str(v).strip()]


def extract_products_with_appid(data: dict) -> List[Tuple[str, str, list]]:
    """返回 [(分类, 产品名, appid_list), ...]"""
    out = []
    for category, products_dict in data.items():
        if not isinstance(products_dict, dict):
            continue
        for product_name, appid_val in products_dict.items():
            if not (product_name and str(product_name).strip()):
                continue
            appids = _normalize_appid(appid_val)
            out.append((category.strip(), str(product_name).strip(), appids))
    return out


async def main(debug: bool = False):
    if not INPUT_FILE.exists():
        print(f"[错误] 未找到 {INPUT_FILE.name}", file=sys.stderr)
        sys.exit(1)

    with open(INPUT_FILE, encoding="utf-8") as f:
        data = json.load(f)

    product_list = extract_products_with_appid(data)
    if not product_list:
        print("[错误] 未解析到任何产品", file=sys.stderr)
        sys.exit(1)

    crawl_date = datetime.now().strftime("%Y-%m-%d")
    crawl_date_short = datetime.now().strftime("%Y%m%d")
    out_file = DATA_DIR / f"ai_products_ua_{crawl_date_short}.json"

    print(f"爬取日期: {crawl_date}")
    print(f"输出文件: {out_file.name}")
    print(f"共 {len(product_list)} 个产品:")
    for category, name, appids in product_list:
        print(f"  - [{category}] {name}  appid={appids}")

    from run_search_workflow import run_batch

    keywords = [product_name for _, product_name, _ in product_list]
    batch_results = await run_batch(keywords, debug=debug, is_tool=True)

    results = []
    for (category, product_name, appids), r in zip(product_list, batch_results):
        top_list = r.get("top_creatives") or []
        if not top_list and r.get("selected"):
            top_list = [r["selected"]]
        top_list = top_list[:3]
        if not top_list:
            print(f"[失败] {product_name}: 未拿到任何素材结果", file=sys.stderr)
            continue
        for rank, sel in enumerate(top_list, 1):
            results.append({
                "category": category,
                "product": product_name,
                "appid": appids,
                "crawl_date": crawl_date,
                "keyword": r.get("keyword"),
                "selected": sel,
                "total_captured": r.get("total_captured", 0),
                "rank": rank,
            })

    # 写入数据库
    from ua_crawl_db import insert_crawl_results

    n = insert_crawl_results(crawl_date, results)
    print(f"\n[完成] 已写入数据库 data/ai_products_ua.db，共 {n} 条")

    # 同时写一份带日期的 JSON 备份
    payload = {
        "crawl_date": crawl_date,
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
