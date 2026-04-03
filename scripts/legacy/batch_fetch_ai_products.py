"""
从 ai_product.json 解析所有产品名，批量调用 run_search_workflow 获取各产品的 UA 素材。

流程：对每个产品名搜索 → 7天 → 素材 → 展示估值 → 返回展示估值最高的素材

用法: python batch_fetch_ai_products.py [--debug]
"""
import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import List, Tuple

from path_util import CONFIG_DIR, DATA_DIR

INPUT_FILE = CONFIG_DIR / "ai_product.json"
OUTPUT_FILE = DATA_DIR / "ai_products_ua_results.json"


def extract_product_names(data: dict) -> List[Tuple[str, str]]:
    """返回 [(分类名, 产品名), ...]"""
    products = []
    for category, products_dict in data.items():
        if isinstance(products_dict, dict):
            for product_name in products_dict.keys():
                if product_name.strip():
                    products.append((category, product_name.strip()))
    return products


async def main(debug: bool = False):
    if not INPUT_FILE.exists():
        print(f"[错误] 未找到 {INPUT_FILE.name}", file=sys.stderr)
        sys.exit(1)

    with open(INPUT_FILE, encoding="utf-8") as f:
        data = json.load(f)

    product_list = extract_product_names(data)
    if not product_list:
        print("[错误] 未解析到任何产品", file=sys.stderr)
        sys.exit(1)

    print(f"共 {len(product_list)} 个产品待获取 UA 素材:")
    for category, name in product_list:
        print(f"  - [{category}] {name}")

    from run_search_workflow import run

    results = []
    for i, (category, product_name) in enumerate(product_list, 1):
        print(f"\n{'='*50}\n[{i}/{len(product_list)}] [{category}] {product_name}\n{'='*50}")
        try:
            r = await run(keyword=product_name, debug=debug, is_tool=True)
            results.append({
                "category": category,
                "product": product_name,
                "keyword": r.get("keyword"),
                "selected": r.get("selected"),
                "total_captured": r.get("total_captured", 0),
            })
        except Exception as e:
            print(f"[失败] {product_name}: {e}", file=sys.stderr)
            results.append({
                "category": category,
                "product": product_name,
                "error": str(e),
            })

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump({"products": results}, f, ensure_ascii=False, indent=2)

    print(f"\n[完成] 结果已写入 {OUTPUT_FILE.name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="显示浏览器")
    args = parser.parse_args()
    asyncio.run(main(debug=args.debug))
