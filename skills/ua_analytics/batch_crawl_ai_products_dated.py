"""
根据 config/ai_product.json 做关键词批量爬取，结果带日期和 appid，写入 data/ai_products_ua.db。
- 使用产品名作为搜索关键词（仍通过原有 run_search_workflow.py）
- 配置中的 value 视为 appid（字符串或列表均转为 list 存储）
- 同时输出 JSON 备份：data/ai_products_ua_YYYYMMDD.json
"""
import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, List, Tuple

from path_util import CONFIG_DIR, DATA_DIR, data_path
from ua_crawl_db import insert_crawl_results


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
    out_file = data_path(f"ai_products_ua_{crawl_date_short}.json")

    print(f"爬取日期: {crawl_date}")
    print(f"输出文件: {out_file.name}")
    print(f"共 {len(product_list)} 个产品:")
    for category, name, appids in product_list:
        print(f"  - [{category}] {name}  appid={appids}")

    # 使用本 skill 目录内的 run_search_workflow（可独立于项目根 scripts 运行）
    skill_dir = Path(__file__).resolve().parent
    if str(skill_dir) not in sys.path:
        sys.path.insert(0, str(skill_dir))
    from run_search_workflow import run  # type: ignore

    results = []
    for i, (category, product_name, appids) in enumerate(product_list, 1):
        print(f"\n{'='*50}\n[{i}/{len(product_list)}] [{category}] {product_name}\n{'='*50}")
        try:
            r = await run(keyword=product_name, debug=debug, is_tool=True)
            results.append({
                "category": category,
                "product": product_name,
                "appid": appids,
                "crawl_date": crawl_date,
                "keyword": r.get("keyword"),
                "selected": r.get("selected"),
                "total_captured": r.get("total_captured", 0),
            })
        except Exception as e:
            print(f"[失败] {product_name}: {e}", file=sys.stderr)
            results.append({
                "category": category,
                "product": product_name,
                "appid": appids,
                "crawl_date": crawl_date,
                "error": str(e),
            })

    # 写入数据库
    n = insert_crawl_results(crawl_date, results)
    print(f"\n[完成] 已写入数据库 data/ai_products_ua.db，共 {n} 条")

    # 写 JSON 备份
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

