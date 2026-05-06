"""
临时脚本：把已有的 ai_products_ua_YYYYMMDD.json 导入到 data/ai_products_ua.db

用法:
  python scripts/import_json_to_db.py
  python scripts/import_json_to_db.py data/ai_products_ua_20260226.json
"""
import json
import sys
from pathlib import Path

from path_util import DATA_DIR
from ua_crawl_db import init_db, insert_crawl_results

DEFAULT_JSON = DATA_DIR / "ai_products_ua_20260226.json"


def main():
    if len(sys.argv) > 1:
        json_path = Path(sys.argv[1])
    else:
        json_path = DEFAULT_JSON

    if not json_path.is_file():
        print(f"[错误] 文件不存在: {json_path}", file=sys.stderr)
        sys.exit(1)

    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    crawl_date = data.get("crawl_date")
    products = data.get("products") or []
    if not crawl_date or not products:
        print("[错误] JSON 里缺少 crawl_date 或 products", file=sys.stderr)
        sys.exit(1)

    print(f"读取 {json_path.name}，crawl_date={crawl_date}，共 {len(products)} 条")
    n = insert_crawl_results(crawl_date, products)
    print(f"已入库 {n} 条 → data/ai_products_ua.db")


if __name__ == "__main__":
    main()
