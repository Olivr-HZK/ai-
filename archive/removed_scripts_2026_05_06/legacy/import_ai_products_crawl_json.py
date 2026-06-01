"""
将 data/ai_products_ua_YYYYMMDD.json 备份回灌到 SQLite（ai_products_crawl）。

背景：
- batch_crawl_ai_products_dated.py 每天会生成一份 JSON 备份：data/ai_products_ua_YYYYMMDD.json
- 你如果误删了某天（比如 2026-03-17）的数据，可以用这个脚本一键恢复该日 ai_products_crawl

用法（项目根目录）：
  .venv/bin/python3 scripts/import_ai_products_crawl_json.py --date 2026-03-17
  .venv/bin/python3 scripts/import_ai_products_crawl_json.py --file data/ai_products_ua_20260317.json

说明：
- 会先删除该 crawl_date 在 ai_products_crawl 里的旧记录，再重新插入（幂等）。
- 只恢复 ai_products_crawl，不会自动重跑 LLM 分析/同步多维表格。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

from path_util import DATA_DIR
from ua_crawl_db import insert_crawl_results


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="从每日 JSON 备份回灌 ai_products_crawl。")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", type=str, help="日期 YYYY-MM-DD（会自动寻找对应 JSON 备份）")
    g.add_argument("--file", type=str, help="直接指定 JSON 备份文件路径")
    return p.parse_args()


def _file_from_date(date_str: str) -> Path:
    ymd = date_str.replace("-", "")
    return DATA_DIR / f"ai_products_ua_{ymd}.json"


def _load_payload(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"未找到备份文件: {path}")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("JSON 备份格式不正确：顶层必须是 object")
    return data


def main() -> None:
    args = _parse_args()
    if args.file:
        path = Path(args.file).expanduser()
        payload = _load_payload(path)
        crawl_date = str(payload.get("crawl_date") or "").strip()
        if not crawl_date:
            raise ValueError("JSON 中缺少 crawl_date，无法回灌")
    else:
        crawl_date = args.date.strip()
        path = _file_from_date(crawl_date)
        payload = _load_payload(path)

    products = payload.get("products") or []
    if not isinstance(products, list):
        raise ValueError("JSON 中 products 字段格式不正确：必须是 list")

    rows: List[Dict[str, Any]] = []
    for item in products:
        if not isinstance(item, dict):
            continue
        # 允许 JSON 内 crawl_date 与参数不一致时，以参数/顶层为准强制覆盖
        item = dict(item)
        item["crawl_date"] = crawl_date
        rows.append(item)

    if not rows:
        print(f"[import_json] {path.name} 中没有可回灌的数据。", file=sys.stderr)
        return

    n = insert_crawl_results(crawl_date, rows)
    print(f"[import_json] 已回灌 ai_products_crawl：crawl_date={crawl_date}，写入 {n} 条（来源 {path.name}）。")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[import_json] 失败：{e}", file=sys.stderr)
        raise

