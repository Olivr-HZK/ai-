"""
热门榜工作流 · 第 1 步：爬取 + 入库（7天 / 素材 / 最新创意，不使用 Top创意 过滤）

- 从 config/ai_product.json 读取竞品（seek / video enhancer）
- 调用 run_search_workflow.run_batch，在广大大搜索页依次执行：
  工具 Tab → 7天 → 素材 → 最新创意（不点 Top创意 下拉）
- 将返回的 all_creatives 按 ad_key 去重后写入 data/competitor_hot_rank.db 的新表
  competitor_latest_creatives_daily

用法（项目根目录）：

  source .venv/bin/activate
  python scripts/hot_rank_step1_crawl.py

可选参数：
  --date YYYY-MM-DD       指定 crawl_date（默认今天）
  --limit-keywords N      仅使用前 N 个关键词（调试用）
  --debug                 显示浏览器窗口（Playwright debug）
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date, datetime
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

from path_util import CONFIG_DIR
from run_search_workflow import run_batch
from competitor_hot_db import insert_latest_creatives

load_dotenv()


def _parse_date(s: str | None) -> str:
    if not s:
        return date.today().isoformat()
    return datetime.strptime(s, "%Y-%m-%d").date().isoformat()


def _load_competitors_from_config() -> List[Dict[str, Any]]:
    cfg_path = CONFIG_DIR / "ai_product.json"
    if not cfg_path.exists():
        raise FileNotFoundError(f"未找到配置文件: {cfg_path}")
    with open(cfg_path, encoding="utf-8") as f:
        data = json.load(f)
    items: List[Dict[str, Any]] = []
    for category, products in data.items():
        if not isinstance(products, dict):
            continue
        for name, pkg in products.items():
            if not name:
                continue
            items.append(
                {
                    "category": category,
                    "product": name,
                    "android_appid": pkg,
                }
            )
    return items


def _build_items_for_db(
    crawl_date: str,
    competitors: List[Dict[str, Any]],
    search_results: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    根据 run_batch 的结果构建入库 items，并按 ad_key 去重（同一 ad_key 仅保留 heat 最大的一条）。
    """
    dedup: Dict[str, Dict[str, Any]] = {}
    for meta, result in zip(competitors, search_results):
        category = meta["category"]
        product = meta["product"]
        android_appid = meta.get("android_appid")
        creatives = result.get("all_creatives") or []
        for c in creatives:
            ad_key = (
                c.get("ad_key")
                or c.get("creative_id")
                or c.get("id")
                or c.get("creativeId")
                or ""
            )
            if not ad_key:
                continue
            heat = int(c.get("heat") or 0)
            old = dedup.get(ad_key)
            if old is not None:
                old_heat = int(((old.get("creative") or {}).get("heat")) or 0)
                if heat <= old_heat:
                    continue
            dedup[ad_key] = {
                "category": category,
                "product": product,
                "android_appid": android_appid,
                "creative": c,
            }
    return list(dedup.values())


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="热门榜工作流 · 第 1 步：按竞品抓取 Top创意→Top1% 素材并入库。"
    )
    p.add_argument(
        "--date",
        type=str,
        default=None,
        help="crawl_date，格式 YYYY-MM-DD，默认今天。",
    )
    p.add_argument(
        "--limit-keywords",
        type=int,
        default=None,
        help="仅使用前 N 个竞品关键词（调试用）。",
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help="Playwright debug 模式（显示浏览器窗口）。",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    crawl_date = _parse_date(args.date)

    competitors = _load_competitors_from_config()
    if args.limit_keywords is not None:
        competitors = competitors[: max(1, args.limit_keywords)]
    if not competitors:
        print("配置中未解析到任何竞品。")
        return

    print(f"[1/3] crawl_date = {crawl_date}")
    print(
        f"[1/3] 将对 {len(competitors)} 个竞品执行「7天 / 素材 / 最新创意」搜索（使用 appid 作为关键词，不使用 Top创意 过滤）："
    )
    for c in competitors:
        print(f"  - [{c['category']}] {c['product']} (android_appid={c.get('android_appid')})")

    # 重要：使用 android_appid 作为搜索关键词，而不是产品名
    keywords = [c["android_appid"] or "" for c in competitors]

    print("[2/3] 调用搜索工作流拉取按「最新创意」排序的素材（不使用 Top创意 过滤）...")
    search_results = asyncio.run(
        run_batch(
            keywords,
            debug=args.debug,
            is_tool=True,
            order_by="latest",
            use_popularity_top1=False,
        )
    )

    items_for_db = _build_items_for_db(crawl_date, competitors, search_results)
    inserted = insert_latest_creatives(crawl_date, items_for_db)

    print(
        f"[3/3] 已将 {inserted} 条按 ad_key 去重后的「最新创意」素材写入 "
        f"data/competitor_hot_rank.db 的 competitor_latest_creatives_daily 表。"
    )
    print("完成（第 1 步：爬取 + 入库）。")


if __name__ == "__main__":
    main()

