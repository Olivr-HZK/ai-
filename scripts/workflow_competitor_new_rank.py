"""
竞品「新晋榜」工作流：

- 目标：每天固定时间（建议 8:00）抓取「近 24 小时当前竞品搜索的所有视频」。
- 实现方式：暂时沿用「7天 / 素材 / 最新创意」排序的搜索结果，完整保留原始 JSON，
  后续可以基于接口字段再做精确的 24h 窗口过滤。

数据落地：
- 原始结果表（必填）：competitor_new_raw_daily
  - 每条素材一行，raw_json 原样保留
- 去重结果表（可选）：competitor_new_creatives_daily
  - 结构与热门榜表一致，用于后续新晋榜报表 / 看板
  - 本脚本通过 --with-dedup 开关控制是否写入；第一天只需要 raw 表时，不加该参数即可。

用法（在项目根目录，已配置 .env 的广大大账号）：

  # 仅写入原始结果表（推荐作为「第一天」测试命令）
  python scripts/workflow_competitor_new_rank.py --limit-keywords 5 --debug

  # 同时写入原始表 + 去重表
  python scripts/workflow_competitor_new_rank.py --limit-keywords 5 --with-dedup --debug
"""

from __future__ import annotations

import argparse
import asyncio
import json
import datetime as dt
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

from path_util import CONFIG_DIR
from run_search_workflow import run_batch
from competitor_hot_db import (
    insert_new_raw_creatives,
    insert_new_dedup_creatives,
)

load_dotenv()


def _parse_date(s: str | None) -> dt.date:
    if not s:
        return dt.date.today()
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def _load_competitors_from_config() -> List[Dict[str, Any]]:
    """
    从 config/ai_product.json 解析竞品列表。
    返回元素形如：
      {
        "category": "seek",
        "product": "AI Chatbot - Nova",
        "android_appid": "com.scaleup.chatai",
      }
    """
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


def _build_dedup_items_for_db(
    crawl_date: str,
    competitors: List[Dict[str, Any]],
    search_results: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    根据 run_batch 的结果构建「新晋榜去重结果」入库 items。
    规则：按 ad_key 去重，同一 ad_key 仅保留 heat 最大的一条。
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


def _build_raw_items_for_db(
    crawl_date: str,
    competitors: List[Dict[str, Any]],
    search_results: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    构建原始结果入库 items：
    - 每个竞品 / 关键词只对应一条记录
    - raw 字段直接保存 run_batch 的 result（包含 all_creatives 等）
    """
    items: List[Dict[str, Any]] = []
    for meta, result in zip(competitors, search_results):
        category = meta["category"]
        product = meta["product"]
        android_appid = meta.get("android_appid")
        items.append(
            {
                "category": category,
                "product": product,
                "android_appid": android_appid,
                "raw": result,
            }
        )
    return items


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="竞品「新晋榜」：抓取近 24 小时当前竞品搜索的所有视频，并写入原始 / 去重结果表。",
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
    p.add_argument(
        "--with-dedup",
        action="store_true",
        help="是否在写入原始表的同时写入去重表（第一天可先不加，只看 raw 表）。",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    crawl_date = _parse_date(args.date).isoformat()

    competitors = _load_competitors_from_config()
    if args.limit_keywords is not None:
        competitors = competitors[: max(1, args.limit_keywords)]
    if not competitors:
        print("配置中未解析到任何竞品。")
        return

    print(f"[1/3] crawl_date = {crawl_date}")
    print(
        f"[1/3] 将对 {len(competitors)} 个竞品执行「7天 / 素材 / 最新创意」搜索（用于新晋榜原始结果抓取）："
    )
    for c in competitors:
        print(f"  - [{c['category']}] {c['product']} (android_appid={c.get('android_appid')})")

    keywords = [c["product"] for c in competitors]

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

    # 1) 写入原始结果表（必做）
    raw_items = _build_raw_items_for_db(crawl_date, competitors, search_results)
    raw_inserted = insert_new_raw_creatives(crawl_date, raw_items)
    print(
        f"[2/3] 已将 {raw_inserted} 条原始素材写入 data/competitor_hot_rank.db 的 competitor_new_raw_daily 表。"
    )

    # 2) 可选：写入去重结果表（字段结构与热门榜一致）
    if args.with_dedup:
        dedup_items = _build_dedup_items_for_db(crawl_date, competitors, search_results)
        dedup_inserted = insert_new_dedup_creatives(crawl_date, dedup_items)
        print(
            f"[3/3] 已将 {dedup_inserted} 条按 ad_key 去重后的新晋素材写入 "
            f"data/competitor_hot_rank.db 的 competitor_new_creatives_daily 表。"
        )
    else:
        print(
            "[3/3] 本次未写入去重表（未传 --with-dedup）。如需生成新晋榜去重结果，可下次运行时加上该参数。"
        )

    print("完成（新晋榜：爬取 + 入库）。")


if __name__ == "__main__":
    main()

