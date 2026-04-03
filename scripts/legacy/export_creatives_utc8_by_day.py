"""
全量导出：所有竞品在某一天（按 UTC+8 计算）的素材关键信息。

逻辑：
1. 从 config/ai_product.json 读取全部竞品（category, product, appid）
2. 用 appid 作为搜索关键词，调用 run_search_workflow.run_batch：
   - 工具 → 7天 → 素材 → 最新创意
   - 获取 7 天窗口内 all_creatives（含滚动加载）
3. 对每条素材取 first_seen（没有则用 created_at），按 UTC+8 计算日期：
   - 若等于目标日期（--day），则提取关键信息：
     idx, day_utc8, heat, all_exposure_value, new_week_exposure_value,
     video_duration, title, media_type（视频/图片）
4. 将所有命中的素材写入一个 JSON 文件：
   data/export_creatives_utc8_<DAY>.json

用法（项目根目录）：
  .venv/bin/python scripts/export_creatives_utc8_by_day.py --day 2026-03-17
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from path_util import CONFIG_DIR, DATA_DIR
from run_search_workflow import run_batch


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="全量导出某天(UTC+8)素材关键信息")
    p.add_argument(
        "--day",
        required=True,
        metavar="YYYY-MM-DD",
        help="目标日期（按 UTC+8：北京时间 00:00~24:00 的那一天）",
    )
    return p.parse_args()


def load_competitors() -> List[Tuple[str, str, str]]:
    """
    返回所有 (category, product, appid) 列表。
    """
    cfg_path = CONFIG_DIR / "ai_product.json"
    data = json.load(cfg_path.open("r", encoding="utf-8"))
    result: List[Tuple[str, str, str]] = []
    if isinstance(data, dict):
        for cat, items in data.items():
            if not isinstance(items, dict):
                continue
            for product, appid in items.items():
                if not product:
                    continue
                result.append((str(cat), str(product), str(appid or "")))
    return result


def pick_ts(c: Dict[str, Any]) -> int | None:
    for k in ("first_seen", "created_at"):
        v = c.get(k)
        if v is None:
            continue
        try:
            return int(v)
        except Exception:
            continue
    return None


def day_utc8(ts: int) -> str:
    tz = timezone(timedelta(hours=8))
    return datetime.fromtimestamp(int(ts), tz=tz).strftime("%Y-%m-%d")


async def main() -> None:
    args = parse_args()
    target_day: str = args.day

    competitors = load_competitors()
    if not competitors:
        print("[终止] ai_product.json 中没有竞品配置")
        return

    # 用 appid 作为关键词
    keywords = [appid for _, _, appid in competitors if appid]
    print(f"[1] 共有 {len(competitors)} 个竞品，将按 appid 搜索 {len(keywords)} 个关键词，目标日期(UTC+8)={target_day}")

    results = await run_batch(
        keywords=keywords,
        debug=False,          # 不开浏览器，全量跑
        is_tool=True,
        order_by="latest",
        use_popularity_top1=False,
    )

    # 建 appid -> (category, product) 映射，方便附加信息
    meta_by_appid: Dict[str, Tuple[str, str]] = {appid: (cat, prod) for cat, prod, appid in competitors}

    tz8 = timezone(timedelta(hours=8))
    # 分产品聚合：key 使用 “<product> (<appid>)” 方便你查找
    by_product: Dict[str, List[Dict[str, Any]]] = {}

    for r in results:
        kw = str(r.get("keyword") or "")
        all_creatives = r.get("all_creatives") or []
        if not isinstance(all_creatives, list) or not all_creatives:
            continue
        cat, prod = meta_by_appid.get(kw, ("", ""))

        for c in all_creatives:
            if not isinstance(c, dict):
                continue
            ts = pick_ts(c)
            if ts is None:
                continue
            if day_utc8(ts) != target_day:
                continue

            heat = c.get("heat")
            all_exp = c.get("all_exposure_value")
            new_week = c.get("new_week_exposure_value")
            vd = c.get("video_duration")
            title = c.get("title") or ""
            res = c.get("resource_urls") or []
            is_video = any(isinstance(r0, dict) and r0.get("video_url") for r0 in res)
            media_type = "视频" if is_video else "图片"

            key = f"{prod or kw} ({kw})"
            by_product.setdefault(key, []).append(
                {
                    # 你要求的关键信息格式（加上 ad_key 方便你回溯页面）
                    "ad_key": c.get("ad_key"),
                    "day_utc8": datetime.fromtimestamp(ts, tz=tz8).strftime("%Y-%m-%d"),
                    "heat": heat,
                    "all_exposure_value": all_exp,
                    "new_week_exposure_value": new_week,
                    "video_duration": vd,
                    "title": title,
                    "media_type": media_type,
                }
            )

    # 为每个产品内部加 idx 序号
    total = 0
    for key, lst in by_product.items():
        for i, item in enumerate(lst, start=1):
            item["idx"] = i
        total += len(lst)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATA_DIR / f"export_creatives_utc8_{target_day}.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "target_day_utc8": target_day,
                "total": total,
                "by_product": by_product,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    print(f"[2] 已导出 {len(exported)} 条素材到 {out_path}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())

