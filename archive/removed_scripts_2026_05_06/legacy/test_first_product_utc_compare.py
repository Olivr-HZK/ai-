"""
单产品时间口径对比工具：

1. 从 config/ai_product.json 里取第一个产品（第一个分类下的第一项）
2. 用它的 appid 作为搜索关键词，在广大大「工具→7天→素材→最新创意」抓 7 天窗口内全部素材
3. 对每条素材取 first_seen（没有则用 created_at）：
   - 按 UTC+0 转成日期，筛出 TARGET_DAY 的，写入 data/test_first_product_<product>_<DAY>_utc0.json
   - 按 UTC+8 转成日期，筛出 TARGET_DAY 的，写入 data/test_first_product_<product>_<DAY>_utc8.json

用法（项目根目录）：
  DEBUG=1 .venv/bin/python scripts/test_first_product_utc_compare.py --day 2026-03-17
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from path_util import CONFIG_DIR, DATA_DIR
from run_search_workflow import run_batch


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="单产品 UTC+0 / UTC+8 日期口径对比（只测第一个产品）")
    p.add_argument(
        "--day",
        default=None,
        metavar="YYYY-MM-DD",
        help="要对比的日期（默认等于今天减 1 天）",
    )
    args = p.parse_args()
    if args.day:
        args.target_day = args.day
    else:
        today = datetime.now().date()
        args.target_day = (today - timedelta(days=1)).isoformat()
    return args


def _pick_first_product() -> tuple[str, str, str]:
    """返回 (category, product, appid)。"""
    cfg_path = CONFIG_DIR / "ai_product.json"
    data = json.load(cfg_path.open("r", encoding="utf-8"))
    if not isinstance(data, dict) or not data:
        raise RuntimeError("ai_product.json 为空或格式不对")
    first_cat = next(iter(data.keys()))
    items = data[first_cat]
    if not isinstance(items, dict) or not items:
        raise RuntimeError(f"分类 {first_cat} 下无产品")
    first_product, first_appid = next(iter(items.items()))
    return str(first_cat), str(first_product), str(first_appid)


def _pick_ts(c: dict[str, Any]) -> int | None:
    for k in ("first_seen", "created_at"):
        v = c.get(k)
        if v is None:
            continue
        try:
            return int(v)
        except Exception:
            continue
    return None


def _day_utc(ts: int, offset_hours: int) -> str:
    tz = timezone(timedelta(hours=offset_hours))
    return datetime.fromtimestamp(int(ts), tz=tz).strftime("%Y-%m-%d")


async def main() -> None:
    args = parse_args()
    target_day: str = args.target_day
    cat, product, appid = _pick_first_product()
    keyword = appid  # 用 appid 做搜索关键词

    print(
        f"[单测] category={cat}, product={product}, appid={appid}, "
        f"target_day={target_day} (对比 UTC+0 / UTC+8)"
    )

    results = await run_batch(
        keywords=[keyword],
        debug=True,          # 默认开界面，方便你观察流程
        is_tool=True,
        order_by="latest",
        use_popularity_top1=False,
    )
    r = results[0] if results else {}
    all_creatives = r.get("all_creatives") or []
    if not isinstance(all_creatives, list):
        all_creatives = []
    print(f"[结果] 7天窗口抓到条数: {len(all_creatives)}")

    utc0_list: list[dict[str, Any]] = []
    utc8_list: list[dict[str, Any]] = []

    for c in all_creatives:
        if not isinstance(c, dict):
            continue
        ts = _pick_ts(c)
        if ts is None:
            continue
        if _day_utc(ts, 0) == target_day:
            utc0_list.append(c)
        if _day_utc(ts, 8) == target_day:
            utc8_list.append(c)

    print(f"[筛选] UTC+0 命中 {len(utc0_list)} 条, UTC+8 命中 {len(utc8_list)} 条")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    safe_product = product.replace(" ", "_").replace("/", "_")
    out0 = DATA_DIR / f"test_first_product_{safe_product}_{target_day}_utc0.json"
    out8 = DATA_DIR / f"test_first_product_{safe_product}_{target_day}_utc8.json"

    meta = {
        "category": cat,
        "product": product,
        "appid": appid,
        "target_day": target_day,
        "total_captured_7d": len(all_creatives),
    }

    with out0.open("w", encoding="utf-8") as f:
        json.dump(
            {**meta, "timezone": "UTC+0", "count": len(utc0_list), "creatives": utc0_list},
            f,
            ensure_ascii=False,
            indent=2,
        )
    with out8.open("w", encoding="utf-8") as f:
        json.dump(
            {**meta, "timezone": "UTC+8", "count": len(utc8_list), "creatives": utc8_list},
            f,
            ensure_ascii=False,
            indent=2,
        )

    print(f"[写入] {out0.name}, {out8.name}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())

