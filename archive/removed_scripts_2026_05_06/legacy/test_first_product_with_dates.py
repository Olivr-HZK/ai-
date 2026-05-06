"""
单产品时间字段展开工具（不筛选日期）：

1. 从 config/ai_product.json 里取第一个产品（第一个分类下的第一项）
2. 用它的 appid 作为搜索关键词，在广大大「工具→7天→素材→最新创意」抓 7 天窗口内全部素材
3. 对每条素材取 first_seen（没有则用 created_at），计算：
   - day_utc0: 按 UTC+0 的日期 YYYY-MM-DD
   - day_utc8: 按 UTC+8（北京时间）的日期 YYYY-MM-DD
4. 原始 creative 不做筛选，全部写入一个 JSON，方便你自己查验时间逻辑

用法（项目根目录）：
  DEBUG=1 .venv/bin/python scripts/test_first_product_with_dates.py
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from path_util import CONFIG_DIR, DATA_DIR
from run_search_workflow import run_batch


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


def _day_from_ts(ts: int, offset_hours: int) -> str:
    tz = timezone(timedelta(hours=offset_hours))
    return datetime.fromtimestamp(int(ts), tz=tz).strftime("%Y-%m-%d")


async def main() -> None:
    cat, product, appid = _pick_first_product()
    keyword = appid  # 用 appid 做搜索关键词

    print(f"[单测] category={cat}, product={product}, appid={appid}（展开 UTC+0 / UTC+8 日期，不做筛选）")

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

    utcz = timezone.utc
    tz8 = timezone(timedelta(hours=8))

    enriched: list[dict[str, Any]] = []
    for c in all_creatives:
        if not isinstance(c, dict):
            continue
        ts = _pick_ts(c)
        day_utc0 = day_utc8 = None
        if ts is not None:
            day_utc0 = datetime.fromtimestamp(int(ts), tz=utcz).strftime("%Y-%m-%d")
            day_utc8 = datetime.fromtimestamp(int(ts), tz=tz8).strftime("%Y-%m-%d")
        enriched.append(
            {
                "ts": ts,
                "day_utc0": day_utc0,
                "day_utc8": day_utc8,
                "creative": c,
            }
        )

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    safe_product = product.replace(" ", "_").replace("/", "_")
    out = DATA_DIR / f"test_first_product_{safe_product}_with_dates.json"

    meta = {
        "category": cat,
        "product": product,
        "appid": appid,
        "total_captured_7d": len(all_creatives),
    }

    with out.open("w", encoding="utf-8") as f:
        json.dump(
            {**meta, "items": enriched},
            f,
            ensure_ascii=False,
            indent=2,
        )

    print(f"[写入] {out.name}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())

