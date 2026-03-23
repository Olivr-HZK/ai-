"""
抓取 Remini 7天窗口内的全部素材，不做任何筛选，直接输出 raw JSON。

用法：
  python scripts/fetch_remini_raw.py
  DEBUG=1 python scripts/fetch_remini_raw.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from path_util import DATA_DIR
from run_search_workflow import run_batch

REMINI_APPID = "com.bigwinepot.nwdn.international"
REMINI_PRODUCT = "Remini - AI Photo Enhancer"


def _ts_to_utc8(ts) -> str:
    try:
        tz8 = timezone(timedelta(hours=8))
        return datetime.fromtimestamp(int(ts), tz=tz8).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


async def main() -> None:
    print(f"[抓取] 产品={REMINI_PRODUCT}  appid={REMINI_APPID}")
    print("[抓取] 不做任何筛选，返回 7 天窗口内全部素材")

    results = await run_batch(
        keywords=[REMINI_APPID],
        debug=bool(os.environ.get("DEBUG")),
        is_tool=True,
        order_by="latest",
        use_popularity_top1=False,
    )

    all_creatives: list[dict] = []
    for r in results:
        raw = r.get("all_creatives") or []
        if isinstance(raw, list):
            all_creatives.extend(c for c in raw if isinstance(c, dict))

    print(f"[结果] 共抓取 {len(all_creatives)} 条素材")

    # 打印每条摘要
    print(f"\n{'='*70}")
    for i, c in enumerate(all_creatives, 1):
        first_seen = _ts_to_utc8(c.get("first_seen")) if c.get("first_seen") else "-"
        created_at = _ts_to_utc8(c.get("created_at")) if c.get("created_at") else "-"
        resume = c.get("resume_advertising_flag", False)
        print(
            f"[{i:03d}] "
            f"first_seen={first_seen}  created_at={created_at}  "
            f"重投={resume}  "
            f"热度={c.get('heat', 0)}  估值={c.get('all_exposure_value', 0)}"
        )

    # 保存
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(tz=timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
    out_path = DATA_DIR / f"remini_raw_crawl_{today}.json"
    payload = {
        "crawl_date": today,
        "product": REMINI_PRODUCT,
        "appid": REMINI_APPID,
        "total": len(all_creatives),
        "creatives": all_creatives,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[输出] {out_path.name}（{len(all_creatives)} 条）")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("中断。", file=sys.stderr)
