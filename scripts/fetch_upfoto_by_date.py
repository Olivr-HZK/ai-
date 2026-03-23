"""
抓取 UpFoto 7天窗口素材，按 first_seen UTC+8 筛指定日期，不过滤重投。

用法：
  python scripts/fetch_upfoto_by_date.py
  python scripts/fetch_upfoto_by_date.py --date 2026-03-18
  DEBUG=1 python scripts/fetch_upfoto_by_date.py --date 2026-03-18
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from path_util import DATA_DIR
from run_search_workflow import run_batch

UPFOTO_APPID = "ai.photo.enhancer.photoclear"
UPFOTO_PRODUCT = "UpFoto - AI Photo Enhancer"


def _ts_to_utc8(ts) -> str:
    try:
        tz8 = timezone(timedelta(hours=8))
        return datetime.fromtimestamp(int(ts), tz=tz8).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def _ts_to_utc8_day(ts) -> str | None:
    try:
        tz8 = timezone(timedelta(hours=8))
        return datetime.fromtimestamp(int(ts), tz=tz8).strftime("%Y-%m-%d")
    except Exception:
        return None


def _pick_video_url(c: dict) -> str:
    if c.get("video_url"):
        return str(c["video_url"])
    for r in c.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("video_url"):
            return str(r["video_url"])
    return ""


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        default=(date.today() - timedelta(days=1)).isoformat(),
        help="目标日期 YYYY-MM-DD（默认昨天）",
    )
    args = parser.parse_args()
    target_date = args.date

    print(f"[抓取] 产品={UPFOTO_PRODUCT}  appid={UPFOTO_APPID}")
    print(f"[目标] first_seen UTC+8 = {target_date}（不过滤重投）")

    results = await run_batch(
        keywords=[UPFOTO_APPID],
        debug=bool(os.environ.get("DEBUG")),
        is_tool=True,
        order_by="latest",
        use_popularity_top1=False,
    )

    all_creatives: list[dict] = []
    for r in results:
        raw = r.get("all_creatives") or []
        all_creatives.extend(c for c in raw if isinstance(c, dict))

    print(f"[抓取] 原始总数={len(all_creatives)}")

    # 只按 first_seen 日期过滤，不过滤重投
    matched: list[dict] = []
    skip_wrong_day = 0
    skip_no_ts = 0
    for c in all_creatives:
        fs = c.get("first_seen")
        if not fs:
            skip_no_ts += 1
            continue
        if _ts_to_utc8_day(fs) == target_date:
            matched.append(c)
        else:
            skip_wrong_day += 1

    print(f"[过滤] 命中={len(matched)}  wrong_day={skip_wrong_day}  no_first_seen={skip_no_ts}")

    print(f"\n{'='*70}")
    for i, c in enumerate(matched, 1):
        first_seen = _ts_to_utc8(c.get("first_seen"))
        created_at = _ts_to_utc8(c.get("created_at")) if c.get("created_at") else "-"
        resume = c.get("resume_advertising_flag", False)
        video_url = _pick_video_url(c)
        print(
            f"[{i:02d}] first_seen={first_seen}  created_at={created_at}  "
            f"重投={resume}  热度={c.get('heat', 0)}  估值={c.get('all_exposure_value', 0)}"
        )
        if video_url:
            print(f"      video: {video_url[:100]}")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATA_DIR / f"upfoto_{target_date}_raw.json"
    out_path.write_text(
        json.dumps(
            {"target_date": target_date, "product": UPFOTO_PRODUCT,
             "appid": UPFOTO_APPID, "total": len(matched), "creatives": matched},
            ensure_ascii=False, indent=2,
        ),
        encoding="utf-8",
    )
    print(f"\n[输出] {out_path.name}（{len(matched)} 条）")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("中断。", file=sys.stderr)
