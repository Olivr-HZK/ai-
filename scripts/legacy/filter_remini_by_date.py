"""
从已抓取的 remini_raw_crawl_<日期>.json 里，
按 first_seen UTC+8 筛选指定日期的素材，输出摘要并保存结果。

用法：
  python scripts/filter_remini_by_date.py
  python scripts/filter_remini_by_date.py --date 2026-03-19
  python scripts/filter_remini_by_date.py --input data/remini_raw_crawl_2026-03-20.json --date 2026-03-19
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from path_util import DATA_DIR


def _ts_to_utc8_day(ts) -> str | None:
    try:
        tz8 = timezone(timedelta(hours=8))
        return datetime.fromtimestamp(int(ts), tz=tz8).strftime("%Y-%m-%d")
    except Exception:
        return None


def _ts_to_utc8(ts) -> str:
    try:
        tz8 = timezone(timedelta(hours=8))
        return datetime.fromtimestamp(int(ts), tz=tz8).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=(date.today() - timedelta(days=1)).isoformat(),
                        help="目标日期 YYYY-MM-DD（默认昨天）")
    parser.add_argument("--input", default="", help="输入 JSON 路径（默认自动找最新的 remini_raw_crawl_*.json）")
    args = parser.parse_args()

    target_date = args.date

    # 找输入文件
    if args.input:
        in_path = Path(args.input)
    else:
        candidates = sorted(DATA_DIR.glob("remini_raw_crawl_*.json"), reverse=True)
        if not candidates:
            print("[错误] 找不到 remini_raw_crawl_*.json，请先跑 fetch_remini_raw.py")
            return
        in_path = candidates[0]

    print(f"[输入] {in_path.name}")
    payload = json.loads(in_path.read_text(encoding="utf-8"))
    all_creatives: list[dict] = payload.get("creatives") or []
    print(f"[总量] {len(all_creatives)} 条  →  筛选 first_seen UTC+8 = {target_date}")

    # 按 first_seen 过滤
    matched: list[dict] = []
    skip_no_ts = 0
    skip_wrong_day = 0
    for c in all_creatives:
        fs = c.get("first_seen")
        if not fs:
            skip_no_ts += 1
            continue
        day = _ts_to_utc8_day(fs)
        if day == target_date:
            matched.append(c)
        else:
            skip_wrong_day += 1

    print(f"[过滤] 命中={len(matched)}  wrong_day={skip_wrong_day}  no_first_seen={skip_no_ts}")

    # 打印摘要
    print(f"\n{'='*70}")
    for i, c in enumerate(matched, 1):
        first_seen = _ts_to_utc8(c.get("first_seen"))
        created_at = _ts_to_utc8(c.get("created_at")) if c.get("created_at") else "-"
        resume = c.get("resume_advertising_flag", False)
        video_url = c.get("video_url") or ""
        if not video_url:
            for r in c.get("resource_urls") or []:
                if isinstance(r, dict) and r.get("video_url"):
                    video_url = r["video_url"]
                    break
        print(
            f"[{i:03d}] first_seen={first_seen}  created_at={created_at}  "
            f"重投={resume}  热度={c.get('heat', 0)}  估值={c.get('all_exposure_value', 0)}"
        )
        if video_url:
            print(f"      video: {video_url[:100]}")

    # 保存
    out_path = DATA_DIR / f"remini_{target_date}_filtered.json"
    out_payload = {
        "source": in_path.name,
        "target_date": target_date,
        "total": len(matched),
        "creatives": matched,
    }
    out_path.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[输出] {out_path.name}（{len(matched)} 条）")


if __name__ == "__main__":
    main()
