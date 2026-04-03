"""
从 daily_creative_insights 历史数据回填 creative_library。

执行策略：
- 按 target_date 升序（从旧到新）逐日处理，确保 first_target_date 准确
- 每日数据先做日内去重，再写入 creative_library（与日常流程完全一致）
- 已在 creative_library 的 ad_key 不覆盖，只更新热度和 appearance_count
- 输出每日去重统计和全局汇总

用法：
  python scripts/backfill_creative_library.py
  python scripts/backfill_creative_library.py --dry-run   # 只统计，不写库
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from video_enhancer_pipeline_db import (
    DB_PATH,
    _ahash_hamming,
    _pick_image_url_from_raw,
    _pick_video_url_from_raw,
    _text_fingerprint,
    AHASH_HAMMING_THRESHOLD,
    init_db,
    upsert_creative_library,
)


def load_history_by_date() -> dict[str, list[dict]]:
    """从 daily_creative_insights 按 target_date 分组，返回 {date: [raw_item, ...]}。"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT target_date, category, product, appid,
                   ad_key, platform, video_url, preview_img_url,
                   video_duration, heat, all_exposure_value, impression,
                   raw_json, insight_analysis
            FROM daily_creative_insights
            ORDER BY target_date ASC, id ASC
            """
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    by_date: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        raw_json_str = row["raw_json"] or "{}"
        try:
            creative = json.loads(raw_json_str)
        except Exception:
            creative = {}

        # 确保基础字段存在（兼容旧数据 raw_json 字段不全的情况）
        creative.setdefault("ad_key", row["ad_key"])
        creative.setdefault("platform", row["platform"])
        creative.setdefault("video_url", row["video_url"] or "")
        creative.setdefault("preview_img_url", row["preview_img_url"] or "")
        creative.setdefault("video_duration", row["video_duration"] or 0)
        creative.setdefault("heat", row["heat"] or 0)
        creative.setdefault("all_exposure_value", row["all_exposure_value"] or 0)
        creative.setdefault("impression", row["impression"] or 0)

        item = {
            "category": row["category"],
            "product": row["product"],
            "appid": row["appid"],
            "creative": creative,
        }
        analysis = row["insight_analysis"] or ""
        by_date[row["target_date"]].append((item, analysis))

    return dict(by_date)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="只统计不写库")
    args = parser.parse_args()

    init_db()

    # 检查 creative_library 当前状态
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) as n FROM creative_library")
    existing_count = cur.fetchone()["n"]
    conn.close()

    print(f"creative_library 当前已有 {existing_count} 条记录")
    print()

    history = load_history_by_date()
    dates = sorted(history.keys())
    print(f"daily_creative_insights 共 {sum(len(v) for v in history.values())} 条，"
          f"跨 {len(dates)} 个目标日期：{dates[0]} ~ {dates[-1]}")
    print()

    total_written = 0
    total_grouped = 0

    for date in dates:
        entries = history[date]
        items = [e[0] for e in entries]
        analysis_by_ad = {
            str((e[0].get("creative") or {}).get("ad_key") or ""): e[1]
            for e in entries if e[1] and not e[1].startswith("[ERROR]")
        }

        raw_payload = {
            "target_date": date,
            "items": items,
            "total": len(items),
        }

        if args.dry_run:
            # 只做日内去重统计，不写库
            groups: dict[str, list] = {}
            ahash_idx: list = []
            text_idx: dict = {}
            url_idx: dict = {}
            n_dedup = 0
            for item in items:
                c = item.get("creative") or {}
                ahash = str(c.get("image_ahash_md5") or "").strip()
                fp = _text_fingerprint(str(c.get("title") or ""), str(c.get("body") or ""))
                vurl = _pick_video_url_from_raw(c)
                iurl = _pick_image_url_from_raw(c) if not vurl else ""
                media = vurl or iurl
                matched = False
                if media and media in url_idx:
                    matched = True
                if not matched and ahash:
                    for (h, _) in ahash_idx:
                        if _ahash_hamming(ahash, h) <= AHASH_HAMMING_THRESHOLD:
                            matched = True
                            break
                if not matched and fp and fp in text_idx:
                    matched = True
                if matched:
                    n_dedup += 1
                else:
                    gid = str(len(groups))
                    groups[gid] = []
                    if media:
                        url_idx[media] = gid
                    if ahash:
                        ahash_idx.append((ahash, gid))
                    if fp:
                        text_idx[fp] = gid
            print(f"  [dry] {date}: {len(items)} 条 → 日内去重后 {len(groups)} 条（去除 {n_dedup} 条）")
            continue

        n_written, n_grouped = upsert_creative_library(date, raw_payload, analysis_by_ad)
        total_written += n_written
        total_grouped += n_grouped
        print(f"  {date}: {len(items)} 条原始 → 写入/更新 {n_written} 条，归组 {n_grouped} 条")

    if not args.dry_run:
        # 最终汇总
        conn2 = sqlite3.connect(DB_PATH)
        conn2.row_factory = sqlite3.Row
        cur2 = conn2.cursor()
        cur2.execute("SELECT COUNT(*) as n FROM creative_library")
        final_count = cur2.fetchone()["n"]
        cur2.execute("SELECT COUNT(DISTINCT dedup_group_id) as g FROM creative_library")
        group_count = cur2.fetchone()["g"]
        cur2.execute("SELECT COUNT(*) as n FROM creative_library WHERE appearance_count > 1")
        repeat_count = cur2.fetchone()["n"]
        cur2.execute("""
            SELECT dedup_reason, COUNT(*) as n
            FROM creative_library
            GROUP BY dedup_reason ORDER BY n DESC
        """)
        reason_rows = cur2.fetchall()
        conn2.close()

        print()
        print("=" * 55)
        print(f"  回填完成！")
        print(f"  creative_library 总记录: {final_count} 条")
        print(f"  唯一去重组:              {group_count} 组")
        print(f"  跨天重复出现的素材:      {repeat_count} 条")
        print(f"  去重原因分布:")
        for r in reason_rows:
            print(f"    {r['dedup_reason']:<20} {r['n']:>4} 条")
        print("=" * 55)


if __name__ == "__main__":
    main()
