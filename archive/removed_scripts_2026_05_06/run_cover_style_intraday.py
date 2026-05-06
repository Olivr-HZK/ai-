#!/usr/bin/env python3
"""
单独跑「封面风格日内」：多模态抽封面 + 同 app 聚类去重，并写入 insight_cover_style。

不跑灵感分析、不去重后分析等后续步骤。

前置：已有抓取产物 raw JSON（默认 data/workflow_video_enhancer_<DATE>_raw.json）。

用法（项目根目录）：
  .venv/bin/python3 scripts/run_cover_style_intraday.py --date 2026-04-02
  .venv/bin/python3 scripts/run_cover_style_intraday.py --date 2026-04-02 --input data/custom_raw.json

环境：
  与 cover_style_intraday 相同；COVER_STYLE_INTRADAY_ENABLED=0 时本脚本会退出。
  重跑时若当日库内已有 insight_cover_style，会自动跳过（与主流程一致）。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from dotenv import load_dotenv

from path_util import DATA_DIR, PROJECT_ROOT

load_dotenv(PROJECT_ROOT / ".env")

from cover_style_intraday import apply_intraday_cover_style_dedupe, is_cover_style_intraday_enabled  # noqa: E402
from video_enhancer_pipeline_db import init_db as init_pipeline_db, upsert_daily_creative_insights  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="单独跑封面风格日内（不写灵感分析）")
    p.add_argument("--date", required=True, help="target_date，YYYY-MM-DD")
    p.add_argument(
        "--input",
        default="",
        help="raw JSON 路径；默认 data/workflow_video_enhancer_<date>_raw.json",
    )
    p.add_argument(
        "--no-upsert-raw",
        action="store_true",
        help="只写回文件与封面字段入库，不调用 upsert_daily_creative_insights 全量同步 raw",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    target_date = (args.date or "").strip()
    if not target_date:
        raise SystemExit("需要 --date")

    if not is_cover_style_intraday_enabled():
        raise SystemExit(
            "COVER_STYLE_INTRADAY_ENABLED=0，封面日内已关闭；请设为 1 或删除该变量后再跑。"
        )

    raw_path = Path(args.input) if args.input else DATA_DIR / f"workflow_video_enhancer_{target_date}_raw.json"
    if not raw_path.exists():
        raise SystemExit(f"找不到 raw 文件：{raw_path}")

    raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
    items = raw_payload.get("items") or []
    if not isinstance(items, list):
        raise SystemExit("raw JSON 缺少 items 列表")

    init_pipeline_db()
    items2, cover_rep = apply_intraday_cover_style_dedupe(
        items, target_date, raw_payload.get("crawl_date")
    )
    raw_payload["items"] = items2
    raw_payload["cover_style_intraday_report"] = cover_rep

    raw_path.write_text(json.dumps(raw_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[cover-only] 已写回 {raw_path.name}")

    out_rep = DATA_DIR / f"workflow_video_enhancer_{target_date}_cover_style_intraday.json"
    out_rep.write_text(json.dumps(cover_rep, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[cover-only] 报告 {out_rep.name}")

    if not args.no_upsert_raw:
        n = upsert_daily_creative_insights(target_date, raw_payload, {})
        print(f"[cover-only] daily_creative_insights 已同步 raw（条数 {n}）。")
    else:
        print("[cover-only] 已跳过 upsert_daily_creative_insights（--no-upsert-raw）。")

    print(
        f"[cover-only] 完成：{cover_rep.get('input_count', len(items))} → "
        f"{cover_rep.get('output_count', len(items2))} 条"
        + (f"，库内复用封面缓存 {cover_rep.get('cover_style_cache_hits', 0)} 条" if cover_rep.get("cover_style_cache_hits") else "")
    )


if __name__ == "__main__":
    main()
