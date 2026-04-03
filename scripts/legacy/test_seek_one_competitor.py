"""
测试：从 config/ai_product.json 的 seek 分类里随机选一个竞品，
抓 7 天窗口素材，按目标日期筛选（默认 2026-03-19），输出 raw JSON。

筛选逻辑与 video enhancer 主工作流完全一致：
  广告主匹配 → first_seen UTC+8 命中目标日 → 重投无 top 则过滤

用法：
  python scripts/test_seek_one_competitor.py
  python scripts/test_seek_one_competitor.py --date 2026-03-19
  python scripts/test_seek_one_competitor.py --date 2026-03-19 --product "Perplexity - Perplexity - Ask Anything"
  DEBUG=1 python scripts/test_seek_one_competitor.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from path_util import CONFIG_DIR, DATA_DIR
from run_search_workflow import run_batch
from workflow_guangdada_competitor_yesterday_creatives import (
    _apply_relaunch_pipeline_tag,
    _creative_hits_target_date,
    _exposure_top_has_any,
    _is_resume_advertising,
    advertiser_matches_product,
)

CONFIG_FILE = CONFIG_DIR / "ai_product.json"
CATEGORY = "seek"


@dataclass(frozen=True)
class Competitor:
    category: str
    product: str
    appid: str


def _load_seek_competitors() -> list[Competitor]:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"未找到配置：{CONFIG_FILE}")
    data = json.load(CONFIG_FILE.open("r", encoding="utf-8"))
    result: list[Competitor] = []
    cat_items = data.get(CATEGORY)
    if not isinstance(cat_items, dict):
        raise ValueError(f"config/ai_product.json 里找不到 '{CATEGORY}' 分类")
    for product, appid in cat_items.items():
        if product and str(appid or "").strip():
            result.append(Competitor(category=CATEGORY, product=str(product), appid=str(appid)))
    return result


def _ts_to_datetime_utc8(ts: int) -> str:
    tz8 = timezone(timedelta(hours=8))
    return datetime.fromtimestamp(int(ts), tz=tz8).strftime("%Y-%m-%d %H:%M:%S")


def _pick_media_link(creative: dict) -> str:
    for r in creative.get("resource_urls") or []:
        if not isinstance(r, dict):
            continue
        if r.get("video_url"):
            return str(r["video_url"])
        if r.get("image_url"):
            return str(r["image_url"])
    if creative.get("video_url"):
        return str(creative["video_url"])
    return ""


def _reduce_creative(creative: dict) -> dict:
    ts = creative.get("first_seen") or creative.get("created_at")
    time_utc8 = _ts_to_datetime_utc8(int(ts)) if ts else ""
    tags = creative.get("pipeline_tags")
    return {
        "人气值": creative.get("impression") or 0,
        "展示估值": creative.get("all_exposure_value") or 0,
        "热度": creative.get("heat") or 0,
        "视频长度": creative.get("video_duration") or 0,
        "素材链接": _pick_media_link(creative),
        "first_seen_utc8": time_utc8,
        "广告主": creative.get("advertiser_name") or creative.get("page_name") or "",
        "标签": list(tags) if isinstance(tags, list) else [],
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="seek 竞品单品测试：随机选一个，抓目标日期的素材")
    parser.add_argument(
        "--date",
        default="2026-03-19",
        metavar="YYYY-MM-DD",
        help="目标日期（UTC+8，按 first_seen 命中），默认 2026-03-19",
    )
    parser.add_argument(
        "--product",
        default="",
        help="可选：指定产品名（需与 config/ai_product.json 完全一致）；不填则随机选一个",
    )
    args = parser.parse_args()
    target_date = args.date

    all_competitors = _load_seek_competitors()
    if not all_competitors:
        print(f"[终止] seek 分类下无可用竞品", file=sys.stderr)
        return

    # 选竞品
    if args.product.strip():
        wanted = args.product.strip().lower()
        matches = [c for c in all_competitors if c.product.strip().lower() == wanted]
        if not matches:
            print(f"[终止] 找不到产品 '{args.product}'，可用列表：{[c.product for c in all_competitors]}", file=sys.stderr)
            return
        comp = matches[0]
        print(f"[指定竞品] {comp.product}  appid={comp.appid}")
    else:
        comp = random.choice(all_competitors)
        print(f"[随机竞品] {comp.product}  appid={comp.appid}")
        print(f"[全部可选] {[c.product for c in all_competitors]}")

    print(f"[目标日期] {target_date}（仅 first_seen UTC+8 命中才保留）")

    # 爬取
    results = await run_batch(
        keywords=[comp.appid],
        debug=bool(os.environ.get("DEBUG")),
        is_tool=True,
        order_by="latest",
        use_popularity_top1=False,
    )

    all_creatives: list[dict[str, Any]] = []
    for r in results:
        raw = r.get("all_creatives") or []
        if isinstance(raw, list):
            all_creatives.extend(raw)

    print(f"[抓取] 原始素材总数={len(all_creatives)}")

    # 广告主过滤
    after_adv = [
        c for c in all_creatives
        if isinstance(c, dict)
        and advertiser_matches_product(
            str(c.get("advertiser_name") or c.get("page_name") or ""), comp.product
        )
    ]
    print(f"[过滤] 广告主匹配后={len(after_adv)}")

    # 日期命中 + 重投无 top 过滤
    candidates: list[dict[str, Any]] = []
    skip_wrong_day = 0
    skip_no_first_seen = 0
    skip_relaunch_no_top = 0
    for c in after_adv:
        hit, reason = _creative_hits_target_date(c, target_date)
        if not hit:
            if reason == "no_first_seen":
                skip_no_first_seen += 1
            else:
                skip_wrong_day += 1
            continue
        if _is_resume_advertising(c) and not _exposure_top_has_any(c):
            skip_relaunch_no_top += 1
            continue
        candidates.append(c)

    print(
        f"[过滤] 日期命中={len(candidates)}  "
        f"（跳过 wrong_day={skip_wrong_day}, no_first_seen={skip_no_first_seen}, "
        f"relaunch_no_top={skip_relaunch_no_top}）"
    )

    # 写 pipeline_tags
    for c in candidates:
        _apply_relaunch_pipeline_tag(c)

    # 打印摘要
    print(f"\n{'='*60}")
    print(f"产品：{comp.product}  |  目标日：{target_date}  |  命中 {len(candidates)} 条")
    print(f"{'='*60}")
    for i, c in enumerate(candidates, 1):
        r = _reduce_creative(c)
        tags_str = "、".join(r["标签"]) if r["标签"] else "-"
        print(
            f"[{i:02d}] {r['广告主'][:30]:<30}  "
            f"热度={r['热度']}  估值={r['展示估值']}  人气={r['人气值']}  "
            f"时长={r['视频长度']}s  标签={tags_str}"
        )
        if r["素材链接"]:
            print(f"      素材: {r['素材链接'][:100]}")
        print(f"      first_seen: {r['first_seen_utc8']}")

    # 保存 raw JSON
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    safe_product = comp.product.replace("/", "_").replace(" ", "_")[:30]
    out_path = DATA_DIR / f"test_seek_{safe_product}_{target_date}_raw.json"
    payload = {
        "target_date": target_date,
        "competitor": {"product": comp.product, "appid": comp.appid, "category": comp.category},
        "total": len(candidates),
        "items": [
            {
                "category": comp.category,
                "product": comp.product,
                "appid": comp.appid,
                "creative": c,
            }
            for c in candidates
        ],
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[输出] {out_path.name}（{len(candidates)} 条）")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("中断。", file=sys.stderr)
