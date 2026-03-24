"""
测试：跑 video enhancer 类下的竞品（仅排除 Hula），抓当前 7 天窗口内素材，不入库，写两个 JSON。
可按日期筛选 target_date；若某个产品在该日命中素材超过 10 条，则仅保留「按热度 heat 降序的前 10 条」。

- 原始 JSON：按产品分组的完整创意列表（raw creative 对象）
- 提炼 JSON：人气值、展示估值、热度、视频长度、素材链接、投放时间(UTC+8)

用法（项目根目录，或 cd scripts 后执行）：
  DEBUG=1 python scripts/test_video_enhancer_two_competitors_318.py
  python scripts/test_video_enhancer_two_competitors_318.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
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

# 排除的竞品名称（含其一即排除）——仅排除 Hula
EXCLUDE_NAMES = ("Hula", "hula")


@dataclass(frozen=True)
class Competitor:
    category: str
    product: str
    appid: str


def _load_video_enhancer_competitors(products: list[str] | None = None):
    """
    从 ai_product.json 读取 video enhancer 分类，排除 Hula。
    - 若指定 products，则只保留这些产品名；
    - 否则返回该分类下全部（除 Hula）。
    """
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"未找到配置：{CONFIG_FILE}")
    data = json.load(CONFIG_FILE.open("r", encoding="utf-8"))
    candidates: list[Competitor] = []
    cat = "video enhancer"
    if isinstance(data, dict) and isinstance(data.get(cat), dict):
        for product, appid in data[cat].items():
            if not product or not (str(appid or "").strip()):
                continue
            if any(ex in product for ex in EXCLUDE_NAMES):
                continue
            candidates.append(Competitor(category=cat, product=str(product), appid=str(appid)))
    if products:
        wanted = {p.strip().lower() for p in products if p and p.strip()}
        selected = [c for c in candidates if c.product.strip().lower() in wanted]
        return selected
    return candidates


def _creative_online_ts(c: dict[str, Any]) -> int | None:
    ts = c.get("created_at") or c.get("first_seen")
    if ts is None:
        return None
    try:
        return int(ts)
    except Exception:
        return None


def _ts_to_datetime_utc8(ts: int) -> str:
    tz8 = timezone(timedelta(hours=8))
    return datetime.fromtimestamp(int(ts), tz=tz8).strftime("%Y-%m-%d %H:%M:%S")


def _pick_media_link(creative: dict) -> str:
    """素材链接：优先视频 URL，否则图片 URL。"""
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
    """单条创意提炼：人气值、展示估值、热度、视频长度、素材链接、投放时间_utc8、标签。"""
    ts = _creative_online_ts(creative)
    time_utc8 = _ts_to_datetime_utc8(ts) if ts else ""
    tags = creative.get("pipeline_tags")
    tag_list = list(tags) if isinstance(tags, list) else []
    return {
        "人气值": creative.get("impression") or 0,
        "展示估值": creative.get("all_exposure_value") or 0,
        "热度": creative.get("heat") or 0,
        "视频长度": creative.get("video_duration") or 0,
        "素材链接": _pick_media_link(creative),
        "投放时间_utc8": time_utc8,
        "标签": tag_list,
    }


async def main():
    parser = argparse.ArgumentParser(description="测试 video enhancer 竞品，抓 7 天窗口素材，筛选逻辑与主工作流一致")
    parser.add_argument(
        "--target-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="可选：按主工作流逻辑筛选目标日期（UTC+8，仅 first_seen 命中；跨日则 pipeline_tags 含「重投」）",
    )
    parser.add_argument(
        "--products",
        default="",
        help="可选：指定产品名（逗号分隔，需与 config/ai_product.json 完全一致）",
    )
    parser.add_argument(
        "--output-prefix",
        default="",
        help="可选：输出文件前缀（默认沿用 test_video_enhancer_2_*）",
    )
    args = parser.parse_args()

    selected_products = [x.strip() for x in (args.products or "").split(",") if x.strip()]
    competitors = _load_video_enhancer_competitors(products=selected_products or None)
    if not competitors:
        print("[终止] video enhancer 中无可用竞品（检查 config/ai_product.json）", file=sys.stderr)
        return
    keywords = [c.appid for c in competitors]
    comp_map = {c.appid: c for c in competitors}

    print(f"[1] 竞品（已排除 Hula）: {[c.product for c in competitors]}")
    if args.target_date:
        print(
            f"[2] 使用主工作流日期筛选：target_date={args.target_date}（仅 first_seen UTC+8；"
            "created_at 与 first_seen 不同日则打「重投」）"
        )
    else:
        print(f"[2] 不传 target_date：仅应用主工作流广告主过滤，返回当前抓取到的全部素材")

    results = await run_batch(
        keywords=keywords,
        debug=bool(__import__("os").environ.get("DEBUG")),
        is_tool=True,
        order_by="latest",
        use_popularity_top1=False,
    )

# 应用与主工作流一致的筛选：广告主匹配 + 可选日期命中
# 截断规则：单个产品在单日候选素材 > 10 条时，仅保留前 5 条（按展示估值 all_exposure_value 降序）
    raw_items: list[dict[str, Any]] = []
    by_product_reduce: dict[str, list[dict]] = {}
    filter_pre_total = 0
    filter_post_total = 0
    filter_threshold = 10
    filter_keep = 5
    filter_sort_metric = "all_exposure_value"
    per_product_before: dict[str, int] = {}
    per_product_after: dict[str, int] = {}
    per_product_truncated: dict[str, bool] = {}

    for r in results:
        kw = str(r.get("keyword") or "")
        all_creatives = r.get("all_creatives") or []
        if not isinstance(all_creatives, list):
            continue
        comp = comp_map.get(kw) or Competitor(category="", product="", appid=kw)

        filtered = [
            c
            for c in all_creatives
            if isinstance(c, dict)
            and advertiser_matches_product(
                str(c.get("advertiser_name") or c.get("page_name") or ""), comp.product or ""
            )
        ]

        # 先按日期命中筛选（若指定 target_date）
        candidates: list[dict[str, Any]] = []
        for c in filtered:
            if args.target_date:
                hit, _ = _creative_hits_target_date(c, args.target_date)
                if not hit:
                    continue

                # 规则：重投素材如果没有 top 标签，则不纳入（仅对重投生效）
                if _is_resume_advertising(c) and not _exposure_top_has_any(c):
                    continue

            candidates.append(c)

        # 若某产品在该日命中候选数 > 10，则按 all_exposure_value 降序取前 5 条
        product_key = comp.product or kw
        before_cnt = len(candidates)
        truncated = False
        if args.target_date and before_cnt > filter_threshold:
            # 按展示估值（all_exposure_value）降序取前 5
            candidates.sort(key=lambda x: int(x.get("all_exposure_value") or 0), reverse=True)
            candidates = candidates[:filter_keep]
            truncated = True

        after_cnt = len(candidates)
        filter_pre_total += before_cnt
        per_product_before[product_key] = per_product_before.get(product_key, 0) + before_cnt
        per_product_after[product_key] = per_product_after.get(product_key, 0) + after_cnt
        per_product_truncated[product_key] = per_product_truncated.get(product_key, False) or truncated

        for c in candidates:
            _apply_relaunch_pipeline_tag(c)
            raw_items.append(
                {
                    "category": comp.category,
                    "product": comp.product,
                    "appid": comp.appid,
                    "keyword": kw,
                    "creative": c,
                }
            )
            by_product_reduce.setdefault(comp.product or kw, []).append(_reduce_creative(c))
    filter_post_total = len(raw_items)

    # 打印筛选统计，便于排查
    if args.target_date:
        print(
            f"[filter] 目标日期={args.target_date} "
            f"筛选前候选总数={filter_pre_total}（按广告主+日期+重投top过滤后、未做单产品截断）"
        )
        print(
            f"[filter] 截断规则：单产品候选>={filter_threshold+1} 条时，仅保留前 {filter_keep} 条；排序字段={filter_sort_metric}"
        )
        for k in sorted(per_product_after.keys()):
            print(
                f"[filter] product={k} before={per_product_before.get(k,0)} after={per_product_after.get(k,0)} "
                f"truncated={per_product_truncated.get(k, False)}"
            )

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if args.output_prefix:
        base = args.output_prefix
    else:
        base = f"test_video_enhancer_2_{args.target_date}" if args.target_date else "test_video_enhancer_2_all"

    # 1) 原始 JSON
    raw_path = DATA_DIR / f"{base}_raw.json"
    raw_payload = {
        "target_date": args.target_date,
        "total": len(raw_items),
        "competitors": [{"product": c.product, "appid": c.appid} for c in competitors],
        "items": raw_items,
        "filter_report": {
            "filter_threshold": filter_threshold,
            "filter_keep": filter_keep,
            "filter_sort_metric": filter_sort_metric,
            "pre_truncation_total": filter_pre_total,
            "post_truncation_total": filter_post_total,
            "per_product": {
                k: {"before": per_product_before.get(k, 0), "after": per_product_after.get(k, 0), "truncated": per_product_truncated.get(k, False)}
                for k in sorted(set(per_product_after.keys()) | set(per_product_before.keys()))
            },
        },
    }
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(raw_payload, f, ensure_ascii=False, indent=2)
    print(f"[3] 原始 JSON 已写: {raw_path.name}（{len(raw_items)} 条）")

    # 2) 提炼 JSON
    reduce_path = DATA_DIR / f"{base}_reduce.json"
    reduce_payload = {
        "target_date": args.target_date,
        "total": len(raw_items),
        "by_product": by_product_reduce,
    }
    with open(reduce_path, "w", encoding="utf-8") as f:
        json.dump(reduce_payload, f, ensure_ascii=False, indent=2)
    print(f"[4] 提炼 JSON 已写: {reduce_path.name}（by_product: {list(by_product_reduce.keys())}）")
    print("完成。请核对两份 JSON 确认抓取内容是否正确。")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("中断。", file=sys.stderr)
