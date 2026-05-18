"""
测试：跑 video enhancer 类下的竞品（仅排除 Hula），不入库，写两个 JSON。
默认使用「UI 指定日期筛选 + 页面卡片逐张点击 detail-v2 + 本地 first_seen 校验」的主路径，
和 Arrow2 最新素材点卡口径对齐；旧 7 天池子仅保留为 --no-ui-date-range 调试回退。
默认不再做单产品硬截断，后续依赖封面/玩法/同步前规则筛选。
重投素材（resume_advertising_flag）一律不纳入。

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
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ua_workflows.shared.config import CONFIG_DIR, DATA_DIR
from ua_workflows.shared.guangdada.search import run_batch
from ua_workflows.shared.guangdada.competitor_utils import (
    _apply_relaunch_pipeline_tag,
    _creative_hits_target_date,
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


TARGET_CATEGORIES = ("video", "photo")


def _load_workflow_competitors(products: list[str] | None = None):
    """
    从 ai_product.json 读取工作流分类（video enhancer），排除 Hula。
    - 若指定 products，则只保留这些产品名；
    - 否则返回该分类下全部（除 Hula）。
    """
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"未找到配置：{CONFIG_FILE}")
    data = json.load(CONFIG_FILE.open("r", encoding="utf-8"))
    candidates: list[Competitor] = []
    if isinstance(data, dict):
        for cat in TARGET_CATEGORIES:
            bucket = data.get(cat)
            if not isinstance(bucket, dict):
                continue
            for product, appid in bucket.items():
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
    parser = argparse.ArgumentParser(description="测试工作流竞品（video enhancer），抓 7 天窗口素材，筛选逻辑与主工作流一致")
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
    parser.add_argument(
        "--napi-list",
        action="store_true",
        help="调试回退：使用旧的 napi 列表结果为主，不逐张点击页面卡片 detail-v2。",
    )
    parser.add_argument(
        "--ui-date-range",
        action="store_true",
        help="兼容参数：传入 --target-date 时默认已同步设置 UI 日期为 target_date ~ target_date。",
    )
    parser.add_argument(
        "--no-ui-date-range",
        action="store_true",
        help="调试回退：传入 --target-date 时仍按旧口径先选 7天，再本地按 first_seen 筛目标日。",
    )
    parser.add_argument(
        "--pause-per-product",
        action="store_true",
        help="调试检查：每个产品爬完后暂停，保持浏览器页面供人工检查。",
    )
    parser.add_argument(
        "--keep-browser-open",
        action="store_true",
        help="调试检查：全部爬完并写出文件后保持浏览器打开，回车后才关闭。",
    )
    args = parser.parse_args()

    selected_products = [x.strip() for x in (args.products or "").split(",") if x.strip()]
    competitors = _load_workflow_competitors(products=selected_products or None)
    if not competitors:
        print("[终止] 工作流分类中无可用竞品（检查 config/ai_product.json 的 video/photo）", file=sys.stderr)
        return
    keywords = [c.appid for c in competitors]
    comp_map = {c.appid: c for c in competitors}

    print(f"[1] 竞品（已排除 Hula，分类={','.join(TARGET_CATEGORIES)}）: {[c.product for c in competitors]}")
    if args.target_date:
        print(
            f"[2] 使用主工作流日期筛选：target_date={args.target_date}（仅 first_seen UTC+8；"
            "created_at 与 first_seen 不同日则打「重投」）"
        )
    else:
        print(f"[2] 不传 target_date：仅应用主工作流广告主过滤，返回当前抓取到的全部素材")
    crawl_mode = "napi_list" if args.napi_list else "dom_detail_click"
    print(f"[2] VE 爬取模式: {crawl_mode}")
    ui_date_range = (
        (args.target_date, args.target_date)
        if args.target_date and not args.no_ui_date_range
        else None
    )
    if ui_date_range:
        print(f"[2] UI 日期范围: {ui_date_range[0]} ~ {ui_date_range[1]}")

    results = await run_batch(
        keywords=keywords,
        debug=bool(os.environ.get("DEBUG")),
        is_tool=True,
        order_by="latest",
        use_popularity_top1=False,
        detail_click_primary=not args.napi_list,
        first_seen_target_ymd=args.target_date,
        date_range=ui_date_range,
        pause_per_keyword=bool(args.pause_per_product),
        keep_browser_open=bool(args.keep_browser_open),
        keyword_labels={c.appid: c.product for c in competitors},
    )

# 应用与主工作流一致的筛选：广告主匹配 + 可选日期命中；重投一律去掉。
# 单产品硬截断默认关闭，避免好素材在封面/玩法去重前被提前丢弃。
    raw_items: list[dict[str, Any]] = []
    by_product_reduce: dict[str, list[dict]] = {}
    filter_pre_total = 0
    filter_post_total = 0
    per_product_truncation_enabled = (os.getenv("VIDEO_ENHANCER_PER_PRODUCT_TRUNCATE_ENABLED") or "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    try:
        filter_threshold = int(os.getenv("VIDEO_ENHANCER_PER_PRODUCT_TRUNCATE_THRESHOLD") or "10")
    except ValueError:
        filter_threshold = 10
    try:
        filter_keep = int(os.getenv("VIDEO_ENHANCER_PER_PRODUCT_TRUNCATE_KEEP") or "10")
    except ValueError:
        filter_keep = 10
    filter_sort_metric = "all_exposure_value"
    per_product_before: dict[str, int] = {}
    per_product_after: dict[str, int] = {}
    per_product_truncated: dict[str, bool] = {}
    per_product_date_hits: dict[str, int] = {}
    per_product_resume_excluded: dict[str, int] = {}
    per_product_duplicate_excluded: dict[str, int] = {}

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
            and str(c.get("ad_key") or "").strip()
            and advertiser_matches_product(
                str(c.get("advertiser_name") or c.get("page_name") or ""), comp.product or ""
            )
        ]

        product_key = comp.product or kw

        # 先按日期命中筛选（若指定 target_date），再按 ad_key 做最终去重。
        candidates: list[dict[str, Any]] = []
        seen_candidate_ad_keys: set[str] = set()
        date_hit_cnt = 0
        resume_excluded_cnt = 0
        duplicate_excluded_cnt = 0
        for c in filtered:
            if args.target_date:
                hit, _ = _creative_hits_target_date(c, args.target_date)
                if not hit:
                    continue
                date_hit_cnt += 1

                # 重投素材一律不纳入
                if _is_resume_advertising(c):
                    resume_excluded_cnt += 1
                    continue

            ad_key = str(c.get("ad_key") or "").strip()
            if ad_key in seen_candidate_ad_keys:
                duplicate_excluded_cnt += 1
                continue
            seen_candidate_ad_keys.add(ad_key)
            candidates.append(c)

        before_cnt = len(candidates)
        truncated = False
        if (
            args.target_date
            and per_product_truncation_enabled
            and filter_threshold > 0
            and filter_keep > 0
            and before_cnt > filter_threshold
        ):
            candidates.sort(key=lambda x: int(x.get("all_exposure_value") or 0), reverse=True)
            candidates = candidates[:filter_keep]
            truncated = True

        after_cnt = len(candidates)
        filter_pre_total += before_cnt
        per_product_before[product_key] = per_product_before.get(product_key, 0) + before_cnt
        per_product_after[product_key] = per_product_after.get(product_key, 0) + after_cnt
        per_product_truncated[product_key] = per_product_truncated.get(product_key, False) or truncated
        per_product_date_hits[product_key] = per_product_date_hits.get(product_key, 0) + date_hit_cnt
        per_product_resume_excluded[product_key] = (
            per_product_resume_excluded.get(product_key, 0) + resume_excluded_cnt
        )
        per_product_duplicate_excluded[product_key] = (
            per_product_duplicate_excluded.get(product_key, 0) + duplicate_excluded_cnt
        )

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
            f"筛选前候选总数={filter_pre_total}（广告主+日期；已排除全部重投）"
        )
        if per_product_truncation_enabled:
            print(
                f"[filter] 截断：单产品候选>{filter_threshold} 条时仅保留前 {filter_keep} 条；排序字段={filter_sort_metric}"
            )
        else:
            print("[filter] 单产品硬截断已关闭：保留日期命中素材，后续由封面/玩法/同步前规则筛选。")
        for k in sorted(per_product_after.keys()):
            print(
                f"[filter] product={k} before={per_product_before.get(k,0)} after={per_product_after.get(k,0)} "
                f"date_hits={per_product_date_hits.get(k,0)} "
                f"resume_excluded={per_product_resume_excluded.get(k,0)} "
                f"duplicate_excluded={per_product_duplicate_excluded.get(k,0)} "
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
        "crawl_mode": crawl_mode,
        "ui_date_range": list(ui_date_range) if ui_date_range else None,
        "total": len(raw_items),
        "competitors": [{"product": c.product, "appid": c.appid} for c in competitors],
        "items": raw_items,
        "filter_report": {
            "filter_threshold": filter_threshold,
            "filter_keep": filter_keep,
            "filter_sort_metric": filter_sort_metric,
            "per_product_truncation_enabled": per_product_truncation_enabled,
            "resume_excluded_all": True,
            "pre_truncation_total": filter_pre_total,
            "post_truncation_total": filter_post_total,
            "per_product": {
                k: {
                    "before": per_product_before.get(k, 0),
                    "after": per_product_after.get(k, 0),
                    "date_hits": per_product_date_hits.get(k, 0),
                    "resume_excluded": per_product_resume_excluded.get(k, 0),
                    "duplicate_excluded": per_product_duplicate_excluded.get(k, 0),
                    "truncated": per_product_truncated.get(k, False),
                }
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
