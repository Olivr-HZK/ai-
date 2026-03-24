"""
广大大工作流：
点击「工具」→ 点击「7天」→ 点击「素材」→ 搜索框依次输入 config/ai_product.json 里的竞品 appid（包名）
抓取每个竞品在界面上可滚动加载到的“全部素材”（7天窗口），
并按 UTC+8 将 first_seen 转为日期，筛选指定 target_date 的素材入库（仅 first_seen 命中）；
若 created_at 与 first_seen 的 UTC+8 日期不同，则在 creative 上写入 pipeline_tags 含「重投」。

入库表：
- guangdada_competitor_yesterday_creatives（按 target_date 维度去重，适合“某天上线素材”）

用法：
  DEBUG=1 python scripts/workflow_guangdada_competitor_yesterday_creatives.py
  python scripts/workflow_guangdada_competitor_yesterday_creatives.py --date 2026-03-18   # 默认入库 2026-03-17
  python scripts/workflow_guangdada_competitor_yesterday_creatives.py --target-date 2026-03-17
  python scripts/workflow_guangdada_competitor_yesterday_creatives.py --limit 3
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
import re
from difflib import SequenceMatcher

from path_util import CONFIG_DIR, DATA_DIR

# 复用现有“工具→7天→素材→搜索→抓 creative/list”能力
from run_search_workflow import run_batch

from guangdada_yesterday_creatives_db import upsert_many


AI_PRODUCT_FILE = CONFIG_DIR / "ai_product.json"


@dataclass(frozen=True)
class Competitor:
    category: str
    product: str
    appid: str


def _load_competitors() -> list[Competitor]:
    if not AI_PRODUCT_FILE.exists():
        raise FileNotFoundError(f"未找到配置文件：{AI_PRODUCT_FILE}")
    data = json.load(open(AI_PRODUCT_FILE, encoding="utf-8"))
    competitors: list[Competitor] = []
    if isinstance(data, dict):
        for cat, items in data.items():
            if not isinstance(items, dict):
                continue
            for product, appid in items.items():
                if not product:
                    continue
                competitors.append(
                    Competitor(
                        category=str(cat),
                        product=str(product),
                        appid=str(appid or ""),
                    )
                )
    return competitors


def _to_local_date_from_unix(ts: Any) -> str | None:
    try:
        if ts is None:
            return None
        ts_int = int(ts)
        if ts_int <= 0:
            return None
        # 以本机时区作为“昨日”的口径
        d = datetime.fromtimestamp(ts_int).astimezone().date()
        return d.isoformat()
    except Exception:
        return None


def _to_int_ts(v: Any) -> int | None:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _ts_to_day(ts: int) -> str | None:
    """将接口 unix 秒按 UTC+8（北京时间）转换为 YYYY-MM-DD。"""
    try:
        from datetime import timezone

        tz8 = timezone(timedelta(hours=8))
        return datetime.fromtimestamp(int(ts), tz=tz8).strftime("%Y-%m-%d")
    except Exception:
        return None


def _first_seen_day_utc8(c: dict[str, Any]) -> str | None:
    fs = _to_int_ts(c.get("first_seen"))
    return _ts_to_day(fs) if fs is not None else None


def _created_at_day_utc8(c: dict[str, Any]) -> str | None:
    ca = _to_int_ts(c.get("created_at"))
    return _ts_to_day(ca) if ca is not None else None


def _creative_hits_target_date(c: dict[str, Any], target_date: str) -> tuple[bool, str]:
    """
    仅按 first_seen 换算为 UTC+8 日期后等于 target_date 才命中。
    返回 (是否命中, 原因)：first_seen / no_first_seen / wrong_day
    """
    fs_day = _first_seen_day_utc8(c)
    if fs_day is None:
        return False, "no_first_seen"
    if fs_day == target_date:
        return True, "first_seen"
    return False, "wrong_day"


def _is_resume_advertising(c: dict[str, Any]) -> bool:
    """是否“重投”，以原始字段 `resume_advertising_flag` 为准。"""
    v = c.get("resume_advertising_flag")
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "t"}
    return False


def _exposure_top_has_any(c: dict[str, Any]) -> bool:
    """
    `exposure_top` 是否存在至少一个命中 top 标签（top1/top5/top10 非空）。
    exposure_top 的 value 在现有数据里通常是 list（非空才算有 top）。
    """
    et = c.get("exposure_top")
    if not isinstance(et, dict):
        return False
    for k in ("top1", "top5", "top10"):
        v = et.get(k)
        if isinstance(v, list):
            if len(v) > 0:
                return True
        elif v:
            # 兼容非 list 的情况（比如 value 直接就是 truthy）
            return True
    return False


def _apply_relaunch_pipeline_tag(c: dict[str, Any]) -> None:
    """
    若 `resume_advertising_flag=true`，打上「重投」标签（写入 creative.pipeline_tags）。
    就地修改 c；并移除旧的「重投」避免重复。
    """
    base = c.get("pipeline_tags")
    tags = list(base) if isinstance(base, list) else []
    tags = [t for t in tags if t != "重投"]
    if _is_resume_advertising(c):
        tags.append("重投")
    if tags:
        c["pipeline_tags"] = tags
    else:
        c.pop("pipeline_tags", None)


def _norm_text(s: str) -> str:
    s = str(s or "").strip().lower()
    # 去掉大部分符号与空白，只保留字母数字与中日韩字符
    return re.sub(r"[^0-9a-z\u4e00-\u9fff\u3040-\u30ff\uac00-\ud7af]+", "", s)


def advertiser_matches_product(advertiser_name: str, product: str) -> bool:
    """
    过滤明显不相关的广告主：
    - 强匹配：归一化后互为包含
    - 弱匹配：相似度阈值
    """
    a = _norm_text(advertiser_name)
    p = _norm_text(product)
    if not a or not p:
        return False
    if a in p or p in a:
        return True
    return SequenceMatcher(None, a, p).ratio() >= 0.58


def parse_args():
    p = argparse.ArgumentParser(description="抓取广大大竞品某日（UTC+8，北京时间）素材并入库（新库新表）")
    p.add_argument(
        "--date",
        default=None,
        metavar="YYYY-MM-DD",
        help="抓取日期（默认今天）。若不传 --target-date，则默认 target-date = date - 1",
    )
    p.add_argument(
        "--target-date",
        default=None,
        metavar="YYYY-MM-DD",
        help="要入库的素材日期（按 UTC+8 计算，仅按 first_seen 命中；重投需同时带 top 标签）",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=0,
        help="只跑前 N 个竞品（0=不限制）",
    )
    p.add_argument(
        "--order-by",
        choices=["latest", "exposure"],
        default="latest",
        help="页面排序方式：latest=最新创意，exposure=展示估值（默认 latest）",
    )
    args = p.parse_args()
    if args.date:
        crawl_date = date.fromisoformat(args.date)
    else:
        crawl_date = date.today()
    args.crawl_date = crawl_date.isoformat()
    if args.target_date:
        args.target_date = date.fromisoformat(args.target_date).isoformat()
    else:
        args.target_date = (crawl_date - timedelta(days=1)).isoformat()
    return args


async def main():
    args = parse_args()
    competitors = _load_competitors()
    if args.limit and args.limit > 0:
        competitors = competitors[: args.limit]
    if not competitors:
        print("[终止] ai_product.json 中未找到任何竞品", file=sys.stderr)
        return

    # 按 appid 搜索（包名），更稳定；同时保留 product 用于入库展示
    keywords = [c.appid for c in competitors if (c.appid or "").strip()]
    print(
        f"[1] 将依次搜索 {len(keywords)} 个竞品（工具→7天→素材/最新创意，关键词=appid），"
        f"抓全量后按 UTC+8(北京时间) 筛选 target_date={args.target_date} 入库…"
    )

    # run_batch 内部会：登录一次、设置一次（工具/7天/素材）、然后循环搜索关键词
    results = await run_batch(
        keywords=keywords,
        debug=False,      # 是否有界面由环境变量 DEBUG 控制（run_search_workflow 内部读取）
        is_tool=True,
        order_by=args.order_by,
        use_popularity_top1=False,
    )

    # 先输出“抓到了/没抓到”的清单（以 7天窗口 all_creatives 是否为空为准）
    hit_products: list[tuple[str, int]] = []
    miss_products: list[str] = []
    for r in results:
        kw = str(r.get("keyword") or "")
        all_creatives = r.get("all_creatives") or []
        n = len(all_creatives) if isinstance(all_creatives, list) else 0
        if n > 0:
            hit_products.append((kw, n))
        else:
            miss_products.append(kw)
    hit_products.sort(key=lambda x: (-x[1], x[0]))
    miss_products.sort()

    print("\n[抓取覆盖]（以 7天窗口抓到的 all_creatives 条数统计）")
    print(f"- 有数据: {len(hit_products)}")
    for kw, n in hit_products:
        print(f"  - {kw}: {n}")
    print(f"- 无数据: {len(miss_products)}")
    for kw in miss_products:
        print(f"  - {kw}")
    print()

    # 建一个 appid -> competitor 的映射，方便补齐 category/product/appid
    comp_map = {c.appid: c for c in competitors if (c.appid or "").strip()}

    to_write: list[dict[str, Any]] = []
    hit_first_seen = 0
    miss_date = 0
    relaunch_kept_with_top = 0
    relaunch_filtered_no_top = 0
    total_seen = 0
    for r in results:
        keyword = str(r.get("keyword") or "")
        all_creatives = r.get("all_creatives") or []
        if not isinstance(all_creatives, list):
            continue
        comp = comp_map.get(keyword) or Competitor(category="", product="", appid=keyword)

        # 按广告主过滤，避免混入明显不相关的素材
        filtered: list[dict[str, Any]] = []
        for c in all_creatives:
            if not isinstance(c, dict):
                continue
            adv = c.get("advertiser_name") or c.get("page_name") or ""
            if advertiser_matches_product(str(adv), comp.product or ""):
                filtered.append(c)
        if len(filtered) != len(all_creatives):
            print(
                f"  [过滤] appid={keyword} product={comp.product} "
                f"all={len(all_creatives)} -> matched={len(filtered)}"
            )
        all_creatives = filtered
        total_seen += len(all_creatives)

        for c in all_creatives:
            if not isinstance(c, dict):
                continue
            hit, _source = _creative_hits_target_date(c, args.target_date)
            if not hit:
                miss_date += 1
                continue

            hit_first_seen += 1
            # 规则：先全量保留 first_seen 命中；若是重投且没有 top 标签，则不入库
            if _is_resume_advertising(c) and not _exposure_top_has_any(c):
                relaunch_filtered_no_top += 1
                continue

            _apply_relaunch_pipeline_tag(c)
            if _is_resume_advertising(c):
                relaunch_kept_with_top += 1

            to_write.append(
                {
                    "category": comp.category,
                    "product": comp.product or keyword,
                    "appid": comp.appid,
                    "keyword": keyword,
                    "creative": c,
                }
            )

    print(
        f"[2] 7天窗口内共抓到 {total_seen} 条素材（未过滤），"
        f"北京时间 {args.target_date} 命中 {len(to_write)} 条，开始入库…"
    )
    print(
        f"    first_seen 口径：命中={hit_first_seen}, "
        f"重投保留(有 top)={relaunch_kept_with_top}, "
        f"重投过滤(无 top)={relaunch_filtered_no_top}, "
        f"未命中日期={miss_date}"
    )
    n = upsert_many(
        crawl_date=args.crawl_date,
        target_date=args.target_date,
        rows=to_write,
    )
    print(
        f"[3] 已写入 data/guangdada_yesterday_creatives.db 表 guangdada_competitor_yesterday_creatives：{n} 行（去重 UPSERT）"
    )

    # 可选：落一份 JSON 方便肉眼检查
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out = DATA_DIR / f"guangdada_creatives_target_{args.target_date}_crawl_{args.crawl_date}.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(
            {"crawl_date": args.crawl_date, "target_date": args.target_date, "items": to_write},
            f,
            ensure_ascii=False,
            indent=2,
        )
    print(f"[4] 已额外保存 JSON 预览：{out.name}")
    print("完成。")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())

