"""
热门榜工作流 · 第 3 步：基于视频分析与标签的聚类分析

目标：
- 从第 1 步（competitor_hot_creatives_daily）和第 2 步（competitor_hot_video_analysis）组合得到素材集合；
- 对每个分类（目前主要是 video enhancer）基于：
  - 视频分析文本（video_analysis）
  - 标题、文案、标签等（raw_json 中已有字段）
 进行相似度聚类与深度解析；
- 将每个聚类的分析结果写入 competitor_hot_clusters 表；
- 同时生成一份文本周报（可选用于群内推送）。

用法（项目根目录）：

  source .venv/bin/activate
  python scripts/hot_rank_step3_cluster.py --date 2026-03-13

参数：
  --date YYYY-MM-DD   指定 crawl_date（与第 1/2 步保持一致）
  --output-json PATH  可选，将聚类结果 JSON 写入指定路径
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

from competitor_hot_db import get_conn, init_db
from workflow_competitor_hot_rank import (
    CreativeItem,
    analyze_clusters_for_category,
    build_weekly_hot_report_text,
)

load_dotenv()


def _load_items_for_clustering(crawl_date: str) -> Dict[str, List[CreativeItem]]:
    """
    从 competitor_hot_creatives_daily + competitor_hot_video_analysis 组合读取素材，
    并按分类（seek / video enhancer）分组，供聚类使用。
    """
    init_db()
    conn = get_conn()
    try:
        sql = """
        SELECT
          c.crawl_date,
          c.category,
          c.product,
          c.ad_key,
          c.advertiser_name,
          c.title,
          c.body,
          c.platform,
          c.video_url,
          c.preview_img_url,
          c.heat,
          c.all_exposure_value,
          c.days_count,
          c.raw_json,
          v.video_analysis
        FROM competitor_hot_creatives_daily AS c
        LEFT JOIN competitor_hot_video_analysis AS v
          ON v.ad_key = c.ad_key
        WHERE c.crawl_date = ?
          AND c.category IN ('seek', 'video enhancer')
        """
        cur = conn.execute(sql, (crawl_date,))
        rows = cur.fetchall()
    finally:
        conn.close()

    by_category: Dict[str, List[CreativeItem]] = {
        "seek": [],
        "video enhancer": [],
    }

    for r in rows:
        category = r["category"] or ""
        if category not in by_category:
            continue
        try:
            raw = json.loads(r["raw_json"] or "{}")
        except Exception:
            raw = {}
        # 将视频分析文本放入 raw_json，便于聚类 Prompt 使用
        if r["video_analysis"]:
            raw["video_analysis"] = r["video_analysis"]
        item = CreativeItem(
            crawl_date=r["crawl_date"],
            category=category,
            product=r["product"] or "",
            ad_key=r["ad_key"] or "",
            advertiser_name=r["advertiser_name"] or "",
            title=r["title"] or "",
            body=r["body"] or "",
            platform=r["platform"] or "",
            video_url=r["video_url"] or "",
            preview_img_url=r["preview_img_url"] or "",
            heat=int(r["heat"] or 0),
            all_exposure_value=int(r["all_exposure_value"] or 0),
            days_count=int(r["days_count"] or 0),
            raw_json=raw,
        )
        by_category[category].append(item)

    # 按热度降序排序，便于模型理解“热点”优先级
    for cat in by_category:
        by_category[cat].sort(key=lambda x: x.heat, reverse=True)
    return by_category


def _insert_clusters(
    crawl_date: str,
    clusters_by_category: Dict[str, Dict[str, Any]],
) -> int:
    """
    将聚类结果写入 competitor_hot_clusters 表。
    返回总写入条数（聚类数）。
    """
    init_db()
    conn = get_conn()
    try:
        cur = conn.cursor()
        # 先删除同一 crawl_date 下的旧聚类结果，避免重复
        cur.execute(
            "DELETE FROM competitor_hot_clusters WHERE crawl_date = ?",
            (crawl_date,),
        )
        insert_sql = """
        INSERT INTO competitor_hot_clusters (
            crawl_date, category, cluster_id, cluster_title,
            repr_count, max_play, representative_ad_keys,
            background, ua_suggestion, product_points, risk,
            trend_label, trend_reason, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        total = 0
        for cat, data in clusters_by_category.items():
            clusters = data.get("clusters") or []
            for c in clusters:
                cluster_id = c.get("cluster_id") or ""
                title = c.get("cluster_title") or ""
                analysis = c.get("analysis") or {}
                repr_count = int(c.get("repr_count") or 0)
                max_play = int(c.get("max_play") or 0)
                rep_keys = c.get("representative_ad_keys") or []
                rep_keys_json = json.dumps(rep_keys, ensure_ascii=False)
                bg = analysis.get("background") or ""
                ua = analysis.get("ua_suggestion") or ""
                prod_pts = analysis.get("product_points") or ""
                risk = analysis.get("risk") or ""
                trend_label = analysis.get("trend_label") or ""
                trend_reason = analysis.get("trend_reason") or ""
                raw_json = json.dumps(c, ensure_ascii=False)
                cur.execute(
                    insert_sql,
                    (
                        crawl_date,
                        cat,
                        cluster_id,
                        title,
                        repr_count,
                        max_play,
                        rep_keys_json,
                        bg,
                        ua,
                        prod_pts,
                        risk,
                        trend_label,
                        trend_reason,
                        raw_json,
                    ),
                )
                total += 1
        conn.commit()
        return total
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="热门榜工作流 · 第 3 步：基于视频分析的聚类分析，并写入 competitor_hot_clusters。"
    )
    p.add_argument(
        "--date",
        type=str,
        required=True,
        help="指定 crawl_date（与第 1/2 步一致），格式 YYYY-MM-DD。",
    )
    p.add_argument(
        "--output-json",
        type=str,
        default=None,
        help="可选，将聚类结果 JSON 写入指定路径。",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    crawl_date = args.date

    items_by_cat = _load_items_for_clustering(crawl_date)
    total_items = sum(len(v) for v in items_by_cat.values())
    print(
        f"[1/3] crawl_date={crawl_date}，用于聚类的素材共 {total_items} 条："
        + ", ".join(f"{k}={len(v)}" for k, v in items_by_cat.items())
    )

    if total_items == 0:
        print("[warn] 当前日期下没有可用素材，终止聚类。")
        return

    window_days = 7
    window_desc = f"{crawl_date}（近 {window_days} 天 Top 创意）"
    board_title = "广大大热度监控周报（热门榜）"

    clusters_by_category: Dict[str, Dict[str, Any]] = {}
    for cat in ("seek", "video enhancer"):
        items = items_by_cat.get(cat) or []
        print(f"[2/3] 对分类 {cat} 的 {len(items)} 条素材做聚类与深度解析...")
        data = analyze_clusters_for_category(
            board_title=board_title,
            category=cat,
            items=items,
            window_desc=window_desc,
        )
        clusters_by_category[cat] = data

    # 写入数据库
    written = _insert_clusters(crawl_date, clusters_by_category)
    print(f"[2/3] 已将 {written} 个聚类结果写入 competitor_hot_clusters 表。")

    # 可选写 JSON
    if args.output_json:
        out_path = Path(args.output_json)
    else:
        out_path = Path("data") / f"competitor_hot_clusters_{crawl_date}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(clusters_by_category, f, ensure_ascii=False, indent=2)
    print(f"[2/3] 聚类+深度解析结果 JSON 已写入 {out_path}")

    # 生成周报文本，方便你直接查看效果
    report_text = build_weekly_hot_report_text(
        end_date=None or __import__("datetime").date.fromisoformat(crawl_date),
        window_days=window_days,
        clusters_by_category=clusters_by_category,
    )
    print("\n[3/3] 热门榜聚类分析文本如下：\n")
    print(report_text)


if __name__ == "__main__":
    main()

