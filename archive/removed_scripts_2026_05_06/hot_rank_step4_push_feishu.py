"""
热门榜工作流 · 第 4 步：飞书卡片推送（热门榜）

数据来源：
- 第 3 步写入的 competitor_hot_clusters 表（聚类+深度解析结果）
- 第 1 步写入的 competitor_hot_creatives_daily 表（用于查代表视频的链接）

卡片格式（文本内容）：
- 标题：广大大热度监控周报（热门榜）
- Metadata：监控平台 / 监控周期（过去 7 天）/ 有效高优爆款数 / 生成时间
- 趋势概览：2–3 句，分别概述 seek / video enhancer 两类标签的情况
- 深度分析：逐条聚类输出
  - [分类标签] 聚类标题
  - 核心数据摘要：代表视频 N 条，最高播放/热度
  - 背景
  - UA 建议
  - 产品对标点
  - 风险提示
  - 趋势阶段判断
  - 参考链接：代表视频的 1–2 条视频 URL（如有），否则以 ad_key 占位

用法（项目根目录）：

  source .venv/bin/activate
  python scripts/hot_rank_step4_push_feishu.py --date 2026-03-13
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

from competitor_hot_db import get_conn, init_db

load_dotenv()


def _load_clusters(crawl_date: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    从 competitor_hot_clusters 按日期加载聚类结果，按 category 分组。
    """
    init_db()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              category,
              cluster_id,
              cluster_title,
              repr_count,
              max_play,
              representative_ad_keys,
              background,
              ua_suggestion,
              product_points,
              risk,
              trend_label,
              trend_reason
            FROM competitor_hot_clusters
            WHERE crawl_date = ?
            ORDER BY category, cluster_id
            """,
            (crawl_date,),
        )
        by_cat: Dict[str, List[Dict[str, Any]]] = {}
        for r in cur.fetchall():
            cat = r["category"] or ""
            rep_keys = []
            if r["representative_ad_keys"]:
                try:
                    rep_keys = json.loads(r["representative_ad_keys"])
                except Exception:
                    rep_keys = []
            item = {
                "category": cat,
                "cluster_id": r["cluster_id"] or "",
                "cluster_title": r["cluster_title"] or "",
                "repr_count": int(r["repr_count"] or 0),
                "max_play": int(r["max_play"] or 0),
                "representative_ad_keys": rep_keys,
                "background": r["background"] or "",
                "ua_suggestion": r["ua_suggestion"] or "",
                "product_points": r["product_points"] or "",
                "risk": r["risk"] or "",
                "trend_label": r["trend_label"] or "",
                "trend_reason": r["trend_reason"] or "",
            }
            by_cat.setdefault(cat, []).append(item)
        return by_cat
    finally:
        conn.close()


def _fetch_video_links_for_cluster(
    crawl_date: str,
    ad_keys: List[str],
    limit: int = 2,
) -> List[str]:
    """
    根据代表 ad_key 列表，从 competitor_hot_creatives_daily 中查找视频链接（最多 limit 条）。
    """
    if not ad_keys:
        return []
    init_db()
    conn = get_conn()
    try:
        placeholders = ",".join("?" for _ in ad_keys)
        params: List[Any] = [crawl_date] + ad_keys + [limit]
        sql = f"""
        SELECT DISTINCT video_url
        FROM competitor_hot_creatives_daily
        WHERE crawl_date = ?
          AND ad_key IN ({placeholders})
          AND video_url IS NOT NULL
          AND video_url != ''
        LIMIT ?
        """
        cur = conn.execute(sql, params)
        urls = [r[0] for r in cur.fetchall() if r[0]]
        return urls
    finally:
        conn.close()


def _build_feishu_card(
    crawl_date: str,
    clusters_by_cat: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    """
    构造更美观的飞书 interactive 卡片 JSON。
    """
    # 监控周期：过去 7 天
    d = date.fromisoformat(crawl_date)
    start = d - timedelta(days=6)
    start_str = f"{start.year}/{start.month:02d}/{start.day:02d}"
    end_str = f"{d.year}/{d.month:02d}/{d.day:02d}"

    gen_time = datetime.now().strftime("%Y-%m-%d %H:%M")

    # 有效高优爆款：按聚类数计
    total_clusters = sum(len(v) for v in clusters_by_cat.values())

    # 趋势概览：简单基于每类聚类数 & 代表视频数生成 1 句
    def _summary(cat: str, display: str) -> str:
        lst = clusters_by_cat.get(cat) or []
        if not lst:
            return f"{display}：本周未监测到明显的高优爆款聚类。"
        total_repr = sum(int(c.get("repr_count") or 0) for c in lst)
        titles = [c.get("cluster_title") for c in lst if c.get("cluster_title")]
        titles_str = "、".join(titles[:3])
        return (
            f"{display}：共识别 {len(lst)} 个关键聚类，覆盖代表视频约 {total_repr} 条，"
            f"较突出的玩法包括【{titles_str}】等。"
        )

    trend_lines = [
        _summary("seek", "seek 方向"),
        _summary("video enhancer", "video enhancer 方向"),
    ]

    # 深度分析 markdown
    detail_parts: List[str] = []
    for cat, display in (("seek", "seek"), ("video enhancer", "video enhancer")):
        lst = clusters_by_cat.get(cat) or []
        for c in lst:
            title = c.get("cluster_title") or "未命名聚类"
            repr_count = c.get("repr_count") or 0
            max_play = c.get("max_play") or 0
            bg = c.get("background") or ""
            ua = c.get("ua_suggestion") or ""
            prod_pts = c.get("product_points") or ""
            risk = c.get("risk") or ""
            trend_label = c.get("trend_label") or ""
            trend_reason = c.get("trend_reason") or ""
            ad_keys = c.get("representative_ad_keys") or []
            video_links = _fetch_video_links_for_cluster(crawl_date, ad_keys, limit=2)
            if video_links:
                ref_link_str = "；".join(video_links)
            else:
                # 若没有视频链接，则用 ad_key 占位
                ref_link_str = "；".join(f"ad_key={k}" for k in ad_keys[:2]) or "（暂无链接，可按 ad_key 在库中查询）"

            part = f"""- [{display}] {title}
  - 核心数据摘要：代表视频 {repr_count} 条，最高播放/热度约 {max_play}
  - 背景：{bg}
  - UA建议：{ua}
  - 产品对标点：{prod_pts}
  - 风险提示：{risk}
  - 趋势阶段判断：{trend_label}（{trend_reason}）
  - 参考链接：{ref_link_str}
"""
            detail_parts.append(part)

    detail_md = "\n".join(detail_parts) if detail_parts else "（当前无聚类结果）"

    metadata_md = (
        f"- 监控平台：广大大\n"
        f"- 监控周期：过去 7天 ({start_str}  - {end_str})\n"
        f"- 有效高优爆款：{total_clusters} 条\n"
        f"- 生成时间：{gen_time}\n"
    )

    # 趋势概览：不用 # 标题，改用加粗 + 表情，兼容飞书卡片解析
    trend_md = "**📈 趋势概览**\n" + "\n".join(f"- {line}" for line in trend_lines)

    # 为 seek / video enhancer 分开做分组标题，更易扫读
    detail_blocks: List[Dict[str, Any]] = []
    for cat, display, emoji in (
        ("seek", "seek 方向", "🔍"),
        ("video enhancer", "video enhancer 方向", "🎬"),
    ):
        lst = clusters_by_cat.get(cat) or []
        if not lst:
            continue
        # 小标题：不使用 #，用加粗 + 表情，避免标题语法不被解析
        detail_blocks.append(
            {
                "tag": "markdown",
                "content": f"**{emoji} {display}**",
            }
        )
        # 具体聚类列表
        parts: List[str] = []
        for c in lst:
            title = c.get("cluster_title") or "未命名聚类"
            repr_count = c.get("repr_count") or 0
            max_play = c.get("max_play") or 0
            bg = c.get("background") or ""
            ua = c.get("ua_suggestion") or ""
            prod_pts = c.get("product_points") or ""
            risk = c.get("risk") or ""
            trend_label = c.get("trend_label") or ""
            trend_reason = c.get("trend_reason") or ""
            ad_keys = c.get("representative_ad_keys") or []
            video_links = _fetch_video_links_for_cluster(crawl_date, ad_keys, limit=2)
            if video_links:
                ref_link_str = "；".join(f"[视频]({u})" for u in video_links)
            else:
                ref_link_str = "；".join(f"`ad_key={k}`" for k in ad_keys[:2]) or "（暂无链接，可按 ad_key 在库中查询）"

            part = f"""- **[{display}] {title}**
  - **📊 核心数据摘要**：代表视频 {repr_count} 条，最高播放/热度约 {max_play}
  - **🎬 背景**：{bg}
  - **🎯 UA建议**：{ua}
  - **🧩 产品对标点**：{prod_pts}
  - **⚠️ 风险提示**：{risk}
  - **📉 趋势阶段判断**：{trend_label}（{trend_reason}）
  - **🔗 参考链接**：{ref_link_str}
"""
            parts.append(part)
        detail_blocks.append(
            {
                "tag": "markdown",
                "content": "\n".join(parts),
            }
        )

    card = {
        "config": {
            "wide_screen_mode": True,
        },
        "header": {
            "template": "turquoise",
            "title": {
                "tag": "plain_text",
                "content": "广大大热度监控周报（热门榜）",
            },
        },
        "elements": [
            {
                "tag": "div",
                "fields": [
                    {
                        "is_short": True,
                        "text": {
                            "tag": "lark_md",
                            "content": f"**监控平台**\n广大大",
                        },
                    },
                    {
                        "is_short": True,
                        "text": {
                            "tag": "lark_md",
                            "content": f"**监控周期**\n{start_str} - {end_str}",
                        },
                    },
                    {
                        "is_short": True,
                        "text": {
                            "tag": "lark_md",
                            "content": f"**高优爆款数**\n{total_clusters} 条",
                        },
                    },
                    {
                        "is_short": True,
                        "text": {
                            "tag": "lark_md",
                            "content": f"**生成时间**\n{gen_time}",
                        },
                    },
                ],
            },
            {
                "tag": "hr",
            },
            {
                "tag": "markdown",
                "content": trend_md,
            },
            {
                "tag": "hr",
            },
            {
                "tag": "markdown",
                "content": "**📊 深度分析**",
            },
            *detail_blocks,
        ],
    }
    return card


def _push_to_feishu(card: Dict[str, Any]) -> None:
    webhook = os.getenv("FEISHU_BOT_WEBHOOK")
    if not webhook:
        print("错误：未在 .env 中配置 FEISHU_BOT_WEBHOOK，无法推送到飞书。")
        return
    payload = {
        "msg_type": "interactive",
        "card": card,
    }
    resp = requests.post(webhook, json=payload, timeout=10)
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}
    if resp.status_code != 200 or data.get("code") not in (None, 0):
        print("推送失败：", data)
    else:
        print("推送成功。")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="热门榜工作流 · 第 4 步：将聚类结果以飞书卡片形式推送。"
    )
    p.add_argument(
        "--date",
        type=str,
        required=True,
        help="指定 crawl_date（与第 1/2/3 步一致），格式 YYYY-MM-DD。",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    crawl_date = args.date

    clusters_by_cat = _load_clusters(crawl_date)
    total_clusters = sum(len(v) for v in clusters_by_cat.values())
    if total_clusters == 0:
        print(f"[info] {crawl_date} 在 competitor_hot_clusters 中没有聚类结果，取消推送。")
        return

    card = _build_feishu_card(crawl_date, clusters_by_cat)
    _push_to_feishu(card)


if __name__ == "__main__":
    main()

