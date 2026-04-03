"""
将一份「当日 AI 玩法日报」推送到飞书群机器人（只看**最新一天**的数据）。

数据来源：SQLite 数据库 data/ai_products_ua.db 中的竞品素材灵感结果：
- 素材本身信息：来自 ad_creative_analysis
- 针对我方产品的 UA 建议：来自 ad_creative_product_suggestion

只取「当天 created_at = 当天」的记录（例如 3.10 跑就只报 3.10 创建的），与多维表格同步口径一致。

用法（在项目根目录，且 .env 中已配置 FEISHU_BOT_WEBHOOK）：
  source .venv/bin/activate
  python scripts/push_ai_weekly_to_feishu.py
"""

import os
import sqlite3
from datetime import date
from pathlib import Path
from typing import List, Dict

import requests
from dotenv import load_dotenv

from path_util import DATA_DIR

load_dotenv()

DB_PATH = DATA_DIR / "ai_products_ua.db"


def _map_category_display(category: str) -> str:
    """根据数据库中的 category 映射为中文展示用分类。"""
    category = (category or "").lower()
    if "video" in category:
        return "AI 视频玩法"
    if "seek" in category or "chat" in category or "tool" in category:
        return "AI 工具玩法"
    if "photo" in category or "image" in category or "图像" in category:
        return "AI 图像玩法"
    return "其他玩法"


def fetch_items_for_date(
    on_date: date,
    limit_per_group: int = 5,
) -> List[Dict]:
    """
    从数据库中拉取「created_at 落在指定日期当天」的「素材 × 产品 UA 建议」数据。
    例如 3.10 跑任务，只取 created_at 在 3.10 的记录，与多维表格同步口径一致。
    返回元素示例：
    {
      "group": "AI 视频玩法",
      "category": "video enhancer",
      "product": "UpFoto - AI Photo Enhancer",
      "our_product": "Photo Enhancer",
      "title": "...",
      "platform": "admob",
      "heat": 123,
      "ua_suggestion": "...",
    }
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              c.crawl_date,
              c.category,
              c.product,
              c.advertiser_name,
              COALESCE(c.title_zh, c.title) AS title,
              COALESCE(c.body_zh, c.body) AS body,
              c.platform,
              c.video_url,
              c.video_duration,
              c.selected_json,
              s.our_product,
              s.ua_suggestion,
              s.created_at
            FROM ad_creative_product_suggestion AS s
            JOIN ad_creative_analysis AS c
              ON c.ad_key = s.ad_key
            WHERE date(s.created_at) = date(?, 'localtime')
            ORDER BY s.created_at DESC
            """,
            (on_date.isoformat(),),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    items: List[Dict] = []
    for r in rows:
        sel_json = r["selected_json"]
        heat = None
        try:
            if sel_json:
                import json

                sj = json.loads(sel_json)
                heat = sj.get("heat")
        except Exception:
            pass
        d: Dict = {
            "group": _map_category_display(r["category"] or ""),
            "category": r["category"] or "",
            "product": r["product"] or "",
            "our_product": r["our_product"] or "",
            "title": r["title"] or r["product"] or "",
            "platform": r["platform"] or "",
            "video_url": r["video_url"] or "",
            "heat": heat if heat is not None else 0,
            "ua_suggestion": r["ua_suggestion"] or "",
        }
        items.append(d)

    # 按 group + heat 排序，然后每组截取前 N 条
    grouped: Dict[str, List[Dict]] = {}
    for it in items:
        grouped.setdefault(it["group"], []).append(it)
    for g in grouped:
        grouped[g].sort(key=lambda x: x["heat"], reverse=True)
        grouped[g] = grouped[g][:limit_per_group]

    out: List[Dict] = []
    for g, lst in grouped.items():
        out.extend(lst)
    return out


def build_weekly_message() -> str:
    """基于「当天 created_at」的竞品素材灵感数据，生成飞书推送文案（与多维表格同步口径一致）。"""
    today = date.today()
    title_range = f"{today.month}/{today.day}"

    items = fetch_items_for_date(on_date=today, limit_per_group=5)
    total = len(items)

    header = [
        f"📡 AI玩法日报 — {title_range}",
        f"📅 {title_range} · 推送时间 自动",
        f"今日共收录 {total} 条 AI 玩法热点（内容 × 我方产品），覆盖视频 / 工具 / 图像多种方向",
        "",
        "📈 趋势速览: 数据来自今日竞品 UA 素材灵感与产品建议，供产品与营销快速对齐。",
        "",
    ]

    # 当日无数据时只发简要说明
    if total == 0:
        return "\n".join(header + ["今日暂无新收录（可能爬取/分析未产出或尚未运行），明日再报。", ""] + tail)

    # 按「我方产品」分组输出（根据产品给建议）
    by_product: Dict[str, List[Dict]] = {}
    for it in items:
        key = it["our_product"] or "（未匹配产品）"
        by_product.setdefault(key, []).append(it)

    blocks: List[str] = []
    # 希望优先展示的我方产品（按内部名称或名称），若不存在则按默认顺序
    priority_order = [
        "Photo Enhancer",  # AI Photo Enhancer - Evoke
        "AI Video",        # AI Video Generator - Toki
        "AI Seek",         # Deep Think - AI Seek Chatbot（聊天助手，已改名）
    ]

    # 按优先顺序排序产品分组
    def sort_key_prod(name: str) -> tuple:
        try:
            idx = priority_order.index(name)
        except ValueError:
            idx = len(priority_order)
        return (idx, name)

    for our_prod in sorted(by_product.keys(), key=sort_key_prod):
        prod_items = by_product[our_prod]
        blocks.append(f"💡 {our_prod}")
        blocks.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
        # 按热度从高到低排列
        prod_items_sorted = sorted(prod_items, key=lambda x: x["heat"], reverse=True)
        for idx, it in enumerate(prod_items_sorted, 1):
            title = it["title"] or it["product"]
            heat = it["heat"]
            ua = (it["ua_suggestion"] or "").strip()
            video_url = it.get("video_url") or ""
            src_prod = it["product"] or ""
            blocks.append(f"{idx}. {title} （竞品: {src_prod}）")
            if heat:
                blocks.append(f"📊 核心数据: `热度: {heat}`")
            blocks.append(f"📌 UA灵感借鉴: {ua or '（模型未给出具体建议）'}")
            if video_url:
                blocks.append(f"🎥 视频素材: {video_url}")
            blocks.append("")  # 空行分隔每一条
        blocks.append("")  # 不同我方产品之间空一行

    tail = [
        "🤖 AI 玩法雷达 · 数据来源: 每日竞品 UA 素材灵感 & 产品建议",
    ]

    return "\n".join(header + blocks + tail)


def push_to_feishu(text: str) -> None:
    webhook = os.getenv("FEISHU_BOT_WEBHOOK")
    if not webhook:
        print("错误: 未在 .env 中配置 FEISHU_BOT_WEBHOOK，无法推送到飞书。")
        return

    # 使用飞书交互卡片，将整段 Markdown 文本放在一个内容块中
    payload = {
        "msg_type": "interactive",
        "card": {
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": text,
                    },
                }
            ],
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": "AI 玩法日报",
                }
            },
        },
    }
    resp = requests.post(webhook, json=payload, timeout=10)
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}
    if resp.status_code != 200 or data.get("code") not in (None, 0):
        print("推送失败:", data)
    else:
        print("推送成功。")


def main() -> None:
    text = build_weekly_message()
    push_to_feishu(text)


if __name__ == "__main__":
    main()

