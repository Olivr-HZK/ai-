"""
推送飞书「VE竞品素材日报」卡片——新素材 + 持续发力，按产品分组，一句话摘要 + 链接。

替代旧版方向卡片格式，改为更紧凑的日报形式：
- 新素材：creative_library.first_target_date = target_date
- 持续发力：封面跨日指纹去重中被去掉的素材（换了ad_key但视频/封面和之前一样）
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

from path_util import DATA_DIR
from video_enhancer_pipeline_db import _get_conn, init_db

# ── 产品主题 ──────────────────────────────────────────
PRODUCT_THEMES: Dict[str, str] = {
    "Remini - AI Photo Enhancer": "AI照片增强/修复/滤镜",
    "Pixverse:AI Video Generator": "AI视频生成",
    "Glam AI:Video & Photo Editor": "AI美颜/换装/照片编辑",
    "Retake AI Face & Selfie Editor": "AI人脸重拍/照片编辑",
    "DreamFace:AI Video Generator": "AI人像动画/照片转视频",
    "GIO - AI Photoshoot Generator": "AI写真/拍照生成",
    "EPIK - AI Photo & Video Editor": "AI照片编辑/滤镜",
    "AI Mirror: AI Photo & Video": "AI照片/视频滤镜",
}


def _default_date() -> str:
    return (date.today() - timedelta(days=1)).isoformat()


# ── 摘要提取 ──────────────────────────────────────────
def _extract_one_liner(analysis_text: str) -> str:
    """从 insight_analysis 提取一句话摘要。

    优先级：
    1. 【一句话说明】行
    2. 第 2 节 Hook / 视觉钩子 首条要点
    3. 首段有意义的行
    """
    if not analysis_text:
        return ""
    m = re.search(r"【一句话说明】\s*(.+)", analysis_text)
    if m:
        return m.group(1).strip()[:40]
    # 尝试取 Hook / 视觉钩子 的第一点
    m2 = re.search(
        r"(?:2\)\s*(?:Hook|视觉钩子|情感基调|创意核心))[^\n]*\n[-•]\s*\*{0,2}(.+?)(?:\*{0,2}[：:]|\s*[：:])\s*(.+)",
        analysis_text,
    )
    if m2:
        label = m2.group(1).strip().lstrip("*").strip()
        desc = m2.group(2).strip()
        return f"{label}：{desc}"[:60]
    # 首段有意义行
    for line in analysis_text.split("\n"):
        line = line.strip().lstrip("-•·* ").strip()
        if len(line) > 8 and not line.startswith("#") and not line.startswith("1)") and not line.startswith("视频") and not line.startswith("图片"):
            return line[:60]
    return ""


# ── 持续发力 ──────────────────────────────────────────
def _load_sustained_effort(target_date: str, lookback_days: int = 7) -> Dict[str, List[Dict[str, Any]]]:
    """从 *_cover_style_intraday.json 报告的 cross_day_fingerprint_removed 读取持续发力。

    返回 {product: [{ad_key, matched_date, days, reason, video_url, preview_img_url, video_duration}]}
    """
    by_product: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    td = date.fromisoformat(target_date)
    seen_keys: set[str] = set()

    for i in range(lookback_days + 1):
        ds = (td - timedelta(days=i)).isoformat()
        fpath = Path(str(DATA_DIR)) / f"workflow_video_enhancer_{ds}_cover_style_intraday.json"
        if not fpath.exists():
            continue
        with open(fpath, "r", encoding="utf-8") as fh:
            rdata = json.load(fh)
        for h in rdata.get("cross_day_fingerprint_removed", []):
            ad_key = h.get("ad_key", "")
            if ad_key in seen_keys:
                continue
            seen_keys.add(ad_key)
            matched_date = h.get("matched_date", "")
            try:
                days = (date.fromisoformat(target_date) - date.fromisoformat(matched_date)).days
            except (ValueError, TypeError):
                days = 0
            # 查产品名 + 媒体链接
            product = "未知"
            video_url = ""
            preview_img_url = ""
            video_duration = 0
            try:
                conn = _get_conn()
                cur = conn.cursor()
                cur.execute(
                    "SELECT product, video_url, preview_img_url, video_duration "
                    "FROM daily_creative_insights WHERE ad_key LIKE ? LIMIT 1",
                    (ad_key[:16] + "%",),
                )
                row = cur.fetchone()
                if row:
                    product = row["product"] or "未知"
                    video_url = row["video_url"] or ""
                    preview_img_url = row["preview_img_url"] or ""
                    video_duration = int(row["video_duration"] or 0)
                conn.close()
            except Exception:
                pass
            by_product[product].append({
                "ad_key": ad_key,
                "matched_date": matched_date,
                "days": days,
                "reason": h.get("reason", ""),
                "video_url": video_url,
                "preview_img_url": preview_img_url,
                "video_duration": video_duration,
            })
    return by_product


# ── 卡片渲染 ──────────────────────────────────────────
def _render_daily_card_markdown(target_date: str, new_items: List[Dict[str, Any]],
                                 sustained_by_product: Dict[str, List[Dict[str, Any]]]) -> str:
    """渲染日报卡片 Markdown。

    格式：按竞品分组，每条素材用一句话说明（effect_one_liner）做可点击链接跳转到视频/图片。
    只展示新素材，持续发力暂不展示。
    """
    by_product: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in new_items:
        by_product[item["product"]].append(item)

    total_new = len(new_items)

    lines: List[str] = []
    lines.append(f"**{target_date}** | 新素材 **{total_new}** 条")
    lines.append("")

    for product in by_product:
        items = by_product[product]
        if not items:
            continue

        short_name = product.split(":")[0].split(" - ")[0].strip()
        theme = PRODUCT_THEMES.get(product, "")
        header = f"**{short_name}**"
        if theme:
            header += f"  ·  {theme}"
        lines.append(header)

        for item in items:
            is_video = int(item.get("video_duration") or 0) > 0
            effect = item.get("_effect_one_liner", "")
            if not effect:
                effect = item.get("_one_liner", "")
            if not effect:
                imp = item.get("best_impression", 0)
                effect = f"展示{imp:,}"

            if is_video and item.get("video_url"):
                link = item["video_url"]
            elif item.get("preview_img_url"):
                link = item["preview_img_url"]
            else:
                link = ""

            icon = "🎬" if is_video else "🖼"
            if link:
                lines.append(f"{icon} [{effect}]({link})")
            else:
                lines.append(f"{icon} {effect}")

        lines.append("")

    return "\n".join(lines)


# ── 推送 ──────────────────────────────────────────────
def push_card(webhook: str, title: str, md_text: str) -> None:
    if not webhook:
        print("[card] 未配置 webhook，跳过卡片推送。")
        return
    card = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True, "enable_forward": True},
            "header": {"title": {"tag": "plain_text", "content": title}, "template": "blue"},
            "elements": [{"tag": "markdown", "content": md_text[:12000]}],
        },
    }
    try:
        resp = requests.post(webhook, json=card, timeout=15)
        print(f"[card] 推送结果: {resp.status_code} {resp.text[:200]}")
    except Exception as exc:
        print(f"[card] 推送失败: {exc}")


# ── 主流程 ─────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="推送飞书 VE竞品素材日报卡片（新素材+持续发力）")
    p.add_argument("--date", default=_default_date(), help="目标日期 YYYY-MM-DD（默认昨天）")
    p.add_argument("--feishu-webhook", default="", help="飞书卡片 webhook")
    p.add_argument("--lookback", type=int, default=7, help="持续发力回溯天数（默认7）")
    return p.parse_args()


def main() -> None:
    load_dotenv()
    init_db()
    args = parse_args()
    target_date = args.date

    # ── 新素材 ──
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT ad_key, product, creative_type, best_impression, best_all_exposure_value, "
        "best_heat, video_url, preview_img_url, video_duration "
        "FROM creative_library WHERE first_target_date = ? ORDER BY best_impression DESC",
        (target_date,),
    )
    new_items = [dict(row) for row in cur.fetchall()]

    # 补充一句话摘要 + effect_one_liner
    for item in new_items:
        cur.execute(
            "SELECT insight_analysis, effect_one_liner FROM daily_creative_insights "
            "WHERE ad_key LIKE ? AND target_date = ? LIMIT 1",
            (item["ad_key"][:16] + "%", target_date),
        )
        row = cur.fetchone()
        item["_one_liner"] = _extract_one_liner(row["insight_analysis"] if row else "")
        effect = ""
        if row and row["effect_one_liner"] and row["effect_one_liner"] != "None":
            effect = row["effect_one_liner"]
        item["_effect_one_liner"] = effect
    conn.close()

    # ── 持续发力 ──
    sustained = _load_sustained_effort(target_date, lookback_days=args.lookback)

    if not new_items and not any(sustained.values()):
        print(f"[feishu-card] {target_date} 无新素材也无持续发力，跳过推送。")
        return

    # ── 渲染并推送 ──
    card_md = _render_daily_card_markdown(target_date, new_items, sustained)

    webhook = (args.feishu_webhook or "").strip()
    if not webhook:
        webhook = os.getenv("FEISHU_UA_WEBHOOK", "") or os.getenv("FEISHU_BOT_WEBHOOK", "")
    webhook = (webhook or "").strip()
    if not webhook:
        print("[feishu-card] 未配置 FEISHU_UA_WEBHOOK/FEISHU_BOT_WEBHOOK，跳过卡片推送。")
        return

    card_title = f"AI工具竞品日报 {target_date}"
    push_card(webhook, card_title, card_md)


if __name__ == "__main__":
    main()
