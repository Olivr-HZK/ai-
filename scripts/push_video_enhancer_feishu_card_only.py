"""
只推送飞书「Video Enhancer 统一UA建议」卡片（不写入多维表）。

为避免重复写入风险，它不复用 `sync_raw_analysis_to_bitable_and_push_card.py` 的「同步表+建卡」逻辑，
而是直接渲染 `_render_card_markdown` 并调用 `push_card`。
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv

from path_util import DATA_DIR

# 复用飞书卡片渲染与发送实现（保证与飞书/企业微信口径一致）
from sync_raw_analysis_to_bitable_and_push_card import (
    _render_card_markdown,
    build_meta_by_ad_from_analysis_payload,
    push_card,
)
from push_video_enhancer_multichannel import _build_summary_text_from_intro


def _default_date() -> str:
    # 默认昨天（UTC+8口径：由上游 workflow 控制；这里保持与现有脚本一致）
    return (date.today() - timedelta(days=1)).isoformat()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="仅推送飞书 Video Enhancer 统一UA建议卡片")
    p.add_argument("--date", default=_default_date(), help="目标日期 YYYY-MM-DD（默认昨天）")
    p.add_argument("--raw", default="", help="raw 文件路径（可选）")
    p.add_argument("--analysis", default="", help="analysis 文件路径（可选，用于 ad_key -> video_url 映射）")
    p.add_argument("--suggestion-md", default="", help="UA建议 md 文件路径（可选）")
    p.add_argument("--suggestion-json", default="", help="UA建议 json 文件路径（可选）")
    p.add_argument(
        "--bitable-url",
        default="",
        help="飞书多维表完整链接（含 table 参数）。不传则读取 VIDEO_ENHANCER_BITABLE_URL",
    )
    p.add_argument("--feishu-webhook", default="", help="飞书卡片 webhook（不填则读 .env FEISHU_UA_WEBHOOK/FEISHU_BOT_WEBHOOK）")
    return p.parse_args()


def _resolve_paths(target_date: str, args: argparse.Namespace) -> tuple[Path, Path, Path, Path]:
    raw = Path(args.raw) if args.raw else (DATA_DIR / f"workflow_video_enhancer_{target_date}_raw.json")
    analysis = (
        Path(args.analysis)
        if args.analysis
        else (DATA_DIR / f"video_analysis_workflow_video_enhancer_{target_date}_raw.json")
    )
    s_md = (
        Path(args.suggestion_md)
        if args.suggestion_md
        else (DATA_DIR / f"ua_suggestion_workflow_video_enhancer_{target_date}.md")
    )
    s_json = (
        Path(args.suggestion_json)
        if args.suggestion_json
        else (DATA_DIR / f"ua_suggestion_workflow_video_enhancer_{target_date}.json")
    )
    return raw, analysis, s_md, s_json


def main() -> None:
    load_dotenv()
    args = parse_args()
    target_date = args.date

    raw_path, analysis_path, s_md_path, s_json_path = _resolve_paths(target_date, args)
    if not raw_path.exists():
        raise FileNotFoundError(f"raw 文件不存在: {raw_path}")
    if not analysis_path.exists():
        raise FileNotFoundError(f"analysis 文件不存在: {analysis_path}")

    raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
    suggestion_md = s_md_path.read_text(encoding="utf-8") if s_md_path.exists() else ""
    suggestion_payload = json.loads(s_json_path.read_text(encoding="utf-8")) if s_json_path.exists() else {}

    analysis_payload = json.loads(analysis_path.read_text(encoding="utf-8"))
    meta_by_ad = build_meta_by_ad_from_analysis_payload(analysis_payload)

    intro_md = _build_summary_text_from_intro(target_date, raw_payload)
    bitable_url = (args.bitable_url or "").strip() or os.getenv("VIDEO_ENHANCER_BITABLE_URL", "").strip()

    card_md = _render_card_markdown(
        suggestion_json=suggestion_payload,
        suggestion_md=suggestion_md,
        meta_by_ad=meta_by_ad,
        intro_md=intro_md,
        bitable_url=bitable_url,
        include_ua_suggestion=False,
        include_product_benchmark=True,
    )

    webhook = (args.feishu_webhook or "").strip()
    if not webhook:
        webhook = os.getenv("FEISHU_UA_WEBHOOK", "") or os.getenv("FEISHU_BOT_WEBHOOK", "")
    webhook = (webhook or "").strip()
    if not webhook:
        print("[feishu-card] 未配置 FEISHU_UA_WEBHOOK/FEISHU_BOT_WEBHOOK，跳过卡片推送。")
        return

    card_title = f"广大大素材日报（{target_date}）"
    push_card(webhook, card_title, card_md)


if __name__ == "__main__":
    main()

