#!/usr/bin/env python3
"""Push Haopeng Top-N acceptance predictions to a Feishu group."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import requests
from dotenv import dotenv_values, load_dotenv

from path_util import DATA_DIR, PROJECT_ROOT


DEFAULT_EXPERIMENT_DIR = DATA_DIR / "haopeng_topn_experiments"
VIDEO_GEN_ENV = PROJECT_ROOT.parent / "视频生成探索" / "video-gen-system" / ".env"


def clamp(value: Any, limit: int) -> str:
    text = str(value or "").replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "..."


def md_escape(value: Any) -> str:
    return str(value or "").replace("\n", " ").replace("|", " ").strip()


def link_escape(value: Any) -> str:
    return md_escape(value).replace("[", "［").replace("]", "］")


def find_default_report_path() -> Path:
    candidates = sorted(DEFAULT_EXPERIMENT_DIR.glob("*_label_prior.json"))
    if not candidates:
        raise FileNotFoundError(f"未找到 TopN 实验文件: {DEFAULT_EXPERIMENT_DIR}/*_label_prior.json")
    return candidates[-1]


def load_report(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"报告不是 JSON object: {path}")
    return data


def classify_play_kind(row: dict[str, Any]) -> str:
    matched = str(row.get("matched_play_label") or "").strip()
    if matched and matched != "新玩法候选":
        return "老玩法新变体"
    return "新玩法候选"


def material_url(row: dict[str, Any]) -> str:
    return str(
        row.get("video_url")
        or row.get("cover_url")
        or row.get("preview_img_url")
        or ""
    ).strip()


def missing_media_links(report: dict[str, Any], top_n: int | None = None) -> bool:
    rows = [r for r in report.get("results") or [] if isinstance(r, dict)]
    if top_n is not None:
        rows = rows[:top_n]
    return any(not material_url(row) for row in rows)


def merge_report_rows_from_source(report: dict[str, Any], source_rows: list[dict[str, Any]]) -> None:
    by_key = {
        str(row.get("ad_key") or "").strip(): row
        for row in source_rows
        if isinstance(row, dict) and str(row.get("ad_key") or "").strip()
    }
    for row in report.get("results") or []:
        if not isinstance(row, dict):
            continue
        source = by_key.get(str(row.get("ad_key") or "").strip())
        if not source:
            continue
        for key in (
            "product",
            "core",
            "play_label",
            "video_url",
            "cover_url",
            "preview_img_url",
            "video_duration",
            "title",
        ):
            if not row.get(key) and source.get(key) not in (None, ""):
                row[key] = source[key]


def material_title(row: dict[str, Any]) -> str:
    return clamp(row.get("core") or row.get("title") or row.get("ad_key") or "未命名素材", 52)


def short_product(product: Any) -> str:
    text = str(product or "未知产品").strip()
    if ":" in text:
        return text.split(":", 1)[0].strip()
    if " - " in text:
        return text.split(" - ", 1)[0].strip()
    return text


def play_name(row: dict[str, Any]) -> str:
    for key in ("play_label", "matched_play_label"):
        value = str(row.get(key) or "").strip()
        if value and value != "新玩法候选":
            return value
    return "待确认"


def actual_summary(report: dict[str, Any], top_n: int) -> str:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    top_summary = summary.get(f"top{top_n}") if isinstance(summary.get(f"top{top_n}"), dict) else {}
    if not top_summary:
        rows = [r for r in report.get("results") or [] if isinstance(r, dict)][:top_n]
        accepted = sum(1 for r in rows if str(r.get("actual_hp") or "").strip() == "采纳")
        library = sum(1 for r in rows if str(r.get("actual_hp") or "").strip() == "入素材库")
        return f"采纳 {accepted}/{top_n}；采纳+入素材库：{accepted + library}/{top_n}"

    accepted = top_summary.get("accepted")
    accepted_or_library = top_summary.get("accepted_or_library")
    if accepted is None or accepted_or_library is None:
        return ""

    parts = [f"采纳 {accepted}/{top_n}", f"采纳+入素材库：{accepted_or_library}/{top_n}"]
    actual_counts = top_summary.get("actual_counts")
    if isinstance(actual_counts, dict) and actual_counts:
        ordered = ["采纳", "入素材库", "重复抓取", "不采纳", "待定", "<空>"]
        count_parts = [
            f"{name} {actual_counts[name]}"
            for name in ordered
            if actual_counts.get(name)
        ]
        extra = [
            f"{name} {count}"
            for name, count in actual_counts.items()
            if name not in ordered and count
        ]
        if count_parts or extra:
            parts.append("实际分布：" + " / ".join(count_parts + extra))
    return "；".join(parts)


def format_material_line(index: int, row: dict[str, Any], include_actual: bool) -> list[str]:
    title = link_escape(material_title(row))
    url = material_url(row)
    head = f"{index}. [{title}]({url})" if url else f"{index}. **{title}**"
    score = row.get("accept_score")
    chips = [
        f"产品：{short_product(row.get('product'))}",
        f"分数：{score}" if score not in (None, "") else "",
        f"类型：{classify_play_kind(row)}",
        f"玩法：{play_name(row)}",
    ]
    confidence = str(row.get("confidence") or "").strip()
    if confidence:
        chips.append(f"置信：{confidence}")
    actual = str(row.get("actual_hp") or "").strip()
    if include_actual and actual:
        chips.append(f"浩鹏实际：{actual}")

    lines = [head, "   " + " · ".join(chip for chip in chips if chip)]
    reason = clamp(row.get("reason"), 92)
    if reason:
        lines.append(f"   理由：{md_escape(reason)}")
    ad_key = str(row.get("ad_key") or "").strip()
    if ad_key:
        lines.append(f"   ID：`{ad_key}`")
    return lines


def render_topn_markdown(
    report: dict[str, Any],
    top_n: int = 10,
    *,
    include_backtest: bool = False,
) -> str:
    target_date = str(report.get("target_date") or "").strip()
    history_window = str(report.get("history_window") or "").strip()
    model = str(report.get("model") or "").strip()
    strategy = str(report.get("name") or report.get("payload_kind") or "label_prior").strip()
    rows = [r for r in report.get("results") or [] if isinstance(r, dict)][:top_n]
    include_actual = include_backtest and any(str(r.get("actual_hp") or "").strip() for r in rows)

    lines = [
        f"**VE 浩鹏采纳预测 Top{top_n} - {target_date}**",
        "",
    ]
    if history_window:
        lines.append(f"- 历史参考：{history_window}")
    if model:
        lines.append(f"- 模型：{model}")
    lines.append(f"- 策略：{strategy}（玩法标签历史先验 + 相似正负样本）")
    summary_text = actual_summary(report, top_n) if include_backtest else ""
    if include_backtest and summary_text:
        lines.append(f"- 回测命中：{summary_text}")
    lines.append("")
    lines.append("**推荐素材**")

    for index, row in enumerate(rows, start=1):
        lines.extend(format_material_line(index, row, include_actual=include_actual))
        lines.append("")

    return "\n".join(lines).strip()


def build_card_payload(title: str, md_text: str) -> dict[str, Any]:
    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True, "enable_forward": True},
            "header": {"title": {"tag": "plain_text", "content": title}, "template": "blue"},
            "elements": [{"tag": "markdown", "content": md_text[:12000]}],
        },
    }


def build_im_card(title: str, md_text: str) -> dict[str, Any]:
    return build_card_payload(title, md_text)["card"]


def resolve_webhook(explicit_webhook: str = "") -> str:
    webhook = (explicit_webhook or "").strip()
    if webhook:
        return webhook
    return (os.getenv("FEISHU_UA_WEBHOOK", "") or os.getenv("FEISHU_BOT_WEBHOOK", "")).strip()


def post_card(webhook: str, title: str, md_text: str) -> requests.Response:
    payload = build_card_payload(title, md_text)
    response = requests.post(webhook, json=payload, timeout=15)
    print(f"[haopeng-topn-card] 推送结果: {response.status_code} {response.text[:200]}")
    response.raise_for_status()
    data = response.json()
    if data.get("code") != 0:
        raise RuntimeError(f"webhook push failed: {data}")
    return response


def default_chat_id() -> str:
    values = dotenv_values(VIDEO_GEN_ENV) if VIDEO_GEN_ENV.exists() else {}
    return str(os.getenv("FEISHU_DAILY_PUSH_CHAT_ID") or values.get("FEISHU_DAILY_PUSH_CHAT_ID") or "").strip()


def mask_id(value: str) -> str:
    text = str(value or "").strip()
    if len(text) <= 10:
        return "***" if text else ""
    return f"{text[:6]}...{text[-4:]}"


def send_im_card(
    *,
    receive_id: str,
    title: str,
    md_text: str,
    receive_id_type: str = "chat_id",
) -> dict[str, Any]:
    from ve_core_play_shadow_report import get_tenant_access_token

    token = get_tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={receive_id_type}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    response = requests.post(
        url,
        headers=headers,
        json={
            "receive_id": receive_id,
            "msg_type": "interactive",
            "content": json.dumps(build_im_card(title, md_text), ensure_ascii=False),
        },
        timeout=20,
    )
    response.raise_for_status()
    data = response.json()
    if data.get("code") != 0:
        raise RuntimeError(f"IM push failed: {data}")
    message_id = str((data.get("data") or {}).get("message_id") or "")
    print(f"[haopeng-topn-card] IM 推送成功: chat_id={mask_id(receive_id)} message_id={message_id}")
    return data


def enrich_results_with_media(report: dict[str, Any]) -> None:
    try:
        from video_enhancer_pipeline_db import _get_conn, init_db
    except Exception as exc:
        print(f"[haopeng-topn-card] 无法加载本地库模块，跳过媒体链接补充: {exc}")
        return

    try:
        init_db()
        conn = _get_conn()
    except Exception as exc:
        print(f"[haopeng-topn-card] 无法打开本地库，跳过媒体链接补充: {exc}")
        return

    target_date = str(report.get("target_date") or "")
    try:
        cur = conn.cursor()
        for row in report.get("results") or []:
            if not isinstance(row, dict):
                continue
            ad_key = str(row.get("ad_key") or "").strip()
            if not ad_key:
                continue
            prefix = ad_key[:16] + "%"
            db_row = None
            if target_date:
                cur.execute(
                    "SELECT product, video_url, preview_img_url, video_duration "
                    "FROM daily_creative_insights WHERE ad_key LIKE ? AND target_date = ? LIMIT 1",
                    (prefix, target_date),
                )
                db_row = cur.fetchone()
            if not db_row:
                cur.execute(
                    "SELECT product, video_url, preview_img_url, video_duration "
                    "FROM creative_library WHERE ad_key LIKE ? LIMIT 1",
                    (prefix,),
                )
                db_row = cur.fetchone()
            if not db_row:
                continue
            values = dict(db_row)
            for key in ("product", "video_url", "preview_img_url", "video_duration"):
                if values.get(key) not in (None, "") and not row.get(key):
                    row[key] = values[key]
            if row.get("preview_img_url") and not row.get("cover_url"):
                row["cover_url"] = row["preview_img_url"]
    finally:
        conn.close()


def enrich_results_from_bitable(report: dict[str, Any], bitable_url: str, reviewer_field: str) -> None:
    if not bitable_url:
        return
    try:
        from ve_core_play_shadow_report import fetch_bitable_rows
    except Exception as exc:
        print(f"[haopeng-topn-card] 无法加载飞书主表读取模块，跳过飞书补链: {exc}")
        return
    try:
        rows = fetch_bitable_rows(bitable_url=bitable_url, reviewer_field=reviewer_field)
    except Exception as exc:
        print(f"[haopeng-topn-card] 飞书主表补链失败，继续使用本地报告: {exc}")
        return
    merge_report_rows_from_source(report, rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="推送 VE 浩鹏采纳预测 TopN 飞书卡片")
    parser.add_argument("--input-json", default="", help="TopN 实验 JSON，默认取最新 *_label_prior.json")
    parser.add_argument("--top-n", type=int, default=10, help="推送条数，默认 10")
    parser.add_argument("--send-mode", choices=["im", "webhook"], default="im", help="发送方式，默认 im")
    parser.add_argument("--chat-id", default="", help="飞书 IM chat_id，默认 FEISHU_DAILY_PUSH_CHAT_ID")
    parser.add_argument("--receive-id-type", default="chat_id", help="飞书 IM receive_id_type，默认 chat_id")
    parser.add_argument("--feishu-webhook", default="", help="飞书群机器人 webhook")
    parser.add_argument("--bitable-url", default="", help="VE 主多维表 URL，用于补视频/封面链接")
    parser.add_argument("--reviewer-field", default="浩鹏接受情况", help="主表接受情况字段名")
    parser.add_argument("--dry-run", action="store_true", help="只打印卡片 Markdown，不推送")
    parser.add_argument("--include-backtest", action="store_true", help="显示回测命中和浩鹏实际结果，默认隐藏")
    parser.add_argument("--no-db-enrich", action="store_true", help="不从本地库补视频/封面链接")
    parser.add_argument("--no-bitable-enrich", action="store_true", help="不从飞书主表补视频/封面链接")
    parser.add_argument("--title", default="", help="覆盖飞书卡片标题")
    return parser.parse_args()


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    load_dotenv(VIDEO_GEN_ENV)
    args = parse_args()
    input_path = Path(args.input_json) if args.input_json else find_default_report_path()
    report = load_report(input_path)
    if not args.no_db_enrich:
        enrich_results_with_media(report)
    bitable_url = (args.bitable_url or os.getenv("VIDEO_ENHANCER_BITABLE_URL") or "").strip()
    if not args.no_bitable_enrich and missing_media_links(report, top_n=args.top_n) and bitable_url:
        enrich_results_from_bitable(report, bitable_url, args.reviewer_field)
    target_date = str(report.get("target_date") or "").strip()
    title = args.title or f"VE浩鹏采纳预测 Top{args.top_n} {target_date}"
    md_text = render_topn_markdown(report, top_n=args.top_n, include_backtest=args.include_backtest)

    if args.dry_run:
        print(f"# {title}\n")
        print(md_text)
        return

    if args.send_mode == "webhook":
        webhook = resolve_webhook(args.feishu_webhook)
        if not webhook:
            raise SystemExit("[haopeng-topn-card] 未配置 FEISHU_UA_WEBHOOK/FEISHU_BOT_WEBHOOK，无法推送。")
        post_card(webhook, title, md_text)
        return

    chat_id = (args.chat_id or default_chat_id()).strip()
    if not chat_id:
        raise SystemExit("[haopeng-topn-card] 未配置 FEISHU_DAILY_PUSH_CHAT_ID 或 --chat-id，无法 IM 推送。")
    send_im_card(
        receive_id=chat_id,
        receive_id_type=args.receive_id_type,
        title=title,
        md_text=md_text,
    )


if __name__ == "__main__":
    main()
