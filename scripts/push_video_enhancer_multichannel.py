"""
多渠道推送与表格同步（基于 workflow 产物）：
1) 企业微信机器人推送（自动按 2048 字节分段）
2) Google Sheet 同步（通过 Apps Script Webhook）

默认读取：
- data/workflow_video_enhancer_<DATE>_raw.json
- data/ua_suggestion_workflow_video_enhancer_<DATE>.md
- data/ua_suggestion_workflow_video_enhancer_<DATE>.json

环境变量：
- WECOM_BOT_WEBHOOK: 企业微信机器人 webhook
- GOOGLE_SHEET_WEBHOOK_URL: Google Apps Script Web App URL（接收 JSON 并写入 Sheet）
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv
import os
import sqlite3

from path_util import DATA_DIR

load_dotenv()

# 复用飞书卡片渲染逻辑，保证两边文案内容一致
from sync_raw_analysis_to_bitable_and_push_card import _render_card_markdown, build_meta_by_ad_from_analysis_payload


def _default_date() -> str:
    return (date.today() - timedelta(days=1)).isoformat()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="企业微信分段推送 + Google Sheet 同步")
    p.add_argument("--date", default=_default_date(), help="目标日期 YYYY-MM-DD（默认昨天）")
    p.add_argument("--raw", default="", help="raw 文件路径（可选）")
    p.add_argument("--suggestion-md", default="", help="UA建议 md 文件路径（可选）")
    p.add_argument("--suggestion-json", default="", help="UA建议 json 文件路径（可选）")
    p.add_argument(
        "--bitable-url",
        default="",
        help="飞书多维表完整链接（含 table 参数）。不传则读取 VIDEO_ENHANCER_BITABLE_URL",
    )
    p.add_argument("--wecom-only", action="store_true", help="仅推企业微信，不同步 Google Sheet")
    p.add_argument("--sheet-only", action="store_true", help="仅同步 Google Sheet，不推企业微信")
    return p.parse_args()


def _resolve_paths(target_date: str, args: argparse.Namespace) -> tuple[Path, Path, Path]:
    raw = Path(args.raw) if args.raw else (DATA_DIR / f"workflow_video_enhancer_{target_date}_raw.json")
    s_md = Path(args.suggestion_md) if args.suggestion_md else (DATA_DIR / f"ua_suggestion_workflow_video_enhancer_{target_date}.md")
    s_json = Path(args.suggestion_json) if args.suggestion_json else (DATA_DIR / f"ua_suggestion_workflow_video_enhancer_{target_date}.json")
    return raw, s_md, s_json


WECOM_MAX_TEXT_BYTES = 4096  # 企业微信单条消息截断字节上限


def _split_by_utf8_bytes(text: str, max_bytes: int = WECOM_MAX_TEXT_BYTES) -> List[str]:
    """
    企业微信 text 内容有长度上限，这里按 utf-8 字节分段，默认每段使用 WECOM_MAX_TEXT_BYTES。
    """
    chunks: List[str] = []
    cur_chars: List[str] = []
    cur_bytes = 0
    for ch in text:
        b = len(ch.encode("utf-8"))
        if cur_bytes + b > max_bytes and cur_chars:
            chunks.append("".join(cur_chars))
            cur_chars = [ch]
            cur_bytes = b
        else:
            cur_chars.append(ch)
            cur_bytes += b
    if cur_chars:
        chunks.append("".join(cur_chars))
    return chunks


def _post_wecom(webhook: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    resp = requests.post(webhook, json=payload, timeout=10)
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}
    if resp.status_code != 200 or data.get("errcode") != 0:
        raise RuntimeError(f"[wecom] 推送失败: status={resp.status_code}, resp={data}")
    return data


def push_wecom_text(webhook: str, text: str) -> None:
    if not webhook:
        print("[wecom] 未配置 WECOM_BOT_WEBHOOK，跳过。")
        return

    # text 类型不支持 markdown 高亮链接，需转换为纯文本样式
    text = _transform_wecom_text(text)

    logical_blocks = _split_wecom_blocks_by_direction(text)
    chunks: List[str] = []
    for b in logical_blocks:
        # 每个“逻辑块”再按字节长度切，避免触发企业微信消息长度限制
        chunks.extend(_split_by_utf8_bytes(b, max_bytes=WECOM_MAX_TEXT_BYTES))

    total = len(chunks)
    for i, part in enumerate(chunks, start=1):
        payload = {"msgtype": "text", "text": {"content": f"[{i}/{total}]\n{part}" if total > 1 else part}}
        _post_wecom(webhook, payload)
    print(f"[wecom] 推送完成，共 {total} 段。")


def push_wecom_markdown(webhook: str, text: str) -> None:
    """
    优先使用 markdown 消息，保留 [视频1](url) / [多维表格链接](url) 的可点击样式。
    """
    if not webhook:
        print("[wecom] 未配置 WECOM_BOT_WEBHOOK，跳过。")
        return

    def _utf8_len(s: str) -> int:
        return len(s.encode("utf-8"))

    def _split_card_md_into_wecom_trend_segments(card_text: str) -> List[str]:
        """
        将卡片 markdown 拆成“趋势段”：
        - 首条：日报标题前缀 + 第一条趋势（避免“广大大日报”单独成消息）
        - 每个“方向卡片”一段（保证单个趋势不会跨两条消息）
        - 最后一段：共性执行建议（如果存在）
        """
        # 企业微信口径：产品对标点不输出，避免信息过载
        lines = [ln for ln in card_text.splitlines() if "产品对标点" not in ln]
        if not lines:
            return [card_text]

        # 方向标题行：**[video enhancer 方向] XXX**
        direction_re = re.compile(r"^\*\*\[video enhancer 方向\]\s+.+\*\*$")
        common_re = re.compile(r"^\*\*共性执行建议\*\*$")

        segments: List[List[str]] = []
        cur: List[str] = []
        prefix: List[str] = []
        in_direction = False

        def flush():
            nonlocal cur
            if cur:
                segments.append(cur)
                cur = []

        for line in lines:
            if common_re.match(line):
                flush()
                cur = [line]
                in_direction = True
                continue

            if direction_re.match(line):
                if not in_direction:
                    # 把标题前缀并入第一条趋势段（避免标题单独一条消息）
                    cur = prefix + [line]
                    prefix = []
                    in_direction = True
                else:
                    flush()
                    cur = [line]
                continue

            if in_direction:
                cur.append(line)
            else:
                prefix.append(line)

        flush()
        if not segments:
            return [card_text]
        return ["\n".join(seg).strip() for seg in segments]

    def _truncate_segment_reference_links(seg: str, max_links: int) -> str:
        """
        将 “🔗 参考链接：...” 行的链接条目截断到 max_links 条，保证段落长度可控。
        """
        lines = seg.splitlines()
        out: List[str] = []
        for line in lines:
            if line.startswith("🔗 参考链接："):
                prefix = "🔗 参考链接："
                links_part = line[len(prefix) :].strip()
                if not links_part:
                    out.append(line)
                    continue
                parts = [p.strip() for p in links_part.split("；") if p.strip()]
                parts = parts[:max_links]
                out.append(prefix + ("；".join(parts) if parts else ""))
            else:
                out.append(line)
        return "\n".join(out).strip()

    def _shrink_until_fit(seg: str, limit_bytes: int) -> str:
        seg_stripped = seg.strip()
        if _utf8_len(seg_stripped) <= limit_bytes:
            return seg_stripped

        # 优先减少参考链接数量，尽量保留完整趋势段结构
        for n in [5, 4, 3, 2, 1]:
            shrunk = _truncate_segment_reference_links(seg_stripped, n)
            if _utf8_len(shrunk) <= limit_bytes:
                return shrunk

        # 极端兜底：仍不够就做硬截断（保证不再跨消息）
        chunks = _split_by_utf8_bytes(seg_stripped, max_bytes=limit_bytes)
        return chunks[0] if chunks else seg_stripped[:limit_bytes]

    def _pack_segments_into_wecom_messages(segments: List[str], limit_bytes: int) -> List[str]:
        # 以“趋势段”为单位打包；不允许趋势段跨消息。
        messages: List[str] = []
        cur_parts: List[str] = []
        cur_bytes = 0

        joiner = "\n\n"

        for seg in segments:
            seg = seg.strip()
            if not seg:
                continue
            seg = _shrink_until_fit(seg, limit_bytes=limit_bytes)
            seg_bytes = _utf8_len(seg)

            if not cur_parts:
                cur_parts = [seg]
                cur_bytes = seg_bytes
                continue

            prospective = cur_bytes + _utf8_len(joiner) + seg_bytes
            if prospective > limit_bytes:
                messages.append("\n\n".join(cur_parts).strip())
                cur_parts = [seg]
                cur_bytes = seg_bytes
            else:
                cur_parts.append(seg)
                cur_bytes = prospective

        if cur_parts:
            messages.append("\n\n".join(cur_parts).strip())
        return [m for m in messages if m]

    segments = _split_card_md_into_wecom_trend_segments(text)
    messages = _pack_segments_into_wecom_messages(segments, limit_bytes=WECOM_MAX_TEXT_BYTES)

    total = len(messages)
    for i, part in enumerate(messages, start=1):
        content = f"[{i}/{total}]\n{part}" if total > 1 else part
        payload = {"msgtype": "markdown", "markdown": {"content": content}}
        _post_wecom(webhook, payload)
    print(f"[wecom] markdown 推送完成，共 {total} 段。")


def _split_wecom_blocks_by_direction(text: str) -> List[str]:
    """
    将飞书卡片 markdown 拆成更“业务化”的企业微信消息块：
    - 头部摘要（含日报/产品分布/估值筛选规则）
    - 每个“方向卡片”一条
    - 最后一条“共性执行建议”
    """

    lines = text.splitlines()
    if not lines:
        return [text]

    # 支持两种“趋势标题”格式：
    # 1) 飞书卡片 markdown：**[video enhancer 方向] XXX**
    # 2) 企业微信精简模板：**XXX**（注意：不能把 **Video Enhancer 方向卡片（精简版）** 这种表头当成趋势标题）
    feishu_direction_re = re.compile(r"^\*\*\[video enhancer 方向\]\s+.+\*\*$")
    compact_direction_re = re.compile(r"^\*\*[^*]+\*\*$")
    common_re = re.compile(r"^\*\*共性执行建议\*\*$")

    blocks: List[str] = []
    cur: List[str] = []

    def flush():
        nonlocal cur
        if cur:
            blocks.append("\n".join(cur).strip())
            cur = []

    in_common = False
    direction_started = False
    for line in lines:
        if common_re.match(line):
            flush()
            cur.append(line)
            in_common = True
            continue

        if not in_common and (feishu_direction_re.match(line) or compact_direction_re.match(line)):
            # 过滤掉“模板表头/其他加粗块”，避免把它当成趋势分隔点
            if compact_direction_re.match(line):
                if (
                    "Video Enhancer" in line
                    or "方向卡片" in line
                    or "共性执行建议" in line
                    or "[video enhancer" in line
                ):
                    cur.append(line)
                    continue
            if direction_started:
                flush()
                cur.append(line)
            else:
                # 首个趋势之前的标题行要跟首条趋势一起发
                cur.append(line)
                direction_started = True
            continue

        cur.append(line)

    flush()
    # 兜底：若解析失败，退回原始文本
    return blocks if blocks else [text]


def _transform_wecom_text(text: str) -> str:
    # 企业微信 text 类型不解析 markdown 链接；此处用于兜底降级，
    # 需要“收回 URL”，避免展示完整链接（不保证可点击）。
    text = text.replace("🔗 参考链接：", "")

    # 将 markdown 链接 [视频1](http...) 改成 视频1：http...
    # 兼容多个分隔符：视频1：url；视频2：url
    text = re.sub(r"\[视频(\d+)\]\((https?://[^)]+)\)", r"视频\1", text)
    # 其他 markdown 链接统一改为“文案”，避免输出 url
    text = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r"\1", text)

    return text


def _build_summary_text(target_date: str, raw_payload: Dict[str, Any], suggestion_md: str) -> str:
    # 保留旧函数签名：直接把 suggestion_md 追加到 summary 前面
    # 新逻辑中会改为统一渲染卡片，避免两边内容不一致
    return _build_summary_text_from_intro(target_date, raw_payload) + "\n\n" + (suggestion_md.strip() or "（无）")


def _build_summary_text_from_intro(target_date: str, raw_payload: Dict[str, Any]) -> str:
    _ = raw_payload
    return f"【Video Enhancer 日报】{target_date}"


def _pick_media_links_for_card(
    card: Dict[str, Any],
    meta_by_ad: Dict[str, Dict[str, Any]],
    max_n: int = 5,
) -> tuple[List[str], List[str]]:
    """从聚类 card 的参考 ad_key 映射出视频/图片 URL（分别编号）。"""
    video_urls: List[str] = []
    image_urls: List[str] = []
    seen_v: set[str] = set()
    seen_i: set[str] = set()
    total = 0
    raw_links = card.get("参考链接") or []
    if not isinstance(raw_links, list):
        return video_urls, image_urls
    for x in raw_links:
        ad_key = str(x or "").strip()
        if not ad_key:
            continue
        meta = meta_by_ad.get(ad_key) or {}
        ct = str(meta.get("creative_type") or "").strip()
        vu = str(meta.get("video_url") or "").strip()
        iu = str(meta.get("image_url") or "").strip()
        pu = str(meta.get("preview_img_url") or "").strip()
        if not ct:
            ct = "image" if (not vu and (iu or pu)) else "video"

        if ct == "image":
            # 纯图片素材优先用 image_url；按你的口径视频素材不回填封面图
            url = iu or ""
            if url and url not in seen_i and total < max_n:
                seen_i.add(url)
                image_urls.append(url)
                total += 1
        else:
            # 有 video_url 就只放 video_url
            url = vu or ""
            if url and url not in seen_v and total < max_n:
                seen_v.add(url)
                video_urls.append(url)
                total += 1

        if total >= max_n:
            break

    return video_urls, image_urls


def _build_wecom_compact_markdown(
    target_date: str,
    suggestion_payload: Dict[str, Any],
    meta_by_ad: Dict[str, Dict[str, Any]],
    bitable_url: str,
) -> str:
    # 标题只应出现在第一条消息里（由 _split_wecom_blocks_by_direction 附着到首个趋势块）
    lines: List[str] = [f"广大大「{target_date}」新素材日报", ""]
    s_obj = suggestion_payload.get("suggestion") if isinstance(suggestion_payload, dict) else None
    cards = s_obj.get("方向卡片") if isinstance(s_obj, dict) else None
    if not isinstance(cards, list) or not cards:
        lines.append("- 今日无可用趋势方向")
    else:
        for card in cards:
            if not isinstance(card, dict):
                continue
            name = str(card.get("方向名称") or "未命名趋势")
            lines.append(f"**{name}**")
            v_urls, i_urls = _pick_media_links_for_card(card, meta_by_ad, max_n=5)
            if v_urls or i_urls:
                parts: List[str] = []
                parts += [f"[视频{i}]({u})" for i, u in enumerate(v_urls, start=1)]
                parts += [f"[图片{i}]({u})" for i, u in enumerate(i_urls, start=1)]
                lines.append(f"🔗 参考链接：{'；'.join(parts)}")
            else:
                lines.append("🔗 参考链接：（无）")
            lines.append("")
    if bitable_url:
        lines.append(f"[多维表格链接]({bitable_url})")
    return "\n".join(lines).strip()


def sync_to_google_sheet(webhook_url: str, target_date: str, raw_payload: Dict[str, Any], suggestion_payload: Dict[str, Any]) -> None:
    """
    通过 Google Apps Script Webhook 同步。
    你可在 Apps Script 里按 payload.rows / payload.cards 写入两个 sheet。
    """
    # 优先：Service Account + gspread 直接写入（替代原 webhook 方式）
    sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
    sa_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()
    rows_sheet_name = os.getenv("GOOGLE_SHEET_ROWS_SHEETNAME", "video_enhancer_rows").strip()
    cards_sheet_name = os.getenv("GOOGLE_SHEET_CARDS_SHEETNAME", "video_enhancer_cards").strip()
    if sheet_id and sa_file:
        try:
            import gspread  # type: ignore
            from google.oauth2.service_account import Credentials  # type: ignore
        except Exception as e:
            print(f"[sheet] 未安装 gspread/google-auth，无法走 Service Account 写入：{e}")
            # 如果未配置 webhook，再直接返回
            if not webhook_url:
                return
        else:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            creds = Credentials.from_service_account_file(sa_file, scopes=scopes)
            client = gspread.authorize(creds)
            ss = client.open_by_key(sheet_id)

            def get_or_create_ws(name: str, header: List[str]) -> Any:
                try:
                    ws = ss.worksheet(name)
                except Exception:
                    ws = ss.add_worksheet(title=name, rows=1000, cols=max(15, len(header) + 2))
                # 确保有表头
                values = ws.get_all_values()
                if not values:
                    ws.append_row(header)
                else:
                    if len(values[0]) < len(header) or values[0] != header[: len(values[0])]:
                        # 简单策略：若表头不存在则追加；不强制覆盖已有表头
                        if "target_date" not in values[0]:
                            # 不使用 delete_rows(1, ws.row_count)，避免触发「不能删空整表」的限制
                            ws.clear()
                            ws.append_row(header)
                return ws

            rows_header = [
                "target_date",
                "product",
                "appid",
                "ad_key",
                "platform",
                "heat",
                "all_exposure_value",
                "impression",
                "video_duration",
                "video_url",
                "preview_img_url",
            ]
            cards_header = ["target_date", "方向名称", "JSON"]

            ws_rows = get_or_create_ws(rows_sheet_name, rows_header)
            ws_cards = get_or_create_ws(cards_sheet_name, cards_header)

            # 先清空同一日期已有记录（避免重复）
            # 不用 delete_rows：Google Sheets API 禁止删除到空表
            def clear_rows_by_date(ws: Any, date_str: str, header_name: str) -> None:
                values = ws.get_all_values()
                if not values or len(values) < 2:
                    return
                header = values[0]
                if not header:
                    return
                try:
                    idx = header.index(header_name)
                except ValueError:
                    return

                header_len = max(1, len(header))

                def col_to_letter(col_1_based: int) -> str:
                    # 1 -> A, 26 -> Z, 27 -> AA ...
                    s = ""
                    n = col_1_based
                    while n > 0:
                        n, r = divmod(n - 1, 26)
                        s = chr(ord("A") + r) + s
                    return s

                ranges: List[str] = []
                for i in range(1, len(values)):
                    row_idx = i + 1  # worksheet row number
                    if len(values[i]) > idx and values[i][idx] == date_str:
                        start_col = 1
                        end_col = header_len
                        ranges.append(
                            f"{col_to_letter(start_col)}{row_idx}:{col_to_letter(end_col)}{row_idx}"
                        )
                if ranges:
                    ws.batch_clear(ranges)

            clear_rows_by_date(ws_rows, target_date, "target_date")
            clear_rows_by_date(ws_cards, target_date, "target_date")

            # 以后默认从 pipeline DB 同步（比 raw/json 更稳）
            db_path = DATA_DIR / "video_enhancer_pipeline.db"
            rows_to_write: List[List[Any]] = []
            cards_to_write: List[List[Any]] = []
            db_ok = db_path.exists()

            if db_ok:
                try:
                    conn = sqlite3.connect(str(db_path))
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()

                    # rows：daily_creative_insights
                    cur.execute(
                        """
                        SELECT product, appid, ad_key, platform, video_url, preview_img_url,
                               video_duration, heat, all_exposure_value, impression
                        FROM daily_creative_insights
                        WHERE target_date = ?
                        """,
                        (target_date,),
                    )
                    ins_rows = cur.fetchall()
                    if ins_rows:
                        for rr in ins_rows:
                            rows_to_write.append(
                                [
                                    target_date,
                                    rr["product"] or "",
                                    rr["appid"] or "",
                                    rr["ad_key"] or "",
                                    rr["platform"] or "",
                                    rr["heat"] or 0,
                                    rr["all_exposure_value"] or 0,
                                    rr["impression"] or 0,
                                    rr["video_duration"] or 0,
                                    rr["video_url"] or "",
                                    rr["preview_img_url"] or "",
                                ]
                            )

                    # cards：daily_ua_push_content
                    cur.execute(
                        """
                        SELECT direction_name, core_summary, background, ua_suggestion,
                               product_benchmark, risk_note, trend_judgement, reference_links_json
                        FROM daily_ua_push_content
                        WHERE target_date = ?
                        """,
                        (target_date,),
                    )
                    card_rows = cur.fetchall()
                    if card_rows:
                        for cr in card_rows:
                            refs_json = cr["reference_links_json"]
                            refs: Any = []
                            if refs_json:
                                try:
                                    refs = json.loads(refs_json)
                                except Exception:
                                    refs = []
                            card_obj = {
                                "方向名称": cr["direction_name"] or "",
                                "核心数据摘要": cr["core_summary"] or "",
                                "背景": cr["background"] or "",
                                "UA建议": cr["ua_suggestion"] or "",
                                "产品对标点": cr["product_benchmark"] or "",
                                "风险提示": cr["risk_note"] or "",
                                "趋势阶段判断": cr["trend_judgement"] or "",
                                "参考链接": refs if isinstance(refs, list) else [],
                            }
                            cards_to_write.append(
                                [
                                    target_date,
                                    cr["direction_name"] or "",
                                    json.dumps(card_obj, ensure_ascii=False),
                                ]
                            )
                    conn.close()
                except Exception:
                    # DB 读失败时回退 raw/json
                    rows_to_write = []
                    cards_to_write = []

            # DB 没有数据时回退：raw_payload + suggestion_payload
            if not rows_to_write:
                for it in raw_payload.get("items") or []:
                    if not isinstance(it, dict):
                        continue
                    c = it.get("creative") or {}
                    if not isinstance(c, dict):
                        continue
                    video_url = ""
                    for r in c.get("resource_urls") or []:
                        if isinstance(r, dict) and r.get("video_url"):
                            video_url = str(r.get("video_url") or "")
                            break
                    rows_to_write.append(
                        [
                            target_date,
                            it.get("product") or "",
                            it.get("appid") or "",
                            c.get("ad_key") or "",
                            c.get("platform") or "",
                            c.get("heat") or 0,
                            c.get("all_exposure_value") or 0,
                            c.get("impression") or 0,
                            c.get("video_duration") or 0,
                            video_url,
                            c.get("preview_img_url") or "",
                        ]
                    )

            if not cards_to_write:
                s_obj = suggestion_payload.get("suggestion") if isinstance(suggestion_payload, dict) else {}
                if isinstance(s_obj, dict):
                    cards = s_obj.get("方向卡片") or []
                else:
                    cards = []
                if isinstance(cards, list):
                    for card in cards:
                        if not isinstance(card, dict):
                            continue
                        cards_to_write.append(
                            [
                                target_date,
                                card.get("方向名称") or "",
                                json.dumps(card, ensure_ascii=False),
                            ]
                        )

            if rows_to_write:
                ws_rows.append_rows(rows_to_write, value_input_option="RAW")
            if cards_to_write:
                ws_cards.append_rows(cards_to_write, value_input_option="RAW")

            print(
                f"[sheet] gspread 写入完成：rows={len(rows_to_write)} cards={len(cards_to_write)} "
                f"-> sheet_id={sheet_id}"
            )
            return
    # 回退：hook 方式
    if not webhook_url:
        print("[sheet] 未配置 GOOGLE_SHEET_WEBHOOK_URL 且未配置 GOOGLE_SHEET_ID/GOOGLE_SERVICE_ACCOUNT_FILE，跳过。")
        return

    rows: List[Dict[str, Any]] = []
    for it in raw_payload.get("items") or []:
        if not isinstance(it, dict):
            continue
        c = it.get("creative") or {}
        if not isinstance(c, dict):
            continue
        video_url = ""
        for r in c.get("resource_urls") or []:
            if isinstance(r, dict) and r.get("video_url"):
                video_url = str(r.get("video_url") or "")
                break
        rows.append(
            {
                "target_date": target_date,
                "product": it.get("product"),
                "appid": it.get("appid"),
                "ad_key": c.get("ad_key"),
                "platform": c.get("platform"),
                "heat": c.get("heat"),
                "all_exposure_value": c.get("all_exposure_value"),
                "impression": c.get("impression"),
                "video_duration": c.get("video_duration"),
                "video_url": video_url,
                "preview_img_url": c.get("preview_img_url"),
            }
        )

    cards = []
    s_obj = suggestion_payload.get("suggestion") if isinstance(suggestion_payload, dict) else {}
    if isinstance(s_obj, dict):
        cards = s_obj.get("方向卡片") or []

    payload = {
        "source": "video_enhancer_workflow",
        "target_date": target_date,
        "rows": rows,
        "cards": cards,
    }
    # 注意：Google Apps Script Webhook 可能会因部署失效/URL 错误返回 HTML 页面，
    # 这时不应中断整条工作流（企业微信/DB 已经写好），仅打印错误便于排查。
    try:
        def _post_once(url: str):
            return requests.post(
                url,
                json=payload,
                timeout=20,
                headers={"Content-Type": "application/json"},
                allow_redirects=False,
            )

        resp = _post_once(webhook_url)
        if resp.status_code in (301, 302, 303, 307, 308) and resp.headers.get("Location"):
            redirect_to = resp.headers.get("Location")
            print(f"[sheet] 检测到重定向 {resp.status_code} -> {redirect_to}，重试一次 POST")
            resp = _post_once(redirect_to)
    except Exception as e:
        print(f"[sheet] 请求异常：{e}")
        return

    preview = (resp.text or "")[:300]
    if resp.status_code != 200:
        print(
            f"[sheet] 同步失败：status={resp.status_code}, webhook_url={webhook_url}, "
            f"resp_preview={preview}"
        )
        return

    # 尝试解析 JSON，否则也不阻断
    try:
        data = resp.json()
        print(f"[sheet] 同步请求已发送，rows={len(rows)}, cards={len(cards)}; resp={str(data)[:200]}")
    except Exception:
        print(f"[sheet] 同步请求已发送，rows={len(rows)}, cards={len(cards)}; resp_preview={preview}")


def main() -> None:
    args = parse_args()
    target_date = args.date
    raw_path, s_md_path, s_json_path = _resolve_paths(target_date, args)

    if not raw_path.exists():
        raise FileNotFoundError(f"raw 文件不存在: {raw_path}")
    raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
    suggestion_md = s_md_path.read_text(encoding="utf-8") if s_md_path.exists() else ""
    suggestion_payload = json.loads(s_json_path.read_text(encoding="utf-8")) if s_json_path.exists() else {}

    wecom_webhook = os.getenv("WECOM_BOT_WEBHOOK", "").strip()
    sheet_webhook = os.getenv("GOOGLE_SHEET_WEBHOOK_URL", "").strip()
    bitable_url = (args.bitable_url or "").strip() or os.getenv("VIDEO_ENHANCER_BITABLE_URL", "").strip()

    # 企业微信首条消息标题（并尽量复用飞书同款卡片细节内容）
    intro_md = f"广大大「{target_date}」新素材日报"

    # 为“参考链接”兜底解析，需要分析文件提供 ad_key -> video_url
    # 默认分析文件名与 workflow_video_enhancer_full_pipeline.py 保持一致
    analysis_path = DATA_DIR / f"video_analysis_workflow_video_enhancer_{target_date}_raw.json"
    meta_by_ad: Dict[str, Dict[str, Any]] = {}
    if analysis_path.exists():
        analysis_payload = json.loads(analysis_path.read_text(encoding="utf-8"))
        meta_by_ad = build_meta_by_ad_from_analysis_payload(analysis_payload)

    suggestion_json = suggestion_payload if isinstance(suggestion_payload, dict) else {}
    card_md = _render_card_markdown(
        suggestion_json=suggestion_json,
        suggestion_md=suggestion_md,
        meta_by_ad=meta_by_ad,
        intro_md=intro_md,
        bitable_url=bitable_url,
        include_ua_suggestion=True,
        include_product_benchmark=False,
    )

    summary_text = card_md

    if not args.sheet_only:
        try:
            # 优先 markdown，满足“名称高亮可点击链接”诉求
            push_wecom_markdown(wecom_webhook, summary_text)
        except Exception as e:
            # markdown 推送失败时不再回退（text 不具备 markdown 链接可点击渲染）
            print(f"[wecom] markdown 推送失败，已跳过本次 wecom 推送：{e}")
    if not args.wecom_only:
        sync_to_google_sheet(sheet_webhook, target_date, raw_payload, suggestion_payload)

    print("[multi] 完成。")


if __name__ == "__main__":
    main()

