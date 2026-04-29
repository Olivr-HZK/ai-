"""
多渠道推送与表格同步（基于 workflow 产物）：
1) 企业微信机器人推送（自动按字节分段，使用新日报格式）
2) Google Sheet 同步（通过 Apps Script Webhook）

环境变量：
- WECOM_BOT_WEBHOOK: 企业微信机器人 webhook
- GOOGLE_SHEET_WEBHOOK_URL: Google Apps Script Web App URL（接收 JSON 并写入 Sheet）
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

from path_util import DATA_DIR
from push_video_enhancer_feishu_card_only import (
    _render_daily_card_markdown,
    _extract_one_liner,
    PRODUCT_THEMES,
)
from video_enhancer_pipeline_db import _get_conn, init_db

load_dotenv()


def _default_date() -> str:
    return (date.today() - timedelta(days=1)).isoformat()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="企业微信分段推送 + Google Sheet 同步")
    p.add_argument("--date", default=_default_date(), help="目标日期 YYYY-MM-DD（默认昨天）")
    p.add_argument("--raw", default="", help="raw 文件路径（可选）")
    p.add_argument("--suggestion-md", default="", help="UA建议 md 文件路径（可选）")
    p.add_argument("--suggestion-json", default="", help="UA建议 json 文件路径（可选）")
    p.add_argument("--wecom-only", action="store_true", help="仅推企业微信，不同步 Google Sheet")
    p.add_argument("--sheet-only", action="store_true", help="仅同步 Google Sheet，不推企业微信")
    return p.parse_args()


def _resolve_paths(target_date: str, args: argparse.Namespace) -> tuple[Path, Path, Path]:
    raw = Path(args.raw) if args.raw else (DATA_DIR / f"workflow_video_enhancer_{target_date}_raw.json")
    s_md = Path(args.suggestion_md) if args.suggestion_md else (DATA_DIR / f"ua_suggestion_workflow_video_enhancer_{target_date}.md")
    s_json = Path(args.suggestion_json) if args.suggestion_json else (DATA_DIR / f"ua_suggestion_workflow_video_enhancer_{target_date}.json")
    return raw, s_md, s_json


WECOM_MAX_TEXT_BYTES = 4096


def _split_by_utf8_bytes(text: str, max_bytes: int = WECOM_MAX_TEXT_BYTES) -> List[str]:
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


def push_wecom_markdown(webhook: str, text: str) -> None:
    """企业微信 markdown 推送，按字节分段。"""
    if not webhook:
        print("[wecom] 未配置 WECOM_BOT_WEBHOOK，跳过。")
        return

    chunks = _split_by_utf8_bytes(text, max_bytes=WECOM_MAX_TEXT_BYTES)
    total = len(chunks)
    for i, part in enumerate(chunks, start=1):
        content = f"[{i}/{total}]\n{part}" if total > 1 else part
        payload = {"msgtype": "markdown", "markdown": {"content": content}}
        _post_wecom(webhook, payload)
    print(f"[wecom] markdown 推送完成，共 {total} 段。")


def _build_daily_card_from_db(target_date: str) -> str:
    """从 DB 读取新素材，用新日报格式渲染 markdown（与飞书卡片一致）。"""
    init_db()
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT ad_key, product, creative_type, best_impression, best_all_exposure_value, "
        "best_heat, video_url, preview_img_url, video_duration "
        "FROM creative_library WHERE first_target_date = ? ORDER BY best_impression DESC",
        (target_date,),
    )
    new_items = [dict(row) for row in cur.fetchall()]

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
    return _render_daily_card_markdown(target_date, new_items, {})


def sync_to_google_sheet(webhook_url: str, target_date: str, raw_payload: Dict[str, Any], suggestion_payload: Dict[str, Any]) -> None:
    """
    通过 Google Apps Script Webhook 同步。
    你可在 Apps Script 里按 payload.rows / payload.cards 写入两个 sheet。
    """
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
                values = ws.get_all_values()
                if not values:
                    ws.append_row(header)
                else:
                    if len(values[0]) < len(header) or values[0] != header[: len(values[0])]:
                        if "target_date" not in values[0]:
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
                    s = ""
                    n = col_1_based
                    while n > 0:
                        n, r = divmod(n - 1, 26)
                        s = chr(ord("A") + r) + s
                    return s

                ranges: List[str] = []
                for i in range(1, len(values)):
                    row_idx = i + 1
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

            db_path = DATA_DIR / "video_enhancer_pipeline.db"
            rows_to_write: List[List[Any]] = []
            cards_to_write: List[List[Any]] = []
            db_ok = db_path.exists()

            if db_ok:
                try:
                    conn = sqlite3.connect(str(db_path))
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()

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
                    rows_to_write = []
                    cards_to_write = []

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
    suggestion_payload = json.loads(s_json_path.read_text(encoding="utf-8")) if s_json_path.exists() else {}

    wecom_webhook = os.getenv("WECOM_BOT_WEBHOOK", "").strip()
    sheet_webhook = os.getenv("GOOGLE_SHEET_WEBHOOK_URL", "").strip()

    card_md = _build_daily_card_from_db(target_date)

    if not args.sheet_only:
        try:
            push_wecom_markdown(wecom_webhook, card_md)
        except Exception as e:
            print(f"[wecom] markdown 推送失败，已跳过本次 wecom 推送：{e}")
    if not args.wecom_only:
        sync_to_google_sheet(sheet_webhook, target_date, raw_payload, suggestion_payload)

    print("[multi] 完成。")


if __name__ == "__main__":
    main()
