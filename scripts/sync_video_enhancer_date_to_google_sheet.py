"""
仅将「指定日期」的 Video Enhancer 工作流结果同步到 Google Sheet（Apps Script Webhook）。

不推企业微信、不写本地 pipeline DB，只做一次 POST。

默认输入（与 workflow 产物命名一致）：
  data/workflow_video_enhancer_<DATE>_raw.json
  data/ua_suggestion_workflow_video_enhancer_<DATE>.json（可选）

环境变量：
  GOOGLE_SHEET_WEBHOOK_URL  — 必填（或用 --webhook-url 覆盖）

用法：
  cd 项目根目录
  .venv/bin/python3 scripts/sync_video_enhancer_date_to_google_sheet.py --date 2026-03-19

  # 指定文件
  .venv/bin/python3 scripts/sync_video_enhancer_date_to_google_sheet.py \\
    --date 2026-03-19 \\
    --raw data/workflow_video_enhancer_2026-03-19_raw.json \\
    --suggestion-json data/ua_suggestion_workflow_video_enhancer_2026-03-19.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

from path_util import DATA_DIR

load_dotenv()


def _first_seen_day_utc8(c: Dict[str, Any]) -> str | None:
    ts = c.get("first_seen")
    if ts is None:
        return None
    try:
        tz8 = timezone(timedelta(hours=8))
        return datetime.fromtimestamp(int(ts), tz=tz8).strftime("%Y-%m-%d")
    except Exception:
        return None


def _build_rows(target_date: str, raw_payload: Dict[str, Any], filter_first_seen: bool) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in raw_payload.get("items") or []:
        if not isinstance(it, dict):
            continue
        c = it.get("creative") or {}
        if not isinstance(c, dict):
            continue
        if filter_first_seen:
            d = _first_seen_day_utc8(c)
            if d != target_date:
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
    return rows


def _build_cards(suggestion_payload: Dict[str, Any]) -> List[Any]:
    s_obj = suggestion_payload.get("suggestion") if isinstance(suggestion_payload, dict) else {}
    if isinstance(s_obj, dict):
        return list(s_obj.get("方向卡片") or [])
    return []


def sync_once(
    webhook_url: str,
    target_date: str,
    raw_payload: Dict[str, Any],
    suggestion_payload: Dict[str, Any],
    *,
    filter_first_seen: bool,
    soft_fail: bool,
) -> bool:
    """返回 True 表示 HTTP 200 且请求发出成功。"""
    if not webhook_url.strip():
        print("[sheet] 未配置 GOOGLE_SHEET_WEBHOOK_URL，也未传 --webhook-url。", file=sys.stderr)
        return False

    rows = _build_rows(target_date, raw_payload, filter_first_seen)
    cards = _build_cards(suggestion_payload)
    payload = {
        "source": "video_enhancer_workflow",
        "target_date": target_date,
        "rows": rows,
        "cards": cards,
    }

    print(
        f"[sheet] 同步日期={target_date} rows={len(rows)} cards={len(cards)} "
        f"(filter_first_seen_utc8={filter_first_seen})"
    )

    try:
        def _post_once(url: str):
            return requests.post(
                url,
                json=payload,
                timeout=30,
                headers={"Content-Type": "application/json"},
                allow_redirects=False,
            )

        resp = _post_once(webhook_url.strip())
        if resp.status_code in (301, 302, 303, 307, 308) and resp.headers.get("Location"):
            # Google 可能会先重定向到另一端点；requests 对 30x 可能会把 POST 变 GET
            # 这里手动跟随一次，确保仍用 POST。
            redirect_to = resp.headers.get("Location")
            print(f"[sheet] 检测到重定向 {resp.status_code} -> {redirect_to}，重试一次 POST")
            resp = _post_once(redirect_to)
    except Exception as e:
        print(f"[sheet] 请求异常：{e}", file=sys.stderr)
        if not soft_fail:
            sys.exit(1)
        return False

    preview = (resp.text or "")[:400]
    if resp.status_code != 200:
        print(
            f"[sheet] 失败 status={resp.status_code} preview={preview}",
            file=sys.stderr,
        )
        if not soft_fail:
            sys.exit(1)
        return False

    try:
        data = resp.json()
        print(f"[sheet] 成功 resp={str(data)[:300]}")
    except Exception:
        print(f"[sheet] 成功（非 JSON 响应）preview={preview}")
    return True


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="仅按指定日期同步 Video Enhancer 结果到 Google Sheet")
    p.add_argument("--date", required=True, metavar="YYYY-MM-DD", help="要同步的素材日（与 workflow --date 一致）")
    p.add_argument("--raw", default="", help="raw JSON 路径（默认 data/workflow_video_enhancer_<date>_raw.json）")
    p.add_argument(
        "--suggestion-json",
        default="",
        help="UA 建议 JSON（默认 data/ua_suggestion_workflow_video_enhancer_<date>.json，可不存在）",
    )
    p.add_argument(
        "--webhook-url",
        default="",
        help="覆盖环境变量 GOOGLE_SHEET_WEBHOOK_URL",
    )
    p.add_argument(
        "--filter-by-first-seen",
        action="store_true",
        help="仅同步 creative.first_seen 在 UTC+8 下等于 --date 的素材（更严；默认以文件为准整包同步）",
    )
    p.add_argument(
        "--allow-date-mismatch",
        action="store_true",
        help="允许 raw 顶层 target_date 与 --date 不一致（默认会报错退出）",
    )
    p.add_argument(
        "--soft-fail",
        action="store_true",
        help="失败时只打印错误，不以非零退出码退出（便于嵌套脚本）",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    target_date = args.date.strip()

    raw_path = Path(args.raw) if args.raw else DATA_DIR / f"workflow_video_enhancer_{target_date}_raw.json"
    sugg_path = (
        Path(args.suggestion_json)
        if args.suggestion_json
        else DATA_DIR / f"ua_suggestion_workflow_video_enhancer_{target_date}.json"
    )

    if not raw_path.exists():
        print(f"[sheet] raw 文件不存在：{raw_path}", file=sys.stderr)
        sys.exit(1)

    raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
    file_td = raw_payload.get("target_date")
    if file_td and str(file_td) != target_date and not args.allow_date_mismatch:
        print(
            f"[sheet] 文件 target_date={file_td!r} 与 --date={target_date!r} 不一致。"
            f"请检查路径或加 --allow-date-mismatch。",
            file=sys.stderr,
        )
        sys.exit(1)

    suggestion_payload: Dict[str, Any] = {}
    if sugg_path.exists():
        suggestion_payload = json.loads(sugg_path.read_text(encoding="utf-8"))
    else:
        print(f"[sheet] 未找到 UA JSON（可选跳过）：{sugg_path}")

    webhook = (args.webhook_url or os.getenv("GOOGLE_SHEET_WEBHOOK_URL", "")).strip()
    ok = sync_once(
        webhook,
        target_date,
        raw_payload,
        suggestion_payload,
        filter_first_seen=bool(args.filter_by_first_seen),
        soft_fail=bool(args.soft_fail),
    )
    if not ok and args.soft_fail:
        sys.exit(0)
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
