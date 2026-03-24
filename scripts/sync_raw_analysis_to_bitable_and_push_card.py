"""
把 raw + 灵感分析写入指定飞书多维表，并把统一 UA 建议推送到飞书卡片。

输入：
- raw: data/test_video_enhancer_2_2026-03-18_raw.json
- analysis: data/video_analysis_test_video_enhancer_2_2026-03-18_raw.json
- suggestion_md: data/video_enhancer_ua_suggestion_from_analysis.md

用法：
python scripts/sync_raw_analysis_to_bitable_and_push_card.py \
  --url "https://scnmrtumk0zm.feishu.cn/base/W8QMbUR1vaiUGUskOF2cwnXenBe?table=tblRAiOqhIyJEAS9&view=vewd67ZK4J"
"""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse

import lark_oapi as lark
import requests
from dotenv import load_dotenv
from lark_oapi.api.drive.v1.model import (
    UploadAllMediaRequest,
    UploadAllMediaRequestBody,
    UploadAllMediaResponse,
)

from path_util import DATA_DIR
from video_enhancer_pipeline_db import init_db as init_pipeline_db, update_push_status

load_dotenv()

FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")
FEISHU_BOT_WEBHOOK = os.getenv("FEISHU_UA_WEBHOOK", "") or os.getenv("FEISHU_BOT_WEBHOOK", "")
BATCH_SIZE = 200

FIELD_DEFS: List[Dict[str, Any]] = [
    {"field_name": "标题", "type": 1},
    {"field_name": "类目", "type": 1},
    {"field_name": "产品", "type": 1},
    {"field_name": "广告主", "type": 1},
    {"field_name": "正文（中文）", "type": 1},
    {"field_name": "平台", "type": 1},
    {"field_name": "视频链接", "type": 1},
    {"field_name": "封面图链接", "type": 1},
    {"field_name": "封面图", "type": 17},
    {"field_name": "AI分析结果", "type": 1},
    {"field_name": "UA灵感借鉴", "type": 1},
    {"field_name": "抓取日期", "type": 5},
    {"field_name": "创建时间", "type": 5},
    {"field_name": "更新时间", "type": 5},
    {"field_name": "视频时长", "type": 2},
    {"field_name": "接受情况", "type": 3, "options": [{"name": "待定"}, {"name": "删除"}, {"name": "接受"}]},
    {"field_name": "我方产品", "type": 1},
    {"field_name": "广告ID", "type": 1},
    {"field_name": "素材标签", "type": 1},
]

CLUSTER_FIELD_DEFS: List[Dict[str, Any]] = [
    {"field_name": "标题", "type": 1},
    {"field_name": "抓取日期", "type": 5},
    {"field_name": "背景", "type": 1},
    {"field_name": "建议", "type": 1},
    {"field_name": "产品对标点", "type": 1},
    {"field_name": "风险提示", "type": 1},
    {"field_name": "视频链接", "type": 1},
    {"field_name": "接受情况", "type": 3, "options": [{"name": "待定"}, {"name": "删除"}, {"name": "接受"}]},
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="同步 raw+灵感分析到多维表并推送飞书卡片")
    p.add_argument("--url", required=True, help="飞书多维表完整链接（含 table 参数）")
    p.add_argument(
        "--cluster-url",
        default="",
        help="聚类结果多维表完整链接（含 table 参数）。每个方向一条，含标题/视频链接/接受情况",
    )
    p.add_argument("--raw", default=str(DATA_DIR / "test_video_enhancer_2_2026-03-18_raw.json"))
    p.add_argument("--analysis", default=str(DATA_DIR / "video_analysis_test_video_enhancer_2_2026-03-18_raw.json"))
    p.add_argument("--suggestion-json", default=str(DATA_DIR / "video_enhancer_ua_suggestion_from_analysis.json"))
    p.add_argument("--suggestion-md", default=str(DATA_DIR / "video_enhancer_ua_suggestion_from_analysis.md"))
    p.add_argument("--no-card", action="store_true", help="只同步表，不发卡片")
    return p.parse_args()


def parse_bitable_url(url: str) -> tuple[str, str]:
    parsed = urlparse(url.strip())
    parts = [p for p in parsed.path.split("/") if p]
    app_token = parts[1] if len(parts) >= 2 and parts[0] == "base" else ""
    table_id = (parse_qs(parsed.query or "").get("table") or [""])[0]
    if not app_token or not table_id:
        raise RuntimeError(f"无法从链接解析 app_token/table_id: {url}")
    return app_token, table_id


def get_tenant_access_token() -> str:
    if not FEISHU_APP_ID or not FEISHU_APP_SECRET:
        raise RuntimeError("请在 .env 配置 FEISHU_APP_ID / FEISHU_APP_SECRET")
    resp = requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"get tenant_access_token failed: {data}")
    return data["tenant_access_token"]


def get_existing_field_names(access_token: str, app_token: str, table_id: str) -> set[str]:
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json; charset=utf-8"}
    out: set[str] = set()
    page_token: str | None = None
    while True:
        params: Dict[str, Any] = {}
        if page_token:
            params["page_token"] = page_token
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"list fields failed: {data}")
        data_obj = data.get("data") or {}
        items = data_obj.get("items") or data_obj.get("fields") or []
        for it in items:
            name = it.get("field_name")
            if name:
                out.add(name)
        if not data_obj.get("has_more"):
            break
        page_token = data_obj.get("page_token")
    return out


def create_field(access_token: str, app_token: str, table_id: str, field: Dict[str, Any]) -> None:
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json; charset=utf-8"}
    body: Dict[str, Any] = {"field_name": field["field_name"], "type": int(field["type"])}
    if field.get("options"):
        body["options"] = field["options"]
    resp = requests.post(url, headers=headers, json=body, timeout=10)
    try:
        data = resp.json()
    except Exception:
        data = {"raw_text": resp.text}
    if resp.status_code != 200 or data.get("code") != 0:
        print(f"[sync] 创建字段失败 {field['field_name']}: {data}")
    else:
        print(f"[sync] 已创建字段：{field['field_name']}")


def ensure_fields(access_token: str, app_token: str, table_id: str) -> None:
    existing = get_existing_field_names(access_token, app_token, table_id)
    for f in FIELD_DEFS:
        if f["field_name"] not in existing:
            create_field(access_token, app_token, table_id, f)


def ensure_cluster_fields(access_token: str, app_token: str, table_id: str) -> None:
    existing = get_existing_field_names(access_token, app_token, table_id)
    for f in CLUSTER_FIELD_DEFS:
        if f["field_name"] not in existing:
            create_field(access_token, app_token, table_id, f)


_LARK_CLIENT: lark.Client | None = None
_IMAGE_CACHE: Dict[str, str] = {}


def get_lark_client() -> lark.Client:
    global _LARK_CLIENT
    if _LARK_CLIENT is None:
        _LARK_CLIENT = (
            lark.Client.builder()
            .app_id(FEISHU_APP_ID)
            .app_secret(FEISHU_APP_SECRET)
            .log_level(lark.LogLevel.ERROR)
            .build()
        )
    return _LARK_CLIENT


def upload_image_as_attachment(image_url: str, app_token: str) -> str | None:
    if not image_url:
        return None
    if image_url in _IMAGE_CACHE:
        return _IMAGE_CACHE[image_url]
    try:
        img = requests.get(image_url, timeout=15)
        img.raise_for_status()
    except Exception as e:
        print(f"[sync] 下载封面图失败: {e}")
        return None
    filename = urlparse(image_url).path.split("/")[-1] or "image.jpg"
    body = (
        UploadAllMediaRequestBody.builder()
        .file_name(filename)
        .parent_type("bitable")
        .parent_node(app_token)
        .size(len(img.content))
        .checksum("")
        .extra("")
        .file(BytesIO(img.content))
        .build()
    )
    req = UploadAllMediaRequest.builder().request_body(body).build()
    resp: UploadAllMediaResponse = get_lark_client().drive.v1.media.upload_all(req)
    if resp.success() and resp.data and getattr(resp.data, "file_token", None):
        tk = resp.data.file_token
        _IMAGE_CACHE[image_url] = tk
        return tk
    return None


def batch_create_records(access_token: str, app_token: str, table_id: str, records: List[Dict[str, Any]]) -> None:
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json; charset=utf-8"}
    params = {"user_id_type": "open_id", "client_token": str(uuid.uuid4())}
    resp = requests.post(url, headers=headers, params=params, json={"records": records}, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"batch_create failed: {data}")


def to_ms_from_date_str(s: str) -> int | None:
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def to_ms_from_unix_sec(v: Any) -> int | None:
    try:
        return int(int(v) * 1000)
    except Exception:
        return None


def pick_video_url(creative: Dict[str, Any]) -> str:
    if creative.get("video_url"):
        return str(creative["video_url"])
    for r in creative.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("video_url"):
            return str(r["video_url"])
    return ""


def pick_video_urls(creative: Dict[str, Any], max_n: int = 5) -> List[str]:
    urls: List[str] = []
    if creative.get("video_url"):
        urls.append(str(creative["video_url"]))
    for r in creative.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("video_url"):
            urls.append(str(r["video_url"]))
    # 去重并保序
    out: List[str] = []
    seen: set[str] = set()
    for u in urls:
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
        if len(out) >= max_n:
            break
    return out


def format_video_links(urls: List[str], max_n: int = 5) -> str:
    picked = [u for u in (urls or []) if u][:max_n]
    if not picked:
        return ""
    # 多维表单元格不会解析 markdown 链接，改为“视频1：https://...”形式，
    # 让 URL 由多维表自动识别为可点击链接。
    return "；".join([f"视频{i}：{u}" for i, u in enumerate(picked, start=1)])


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
    resp = requests.post(webhook, json=card, timeout=10)
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}
    if resp.status_code != 200 or data.get("code") != 0:
        raise RuntimeError(f"飞书卡片推送失败: status={resp.status_code}, resp={data}")
    print("[card] 推送成功。")


def _card_video_links(video_urls: List[str], max_n: int = 5) -> str:
    picked = [u for u in video_urls if u][:max_n]
    if not picked:
        return "（无）"
    links = [f"[视频{i}]({u})" for i, u in enumerate(picked, start=1)]
    return "；".join(links)


def _render_card_markdown(
    suggestion_json: Dict[str, Any] | None,
    suggestion_md: str,
    adkey_to_video: Dict[str, str],
    fallback_videos: List[str],
    intro_md: str = "",
    bitable_url: str = "",
) -> str:
    """
    优先使用 suggestion_json 的方向卡片结构渲染；
    若没有则回退 suggestion_md。
    - 参考链接通过 ad_key 映射到 video_url，可点击且命名为 [视频1]/[视频2]...
    """
    intro_md = (intro_md or "").strip()
    bitable_url = (bitable_url or "").strip()

    def _append_bitable_link(text: str) -> str:
        if not bitable_url:
            return text
        suffix = f"\n\n[多维表格链接]({bitable_url})"
        return (text or "").rstrip() + suffix

    if not suggestion_json:
        base = suggestion_md
        if intro_md:
            return _append_bitable_link(intro_md + "\n\n" + base)
        return _append_bitable_link(base)

    s_obj = suggestion_json.get("suggestion") if isinstance(suggestion_json, dict) else None
    cards = s_obj.get("方向卡片") if isinstance(s_obj, dict) else None
    common = s_obj.get("共性执行建议") if isinstance(s_obj, dict) else None
    if not isinstance(cards, list) or not cards:
        base = suggestion_md
        if intro_md:
            return _append_bitable_link(intro_md + "\n\n" + base)
        return _append_bitable_link(base)

    lines: List[str] = []
    if intro_md:
        lines.append(intro_md)
        lines.append("")

    lines.append("**Video Enhancer 方向卡片（精简版）**")
    lines.append("")

    for card in cards:
        if not isinstance(card, dict):
            continue
        name = str(card.get("方向名称") or "未命名方向")
        lines.append(f"**[video enhancer 方向] {name}**")
        lines.append(f"🎬 背景：{card.get('背景', '')}")
        lines.append(f"🎯 UA建议：{card.get('UA建议', '')}")
        lines.append(f"🧩 产品对标点：{card.get('产品对标点', '')}")
        lines.append(f"⚠️ 风险提示：{card.get('风险提示', '')}")

        # 严格按 ad_key 映射到 video_url，不接受模型直接给 URL（防止错链）
        mapped: List[str] = []
        raw_links = card.get("参考链接") or []
        if isinstance(raw_links, list):
            for x in raw_links:
                sx = str(x or "").strip()
                if not sx:
                    continue
                if sx in adkey_to_video and adkey_to_video[sx]:
                    mapped.append(adkey_to_video[sx])

        # 去重保序
        dedup: List[str] = []
        seen: set[str] = set()
        for u in mapped:
            if not u or u in seen:
                continue
            seen.add(u)
            dedup.append(u)
        mapped = dedup

        lines.append(f"🔗 参考链接：{_card_video_links(mapped, max_n=5)}")
        lines.append("")

    lines.append("**共性执行建议**")
    if isinstance(common, list):
        for c in common:
            lines.append(f"- {c}")
    elif common:
        lines.append(f"- {common}")
    else:
        lines.append("- 保持短时长强钩子 + 中时长展示信任的双轨素材结构。")

    return _append_bitable_link("\n".join(lines))


def _extract_card_video_urls(card: Dict[str, Any], adkey_to_video: Dict[str, str], fallback_videos: List[str], start_idx: int) -> tuple[List[str], int]:
    _ = fallback_videos
    _ = start_idx
    mapped: List[str] = []
    raw_links = card.get("参考链接") or []
    if isinstance(raw_links, list):
        for x in raw_links:
            sx = str(x or "").strip()
            if not sx:
                continue
            if sx in adkey_to_video and adkey_to_video[sx]:
                mapped.append(adkey_to_video[sx])
    # 去重保序
    out: List[str] = []
    seen: set[str] = set()
    for u in mapped:
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out[:5], start_idx


def sync_cluster_cards_to_bitable(
    access_token: str,
    cluster_url: str,
    target_date: str,
    suggestion_json: Dict[str, Any] | None,
    adkey_to_video: Dict[str, str],
    fallback_videos: List[str],
) -> int:
    cluster_url = (cluster_url or "").strip()
    if not cluster_url:
        return 0
    app_token, table_id = parse_bitable_url(cluster_url)
    ensure_cluster_fields(access_token, app_token, table_id)

    s_obj = suggestion_json.get("suggestion") if isinstance(suggestion_json, dict) else None
    cards = s_obj.get("方向卡片") if isinstance(s_obj, dict) else None
    if not isinstance(cards, list) or not cards:
        print("[cluster-sync] suggestion_json 中无方向卡片，跳过聚类多维表同步。")
        return 0

    target_ms = to_ms_from_date_str(target_date)
    records: List[Dict[str, Any]] = []
    fb_idx = 0
    for card in cards:
        if not isinstance(card, dict):
            continue
        name = str(card.get("方向名称") or "未命名方向")
        urls, fb_idx = _extract_card_video_urls(card, adkey_to_video, fallback_videos, fb_idx)
        fields: Dict[str, Any] = {
            "标题": name,
            "背景": str(card.get("背景") or ""),
            "建议": str(card.get("UA建议") or ""),
            "产品对标点": str(card.get("产品对标点") or ""),
            "风险提示": str(card.get("风险提示") or ""),
            "视频链接": format_video_links(urls, max_n=5),
            "接受情况": "待定",
        }
        if target_ms is not None:
            fields["抓取日期"] = target_ms
        records.append({"fields": fields})

    total = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        if not batch:
            continue
        batch_create_records(access_token, app_token, table_id, batch)
        total += len(batch)
    print(f"[cluster-sync] 已写入聚类方向到多维表：{total} 条。")
    return total


def main() -> None:
    args = parse_args()
    app_token, table_id = parse_bitable_url(args.url)

    raw = json.loads(Path(args.raw).read_text(encoding="utf-8"))
    analysis = json.loads(Path(args.analysis).read_text(encoding="utf-8"))
    suggestion_json: Dict[str, Any] | None = None
    sjson_path = Path(args.suggestion_json)
    if sjson_path.exists():
        try:
            suggestion_json = json.loads(sjson_path.read_text(encoding="utf-8"))
        except Exception:
            suggestion_json = None
    suggestion_md = Path(args.suggestion_md).read_text(encoding="utf-8") if Path(args.suggestion_md).exists() else ""

    analysis_by_ad: Dict[str, str] = {}
    video_by_ad: Dict[str, str] = {}
    fallback_videos: List[str] = []
    for it in analysis.get("results") or []:
        if isinstance(it, dict):
            k = str(it.get("ad_key") or "")
            if k:
                analysis_by_ad[k] = str(it.get("analysis") or "")
                v = str(it.get("video_url") or "")
                if v:
                    video_by_ad[k] = v
                    fallback_videos.append(v)

    token = get_tenant_access_token()
    ensure_fields(token, app_token, table_id)

    records: List[Dict[str, Any]] = []
    target_date = str(raw.get("target_date") or "")
    target_ms = to_ms_from_date_str(target_date)
    ua_suggestion_text = suggestion_md.strip()[:50000] if suggestion_md else ""
    # 卡片前置信息：仅保留日期（不展示筛选规则/计数/产品分布）
    raw_items = raw.get("items") or []
    by_product: Dict[str, int] = {}
    for it in raw_items:
        if isinstance(it, dict):
            p = str(it.get("product") or "(unknown)")
            by_product[p] = by_product.get(p, 0) + 1

    fr = raw.get("filter_report")
    raw_total = int(raw.get("total") or len(raw_items) or 0)
    pre_total = 0
    post_total = 0
    threshold = 0
    keep = 0
    sort_metric = ""
    if isinstance(fr, dict) and fr:
        threshold = int(fr.get("filter_threshold") or 0)
        keep = int(fr.get("filter_keep") or 0)
        sort_metric = str(fr.get("filter_sort_metric") or "")
        pre_total = int(fr.get("pre_truncation_total") or 0)
        post_total = int(fr.get("post_truncation_total") or 0)

    intro_lines: List[str] = [f"【Video Enhancer 日报】{target_date}"]

    intro_md = "\n".join(intro_lines)
    for item in raw.get("items") or []:
        if not isinstance(item, dict):
            continue
        c = item.get("creative") or {}
        if not isinstance(c, dict):
            continue
        ad_key = str(c.get("ad_key") or "")
        fields: Dict[str, Any] = {
            "标题": str(c.get("title") or ""),
            "类目": str(item.get("category") or ""),
            "产品": str(item.get("product") or ""),
            "广告主": str(c.get("advertiser_name") or ""),
            "正文（中文）": str(c.get("body") or ""),
            "平台": str(c.get("platform") or ""),
            "视频链接": pick_video_url(c),
            "封面图链接": str(c.get("preview_img_url") or ""),
            "AI分析结果": analysis_by_ad.get(ad_key, ""),
            "UA灵感借鉴": ua_suggestion_text,
            "视频时长": int(c.get("video_duration") or 0),
            "接受情况": "待定",
            "我方产品": "video enhancer产品线",
            "广告ID": ad_key,
        }
        if target_ms is not None:
            fields["抓取日期"] = target_ms
        pt = c.get("pipeline_tags")
        if isinstance(pt, list) and pt:
            fields["素材标签"] = "、".join(str(x) for x in pt if x)
        else:
            fields["素材标签"] = ""
        created_ms = to_ms_from_unix_sec(c.get("created_at"))
        first_seen_ms = to_ms_from_unix_sec(c.get("first_seen"))
        if created_ms is not None:
            fields["创建时间"] = created_ms
            fields["更新时间"] = created_ms
        elif first_seen_ms is not None:
            fields["创建时间"] = first_seen_ms
            fields["更新时间"] = first_seen_ms

        img_url = str(c.get("preview_img_url") or "")
        if img_url:
            ft = upload_image_as_attachment(img_url, app_token)
            if ft:
                fields["封面图"] = [{"file_token": ft}]

        records.append({"fields": fields})

    total = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        if not batch:
            continue
        batch_create_records(token, app_token, table_id, batch)
        total += len(batch)
        print(f"[sync] 已写入 {total}/{len(records)}")
        time.sleep(0.2)

    print(f"[sync] 完成，共写入 {total} 条。")

    if not args.no_card and (suggestion_md.strip() or suggestion_json):
        card_md = _render_card_markdown(
            suggestion_json=suggestion_json,
            suggestion_md=suggestion_md,
            adkey_to_video=video_by_ad,
            fallback_videos=fallback_videos,
            intro_md=intro_md,
            bitable_url=args.url,
        )
        try:
            push_card(
                FEISHU_BOT_WEBHOOK,
                "Video Enhancer 统一UA建议（基于竞品素材）",
                card_md,
            )
            # 推送成功后，更新 DB 状态
            init_pipeline_db()
            if target_date and app_token and table_id:
                update_push_status(
                    target_date=target_date,
                    bitable_app_token=app_token,
                    bitable_table_id=table_id,
                    status="sent",
                    response="ok",
                )
        except Exception as e:
            init_pipeline_db()
            if target_date and app_token and table_id:
                update_push_status(
                    target_date=target_date,
                    bitable_app_token=app_token,
                    bitable_table_id=table_id,
                    status="failed",
                    response=str(e),
                )
            raise

    if suggestion_json:
        try:
            sync_cluster_cards_to_bitable(
                access_token=token,
                cluster_url=args.cluster_url,
                target_date=target_date,
                suggestion_json=suggestion_json,
                adkey_to_video=video_by_ad,
                fallback_videos=fallback_videos,
            )
        except Exception as e:
            print(f"[cluster-sync] 聚类多维表同步失败：{e}")


if __name__ == "__main__":
    main()

