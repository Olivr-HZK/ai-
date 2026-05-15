"""
把 raw + 灵感分析写入指定飞书多维表，并把统一 UA 建议推送到飞书卡片。

主表同步：写入本次 analysis JSON 中「成功灵感分析」的素材（analysis 非空且非 [ERROR]）。
成人风险、低适配主题、已投放等硬风险仍不同步；玩法重复默认仅打标/归类，避免主表过瘦。
同步前会优先使用视频分析阶段的 AI 玩法资产判断；缺失或无效时补跑规则玩法资产匹配，
并继续补成人风险、低适配、已投放等匹配，为结果补全标签与字段。
封面图、**视频**均尽量下载为附件上传（`VIDEO_BITABLE_MAX_MB` 限制大小；
`VIDEO_BITABLE_UPLOAD=0` 可关视频上传）。
不写入 raw 中仅入库、未分析或分析失败的条目。聚类表逻辑不变。

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
import re
import time
import uuid
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse, urlunparse

import lark_oapi as lark
import requests
from dotenv import load_dotenv
from lark_oapi.api.drive.v1.model import (
    UploadAllMediaRequest,
    UploadAllMediaRequestBody,
    UploadAllMediaResponse,
)

from ua_workflows.shared.config import DATA_DIR
from ua_workflows.shared.db.video_enhancer import (
    apply_embedding_duplicate_candidate_tags,
    apply_effect_embedding_duplicate_filter,
    apply_intraday_effect_bitable_filter,
    apply_old_effect_bitable_filter,
    init_db as init_pipeline_db,
    update_push_status,
)
from ua_workflows.video_enhancer.content_filters import (
    apply_adult_content_filter,
    apply_low_fit_theme_filter,
)
from ua_workflows.video_enhancer.play_asset_doc_sync import maybe_pull_play_asset_doc
from ua_workflows.video_enhancer.play_asset_report import (
    annotate_daily_play_asset_novelty,
    build_daily_asset_variant_report,
)

load_dotenv()


def normalize_cover_image_url_for_bitable(url: str) -> str:
    """
    广大大等来源的封面 URL 路径若以 .image 结尾，飞书多维表「链接」字段常无法预览；
    将路径后缀改为 .png（多数 CDN 同资源可访问）。
    """
    u = (url or "").strip()
    if not u:
        return u
    try:
        p = urlparse(u)
        path = p.path
        if not re.search(r"\.image$", path, re.IGNORECASE):
            return u
        new_path = re.sub(r"\.image$", ".png", path, flags=re.IGNORECASE)
        return urlunparse((p.scheme, p.netloc, new_path, p.params, p.query, p.fragment))
    except Exception:
        return u


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
    {"field_name": "视频附件", "type": 17},
    {"field_name": "核心卖点", "type": 1},
    {"field_name": "Hook解析", "type": 1},
    {"field_name": "脚本/口播", "type": 1},
    {"field_name": "玩法资产", "type": 1},
    {"field_name": "玩法变种", "type": 1},
    {"field_name": "玩法新旧", "type": 1},
    {"field_name": "玩法资产ID", "type": 1},
    {"field_name": "玩法变种ID", "type": 1},
    {"field_name": "玩法判断来源", "type": 1},
    {"field_name": "玩法判断理由", "type": 1},
    {"field_name": "玩法指纹", "type": 1},
    {"field_name": "差异点", "type": 1},
    {
        "field_name": "风险等级",
        "type": 3,
        "options": [{"name": "低风险"}, {"name": "中风险"}, {"name": "高风险"}],
    },
    {"field_name": "AI分析结果", "type": 1},
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
    {"field_name": "UA建议", "type": 1},
    {"field_name": "产品对标点", "type": 1},
    {"field_name": "风险提示", "type": 1},
    {"field_name": "视频链接", "type": 1},
    {"field_name": "图片链接", "type": 1},
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
    p.add_argument(
        "--sync-target",
        choices=["both", "raw", "cluster"],
        default="both",
        help="多维表同步目标：both=主表+聚类表，raw=仅主表，cluster=仅聚类表",
    )
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
_VIDEO_CACHE: Dict[str, str] = {}


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


def _video_bitable_max_bytes() -> int:
    try:
        mb = float((os.getenv("VIDEO_BITABLE_MAX_MB") or "32").strip())
    except ValueError:
        mb = 32.0
    return int(mb * 1024 * 1024)


def _guess_video_filename(url: str, ad_key: str) -> str:
    path = urlparse(url).path
    name = (path.split("/")[-1] or "").strip() or f"video_{(ad_key or 'a')[:16]}.mp4"
    return name[:200]


def upload_video_as_attachment(video_url: str, app_token: str, ad_key: str = "") -> str | None:
    """
    将可直链下载的视频以附件形式上传至多维表，返回 file_token。
    非直链/过大/HTML 回包时放弃，仅依赖「视频链接」文本列也能工作。
    """
    v = (video_url or "").strip()
    if not v or not v.startswith("http"):
        return None
    if v in _VIDEO_CACHE:
        return _VIDEO_CACHE[v]
    env_up = (os.getenv("VIDEO_BITABLE_UPLOAD") or "1").strip().lower()
    if env_up in ("0", "false", "no", "off", ""):
        return None
    low = v.lower()
    if "youtube.com" in low or "youtu.be" in low:
        return None
    max_b = _video_bitable_max_bytes()
    content_type = ""
    try:
        with requests.get(v, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            cl = resp.headers.get("Content-Length")
            if cl:
                try:
                    if int(cl) > max_b:
                        print(f"[sync] 视频过大跳过附件 length={cl} url={v[:80]}")
                        return None
                except ValueError:
                    pass
            content_type = (resp.headers.get("Content-Type") or "").lower()
            parts: list[bytes] = []
            n = 0
            for chunk in resp.iter_content(chunk_size=65536):
                if not chunk:
                    continue
                n += len(chunk)
                if n > max_b:
                    print(f"[sync] 视频流超过上限 {max_b}B，跳过 url={v[:80]}")
                    return None
                parts.append(chunk)
    except Exception as e:
        print(f"[sync] 下载视频失败: {e} url={v[:80]!r}")
        return None
    content = b"".join(parts)
    if len(content) < 500:
        return None
    if len(content) < 2000 and (
        b"<html" in content[:2000].lower() or b"<!doctype" in content[:2000].lower()
    ):
        print(f"[sync] 视频 URL 返回 HTML 非直链，跳过附件 url={v[:80]}")
        return None
    name = _guess_video_filename(v, ad_key)
    if not re.search(r"\.(mp4|mov|webm|m4v)$", name, re.I):
        if "webm" in content_type:
            name = re.sub(r"\.[^.]+$", "", name) + ".webm" if name else "video.webm"
        else:
            name = re.sub(r"\.[^.]+$", "", name) + ".mp4" if name else "video.mp4"
    name = (name or "video.mp4")[:200]
    body = (
        UploadAllMediaRequestBody.builder()
        .file_name(name)
        .parent_type("bitable")
        .parent_node(app_token)
        .size(len(content))
        .checksum("")
        .extra("")
        .file(BytesIO(content))
        .build()
    )
    req = UploadAllMediaRequest.builder().request_body(body).build()
    resp2: UploadAllMediaResponse = get_lark_client().drive.v1.media.upload_all(req)
    if resp2.success() and resp2.data and getattr(resp2.data, "file_token", None):
        tk = resp2.data.file_token
        _VIDEO_CACHE[v] = tk
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


def format_video_links(urls: List[str], max_n: int = 0) -> str:
    picked = [u for u in (urls or []) if u]
    if max_n and max_n > 0:
        picked = picked[:max_n]
    if not picked:
        return ""
    # 多维表单元格对 URL 识别更偏好“换行分隔”，用每行一个 URL 避免只显示前几个。
    return "\n".join([f"视频{i}：{u}" for i, u in enumerate(picked, start=1)])


def format_image_links(urls: List[str], max_n: int = 0) -> str:
    picked = [u for u in (urls or []) if u]
    if max_n and max_n > 0:
        picked = picked[:max_n]
    if not picked:
        return ""
    return "\n".join([f"图片{i}：{u}" for i, u in enumerate(picked, start=1)])


def build_meta_by_ad_from_analysis_payload(analysis: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """从 video_analysis_* JSON 的 results 构建 ad_key → 媒体元信息（供卡片/聚类表映射）。"""
    meta_by_ad: Dict[str, Dict[str, Any]] = {}
    for it in (analysis or {}).get("results") or []:
        if not isinstance(it, dict):
            continue
        k = str(it.get("ad_key") or "").strip()
        if not k:
            continue
        vu = str(it.get("video_url") or "").strip()
        iu = normalize_cover_image_url_for_bitable(str(it.get("image_url") or "").strip())
        pu = normalize_cover_image_url_for_bitable(str(it.get("preview_img_url") or "").strip())
        ct = str(it.get("creative_type") or "").strip() or ("image" if (not vu and iu) else "video")
        meta_by_ad[k] = {
            "creative_type": ct,
            "video_url": vu,
            "image_url": iu,
            "preview_img_url": pu,
        }
    return meta_by_ad


def _media_slices_from_meta(meta: Dict[str, Any]) -> tuple[List[str], List[str]]:
    """单条素材 meta（来自 analysis results）拆成 (视频 URL 列表, 图片 URL 列表)。"""
    ct = str(meta.get("creative_type") or "video")
    vu = str(meta.get("video_url") or "").strip()
    iu = str(meta.get("image_url") or "").strip()
    pu = str(meta.get("preview_img_url") or "").strip()
    videos: List[str] = []
    images: List[str] = []
    if ct == "image":
        if iu:
            images.append(iu)
        elif pu:
            images.append(pu)
    else:
        if vu:
            videos.append(vu)
    return videos, images


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


def _card_mixed_links(video_urls: List[str], image_urls: List[str], max_n: int = 5) -> str:
    v = [u for u in video_urls if u][:max_n]
    im = [u for u in image_urls if u][:max_n]
    if not v and not im:
        return "（无）"
    parts = [f"[视频{i}]({u})" for i, u in enumerate(v, start=1)]
    parts += [f"[图片{i}]({u})" for i, u in enumerate(im, start=1)]
    return "；".join(parts)


def _render_card_markdown(
    suggestion_json: Dict[str, Any] | None,
    suggestion_md: str,
    meta_by_ad: Dict[str, Dict[str, Any]],
    intro_md: str = "",
    bitable_url: str = "",
    include_ua_suggestion: bool = True,
    include_product_benchmark: bool = True,
) -> str:
    """
    优先使用 suggestion_json 的方向卡片结构渲染；
    若没有则回退 suggestion_md。
    - 参考链接通过 ad_key 映射到视频/图片 URL（见 meta_by_ad），飞书卡片内为 [视频n]/[图片n]。
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
        lines.append(f"**🎬 背景：**{card.get('背景', '')}")
        if include_ua_suggestion:
            lines.append(f"**🎯 UA建议：**{card.get('UA建议', '')}")
        if include_product_benchmark:
            lines.append(f"**🧩 产品对标点：**{card.get('产品对标点', '')}")
        lines.append(f"**⚠️ 风险提示：**{card.get('风险提示', '')}")

        v_merged: List[str] = []
        i_merged: List[str] = []
        seen_v: set[str] = set()
        seen_i: set[str] = set()
        raw_links = card.get("参考链接") or []
        if isinstance(raw_links, list):
            for x in raw_links:
                sx = str(x or "").strip()
                if not sx or sx not in meta_by_ad:
                    continue
                tv, ti = _media_slices_from_meta(meta_by_ad[sx])
                for u in tv:
                    if u and u not in seen_v:
                        seen_v.add(u)
                        v_merged.append(u)
                for u in ti:
                    if u and u not in seen_i:
                        seen_i.add(u)
                        i_merged.append(u)

        lines.append(f"🔗 参考链接：{_card_mixed_links(v_merged, i_merged, max_n=5)}")
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


def _extract_card_media(
    card: Dict[str, Any],
    meta_by_ad: Dict[str, Dict[str, Any]],
    max_each: int = 0,
) -> tuple[List[str], List[str]]:
    v_out: List[str] = []
    i_out: List[str] = []
    seen_v: set[str] = set()
    seen_i: set[str] = set()
    raw_links = card.get("参考链接") or []
    if isinstance(raw_links, list):
        for x in raw_links:
            sx = str(x or "").strip()
            if not sx or sx not in meta_by_ad:
                continue
            tv, ti = _media_slices_from_meta(meta_by_ad[sx])
            for u in tv:
                if u and u not in seen_v and (not max_each or len(v_out) < max_each):
                    seen_v.add(u)
                    v_out.append(u)
            for u in ti:
                if u and u not in seen_i and (not max_each or len(i_out) < max_each):
                    seen_i.add(u)
                    i_out.append(u)
    return v_out, i_out


def _analysis_exclusion_is_hard(row: Dict[str, Any]) -> bool:
    if not row.get("exclude_from_bitable"):
        return False
    hard_keys = (
        "adult_content_filter_match",
        "low_fit_theme_filter_match",
        "launched_effect_match",
    )
    if any(row.get(key) for key in hard_keys):
        return True
    soft_play_keys = (
        "intraday_effect_match",
        "old_effect_match",
        "effect_embedding_duplicate_match",
    )
    if any(row.get(key) for key in soft_play_keys) and _env_enabled(
        "BITABLE_SYNC_INCLUDE_PLAY_DUPLICATE_EXCLUDES",
        "1",
    ):
        return False
    return True


def raw_items_with_successful_analysis(
    raw: Dict[str, Any],
    analysis: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    主表仅同步「本次 analysis 里有成功灵感分析」的素材：非空且不以 [ERROR] 开头。
    顺序与 analysis JSON 的 results 一致；同一 ad_key 只出现一次。
    """
    raw_by_ad: Dict[str, Dict[str, Any]] = {}
    for item in raw.get("items") or []:
        if not isinstance(item, dict):
            continue
        c = item.get("creative") or {}
        if not isinstance(c, dict):
            continue
        k = str(c.get("ad_key") or "").strip()
        if k:
            raw_by_ad[k] = item

    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for it in analysis.get("results") or []:
        if not isinstance(it, dict):
            continue
        k = str(it.get("ad_key") or "").strip()
        a = str(it.get("analysis") or "")
        if not k or k in seen:
            continue
        if not a.strip() or a.startswith("[ERROR]"):
            continue
        if _analysis_exclusion_is_hard(it):
            continue
        item = raw_by_ad.get(k)
        if item is None:
            continue
        seen.add(k)
        out.append(item)
    return out


def normalize_risk_level_for_bitable(value: Any, tags: List[str] | None = None) -> str:
    s = str(value or "").strip()
    if "高" in s:
        return "高风险"
    if "中" in s:
        return "中风险"
    if "低" in s or "无明显" in s or "无风险" in s:
        return "低风险"

    joined = "、".join(str(t or "") for t in (tags or []))
    if any(x in joined for x in ("成人色情风险", "色情/成人风险", "成人风险", "露点", "裸体")):
        return "高风险"
    if any(x in joined for x in ("擦边露肤风险", "版权名人风险", "产品不适配风险", "低质素材风险")):
        return "中风险"
    return ""


def _env_enabled(name: str, default: str = "1") -> bool:
    raw = (os.getenv(name) or default).strip().lower()
    return raw not in ("0", "false", "no", "off", "")


def _env_int(name: str, default: int, *, min_value: int = 1, max_value: int = 60) -> int:
    try:
        value = int(os.getenv(name) or default)
    except ValueError:
        value = default
    return max(min_value, min(max_value, value))


def _short_tag_text(value: Any, max_chars: int = 36) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if len(text) <= max_chars:
        return text
    return text[: max(1, max_chars - 1)].rstrip() + "…"


def _add_tags(
    tag_map: Dict[str, List[str]],
    ad_key: Any,
    tags: List[str],
) -> None:
    key = str(ad_key or "").strip()
    if not key:
        return
    bucket = tag_map.setdefault(key, [])
    for tag in tags:
        t = str(tag or "").strip()
        if t and t not in bucket:
            bucket.append(t)


def build_daily_bitable_tag_map(target_date: str) -> Dict[str, List[str]]:
    """
    Build creation-time labels for Bitable rows from the same daily report logic
    used by Feishu pushes. This deliberately writes into the existing `素材标签`
    text field, so tomorrow's new rows can be distinguished without requiring
    record-update or schema permissions.
    """
    if not target_date or not _env_enabled("BITABLE_DAILY_PLAY_TAGS_ENABLED", "1"):
        return {}

    lookback = _env_int(
        "BITABLE_DAILY_PLAY_TAG_LOOKBACK_DAYS",
        _env_int("OLD_EFFECT_LOOKBACK_DAYS", 7),
    )
    report = build_daily_asset_variant_report(target_date, lookback_days=lookback)
    tag_map: Dict[str, List[str]] = {}

    for item in report.get("asset_variant_items") or []:
        if not isinstance(item, dict):
            continue
        tags: List[str] = []
        asset_name = _short_tag_text(item.get("play_asset_name"))
        variant_name = _short_tag_text(item.get("play_asset_variant_name"))
        novelty = str(item.get("play_asset_novelty_label") or "").strip()
        if asset_name:
            tags.append(f"玩法资产:{asset_name}")
        if variant_name:
            tags.append(f"玩法变种:{variant_name}")
        if novelty == "新玩法":
            tags.append("日报:新玩法资产")
        elif novelty == "新变种":
            tags.append("日报:新玩法变种")
        elif novelty:
            tags.append("日报:已沉淀玩法")
        _add_tags(tag_map, item.get("ad_key"), tags)

    for cluster in report.get("new_asset_variant_clusters") or []:
        if not isinstance(cluster, dict):
            continue
        asset_name = _short_tag_text(cluster.get("play_asset_name"))
        variant_name = _short_tag_text(cluster.get("play_asset_variant_name"))
        count = int(cluster.get("material_count") or 1)
        rep_ad_key = str(cluster.get("representative_ad_key") or "").strip()
        novelty_label = str(cluster.get("novelty_label") or "新变种")
        base_tags = ["日报:新玩法" if novelty_label == "新玩法" else "日报:新玩法变种"]
        if asset_name:
            base_tags.append(f"玩法资产:{asset_name}")
        if variant_name:
            base_tags.append(f"玩法变种:{variant_name}")
        if count > 1:
            base_tags.append(f"同资产变种素材数:{count}")

        members = cluster.get("members") or []
        for member in members:
            if not isinstance(member, dict):
                continue
            member_ad_key = str(member.get("ad_key") or "").strip()
            member_tags = list(base_tags)
            if member_ad_key and member_ad_key == rep_ad_key:
                member_tags.insert(0, "日报:新玩法/新变种代表")
            else:
                member_tags.append("日报:同资产变种素材")
            _add_tags(tag_map, member_ad_key, member_tags)

        if rep_ad_key:
            _add_tags(tag_map, rep_ad_key, ["日报:新玩法/新变种代表", *base_tags])

    for item in report.get("old_play_items") or []:
        if not isinstance(item, dict):
            continue
        current_tags = tag_map.get(str(item.get("ad_key") or "").strip(), [])
        has_asset_novelty = any(
            tag in current_tags
            for tag in (
                "日报:新玩法资产",
                "日报:新玩法变种",
                "日报:新玩法",
                "日报:新玩法/新变种代表",
            )
        )
        tags = ["一句话口径:老玩法"] if has_asset_novelty else ["日报:老玩法换素材"]
        first_seen = str(item.get("effect_first_seen_date") or "").strip()
        if first_seen:
            tags.append(f"玩法首次:{first_seen}")
        matched = _short_tag_text(
            item.get("effect_matched_one_liner")
            or item.get("daily_play_cluster_key")
            or item.get("effect_one_liner")
        )
        if matched:
            tags.append(f"匹配玩法:{matched}")
        _add_tags(tag_map, item.get("ad_key"), tags)

    for item in report.get("unknown_play_items") or []:
        if isinstance(item, dict):
            _add_tags(tag_map, item.get("ad_key"), ["日报:玩法待复核"])

    return tag_map


def lookup_daily_bitable_tags(
    tag_map: Dict[str, List[str]],
    ad_key: str,
) -> List[str]:
    key = str(ad_key or "").strip()
    if not key or not tag_map:
        return []
    if key in tag_map:
        return tag_map[key]
    prefix = key[:16]
    if not prefix:
        return []
    for k, tags in tag_map.items():
        if k[:16] == prefix:
            return tags
    return []


def should_skip_bitable_same_play_member(
    daily_tags: List[str],
) -> bool:
    """
    Optional legacy narrow-sync mode. The default is now to sync more rows into
    Bitable and use play asset / variant fields for review grouping.
    """
    if not _env_enabled("BITABLE_SYNC_DAILY_PLAY_REPRESENTATIVES_ONLY", "0"):
        return False
    tag_set = {str(t or "").strip() for t in daily_tags}
    return (
        ("日报:同玩法素材" in tag_set or "日报:同资产变种素材" in tag_set)
        and "日报:新玩法代表" not in tag_set
        and "日报:新玩法/新变种代表" not in tag_set
    )


_HIGH_ACCEPTANCE_THEME_PATTERNS: List[tuple[str, str, int]] = [
    ("球赛抓拍", r"球赛|球场|足球|棒球|赛场|体育场|球迷|观众视角|看台|cowboy|牛仔", 4),
    ("机甲科幻变身", r"机甲|战甲|装甲|科幻|超级英雄|变身|奇幻|火焰翅膀|战斗特效|电影级", 3),
    ("手绘漫画", r"漫画|手绘|素描|涂鸦|卡通|贴纸|泡泡贴纸", 3),
    ("亲情合影", r"母亲节|母女|亲人|逝者|重逢|拥抱|温馨合影", 3),
    ("热门模板同款", r"模板|同款|热门趋势|viral|流行模板|trend|套用", 3),
    ("人物形象替换", r"性别|换性别|外貌|造型转换|形象替换|变老变性|任意性别", 3),
    ("生日写真", r"生日|影楼|生日写真|birthday", 3),
    ("明星合影红毯", r"明星|名人合影|名人同款|红毯|走红毯|已故巨星", 3),
    ("剧情短片", r"剧情短片|连续剧情|末日剧情|爽剧|故事短片", 2),
    ("年龄变化", r"年龄|幼年|老年|多年龄段|年龄过渡", 2),
    ("求职商务照", r"求职|商务头像|职业头像|证件照", 2),
]


def acceptance_priority_tags(
    *,
    creative: Dict[str, Any],
    analysis_text: str,
    effect_text: str,
    hook_text: str,
    voiceover_text: str,
    material_tags: List[str],
    daily_tags: List[str],
    risk_level: str,
) -> tuple[int, List[str]]:
    text = " ".join(
        str(x or "")
        for x in [
            creative.get("title"),
            creative.get("body"),
            analysis_text,
            effect_text,
            hook_text,
            voiceover_text,
            " ".join(material_tags),
            " ".join(daily_tags),
            risk_level,
        ]
    )
    score = 0
    tags: List[str] = []
    tag_set = {str(t or "").strip() for t in daily_tags}

    if "日报:新玩法代表" in tag_set or "日报:新玩法/新变种代表" in tag_set:
        score += 3
        tags.append("采纳优先:新玩法代表")
    if "日报:老玩法换素材" in tag_set:
        score -= 1

    for name, pattern, weight in _HIGH_ACCEPTANCE_THEME_PATTERNS:
        if re.search(pattern, text, flags=re.I):
            score += weight
            tags.append(f"高采纳主题:{name}")

    if str(risk_level or "").strip() == "高风险" and not any(t.startswith("高采纳主题:") for t in tags):
        score -= 2
    if any("embedding重复候选" == str(t).strip() for t in material_tags):
        score -= 1

    return score, tags


def should_skip_low_acceptance_candidate(
    *,
    score: int,
    daily_tags: List[str],
) -> bool:
    if not _env_enabled("BITABLE_ACCEPTANCE_PRIORITY_SYNC_ENABLED", "0"):
        return False
    tag_set = {str(t or "").strip() for t in daily_tags}
    # New-play representatives are already sparse and useful for calibration.
    if "日报:新玩法代表" in tag_set or "日报:新玩法/新变种代表" in tag_set:
        return False
    threshold = _env_int(
        "BITABLE_ACCEPTANCE_PRIORITY_MIN_SCORE",
        3,
        min_value=-10,
        max_value=10,
    )
    return score < threshold


def sync_cluster_cards_to_bitable(
    access_token: str,
    cluster_url: str,
    target_date: str,
    suggestion_json: Dict[str, Any] | None,
    meta_by_ad: Dict[str, Dict[str, Any]],
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
    for card in cards:
        if not isinstance(card, dict):
            continue
        name = str(card.get("方向名称") or "未命名方向")
        v_urls, i_urls = _extract_card_media(card, meta_by_ad, max_each=0)
        fields: Dict[str, Any] = {
            "标题": name,
            "背景": str(card.get("背景") or ""),
            "UA建议": str(card.get("UA建议") or ""),
            "产品对标点": str(card.get("产品对标点") or ""),
            "风险提示": str(card.get("风险提示") or ""),
            "视频链接": format_video_links(v_urls, max_n=0),
            "图片链接": format_image_links(i_urls, max_n=0),
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
    maybe_pull_play_asset_doc()
    app_token, table_id = parse_bitable_url(args.url)

    raw = json.loads(Path(args.raw).read_text(encoding="utf-8"))
    analysis = json.loads(Path(args.analysis).read_text(encoding="utf-8"))
    target_date = str(raw.get("target_date") or "")
    suggestion_json: Dict[str, Any] | None = None
    sjson_path = Path(args.suggestion_json)
    if sjson_path.exists():
        try:
            suggestion_json = json.loads(sjson_path.read_text(encoding="utf-8"))
        except Exception:
            suggestion_json = None
    suggestion_md = Path(args.suggestion_md).read_text(encoding="utf-8") if Path(args.suggestion_md).exists() else ""

    analysis_by_ad: Dict[str, str] = {}
    effect_by_ad: Dict[str, str] = {}
    hook_by_ad: Dict[str, str] = {}
    voiceover_by_ad: Dict[str, str] = {}
    play_fingerprint_by_ad: Dict[str, str] = {}
    differentiator_by_ad: Dict[str, str] = {}
    meta_by_ad = build_meta_by_ad_from_analysis_payload(analysis)
    for it in analysis.get("results") or []:
        if isinstance(it, dict):
            k = str(it.get("ad_key") or "").strip()
            if k:
                analysis_by_ad[k] = str(it.get("analysis") or "")
                effect_by_ad[k] = str(it.get("effect_one_liner") or "")
                hook_by_ad[k] = str(it.get("hook_one_liner") or "")
                voiceover_by_ad[k] = str(it.get("voiceover_script") or "")
                play_fingerprint_by_ad[k] = str(it.get("play_fingerprint") or "")
                differentiator_by_ad[k] = str(it.get("differentiator") or "")

    need_raw_sync = args.sync_target in ("both", "raw")
    need_cluster_sync = args.sync_target in ("both", "cluster")
    token = get_tenant_access_token()
    if need_raw_sync:
        ensure_fields(token, app_token, table_id)
        res_list = analysis.get("results")
        if isinstance(res_list, list) and res_list:
            n_adult, _ = apply_adult_content_filter(res_list)
            if n_adult:
                print(f"[sync] 成人/色情风险处理 {n_adult} 条（排除/补标）")

            low_fit_on = (os.getenv("LOW_FIT_THEME_FILTER_ENABLED") or "1").strip().lower() not in (
                "0",
                "false",
                "no",
                "off",
                "",
            )
            if low_fit_on:
                n_low_fit, _ = apply_low_fit_theme_filter(res_list)
                if n_low_fit:
                    print(f"[sync] 低采纳主题处理 {n_low_fit} 条（排除/补标）")

            intraday_effect_on = (os.getenv("INTRADAY_EFFECT_FILTER_ENABLED") or "0").strip().lower() not in (
                "0",
                "false",
                "no",
                "off",
                "",
            )
            if intraday_effect_on:
                try:
                    n_intraday, _ = apply_intraday_effect_bitable_filter(res_list)
                    if n_intraday:
                        print(f"[sync] 日内玩法重复处理 {n_intraday} 条（排除/补标）")
                except Exception as e:
                    print(f"[sync] intraday_effect_filter 跳过: {e}")

            old_effect_on = (os.getenv("OLD_EFFECT_BITABLE_FILTER_ENABLED") or "0").strip().lower() not in (
                "0",
                "false",
                "no",
                "off",
                "",
            )
            if old_effect_on and target_date:
                try:
                    n_old, _ = apply_old_effect_bitable_filter(target_date, res_list)
                    if n_old:
                        print(f"[sync] 老玩法重复处理 {n_old} 条（排除/补标）")
                except Exception as e:
                    print(f"[sync] old_effect_filter 跳过: {e}")

            effect_embedding_dup_on = (os.getenv("EFFECT_EMBEDDING_DUP_FILTER_ENABLED") or "0").strip().lower() not in (
                "0",
                "false",
                "no",
                "off",
                "",
            )
            if effect_embedding_dup_on and target_date:
                try:
                    n_effect_emb, _ = apply_effect_embedding_duplicate_filter(target_date, res_list)
                    if n_effect_emb:
                        print(f"[sync] embedding 玩法重复处理 {n_effect_emb} 条（排除/补标）")
                except Exception as e:
                    print(f"[sync] effect_embedding_duplicate_filter 跳过: {e}")

            embedding_dup_on = (os.getenv("EMBEDDING_DUP_CANDIDATE_ENABLED") or "1").strip().lower() not in (
                "0",
                "false",
                "no",
                "off",
                "",
            )
            if embedding_dup_on and target_date:
                try:
                    n_emb_dup, _ = apply_embedding_duplicate_candidate_tags(target_date, res_list)
                    if n_emb_dup:
                        print(f"[sync] embedding 重复候选标记 {n_emb_dup} 条（仅打标，不排除）")
                except Exception as e:
                    print(f"[sync] embedding_dup_candidate 跳过: {e}")

        le_on = (os.getenv("LAUNCHED_EFFECTS_ENABLED") or "0").strip().lower() not in ("0", "false", "no", "off", "")
        if le_on:
            if isinstance(res_list, list) and res_list:
                try:
                    from ua_workflows.video_enhancer.launched_effects import apply_launched_effects_filter

                    n_le, _ = apply_launched_effects_filter(res_list)
                    if n_le:
                        print(f"[sync] 我方已投放（关键词/embedding）处理 {n_le} 条（排除/补标）")
                except Exception as e:
                    print(f"[sync] launched_effects 跳过: {e}")

    play_asset_by_ad: Dict[str, Dict[str, Any]] = {}
    res_list_for_assets = analysis.get("results")
    if isinstance(res_list_for_assets, list) and res_list_for_assets:
        try:
            annotate_daily_play_asset_novelty(res_list_for_assets, target_date)
            for it in res_list_for_assets:
                if not isinstance(it, dict):
                    continue
                k = str(it.get("ad_key") or "").strip()
                if k:
                    play_asset_by_ad[k] = {
                        "play_asset_name": str(it.get("play_asset_name") or ""),
                        "play_asset_variant_name": str(it.get("play_asset_variant_name") or ""),
                        "play_asset_novelty_label": str(it.get("play_asset_novelty_label") or ""),
                        "play_asset_id": str(it.get("play_asset_id") or ""),
                        "play_asset_variant_key": str(it.get("play_asset_variant_key") or ""),
                        "play_asset_matched_keywords": str(it.get("play_asset_matched_keywords") or ""),
                        "play_asset_match_source": str(it.get("play_asset_match_source") or ""),
                        "play_asset_classification_reason": str(it.get("play_asset_classification_reason") or ""),
                    }
            if play_asset_by_ad:
                print(f"[sync] 已补全玩法资产/变种字段 {len(play_asset_by_ad)} 条。")
        except Exception as e:
            print(f"[sync] 玩法资产/变种补全失败，已跳过: {e}")

    material_tags_by_ad: Dict[str, List[str]] = {}
    risk_level_by_ad: Dict[str, str] = {}
    for it in analysis.get("results") or []:
        if not isinstance(it, dict):
            continue
        k = str(it.get("ad_key") or "").strip()
        tags = it.get("material_tags")
        if k and isinstance(tags, list):
            material_tags_by_ad[k] = [str(x) for x in tags if x]
        if k:
            risk_level_by_ad[k] = normalize_risk_level_for_bitable(
                it.get("risk_level"),
                material_tags_by_ad.get(k),
            )

    daily_tags_by_ad: Dict[str, List[str]] = {}
    if need_raw_sync and target_date:
        try:
            daily_tags_by_ad = build_daily_bitable_tag_map(target_date)
            if daily_tags_by_ad:
                print(
                    f"[sync] 已生成日报玩法标签 {len(daily_tags_by_ad)} 条（写入素材标签字段）。"
                )
        except Exception as e:
            print(f"[sync] 日报玩法标签生成失败，已跳过: {e}")

    records: List[Dict[str, Any]] = []
    target_ms = to_ms_from_date_str(target_date)
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
    if need_raw_sync:
        n_style_skip = sum(
            1
            for it in (analysis.get("results") or [])
            if isinstance(it, dict) and it.get("exclude_from_bitable")
        )
        if n_style_skip:
            print(f"[sync] 主表将跳过已标记排除素材 {n_style_skip} 条（不同步多维表）。")
        items_to_sync = raw_items_with_successful_analysis(raw, analysis)
        n_raw_items = len(raw.get("items") or [])
        if n_raw_items > len(items_to_sync):
            print(
                f"[sync] 主表仅同步成功灵感分析：{len(items_to_sync)} 条（raw 共 {n_raw_items} 条，其余未写入主表）。"
            )
        elif not items_to_sync and n_raw_items:
            print(
                "[sync] 警告：raw 有素材但 analysis 中无成功灵感分析，主表将写入 0 条。"
            )
        for item in items_to_sync:
            c = item.get("creative") or {}
            if not isinstance(c, dict):
                continue
            ad_key = str(c.get("ad_key") or "")
            daily_tag_list = lookup_daily_bitable_tags(daily_tags_by_ad, ad_key)
            if should_skip_bitable_same_play_member(daily_tag_list):
                print(f"[sync] 跳过同玩法非代表素材 ad_key={ad_key[:12]}（仅同步玩法代表）。")
                continue
            base_material_tags = material_tags_by_ad.get(ad_key, [])
            risk_level = risk_level_by_ad.get(ad_key, "")
            play_asset_info = play_asset_by_ad.get(ad_key, {})
            priority_score, priority_tags = acceptance_priority_tags(
                creative=c,
                analysis_text=analysis_by_ad.get(ad_key, ""),
                effect_text=effect_by_ad.get(ad_key, ""),
                hook_text=hook_by_ad.get(ad_key, ""),
                voiceover_text=voiceover_by_ad.get(ad_key, ""),
                material_tags=base_material_tags,
                daily_tags=daily_tag_list,
                risk_level=risk_level,
            )
            if should_skip_low_acceptance_candidate(score=priority_score, daily_tags=daily_tag_list):
                print(
                    f"[sync] 跳过低采纳优先级素材 ad_key={ad_key[:12]} "
                    f"score={priority_score} tags={'/'.join(priority_tags) or '-'}"
                )
                continue
            category = str(item.get("category") or "").strip()
            if category:
                own_product_line = f"{category}产品线"
            else:
                own_product_line = "unknown产品线"
            fields: Dict[str, Any] = {
                "标题": str(c.get("title") or ""),
                "类目": category,
                "产品": str(item.get("product") or ""),
                "广告主": str(c.get("advertiser_name") or ""),
                "正文（中文）": str(c.get("body") or ""),
                "平台": str(c.get("platform") or ""),
                "视频链接": pick_video_url(c),
                "封面图链接": normalize_cover_image_url_for_bitable(
                    str(c.get("preview_img_url") or "")
                ),
                "AI分析结果": analysis_by_ad.get(ad_key, ""),
                "核心卖点": effect_by_ad.get(ad_key, ""),
                "Hook解析": hook_by_ad.get(ad_key, ""),
                "脚本/口播": voiceover_by_ad.get(ad_key, ""),
                "玩法资产": play_asset_info.get("play_asset_name", ""),
                "玩法变种": play_asset_info.get("play_asset_variant_name", ""),
                "玩法新旧": play_asset_info.get("play_asset_novelty_label", ""),
                "玩法资产ID": play_asset_info.get("play_asset_id", ""),
                "玩法变种ID": play_asset_info.get("play_asset_variant_key", ""),
                "玩法判断来源": play_asset_info.get("play_asset_match_source", ""),
                "玩法判断理由": play_asset_info.get("play_asset_classification_reason", ""),
                "玩法指纹": play_fingerprint_by_ad.get(ad_key, ""),
                "差异点": differentiator_by_ad.get(ad_key, ""),
                "风险等级": risk_level,
                "视频时长": int(c.get("video_duration") or 0),
                "接受情况": "待定",
                "我方产品": own_product_line,
                "广告ID": ad_key,
            }
            if target_ms is not None:
                fields["抓取日期"] = target_ms
            tag_list: List[str] = []
            pt = c.get("pipeline_tags")
            if isinstance(pt, list):
                tag_list.extend(str(x) for x in pt if x)
            tag_list.extend(base_material_tags)
            tag_list.extend(daily_tag_list)
            tag_list.extend(priority_tags)
            if play_asset_info.get("play_asset_name"):
                tag_list.append(f"玩法资产:{play_asset_info.get('play_asset_name')}")
            if play_asset_info.get("play_asset_variant_name"):
                tag_list.append(f"玩法变种:{play_asset_info.get('play_asset_variant_name')}")
            if play_asset_info.get("play_asset_match_source") == "ai":
                tag_list.append("玩法判断:AI")
            fields["素材标签"] = "、".join(dict.fromkeys(tag_list))
            created_ms = to_ms_from_unix_sec(c.get("created_at"))
            first_seen_ms = to_ms_from_unix_sec(c.get("first_seen"))
            if created_ms is not None:
                fields["创建时间"] = created_ms
                fields["更新时间"] = created_ms
            elif first_seen_ms is not None:
                fields["创建时间"] = first_seen_ms
                fields["更新时间"] = first_seen_ms

            img_url = normalize_cover_image_url_for_bitable(
                str(c.get("preview_img_url") or "")
            )
            if img_url:
                ft = upload_image_as_attachment(img_url, app_token)
                if ft:
                    fields["封面图"] = [{"file_token": ft}]

            v_direct = (pick_video_url(c) or "").strip()
            if v_direct and int(c.get("video_duration") or 0) > 0:
                vf = upload_video_as_attachment(v_direct, app_token, ad_key)
                if vf:
                    fields["视频附件"] = [{"file_token": vf}]

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
        print(
            f"[sync] 主表同步完成，共写入 {total} 条（仅含成功灵感分析）。"
        )
    else:
        print("[sync] 已按参数跳过主表同步（--sync-target=cluster）。")

    if not args.no_card and (suggestion_md.strip() or suggestion_json):
        card_md = _render_card_markdown(
            suggestion_json=suggestion_json,
            suggestion_md=suggestion_md,
            meta_by_ad=meta_by_ad,
            intro_md=intro_md,
            bitable_url=args.url,
            include_ua_suggestion=False,
            include_product_benchmark=True,
        )
        card_title = f"广大大素材日报（{target_date}）" if target_date else "广大大素材日报"
        try:
            push_card(
                FEISHU_BOT_WEBHOOK,
                card_title,
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

    if need_cluster_sync and suggestion_json:
        try:
            sync_cluster_cards_to_bitable(
                access_token=token,
                cluster_url=args.cluster_url,
                target_date=target_date,
                suggestion_json=suggestion_json,
                meta_by_ad=meta_by_ad,
            )
        except Exception as e:
            print(f"[cluster-sync] 聚类多维表同步失败：{e}")
    elif not need_cluster_sync:
        print("[cluster-sync] 已按参数跳过聚类表同步（--sync-target=raw）。")


if __name__ == "__main__":
    main()
