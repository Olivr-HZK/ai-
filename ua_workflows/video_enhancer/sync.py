"""
把 raw + 灵感分析写入指定飞书多维表，并把统一 UA 建议推送到飞书卡片。

主表同步：写入本次 analysis JSON 中「成功灵感分析」的素材（analysis 非空且非 [ERROR]）。
成人风险、已投放等硬风险仍不同步；玩法重复默认仅打标/归类，避免主表过瘦。
同步前会优先使用视频分析阶段的 AI 玩法资产判断；缺失或无效时补跑规则玩法资产匹配，
并继续补成人风险、已投放等匹配，为结果补全标签与字段。
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
from collections import Counter, defaultdict
from datetime import datetime, timezone
from difflib import SequenceMatcher
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse, urlunparse

import lark_oapi as lark
import requests
from lark_oapi.api.drive.v1.model import (
    UploadAllMediaRequest,
    UploadAllMediaRequestBody,
    UploadAllMediaResponse,
)

from ua_workflows.shared.config import DATA_DIR, load_project_env
from ua_workflows.shared.db.video_enhancer import (
    apply_embedding_duplicate_candidate_tags,
    apply_effect_embedding_duplicate_filter,
    apply_intraday_effect_bitable_filter,
    apply_old_effect_bitable_filter,
    init_db as init_pipeline_db,
    load_cover_embedding_blob_map_by_ad_keys,
    normalize_effect_one_liner,
    update_push_status,
)
from ua_workflows.shared.media.resolve import normalize_video_url_for_consumption
from ua_workflows.shared.llm.client import bytes_to_embedding, cosine_similarity
from ua_workflows.video_enhancer.content_filters import (
    apply_adult_content_filter,
    apply_human_photo_effect_filter,
)
from ua_workflows.video_enhancer.crawl_similarity import (
    build_crawl_similarity_count_map,
)
from ua_workflows.video_enhancer.play_asset_doc_sync import maybe_pull_play_asset_doc
from ua_workflows.video_enhancer.play_assets import legacy_play_library_enabled
from ua_workflows.video_enhancer.play_asset_report import (
    annotate_daily_play_asset_novelty,
    build_daily_asset_variant_report,
)

load_project_env()


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
    {"field_name": "玩法", "type": 1},
    {"field_name": "玩法资产", "type": 1},
    {"field_name": "玩法变种", "type": 1},
    {"field_name": "玩法新旧", "type": 1},
    {"field_name": "玩法资产ID", "type": 1},
    {"field_name": "玩法变种ID", "type": 1},
    {"field_name": "玩法判断来源", "type": 1},
    {"field_name": "玩法判断理由", "type": 1},
    {"field_name": "玩法指纹", "type": 1},
    {"field_name": "差异点", "type": 1},
    {"field_name": "模板指纹", "type": 1},
    {"field_name": "狭义新判断", "type": 1},
    {"field_name": "狭义新理由", "type": 1},
    {"field_name": "日内相似素材数", "type": 2},
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


def get_existing_field_map(access_token: str, app_token: str, table_id: str) -> Dict[str, Dict[str, Any]]:
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json; charset=utf-8"}
    out: Dict[str, Dict[str, Any]] = {}
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
                out[str(name)] = it
        if not data_obj.get("has_more"):
            break
        page_token = data_obj.get("page_token")
    return out


def get_existing_field_names(access_token: str, app_token: str, table_id: str) -> set[str]:
    return set(get_existing_field_map(access_token, app_token, table_id).keys())


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
    v = normalize_video_url_for_consumption((video_url or "").strip())
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


def _split_multi_option_text(value: str) -> List[str]:
    parts = re.split(r"[、,，;；\n]+", value)
    return [p.strip() for p in parts if p.strip()]


def _field_option_names(field_info: Dict[str, Any] | None) -> set[str]:
    if not isinstance(field_info, dict):
        return set()
    options: List[Any] = []
    prop = field_info.get("property")
    if isinstance(prop, dict) and isinstance(prop.get("options"), list):
        options.extend(prop.get("options") or [])
    if isinstance(field_info.get("options"), list):
        options.extend(field_info.get("options") or [])
    names: set[str] = set()
    for option in options:
        if isinstance(option, dict):
            text = str(option.get("name") or option.get("text") or option.get("value") or "").strip()
        else:
            text = str(option or "").strip()
        if text:
            names.add(text)
    return names


def _normalize_bitable_field_value(field_name: str, value: Any, field_info: Dict[str, Any] | None) -> Any:
    field_type = int((field_info or {}).get("type") or 0)
    if field_type == 4:
        if value is None:
            return []
        if isinstance(value, (list, tuple, set)):
            out: List[str] = []
            for item in value:
                if item is None:
                    continue
                if isinstance(item, dict):
                    text = str(item.get("text") or item.get("name") or item.get("value") or "").strip()
                else:
                    text = str(item).strip()
                if text:
                    out.append(text)
            values = list(dict.fromkeys(out))
        else:
            text = str(value or "").strip()
            values = _split_multi_option_text(text) if text else []
        option_names = _field_option_names(field_info)
        if option_names:
            values = [v for v in values if v in option_names]
        return values
    return value


def batch_create_records(access_token: str, app_token: str, table_id: str, records: List[Dict[str, Any]]) -> None:
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json; charset=utf-8"}
    params = {"user_id_type": "open_id", "client_token": str(uuid.uuid4())}
    existing_field_map = get_existing_field_map(access_token, app_token, table_id)
    existing_fields = set(existing_field_map.keys())
    missing_fields: set[str] = set()
    filtered_records: List[Dict[str, Any]] = []
    for record in records:
        fields = record.get("fields") if isinstance(record, dict) else None
        if not isinstance(fields, dict):
            filtered_records.append(record)
            continue
        filtered_fields: Dict[str, Any] = {}
        for k, v in fields.items():
            if k not in existing_fields:
                continue
            field_info = existing_field_map.get(k)
            normalized = _normalize_bitable_field_value(k, v, field_info)
            if int((field_info or {}).get("type") or 0) == 4 and not normalized:
                continue
            filtered_fields[k] = normalized
        missing_fields.update(k for k in fields if k not in existing_fields)
        filtered_records.append({"fields": filtered_fields})
    if missing_fields:
        print(f"[sync] 跳过当前表不存在字段：{', '.join(sorted(missing_fields))}")
    records = filtered_records
    resp = requests.post(url, headers=headers, params=params, json={"records": records}, timeout=20)
    if not resp.ok:
        raise RuntimeError(f"batch_create http failed: status={resp.status_code} body={resp.text[:2000]}")
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
        return normalize_video_url_for_consumption(str(creative["video_url"]))
    for r in creative.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("video_url"):
            return normalize_video_url_for_consumption(str(r["video_url"]))
    return ""


def pick_video_urls(creative: Dict[str, Any], max_n: int = 5) -> List[str]:
    urls: List[str] = []
    if creative.get("video_url"):
        urls.append(normalize_video_url_for_consumption(str(creative["video_url"])))
    for r in creative.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("video_url"):
            urls.append(normalize_video_url_for_consumption(str(r["video_url"])))
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
        vu = normalize_video_url_for_consumption(str(it.get("video_url") or "").strip())
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
    vu = normalize_video_url_for_consumption(str(meta.get("video_url") or "").strip())
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
        "human_photo_effect_filter_match",
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


def _sync_product_from_item(item: Dict[str, Any]) -> str:
    c = item.get("creative") or {}
    if not isinstance(c, dict):
        c = {}
    return str(
        item.get("product")
        or c.get("product")
        or c.get("advertiser_name")
        or item.get("keyword")
        or "未知产品"
    ).strip() or "未知产品"


def _sync_report_row(rows: Dict[str, Dict[str, Any]], product: str) -> Dict[str, Any]:
    p = product or "未知产品"
    if p not in rows:
        rows[p] = {
            "product": p,
            "successful_analysis": 0,
            "hard_excluded": 0,
            "after_hard_exclusion": 0,
            "template_dedup_removed": 0,
            "after_template_dedup": 0,
            "same_play_non_representative_removed": 0,
            "low_acceptance_removed": 0,
            "synced_records": 0,
            "removed_total": 0,
            "removed_reasons": {},
        }
    return rows[p]


def _sync_report_add_reason(
    reasons: Dict[str, Counter[str]],
    product: str,
    reason: str,
    count: int = 1,
) -> None:
    if count <= 0:
        return
    reasons[product or "未知产品"][reason or "unknown"] += int(count)


def _sync_exclude_reason(row: Dict[str, Any]) -> str:
    if row.get("adult_content_filter_match"):
        return "adult_content"
    if row.get("human_photo_effect_filter_match"):
        match = row.get("human_photo_effect_filter_match") or {}
        if isinstance(match, dict):
            reason = str(match.get("reason") or "").strip()
            if reason:
                return reason
        return "non_human_photo_effect"
    if row.get("launched_effect_match"):
        return "launched_effect"
    if row.get("intraday_effect_match"):
        return "intraday_effect_duplicate"
    if row.get("old_effect_match"):
        return "old_effect_duplicate"
    if row.get("effect_embedding_duplicate_match"):
        return "effect_embedding_duplicate"
    return "exclude_from_bitable_other"


def _write_sync_report(
    *,
    target_date: str,
    sync_target: str,
    need_raw_sync: bool,
    rows_by_product: Dict[str, Dict[str, Any]],
    reasons_by_product: Dict[str, Counter[str]],
) -> Path:
    per_product: List[Dict[str, Any]] = []
    for product, row in sorted(rows_by_product.items()):
        reasons = {
            key: int(value)
            for key, value in sorted(reasons_by_product.get(product, Counter()).items())
            if int(value) > 0
        }
        out = dict(row)
        out["removed_reasons"] = reasons
        out["removed_total"] = int(sum(reasons.values()))
        per_product.append(out)
    totals: Dict[str, Any] = {}
    numeric_keys = (
        "successful_analysis",
        "hard_excluded",
        "after_hard_exclusion",
        "template_dedup_removed",
        "after_template_dedup",
        "same_play_non_representative_removed",
        "low_acceptance_removed",
        "synced_records",
        "removed_total",
    )
    for key in numeric_keys:
        totals[key] = sum(int(row.get(key) or 0) for row in per_product)
    reason_totals: Counter[str] = Counter()
    for row in per_product:
        for key, value in (row.get("removed_reasons") or {}).items():
            reason_totals[str(key)] += int(value or 0)
    totals["removed_reasons"] = {
        key: int(value)
        for key, value in sorted(reason_totals.items())
        if int(value) > 0
    }
    payload = {
        "target_date": target_date,
        "sync_target": sync_target,
        "main_sync_enabled": bool(need_raw_sync),
        "totals": totals,
        "per_product": per_product,
    }
    path = DATA_DIR / f"workflow_video_enhancer_{target_date}_sync_report.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[sync-report] 已写 {path.name}")
    return path


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


def _env_float(name: str, default: float, *, min_value: float = 0.0, max_value: float = 1.0) -> float:
    try:
        value = float(os.getenv(name) or default)
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
        novelty = str(item.get("narrow_novelty_label") or item.get("play_asset_novelty_label") or "").strip()
        if asset_name:
            tags.append(f"玩法资产:{asset_name}")
        if variant_name:
            tags.append(f"玩法变种:{variant_name}")
        if novelty == "新玩法":
            tags.append("日报:新玩法")
        elif novelty == "老玩法新迭代":
            tags.append("日报:老玩法新迭代")
        elif novelty == "老玩法换皮":
            tags.append("日报:老玩法换皮")
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
        novelty_label = str(cluster.get("novelty_label") or "老玩法新迭代")
        if novelty_label == "新玩法":
            base_tags = ["日报:新玩法"]
        elif novelty_label == "老玩法新迭代":
            base_tags = ["日报:老玩法新迭代"]
        else:
            base_tags = [f"日报:{novelty_label}"]
        if asset_name:
            base_tags.append(f"玩法资产:{asset_name}")
        if variant_name:
            base_tags.append(f"玩法变种:{variant_name}")
        if count > 1:
            base_tags.append(f"同狭义新素材数:{count}")

        members = cluster.get("members") or []
        for member in members:
            if not isinstance(member, dict):
                continue
            member_ad_key = str(member.get("ad_key") or "").strip()
            member_tags = list(base_tags)
            if member_ad_key and member_ad_key == rep_ad_key:
                member_tags.insert(0, "日报:狭义新代表")
            else:
                member_tags.append("日报:同狭义新素材")
            _add_tags(tag_map, member_ad_key, member_tags)

        if rep_ad_key:
            _add_tags(tag_map, rep_ad_key, ["日报:狭义新代表", *base_tags])

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
                "日报:老玩法新迭代",
                "日报:狭义新代表",
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
        ("日报:同玩法素材" in tag_set or "日报:同资产变种素材" in tag_set or "日报:同狭义新素材" in tag_set)
        and "日报:新玩法代表" not in tag_set
        and "日报:新玩法/新变种代表" not in tag_set
        and "日报:狭义新代表" not in tag_set
    )


def _daily_similarity_group_key(
    item: Dict[str, Any],
    *,
    play_asset_info: Dict[str, Any],
    effect_by_ad: Dict[str, str],
    play_fingerprint_by_ad: Dict[str, str],
) -> str:
    c = item.get("creative") or {}
    if not isinstance(c, dict):
        c = {}
    ad_key = str(c.get("ad_key") or "").strip()
    appid = str(c.get("appid") or item.get("appid") or "").strip()
    product = str(item.get("product") or c.get("advertiser_name") or "").strip()
    scope = appid or product or "unknown"

    variant_key = str(play_asset_info.get("play_asset_variant_key") or "").strip()
    if variant_key and not variant_key.startswith("unmatched::"):
        return f"{scope}::variant::{variant_key}"

    normalized = normalize_effect_one_liner(
        play_fingerprint_by_ad.get(ad_key) or effect_by_ad.get(ad_key) or ""
    )
    if normalized:
        return f"{scope}::text::{normalized[:120]}"
    return f"{scope}::ad::{ad_key}"


def build_daily_similarity_count_map(
    items: List[Dict[str, Any]],
    *,
    play_asset_by_ad: Dict[str, Dict[str, Any]],
    effect_by_ad: Dict[str, str],
    play_fingerprint_by_ad: Dict[str, str],
    cover_intraday_report: Dict[str, Any] | None = None,
) -> Dict[str, int]:
    """
    Count same-day similar materials for each synced candidate.

    The count is intentionally intraday only: same app/product scope, same play
    asset variant first; unclassified rows fall back to normalized play text.
    Same-day cover CLIP removals are added to the kept representative's group,
    because those removed cards are still evidence that a template is crowded.
    A value of 1 means this material has no same-day similar sibling.
    """
    groups: Dict[str, List[str]] = defaultdict(list)
    group_key_by_ad: Dict[str, str] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        c = item.get("creative") or {}
        if not isinstance(c, dict):
            continue
        ad_key = str(c.get("ad_key") or "").strip()
        if not ad_key:
            continue
        key = _daily_similarity_group_key(
            item,
            play_asset_info=play_asset_by_ad.get(ad_key, {}),
            effect_by_ad=effect_by_ad,
            play_fingerprint_by_ad=play_fingerprint_by_ad,
        )
        groups[key].append(ad_key)
        group_key_by_ad[ad_key] = key

    group_key_by_prefix = {
        ad_key[:16]: group_key
        for ad_key, group_key in group_key_by_ad.items()
        if ad_key[:16]
    }
    synced_ad_keys = set(group_key_by_ad)
    cover_removed_by_group: Dict[str, set[str]] = defaultdict(set)
    if isinstance(cover_intraday_report, dict):
        per_appid = cover_intraday_report.get("per_appid") or []
        if isinstance(per_appid, dict):
            buckets = per_appid.values()
        elif isinstance(per_appid, list):
            buckets = per_appid
        else:
            buckets = []
        for bucket in buckets:
            if not isinstance(bucket, dict):
                continue
            for removed in bucket.get("removed") or []:
                if not isinstance(removed, dict):
                    continue
                if str(removed.get("reason") or "").strip() not in (
                    "cover_style_cluster",
                    "cover_style_cluster_history_refresh",
                ):
                    continue
                removed_ad_key = str(removed.get("ad_key") or "").strip()
                if removed_ad_key and removed_ad_key in synced_ad_keys:
                    continue
                kept_ad_key = str(removed.get("kept_ad_key") or "").strip()
                group_key = group_key_by_ad.get(kept_ad_key)
                if not group_key and kept_ad_key:
                    group_key = group_key_by_prefix.get(kept_ad_key[:16])
                if not group_key:
                    continue
                removed_key = removed_ad_key or str(removed.get("cover_url") or "").strip()
                if not removed_key:
                    removed_key = f"{kept_ad_key}:{len(cover_removed_by_group[group_key])}"
                cover_removed_by_group[group_key].add(removed_key)

    out: Dict[str, int] = {}
    for group_key, ad_keys in groups.items():
        count = len(set(ad_keys)) + len(cover_removed_by_group.get(group_key, set()))
        for ad_key in ad_keys:
            out[ad_key] = count
    return out


def build_cover_history_refresh_tag_map(
    cover_intraday_report: Dict[str, Any] | None,
) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = defaultdict(list)
    if not isinstance(cover_intraday_report, dict):
        return out
    per_appid = cover_intraday_report.get("per_appid") or []
    if isinstance(per_appid, dict):
        buckets = per_appid.values()
    elif isinstance(per_appid, list):
        buckets = per_appid
    else:
        buckets = []
    for bucket in buckets:
        if not isinstance(bucket, dict):
            continue
        for row in bucket.get("history_refresh") or []:
            if not isinstance(row, dict):
                continue
            ad_key = str(row.get("ad_key") or row.get("kept_ad_key") or "").strip()
            if not ad_key:
                continue
            tags = out[ad_key]
            tags.append("历史簇持续发力")
            matched_date = str(row.get("matched_date") or "").strip()
            if matched_date:
                tags.append(f"历史命中:{matched_date}")
            age = row.get("history_age_days")
            if age not in (None, ""):
                try:
                    tags.append(f"历史间隔:{int(age)}天")
                except Exception:
                    pass
            sim = row.get("similarity")
            if sim not in (None, ""):
                try:
                    tags.append(f"历史封面相似度:{float(sim):.2f}")
                except Exception:
                    pass
    return {ad_key: list(dict.fromkeys(tags)) for ad_key, tags in out.items()}


def _template_dedup_score(item: Dict[str, Any]) -> tuple[int, int, int, str]:
    c = item.get("creative") or {}
    if not isinstance(c, dict):
        c = {}

    def as_int(key: str) -> int:
        try:
            return int(c.get(key) or item.get(key) or 0)
        except Exception:
            return 0

    return (
        as_int("all_exposure_value"),
        as_int("impression"),
        as_int("heat"),
        str(c.get("ad_key") or ""),
    )


def _template_dedup_key(
    item: Dict[str, Any],
    *,
    play_asset_info: Dict[str, Any],
    effect_by_ad: Dict[str, str],
    play_fingerprint_by_ad: Dict[str, str],
    template_fingerprint_by_ad: Dict[str, str],
) -> str:
    c = item.get("creative") or {}
    if not isinstance(c, dict):
        c = {}
    ad_key = str(c.get("ad_key") or "").strip()
    appid = str(c.get("appid") or item.get("appid") or "").strip()
    product = str(item.get("product") or c.get("advertiser_name") or "").strip()
    scope = appid or product
    if not scope or not ad_key:
        return ""

    play_text = play_fingerprint_by_ad.get(ad_key) or effect_by_ad.get(ad_key) or ""
    play_key = normalize_effect_one_liner(play_text)[:140]
    template_text = (
        str(play_asset_info.get("template_fingerprint") or "").strip()
        or template_fingerprint_by_ad.get(ad_key)
        or ""
    )
    template_key = normalize_effect_one_liner(template_text)[:180]
    if not play_key or not template_key:
        return ""
    return f"{scope}::play::{play_key}::template::{template_key}"


def _template_dedup_text_similarity_threshold() -> float:
    return _env_float("BITABLE_TEMPLATE_DEDUP_TEXT_SIMILARITY_THRESHOLD", 0.78, min_value=0.5)


def _template_dedup_text_similarity_enabled() -> bool:
    return _env_enabled("BITABLE_TEMPLATE_DEDUP_TEXT_SIMILARITY_ENABLED", "0")


def _template_dedup_clip_threshold() -> float:
    return _env_float("BITABLE_TEMPLATE_DEDUP_CLIP_THRESHOLD", 0.70, min_value=0.5)


def _template_text_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    return max(
        float(SequenceMatcher(None, a, b).ratio()),
        float(SequenceMatcher(None, b, a).ratio()),
    )


def _valid_play_asset_id(value: Any) -> str:
    text = str(value or "").strip()
    if not text or text.lower() in {
        "none",
        "null",
        "unknown",
        "new_play",
        "unmatched_play",
        "unmatched",
        "待沉淀",
        "待人工归类",
    }:
        return ""
    return text


def _build_bitable_play_fields(
    play_asset_info: Dict[str, Any],
    *,
    play_fingerprint: str = "",
    differentiator: str = "",
    template_fingerprint: str = "",
) -> Dict[str, Any]:
    asset_id = _valid_play_asset_id(play_asset_info.get("play_asset_id"))
    play_name = str(play_asset_info.get("play_asset_name") or "").strip() if asset_id else ""
    return {
        "玩法": play_name,
        "玩法指纹": str(play_fingerprint or "").strip(),
        "差异点": str(differentiator or "").strip(),
        "模板指纹": str(play_asset_info.get("template_fingerprint") or template_fingerprint or "").strip(),
    }


def _template_dedup_context(
    item: Dict[str, Any],
    *,
    play_asset_info: Dict[str, Any],
    effect_by_ad: Dict[str, str],
    play_fingerprint_by_ad: Dict[str, str],
    template_fingerprint_by_ad: Dict[str, str],
) -> Dict[str, Any]:
    c = item.get("creative") or {}
    if not isinstance(c, dict):
        c = {}
    ad_key = str(c.get("ad_key") or "").strip()
    appid = str(c.get("appid") or item.get("appid") or "").strip()
    product = str(item.get("product") or c.get("advertiser_name") or "").strip()
    scope = appid or product
    if not scope or not ad_key:
        return {}

    play_text = play_fingerprint_by_ad.get(ad_key) or effect_by_ad.get(ad_key) or ""
    play_key = normalize_effect_one_liner(play_text)[:140]
    asset_id = _valid_play_asset_id(play_asset_info.get("play_asset_id"))
    if asset_id:
        play_bucket = f"{scope}::asset::{asset_id}"
    elif play_key:
        play_bucket = f"{scope}::play::{play_key}"
    else:
        return {}

    template_text = (
        str(play_asset_info.get("template_fingerprint") or "").strip()
        or template_fingerprint_by_ad.get(ad_key)
        or ""
    )
    template_key = normalize_effect_one_liner(template_text)[:180]
    if not template_key:
        return {}
    return {
        "ad_key": ad_key,
        "item": item,
        "scope": scope,
        "product": product,
        "play_bucket": play_bucket,
        "play_key": play_key,
        "asset_id": asset_id,
        "template_text": template_text,
        "template_key": template_key,
        "exact_key": f"{play_bucket}::template::{template_key}",
    }


def _template_dedup_clip_vectors(contexts: List[Dict[str, Any]]) -> Dict[str, List[float]]:
    if not _env_enabled("BITABLE_TEMPLATE_DEDUP_CLIP_ENABLED", "1"):
        return {}
    ad_keys = [str(ctx.get("ad_key") or "") for ctx in contexts if ctx.get("ad_key")]
    if not ad_keys:
        return {}
    out: Dict[str, List[float]] = {}
    try:
        blob_map = load_cover_embedding_blob_map_by_ad_keys(ad_keys)
    except Exception:
        return {}
    for ad_key, blob in blob_map.items():
        try:
            out[str(ad_key)] = bytes_to_embedding(blob)
        except Exception:
            continue
    return out


def apply_template_dedup_for_bitable(
    items: List[Dict[str, Any]],
    *,
    play_asset_by_ad: Dict[str, Dict[str, Any]],
    effect_by_ad: Dict[str, str],
    play_fingerprint_by_ad: Dict[str, str],
    template_fingerprint_by_ad: Dict[str, str],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Hard-filter same-day template duplicates before Bitable sync.

    This is stricter than the push/report novelty logic and only affects Bitable
    rows. Same app/product + same normalized play fingerprint + same normalized
    template fingerprint is treated as one material, so demographic swaps do not
    create multiple review rows.
    """
    if not _env_enabled("BITABLE_TEMPLATE_DEDUP_ENABLED", "1"):
        return items, []

    contexts: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        c = item.get("creative") or {}
        if not isinstance(c, dict):
            continue
        ad_key = str(c.get("ad_key") or "").strip()
        ctx = _template_dedup_context(
            item,
            play_asset_info=play_asset_by_ad.get(ad_key, {}),
            effect_by_ad=effect_by_ad,
            play_fingerprint_by_ad=play_fingerprint_by_ad,
            template_fingerprint_by_ad=template_fingerprint_by_ad,
        )
        if not ctx:
            continue
        contexts.append(ctx)

    if not contexts:
        return items, []

    parent: Dict[str, str] = {str(ctx["ad_key"]): str(ctx["ad_key"]) for ctx in contexts}
    pair_evidence: Dict[frozenset[str], Dict[str, Any]] = {}

    def find(ad_key: str) -> str:
        while parent[ad_key] != ad_key:
            parent[ad_key] = parent[parent[ad_key]]
            ad_key = parent[ad_key]
        return ad_key

    def union(a: str, b: str) -> None:
        ra = find(a)
        rb = find(b)
        if ra != rb:
            parent[rb] = ra

    def remember_pair_evidence(
        a: str,
        b: str,
        *,
        reason: str,
        template_similarity: float | None = None,
        cover_clip_similarity: float | None = None,
    ) -> None:
        key = frozenset((a, b))
        existing = pair_evidence.get(key)
        score = (
            3 if reason == "template_exact" else 2 if reason == "template_fuzzy_text" else 1,
            float(template_similarity or 0.0),
            float(cover_clip_similarity or 0.0),
        )
        existing_score = (
            3 if existing and existing.get("match_reason") == "template_exact" else 2 if existing and existing.get("match_reason") == "template_fuzzy_text" else 1,
            float(existing.get("template_similarity") or 0.0) if existing else 0.0,
            float(existing.get("cover_clip_similarity") or 0.0) if existing else 0.0,
        )
        if existing and existing_score >= score:
            return
        pair_evidence[key] = {
            "match_reason": reason,
            "template_similarity": round(float(template_similarity or 0.0), 4) if template_similarity is not None else None,
            "cover_clip_similarity": round(float(cover_clip_similarity or 0.0), 4) if cover_clip_similarity is not None else None,
        }

    contexts_by_exact: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    contexts_by_bucket: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for ctx in contexts:
        contexts_by_exact[str(ctx["exact_key"])].append(ctx)
        contexts_by_bucket[str(ctx["play_bucket"])].append(ctx)

    for rows in contexts_by_exact.values():
        if len(rows) <= 1:
            continue
        first = str(rows[0]["ad_key"])
        for ctx in rows[1:]:
            right_key = str(ctx["ad_key"])
            remember_pair_evidence(
                first,
                right_key,
                reason="template_exact",
                template_similarity=1.0,
            )
            union(first, right_key)

    text_enabled = _template_dedup_text_similarity_enabled()
    text_threshold = _template_dedup_text_similarity_threshold()
    clip_threshold = _template_dedup_clip_threshold()
    clip_vecs = _template_dedup_clip_vectors(contexts)
    for rows in contexts_by_bucket.values():
        if len(rows) <= 1:
            continue
        for i, left in enumerate(rows):
            left_key = str(left["ad_key"])
            for right in rows[i + 1 :]:
                right_key = str(right["ad_key"])
                text_sim = _template_text_similarity(str(left["template_key"]), str(right["template_key"]))
                if text_enabled and text_sim >= text_threshold:
                    remember_pair_evidence(
                        left_key,
                        right_key,
                        reason="template_fuzzy_text",
                        template_similarity=text_sim,
                    )
                    union(left_key, right_key)
                    continue
                left_vec = clip_vecs.get(left_key)
                right_vec = clip_vecs.get(right_key)
                clip_sim = float(cosine_similarity(left_vec, right_vec)) if left_vec and right_vec else 0.0
                if clip_sim >= clip_threshold:
                    remember_pair_evidence(
                        left_key,
                        right_key,
                        reason="template_clip_same_play",
                        template_similarity=text_sim,
                        cover_clip_similarity=clip_sim,
                    )
                    union(left_key, right_key)

    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    context_by_ad: Dict[str, Dict[str, Any]] = {}
    for ctx in contexts:
        ad_key = str(ctx["ad_key"])
        context_by_ad[ad_key] = ctx
        groups[find(ad_key)].append(ctx)

    kept_ad_keys: set[str] = set()
    skipped: List[Dict[str, Any]] = []
    for _key, rows in groups.items():
        if len(rows) <= 1:
            kept_ad_keys.add(str(rows[0].get("ad_key") or ""))
            continue
        rows_sorted = sorted(rows, key=lambda ctx: _template_dedup_score(ctx["item"]), reverse=True)
        kept = rows_sorted[0]
        kept_ad_key = str(kept.get("ad_key") or "").strip()
        if kept_ad_key:
            kept_ad_keys.add(kept_ad_key)
        kept_template = str(kept.get("template_key") or "")
        kept_vec = clip_vecs.get(kept_ad_key)
        for ctx in rows_sorted[1:]:
            ad_key = str(ctx.get("ad_key") or "").strip()
            template_sim = _template_text_similarity(str(ctx.get("template_key") or ""), kept_template)
            clip_sim = 0.0
            vec = clip_vecs.get(ad_key)
            if vec and kept_vec:
                clip_sim = float(cosine_similarity(vec, kept_vec))
            reason = "template_exact"
            if str(ctx.get("exact_key") or "") != str(kept.get("exact_key") or ""):
                reason = "template_fuzzy_text" if template_sim >= text_threshold else "template_clip_same_play"
            match_ad_key = kept_ad_key
            evidence = pair_evidence.get(frozenset((ad_key, kept_ad_key)))
            if evidence is None:
                best_evidence: tuple[tuple[int, float, float, int], str, Dict[str, Any]] | None = None
                for other in rows_sorted:
                    other_ad_key = str(other.get("ad_key") or "").strip()
                    if not other_ad_key or other_ad_key == ad_key:
                        continue
                    ev = pair_evidence.get(frozenset((ad_key, other_ad_key)))
                    if not ev:
                        continue
                    ev_reason = str(ev.get("match_reason") or "")
                    ev_score = (
                        3 if ev_reason == "template_exact" else 2 if ev_reason == "template_fuzzy_text" else 1,
                        float(ev.get("template_similarity") or 0.0),
                        float(ev.get("cover_clip_similarity") or 0.0),
                        1 if other_ad_key == kept_ad_key else 0,
                    )
                    if best_evidence is None or ev_score > best_evidence[0]:
                        best_evidence = (ev_score, other_ad_key, ev)
                if best_evidence is not None:
                    _, match_ad_key, evidence = best_evidence
            if evidence is not None:
                reason = str(evidence.get("match_reason") or reason)
                if evidence.get("template_similarity") is not None:
                    template_sim = float(evidence.get("template_similarity") or 0.0)
                if evidence.get("cover_clip_similarity") is not None:
                    clip_sim = float(evidence.get("cover_clip_similarity") or 0.0)
            skipped.append(
                {
                    "ad_key": ad_key,
                    "kept_ad_key": kept_ad_key,
                    "match_ad_key": match_ad_key,
                    "group_key": str(kept.get("play_bucket") or ""),
                    "score": _template_dedup_score(ctx["item"]),
                    "kept_score": _template_dedup_score(kept["item"]),
                    "product": str(ctx.get("product") or ""),
                    "match_reason": reason,
                    "template_similarity": round(template_sim, 4),
                    "cover_clip_similarity": round(clip_sim, 4) if clip_sim else None,
                    "template_key": str(ctx.get("template_key") or ""),
                    "kept_template_key": kept_template,
                }
            )

    if not skipped:
        return items, []

    skipped_ad_keys = {str(row.get("ad_key") or "") for row in skipped}
    filtered: List[Dict[str, Any]] = []
    for item in items:
        c = item.get("creative") or {}
        ad_key = str(c.get("ad_key") or "").strip() if isinstance(c, dict) else ""
        if ad_key in skipped_ad_keys:
            continue
        ctx = context_by_ad.get(ad_key)
        group_key = find(ad_key) if ctx else ""
        if ctx and ad_key not in kept_ad_keys and len(groups.get(group_key, [])) > 1:
            continue
        filtered.append(item)

    group_count = len({str(row.get("group_key") or "") for row in skipped})
    reason_counts = Counter(str(row.get("match_reason") or "unknown") for row in skipped)
    print(
        f"[sync] 同模板换人/性别去重：跳过 {len(skipped)} 条，"
        f"保留 {group_count} 个模板代表；原因 {dict(reason_counts)}。"
    )
    for row in skipped[:20]:
        print(
            f"[sync] 同模板跳过 ad_key={str(row.get('ad_key') or '')[:12]} "
            f"kept={str(row.get('kept_ad_key') or '')[:12]} product={row.get('product') or '-'} "
            f"reason={row.get('match_reason') or '-'} "
            f"text_sim={row.get('template_similarity') or '-'} clip_sim={row.get('cover_clip_similarity') or '-'}"
        )
    if len(skipped) > 20:
        print(f"[sync] 同模板跳过明细仅展示前 20 条，剩余 {len(skipped) - 20} 条。")
    return filtered, skipped


def merge_template_dedup_similarity_counts(
    daily_similarity_count_by_ad: Dict[str, int],
    template_skipped: List[Dict[str, Any]],
) -> None:
    grouped: Dict[str, set[str]] = defaultdict(set)
    for row in template_skipped:
        kept_ad_key = str(row.get("kept_ad_key") or "").strip()
        skipped_ad_key = str(row.get("ad_key") or "").strip()
        if not kept_ad_key:
            continue
        grouped[kept_ad_key].add(kept_ad_key)
        if skipped_ad_key:
            grouped[kept_ad_key].add(skipped_ad_key)
    for kept_ad_key, ad_keys in grouped.items():
        daily_similarity_count_by_ad[kept_ad_key] = max(
            int(daily_similarity_count_by_ad.get(kept_ad_key) or 1),
            len(ad_keys),
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

    if "日报:新玩法代表" in tag_set or "日报:新玩法/新变种代表" in tag_set or "日报:狭义新代表" in tag_set:
        score += 3
        tags.append("采纳优先:狭义新代表")
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
    if "日报:新玩法代表" in tag_set or "日报:新玩法/新变种代表" in tag_set or "日报:狭义新代表" in tag_set:
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
    if legacy_play_library_enabled():
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
    template_fingerprint_by_ad: Dict[str, str] = {}
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
                template_fingerprint_by_ad[k] = str(it.get("template_fingerprint") or "")

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
            n_human_photo, _ = apply_human_photo_effect_filter(res_list)
            if n_human_photo:
                print(f"[sync] 非人物照片加工/电商素材处理 {n_human_photo} 条（排除/补标）")

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
                        "asset_variant_novelty_label": str(it.get("asset_variant_novelty_label") or ""),
                        "narrow_novelty_label": str(it.get("narrow_novelty_label") or it.get("play_asset_novelty_label") or ""),
                        "narrow_novelty_reason": str(it.get("narrow_novelty_reason") or ""),
                        "play_asset_id": str(it.get("play_asset_id") or ""),
                        "play_asset_variant_key": str(it.get("play_asset_variant_key") or ""),
                        "play_asset_matched_keywords": str(it.get("play_asset_matched_keywords") or ""),
                        "play_asset_match_source": str(it.get("play_asset_match_source") or ""),
                        "play_asset_classification_reason": str(it.get("play_asset_classification_reason") or ""),
                        "template_fingerprint": str(it.get("template_fingerprint") or ""),
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
    sync_rows_by_product: Dict[str, Dict[str, Any]] = {}
    sync_reasons_by_product: Dict[str, Counter[str]] = defaultdict(Counter)
    ad_product_by_ad: Dict[str, str] = {}
    for it in raw_items:
        if not isinstance(it, dict):
            continue
        c = it.get("creative") or {}
        if not isinstance(c, dict):
            continue
        ad_key = str(c.get("ad_key") or "").strip()
        if ad_key:
            ad_product_by_ad[ad_key] = _sync_product_from_item(it)
    for it in analysis.get("results") or []:
        if not isinstance(it, dict):
            continue
        ad_key = str(it.get("ad_key") or "").strip()
        text = str(it.get("analysis") or "").strip()
        if not ad_key or not text or text.startswith("[ERROR]"):
            continue
        product = str(it.get("product") or ad_product_by_ad.get(ad_key) or "未知产品").strip() or "未知产品"
        srow = _sync_report_row(sync_rows_by_product, product)
        srow["successful_analysis"] += 1
        if _analysis_exclusion_is_hard(it):
            srow["hard_excluded"] += 1
            _sync_report_add_reason(sync_reasons_by_product, product, _sync_exclude_reason(it))
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
    items_to_sync: List[Dict[str, Any]] = []
    daily_similarity_count_by_ad: Dict[str, int] = {}
    cover_history_refresh_tags_by_ad: Dict[str, List[str]] = build_cover_history_refresh_tag_map(
        raw.get("cover_style_intraday_report")
    )
    if need_raw_sync:
        n_style_skip = sum(
            1
            for it in (analysis.get("results") or [])
            if isinstance(it, dict) and it.get("exclude_from_bitable")
        )
        if n_style_skip:
            print(f"[sync] 主表将跳过已标记排除素材 {n_style_skip} 条（不同步多维表）。")
        items_to_sync = raw_items_with_successful_analysis(raw, analysis)
        for item in items_to_sync:
            _sync_report_row(sync_rows_by_product, _sync_product_from_item(item))["after_hard_exclusion"] += 1
        daily_similarity_count_by_ad = build_crawl_similarity_count_map(raw, items_to_sync)
        fallback_similarity_count_by_ad = build_daily_similarity_count_map(
            items_to_sync,
            play_asset_by_ad=play_asset_by_ad,
            effect_by_ad=effect_by_ad,
            play_fingerprint_by_ad=play_fingerprint_by_ad,
            cover_intraday_report=raw.get("cover_style_intraday_report"),
        )
        for ad_key, count in fallback_similarity_count_by_ad.items():
            daily_similarity_count_by_ad[ad_key] = max(
                int(daily_similarity_count_by_ad.get(ad_key) or 1),
                int(count or 1),
            )
        items_to_sync, template_skipped = apply_template_dedup_for_bitable(
            items_to_sync,
            play_asset_by_ad=play_asset_by_ad,
            effect_by_ad=effect_by_ad,
            play_fingerprint_by_ad=play_fingerprint_by_ad,
            template_fingerprint_by_ad=template_fingerprint_by_ad,
        )
        for skipped_row in template_skipped:
            product = str(skipped_row.get("product") or "未知产品").strip() or "未知产品"
            srow = _sync_report_row(sync_rows_by_product, product)
            srow["template_dedup_removed"] += 1
            _sync_report_add_reason(
                sync_reasons_by_product,
                product,
                "same_template_demographic_or_gender_swap",
            )
        for item in items_to_sync:
            _sync_report_row(sync_rows_by_product, _sync_product_from_item(item))["after_template_dedup"] += 1
        merge_template_dedup_similarity_counts(daily_similarity_count_by_ad, template_skipped)
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
                product = _sync_product_from_item(item)
                _sync_report_row(sync_rows_by_product, product)["same_play_non_representative_removed"] += 1
                _sync_report_add_reason(sync_reasons_by_product, product, "same_play_non_representative")
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
                product = _sync_product_from_item(item)
                _sync_report_row(sync_rows_by_product, product)["low_acceptance_removed"] += 1
                _sync_report_add_reason(sync_reasons_by_product, product, "low_acceptance_priority")
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
            play_fields = _build_bitable_play_fields(
                play_asset_info,
                play_fingerprint=play_fingerprint_by_ad.get(ad_key, ""),
                differentiator=differentiator_by_ad.get(ad_key, ""),
                template_fingerprint=template_fingerprint_by_ad.get(ad_key, ""),
            )
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
                **play_fields,
                "日内相似素材数": int(daily_similarity_count_by_ad.get(ad_key) or 1),
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
            tag_list.extend(cover_history_refresh_tags_by_ad.get(ad_key, []))
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

            raw_img_url = str(c.get("preview_img_url") or "").strip()
            img_url = normalize_cover_image_url_for_bitable(raw_img_url)
            if raw_img_url or img_url:
                ft = upload_image_as_attachment(raw_img_url, app_token) if raw_img_url else None
                if not ft and img_url and img_url != raw_img_url:
                    ft = upload_image_as_attachment(img_url, app_token)
                if ft:
                    fields["封面图"] = [{"file_token": ft}]

            v_direct = (pick_video_url(c) or "").strip()
            if v_direct and int(c.get("video_duration") or 0) > 0:
                vf = upload_video_as_attachment(v_direct, app_token, ad_key)
                if vf:
                    fields["视频附件"] = [{"file_token": vf}]

            records.append({"fields": fields})
            _sync_report_row(sync_rows_by_product, _sync_product_from_item(item))["synced_records"] += 1

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

    if target_date:
        _write_sync_report(
            target_date=target_date,
            sync_target=args.sync_target,
            need_raw_sync=need_raw_sync,
            rows_by_product=sync_rows_by_product,
            reasons_by_product=sync_reasons_by_product,
        )

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
