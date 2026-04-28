"""
将 Arrow2 的 raw + 视频分析结果同步到飞书多维表（仅主表，无聚类、无卡片、无「我方已投」筛选）。

依赖 .env：FEISHU_APP_ID、FEISHU_APP_SECRET、ARROW2_BITABLE_URL（或 --url）。

列含：人气值、展示估值、热度、投放天数、素材标签（视频/图片；试玩不写）、AI 分析等。「产品」为单选，选项来自 `config/arrow2_competitor.json` 的 `products`（另含「其他」）；若表里「产品」已是文本列需改类型或删列后由脚本自动建。
不写「标题」列；不写单独「素材分类」列（与「素材标签」合并）。「广大大链接」与 VE 相同拼法，但 SPA 参数 `type=1`（VE 主表为 `type=2`）；须在已登录广大大的浏览器中打开。
「人气值」列写入 `all_exposure_value`，「展示估值」列写入 `impression`（与此前对调）。
「创建时间」「更新时间」来自 raw 里 `creative.created_at` / `first_seen` / `last_seen`（Unix 秒转毫秒），与 VE 主表逻辑一致；缺字段则该列为空。
「投放地区」：将 ISO3（如 USA）转为中文国名（如 美国）后用顿号拼接；映射表见 `config/iso3166_alpha3_zh.json`（缺文件时回退内置子集）；已是中文的片段原样保留。
「素材类型」与三条链接列**互斥**：视频→仅填「视频链接」；图片→仅填「封面图链接」（主图 URL）；试玩广告→仅填「试玩链接」。其余两条链接列为空。
「视频链接」：mp4/直链类；「试玩链接」：HTML 试玩壳（`resource_urls.html_url` / `cdn_url`）；「封面图链接」：图片素材的主图（优先 `resource_urls.image_url`，否则 `preview_img_url`）。
可选「视频附件」：仅**视频**类型尝试上传。「试玩附件」：仅**试玩广告**类型将试玩 HTML 直链下载后上传为附件（字段名「试玩附件」）；可用 `ARROW2_PLAYABLE_HTML_ATTACH_ENABLED=0` 关闭。「封面图」附件：视频 / 图片 / 试玩均尝试上传（字段名「封面图」）。
「素材标签」：飞书列为**多选**（type 4）；**试玩广告**不写该列。其余类型写入选项名数组（若表内仍为旧「文本」列，需先在飞书改类型或建新列并配 ARROW2_FIELD_ALIAS_JSON）。

默认会调用飞书 API **补齐** `ARROW2_FIELD_DEFS` 中缺失列（需应用有建列权限）。
若仍无权限或只想写已有列：设 `ARROW2_ENSURE_FIELDS=0` 或传 `--skip-ensure-fields`。

用法（项目根目录）：
  .venv/bin/python scripts/sync_arrow2_to_bitable.py \\
    --raw data/workflow_arrow2_2026-04-14_raw.json \\
    --analysis data/video_analysis_workflow_arrow2_2026-04-14_raw.json
  .venv/bin/python scripts/sync_arrow2_to_bitable.py --from-db 2026-04-20
    # 从 data/arrow2_pipeline.db 的 arrow2_daily_insights 按业务日组装并同步
"""

from __future__ import annotations

# 必须先加载项目根 .env，再 import sync_raw（否则会先绑定空的 FEISHU_APP_ID）
import os

from dotenv import load_dotenv

from path_util import PROJECT_ROOT, config_path

load_dotenv(PROJECT_ROOT / ".env")

import argparse
import json
import re
import time
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse

import requests
import lark_oapi as lark
from lark_oapi.api.drive.v1.model import (
    UploadAllMediaRequest,
    UploadAllMediaRequestBody,
    UploadAllMediaResponse,
)
from guangdada_detail_url import try_build_url_spa
from tiktok_video_resolve import is_playable_ads_creative, pick_playable_html_url
from sync_raw_analysis_to_bitable_and_push_card import (
    BATCH_SIZE,
    batch_create_records,
    normalize_cover_image_url_for_bitable,
    pick_video_url,
    pick_video_urls,
    to_ms_from_date_str,
    to_ms_from_unix_sec,
    upload_image_as_attachment,
    upload_video_as_attachment,
)

from arrow2_pipeline_db import get_arrow2_pipeline_items_from_raw_payload


def get_tenant_access_token() -> str:
    """与 Video Enhancer 同步相同接口；每次从环境变量读取，避免 import 顺序导致空密钥。"""
    app_id = (os.getenv("FEISHU_APP_ID") or "").strip()
    app_secret = (os.getenv("FEISHU_APP_SECRET") or "").strip()
    if not app_id or not app_secret:
        raise RuntimeError("请在 .env 配置 FEISHU_APP_ID / FEISHU_APP_SECRET")
    resp = requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": app_id, "app_secret": app_secret},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"get tenant_access_token failed: {data}")
    return str(data["tenant_access_token"])


def _get_lark_client() -> lark.Client:
    """构建 lark.Client（复用 .env 中的 FEISHU_APP_ID / FEISHU_APP_SECRET）。"""
    app_id = (os.getenv("FEISHU_APP_ID") or "").strip()
    app_secret = (os.getenv("FEISHU_APP_SECRET") or "").strip()
    return lark.Client.builder().app_id(app_id).app_secret(app_secret).build()

DEFAULT_ARROW2_BITABLE_URL = (
    "https://scnmrtumk0zm.feishu.cn/base/W8QMbUR1vaiUGUskOF2cwnXenBe"
    "?table=tblQYmtjrgcS21xO&view=vewaeIFfng"
)


def _load_arrow2_product_single_select_options() -> List[Dict[str, str]]:
    """从 config/arrow2_competitor.json 的 products 生成单选项；末尾加「其他」兜底。"""
    path = PROJECT_ROOT / "config" / "arrow2_competitor.json"
    names: list[str] = []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return [{"name": "其他"}]
    for p in data.get("products") or []:
        if not isinstance(p, dict):
            continue
        k = str(p.get("keyword") or p.get("match") or "").strip()
        if k:
            names.append(k)
    seen: set[str] = set()
    out: List[Dict[str, str]] = []
    for n in names:
        if n not in seen:
            seen.add(n)
            out.append({"name": n})
    out.append({"name": "其他"})
    return out


_ARROW2_PRODUCT_SINGLE_OPTIONS = _load_arrow2_product_single_select_options()
ARROW2_PRODUCT_OPTION_NAMES = frozenset(o["name"] for o in _ARROW2_PRODUCT_SINGLE_OPTIONS)

_ARROW2_MATERIAL_TYPE_OPTIONS: List[Dict[str, str]] = [
    {"name": "视频"},
    {"name": "图片"},
    {"name": "试玩广告"},
]
ARROW2_MATERIAL_TYPE_NAMES = frozenset(o["name"] for o in _ARROW2_MATERIAL_TYPE_OPTIONS)


def _normalize_arrow2_material_type_for_select(raw: str) -> str:
    s = (raw or "").strip()
    if s in ARROW2_MATERIAL_TYPE_NAMES:
        return s
    return "视频"


def _normalize_arrow2_product_for_select(raw: str) -> str:
    s = (raw or "").strip()
    if s in ARROW2_PRODUCT_OPTION_NAMES:
        return s
    return "其他"


ARROW2_FIELD_DEFS: List[Dict[str, Any]] = [
    {"field_name": "产品", "type": 3, "options": _ARROW2_PRODUCT_SINGLE_OPTIONS},
    {"field_name": "素材类型", "type": 3, "options": _ARROW2_MATERIAL_TYPE_OPTIONS},
    {"field_name": "广告主", "type": 1},
    {"field_name": "正文（中文）", "type": 1},
    {"field_name": "平台", "type": 1},
    {"field_name": "投放地区", "type": 1},
    {"field_name": "视频链接", "type": 1},
    {"field_name": "试玩链接", "type": 1},
    {"field_name": "试玩附件", "type": 17},
    {"field_name": "广大大链接", "type": 1},
    {"field_name": "视频附件", "type": 17},
    {"field_name": "封面图链接", "type": 1},
    {"field_name": "封面图", "type": 17},
    {"field_name": "AI分析结果", "type": 1},
    {"field_name": "UA灵感借鉴", "type": 1},
    {"field_name": "抓取日期", "type": 5},
    {"field_name": "创建时间", "type": 5},
    {"field_name": "更新时间", "type": 5},
    {"field_name": "视频时长", "type": 2},
    {"field_name": "人气值", "type": 2},
    {"field_name": "展示估值", "type": 2},
    {"field_name": "热度", "type": 2},
    {"field_name": "投放天数", "type": 2},
    {"field_name": "一句话说明", "type": 1},
    {"field_name": "素材标签", "type": 4, "options": []},
    {"field_name": "广告ID", "type": 1},
]

# 飞书表头若与脚本不一致，按序尝试下列别名（首个在表中存在的生效）
ARROW2_FIELD_NAME_ALIASES: Dict[str, tuple[str, ...]] = {
    "UA灵感借鉴": ("UA灵感借鉴", "UA 灵感借鉴", "UA借鉴", "灵感借鉴"),
    "投放地区": ("投放地区", "地区", "国家/地区", "投放国家"),
    "素材标签": ("素材标签", "标签", "素材标签（多选）"),
    "视频链接": ("视频链接", "素材视频链接", "视频URL"),
    "试玩链接": ("试玩链接", "Playable链接", "HTML试玩"),
    "试玩附件": ("试玩附件", "HTML试玩附件", "Playable附件"),
    "素材类型": ("素材类型", "素材分类", "媒体类型"),
}


def _load_arrow2_field_alias_overrides() -> Dict[str, str]:
    """环境变量 ARROW2_FIELD_ALIAS_JSON，例：{\"UA灵感借鉴\":\"你的列名\"}"""
    raw = (os.getenv("ARROW2_FIELD_ALIAS_JSON") or "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(obj, dict):
        return {}
    return {str(k): str(v) for k, v in obj.items() if str(v).strip()}


def resolve_feishu_field_name(logical: str, existing: set[str]) -> str | None:
    """将脚本内逻辑列名解析为多维表中实际存在的 field_name。"""
    ovr = _load_arrow2_field_alias_overrides().get(logical)
    candidates: list[str] = []
    if ovr:
        candidates.append(ovr)
    candidates.append(logical)
    candidates.extend(ARROW2_FIELD_NAME_ALIASES.get(logical, ()))
    seen: set[str] = set()
    for c in candidates:
        if c in seen:
            continue
        seen.add(c)
        if c in existing:
            return c
    return None


def remap_record_fields_to_existing(
    fields: Dict[str, Any],
    existing: set[str],
) -> tuple[Dict[str, Any], list[str]]:
    """只保留表中存在的列；无法解析且值非空的逻辑列记入 missing_report。"""
    out: Dict[str, Any] = {}
    missing: list[str] = []
    for logical, val in fields.items():
        name = resolve_feishu_field_name(logical, existing)
        if name is None:
            empty = val is None or val == "" or val == [] or val == {}
            if not empty:
                missing.append(logical)
            continue
        out[name] = val
    return out, missing


def _arrow2_ensure_fields_from_env() -> bool:
    """默认 True（尝试自动建列）。仅当 ARROW2_ENSURE_FIELDS 为 0/false/no/off 时关闭。"""
    v = (os.getenv("ARROW2_ENSURE_FIELDS") or "").strip().lower()
    if not v:
        return True
    return v not in ("0", "false", "no", "off")


def parse_bitable_url(url: str) -> tuple[str, str]:
    parsed = urlparse(url.strip())
    parts = [p for p in parsed.path.split("/") if p]
    app_token = parts[1] if len(parts) >= 2 and parts[0] == "base" else ""
    table_id = (parse_qs(parsed.query or "").get("table") or [""])[0]
    if not app_token or not table_id:
        raise RuntimeError(f"无法从链接解析 app_token/table_id: {url}")
    return app_token, table_id


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
        if resp.status_code >= 400:
            try:
                err_detail = resp.json()
            except Exception:
                err_detail = (resp.text or "")[:2000]
            raise RuntimeError(f"HTTP {resp.status_code} list fields: {err_detail}")
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


def create_field(access_token: str, app_token: str, table_id: str, field: Dict[str, Any]) -> int | None:
    """成功返回 None；失败返回飞书业务 code（如 1254302），或 HTTP 状态码的负数。"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json; charset=utf-8"}
    body: Dict[str, Any] = {"field_name": field["field_name"], "type": int(field["type"])}
    ft = int(field["type"])
    opts = field.get("options")
    if ft == 4:
        body["property"] = {"options": list(opts) if isinstance(opts, list) else []}
    elif opts:
        body["options"] = opts
    resp = requests.post(url, headers=headers, json=body, timeout=10)
    try:
        data = resp.json()
    except Exception:
        data = {"raw_text": resp.text}
    if resp.status_code == 200 and isinstance(data, dict) and data.get("code") == 0:
        print(f"[arrow2-sync] 已创建字段：{field['field_name']}")
        return None
    print(f"[arrow2-sync] 创建字段失败 {field['field_name']}: {data}")
    if isinstance(data, dict) and data.get("code") is not None:
        try:
            return int(data["code"])
        except (TypeError, ValueError):
            pass
    return -int(resp.status_code)


# 无「管理字段 / 建列」权限时飞书常见 code，命中后不再尝试后续缺失列
_FEISHU_FIELD_CREATE_PERMISSION_DENIED_CODES = frozenset({1254302})


def ensure_arrow2_fields(access_token: str, app_token: str, table_id: str) -> None:
    existing = get_existing_field_names(access_token, app_token, table_id)
    for f in ARROW2_FIELD_DEFS:
        if f["field_name"] in existing:
            continue
        err = create_field(access_token, app_token, table_id, f)
        if err is not None and (
            err in _FEISHU_FIELD_CREATE_PERMISSION_DENIED_CODES or err == -403
        ):
            print(
                "[arrow2-sync] 当前应用无权通过 OpenAPI 自动建列（见上一条错误）。"
                "已停止继续建列；请在本多维表中手动添加与脚本一致的列名（见 ARROW2_FIELD_DEFS），"
                "或在飞书「高级权限」中为该应用开放本表的编辑/管理字段能力后再跑。"
            )
            break


def _arrow2_video_attach_enabled() -> bool:
    v = (os.getenv("ARROW2_VIDEO_ATTACH_ENABLED") or "").strip().lower()
    if v in ("0", "false", "no", "off"):
        return False
    return True


def _arrow2_playable_html_attach_enabled() -> bool:
    v = (os.getenv("ARROW2_PLAYABLE_HTML_ATTACH_ENABLED") or "").strip().lower()
    if v in ("0", "false", "no", "off"):
        return False
    return True


def _arrow2_upload_cover_image_with_fallback(
    creative: Dict[str, Any], app_token: str
) -> str | None:
    """上传封面图附件：优先 resource_urls.image_url，其次 preview_img_url（.image→.png）。"""
    cover_url = ""
    for r in creative.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("image_url"):
            cover_url = str(r["image_url"])
            break
    if not cover_url:
        cover_url = str(creative.get("preview_img_url") or "").strip()
    cover_url = normalize_cover_image_url_for_bitable(cover_url)
    if not cover_url:
        return None
    return upload_image_as_attachment(cover_url, app_token)


def _arrow2_upload_playable_html_as_attachment(
    html_url: str, app_token: str, *, log_errors: bool = True
) -> str | None:
    """下载试玩 HTML 直链后上传为飞书附件，返回 file_token。"""
    if not html_url:
        return None
    try:
        resp = requests.get(html_url, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        if log_errors:
            print(f"[sync] 下载试玩 HTML 失败: {e}")
        return None
    filename = "playable.html"
    body = (
        UploadAllMediaRequestBody.builder()
        .file_name(filename)
        .parent_type("bitable")
        .parent_node(app_token)
        .size(len(resp.content))
        .checksum("")
        .extra("")
        .file(BytesIO(resp.content))
        .build()
    )
    req = UploadAllMediaRequest.builder().request_body(body).build()
    resp_obj: UploadAllMediaResponse = _get_lark_client().drive.v1.media.upload_all(req)
    if resp_obj.success() and resp_obj.data and getattr(resp_obj.data, "file_token", None):
        return resp_obj.data.file_token
    if log_errors:
        print(f"[sync] 上传试玩 HTML 附件失败: code={getattr(resp_obj, 'code', '?')}")
    return None


def _try_arrow2_video_file_token(
    vu: str,
    vurls: List[str],
    app_token: str,
) -> str | None:
    """优先主视频 URL，失败则尝试列表中其它直链；用于「视频附件」列。"""
    if not _arrow2_video_attach_enabled():
        return None
    cands: list[str] = []
    if vu:
        cands.append(vu)
    for x in vurls:
        if x and x not in cands:
            cands.append(x)
    slice_c = cands[:3]
    for i, u in enumerate(slice_c):
        ft = upload_video_as_attachment(u, app_token)
        if ft:
            return ft
    return None


def _format_tags(material_tags: Any, pipeline_tags: Any) -> str:
    parts: list[str] = []
    if isinstance(material_tags, list):
        parts.extend(str(x) for x in material_tags if str(x).strip())
    elif isinstance(material_tags, str) and material_tags.strip():
        parts.append(material_tags.strip())
    if isinstance(pipeline_tags, list):
        for x in pipeline_tags:
            s = str(x).strip()
            if s and s not in parts:
                parts.append(s)
    return "、".join(parts) if parts else ""


# 常见 ISO3166-1 alpha-3 → 中文（`config/iso3166_alpha3_zh.json` 缺失或损坏时回退）
_ISO3_TO_ZH_FALLBACK: Dict[str, str] = {
    "USA": "美国",
    "JPN": "日本",
    "KOR": "韩国",
    "GBR": "英国",
    "CAN": "加拿大",
    "AUS": "澳大利亚",
    "BRA": "巴西",
    "MEX": "墨西哥",
    "RUS": "俄罗斯",
    "DEU": "德国",
    "FRA": "法国",
    "ITA": "意大利",
    "ESP": "西班牙",
    "NLD": "荷兰",
    "POL": "波兰",
    "TUR": "土耳其",
    "SAU": "沙特阿拉伯",
    "ARE": "阿联酋",
    "IND": "印度",
    "IDN": "印度尼西亚",
    "VNM": "越南",
    "THA": "泰国",
    "MYS": "马来西亚",
    "PHL": "菲律宾",
    "SGP": "新加坡",
    "CHN": "中国",
    "TWN": "台湾",
    "HKG": "香港",
    "MAC": "澳门",
    "PRT": "葡萄牙",
    "SWE": "瑞典",
    "NOR": "挪威",
    "DNK": "丹麦",
    "FIN": "芬兰",
    "CZE": "捷克",
    "AUT": "奥地利",
    "CHE": "瑞士",
    "BEL": "比利时",
    "IRL": "爱尔兰",
    "NZL": "新西兰",
    "ZAF": "南非",
    "EGY": "埃及",
    "NGA": "尼日利亚",
    "ARG": "阿根廷",
    "CHL": "智利",
    "COL": "哥伦比亚",
    "UKR": "乌克兰",
    "ISR": "以色列",
}


def _load_iso3_to_zh() -> Dict[str, str]:
    path = config_path("iso3166_alpha3_zh.json")
    if path.is_file():
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and data:
                out: Dict[str, str] = {}
                for k, v in data.items():
                    ku = str(k).strip().upper()
                    if len(ku) == 3 and ku.isalpha():
                        out[ku] = str(v).strip()
                if out:
                    return out
        except Exception:
            pass
    return dict(_ISO3_TO_ZH_FALLBACK)


_ISO3_TO_ZH: Dict[str, str] = _load_iso3_to_zh()


def _looks_like_chinese(s: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", s or ""))


def _geo_split_tokens(s: str) -> List[str]:
    """将广大大常见的「USA,JPN,KOR」整段或列表项拆成原子片段。"""
    s = (s or "").strip()
    if not s:
        return []
    parts = re.split(r"[,，\s]+", s)
    return [p.strip() for p in parts if p.strip()]


def _geo_token_to_zh(token: str) -> str:
    t = (token or "").strip()
    if not t:
        return ""
    if _looks_like_chinese(t):
        return t
    up = re.sub(r"\s+", "", t).upper()
    if up == "GLOBAL":
        return "全球"
    if len(up) == 3 and up.isalpha():
        return _ISO3_TO_ZH.get(up, t)
    return t


def _format_arrow2_geo_cell(c: Dict[str, Any]) -> str:
    """爬取补全的国家/地区：ISO3 转中文名，顿号拼接（支持逗号拼接的多国码）。"""
    co = c.get("countries")
    if isinstance(co, list) and co:
        parts: List[str] = []
        for x in co:
            if not str(x).strip():
                continue
            for tok in _geo_split_tokens(str(x)):
                z = _geo_token_to_zh(tok)
                if z:
                    parts.append(z)
        return "、".join(parts) if parts else ""
    s = str(c.get("country") or "").strip()
    if s:
        zs = [_geo_token_to_zh(t) for t in _geo_split_tokens(s)]
        zs = [z for z in zs if z]
        return "、".join(zs) if zs else ""
    s = str(c.get("region") or "").strip()
    if s:
        zs = [_geo_token_to_zh(t) for t in _geo_split_tokens(s)]
        zs = [z for z in zs if z]
        return "、".join(zs) if zs else ""
    return ""


def _arrow2_tags_list(it: Dict[str, Any], c: Dict[str, Any]) -> List[str]:
    """飞书多选：选项名列表（去重、限长）。"""
    parts: list[str] = []
    if isinstance(it.get("material_tags"), list):
        parts.extend(str(x).strip() for x in it["material_tags"] if str(x).strip())
    elif isinstance(it.get("material_tags"), str) and str(it.get("material_tags") or "").strip():
        parts.append(str(it["material_tags"]).strip())
    if isinstance(c.get("pipeline_tags"), list):
        for x in c["pipeline_tags"]:
            s = str(x).strip()
            if s:
                parts.append(s)
    cat = str(it.get("arrow2_material_category") or "").strip()
    if cat:
        parts.insert(0, cat)
    expanded: list[str] = []
    for p in parts:
        for seg in re.split(r"[、,，;；\n]+", p):
            t = seg.strip()
            if t:
                expanded.append(t)
    out: list[str] = []
    seen: set[str] = set()
    for p in expanded:
        if p not in seen:
            seen.add(p)
            out.append(p[:200])
    return out[:50]


def _arrow2_tags_cell(it: Dict[str, Any], c: Dict[str, Any]) -> str:
    """兼容：合并为可读串（调试或非多选场景）。"""
    lst = _arrow2_tags_list(it, c)
    return "、".join(lst) if lst else ""


def _raw_items_list(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    return get_arrow2_pipeline_items_from_raw_payload(raw)


def classify_arrow2_material_type(c: Dict[str, Any]) -> str:
    """视频 / 图片 / 试玩广告（与飞书「素材类型」单选项一致）。"""
    if not isinstance(c, dict):
        return "视频"
    if is_playable_ads_creative(c):
        return "试玩广告"
    v2p = int(c.get("video2pic") or 0)
    if v2p == 1:
        return "图片"
    vd = int(c.get("video_duration") or 0)
    if vd > 0:
        return "视频"
    for r in c.get("resource_urls") or []:
        if isinstance(r, dict) and str(r.get("video_url") or "").strip():
            return "视频"
    if str(c.get("video_url") or "").strip():
        return "视频"
    for r in c.get("resource_urls") or []:
        if isinstance(r, dict) and str(r.get("image_url") or "").strip():
            return "图片"
    if str(c.get("preview_img_url") or "").strip():
        return "图片"
    if pick_playable_html_url(c):
        return "试玩广告"
    return "视频"


def _pick_arrow2_image_url_for_link(c: Dict[str, Any]) -> str:
    """图片素材主图 URL：优先 resource_urls 中纯图条目，其次 preview_img_url。"""
    if not isinstance(c, dict):
        return ""
    for r in c.get("resource_urls") or []:
        if not isinstance(r, dict):
            continue
        iu = str(r.get("image_url") or "").strip()
        if iu and not str(r.get("video_url") or "").strip():
            return iu
    return str(c.get("preview_img_url") or "").strip()


def _arrow2_row_fields_dict(
    item: Dict[str, Any],
    c: Dict[str, Any],
    it: Dict[str, Any],
    *,
    app_token: str,
    target_ms: int | None,
) -> Dict[str, Any]:
    """it 为分析结果行；未跑分析时传空 dict。"""
    ak = str(c.get("ad_key") or "").strip()
    a = str(it.get("analysis") or "")
    body = str(c.get("body") or "")
    preview_raw = str(c.get("preview_img_url") or "").strip()
    mat_type = _normalize_arrow2_material_type_for_select(classify_arrow2_material_type(c))
    vu_base = str(it.get("video_url") or "").strip() or pick_video_url(c)
    playable_base = pick_playable_html_url(c)

    link_video = ""
    link_playable = ""
    link_cover = ""
    if mat_type == "试玩广告":
        link_playable = (playable_base or "")[:2000]
    elif mat_type == "图片":
        img_u = _pick_arrow2_image_url_for_link(c) or preview_raw
        link_cover = (normalize_cover_image_url_for_bitable(img_u) if img_u else "")[:2000]
    else:
        link_video = (vu_base or "")[:2000]

    vurls = pick_video_urls(c) if mat_type == "视频" and link_video else ([link_video] if link_video else [])
    cover_tok = _arrow2_upload_cover_image_with_fallback(c, app_token)
    video_file_tok = None
    if mat_type == "视频":
        video_file_tok = _try_arrow2_video_file_token(link_video, vurls, app_token)
    playable_html_tok = None
    if mat_type == "试玩广告" and link_playable and _arrow2_playable_html_attach_enabled():
        playable_html_tok = _arrow2_upload_playable_html_as_attachment(
            link_playable, app_token, log_errors=True
        )

    imp = int(c.get("impression") or 0)
    exp = int(c.get("all_exposure_value") or 0)
    heat = int(c.get("heat") or 0)
    days = int(c.get("days_count") or 0)
    vd = int(c.get("video_duration") or 0)

    tag_list = [] if mat_type == "试玩广告" else _arrow2_tags_list(it, c)
    liner = str(it.get("ad_one_liner") or "").strip()[:80]
    gd_url = try_build_url_spa(c, creative_type=1)
    geo_cell = _format_arrow2_geo_cell(c)[:2000]

    fields: Dict[str, Any] = {
        "产品": _normalize_arrow2_product_for_select(str(item.get("product") or "")),
        "素材类型": mat_type,
        "广告主": str(c.get("advertiser_name") or c.get("page_name") or ""),
        "正文（中文）": body[:4000] if body else "",
        "平台": str(c.get("platform") or ""),
        "投放地区": geo_cell,
        "视频链接": link_video,
        "试玩链接": link_playable,
        "广大大链接": gd_url,
        "封面图链接": link_cover,
        "AI分析结果": a[:20000],
        "UA灵感借鉴": str(it.get("ua_suggestion_single") or "")[:8000],
        "视频时长": vd,
        "人气值": exp,
        "展示估值": imp,
        "热度": heat,
        "投放天数": days,
        "一句话说明": liner,
        "广告ID": ak[:200],
    }
    if tag_list:
        fields["素材标签"] = tag_list
    if cover_tok:
        fields["封面图"] = [{"file_token": cover_tok}]
    if video_file_tok:
        fields["视频附件"] = [{"file_token": video_file_tok}]
    if playable_html_tok:
        fields["试玩附件"] = [{"file_token": playable_html_tok}]
    if target_ms is not None:
        fields["抓取日期"] = target_ms
    created_ms = to_ms_from_unix_sec(c.get("created_at"))
    first_seen_ms = to_ms_from_unix_sec(c.get("first_seen"))
    last_seen_ms = to_ms_from_unix_sec(c.get("last_seen"))
    ctime_ms = created_ms if created_ms is not None else first_seen_ms
    if ctime_ms is not None:
        fields["创建时间"] = ctime_ms
    utime_ms = last_seen_ms if last_seen_ms is not None else ctime_ms
    if utime_ms is not None:
        fields["更新时间"] = utime_ms
    return fields


def build_records(
    raw: Dict[str, Any],
    analysis: Dict[str, Any],
    app_token: str,
) -> List[Dict[str, Any]]:
    if analysis.get("sync_from_raw_only"):
        return _build_records_from_raw_only(raw, app_token)

    raw_by_ad: Dict[str, Dict[str, Any]] = {}
    for item in _raw_items_list(raw):
        if not isinstance(item, dict):
            continue
        c = item.get("creative") or {}
        if not isinstance(c, dict):
            continue
        ak = str(c.get("ad_key") or "").strip()
        if ak:
            raw_by_ad[ak] = item

    target_date = str(raw.get("target_date") or "")
    target_ms = to_ms_from_date_str(target_date)

    records: List[Dict[str, Any]] = []
    for it in analysis.get("results") or []:
        if not isinstance(it, dict):
            continue
        ak = str(it.get("ad_key") or "").strip()
        a = str(it.get("analysis") or "")
        if not ak or not a.strip() or a.startswith("[ERROR]"):
            continue
        item = raw_by_ad.get(ak)
        if not item:
            continue
        c = item.get("creative") or {}
        if not isinstance(c, dict):
            c = {}

        fields = _arrow2_row_fields_dict(item, c, it, app_token=app_token, target_ms=target_ms)
        records.append({"fields": fields})

    return records


def _build_records_from_raw_only(raw: Dict[str, Any], app_token: str) -> List[Dict[str, Any]]:
    target_ms = to_ms_from_date_str(str(raw.get("target_date") or ""))
    records: List[Dict[str, Any]] = []
    for item in _raw_items_list(raw):
        if not isinstance(item, dict):
            continue
        c = item.get("creative") or {}
        if not isinstance(c, dict):
            continue
        ak = str(c.get("ad_key") or "").strip()
        if not ak:
            continue
        fields = _arrow2_row_fields_dict(item, c, {}, app_token=app_token, target_ms=target_ms)
        records.append({"fields": fields})
    return records


def _batch_create_with_feishu_hint(
    access_token: str,
    app_token: str,
    table_id: str,
    records_batch: List[Dict[str, Any]],
) -> None:
    """包装 batch_create：失败时打印飞书 JSON（如 1254302），便于与 Video 主表权限区分排查。"""
    hint = (
        "  说明：code 1254302（The role has no permissions）= 当前应用（tenant token）"
        "对该 Base/子表无此 API 权限，与脚本逻辑无关。\n"
        "  请检查：①开放平台该应用权限含「多维表格」；②多维表协作者里该应用为可编辑；\n"
        "  ③若开启高级权限：在权限方案里为该应用勾选本数据表可编辑/新增记录；\n"
        "  ④文档：https://open.feishu.cn/document/server-docs/docs/bitable-v1/bitable-overview"
    )
    try:
        batch_create_records(access_token, app_token, table_id, records_batch)
    except requests.HTTPError as e:
        detail = ""
        if e.response is not None:
            try:
                detail = json.dumps(e.response.json(), ensure_ascii=False)
            except Exception:
                detail = (e.response.text or "")[:2500]
        print(
            "[arrow2-sync] batch_create HTTP 错误，响应：\n"
            f"  {detail}\n" + hint
        )
        raise
    except RuntimeError as e:
        msg = str(e)
        print("[arrow2-sync] batch_create 业务错误：\n  " + msg + "\n" + hint)
        raise


def _parse_db_material_tags(raw: Any) -> List[str]:
    if raw is None:
        return []
    s = str(raw).strip()
    if not s:
        return []
    if s.startswith("["):
        try:
            v = json.loads(s)
            if isinstance(v, list):
                return [str(x).strip() for x in v if str(x).strip()]
            return [str(v).strip()] if str(v).strip() else []
        except Exception:
            return [s]
    return [s]


def _load_arrow2_sync_from_db(target_date: str) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """从 arrow2_daily_insights 组装与 JSON 同步等价的 raw + analysis。"""
    from arrow2_pipeline_db import init_db, _conn

    init_db()
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT product, appid, raw_json, insight_analysis, insight_ua_suggestion,
                   material_tags, insight_material_category, ad_one_liner
            FROM arrow2_daily_insights
            WHERE target_date = ?
            ORDER BY ad_key
            """,
            (target_date,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    items: List[Dict[str, Any]] = []
    results: List[Dict[str, Any]] = []
    for r in rows:
        raw_js = str(r["raw_json"] or "").strip()
        if not raw_js:
            continue
        try:
            c = json.loads(raw_js)
        except Exception:
            continue
        if not isinstance(c, dict):
            continue
        ak = str(c.get("ad_key") or "").strip()
        if not ak:
            continue
        product = str(r["product"] or "")
        appid = str(r["appid"] or "")
        items.append({"product": product, "appid": appid, "creative": c})
        mt = _parse_db_material_tags(r["material_tags"])
        results.append(
            {
                "ad_key": ak,
                "analysis": str(r["insight_analysis"] or ""),
                "ua_suggestion_single": str(r["insight_ua_suggestion"] or ""),
                "material_tags": mt,
                "arrow2_material_category": str(r["insight_material_category"] or ""),
                "ad_one_liner": str(r["ad_one_liner"] or ""),
            }
        )

    raw: Dict[str, Any] = {
        "target_date": target_date,
        "workflow": "arrow2_competitor",
        "items": items,
    }
    analysis: Dict[str, Any] = {
        "workflow": "arrow2_competitor",
        "results": results,
    }
    return raw, analysis


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Arrow2 同步 raw+分析到飞书多维表")
    p.add_argument(
        "--url",
        default="",
        help="多维表完整 URL；默认读 ARROW2_BITABLE_URL，否则使用项目内置默认表",
    )
    p.add_argument(
        "--from-db",
        default="",
        help="从 SQLite arrow2_daily_insights 按 target_date（YYYY-MM-DD）组装数据；无需 --raw/--analysis",
    )
    p.add_argument("--raw", default="", help="*_raw.json（与 --analysis 成对；与 --from-db 二选一）")
    p.add_argument("--analysis", default="", help="video_analysis_*_raw.json")
    p.add_argument(
        "--ensure-fields",
        action="store_true",
        help="强制走自动建列（默认已开启；与未设置 ARROW2_ENSURE_FIELDS 或设为 1 时相同）",
    )
    p.add_argument(
        "--skip-ensure-fields",
        action="store_true",
        help="跳过列举/创建字段，只写入表中已有列（等价于 ARROW2_ENSURE_FIELDS=0）",
    )
    p.add_argument(
        "--only-ad-key",
        default="",
        help="仅同步该 ad_key 一条（用于试跑附件等；需该条在 analysis 中且分析非空）",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    url = (args.url or os.getenv("ARROW2_BITABLE_URL") or "").strip() or DEFAULT_ARROW2_BITABLE_URL
    if not (os.getenv("FEISHU_APP_ID") or "").strip() or not (os.getenv("FEISHU_APP_SECRET") or "").strip():
        raise SystemExit("请配置 FEISHU_APP_ID / FEISHU_APP_SECRET（项目根 .env）")

    from_db = (args.from_db or "").strip()
    if from_db:
        raw, analysis = _load_arrow2_sync_from_db(from_db)
        n_it = len(_raw_items_list(raw))
        n_rs = len(analysis.get("results") or [])
        print(f"[arrow2-sync] 已从数据库加载 target_date={from_db!r}，items={n_it}，analysis 行={n_rs}")
    elif (args.raw or "").strip() and (args.analysis or "").strip():
        raw = json.loads(Path(args.raw).read_text(encoding="utf-8"))
        analysis = json.loads(Path(args.analysis).read_text(encoding="utf-8"))
    else:
        raise SystemExit("请指定 --from-db YYYY-MM-DD，或同时提供 --raw 与 --analysis")

    only_ak = (args.only_ad_key or "").strip()
    if only_ak:

        def _filter_raw_by_ad_key(r: Dict[str, Any]) -> Dict[str, Any]:
            items = [
                it
                for it in _raw_items_list(r)
                if isinstance(it, dict)
                and str((it.get("creative") or {}).get("ad_key") or "").strip() == only_ak
            ]
            out = dict(r)
            out["items"] = items
            return out

        def _filter_analysis_by_ad_key(a: Dict[str, Any]) -> Dict[str, Any]:
            rs = [
                x
                for x in (a.get("results") or [])
                if isinstance(x, dict) and str(x.get("ad_key") or "").strip() == only_ak
            ]
            out = dict(a)
            out["results"] = rs
            return out

        raw = _filter_raw_by_ad_key(raw)
        analysis = _filter_analysis_by_ad_key(analysis)
        print(f"[arrow2-sync] --only-ad-key={only_ak!r}，将只写入 0 或 1 条（视是否存在且分析成功）")

    app_token, table_id = parse_bitable_url(url)
    token = get_tenant_access_token()

    if args.ensure_fields and args.skip_ensure_fields:
        raise SystemExit("不能同时指定 --ensure-fields 与 --skip-ensure-fields")
    if args.skip_ensure_fields:
        want_ensure_fields = False
    elif args.ensure_fields:
        want_ensure_fields = True
    else:
        want_ensure_fields = _arrow2_ensure_fields_from_env()

    if not want_ensure_fields:
        print(
            "[arrow2-sync] 已跳过自动建列（ARROW2_ENSURE_FIELDS=0 或 --skip-ensure-fields）。"
            "仅写入表中已有列；列名不一致可用 ARROW2_FIELD_ALIAS_JSON。"
        )
    else:
        try:
            ensure_arrow2_fields(token, app_token, table_id)
        except RuntimeError as e:
            msg = str(e)
            if "403" in msg or "deny" in msg.lower() or "999916" in msg:
                print(
                    "[arrow2-sync] 字段 API 被拒绝（403/deny 常见）：应用对该表无「查看/管理字段」权限，"
                    "但可能仍可「新增记录」。将跳过自动建列并继续写入。\n"
                    f"  详情: {msg[:900]}"
                )
            else:
                raise
        except requests.HTTPError as e:
            body = (e.response.text or "")[:1200] if e.response is not None else ""
            code = e.response.status_code if e.response is not None else 0
            print(
                f"[arrow2-sync] 字段接口失败 HTTP {code}\n"
                f"  响应片段: {body}\n"
                f"  将跳过自动建字段并尝试直接写入记录。"
            )
            if code not in (403, 400):
                raise

    records = build_records(raw, analysis, app_token)
    existing_names = get_existing_field_names(token, app_token, table_id)
    remapped: List[Dict[str, Any]] = []
    missing_logical: set[str] = set()
    for r in records:
        fields, miss = remap_record_fields_to_existing(r.get("fields") or {}, existing_names)
        missing_logical.update(miss)
        if fields:
            remapped.append({"fields": fields})
    records = remapped
    if missing_logical:
        print(
            "[arrow2-sync] 下列逻辑列在多维表中无匹配列名（已跳过该列写入；可用 ARROW2_FIELD_ALIAS_JSON 指定映射）："
            + ", ".join(sorted(missing_logical))
        )

    total = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        if not batch:
            continue
        _batch_create_with_feishu_hint(token, app_token, table_id, batch)
        total += len(batch)
        time.sleep(0.2)
    print(f"[arrow2-sync] 已写入 {total} 条到多维表。")


if __name__ == "__main__":
    main()
