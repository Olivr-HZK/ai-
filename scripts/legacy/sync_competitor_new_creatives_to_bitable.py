"""
将 daily UA job 每天抓到、按 ad_key 去重后的「raw 素材」同步到新的飞书多维表格。

- 数据来源：data/ai_products_ua.db -> ai_products_crawl（daily_ua_job 的步骤 1 和 2）
- 只看 crawl_date = 今天 的数据
- 去重逻辑：同一 ad_key 只保留一条（优先保留最先出现的记录）

用法（项目根目录）：
  python scripts/sync_competitor_new_creatives_to_bitable.py
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv
import lark_oapi as lark
from lark_oapi.api.drive.v1.model import (
    UploadAllMediaRequest,
    UploadAllMediaRequestBody,
    UploadAllMediaResponse,
)

from ua_crawl_db import get_conn as get_ua_conn


load_dotenv()

FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")

NEW_COMP_BITABLE_APP_TOKEN = os.getenv("NEW_COMP_BITABLE_APP_TOKEN", "")
NEW_COMP_BITABLE_TABLE_ID = os.getenv("NEW_COMP_BITABLE_TABLE_ID", "")

BATCH_SIZE = 200

# 本表会用到的字段名集合（仅用于 ensure，不限制必需字段）
NEW_BITABLE_FIELD_NAMES = {
    "抓取日期",
    "类目",
    "产品",
    "AppID",
    "是否我方产品",
    "UA关键词",
    "广告ID",
    "广告主",
    "标题",
    "正文",
    "平台",
    "视频链接",
    "封面图链接",
    "封面图",
    "投放天数",
    "展示估值",
    "热度",
    "total_captured",
    "原始JSON",
}

# 字段定义（类型与简单属性），若字段不存在则按此创建
NEW_FIELD_DEFS: List[Dict[str, Any]] = [
    {"field_name": "抓取日期", "type": 5},
    {"field_name": "类目", "type": 1},
    {"field_name": "产品", "type": 1},
    {"field_name": "AppID", "type": 1},
    {"field_name": "是否我方产品", "type": 1},
    {"field_name": "UA关键词", "type": 1},
    {"field_name": "广告ID", "type": 1},
    {"field_name": "广告主", "type": 1},
    {"field_name": "标题", "type": 1},
    {"field_name": "正文", "type": 1},
    {"field_name": "平台", "type": 1},
    {"field_name": "视频链接", "type": 1},
    {"field_name": "封面图链接", "type": 1},
    {"field_name": "封面图", "type": 17},
    {"field_name": "投放天数", "type": 2},
    {"field_name": "展示估值", "type": 2},
    {"field_name": "热度", "type": 2},
    {"field_name": "total_captured", "type": 2},
    {"field_name": "原始JSON", "type": 1},
]


def get_tenant_access_token() -> str:
    """
    获取 tenant_access_token（沿用现有飞书应用）。
    """
    if not FEISHU_APP_ID or not FEISHU_APP_SECRET:
        raise RuntimeError("请在 .env 中配置 FEISHU_APP_ID 和 FEISHU_APP_SECRET")
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(
        url,
        json={
            "app_id": FEISHU_APP_ID,
            "app_secret": FEISHU_APP_SECRET,
        },
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"get tenant_access_token failed: {data}")
    return data["tenant_access_token"]


# 全局 lark 客户端，仅用于在新 Base 下上传封面图附件
_LARK_CLIENT: lark.Client | None = None


def _get_lark_client() -> lark.Client:
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


def _get_existing_field_names(access_token: str) -> set[str]:
    """
    列出当前目标多维表格中已有的字段名。
    """
    if not NEW_COMP_BITABLE_APP_TOKEN or not NEW_COMP_BITABLE_TABLE_ID:
        raise RuntimeError(
            "请在 .env 中配置 NEW_COMP_BITABLE_APP_TOKEN 和 NEW_COMP_BITABLE_TABLE_ID"
        )
    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{NEW_COMP_BITABLE_APP_TOKEN}/tables/{NEW_COMP_BITABLE_TABLE_ID}/fields"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    field_names: set[str] = set()
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
        for item in items:
            name = item.get("field_name")
            if name:
                field_names.add(name)
        if not data_obj.get("has_more"):
            break
        page_token = data_obj.get("page_token")
    return field_names


def _create_field(
    access_token: str,
    field_name: str,
    field_type: int,
) -> None:
    """
    在目标多维表中创建一个简单字段。
    """
    if not NEW_COMP_BITABLE_APP_TOKEN or not NEW_COMP_BITABLE_TABLE_ID:
        raise RuntimeError(
            "请在 .env 中配置 NEW_COMP_BITABLE_APP_TOKEN 和 NEW_COMP_BITABLE_TABLE_ID"
        )
    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{NEW_COMP_BITABLE_APP_TOKEN}/tables/{NEW_COMP_BITABLE_TABLE_ID}/fields"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    body: Dict[str, Any] = {
        "field_name": field_name,
        "type": field_type,
    }
    resp = requests.post(url, headers=headers, json=body, timeout=10)
    try:
        data = resp.json()
    except Exception:
        data = {"raw_text": resp.text}
    if resp.status_code != 200 or data.get("code") != 0:
        print(
            f"[sync_daily_raw_ua] 创建字段失败（{field_name}），"
            f"status={resp.status_code}, resp={data}"
        )
    else:
        print(f"[sync_daily_raw_ua] 已创建字段：{field_name}")


def ensure_new_table_fields(access_token: str) -> None:
    """
    确保新多维表中存在脚本使用到的字段，不存在的会自动创建。
    """
    existing = _get_existing_field_names(access_token)
    for f in NEW_FIELD_DEFS:
        name = f["field_name"]
        if name in existing:
            continue
        _create_field(access_token, field_name=name, field_type=int(f["type"]))


def upload_image_for_new_table(access_token: str, image_url: str) -> str | None:
    """
    将网络图片上传为附件，返回 file_token（父节点为新 Base）。
    """
    if not image_url:
        return None

    try:
        resp = requests.get(image_url, timeout=15)
        resp.raise_for_status()
        content = resp.content
    except Exception as e:
        print(f"[sync_daily_raw_ua] 下载封面图失败：{image_url}，错误：{e}")
        return None

    from urllib.parse import urlparse
    import mimetypes
    import os
    from io import BytesIO

    parsed = urlparse(image_url)
    filename = os.path.basename(parsed.path) or "image"
    if "." not in filename:
        ext = mimetypes.guess_extension(resp.headers.get("Content-Type", "image/jpeg"))
        if ext:
            filename = filename + ext

    client = _get_lark_client()
    try:
        body = (
            UploadAllMediaRequestBody.builder()
            .file_name(filename)
            .parent_type("bitable")
            .parent_node(NEW_COMP_BITABLE_APP_TOKEN)
            .size(len(content))
            .checksum("")
            .extra("")
            .file(BytesIO(content))
            .build()
        )
        req = UploadAllMediaRequest.builder().request_body(body).build()
        resp2: UploadAllMediaResponse = client.drive.v1.media.upload_all(req)
        if resp2.success() and resp2.data and getattr(resp2.data, "file_token", None):
            return resp2.data.file_token
        raw = {}
        if resp2.raw and getattr(resp2.raw, "content", None):
            try:
                raw = json.loads(resp2.raw.content)
            except Exception:
                raw = {"raw_text": str(resp2.raw.content)}
        print(
            f"[sync_daily_raw_ua] 上传封面图失败：{image_url}，"
            f"code={resp2.code}, msg={resp2.msg}, body={raw}"
        )
    except Exception as e:
        print(f"[sync_daily_raw_ua] 上传封面图异常：{image_url}，错误：{e}")
    return None


def batch_create_records(
    access_token: str,
    records: List[Dict[str, Any]],
    client_token: str | None = None,
) -> None:
    """
    新增多条记录到「daily 原始 UA 素材」多维表格（新 Base）。
    """
    if not NEW_COMP_BITABLE_APP_TOKEN or not NEW_COMP_BITABLE_TABLE_ID:
        raise RuntimeError(
            "请在 .env 中配置 NEW_COMP_BITABLE_APP_TOKEN 和 NEW_COMP_BITABLE_TABLE_ID"
        )

    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{NEW_COMP_BITABLE_APP_TOKEN}/tables/{NEW_COMP_BITABLE_TABLE_ID}/records/batch_create"
    )

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=utf-8",
    }

    if client_token is None:
        client_token = str(uuid.uuid4())

    params = {
        "user_id_type": "open_id",
        "client_token": client_token,
    }

    body = {
        "records": records,
    }

    resp = requests.post(
        url,
        headers=headers,
        params=params,
        json=body,
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"batch_create failed: {data}")


def fetch_today_ua_rows(
    conn: sqlite3.Connection,
    crawl_date: str,
    batch_size: int,
):
    """
    以批次方式读取 ai_products_crawl 中指定日期的「竞品」数据。
    仅选择 is_our_product = 0 的行。
    """
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          crawl_date,
          category,
          product,
          appid,
          keyword,
          selected,
          total_captured
        FROM ai_products_crawl
        WHERE crawl_date = ?
          AND COALESCE(is_our_product, 0) = 0
        ORDER BY category, product, id
        """,
        (crawl_date,),
    )
    while True:
        batch = cur.fetchmany(batch_size)
        if not batch:
            break
        yield batch


def _parse_selected(selected_val: Any) -> dict:
    """
    ai_products_crawl.selected 可能是 JSON 字符串或已反序列化的 dict。
    """
    if not selected_val:
        return {}
    if isinstance(selected_val, dict):
        return selected_val
    if isinstance(selected_val, str):
        try:
            return json.loads(selected_val)
        except Exception:
            return {}
    return {}


def dedup_by_ad_key(
    conn: sqlite3.Connection,
    crawl_date: str,
    batch_size: int,
) -> List[Dict[str, Any]]:
    """
    从 ai_products_crawl 中读取今天的数据，按 ad_key 去重，返回去重后的列表。
    每个元素包含 meta + creative：
      {
        "crawl_date", "category", "product", "appid",
        "keyword", "is_our_product", "total_captured",
        "creative": {... 原始 selected dict ...}
      }
    """
    seen: set[str] = set()
    items: List[Dict[str, Any]] = []
    for rows in fetch_today_ua_rows(conn, crawl_date, batch_size):
        for row in rows:
            sel = _parse_selected(row["selected"])
            if not isinstance(sel, dict):
                continue
            ad_key = (
                sel.get("ad_key")
                or sel.get("creative_id")
                or sel.get("id")
                or sel.get("creativeId")
                or ""
            )
            if not ad_key:
                continue
            if ad_key in seen:
                continue
            seen.add(ad_key)
            items.append(
                {
                    "crawl_date": row["crawl_date"],
                    "category": row["category"],
                    "product": row["product"],
                    "appid": row["appid"],
                    "keyword": row["keyword"],
                    "total_captured": row["total_captured"],
                    "creative": sel,
                }
            )
    # 尝试从 ad_creative_analysis 中补充 title_zh / body_zh / 统一封面图等信息
    if not items:
        return items
    ad_keys = [
        (
            it.get("creative") or {}
        ).get("ad_key")
        for it in items
        if isinstance(it.get("creative"), dict)
    ]
    ad_keys = [k for k in ad_keys if k]
    if not ad_keys:
        return items
    placeholders = ",".join("?" for _ in ad_keys)
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT ad_key, title_zh, body_zh, preview_img_url, video_url
            FROM ad_creative_analysis
            WHERE ad_key IN ({placeholders})
            """,
            ad_keys,
        )
        zh_map: Dict[str, Dict[str, Any]] = {}
        for r in cur.fetchall():
            zh_map[r[0]] = {
                "title_zh": r[1],
                "body_zh": r[2],
                "preview_img_url": r[3],
                "video_url": r[4],
            }
        for it in items:
            creative = it.get("creative") or {}
            if not isinstance(creative, dict):
                continue
            ad_key = creative.get("ad_key")
            if not ad_key:
                continue
            extra = zh_map.get(ad_key)
            if not extra:
                continue
            if extra.get("title_zh"):
                creative["title_zh"] = extra["title_zh"]
            if extra.get("body_zh"):
                creative["body_zh"] = extra["body_zh"]
            if extra.get("preview_img_url"):
                creative["preview_img_url"] = extra["preview_img_url"]
            if extra.get("video_url"):
                creative["video_url"] = extra["video_url"]
    except Exception as e:
        print(f"[sync_daily_raw_ua] 补充翻译字段失败，将继续使用原始文案: {e}")
    return items


def row_to_fields(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    将一条去重后的 daily UA 原始素材转换为多维表格字段。

    建议在新多维表格中创建以下字段（若不存在，可在表中手动添加）：
      - 抓取日期
      - 类目
      - 产品
      - AppID
      - 是否我方产品
      - UA关键词
      - 广告ID
      - 广告主
      - 标题
      - 正文
      - 平台
      - 视频链接
      - 封面图链接
      - 投放天数
      - 展示估值
      - 热度
      - total_captured
      - 原始JSON
    """
    creative = item.get("creative") or {}

    def _video_url(c: dict) -> str:
        if c.get("video_url"):
            return c["video_url"]
        for r in c.get("resource_urls") or []:
            if r.get("video_url"):
                return r["video_url"]
        return ""

    title = creative.get("title_zh") or creative.get("title") or ""
    body = creative.get("body_zh") or creative.get("body") or ""
    preview = creative.get("preview_img_url") or ""

    fields: Dict[str, Any] = {
        # 新表中的「抓取日期」当前是文本字段，这里直接写 YYYY-MM-DD 字符串
        "抓取日期": item.get("crawl_date") or "",
        "类目": item.get("category") or "",
        "产品": item.get("product") or "",
        "AppID": json.dumps(item.get("appid") or [], ensure_ascii=False),
        "是否我方产品": "否",
        "UA关键词": item.get("keyword") or "",
        "广告ID": creative.get("ad_key") or "",
        "广告主": creative.get("advertiser_name") or creative.get("page_name") or "",
        "标题": title,
        "正文": body,
        "平台": creative.get("platform") or "",
        "视频链接": _video_url(creative),
        "封面图链接": preview,
        "投放天数": int(creative.get("days_count") or 0),
        "展示估值": int(creative.get("all_exposure_value") or 0),
        "热度": int(creative.get("heat") or 0),
        "total_captured": int(item.get("total_captured") or 0),
        "原始JSON": json.dumps(creative, ensure_ascii=False),
    }
    # 封面图附件（如果字段存在）
    if preview:
        # 这里不检查字段是否存在，由飞书后端忽略未知字段
        file_token = upload_image_for_new_table("", preview)
        # upload_image_for_new_table 内部自行使用 lark client & app token
        if file_token:
            fields["封面图"] = [{"file_token": file_token}]
    return fields


def main() -> None:
    crawl_date = datetime.now().strftime("%Y-%m-%d")
    print(f"[sync_daily_raw_ua] 准备同步日期为 {crawl_date} 的去重 raw 素材到新多维表格...")

    token = get_tenant_access_token()
    # 确保新表中有脚本会写入的字段
    ensure_new_table_fields(token)

    conn = get_ua_conn()
    try:
        # 按 daily UA job 结果，从 ai_products_crawl 中读今天的数据并按 ad_key 去重
        items = dedup_by_ad_key(conn, crawl_date, BATCH_SIZE)
        if not items:
            print(f"[sync_daily_raw_ua] {crawl_date} 在 ai_products_crawl 中没有可用素材，本次不同步。")
            return

        print(f"[sync_daily_raw_ua] {crawl_date} 去重后共有 {len(items)} 条素材待同步。")

        total_sent = 0
        batch_idx = 0
        for i in range(0, len(items), BATCH_SIZE):
            batch = items[i : i + BATCH_SIZE]
            batch_idx += 1
            records: List[Dict[str, Any]] = []
            for item in batch:
                fields = row_to_fields(item)
                records.append({"fields": fields})

            if not records:
                continue

            client_token = str(uuid.uuid4())
            batch_create_records(token, records, client_token=client_token)

            total_sent += len(records)
            print(
                f"[sync_daily_raw_ua] 已同步批次 {batch_idx}，本批 {len(records)} 条，总计 {total_sent} 条"
            )
            time.sleep(0.2)

        print(f"[sync_daily_raw_ua] 完成，本次共写入记录数：{total_sent}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

