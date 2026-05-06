"""
按指定日期，将 SQLite 中 ad_creative_analysis 表「当天新创建」的创意
同步到「基础创意信息」多维表（只用中文标题/正文，并上传封面图）。

- 数据来源：data/ai_products_ua.db -> ad_creative_analysis
- 仅同步 date(created_at) = 指定日期 的记录
- 标题/正文：仅使用 title_zh / body_zh，不回退到原始英文 title/body
- 封面图：使用 preview_img_url 下载并上传为附件写入「封面图」字段

用法（项目根目录）：
  source .venv/bin/activate
  python scripts/sync_ad_creative_basic_by_date.py --date 2026-03-17
"""

from __future__ import annotations

import argparse
import sqlite3
import time
import uuid
from typing import Dict, Any, List

from sync_ad_creative_to_bitable import (  # type: ignore
    DB_PATH,
    BATCH_SIZE,
    get_tenant_access_token,
    parse_datetime_to_ms,
)

import os
import requests
import json

# 仅用于 basic 表按日期同步：写死到基础创意多维表
# 当前表链接: https://scnmrtumk0zm.feishu.cn/base/NeQMbzw3Sa8acEs9TEWcyXB3nvh?table=tblav2t9VjD3MFsJ
BASIC_APP_TOKEN = "NeQMbzw3Sa8acEs9TEWcyXB3nvh"
BASIC_TABLE_ID = "tblav2t9VjD3MFsJ"

FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "your_app_id_here")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "your_app_secret_here")


def fetch_ad_creatives_by_date(
    conn: sqlite3.Connection,
    date_str: str,
    batch_size: int,
):
    """
    以批次方式读取 ad_creative_analysis 中「created_at 落在指定日期当天」的记录。
    """
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          ad_key,
          crawl_date,
          category,
          product,
          advertiser_name,
          title_zh,
          body_zh,
          platform,
          video_url,
          video_duration,
          preview_img_url,
          llm_analysis,
          created_at,
          updated_at
        FROM ad_creative_analysis
        WHERE date(created_at) = date(?, 'localtime')
        ORDER BY created_at, ad_key
        """,
        (date_str,),
    )
    while True:
        batch = cur.fetchmany(batch_size)
        if not batch:
            break
        yield batch


def row_to_basic_fields(row: sqlite3.Row, access_token: str) -> Dict[str, Any]:
    """
    将一行 ad_creative_analysis 转为多维表格记录字段：
      - 标题/正文仅用中文版本（title_zh/body_zh），不写入原始英文
      - 上传封面图到「封面图」字段
    依赖 sync_ad_creative_to_bitable 中已存在的字段定义：
      标题 / 类目 / 产品 / 广告主 / 正文（中文） / 平台 / 视频链接 / 封面图链接 / 封面图 /
      AI分析结果 / 抓取日期 / 创建时间 / 更新时间 / 视频时长
    """
    fields: Dict[str, Any] = {}

    # 文本类字段（只用中文标题/正文）
    title_zh = (row["title_zh"] or "").strip() if row["title_zh"] is not None else ""
    body_zh = (row["body_zh"] or "").strip() if row["body_zh"] is not None else ""

    if title_zh:
        fields["标题"] = title_zh
    if row["category"] is not None:
        fields["类目"] = str(row["category"])
    if row["product"] is not None:
        fields["产品"] = str(row["product"])
    if row["advertiser_name"] is not None:
        fields["广告主"] = str(row["advertiser_name"])
    if body_zh:
        fields["正文（中文）"] = body_zh
    if row["platform"] is not None:
        fields["平台"] = str(row["platform"])
    if row["video_url"] is not None:
        fields["视频链接"] = str(row["video_url"])
    if row["llm_analysis"] is not None:
        fields["AI分析结果"] = str(row["llm_analysis"])

    # 封面图链接
    preview_img_url = row["preview_img_url"]
    if preview_img_url is not None:
        fields["封面图链接"] = str(preview_img_url)

    # 当前 basic 表暂不上传封面图附件，仅保留链接，避免 AttachPermNotAllow

    # 日期字段（创建时间/更新时间）
    if row["created_at"]:
        ts = parse_datetime_to_ms(str(row["created_at"]))
        fields["创建时间"] = ts if ts is not None else str(row["created_at"])
    if row["updated_at"]:
        ts = parse_datetime_to_ms(str(row["updated_at"]))
        fields["更新时间"] = ts if ts is not None else str(row["updated_at"])

    # 数字字段：视频时长
    if row["video_duration"] is not None:
        fields["视频时长"] = int(row["video_duration"])

    return fields


def _get_existing_field_names(access_token: str) -> set[str]:
    """
    列出当前 basic 多维表中已有的字段名。
    """
    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{BASIC_APP_TOKEN}/tables/{BASIC_TABLE_ID}/fields"
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
    在 basic 多维表中创建一个简单字段。
    """
    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{BASIC_APP_TOKEN}/tables/{BASIC_TABLE_ID}/fields"
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
            f"[sync_basic_by_date] 创建字段失败（{field_name}），"
            f"status={resp.status_code}, resp={data}"
        )
    else:
        print(f"[sync_basic_by_date] 已创建字段：{field_name}")


_BASIC_FIELD_DEFS: List[Dict[str, Any]] = [
    {"field_name": "标题", "type": 1},
    {"field_name": "类目", "type": 1},
    {"field_name": "产品", "type": 1},
    {"field_name": "广告主", "type": 1},
    {"field_name": "正文（中文）", "type": 1},
    {"field_name": "平台", "type": 1},
    {"field_name": "视频链接", "type": 1},
    {"field_name": "封面图链接", "type": 1},
    {"field_name": "AI分析结果", "type": 1},
    {"field_name": "创建时间", "type": 5},
    {"field_name": "更新时间", "type": 5},
    {"field_name": "视频时长", "type": 2},
]


def ensure_basic_fields(access_token: str) -> None:
    """
    确保 basic 多维表中存在我们需要的字段，不存在的会自动创建。
    """
    existing = _get_existing_field_names(access_token)
    for f in _BASIC_FIELD_DEFS:
        name = f["field_name"]
        if name in existing:
            continue
        _create_field(access_token, field_name=name, field_type=int(f["type"]))


def batch_create_basic_records(
    access_token: str,
    records: List[Dict[str, Any]],
    client_token: str | None = None,
) -> None:
    """
    调用 basic 表的「新增多条记录」接口。
    """
    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{BASIC_APP_TOKEN}/tables/{BASIC_TABLE_ID}/records/batch_create"
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
    body = {"records": records}
    resp = requests.post(url, headers=headers, params=params, json=body, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"batch_create (basic by date) failed: {data}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="按指定日期，将 ad_creative_analysis 当天新建的创意同步到基础创意多维表。"
    )
    p.add_argument(
        "--date",
        required=True,
        help="仅同步 date(created_at) = 该日期 的记录，格式 YYYY-MM-DD，例如 2026-03-17",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    date_str = args.date

    token = get_tenant_access_token()
    # 确保 basic 表中有我们需要的字段
    ensure_basic_fields(token)

    conn = sqlite3.connect(DB_PATH)
    total_sent = 0
    try:
        for batch_idx, rows in enumerate(
            fetch_ad_creatives_by_date(conn, date_str, BATCH_SIZE),
            start=1,
        ):
            records: List[Dict[str, Any]] = []
            for row in rows:
                fields = row_to_basic_fields(row, token)
                if not fields:
                    continue
                records.append({"fields": fields})

            if not records:
                continue

            client_token = str(uuid.uuid4())
            batch_create_basic_records(token, records, client_token=client_token)

            total_sent += len(records)
            print(
                f"[sync_ad_creative_basic_by_date] 日期 {date_str} 已同步批次 {batch_idx}，"
                f"本批 {len(records)} 条，总计 {total_sent} 条"
            )
            time.sleep(0.2)
    finally:
        conn.close()

    print(
        f"[sync_ad_creative_basic_by_date] 同步完成，"
        f"日期 {date_str} 共写入记录数：{total_sent}"
    )


if __name__ == "__main__":
    main()

