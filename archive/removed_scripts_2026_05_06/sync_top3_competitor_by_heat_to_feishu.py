"""
将 `guangdada_yesterday_creatives.db` 中：
  - target_date = 指定日期集合（默认：2026-03-17/2026-03-18）
  - 对每个产品(product) 按热度 heat 降序取 Top3
同步到指定的飞书「新多维表格」。

字段命名尽量沿用你现有的竞品素材多维表（与 sync_competitor_new_creatives_to_bitable.py 一致）。

用法（项目根目录）：
  python scripts/sync_top3_competitor_by_heat_to_feishu.py --date 2026-03-17 --date 2026-03-18

默认会不清空表，若你不确定表是否已存在数据，可以加 --clear
（清空将不可撤销）。
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, List, Tuple

import requests
import lark_oapi as lark
from dotenv import load_dotenv
from lark_oapi.api.drive.v1.model import UploadAllMediaRequest, UploadAllMediaRequestBody, UploadAllMediaResponse

from path_util import DATA_DIR

load_dotenv()

FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")

# 你提供的飞书表 URL：
# https://scnmrtumk0zm.feishu.cn/base/HDv2bOb5JaAZlvshTCzcSOgRnQh?table=tbl59Gr3EJEvW01n&view=vewV0hrOgE
# 在 bitable API 中：
# - base 的那段通常对应 app_token
# - table 参数对应 table_id
TARGET_BITABLE_APP_TOKEN = "HDv2bOb5JaAZlvshTCzcSOgRnQh"
TARGET_BITABLE_TABLE_ID = "tbl59Gr3EJEvW01n"

BATCH_SIZE = 200

DB_PATH = DATA_DIR / "guangdada_yesterday_creatives.db"

# 表中用到的字段集合（仅用于 ensure / 写入时尽量对齐）
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
    # 兼容：目标表如果也有这些日期字段，需要同样写入时间戳
    {"field_name": "创建时间", "type": 5},
    {"field_name": "更新时间", "type": 5},
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="同步每个产品 heat Top3 素材到新飞书多维表格")
    p.add_argument(
        "--date",
        action="append",
        dest="dates",
        help="target_date（可多次传入，如 --date 2026-03-17 --date 2026-03-18）",
    )
    p.add_argument(
        "--clear",
        action="store_true",
        help="清空目标多维表全部记录（不可撤销）。默认不清空，仅追加写入。",
    )
    return p.parse_args()


def get_tenant_access_token() -> str:
    if not FEISHU_APP_ID or not FEISHU_APP_SECRET:
        raise RuntimeError("请在 .env 中配置 FEISHU_APP_ID 和 FEISHU_APP_SECRET")
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(
        url,
        json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"get tenant_access_token failed: {data}")
    return data["tenant_access_token"]


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
    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{TARGET_BITABLE_APP_TOKEN}/tables/{TARGET_BITABLE_TABLE_ID}/fields"
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


def _create_field(access_token: str, field_name: str, field_type: int) -> None:
    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{TARGET_BITABLE_APP_TOKEN}/tables/{TARGET_BITABLE_TABLE_ID}/fields"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    body: Dict[str, Any] = {"field_name": field_name, "type": int(field_type)}
    resp = requests.post(url, headers=headers, json=body, timeout=10)
    try:
        data = resp.json()
    except Exception:
        data = {"raw_text": resp.text}
    if resp.status_code != 200 or data.get("code") != 0:
        print(f"[sync_top3] 创建字段失败 field={field_name} status={resp.status_code} resp={data}")
    else:
        print(f"[sync_top3] 已创建字段：{field_name}")


def ensure_fields(access_token: str) -> None:
    existing = _get_existing_field_names(access_token)
    for f in NEW_FIELD_DEFS:
        name = f["field_name"]
        if name in existing:
            continue
        _create_field(access_token, field_name=name, field_type=int(f["type"]))


def _guess_filename_from_url(image_url: str) -> str:
    from urllib.parse import urlparse
    import os

    parsed = urlparse(image_url)
    name = os.path.basename(parsed.path) or "image"
    return name


_IMAGE_TOKEN_CACHE: Dict[str, str] = {}


def upload_image_as_attachment(image_url: str) -> str | None:
    """
    把网络图片上传为附件，返回 file_token。
    """
    if not image_url:
        return None
    if image_url in _IMAGE_TOKEN_CACHE:
        return _IMAGE_TOKEN_CACHE[image_url]

    try:
        resp = requests.get(image_url, timeout=15)
        resp.raise_for_status()
        content = resp.content
    except Exception as e:
        print(f"[sync_top3] 下载封面图失败: {image_url}, err={e}")
        return None

    filename = _guess_filename_from_url(image_url)
    client = _get_lark_client()
    try:
        body = (
            UploadAllMediaRequestBody.builder()
            .file_name(filename)
            .parent_type("bitable")
            .parent_node(TARGET_BITABLE_APP_TOKEN)
            .size(len(content))
            .checksum("")
            .extra("")
            .file(BytesIO(content))
            .build()
        )
        req = UploadAllMediaRequest.builder().request_body(body).build()
        resp2: UploadAllMediaResponse = client.drive.v1.media.upload_all(req)
        if resp2.success() and resp2.data and getattr(resp2.data, "file_token", None):
            ft = resp2.data.file_token
            _IMAGE_TOKEN_CACHE[image_url] = ft
            return ft
    except Exception as e:
        print(f"[sync_top3] 上传封面图异常: {image_url}, err={e}")
    return None


def batch_create_records(access_token: str, records: List[Dict[str, Any]], client_token: str) -> None:
    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{TARGET_BITABLE_APP_TOKEN}/tables/{TARGET_BITABLE_TABLE_ID}/records/batch_create"
    )
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json; charset=utf-8"}
    params = {"user_id_type": "open_id", "client_token": client_token}
    body = {"records": records}
    resp = requests.post(url, headers=headers, params=params, json=body, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"batch_create failed: {data}")


def clear_all_records(access_token: str) -> None:
    """
    清空目标表全部记录。
    ⚠️不可撤销，请谨慎使用。
    """
    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{TARGET_BITABLE_APP_TOKEN}/tables/{TARGET_BITABLE_TABLE_ID}/records"
    )
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json; charset=utf-8"}

    record_ids: List[str] = []
    page_token: str | None = None
    while True:
        params: Dict[str, Any] = {}
        if page_token:
            params["page_token"] = page_token
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"list records failed: {data}")
        data_obj = data.get("data") or {}
        items = data_obj.get("items") or data_obj.get("records") or []
        for item in items:
            rid = item.get("record_id")
            if rid:
                record_ids.append(rid)
        if not data_obj.get("has_more"):
            break
        page_token = data_obj.get("page_token")

    if not record_ids:
        print("[sync_top3] 当前表无记录，无需清空。")
        return

    print(f"[sync_top3] 清空记录: {len(record_ids)} 条（不可撤销）")
    for i, rid in enumerate(record_ids, start=1):
        del_url = (
            "https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{TARGET_BITABLE_APP_TOKEN}/tables/{TARGET_BITABLE_TABLE_ID}/records/{rid}"
        )
        dresp = requests.delete(del_url, headers=headers, timeout=10)
        try:
            ddata = dresp.json()
        except Exception:
            ddata = {"raw_text": dresp.text}
        if dresp.status_code != 200 or ddata.get("code") != 0:
            print(f"[sync_top3] 删除失败 rid={rid} resp={ddata}")
        if i % 50 == 0:
            time.sleep(0.2)
    print("[sync_top3] 清空完成。")


def fetch_top3_records(target_dates: List[str]) -> List[Dict[str, Any]]:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"数据库不存在: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        placeholders = ",".join("?" for _ in target_dates)
        cur.execute(
            f"""
            SELECT
                target_date,
                category,
                product,
                appid,
                keyword,
                ad_key,
                advertiser_name,
                title,
                body,
                platform,
                video_url,
                preview_img_url,
                days_count,
                heat,
                all_exposure_value,
                created_at
            FROM guangdada_competitor_yesterday_creatives
            WHERE target_date IN ({placeholders})
            """,
            tuple(target_dates),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    # 分组：product + target_date
    grouped: Dict[Tuple[str, str], List[sqlite3.Row]] = {}
    for r in rows:
        key = (str(r["product"]), str(r["target_date"]))
        grouped.setdefault(key, []).append(r)

    selected: List[Dict[str, Any]] = []
    for (product, tdate), lst in grouped.items():
        lst_sorted = sorted(
            lst,
            key=lambda x: (int(x["heat"] or 0), int(x["all_exposure_value"] or 0)),
            reverse=True,
        )
        for rank, r in enumerate(lst_sorted[:3], start=1):
            selected.append(
                {
                    "target_date": str(r["target_date"]),
                    "category": r["category"],
                    "product": r["product"],
                    "appid": r["appid"],
                    "keyword": r["keyword"],
                    "ad_key": r["ad_key"],
                    "advertiser_name": r["advertiser_name"],
                    "title": r["title"],
                    "body": r["body"],
                    "platform": r["platform"],
                    "video_url": r["video_url"],
                    "preview_img_url": r["preview_img_url"],
                    "days_count": r["days_count"],
                    "heat": r["heat"],
                    "all_exposure_value": r["all_exposure_value"],
                    "rank": rank,
                        "created_at": r["created_at"],
                }
            )
    return selected


def row_to_feishu_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    preview = row.get("preview_img_url") or ""

    # AppID 字段在旧脚本里写入的是 json.dumps(item.get("appid") or [], ...)
    appid_field = row.get("appid")
    appid_json = json.dumps([appid_field] if appid_field else [], ensure_ascii=False)

    def parse_date_to_ms(date_str: str | None) -> int | None:
        """
        将 YYYY-MM-DD 转为 unix 毫秒级时间戳（Date/Datetime 字段通常需要 ms）。
        """
        if not date_str:
            return None
        try:
            d = datetime.strptime(str(date_str).strip(), "%Y-%m-%d")
            d = d.replace(tzinfo=timezone.utc)
            return int(d.timestamp() * 1000)
        except Exception:
            return None

    def parse_datetime_to_ms(dt_str: str | None) -> int | None:
        """
        将 'YYYY-MM-DD HH:MM:SS' / ISO / 'YYYY-MM-DD' 转为 unix 毫秒级时间戳。
        """
        if not dt_str:
            return None
        s = str(dt_str).strip()
        if not s:
            return None
        fmts = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
        ]
        for fmt in fmts:
            try:
                dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
                return int(dt.timestamp() * 1000)
            except Exception:
                continue
        return None

    fields: Dict[str, Any] = {
        "类目": row.get("category") or "",
        "产品": row.get("product") or "",
        "AppID": appid_json,
        "是否我方产品": "否",
        "UA关键词": row.get("keyword") or "",
        "广告ID": row.get("ad_key") or "",
        "广告主": row.get("advertiser_name") or "",
        "标题": row.get("title") or "",
        "正文": row.get("body") or "",
        "平台": row.get("platform") or "",
        "视频链接": row.get("video_url") or "",
        "封面图链接": preview,
        "投放天数": int(row.get("days_count") or 0),
        "展示估值": int(row.get("all_exposure_value") or 0),
        "热度": int(row.get("heat") or 0),
    }

    # 日期字段：不写死 0，避免显示 1970/01/01
    ts_days = parse_date_to_ms(row.get("target_date"))
    if ts_days is not None:
        fields["抓取日期"] = ts_days

    created_ts = parse_datetime_to_ms(row.get("created_at"))
    if created_ts is not None:
        fields["创建时间"] = created_ts
        fields["更新时间"] = created_ts

    if preview:
        file_token = upload_image_as_attachment(preview)
        if file_token:
            fields["封面图"] = [{"file_token": file_token}]
    return fields


def main() -> None:
    args = parse_args()
    target_dates = args.dates or ["2026-03-17", "2026-03-18"]

    access_token = get_tenant_access_token()
    ensure_fields(access_token)

    if args.clear:
        clear_all_records(access_token)

    top3_rows = fetch_top3_records(target_dates)
    print(f"[sync_top3] 本次筛出记录数: {len(top3_rows)}（每个产品每一天 Top3）")
    if not top3_rows:
        print("[sync_top3] 没有命中数据，退出。")
        return

    total_sent = 0
    for i in range(0, len(top3_rows), BATCH_SIZE):
        batch = top3_rows[i : i + BATCH_SIZE]
        records: List[Dict[str, Any]] = []
        for row in batch:
            fields = row_to_feishu_fields(row)
            records.append({"fields": fields})
        client_token = str(uuid.uuid4())
        batch_create_records(access_token, records, client_token=client_token)
        total_sent += len(records)
        print(f"[sync_top3] 已同步 {total_sent}/{len(top3_rows)}")
        time.sleep(0.2)

    print(f"[sync_top3] 完成，总计同步 {total_sent} 条记录。")


if __name__ == "__main__":
    main()

