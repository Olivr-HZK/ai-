import os
import sqlite3
import time
import uuid
from datetime import datetime
from io import BytesIO
from typing import Dict, Any, List
from urllib.parse import urlparse

import mimetypes
import requests
from dotenv import load_dotenv

import lark_oapi as lark
from lark_oapi.api.drive.v1.model import (
    UploadAllMediaRequest,
    UploadAllMediaRequestBody,
    UploadAllMediaResponse,
)


load_dotenv()

FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "your_app_id_here")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "your_app_secret_here")

BITABLE_APP_TOKEN = os.getenv("BITABLE_APP_TOKEN", "appXXXXXXXXXXXXXXX")
BITABLE_TABLE_ID = os.getenv("BITABLE_TABLE_ID", "tblXXXXXXXXXXXXXXX")

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "ai_products_ua.db")
BATCH_SIZE = 500

# 当前多维表格已有的字段（与「列出字段」一致，只写入这些列）
BITABLE_FIELD_NAMES = {
    "标题",
    "类目",
    "产品",
    "广告主",
    "正文（中文）",
    "平台",
    "视频链接",
    "封面图链接",
    "封面图",  # 附件字段，用于实际显示图片
    "AI分析结果",
    "抓取日期",
    "创建时间",
    "更新时间",
    "视频时长",
    "接受情况",
}

# 数据库列名 -> 多维表格字段名（仅包含当前表中存在的字段）
FIELD_MAP: Dict[str, str] = {
    "crawl_date": "抓取日期",
    "category": "类目",
    "product": "产品",
    "advertiser_name": "广告主",
    "title": "标题",
    "body_zh": "正文（中文）",
    "platform": "平台",
    "video_url": "视频链接",
    "video_duration": "视频时长",
    "preview_img_url": "封面图链接",
    "llm_analysis": "AI分析结果",
    "created_at": "创建时间",
    "updated_at": "更新时间",
}

ACCEPT_FIELD_NAME = "接受情况"
ACCEPT_DEFAULT_VALUE = "待定"

# 仅用于 ensure_fields：与当前表已有字段一致，避免创建多余列
FIELD_DEFS: List[Dict[str, Any]] = [
    {"field_name": "标题", "type": 1},
    {"field_name": "类目", "type": 1},
    {"field_name": "产品", "type": 1},
    {"field_name": "广告主", "type": 1},
    {"field_name": "正文（中文）", "type": 1},
    {"field_name": "平台", "type": 1},
    {"field_name": "视频链接", "type": 1},
    {"field_name": "封面图链接", "type": 1},
    {"field_name": "封面图", "type": 17},  # 附件字段
    {"field_name": "AI分析结果", "type": 1},
    {"field_name": "抓取日期", "type": 5},
    {"field_name": "创建时间", "type": 5},
    {"field_name": "更新时间", "type": 5},
    {"field_name": "视频时长", "type": 2},
    {
        "field_name": ACCEPT_FIELD_NAME,
        "type": 3,
        "options": [
            {"name": "待定"},
            {"name": "删除"},
            {"name": "接受"},
        ],
    },
]


def get_tenant_access_token() -> str:
    """
    按飞书开放平台文档获取 tenant_access_token。
    """
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


def get_existing_field_names(access_token: str) -> set[str]:
    """
    列出当前多维表格数据表中已有的字段名，用于避免重复创建。
    """
    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{BITABLE_APP_TOKEN}/tables/{BITABLE_TABLE_ID}/fields"
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
        # 有的接口字段名为 items，有的为 fields，这里都兼容一下
        items = data_obj.get("items") or data_obj.get("fields") or []
        for item in items:
            name = item.get("field_name")
            if name:
                field_names.add(name)

        if not data_obj.get("has_more"):
            break
        page_token = data_obj.get("page_token")

    return field_names


def create_field(
    access_token: str,
    field_name: str,
    field_type: int,
    property_obj: Dict[str, Any] | None = None,
    options: List[Dict[str, Any]] | None = None,
) -> None:
    """
    调用「新增字段」接口，在指定数据表中创建一个字段。
    HTTP URL 形如：
    POST https://open.feishu.cn/open-apis/bitable/v1/apps/:app_token/tables/:table_id/fields
    """
    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{BITABLE_APP_TOKEN}/tables/{BITABLE_TABLE_ID}/fields"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=utf-8",
    }

    body: Dict[str, Any] = {
        "field_name": field_name,
        "type": field_type,
    }
    if property_obj:
        body["property"] = property_obj
    if options:
        body["options"] = options

    resp = requests.post(url, headers=headers, json=body, timeout=10)

    # 为了便于排查，打印详细返回
    try:
        data = resp.json()
    except Exception:
        data = {"raw_text": resp.text}

    if resp.status_code != 200 or data.get("code") != 0:
        print(
            f"创建字段失败（{field_name}），"
            f"status={resp.status_code}, resp={data}"
        )
    else:
        print(f"已创建字段：{field_name}")


def ensure_fields(access_token: str) -> None:
    """
    确保多维表格中存在我们需要的所有字段，不存在的会自动创建。
    """
    existing = get_existing_field_names(access_token)
    for f in FIELD_DEFS:
        name = f["field_name"]
        if name in existing:
            continue
        create_field(
            access_token,
            field_name=name,
            field_type=int(f["type"]),
            property_obj=f.get("property"),
            options=f.get("options"),
        )


# 预览图 URL -> file_token 的简单内存缓存，避免重复上传相同图片
IMAGE_TOKEN_CACHE: Dict[str, str] = {}

# 全局 lark 客户端，用于上传附件（和你给的示例保持一致）
LARK_CLIENT: lark.Client | None = None


def get_lark_client() -> lark.Client:
    global LARK_CLIENT
    if LARK_CLIENT is None:
        LARK_CLIENT = (
            lark.Client.builder()
            .app_id(FEISHU_APP_ID)
            .app_secret(FEISHU_APP_SECRET)
            .log_level(lark.LogLevel.ERROR)
            .build()
        )
    return LARK_CLIENT


def _guess_filename_from_url(image_url: str) -> str:
    parsed = urlparse(image_url)
    name = os.path.basename(parsed.path) or "image"
    if "." not in name:
        # 没有后缀时，尽量根据 content-type 再补一次，这里先简单返回
        return name
    return name


def upload_image_as_attachment(access_token: str, image_url: str) -> str | None:
    """
    调用飞书上传素材接口，将网络图片上传为附件，返回 file_token。
    文档对应：POST /drive/v1/media/upload_all
    """
    if not image_url:
        return None

    # 先查缓存
    if image_url in IMAGE_TOKEN_CACHE:
        return IMAGE_TOKEN_CACHE[image_url]

    try:
        resp = requests.get(image_url, timeout=15)
        resp.raise_for_status()
        content = resp.content
    except Exception as e:
        print(f"下载封面图失败：{image_url}，错误：{e}")
        return None

    filename = _guess_filename_from_url(image_url)

    # 使用 lark_oapi 的 drive.v1.media.upload_all，与参考脚本保持一致
    client = get_lark_client()
    try:
        body = (
            UploadAllMediaRequestBody.builder()
            .file_name(filename)
            .parent_type("bitable")
            .parent_node(BITABLE_APP_TOKEN)
            .size(len(content))
            .checksum("")
            .extra("")
            .file(BytesIO(content))
            .build()
        )
        req = UploadAllMediaRequest.builder().request_body(body).build()
        resp: UploadAllMediaResponse = client.drive.v1.media.upload_all(req)
        if resp.success() and resp.data and getattr(resp.data, "file_token", None):
            file_token = resp.data.file_token
            IMAGE_TOKEN_CACHE[image_url] = file_token
            return file_token
        # 打印详细错误信息，便于排查
        raw = {}
        if resp.raw and getattr(resp.raw, "content", None):
            try:
                raw = json.loads(resp.raw.content)
            except Exception:
                raw = {"raw_text": str(resp.raw.content)}
        print(
            f"上传封面图失败：{image_url}，"
            f"code={resp.code}, msg={resp.msg}, body={raw}"
        )
    except Exception as e:
        print(f"上传封面图异常：{image_url}，错误：{e}")
    return None


def batch_create_records(
    access_token: str,
    records: List[Dict[str, Any]],
    client_token: str | None = None,
) -> None:
    """
    调用「新增多条记录」接口：
    POST https://open.feishu.cn/open-apis/bitable/v1/apps/:app_token/tables/:table_id/records/batch_create
    """
    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{BITABLE_APP_TOKEN}/tables/{BITABLE_TABLE_ID}/records/batch_create"
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


def parse_datetime_to_ms(dt_str: str | None) -> int | None:
    """
    将字符串日期尽量转换为毫秒级时间戳，供多维表格的「日期」字段使用。
    """
    if not dt_str:
        return None

    dt_str = dt_str.strip()
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(dt_str, fmt)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    return None


def row_to_fields(row: sqlite3.Row, access_token: str) -> Dict[str, Any]:
    """
    将一行 SQLite 记录转换成多维表格记录的 fields。
    只写入当前多维表格中存在的字段（见 BITABLE_FIELD_NAMES）。
    """
    fields: Dict[str, Any] = {}

    # 文本类字段（仅写入表中存在的列）
    # 标题字段写入「中文标题」title_zh
    text_map = {
        "title_zh": "标题",
        "category": "类目",
        "product": "产品",
        "advertiser_name": "广告主",
        "body_zh": "正文（中文）",
        "platform": "平台",
        "video_url": "视频链接",
        "llm_analysis": "AI分析结果",
    }
    for col, fname in text_map.items():
        value = row[col]
        if value is not None and fname in BITABLE_FIELD_NAMES:
            fields[fname] = str(value)

    # 封面图链接：始终写入以便在表格中显示（无则写空字符串）
    if "封面图链接" in BITABLE_FIELD_NAMES:
        v = row["preview_img_url"]
        fields["封面图链接"] = str(v) if v is not None else ""

    # 封面图附件：有链接时下载并上传为附件，方便在多维表格中直接预览图片
    if "封面图" in BITABLE_FIELD_NAMES:
        img_url = row["preview_img_url"]
        if img_url:
            file_token = upload_image_as_attachment(access_token, img_url)
            if file_token:
                fields["封面图"] = [{"file_token": file_token}]

    # 日期字段 -> 毫秒时间戳
    for col in ["crawl_date", "created_at", "updated_at"]:
        fname = FIELD_MAP[col]
        if fname not in BITABLE_FIELD_NAMES:
            continue
        value = row[col]
        if value is None:
            continue
        ts_ms = parse_datetime_to_ms(value)
        if ts_ms is not None:
            fields[fname] = ts_ms
        else:
            fields[fname] = str(value)

    # 数字字段：视频时长
    if "视频时长" in BITABLE_FIELD_NAMES and row["video_duration"] is not None:
        fields["视频时长"] = int(row["video_duration"])

    # 接受情况（默认“待定”）
    if ACCEPT_FIELD_NAME in BITABLE_FIELD_NAMES:
        fields[ACCEPT_FIELD_NAME] = ACCEPT_DEFAULT_VALUE

    return fields


def fetch_rows_in_batches(conn: sqlite3.Connection, batch_size: int):
    """
    以批次方式从 ad_creative_analysis 读取数据。
    """
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM ad_creative_analysis ORDER BY crawl_date, ad_key")
    while True:
        batch = cur.fetchmany(batch_size)
        if not batch:
            break
        yield batch


def main() -> None:
    token = get_tenant_access_token()
    # 先确保多维表格中有我们需要的字段
    ensure_fields(token)
    conn = sqlite3.connect(DB_PATH)

    total_sent = 0
    try:
        for batch_idx, rows in enumerate(
            fetch_rows_in_batches(conn, BATCH_SIZE),
            start=1,
        ):
            records: List[Dict[str, Any]] = []
            for row in rows:
                fields = row_to_fields(row, token)
                if not fields:
                    continue
                records.append({"fields": fields})

            if not records:
                continue

            client_token = str(uuid.uuid4())
            batch_create_records(token, records, client_token=client_token)

            total_sent += len(records)
            print(f"已同步批次 {batch_idx}，本批 {len(records)} 条，总计 {total_sent} 条")
            time.sleep(0.2)
    finally:
        conn.close()

    print("同步完成，总计写入记录数：", total_sent)


if __name__ == "__main__":
    main()

