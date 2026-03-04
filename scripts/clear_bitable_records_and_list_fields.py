import os
import time
from typing import Any, Dict, List, Tuple

import requests
from dotenv import load_dotenv


load_dotenv()

FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "your_app_id_here")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "your_app_secret_here")

BITABLE_APP_TOKEN = os.getenv("BITABLE_APP_TOKEN", "appXXXXXXXXXXXXXXX")
BITABLE_TABLE_ID = os.getenv("BITABLE_TABLE_ID", "tblXXXXXXXXXXXXXXX")


def get_tenant_access_token() -> str:
    """
    获取 tenant_access_token，用于后续调用多维表格接口。
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
        raise RuntimeError(f"获取 tenant_access_token 失败: {data}")
    return data["tenant_access_token"]


def list_all_record_ids(access_token: str) -> List[str]:
    """
    使用「批量获取记录」接口，分页列出当前数据表中所有记录的 record_id。
    文档对应：GET /bitable/v1/apps/:app_token/tables/:table_id/records
    """
    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{BITABLE_APP_TOKEN}/tables/{BITABLE_TABLE_ID}/records"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
    }

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
            raise RuntimeError(f"批量获取记录失败: {data}")

        data_obj = data.get("data") or {}
        items = data_obj.get("items") or data_obj.get("records") or []
        for item in items:
            rid = item.get("record_id") or item.get("id")
            if rid:
                record_ids.append(rid)

        if not data_obj.get("has_more"):
            break
        page_token = data_obj.get("page_token")

    return record_ids


def delete_record(access_token: str, record_id: str) -> Tuple[bool, Dict[str, Any]]:
    """
    删除一条记录。
    文档对应：DELETE /bitable/v1/apps/:app_token/tables/:table_id/records/:record_id
    """
    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{BITABLE_APP_TOKEN}/tables/{BITABLE_TABLE_ID}/records/{record_id}"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
    }

    resp = requests.delete(url, headers=headers, timeout=10)

    try:
        data = resp.json()
    except Exception:
        data = {"raw_text": resp.text}

    ok = resp.status_code == 200 and data.get("code") == 0
    return ok, data


def clear_all_records(access_token: str) -> None:
    """
    列出当前表的所有记录并逐条删除。
    """
    record_ids = list_all_record_ids(access_token)
    print(f"当前表共有 {len(record_ids)} 条记录，将全部删除。")

    for idx, rid in enumerate(record_ids, start=1):
        ok, data = delete_record(access_token, rid)
        if not ok:
            print(f"删除记录失败 record_id={rid}，响应={data}")
        else:
            print(f"[{idx}/{len(record_ids)}] 已删除记录 record_id={rid}")
        # 简单限流，避免触发 QPS 限制
        time.sleep(0.05)

    print("记录清空完成。")


def list_fields(access_token: str) -> None:
    """
    使用「列出字段」接口查看当前数据表所有字段。
    文档对应：GET /bitable/v1/apps/:app_token/tables/:table_id/fields
    """
    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{BITABLE_APP_TOKEN}/tables/{BITABLE_TABLE_ID}/fields"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
    }

    page_token: str | None = None
    all_fields: List[Dict[str, Any]] = []

    while True:
        params: Dict[str, Any] = {}
        if page_token:
            params["page_token"] = page_token

        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"列出字段失败: {data}")

        data_obj = data.get("data") or {}
        items = data_obj.get("items") or data_obj.get("fields") or []
        all_fields.extend(items)

        if not data_obj.get("has_more"):
            break
        page_token = data_obj.get("page_token")

    print("\n当前数据表字段列表：")
    for f in all_fields:
        fname = f.get("field_name")
        ftype = f.get("type")
        fid = f.get("field_id")
        print(f"- 字段名: {fname} | 类型(type): {ftype} | field_id: {fid}")


def main() -> None:
    token = get_tenant_access_token()

    # 1. 清空当前数据表的所有记录
    clear_all_records(token)

    # 2. 列出当前表的字段
    list_fields(token)


if __name__ == "__main__":
    main()

