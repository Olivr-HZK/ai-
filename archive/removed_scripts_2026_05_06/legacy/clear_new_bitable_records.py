"""
清空「daily 原始 UA 素材」对应的新多维表格中的所有记录。

⚠️ 操作不可撤销，请确认仅用于新建的统计表。

用法（项目根目录）：
  python scripts/clear_new_bitable_records.py
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

load_dotenv()

FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")
NEW_COMP_BITABLE_APP_TOKEN = os.getenv("NEW_COMP_BITABLE_APP_TOKEN", "")
NEW_COMP_BITABLE_TABLE_ID = os.getenv("NEW_COMP_BITABLE_TABLE_ID", "")


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


def list_record_ids(access_token: str) -> List[str]:
    """
    列出当前表中所有记录的 record_id。
    """
    if not NEW_COMP_BITABLE_APP_TOKEN or not NEW_COMP_BITABLE_TABLE_ID:
        raise RuntimeError(
            "请在 .env 中配置 NEW_COMP_BITABLE_APP_TOKEN 和 NEW_COMP_BITABLE_TABLE_ID"
        )
    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{NEW_COMP_BITABLE_APP_TOKEN}/tables/{NEW_COMP_BITABLE_TABLE_ID}/records"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=utf-8",
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
    return record_ids


def batch_delete_records(access_token: str, record_ids: List[str]) -> None:
    """
    删除一批记录。
    这里为了简单和稳定性，逐条调用单记录删除接口：
      DELETE /bitable/v1/apps/:app_token/tables/:table_id/records/:record_id
    """
    if not record_ids:
        return
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    for rid in record_ids:
        url = (
            "https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{NEW_COMP_BITABLE_APP_TOKEN}/tables/{NEW_COMP_BITABLE_TABLE_ID}/records/{rid}"
        )
        resp = requests.delete(url, headers=headers, timeout=10)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text}
        if resp.status_code != 200 or data.get("code") != 0:
            print(
                f"[clear_new_bitable] 删除记录失败 record_id={rid}, "
                f"status={resp.status_code}, resp={data}"
            )


def main() -> None:
    print("[clear_new_bitable] 准备清空新多维表格中的所有记录...")
    token = get_tenant_access_token()
    record_ids = list_record_ids(token)
    if not record_ids:
        print("[clear_new_bitable] 当前表中没有记录，无需删除。")
        return
    print(f"[clear_new_bitable] 当前表共有 {len(record_ids)} 条记录，将分批删除。")

    BATCH = 500
    deleted = 0
    for i in range(0, len(record_ids), BATCH):
        batch = record_ids[i : i + BATCH]
        batch_delete_records(token, batch)
        deleted += len(batch)
        print(f"[clear_new_bitable] 已删除 {deleted}/{len(record_ids)} 条")
        time.sleep(0.2)

    print("[clear_new_bitable] 清空完成。")


if __name__ == "__main__":
    main()

