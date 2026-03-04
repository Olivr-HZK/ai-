"""
每天将数据库中「最新 crawl_date」的一批 ad_creative_analysis 同步到飞书多维表格。

参考 sync_ad_creative_to_bitable.py，只同步最新日期的数据，避免每次全量重跑。
"""
import sqlite3
import time
import uuid
from typing import Dict, Any, List

from sync_ad_creative_to_bitable import (  # type: ignore
    DB_PATH,
    BATCH_SIZE,
    get_tenant_access_token,
    ensure_fields,
    row_to_fields,
    batch_create_records,
)


def fetch_latest_date(conn: sqlite3.Connection) -> str | None:
    cur = conn.cursor()
    cur.execute("SELECT MAX(crawl_date) FROM ad_creative_analysis")
    row = cur.fetchone()
    if not row:
        return None
    return row[0]


def fetch_rows_for_date(conn: sqlite3.Connection, crawl_date: str, batch_size: int):
    """
    以批次方式读取指定 crawl_date 的数据。
    """
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM ad_creative_analysis WHERE crawl_date = ? ORDER BY ad_key",
        (crawl_date,),
    )
    while True:
        batch = cur.fetchmany(batch_size)
        if not batch:
            break
        yield batch


def main() -> None:
    token = get_tenant_access_token()
    ensure_fields(token)

    conn = sqlite3.connect(DB_PATH)
    try:
        latest_date = fetch_latest_date(conn)
        if not latest_date:
            print("ad_creative_analysis 中没有任何数据，可同步条数为 0。")
            return

        print(f"本次将同步 crawl_date = {latest_date} 的数据到多维表格。")

        total_sent = 0
        for batch_idx, rows in enumerate(
            fetch_rows_for_date(conn, latest_date, BATCH_SIZE),
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
            print(
                f"[daily_sync] 已同步批次 {batch_idx}，本批 {len(records)} 条，总计 {total_sent} 条"
            )
            time.sleep(0.2)

        print(f"[daily_sync] 完成，本次 crawl_date={latest_date} 共写入记录数：{total_sent}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

