"""
每天将数据库中「创建时间为今天」的一批 ad_creative_analysis 同步到飞书多维表格。

仅同步 created_at 为指定日期（默认今天）的数据；若该日没有新写入的记录，则直接跳过不同步。
参考 sync_ad_creative_to_bitable.py，只同步当日新数据，避免重复。
"""
import argparse
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


def has_created_data_for_date(conn: sqlite3.Connection, date_str: str) -> bool:
    """
    检查 ad_creative_product_suggestion 表中是否存在 created_at 为指定日期的数据。
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM ad_creative_product_suggestion WHERE date(created_at) = date(?, 'localtime')",
        (date_str,),
    )
    count = cur.fetchone()[0] or 0
    return count > 0


def _where_clause_for_suggestion_date(
    date_str: str | None,
    start_date: str | None,
    end_date: str | None,
    all_rows: bool,
) -> tuple[str, tuple]:
    """
    返回 (where_sql, params) 只作用于 s.created_at 的日期范围过滤。
    - all_rows=True：不过滤
    - 仅 date_str：过滤到某一天
    - start/end：过滤到闭区间
    """
    if all_rows:
        return "1=1", tuple()
    if date_str:
        return "date(s.created_at) = date(?, 'localtime')", (date_str,)
    if start_date and end_date:
        return (
            "date(s.created_at) >= date(?, 'localtime') AND date(s.created_at) <= date(?, 'localtime')",
            (start_date, end_date),
        )
    if start_date:
        return "date(s.created_at) >= date(?, 'localtime')", (start_date,)
    if end_date:
        return "date(s.created_at) <= date(?, 'localtime')", (end_date,)
    # 默认：今天
    return "date(s.created_at) = date('now', 'localtime')", tuple()


def has_suggestion_data(conn: sqlite3.Connection, where_sql: str, params: tuple) -> bool:
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) FROM ad_creative_product_suggestion AS s WHERE {where_sql}",
        params,
    )
    count = cur.fetchone()[0] or 0
    return count > 0


def fetch_rows(conn: sqlite3.Connection, where_sql: str, params: tuple, batch_size: int):
    """
    以批次方式读取「产品级 UA 建议」对应的记录：
    一条素材 × 一条我方产品 × 一条 UA 建议。
    """
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
          c.*,
          s.our_product,
          s.ua_suggestion,
          s.created_at AS sugg_created_at
        FROM ad_creative_product_suggestion AS s
        JOIN ad_creative_analysis AS c
          ON c.ad_key = s.ad_key
        WHERE {where_sql}
        ORDER BY c.crawl_date, c.ad_key, s.id
        """
        ,
        params,
    )
    while True:
        batch = cur.fetchmany(batch_size)
        if not batch:
            break
        yield batch


def main() -> None:
    p = argparse.ArgumentParser(description="同步指定日期的灵感表数据到老多维表格（默认今天）。")
    p.add_argument("--date", default=None, help="日期 YYYY-MM-DD（默认今天）")
    p.add_argument("--start-date", default=None, help="起始日期 YYYY-MM-DD（含）")
    p.add_argument("--end-date", default=None, help="结束日期 YYYY-MM-DD（含）")
    p.add_argument("--all", action="store_true", help="全量同步：忽略日期过滤")
    args = p.parse_args()

    date_str = (args.date or "").strip() or None
    start_date = (args.start_date or "").strip() or None
    end_date = (args.end_date or "").strip() or None
    all_rows = bool(args.all)

    where_sql, params = _where_clause_for_suggestion_date(
        date_str=date_str,
        start_date=start_date,
        end_date=end_date,
        all_rows=all_rows,
    )

    token = get_tenant_access_token()
    ensure_fields(token)

    conn = sqlite3.connect(DB_PATH)
    try:
        if not has_suggestion_data(conn, where_sql, params):
            print(
                "[daily_sync] 在 ad_creative_product_suggestion 中没有符合条件的数据，本次不同步。"
            )
            return

        if all_rows:
            print("[daily_sync] 本次将全量同步（ad_creative_product_suggestion 全表）到多维表格。")
        else:
            print("[daily_sync] 本次将按条件同步到多维表格。")

        total_sent = 0
        for batch_idx, rows in enumerate(
            fetch_rows(conn, where_sql, params, BATCH_SIZE),
            start=1,
        ):
            records: List[Dict[str, Any]] = []
            for row in rows:
                # sqlite3.Row 不支持 .get，用下标访问
                our_product = row["our_product"] if "our_product" in row.keys() else None
                ua_sugg = row["ua_suggestion"] if "ua_suggestion" in row.keys() else None
                fields = row_to_fields(
                    row,
                    token,
                    our_product=str(our_product) if our_product is not None else None,
                    ua_suggestion_override=str(ua_sugg) if ua_sugg is not None else None,
                )
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

        print(f"[daily_sync] 完成，本次共写入记录数：{total_sent}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

