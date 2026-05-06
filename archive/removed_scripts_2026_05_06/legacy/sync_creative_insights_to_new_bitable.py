"""
将「创意生成结果」同步到指定飞书多维表（自动建字段）。

数据来源：
- ai_products_ua.db
- ad_creative_analysis + ad_creative_product_suggestion（即“之前创意生成”结果）

特性：
1) 支持直接传飞书表链接（自动解析 app_token/table_id）
2) 自动复用 sync_ad_creative_to_bitable.py 的字段创建和写入逻辑
3) 默认按 ad_creative_product_suggestion.created_at=今天过滤，也可按日期/区间/全量

用法：
  python scripts/sync_creative_insights_to_new_bitable.py --url "https://scnmrtumk0zm.feishu.cn/base/W8QMbUR1vaiUGUskOF2cwnXenBe?table=tblRAiOqhIyJEAS9&view=vewd67ZK4J" --date 2026-03-18
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import time
import uuid
from typing import Any, Dict, List, Tuple
from urllib.parse import parse_qs, urlparse


def _parse_bitable_url(url: str) -> tuple[str, str]:
    parsed = urlparse(url.strip())
    parts = [p for p in parsed.path.split("/") if p]
    # 形如 /base/<app_token>
    app_token = ""
    if len(parts) >= 2 and parts[0] == "base":
        app_token = parts[1]
    qs = parse_qs(parsed.query or "")
    table_id = (qs.get("table") or [""])[0]
    if not app_token or not table_id:
        raise ValueError(f"无法从链接解析 app_token/table_id：{url}")
    return app_token, table_id


def _where_clause_for_suggestion_date(
    date_str: str | None,
    start_date: str | None,
    end_date: str | None,
    all_rows: bool,
) -> tuple[str, tuple]:
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
        """,
        params,
    )
    while True:
        batch = cur.fetchmany(batch_size)
        if not batch:
            break
        yield batch


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="同步创意生成结果到指定飞书多维表（自动建字段）")
    p.add_argument("--url", required=True, help="飞书多维表完整链接（含 table 参数）")
    p.add_argument("--date", default=None, help="日期 YYYY-MM-DD（默认今天）")
    p.add_argument("--start-date", default=None, help="起始日期 YYYY-MM-DD（含）")
    p.add_argument("--end-date", default=None, help="结束日期 YYYY-MM-DD（含）")
    p.add_argument("--all", action="store_true", help="全量同步：忽略日期过滤")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    app_token, table_id = _parse_bitable_url(args.url)

    # 关键：在 import 旧同步模块前，覆盖目标表环境变量
    os.environ["BITABLE_APP_TOKEN"] = app_token
    os.environ["BITABLE_TABLE_ID"] = table_id

    from sync_ad_creative_to_bitable import (  # type: ignore
        DB_PATH,
        BATCH_SIZE,
        get_tenant_access_token,
        ensure_fields,
        row_to_fields,
        batch_create_records,
    )

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

    print(f"[sync_new] 目标表 app_token={app_token}, table_id={table_id}")
    token = get_tenant_access_token()
    ensure_fields(token)

    conn = sqlite3.connect(DB_PATH)
    try:
        if not has_suggestion_data(conn, where_sql, params):
            print("[sync_new] ad_creative_product_suggestion 中没有符合条件的数据，本次不同步。")
            return

        total_sent = 0
        for batch_idx, rows in enumerate(fetch_rows(conn, where_sql, params, BATCH_SIZE), start=1):
            records: List[Dict[str, Any]] = []
            for row in rows:
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
            print(f"[sync_new] 已同步批次 {batch_idx}，本批 {len(records)} 条，总计 {total_sent} 条")
            time.sleep(0.2)

        print(f"[sync_new] 完成，共写入 {total_sent} 条。")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

