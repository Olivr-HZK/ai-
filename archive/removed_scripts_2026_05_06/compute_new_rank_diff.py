"""
新晋榜 · 计算「今天相对昨天的新增素材」。

- 数据来源：data/competitor_hot_rank.db 中的 competitor_new_raw_daily（原始快照表）
- 算法：
  - 对昨天和今天的 raw_json（run_batch result）展开 all_creatives
  - 以 (category, product, ad_key) 为键：
      - 昨天没有、今天有 → 视为「新增」
      - 同一 ad_key 出现多次时，保留 heat 最大的一条
- 结果写入：competitor_new_creatives_daily（结构与热门榜/最新创意去重表一致）

用法示例（在项目根目录）：

  # 对今天相对昨天计算新增（默认 date=今天, prev-date=昨天）
  python scripts/compute_new_rank_diff.py

  # 手动指定日期
  python scripts/compute_new_rank_diff.py --date 2026-03-14 --prev-date 2026-03-13
"""

from __future__ import annotations

import argparse
import datetime as dt

from competitor_hot_db import compute_new_rank_new_creatives, get_conn


def _parse_crawl_date(date_str: str | None) -> str:
    """解析目标日期（crawl_date），默认今天。"""
    if date_str:
        crawl_date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        crawl_date = dt.date.today()
    return crawl_date.strftime("%Y-%m-%d")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="新晋榜 · 计算今天相对昨天的新增素材，并写入 competitor_new_creatives_daily。",
    )
    p.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        default=None,
        help="目标日期（默认今天）",
    )
    p.add_argument(
        "--prev-date",
        metavar="YYYY-MM-DD",
        default=None,
        help="对比日期（默认等于 date 的前一天）",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    crawl_date = _parse_crawl_date(args.date)

    # 确定基线日期（prev_date）：
    # - 若显式传入 --prev-date，则直接使用
    # - 否则从 competitor_new_raw_daily 中查找「小于 crawl_date 的最近一日」
    if args.prev_date:
        prev_date = args.prev_date
    else:
        conn = get_conn()
        try:
            cur = conn.execute(
                """
                SELECT MAX(crawl_date) AS prev_date
                FROM competitor_new_raw_daily
                WHERE crawl_date < ?
                """,
                (crawl_date,),
            )
            row = cur.fetchone()
            prev_date = row["prev_date"] if row and row["prev_date"] else None
        finally:
            conn.close()

    if not prev_date:
        print(
            f"新晋榜差异计算：目标日期 = {crawl_date}。"
        )
        print(
            "未在 competitor_new_raw_daily 中找到比目标日期更早的 crawl_date，"
            "无法自动确定基线日期（prev_date）。请先确保已有历史 raw 数据，"
            "或使用 --prev-date 手动指定基线。"
        )
        return

    print(
        f"新晋榜差异计算：目标日期 = {crawl_date}，对比日期 = {prev_date}。"
    )
    print(
        "数据来源：competitor_new_raw_daily（两天的原始快照），"
        "结果写入：competitor_new_creatives_daily（仅包含新增素材）。"
    )

    inserted = compute_new_rank_new_creatives(crawl_date, prev_date)
    print(
        f"完成：已将 {inserted} 条按 ad_key 去重后的新增素材写入 "
        f"competitor_new_creatives_daily（crawl_date={crawl_date}）。"
    )


if __name__ == "__main__":
    main()

