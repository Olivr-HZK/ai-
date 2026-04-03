"""
依次获取「AI 图像生成」「AI 视频生成」「AI 聊天与助手」三类工具榜单，
不合并，每类最多保留 100 条，写入 data/最新素材库_<榜>.json 及对应表（热点榜/飙升榜/新创意榜）。

用法（在项目根目录）：
  # 热点榜（每周热门榜，默认）
  python scripts/fetch_latest_creative_library.py
  python scripts/fetch_latest_creative_library.py --chart=hot

  # 飙升榜（每周飙升榜）
  python scripts/fetch_latest_creative_library.py --chart=surge

  # 新创意榜
  python scripts/fetch_latest_creative_library.py --chart=new

  DEBUG=1 python scripts/fetch_latest_creative_library.py --chart=surge  # 有界面
"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

# 保证能导入同目录及项目根模块
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from path_util import DATA_DIR
from guangdada_login import login
from scrape_guangdada_hot_charts import (
    prepare_hot_charts_page,
    collect_hot_charts_category,
    CHART_TYPE_HOT,
    CHART_TYPE_SURGE,
    CHART_TYPE_NEW,
)

# 三类标签，按顺序抓取；每类最多保留条数（一个排行榜 100 条）
CATEGORIES = ["AI 图像生成", "AI 视频生成", "AI 聊天与助手"]
MAX_PER_CATEGORY = 100

# 排行榜参数：--chart 与 榜名称、表名、页面 Tab 文案 的映射
CHART_OPTIONS = {
    "hot": {"name": "热点榜", "chart_type": CHART_TYPE_HOT, "table": "latest_creative_library_hot"},
    "surge": {"name": "飙升榜", "chart_type": CHART_TYPE_SURGE, "table": "latest_creative_library_surge"},
    "new": {"name": "新创意榜", "chart_type": CHART_TYPE_NEW, "table": "latest_creative_library_new"},
}

DB_PATH = DATA_DIR / "ai_products_ua.db"

# 单表建表 SQL（三张表结构一致，含爬取日期 crawl_date，每周一爬）
def _create_table_sql(table_name: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_date TEXT NOT NULL,
    category TEXT NOT NULL,
    ad_key TEXT NOT NULL,
    advertiser_name TEXT,
    platform TEXT,
    heat INTEGER,
    all_exposure_value INTEGER,
    new_week_exposure_value INTEGER,
    days_count INTEGER,
    preview_img_url TEXT,
    video_url TEXT,
    raw_json TEXT,
    created_at TEXT DEFAULT (datetime('now', 'localtime'))
);
CREATE INDEX IF NOT EXISTS idx_{table_name}_crawl_date ON {table_name}(crawl_date);
CREATE INDEX IF NOT EXISTS idx_{table_name}_category ON {table_name}(category);
CREATE INDEX IF NOT EXISTS idx_{table_name}_platform ON {table_name}(platform);
"""


def _video_url(item: dict) -> str:
    for r in (item.get("resource_urls") or []):
        if r.get("video_url"):
            return r["video_url"]
    return ""


def _trim_per_category(per_category: list[tuple[str, list]]) -> list[tuple[str, list]]:
    """每类最多保留 MAX_PER_CATEGORY 条，不合并。"""
    return [(cat, lst[:MAX_PER_CATEGORY]) for cat, lst in per_category]


def init_tables(conn, table_name: str) -> None:
    """确保当前榜对应表存在（三张表结构一致，按需创建）。热点榜会从旧表名迁移；缺 crawl_date 则补列。"""
    cur = conn.cursor()
    # 热点榜：若存在旧表 latest_creative_library，重命名为 latest_creative_library_hot
    if table_name == "latest_creative_library_hot":
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='latest_creative_library'"
        )
        if cur.fetchone():
            cur.execute("ALTER TABLE latest_creative_library RENAME TO latest_creative_library_hot")
            conn.commit()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    )
    if not cur.fetchone():
        conn.executescript(_create_table_sql(table_name))
        conn.commit()
    else:
        # 已有表：若无 crawl_date 列则添加（兼容旧库）
        cur.execute(f"PRAGMA table_info({table_name})")
        cols = [row[1] for row in cur.fetchall()]
        if "crawl_date" not in cols:
            cur.execute(f"ALTER TABLE {table_name} ADD COLUMN crawl_date TEXT")
            conn.commit()


def write_to_db(per_category: list[tuple[str, list]], table_name: str, crawl_date: str) -> int:
    """按类写入指定表，每类最多 MAX_PER_CATEGORY 条，不合并。crawl_date 格式 YYYY-MM-DD。"""
    import sqlite3
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        init_tables(conn, table_name)
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {table_name} WHERE crawl_date = ?", (crawl_date,))
        insert_sql = f"""
        INSERT INTO {table_name}
        (crawl_date, category, ad_key, advertiser_name, platform, heat, all_exposure_value, new_week_exposure_value, days_count, preview_img_url, video_url, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        count = 0
        for category, creatives in per_category:
            for c in creatives:
                ad_key = c.get("ad_key") or ""
                advertiser_name = c.get("advertiser_name") or c.get("page_name") or ""
                platform = c.get("platform") or ""
                heat = c.get("heat") if c.get("heat") is not None else 0
                all_exp = c.get("all_exposure_value") if c.get("all_exposure_value") is not None else 0
                new_week = c.get("new_week_exposure_value") if c.get("new_week_exposure_value") is not None else 0
                days = c.get("days_count") if c.get("days_count") is not None else 0
                preview_img_url = c.get("preview_img_url") or ""
                video_url = _video_url(c)
                raw_json = json.dumps(c, ensure_ascii=False)
                cur.execute(insert_sql, (crawl_date, category, ad_key, advertiser_name, platform, heat, all_exp, new_week, days, preview_img_url, video_url, raw_json))
                count += 1
        conn.commit()
        return count
    finally:
        conn.close()


def parse_args():
    from datetime import date
    p = argparse.ArgumentParser(description="获取工具榜三类（AI 图像生成/视频生成/聊天与助手）写入最新素材库")
    p.add_argument(
        "--chart",
        choices=list(CHART_OPTIONS.keys()),
        default="hot",
        help="排行榜类型: hot=热点榜(每周热门榜), surge=飙升榜(每周飙升榜), new=新创意榜",
    )
    p.add_argument(
        "--date",
        default=None,
        metavar="YYYY-MM-DD",
        help="爬取日期，默认今天。每周一爬可传该周一日期，如 2026-03-09",
    )
    p.add_argument(
        "--date-range-index",
        type=int,
        default=None,
        metavar="N",
        help="榜单页日期范围下拉选第几项：0=第一项(默认周)，1=第二项，不传则不切换日期",
    )
    args = p.parse_args()
    if args.date:
        args.crawl_date = args.date
    else:
        args.crawl_date = date.today().strftime("%Y-%m-%d")
    return args


async def main():
    args = parse_args()
    opt = CHART_OPTIONS[args.chart]
    chart_type = opt["chart_type"]
    table_name = opt["table"]
    chart_name = opt["name"]

    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not email or not password:
        print("错误: 请在 .env 中设置 GUANGDADA_EMAIL 和 GUANGDADA_PASSWORD", file=sys.stderr)
        sys.exit(1)

    debug = os.getenv("DEBUG", "").lower() in ("1", "true", "yes")
    per_category = []
    total_found = 0
    empty_categories: list[str] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not debug)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = await context.new_page()
        try:
            print(f"[1] 登录（当前榜: {chart_name} / {chart_type}）...")
            if not await login(page, email, password):
                print("登录失败", file=sys.stderr)
                sys.exit(2)
            try:
                await prepare_hot_charts_page(
                    page, chart_type=chart_type, date_range_index=args.date_range_index
                )
            except Exception as e:
                print(f"[错误] 榜单准备失败：{e}")
                return
            for i, category in enumerate(CATEGORIES, 1):
                step_label = f"[3.{i}]"
                try:
                    creatives = await collect_hot_charts_category(page, category, step_label=step_label)
                except Exception as e:
                    print(f"  {step_label} 执行失败：{e}")
                    creatives = []
                per_category.append((category, creatives))
                if len(creatives) == 0:
                    empty_categories.append(category)
                else:
                    total_found += len(creatives)
        finally:
            await browser.close()

    # 只有三类都成功（每类都有至少一条）时才写入
    if empty_categories:
        print(
            "\n[终止] 以下分类未获取到任何素材，本次不生成最新素材库 JSON，也不写入数据库： "
            + "、".join(empty_categories)
        )
        return

    per_category = _trim_per_category(per_category)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    library = {cat: lst for cat, lst in per_category}
    library_json = DATA_DIR / f"最新素材库_{chart_name}.json"
    with open(library_json, "w", encoding="utf-8") as f:
        json.dump(library, f, ensure_ascii=False, indent=2)
    total = sum(len(lst) for _, lst in per_category)
    print(f"\n[4] 未合并，每类最多 {MAX_PER_CATEGORY} 条 → {library_json.name}，共 {total} 条")

    n = write_to_db(per_category, table_name, args.crawl_date)
    print(f"[5] 已写入表 {table_name}（{chart_name}），爬取日期 {args.crawl_date}，共 {n} 条")
    print("完成。")


if __name__ == "__main__":
    asyncio.run(main())
