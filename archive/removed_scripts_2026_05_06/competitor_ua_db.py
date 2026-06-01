"""
竞品 UA 素材专用 SQLite 数据库。
数据库文件：data/competitor_ua.db
"""

import json
import sqlite3
from pathlib import Path
from typing import Any, List

from path_util import DATA_DIR

DB_PATH = DATA_DIR / "competitor_ua.db"

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS competitor_ua_creatives_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_date TEXT NOT NULL,
    category TEXT NOT NULL,
    product TEXT NOT NULL,
    ios_appid TEXT,
    android_appid TEXT,
    ad_key TEXT NOT NULL,
    advertiser_name TEXT,
    title TEXT,
    body TEXT,
    platform TEXT,
    video_url TEXT,
    preview_img_url TEXT,
    heat INTEGER,
    all_exposure_value INTEGER,
    days_count INTEGER,
    raw_json TEXT,
    created_at TEXT DEFAULT (datetime('now', 'localtime'))
);
CREATE INDEX IF NOT EXISTS idx_comp_ua_daily_date ON competitor_ua_creatives_daily(crawl_date);
CREATE INDEX IF NOT EXISTS idx_comp_ua_daily_product ON competitor_ua_creatives_daily(product);
CREATE INDEX IF NOT EXISTS idx_comp_ua_daily_ad_key ON competitor_ua_creatives_daily(ad_key);

CREATE TABLE IF NOT EXISTS competitor_ua_new_creatives (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_date TEXT NOT NULL,
    prev_date TEXT NOT NULL,
    category TEXT NOT NULL,
    product TEXT NOT NULL,
    ios_appid TEXT,
    android_appid TEXT,
    ad_key TEXT NOT NULL,
    advertiser_name TEXT,
    title TEXT,
    body TEXT,
    platform TEXT,
    video_url TEXT,
    preview_img_url TEXT,
    heat INTEGER,
    all_exposure_value INTEGER,
    days_count INTEGER,
    raw_json TEXT,
    created_at TEXT DEFAULT (datetime('now', 'localtime'))
);
CREATE INDEX IF NOT EXISTS idx_comp_ua_new_date ON competitor_ua_new_creatives(crawl_date);
CREATE INDEX IF NOT EXISTS idx_comp_ua_new_product ON competitor_ua_new_creatives(product);
CREATE INDEX IF NOT EXISTS idx_comp_ua_new_ad_key ON competitor_ua_new_creatives(ad_key);
"""


def get_conn() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_conn()
    try:
        conn.executescript(CREATE_SQL)
        conn.commit()
    finally:
        conn.close()


def insert_competitor_creatives(
    crawl_date: str,
    items: List[dict],
) -> int:
    """
    插入一批竞品 UA 素材快照到 competitor_ua_creatives_daily。
    items 中每项需包含:
      - category, product, ios_appid, android_appid
      - creative: 原始创意 dict（含 ad_key / advertiser_name / title / body / platform 等）
    """
    if not items:
        return 0
    init_db()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM competitor_ua_creatives_daily WHERE crawl_date = ?",
            (crawl_date,),
        )
        insert_sql = """
        INSERT INTO competitor_ua_creatives_daily (
            crawl_date, category, product, ios_appid, android_appid,
            ad_key, advertiser_name, title, body, platform,
            video_url, preview_img_url, heat, all_exposure_value, days_count, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        count = 0
        for item in items:
            creative: dict[str, Any] = item.get("creative") or {}
            ad_key = creative.get("ad_key") or ""
            advertiser_name = creative.get("advertiser_name") or creative.get("page_name") or ""
            title = creative.get("title") or ""
            body = creative.get("body") or ""
            platform = creative.get("platform") or ""
            video_url = None
            for r in creative.get("resource_urls") or []:
                if r.get("video_url"):
                    video_url = r["video_url"]
                    break
            preview_img_url = creative.get("preview_img_url") or ""
            heat = creative.get("heat") if creative.get("heat") is not None else 0
            all_exp = creative.get("all_exposure_value") if creative.get("all_exposure_value") is not None else 0
            days = creative.get("days_count") if creative.get("days_count") is not None else 0
            raw_json = json.dumps(creative, ensure_ascii=False)
            cur.execute(
                insert_sql,
                (
                    crawl_date,
                    item.get("category", ""),
                    item.get("product", ""),
                    item.get("ios_appid"),
                    item.get("android_appid"),
                    ad_key,
                    advertiser_name,
                    title,
                    body,
                    platform,
                    video_url or "",
                    preview_img_url,
                    heat,
                    all_exp,
                    days,
                    raw_json,
                ),
            )
            count += 1
        conn.commit()
        return count
    finally:
        conn.close()


def compute_competitor_new_creatives(
    crawl_date: str,
    prev_date: str,
) -> int:
    """
    计算某日相较前一日的新增竞品素材。
    逻辑：同一 product/category 下，当日 ad_key 在前一日不存在即视为新增。
    将结果写入 competitor_ua_new_creatives，并返回新增条数。
    """
    init_db()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM competitor_ua_new_creatives WHERE crawl_date = ?",
            (crawl_date,),
        )
        insert_sql = """
        INSERT INTO competitor_ua_new_creatives (
            crawl_date, prev_date, category, product, ios_appid, android_appid,
            ad_key, advertiser_name, title, body, platform,
            video_url, preview_img_url, heat, all_exposure_value, days_count, raw_json
        )
        SELECT
            ? AS crawl_date,
            ? AS prev_date,
            d.category,
            d.product,
            d.ios_appid,
            d.android_appid,
            d.ad_key,
            d.advertiser_name,
            d.title,
            d.body,
            d.platform,
            d.video_url,
            d.preview_img_url,
            d.heat,
            d.all_exposure_value,
            d.days_count,
            d.raw_json
        FROM competitor_ua_creatives_daily d
        WHERE d.crawl_date = ?
          AND NOT EXISTS (
              SELECT 1
              FROM competitor_ua_creatives_daily p
              WHERE p.crawl_date = ?
                AND p.category = d.category
                AND p.product = d.product
                AND p.ad_key = d.ad_key
          )
        """
        cur.execute(
            insert_sql,
            (crawl_date, prev_date, crawl_date, prev_date),
        )
        conn.commit()
        return cur.rowcount or 0
    finally:
        conn.close()

