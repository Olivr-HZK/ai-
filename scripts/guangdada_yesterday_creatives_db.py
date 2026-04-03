"""
广大大「前一天上线素材」专用 SQLite 数据库。
数据库文件：data/guangdada_yesterday_creatives.db
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from path_util import DATA_DIR

DB_PATH = DATA_DIR / "guangdada_yesterday_creatives.db"

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS guangdada_competitor_yesterday_creatives (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_date TEXT NOT NULL,        -- 执行抓取的日期（通常是今天）
    target_date TEXT NOT NULL,       -- 上线日期（前一天）
    category TEXT NOT NULL,
    product TEXT NOT NULL,
    appid TEXT,
    keyword TEXT NOT NULL,

    ad_key TEXT NOT NULL,
    advertiser_name TEXT,
    title TEXT,
    body TEXT,
    platform TEXT,
    first_seen INTEGER,
    last_seen INTEGER,
    days_count INTEGER,
    heat INTEGER,
    all_exposure_value INTEGER,
    preview_img_url TEXT,
    video_url TEXT,
    raw_json TEXT,

    created_at TEXT DEFAULT (datetime('now', 'localtime'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_gd_yesterday_unique
ON guangdada_competitor_yesterday_creatives(target_date, product, ad_key);

CREATE INDEX IF NOT EXISTS idx_gd_yesterday_crawl_date
ON guangdada_competitor_yesterday_creatives(crawl_date);

CREATE INDEX IF NOT EXISTS idx_gd_yesterday_target_date
ON guangdada_competitor_yesterday_creatives(target_date);

CREATE INDEX IF NOT EXISTS idx_gd_yesterday_product
ON guangdada_competitor_yesterday_creatives(product);

-- 7天窗口下“界面可见的全部素材”（不按昨天过滤）
CREATE TABLE IF NOT EXISTS guangdada_competitor_creatives_7d_all (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_date TEXT NOT NULL,        -- 执行抓取的日期（通常是今天）
    category TEXT NOT NULL,
    product TEXT NOT NULL,
    appid TEXT,
    keyword TEXT NOT NULL,

    ad_key TEXT NOT NULL,
    advertiser_name TEXT,
    title TEXT,
    body TEXT,
    platform TEXT,
    first_seen INTEGER,
    last_seen INTEGER,
    days_count INTEGER,
    heat INTEGER,
    all_exposure_value INTEGER,
    preview_img_url TEXT,
    video_url TEXT,
    raw_json TEXT,

    created_at TEXT DEFAULT (datetime('now', 'localtime'))
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_gd_7d_all_unique
ON guangdada_competitor_creatives_7d_all(crawl_date, product, ad_key);
CREATE INDEX IF NOT EXISTS idx_gd_7d_all_crawl_date
ON guangdada_competitor_creatives_7d_all(crawl_date);
CREATE INDEX IF NOT EXISTS idx_gd_7d_all_product
ON guangdada_competitor_creatives_7d_all(product);
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


def _video_url(creative: dict[str, Any]) -> str:
    for r in creative.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("video_url"):
            return str(r["video_url"])
    return ""


def upsert_many(
    crawl_date: str,
    target_date: str,
    rows: list[dict[str, Any]],
) -> int:
    """
    写入多条素材（UPSERT）。
    rows 每项建议包含：
      - category, product, appid, keyword, creative(dict)
    返回：实际写入（insert 或 update）的条数（sqlite 的 rowcount 行为依赖版本，这里以循环计数为准）。
    """
    if not rows:
        return 0
    init_db()
    conn = get_conn()
    try:
        cur = conn.cursor()
        sql = """
        INSERT INTO guangdada_competitor_yesterday_creatives (
            crawl_date, target_date, category, product, appid, keyword,
            ad_key, advertiser_name, title, body, platform, first_seen, last_seen,
            days_count, heat, all_exposure_value, preview_img_url, video_url, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(target_date, product, ad_key) DO UPDATE SET
            crawl_date=excluded.crawl_date,
            category=excluded.category,
            appid=COALESCE(NULLIF(excluded.appid, ''), guangdada_competitor_yesterday_creatives.appid),
            keyword=excluded.keyword,
            advertiser_name=excluded.advertiser_name,
            title=excluded.title,
            body=excluded.body,
            platform=excluded.platform,
            first_seen=excluded.first_seen,
            last_seen=excluded.last_seen,
            days_count=excluded.days_count,
            heat=excluded.heat,
            all_exposure_value=excluded.all_exposure_value,
            preview_img_url=excluded.preview_img_url,
            video_url=excluded.video_url,
            raw_json=excluded.raw_json
        """
        n = 0
        for item in rows:
            creative = item.get("creative") or {}
            if not isinstance(creative, dict):
                continue
            ad_key = str(creative.get("ad_key") or creative.get("creative_id") or creative.get("id") or "").strip()
            if not ad_key:
                continue

            advertiser_name = creative.get("advertiser_name") or creative.get("page_name") or ""
            title = creative.get("title") or ""
            body = creative.get("body") or creative.get("message") or ""
            platform = creative.get("platform") or ""
            first_seen = creative.get("first_seen") or creative.get("created_at")
            last_seen = creative.get("last_seen")
            days_count = creative.get("days_count") if creative.get("days_count") is not None else 0
            heat = creative.get("heat") if creative.get("heat") is not None else 0
            all_exp = creative.get("all_exposure_value") if creative.get("all_exposure_value") is not None else 0
            preview_img_url = creative.get("preview_img_url") or ""
            video_url = _video_url(creative)
            raw_json = json.dumps(creative, ensure_ascii=False)

            cur.execute(
                sql,
                (
                    crawl_date,
                    target_date,
                    str(item.get("category") or ""),
                    str(item.get("product") or ""),
                    str(item.get("appid") or ""),
                    str(item.get("keyword") or ""),
                    ad_key,
                    str(advertiser_name),
                    str(title),
                    str(body),
                    str(platform),
                    int(first_seen) if first_seen is not None else None,
                    int(last_seen) if last_seen is not None else None,
                    int(days_count or 0),
                    int(heat or 0),
                    int(all_exp or 0),
                    str(preview_img_url),
                    str(video_url),
                    raw_json,
                ),
            )
            n += 1
        conn.commit()
        return n
    finally:
        conn.close()


def upsert_many_7d_all(
    crawl_date: str,
    rows: list[dict[str, Any]],
) -> int:
    """
    写入 7天窗口内“界面可见的全部素材”（不按昨天过滤），UPSERT。
    rows 每项建议包含：
      - category, product, appid, keyword, creative(dict)
    """
    if not rows:
        return 0
    init_db()
    conn = get_conn()
    try:
        cur = conn.cursor()
        sql = """
        INSERT INTO guangdada_competitor_creatives_7d_all (
            crawl_date, category, product, appid, keyword,
            ad_key, advertiser_name, title, body, platform, first_seen, last_seen,
            days_count, heat, all_exposure_value, preview_img_url, video_url, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(crawl_date, product, ad_key) DO UPDATE SET
            category=excluded.category,
            appid=COALESCE(NULLIF(excluded.appid, ''), guangdada_competitor_creatives_7d_all.appid),
            keyword=excluded.keyword,
            advertiser_name=excluded.advertiser_name,
            title=excluded.title,
            body=excluded.body,
            platform=excluded.platform,
            first_seen=excluded.first_seen,
            last_seen=excluded.last_seen,
            days_count=excluded.days_count,
            heat=excluded.heat,
            all_exposure_value=excluded.all_exposure_value,
            preview_img_url=excluded.preview_img_url,
            video_url=excluded.video_url,
            raw_json=excluded.raw_json
        """
        n = 0
        for item in rows:
            creative = item.get("creative") or {}
            if not isinstance(creative, dict):
                continue
            ad_key = str(creative.get("ad_key") or creative.get("creative_id") or creative.get("id") or "").strip()
            if not ad_key:
                continue

            advertiser_name = creative.get("advertiser_name") or creative.get("page_name") or ""
            title = creative.get("title") or ""
            body = creative.get("body") or creative.get("message") or ""
            platform = creative.get("platform") or ""
            first_seen = creative.get("first_seen") or creative.get("created_at")
            last_seen = creative.get("last_seen")
            days_count = creative.get("days_count") if creative.get("days_count") is not None else 0
            heat = creative.get("heat") if creative.get("heat") is not None else 0
            all_exp = creative.get("all_exposure_value") if creative.get("all_exposure_value") is not None else 0
            preview_img_url = creative.get("preview_img_url") or ""
            video_url = _video_url(creative)
            raw_json = json.dumps(creative, ensure_ascii=False)

            cur.execute(
                sql,
                (
                    crawl_date,
                    str(item.get("category") or ""),
                    str(item.get("product") or ""),
                    str(item.get("appid") or ""),
                    str(item.get("keyword") or ""),
                    ad_key,
                    str(advertiser_name),
                    str(title),
                    str(body),
                    str(platform),
                    int(first_seen) if first_seen is not None else None,
                    int(last_seen) if last_seen is not None else None,
                    int(days_count or 0),
                    int(heat or 0),
                    int(all_exp or 0),
                    str(preview_img_url),
                    str(video_url),
                    raw_json,
                ),
            )
            n += 1
        conn.commit()
        return n
    finally:
        conn.close()

