"""
AI 产品 UA 爬取结果写入 SQLite 数据库（skills 版）。
数据库文件：data/ai_products_ua.db
"""
import json
import sqlite3
from typing import List, Optional

from path_util import DATA_DIR

DB_PATH = DATA_DIR / "ai_products_ua.db"

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS ai_products_crawl (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_date TEXT NOT NULL,
    category TEXT NOT NULL,
    product TEXT NOT NULL,
    appid TEXT NOT NULL,
    keyword TEXT,
    selected TEXT,
    total_captured INTEGER,
    error TEXT,
    created_at TEXT DEFAULT (datetime('now', 'localtime'))
);

CREATE INDEX IF NOT EXISTS idx_crawl_date ON ai_products_crawl(crawl_date);
CREATE INDEX IF NOT EXISTS idx_category ON ai_products_crawl(category);
CREATE INDEX IF NOT EXISTS idx_product ON ai_products_crawl(product);

CREATE TABLE IF NOT EXISTS ad_creative_analysis (
    ad_key TEXT PRIMARY KEY,
    crawl_date TEXT,
    category TEXT,
    product TEXT,
    advertiser_name TEXT,
    title TEXT,
    body TEXT,
    title_zh TEXT,
    body_zh TEXT,
    platform TEXT,
    video_url TEXT,
    video_duration INTEGER,
    preview_img_url TEXT,
    selected_json TEXT,
    llm_analysis TEXT,
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    updated_at TEXT DEFAULT (datetime('now', 'localtime'))
);
CREATE INDEX IF NOT EXISTS idx_creative_crawl_date ON ad_creative_analysis(crawl_date);
CREATE INDEX IF NOT EXISTS idx_creative_category ON ad_creative_analysis(category);
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
        # 兼容旧库，补充 title_zh / body_zh
        for col in ("title_zh", "body_zh"):
            try:
                conn.execute(f"ALTER TABLE ad_creative_analysis ADD COLUMN {col} TEXT")
                conn.commit()
            except sqlite3.OperationalError:
                pass
    finally:
        conn.close()


def insert_crawl_results(crawl_date: str, products: List[dict]) -> int:
    """
    插入一批爬取结果。products 中每项需含 category, product, appid, keyword, selected, total_captured 或 error。
    appid 为 list 会转为 JSON 字符串；selected 为 dict 会转为 JSON 字符串。
    返回插入行数。
    """
    init_db()
    conn = get_conn()
    try:
        cur = conn.cursor()
        insert_sql = """
        INSERT INTO ai_products_crawl (crawl_date, category, product, appid, keyword, selected, total_captured, error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        count = 0
        for row in products:
            appid_str = json.dumps(row.get("appid") or [], ensure_ascii=False)
            selected = row.get("selected")
            selected_str = json.dumps(selected, ensure_ascii=False) if selected is not None else None
            keyword = row.get("keyword") or ""
            total = row.get("total_captured")
            if total is None and "error" in row:
                total = None
            error = row.get("error")
            cur.execute(
                insert_sql,
                (
                    crawl_date,
                    row.get("category", ""),
                    row.get("product", ""),
                    appid_str,
                    keyword,
                    selected_str,
                    total,
                    error,
                ),
            )
            count += 1
        conn.commit()
        return count
    finally:
        conn.close()


def upsert_creative(
    ad_key: str,
    crawl_date: str,
    category: str,
    product: str,
    selected: dict,
    llm_analysis: Optional[str] = None,
    title_zh: Optional[str] = None,
    body_zh: Optional[str] = None,
) -> None:
    """插入或更新单条广告创意；以 ad_key 区分。"""
    init_db()
    video_url = None
    for r in (selected.get("resource_urls") or []):
        if r.get("video_url"):
            video_url = r["video_url"]
            break
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO ad_creative_analysis (
                ad_key, crawl_date, category, product, advertiser_name, title, body, title_zh, body_zh,
                platform, video_url, video_duration, preview_img_url, selected_json, llm_analysis, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now', 'localtime'))
            ON CONFLICT(ad_key) DO UPDATE SET
                crawl_date=excluded.crawl_date,
                category=excluded.category,
                product=excluded.product,
                advertiser_name=excluded.advertiser_name,
                title=excluded.title,
                body=excluded.body,
                title_zh=COALESCE(NULLIF(excluded.title_zh, ''), ad_creative_analysis.title_zh),
                body_zh=COALESCE(NULLIF(excluded.body_zh, ''), ad_creative_analysis.body_zh),
                platform=excluded.platform,
                video_url=excluded.video_url,
                video_duration=excluded.video_duration,
                preview_img_url=excluded.preview_img_url,
                selected_json=excluded.selected_json,
                llm_analysis=COALESCE(NULLIF(excluded.llm_analysis, ''), ad_creative_analysis.llm_analysis),
                updated_at=datetime('now', 'localtime')
            """,
            (
                ad_key,
                crawl_date,
                category,
                product,
                selected.get("advertiser_name") or "",
                selected.get("title") or "",
                selected.get("body") or "",
                title_zh or "",
                body_zh or "",
                selected.get("platform") or "",
                video_url,
                selected.get("video_duration") or 0,
                selected.get("preview_img_url") or "",
                json.dumps(selected, ensure_ascii=False),
                llm_analysis,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def update_creative_llm_analysis(ad_key: str, llm_analysis: str) -> None:
    """仅更新某条创意的 LLM 分析内容。"""
    init_db()
    conn = get_conn()
    try:
        conn.execute(
            "UPDATE ad_creative_analysis SET llm_analysis = ?, updated_at = datetime('now', 'localtime') WHERE ad_key = ?",
            (llm_analysis, ad_key),
        )
        conn.commit()
    finally:
        conn.close()


def query_by_date(crawl_date: str) -> List[dict]:
    """按爬取日期查询，返回 list of dict（selected 为解析后的 dict）。"""
    init_db()
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT crawl_date, category, product, appid, keyword, selected, total_captured, error FROM ai_products_crawl WHERE crawl_date = ? ORDER BY id",
            (crawl_date,),
        )
        rows = []
        for r in cur.fetchall():
            d = dict(r)
            if d.get("appid"):
                try:
                    d["appid"] = json.loads(d["appid"])
                except Exception:
                    pass
            if d.get("selected"):
                try:
                    d["selected"] = json.loads(d["selected"])
                except Exception:
                    pass
            rows.append(d)
        return rows
    finally:
        conn.close()

