"""
竞品热门榜（人气值Top1%）专用 SQLite 数据库。
数据库文件：data/competitor_hot_rank.db
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, List

from path_util import DATA_DIR

DB_PATH = DATA_DIR / "competitor_hot_rank.db"

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS competitor_hot_creatives_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_date TEXT NOT NULL,
    category TEXT NOT NULL,
    product TEXT NOT NULL,
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
CREATE INDEX IF NOT EXISTS idx_comp_hot_daily_date ON competitor_hot_creatives_daily(crawl_date);
CREATE INDEX IF NOT EXISTS idx_comp_hot_daily_product ON competitor_hot_creatives_daily(product);
CREATE INDEX IF NOT EXISTS idx_comp_hot_daily_ad_key ON competitor_hot_creatives_daily(ad_key);

-- 「7天 / 素材 / 最新创意」结果快照表（不使用 Top创意 过滤）
CREATE TABLE IF NOT EXISTS competitor_latest_creatives_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_date TEXT NOT NULL,
    category TEXT NOT NULL,
    product TEXT NOT NULL,
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
CREATE INDEX IF NOT EXISTS idx_comp_latest_daily_date ON competitor_latest_creatives_daily(crawl_date);
CREATE INDEX IF NOT EXISTS idx_comp_latest_daily_product ON competitor_latest_creatives_daily(product);
CREATE INDEX IF NOT EXISTS idx_comp_latest_daily_ad_key ON competitor_latest_creatives_daily(ad_key);

-- 新晋榜 · 原始结果表（近 24 小时，按竞品抓取的所有素材，逐条保留 JSON）
CREATE TABLE IF NOT EXISTS competitor_new_raw_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_date TEXT NOT NULL,
    category TEXT NOT NULL,
    product TEXT NOT NULL,
    android_appid TEXT,
    ad_key TEXT,
    raw_json TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now', 'localtime'))
);
CREATE INDEX IF NOT EXISTS idx_comp_new_raw_date ON competitor_new_raw_daily(crawl_date);
CREATE INDEX IF NOT EXISTS idx_comp_new_raw_product ON competitor_new_raw_daily(product);
CREATE INDEX IF NOT EXISTS idx_comp_new_raw_ad_key ON competitor_new_raw_daily(ad_key);

-- 新晋榜 · 去重结果表（字段结构与热门榜一致，用于后续新晋榜报表）
CREATE TABLE IF NOT EXISTS competitor_new_creatives_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_date TEXT NOT NULL,
    category TEXT NOT NULL,
    product TEXT NOT NULL,
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
CREATE INDEX IF NOT EXISTS idx_comp_new_daily_date ON competitor_new_creatives_daily(crawl_date);
CREATE INDEX IF NOT EXISTS idx_comp_new_daily_product ON competitor_new_creatives_daily(product);
CREATE INDEX IF NOT EXISTS idx_comp_new_daily_ad_key ON competitor_new_creatives_daily(ad_key);

-- 第 2 步：视频级分析表（只存 ad_key 与视频分析对应关系）
CREATE TABLE IF NOT EXISTS competitor_hot_video_analysis (
    ad_key TEXT PRIMARY KEY,
    crawl_date TEXT,
    video_url TEXT,
    video_analysis TEXT,  -- 使用 Gemini 多模态模型生成的视频内容解析（Markdown 或结构化文本）
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    updated_at TEXT DEFAULT (datetime('now', 'localtime'))
);
CREATE INDEX IF NOT EXISTS idx_comp_hot_video_crawl_date ON competitor_hot_video_analysis(crawl_date);

-- 第 3 步：聚类 + 深度解析结果表
CREATE TABLE IF NOT EXISTS competitor_hot_clusters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crawl_date TEXT NOT NULL,
    category TEXT NOT NULL,         -- seek / video enhancer
    cluster_id TEXT NOT NULL,       -- 例如 seek-1, video_enhancer-2
    cluster_title TEXT,             -- 聚类标题
    repr_count INTEGER,             -- 代表视频条数
    max_play INTEGER,               -- 最高播放/热度
    representative_ad_keys TEXT,    -- JSON 数组字符串
    background TEXT,                -- 背景（热点内容）
    ua_suggestion TEXT,             -- UA 建议
    product_points TEXT,            -- 产品对标点
    risk TEXT,                      -- 风险提示
    trend_label TEXT,               -- 趋势阶段标签
    trend_reason TEXT,              -- 趋势阶段依据
    raw_json TEXT,                  -- 原始 LLM 聚类 JSON（单聚类）
    created_at TEXT DEFAULT (datetime('now', 'localtime'))
);
CREATE INDEX IF NOT EXISTS idx_comp_hot_clusters_date ON competitor_hot_clusters(crawl_date);
CREATE INDEX IF NOT EXISTS idx_comp_hot_clusters_cat ON competitor_hot_clusters(category);
CREATE INDEX IF NOT EXISTS idx_comp_hot_clusters_cluster ON competitor_hot_clusters(cluster_id);
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


def insert_hot_creatives(
    crawl_date: str,
    items: List[dict],
) -> int:
    """
    插入一批「人气值Top1%」竞品 UA 素材快照到 competitor_hot_creatives_daily。

    items 中每项需包含:
      - category, product, android_appid
      - creative: 原始创意 dict（含 ad_key / advertiser_name / title / body / platform 等）
    """
    if not items:
        return 0
    init_db()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM competitor_hot_creatives_daily WHERE crawl_date = ?",
            (crawl_date,),
        )
        insert_sql = """
        INSERT INTO competitor_hot_creatives_daily (
            crawl_date, category, product, android_appid,
            ad_key, advertiser_name, title, body, platform,
            video_url, preview_img_url, heat, all_exposure_value, days_count, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        count = 0
        for item in items:
            creative: dict[str, Any] = item.get("creative") or {}
            ad_key = (
                creative.get("ad_key")
                or creative.get("creative_id")
                or creative.get("id")
                or creative.get("creativeId")
                or ""
            )
            if not ad_key:
                continue
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


def insert_latest_creatives(
    crawl_date: str,
    items: List[dict],
) -> int:
    """
    插入一批按「7天 / 素材 / 最新创意」筛选抓取的竞品 UA 素材快照到
    competitor_latest_creatives_daily 表（不使用 Top创意 过滤）。

    items 中每项需包含:
      - category, product, android_appid
      - creative: 原始创意 dict（含 ad_key / advertiser_name / title / body / platform 等）
    """
    if not items:
        return 0
    init_db()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM competitor_latest_creatives_daily WHERE crawl_date = ?",
            (crawl_date,),
        )
        insert_sql = """
        INSERT INTO competitor_latest_creatives_daily (
            crawl_date, category, product, android_appid,
            ad_key, advertiser_name, title, body, platform,
            video_url, preview_img_url, heat, all_exposure_value, days_count, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        count = 0
        for item in items:
            creative: dict[str, Any] = item.get("creative") or {}
            ad_key = (
                creative.get("ad_key")
                or creative.get("creative_id")
                or creative.get("id")
                or creative.get("creativeId")
                or ""
            )
            if not ad_key:
                continue
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


def insert_new_raw_creatives(
    crawl_date: str,
    items: List[dict],
) -> int:
    """
    新晋榜 · 原始结果入库：
    - 每个竞品 / 关键词仅写入一行原始响应快照到 competitor_new_raw_daily
    - 通常 items 中一条对应 run_batch 的一个 result（包含 all_creatives 等字段）
    - 仅做 JSON 快照，后续新晋榜逻辑可以基于该表做对比 / 过滤
    """
    if not items:
        return 0
    init_db()
    conn = get_conn()
    try:
        cur = conn.cursor()
        # 每天全量覆盖该日期对应的数据
        cur.execute(
            "DELETE FROM competitor_new_raw_daily WHERE crawl_date = ?",
            (crawl_date,),
        )
        insert_sql = """
        INSERT INTO competitor_new_raw_daily (
            crawl_date, category, product, android_appid, ad_key, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?)
        """
        count = 0
        for item in items:
            # raw_item 通常是 run_batch 返回的单条 result（包含 keyword / all_creatives 等字段）
            raw_obj = item.get("raw") or {}
            ad_key = item.get("ad_key")  # 预留字段，可为 None
            raw_json = json.dumps(raw_obj, ensure_ascii=False)
            cur.execute(
                insert_sql,
                (
                    crawl_date,
                    item.get("category", ""),
                    item.get("product", ""),
                    item.get("android_appid"),
                    ad_key,
                    raw_json,
                ),
            )
            count += 1
        conn.commit()
        return count
    finally:
        conn.close()


def insert_new_dedup_creatives(
    crawl_date: str,
    items: List[dict],
) -> int:
    """
    新晋榜 · 去重结果入库：
    - 字段结构与 competitor_hot_creatives_daily / competitor_latest_creatives_daily 一致
    - 由上游脚本按 ad_key 去重后传入（通常保留同一 ad_key 下 heat 最大的一条）
    """
    if not items:
        return 0
    init_db()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM competitor_new_creatives_daily WHERE crawl_date = ?",
            (crawl_date,),
        )
        insert_sql = """
        INSERT INTO competitor_new_creatives_daily (
            crawl_date, category, product, android_appid,
            ad_key, advertiser_name, title, body, platform,
            video_url, preview_img_url, heat, all_exposure_value, days_count, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        count = 0
        for item in items:
            creative: dict[str, Any] = item.get("creative") or {}
            ad_key = (
                creative.get("ad_key")
                or creative.get("creative_id")
                or creative.get("id")
                or creative.get("creativeId")
                or ""
            )
            if not ad_key:
                continue
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


def compute_new_rank_new_creatives(
    crawl_date: str,
    prev_date: str,
) -> int:
    """
    新晋榜 · 计算「今天相对昨天的新增素材」，结果写入 competitor_new_creatives_daily。

    逻辑：
    - 来源：competitor_new_raw_daily 中指定日期与前一日的 raw_json（run_batch result）
    - 对每条 raw_json 读取 result["all_creatives"] 作为素材列表
    - 对于同一 category/product 下：
        - 今天有、昨天没有的 ad_key 视为「新增」
        - 若同一 ad_key 在今天多次出现，仅保留 heat 最大的一条
    - 先清空目标日期在 competitor_new_creatives_daily 中的记录，再写入新增结果。

    返回写入的新增条数。
    """
    init_db()
    conn = get_conn()
    try:
        cur = conn.cursor()

        # 1. 读取昨天的 raw 快照，构建「已有 ad_key」集合
        prev_keys: set[tuple[str, str, str]] = set()
        for r in cur.execute(
            """
            SELECT category, product, raw_json
            FROM competitor_new_raw_daily
            WHERE crawl_date = ?
            """,
            (prev_date,),
        ):
            category = r["category"]
            product = r["product"]
            try:
                raw_obj = json.loads(r["raw_json"] or "{}")
            except Exception:
                raw_obj = {}
            creatives = raw_obj.get("all_creatives") or []
            if not isinstance(creatives, list):
                continue
            for c in creatives:
                if not isinstance(c, dict):
                    continue
                ad_key = (
                    c.get("ad_key")
                    or c.get("creative_id")
                    or c.get("id")
                    or c.get("creativeId")
                    or ""
                )
                if not ad_key:
                    continue
                prev_keys.add((category, product, ad_key))

        # 2. 读取今天的 raw 快照，筛选出「今天有、昨天没有」的 ad_key，并按 heat 去重
        dedup: dict[tuple[str, str, str], dict[str, Any]] = {}
        for r in cur.execute(
            """
            SELECT category, product, android_appid, raw_json
            FROM competitor_new_raw_daily
            WHERE crawl_date = ?
            """,
            (crawl_date,),
        ):
            category = r["category"]
            product = r["product"]
            android_appid = r["android_appid"]
            try:
                raw_obj = json.loads(r["raw_json"] or "{}")
            except Exception:
                raw_obj = {}
            creatives = raw_obj.get("all_creatives") or []
            if not isinstance(creatives, list):
                continue
            for c in creatives:
                if not isinstance(c, dict):
                    continue
                ad_key = (
                    c.get("ad_key")
                    or c.get("creative_id")
                    or c.get("id")
                    or c.get("creativeId")
                    or ""
                )
                if not ad_key:
                    continue
                key = (category, product, ad_key)
                # 过滤掉昨天已出现的素材
                if key in prev_keys:
                    continue
                heat = int(c.get("heat") or 0)
                old = dedup.get(key)
                if old is not None:
                    old_heat = int(((old.get("creative") or {}).get("heat")) or 0)
                    if heat <= old_heat:
                        continue
                dedup[key] = {
                    "category": category,
                    "product": product,
                    "android_appid": android_appid,
                    "creative": c,
                }

        # 3. 将新增素材写入去重结果表
        items = list(dedup.values())
        return insert_new_dedup_creatives(crawl_date, items)
    finally:
        conn.close()


def upsert_video_analysis(
    ad_key: str,
    crawl_date: str | None,
    video_url: str | None,
    video_analysis: str,
) -> None:
    """
    第 2 步：为某条素材写入/更新视频分析结果。
    仅以 ad_key 为主键，其他字段作为附加信息。
    """
    if not ad_key:
        return
    init_db()
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO competitor_hot_video_analysis (
                ad_key, crawl_date, video_url, video_analysis, created_at, updated_at
            ) VALUES (?, ?, ?, ?, datetime('now','localtime'), datetime('now','localtime'))
            ON CONFLICT(ad_key) DO UPDATE SET
                crawl_date = COALESCE(excluded.crawl_date, competitor_hot_video_analysis.crawl_date),
                video_url = COALESCE(excluded.video_url, competitor_hot_video_analysis.video_url),
                video_analysis = excluded.video_analysis,
                updated_at = datetime('now','localtime')
            """,
            (ad_key, crawl_date, video_url or "", video_analysis or ""),
        )
        conn.commit()
    finally:
        conn.close()


def fetch_videos_without_analysis(
    crawl_date: str | None = None,
    limit: int = 100,
) -> list[dict]:
    """
    查询还没有视频分析结果的素材列表：
    - 来源表：competitor_hot_creatives_daily（限定有 video_url）
    - 去除在 competitor_hot_video_analysis 已有记录的 ad_key
    可选按 crawl_date 过滤。
    """
    init_db()
    conn = get_conn()
    try:
        params: list[Any] = []
        where = ["c.video_url IS NOT NULL", "c.video_url != ''"]
        if crawl_date:
            where.append("c.crawl_date = ?")
            params.append(crawl_date)
        where_clause = " AND ".join(where)
        sql = f"""
        SELECT
          c.crawl_date,
          c.category,
          c.product,
          c.ad_key,
          c.video_url
        FROM competitor_hot_creatives_daily AS c
        LEFT JOIN competitor_hot_video_analysis AS v
          ON v.ad_key = c.ad_key
        WHERE {where_clause}
          AND v.ad_key IS NULL
        ORDER BY c.crawl_date DESC, c.category, c.product, c.ad_key
        LIMIT ?
        """
        params.append(limit)
        cur = conn.execute(sql, params)
        rows = []
        for r in cur.fetchall():
            rows.append(
                {
                    "crawl_date": r["crawl_date"],
                    "category": r["category"],
                    "product": r["product"],
                    "ad_key": r["ad_key"],
                    "video_url": r["video_url"],
                }
            )
        return rows
    finally:
        conn.close()


