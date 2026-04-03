"""
AI 产品 UA 爬取结果写入 SQLite 数据库。数据库文件：data/ai_products_ua.db
"""
import json
import os
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    is_our_product INTEGER DEFAULT 0,
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
    our_products TEXT,          -- 适用我方产品列表（建议存 JSON 数组，如 ['Photo Enhancer', 'AI Image Playground']）
    our_ua_suggestions TEXT,    -- 针对各产品的完整 UA 建议（可存 Markdown 或 JSON 映射）
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    updated_at TEXT DEFAULT (datetime('now', 'localtime'))
);
CREATE INDEX IF NOT EXISTS idx_creative_crawl_date ON ad_creative_analysis(crawl_date);
CREATE INDEX IF NOT EXISTS idx_creative_category ON ad_creative_analysis(category);

CREATE TABLE IF NOT EXISTS ad_creative_product_suggestion (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ad_key TEXT NOT NULL,
    our_product TEXT NOT NULL,       -- 我方产品（内部名称或唯一标识）
    ua_suggestion TEXT NOT NULL,     -- 针对该产品的完整 UA 建议（创意方向 + 文案 + 避坑等）
    created_at TEXT DEFAULT (datetime('now', 'localtime'))
);
CREATE INDEX IF NOT EXISTS idx_creative_prod_sugg_ad_key ON ad_creative_product_suggestion(ad_key);
CREATE INDEX IF NOT EXISTS idx_creative_prod_sugg_product ON ad_creative_product_suggestion(our_product);
-- UNIQUE 索引将由 init_db 在「去重后」创建，避免历史重复导致建表失败

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

CREATE TABLE IF NOT EXISTS ai_llm_usage_daily (
    date TEXT PRIMARY KEY,          -- 统计日期 YYYY-MM-DD
    usage_json TEXT NOT NULL,       -- 每个模型/提供方的 token 使用情况 JSON
    created_at TEXT DEFAULT (datetime('now', 'localtime')),
    updated_at TEXT DEFAULT (datetime('now', 'localtime'))
);
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
        # 已有数据库可能缺新列，补列
        for col in ("title_zh", "body_zh", "our_products", "our_ua_suggestions"):
            try:
                conn.execute(f"ALTER TABLE ad_creative_analysis ADD COLUMN {col} TEXT")
                conn.commit()
            except sqlite3.OperationalError:
                pass
        try:
            conn.execute("ALTER TABLE ai_products_crawl ADD COLUMN is_our_product INTEGER DEFAULT 0")
            conn.commit()
        except sqlite3.OperationalError:
            pass

        # 历史数据可能存在重复：(ad_key, our_product) 多行。
        # 先做一次去重，再创建唯一索引，避免历史重复导致创建失败。
        try:
            conn.execute(
                """
                DELETE FROM ad_creative_product_suggestion
                WHERE id NOT IN (
                  -- 保留最新插入的那条（id 最大）
                  SELECT MAX(id)
                  FROM ad_creative_product_suggestion
                  GROUP BY ad_key, our_product
                )
                """
            )
            conn.commit()
        except Exception:
            pass

        # 去重后创建唯一索引，杜绝未来重复
        try:
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_creative_prod_sugg_ad_key_product
                ON ad_creative_product_suggestion(ad_key, our_product)
                """
            )
            conn.commit()
        except Exception:
            pass
    finally:
        conn.close()


def insert_crawl_results(
    crawl_date: str,
    products: List[dict],
    is_our_product: int = 0,
) -> int:
    """
    插入一批爬取结果。products 中每项需含 category, product, appid, keyword, selected, total_captured 或 error。
    appid 为 list 会转为 JSON 字符串；selected 为 dict 会转为 JSON 字符串。
    is_our_product=1 表示我方产品素材。
    返回插入行数。
    """
    init_db()
    conn = get_conn()
    try:
        cur = conn.cursor()
        insert_sql = """
        INSERT INTO ai_products_crawl (crawl_date, category, product, appid, keyword, selected, total_captured, error, is_our_product)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    is_our_product,
                ),
            )
            count += 1
        conn.commit()
        return count
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
            creative = item.get("creative") or {}
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
    """插入或更新单条广告创意；以 ad_key 区分。selected 为创意详情 dict。title_zh/body_zh 为中文翻译。"""
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
            """
            UPDATE ad_creative_analysis
            SET llm_analysis = ?, updated_at = datetime('now', 'localtime')
            WHERE ad_key = ?
            """,
            (llm_analysis, ad_key),
        )
        conn.commit()
    finally:
        conn.close()


def touch_creative_updated_at(ad_key: str) -> None:
    """
    仅刷新某条创意的 updated_at（不改任何业务字段）。
    用于在每日流程中标记「这条素材今天被再次看到/处理」。
    """
    if not ad_key:
        return
    init_db()
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE ad_creative_analysis
            SET updated_at = datetime('now', 'localtime')
            WHERE ad_key = ?
            """,
            (ad_key,),
        )
        conn.commit()
    finally:
        conn.close()

def update_creative_product_suggestions(
    ad_key: str,
    our_products: List[str],
    our_ua_suggestions: str,
) -> None:
    """
    更新某条创意的「适用我方产品列表」与「完整 UA 建议」字段。
    our_products 建议为产品内部名称列表，将以 JSON 字符串形式写入。
    our_ua_suggestions 为 Markdown 或纯文本。
    """
    init_db()
    conn = get_conn()
    try:
        products_json = json.dumps(our_products or [], ensure_ascii=False)
        conn.execute(
            """
            UPDATE ad_creative_analysis
            SET our_products = ?, our_ua_suggestions = ?, updated_at = datetime('now', 'localtime')
            WHERE ad_key = ?
            """,
            (products_json, our_ua_suggestions or "", ad_key),
        )
        conn.commit()
    finally:
        conn.close()


def insert_product_suggestions(
    ad_key: str,
    suggestions: List[dict],
) -> int:
    """
    为某条素材写入「素材 × 我方产品」级别的 UA 建议到独立表 ad_creative_product_suggestion。
    suggestions: 每项需包含 our_product（内部名称或唯一标识）和 ua_suggestion（完整 UA 建议文本）。
    返回插入条数。
    """
    if not ad_key or not suggestions:
        return 0
    init_db()
    conn = get_conn()
    try:
        cur = conn.cursor()
        # 有唯一索引 ux_creative_prod_sugg_ad_key_product 时，重复键会走更新，避免同一产品多条建议
        sql = """
        INSERT INTO ad_creative_product_suggestion (ad_key, our_product, ua_suggestion, created_at)
        VALUES (?, ?, ?, datetime('now', 'localtime'))
        ON CONFLICT(ad_key, our_product) DO UPDATE SET
          ua_suggestion = excluded.ua_suggestion,
          created_at = excluded.created_at
        """
        count = 0
        for item in suggestions:
            our_product = (item.get("our_product") or "").strip()
            ua_sugg = (item.get("ua_suggestion") or "").strip()
            if not our_product or not ua_sugg:
                continue
            cur.execute(sql, (ad_key, our_product, ua_sugg))
            count += 1
        conn.commit()
        return count
    finally:
        conn.close()


def query_by_date(crawl_date: str, is_our_product: Optional[int] = None) -> List[dict]:
    """按爬取日期查询，返回 list of dict（selected 为解析后的 dict）。is_our_product 为 1 仅查我方素材，0 仅竞品，None 不筛选。"""
    init_db()
    conn = get_conn()
    try:
        if is_our_product is not None:
            cur = conn.execute(
                "SELECT crawl_date, category, product, appid, keyword, selected, total_captured, error, is_our_product FROM ai_products_crawl WHERE crawl_date = ? AND is_our_product = ? ORDER BY id",
                (crawl_date, is_our_product),
            )
        else:
            cur = conn.execute(
                "SELECT crawl_date, category, product, appid, keyword, selected, total_captured, error, is_our_product FROM ai_products_crawl WHERE crawl_date = ? ORDER BY id",
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


def upsert_llm_usage(date: str, usage_json: str) -> None:
    """
    记录某天的 LLM token 使用统计。
    usage_json 建议为形如：
    {
      "openrouter:google/gemini-2.5-flash": {"prompt_tokens": 123, "completion_tokens": 456, "total_tokens": 579},
      "openai:gpt-4o-mini": {...}
    }
    """
    if not date or not usage_json:
        return
    init_db()
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO ai_llm_usage_daily (date, usage_json)
            VALUES (?, ?)
            ON CONFLICT(date) DO UPDATE SET
                usage_json = excluded.usage_json,
                updated_at = datetime('now', 'localtime')
            """,
            (date, usage_json),
        )
        conn.commit()
    finally:
        conn.close()


def _merge_token_stats(
    prev: Dict[str, Any],
    add: Dict[str, Any],
) -> Dict[str, int]:
    p = int(prev.get("prompt_tokens", 0) or 0)
    c = int(prev.get("completion_tokens", 0) or 0)
    t = int(prev.get("total_tokens", 0) or 0)
    return {
        "prompt_tokens": p + int(add.get("prompt_tokens", 0) or 0),
        "completion_tokens": c + int(add.get("completion_tokens", 0) or 0),
        "total_tokens": t + int(add.get("total_tokens", 0) or 0),
    }


def _recompute_video_enhancer_summary(existing: Dict[str, Any]) -> None:
    """汇总所有 video_enhancer:* 模型行，并可选按 total_tokens 估算美元。"""
    pt = ct = tt = 0
    for k, v in existing.items():
        if not isinstance(k, str) or not k.startswith("video_enhancer:"):
            continue
        if k.startswith("video_enhancer:_"):
            continue
        if not isinstance(v, dict):
            continue
        pt += int(v.get("prompt_tokens", 0) or 0)
        ct += int(v.get("completion_tokens", 0) or 0)
        tt += int(v.get("total_tokens", 0) or 0)
    summary: Dict[str, Any] = {
        "prompt_tokens": pt,
        "completion_tokens": ct,
        "total_tokens": tt,
    }
    rate = (os.getenv("VIDEO_ENHANCER_USD_PER_MILLION_TOTAL_TOKENS") or "").strip()
    if rate:
        try:
            summary["estimated_cost_usd"] = round((tt / 1_000_000.0) * float(rate), 6)
        except ValueError:
            pass
    existing["video_enhancer:_summary"] = summary


_USAGE_ACCUMULATE_LOCK = threading.Lock()


def accumulate_usage_tokens(
    patch: Dict[str, Dict[str, int]],
    provider: str,
    model: str,
    usage: Any,
) -> None:
    """
    将单次 chat.completions 的 usage 累加到 patch（key: video_enhancer:<provider>:<model>）。
    provider 如 openrouter、openai。
    多线程并行分析时安全。
    """
    if not model or usage is None:
        return
    try:
        p = int(getattr(usage, "prompt_tokens", 0) or 0)
        c = int(getattr(usage, "completion_tokens", 0) or 0)
        t = int(getattr(usage, "total_tokens", 0) or 0)
        if not t and (p or c):
            t = p + c
    except Exception:
        return
    key = f"video_enhancer:{provider}:{model}"
    with _USAGE_ACCUMULATE_LOCK:
        prev = patch.get(key)
        if isinstance(prev, dict):
            p += int(prev.get("prompt_tokens", 0) or 0)
            c += int(prev.get("completion_tokens", 0) or 0)
            t += int(prev.get("total_tokens", 0) or 0)
        patch[key] = {
            "prompt_tokens": p,
            "completion_tokens": c,
            "total_tokens": t,
        }


def merge_llm_usage_daily(date: str, patch: Dict[str, Dict[str, int]]) -> None:
    """
    将 patch 合并进 ai_llm_usage_daily 当日行（按 key 累加 token，不覆盖其它业务写入的 key）。
    patch 的 key 建议使用前缀 `video_enhancer:openrouter:<model>` 等。
    合并后刷新 `video_enhancer:_summary`（含可选 estimated_cost_usd，需设置 VIDEO_ENHANCER_USD_PER_MILLION_TOTAL_TOKENS）。
    """
    if not date or not patch:
        return
    init_db()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT usage_json FROM ai_llm_usage_daily WHERE date = ?", (date,))
        row = cur.fetchone()
        if row and row["usage_json"]:
            try:
                existing = json.loads(row["usage_json"])
            except Exception:
                existing = {}
            if not isinstance(existing, dict):
                existing = {}
        else:
            existing = {}
        for k, v in patch.items():
            if not isinstance(v, dict):
                continue
            prev = existing.get(k)
            if isinstance(prev, dict):
                existing[k] = _merge_token_stats(prev, v)
            else:
                existing[k] = {
                    "prompt_tokens": int(v.get("prompt_tokens", 0) or 0),
                    "completion_tokens": int(v.get("completion_tokens", 0) or 0),
                    "total_tokens": int(v.get("total_tokens", 0) or 0),
                }
        _recompute_video_enhancer_summary(existing)
        out = json.dumps(existing, ensure_ascii=False)
        conn.execute(
            """
            INSERT INTO ai_llm_usage_daily (date, usage_json)
            VALUES (?, ?)
            ON CONFLICT(date) DO UPDATE SET
                usage_json = excluded.usage_json,
                updated_at = datetime('now', 'localtime')
            """,
            (date, out),
        )
        conn.commit()
    finally:
        conn.close()


def format_video_enhancer_usage_log_line(date: str) -> str:
    """
    从 ai_llm_usage_daily 读取当日 video_enhancer:_summary，返回一行可打日志的文案；无数据则返回空串。
    """
    if not date:
        return ""
    init_db()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT usage_json FROM ai_llm_usage_daily WHERE date = ?", (date,))
        row = cur.fetchone()
        if not row or not row["usage_json"]:
            return ""
        d = json.loads(row["usage_json"])
        if not isinstance(d, dict):
            return ""
        s = d.get("video_enhancer:_summary")
        if not isinstance(s, dict):
            return ""
        tt = int(s.get("total_tokens", 0) or 0)
        pt = int(s.get("prompt_tokens", 0) or 0)
        ct = int(s.get("completion_tokens", 0) or 0)
        usd = s.get("estimated_cost_usd")
        if usd is not None:
            return (
                f"[usage] Video Enhancer 当日汇总 {date}: "
                f"prompt={pt} completion={ct} total={tt} "
                f"估算费用≈${usd} USD（按 VIDEO_ENHANCER_USD_PER_MILLION_TOTAL_TOKENS）"
            )
        return (
            f"[usage] Video Enhancer 当日汇总 {date}: "
            f"prompt={pt} completion={ct} total={tt}"
        )
    except Exception:
        return ""
    finally:
        conn.close()
