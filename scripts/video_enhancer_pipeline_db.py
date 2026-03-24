"""
Video Enhancer 工作流专用 SQLite 库：
- daily_creative_insights：每天的素材 + 灵感分析明细
- daily_ua_push_content：每天推送到飞书的方向卡片/UA 建议
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from path_util import DATA_DIR

DB_PATH = DATA_DIR / "video_enhancer_pipeline.db"


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = _get_conn()
    try:
        cur = conn.cursor()
        # 明细表：每天素材+分析
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS daily_creative_insights (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              crawl_date TEXT,
              target_date TEXT NOT NULL,
              category TEXT,
              product TEXT,
              appid TEXT,
              ad_key TEXT NOT NULL,
              platform TEXT,
              video_url TEXT,
              preview_img_url TEXT,
              video_duration INTEGER,
              first_seen INTEGER,
              created_at INTEGER,
              last_seen INTEGER,
              heat INTEGER,
              all_exposure_value INTEGER,
              impression INTEGER,
              raw_json TEXT,
              insight_analysis TEXT,
              created_at_local TEXT DEFAULT (datetime('now','localtime')),
              updated_at_local TEXT DEFAULT (datetime('now','localtime')),
              UNIQUE(target_date, appid, ad_key)
            );
            """
        )
        # 汇总表：每天推送 UA 建议
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS daily_ua_push_content (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              target_date TEXT NOT NULL,
              direction_name TEXT,
              core_summary TEXT,
              background TEXT,
              ua_suggestion TEXT,
              product_benchmark TEXT,
              risk_note TEXT,
              trend_judgement TEXT,
              reference_links_json TEXT,
              card_markdown TEXT,
              bitable_app_token TEXT,
              bitable_table_id TEXT,
              push_status TEXT,
              push_response TEXT,
              created_at_local TEXT DEFAULT (datetime('now','localtime')),
              updated_at_local TEXT DEFAULT (datetime('now','localtime')),
              UNIQUE(target_date, direction_name, bitable_app_token, bitable_table_id)
            );
            """
        )
        # 新增：视频增强工作流的“抓取候选->截断->保留”筛选统计
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS daily_video_enhancer_filter_log (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              target_date TEXT NOT NULL,
              filter_threshold INTEGER,
              filter_keep INTEGER,
              filter_sort_metric TEXT,
              pre_truncation_total INTEGER,

              product TEXT NOT NULL,
              before_cnt INTEGER NOT NULL,
              after_cnt INTEGER NOT NULL,
              truncated INTEGER NOT NULL DEFAULT 0,

              created_at_local TEXT DEFAULT (datetime('now','localtime')),
              updated_at_local TEXT DEFAULT (datetime('now','localtime')),

              UNIQUE(target_date, product)
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def upsert_daily_creative_insights(
    target_date: str,
    raw_payload: Dict[str, Any],
    analysis_by_ad: Dict[str, str],
) -> int:
    """
    基于 raw JSON + analysis 映射，写入/更新 daily_creative_insights。

    raw_payload 结构为 test_video_enhancer_..._raw.json：
    {
      "target_date": "...",
      "total": ...,
      "items": [
        {
          "category": ...,
          "product": ...,
          "appid": ...,
          "creative": {...}
        }
      ]
    }
    """
    init_db()
    items = raw_payload.get("items") or []
    if not isinstance(items, list):
        return 0

    crawl_date = raw_payload.get("crawl_date") or None
    conn = _get_conn()
    try:
        cur = conn.cursor()
        sql = """
        INSERT INTO daily_creative_insights (
          crawl_date, target_date, category, product, appid,
          ad_key, platform, video_url, preview_img_url, video_duration,
          first_seen, created_at, last_seen,
          heat, all_exposure_value, impression,
          raw_json, insight_analysis,
          created_at_local, updated_at_local
        ) VALUES (
          ?, ?, ?, ?, ?,
          ?, ?, ?, ?, ?,
          ?, ?, ?,
          ?, ?, ?,
          ?, ?,
          datetime('now','localtime'), datetime('now','localtime')
        )
        ON CONFLICT(target_date, appid, ad_key) DO UPDATE SET
          platform=excluded.platform,
          video_url=excluded.video_url,
          preview_img_url=excluded.preview_img_url,
          video_duration=excluded.video_duration,
          first_seen=excluded.first_seen,
          created_at=excluded.created_at,
          last_seen=excluded.last_seen,
          heat=excluded.heat,
          all_exposure_value=excluded.all_exposure_value,
          impression=excluded.impression,
          raw_json=excluded.raw_json,
          insight_analysis=CASE
            WHEN COALESCE(TRIM(excluded.insight_analysis), '') <> ''
                 AND excluded.insight_analysis NOT LIKE '[ERROR]%'
            THEN excluded.insight_analysis
            ELSE daily_creative_insights.insight_analysis
          END,
          updated_at_local=datetime('now','localtime');
        """
        n = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            category = item.get("category")
            product = item.get("product")
            appid = item.get("appid")
            creative = item.get("creative") or {}
            if not isinstance(creative, dict):
                continue
            ad_key = str(creative.get("ad_key") or "")
            if not ad_key:
                continue
            platform = creative.get("platform")
            video_url = ""
            if creative.get("video_url"):
                video_url = str(creative["video_url"])
            else:
                for r in creative.get("resource_urls") or []:
                    if isinstance(r, dict) and r.get("video_url"):
                        video_url = str(r["video_url"])
                        break
            preview = creative.get("preview_img_url")
            video_duration = creative.get("video_duration")
            first_seen = creative.get("first_seen")
            created_at = creative.get("created_at")
            last_seen = creative.get("last_seen")
            heat = creative.get("heat")
            all_exp = creative.get("all_exposure_value")
            impression = creative.get("impression")
            raw_json = json.dumps(creative, ensure_ascii=False)
            insight = analysis_by_ad.get(ad_key) or ""
            cur.execute(
                sql,
                (
                    crawl_date,
                    target_date,
                    category,
                    product,
                    appid,
                    ad_key,
                    platform,
                    video_url,
                    preview,
                    int(video_duration or 0),
                    int(first_seen or 0) if first_seen is not None else None,
                    int(created_at or 0) if created_at is not None else None,
                    int(last_seen or 0) if last_seen is not None else None,
                    int(heat or 0) if heat is not None else None,
                    int(all_exp or 0) if all_exp is not None else None,
                    int(impression or 0) if impression is not None else None,
                    raw_json,
                    insight,
                ),
            )
            n += 1
        conn.commit()
        return n
    finally:
        conn.close()


def upsert_daily_video_enhancer_filter_log(
    target_date: str,
    filter_report: Dict[str, Any] | None,
) -> int:
    """
    写入/更新 daily_video_enhancer_filter_log。

    filter_report 来自 test_video_enhancer_two_competitors_318.py raw.json 中的 filter_report 字段：
    {
      filter_threshold, filter_keep, filter_sort_metric,
      pre_truncation_total, post_truncation_total,
      per_product: { product_key: {before, after, truncated} }
    }
    """
    init_db()
    if not isinstance(filter_report, dict):
        return 0

    threshold = int(filter_report.get("filter_threshold") or 0)
    keep = int(filter_report.get("filter_keep") or 0)
    sort_metric = str(filter_report.get("filter_sort_metric") or "")
    pre_total = int(filter_report.get("pre_truncation_total") or 0)
    post_total = int(filter_report.get("post_truncation_total") or 0)
    per_product = filter_report.get("per_product") or {}
    if not isinstance(per_product, dict):
        per_product = {}

    conn = _get_conn()
    try:
        cur = conn.cursor()
        sql = """
        INSERT INTO daily_video_enhancer_filter_log (
          target_date, filter_threshold, filter_keep, filter_sort_metric, pre_truncation_total,
          product, before_cnt, after_cnt, truncated,
          created_at_local, updated_at_local
        ) VALUES (
          ?, ?, ?, ?, ?,
          ?, ?, ?, ?,
          datetime('now','localtime'), datetime('now','localtime')
        )
        ON CONFLICT(target_date, product) DO UPDATE SET
          filter_threshold=excluded.filter_threshold,
          filter_keep=excluded.filter_keep,
          filter_sort_metric=excluded.filter_sort_metric,
          pre_truncation_total=excluded.pre_truncation_total,
          before_cnt=excluded.before_cnt,
          after_cnt=excluded.after_cnt,
          truncated=excluded.truncated,
          updated_at_local=datetime('now','localtime');
        """

        n = 0
        # 总计行
        cur.execute(
            sql,
            (
                target_date,
                threshold,
                keep,
                sort_metric,
                pre_total,
                "__TOTAL__",
                pre_total,
                post_total,
                1 if post_total != pre_total else 0,
            ),
        )
        n += 1

        # 各产品行
        for product, info in per_product.items():
            if not isinstance(info, dict):
                continue
            before_cnt = int(info.get("before") or 0)
            after_cnt = int(info.get("after") or 0)
            truncated = int(1 if info.get("truncated") else 0)
            cur.execute(
                sql,
                (
                    target_date,
                    threshold,
                    keep,
                    sort_metric,
                    pre_total,
                    str(product),
                    before_cnt,
                    after_cnt,
                    truncated,
                ),
            )
            n += 1

        conn.commit()
        return n
    finally:
        conn.close()


def upsert_daily_push_content(
    target_date: str,
    suggestion_obj: Dict[str, Any] | None,
    card_markdown: str,
    bitable_app_token: str,
    bitable_table_id: str,
    push_status: str | None = None,
    push_response: str | None = None,
) -> int:
    """
    把方向卡片 / UA 建议写入 daily_ua_push_content。
    suggestion_obj 为 generate_video_enhancer_ua_suggestions_from_analysis 的 JSON 结果。
    """
    init_db()
    if not isinstance(suggestion_obj, dict):
        return 0
    s = suggestion_obj.get("suggestion") or suggestion_obj
    cards = s.get("方向卡片") if isinstance(s, dict) else None
    if not isinstance(cards, list):
        return 0

    conn = _get_conn()
    try:
        cur = conn.cursor()
        sql = """
        INSERT INTO daily_ua_push_content (
          target_date, direction_name,
          core_summary, background, ua_suggestion,
          product_benchmark, risk_note, trend_judgement,
          reference_links_json, card_markdown,
          bitable_app_token, bitable_table_id,
          push_status, push_response,
          created_at_local, updated_at_local
        ) VALUES (
          ?, ?,
          ?, ?, ?,
          ?, ?, ?,
          ?, ?,
          ?, ?,
          ?, ?,
          datetime('now','localtime'), datetime('now','localtime')
        )
        ON CONFLICT(target_date, direction_name, bitable_app_token, bitable_table_id) DO UPDATE SET
          core_summary=excluded.core_summary,
          background=excluded.background,
          ua_suggestion=excluded.ua_suggestion,
          product_benchmark=excluded.product_benchmark,
          risk_note=excluded.risk_note,
          trend_judgement=excluded.trend_judgement,
          reference_links_json=excluded.reference_links_json,
          card_markdown=excluded.card_markdown,
          push_status=COALESCE(excluded.push_status, daily_ua_push_content.push_status),
          push_response=COALESCE(excluded.push_response, daily_ua_push_content.push_response),
          updated_at_local=datetime('now','localtime');
        """
        total = 0
        for card in cards:
            if not isinstance(card, dict):
                continue
            name = str(card.get("方向名称") or "未命名方向")
            core = str(card.get("核心数据摘要") or "")
            bg = str(card.get("背景") or "")
            ua = str(card.get("UA建议") or "")
            pb = str(card.get("产品对标点") or "")
            risk = str(card.get("风险提示") or "")
            trend = str(card.get("趋势阶段判断") or "")
            refs = card.get("参考链接") or []
            if isinstance(refs, list):
                refs_json = json.dumps(refs, ensure_ascii=False)
            else:
                refs_json = json.dumps([], ensure_ascii=False)
            cur.execute(
                sql,
                (
                    target_date,
                    name,
                    core,
                    bg,
                    ua,
                    pb,
                    risk,
                    trend,
                    refs_json,
                    card_markdown,
                    bitable_app_token,
                    bitable_table_id,
                    push_status,
                    push_response,
                ),
            )
            total += 1
        conn.commit()
        return total
    finally:
        conn.close()


def update_push_status(
    target_date: str,
    bitable_app_token: str,
    bitable_table_id: str,
    status: str,
    response: str | None = None,
) -> None:
    """
    在 daily_ua_push_content 上更新 push_status/push_response。
    多方向时全部更新为同一状态。
    """
    init_db()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE daily_ua_push_content
            SET push_status = ?,
                push_response = COALESCE(?, push_response),
                updated_at_local = datetime('now','localtime')
            WHERE target_date = ?
              AND bitable_app_token = ?
              AND bitable_table_id = ?
            """,
            (status, response, target_date, bitable_app_token, bitable_table_id),
        )
        conn.commit()
    finally:
        conn.close()


def load_existing_success_analysis_by_ad_keys(ad_keys: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    按 ad_key 批量读取“已成功”的历史分析结果（跨日期复用）。
    成功定义：insight_analysis 非空且不以 [ERROR] 开头。
    返回：
      {
        ad_key: {
          "category","product","appid","ad_key","platform",
          "video_duration","video_url","pipeline_tags","analysis"
        }
      }
    """
    init_db()
    keys = [str(k).strip() for k in (ad_keys or []) if str(k).strip()]
    if not keys:
        return {}

    conn = _get_conn()
    try:
        cur = conn.cursor()
        out: Dict[str, Dict[str, Any]] = {}
        # SQLite 单次变量上限通常 999，这里分批查询
        batch_size = 400
        for i in range(0, len(keys), batch_size):
            chunk = keys[i : i + batch_size]
            placeholders = ",".join(["?"] * len(chunk))
            sql = f"""
            SELECT
              category, product, appid, ad_key, platform,
              video_duration, video_url, raw_json, insight_analysis,
              updated_at_local, id
            FROM daily_creative_insights
            WHERE ad_key IN ({placeholders})
              AND COALESCE(TRIM(insight_analysis), '') <> ''
              AND insight_analysis NOT LIKE '[ERROR]%'
            ORDER BY updated_at_local DESC, id DESC
            """
            cur.execute(sql, chunk)
            for row in cur.fetchall():
                ad_key = str(row["ad_key"] or "")
                if not ad_key or ad_key in out:
                    continue
                pipeline_tags: List[str] = []
                rj: Dict[str, Any] = {}
                raw_json = row["raw_json"]
                if raw_json:
                    try:
                        rj = json.loads(str(raw_json))
                        pt = rj.get("pipeline_tags")
                        if isinstance(pt, list):
                            pipeline_tags = [str(x) for x in pt if x]
                    except Exception:
                        pipeline_tags = []
                out[ad_key] = {
                    "category": row["category"],
                    "product": row["product"],
                    "appid": row["appid"],
                    "ad_key": ad_key,
                    "platform": row["platform"],
                    "video_duration": row["video_duration"],
                    "all_exposure_value": rj.get("all_exposure_value"),
                    "heat": rj.get("heat"),
                    "impression": rj.get("impression"),
                    "video_url": row["video_url"] or "",
                    "pipeline_tags": pipeline_tags,
                    "analysis": str(row["insight_analysis"] or ""),
                }
        return out
    finally:
        conn.close()

