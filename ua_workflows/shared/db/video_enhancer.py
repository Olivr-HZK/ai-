"""
Video Enhancer 工作流专用 SQLite 库：
- daily_creative_insights：每天的素材 + 灵感分析明细
- daily_ua_push_content：每天推送到飞书的方向卡片/UA 建议
- creative_library：跨天去重主库，多维度相似性归组
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import sqlite3
from pathlib import Path
from collections import defaultdict
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

from ua_workflows.shared.config import DATA_DIR

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
              insight_ua_suggestion TEXT,
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
        # 素材主库：跨天去重
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS creative_library (
              id INTEGER PRIMARY KEY AUTOINCREMENT,

              -- 素材唯一标识
              ad_key TEXT NOT NULL UNIQUE,

              -- 去重归组
              dedup_group_id TEXT,          -- 同组视为重复素材，格式: ahash_{hex8} 或 adkey_{ad_key[:8]}
              canonical_ad_key TEXT,        -- 组内代表（热度最高者）
              is_canonical INTEGER DEFAULT 1, -- 1=本条是组代表

              -- 视觉指纹
              image_ahash_md5 TEXT,         -- 广大大提供的感知哈希（16进制字符串）

              -- 文案指纹
              text_fingerprint TEXT,        -- sha1(normalize(title+body))

              -- 素材基本信息
              category TEXT,
              product TEXT,
              appid TEXT,
              platform TEXT,
              creative_type TEXT,           -- 'video' 或 'image'
              video_duration INTEGER,
              title TEXT,
              body TEXT,
              video_url TEXT,
              image_url TEXT,
              preview_img_url TEXT,

              -- 热度指标（取历史最高值）
              best_heat INTEGER DEFAULT 0,
              best_impression INTEGER DEFAULT 0,
              best_all_exposure_value INTEGER DEFAULT 0,

              -- 出现记录
              first_target_date TEXT,       -- 第一次出现的目标日期
              last_target_date TEXT,        -- 最近一次出现的目标日期
              appearance_count INTEGER DEFAULT 1, -- 跨天出现次数

              -- 分析结果
              insight_analysis TEXT,
              insight_ua_suggestion TEXT,

              -- 相似性备注
              dedup_reason TEXT,            -- 'exact'|'ahash'|'text'|'manual'

              created_at_local TEXT DEFAULT (datetime('now','localtime')),
              updated_at_local TEXT DEFAULT (datetime('now','localtime'))
            );
            CREATE INDEX IF NOT EXISTS idx_cl_dedup_group ON creative_library(dedup_group_id);
            CREATE INDEX IF NOT EXISTS idx_cl_ahash ON creative_library(image_ahash_md5);
            CREATE INDEX IF NOT EXISTS idx_cl_text_fp ON creative_library(text_fingerprint);
            CREATE INDEX IF NOT EXISTS idx_cl_product ON creative_library(product, category);
            CREATE INDEX IF NOT EXISTS idx_cl_target_date ON creative_library(last_target_date);
            """
        )
        # 兼容历史库：补齐新增列
        cur.execute("PRAGMA table_info(daily_creative_insights)")
        dci_cols = {str(r["name"]) for r in cur.fetchall()}
        if "insight_ua_suggestion" not in dci_cols:
            cur.execute("ALTER TABLE daily_creative_insights ADD COLUMN insight_ua_suggestion TEXT")
        cur.execute("PRAGMA table_info(creative_library)")
        cl_cols = {str(r["name"]) for r in cur.fetchall()}
        if "insight_ua_suggestion" not in cl_cols:
            cur.execute("ALTER TABLE creative_library ADD COLUMN insight_ua_suggestion TEXT")
        cur.execute("PRAGMA table_info(daily_creative_insights)")
        dci_cols = {str(r["name"]) for r in cur.fetchall()}
        cur.execute("PRAGMA table_info(creative_library)")
        cl_cols = {str(r["name"]) for r in cur.fetchall()}
        if "insight_cover_style" not in dci_cols:
            cur.execute(
                "ALTER TABLE daily_creative_insights ADD COLUMN insight_cover_style TEXT"
            )
        if "insight_cover_style" not in cl_cols:
            cur.execute("ALTER TABLE creative_library ADD COLUMN insight_cover_style TEXT")
        cur.execute("PRAGMA table_info(creative_library)")
        cl_cols3 = {str(r["name"]) for r in cur.fetchall()}
        if "cover_embedding" not in cl_cols3:
            cur.execute("ALTER TABLE creative_library ADD COLUMN cover_embedding BLOB")
        # effect_one_liner：VE 流程的特效玩法一句话
        cur.execute("PRAGMA table_info(daily_creative_insights)")
        dci_cols4 = {str(r["name"]) for r in cur.fetchall()}
        if "effect_one_liner" not in dci_cols4:
            cur.execute("ALTER TABLE daily_creative_insights ADD COLUMN effect_one_liner TEXT")
        cur.execute("PRAGMA table_info(creative_library)")
        cl_cols4 = {str(r["name"]) for r in cur.fetchall()}
        if "effect_one_liner" not in cl_cols4:
            cur.execute("ALTER TABLE creative_library ADD COLUMN effect_one_liner TEXT")
        # ad_one_liner：VE 流程的一句话说明
        cur.execute("PRAGMA table_info(daily_creative_insights)")
        dci_cols5 = {str(r["name"]) for r in cur.fetchall()}
        if "ad_one_liner" not in dci_cols5:
            cur.execute("ALTER TABLE daily_creative_insights ADD COLUMN ad_one_liner TEXT")
        cur.execute("PRAGMA table_info(creative_library)")
        cl_cols5 = {str(r["name"]) for r in cur.fetchall()}
        if "ad_one_liner" not in cl_cols5:
            cur.execute("ALTER TABLE creative_library ADD COLUMN ad_one_liner TEXT")
        # play_fingerprint / differentiator: VE prompt separates human-facing
        # selling point from machine-facing dedupe structure.
        cur.execute("PRAGMA table_info(daily_creative_insights)")
        dci_cols_play = {str(r["name"]) for r in cur.fetchall()}
        if "play_fingerprint" not in dci_cols_play:
            cur.execute("ALTER TABLE daily_creative_insights ADD COLUMN play_fingerprint TEXT")
        if "differentiator" not in dci_cols_play:
            cur.execute("ALTER TABLE daily_creative_insights ADD COLUMN differentiator TEXT")
        cur.execute("PRAGMA table_info(creative_library)")
        cl_cols_play = {str(r["name"]) for r in cur.fetchall()}
        if "play_fingerprint" not in cl_cols_play:
            cur.execute("ALTER TABLE creative_library ADD COLUMN play_fingerprint TEXT")
        if "differentiator" not in cl_cols_play:
            cur.execute("ALTER TABLE creative_library ADD COLUMN differentiator TEXT")
        play_asset_columns = {
            "play_asset_id": "TEXT",
            "play_asset_name": "TEXT",
            "play_asset_subtag_ids": "TEXT",
            "play_asset_subtag_names": "TEXT",
            "play_asset_novelty_label": "TEXT",
            "play_asset_match_source": "TEXT",
            "play_asset_classification_reason": "TEXT",
        }
        cur.execute("PRAGMA table_info(daily_creative_insights)")
        dci_cols_asset = {str(r["name"]) for r in cur.fetchall()}
        for col, typ in play_asset_columns.items():
            if col not in dci_cols_asset:
                cur.execute(f"ALTER TABLE daily_creative_insights ADD COLUMN {col} {typ}")
        cur.execute("PRAGMA table_info(creative_library)")
        cl_cols_asset = {str(r["name"]) for r in cur.fetchall()}
        for col, typ in play_asset_columns.items():
            if col not in cl_cols_asset:
                cur.execute(f"ALTER TABLE creative_library ADD COLUMN {col} {typ}")
        # 玩法 embedding + duplicate evidence for VE dedupe calibration.
        cur.execute("PRAGMA table_info(creative_library)")
        cl_cols6 = {str(r["name"]) for r in cur.fetchall()}
        if "effect_one_liner_embedding" not in cl_cols6:
            cur.execute("ALTER TABLE creative_library ADD COLUMN effect_one_liner_embedding BLOB")
        cur.execute("PRAGMA table_info(daily_creative_insights)")
        dci_cols6 = {str(r["name"]) for r in cur.fetchall()}
        if "effect_embedding_duplicate_json" not in dci_cols6:
            cur.execute(
                "ALTER TABLE daily_creative_insights ADD COLUMN effect_embedding_duplicate_json TEXT"
            )
        if "embedding_duplicate_candidate_json" not in dci_cols6:
            cur.execute(
                "ALTER TABLE daily_creative_insights ADD COLUMN embedding_duplicate_candidate_json TEXT"
            )
        # Analysis-time filter metadata used by daily reports and sync replay.
        cur.execute("PRAGMA table_info(daily_creative_insights)")
        dci_cols_filter = {str(r["name"]) for r in cur.fetchall()}
        if "material_tags" not in dci_cols_filter:
            cur.execute("ALTER TABLE daily_creative_insights ADD COLUMN material_tags TEXT")
        if "exclude_from_bitable" not in dci_cols_filter:
            cur.execute("ALTER TABLE daily_creative_insights ADD COLUMN exclude_from_bitable INTEGER")
        if "exclude_from_cluster" not in dci_cols_filter:
            cur.execute("ALTER TABLE daily_creative_insights ADD COLUMN exclude_from_cluster INTEGER")
        if "launched_effect_match_json" not in dci_cols_filter:
            cur.execute("ALTER TABLE daily_creative_insights ADD COLUMN launched_effect_match_json TEXT")
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 工具函数：感知哈希汉明距离
# ---------------------------------------------------------------------------

def _ahash_hamming(h1: str, h2: str) -> int:
    """计算两个 16 进制感知哈希字符串的汉明距离。长度不等则返回 999。"""
    if not h1 or not h2 or len(h1) != len(h2):
        return 999
    try:
        b1 = int(h1, 16)
        b2 = int(h2, 16)
        return bin(b1 ^ b2).count("1")
    except ValueError:
        return 999


def _text_fingerprint(title: str, body: str) -> str:
    """对 title+body 做归一化后取 sha1，用于文案去重。空文案返回空字符串。"""
    raw = " ".join((title or "").split()) + " " + " ".join((body or "").split())
    raw = raw.strip().lower()
    if not raw:
        return ""
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _allow_text_only_dedup(appid: str, ahash: str, media_url: str, text_fp: str) -> bool:
    """
    文案去重兜底规则（避免"同文案不同素材"误判）：
    - 必须有 appid（仅同 app 内比较）
    - 必须有 text_fingerprint
    - 且当前素材没有可用视觉/媒体特征（无 ahash、无媒体 URL）

    环境变量 TEXT_FINGERPRINT_DEDUP_ENABLED（默认 0/关闭）可全局开关文案去重。
    """
    env_val = (os.getenv("TEXT_FINGERPRINT_DEDUP_ENABLED") or "0").strip().lower()
    if env_val in ("0", "false", "no", "off", ""):
        return False
    if not str(appid or "").strip():
        return False
    if not str(text_fp or "").strip():
        return False
    if str(ahash or "").strip():
        return False
    if str(media_url or "").strip():
        return False
    return True

def _pick_video_url_from_raw(creative: Dict[str, Any]) -> str:
    if creative.get("video_url"):
        return str(creative["video_url"])
    for r in creative.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("video_url"):
            return str(r["video_url"])
    return ""


def _pick_image_url_from_raw(creative: Dict[str, Any]) -> str:
    for r in creative.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("image_url") and not r.get("video_url"):
            return str(r["image_url"])
    if creative.get("preview_img_url"):
        return str(creative["preview_img_url"])
    return ""


# ---------------------------------------------------------------------------
# creative_library：写入与去重逻辑
# ---------------------------------------------------------------------------

AHASH_HAMMING_THRESHOLD = 8   # 汉明距离 <= 此值视为视觉相同
AHASH_LOOKUP_LIMIT = 2000      # 查询候选时最多取多少条（按热度降序）


def upsert_creative_library(
    target_date: str,
    raw_payload: Dict[str, Any],
    analysis_by_ad: Optional[Dict[str, Any]] = None,
) -> Tuple[int, int]:
    """
    将 raw_payload 中的素材写入 creative_library，执行多维去重归组。

    去重优先级：
      1. ad_key 完全匹配 → 直接更新（exact）
      2. image_ahash_md5 汉明距离 <= AHASH_HAMMING_THRESHOLD → 视觉相同（ahash）
      3. text_fingerprint 兜底匹配（仅无 ahash/无媒体 URL、且同 appid）→ 文案完全相同（text）
      4. 无匹配 → 新建，自立为组代表

    返回 (upserted, grouped) - 写入条数、发现重复并归组的条数
    """
    init_db()
    if analysis_by_ad is None:
        analysis_by_ad = {}

    items = raw_payload.get("items") or []
    if not isinstance(items, list):
        return 0, 0

    conn = _get_conn()
    upserted = 0
    grouped = 0
    try:
        cur = conn.cursor()

        # 预加载库中所有 ahash（用于汉明距离比对）
        cur.execute(
            "SELECT ad_key, image_ahash_md5, dedup_group_id FROM creative_library "
            "WHERE image_ahash_md5 IS NOT NULL AND image_ahash_md5 != '' "
            "ORDER BY best_impression DESC LIMIT ?",
            (AHASH_LOOKUP_LIMIT,),
        )
        existing_ahash_rows: List[Dict[str, Any]] = [
            {"ad_key": r["ad_key"], "image_ahash_md5": r["image_ahash_md5"], "dedup_group_id": r["dedup_group_id"]}
            for r in cur.fetchall()
        ]

        for item in items:
            if not isinstance(item, dict):
                continue
            creative = item.get("creative") or {}
            if not isinstance(creative, dict):
                continue
            ad_key = str(creative.get("ad_key") or "").strip()
            if not ad_key:
                continue

            # --- 提取字段 ---
            ahash = str(creative.get("image_ahash_md5") or "").strip()
            title = str(creative.get("title") or "").strip()
            body = str(creative.get("body") or "").strip()
            text_fp = _text_fingerprint(title, body)
            video_url = _pick_video_url_from_raw(creative)
            image_url = _pick_image_url_from_raw(creative) if not video_url else ""
            media_url = video_url or image_url
            creative_type = "video" if video_url else ("image" if image_url else "unknown")
            preview_img_url = str(creative.get("preview_img_url") or "")
            heat = int(creative.get("heat") or 0)
            impression = int(creative.get("impression") or 0)
            all_exp = int(creative.get("all_exposure_value") or 0)
            analysis_raw = analysis_by_ad.get(ad_key, "")
            if isinstance(analysis_raw, dict):
                analysis = str(analysis_raw.get("analysis") or "")
                ua_single = str(analysis_raw.get("ua_suggestion_single") or "")
                effect_one_liner = str(analysis_raw.get("effect_one_liner") or "")
                ad_one_liner = str(analysis_raw.get("ad_one_liner") or "")
                play_fingerprint = str(analysis_raw.get("play_fingerprint") or "")
                differentiator = str(analysis_raw.get("differentiator") or "")
                play_asset_id = str(analysis_raw.get("play_asset_id") or "")
                play_asset_name = str(analysis_raw.get("play_asset_name") or "")
                play_asset_subtag_ids = str(analysis_raw.get("play_asset_subtag_ids") or "")
                play_asset_subtag_names = str(analysis_raw.get("play_asset_subtag_names") or "")
                play_asset_novelty_label = str(analysis_raw.get("play_asset_novelty_label") or "")
                play_asset_match_source = str(analysis_raw.get("play_asset_match_source") or "")
                play_asset_classification_reason = str(analysis_raw.get("play_asset_classification_reason") or "")
            else:
                analysis = str(analysis_raw or "")
                ua_single = ""
                effect_one_liner = ""
                ad_one_liner = ""
                play_fingerprint = ""
                differentiator = ""
                play_asset_id = ""
                play_asset_name = ""
                play_asset_subtag_ids = ""
                play_asset_subtag_names = ""
                play_asset_novelty_label = ""
                play_asset_match_source = ""
                play_asset_classification_reason = ""
            appid = str(item.get("appid") or "").strip()
            cs = item.get("cover_style")
            if isinstance(cs, dict):
                cover_style_str = json.dumps(cs, ensure_ascii=False)
            elif cs is not None and str(cs).strip():
                cover_style_str = str(cs).strip()
            else:
                cover_style_str = ""

            # --- 检查是否已存在（完全匹配） ---
            cur.execute(
                "SELECT id, dedup_group_id, canonical_ad_key, appearance_count, "
                "best_heat, best_impression, best_all_exposure_value, last_target_date "
                "FROM creative_library WHERE ad_key = ?",
                (ad_key,),
            )
            existing = cur.fetchone()

            if existing:
                new_heat = max(heat, int(existing["best_heat"] or 0))
                new_imp = max(impression, int(existing["best_impression"] or 0))
                new_exp = max(all_exp, int(existing["best_all_exposure_value"] or 0))
                # 仅当 target_date 真正变化时才递增 appearance_count
                prev_td = str(existing["last_target_date"] or "")
                inc = 1 if prev_td != target_date else 0
                cur.execute(
                    """UPDATE creative_library SET
                         last_target_date = ?,
                         appearance_count = appearance_count + ?,
                         best_heat = ?,
                         best_impression = ?,
                         best_all_exposure_value = ?,
                         insight_analysis = CASE
                           WHEN COALESCE(TRIM(?), '') <> '' AND ? NOT LIKE '[ERROR]%'
                           THEN ? ELSE insight_analysis END,
                         insight_ua_suggestion = CASE
                           WHEN COALESCE(TRIM(?), '') <> '' AND ? NOT LIKE '[ERROR]%'
                           THEN ? ELSE insight_ua_suggestion END,
                         insight_cover_style = CASE
                           WHEN COALESCE(TRIM(?), '') <> ''
                           THEN ? ELSE insight_cover_style END,
                         effect_one_liner = CASE
                           WHEN COALESCE(TRIM(?), '') <> ''
                           THEN ? ELSE effect_one_liner END,
                         ad_one_liner = CASE
                           WHEN COALESCE(TRIM(?), '') <> ''
                           THEN ? ELSE ad_one_liner END,
                         play_fingerprint = CASE
                           WHEN COALESCE(TRIM(?), '') <> ''
                           THEN ? ELSE play_fingerprint END,
                         differentiator = CASE
                           WHEN COALESCE(TRIM(?), '') <> ''
                           THEN ? ELSE differentiator END,
                         play_asset_id = CASE
                           WHEN COALESCE(TRIM(?), '') <> ''
                           THEN ? ELSE play_asset_id END,
                         play_asset_name = CASE
                           WHEN COALESCE(TRIM(?), '') <> ''
                           THEN ? ELSE play_asset_name END,
                         play_asset_subtag_ids = CASE
                           WHEN COALESCE(TRIM(?), '') <> ''
                           THEN ? ELSE play_asset_subtag_ids END,
                         play_asset_subtag_names = CASE
                           WHEN COALESCE(TRIM(?), '') <> ''
                           THEN ? ELSE play_asset_subtag_names END,
                         play_asset_novelty_label = CASE
                           WHEN COALESCE(TRIM(?), '') <> ''
                           THEN ? ELSE play_asset_novelty_label END,
                         play_asset_match_source = CASE
                           WHEN COALESCE(TRIM(?), '') <> ''
                           THEN ? ELSE play_asset_match_source END,
                         play_asset_classification_reason = CASE
                           WHEN COALESCE(TRIM(?), '') <> ''
                           THEN ? ELSE play_asset_classification_reason END,
                         updated_at_local = datetime('now','localtime')
                       WHERE ad_key = ?""",
                    (target_date, inc, new_heat, new_imp, new_exp,
                     analysis, analysis, analysis,
                     ua_single, ua_single, ua_single,
                     cover_style_str, cover_style_str,
                     effect_one_liner, effect_one_liner,
                     ad_one_liner, ad_one_liner,
                     play_fingerprint, play_fingerprint,
                     differentiator, differentiator,
                     play_asset_id, play_asset_id,
                     play_asset_name, play_asset_name,
                     play_asset_subtag_ids, play_asset_subtag_ids,
                     play_asset_subtag_names, play_asset_subtag_names,
                     play_asset_novelty_label, play_asset_novelty_label,
                     play_asset_match_source, play_asset_match_source,
                     play_asset_classification_reason, play_asset_classification_reason,
                     ad_key),
                )
                upserted += 1
                conn.commit()
                continue

            # --- 新素材：查找相似者，确定归组 ---
            dedup_group_id: str = ""
            canonical: str = ad_key
            dedup_reason: str = "new"

            # 维度2：ahash 汉明距离
            if ahash:
                best_dist = 999
                best_match_ad_key = ""
                best_match_group = ""
                for row in existing_ahash_rows:
                    dist = _ahash_hamming(ahash, str(row["image_ahash_md5"] or ""))
                    if dist <= AHASH_HAMMING_THRESHOLD and dist < best_dist:
                        best_dist = dist
                        best_match_ad_key = str(row["ad_key"])
                        best_match_group = str(row["dedup_group_id"] or "")
                if best_match_ad_key:
                    dedup_group_id = best_match_group or f"ahash_{ahash[:8]}"
                    # 组代表取热度最高者
                    cur.execute(
                        "SELECT ad_key, best_impression FROM creative_library "
                        "WHERE dedup_group_id = ? ORDER BY best_impression DESC LIMIT 1",
                        (dedup_group_id,),
                    )
                    top = cur.fetchone()
                    canonical = str(top["ad_key"]) if top and int(top["best_impression"] or 0) >= impression else ad_key
                    dedup_reason = f"ahash(dist={best_dist})"
                    grouped += 1

            # 维度3：文案指纹兜底（仅低信息素材，且同 appid）
            if not dedup_group_id and _allow_text_only_dedup(appid, ahash, media_url, text_fp):
                cur.execute(
                    "SELECT ad_key, dedup_group_id, best_impression FROM creative_library "
                    "WHERE text_fingerprint = ? "
                    "  AND appid = ? "
                    "  AND COALESCE(image_ahash_md5, '') = '' "
                    "  AND COALESCE(video_url, '') = '' "
                    "  AND COALESCE(image_url, '') = '' "
                    "LIMIT 1",
                    (text_fp, appid),
                )
                text_match = cur.fetchone()
                if text_match:
                    dedup_group_id = str(text_match["dedup_group_id"] or f"text_{text_fp[:8]}")
                    canonical_imp = int(text_match["best_impression"] or 0)
                    canonical = str(text_match["ad_key"]) if canonical_imp >= impression else ad_key
                    dedup_reason = "text"
                    grouped += 1

            # 无相似 → 自立新组
            if not dedup_group_id:
                dedup_group_id = f"adkey_{ad_key[:8]}"
                canonical = ad_key
                dedup_reason = "new"

            is_canonical = 1 if canonical == ad_key else 0

            # 新素材热度超过原组代表 → 清零旧代表，全组指向新代表
            if canonical == ad_key and dedup_reason != "new":
                cur.execute(
                    "UPDATE creative_library SET is_canonical = 0, updated_at_local = datetime('now','localtime') "
                    "WHERE dedup_group_id = ? AND is_canonical = 1",
                    (dedup_group_id,),
                )
                cur.execute(
                    "UPDATE creative_library SET canonical_ad_key = ?, updated_at_local = datetime('now','localtime') "
                    "WHERE dedup_group_id = ?",
                    (ad_key, dedup_group_id),
                )

            cur.execute(
                """INSERT INTO creative_library (
                     ad_key, dedup_group_id, canonical_ad_key, is_canonical,
                     image_ahash_md5, text_fingerprint,
                     category, product, appid, platform,
                     creative_type, video_duration,
                     title, body, video_url, image_url, preview_img_url,
                     best_heat, best_impression, best_all_exposure_value,
                     first_target_date, last_target_date, appearance_count,
                     insight_analysis, insight_ua_suggestion, insight_cover_style, dedup_reason,
                     effect_one_liner, ad_one_liner,
                     play_fingerprint, differentiator,
                     play_asset_id, play_asset_name, play_asset_subtag_ids, play_asset_subtag_names,
                     play_asset_novelty_label, play_asset_match_source, play_asset_classification_reason,
                     created_at_local, updated_at_local
                   ) VALUES (
                     ?, ?, ?, ?,
                     ?, ?,
                     ?, ?, ?, ?,
                     ?, ?,
                     ?, ?, ?, ?, ?,
                     ?, ?, ?,
                     ?, ?, 1,
                     ?, ?, ?, ?,
                     ?, ?,
                     ?, ?,
                     ?, ?, ?, ?,
                     ?, ?, ?,
                     datetime('now','localtime'), datetime('now','localtime')
                   )""",
                (
                    ad_key, dedup_group_id, canonical, is_canonical,
                    ahash, text_fp,
                    item.get("category"), item.get("product"), item.get("appid"),
                    creative.get("platform"),
                    creative_type, int(creative.get("video_duration") or 0),
                    title, body, video_url, image_url, preview_img_url,
                    heat, impression, all_exp,
                    target_date, target_date,
                    analysis, ua_single, cover_style_str, dedup_reason,
                    effect_one_liner, ad_one_liner,
                    play_fingerprint, differentiator,
                    play_asset_id, play_asset_name, play_asset_subtag_ids, play_asset_subtag_names,
                    play_asset_novelty_label, play_asset_match_source, play_asset_classification_reason,
                ),
            )
            upserted += 1
            # 将新条目加入内存 ahash 缓存，供本批次后续条目比对
            if ahash:
                existing_ahash_rows.append({"ad_key": ad_key, "image_ahash_md5": ahash, "dedup_group_id": dedup_group_id})
            conn.commit()

    finally:
        conn.close()
    return upserted, grouped


def query_dedup_summary(target_date: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    查询去重摘要：每个 dedup_group 的代表素材 + 重复条数。
    target_date 不为空时，只看该日期出现过的 group。
    """
    init_db()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        if target_date:
            cur.execute(
                """
                SELECT
                  cl.dedup_group_id,
                  cl.canonical_ad_key,
                  cl.creative_type,
                  cl.product,
                  cl.platform,
                  cl.video_url,
                  cl.image_url,
                  cl.title,
                  cl.dedup_reason,
                  COUNT(*) AS group_size,
                  MAX(cl.best_impression) AS top_impression
                FROM creative_library cl
                WHERE cl.dedup_group_id IN (
                  SELECT DISTINCT dedup_group_id FROM creative_library
                  WHERE last_target_date = ? OR first_target_date = ?
                )
                GROUP BY cl.dedup_group_id
                ORDER BY group_size DESC, top_impression DESC
                """,
                (target_date, target_date),
            )
        else:
            cur.execute(
                """
                SELECT
                  dedup_group_id,
                  canonical_ad_key,
                  creative_type,
                  product,
                  platform,
                  video_url,
                  image_url,
                  title,
                  dedup_reason,
                  COUNT(*) AS group_size,
                  MAX(best_impression) AS top_impression
                FROM creative_library
                GROUP BY dedup_group_id
                ORDER BY group_size DESC, top_impression DESC
                """
            )
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


UPSERT_DAILY_CREATIVE_INSIGHT_SQL = """
        INSERT INTO daily_creative_insights (
          crawl_date, target_date, category, product, appid,
          ad_key, platform, video_url, preview_img_url, video_duration,
          first_seen, created_at, last_seen,
          heat, all_exposure_value, impression,
          raw_json, insight_analysis, insight_ua_suggestion, insight_cover_style,
          effect_one_liner, ad_one_liner,
          play_fingerprint, differentiator,
          play_asset_id, play_asset_name, play_asset_subtag_ids, play_asset_subtag_names,
          play_asset_novelty_label, play_asset_match_source, play_asset_classification_reason,
          effect_embedding_duplicate_json, embedding_duplicate_candidate_json,
          material_tags, exclude_from_bitable, exclude_from_cluster, launched_effect_match_json,
          created_at_local, updated_at_local
        ) VALUES (
          ?, ?, ?, ?, ?,
          ?, ?, ?, ?, ?,
          ?, ?, ?,
          ?, ?, ?,
          ?, ?, ?, ?,
          ?, ?,
          ?, ?,
          ?, ?, ?, ?,
          ?, ?, ?,
          ?, ?,
          ?, ?, ?, ?,
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
          insight_ua_suggestion=CASE
            WHEN COALESCE(TRIM(excluded.insight_ua_suggestion), '') <> ''
                 AND excluded.insight_ua_suggestion NOT LIKE '[ERROR]%'
            THEN excluded.insight_ua_suggestion
            ELSE daily_creative_insights.insight_ua_suggestion
          END,
          insight_cover_style=CASE
            WHEN COALESCE(TRIM(excluded.insight_cover_style), '') <> ''
            THEN excluded.insight_cover_style
            ELSE daily_creative_insights.insight_cover_style
          END,
          effect_one_liner=CASE
            WHEN COALESCE(TRIM(excluded.effect_one_liner), '') <> ''
            THEN excluded.effect_one_liner
            ELSE daily_creative_insights.effect_one_liner
          END,
          ad_one_liner=CASE
            WHEN COALESCE(TRIM(excluded.ad_one_liner), '') <> ''
            THEN excluded.ad_one_liner
            ELSE daily_creative_insights.ad_one_liner
          END,
          play_fingerprint=CASE
            WHEN COALESCE(TRIM(excluded.play_fingerprint), '') <> ''
            THEN excluded.play_fingerprint
            ELSE daily_creative_insights.play_fingerprint
          END,
          differentiator=CASE
            WHEN COALESCE(TRIM(excluded.differentiator), '') <> ''
            THEN excluded.differentiator
            ELSE daily_creative_insights.differentiator
          END,
          play_asset_id=CASE
            WHEN COALESCE(TRIM(excluded.play_asset_id), '') <> ''
            THEN excluded.play_asset_id
            ELSE daily_creative_insights.play_asset_id
          END,
          play_asset_name=CASE
            WHEN COALESCE(TRIM(excluded.play_asset_name), '') <> ''
            THEN excluded.play_asset_name
            ELSE daily_creative_insights.play_asset_name
          END,
          play_asset_subtag_ids=CASE
            WHEN COALESCE(TRIM(excluded.play_asset_subtag_ids), '') <> ''
            THEN excluded.play_asset_subtag_ids
            ELSE daily_creative_insights.play_asset_subtag_ids
          END,
          play_asset_subtag_names=CASE
            WHEN COALESCE(TRIM(excluded.play_asset_subtag_names), '') <> ''
            THEN excluded.play_asset_subtag_names
            ELSE daily_creative_insights.play_asset_subtag_names
          END,
          play_asset_novelty_label=CASE
            WHEN COALESCE(TRIM(excluded.play_asset_novelty_label), '') <> ''
            THEN excluded.play_asset_novelty_label
            ELSE daily_creative_insights.play_asset_novelty_label
          END,
          play_asset_match_source=CASE
            WHEN COALESCE(TRIM(excluded.play_asset_match_source), '') <> ''
            THEN excluded.play_asset_match_source
            ELSE daily_creative_insights.play_asset_match_source
          END,
          play_asset_classification_reason=CASE
            WHEN COALESCE(TRIM(excluded.play_asset_classification_reason), '') <> ''
            THEN excluded.play_asset_classification_reason
            ELSE daily_creative_insights.play_asset_classification_reason
          END,
          effect_embedding_duplicate_json=excluded.effect_embedding_duplicate_json,
          embedding_duplicate_candidate_json=excluded.embedding_duplicate_candidate_json,
          material_tags=CASE
            WHEN COALESCE(TRIM(excluded.material_tags), '') <> ''
            THEN excluded.material_tags
            ELSE daily_creative_insights.material_tags
          END,
          exclude_from_bitable=CASE
            WHEN excluded.exclude_from_bitable IS NOT NULL
            THEN excluded.exclude_from_bitable
            ELSE daily_creative_insights.exclude_from_bitable
          END,
          exclude_from_cluster=CASE
            WHEN excluded.exclude_from_cluster IS NOT NULL
            THEN excluded.exclude_from_cluster
            ELSE daily_creative_insights.exclude_from_cluster
          END,
          launched_effect_match_json=CASE
            WHEN COALESCE(TRIM(excluded.launched_effect_match_json), '') <> ''
            THEN excluded.launched_effect_match_json
            ELSE daily_creative_insights.launched_effect_match_json
          END,
          updated_at_local=datetime('now','localtime');
        """


def _json_for_optional(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        if not value:
            return ""
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    text = str(value).strip()
    return text


def _params_tuple_for_daily_creative_insight(
    crawl_date: Any,
    target_date: str,
    item: Dict[str, Any],
    analysis_raw: Any,
) -> Optional[Tuple[Any, ...]]:
    if not isinstance(item, dict):
        return None
    category = item.get("category")
    product = item.get("product")
    appid = item.get("appid")
    creative = item.get("creative") or {}
    if not isinstance(creative, dict):
        return None
    ad_key = str(creative.get("ad_key") or "")
    if not ad_key:
        return None
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
    if isinstance(analysis_raw, dict):
        insight = str(analysis_raw.get("analysis") or "")
        ua_single = str(analysis_raw.get("ua_suggestion_single") or "")
        effect_one_liner = str(analysis_raw.get("effect_one_liner") or "")
        ad_one_liner = str(analysis_raw.get("ad_one_liner") or "")
        play_fingerprint = str(analysis_raw.get("play_fingerprint") or "")
        differentiator = str(analysis_raw.get("differentiator") or "")
        play_asset_id = str(analysis_raw.get("play_asset_id") or "")
        play_asset_name = str(analysis_raw.get("play_asset_name") or "")
        play_asset_subtag_ids = str(analysis_raw.get("play_asset_subtag_ids") or "")
        play_asset_subtag_names = str(analysis_raw.get("play_asset_subtag_names") or "")
        play_asset_novelty_label = str(analysis_raw.get("play_asset_novelty_label") or "")
        play_asset_match_source = str(analysis_raw.get("play_asset_match_source") or "")
        play_asset_classification_reason = str(analysis_raw.get("play_asset_classification_reason") or "")
        effect_embedding_duplicate_json = _json_for_optional(
            analysis_raw.get("effect_embedding_duplicate_match")
            or analysis_raw.get("effect_embedding_duplicate")
        )
        embedding_duplicate_candidate_json = _json_for_optional(
            analysis_raw.get("embedding_duplicate_candidate")
        )
        tags = analysis_raw.get("material_tags")
        if isinstance(tags, list) and tags:
            material_tags_json = json.dumps([str(t) for t in tags if str(t).strip()], ensure_ascii=False)
        else:
            material_tags_json = ""
        exclude_from_bitable = 1 if bool(analysis_raw.get("exclude_from_bitable")) else 0
        exclude_from_cluster = 1 if bool(analysis_raw.get("exclude_from_cluster")) else 0
        launched_effect_match_json = _json_for_optional(analysis_raw.get("launched_effect_match"))
    else:
        insight = str(analysis_raw or "")
        ua_single = ""
        effect_one_liner = ""
        ad_one_liner = ""
        play_fingerprint = ""
        differentiator = ""
        play_asset_id = ""
        play_asset_name = ""
        play_asset_subtag_ids = ""
        play_asset_subtag_names = ""
        play_asset_novelty_label = ""
        play_asset_match_source = ""
        play_asset_classification_reason = ""
        effect_embedding_duplicate_json = ""
        embedding_duplicate_candidate_json = ""
        material_tags_json = ""
        exclude_from_bitable = None
        exclude_from_cluster = None
        launched_effect_match_json = ""
    cs = item.get("cover_style")
    if isinstance(cs, dict):
        cover_style_str = json.dumps(cs, ensure_ascii=False)
    elif cs is not None and str(cs).strip():
        cover_style_str = str(cs).strip()
    else:
        cover_style_str = ""
    return (
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
        ua_single,
        cover_style_str,
        effect_one_liner,
        ad_one_liner,
        play_fingerprint,
        differentiator,
        play_asset_id,
        play_asset_name,
        play_asset_subtag_ids,
        play_asset_subtag_names,
        play_asset_novelty_label,
        play_asset_match_source,
        play_asset_classification_reason,
        effect_embedding_duplicate_json,
        embedding_duplicate_candidate_json,
        material_tags_json,
        exclude_from_bitable,
        exclude_from_cluster,
        launched_effect_match_json,
    )


def upsert_single_cover_style_insight(
    target_date: str,
    crawl_date: Any,
    item: Dict[str, Any],
) -> bool:
    """
    仅写入/更新 insight_cover_style；analysis / ua 传空串，不覆盖库里已有灵感分析。
    供 cover_style_intraday 每抽完一条封面风格即入库。
    """
    return upsert_single_daily_creative_insight(
        target_date,
        crawl_date,
        item,
        {"analysis": "", "ua_suggestion_single": ""},
    )


def upsert_single_daily_creative_insight(
    target_date: str,
    crawl_date: Any,
    item: Dict[str, Any],
    analysis_raw: Any,
) -> bool:
    """
    单条写入/更新 daily_creative_insights（供 analyze_video_from_raw_json 每分析完一条即入库）。
    返回 True 表示执行了一条 UPSERT。
    """
    init_db()
    params = _params_tuple_for_daily_creative_insight(
        crawl_date, target_date, item, analysis_raw
    )
    if params is None:
        return False
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(UPSERT_DAILY_CREATIVE_INSIGHT_SQL, params)
        conn.commit()
        return True
    finally:
        conn.close()


def prune_daily_creative_insights_not_in_raw(
    target_date: str,
    raw_payload: Dict[str, Any],
) -> int:
    """
    删除 daily_creative_insights 中 target_date 下、ad_key 不在当前 raw_payload.items 里的行。
    用于「先全量抓取入库、再封面去重缩条」时，避免库里残留已剔除素材。
    """
    items = raw_payload.get("items") or []
    if not isinstance(items, list):
        return 0
    keep: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        c = item.get("creative") or {}
        if not isinstance(c, dict):
            continue
        ak = str(c.get("ad_key") or "").strip()
        if ak:
            keep.add(ak)
    if not keep:
        return 0
    init_db()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        placeholders = ",".join("?" * len(keep))
        cur.execute(
            f"""
            DELETE FROM daily_creative_insights
            WHERE target_date = ?
              AND ad_key NOT IN ({placeholders})
            """,
            (target_date, *sorted(keep)),
        )
        n = cur.rowcount if cur.rowcount is not None else 0
        conn.commit()
        return int(n)
    finally:
        conn.close()


def upsert_daily_creative_insights(
    target_date: str,
    raw_payload: Dict[str, Any],
    analysis_by_ad: Dict[str, Any],
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
        n = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            creative = item.get("creative") or {}
            if not isinstance(creative, dict):
                continue
            ad_key = str(creative.get("ad_key") or "")
            if not ad_key:
                continue
            analysis_raw = analysis_by_ad.get(ad_key) or ""
            params = _params_tuple_for_daily_creative_insight(
                crawl_date, target_date, item, analysis_raw
            )
            if params is None:
                continue
            cur.execute(UPSERT_DAILY_CREATIVE_INSIGHT_SQL, params)
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


def should_persist_suggestion_to_push_table(suggestion_obj: Dict[str, Any] | None) -> bool:
    """
    聚类/方向建议是否应写入 daily_ua_push_content。
    LLM 失败、skipped_llm、无方向卡片或卡片无实质内容时返回 False。
    """
    if not isinstance(suggestion_obj, dict):
        return False
    if suggestion_obj.get("skipped_llm"):
        return False
    err = suggestion_obj.get("llm_error")
    if err is not None and str(err).strip():
        return False
    s = suggestion_obj.get("suggestion") or suggestion_obj
    if not isinstance(s, dict):
        return False
    cards = s.get("方向卡片")
    if not isinstance(cards, list) or len(cards) == 0:
        return False

    def _card_has_body(card: Any) -> bool:
        if not isinstance(card, dict):
            return False
        for k in ("方向名称", "背景", "UA建议", "产品对标点", "风险提示"):
            if str(card.get(k) or "").strip():
                return True
        refs = card.get("参考链接") or []
        return isinstance(refs, list) and any(str(x or "").strip() for x in refs)

    return any(_card_has_body(c) for c in cards)


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
    if not should_persist_suggestion_to_push_table(suggestion_obj):
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
              video_duration, video_url, raw_json, insight_analysis, insight_ua_suggestion,
              effect_one_liner, ad_one_liner, play_fingerprint, differentiator,
              play_asset_id, play_asset_name, play_asset_subtag_ids, play_asset_subtag_names,
              play_asset_novelty_label, play_asset_match_source, play_asset_classification_reason,
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
                        rj = {}
                        pipeline_tags = []
                vu = str(row["video_url"] or "").strip()
                pu = str(rj.get("preview_img_url") or "").strip()
                iu = ""
                if not vu:
                    for rr in rj.get("resource_urls") or []:
                        if isinstance(rr, dict) and rr.get("image_url") and not str(rr.get("video_url") or "").strip():
                            iu = str(rr["image_url"])
                            break
                    if not iu and pu:
                        iu = pu
                ct = "image" if (not vu and iu) else "video"
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
                    "creative_type": ct,
                    "video_url": vu,
                    "image_url": iu if ct == "image" else "",
                    "preview_img_url": pu if ct == "video" else "",
                    "pipeline_tags": pipeline_tags,
                    "analysis": str(row["insight_analysis"] or ""),
                    "ua_suggestion_single": str(row["insight_ua_suggestion"] or ""),
                    "effect_one_liner": str(row["effect_one_liner"] or ""),
                    "ad_one_liner": str(row["ad_one_liner"] or ""),
                    "play_fingerprint": str(row["play_fingerprint"] or ""),
                    "differentiator": str(row["differentiator"] or ""),
                    "play_asset_id": str(row["play_asset_id"] or ""),
                    "play_asset_name": str(row["play_asset_name"] or ""),
                    "play_asset_subtag_ids": str(row["play_asset_subtag_ids"] or ""),
                    "play_asset_subtag_names": str(row["play_asset_subtag_names"] or ""),
                    "play_asset_novelty_label": str(row["play_asset_novelty_label"] or ""),
                    "play_asset_match_source": str(row["play_asset_match_source"] or ""),
                    "play_asset_classification_reason": str(row["play_asset_classification_reason"] or ""),
                }
        return out
    finally:
        conn.close()


def load_existing_cover_style_by_ad_keys_for_date(
    target_date: str,
    ad_keys: List[str],
) -> Dict[str, Dict[str, Any]]:
    """
    指定 target_date 下已有非空 insight_cover_style 的素材（用于重跑时跳过封面多模态）。
    返回 ad_key -> 解析后的 dict（与 item['cover_style'] 结构一致）。
    """
    init_db()
    td = (target_date or "").strip()
    if not td:
        return {}
    keys = [str(k).strip() for k in (ad_keys or []) if str(k).strip()]
    if not keys:
        return {}

    conn = _get_conn()
    try:
        cur = conn.cursor()
        out: Dict[str, Dict[str, Any]] = {}
        batch_size = 400
        for i in range(0, len(keys), batch_size):
            chunk = keys[i : i + batch_size]
            placeholders = ",".join(["?"] * len(chunk))
            sql = f"""
            SELECT ad_key, insight_cover_style
            FROM daily_creative_insights
            WHERE target_date = ?
              AND ad_key IN ({placeholders})
              AND COALESCE(TRIM(insight_cover_style), '') <> ''
            """
            cur.execute(sql, (td, *chunk))
            for row in cur.fetchall():
                ak = str(row["ad_key"] or "").strip()
                raw = str(row["insight_cover_style"] or "").strip()
                if not ak or ak in out:
                    continue
                try:
                    obj = json.loads(raw)
                    if isinstance(obj, dict):
                        out[ak] = obj
                except Exception:
                    continue
        return out
    finally:
        conn.close()


def load_cover_style_rows_for_date_grouped_by_appid(
    target_date: str,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    某日各 appid 下已有非空 insight_cover_style 的素材（用于跨日封面去重）。
    返回 appid -> [{ ad_key, style_json, exposure }]，exposure 取 daily_creative_insights.all_exposure_value。
    """
    init_db()
    td = (target_date or "").strip()
    if not td:
        return {}

    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ad_key, appid, insight_cover_style, COALESCE(all_exposure_value, 0) AS exp
            FROM daily_creative_insights
            WHERE target_date = ?
              AND COALESCE(TRIM(insight_cover_style), '') <> ''
              AND COALESCE(TRIM(ad_key), '') <> ''
              AND COALESCE(TRIM(appid), '') <> ''
            """,
            (td,),
        )
        by_app: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in cur.fetchall():
            ak = str(row["ad_key"] or "").strip()
            aid = str(row["appid"] or "").strip()
            raw = str(row["insight_cover_style"] or "").strip()
            if not ak or not aid or not raw:
                continue
            try:
                obj = json.loads(raw)
                if not isinstance(obj, dict):
                    continue
            except Exception:
                continue
            by_app[aid].append(
                {
                    "ad_key": ak,
                    "style_json": obj,
                    "exposure": int(row["exp"] or 0),
                }
            )
        return dict(by_app)
    finally:
        conn.close()


def load_cover_style_rows_for_dates_grouped_by_appid(
    dates: List[str],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    多个日历日合并：各日非空 insight_cover_style，按 appid 分组（用于 CLIP 历史参加聚类）。
    """
    dlist = [d.strip()[:10] for d in (dates or []) if (d or "").strip()]
    if not dlist:
        return {}
    init_db()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        ph = ",".join(["?"] * len(dlist))
        cur.execute(
            f"""
            SELECT target_date, ad_key, appid, insight_cover_style, COALESCE(all_exposure_value, 0) AS exp
            FROM daily_creative_insights
            WHERE target_date IN ({ph})
              AND COALESCE(TRIM(insight_cover_style), '') <> ''
              AND COALESCE(TRIM(ad_key), '') <> ''
              AND COALESCE(TRIM(appid), '') <> ''
            """,
            dlist,
        )
        by_app: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in cur.fetchall():
            ak = str(row["ad_key"] or "").strip()
            aid = str(row["appid"] or "").strip()
            raw = str(row["insight_cover_style"] or "").strip()
            if not ak or not aid or not raw:
                continue
            try:
                obj = json.loads(raw)
                if not isinstance(obj, dict):
                    continue
            except Exception:
                continue
            by_app[aid].append(
                {
                    "target_date": str(row["target_date"] or ""),
                    "ad_key": ak,
                    "style_json": obj,
                    "exposure": int(row["exp"] or 0),
                }
            )
        return dict(by_app)
    finally:
        conn.close()


def load_cover_embedding_blob_map_by_ad_keys(ad_keys: List[str]) -> Dict[str, bytes]:
    """批量读取 creative_library.cover_embedding（非空 BLOB）。"""
    init_db()
    keys = [k for k in {str(x or "").strip() for x in (ad_keys or [])} if k]
    if not keys:
        return {}
    conn = _get_conn()
    out: Dict[str, bytes] = {}
    try:
        cur = conn.cursor()
        batch = 400
        for i in range(0, len(keys), batch):
            chunk = keys[i : i + batch]
            ph = ",".join(["?"] * len(chunk))
            cur.execute(
                f"SELECT ad_key, cover_embedding FROM creative_library "
                f"WHERE ad_key IN ({ph}) AND cover_embedding IS NOT NULL",
                chunk,
            )
            for r in cur.fetchall():
                b = r["cover_embedding"]
                if b:
                    out[str(r["ad_key"])] = bytes(b)
        return out
    finally:
        conn.close()


def upsert_cover_embedding(ad_key: str, embedding_blob: bytes) -> bool:
    """写入/更新主库 creative_library.cover_embedding（用于封面 CLIP 与日内聚类复用）。"""
    if not (ad_key or "").strip() or not embedding_blob:
        return False
    init_db()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE creative_library
            SET cover_embedding = ?,
                updated_at_local = datetime('now', 'localtime')
            WHERE ad_key = ?
            """,
            (embedding_blob, ad_key),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
def _dedupe_intraday_union_by_appid(
    items: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    按 appid 分桶；桶内若 ad_key 相同、或主媒体 URL 相同、或封面 ahash 汉明距离 ≤ 阈值，则视为同一组。
    同组保留 impression 最高的一条。
    返回 (代表素材列表, intraday_removed 明细)。
    """
    by_app: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in items:
        if not isinstance(item, dict):
            continue
        c = item.get("creative") or {}
        if not isinstance(c, dict):
            continue
        if not str(c.get("ad_key") or "").strip():
            continue
        appid = str(item.get("appid") or "").strip()
        by_app[appid].append(item)

    canonical_all: List[Dict[str, Any]] = []
    intraday_removed: List[Dict[str, Any]] = []

    for appid, bucket in by_app.items():
        n = len(bucket)
        parent = list(range(n))

        def find(i: int) -> int:
            while parent[i] != i:
                parent[i] = parent[parent[i]]
                i = parent[i]
            return i

        def union(i: int, j: int) -> None:
            pi, pj = find(i), find(j)
            if pi != pj:
                parent[pi] = pj

        meta: List[Dict[str, Any]] = []
        for item in bucket:
            c = item.get("creative") or {}
            vurl = _pick_video_url_from_raw(c)
            iurl = _pick_image_url_from_raw(c) if not vurl else ""
            media = (vurl or iurl).strip()
            ahash = str(c.get("image_ahash_md5") or "").strip()
            ak = str(c.get("ad_key") or "").strip()
            imp = int(c.get("impression") or 0)
            meta.append({"ad_key": ak, "media": media, "ahash": ahash, "imp": imp, "item": item})

        for i in range(n):
            for j in range(i + 1, n):
                a, b = meta[i], meta[j]
                if a["ad_key"] and a["ad_key"] == b["ad_key"]:
                    union(i, j)
                    continue
                if a["media"] and a["media"] == b["media"]:
                    union(i, j)
                    continue
                if a["ahash"] and b["ahash"]:
                    d = _ahash_hamming(a["ahash"], b["ahash"])
                    if d <= AHASH_HAMMING_THRESHOLD:
                        union(i, j)

        groups: Dict[int, List[int]] = defaultdict(list)
        for i in range(n):
            groups[find(i)].append(i)

        for _root, idxs in groups.items():
            best_i = max(idxs, key=lambda ii: meta[ii]["imp"])
            canonical_all.append(meta[best_i]["item"])
            best_ak = meta[best_i]["ad_key"]
            for ii in idxs:
                if ii == best_i:
                    continue
                intraday_removed.append({
                    "ad_key": meta[ii]["ad_key"],
                    "reason": "same_group",
                    "kept_ad_key": best_ak,
                    "appid": appid,
                })

    return canonical_all, intraday_removed


def crossday_filter_items_against_creative_library(
    target_date: str,
    items: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    与 get_deduped_items_for_analysis 的 Step B 相同：
    将 items 与 creative_library 中「早于 target_date 已入库」的记录按同 appid 比对，
    ad_key / 主媒体 URL / 封面 ahash 汉明距离命中则剔除。

    供封面流程在「本日聚类」之前先做一层跨日指纹去重，与灵感分析侧逻辑一致。
    """
    init_db()
    deduped_items: List[Dict[str, Any]] = []
    crossday_removed: List[Dict[str, Any]] = []

    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ad_key, appid, image_ahash_md5, video_url, image_url, first_target_date
            FROM creative_library
            WHERE first_target_date < ?
              AND first_target_date IS NOT NULL
            """,
            (target_date,),
        )
        hist_rows = cur.fetchall()

        hist_adkey_date: Dict[str, Dict[str, str]] = defaultdict(dict)
        hist_urls: Dict[str, Dict[str, Tuple[str, str]]] = defaultdict(dict)
        hist_ahash: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list)

        for r in hist_rows:
            aid = str(r["appid"] or "").strip()
            ak = str(r["ad_key"] or "").strip()
            fdt = str(r["first_target_date"] or "")
            if ak:
                hist_adkey_date[aid][ak] = fdt
            vurl = str(r["video_url"] or "").strip()
            iurl = str(r["image_url"] or "").strip()
            media = vurl or iurl
            if media:
                hist_urls[aid][media] = (ak, fdt)
            h = str(r["image_ahash_md5"] or "").strip()
            if h:
                hist_ahash[aid].append((h, ak, fdt))

        for item in items:
            if not isinstance(item, dict):
                continue
            c = item.get("creative") or {}
            if not isinstance(c, dict):
                continue
            ad_key = str(c.get("ad_key") or "").strip()
            ahash = str(c.get("image_ahash_md5") or "").strip()
            vurl = _pick_video_url_from_raw(c)
            iurl = _pick_image_url_from_raw(c) if not vurl else ""
            media = (vurl or iurl).strip()
            appid = str(item.get("appid") or "").strip()

            hit_ak = ""
            hit_date = ""
            reason_b = ""

            if appid and ad_key and ad_key in hist_adkey_date.get(appid, {}):
                hit_ak = ad_key
                hit_date = hist_adkey_date[appid][ad_key]
                reason_b = "ad_key"

            if not hit_ak and appid and media and media in hist_urls.get(appid, {}):
                hit_ak, hit_date = hist_urls[appid][media]
                reason_b = "url"

            if not hit_ak and appid and ahash:
                best_dist = 999
                for (h, ak, dt) in hist_ahash.get(appid, []):
                    d = _ahash_hamming(ahash, h)
                    if d <= AHASH_HAMMING_THRESHOLD and d < best_dist:
                        best_dist, hit_ak, hit_date = d, ak, dt
                if hit_ak:
                    reason_b = f"ahash(dist={best_dist})"

            if hit_ak:
                crossday_removed.append(
                    {
                        "ad_key": ad_key,
                        "reason": reason_b,
                        "matched_ad_key": hit_ak,
                        "matched_date": hit_date,
                        "appid": appid,
                        "product": str(item.get("product") or "").strip(),
                        "preview_img_url": str(c.get("preview_img_url") or "").strip(),
                        "image_url": _pick_image_url_from_raw(c),
                        "video_url": _pick_video_url_from_raw(c),
                        "all_exposure_value": int(c.get("all_exposure_value") or 0),
                        "title": str(c.get("title") or "").strip(),
                        "body": str(c.get("body") or "").strip(),
                    }
                )
            else:
                deduped_items.append(item)
    finally:
        conn.close()

    return deduped_items, crossday_removed


# 多维去重过滤：日内 + 跨日，返回进入分析的唯一素材集
# ---------------------------------------------------------------------------


def resolve_inspiration_crossday_lookback_days() -> int:
    """与 dedup 报告、日志中 lookback 展示一致（跨日 B 步仍用全历史；本值作展示/对账）。"""
    raw = (
        os.getenv("INSPIRATION_DEDUP_LOOKBACK_DAYS")
        or os.getenv("INSPIRATION_CROSSDAY_LOOKBACK_DAYS")
        or "7"
    ).strip()
    try:
        n = int(raw)
    except ValueError:
        n = 7
    return max(0, min(365, n))


def build_inspiration_dedup_redirect_map(dedup_report: Dict[str, Any]) -> Dict[str, str]:
    """
    去重中「被删」的 ad_key -> 可沿用其分析的代表 ad_key（日内同簇 or 跨日匹配）。
    """
    m: Dict[str, str] = {}
    for r in (dedup_report or {}).get("intraday_removed") or []:
        if not isinstance(r, dict):
            continue
        ak = str(r.get("ad_key") or "").strip()
        kk = str(r.get("kept_ad_key") or "").strip()
        if ak and kk:
            m[ak] = kk
    for r in (dedup_report or {}).get("crossday_removed") or []:
        if not isinstance(r, dict):
            continue
        ak = str(r.get("ad_key") or "").strip()
        mk = str(r.get("matched_ad_key") or "").strip()
        if ak and mk:
            m[ak] = mk
    return m


def _row_from_item_and_patched_analysis(
    item: Dict[str, Any], analysis: str, ua_sugg: str, ex_meta: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    c = item.get("creative") if isinstance(item.get("creative"), dict) else {}
    ex_meta = ex_meta or {}
    ak = str(c.get("ad_key") or "").strip()
    vurl = str(c.get("video_url") or "").strip()
    iu = ex_meta.get("image_url", "")
    if not isinstance(iu, str):
        iu = str(iu or "")
    if not vurl and not iu and ex_meta:
        vurl = str(ex_meta.get("video_url") or "")
        iu = str(ex_meta.get("image_url") or "")
    creative_type = str(ex_meta.get("creative_type") or ("image" if iu and not vurl else "video"))
    return {
        "category": item.get("category"),
        "product": item.get("product"),
        "appid": item.get("appid"),
        "ad_key": ak,
        "creative_type": creative_type,
        "platform": c.get("platform"),
        "video_duration": c.get("video_duration"),
        "all_exposure_value": c.get("all_exposure_value"),
        "heat": c.get("heat"),
        "impression": c.get("impression"),
        "video_url": vurl,
        "tiktok_ytdlp_used": False,
        "youtube_ytdlp_used": False,
        "image_url": iu,
        "preview_img_url": c.get("preview_img_url") or ex_meta.get("preview_img_url") or "",
        "title": c.get("title") or "",
        "body": c.get("body") or "",
        "pipeline_tags": c.get("pipeline_tags")
        if isinstance(c.get("pipeline_tags"), list)
        else list(ex_meta.get("pipeline_tags") or []),
        "analysis": analysis,
        "inspiration_enrichment": "none",
        "ua_suggestion_single": ua_sugg,
        "style_filter_match_summary": "",
        "material_tags": list(ex_meta.get("material_tags") or []),
        "arrow2_material_category": str(ex_meta.get("arrow2_material_category") or ""),
        "ad_one_liner": str(ex_meta.get("ad_one_liner") or ""),
        "effect_one_liner": str(ex_meta.get("effect_one_liner") or ""),
        "play_fingerprint": str(ex_meta.get("play_fingerprint") or ""),
        "differentiator": str(ex_meta.get("differentiator") or ""),
        "play_asset_id": str(ex_meta.get("play_asset_id") or ""),
        "play_asset_name": str(ex_meta.get("play_asset_name") or ""),
        "play_asset_subtag_ids": str(ex_meta.get("play_asset_subtag_ids") or ""),
        "play_asset_subtag_names": str(ex_meta.get("play_asset_subtag_names") or ""),
        "play_asset_novelty_label": str(ex_meta.get("play_asset_novelty_label") or ""),
        "play_asset_match_source": str(ex_meta.get("play_asset_match_source") or ""),
        "play_asset_classification_reason": str(ex_meta.get("play_asset_classification_reason") or ""),
        "exclude_from_bitable": bool(ex_meta.get("exclude_from_bitable", False)),
        "exclude_from_cluster": bool(ex_meta.get("exclude_from_cluster", False)),
    }


def combined_analysis_results_for_pipeline(
    pipeline_items: List[Dict[str, Any]],
    new_success_by_ad: Dict[str, Dict[str, Any]],
    existing_analysis: Dict[str, Dict[str, Any]],
    dedup_redirect: Dict[str, str],
) -> List[Dict[str, Any]]:
    """
    为 raw 中每条 item 产出一行与 analyze_video_from_raw_json 近似的 result。
    本次成功 > 历史缓存 > 去重重定向；否则为失败占位。
    """
    out: List[Dict[str, Any]] = []
    for item in pipeline_items or []:
        if not isinstance(item, dict):
            continue
        c = item.get("creative") or {}
        if not isinstance(c, dict):
            continue
        ak = str(c.get("ad_key") or "").strip()
        if not ak:
            continue

        if ak in new_success_by_ad:
            out.append(copy.deepcopy(new_success_by_ad[ak]))
            continue
        if ak in existing_analysis:
            ex = existing_analysis[ak]
            out.append(
                _row_from_item_and_patched_analysis(
                    item,
                    str(ex.get("analysis") or ""),
                    str(
                        ex.get("ua_suggestion_single")
                        or ex.get("ua_suggestion", "")
                        or ""
                    ),
                    {
                        "creative_type": ex.get("creative_type"),
                        "image_url": ex.get("image_url", ""),
                        "video_url": ex.get("video_url", ""),
                        "preview_img_url": ex.get("preview_img_url", ""),
                        "pipeline_tags": ex.get("pipeline_tags") or [],
                        "effect_one_liner": ex.get("effect_one_liner", ""),
                        "ad_one_liner": ex.get("ad_one_liner", ""),
                        "play_fingerprint": ex.get("play_fingerprint", ""),
                        "differentiator": ex.get("differentiator", ""),
                        "play_asset_id": ex.get("play_asset_id", ""),
                        "play_asset_name": ex.get("play_asset_name", ""),
                        "play_asset_subtag_ids": ex.get("play_asset_subtag_ids", ""),
                        "play_asset_subtag_names": ex.get("play_asset_subtag_names", ""),
                        "play_asset_novelty_label": ex.get("play_asset_novelty_label", ""),
                        "play_asset_match_source": ex.get("play_asset_match_source", ""),
                        "play_asset_classification_reason": ex.get("play_asset_classification_reason", ""),
                    },
                )
            )
            continue
        if ak in dedup_redirect:
            src = str(dedup_redirect[ak] or "").strip()
            if not src:
                out.append(
                    {
                        "ad_key": ak,
                        "category": item.get("category"),
                        "product": item.get("product"),
                        "appid": item.get("appid"),
                        "analysis": "[ERROR] 去重映射目标为空",
                    }
                )
                continue
            if src in new_success_by_ad:
                base = copy.deepcopy(new_success_by_ad[src])
            elif src in existing_analysis:
                ex0 = existing_analysis[src]
                base = _row_from_item_and_patched_analysis(
                    {
                        "creative": c,
                        "category": item.get("category"),
                        "product": item.get("product"),
                        "appid": item.get("appid"),
                    },
                    str(ex0.get("analysis") or ""),
                    str(
                        ex0.get("ua_suggestion_single")
                        or ex0.get("ua_suggestion", "")
                        or ""
                    ),
                    {
                        "creative_type": ex0.get("creative_type"),
                        "image_url": ex0.get("image_url", ""),
                        "video_url": ex0.get("video_url", ""),
                        "preview_img_url": ex0.get("preview_img_url", ""),
                        "pipeline_tags": ex0.get("pipeline_tags") or [],
                        "effect_one_liner": ex0.get("effect_one_liner", ""),
                        "ad_one_liner": ex0.get("ad_one_liner", ""),
                        "play_fingerprint": ex0.get("play_fingerprint", ""),
                        "differentiator": ex0.get("differentiator", ""),
                        "play_asset_id": ex0.get("play_asset_id", ""),
                        "play_asset_name": ex0.get("play_asset_name", ""),
                        "play_asset_subtag_ids": ex0.get("play_asset_subtag_ids", ""),
                        "play_asset_subtag_names": ex0.get("play_asset_subtag_names", ""),
                        "play_asset_novelty_label": ex0.get("play_asset_novelty_label", ""),
                        "play_asset_match_source": ex0.get("play_asset_match_source", ""),
                        "play_asset_classification_reason": ex0.get("play_asset_classification_reason", ""),
                    },
                )
            else:
                out.append(
                    {
                        "ad_key": ak,
                        "category": item.get("category"),
                        "product": item.get("product"),
                        "appid": item.get("appid"),
                        "analysis": f"[ERROR] 去重重定向 {src!r} 无可用分析",
                    }
                )
                continue
            b = copy.deepcopy(base) if isinstance(base, dict) else {}
            b["ad_key"] = ak
            b["category"] = item.get("category")
            b["product"] = item.get("product")
            b["appid"] = item.get("appid")
            b["video_url"] = str(c.get("video_url") or b.get("video_url") or "")
            b["image_url"] = str(c.get("image_url") or b.get("image_url") or "")
            b["preview_img_url"] = str(c.get("preview_img_url") or b.get("preview_img_url") or "")
            b["impression"] = c.get("impression", b.get("impression"))
            b["title"] = c.get("title") or b.get("title", "")
            b["body"] = c.get("body") or b.get("body", "")
            out.append(b)
            continue

        out.append(
            {
                "ad_key": ak,
                "category": item.get("category"),
                "product": item.get("product"),
                "appid": item.get("appid"),
                "analysis": "[ERROR] 本批无分析且未匹配历史/去重映射",
            }
        )
    return out


def get_deduped_items_for_analysis(
    target_date: str,
    raw_payload: Dict[str, Any],
    *,
    history_lookback_days: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    对 raw_payload 中的素材做两步去重，返回 (deduped_items, report)。

    仅在同一 appid 内比较（不同产品互不影响）。

    Step A — 日内去重：
      同一 appid 内，若 ad_key 相同、或主媒体 URL 相同、或封面 ahash 汉明距离 ≤ 阈值，则归为一组；
      同组只保留 impression 最高的一条。

    Step B — 跨日去重：
      将 Step A 的代表素材与 creative_library 中历史记录比对（仅同 appid）：
      若 ad_key 已在库中、或主媒体 URL 已出现、或 ahash 与历史条在阈值内相似，则剔除。

    report 结构：
    {
      "total_input": N,
      "after_intraday": N,
      "after_crossday": N,
      "intraday_removed": [...],
      "crossday_removed": [...],
    }
    """
    init_db()
    items = raw_payload.get("items") or []
    valid_items: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        c = item.get("creative") or {}
        if not isinstance(c, dict):
            continue
        if not str(c.get("ad_key") or "").strip():
            continue
        valid_items.append(item)

    canonical_items_a, intraday_removed = _dedupe_intraday_union_by_appid(valid_items)
    after_intraday = len(canonical_items_a)

    deduped_items, crossday_removed = crossday_filter_items_against_creative_library(
        target_date, canonical_items_a
    )

    hlb = history_lookback_days
    if hlb is None:
        hlb = resolve_inspiration_crossday_lookback_days()
    report: Dict[str, Any] = {
        "total_input": len(items),
        "after_intraday": after_intraday,
        "after_crossday": len(deduped_items),
        "intraday_removed_count": len(intraday_removed),
        "crossday_removed_count": len(crossday_removed),
        "intraday_removed": intraday_removed,
        "crossday_removed": crossday_removed,
        "history_lookback_days": hlb,
    }
    return deduped_items, report


def upsert_effect_one_liner_embedding(ad_key: str, embedding_blob: bytes) -> bool:
    """将 effect_one_liner 的嵌入向量写入 creative_library。"""
    init_db()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE creative_library SET effect_one_liner_embedding = ?, "
            "updated_at_local = datetime('now','localtime') WHERE ad_key = ?",
            (embedding_blob, ad_key),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 趋势信号：从 creative_library 聚合
# ---------------------------------------------------------------------------
def compute_trend_signals(target_date: str, lookback_days: int = 14) -> Dict[str, Any]:
    """
    计算各产品的素材趋势信号。

    返回::

        {
          "target_date": "...",
          "per_product": {
            "ProductName": {
              "this_week_new": 5,
              "prev_week_new": 3,
              "trend": "rising",      # rising / declining / stable
              "total_unique": 42,
              "avg_appearance": 1.3,
            }
          },
          "overall": { ... }
        }
    """
    from datetime import date, timedelta
    init_db()

    try:
        td = date.fromisoformat(target_date)
    except ValueError:
        return {"target_date": target_date, "per_product": {}, "overall": {}}

    week1_start = (td - timedelta(days=6)).isoformat()
    week1_end = td.isoformat()
    week2_start = (td - timedelta(days=13)).isoformat()
    week2_end = (td - timedelta(days=7)).isoformat()

    conn = _get_conn()
    try:
        cur = conn.cursor()

        def _count_new(start: str, end: str) -> Dict[str, int]:
            cur.execute(
                "SELECT product, COUNT(*) as cnt FROM creative_library "
                "WHERE first_target_date >= ? AND first_target_date <= ? "
                "AND product IS NOT NULL AND product != '' "
                "GROUP BY product",
                (start, end),
            )
            return {r["product"]: r["cnt"] for r in cur.fetchall()}

        this_week = _count_new(week1_start, week1_end)
        prev_week = _count_new(week2_start, week2_end)

        all_products = set(this_week.keys()) | set(prev_week.keys())
        per_product: Dict[str, Dict[str, Any]] = {}
        for p in sorted(all_products):
            tw = this_week.get(p, 0)
            pw = prev_week.get(p, 0)
            if tw > pw * 1.3:
                trend = "rising"
            elif tw < pw * 0.7:
                trend = "declining"
            else:
                trend = "stable"
            per_product[p] = {
                "this_week_new": tw,
                "prev_week_new": pw,
                "trend": trend,
            }

        cur.execute(
            "SELECT product, COUNT(*) as cnt, AVG(appearance_count) as avg_app "
            "FROM creative_library WHERE product IS NOT NULL AND product != '' "
            "GROUP BY product"
        )
        for r in cur.fetchall():
            p = r["product"]
            if p in per_product:
                per_product[p]["total_unique"] = r["cnt"]
                per_product[p]["avg_appearance"] = round(float(r["avg_app"] or 1), 2)

        tw_total = sum(this_week.values())
        pw_total = sum(prev_week.values())
        overall_trend = "stable"
        if tw_total > pw_total * 1.3:
            overall_trend = "rising"
        elif tw_total < pw_total * 0.7:
            overall_trend = "declining"

        return {
            "target_date": target_date,
            "per_product": per_product,
            "overall": {
                "this_week_new": tw_total,
                "prev_week_new": pw_total,
                "trend": overall_trend,
            },
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 历史方向卡片查询
# ---------------------------------------------------------------------------
def load_recent_direction_cards(
    target_date: str,
    n_days: int = 3,
) -> List[Dict[str, Any]]:
    """
    读取 target_date 前 n_days 天的方向卡片摘要（从 daily_ua_push_content）。
    返回 [{target_date, direction_name, background, ua_suggestion, risk_note}]。
    """
    from datetime import date, timedelta
    init_db()

    try:
        td = date.fromisoformat(target_date)
    except ValueError:
        return []

    start = (td - timedelta(days=n_days)).isoformat()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT target_date, direction_name, background, ua_suggestion, risk_note
            FROM daily_ua_push_content
            WHERE target_date >= ? AND target_date < ?
              AND COALESCE(TRIM(direction_name), '') != ''
              AND COALESCE(TRIM(ua_suggestion), '') != ''
            ORDER BY target_date DESC, id ASC
            """,
            (start, target_date),
        )
        return [
            {
                "target_date": r["target_date"],
                "direction_name": r["direction_name"],
                "background": r["background"] or "",
                "ua_suggestion": r["ua_suggestion"] or "",
                "risk_note": r["risk_note"] or "",
            }
            for r in cur.fetchall()
        ]
    finally:
        conn.close()



# ---------------------------------------------------------------------------
# 特效玩法去重 & 新素材识别 & 持续发力信号
# ---------------------------------------------------------------------------

_EFFECT_FILLER_PATTERNS = [
    r"ai",
    r"人工智能",
    r"一键",
    r"自动",
    r"智能",
    r"生成",
    r"制作",
    r"打造",
    r"输出",
    r"特效",
    r"玩法",
    r"素材",
    r"广告",
    r"视频",
    r"图片",
    r"照片",
]

_EFFECT_SYNONYM_REPLACEMENTS = [
    ("polaroid", "宝丽来"),
    ("拍立得", "宝丽来"),
    ("明星", "名人"),
    ("偶像", "名人"),
    ("艺人", "名人"),
    ("合照", "合影"),
    ("同框", "合影"),
    ("姿态", "姿势"),
    ("骑行", "骑乘"),
    ("修脸", "修图"),
    ("修复", "修图"),
    ("美颜", "修图"),
    ("美化", "修图"),
    ("动画", "视频"),
]


def normalize_effect_one_liner(text: str) -> str:
    """Normalize short effect descriptions for cross-day play matching."""
    s = str(text or "").strip().lower()
    if not s or s == "none":
        return ""
    s = re.sub(r"[（(].*?[）)]", "", s)
    s = re.sub(r"[【】\[\]{}<>《》\"'“”‘’]", "", s)
    for src, dst in _EFFECT_SYNONYM_REPLACEMENTS:
        s = s.replace(src, dst)
    s = re.sub(r"[\s,，.。:：;；!！?？/\\|_+\-—~·•]+", "", s)
    for pat in _EFFECT_FILLER_PATTERNS:
        s = re.sub(pat, "", s, flags=re.I)
    return s.strip()


def _effect_similarity_threshold() -> float:
    raw = (os.getenv("EFFECT_ONE_LINER_SIMILARITY_THRESHOLD") or "0.82").strip()
    try:
        return max(0.5, min(1.0, float(raw)))
    except ValueError:
        return 0.82


def _intraday_effect_similarity_threshold() -> float:
    raw = (
        os.getenv("INTRADAY_EFFECT_SIMILARITY_THRESHOLD")
        or os.getenv("EFFECT_ONE_LINER_SIMILARITY_THRESHOLD")
        or "0.94"
    ).strip()
    try:
        return max(0.5, min(1.0, float(raw)))
    except ValueError:
        return 0.94


def _old_effect_bitable_similarity_threshold() -> float:
    raw = (os.getenv("OLD_EFFECT_SIMILARITY_THRESHOLD") or "0.94").strip()
    try:
        return max(0.5, min(1.0, float(raw)))
    except ValueError:
        return 0.94


def _effect_embedding_intraday_hard_threshold() -> float:
    raw = (os.getenv("EFFECT_EMBEDDING_INTRADAY_HARD_THRESHOLD") or "0.95").strip()
    try:
        return max(0.5, min(1.0, float(raw)))
    except ValueError:
        return 0.95


def _effect_embedding_crossday_hard_threshold() -> float:
    raw = (os.getenv("EFFECT_EMBEDDING_CROSSDAY_HARD_THRESHOLD") or "0.96").strip()
    try:
        return max(0.5, min(1.0, float(raw)))
    except ValueError:
        return 0.96


def _embedding_duplicate_candidate_threshold() -> float:
    raw = (os.getenv("EMBEDDING_DUP_CANDIDATE_THRESHOLD") or "0.90").strip()
    try:
        return max(0.5, min(1.0, float(raw)))
    except ValueError:
        return 0.90


def _daily_play_cluster_text_threshold() -> float:
    raw = (
        os.getenv("DAILY_PLAY_CLUSTER_TEXT_THRESHOLD")
        or os.getenv("EFFECT_ONE_LINER_SIMILARITY_THRESHOLD")
        or "0.82"
    ).strip()
    try:
        return max(0.5, min(1.0, float(raw)))
    except ValueError:
        return 0.82


def _daily_play_cluster_embedding_threshold() -> float:
    raw = (
        os.getenv("DAILY_PLAY_CLUSTER_EMBEDDING_THRESHOLD")
        or os.getenv("EMBEDDING_DUP_CANDIDATE_THRESHOLD")
        or "0.90"
    ).strip()
    try:
        return max(0.5, min(1.0, float(raw)))
    except ValueError:
        return 0.90


def _daily_play_cluster_embedding_enabled() -> bool:
    raw = os.getenv("DAILY_PLAY_CLUSTER_EMBEDDING_ENABLED", "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _daily_play_global_history_enabled() -> bool:
    raw = os.getenv("DAILY_PLAY_GLOBAL_HISTORY_ENABLED", "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


_EFFECT_SIGNATURES = [
    ("名人宝丽来合影", ("名人", "宝丽来", "合影"), 0.95),
    ("派对修图前后对比", ("派对", "修图", "前后对比"), 0.95),
    ("母亲节手绘合影", ("母亲节", "手绘", "合影"), 0.95),
]


def _effect_signature_similarity(na: str, nb: str) -> float:
    best = 0.0
    for _, tokens, score in _EFFECT_SIGNATURES:
        if all(t in na for t in tokens) and all(t in nb for t in tokens):
            best = max(best, score)
    return best


def _effect_text_similarity(a: str, b: str) -> float:
    a0 = str(a or "").strip()
    b0 = str(b or "").strip()
    if not a0 or not b0:
        return 0.0
    if a0 == b0:
        return 1.0
    na = normalize_effect_one_liner(a0)
    nb = normalize_effect_one_liner(b0)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    if len(na) >= 5 and len(nb) >= 5 and (na in nb or nb in na):
        return 0.92
    return max(_effect_signature_similarity(na, nb), float(SequenceMatcher(None, na, nb).ratio()))


def _play_dedupe_text_from_values(play_fingerprint: Any, effect_one_liner: Any) -> str:
    fingerprint = str(play_fingerprint or "").strip()
    if fingerprint and fingerprint not in ("None", "无"):
        return fingerprint
    effect = str(effect_one_liner or "").strip()
    if effect and effect != "None":
        return effect
    return ""


def _play_dedupe_text(row: Dict[str, Any]) -> str:
    return _play_dedupe_text_from_values(
        row.get("play_fingerprint"),
        row.get("effect_one_liner"),
    )


def _contains_any(text: str, tokens: Tuple[str, ...]) -> bool:
    return any(token in text for token in tokens)


def _coarse_play_cluster_key(play_text: Any, display_text: Any = "") -> str:
    """
    Collapse detailed play fingerprints into product-level play families.

    This is intentionally coarser than duplicate detection: scenes, props, holidays
    and style adjectives should not become separate "new play" buckets by default.
    """
    raw = f"{play_text or ''} {display_text or ''}".strip()
    if not raw:
        return ""
    compact = normalize_effect_one_liner(raw)
    joined = raw + compact

    def has(*tokens: str) -> bool:
        return _contains_any(joined, tuple(tokens))

    if has("贴纸", "sticker"):
        return "照片生成贴纸包"
    if has("诊断", "脸型", "妆容", "色彩分析", "发型红黑榜"):
        return "照片生成发型妆容诊断"
    if has("两张", "双人", "双照片", "母女", "母子", "亲人", "同框", "合影", "合照", "融合", "重逢"):
        if has("合影", "合照", "融合", "重逢", "同框", "母女", "母子", "亲人", "拥抱"):
            return "双人照片融合合影"
    if has("老照片", "低清", "4k", "黑白照", "彩色动态", "画质增强"):
        return "老照片修复增强动态化"
    if has("手绘", "素描", "插画", "涂鸦", "蜡笔"):
        return "照片转手绘插画风格"
    if has("舞蹈", "跳舞", "dance"):
        return "照片生成舞蹈视频"
    if has("年龄", "变老", "衰老", "变龄", "性别", "变性"):
        return "照片生成变龄性别视频"
    if has("提示词", "剧情", "prompt", "冲突剧情", "故事"):
        return "提示词生成剧情视频"
    if has("机甲", "战甲", "装甲", "超级英雄", "火焰", "恶魔", "冰霜", "翅膀", "黄金铠甲", "神经网络"):
        return "照片生成特效变身视频"
    if has("罚单", "测速", "罚款", "交通"):
        return "照片生成趣味证件票据场景"
    if has("恐龙", "动物", "鲨鱼", "豹子", "珠宝豹", "巨虫", "骑乘", "骑行"):
        return "人物动物融合奇幻视频"
    if has("商品", "电商", "广告视频"):
        return "商品图生成动态广告视频"
    if has("写真", "肖像", "头像", "杂志封面", "生日", "商务", "CEO", "红毯", "影楼"):
        return "自拍生成风格写真肖像"
    if has("美颜", "美化", "面部优化", "自然修图", "人像精修", "ai修图", "AI修图", "修图前后"):
        return "真人照片AI修图前后对比"
    if has("动态视频", "生成视频", "视频输出", "照片生成", "照片输入", "静态照片", "自拍照片", "真人照片"):
        return "照片生成动态视频"
    return str(play_text or display_text or "").strip()


def _play_cluster_text(row: Dict[str, Any]) -> str:
    explicit = str(row.get("play_cluster_key") or row.get("daily_play_cluster_key") or "").strip()
    if explicit and explicit not in ("None", "无"):
        return explicit
    return _coarse_play_cluster_key(_play_dedupe_text(row), row.get("effect_one_liner"))


def _append_material_tag(row: Dict[str, Any], tag: str) -> None:
    tags = row.get("material_tags")
    if not isinstance(tags, list):
        tags = []
    if tag not in tags:
        tags.append(tag)
    row["material_tags"] = tags


def _effect_row_score(row: Dict[str, Any]) -> Tuple[int, int, int, str]:
    def as_int(value: Any) -> int:
        try:
            return int(float(value or 0))
        except Exception:
            return 0

    return (
        as_int(row.get("all_exposure_value")),
        as_int(row.get("impression")),
        as_int(row.get("heat")),
        str(row.get("ad_key") or ""),
    )


def apply_intraday_effect_bitable_filter(
    rows: List[Dict[str, Any]],
    threshold: float | None = None,
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Mark same-day duplicate plays within the same appid.

    It keeps the strongest representative in each connected similarity group and
    excludes the rest from Bitable/direction cards. This catches duplicate captures
    inside one daily batch, which cross-day history matching cannot see.
    """
    if not rows:
        return 0, []
    if threshold is None:
        threshold = _intraday_effect_similarity_threshold()

    by_app: Dict[str, List[int]] = defaultdict(list)
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        if row.get("exclude_from_bitable"):
            continue
        appid = str(row.get("appid") or "").strip()
        effect = _play_dedupe_text(row)
        if appid and effect and normalize_effect_one_liner(effect):
            by_app[appid].append(idx)

    details: List[Dict[str, Any]] = []
    newly_marked = 0
    for indices in by_app.values():
        kept_indices: List[int] = []
        for idx in sorted(indices, key=lambda i: _effect_row_score(rows[i]), reverse=True):
            row = rows[idx]
            effect = _play_dedupe_text(row)
            best_keeper_idx: int | None = None
            best_sim = 0.0
            for kept_idx in kept_indices:
                sim = _effect_text_similarity(effect, _play_dedupe_text(rows[kept_idx]))
                if sim > best_sim:
                    best_sim = sim
                    best_keeper_idx = kept_idx
            if best_keeper_idx is None or best_sim < threshold:
                kept_indices.append(idx)
                continue

            keeper = rows[best_keeper_idx]
            keeper_effect = _play_dedupe_text(keeper)
            was_excluded = bool(row.get("exclude_from_bitable"))
            row["exclude_from_bitable"] = True
            row["exclude_from_cluster"] = True
            row["intraday_effect_match"] = {
                "kept_ad_key": str(keeper.get("ad_key") or ""),
                "kept_effect_one_liner": keeper_effect,
                "kept_play_fingerprint": str(keeper.get("play_fingerprint") or ""),
                "similarity": round(best_sim, 3),
                "threshold": round(float(threshold), 3),
                "match_type": "exact" if best_sim >= 0.999 else "similar",
            }
            _append_material_tag(row, "日内玩法重复")
            if not was_excluded:
                newly_marked += 1
            details.append(
                {
                    "ad_key": str(row.get("ad_key") or ""),
                    "product": str(row.get("product") or ""),
                    "effect_one_liner": effect,
                    "kept_ad_key": str(keeper.get("ad_key") or ""),
                    "kept_effect_one_liner": keeper_effect,
                    "kept_play_fingerprint": str(keeper.get("play_fingerprint") or ""),
                    "similarity": round(best_sim, 3),
                    "match_type": "exact" if best_sim >= 0.999 else "similar",
                    "newly_marked": not was_excluded,
                }
            )
    return newly_marked, details


def _load_effect_embedding_history_rows(
    target_date: str,
    lookback_days: int,
) -> Dict[str, List[Dict[str, Any]]]:
    from datetime import date, timedelta

    try:
        td = date.fromisoformat(target_date)
    except ValueError:
        return {}
    start_date = (td - timedelta(days=lookback_days)).isoformat()
    init_db()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ad_key, appid, effect_one_liner, play_fingerprint, differentiator,
                   effect_one_liner_embedding,
                   first_target_date, last_target_date,
                   best_impression, best_all_exposure_value
            FROM creative_library
            WHERE first_target_date >= ? AND first_target_date < ?
              AND COALESCE(TRIM(effect_one_liner), '') != ''
            """,
            (start_date, target_date),
        )
        by_app: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in cur.fetchall():
            appid = str(row["appid"] or "").strip()
            effect = _play_dedupe_text_from_values(row["play_fingerprint"], row["effect_one_liner"])
            if not appid or not effect or effect == "None":
                continue
            blob = row["effect_one_liner_embedding"]
            by_app[appid].append(
                {
                    "ad_key": str(row["ad_key"] or ""),
                    "appid": appid,
                    "effect_one_liner": effect,
                    "display_effect_one_liner": str(row["effect_one_liner"] or ""),
                    "play_fingerprint": str(row["play_fingerprint"] or ""),
                    "differentiator": str(row["differentiator"] or ""),
                    "effect_one_liner_embedding": bytes(blob) if blob else None,
                    "first_target_date": str(row["first_target_date"] or ""),
                    "last_target_date": str(row["last_target_date"] or ""),
                    "best_impression": int(row["best_impression"] or 0),
                    "best_all_exposure_value": int(row["best_all_exposure_value"] or 0),
                }
            )
        return dict(by_app)
    finally:
        conn.close()


def apply_effect_embedding_duplicate_filter(
    target_date: str,
    rows: List[Dict[str, Any]],
    lookback_days: int = 7,
    intraday_threshold: float | None = None,
    crossday_threshold: float | None = None,
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Hard-exclude high-confidence duplicate plays using play fingerprint embeddings.

    Text rules handle exact/normalized duplicates first. This layer catches short
    paraphrases such as "AI restores an old photo" vs "turn blurred vintage photos
    sharp again". It records the threshold and matched evidence on each row.
    """
    if not rows:
        return 0, []
    try:
        lookback_days = int(os.getenv("EFFECT_EMBEDDING_LOOKBACK_DAYS") or lookback_days)
    except ValueError:
        lookback_days = 7
    lookback_days = max(1, min(60, lookback_days))
    if intraday_threshold is None:
        intraday_threshold = _effect_embedding_intraday_hard_threshold()
    if crossday_threshold is None:
        crossday_threshold = _effect_embedding_crossday_hard_threshold()

    try:
        from ua_workflows.shared.llm.client import (
            bytes_to_embedding,
            call_embedding,
            cosine_similarity,
            embedding_to_bytes,
        )
    except Exception:
        return 0, []

    embed_cache: Dict[str, List[float]] = {}

    def embed(text: str) -> Optional[List[float]]:
        effect = str(text or "").strip()
        if not effect or effect == "None":
            return None
        prompt_text = f"特效玩法：{effect[:300]}"
        if prompt_text not in embed_cache:
            try:
                embed_cache[prompt_text] = call_embedding(prompt_text)
            except Exception:
                return None
        return embed_cache.get(prompt_text)

    hist_map = _load_effect_embedding_history_rows(target_date, lookback_days)

    def hist_vec(hist: Dict[str, Any]) -> Optional[List[float]]:
        blob = hist.get("effect_one_liner_embedding")
        if blob:
            try:
                return bytes_to_embedding(blob)
            except Exception:
                pass
        vec = embed(str(hist.get("effect_one_liner") or ""))
        if vec is not None:
            ad_key = str(hist.get("ad_key") or "")
            if ad_key:
                try:
                    upsert_effect_one_liner_embedding(ad_key, embedding_to_bytes(vec))
                except Exception:
                    pass
        return vec

    row_vec_cache: Dict[int, List[float]] = {}

    def row_vec(idx: int) -> Optional[List[float]]:
        if idx in row_vec_cache:
            return row_vec_cache[idx]
        effect = _play_dedupe_text(rows[idx])
        vec = embed(effect)
        if vec is not None:
            row_vec_cache[idx] = vec
            ad_key = str(rows[idx].get("ad_key") or "")
            if ad_key:
                try:
                    upsert_effect_one_liner_embedding(ad_key, embedding_to_bytes(vec))
                except Exception:
                    pass
        return vec

    def mark_duplicate(
        row: Dict[str, Any],
        *,
        source: str,
        similarity: float,
        threshold: float,
        match: Dict[str, Any],
    ) -> bool:
        was_excluded = bool(row.get("exclude_from_bitable"))
        row["exclude_from_bitable"] = True
        row["exclude_from_cluster"] = True
        row["effect_embedding_duplicate_match"] = {
            "source": source,
            "similarity": round(float(similarity), 3),
            "threshold": round(float(threshold), 3),
            "scope": "play_fingerprint",
            "action": "exclude_from_bitable",
            "lookback_days": lookback_days,
            **match,
        }
        _append_material_tag(row, "embedding玩法重复")
        return not was_excluded

    details: List[Dict[str, Any]] = []
    newly_marked = 0

    by_app: Dict[str, List[int]] = defaultdict(list)
    for idx, row in enumerate(rows):
        if not isinstance(row, dict) or row.get("exclude_from_bitable"):
            continue
        appid = str(row.get("appid") or "").strip()
        effect = _play_dedupe_text(row)
        if appid and effect:
            by_app[appid].append(idx)

    # Same-day high-confidence duplicate plays. Keep the strongest representative.
    for indices in by_app.values():
        kept_indices: List[int] = []
        for idx in sorted(indices, key=lambda i: _effect_row_score(rows[i]), reverse=True):
            vec = row_vec(idx)
            if vec is None:
                kept_indices.append(idx)
                continue
            best_keeper_idx: int | None = None
            best_sim = 0.0
            for kept_idx in kept_indices:
                kvec = row_vec(kept_idx)
                if kvec is None:
                    continue
                sim = float(cosine_similarity(vec, kvec))
                if sim > best_sim:
                    best_sim = sim
                    best_keeper_idx = kept_idx
            if best_keeper_idx is not None and best_sim >= intraday_threshold:
                keeper = rows[best_keeper_idx]
                row = rows[idx]
                match = {
                    "matched_ad_key": str(keeper.get("ad_key") or ""),
                    "matched_effect_one_liner": str(keeper.get("effect_one_liner") or ""),
                    "matched_play_fingerprint": _play_dedupe_text(keeper),
                    "matched_date": target_date,
                }
                is_new = mark_duplicate(
                    row,
                    source="same_day_effect_embedding",
                    similarity=best_sim,
                    threshold=intraday_threshold,
                    match=match,
                )
                if is_new:
                    newly_marked += 1
                details.append(
                    {
                        "ad_key": str(row.get("ad_key") or ""),
                        "product": str(row.get("product") or ""),
                        "effect_one_liner": str(row.get("effect_one_liner") or ""),
                        "play_fingerprint": _play_dedupe_text(row),
                        "source": "same_day_effect_embedding",
                        "similarity": round(float(best_sim), 3),
                        "threshold": round(float(intraday_threshold), 3),
                        "newly_marked": is_new,
                        **match,
                    }
                )
            else:
                kept_indices.append(idx)

    # Cross-day high-confidence duplicate plays against recent history.
    for idx, row in enumerate(rows):
        if not isinstance(row, dict) or row.get("exclude_from_bitable"):
            continue
        appid = str(row.get("appid") or "").strip()
        vec = row_vec(idx)
        if not appid or vec is None:
            continue
        best_hist: Optional[Dict[str, Any]] = None
        best_sim = 0.0
        for hist in hist_map.get(appid, []):
            hvec = hist_vec(hist)
            if hvec is None:
                continue
            sim = float(cosine_similarity(vec, hvec))
            if sim > best_sim:
                best_sim = sim
                best_hist = hist
        if best_hist is None or best_sim < crossday_threshold:
            continue
        match = {
            "matched_ad_key": str(best_hist.get("ad_key") or ""),
            "matched_effect_one_liner": str(best_hist.get("display_effect_one_liner") or best_hist.get("effect_one_liner") or ""),
            "matched_play_fingerprint": str(best_hist.get("play_fingerprint") or best_hist.get("effect_one_liner") or ""),
            "matched_first_seen_date": str(best_hist.get("first_target_date") or ""),
            "matched_latest_seen_date": str(best_hist.get("last_target_date") or ""),
        }
        is_new = mark_duplicate(
            row,
            source="crossday_effect_embedding",
            similarity=best_sim,
            threshold=crossday_threshold,
            match=match,
        )
        if is_new:
            newly_marked += 1
        details.append(
            {
                "ad_key": str(row.get("ad_key") or ""),
                "product": str(row.get("product") or ""),
                "effect_one_liner": str(row.get("effect_one_liner") or ""),
                "play_fingerprint": _play_dedupe_text(row),
                "source": "crossday_effect_embedding",
                "similarity": round(float(best_sim), 3),
                "threshold": round(float(crossday_threshold), 3),
                "newly_marked": is_new,
                **match,
            }
        )

    return newly_marked, details


def apply_embedding_duplicate_candidate_tags(
    target_date: str,
    rows: List[Dict[str, Any]],
    lookback_days: int = 7,
    threshold: float | None = None,
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Mark embedding-based duplicate candidates without excluding them.

    This is a soft calibration signal only: it appends `embedding重复候选` and writes
    match metadata, but deliberately does not set exclude_from_bitable/cluster.
    """
    if not rows:
        return 0, []
    if threshold is None:
        threshold = _embedding_duplicate_candidate_threshold()
    try:
        lookback_days = int(os.getenv("EMBEDDING_DUP_CANDIDATE_LOOKBACK_DAYS") or lookback_days)
    except ValueError:
        lookback_days = 7
    lookback_days = max(1, min(60, lookback_days))

    try:
        from ua_workflows.shared.llm.client import call_embedding, cosine_similarity
    except Exception:
        return 0, []

    embed_cache: Dict[str, List[float]] = {}

    def embed(text: str) -> Optional[List[float]]:
        t = str(text or "").strip()
        if not t:
            return None
        if t not in embed_cache:
            try:
                embed_cache[t] = call_embedding(t[:300])
            except Exception:
                return None
        return embed_cache.get(t)

    conn = _get_conn()
    try:
        cur = conn.cursor()
        hist_map = _load_effect_history_rows(cur, target_date, lookback_days)
    finally:
        conn.close()

    candidates: List[Tuple[int, str, Dict[str, Any], float]] = []

    by_app: Dict[str, List[int]] = defaultdict(list)
    for idx, row in enumerate(rows):
        if not isinstance(row, dict) or row.get("exclude_from_bitable"):
            continue
        appid = str(row.get("appid") or "").strip()
        effect = _play_dedupe_text(row)
        if appid and effect:
            by_app[appid].append(idx)

    # Same-day soft candidates: compare to kept representatives, ordered by strength.
    for indices in by_app.values():
        kept_indices: List[int] = []
        for idx in sorted(indices, key=lambda i: _effect_row_score(rows[i]), reverse=True):
            row = rows[idx]
            effect = _play_dedupe_text(row)
            vec = embed(effect)
            if vec is None:
                kept_indices.append(idx)
                continue
            best_keeper_idx: int | None = None
            best_sim = 0.0
            for kept_idx in kept_indices:
                kvec = embed(_play_dedupe_text(rows[kept_idx]))
                if kvec is None:
                    continue
                sim = float(cosine_similarity(vec, kvec))
                if sim > best_sim:
                    best_sim = sim
                    best_keeper_idx = kept_idx
            if best_keeper_idx is not None and best_sim >= threshold:
                keeper = rows[best_keeper_idx]
                candidates.append(
                    (
                        idx,
                        "same_day_effect_embedding",
                        {
                            "matched_ad_key": str(keeper.get("ad_key") or ""),
                            "matched_effect_one_liner": str(keeper.get("effect_one_liner") or ""),
                            "matched_play_fingerprint": _play_dedupe_text(keeper),
                        },
                        best_sim,
                    )
                )
            kept_indices.append(idx)

    # Cross-day soft candidates against recent history.
    for idx, row in enumerate(rows):
        if not isinstance(row, dict) or row.get("exclude_from_bitable"):
            continue
        appid = str(row.get("appid") or "").strip()
        effect = _play_dedupe_text(row)
        vec = embed(effect)
        if not appid or vec is None:
            continue
        best_hist: Optional[Dict[str, Any]] = None
        best_sim = 0.0
        for hist in hist_map.get(appid, []):
            hvec = embed(str(hist.get("effect_one_liner") or ""))
            if hvec is None:
                continue
            sim = float(cosine_similarity(vec, hvec))
            if sim > best_sim:
                best_sim = sim
                best_hist = hist
        if best_hist and best_sim >= threshold:
            candidates.append(
                (
                    idx,
                    "crossday_effect_embedding",
                    {
                        "matched_effect_one_liner": str(best_hist.get("effect_one_liner") or ""),
                        "matched_play_fingerprint": str(best_hist.get("play_fingerprint") or best_hist.get("effect_one_liner") or ""),
                        "matched_first_seen_date": str(best_hist.get("earliest_date") or ""),
                        "matched_latest_seen_date": str(best_hist.get("latest_date") or ""),
                    },
                    best_sim,
                )
            )

    best_by_idx: Dict[int, Tuple[str, Dict[str, Any], float]] = {}
    for idx, source, match, sim in candidates:
        prev = best_by_idx.get(idx)
        if prev is None or sim > prev[2]:
            best_by_idx[idx] = (source, match, sim)

    details: List[Dict[str, Any]] = []
    for idx, (source, match, sim) in best_by_idx.items():
        row = rows[idx]
        row["embedding_duplicate_candidate"] = {
            "source": source,
            "similarity": round(float(sim), 3),
            "threshold": round(float(threshold), 3),
            "scope": "play_fingerprint",
            **match,
        }
        _append_material_tag(row, "embedding重复候选")
        details.append(
            {
                "ad_key": str(row.get("ad_key") or ""),
                "product": str(row.get("product") or ""),
                "effect_one_liner": str(row.get("effect_one_liner") or ""),
                "play_fingerprint": _play_dedupe_text(row),
                "source": source,
                "similarity": round(float(sim), 3),
                **match,
            }
        )
    return len(details), details


def _load_effect_history_rows(
    cur: sqlite3.Cursor,
    target_date: str,
    lookback_days: int,
) -> Dict[str, List[Dict[str, Any]]]:
    from datetime import date, timedelta

    try:
        td = date.fromisoformat(target_date)
    except ValueError:
        return {}
    start_date = (td - timedelta(days=lookback_days)).isoformat()
    cur.execute(
        """
        SELECT appid,
               COALESCE(NULLIF(TRIM(play_fingerprint), ''), effect_one_liner) AS dedupe_text,
               MIN(effect_one_liner) AS display_effect_one_liner,
               MIN(play_fingerprint) AS play_fingerprint,
               MIN(first_target_date) AS earliest_date,
               MAX(last_target_date) AS latest_date,
               COUNT(*) AS hist_count,
               MAX(best_impression) AS max_impression
        FROM creative_library
        WHERE first_target_date >= ? AND first_target_date < ?
          AND COALESCE(NULLIF(TRIM(play_fingerprint), ''), TRIM(effect_one_liner), '') != ''
        GROUP BY appid, dedupe_text
        """,
        (start_date, target_date),
        )
    by_app: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in cur.fetchall():
        appid = str(row["appid"] or "").strip()
        eff = str(row["dedupe_text"] or "").strip()
        if not appid or not eff or eff == "None":
            continue
        by_app[appid].append(
            {
                "effect_one_liner": eff,
                "display_effect_one_liner": str(row["display_effect_one_liner"] or ""),
                "play_fingerprint": str(row["play_fingerprint"] or ""),
                "normalized_effect": normalize_effect_one_liner(eff),
                "earliest_date": str(row["earliest_date"] or ""),
                "latest_date": str(row["latest_date"] or ""),
                "history_count": int(row["hist_count"] or 0),
                "max_impression": int(row["max_impression"] or 0),
            }
        )
    return dict(by_app)


def _load_play_cluster_history_rows(
    cur: sqlite3.Cursor,
    target_date: str,
    lookback_days: int,
) -> Dict[str, List[Dict[str, Any]]]:
    from datetime import date, timedelta

    try:
        td = date.fromisoformat(target_date)
    except ValueError:
        return {}
    start_date = (td - timedelta(days=lookback_days)).isoformat()
    cur.execute(
        """
        SELECT cl.ad_key, cl.appid, cl.effect_one_liner, cl.play_fingerprint,
               cl.first_target_date, cl.last_target_date, cl.best_impression
        FROM creative_library cl
        WHERE cl.first_target_date >= ? AND cl.first_target_date < ?
          AND COALESCE(NULLIF(TRIM(cl.play_fingerprint), ''), TRIM(cl.effect_one_liner), '') != ''
          AND NOT EXISTS (
            SELECT 1
            FROM daily_creative_insights d
            WHERE d.target_date = cl.first_target_date
              AND COALESCE(d.exclude_from_bitable, 0) = 1
              AND (
                d.ad_key = cl.ad_key
                OR d.ad_key LIKE SUBSTR(cl.ad_key, 1, 16) || '%'
                OR cl.ad_key LIKE SUBSTR(d.ad_key, 1, 16) || '%'
              )
          )
        """,
        (start_date, target_date),
    )

    buckets: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in cur.fetchall():
        appid = str(row["appid"] or "").strip()
        if not appid:
            continue
        cluster_key = _coarse_play_cluster_key(row["play_fingerprint"], row["effect_one_liner"])
        if not cluster_key or cluster_key == "None":
            continue
        key = (appid, cluster_key)
        bucket = buckets.get(key)
        first_date = str(row["first_target_date"] or "")
        latest_date = str(row["last_target_date"] or first_date)
        impression = int(row["best_impression"] or 0)
        if bucket is None:
            buckets[key] = {
                "effect_one_liner": cluster_key,
                "display_effect_one_liner": str(row["effect_one_liner"] or ""),
                "play_fingerprint": cluster_key,
                "normalized_effect": normalize_effect_one_liner(cluster_key),
                "earliest_date": first_date,
                "latest_date": latest_date,
                "history_count": 1,
                "max_impression": impression,
            }
            continue
        if first_date and (not bucket.get("earliest_date") or first_date < str(bucket.get("earliest_date"))):
            bucket["earliest_date"] = first_date
        if latest_date and latest_date > str(bucket.get("latest_date") or ""):
            bucket["latest_date"] = latest_date
        bucket["history_count"] = int(bucket.get("history_count") or 0) + 1
        if impression > int(bucket.get("max_impression") or 0):
            bucket["max_impression"] = impression
            bucket["display_effect_one_liner"] = str(row["effect_one_liner"] or "")

    by_app: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for (appid, _cluster_key), row in buckets.items():
        by_app[appid].append(row)
    return dict(by_app)


def _best_effect_history_match(
    effect_one_liner: str,
    history_rows: List[Dict[str, Any]],
    threshold: float,
) -> Optional[Dict[str, Any]]:
    best: Optional[Dict[str, Any]] = None
    best_sim = 0.0
    for hist in history_rows:
        sim = _effect_text_similarity(effect_one_liner, str(hist.get("effect_one_liner") or ""))
        if sim > best_sim:
            best_sim = sim
            best = hist
    if best is None or best_sim < threshold:
        return None
    out = dict(best)
    out["similarity"] = round(best_sim, 3)
    out["match_type"] = "exact" if best_sim >= 0.999 else "similar"
    return out


def _global_play_history_rows(
    hist_by_app: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    buckets: Dict[str, Dict[str, Any]] = {}
    for rows in hist_by_app.values():
        for row in rows:
            effect = str(row.get("effect_one_liner") or "").strip()
            if not effect:
                continue
            bucket = buckets.get(effect)
            if bucket is None:
                buckets[effect] = dict(row)
                continue
            earliest = str(row.get("earliest_date") or "")
            latest = str(row.get("latest_date") or "")
            if earliest and (not bucket.get("earliest_date") or earliest < str(bucket.get("earliest_date"))):
                bucket["earliest_date"] = earliest
            if latest and latest > str(bucket.get("latest_date") or ""):
                bucket["latest_date"] = latest
            bucket["history_count"] = int(bucket.get("history_count") or 0) + int(row.get("history_count") or 0)
            if int(row.get("max_impression") or 0) > int(bucket.get("max_impression") or 0):
                bucket["max_impression"] = int(row.get("max_impression") or 0)
                bucket["display_effect_one_liner"] = str(row.get("display_effect_one_liner") or "")
    return list(buckets.values())


def apply_old_effect_bitable_filter(
    target_date: str,
    rows: List[Dict[str, Any]],
    lookback_days: int = 7,
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Mark rows whose same-app effect_one_liner has appeared in the recent history.

    This is intentionally stricter than the daily report wording: matched rows are
    excluded from the Bitable main table and direction cards so the daily push focuses
    on new material/new play candidates.
    """
    if not rows:
        return 0, []
    init_db()
    try:
        lookback_days = int(os.getenv("OLD_EFFECT_LOOKBACK_DAYS") or lookback_days)
    except ValueError:
        lookback_days = 7
    lookback_days = max(1, min(60, lookback_days))

    conn = _get_conn()
    try:
        cur = conn.cursor()
        hist_map = _load_effect_history_rows(cur, target_date, lookback_days)
    finally:
        conn.close()

    threshold = _old_effect_bitable_similarity_threshold()
    details: List[Dict[str, Any]] = []
    newly_marked = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        appid = str(row.get("appid") or "").strip()
        effect = _play_dedupe_text(row)
        if not appid or not effect:
            continue
        hit = _best_effect_history_match(effect, hist_map.get(appid, []), threshold)
        if not hit:
            continue

        was_excluded = bool(row.get("exclude_from_bitable"))
        row["exclude_from_bitable"] = True
        row["exclude_from_cluster"] = True
        row["old_effect_match"] = {
            "matched_effect_one_liner": str(hit.get("display_effect_one_liner") or hit.get("effect_one_liner") or ""),
            "matched_play_fingerprint": str(hit.get("play_fingerprint") or hit.get("effect_one_liner") or ""),
            "first_seen_date": str(hit.get("earliest_date") or ""),
            "latest_seen_date": str(hit.get("latest_date") or ""),
            "history_count": int(hit.get("history_count") or 0),
            "similarity": float(hit.get("similarity") or 0.0),
            "match_type": str(hit.get("match_type") or ""),
            "lookback_days": lookback_days,
        }
        tags = row.get("material_tags")
        if not isinstance(tags, list):
            tags = []
        if "老玩法重复" not in tags:
            tags.append("老玩法重复")
        row["material_tags"] = tags
        if not was_excluded:
            newly_marked += 1
        details.append(
            {
                "ad_key": str(row.get("ad_key") or ""),
                "product": str(row.get("product") or ""),
                "effect_one_liner": effect,
                "matched_effect_one_liner": str(hit.get("display_effect_one_liner") or hit.get("effect_one_liner") or ""),
                "matched_play_fingerprint": str(hit.get("play_fingerprint") or hit.get("effect_one_liner") or ""),
                "first_seen_date": str(hit.get("earliest_date") or ""),
                "similarity": float(hit.get("similarity") or 0.0),
                "match_type": str(hit.get("match_type") or ""),
                "newly_marked": not was_excluded,
            }
        )
    return newly_marked, details


def effect_based_crossday_dedup(
    target_date: str,
    items: List[Dict[str, Any]],
    lookback_days: int = 7,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    用 effect_one_liner 做跨天去重：同 appid 内，若某素材的 effect_one_liner
    在历史 N 天内已出现过或高度相似，则标记为「非新玩法」。

    返回 (report, updated_items)。
    report 包含：
      - total_input: 输入素材数
      - new_count: 新素材数（历史未出现过该特效玩法）
      - seen_count: 非新素材数
      - new_items: 新素材列表
      - seen_items: 非新素材列表
      - history_dates: 查询的历史日期范围
    updated_items 是输入 items 的副本，每条增加了：
      - effect_is_new: bool
      - effect_first_seen_date: str (历史上首次出现该玩法的日期，新素材则为 target_date)
      - effect_history_count: int (历史上出现该玩法的素材条数)
    """
    init_db()
    from datetime import date, timedelta
    try:
        td = date.fromisoformat(target_date)
    except ValueError:
        return {"total_input": len(items), "new_count": 0, "seen_count": 0}, items

    start_date = (td - timedelta(days=lookback_days)).isoformat()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        hist_map = _load_effect_history_rows(cur, target_date, lookback_days)
    finally:
        conn.close()

    report: Dict[str, Any] = {
        "total_input": len(items),
        "new_count": 0,
        "seen_count": 0,
        "new_items": [],
        "seen_items": [],
        "history_dates": f"{start_date}~{target_date}",
        "lookback_days": lookback_days,
    }
    updated: List[Dict[str, Any]] = []
    for item in items:
        item2 = dict(item)
        c = item2.get("creative") or {}
        if not isinstance(c, dict):
            c = {}
        appid = str(item2.get("appid") or "").strip()
        eff = _play_dedupe_text_from_values(
            c.get("play_fingerprint") or item2.get("play_fingerprint"),
            c.get("effect_one_liner") or item2.get("effect_one_liner"),
        )

        # 查 analysis_by_ad 中的 effect_one_liner
        analysis_raw = item2.get("analysis_raw")
        if not eff and isinstance(analysis_raw, dict):
            eff = _play_dedupe_text_from_values(
                analysis_raw.get("play_fingerprint"),
                analysis_raw.get("effect_one_liner"),
            )

        is_new = True
        first_date = target_date
        hist_count = 0
        hit: Optional[Dict[str, Any]] = None
        if appid and eff:
            hit = _best_effect_history_match(
                eff,
                hist_map.get(appid, []),
                _effect_similarity_threshold(),
            )
            if hit:
                is_new = False
                first_date = hit["earliest_date"]
                hist_count = int(hit.get("history_count") or 0)

        item2["effect_is_new"] = is_new
        item2["effect_first_seen_date"] = first_date
        item2["effect_history_count"] = hist_count
        if not is_new and hit:
            item2["effect_matched_one_liner"] = hit.get("effect_one_liner")
            item2["effect_match_similarity"] = hit.get("similarity")
            item2["effect_match_type"] = hit.get("match_type")

        if is_new:
            report["new_count"] += 1
            report["new_items"].append({
                "ad_key": str(c.get("ad_key") or ""),
                "product": str(item2.get("product") or ""),
                "effect_one_liner": eff,
            })
        else:
            report["seen_count"] += 1
            report["seen_items"].append({
                "ad_key": str(c.get("ad_key") or ""),
                "product": str(item2.get("product") or ""),
                "effect_one_liner": eff,
                "first_seen_date": first_date,
                "history_count": hist_count,
            })
        updated.append(item2)

    return report, updated


def compute_sustained_effort_signals(
    target_date: str,
    lookback_days: int = 7,
) -> Dict[str, Any]:
    """
    基于去重流程中被去掉的素材，汇总持续发力信号。

    三个来源：
      1. 封面跨日指纹去掉的素材（从 cover_style 报告 JSON 读取）
         → 同画面/URL跨天重复出现 = 素材本身在持续投
      2. ahash 去重组跨天的素材（从 creative_library 查询）
         → 同一画面换了 ad_key 反复投
      3. effect_one_liner 跨天的素材（从 creative_library 查询）
         → 同一玩法换了不同画面继续投

    返回：
      {
        "target_date": str,
        "lookback_days": int,
        "cover_crossday_removed": [  # 来源1: 封面去重去掉的
          {ad_key, matched_ad_key, matched_date, reason, product, appid}
        ],
        "ahash_group_crossday": [    # 来源2: ahash去重组跨天
          {dedup_group_id, products, day_span, material_count, max_impression,
           earliest_date, latest_date, ad_keys_sample}
        ],
        "effect_crossday": [         # 来源3: 特效玩法跨天
          {effect_one_liner, product, day_span, material_count, max_impression,
           earliest_date, latest_date}
        ],
        "summary": {
          "cover_crossday_removed_count": int,
          "ahash_group_count": int,
          "effect_crossday_count": int,
          "total_sustained_signals": int,
        }
      }
    """
    init_db()
    from datetime import date, timedelta
    try:
        td = date.fromisoformat(target_date)
    except ValueError:
        return {"target_date": target_date, "lookback_days": lookback_days, "summary": {}}

    start_date = (td - timedelta(days=lookback_days)).isoformat()
    conn = _get_conn()
    try:
        cur = conn.cursor()

        # --- 来源2: ahash 去重组跨天 ---
        cur.execute(
            """
            SELECT dedup_group_id,
                   COUNT(DISTINCT first_target_date) as day_span,
                   COUNT(*) as material_count,
                   MAX(best_impression) as max_impression,
                   MIN(first_target_date) as earliest_date,
                   MAX(last_target_date) as latest_date,
                   GROUP_CONCAT(DISTINCT product) as products
            FROM creative_library
            WHERE first_target_date >= ? AND first_target_date <= ?
            GROUP BY dedup_group_id
            HAVING day_span >= 2 AND latest_date = ?
            ORDER BY max_impression DESC
            """,
            (start_date, target_date, target_date),
        )
        ahash_groups: List[Dict[str, Any]] = []
        for r in cur.fetchall():
            gid = str(r["dedup_group_id"] or "")
            cur2 = conn.cursor()
            cur2.execute(
                "SELECT ad_key, first_target_date FROM creative_library "
                "WHERE dedup_group_id = ? ORDER BY best_impression DESC LIMIT 5",
                (gid,),
            )
            samples = [
                {"ad_key": str(row["ad_key"] or ""), "date": str(row["first_target_date"] or "")}
                for row in cur2.fetchall()
            ]
            ahash_groups.append({
                "dedup_group_id": gid,
                "products": str(r["products"] or ""),
                "day_span": int(r["day_span"] or 0),
                "material_count": int(r["material_count"] or 0),
                "max_impression": int(r["max_impression"] or 0),
                "earliest_date": str(r["earliest_date"] or ""),
                "latest_date": str(r["latest_date"] or ""),
                "ad_keys_sample": samples,
            })
            # 追溯：查组内代表素材的 effect_one_liner + 媒体链接
            _cur3 = conn.cursor()
            _cur3.execute(
                "SELECT ad_key, effect_one_liner, product, video_url, preview_img_url, "
                "video_duration, best_impression FROM creative_library "
                "WHERE dedup_group_id = ? AND is_canonical = 1 LIMIT 1",
                (gid,),
            )
            canonical = _cur3.fetchone()
            if canonical:
                ahash_groups[-1]["canonical_effect_one_liner"] = str(canonical["effect_one_liner"] or "")
                ahash_groups[-1]["canonical_product"] = str(canonical["product"] or "")
                ahash_groups[-1]["canonical_video_url"] = str(canonical["video_url"] or "")
                ahash_groups[-1]["canonical_preview_img_url"] = str(canonical["preview_img_url"] or "")
                ahash_groups[-1]["canonical_video_duration"] = int(canonical["video_duration"] or 0)
                ahash_groups[-1]["canonical_best_impression"] = int(canonical["best_impression"] or 0)
            else:
                # 无 canonical 时用组内第一条 sample 的信息
                if samples:
                    sk = samples[0]["ad_key"]
                    _cur3.execute(
                        "SELECT effect_one_liner, product, video_url, preview_img_url, "
                        "video_duration FROM creative_library WHERE ad_key = ? LIMIT 1",
                        (sk,),
                    )
                    srow = _cur3.fetchone()
                    if srow:
                        ahash_groups[-1]["canonical_effect_one_liner"] = str(srow["effect_one_liner"] or "")
                        ahash_groups[-1]["canonical_product"] = str(srow["product"] or "")
                        ahash_groups[-1]["canonical_video_url"] = str(srow["video_url"] or "")
                        ahash_groups[-1]["canonical_preview_img_url"] = str(srow["preview_img_url"] or "")
                        ahash_groups[-1]["canonical_video_duration"] = int(srow["video_duration"] or 0)

        # --- 来源3: effect_one_liner 跨天 ---
        cur.execute(
            """
            SELECT appid, effect_one_liner,
                   COUNT(DISTINCT first_target_date) as day_span,
                   COUNT(*) as material_count,
                   MAX(best_impression) as max_impression,
                   MIN(first_target_date) as earliest_date,
                   MAX(last_target_date) as latest_date,
                   GROUP_CONCAT(DISTINCT product) as products
            FROM creative_library
            WHERE first_target_date >= ? AND first_target_date <= ?
              AND COALESCE(TRIM(effect_one_liner), '') != ''
            GROUP BY appid, effect_one_liner
            HAVING day_span >= 2 AND latest_date = ?
            ORDER BY max_impression DESC
            """,
            (start_date, target_date, target_date),
        )
        effect_crossday: List[Dict[str, Any]] = []
        for r in cur.fetchall():
            eff = str(r["effect_one_liner"] or "")
            appid = str(r["appid"] or "")
            entry = {
                "effect_one_liner": eff,
                "product": str(r["products"] or ""),
                "appid": appid,
                "day_span": int(r["day_span"] or 0),
                "material_count": int(r["material_count"] or 0),
                "max_impression": int(r["max_impression"] or 0),
                "earliest_date": str(r["earliest_date"] or ""),
                "latest_date": str(r["latest_date"] or ""),
            }
            # 追溯：查该玩法下展示量最高的一条素材的媒体链接
            _cur4 = conn.cursor()
            _cur4.execute(
                "SELECT ad_key, product, video_url, preview_img_url, video_duration "
                "FROM creative_library WHERE effect_one_liner = ? AND appid = ? "
                "ORDER BY best_impression DESC LIMIT 1",
                (eff, appid),
            )
            top_row = _cur4.fetchone()
            if top_row:
                entry["top_ad_key"] = str(top_row["ad_key"] or "")
                entry["top_product"] = str(top_row["product"] or "")
                entry["top_video_url"] = str(top_row["video_url"] or "")
                entry["top_preview_img_url"] = str(top_row["preview_img_url"] or "")
                entry["top_video_duration"] = int(top_row["video_duration"] or 0)
            effect_crossday.append(entry)

    finally:
        conn.close()

    # --- 来源1: 封面跨日指纹去掉的 + CLIP向量去重中去掉的（从 JSON 报告文件读取） ---
    cover_crossday: List[Dict[str, Any]] = []
    _conn2 = _get_conn()
    _cur2 = _conn2.cursor()
    try:
        for ds in (target_date,):
            fname = f"workflow_video_enhancer_{ds}_cover_style_intraday.json"
            fpath = os.path.join(str(DATA_DIR), fname)
            if os.path.exists(fpath):
                with open(fpath, "r", encoding="utf-8") as fh:
                    rdata = json.load(fh)
                # 1a: cross_day_fingerprint_removed（指纹去重）
                for item in rdata.get("cross_day_fingerprint_removed", []):
                    item2 = dict(item)
                    item2["report_date"] = ds
                    item2["source"] = "fingerprint"
                    matched_key = item2.get("matched_ad_key", "")
                    if matched_key:
                        _cur2.execute(
                            "SELECT effect_one_liner, product, video_url, preview_img_url, "
                            "video_duration FROM creative_library WHERE ad_key = ? LIMIT 1",
                            (matched_key,),
                        )
                        mrow = _cur2.fetchone()
                        if mrow:
                            eff = str(mrow["effect_one_liner"] or "")
                            if eff == "None":
                                eff = ""
                            item2["matched_effect_one_liner"] = eff
                            item2["matched_product"] = str(mrow["product"] or "")
                            item2["matched_video_url"] = str(mrow["video_url"] or "")
                            item2["matched_preview_img_url"] = str(mrow["preview_img_url"] or "")
                            item2["matched_video_duration"] = int(mrow["video_duration"] or 0)
                    cover_crossday.append(item2)
                # 1b: CLIP 向量去重中去掉的（per_appid[].removed）
                for pa in rdata.get("per_appid", []):
                    pa_product = pa.get("product", "")
                    for rem in pa.get("removed", []):
                        reason = rem.get("reason", "")
                        kept_key = rem.get("kept_ad_key", "")
                        rem_ad_key = rem.get("ad_key", "")
                        if not kept_key or reason not in (
                            "cover_style_cluster_vs_yesterday",
                            "cover_style_cluster",
                        ):
                            continue
                        entry = {
                            "ad_key": rem_ad_key,
                            "reason": reason,
                            "kept_ad_key": kept_key,
                            "report_date": ds,
                            "source": "clip_cluster",
                            "product": pa_product,
                        }
                        # 追溯：查被保留素材的 effect_one_liner + 媒体链接
                        _cur2.execute(
                            "SELECT effect_one_liner, product, video_url, preview_img_url, "
                            "video_duration FROM creative_library WHERE ad_key = ? LIMIT 1",
                            (kept_key,),
                        )
                        krow = _cur2.fetchone()
                        if krow:
                            eff = str(krow["effect_one_liner"] or "")
                            if eff == "None":
                                eff = ""
                            entry["matched_effect_one_liner"] = eff
                            entry["matched_product"] = str(krow["product"] or pa_product)
                            entry["matched_video_url"] = str(krow["video_url"] or "")
                            entry["matched_preview_img_url"] = str(krow["preview_img_url"] or "")
                            entry["matched_video_duration"] = int(krow["video_duration"] or 0)
                        cover_crossday.append(entry)
    except Exception:
        pass
    finally:
        _conn2.close()

    ahash_cnt = len(ahash_groups)
    effect_cnt = len(effect_crossday)
    cover_cnt = len(cover_crossday)

    return {
        "target_date": target_date,
        "lookback_days": lookback_days,
        "cover_crossday_removed": cover_crossday,
        "ahash_group_crossday": ahash_groups,
        "effect_crossday": effect_crossday,
        "summary": {
            "cover_crossday_removed_count": cover_cnt,
            "ahash_group_count": ahash_cnt,
            "effect_crossday_count": effect_cnt,
            "total_sustained_signals": cover_cnt + ahash_cnt + effect_cnt,
        },
    }


def load_new_creatives_for_date(
    target_date: str,
) -> List[Dict[str, Any]]:
    """
    返回指定日期首次出现的素材候选—— creative_library.first_target_date = target_date。
    日报里的严格「新素材」还会在 load_daily_material_report 中继续要求玩法也为新。
    """
    init_db()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT cl.ad_key, cl.product, cl.appid, cl.platform, cl.creative_type,
                   cl.title, cl.best_heat, cl.best_impression, cl.best_all_exposure_value,
                   cl.effect_one_liner, cl.play_fingerprint, cl.differentiator,
                   cl.play_asset_id, cl.play_asset_name, cl.play_asset_subtag_ids, cl.play_asset_subtag_names,
                   cl.play_asset_novelty_label, cl.play_asset_match_source, cl.play_asset_classification_reason,
                   cl.first_target_date, cl.dedup_group_id,
                   cl.preview_img_url, cl.video_url, cl.video_duration
            FROM creative_library cl
            WHERE cl.first_target_date = ?
              AND NOT EXISTS (
                SELECT 1
                FROM daily_creative_insights d
                WHERE d.target_date = ?
                  AND COALESCE(d.exclude_from_bitable, 0) = 1
                  AND (
                    d.ad_key = cl.ad_key
                    OR d.ad_key LIKE SUBSTR(cl.ad_key, 1, 16) || '%'
                    OR cl.ad_key LIKE SUBSTR(d.ad_key, 1, 16) || '%'
                  )
              )
            ORDER BY cl.best_impression DESC
            """,
            (target_date, target_date),
        )
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _media_link_from_row(row: Dict[str, Any]) -> Tuple[str, bool]:
    video_url = str(row.get("video_url") or row.get("top_video_url") or row.get("canonical_video_url") or row.get("matched_video_url") or "")
    preview_url = str(row.get("preview_img_url") or row.get("top_preview_img_url") or row.get("canonical_preview_img_url") or row.get("matched_preview_img_url") or "")
    duration = int(row.get("video_duration") or row.get("top_video_duration") or row.get("canonical_video_duration") or row.get("matched_video_duration") or 0)
    if video_url and (duration > 0 or str(row.get("creative_type") or "").lower() == "video"):
        return video_url, True
    if preview_url:
        return preview_url, False
    return video_url or preview_url, duration > 0


def _append_sustained_display_item(
    bucket: Dict[str, List[Dict[str, Any]]],
    seen: set[str],
    item: Dict[str, Any],
) -> None:
    product = str(item.get("product") or item.get("matched_product") or item.get("top_product") or item.get("canonical_product") or "").strip()
    if not product:
        product = "未知"
    effect = str(
        item.get("effect_one_liner")
        or item.get("matched_effect_one_liner")
        or item.get("top_effect_one_liner")
        or item.get("canonical_effect_one_liner")
        or ""
    ).strip()
    if effect == "None":
        effect = ""
    link, is_video = _media_link_from_row(item)
    key = "|".join(
        [
            product,
            effect or str(item.get("ad_key") or item.get("top_ad_key") or item.get("dedup_group_id") or ""),
            link,
            str(item.get("source") or item.get("signal_type") or ""),
        ]
    )
    if key in seen:
        return
    seen.add(key)
    out = dict(item)
    out["product"] = product
    out["effect_one_liner"] = effect or "同画面/同玩法跨日重复投放"
    out["link"] = link
    out["is_video"] = is_video
    bucket[product].append(out)


def _excluded_ad_key_prefixes_for_date(target_date: str) -> set[str]:
    if not target_date:
        return set()
    init_db()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ad_key
            FROM daily_creative_insights
            WHERE target_date = ?
              AND COALESCE(exclude_from_bitable, 0) = 1
            """,
            (target_date,),
        )
        return {str(r["ad_key"] or "")[:16] for r in cur.fetchall() if str(r["ad_key"] or "").strip()}
    finally:
        conn.close()


def _sustained_item_is_excluded(item: Dict[str, Any], excluded_prefixes: set[str]) -> bool:
    if not excluded_prefixes:
        return False
    for key in (
        "ad_key",
        "matched_ad_key",
        "kept_ad_key",
        "top_ad_key",
        "canonical_ad_key",
    ):
        prefix = str(item.get(key) or "")[:16]
        if prefix and prefix in excluded_prefixes:
            return True
    return False


def _sustained_display_by_product(
    signals: Dict[str, Any],
    per_product_limit: int = 3,
) -> Dict[str, List[Dict[str, Any]]]:
    by_product: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    seen: set[str] = set()
    excluded_prefixes = _excluded_ad_key_prefixes_for_date(str(signals.get("target_date") or ""))

    for row in signals.get("effect_crossday") or []:
        if not isinstance(row, dict):
            continue
        item = dict(row)
        if _sustained_item_is_excluded(item, excluded_prefixes):
            continue
        item["signal_type"] = "effect"
        item["signal_label"] = "玩法跨天"
        item["product"] = str(row.get("top_product") or row.get("product") or "")
        item["video_url"] = row.get("top_video_url")
        item["preview_img_url"] = row.get("top_preview_img_url")
        item["video_duration"] = row.get("top_video_duration")
        _append_sustained_display_item(by_product, seen, item)

    for row in signals.get("ahash_group_crossday") or []:
        if not isinstance(row, dict):
            continue
        item = dict(row)
        if _sustained_item_is_excluded(item, excluded_prefixes):
            continue
        item["signal_type"] = "ahash"
        item["signal_label"] = "同画面换素材"
        item["product"] = str(row.get("canonical_product") or row.get("products") or "")
        item["effect_one_liner"] = str(row.get("canonical_effect_one_liner") or "")
        item["video_url"] = row.get("canonical_video_url")
        item["preview_img_url"] = row.get("canonical_preview_img_url")
        item["video_duration"] = row.get("canonical_video_duration")
        _append_sustained_display_item(by_product, seen, item)

    for row in signals.get("cover_crossday_removed") or []:
        if not isinstance(row, dict):
            continue
        item = dict(row)
        if _sustained_item_is_excluded(item, excluded_prefixes):
            continue
        item["signal_type"] = str(row.get("source") or "cover")
        item["signal_label"] = "同画面/URL复投"
        item["product"] = str(row.get("matched_product") or row.get("product") or "")
        item["effect_one_liner"] = str(row.get("matched_effect_one_liner") or "")
        item["video_url"] = row.get("matched_video_url")
        item["preview_img_url"] = row.get("matched_preview_img_url")
        item["video_duration"] = row.get("matched_video_duration")
        _append_sustained_display_item(by_product, seen, item)

    def score(row: Dict[str, Any]) -> Tuple[int, int, str]:
        return (
            int(row.get("max_impression") or row.get("canonical_best_impression") or 0),
            int(row.get("day_span") or row.get("material_count") or 0),
            str(row.get("effect_one_liner") or ""),
        )

    limited: Dict[str, List[Dict[str, Any]]] = {}
    for product, rows in by_product.items():
        rows2 = sorted(rows, key=score, reverse=True)
        limited[product] = rows2[: max(1, per_product_limit)]
    return limited


def _cluster_daily_new_play_items(
    target_date: str,
    items: List[Dict[str, Any]],
    *,
    text_threshold: float | None = None,
    embedding_threshold: float | None = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Cluster same-day new play materials by product without forcing a fixed count.

    The report should count "new play" as clusters of similar play fingerprints,
    not raw material rows. This keeps multiple captures of the same idea together
    while still allowing a product to have more than two plays when the data says so.
    """
    if not items:
        return [], []

    text_threshold = (
        _daily_play_cluster_text_threshold()
        if text_threshold is None
        else max(0.5, min(1.0, float(text_threshold)))
    )
    embedding_threshold = (
        _daily_play_cluster_embedding_threshold()
        if embedding_threshold is None
        else max(0.5, min(1.0, float(embedding_threshold)))
    )

    cosine_similarity = None
    call_embedding = None
    if _daily_play_cluster_embedding_enabled():
        try:
            from ua_workflows.shared.llm.client import call_embedding as _call_embedding
            from ua_workflows.shared.llm.client import cosine_similarity as _cosine_similarity

            call_embedding = _call_embedding
            cosine_similarity = _cosine_similarity
        except Exception:
            call_embedding = None
            cosine_similarity = None

    embed_cache: Dict[str, Optional[List[float]]] = {}

    def embed(text: str) -> Optional[List[float]]:
        if call_embedding is None:
            return None
        effect = str(text or "").strip()
        if not effect or effect == "None":
            return None
        prompt_text = f"特效玩法：{effect[:300]}"
        if prompt_text not in embed_cache:
            try:
                embed_cache[prompt_text] = call_embedding(prompt_text)
            except Exception:
                embed_cache[prompt_text] = None
        return embed_cache.get(prompt_text)

    by_app: Dict[str, List[int]] = defaultdict(list)
    for idx, item in enumerate(items):
        appid = str(item.get("appid") or "").strip()
        effect = _play_cluster_text(item)
        if appid and effect and effect != "None":
            by_app[appid].append(idx)

    representatives: List[Dict[str, Any]] = []
    clusters: List[Dict[str, Any]] = []

    for appid, indices in by_app.items():
        parent = {idx: idx for idx in indices}

        def find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a: int, b: int) -> None:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[rb] = ra

        edge_map: Dict[Tuple[int, int], Dict[str, Any]] = {}
        for pos, idx_a in enumerate(indices):
            text_a = _play_cluster_text(items[idx_a])
            for idx_b in indices[pos + 1 :]:
                text_b = _play_cluster_text(items[idx_b])
                text_sim = _effect_text_similarity(text_a, text_b)
                match_reason = ""
                match_score = 0.0
                embedding_sim: Optional[float] = None
                if text_sim >= text_threshold:
                    match_reason = "text"
                    match_score = text_sim
                elif cosine_similarity is not None:
                    vec_a = embed(text_a)
                    vec_b = embed(text_b)
                    if vec_a is not None and vec_b is not None:
                        embedding_sim = float(cosine_similarity(vec_a, vec_b))
                        if embedding_sim >= embedding_threshold:
                            match_reason = "embedding"
                            match_score = embedding_sim

                if not match_reason:
                    continue
                union(idx_a, idx_b)
                a, b = sorted((idx_a, idx_b))
                edge_map[(a, b)] = {
                    "a": str(items[a].get("ad_key") or ""),
                    "b": str(items[b].get("ad_key") or ""),
                    "reason": match_reason,
                    "similarity": round(float(match_score), 3),
                    "text_similarity": round(float(text_sim), 3),
                    "embedding_similarity": round(float(embedding_sim), 3) if embedding_sim is not None else None,
                    "a_fingerprint": _play_dedupe_text(items[a]),
                    "b_fingerprint": _play_dedupe_text(items[b]),
                    "a_cluster_key": _play_cluster_text(items[a]),
                    "b_cluster_key": _play_cluster_text(items[b]),
                }

        grouped: Dict[int, List[int]] = defaultdict(list)
        for idx in indices:
            grouped[find(idx)].append(idx)

        for group_indices in grouped.values():
            rep_idx = max(group_indices, key=lambda i: _effect_row_score(items[i]))
            rep = items[rep_idx]
            rep_key = str(rep.get("ad_key") or f"cluster_{len(clusters) + 1}")
            cluster_id = f"{target_date}:{appid}:{rep_key[:12]}"

            member_edges = [
                edge
                for (a, b), edge in edge_map.items()
                if a in group_indices and b in group_indices
            ]
            member_edges = sorted(
                member_edges,
                key=lambda e: float(e.get("similarity") or 0.0),
                reverse=True,
            )
            members = []
            for idx in sorted(group_indices, key=lambda i: _effect_row_score(items[i]), reverse=True):
                item = items[idx]
                is_rep = idx == rep_idx
                item["daily_play_cluster_id"] = cluster_id
                item["daily_play_cluster_key"] = _play_cluster_text(rep)
                item["daily_play_cluster_representative_ad_key"] = rep_key
                item["daily_play_cluster_size"] = len(group_indices)
                item["is_daily_play_representative"] = is_rep
                if not is_rep:
                    item["daily_play_cluster_matched_effect_one_liner"] = str(rep.get("effect_one_liner") or "")
                    item["daily_play_cluster_matched_play_fingerprint"] = _play_dedupe_text(rep)
                members.append(
                    {
                        "ad_key": str(item.get("ad_key") or ""),
                        "effect_one_liner": str(item.get("effect_one_liner") or ""),
                        "play_fingerprint": _play_dedupe_text(item),
                        "play_cluster_key": _play_cluster_text(item),
                        "differentiator": str(item.get("differentiator") or ""),
                        "best_impression": int(item.get("best_impression") or 0),
                        "best_all_exposure_value": int(item.get("best_all_exposure_value") or 0),
                        "is_representative": is_rep,
                    }
                )

            rep["daily_play_cluster_id"] = cluster_id
            rep["daily_play_cluster_key"] = _play_cluster_text(rep)
            rep["daily_play_cluster_size"] = len(group_indices)
            rep["cluster_material_count"] = len(group_indices)
            rep["daily_play_cluster_members"] = members
            rep["daily_play_cluster_edges"] = member_edges[:12]
            representatives.append(rep)
            clusters.append(
                {
                    "cluster_id": cluster_id,
                    "appid": appid,
                    "product": str(rep.get("product") or ""),
                    "representative_ad_key": rep_key,
                    "effect_one_liner": str(rep.get("effect_one_liner") or ""),
                    "play_fingerprint": _play_dedupe_text(rep),
                    "play_cluster_key": _play_cluster_text(rep),
                    "differentiator": str(rep.get("differentiator") or ""),
                    "material_count": len(group_indices),
                    "members": members,
                    "edges": member_edges[:12],
                }
            )

    # Items without usable appid/effect remain singleton representatives.
    clustered_indices = {idx for indices in by_app.values() for idx in indices}
    for idx, item in enumerate(items):
        if idx in clustered_indices:
            continue
        rep_key = str(item.get("ad_key") or f"cluster_{len(clusters) + 1}")
        cluster_id = f"{target_date}:unknown:{rep_key[:12]}"
        item["daily_play_cluster_id"] = cluster_id
        item["daily_play_cluster_key"] = _play_cluster_text(item)
        item["daily_play_cluster_representative_ad_key"] = rep_key
        item["daily_play_cluster_size"] = 1
        item["is_daily_play_representative"] = True
        item["cluster_material_count"] = 1
        representatives.append(item)
        clusters.append(
            {
                "cluster_id": cluster_id,
                "appid": str(item.get("appid") or ""),
                "product": str(item.get("product") or ""),
                "representative_ad_key": rep_key,
                "effect_one_liner": str(item.get("effect_one_liner") or ""),
                "play_fingerprint": _play_dedupe_text(item),
                "play_cluster_key": _play_cluster_text(item),
                "differentiator": str(item.get("differentiator") or ""),
                "material_count": 1,
                "members": [
                    {
                        "ad_key": rep_key,
                        "effect_one_liner": str(item.get("effect_one_liner") or ""),
                        "play_fingerprint": _play_dedupe_text(item),
                        "play_cluster_key": _play_cluster_text(item),
                        "differentiator": str(item.get("differentiator") or ""),
                        "best_impression": int(item.get("best_impression") or 0),
                        "best_all_exposure_value": int(item.get("best_all_exposure_value") or 0),
                        "is_representative": True,
                    }
                ],
                "edges": [],
            }
        )

    representatives.sort(key=_effect_row_score, reverse=True)
    clusters.sort(
        key=lambda c: _effect_row_score(
            {
                "all_exposure_value": max((m.get("best_all_exposure_value") or 0) for m in c.get("members", [])),
                "impression": max((m.get("best_impression") or 0) for m in c.get("members", [])),
                "heat": 0,
                "ad_key": str(c.get("representative_ad_key") or ""),
            }
        ),
        reverse=True,
    )
    return representatives, clusters


def load_daily_material_report(
    target_date: str,
    lookback_days: int = 7,
    sustained_per_product_limit: int = 3,
) -> Dict[str, Any]:
    """
    Build the daily material report used by push channels and acceptance.

    Definitions:
      - candidate material: creative_library.first_target_date == target_date
      - new material: candidate material whose play fingerprint is new within
        the VE lookback window; by default this checks global VE play history,
        not only the same appid, so "new play" means genuinely new idea.
      - new play: same-day clusters of new materials by play fingerprint
      - old-play material: first seen today, but the play/effect has appeared before
      - sustained effort: cross-day visual/url/hash/effect signals, grouped for display
    """
    init_db()
    threshold = _effect_similarity_threshold()
    candidate_items = load_new_creatives_for_date(target_date)

    conn = _get_conn()
    try:
        cur = conn.cursor()
        hist_by_app = _load_play_cluster_history_rows(cur, target_date, lookback_days)
        global_hist = _global_play_history_rows(hist_by_app) if _daily_play_global_history_enabled() else []
        for item in candidate_items:
            ad_key = str(item.get("ad_key") or "")
            effect = _play_cluster_text(item)
            if (not effect or effect == "None") and ad_key:
                cur.execute(
                    "SELECT insight_analysis, effect_one_liner, play_fingerprint FROM daily_creative_insights "
                    "WHERE ad_key LIKE ? AND target_date = ? LIMIT 1",
                    (ad_key[:16] + "%", target_date),
                )
                row = cur.fetchone()
                if row:
                    effect = _coarse_play_cluster_key(row["play_fingerprint"], row["effect_one_liner"])
                    item["_analysis_one_liner"] = str(row["insight_analysis"] or "")
            if effect == "None":
                effect = ""
            item["daily_play_cluster_key"] = effect
            item["play_fingerprint"] = str(item.get("play_fingerprint") or effect)
            if not str(item.get("effect_one_liner") or "").strip():
                item["effect_one_liner"] = effect

            appid = str(item.get("appid") or "")
            match_scope = ""
            match = _best_effect_history_match(effect, hist_by_app.get(appid, []), threshold) if effect else None
            if match:
                match_scope = "appid"
            elif effect and global_hist:
                match = _best_effect_history_match(effect, global_hist, threshold)
                if match:
                    match_scope = "global"
            item["material_is_new"] = True
            item["effect_is_new"] = (match is None) if effect else None
            if effect and match is None:
                first_seen_date = target_date
            elif match:
                first_seen_date = str(match.get("earliest_date") or "")
            else:
                first_seen_date = ""
            item["effect_first_seen_date"] = first_seen_date
            item["effect_history_count"] = 0 if match is None else int(match.get("history_count") or 0)
            if match:
                item["effect_matched_one_liner"] = str(match.get("effect_one_liner") or "")
                item["effect_match_similarity"] = float(match.get("similarity") or 0)
                item["effect_match_type"] = str(match.get("match_type") or "")
                item["effect_match_scope"] = match_scope
    finally:
        conn.close()

    sustained_signals = compute_sustained_effort_signals(target_date, lookback_days=lookback_days)
    sustained_by_product = _sustained_display_by_product(
        sustained_signals,
        per_product_limit=sustained_per_product_limit,
    )

    sustained_display_count = sum(len(v) for v in sustained_by_product.values())
    new_material_items = [x for x in candidate_items if x.get("effect_is_new") is True]
    new_play_items, new_play_clusters = _cluster_daily_new_play_items(target_date, new_material_items)
    old_play_items = [x for x in candidate_items if x.get("effect_is_new") is False]
    unknown_play_items = [x for x in candidate_items if x.get("effect_is_new") is None]
    candidate_effect_present_count = sum(1 for x in candidate_items if str(x.get("effect_one_liner") or "").strip())
    new_effect_present_count = sum(1 for x in new_material_items if str(x.get("effect_one_liner") or "").strip())

    return {
        "target_date": target_date,
        "lookback_days": lookback_days,
        "effect_similarity_threshold": threshold,
        "daily_play_cluster_text_threshold": _daily_play_cluster_text_threshold(),
        "daily_play_cluster_embedding_threshold": _daily_play_cluster_embedding_threshold(),
        "daily_play_cluster_embedding_enabled": _daily_play_cluster_embedding_enabled(),
        "daily_play_global_history_enabled": _daily_play_global_history_enabled(),
        "candidate_items": candidate_items,
        "new_items": new_material_items,
        "new_play_items": new_play_items,
        "new_play_clusters": new_play_clusters,
        "old_play_items": old_play_items,
        "unknown_play_items": unknown_play_items,
        "sustained_by_product": sustained_by_product,
        "sustained_signals": sustained_signals,
        "summary": {
            "candidate_material_count": len(candidate_items),
            "new_material_count": len(new_material_items),
            "new_effect_count": len(new_play_clusters),
            "new_play_cluster_count": len(new_play_clusters),
            "new_play_representative_count": len(new_play_items),
            "new_play_duplicate_material_count": max(0, len(new_material_items) - len(new_play_clusters)),
            "old_effect_new_material_count": len(old_play_items),
            "unknown_effect_new_material_count": len(unknown_play_items),
            "effect_one_liner_present_count": new_effect_present_count,
            "effect_one_liner_coverage": round(new_effect_present_count / len(new_material_items), 4) if new_material_items else 1.0,
            "candidate_effect_one_liner_present_count": candidate_effect_present_count,
            "candidate_effect_one_liner_coverage": round(candidate_effect_present_count / len(candidate_items), 4) if candidate_items else 1.0,
            "sustained_display_count": sustained_display_count,
            "sustained_signal_count": int((sustained_signals.get("summary") or {}).get("total_sustained_signals") or 0),
        },
    }
