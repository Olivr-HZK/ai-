"""
将封面预览图 URL 编码为 CLIP 向量，写入 creative_library.cover_embedding。

依赖 sentence-transformers（requirements.txt 已含）与 .env 中
COVER_EMBEDDING_ENABLED（默认开启；设为 0 关闭本模块任务）。
"""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from PIL import Image

from path_util import DATA_DIR, PROJECT_ROOT

load_dotenv(PROJECT_ROOT / ".env")

from llm_client import embedding_to_bytes
from video_enhancer_pipeline_db import DB_PATH, init_db, upsert_cover_embedding

# 延迟加载，避免 import 即拉模型
_st_model: Any = None


def is_cover_embedding_enabled() -> bool:
    v = (os.getenv("COVER_EMBEDDING_ENABLED", "1") or "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def resolve_cover_embedding_model_name() -> str:
    return (os.getenv("LOCAL_COVER_EMBEDDING_MODEL") or "clip-ViT-B-32").strip()


def _get_clip_model() -> Any:
    global _st_model
    if _st_model is None:
        from sentence_transformers import SentenceTransformer

        name = resolve_cover_embedding_model_name()
        _st_model = SentenceTransformer(name)
    return _st_model


def compute_cover_embedding_vector_from_url(url: str) -> List[float]:
    """
    下载单张封面，输出 CLIP 图像向量（与历史 cover_style CLIP 聚类一致）。
    """
    u = (url or "").strip()
    if not u:
        raise ValueError("empty image url")
    r = requests.get(
        u,
        timeout=60,
        headers={"User-Agent": "Mozilla/5.0 (cover_embedding)"},
    )
    r.raise_for_status()
    img = Image.open(
        __import__("io").BytesIO(r.content)
    ).convert("RGB")
    m = _get_clip_model()
    vec = m.encode([img], show_progress_bar=False, convert_to_numpy=True)[0]
    return [float(x) for x in vec.tolist()]


def _rows_missing_embedding(target_date: str, limit: int) -> List[Dict[str, Any]]:
    init_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        sql = """
        SELECT ad_key, preview_img_url
        FROM creative_library
        WHERE COALESCE(last_target_date, '') = ?
          AND (cover_embedding IS NULL OR length(cover_embedding) < 8)
          AND COALESCE(TRIM(preview_img_url), '') <> ''
        """
        if limit and limit > 0:
            sql += f" LIMIT {int(limit)}"
        return [dict(r) for r in conn.execute(sql, (target_date,)).fetchall()]
    finally:
        conn.close()


def run_cover_embedding_job(
    target_date: str,
    *,
    missing_only: bool = True,
    limit: int = 0,
    write_report: bool = True,
) -> Tuple[int, int, float]:
    """
    为当日本库行补写 cover_embedding。返回 (成功, 失败, 秒)。
    """
    if not is_cover_embedding_enabled():
        return 0, 0, 0.0
    init_db()
    rows = _rows_missing_embedding(target_date, limit)
    if not rows:
        if write_report:
            p = DATA_DIR / f"cover_embedding_report_{target_date}.json"
            p.write_text('{"ok":0,"fail":0,"rows":0}\n', encoding="utf-8")
        return 0, 0, 0.0
    t0 = time.perf_counter()
    ok, fail = 0, 0
    for r in rows:
        ak = str(r.get("ad_key") or "").strip()
        url = str(r.get("preview_img_url") or "").strip()
        if not ak or not url:
            fail += 1
            continue
        try:
            vec = compute_cover_embedding_vector_from_url(url)
            blob = embedding_to_bytes(vec)
            if upsert_cover_embedding(ak, blob):
                ok += 1
            else:
                fail += 1
        except Exception:
            fail += 1
    sec = time.perf_counter() - t0
    if write_report:
        rep = {
            "target_date": target_date,
            "ok": ok,
            "fail": fail,
            "seconds": round(sec, 2),
        }
        p = DATA_DIR / f"cover_embedding_report_{target_date}.json"
        p.write_text(
            __import__("json").dumps(rep, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return ok, fail, sec


def maybe_run_cover_embedding_after_library(target_date: str) -> None:
    if not is_cover_embedding_enabled():
        return
    ok, fail, sec = run_cover_embedding_job(
        target_date,
        missing_only=True,
        limit=0,
        write_report=True,
    )
    if ok or fail:
        print(
            f"[cover-embedding] 完成：成功 {ok}，失败 {fail}，耗时 {sec:.2f}s；"
            f"报告 cover_embedding_report_{target_date}.json"
        )
    else:
        print(f"[cover-embedding] 无需补写（{target_date} 无待编码行或已全部有向量）")
