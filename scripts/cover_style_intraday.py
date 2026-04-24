"""
日内、同一 appid 下：封面 **本机 CLIP 向量**（sentence-transformers / clip-ViT-B-32）余弦聚类去重，
每簇保留「展示估值」最高的一条；非多模态 LLM。

跨日两层：
1) 指纹（默认开）：与 creative_library 早于当日的记录比对（与灵感分析 Step B 一致），见
   video_enhancer_pipeline_db.crossday_filter_items_against_creative_library。
2) 向量（默认开）：过去 N 日同 appid 的 insight_cover_style + 库内 cover_embedding 与今日向量并查集；
   历史估值更高时剔除今日重复（reason: cover_style_cluster_vs_yesterday）。

环境变量同 Arrow2 / .env.example：COVER_STYLE_INTRADAY_ENABLED、COVER_VISUAL_DEDUP_THRESHOLD、
COVER_STYLE_CROSS_DAY_FINGERPRINT_ENABLED、COVER_STYLE_CROSS_DAY_ENABLED、COVER_STYLE_HISTORY_LOOKBACK_DAYS 等。

向量先读主库 cover_embedding，缺失时本地 compute 并回写主库，再写 insight_cover_style 占位 JSON。
"""

from __future__ import annotations

import os
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Any, Dict, List, Set, Tuple

from dotenv import load_dotenv

from path_util import PROJECT_ROOT

load_dotenv(PROJECT_ROOT / ".env")

from analyze_video_from_raw_json import _pick_image_url  # noqa: E402
from cover_embedding import compute_cover_embedding_vector_from_url  # noqa: E402
from llm_client import bytes_to_embedding, cosine_similarity, embedding_to_bytes  # noqa: E402
from video_enhancer_pipeline_db import (  # noqa: E402
    crossday_filter_items_against_creative_library,
    load_cover_embedding_blob_map_by_ad_keys,
    load_cover_style_rows_for_dates_grouped_by_appid,
    upsert_cover_embedding,
    upsert_single_cover_style_insight,
)

_COVER_DB_LOCK = threading.Lock()


def _resolve_cover_workers() -> int:
    raw = os.getenv("COVER_STYLE_WORKERS", "3").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 3
    return max(1, n)


def _cover_visual_threshold() -> float:
    raw = os.getenv("COVER_VISUAL_DEDUP_THRESHOLD", "0.8").strip()
    try:
        v = float(raw)
    except ValueError:
        v = 0.8
    return max(0.0, min(1.0, v))


def _clip_style_json(threshold: float) -> Dict[str, Any]:
    m = (os.getenv("LOCAL_COVER_EMBEDDING_MODEL") or "clip-ViT-B-32").strip()
    return {
        "style_type": "CLIP视觉",
        "style_tags": ["embedding", m],
        "one_line": f"封面视觉去重（cosine≥{threshold}，{m}，本地）",
    }


def pick_cover_url(creative: Dict[str, Any]) -> str:
    pu = str(creative.get("preview_img_url") or "").strip()
    if pu:
        return pu
    return str(_pick_image_url(creative) or "").strip()


def _exposure_value(creative: Dict[str, Any]) -> int:
    return int(creative.get("all_exposure_value") or 0)


def is_cover_style_intraday_enabled() -> bool:
    v = os.getenv("COVER_STYLE_INTRADAY_ENABLED", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def is_cover_style_cross_day_enabled() -> bool:
    v = os.getenv("COVER_STYLE_CROSS_DAY_ENABLED", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def is_cover_style_cross_day_fingerprint_enabled() -> bool:
    v = os.getenv("COVER_STYLE_CROSS_DAY_FINGERPRINT_ENABLED", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def _cover_history_lookback_days() -> int:
    raw = os.getenv("COVER_STYLE_HISTORY_LOOKBACK_DAYS", "7").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 7
    return max(0, min(60, n))


def _history_reference_dates(target_date: str, n_days: int) -> List[str]:
    if n_days <= 0:
        return []
    try:
        d = date.fromisoformat((target_date or "").strip()[:10])
    except ValueError:
        return []
    return [(d - timedelta(days=i)).isoformat() for i in range(1, n_days + 1)]


def _count_cover_encode_needed(
    items: List[Dict[str, Any]],
    blob_map: Dict[str, bytes],
) -> int:
    n = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        c = item.get("creative") or {}
        if not isinstance(c, dict):
            continue
        ak = str(c.get("ad_key") or "").strip()
        if not pick_cover_url(c) or not ak:
            continue
        if ak in blob_map and blob_map[ak]:
            continue
        n += 1
    return n


def _persist_cover_style_row(
    item: Dict[str, Any],
    target_date: str,
    crawl_date: Any,
    idx: int,
    total: int,
    ad_key_short: str,
    product: str,
) -> None:
    with _COVER_DB_LOCK:
        try:
            ok = upsert_single_cover_style_insight(target_date, crawl_date, item)
            if ok:
                print(
                    f"[cover-style] ✓ 第 {idx}/{total} 张封面 已写入库 insight_cover_style · ad_key={ad_key_short}…",
                    flush=True,
                )
        except Exception as e:
            print(
                f"[cover-style] ✗ 第 {idx}/{total} 张封面 入库失败 · ad_key={ad_key_short}… reason={e}",
                flush=True,
            )


def _encode_row_clip(
    row: Dict[str, Any],
    blob_map: Dict[str, bytes],
    threshold: float,
    target_date: str,
    crawl_date: Any,
    cur_idx: int,
    total_enc: int,
) -> None:
    it = row["item"]
    c = it.get("creative") or {}
    if not isinstance(c, dict):
        c = {}
    ad_key = str(row.get("ad_key") or "").strip()
    prod = str(it.get("product") or "")[:48]
    print(
        f"\n[cover-style] ▶ 第 {cur_idx}/{total_enc} 张封面 · CLIP 编码 ad_key={ad_key[:12]}…"
        + (f" · {prod}" if prod else ""),
        flush=True,
    )
    t0 = time.perf_counter()
    row["vec"] = None
    row["style_error"] = ""
    blob = blob_map.get(ad_key)
    try:
        if blob:
            row["vec"] = bytes_to_embedding(blob)
        else:
            row["vec"] = compute_cover_embedding_vector_from_url(row["cover_url"])
            if row["vec"]:
                upsert_cover_embedding(ad_key, embedding_to_bytes(row["vec"]))
    except Exception as e:
        row["style_error"] = str(e)
    sec = time.perf_counter() - t0
    print(
        f"[cover-style] 封面 CLIP 耗时 {sec:.1f}s · 第 {cur_idx}/{total_enc} 张 · ad_key={ad_key[:12]}…",
        flush=True,
    )
    sj = _clip_style_json(threshold)
    row["style_json"] = sj
    it["cover_style"] = sj
    _persist_cover_style_row(
        it, target_date, crawl_date, cur_idx, total_enc, ad_key[:12], prod
    )


class _UF:
    def __init__(self, n: int) -> None:
        self.p = list(range(n))

    def find(self, x: int) -> int:
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def _cluster_clip_dedupe(
    *,
    threshold: float,
    today_rows: List[Dict[str, Any]],
    history_hist: List[Dict[str, Any]],
    history_emb: Dict[str, bytes],
) -> Tuple[Set[str], List[Dict[str, Any]]]:
    """返回 (今日保留 ad_key 集合, 剔除明细)。"""
    nodes: List[Dict[str, Any]] = []
    for r in today_rows:
        if r.get("vec") is None:
            continue
        nodes.append(
            {
                "kind": "today",
                "ad_key": r["ad_key"],
                "exposure": int(r["exposure"]),
                "vec": r["vec"],
            }
        )
    for h in history_hist:
        ak = str(h.get("ad_key") or "").strip()
        blob = history_emb.get(ak)
        if not blob or not ak:
            continue
        try:
            vec = bytes_to_embedding(blob)
        except Exception:
            continue
        nodes.append(
            {
                "kind": "history",
                "ad_key": ak,
                "exposure": int(h.get("exposure") or 0),
                "vec": vec,
            }
        )

    n = len(nodes)
    kept_today: Set[str] = set()
    removed_detail: List[Dict[str, Any]] = []

    if n < 2:
        for r in today_rows:
            kept_today.add(r["ad_key"])
        return kept_today, removed_detail

    uf = _UF(n)
    for i in range(n):
        for j in range(i + 1, n):
            sim = cosine_similarity(nodes[i]["vec"], nodes[j]["vec"])
            if sim >= threshold:
                uf.union(i, j)

    groups: Dict[int, List[int]] = defaultdict(list)
    for i in range(n):
        groups[uf.find(i)].append(i)

    for _root, idxs in groups.items():
        best_i = max(idxs, key=lambda ii: nodes[ii]["exposure"])
        best = nodes[best_i]
        today_in = [ii for ii in idxs if nodes[ii]["kind"] == "today"]
        if not today_in:
            continue
        if best["kind"] == "history":
            for ii in today_in:
                ak = nodes[ii]["ad_key"]
                removed_detail.append(
                    {
                        "ad_key": ak,
                        "reason": "cover_style_cluster_vs_yesterday",
                        "cluster_label": f"CLIP≥{threshold}",
                        "kept_ad_key": best["ad_key"],
                    }
                )
        else:
            bk = best["ad_key"]
            kept_today.add(bk)
            for ii in today_in:
                ak = nodes[ii]["ad_key"]
                if ak != bk:
                    removed_detail.append(
                        {
                            "ad_key": ak,
                            "reason": "cover_style_cluster",
                            "cluster_label": f"CLIP≥{threshold}",
                            "kept_ad_key": bk,
                        }
                    )

    for r in today_rows:
        if r.get("vec") is None:
            kept_today.add(r["ad_key"])

    return kept_today, removed_detail


def apply_intraday_cover_style_dedupe(
    items: List[Dict[str, Any]],
    target_date: str,
    crawl_date: Any = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not is_cover_style_intraday_enabled():
        return items, {
            "skipped": True,
            "reason": "COVER_STYLE_INTRADAY_DISABLED",
            "input_count": len(items),
            "output_count": len(items),
        }

    threshold = _cover_visual_threshold()
    input_count_before_fingerprint = len(items)
    no_cover_passthrough = 0
    for _it in items or []:
        if not isinstance(_it, dict):
            continue
        _c = _it.get("creative") or {}
        if isinstance(_c, dict) and not pick_cover_url(_c):
            no_cover_passthrough += 1
    if no_cover_passthrough:
        print(
            f"[cover-style] 无封面图 URL 的素材 {no_cover_passthrough} 条："
            f"不参与 CLIP 聚类、不剔除，原样进入后续",
            flush=True,
        )

    cross_fp_removed: List[Dict[str, Any]] = []
    if is_cover_style_cross_day_fingerprint_enabled():
        items, cross_fp_removed = crossday_filter_items_against_creative_library(
            target_date, items
        )
        if cross_fp_removed:
            print(
                f"[cover-style] 跨日指纹去重（creative_library）: 剔除 {len(cross_fp_removed)} 条",
                flush=True,
            )
    if not items:
        return [], {
            "skipped": False,
            "reason": "empty_after_cross_day_fingerprint",
            "target_date": target_date,
            "input_count": input_count_before_fingerprint,
            "no_cover_passthrough_count": no_cover_passthrough,
            "output_count": 0,
            "cross_day_fingerprint_removed_count": len(cross_fp_removed),
            "cross_day_fingerprint_removed": cross_fp_removed,
            "per_appid": [],
            "removed_total": 0,
        }

    all_ad_keys: List[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        c = item.get("creative") or {}
        if isinstance(c, dict):
            ak = str(c.get("ad_key") or "").strip()
            if ak:
                all_ad_keys.append(ak)
    emb_blob_map = load_cover_embedding_blob_map_by_ad_keys(all_ad_keys)
    encode_n = _count_cover_encode_needed(items, emb_blob_map)
    if emb_blob_map:
        print(
            f"[cover-style] 库内已有 cover_embedding {len(emb_blob_map)} 条，本次需新编码 {encode_n} 张",
            flush=True,
        )
    total_enc = max(encode_n, 1)
    idx_holder: List[int] = [0]
    if encode_n > 0:
        print(
            f"\n[cover-style] ========== 共 {encode_n} 张封面需 CLIP 编码（余弦≥{threshold}，本地模型）==========\n",
            flush=True,
        )

    by_app: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in items:
        if not isinstance(item, dict):
            continue
        appid = str(item.get("appid") or "").strip()
        by_app[appid].append(item)

    out: List[Dict[str, Any]] = []
    lookback = _cover_history_lookback_days() if is_cover_style_cross_day_enabled() else 0
    history_dates = _history_reference_dates(target_date, lookback)
    history_by_app: Dict[str, List[Dict[str, Any]]] = {}
    if history_dates:
        history_by_app = load_cover_style_rows_for_dates_grouped_by_appid(history_dates)
        if history_by_app:
            n_h = sum(len(v) for v in history_by_app.values())
            print(
                f"[cover-style] 历史参照：{len(history_dates)} 个日历日 "
                f"insight_cover_style 共 {n_h} 条（向量需主库有 cover_embedding）",
                flush=True,
            )

    report: Dict[str, Any] = {
        "skipped": False,
        "target_date": target_date,
        "cover_dedupe_mode": "clip_visual_local",
        "cosine_threshold": threshold,
        "per_appid": [],
        "input_count": len(items),
        "no_cover_passthrough_count": no_cover_passthrough,
        "input_count_before_cross_day_fingerprint": input_count_before_fingerprint,
        "removed_total": 0,
        "cover_clip_encode_total": encode_n,
        "cover_style_vision_total": encode_n,
        "cover_embedding_cache_hits": len(emb_blob_map),
        "cross_day_fingerprint_removed_count": len(cross_fp_removed),
        "cross_day_fingerprint_removed": cross_fp_removed,
        "cross_day_history_dates": list(history_dates),
        "cross_day_history_lookback_days": lookback,
        "cross_day_rows_loaded": sum(len(v) for v in history_by_app.values()) if history_by_app else 0,
    }

    for appid, bucket in by_app.items():
        product = str(bucket[0].get("product") or "") if bucket else ""
        hist = history_by_app.get(appid) or []
        h_keys = [str(h["ad_key"]) for h in hist if h.get("ad_key")]
        history_emb = load_cover_embedding_blob_map_by_ad_keys(h_keys)

        enriched: List[Dict[str, Any]] = []
        for item in bucket:
            c = item.get("creative") or {}
            if not isinstance(c, dict):
                continue
            ad_key = str(c.get("ad_key") or "").strip()
            cover = pick_cover_url(c)
            enriched.append(
                {
                    "item": item,
                    "ad_key": ad_key,
                    "cover_url": cover,
                    "exposure": _exposure_value(c),
                    "style_json": None,
                    "style_error": "",
                    "vec": None,
                }
            )

        with_cover = [x for x in enriched if x["cover_url"]]
        without_cover = [x for x in enriched if not x["cover_url"]]

        need_enc: List[Dict[str, Any]] = []
        for r in with_cover:
            blob = emb_blob_map.get(r["ad_key"])
            if blob:
                try:
                    r["vec"] = bytes_to_embedding(blob)
                except Exception:
                    r["vec"] = None
                r["style_json"] = _clip_style_json(threshold)
                r["item"]["cover_style"] = r["style_json"]
                idx_holder[0] += 1
                ci = idx_holder[0]
                _persist_cover_style_row(
                    r["item"],
                    target_date,
                    crawl_date,
                    ci,
                    total_enc,
                    str(r["ad_key"])[:12],
                    str(r["item"].get("product") or "")[:48],
                )
            else:
                need_enc.append(r)

        cw = _resolve_cover_workers()
        for i, row in enumerate(need_enc):
            row["_cur_idx"] = idx_holder[0] + i + 1

        if need_enc:
            if len(need_enc) > 1 and cw > 1:
                print(
                    f"[cover-style] 同一产品内 CLIP 编码并发 workers={cw}（COVER_STYLE_WORKERS）\n",
                    flush=True,
                )
                with ThreadPoolExecutor(max_workers=min(cw, len(need_enc))) as ex:
                    futs = []
                    for row in need_enc:
                        ci = int(row.pop("_cur_idx"))
                        futs.append(
                            ex.submit(
                                _encode_row_clip,
                                row,
                                emb_blob_map,
                                threshold,
                                target_date,
                                crawl_date,
                                ci,
                                total_enc,
                            )
                        )
                    for f in as_completed(futs):
                        f.result()
            else:
                for row in need_enc:
                    ci = int(row.pop("_cur_idx"))
                    _encode_row_clip(
                        row,
                        emb_blob_map,
                        threshold,
                        target_date,
                        crawl_date,
                        ci,
                        total_enc,
                    )
        idx_holder[0] += len(need_enc)

        ad_to_row = {r["ad_key"]: r for r in with_cover}
        kept_today, removed_detail = _cluster_clip_dedupe(
            threshold=threshold,
            today_rows=with_cover,
            history_hist=hist,
            history_emb=history_emb,
        )
        kept_keys: Set[str] = set(kept_today)
        for r in without_cover:
            kept_keys.add(r["ad_key"])

        ad_to_item = {
            str((it.get("creative") or {}).get("ad_key") or "").strip(): it
            for it in bucket
        }
        for ak in sorted(kept_keys):
            if ak in ad_to_item:
                it = ad_to_item[ak]
                if ak in ad_to_row:
                    row = ad_to_row[ak]
                    it["cover_style"] = row.get("style_json")
                else:
                    it["cover_style"] = None
                out.append(it)

        run_cluster = len(with_cover) >= 2 and (
            len([r for r in with_cover if r.get("vec") is not None]) >= 2 or bool(hist)
        )
        per_action = "cover_clip_cluster" if run_cluster else "cover_cluster_skip"
        report["per_appid"].append(
            {
                "appid": appid,
                "product": product,
                "action": per_action,
                "history_ref_count": len(hist),
                "yesterday_ref_count": len(hist),
                "input": len(bucket),
                "kept": len(kept_keys),
                "removed": removed_detail,
            }
        )
        report["removed_total"] += len(removed_detail)

    report["output_count"] = len(out)
    if encode_n > 0 or emb_blob_map:
        print(
            f"\n[cover-style] ========== 封面 CLIP 完成：新编码 {encode_n} 张，"
            f"库内向量复用 {len(emb_blob_map)} 条；insight_cover_style 已写入（CLIP 占位 JSON）==========\n",
            flush=True,
        )
    return out, report
