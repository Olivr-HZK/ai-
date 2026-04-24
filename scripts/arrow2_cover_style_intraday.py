"""
Arrow2：封面 CLIP 去重（逻辑与 Video Enhancer `cover_style_intraday` 一致，数据写入 arrow2_pipeline.db）。
复用环境变量：COVER_STYLE_INTRADAY_ENABLED、COVER_VISUAL_DEDUP_THRESHOLD、COVER_STYLE_* 等。
"""

from __future__ import annotations

import os
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Set, Tuple

from dotenv import load_dotenv

from path_util import PROJECT_ROOT

load_dotenv(PROJECT_ROOT / ".env")

from arrow2_pipeline_db import (  # noqa: E402
    crossday_filter_arrow2_items,
    load_arrow2_cover_embedding_blob_map_by_ad_keys,
    load_arrow2_cover_style_rows_for_dates_grouped_by_appid,
    upsert_arrow2_cover_embedding,
    upsert_arrow2_single_cover_style_insight,
)
from cover_embedding import compute_cover_embedding_vector_from_url  # noqa: E402
from cover_style_intraday import (  # noqa: E402
    _cluster_clip_dedupe,
    _clip_style_json,
    _count_cover_encode_needed,
    _cover_history_lookback_days,
    _cover_visual_threshold,
    _exposure_value,
    _history_reference_dates,
    is_cover_style_cross_day_enabled,
    is_cover_style_cross_day_fingerprint_enabled,
    is_cover_style_intraday_enabled,
    pick_cover_url,
    _resolve_cover_workers,
)
from llm_client import bytes_to_embedding, embedding_to_bytes  # noqa: E402

_A2_COVER_LOCK = threading.Lock()


def _persist_arrow2_cover_row(
    item: Dict[str, Any],
    target_date: str,
    crawl_date: Any,
    idx: int,
    total: int,
    ad_key_short: str,
    product: str,
) -> None:
    with _A2_COVER_LOCK:
        try:
            ok = upsert_arrow2_single_cover_style_insight(target_date, crawl_date, item)
            if ok:
                print(
                    f"[arrow2-cover] ✓ 第 {idx}/{total} 张封面 已写入 arrow2_daily_insights · ad_key={ad_key_short}…",
                    flush=True,
                )
        except Exception as e:
            print(
                f"[arrow2-cover] ✗ 第 {idx}/{total} 张封面 入库失败 · ad_key={ad_key_short}… reason={e}",
                flush=True,
            )


def _encode_row_clip_arrow2(
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
        f"\n[arrow2-cover] ▶ 第 {cur_idx}/{total_enc} 张封面 · CLIP 编码 ad_key={ad_key[:12]}…"
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
                upsert_arrow2_cover_embedding(ad_key, embedding_to_bytes(row["vec"]))
    except Exception as e:
        row["style_error"] = str(e)
    sec = time.perf_counter() - t0
    print(
        f"[arrow2-cover] 封面 CLIP 耗时 {sec:.1f}s · 第 {cur_idx}/{total_enc} 张 · ad_key={ad_key[:12]}…",
        flush=True,
    )
    sj = _clip_style_json(threshold)
    row["style_json"] = sj
    it["cover_style"] = sj
    _persist_arrow2_cover_row(
        it,
        target_date,
        crawl_date,
        cur_idx,
        total_enc,
        ad_key[:12],
        prod,
    )


def apply_arrow2_cover_style_dedupe(
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
            "pipeline": "arrow2",
        }

    threshold = _cover_visual_threshold()
    input_count_before_fingerprint = len(items)
    no_cover_passthrough_count = 0
    for _it in items:
        if not isinstance(_it, dict):
            continue
        _c = _it.get("creative") or {}
        if isinstance(_c, dict) and not pick_cover_url(_c):
            no_cover_passthrough_count += 1
    if no_cover_passthrough_count:
        print(
            f"[arrow2-cover] 无封面图 URL 的素材 {no_cover_passthrough_count} 条："
            f"不参与 CLIP 聚类、不因无封面被剔除，一律保留进入下一轮",
            flush=True,
        )
    cross_fp_removed: List[Dict[str, Any]] = []
    if is_cover_style_cross_day_fingerprint_enabled():
        items, cross_fp_removed = crossday_filter_arrow2_items(target_date, items)
        if cross_fp_removed:
            print(
                f"[arrow2-cover] 跨日指纹（arrow2_creative_library）: 剔除 {len(cross_fp_removed)} 条",
                flush=True,
            )
    if not items:
        return [], {
            "skipped": False,
            "reason": "empty_after_cross_day_fingerprint",
            "target_date": target_date,
            "input_count": input_count_before_fingerprint,
            "no_cover_passthrough_count": no_cover_passthrough_count,
            "output_count": 0,
            "cross_day_fingerprint_removed_count": len(cross_fp_removed),
            "cross_day_fingerprint_removed": cross_fp_removed,
            "per_appid": [],
            "removed_total": 0,
            "pipeline": "arrow2",
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
    emb_blob_map = load_arrow2_cover_embedding_blob_map_by_ad_keys(all_ad_keys)
    encode_n = _count_cover_encode_needed(items, emb_blob_map)
    if emb_blob_map:
        print(
            f"[arrow2-cover] 库内已有 cover_embedding {len(emb_blob_map)} 条，本次需新编码 {encode_n} 张",
            flush=True,
        )

    total_enc = max(encode_n, 1)
    idx_holder: List[int] = [0]
    if encode_n > 0:
        print(
            f"\n[arrow2-cover] ========== 共 {encode_n} 张封面需 CLIP 编码（阈值 cosine≥{threshold}）==========\n",
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
        history_by_app = load_arrow2_cover_style_rows_for_dates_grouped_by_appid(history_dates)
        if history_by_app:
            n_h = sum(len(v) for v in history_by_app.values())
            print(
                f"[arrow2-cover] 历史参照：已加载 {len(history_dates)} 个日历日 "
                f"insight_cover_style 共 {n_h} 条（向量需 arrow2_creative_library.cover_embedding）",
                flush=True,
            )

    report: Dict[str, Any] = {
        "skipped": False,
        "target_date": target_date,
        "cover_dedupe_mode": "clip_visual_arrow2",
        "cosine_threshold": threshold,
        "per_appid": [],
        "input_count": len(items),
        "no_cover_passthrough_count": no_cover_passthrough_count,
        "input_count_before_cross_day_fingerprint": input_count_before_fingerprint,
        "removed_total": 0,
        "cover_clip_encode_total": encode_n,
        "cover_embedding_cache_hits": len(emb_blob_map),
        "cross_day_fingerprint_removed_count": len(cross_fp_removed),
        "cross_day_fingerprint_removed": cross_fp_removed,
        "cross_day_history_dates": list(history_dates),
        "cross_day_history_lookback_days": lookback,
        "cross_day_rows_loaded": sum(len(v) for v in history_by_app.values()) if history_by_app else 0,
        "pipeline": "arrow2",
    }

    for appid, bucket in by_app.items():
        product = ""
        if bucket:
            product = str(bucket[0].get("product") or "")

        hist = history_by_app.get(appid) or []
        h_keys = [str(h["ad_key"]) for h in hist if h.get("ad_key")]
        history_emb = load_arrow2_cover_embedding_blob_map_by_ad_keys(h_keys)

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
        app_total = max(len(with_cover), 1)
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
                _persist_arrow2_cover_row(
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
                    f"[arrow2-cover] 同一产品内 CLIP 编码并发 workers={cw}\n",
                    flush=True,
                )
                with ThreadPoolExecutor(max_workers=min(cw, len(need_enc))) as ex:
                    futs = []
                    for row in need_enc:
                        ci = int(row.pop("_cur_idx"))
                        futs.append(
                            ex.submit(
                                _encode_row_clip_arrow2,
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
                    _encode_row_clip_arrow2(
                        row,
                        emb_blob_map,
                        threshold,
                        target_date,
                        crawl_date,
                        ci,
                        app_total,
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

        ad_to_item = {str((it.get("creative") or {}).get("ad_key") or "").strip(): it for it in bucket}

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
            f"\n[arrow2-cover] ========== 封面 CLIP 完成：新编码 {encode_n} 张，"
            f"库内向量复用 {len(emb_blob_map)} 条 ==========\n",
            flush=True,
        )
    return out, report
