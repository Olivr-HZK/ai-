"""
日内、同一 appid（同产品）下：多模态抽取封面风格 → 文本 LLM 聚类 → 每簇仅保留「展示估值」最高的一条。

**跨日两层：**
1) **指纹（默认开）**：与 creative_library 中早于当日的记录比对（同 appid 下 ad_key / URL / 封面 ahash），与灵感分析 `get_deduped_items_for_analysis` 的 Step B 一致；命中则不再做封面多模态。
2) **语义聚类（默认开）**：载入**昨日**同 appid 的 insight_cover_style，与今日条目标注一并交给 LLM 聚类；若簇内昨日条展示估值更高，则剔除今日重复条。

与 analyze_video_from_raw_json 使用相同的多模态链路（_call_llm_image）。

默认**开启**多模态封面去重；若需关闭可设 COVER_STYLE_INTRADAY_ENABLED=0。
主流程仍会使用 video_enhancer_pipeline_db.get_deduped_items_for_analysis
（同一 appid 内 ad_key / 媒体 URL / 封面 ahash 汉明距离 + 跨日库）。

进度：终端输出 [cover-style] [i/total] 开始/已入库，与视频灵感分析类似。
入库：每完成一条封面多模态，即写入 daily_creative_insights.insight_cover_style（非等全部结束再批量）。

重跑：若当日 daily_creative_insights 中该 ad_key 已有非空 insight_cover_style，则跳过多模态，直接复用库内 JSON（与灵感分析「只跑未分析」一致）。

环境变量：
  COVER_STYLE_INTRADAY_ENABLED=0/false/no/off  关闭多模态封面聚类去重
  COVER_STYLE_CROSS_DAY_FINGERPRINT_ENABLED=0/false/no/off  关闭「与素材主库」跨日指纹去重（默认开启）
  COVER_STYLE_CROSS_DAY_ENABLED=0/false/no/off  关闭「与昨日 insight_cover_style」LLM 跨日参照（默认开启）
  COVER_STYLE_WORKERS  同一 appid 内多条封面多模态并发数（默认 3；设为 1 串行）
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv

from path_util import PROJECT_ROOT

load_dotenv(PROJECT_ROOT / ".env")

from analyze_video_from_raw_json import (  # noqa: E402
    _call_llm_image,
    _call_llm_text,
    _pick_image_url,
)
from video_enhancer_pipeline_db import (  # noqa: E402
    crossday_filter_items_against_creative_library,
    load_cover_style_rows_for_date_grouped_by_appid,
    load_existing_cover_style_by_ad_keys_for_date,
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


def pick_cover_url(creative: Dict[str, Any]) -> str:
    pu = str(creative.get("preview_img_url") or "").strip()
    if pu:
        return pu
    return str(_pick_image_url(creative) or "").strip()


def _strip_json_fence(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = re.sub(r"^```(?:json)?\s*", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\s*```$", "", t)
    return t.strip()


def parse_json_object(text: str) -> Optional[Dict[str, Any]]:
    t = _strip_json_fence(text)
    try:
        obj = json.loads(t)
        return obj if isinstance(obj, dict) else None
    except json.JSONDecodeError:
        m = re.search(r"\{[\s\S]*\}", t)
        if m:
            try:
                obj = json.loads(m.group(0))
                return obj if isinstance(obj, dict) else None
            except json.JSONDecodeError:
                pass
    return None


def _cover_style_user_prompt(item: Dict[str, Any], creative: Dict[str, Any]) -> str:
    return (
        "你正在看的是一条广告素材的**封面/缩略图**（不是整支视频）。\n"
        f"- 产品: {item.get('product', '')}\n"
        f"- 标题: {creative.get('title') or '无'}\n\n"
        "请只根据画面与版式，输出**仅一段 JSON 对象**（不要 markdown 代码块），字段如下：\n"
        '  "style_type": 字符串，用 4~12 个字概括封面视觉风格类型\n'
        '  "style_tags": 字符串数组，3~6 个极短中文标签\n'
        '  "one_line": 一句话中文概括该封面的视觉策略\n'
        "要求：简体中文；禁止输出除 JSON 以外的任何字符。"
    )


def _cluster_same_appid_prompt(
    target_date: str,
    appid: str,
    product: str,
    rows: List[Dict[str, Any]],
    *,
    historical_date: Optional[str] = None,
) -> str:
    lines = []
    for r in rows:
        payload: Dict[str, Any] = {
            "ad_key": r.get("ad_key"),
            "style_type": (r.get("style_json") or {}).get("style_type"),
            "style_tags": (r.get("style_json") or {}).get("style_tags"),
            "one_line": (r.get("style_json") or {}).get("one_line"),
        }
        src = r.get("source")
        if isinstance(src, str) and src.strip():
            payload["source"] = src.strip()
        lines.append(json.dumps(payload, ensure_ascii=False))
    blob = "\n".join(lines)
    cross = ""
    if historical_date:
        cross = (
            f"数据包含「昨日」{historical_date} 与「今日」{target_date}；每行 JSON 的 \"source\" 为 \"yesterday\" 或 \"today\"。\n"
            "若今日某条与昨日某条视觉同类，必须归入同一簇；簇内比较展示估值时昨日与今日一视同仁。\n"
        )
    return (
        f"以下**仅同一产品**（appid={appid}，{product}）的广告封面风格描述（每行一个 JSON）。\n"
        f"{cross}"
        "请根据 style_type / style_tags / one_line 的语义，把**视觉风格属于同一类**的素材归为一簇（允许一簇只有 1 条）。\n"
        "禁止把不同视觉套路的素材强行合并。\n\n"
        f"{blob}\n\n"
        "输出**仅一段** JSON 对象（不要 markdown），格式：\n"
        '{\n'
        '  "clusters": [\n'
        '    { "cluster_id": 1, "label": "簇的简短中文名", "ad_keys": ["...", "..."] }\n'
        "  ],\n"
        '  "notes": "可选说明"\n'
        "}\n"
    )


def _exposure_value(creative: Dict[str, Any]) -> int:
    return int(creative.get("all_exposure_value") or 0)


def is_cover_style_intraday_enabled() -> bool:
    """是否启用多模态封面日内聚类去重（默认 True；显式设为 0/false/no/off 则关闭）。"""
    v = os.getenv("COVER_STYLE_INTRADAY_ENABLED", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def is_cover_style_cross_day_enabled() -> bool:
    """聚类时是否载入昨日 insight_cover_style 作参照（默认 True）。"""
    v = os.getenv("COVER_STYLE_CROSS_DAY_ENABLED", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def is_cover_style_cross_day_fingerprint_enabled() -> bool:
    """封面流程开头是否先做与 creative_library 的跨日指纹去重（与灵感分析 Step B 一致，默认 True）。"""
    v = os.getenv("COVER_STYLE_CROSS_DAY_FINGERPRINT_ENABLED", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def _prev_date_str(target_date: str) -> str:
    from datetime import date, timedelta

    try:
        d = date.fromisoformat((target_date or "").strip()[:10])
    except ValueError:
        return ""
    return (d - timedelta(days=1)).isoformat()


def _count_cover_vision_needed(
    items: List[Dict[str, Any]],
    skip_ad_keys: Set[str],
) -> int:
    """本次仍需封面多模态的条数（排除 skip_ad_keys 中已有当日库内封面的 ad_key）。"""
    by_app: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in items:
        if not isinstance(item, dict):
            continue
        appid = str(item.get("appid") or "").strip()
        by_app[appid].append(item)
    n = 0
    for bucket in by_app.values():
        for it in bucket:
            c = it.get("creative") or {}
            if not isinstance(c, dict):
                continue
            ak = str(c.get("ad_key") or "").strip()
            if not pick_cover_url(c) or not ak or ak in skip_ad_keys:
                continue
            n += 1
    return n


def _total_cover_style_vision_calls(items: List[Dict[str, Any]]) -> int:
    """与本次 run 内「无缓存时」封面多模态调用次数一致（兼容旧逻辑）。"""
    return _count_cover_vision_needed(items, set())


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


def _cover_row_multimodal_and_persist(
    row: Dict[str, Any],
    cur_idx: int,
    total_vision: int,
    target_date: str,
    crawl_date: Any,
) -> None:
    """同 appid 桶内单条封面：多模态 + 写 item.cover_style + 入库（供并发调用）。"""
    it = row["item"]
    c = it.get("creative") or {}
    if not isinstance(c, dict):
        c = {}
    ad_key = str(row.get("ad_key") or "").strip()
    prod = str(it.get("product") or "")[:48]
    print(
        f"\n[cover-style] ▶ 第 {cur_idx}/{total_vision} 张封面 · 调用多模态 ad_key={ad_key[:12]}…"
        + (f" · {prod}" if prod else ""),
        flush=True,
    )
    t_cover = time.perf_counter()
    try:
        raw = _call_llm_image(
            _cover_style_user_prompt(it, c),
            row["cover_url"],
            quiet=True,
        )
    except Exception as e:
        raw = f"[ERROR] {e}"
        row["style_error"] = str(e)
    cover_sec = time.perf_counter() - t_cover
    print(
        f"[cover-style] 封面多模态耗时 {cover_sec:.1f}s · 第 {cur_idx}/{total_vision} 张 · ad_key={ad_key[:12]}…",
        flush=True,
    )
    if raw and not str(raw).startswith("[ERROR]"):
        row["style_json"] = parse_json_object(raw)
    if not row.get("style_json"):
        row["style_json"] = {"style_type": "未知", "style_tags": [], "one_line": ""}
    it["cover_style"] = row["style_json"]
    _persist_cover_style_row(
        it,
        target_date,
        crawl_date,
        cur_idx,
        total_vision,
        ad_key[:12],
        prod,
    )


def apply_intraday_cover_style_dedupe(
    items: List[Dict[str, Any]],
    target_date: str,
    crawl_date: Any = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    按 appid 分桶；桶内多模态抽风格 + 聚类；每簇保留 all_exposure_value 最高的一条。

    若 COVER_STYLE_INTRADAY_ENABLED 关闭，立即原样返回（不调 LLM）。

    每完成一条封面的多模态抽取，即写入 daily_creative_insights.insight_cover_style（与视频分析逐条入库一致）。
    """
    if not is_cover_style_intraday_enabled():
        return items, {
            "skipped": True,
            "reason": "COVER_STYLE_INTRADAY_DISABLED",
            "input_count": len(items),
            "output_count": len(items),
        }

    input_count_before_fingerprint = len(items)
    cross_fp_removed: List[Dict[str, Any]] = []
    if is_cover_style_cross_day_fingerprint_enabled():
        items, cross_fp_removed = crossday_filter_items_against_creative_library(target_date, items)
        if cross_fp_removed:
            print(
                f"[cover-style] 跨日指纹去重（creative_library，同灵感分析 Step B）: "
                f"剔除 {len(cross_fp_removed)} 条",
                flush=True,
            )
    if not items:
        return [], {
            "skipped": False,
            "reason": "empty_after_cross_day_fingerprint",
            "target_date": target_date,
            "input_count": input_count_before_fingerprint,
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
        if not isinstance(c, dict):
            continue
        ak = str(c.get("ad_key") or "").strip()
        if ak:
            all_ad_keys.append(ak)
    cover_cache = load_existing_cover_style_by_ad_keys_for_date(target_date, all_ad_keys)
    skip_keys = set(cover_cache.keys())
    cover_style_vision_n = _count_cover_vision_needed(items, skip_keys)
    if cover_cache:
        print(
            f"[cover-style] 当日库内已有封面风格 {len(cover_cache)} 条，将跳过多模态（本次需新分析 {cover_style_vision_n} 条）",
            flush=True,
        )

    total_vision = max(cover_style_vision_n, 1)
    idx_holder: List[int] = [0]
    if cover_style_vision_n > 0:
        print(
            f"\n[cover-style] ========== 共 {cover_style_vision_n} 张封面需多模态，按「第 i/N 张」处理（刷屏的模型 WARN 已静默）==========\n",
            flush=True,
        )
    elif cover_cache:
        print(
            "\n[cover-style] ========== 当日封面风格均已入库，跳过封面多模态（仅聚类/去重）==========\n",
            flush=True,
        )

    by_app: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in items:
        if not isinstance(item, dict):
            continue
        appid = str(item.get("appid") or "").strip()
        by_app[appid].append(item)

    out: List[Dict[str, Any]] = []
    prev_date_str = _prev_date_str(target_date)
    yesterday_by_app: Dict[str, List[Dict[str, Any]]] = {}
    if is_cover_style_cross_day_enabled() and prev_date_str:
        yesterday_by_app = load_cover_style_rows_for_date_grouped_by_appid(prev_date_str)
        if yesterday_by_app:
            n_y = sum(len(v) for v in yesterday_by_app.values())
            print(
                f"[cover-style] 跨日参照：已加载 {prev_date_str} 的 insight_cover_style 共 {n_y} 条（按 appid 参与聚类）",
                flush=True,
            )

    report: Dict[str, Any] = {
        "skipped": False,
        "target_date": target_date,
        "per_appid": [],
        "input_count": len(items),
        "input_count_before_cross_day_fingerprint": input_count_before_fingerprint,
        "removed_total": 0,
        "cover_style_vision_total": cover_style_vision_n,
        "cover_style_cache_hits": len(cover_cache),
        "db_incremental_cover_style": True,
        "cross_day_fingerprint_removed_count": len(cross_fp_removed),
        "cross_day_fingerprint_removed": cross_fp_removed,
        "cross_day_prev_date": prev_date_str if (is_cover_style_cross_day_enabled() and prev_date_str) else "",
        "cross_day_rows_loaded": sum(len(v) for v in yesterday_by_app.values()) if yesterday_by_app else 0,
    }

    for appid, bucket in by_app.items():
        product = ""
        if bucket:
            product = str(bucket[0].get("product") or "")

        hist = yesterday_by_app.get(appid) or []
        yesterday_meta = {h["ad_key"]: int(h.get("exposure") or 0) for h in hist}
        yesterday_key_set = set(yesterday_meta.keys())

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
                }
            )

        with_cover = [x for x in enriched if x["cover_url"]]
        without_cover = [x for x in enriched if not x["cover_url"]]

        for row in with_cover:
            ak = row["ad_key"]
            if ak in cover_cache:
                row["style_json"] = dict(cover_cache[ak])
                row["item"]["cover_style"] = cover_cache[ak]
                print(
                    f"[cover-style] 复用当日库内封面风格 · ad_key={ak[:12]}…",
                    flush=True,
                )

        with_cover_need_llm = [r for r in with_cover if r["style_json"] is None]

        cw = _resolve_cover_workers()
        start_idx = idx_holder[0]
        for i, row in enumerate(with_cover_need_llm):
            row["_cur_idx"] = start_idx + i + 1
        idx_holder[0] += len(with_cover_need_llm)

        if with_cover_need_llm:
            if len(with_cover_need_llm) > 1 and cw > 1:
                print(
                    f"[cover-style] 同一产品内封面多模态并发 workers={cw}（COVER_STYLE_WORKERS）\n",
                    flush=True,
                )
                with ThreadPoolExecutor(max_workers=min(cw, len(with_cover_need_llm))) as ex:
                    futs = []
                    for row in with_cover_need_llm:
                        ci = int(row.pop("_cur_idx"))
                        futs.append(
                            ex.submit(
                                _cover_row_multimodal_and_persist,
                                row,
                                ci,
                                total_vision,
                                target_date,
                                crawl_date,
                            )
                        )
                    for f in as_completed(futs):
                        f.result()
            else:
                for row in with_cover_need_llm:
                    ci = int(row.pop("_cur_idx"))
                    _cover_row_multimodal_and_persist(
                        row,
                        ci,
                        total_vision,
                        target_date,
                        crawl_date,
                    )

        ad_to_row = {r["ad_key"]: r for r in with_cover}
        kept_keys: set[str] = set()
        removed_detail: List[Dict[str, Any]] = []

        for r in without_cover:
            kept_keys.add(r["ad_key"])

        hist_rows: List[Dict[str, Any]] = [
            {"ad_key": h["ad_key"], "style_json": h["style_json"], "source": "yesterday"}
            for h in hist
        ]
        today_cluster_rows: List[Dict[str, Any]] = [
            {"ad_key": r["ad_key"], "style_json": r["style_json"], "source": "today"}
            for r in with_cover
            if r.get("style_json")
        ]
        cluster_payload_rows = hist_rows + today_cluster_rows
        run_cluster_llm = bool(today_cluster_rows) and len(cluster_payload_rows) > 1

        def _exp_for_key(k: str) -> int:
            if k in ad_to_row:
                return int(ad_to_row[k]["exposure"])
            if k in yesterday_meta:
                return yesterday_meta[k]
            return 0

        if not run_cluster_llm:
            for r in with_cover:
                kept_keys.add(r["ad_key"])
        else:
            cp = _cluster_same_appid_prompt(
                target_date,
                appid,
                product,
                cluster_payload_rows,
                historical_date=prev_date_str if hist_rows else None,
            )
            t_cluster = time.perf_counter()
            try:
                cluster_raw = _call_llm_text(
                    "你是素材视觉归类助手，只输出合法 JSON，不要多余解释。",
                    cp,
                )
                cluster_obj = parse_json_object(cluster_raw) or {}
            except Exception as e:
                cluster_obj = {"clusters": [], "error": str(e)}
            cluster_sec = time.perf_counter() - t_cluster
            prod_short = (product or "")[:32]
            print(
                f"[cover-style] 封面聚类 LLM 耗时 {cluster_sec:.1f}s · appid={appid}"
                + (f" · {prod_short}" if prod_short else ""),
                flush=True,
            )

            clusters = cluster_obj.get("clusters") if isinstance(cluster_obj, dict) else None
            if not isinstance(clusters, list):
                clusters = []

            assigned: set[str] = set()
            if clusters:
                for cl in clusters:
                    if not isinstance(cl, dict):
                        continue
                    aks = cl.get("ad_keys") or []
                    if not isinstance(aks, list):
                        continue
                    keys_in = [
                        str(a).strip()
                        for a in aks
                        if str(a).strip() in ad_to_row or str(a).strip() in yesterday_meta
                    ]
                    today_keys_in_cluster = [k for k in keys_in if k in ad_to_row]
                    for k in today_keys_in_cluster:
                        assigned.add(k)
                    if not today_keys_in_cluster:
                        continue
                    best_k = max(keys_in, key=_exp_for_key)
                    if best_k in yesterday_key_set:
                        for k in today_keys_in_cluster:
                            removed_detail.append(
                                {
                                    "ad_key": k,
                                    "reason": "cover_style_cluster_vs_yesterday",
                                    "cluster_label": str(cl.get("label") or ""),
                                    "kept_ad_key": best_k,
                                }
                            )
                    else:
                        kept_keys.add(best_k)
                        for k in today_keys_in_cluster:
                            if k != best_k:
                                removed_detail.append(
                                    {
                                        "ad_key": k,
                                        "reason": "cover_style_cluster",
                                        "cluster_label": str(cl.get("label") or ""),
                                        "kept_ad_key": best_k,
                                    }
                                )
            else:
                for r in with_cover:
                    kept_keys.add(r["ad_key"])

            for r in with_cover:
                if r["ad_key"] not in assigned:
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

        if not run_cluster_llm:
            per_action = (
                "single_or_empty"
                if len(bucket) <= 1 and not hist_rows
                else "cover_cluster_skip"
            )
        else:
            per_action = "cover_cluster"
        report["per_appid"].append(
            {
                "appid": appid,
                "product": product,
                "action": per_action,
                "yesterday_ref_count": len(hist_rows),
                "input": len(bucket),
                "kept": len(kept_keys),
                "removed": removed_detail,
            }
        )
        report["removed_total"] += len(removed_detail)

    report["output_count"] = len(out)
    if cover_style_vision_n > 0 or cover_cache:
        print(
            f"\n[cover-style] ========== 封面风格完成：多模态新跑 {cover_style_vision_n} 张（本段完成 {idx_holder[0]} 次），"
            f"当日库内复用 {len(cover_cache)} 条；insight_cover_style 已就绪 ==========\n",
            flush=True,
        )
    return out, report

