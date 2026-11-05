#!/usr/bin/env python3
"""AI-first VE material duplicate report.

This report is evaluation-only. It does not mutate Bitable, send messages, or
alter the main VE workflow.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from path_util import PROJECT_ROOT
from ve_core_play_shadow_report import (
    DECISIVE_STATUSES,
    NEGATIVE_STATUSES,
    POSITIVE_STATUSES,
    apply_local_fallbacks,
    call_json,
    default_db_path,
    fetch_bitable_rows,
    history_window,
    load_local_fallbacks,
    md_escape,
)


load_dotenv(PROJECT_ROOT / ".env")


DEFAULT_MODEL = "qwen/qwen3.7-max"
DEDUPE_STATUSES = {"new_play", "iteration", "watch", "duplicate_drop"}
KEEP_STATUSES = {"new_play", "iteration"}
DROP_STATUSES = {"duplicate_drop"}
GENERIC_PLAY_LABELS = {
    "手绘",
    "漫画风",
    "赛场转播",
    "插画风",
    "静图活化",
    "奇幻变身",
}


def today_shanghai() -> str:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).strftime("%Y-%m-%d")


def default_workers() -> int:
    raw = (os.getenv("VE_AI_DEDUPE_WORKERS") or "4").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 4


def cosine_similarity(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(float(x) * float(y) for x, y in zip(a, b))
    na = math.sqrt(sum(float(x) * float(x) for x in a))
    nb = math.sqrt(sum(float(y) * float(y) for y in b))
    if not na or not nb:
        return 0.0
    return dot / (na * nb)


def top_visual_history_refs(
    candidate: dict[str, Any],
    history: list[dict[str, Any]],
    vectors: dict[str, list[float]],
    *,
    top_k: int = 3,
    min_similarity: float = 0.0,
) -> list[dict[str, Any]]:
    cand_key = str(candidate.get("ad_key") or "")
    cand_vec = vectors.get(cand_key)
    if not cand_vec:
        return []
    product = str(candidate.get("product") or "")
    refs: list[dict[str, Any]] = []
    for row in history:
        if str(row.get("product") or "") != product:
            continue
        hist_key = str(row.get("ad_key") or "")
        hist_vec = vectors.get(hist_key)
        if not hist_vec:
            continue
        sim = cosine_similarity(cand_vec, hist_vec)
        if sim < min_similarity:
            continue
        refs.append(
            {
                "ad_key": hist_key,
                "similarity": round(float(sim), 3),
                "hp": row.get("actual_hp", ""),
                "core": row.get("core", ""),
                "play_label": row.get("play_label", ""),
            }
        )
    refs.sort(key=lambda x: float(x.get("similarity") or 0), reverse=True)
    return refs[:top_k]


def infer_gameplay_name_from_core(core: str) -> str:
    text = str(core or "").strip()
    rules = [
        (("开口唱歌", "唱歌"), "照片开口唱歌"),
        (("手办包装", "玩具手办"), "玩具手办包装"),
        (("星座", "3D"), "星座3D海报"),
        (("星座", "插画"), "星座主题插画"),
        (("观众席", "赛场"), "赛场观众席融入"),
        (("比分牌", "台标"), "比分牌台标转播"),
        (("巨手", "微缩"), "巨手托举微缩"),
        (("指尖", "微缩"), "指尖微缩互动"),
        (("透明足球", "巨靴"), "足球巨靴微缩"),
        (("透明足球", "小人"), "透明足球小人"),
        (("火焰水流",), "物品火焰水流特效"),
        (("电影感", "光影"), "电影感图生视频"),
        (("婚礼", "手绘"), "婚礼手绘写真"),
        (("职业", "水彩"), "职业水彩拼贴"),
        (("妈妈", "拼贴"), "妈妈身份拼贴"),
        (("复古", "插画"), "复古插画风"),
        (("复古", "球员卡"), "复古球员卡"),
    ]
    for keys, label in rules:
        if all(k in text for k in keys):
            return label
    cleaned = (
        text.replace("真人照片", "")
        .replace("单人照片", "")
        .replace("静态照片", "")
        .replace("照片", "")
        .replace("一键", "")
        .replace("生成", "")
        .replace("效果", "")
        .replace("。", "")
        .strip(" ，,")
    )
    return cleaned[:10] if cleaned else ""


def build_ai_dedupe_prompt(
    *,
    product: str,
    target_date: str,
    history: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> str:
    return f"""你是 VE 竞品素材去重评估助手。请直接判断“剩余素材”是否重复，不要按玩法标签硬分桶；玩法标签只作为弱参考。

你只能使用输入里的核心卖点、历史浩鹏反馈、玩法标签参考、封面图链接，以及“封面视觉近似证据”。如果只有封面链接但没有视觉近似证据，不要臆测图片细节。

判断目标：
- 找出重复素材：历史已处理过且当前只是同款/换皮/无新模板。
- 标出新玩法：历史中没有同款具体玩法或模板。
- 标出老玩法有效迭代：同玩法家族，但有新模板、新场景、新机制或新视觉包装。

状态定义：
- new_play：未命中具体已有玩法标签，且确实是新玩法或明显新模板。
- iteration：老玩法有效迭代，有新模板/场景/机制/视觉包装，并说明历史关联。
- watch：不确定，需要人工观察。
- duplicate_drop：与历史已处理素材几乎同款，且没有新模板/新视觉/新场景，去重丢弃。

判定原则：
- 历史采纳/入素材库不是自动放行理由，只说明类似玩法曾被处理过。
- 历史不采纳/重复抓取也不是自动拦截理由；若当前有明显新模板/场景，仍可 iteration。
- 如果当前候选有具体已有玩法标签（不是“手绘/漫画风/赛场转播”等泛风格词），不能判为 new_play；有新模板时判 iteration，无新模板时判 duplicate_drop。
- 只有“核心卖点相近 + 无新增模板/视觉/场景/机制”才 duplicate_drop。
- 如果封面视觉近似证据 similarity 很高，并且核心卖点也接近，更倾向 duplicate_drop。
- 如果核心卖点是新场景/新机制，即使玩法家族相似，也应 iteration 或 new_play。

产品：{product}
测试日期：{target_date}
历史已处理素材：{json.dumps(history, ensure_ascii=False)}
当前候选：{json.dumps(candidates, ensure_ascii=False)}

只输出 JSON 数组，每个候选一个对象：
{{"idx":0,"ad_key":"...","status":"new_play|iteration|watch|duplicate_drop","duplicate_level":"none|related|near_duplicate|same_template","is_new_play":true,"matched_history_ad_keys":["..."],"matched_history_statuses":["..."],"play_name":"短玩法名","reason":"..."}}"""


def normalize_dedupe_decisions(
    *,
    product: str,
    candidates: list[dict[str, Any]],
    raw_decisions: Any,
) -> list[dict[str, Any]]:
    if not isinstance(raw_decisions, list):
        raw_decisions = []
    by_idx: dict[int, dict[str, Any]] = {}
    by_ad_key: dict[str, dict[str, Any]] = {}
    for item in raw_decisions:
        if not isinstance(item, dict):
            continue
        try:
            by_idx[int(item.get("idx"))] = item
        except (TypeError, ValueError):
            pass
        ad_key = str(item.get("ad_key") or "")
        if ad_key:
            by_ad_key[ad_key] = item

    out: list[dict[str, Any]] = []
    for idx, cand in enumerate(candidates):
        item = by_idx.get(idx) or by_ad_key.get(str(cand.get("ad_key") or "")) or {}
        status = str(item.get("status") or "watch")
        if status not in DEDUPE_STATUSES:
            status = "watch"
        play_label = str(cand.get("play_label") or "").strip()
        has_specific_existing_play = bool(play_label and play_label not in GENERIC_PLAY_LABELS)
        reason = str(item.get("reason") or "模型未返回该素材，默认 watch")
        if status == "new_play" and has_specific_existing_play:
            status = "iteration"
            reason = (
                f"命中已有玩法标签“{play_label}”，按老玩法新变种处理；"
                + reason
            )
        duplicate_level = str(item.get("duplicate_level") or "none")
        if duplicate_level not in {"none", "related", "near_duplicate", "same_template"}:
            duplicate_level = "none"
        out.append(
            {
                **cand,
                "product": product,
                "status": status,
                "duplicate_level": duplicate_level,
                "is_new_play": bool(False if has_specific_existing_play else item.get("is_new_play", status == "new_play")),
                "matched_history_ad_keys": item.get("matched_history_ad_keys") or [],
                "matched_history_statuses": item.get("matched_history_statuses") or [],
                "play_name": str(
                    item.get("play_name")
                    or play_label
                    or infer_gameplay_name_from_core(str(cand.get("core") or ""))
                ),
                "reason": reason,
            }
        )
    return out


def _binary_metrics(rows: list[dict[str, Any]], *, predicted_key: str, actual_key: str) -> dict[str, Any]:
    tp = fp = tn = fn = 0
    for row in rows:
        pred = bool(row[predicted_key])
        actual = bool(row[actual_key])
        if pred and actual:
            tp += 1
        elif pred and not actual:
            fp += 1
        elif not pred and actual:
            fn += 1
        else:
            tn += 1
    predicted = tp + fp
    actual_total = tp + fn
    return {
        "TP": tp,
        "FP": fp,
        "FN": fn,
        "TN": tn,
        "precision": round(tp / predicted, 3) if predicted else None,
        "recall": round(tp / actual_total, 3) if actual_total else 0,
        "false_positive_rate": round(fp / (fp + tn), 3) if fp + tn else 0,
    }


def summarize_dedupe_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    decisive = [r for r in rows if str(r.get("actual_hp") or "") in DECISIVE_STATUSES]
    enriched = []
    for row in decisive:
        status = str(row.get("status") or "watch")
        actual = str(row.get("actual_hp") or "")
        enriched.append(
            {
                **row,
                "_pred_drop": status in DROP_STATUSES,
                "_pred_keep": status in KEEP_STATUSES,
                "_actual_negative": actual in NEGATIVE_STATUSES,
                "_actual_positive": actual in POSITIVE_STATUSES,
            }
        )

    drop_metrics = _binary_metrics(
        enriched,
        predicted_key="_pred_drop",
        actual_key="_actual_negative",
    )
    keep_metrics = _binary_metrics(
        enriched,
        predicted_key="_pred_keep",
        actual_key="_actual_positive",
    )
    return {
        "decisive_count": len(decisive),
        "status_counts": dict(Counter(str(r.get("status") or "watch") for r in decisive)),
        "actual_status_counts": dict(Counter(str(r.get("actual_hp") or "") for r in decisive)),
        "all_status_counts": dict(Counter(str(r.get("status") or "watch") for r in rows)),
        "drop_vs_negative": drop_metrics,
        "keep_vs_positive": keep_metrics,
    }


def run_ai_dedupe(
    *,
    rows: list[dict[str, Any]],
    target_date: str,
    lookback_days: int,
    model: str,
    workers: int = 1,
    vectors: dict[str, list[float]] | None = None,
) -> list[dict[str, Any]]:
    start, end = history_window(target_date, lookback_days)
    history = [
        r
        for r in rows
        if start <= str(r.get("date") or "") <= end
        and str(r.get("actual_hp") or "") in DECISIVE_STATUSES
        and str(r.get("core") or "").strip()
    ]
    candidates = [
        r
        for r in rows
        if str(r.get("date") or "") == target_date and str(r.get("core") or "").strip()
    ]
    vectors = vectors or {}

    grouped: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: {"history": [], "candidates": []})
    for row in history:
        grouped[str(row.get("product") or "")]["history"].append(row)
    for row in candidates:
        grouped[str(row.get("product") or "")]["candidates"].append(row)

    def run_product(product: str) -> list[dict[str, Any]]:
        product_history = grouped[product]["history"]
        product_candidates = grouped[product]["candidates"]
        if not product_candidates:
            return []
        if not product_history:
            return [
                {
                    **cand,
                    "product": product,
                    "status": "new_play",
                    "duplicate_level": "none",
                    "is_new_play": True,
                    "matched_history_ad_keys": [],
                    "matched_history_statuses": [],
                    "play_name": str(
                        cand.get("play_label")
                        or infer_gameplay_name_from_core(str(cand.get("core") or ""))
                    ),
                    "reason": "同产品历史窗口内无明确反馈素材，默认作为新玩法候选进入去重评估。",
                }
                for cand in product_candidates
            ]

        history_payload = [
            {
                "hidx": idx,
                "ad_key": h.get("ad_key", ""),
                "date": h.get("date", ""),
                "hp": h.get("actual_hp", ""),
                "core": h.get("core", ""),
                "play_label": h.get("play_label", ""),
                "cover_url": h.get("cover_url", ""),
            }
            for idx, h in enumerate(product_history)
        ]
        candidate_payload = []
        for idx, cand in enumerate(product_candidates):
            candidate_payload.append(
                {
                    "idx": idx,
                    "ad_key": cand.get("ad_key", ""),
                    "core": cand.get("core", ""),
                    "play_label": cand.get("play_label", ""),
                    "cover_url": cand.get("cover_url", ""),
                    "visual_history_refs": top_visual_history_refs(
                        cand,
                        product_history,
                        vectors,
                        top_k=3,
                        min_similarity=0.78,
                    ),
                }
            )
        print(
            f"[ai-dedupe] {product}: history={len(product_history)} candidates={len(product_candidates)} calling {model}",
            flush=True,
        )
        raw = call_json(
            build_ai_dedupe_prompt(
                product=product,
                target_date=target_date,
                history=history_payload,
                candidates=candidate_payload,
            ),
            model=model,
        )
        result = normalize_dedupe_decisions(
            product=product,
            candidates=product_candidates,
            raw_decisions=raw,
        )
        print(f"[ai-dedupe] {product}: returned={len(result)}", flush=True)
        return result

    product_order = sorted(grouped)
    if workers <= 1:
        out: list[dict[str, Any]] = []
        for product in product_order:
            out.extend(run_product(product))
        return out

    out_by_product: dict[str, list[dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(run_product, product): product for product in product_order}
        for future in as_completed(futures):
            product = futures[future]
            out_by_product[product] = future.result()
    out = []
    for product in product_order:
        out.extend(out_by_product.get(product, []))
    return out


def load_or_compute_cover_vectors(
    rows: list[dict[str, Any]],
    *,
    cache_path: Path,
    workers: int = 4,
) -> dict[str, list[float]]:
    if cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if isinstance(cached, dict):
                return {
                    str(k): [float(x) for x in v]
                    for k, v in cached.items()
                    if isinstance(v, list)
                }
        except Exception:
            pass

    targets = []
    seen = set()
    for row in rows:
        ad_key = str(row.get("ad_key") or "").strip()
        cover_url = str(row.get("cover_url") or "").strip()
        if ad_key and cover_url and ad_key not in seen:
            seen.add(ad_key)
            targets.append((ad_key, cover_url))

    vectors: dict[str, list[float]] = {}
    if not targets:
        return vectors

    def compute(one: tuple[str, str]) -> tuple[str, list[float] | None]:
        ad_key, cover_url = one
        try:
            from cover_embedding import compute_cover_embedding_vector_from_url

            return ad_key, compute_cover_embedding_vector_from_url(cover_url)
        except Exception as exc:
            print(f"[ai-dedupe] cover vector failed ad_key={ad_key[:12]} error={exc}", flush=True)
            return ad_key, None

    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as executor:
        futures = [executor.submit(compute, item) for item in targets]
        for idx, future in enumerate(as_completed(futures), start=1):
            ad_key, vec = future.result()
            if vec:
                vectors[ad_key] = vec
            if idx % 10 == 0 or idx == len(targets):
                print(f"[ai-dedupe] cover vectors {idx}/{len(targets)} ok={len(vectors)}", flush=True)

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(vectors), encoding="utf-8")
    return vectors


def render_markdown(report: dict[str, Any]) -> str:
    metrics = report.get("metrics") or {}
    drop = metrics.get("drop_vs_negative") or {}
    keep = metrics.get("keep_vs_positive") or {}
    rows = [r for r in report.get("results") or [] if isinstance(r, dict)]
    lines = [
        f"# VE AI 去重评估报告 - {report.get('target_date')}",
        "",
        f"- 模型：`{report.get('model')}`",
        f"- 口径：同产品近 {report.get('lookback_days')} 天历史反馈作为参照，AI 直接判断重复、新玩法、老玩法迭代；玩法标签只作弱参考。本报告只输出去重评估，不生成玩法聚合结果。",
        f"- 全量状态分布：{json.dumps(metrics.get('all_status_counts', {}), ensure_ascii=False)}",
        f"- 浩鹏有效反馈样本：{metrics.get('decisive_count', 0)}",
        f"- Drop vs 浩鹏负反馈：TP={drop.get('TP', 0)} / FP={drop.get('FP', 0)} / FN={drop.get('FN', 0)} / TN={drop.get('TN', 0)}，precision={drop.get('precision')}，recall={drop.get('recall')}，误杀率={drop.get('false_positive_rate')}",
        f"- Keep vs 浩鹏正反馈：TP={keep.get('TP', 0)} / FP={keep.get('FP', 0)} / FN={keep.get('FN', 0)} / TN={keep.get('TN', 0)}，precision={keep.get('precision')}，recall={keep.get('recall')}，保留误报率={keep.get('false_positive_rate')}",
        "",
    ]
    lines.extend(["## Drop / Watch 明细", ""])
    rows_show = [r for r in rows if str(r.get("status") or "") not in KEEP_STATUSES]
    if rows_show:
        lines.append("| 状态 | 浩鹏实际 | 产品 | 广告ID | 玩法名 | 核心卖点 | 理由 |")
        lines.append("|---|---|---|---|---|---|---|")
        for row in rows_show:
            lines.append(
                "| "
                + " | ".join(
                    [
                        md_escape(row.get("status")),
                        md_escape(row.get("actual_hp")),
                        md_escape(row.get("product")),
                        md_escape(row.get("ad_key")),
                        md_escape(row.get("play_name")),
                        md_escape(row.get("core")),
                        md_escape(row.get("reason")),
                    ]
                )
                + " |"
            )
    else:
        lines.append("无 drop/watch。")

    lines.extend(["", "## 全量素材", ""])
    lines.append("| 状态 | 浩鹏实际 | 产品 | 广告ID | 玩法标签 | 玩法名 | 核心卖点 | 理由 |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    md_escape(row.get("status")),
                    md_escape(row.get("actual_hp")),
                    md_escape(row.get("product")),
                    md_escape(row.get("ad_key")),
                    md_escape(row.get("play_label")),
                    md_escape(row.get("play_name")),
                    md_escape(row.get("core")),
                    md_escape(row.get("reason")),
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def write_report(report: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    target_date = str(report.get("target_date") or "unknown")
    json_path = output_dir / f"ve_ai_dedupe_report_{target_date}.json"
    md_path = output_dir / f"ve_ai_dedupe_report_{target_date}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, md_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="VE AI-first 素材重复判断报告")
    parser.add_argument("--date", default=today_shanghai(), help="目标日期 YYYY-MM-DD")
    parser.add_argument("--lookback-days", type=int, default=1, help="历史反馈回看天数")
    parser.add_argument(
        "--model",
        default=(os.getenv("VE_AI_DEDUPE_MODEL") or DEFAULT_MODEL).strip(),
    )
    parser.add_argument("--bitable-url", default=(os.getenv("VIDEO_ENHANCER_BITABLE_URL") or "").strip())
    parser.add_argument("--reviewer-field", default="浩鹏接受情况")
    parser.add_argument("--db-path", default=str(default_db_path()))
    parser.add_argument("--output-dir", default=str(PROJECT_ROOT / "data" / "ve_ai_dedupe_report"))
    parser.add_argument("--workers", type=int, default=default_workers())
    parser.add_argument("--cover-sim", action="store_true", help="计算封面 CLIP 相似证据后再交给模型")
    parser.add_argument("--cover-workers", type=int, default=4)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.bitable_url:
        raise SystemExit("请配置 VIDEO_ENHANCER_BITABLE_URL 或传入 --bitable-url")

    rows = fetch_bitable_rows(bitable_url=args.bitable_url, reviewer_field=args.reviewer_field)
    apply_local_fallbacks(rows, load_local_fallbacks(Path(args.db_path)))
    start, end = history_window(args.date, args.lookback_days)
    history_count = sum(
        1
        for r in rows
        if start <= str(r.get("date") or "") <= end
        and str(r.get("actual_hp") or "") in DECISIVE_STATUSES
        and str(r.get("core") or "").strip()
    )
    target_count = sum(1 for r in rows if str(r.get("date") or "") == args.date and str(r.get("core") or "").strip())
    print(
        f"[ai-dedupe] date={args.date} history_window={start}..{end} "
        f"history_effective={history_count} target_candidates={target_count}",
        flush=True,
    )

    vectors: dict[str, list[float]] = {}
    output_dir = Path(args.output_dir)
    if args.cover_sim:
        vectors = load_or_compute_cover_vectors(
            [
                r
                for r in rows
                if (start <= str(r.get("date") or "") <= end or str(r.get("date") or "") == args.date)
                and str(r.get("product") or "")
            ],
            cache_path=output_dir / f"ve_ai_dedupe_cover_vectors_{args.date}.json",
            workers=max(1, int(args.cover_workers)),
        )
        print(f"[ai-dedupe] cover vectors ready={len(vectors)}", flush=True)

    results = run_ai_dedupe(
        rows=rows,
        target_date=args.date,
        lookback_days=args.lookback_days,
        model=args.model,
        workers=max(1, int(args.workers)),
        vectors=vectors,
    )
    report = {
        "target_date": args.date,
        "lookback_days": args.lookback_days,
        "reviewer_field": args.reviewer_field,
        "model": args.model,
        "history_window": {"start": start, "end": end, "effective_count": history_count},
        "target_candidate_count": target_count,
        "cover_similarity_enabled": bool(args.cover_sim),
        "cover_vectors_count": len(vectors),
        "metrics": summarize_dedupe_metrics(results),
        "results": results,
    }
    json_path, md_path = write_report(report, output_dir)
    try:
        from llm_client import flush_usage

        flush_usage(args.date)
    except Exception as exc:
        print(f"[ai-dedupe] flush_usage skipped: {exc}", flush=True)
    print(f"[ai-dedupe] wrote {json_path}", flush=True)
    print(f"[ai-dedupe] wrote {md_path}", flush=True)


if __name__ == "__main__":
    main()
