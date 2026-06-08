"""
Video Enhancer 全流程工作流（一键）：
1) 抓取前一天素材（按日期 + 指定产品）
2) 可选封面日内去重（可用 --skip-cover-dedupe 跳过；与抓取拆分请用 workflow_video_enhancer_steps crawl_store --crawl-only + cover_store）
2.0) 统计灵感分析准入（不删 raw；不符准入的不进分析）
3) 视频灵感分析（基于 raw JSON）
4) 成人/色情、日内重复、历史老玩法、embedding 重复等去重/拦截
5) 同步去重后的可复核素材到多维表
6) 推送「新素材 / 新玩法 / 持续发力」日报

旧的 UA 方向卡片仍会尽量生成作兼容产物，但不再阻塞主表同步和新玩法日报推送。

示例：
python scripts/workflow_video_enhancer_full_pipeline.py \
  --date 2026-03-18 \
  --products "UpFoto - AI Photo Enhancer,Remini - AI Photo Enhancer"

多维表链接可写在 .env：`VIDEO_ENHANCER_BITABLE_URL=...`（含 table= 的完整浏览器地址）。
也可用 `--bitable-url` 临时覆盖环境变量。
不设 `VIDEO_ENHANCER_CLUSTER_BITABLE_URL` 时仅同步主表（raw/分析），不写聚类表。

稳定性：可选 `WORKFLOW_SUBPROCESS_TIMEOUT_SEC`（秒，>0 时对子进程生效；默认 0 不限制，避免长任务被误杀）。
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from ua_workflows.shared.config import DATA_DIR, PROJECT_ROOT, load_project_env, project_env_values

load_project_env()

from ua_workflows.shared.llm.client import print_openrouter_key_meter

from ua_workflows.shared.media.cover_embedding import maybe_run_cover_embedding_after_library
from ua_workflows.shared.media.resolve import normalize_video_url_for_consumption
from ua_workflows.video_enhancer.cover_dedupe import apply_intraday_cover_style_dedupe, is_cover_style_intraday_enabled
from ua_workflows.video_enhancer.crawl_similarity import merge_cover_similarity_counts
from ua_workflows.video_enhancer.review_dashboard import write_filter_review_dashboard

from ua_workflows.video_enhancer.filter_reports import (
    write_cover_filter_step_json,
    write_cover_filter_step_json_skipped,
    write_launched_filter_step_json,
)
from ua_workflows.video_enhancer.content_filters import (
    apply_adult_content_filter,
    apply_human_photo_effect_filter,
)

from ua_workflows.video_enhancer.analyze import is_creative_analyzable

from ua_workflows.shared.media.resolve import (
    classify_ineligible_reason,
    format_inspiration_detail_lines,
    ineligible_reason_label_cn,
    merge_inspiration_filter_stats,
)

from ua_workflows.shared.db.ua_crawl import format_video_enhancer_usage_log_line

from ua_workflows.shared.db.video_enhancer import (
    apply_embedding_duplicate_candidate_tags,
    apply_effect_embedding_duplicate_filter,
    apply_intraday_effect_bitable_filter,
    apply_old_effect_bitable_filter,
    build_inspiration_dedup_redirect_map,
    combined_analysis_results_for_pipeline,
    get_deduped_items_for_analysis,
    init_db as init_pipeline_db,
    load_existing_success_analysis_by_ad_keys,
    resolve_inspiration_crossday_lookback_days,
    should_persist_suggestion_to_push_table,
    upsert_effect_one_liner_embedding,
    upsert_creative_library,
    upsert_daily_creative_insights,
    upsert_daily_push_content,
    upsert_daily_video_enhancer_filter_log,
)


def _store_effect_one_liner_embeddings(analysis_by_ad: dict[str, dict]) -> None:
    """对玩法一句话计算嵌入向量并写入 creative_library.effect_one_liner_embedding。"""
    try:
        from ua_workflows.shared.llm import client as llm_client
    except ImportError:
        return
    stored = 0
    failed = 0
    seen_texts: dict[str, list[float]] = {}
    for ad_key, info in analysis_by_ad.items():
        if not isinstance(info, dict):
            continue
        effect = str(info.get("play_fingerprint") or info.get("effect_one_liner") or "").strip()
        if not effect or effect == "None":
            continue
        prompt_text = f"特效玩法：{effect[:300]}"
        try:
            if prompt_text not in seen_texts:
                seen_texts[prompt_text] = llm_client.call_embedding(prompt_text)
            blob = llm_client.embedding_to_bytes(seen_texts[prompt_text])
            upsert_effect_one_liner_embedding(ad_key, blob)
            stored += 1
        except Exception as e:
            failed += 1
            print(f"[effect-embedding] failed ad_key={ad_key[:12]}: {e}")
    if stored:
        print(f"[effect-embedding] 已为 {stored} 条素材写入玩法一句话嵌入向量。")
    if failed:
        print(f"[effect-embedding] {failed} 条玩法 embedding 失败（不影响主流程）。")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Video Enhancer：爬取->灵感分析->多维表同步->飞书推送 全流程")
    p.add_argument(
        "--date",
        default=None,
        metavar="YYYY-MM-DD",
        help="目标日期（UTC+8）。默认昨天。",
    )
    p.add_argument(
        "--products",
        default="",
        help="要跑的产品名（逗号分隔，需与 config/ai_product.json 的 key 完全一致）",
    )
    p.add_argument(
        "--bitable-url",
        default=None,
        help="飞书多维表完整链接（含 table 参数）。不传则使用环境变量 VIDEO_ENHANCER_BITABLE_URL",
    )
    p.add_argument(
        "--cluster-bitable-url",
        default=None,
        help="聚类方向结果多维表链接（含 table 参数）。不传则使用环境变量 VIDEO_ENHANCER_CLUSTER_BITABLE_URL",
    )
    p.add_argument(
        "--no-card",
        action="store_true",
        help="只同步多维表，不推送飞书卡片",
    )
    p.add_argument(
        "--no-bitable-sync",
        action="store_true",
        help="跳过多维表同步（主表+聚类表），仅继续后续本地入库/其他推送流程",
    )
    p.add_argument(
        "--no-wecom",
        action="store_true",
        help="跳过企业微信推送",
    )
    p.add_argument(
        "--no-sheet",
        action="store_true",
        help="跳过 Google Sheet 同步",
    )
    p.add_argument(
        "--skip-cover-dedupe",
        action="store_true",
        help="跳过封面日内 CLIP 视觉聚类去重（抓取后直接用全量 raw 入库并继续后续步骤）",
    )
    return p.parse_args()


def _resolve_bitable_url(args: argparse.Namespace) -> str:
    url = (args.bitable_url or "").strip()
    if not url:
        url = (os.getenv("VIDEO_ENHANCER_BITABLE_URL") or "").strip()
    if not url:
        raise SystemExit(
            "未配置飞书多维表链接：请在 .env 中设置 VIDEO_ENHANCER_BITABLE_URL，"
            "或运行时传入 --bitable-url（需含 table= 的完整链接）"
        )
    return url


def _resolve_cluster_bitable_url(args: argparse.Namespace) -> str:
    url = (args.cluster_bitable_url or "").strip()
    if not url:
        url = (os.getenv("VIDEO_ENHANCER_CLUSTER_BITABLE_URL") or "").strip()
    return url


def _subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    for key, value in project_env_values().items():
        if value is not None:
            env[str(key)] = str(value)
    # cron 等非 TTY 下子进程继承后仍可无缓冲输出，便于日志实时落盘
    env.setdefault("PYTHONUNBUFFERED", "1")
    return env


def _ensure_line_buffered_stdio() -> None:
    """非 TTY（如 cron | tee）下默认块缓冲，尽早改为行缓冲以便观察进度。"""
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(line_buffering=True)
            sys.stderr.reconfigure(line_buffering=True)
        except (OSError, ValueError, AttributeError):
            pass


def _run(cmd: list[str]) -> None:
    print("\n$ " + " ".join(cmd))
    timeout_raw = (os.getenv("WORKFLOW_SUBPROCESS_TIMEOUT_SEC") or "0").strip()
    try:
        timeout_sec = int(timeout_raw)
    except ValueError:
        timeout_sec = 0
    kwargs: dict = {"check": True, "cwd": str(PROJECT_ROOT), "env": _subprocess_env()}
    if timeout_sec > 0:
        kwargs["timeout"] = timeout_sec
    subprocess.run(cmd, **kwargs)


def _extract_ad_keys(raw_payload: dict) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in raw_payload.get("items") or []:
        if not isinstance(item, dict):
            continue
        creative = item.get("creative") or {}
        if not isinstance(creative, dict):
            continue
        k = str(creative.get("ad_key") or "").strip()
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(k)
    return out


_CRAWL_REMOVAL_LABELS = {
    "advertiser_mismatch": "广告主不匹配",
    "non_target_date": "非目标日期",
    "resume_advertising": "重投素材",
    "duplicate_ad_key": "同产品重复 ad_key",
    "per_product_truncated": "单产品硬截断",
    "cover_crossday_fingerprint": "跨日封面/URL/指纹重复",
    "cover_clip_crossday": "跨日 CLIP 封面重复",
    "cover_clip_intraday": "同日 CLIP 封面重复",
}


def _product_from_item(item: dict) -> str:
    creative = item.get("creative") or {}
    if not isinstance(creative, dict):
        creative = {}
    return str(
        item.get("product")
        or creative.get("product")
        or creative.get("advertiser_name")
        or item.get("keyword")
        or "未知产品"
    ).strip() or "未知产品"


def _count_items_by_product(items: list) -> Counter[str]:
    counts: Counter[str] = Counter()
    for item in items or []:
        if isinstance(item, dict):
            counts[_product_from_item(item)] += 1
    return counts


def _increment(counter_by_product: dict[str, Counter[str]], product: str, reason: str, count: int = 1) -> None:
    if count <= 0:
        return
    counter_by_product.setdefault(product or "未知产品", Counter())[reason] += int(count)


def _build_crawl_product_retention_report(raw_payload: dict) -> dict:
    """
    Build a per-product crawl funnel: clicked/captured -> crawl kept -> cover kept.

    The analysis gate is reported as a non-deleting reason so daily crawl review can
    still see why an item did not enter LLM analysis.
    """
    items = raw_payload.get("items") or []
    if not isinstance(items, list):
        items = []
    final_counts = _count_items_by_product(items)
    filter_report = raw_payload.get("filter_report") or {}
    if not isinstance(filter_report, dict):
        filter_report = {}
    per_product = filter_report.get("per_product") or {}
    if not isinstance(per_product, dict):
        per_product = {}

    cover_report = raw_payload.get("cover_style_intraday_report") or {}
    if not isinstance(cover_report, dict):
        cover_report = {}

    removal_by_product: dict[str, Counter[str]] = defaultdict(Counter)
    cover_removed_by_product: dict[str, Counter[str]] = defaultdict(Counter)
    products: set[str] = set(final_counts.keys()) | {str(k) for k in per_product.keys()}

    for product, info in per_product.items():
        if not isinstance(info, dict):
            continue
        product_name = str(product or "未知产品")
        _increment(removal_by_product, product_name, "advertiser_mismatch", int(info.get("advertiser_excluded") or 0))
        _increment(removal_by_product, product_name, "non_target_date", int(info.get("date_filtered") or 0))
        _increment(removal_by_product, product_name, "resume_advertising", int(info.get("resume_excluded") or 0))
        _increment(removal_by_product, product_name, "duplicate_ad_key", int(info.get("duplicate_excluded") or 0))
        _increment(removal_by_product, product_name, "per_product_truncated", int(info.get("truncated_excluded") or 0))

    for row in cover_report.get("cross_day_fingerprint_removed") or []:
        if not isinstance(row, dict):
            continue
        product = str(row.get("product") or "未知产品")
        products.add(product)
        _increment(removal_by_product, product, "cover_crossday_fingerprint")
        _increment(cover_removed_by_product, product, "cover_crossday_fingerprint")

    per_appid = cover_report.get("per_appid") or []
    if isinstance(per_appid, dict):
        cover_buckets = per_appid.values()
    elif isinstance(per_appid, list):
        cover_buckets = per_appid
    else:
        cover_buckets = []
    for bucket in cover_buckets:
        if not isinstance(bucket, dict):
            continue
        product = str(bucket.get("product") or "未知产品")
        products.add(product)
        for row in bucket.get("removed") or []:
            if not isinstance(row, dict):
                continue
            reason = str(row.get("reason") or "").strip()
            if reason == "cover_style_cluster_vs_yesterday":
                key = "cover_clip_crossday"
            elif reason == "cover_style_cluster_history_refresh":
                key = "cover_clip_history_refresh_intraday"
            elif reason == "cover_style_cluster":
                key = "cover_clip_intraday"
            else:
                key = reason or "cover_unknown"
            _increment(removal_by_product, product, key)
            _increment(cover_removed_by_product, product, key)

    analysis_ineligible_by_product: Counter[str] = Counter()
    for item in items:
        if not isinstance(item, dict):
            continue
        creative = item.get("creative") or {}
        if not isinstance(creative, dict) or is_creative_analyzable(creative):
            continue
        product = _product_from_item(item)
        analysis_ineligible_by_product[product] += 1
        products.add(product)

    rows: list[dict] = []
    for product in sorted(products):
        info = per_product.get(product) if isinstance(per_product.get(product), dict) else {}
        reason_counts = {
            key: int(value)
            for key, value in sorted(removal_by_product.get(product, Counter()).items())
            if int(value) > 0
        }
        rows.append(
            {
                "product": product,
                "clicked_detail_rows": int(info.get("clicked_detail_rows") or 0),
                "dom_cards": int(info.get("dom_cards") or 0),
                "captured_materials": int(info.get("captured") or 0),
                "target_date_hits": int(info.get("date_hits") or 0),
                "kept_after_crawl_filter": int(info.get("after") or 0),
                "kept_after_cover_filter": int(final_counts.get(product, 0)),
                "removed_total": int(sum(reason_counts.values())),
                "removed_reasons": reason_counts,
                "removed_reason_labels": {
                    key: _CRAWL_REMOVAL_LABELS.get(key, key) for key in reason_counts
                },
                "cover_removed_reasons": {
                    key: int(value)
                    for key, value in sorted(cover_removed_by_product.get(product, Counter()).items())
                    if int(value) > 0
                },
                "analysis_gate_ineligible": int(analysis_ineligible_by_product.get(product, 0)),
            }
        )

    summary_reasons: Counter[str] = Counter()
    for row in rows:
        for key, value in (row.get("removed_reasons") or {}).items():
            summary_reasons[str(key)] += int(value or 0)
    summary = {
        "clicked_detail_rows": sum(int(row.get("clicked_detail_rows") or 0) for row in rows),
        "captured_materials": sum(int(row.get("captured_materials") or 0) for row in rows),
        "kept_after_crawl_filter": sum(int(row.get("kept_after_crawl_filter") or 0) for row in rows),
        "kept_after_cover_filter": sum(int(row.get("kept_after_cover_filter") or 0) for row in rows),
        "removed_total": sum(int(row.get("removed_total") or 0) for row in rows),
        "removed_reasons": {
            key: int(value)
            for key, value in sorted(summary_reasons.items())
            if int(value) > 0
        },
        "removed_reason_labels": {
            key: _CRAWL_REMOVAL_LABELS.get(key, key)
            for key, value in sorted(summary_reasons.items())
            if int(value) > 0
        },
    }

    return {
        "target_date": str(raw_payload.get("target_date") or ""),
        "crawl_mode": raw_payload.get("crawl_mode"),
        "ui_date_range": raw_payload.get("ui_date_range"),
        "scope": "crawl_and_pre_analysis",
        "summary": summary,
        "per_product": rows,
    }


def _write_crawl_product_retention_report(
    raw_payload: dict,
    *,
    output_prefix: str,
    target_date: str,
) -> Path:
    report = _build_crawl_product_retention_report(raw_payload)
    report["target_date"] = target_date
    raw_payload["crawl_product_retention_report"] = report
    path = DATA_DIR / f"{output_prefix}_crawl_product_retention.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    summary = report.get("summary") or {}
    print(
        "\n[step:crawl-retention] 产品抓取保留漏斗："
        f"点卡 {summary.get('clicked_detail_rows', 0)} / "
        f"抓到 {summary.get('captured_materials', 0)} / "
        f"爬取保留 {summary.get('kept_after_crawl_filter', 0)} / "
        f"封面后保留 {summary.get('kept_after_cover_filter', 0)}；报告 {path.name}"
    )
    for row in report.get("per_product") or []:
        if not isinstance(row, dict):
            continue
        reasons = row.get("removed_reasons") or {}
        labels = row.get("removed_reason_labels") or {}
        reason_text = ", ".join(
            f"{labels.get(key, key)}={value}"
            for key, value in reasons.items()
            if int(value or 0) > 0
        ) or "-"
        print(
            "  · "
            f"{row.get('product')}: "
            f"点卡 {row.get('clicked_detail_rows', 0)}，"
            f"抓到 {row.get('captured_materials', 0)}，"
            f"保留 {row.get('kept_after_cover_filter', 0)}，"
            f"去掉 {row.get('removed_total', 0)}（{reason_text}）"
        )
    return path


def main() -> None:
    _ensure_line_buffered_stdio()
    print_openrouter_key_meter("工作流开始前")
    args = parse_args()
    bitable_url = _resolve_bitable_url(args)
    cluster_bitable_url = _resolve_cluster_bitable_url(args)
    if args.date:
        target_date = args.date
    else:
        target_date = (date.today() - timedelta(days=1)).isoformat()

    output_prefix = f"workflow_video_enhancer_{target_date}"
    raw_path = DATA_DIR / f"{output_prefix}_raw.json"
    analysis_path = DATA_DIR / f"video_analysis_{output_prefix}_raw.json"
    sugg_json_path = DATA_DIR / f"ua_suggestion_{output_prefix}.json"
    sugg_md_path = DATA_DIR / f"ua_suggestion_{output_prefix}.md"

    py = sys.executable

    # 1) 抓取
    crawl_cmd = [
        py,
        "-m",
        "ua_workflows.video_enhancer.crawl",
        "--target-date",
        target_date,
        "--output-prefix",
        output_prefix,
    ]
    if args.products.strip():
        crawl_cmd += ["--products", args.products.strip()]
    _run(crawl_cmd)

    # 2) 先把“原始素材”落库（不带分析），再做增量分析
    init_pipeline_db()
    raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
    n_after_crawl = len(raw_payload.get("items") or [])
    print(f"\n[step:crawl] 抓取完成，raw items={n_after_crawl} 条（{raw_path.name}）")

    if args.skip_cover_dedupe:
        print(
            "[cover-style] 已跳过（--skip-cover-dedupe）。"
            "若需与抓取拆分：请用 workflow_video_enhancer_steps.py crawl_store --crawl-only 后再 cover_store。"
        )
        print(
            f"\n[step:cover-style] 未做封面筛选，全量 {len(raw_payload.get('items') or [])} 条进入后续"
        )
        sk_path = write_cover_filter_step_json_skipped(
            DATA_DIR, output_prefix, target_date, "SKIP_COVER_DEDUPE_CLI_FLAG"
        )
        print(f"[filter-json] {sk_path.name}（封面步骤已跳过）")
    elif is_cover_style_intraday_enabled():
        items = raw_payload.get("items") or []
        items2, cover_rep = apply_intraday_cover_style_dedupe(
            items, target_date, raw_payload.get("crawl_date")
        )
        raw_payload["items"] = items2
        raw_payload["cover_style_intraday_report"] = cover_rep
        raw_path.write_text(json.dumps(raw_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        n_in = int(cover_rep.get("input_count", len(items)))
        n_out = int(cover_rep.get("output_count", len(items2)))
        n_rm = n_in - n_out
        print(
            f"\n[step:cover-style] 日内封面聚类筛选: 入 {n_in} 条 → 出 {n_out} 条，本环节剔除 {n_rm} 条"
        )
        print(
            f"[cover-style] 日内同产品封面聚类（展示估值最高保留）: "
            f"{cover_rep.get('input_count', len(items))} → {cover_rep.get('output_count', len(items2))} 条"
            + (f", removed={cover_rep.get('removed_total', 0)}" if not cover_rep.get("skipped") else "")
        )
        cover_rep_path = DATA_DIR / f"{output_prefix}_cover_style_intraday.json"
        cover_rep_path.write_text(json.dumps(cover_rep, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[cover-style] 报告已写 {cover_rep_path.name}")
        cf_path = write_cover_filter_step_json(DATA_DIR, output_prefix, target_date, cover_rep)
        print(f"[filter-json] {cf_path.name}（封面：跨日指纹 + 日内聚类剔除明细）")
    else:
        print(
            "[cover-style] 封面 CLIP 视觉去重已关闭（COVER_STYLE_INTRADAY_ENABLED=0）。"
        )
        print(
            f"\n[step:cover-style] 未启用封面筛选，全量 {len(raw_payload.get('items') or [])} 条进入后续"
        )
        sk_path = write_cover_filter_step_json_skipped(
            DATA_DIR, output_prefix, target_date, "COVER_STYLE_INTRADAY_DISABLED"
        )
        print(f"[filter-json] {sk_path.name}（封面步骤已跳过）")

    merge_cover_similarity_counts(raw_payload)
    raw_payload, _tot, _elig, _skip = merge_inspiration_filter_stats(raw_payload)
    _write_crawl_product_retention_report(
        raw_payload,
        output_prefix=output_prefix,
        target_date=target_date,
    )
    raw_path.write_text(json.dumps(raw_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    detail = (raw_payload.get("filter_report") or {}).get("inspiration_detail") or {}
    print("\n[step:inspiration-gate] 灵感分析准入（raw 不删条，仅统计）")
    print(
        f"  · 汇总: 全量 {_tot} 条，可分析 {_elig} 条，本关口不可分析 {_skip} 条"
    )
    for line in format_inspiration_detail_lines(detail):
        print(line)
    print("  （filter_report.inspiration_detail 已写入）")

    n_raw = upsert_daily_creative_insights(target_date, raw_payload, {})
    print(
        f"\n[step:db-raw] daily_creative_insights 原始快照入库 {n_raw} 行（analysis 仍为空）"
    )

    # 2b) 写入素材主库并执行多维去重归组（记录全量）
    n_lib, n_grouped = upsert_creative_library(target_date, raw_payload)
    print(
        f"[step:db-library] creative_library 写入/更新 {n_lib} 条，当日重复归组 {n_grouped} 条"
    )
    maybe_run_cover_embedding_after_library(target_date)

    # 2c) 全量 raw 进入后续；灵感分析仅处理「准入 + 未缓存成功分析」
    pipeline_items: list[dict] = [x for x in (raw_payload.get("items") or []) if isinstance(x, dict)]

    pipeline_raw_payload = {
        "target_date": raw_payload.get("target_date"),
        "crawl_date": raw_payload.get("crawl_date"),
        "total": len(pipeline_items),
        "items": pipeline_items,
        "filter_report": raw_payload.get("filter_report"),
    }

    pipeline_ad_keys = _extract_ad_keys(pipeline_raw_payload)
    existing_analysis = load_existing_success_analysis_by_ad_keys(pipeline_ad_keys)

    _lb = resolve_inspiration_crossday_lookback_days()
    deduped_items, dedup_report = get_deduped_items_for_analysis(
        target_date,
        {"items": pipeline_items},
        history_lookback_days=_lb,
    )
    dedup_report_path = DATA_DIR / f"{output_prefix}_analysis_dedup_report.json"
    dedup_report_path.write_text(
        json.dumps(dedup_report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    dedup_kept: set[str] = set()
    for _it in deduped_items:
        if not isinstance(_it, dict):
            continue
        _c = _it.get("creative") or {}
        if isinstance(_c, dict):
            _ak = str(_c.get("ad_key") or "").strip()
            if _ak:
                dedup_kept.add(_ak)
    cross_matched = sorted(
        {
            str(r.get("matched_ad_key") or "").strip()
            for r in (dedup_report.get("crossday_removed") or [])
            if isinstance(r, dict)
        }
        - {""}
    )
    if cross_matched:
        existing_analysis.update(load_existing_success_analysis_by_ad_keys(cross_matched))

    dedup_redirect = build_inspiration_dedup_redirect_map(dedup_report)

    pending_items: list[dict] = []
    skipped_no_media = 0
    skipped_cache = 0
    skipped_dedup = 0
    ineligible_reasons: dict[str, int] = defaultdict(int)
    analysis_queue_by_product: dict[str, dict] = defaultdict(
        lambda: {
            "raw_after_cover": 0,
            "dedup_removed": 0,
            "cache_reused": 0,
            "ineligible_total": 0,
            "ineligible_reasons": defaultdict(int),
            "llm_queued": 0,
        }
    )
    for item in pipeline_items:
        if not isinstance(item, dict):
            continue
        creative = item.get("creative") or {}
        if not isinstance(creative, dict):
            continue
        product = _product_from_item(item)
        analysis_queue_by_product[product]["raw_after_cover"] += 1
        k = str(creative.get("ad_key") or "").strip()
        if not k:
            continue
        if k not in dedup_kept:
            skipped_dedup += 1
            analysis_queue_by_product[product]["dedup_removed"] += 1
            continue
        if k in existing_analysis:
            skipped_cache += 1
            analysis_queue_by_product[product]["cache_reused"] += 1
            continue
        if not is_creative_analyzable(creative):
            skipped_no_media += 1
            reason = classify_ineligible_reason(creative)
            ineligible_reasons[reason] += 1
            analysis_queue_by_product[product]["ineligible_total"] += 1
            analysis_queue_by_product[product]["ineligible_reasons"][reason] += 1
            continue
        pending_items.append(item)
        analysis_queue_by_product[product]["llm_queued"] += 1

    pending_raw_payload = {
        "target_date": pipeline_raw_payload.get("target_date"),
        "crawl_date": pipeline_raw_payload.get("crawl_date"),
        "total": len(pending_items),
        "items": pending_items,
    }
    pending_raw_path = DATA_DIR / f"{output_prefix}_raw_pending_analysis.json"
    pending_raw_path.write_text(json.dumps(pending_raw_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    queue_rows: list[dict] = []
    for product, info in sorted(analysis_queue_by_product.items()):
        raw_after_cover = int(info.get("raw_after_cover") or 0)
        dedup_removed = int(info.get("dedup_removed") or 0)
        ineligible_detail = {
            str(k): int(v)
            for k, v in sorted((info.get("ineligible_reasons") or {}).items())
            if int(v) > 0
        }
        queue_rows.append(
            {
                "product": product,
                "raw_after_cover": raw_after_cover,
                "dedup_removed": dedup_removed,
                "after_dedup": max(0, raw_after_cover - dedup_removed),
                "cache_reused": int(info.get("cache_reused") or 0),
                "ineligible_total": int(info.get("ineligible_total") or 0),
                "ineligible_reasons": ineligible_detail,
                "llm_queued": int(info.get("llm_queued") or 0),
            }
        )
    queue_report = {
        "target_date": target_date,
        "total": {
            "raw_after_cover": len(pipeline_items),
            "dedup_removed": skipped_dedup,
            "after_dedup": int(dedup_report.get("after_crossday") or 0),
            "cache_reused": skipped_cache,
            "ineligible_total": skipped_no_media,
            "llm_queued": len(pending_items),
        },
        "per_product": queue_rows,
    }
    queue_report_path = DATA_DIR / f"{output_prefix}_analysis_queue_report.json"
    queue_report_path.write_text(json.dumps(queue_report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\n[step:analysis-queue] 本次灵感分析入队")
    print(f"  · 全量 items: {len(pipeline_items)}（去重 ad_key 约 {len(pipeline_ad_keys)} 个）")
    print(
        f"  · 多维去重（日内+跨日，跨日窗口 lookback={dedup_report.get('history_lookback_days')}）: "
        f"{dedup_report.get('total_input')} → {dedup_report.get('after_crossday')} 条；"
        f"报告 {dedup_report_path.name}"
    )
    print(f"  · 因与代表/历史重复跳过 LLM: {skipped_dedup}")
    print(f"  · 历史已成功分析，跳过（不重复调 LLM）: {skipped_cache}")
    print(f"  · 不符准入，本环节不上分析: {skipped_no_media}")
    for rk, rv in sorted(ineligible_reasons.items(), key=lambda x: -x[1]):
        if rv:
            print(
                f"      └ {rk}（{ineligible_reason_label_cn(rk)}）: {rv}"
            )
    print(f"  · 本次将调用 analyze 子进程: {len(pending_items)} 条 → {pending_raw_path.name}")
    print(f"  · 产品入队漏斗报告: {queue_report_path.name}")

    new_analysis_payload = {
        "input_file": str(pending_raw_path),
        "total_items": len(pending_items),
        "analyzed_items": 0,
        "results": [],
    }
    if pending_items:
        _run(
            [
                py,
                "-m",
                "ua_workflows.video_enhancer.analyze",
                "--input",
                str(pending_raw_path),
                "--output",
                str(analysis_path),
            ]
        )
        new_analysis_payload = json.loads(analysis_path.read_text(encoding="utf-8"))

    # 过滤失败分析：失败不入库，并输出失败清单
    failed_analysis: list[dict] = []
    new_success_by_ad: dict[str, dict] = {}
    for it in new_analysis_payload.get("results") or []:
        if not isinstance(it, dict):
            continue
        ad_key = str(it.get("ad_key") or "").strip()
        if not ad_key:
            continue
        text = str(it.get("analysis") or "")
        if not text or text.startswith("[ERROR]"):
            failed_analysis.append(
                {
                    "ad_key": ad_key,
                    "video_url": normalize_video_url_for_consumption(str(it.get("video_url") or "")),
                    "error": text or "empty analysis",
                }
            )
            continue
        new_success_by_ad[ad_key] = it

    failed_path = DATA_DIR / f"{output_prefix}_analysis_failed.json"
    failed_path.write_text(
        json.dumps(
            {
                "target_date": target_date,
                "failed_count": len(failed_analysis),
                "failed": failed_analysis,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    if failed_analysis:
        print(f"[analysis] 失败 {len(failed_analysis)} 条（未入库），详见 {failed_path}")
        for x in failed_analysis:
            print(f"  - ad_key={x.get('ad_key')} error={x.get('error')}")

    # 组装「完整分析结果」（历史成功 + 本次成功 + 去重副本沿用 canonical 分析）
    combined_results: list[dict] = combined_analysis_results_for_pipeline(
        pipeline_items,
        new_success_by_ad,
        existing_analysis,
        dedup_redirect,
    )

    analysis_payload = {
        "input_file": str(raw_path),
        "total_items": len(raw_payload.get("items") or []),
        "pipeline_items": len(pipeline_items),
        "analyzed_items": len(combined_results),
        "reused_existing": len(existing_analysis),
        "new_success": len(new_success_by_ad),
        "new_failed": len(failed_analysis),
        "results": combined_results,
    }
    analysis_path.write_text(json.dumps(analysis_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[analysis] 合并分析结果 {len(combined_results)} 条，输出 {analysis_path}")

    # 2.8) 成人/色情素材拦截：不依赖 effect_one_liner，综合结构化字段/标题/正文/标签判断
    n_adult, adult_details = apply_adult_content_filter(combined_results)
    if n_adult:
        print(f"[content-filter] 标记 {n_adult} 条成人/色情风险素材（主表不同步；不进入方向卡片）")

    n_human_photo, human_photo_details = apply_human_photo_effect_filter(combined_results)
    if n_human_photo:
        print(
            f"[content-filter] 标记 {n_human_photo} 条非人物照片加工/电商素材"
            "（主表不同步；不进入方向卡片）"
        )

    # 2.8d) 日内玩法硬去重（默认关闭）：同 appid 同批次相似玩法仅保留展示估值更高的代表素材
    intraday_effect_details: list[dict] = []
    n_intraday_effect = 0
    intraday_effect_enabled = (os.getenv("INTRADAY_EFFECT_FILTER_ENABLED") or "0").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
        "",
    )
    if intraday_effect_enabled:
        try:
            n_intraday_effect, intraday_effect_details = apply_intraday_effect_bitable_filter(combined_results)
            if n_intraday_effect:
                print(f"[intraday-effect-filter] 标记 {n_intraday_effect} 条日内玩法重复素材（主表不同步；不进入方向卡片）")
        except Exception as e:
            print(f"[intraday-effect-filter] skipped: {e}")
            intraday_effect_details = []
            n_intraday_effect = 0
    else:
        print("[intraday-effect-filter] 已关闭（INTRADAY_EFFECT_FILTER_ENABLED=0），跳过日内玩法筛选。")

    # 2.8e) 老玩法硬拦截（默认关闭）：同 appid 近 N 天已出现过的玩法不进主表
    old_effect_details: list[dict] = []
    n_old_effect = 0
    old_effect_enabled = (os.getenv("OLD_EFFECT_BITABLE_FILTER_ENABLED") or "0").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
        "",
    )
    if old_effect_enabled:
        try:
            n_old_effect, old_effect_details = apply_old_effect_bitable_filter(target_date, combined_results)
            if n_old_effect:
                print(f"[old-effect-filter] 标记 {n_old_effect} 条老玩法重复素材（主表不同步；不进入方向卡片）")
        except Exception as e:
            print(f"[old-effect-filter] skipped: {e}")
            old_effect_details = []
            n_old_effect = 0
    else:
        print("[old-effect-filter] 已关闭（OLD_EFFECT_BITABLE_FILTER_ENABLED=0），跳过老玩法同步前筛选。")

    # 2.8f) 玩法 embedding 硬拦截（默认关闭）：高置信同义玩法不进主表/方向卡片
    effect_embedding_dup_details: list[dict] = []
    n_effect_embedding_dup = 0
    effect_embedding_dup_enabled = (os.getenv("EFFECT_EMBEDDING_DUP_FILTER_ENABLED") or "0").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
        "",
    )
    if effect_embedding_dup_enabled:
        try:
            n_effect_embedding_dup, effect_embedding_dup_details = apply_effect_embedding_duplicate_filter(
                target_date,
                combined_results,
            )
            if n_effect_embedding_dup:
                print(
                    f"[effect-embedding-filter] 标记 {n_effect_embedding_dup} 条高置信 embedding 玩法重复素材"
                    "（主表不同步；不进入方向卡片）"
                )
        except Exception as e:
            print(f"[effect-embedding-filter] skipped: {e}")
            effect_embedding_dup_details = []
            n_effect_embedding_dup = 0
    else:
        print("[effect-embedding-filter] 已关闭（EFFECT_EMBEDDING_DUP_FILTER_ENABLED=0），跳过玩法 embedding 硬拦截。")

    # 2.8g) embedding 重复候选：只打标签，不排除主表/方向卡片，用于后续人工校准
    embedding_dup_details: list[dict] = []
    n_embedding_dup = 0
    embedding_dup_enabled = (os.getenv("EMBEDDING_DUP_CANDIDATE_ENABLED") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
        "",
    )
    if embedding_dup_enabled:
        try:
            n_embedding_dup, embedding_dup_details = apply_embedding_duplicate_candidate_tags(
                target_date,
                combined_results,
            )
            if n_embedding_dup:
                print(f"[embedding-dup-candidate] 标记 {n_embedding_dup} 条 embedding 重复候选（仅打标，不排除）")
        except Exception as e:
            print(f"[embedding-dup-candidate] skipped: {e}")
            embedding_dup_details = []
            n_embedding_dup = 0
    else:
        print("[embedding-dup-candidate] 已关闭（EMBEDDING_DUP_CANDIDATE_ENABLED=0），跳过 embedding 候选打标。")

    # 2.9) vs 我方已投放特效库：命中则排除出方向卡片 + 主表不同步 + 打标
    # 当前默认关闭（LAUNCHED_EFFECTS_ENABLED=0），仅做记录不打 exclude
    launched_details: list[dict] = []
    n_le = 0
    le_enabled = (os.getenv("LAUNCHED_EFFECTS_ENABLED") or "0").strip().lower() not in ("0", "false", "no", "off", "")
    if le_enabled:
        try:
            from ua_workflows.video_enhancer.launched_effects import apply_launched_effects_filter

            n_le, launched_details = apply_launched_effects_filter(combined_results)
            if n_le:
                print(
                    f"[launched-effects] 标记 {n_le} 条与我方已投放特效匹配的素材"
                    "（主表不同步；仍写入本地 analysis JSON 备查）"
                )
        except Exception as e:
            print(f"[launched-effects] skipped: {e}")
            n_le = 0
            launched_details = []
    else:
        print("[launched-effects] 已关闭（LAUNCHED_EFFECTS_ENABLED=0），跳过我方已投筛选。")

    lf_path = write_launched_filter_step_json(
        DATA_DIR, output_prefix, target_date, launched_details, marked_count=n_le
    )
    print(f"[filter-json] {lf_path.name}（我方已投放特效库匹配 {n_le} 条）")

    if adult_details:
        sample = adult_details[:3]
        print(f"[content-filter] 命中样例：{json.dumps(sample, ensure_ascii=False)}")
    if human_photo_details:
        sample = human_photo_details[:3]
        print(f"[human-photo-filter] 命中样例：{json.dumps(sample, ensure_ascii=False)}")
    if intraday_effect_details:
        sample = intraday_effect_details[:3]
        print(f"[intraday-effect-filter] 命中样例：{json.dumps(sample, ensure_ascii=False)}")
    if old_effect_details:
        sample = old_effect_details[:3]
        print(f"[old-effect-filter] 命中样例：{json.dumps(sample, ensure_ascii=False)}")
    if effect_embedding_dup_details:
        sample = effect_embedding_dup_details[:3]
        print(f"[effect-embedding-filter] 命中样例：{json.dumps(sample, ensure_ascii=False)}")
    if embedding_dup_details:
        sample = embedding_dup_details[:3]
        print(f"[embedding-dup-candidate] 命中样例：{json.dumps(sample, ensure_ascii=False)}")

    # 如有 2.8/2.9 标记，回写 analysis JSON
    if n_adult or n_human_photo or n_intraday_effect or n_old_effect or n_effect_embedding_dup or n_embedding_dup or n_le:
        analysis_payload["results"] = combined_results
        analysis_path.write_text(json.dumps(analysis_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # 2.5) 回写成功分析 + 标签到 DB（须在 2.9 特效库打标之后，以便写入 launched_effect_match）
    analysis_by_ad: dict[str, dict] = {}
    for it in combined_results:
        if isinstance(it, dict):
            k = str(it.get("ad_key") or "")
            v = str(it.get("analysis") or "")
            if k and v and not v.startswith("[ERROR]"):
                mtags = it.get("material_tags")
                if not isinstance(mtags, list):
                    mtags = []
                analysis_by_ad[k] = {
                    "analysis": v,
                    "material_tags": mtags,
                    "exclude_from_bitable": bool(it.get("exclude_from_bitable")),
                    "exclude_from_cluster": bool(it.get("exclude_from_cluster")),
                    "style_filter_match_summary": str(it.get("style_filter_match_summary") or ""),
                    "launched_effect_match": it.get("launched_effect_match"),
                    "effect_one_liner": str(it.get("effect_one_liner") or ""),
                    "ad_one_liner": str(it.get("ad_one_liner") or ""),
                    "play_fingerprint": str(it.get("play_fingerprint") or ""),
                    "differentiator": str(it.get("differentiator") or ""),
                    "template_fingerprint": str(it.get("template_fingerprint") or ""),
                    "play_asset_id": str(it.get("play_asset_id") or ""),
                    "play_asset_name": str(it.get("play_asset_name") or ""),
                    "play_asset_subtag_ids": str(it.get("play_asset_subtag_ids") or ""),
                    "play_asset_subtag_names": str(it.get("play_asset_subtag_names") or ""),
                    "play_asset_novelty_label": str(it.get("play_asset_novelty_label") or ""),
                    "play_asset_match_source": str(it.get("play_asset_match_source") or ""),
                    "play_asset_classification_reason": str(it.get("play_asset_classification_reason") or ""),
                    "effect_embedding_duplicate_match": it.get("effect_embedding_duplicate_match"),
                    "embedding_duplicate_candidate": it.get("embedding_duplicate_candidate"),
                }
    n_insights = upsert_daily_creative_insights(target_date, raw_payload, analysis_by_ad)
    n_filter_logs = upsert_daily_video_enhancer_filter_log(
        target_date,
        raw_payload.get("filter_report") if isinstance(raw_payload, dict) else None,
    )
    print(
        f"[DB] 已写入 daily_creative_insights: {n_insights} 条（含重复，完整记录），"
        f"daily_video_enhancer_filter_log: {n_filter_logs} 行。"
    )
    # 2.6) 把本次成功分析结果同步回 creative_library（带 analysis 更新）
    _, _ = upsert_creative_library(target_date, raw_payload, analysis_by_ad)
    print(f"[DB] creative_library 分析结果已同步。")

    # 2.7) 玩法嵌入：仅对结构化玩法指纹/核心卖点计算 embedding，供玩法重复候选使用。
    _store_effect_one_liner_embeddings(analysis_by_ad)

    try:
        review_dashboard_path = write_filter_review_dashboard(target_date)
        print(f"[review-dashboard] 已写筛选复核看板：{review_dashboard_path}")
    except Exception as e:
        print(f"[review-dashboard] 生成失败（不影响主流程）：{e}")

    # 有失败时：成功率 ≥ 90% 则继续推送，否则停止后续 UA 建议/推送入库
    total_analyzed = len(new_success_by_ad) + len(failed_analysis)
    success_rate = (len(new_success_by_ad) / total_analyzed * 100) if total_analyzed > 0 else 100.0
    if failed_analysis:
        print(
            f"[workflow] 视频分析：成功 {len(new_success_by_ad)}/{total_analyzed}"
            f"（成功率 {success_rate:.0f}%），失败明细见：{failed_path}"
        )
        if success_rate < 90:
            print(
                "[workflow] 成功率低于 90%，停止后续 UA 建议/推送入库。"
            )
            try:
                from ua_workflows.video_enhancer.acceptance import run_acceptance_after_workflow

                run_acceptance_after_workflow(target_date, partial=True)
            except SystemExit:
                try:
                    from ua_workflows.video_enhancer.flow_report import run_flow_report_after_workflow

                    run_flow_report_after_workflow(target_date, partial=True)
                except Exception as e:
                    print(f"[flow-report] {e}")
                raise
            except Exception as e:
                print(f"[acceptance] {e}")
            try:
                from ua_workflows.video_enhancer.flow_report import run_flow_report_after_workflow

                run_flow_report_after_workflow(target_date, partial=True)
            except Exception as e:
                print(f"[flow-report] {e}")
            return
        else:
            print("[workflow] 成功率 ≥ 90%，继续执行后续步骤。")

    # 3) 兼容生成旧 UA 建议（方向卡片）。失败不阻塞主表同步和新玩法日报推送。
    try:
        _run(
            [
                py,
                "-m",
                "ua_workflows.video_enhancer.suggestions",
                "--input",
                str(analysis_path),
                "--output-json",
                str(sugg_json_path),
                "--output-md",
                str(sugg_md_path),
            ]
        )
    except subprocess.CalledProcessError as e:
        print(
            f"[workflow] 方向卡片生成失败（exit={e.returncode}），"
            "继续执行主表同步和新玩法日报推送。"
        )

    # 检查兼容聚类结果：仅用于决定是否同步旧聚类表，不阻塞主表/日报。
    cluster_ok = True
    if sugg_json_path.exists():
        try:
            _sugg_check = json.loads(sugg_json_path.read_text(encoding="utf-8"))
        except Exception:
            _sugg_check = {}
        if _sugg_check.get("skipped_llm"):
            cluster_ok = False
            print(
                "[workflow] 方向卡片生成失败（skipped_llm=True），"
                "仅跳过旧聚类表同步，继续主表同步/日报推送。"
            )
        else:
            _cards = ((_sugg_check.get("suggestion") or {}).get("方向卡片") or [])
            if not isinstance(_cards, list) or not any(
                isinstance(c, dict) and c for c in _cards
            ):
                cluster_ok = False
                print(
                    "[workflow] 方向卡片为空（无有效方向），"
                    "仅跳过旧聚类表同步，继续主表同步/日报推送。"
                )
    else:
        cluster_ok = False
        print("[workflow] 方向卡片 JSON 不存在，仅跳过旧聚类表同步。")

    # 4) 同步多维表（与飞书卡片推送解耦）
    if not args.no_bitable_sync:
        sync_target = "both" if cluster_bitable_url and cluster_ok else "raw"
        if cluster_bitable_url and not cluster_ok:
            print("[sync] 聚类表链接已配置，但方向卡片不可用，本次仅同步主表。")
        sync_cmd = [
            py,
            "-m",
            "ua_workflows.video_enhancer.sync",
            "--url",
            bitable_url,
            "--raw",
            str(raw_path),
            "--analysis",
            str(analysis_path),
            "--suggestion-json",
            str(sugg_json_path),
            "--suggestion-md",
            str(sugg_md_path),
            "--sync-target",
            sync_target,
            "--no-card",
        ]
        if cluster_bitable_url and cluster_ok:
            sync_cmd.extend(["--cluster-url", cluster_bitable_url])
        _run(sync_cmd)
    else:
        print("[sync] 已按参数跳过多维表同步（--no-bitable-sync）。")

    # 5) 飞书卡片推送（独立于多维表同步）
    if not args.no_card:
        card_cmd = [
            py,
            "-m",
            "ua_workflows.video_enhancer.push_feishu",
            "--date",
            target_date,
        ]
        test_webhook = os.getenv("FEISHU_TEST_WEBHOOK", "").strip()
        if test_webhook:
            card_cmd.extend(["--feishu-webhook", test_webhook])
        _run(card_cmd)
    else:
        print("[card] 已按参数跳过飞书卡片推送（--no-card）。")

    # 6) 将本次推送建议写入专用 pipeline DB（推送表）
    init_pipeline_db()
    suggestion_payload = json.loads(sugg_json_path.read_text(encoding="utf-8")) if sugg_json_path.exists() else {}

    parsed = urlparse(bitable_url)
    parts = [p for p in parsed.path.split("/") if p]
    app_token = parts[1] if len(parts) >= 2 and parts[0] == "base" else ""
    table_id = (parse_qs(parsed.query or "").get("table") or [""])[0]
    card_md = sugg_md_path.read_text(encoding="utf-8") if sugg_md_path.exists() else ""
    n_push = 0
    if app_token and table_id and suggestion_payload:
        n_push = upsert_daily_push_content(
            target_date,
            suggestion_payload,
            card_md,
            app_token,
            table_id,
            push_status="pending" if not args.no_card else "synced_without_push",
            push_response=None,
        )
        if n_push == 0 and not should_persist_suggestion_to_push_table(suggestion_payload):
            print("[workflow] 聚类建议未写入 daily_ua_push_content（结果为空或 LLM 失败）。")

    print(
        f"[DB] 已写入 daily_creative_insights: {n_insights} 条，"
        f"daily_video_enhancer_filter_log: {n_filter_logs} 行，"
        f"daily_ua_push_content: {n_push} 条。"
    )

    # 7) 企业微信推送 + Google Sheet 同步
    multi_cmd = [
        py,
        "-m",
        "ua_workflows.video_enhancer.push_multichannel",
        "--date",
        target_date,
        "--raw",
        str(raw_path),
        "--suggestion-md",
        str(sugg_md_path),
        "--suggestion-json",
        str(sugg_json_path),
    ]
    if args.no_wecom:
        multi_cmd.append("--sheet-only")
    if args.no_sheet:
        multi_cmd.append("--wecom-only")
    # 只有同时 no_wecom + no_sheet 时才跳过
    if not (args.no_wecom and args.no_sheet):
        _run(multi_cmd)

    print_openrouter_key_meter("工作流结束后")

    try:
        from ua_workflows.video_enhancer.acceptance import run_acceptance_after_workflow

        run_acceptance_after_workflow(target_date, partial=False)
    except SystemExit:
        try:
            from ua_workflows.video_enhancer.flow_report import run_flow_report_after_workflow

            run_flow_report_after_workflow(target_date, partial=False)
        except Exception as e:
            print(f"[flow-report] {e}")
        raise
    except Exception as e:
        print(f"[acceptance] {e}")

    try:
        from ua_workflows.video_enhancer.flow_report import run_flow_report_after_workflow

        run_flow_report_after_workflow(target_date, partial=False)
    except Exception as e:
        print(f"[flow-report] {e}")

    print("\n[完成] 全流程执行完成。")
    print(f"- raw: {raw_path}")
    print(f"- analysis: {analysis_path}")
    print(f"- suggestion_json: {sugg_json_path}")
    print(f"- suggestion_md: {sugg_md_path}")
    usage_line = format_video_enhancer_usage_log_line(target_date)
    if usage_line:
        print(usage_line)


if __name__ == "__main__":
    main()
