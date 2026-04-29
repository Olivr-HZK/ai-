"""
Video Enhancer 全流程工作流（一键）：
1) 抓取（按日期 + 指定产品）
2) 可选封面日内去重（可用 --skip-cover-dedupe 跳过；与抓取拆分请用 workflow_video_enhancer_steps crawl_store --crawl-only + cover_store）
2.0) DOM 详情补全（广大大登录，对无 video_url 的条目补 source_url 等；需 .env 账号；可用 --skip-dom-enrich 跳过）
2.0b) 统计灵感分析准入（不删 raw；不符准入的不进分析）
3) 视频灵感分析（基于 raw JSON）
4) 生成统一 UA 建议（方向卡片）
5) 同步 raw + 分析 到多维表
6) 推送 UA 建议飞书卡片

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
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv

from path_util import DATA_DIR, PROJECT_ROOT

load_dotenv(PROJECT_ROOT / ".env")

from llm_client import print_openrouter_key_meter

from cover_embedding import maybe_run_cover_embedding_after_library
from cover_style_intraday import apply_intraday_cover_style_dedupe, is_cover_style_intraday_enabled

from filter_step_report_util import (
    write_cover_filter_step_json,
    write_cover_filter_step_json_skipped,
    write_launched_filter_step_json,
)

from analyze_video_from_raw_json import is_creative_analyzable

from tiktok_video_resolve import (
    classify_ineligible_reason,
    format_inspiration_detail_lines,
    ineligible_reason_label_cn,
    merge_inspiration_filter_stats,
)

from ua_crawl_db import format_video_enhancer_usage_log_line

from video_enhancer_pipeline_db import (
    build_inspiration_dedup_redirect_map,
    combined_analysis_results_for_pipeline,
    get_deduped_items_for_analysis,
    init_db as init_pipeline_db,
    load_existing_success_analysis_by_ad_keys,
    resolve_inspiration_crossday_lookback_days,
    should_persist_suggestion_to_push_table,
    upsert_analysis_embedding,
    upsert_creative_library,
    upsert_daily_creative_insights,
    upsert_daily_push_content,
    upsert_daily_video_enhancer_filter_log,
)


def _store_analysis_embeddings(analysis_by_ad: dict[str, dict]) -> None:
    """对有分析文本的素材计算嵌入向量并写入 creative_library.analysis_embedding。"""
    try:
        import llm_client
    except ImportError:
        return
    stored = 0
    failed = 0
    for ad_key, info in analysis_by_ad.items():
        text = str(info.get("analysis") or "") if isinstance(info, dict) else str(info or "")
        if not text or text.startswith("[ERROR]") or len(text) < 20:
            continue
        try:
            vec = llm_client.call_embedding(text[:2000])
            blob = llm_client.embedding_to_bytes(vec)
            upsert_analysis_embedding(ad_key, blob)
            stored += 1
        except Exception as e:
            failed += 1
            print(f"[embedding] failed ad_key={ad_key[:12]}: {e}")
    if stored:
        print(f"[embedding] 已为 {stored} 条素材写入分析嵌入向量。")
    if failed:
        print(f"[embedding] {failed} 条 embedding 失败（不影响主流程）。")


def _apply_semantic_dedup(target_date: str, combined_results: list[dict]) -> int:
    """对 combined_results 中有分析文本的素材与历史嵌入做语义比对。
    命中则设 exclude_from_cluster=True，返回标记数。"""
    try:
        import llm_client
        from video_enhancer_pipeline_db import SEMANTIC_DEDUP_THRESHOLD, load_embeddings_for_crossday
    except ImportError:
        return 0

    all_hist = load_embeddings_for_crossday(target_date)
    if not all_hist:
        return 0

    from collections import defaultdict
    hist_by_app: dict[str, list] = defaultdict(list)
    for h in all_hist:
        aid = str(h.get("appid") or "")
        hist_by_app[aid].append(h)

    marked = 0
    for r in combined_results:
        if not isinstance(r, dict) or r.get("exclude_from_cluster"):
            continue
        analysis = str(r.get("analysis") or "")
        if not analysis or len(analysis) < 20 or analysis.startswith("[ERROR]"):
            continue
        appid = str(r.get("appid") or "")
        bucket = hist_by_app.get(appid, [])
        if not bucket:
            continue
        try:
            vec = llm_client.call_embedding(analysis[:2000])
        except Exception:
            continue

        best_sim = 0.0
        best_ak = ""
        for h in bucket:
            h_vec = llm_client.bytes_to_embedding(h["analysis_embedding"])
            sim = llm_client.cosine_similarity(vec, h_vec)
            if sim > best_sim:
                best_sim = sim
                best_ak = h["ad_key"]

        if best_sim >= SEMANTIC_DEDUP_THRESHOLD:
            r["exclude_from_cluster"] = True
            r["semantic_dedup_matched"] = best_ak
            r["semantic_dedup_similarity"] = round(best_sim, 3)
            ad_key = str(r.get("ad_key") or "")[:12]
            print(f"[semantic-dedup] {ad_key} ≈ {best_ak[:12]} (sim={best_sim:.3f}) → exclude_from_cluster")
            marked += 1

    return marked


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
    p.add_argument(
        "--skip-dom-enrich",
        action="store_true",
        help="跳过 DOM 详情补全（enrich_raw_with_dom_detail.py，无账号或调试时可关）",
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


def _print_dom_enrich_report_summary(output_prefix: str) -> None:
    """读取 DOM 补全报告，打印待处理/成功/失败条数。"""
    report_path = DATA_DIR / f"{output_prefix}_dom_enrich_report.json"
    if not report_path.exists():
        print("[dom-enrich] 本轮未生成 dom_enrich_report（跳过补全或尚未写出）")
        return
    try:
        r = json.loads(report_path.read_text(encoding="utf-8"))
    except OSError as e:
        print(f"[dom-enrich] 读报告失败: {e}")
        return
    pending = int(r.get("pending_total") or 0)
    results = r.get("results") or []
    ok = sum(1 for x in results if isinstance(x, dict) and x.get("status") == "ok")
    fail = sum(1 for x in results if isinstance(x, dict) and x.get("status") != "ok")
    note = str(r.get("note") or "")
    extra = f" note={note}" if note else ""
    print(
        f"[dom-enrich] 补全统计: 待处理 {pending} 条，详情合并成功 {ok} 条，未成功/失败 {fail} 条"
        f"（报告 {report_path.name}）{extra}"
    )


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
        "scripts/test_video_enhancer_two_competitors_318.py",
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

    # 2.0) 列表无直链时：浏览器进广大大 DOM 点详情，合并 source_url / resource_urls 等到 raw（再入库）
    if not args.skip_dom_enrich:
        email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
        pwd = os.getenv("GUANGDADA_PASSWORD")
        if email and pwd:
            print("\n[dom-enrich] 开始 DOM 详情补全（无 video_url 的素材）…")
            enriched_path = DATA_DIR / f"{output_prefix}_raw_dom_enriched.json"
            try:
                enriched_path.unlink(missing_ok=True)
            except OSError:
                pass
            try:
                _run(
                    [
                        py,
                        "scripts/enrich_raw_with_dom_detail.py",
                        "--raw",
                        str(raw_path),
                    ]
                )
            except subprocess.CalledProcessError as e:
                print(f"[dom-enrich] 子进程失败，沿用补全前 raw: {e}")
            if enriched_path.exists():
                raw_payload = json.loads(enriched_path.read_text(encoding="utf-8"))
                raw_path.write_text(
                    json.dumps(raw_payload, ensure_ascii=False, indent=2), encoding="utf-8"
                )
                print(f"[dom-enrich] 已写回 {raw_path.name}（合并自 {enriched_path.name}）")
        else:
            print(
                "[dom-enrich] 跳过：未配置 GUANGDADA_EMAIL（或 GUANGDADA_USERNAME）/ GUANGDADA_PASSWORD"
            )
    else:
        print("[dom-enrich] 已跳过（--skip-dom-enrich）")

    _print_dom_enrich_report_summary(output_prefix)

    raw_payload, _tot, _elig, _skip = merge_inspiration_filter_stats(raw_payload)
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
    for item in pipeline_items:
        if not isinstance(item, dict):
            continue
        creative = item.get("creative") or {}
        if not isinstance(creative, dict):
            continue
        k = str(creative.get("ad_key") or "").strip()
        if not k:
            continue
        if k not in dedup_kept:
            skipped_dedup += 1
            continue
        if k in existing_analysis:
            skipped_cache += 1
            continue
        if not is_creative_analyzable(creative):
            skipped_no_media += 1
            ineligible_reasons[classify_ineligible_reason(creative)] += 1
            continue
        pending_items.append(item)

    pending_raw_payload = {
        "target_date": pipeline_raw_payload.get("target_date"),
        "crawl_date": pipeline_raw_payload.get("crawl_date"),
        "total": len(pending_items),
        "items": pending_items,
    }
    pending_raw_path = DATA_DIR / f"{output_prefix}_raw_pending_analysis.json"
    pending_raw_path.write_text(json.dumps(pending_raw_payload, ensure_ascii=False, indent=2), encoding="utf-8")
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
                "scripts/analyze_video_from_raw_json.py",
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
                    "video_url": str(it.get("video_url") or ""),
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

    # 2.8) 语义去重：对 combined_results 中的素材与历史嵌入比对，命中则排除出方向卡片
    n_sem = _apply_semantic_dedup(target_date, combined_results)
    if n_sem:
        print(f"[semantic-dedup] 标记 {n_sem} 条语义重复素材（exclude_from_cluster）")

    # 2.9) vs 我方已投放特效库：命中则排除出方向卡片 + 主表不同步 + 打标
    # 当前默认关闭（LAUNCHED_EFFECTS_ENABLED=0），仅做记录不打 exclude
    launched_details: list[dict] = []
    n_le = 0
    le_enabled = (os.getenv("LAUNCHED_EFFECTS_ENABLED") or "0").strip().lower() not in ("0", "false", "no", "off", "")
    if le_enabled:
        try:
            from launched_effects_db import apply_launched_effects_filter

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

    # 如有 2.8/2.9 标记，回写 analysis JSON
    if n_sem or n_le:
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
                    "ua_suggestion_single": str(it.get("ua_suggestion_single") or ""),
                    "material_tags": mtags,
                    "exclude_from_bitable": bool(it.get("exclude_from_bitable")),
                    "exclude_from_cluster": bool(it.get("exclude_from_cluster")),
                    "style_filter_match_summary": str(it.get("style_filter_match_summary") or ""),
                    "launched_effect_match": it.get("launched_effect_match"),
                    "effect_one_liner": str(it.get("effect_one_liner") or ""),
                    "ad_one_liner": str(it.get("ad_one_liner") or ""),
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

    # 2.7) 语义嵌入：对有分析文本的素材计算 embedding 并存入 creative_library
    _store_analysis_embeddings(analysis_by_ad)

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
                from workflow_video_enhancer_acceptance import run_acceptance_after_workflow

                run_acceptance_after_workflow(target_date, partial=True)
            except SystemExit:
                raise
            except Exception as e:
                print(f"[acceptance] {e}")
            return
        else:
            print("[workflow] 成功率 ≥ 90%，继续执行后续步骤。")

    # 3) 统一 UA 建议（方向卡片）
    _run(
        [
            py,
            "scripts/generate_video_enhancer_ua_suggestions_from_analysis.py",
            "--input",
            str(analysis_path),
            "--output-json",
            str(sugg_json_path),
            "--output-md",
            str(sugg_md_path),
        ]
    )

    # 检查聚类结果：若 skipped_llm=True 或无方向卡片，视为聚类失败，阻止后续推送/同步
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
                "跳过后续多维表同步/飞书卡片/企微推送。"
            )
        else:
            _cards = ((_sugg_check.get("suggestion") or {}).get("方向卡片") or [])
            if not isinstance(_cards, list) or not any(
                isinstance(c, dict) and c for c in _cards
            ):
                cluster_ok = False
                print(
                    "[workflow] 方向卡片为空（无有效方向），"
                    "跳过后续多维表同步/飞书卡片/企微推送。"
                )
    else:
        cluster_ok = False
        print("[workflow] 方向卡片 JSON 不存在，跳过后续推送。")

    if not cluster_ok:
        try:
            from workflow_video_enhancer_acceptance import run_acceptance_after_workflow
            run_acceptance_after_workflow(target_date, partial=True)
        except SystemExit:
            raise
        except Exception as e:
            print(f"[acceptance] {e}")
        return

    # 4) 同步多维表（与飞书卡片推送解耦）
    if not args.no_bitable_sync:
        sync_cmd = [
            py,
            "scripts/sync_raw_analysis_to_bitable_and_push_card.py",
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
            "both" if cluster_bitable_url else "raw",
            "--no-card",
        ]
        if cluster_bitable_url:
            sync_cmd.extend(["--cluster-url", cluster_bitable_url])
        _run(sync_cmd)
    else:
        print("[sync] 已按参数跳过多维表同步（--no-bitable-sync）。")

    # 5) 飞书卡片推送（独立于多维表同步）
    if not args.no_card:
        _run(
            [
                py,
                "scripts/push_video_enhancer_feishu_card_only.py",
                "--date",
                target_date,
            ]
        )
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
        "scripts/push_video_enhancer_multichannel.py",
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
        from workflow_video_enhancer_acceptance import run_acceptance_after_workflow

        run_acceptance_after_workflow(target_date, partial=False)
    except SystemExit:
        raise
    except Exception as e:
        print(f"[acceptance] {e}")

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

