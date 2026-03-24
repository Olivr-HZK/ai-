"""
Video Enhancer 全流程工作流（一键）：
1) 抓取（按日期 + 指定产品）
2) 视频灵感分析（基于 raw JSON）
3) 生成统一 UA 建议（方向卡片）
4) 同步 raw + 分析 到多维表
5) 推送 UA 建议飞书卡片

示例：
python scripts/workflow_video_enhancer_full_pipeline.py \
  --date 2026-03-18 \
  --products "UpFoto - AI Photo Enhancer,Remini - AI Photo Enhancer"

多维表链接可写在 .env：`VIDEO_ENHANCER_BITABLE_URL=...`（含 table= 的完整浏览器地址）。
也可用 `--bitable-url` 临时覆盖环境变量。
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv

from path_util import DATA_DIR, PROJECT_ROOT

load_dotenv(PROJECT_ROOT / ".env")
from video_enhancer_pipeline_db import (
    init_db as init_pipeline_db,
    load_existing_success_analysis_by_ad_keys,
    upsert_daily_creative_insights,
    upsert_daily_push_content,
    upsert_daily_video_enhancer_filter_log,
)


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


def _run(cmd: list[str]) -> None:
    print("\n$ " + " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=str(PROJECT_ROOT))


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
    n_raw = upsert_daily_creative_insights(target_date, raw_payload, {})
    print(f"[DB] 原始素材已落库 daily_creative_insights: {n_raw} 条（analysis 为空）。")

    ad_keys = _extract_ad_keys(raw_payload)
    existing_analysis = load_existing_success_analysis_by_ad_keys(ad_keys)

    pending_items: list[dict] = []
    for item in raw_payload.get("items") or []:
        if not isinstance(item, dict):
            continue
        creative = item.get("creative") or {}
        if not isinstance(creative, dict):
            continue
        k = str(creative.get("ad_key") or "").strip()
        if not k:
            continue
        if k in existing_analysis:
            continue
        pending_items.append(item)

    pending_raw_payload = {
        "target_date": raw_payload.get("target_date"),
        "crawl_date": raw_payload.get("crawl_date"),
        "total": len(pending_items),
        "items": pending_items,
    }
    pending_raw_path = DATA_DIR / f"{output_prefix}_raw_pending_analysis.json"
    pending_raw_path.write_text(json.dumps(pending_raw_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        f"[analysis] 总素材 {len(ad_keys)} 条；命中历史成功分析 {len(existing_analysis)} 条；"
        f"本次需新分析 {len(pending_items)} 条。"
    )

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

    # 组装“完整分析结果”（历史成功 + 本次成功），供后续建议/推送统一使用
    combined_results: list[dict] = []
    for item in raw_payload.get("items") or []:
        if not isinstance(item, dict):
            continue
        creative = item.get("creative") or {}
        if not isinstance(creative, dict):
            continue
        ad_key = str(creative.get("ad_key") or "").strip()
        if not ad_key:
            continue
        if ad_key in new_success_by_ad:
            combined_results.append(new_success_by_ad[ad_key])
        elif ad_key in existing_analysis:
            combined_results.append(existing_analysis[ad_key])

    analysis_payload = {
        "input_file": str(raw_path),
        "total_items": len(raw_payload.get("items") or []),
        "analyzed_items": len(combined_results),
        "reused_existing": len(existing_analysis),
        "new_success": len(new_success_by_ad),
        "new_failed": len(failed_analysis),
        "results": combined_results,
    }
    analysis_path.write_text(json.dumps(analysis_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[analysis] 合并分析结果 {len(combined_results)} 条，输出 {analysis_path}")

    # 2.5) 回写成功分析到 DB；失败不入库
    analysis_by_ad: dict[str, str] = {}
    for it in combined_results:
        if isinstance(it, dict):
            k = str(it.get("ad_key") or "")
            v = str(it.get("analysis") or "")
            if k and v and not v.startswith("[ERROR]"):
                analysis_by_ad[k] = v
    n_insights = upsert_daily_creative_insights(target_date, raw_payload, analysis_by_ad)
    n_filter_logs = upsert_daily_video_enhancer_filter_log(
        target_date,
        raw_payload.get("filter_report") if isinstance(raw_payload, dict) else None,
    )
    print(
        f"[DB] 已写入 daily_creative_insights: {n_insights} 条，"
        f"daily_video_enhancer_filter_log: {n_filter_logs} 行。"
    )

    # 有失败时：不生成 UA 建议，不入库 push_content，不做后续推送
    if failed_analysis:
        print(
            "[workflow] 检测到视频分析失败，已按要求停止后续 UA 建议/推送入库。"
            f"失败明细见：{failed_path}"
        )
        return

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

    # 4+5) 同步多维表 + 飞书卡片
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
        ]
        if cluster_bitable_url:
            sync_cmd += ["--cluster-url", cluster_bitable_url]
        if args.no_card:
            sync_cmd.append("--no-card")
        _run(sync_cmd)
    else:
        print("[sync] 已按参数跳过多维表同步（--no-bitable-sync）。")

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
        "--bitable-url",
        bitable_url,
    ]
    if args.no_wecom:
        multi_cmd.append("--sheet-only")
    if args.no_sheet:
        multi_cmd.append("--wecom-only")
    # 只有同时 no_wecom + no_sheet 时才跳过
    if not (args.no_wecom and args.no_sheet):
        _run(multi_cmd)

    print("\n[完成] 全流程执行完成。")
    print(f"- raw: {raw_path}")
    print(f"- analysis: {analysis_path}")
    print(f"- suggestion_json: {sugg_json_path}")
    print(f"- suggestion_md: {sugg_md_path}")


if __name__ == "__main__":
    main()

