"""
Video Enhancer 分步工作流（可单步执行）：
1) crawl_store: 爬取；可选仅写 raw（--crawl-only，不入库、不做封面去重）
2) cover_store: 封面日内去重（可选）+ 原始入库（与 crawl 拆分时使用）
3) analyze_store: 灵感分析 + 入库（按 ad_key 去重，仅成功分析入库）
4) cluster_store: 聚类/方向卡片生成 + 入库
5) push_sync: 推送与多维表同步
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv

from path_util import DATA_DIR, PROJECT_ROOT

from cover_style_intraday import apply_intraday_cover_style_dedupe, is_cover_style_intraday_enabled

from video_enhancer_pipeline_db import (
    get_deduped_items_for_analysis,
    init_db as init_pipeline_db,
    load_existing_success_analysis_by_ad_keys,
    prune_daily_creative_insights_not_in_raw,
    should_persist_suggestion_to_push_table,
    upsert_analysis_embedding,
    upsert_creative_library,
    upsert_daily_creative_insights,
    upsert_daily_push_content,
    upsert_daily_video_enhancer_filter_log,
)

load_dotenv(PROJECT_ROOT / ".env")

DB_PATH = DATA_DIR / "video_enhancer_pipeline.db"


def _default_date() -> str:
    return (date.today() - timedelta(days=1)).isoformat()


def _subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    return env


def _ensure_line_buffered_stdio() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(line_buffering=True)
            sys.stderr.reconfigure(line_buffering=True)
        except (OSError, ValueError, AttributeError):
            pass


def _run(cmd: list[str]) -> None:
    print("\n$ " + " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=str(PROJECT_ROOT), env=_subprocess_env())


def _paths(target_date: str) -> dict[str, Path]:
    prefix = f"workflow_video_enhancer_{target_date}"
    return {
        "raw": DATA_DIR / f"{prefix}_raw.json",
        "analysis": DATA_DIR / f"video_analysis_{prefix}_raw.json",
        "suggestion_json": DATA_DIR / f"ua_suggestion_{prefix}.json",
        "suggestion_md": DATA_DIR / f"ua_suggestion_{prefix}.md",
        "pending_raw": DATA_DIR / f"{prefix}_raw_pending_analysis.json",
        "failed": DATA_DIR / f"{prefix}_analysis_failed.json",
    }


def _extract_ad_keys(raw_payload: dict) -> list[str]:
    keys: list[str] = []
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
        keys.append(k)
    return keys


def _parse_bitable(url: str) -> tuple[str, str]:
    parsed = urlparse((url or "").strip())
    parts = [p for p in parsed.path.split("/") if p]
    app_token = parts[1] if len(parts) >= 2 and parts[0] == "base" else ""
    table_id = (parse_qs(parsed.query or "").get("table") or [""])[0]
    return app_token, table_id


def _load_raw_from_db(target_date: str) -> dict:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT category, product, appid, raw_json, insight_cover_style
            FROM daily_creative_insights
            WHERE target_date = ?
            ORDER BY id ASC
            """,
            (target_date,),
        )
        items: list[dict] = []
        for row in cur.fetchall():
            raw = str(row["raw_json"] or "").strip()
            if not raw:
                continue
            try:
                creative = json.loads(raw)
            except Exception:
                continue
            rec: Dict[str, Any] = {
                "category": row["category"],
                "product": row["product"],
                "appid": row["appid"],
                "creative": creative,
            }
            try:
                ics = row["insight_cover_style"]
            except Exception:
                ics = None
            if ics and str(ics).strip():
                try:
                    rec["cover_style"] = json.loads(str(ics))
                except Exception:
                    rec["cover_style"] = None
            items.append(rec)
        return {"target_date": target_date, "items": items, "total": len(items)}
    finally:
        conn.close()


def _load_analysis_from_db(target_date: str) -> dict:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT category, product, appid, ad_key, platform, video_duration, video_url, raw_json, insight_analysis, insight_ua_suggestion
            FROM daily_creative_insights
            WHERE target_date = ?
              AND COALESCE(TRIM(insight_analysis), '') <> ''
              AND insight_analysis NOT LIKE '[ERROR]%'
            ORDER BY id ASC
            """,
            (target_date,),
        )
        results: list[dict] = []
        for row in cur.fetchall():
            pipeline_tags: list[str] = []
            rj: Dict[str, Any] = {}
            raw = str(row["raw_json"] or "").strip()
            if raw:
                try:
                    rj = json.loads(raw) or {}
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
            rec: Dict[str, Any] = {
                "category": row["category"],
                "product": row["product"],
                "appid": row["appid"],
                "ad_key": row["ad_key"],
                "platform": row["platform"],
                "video_duration": row["video_duration"],
                "creative_type": ct,
                "video_url": vu,
                "image_url": iu if ct == "image" else "",
                "preview_img_url": pu if ct == "video" else "",
                "pipeline_tags": pipeline_tags,
                "analysis": str(row["insight_analysis"] or ""),
                "ua_suggestion_single": str(row["insight_ua_suggestion"] or ""),
            }
            results.append(rec)
        return {"target_date": target_date, "results": results, "analyzed_items": len(results)}
    finally:
        conn.close()


def step_crawl_store(args: argparse.Namespace) -> None:
    target_date = args.date
    p = _paths(target_date)
    py = sys.executable
    output_prefix = f"workflow_video_enhancer_{target_date}"

    cmd = [
        py,
        "scripts/test_video_enhancer_two_competitors_318.py",
        "--target-date",
        target_date,
        "--output-prefix",
        output_prefix,
    ]
    if args.products.strip():
        cmd += ["--products", args.products.strip()]
    _run(cmd)

    raw_payload = json.loads(p["raw"].read_text(encoding="utf-8"))
    if getattr(args, "crawl_only", False):
        print(
            "[crawl_store] --crawl-only：已写入 raw，跳过封面去重与入库。"
            f" 下一步请执行: python scripts/workflow_video_enhancer_steps.py cover_store --date {target_date}"
        )
        return

    if is_cover_style_intraday_enabled():
        items = raw_payload.get("items") or []
        items2, cover_rep = apply_intraday_cover_style_dedupe(
            items, target_date, raw_payload.get("crawl_date")
        )
        raw_payload["items"] = items2
        raw_payload["cover_style_intraday_report"] = cover_rep
        p["raw"].write_text(json.dumps(raw_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(
            f"[crawl_store] cover-style: {cover_rep.get('input_count', len(items))} → "
            f"{cover_rep.get('output_count', len(items2))} 条"
        )
    else:
        print(
            "[crawl_store] 多模态封面去重未启用（COVER_STYLE_INTRADAY_ENABLED=0），raw 未改写。"
        )

    init_pipeline_db()
    n1 = upsert_daily_creative_insights(target_date, raw_payload, {})
    n_lib, n_grouped = upsert_creative_library(target_date, raw_payload)
    n2 = upsert_daily_video_enhancer_filter_log(
        target_date, raw_payload.get("filter_report") if isinstance(raw_payload, dict) else None
    )
    print(
        f"[crawl_store] 原始入库完成: daily_creative_insights={n1}, "
        f"creative_library={n_lib}（归组 {n_grouped}）, filter_log={n2}"
    )


def step_cover_store(args: argparse.Namespace) -> None:
    """读取已抓取的 raw，做封面日内去重（若开启），再入库；与 crawl_store --crawl-only 配对。"""
    target_date = args.date
    p = _paths(target_date)
    if not p["raw"].exists():
        raise SystemExit(f"找不到 raw 文件: {p['raw']}，请先 crawl_store --crawl-only 或抓取脚本。")

    raw_payload = json.loads(p["raw"].read_text(encoding="utf-8"))
    output_prefix = f"workflow_video_enhancer_{target_date}"

    if is_cover_style_intraday_enabled():
        items = raw_payload.get("items") or []
        items2, cover_rep = apply_intraday_cover_style_dedupe(
            items, target_date, raw_payload.get("crawl_date")
        )
        raw_payload["items"] = items2
        raw_payload["cover_style_intraday_report"] = cover_rep
        p["raw"].write_text(json.dumps(raw_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        cover_rep_path = DATA_DIR / f"{output_prefix}_cover_style_intraday.json"
        cover_rep_path.write_text(json.dumps(cover_rep, ensure_ascii=False, indent=2), encoding="utf-8")
        print(
            f"[cover_store] cover-style: {cover_rep.get('input_count', len(items))} → "
            f"{cover_rep.get('output_count', len(items2))} 条；报告 {cover_rep_path.name}"
        )
    else:
        print("[cover_store] COVER_STYLE_INTRADAY_ENABLED=0，跳过封面多模态聚类，仅按当前 raw 入库。")

    init_pipeline_db()
    pruned = prune_daily_creative_insights_not_in_raw(target_date, raw_payload)
    if pruned:
        print(f"[cover_store] 已删除当日库内不在 raw 中的素材行: {pruned} 条")
    n1 = upsert_daily_creative_insights(target_date, raw_payload, {})
    n_lib, n_grouped = upsert_creative_library(target_date, raw_payload)
    n2 = upsert_daily_video_enhancer_filter_log(
        target_date, raw_payload.get("filter_report") if isinstance(raw_payload, dict) else None
    )
    print(
        f"[cover_store] 原始入库完成: daily_creative_insights={n1}, "
        f"creative_library={n_lib}（归组 {n_grouped}）, filter_log={n2}"
    )


def step_analyze_store(args: argparse.Namespace) -> None:
    target_date = args.date
    p = _paths(target_date)
    py = sys.executable

    if args.source == "db":
        raw_payload = _load_raw_from_db(target_date)
        p["raw"].write_text(json.dumps(raw_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        raw_payload = json.loads(p["raw"].read_text(encoding="utf-8"))

    if is_cover_style_intraday_enabled() and not raw_payload.get("cover_style_intraday_report"):
        items = raw_payload.get("items") or []
        items2, cover_rep = apply_intraday_cover_style_dedupe(
            items, target_date, raw_payload.get("crawl_date")
        )
        raw_payload["items"] = items2
        raw_payload["cover_style_intraday_report"] = cover_rep
        p["raw"].write_text(json.dumps(raw_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[analyze_store] 已补跑 cover-style: {cover_rep.get('input_count')} → {cover_rep.get('output_count')} 条")

    init_pipeline_db()
    deduped_items, _dedup_rep = get_deduped_items_for_analysis(target_date, raw_payload)

    ad_keys = _extract_ad_keys({"items": deduped_items})
    existing = load_existing_success_analysis_by_ad_keys(ad_keys)

    pending_items: list[dict] = []
    for item in deduped_items:
        if not isinstance(item, dict):
            continue
        c = item.get("creative") or {}
        if not isinstance(c, dict):
            continue
        k = str(c.get("ad_key") or "").strip()
        if not k or k in existing:
            continue
        pending_items.append(item)

    pending_payload = {
        "target_date": target_date,
        "crawl_date": raw_payload.get("crawl_date"),
        "total": len(pending_items),
        "items": pending_items,
    }
    p["pending_raw"].write_text(json.dumps(pending_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    new_results: list[dict] = []
    if pending_items:
        _run(
            [
                py,
                "scripts/analyze_video_from_raw_json.py",
                "--input",
                str(p["pending_raw"]),
                "--output",
                str(p["analysis"]),
            ]
        )
        new_results = (json.loads(p["analysis"].read_text(encoding="utf-8")).get("results") or [])

    failed: list[dict] = []
    success_new: dict[str, dict] = {}
    for it in new_results:
        if not isinstance(it, dict):
            continue
        k = str(it.get("ad_key") or "").strip()
        if not k:
            continue
        txt = str(it.get("analysis") or "")
        if (not txt) or txt.startswith("[ERROR]"):
            failed.append({"ad_key": k, "video_url": str(it.get("video_url") or ""), "error": txt or "empty"})
            continue
        success_new[k] = it

    combined: list[dict] = []
    for item in deduped_items:
        if not isinstance(item, dict):
            continue
        c = item.get("creative") or {}
        if not isinstance(c, dict):
            continue
        k = str(c.get("ad_key") or "").strip()
        if not k:
            continue
        if k in success_new:
            combined.append(success_new[k])
        elif k in existing:
            combined.append(existing[k])

    payload = {
        "target_date": target_date,
        "total_items": len(raw_payload.get("items") or []),
        "analyzed_items": len(combined),
        "reused_existing": len(existing),
        "new_success": len(success_new),
        "new_failed": len(failed),
        "results": combined,
    }
    p["analysis"].write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    p["failed"].write_text(
        json.dumps({"target_date": target_date, "failed_count": len(failed), "failed": failed}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    analysis_by_ad = {}
    for x in combined:
        if not isinstance(x, dict):
            continue
        k = str(x.get("ad_key") or "").strip()
        v = str(x.get("analysis") or "")
        if not k or not v or v.startswith("[ERROR]"):
            continue
        analysis_by_ad[k] = {
            "analysis": v,
            "ua_suggestion_single": str(x.get("ua_suggestion_single") or ""),
        }
    n = upsert_daily_creative_insights(target_date, raw_payload, analysis_by_ad)
    print(f"[analyze_store] 成功入库分析 {len(analysis_by_ad)} 条，upsert={n}。失败 {len(failed)} 条（未入库）。")

    # 语义嵌入
    try:
        import llm_client
        stored = 0
        for ak, info in analysis_by_ad.items():
            txt = str(info.get("analysis") or "") if isinstance(info, dict) else ""
            if not txt or len(txt) < 20:
                continue
            try:
                vec = llm_client.call_embedding(txt[:2000])
                upsert_analysis_embedding(ak, llm_client.embedding_to_bytes(vec))
                stored += 1
            except Exception:
                break
        if stored:
            print(f"[embedding] 已为 {stored} 条素材写入分析嵌入向量。")
    except ImportError:
        pass


def step_cluster_store(args: argparse.Namespace) -> None:
    target_date = args.date
    p = _paths(target_date)
    py = sys.executable

    if args.source == "db":
        analysis_payload = _load_analysis_from_db(target_date)
        p["analysis"].write_text(json.dumps(analysis_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    _run(
        [
            py,
            "scripts/generate_video_enhancer_ua_suggestions_from_analysis.py",
            "--input",
            str(p["analysis"]),
            "--output-json",
            str(p["suggestion_json"]),
            "--output-md",
            str(p["suggestion_md"]),
        ]
    )

    suggestion_payload = json.loads(p["suggestion_json"].read_text(encoding="utf-8")) if p["suggestion_json"].exists() else {}
    card_md = p["suggestion_md"].read_text(encoding="utf-8") if p["suggestion_md"].exists() else ""

    url = (args.bitable_url or "").strip() or (os.getenv("VIDEO_ENHANCER_BITABLE_URL") or "").strip()
    app_token, table_id = _parse_bitable(url) if url else ("__local__", "__local__")
    n = upsert_daily_push_content(
        target_date,
        suggestion_payload,
        card_md,
        app_token,
        table_id,
        push_status="generated",
        push_response=None,
    )
    if should_persist_suggestion_to_push_table(suggestion_payload):
        print(f"[cluster_store] 聚类建议已入库 daily_ua_push_content={n} 条")
    else:
        print("[cluster_store] 聚类失败或方向卡片为空，已跳过 daily_ua_push_content 入库（仍保留本地 json/md）。")


def step_push_sync(args: argparse.Namespace) -> None:
    target_date = args.date
    p = _paths(target_date)
    py = sys.executable
    bitable_url = (args.bitable_url or "").strip() or (os.getenv("VIDEO_ENHANCER_BITABLE_URL") or "").strip()
    cluster_url = (args.cluster_bitable_url or "").strip() or (os.getenv("VIDEO_ENHANCER_CLUSTER_BITABLE_URL") or "").strip()
    if not args.no_bitable_sync and not bitable_url:
        raise SystemExit("push_sync 需要 bitable_url（参数或 VIDEO_ENHANCER_BITABLE_URL）")
    if not args.no_bitable_sync:
        sync_cmd = [
            py,
            "scripts/sync_raw_analysis_to_bitable_and_push_card.py",
            "--url",
            bitable_url,
            "--raw",
            str(p["raw"]),
            "--analysis",
            str(p["analysis"]),
            "--suggestion-json",
            str(p["suggestion_json"]),
            "--suggestion-md",
            str(p["suggestion_md"]),
            "--sync-target",
            args.sync_target,
            "--no-card",
        ]
        if cluster_url:
            sync_cmd.extend(["--cluster-url", cluster_url])
        _run(sync_cmd)
    else:
        print("[push_sync] 已按参数跳过多维表同步（--no-bitable-sync）。")

    # 飞书卡片推送与多维表同步解耦：无论是否同步多维表，都可独立推飞书卡片
    if not args.no_card:
        push_bitable_url = cluster_url or bitable_url
        feishu_cmd = [
            py,
            "scripts/push_video_enhancer_feishu_card_only.py",
            "--date",
            target_date,
            "--raw",
            str(p["raw"]),
            "--analysis",
            str(p["analysis"]),
            "--suggestion-md",
            str(p["suggestion_md"]),
            "--suggestion-json",
            str(p["suggestion_json"]),
            "--bitable-url",
            push_bitable_url,
        ]
        _run(feishu_cmd)
    else:
        print("[push_sync] 已按参数跳过飞书卡片推送（--no-card）。")

    # 推送消息里展示的多维表链接优先使用聚类表
    push_bitable_url = cluster_url or bitable_url
    multi_cmd = [
        py,
        "scripts/push_video_enhancer_multichannel.py",
        "--date",
        target_date,
        "--raw",
        str(p["raw"]),
        "--suggestion-md",
        str(p["suggestion_md"]),
        "--suggestion-json",
        str(p["suggestion_json"]),
        "--bitable-url",
        push_bitable_url,
    ]
    if args.no_wecom:
        multi_cmd.append("--sheet-only")
    if args.no_sheet:
        multi_cmd.append("--wecom-only")
    if not (args.no_wecom and args.no_sheet):
        _run(multi_cmd)

    print("[push_sync] 推送与同步完成（飞书与多维表已解耦）。")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Video Enhancer 分步工作流")
    sub = p.add_subparsers(dest="step", required=True)

    p1 = sub.add_parser("crawl_store", help="1) 爬取并入库（默认含封面去重；可用 --crawl-only 仅抓取）")
    p1.add_argument("--date", default=_default_date())
    p1.add_argument("--products", default="", help="逗号分隔产品名")
    p1.add_argument(
        "--crawl-only",
        action="store_true",
        help="只跑抓取并写 raw.json，不做封面去重、不入库；随后请执行 cover_store",
    )
    p1.set_defaults(func=step_crawl_store)

    p1b = sub.add_parser("cover_store", help="2) 封面日内去重（可选）+ 原始入库（与 crawl-only 配对）")
    p1b.add_argument("--date", default=_default_date())
    p1b.set_defaults(func=step_cover_store)

    p2 = sub.add_parser("analyze_store", help="3) 灵感分析并入库（去重+失败不入库）")
    p2.add_argument("--date", default=_default_date())
    p2.add_argument("--source", choices=["json", "db"], default="json", help="raw 数据来源")
    p2.set_defaults(func=step_analyze_store)

    p3 = sub.add_parser("cluster_store", help="4) 聚类分析并入库")
    p3.add_argument("--date", default=_default_date())
    p3.add_argument("--source", choices=["json", "db"], default="json", help="analysis 数据来源")
    p3.add_argument("--bitable-url", default="", help="用于 daily_ua_push_content 维度标识（可选）")
    p3.set_defaults(func=step_cluster_store)

    p4 = sub.add_parser("push_sync", help="5) 推送与多维表同步")
    p4.add_argument("--date", default=_default_date())
    p4.add_argument("--bitable-url", default="", help="主多维表 URL")
    p4.add_argument("--cluster-bitable-url", default="", help="聚类多维表 URL")
    p4.add_argument(
        "--sync-target",
        choices=["both", "raw", "cluster"],
        default="raw",
        help="多维表同步范围：both=主表+聚类表，raw=仅主表，cluster=仅聚类表",
    )
    p4.add_argument("--no-bitable-sync", action="store_true", help="跳过多维表同步")
    p4.add_argument("--no-card", action="store_true")
    p4.add_argument("--no-wecom", action="store_true")
    p4.add_argument("--no-sheet", action="store_true")
    p4.set_defaults(func=step_push_sync)

    return p


def main() -> None:
    _ensure_line_buffered_stdio()
    args = build_parser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()

