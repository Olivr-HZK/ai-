"""Daily VE workflow node status summaries.

The report is artifact-based so it can be rebuilt after a run without parsing
terminal logs. Each node uses the daily `workflow_video_enhancer_{date}` files
as evidence and classifies the step as ok, warn, failed, skipped, or pending.
"""

from __future__ import annotations

import json
import os
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from ua_workflows.shared.config import DATA_DIR, PROJECT_ROOT, REPORTS_DIR


STATUS_LABELS = {
    "ok": "正常",
    "warn": "提示",
    "failed": "失败",
    "skipped": "跳过",
    "pending": "未到达",
}


def _prefix(target_date: str) -> str:
    return f"workflow_video_enhancer_{target_date}"


def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _node(
    node_id: str,
    name: str,
    status: str,
    message: str,
    *,
    artifact: Path | str | None = None,
    restart: str = "",
    severity: str = "warn",
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "id": node_id,
        "name": name,
        "status": status,
        "status_label": STATUS_LABELS.get(status, status),
        "message": message,
        "restart": restart,
        "severity": severity,
    }
    if artifact:
        out["artifact"] = str(artifact)
    return out


def _existing_path(*paths: Path) -> Path | None:
    for path in paths:
        if path.is_file():
            return path
    return paths[0] if paths else None


def _sum_total(payload: Dict[str, Any], *keys: str) -> int:
    total = payload.get("total")
    if isinstance(total, dict):
        return sum(_safe_int(total.get(key)) for key in keys)
    return 0


def _env_int(key: str, default: int) -> int:
    try:
        return int((os.getenv(key) or str(default)).strip())
    except ValueError:
        return int(default)


def _env_float(key: str, default: float) -> float:
    try:
        return float((os.getenv(key) or str(default)).strip())
    except ValueError:
        return float(default)


def _previous_dates(target_date: str, days: int) -> list[str]:
    try:
        base = datetime.strptime(target_date, "%Y-%m-%d").date()
    except ValueError:
        return []
    return [(base - timedelta(days=i)).isoformat() for i in range(1, days + 1)]


def _round1(value: float) -> float:
    return round(float(value or 0.0), 1)


def _crawl_and_cover_counts(target_date: str) -> tuple[dict[str, int], dict[str, int]]:
    report = _read_json(DATA_DIR / f"{_prefix(target_date)}_crawl_product_retention.json")
    crawl: dict[str, int] = {}
    cover: dict[str, int] = {}
    if isinstance(report, dict):
        for row in report.get("per_product") or []:
            if not isinstance(row, dict):
                continue
            product = str(row.get("product") or "未知广告主").strip() or "未知广告主"
            crawl[product] = _safe_int(row.get("kept_after_crawl_filter"))
            cover[product] = _safe_int(row.get("kept_after_cover_filter"))
    if cover:
        return crawl, cover

    cover_report = _read_json(DATA_DIR / f"{_prefix(target_date)}_cover_style_intraday.json")
    if not cover_report:
        wrapped = _read_json(DATA_DIR / f"{_prefix(target_date)}_filter_step3_cover.json")
        cover_report = wrapped.get("report") if isinstance(wrapped, dict) and isinstance(wrapped.get("report"), dict) else None
    if isinstance(cover_report, dict):
        for row in cover_report.get("per_appid") or []:
            if not isinstance(row, dict):
                continue
            product = str(row.get("product") or row.get("appid") or "未知广告主").strip() or "未知广告主"
            cover[product] = cover.get(product, 0) + _safe_int(row.get("kept"))
    return crawl, cover


def _video_content_counts(target_date: str) -> dict[str, int]:
    report = _read_json(DATA_DIR / f"{_prefix(target_date)}_llm_video_content_filter.json")
    counts: dict[str, int] = {}
    if not isinstance(report, dict):
        return counts
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    advertiser_counts = summary.get("advertiser_counts") if isinstance(summary.get("advertiser_counts"), dict) else {}
    for advertiser, decisions in advertiser_counts.items():
        if not isinstance(decisions, dict):
            continue
        name = str(advertiser or "未知广告主").strip() or "未知广告主"
        counts[name] = _safe_int(decisions.get("保留"))
    if counts:
        return counts

    for record in report.get("records") or []:
        if not isinstance(record, dict):
            continue
        advertiser = str(record.get("advertiser_name") or record.get("product") or record.get("advertiser_id") or "未知广告主").strip() or "未知广告主"
        if str(record.get("final_decision") or "").strip() == "保留":
            counts[advertiser] = counts.get(advertiser, 0) + 1
    return counts


def _stage_counts(target_date: str, stage_id: str) -> dict[str, int]:
    crawl, cover = _crawl_and_cover_counts(target_date)
    if stage_id == "crawl":
        return crawl
    if stage_id == "cover_dedupe":
        return cover
    if stage_id == "video_content_dedupe":
        return _video_content_counts(target_date)
    return {}


def _stage_dashboard(target_date: str, stage_id: str) -> str:
    pre = _prefix(target_date)
    if stage_id == "crawl":
        return str(DATA_DIR / f"{pre}_crawl_product_retention.json")
    if stage_id == "cover_dedupe":
        html = REPORTS_DIR / f"ve_filter_review_{target_date}.html"
        if html.is_file():
            return str(html)
        return str(DATA_DIR / f"{pre}_cover_style_intraday.json")
    if stage_id == "video_content_dedupe":
        html = REPORTS_DIR / f"ve_llm_filter_dashboard_{target_date}.html"
        if html.is_file():
            return str(html)
        return str(DATA_DIR / f"{pre}_llm_video_content_filter.json")
    return ""


CORE_STAGE_DEFS: tuple[dict[str, str], ...] = (
    {"id": "crawl", "name": "爬取", "metric": "爬取后保留素材"},
    {"id": "cover_dedupe", "name": "封面图去重", "metric": "封面去重后保留素材"},
    {"id": "video_content_dedupe", "name": "视频内容去重", "metric": "视频内容去重后保留素材"},
)


def build_core_stage_advertiser_report(
    target_date: str,
    *,
    history_days: int | None = None,
    low_ratio: float | None = None,
    high_ratio: float | None = None,
    min_history_days: int | None = None,
    min_baseline: float | None = None,
) -> Dict[str, Any]:
    """Compare three core VE stages by advertiser against previous 7 days."""
    hist_days = int(history_days if history_days is not None else _env_int("VE_CORE_STAGE_HISTORY_DAYS", 7))
    low = float(low_ratio if low_ratio is not None else _env_float("VE_CORE_STAGE_LOW_RATIO", 0.5))
    high = float(high_ratio if high_ratio is not None else _env_float("VE_CORE_STAGE_HIGH_RATIO", 2.0))
    min_hist = int(min_history_days if min_history_days is not None else _env_int("VE_CORE_STAGE_MIN_HISTORY_DAYS", 2))
    min_base = float(min_baseline if min_baseline is not None else _env_float("VE_CORE_STAGE_MIN_BASELINE", 3.0))

    history_dates = _previous_dates(target_date, hist_days)
    stages: list[dict[str, Any]] = []
    all_alerts: list[dict[str, Any]] = []
    for stage_def in CORE_STAGE_DEFS:
        stage_id = stage_def["id"]
        today_counts = _stage_counts(target_date, stage_id)
        history_by_advertiser: dict[str, list[int]] = {}
        for ds in history_dates:
            day_counts = _stage_counts(ds, stage_id)
            for advertiser, value in day_counts.items():
                history_by_advertiser.setdefault(advertiser, []).append(int(value))

        advertisers = sorted(
            set(today_counts) | set(history_by_advertiser),
            key=lambda name: (-int(today_counts.get(name, 0)), name),
        )
        rows: list[dict[str, Any]] = []
        stage_alerts: list[dict[str, Any]] = []
        for advertiser in advertisers:
            values = history_by_advertiser.get(advertiser, [])
            today = int(today_counts.get(advertiser, 0))
            avg = (sum(values) / len(values)) if values else 0.0
            ratio = (today / avg) if avg else None
            status = "无历史"
            alert = False
            if len(values) >= min_hist and avg >= min_base and ratio is not None:
                if ratio < low:
                    status = "偏低"
                    alert = True
                elif ratio > high:
                    status = "偏高"
                    alert = True
                else:
                    status = "正常"
            elif values:
                status = "历史不足"

            row = {
                "advertiser": advertiser,
                "today": today,
                "history_avg": _round1(avg),
                "history_days": len(values),
                "history_values": values,
                "ratio": _round1(ratio) if ratio is not None else None,
                "status": status,
            }
            rows.append(row)
            if alert:
                alert_row = {
                    "stage_id": stage_id,
                    "stage_name": stage_def["name"],
                    "advertiser": advertiser,
                    "today": today,
                    "history_avg": _round1(avg),
                    "history_days": len(values),
                    "ratio": _round1(ratio or 0),
                    "status": status,
                    "dashboard": _stage_dashboard(target_date, stage_id),
                }
                stage_alerts.append(alert_row)
                all_alerts.append(alert_row)

        total_today = sum(int(v) for v in today_counts.values())
        total_history_avg = sum(float(row["history_avg"]) for row in rows if row.get("history_days"))
        stages.append(
            {
                **stage_def,
                "dashboard": _stage_dashboard(target_date, stage_id),
                "total_today": int(total_today),
                "total_history_avg": _round1(total_history_avg),
                "advertisers": rows,
                "alerts": stage_alerts,
            }
        )

    return {
        "target_date": target_date,
        "history_days": hist_days,
        "thresholds": {
            "low_ratio": low,
            "high_ratio": high,
            "min_history_days": min_hist,
            "min_baseline": min_base,
        },
        "summary": {
            "history_days": hist_days,
            "stage_count": len(stages),
            "alert_count": len(all_alerts),
        },
        "stages": stages,
        "alerts": all_alerts,
    }


def render_core_stage_markdown(report: Dict[str, Any] | None) -> str:
    if not isinstance(report, dict):
        return ""
    lines: list[str] = []
    lines.append("## 三阶段广告主留存对比\n\n")
    lines.append(f"历史窗口：近 {report.get('history_days', 7)} 天；告警 {len(report.get('alerts') or [])} 条。\n\n")
    lines.append("| 阶段 | 广告主 | 今日保留 | 历史均值 | 历史天数 | 比例 | 状态 | 看板/证据 |\n")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | --- | --- |\n")
    for stage in report.get("stages") or []:
        if not isinstance(stage, dict):
            continue
        dashboard = Path(str(stage.get("dashboard") or "")).name if stage.get("dashboard") else "-"
        rows = [row for row in (stage.get("advertisers") or []) if isinstance(row, dict)]
        rows.sort(key=lambda row: (0 if row.get("status") in {"偏低", "偏高"} else 1, -int(row.get("today") or 0), str(row.get("advertiser") or "")))
        for row in rows:
            ratio = row.get("ratio")
            ratio_text = "-" if ratio is None else str(ratio)
            lines.append(
                f"| {stage.get('name')} | {str(row.get('advertiser') or '').replace('|', '/')} | "
                f"{_safe_int(row.get('today'))} | {row.get('history_avg', 0)} | "
                f"{_safe_int(row.get('history_days'))} | {ratio_text} | {row.get('status') or '-'} | {dashboard} |\n"
            )
    lines.append("\n")
    alerts = [a for a in (report.get("alerts") or []) if isinstance(a, dict)]
    if alerts:
        lines.append("**三阶段告警：**\n")
        for alert in alerts:
            lines.append(
                f"- {alert.get('stage_name')} / {alert.get('advertiser')}: "
                f"今日 {alert.get('today')}，历史均值 {alert.get('history_avg')}，"
                f"比例 {alert.get('ratio')}，状态 {alert.get('status')}；看板 {alert.get('dashboard')}\n"
            )
        lines.append("\n")
    return "".join(lines)


def build_daily_node_report(target_date: str, *, partial: bool = False) -> Dict[str, Any]:
    pre = _prefix(target_date)

    raw_path = DATA_DIR / f"{pre}_raw.json"
    crawl_retention_path = DATA_DIR / f"{pre}_crawl_product_retention.json"
    cover_path = DATA_DIR / f"{pre}_cover_style_intraday.json"
    cover_step_path = DATA_DIR / f"{pre}_filter_step3_cover.json"
    video_content_path = DATA_DIR / f"{pre}_video_content_fill_report.json"
    video_filter_path = DATA_DIR / f"{pre}_llm_video_content_filter.json"
    minimal_path = DATA_DIR / f"{pre}_video_content_minimal_analysis.json"
    dedup_path = DATA_DIR / f"{pre}_analysis_dedup_report.json"
    queue_path = DATA_DIR / f"{pre}_analysis_queue_report.json"
    pending_path = DATA_DIR / f"{pre}_raw_pending_analysis.json"
    analysis_path = DATA_DIR / f"video_analysis_{pre}_raw.json"
    failed_path = DATA_DIR / f"{pre}_analysis_failed.json"
    launched_path = DATA_DIR / f"{pre}_filter_step_launched_effects.json"
    suggestion_path = DATA_DIR / f"ua_suggestion_{pre}.json"
    sync_path = DATA_DIR / f"{pre}_sync_report.json"
    review_path = REPORTS_DIR / f"ve_filter_review_{target_date}.html"
    acceptance_path = DATA_DIR / f"{pre}_acceptance.json"
    flow_path = DATA_DIR / f"{pre}_flow_report.json"

    raw = _read_json(raw_path)
    crawl_retention = _read_json(crawl_retention_path)
    cover = _read_json(cover_path) or _read_json(cover_step_path)
    video_content = _read_json(video_content_path)
    video_filter = _read_json(video_filter_path)
    minimal = _read_json(minimal_path)
    dedup = _read_json(dedup_path)
    queue = _read_json(queue_path)
    pending = _read_json(pending_path)
    analysis = _read_json(analysis_path)
    failed = _read_json(failed_path)
    launched = _read_json(launched_path)
    suggestion = _read_json(suggestion_path)
    sync = _read_json(sync_path)

    nodes: List[Dict[str, Any]] = []

    if raw is None:
        nodes.append(
            _node(
                "crawl",
                "广大大抓取与 raw 落盘",
                "pending" if partial else "failed",
                f"缺少 raw 产物或 JSON 无法解析：{raw_path.name}",
                artifact=raw_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
                severity="hard",
            )
        )
    else:
        items = raw.get("items") if isinstance(raw.get("items"), list) else []
        fr = raw.get("filter_report") if isinstance(raw.get("filter_report"), dict) else {}
        kept = _safe_int(fr.get("post_truncation_total")) or len(items)
        pre_total = _safe_int(fr.get("pre_truncation_total"))
        retention_note = "，产品漏斗已记录" if crawl_retention else "，产品漏斗未单独落盘"
        nodes.append(
            _node(
                "crawl",
                "广大大抓取与 raw 落盘",
                "ok",
                f"raw {len(items)} 条；筛选前 {pre_total}，筛选后 {kept}{retention_note}",
                artifact=raw_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
            )
        )

    if cover is None:
        status = "pending" if raw is None else "skipped"
        message = "未到封面去重节点" if raw is None else "未发现封面去重报告，通常表示配置关闭或旧日期未产出"
        nodes.append(
            _node(
                "cover_dedupe",
                "封面/素材视觉去重",
                status,
                message,
                artifact=_existing_path(cover_path, cover_step_path),
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
            )
        )
    else:
        skipped = bool(cover.get("skipped"))
        n_in = _safe_int(cover.get("input_count") or cover.get("input"))
        n_out = _safe_int(cover.get("output_count") or cover.get("output"))
        removed = _safe_int(cover.get("removed_total")) or max(0, n_in - n_out)
        nodes.append(
            _node(
                "cover_dedupe",
                "封面/素材视觉去重",
                "skipped" if skipped else "ok",
                (str(cover.get("skip_reason") or "配置关闭") if skipped else f"输入 {n_in}，输出 {n_out}，剔除 {removed}"),
                artifact=_existing_path(cover_path, cover_step_path),
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
            )
        )

    if video_content is None:
        nodes.append(
            _node(
                "video_content_fill",
                "视频内容补全",
                "pending" if raw is None else "skipped",
                "未发现补全报告；旧日期或无需补抓时可能没有该产物",
                artifact=video_content_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
            )
        )
    else:
        missing = _safe_int(video_content.get("missing_video_content"))
        status = "warn" if missing else "ok"
        nodes.append(
            _node(
                "video_content_fill",
                "视频内容补全",
                status,
                f"初始缺失 {video_content.get('initial_missing_video_content', 0)}，最终缺失 {missing}",
                artifact=video_content_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
            )
        )

    if video_filter is None:
        nodes.append(
            _node(
                "video_content_filter",
                "视频内容 LLM 去重/硬拦",
                "pending" if raw is None else "skipped",
                "未发现视频内容 LLM 筛选报告；可能配置关闭或旧流程未启用",
                artifact=video_filter_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
            )
        )
    elif video_filter.get("error"):
        nodes.append(
            _node(
                "video_content_filter",
                "视频内容 LLM 去重/硬拦",
                "warn",
                f"节点报错但主流程继续：{video_filter.get('error')}",
                artifact=video_filter_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
                severity="soft",
            )
        )
    else:
        summary = video_filter.get("summary") if isinstance(video_filter.get("summary"), dict) else {}
        final_counts = summary.get("final_counts") if isinstance(summary.get("final_counts"), dict) else {}
        llm_failures = _safe_int(summary.get("llm_failures") or video_filter.get("llm_failures"))
        status = "warn" if llm_failures else "ok"
        message = (
            f"目标 {summary.get('target_records', 0)}；"
            f"保留 {final_counts.get('保留', 0)}，业务硬拦 {final_counts.get('业务硬拦', 0)}，"
            f"历史重复 {final_counts.get('历史重复', 0)}，日内重复 {final_counts.get('日内重复', 0)}"
        )
        if llm_failures:
            message += f"；LLM 失败 {llm_failures}"
        nodes.append(
            _node(
                "video_content_filter",
                "视频内容 LLM 去重/硬拦",
                status,
                message,
                artifact=video_filter_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
                severity="soft",
            )
        )

    if minimal is None:
        nodes.append(
            _node(
                "minimal_analysis",
                "视频内容轻量分析",
                "pending" if raw is None else "skipped",
                "未发现轻量分析报告；可能关闭 text-only 流程或旧日期未产出",
                artifact=minimal_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
            )
        )
    else:
        summary = minimal.get("minimal_analysis_summary")
        if not isinstance(summary, dict):
            summary = {}
        nodes.append(
            _node(
                "minimal_analysis",
                "视频内容轻量分析",
                "ok",
                f"候选 {summary.get('total', 0)}，生成 {summary.get('generated', 0)}，本地兜底 {summary.get('fallback', 0)}",
                artifact=minimal_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
            )
        )

    if dedup is None:
        nodes.append(
            _node(
                "analysis_dedup",
                "分析前日内/跨日去重",
                "pending" if raw is None else "failed",
                "缺少 analysis_dedup_report，无法确认分析前去重",
                artifact=dedup_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
                severity="soft",
            )
        )
    else:
        nodes.append(
            _node(
                "analysis_dedup",
                "分析前日内/跨日去重",
                "ok",
                f"输入 {dedup.get('total_input', 0)}，跨日后 {dedup.get('after_crossday', 0)}",
                artifact=dedup_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
            )
        )

    if queue is None:
        nodes.append(
            _node(
                "analysis_queue",
                "AI 分析入队",
                "pending" if raw is None else "failed",
                "缺少 analysis_queue_report，无法确认本次 AI 入队",
                artifact=queue_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
                severity="soft",
            )
        )
    else:
        queued = _sum_total(queue, "llm_queued")
        after_dedup = _sum_total(queue, "after_dedup")
        cache_reused = _sum_total(queue, "cache_reused")
        pending_count = len(pending.get("items") or []) if isinstance(pending, dict) and isinstance(pending.get("items"), list) else queued
        nodes.append(
            _node(
                "analysis_queue",
                "AI 分析入队",
                "ok",
                f"去重后 {after_dedup}，复用历史 {cache_reused}，新送 AI {queued}，pending {pending_count}",
                artifact=queue_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
            )
        )

    if analysis is None:
        nodes.append(
            _node(
                "analysis",
                "AI 灵感分析",
                "pending" if raw is None or partial else "failed",
                f"缺少 analysis 产物或 JSON 无法解析：{analysis_path.name}",
                artifact=analysis_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
                severity="hard" if not partial else "soft",
            )
        )
    else:
        new_failed = _safe_int(analysis.get("new_failed"))
        failed_count = _safe_int((failed or {}).get("failed_count")) if failed else new_failed
        new_success = _safe_int(analysis.get("new_success"))
        analyzed = _safe_int(analysis.get("analyzed_items"))
        status = "failed" if new_failed or failed_count else "ok"
        message = f"合并分析 {analyzed} 条，新成功 {new_success} 条"
        if new_failed or failed_count:
            message += f"；本轮新分析失败 {new_failed or failed_count} 条"
        nodes.append(
            _node(
                "analysis",
                "AI 灵感分析",
                status,
                message,
                artifact=analysis_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
                severity="soft",
            )
        )

    if launched is None:
        nodes.append(
            _node(
                "business_filters",
                "同步前业务筛选标记",
                "pending" if analysis is None else "skipped",
                "未发现我方已投/业务筛选步骤报告；旧日期可能未产出",
                artifact=launched_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
            )
        )
    else:
        nodes.append(
            _node(
                "business_filters",
                "同步前业务筛选标记",
                "ok",
                f"我方已投标记 {launched.get('marked_count', 0)} 条；更多硬拦见 sync_report",
                artifact=launched_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
            )
        )

    if suggestion is None:
        nodes.append(
            _node(
                "suggestions",
                "方向卡片兼容生成",
                "pending" if analysis is None or partial else "warn",
                "未发现方向卡片 JSON；主表同步可继续，但旧聚类表/旧推送可能不可用",
                artifact=suggestion_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
                severity="soft",
            )
        )
    else:
        skipped_llm = bool(suggestion.get("skipped_llm"))
        llm_error = suggestion.get("llm_error")
        inner = suggestion.get("suggestion") if isinstance(suggestion.get("suggestion"), dict) else suggestion
        cards = inner.get("方向卡片") if isinstance(inner, dict) else []
        card_count = len(cards) if isinstance(cards, list) else 0
        status = "warn" if skipped_llm or llm_error else "ok"
        message = f"方向卡片 {card_count} 张"
        if skipped_llm:
            message += "；skipped_llm=True"
        if llm_error:
            message += f"；LLM 错误：{llm_error}"
        nodes.append(
            _node(
                "suggestions",
                "方向卡片兼容生成",
                status,
                message,
                artifact=suggestion_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}",
                severity="soft",
            )
        )

    if sync is None:
        nodes.append(
            _node(
                "bitable_sync",
                "多维表同步",
                "pending" if partial or analysis is None else "failed",
                "缺少 sync_report，无法确认主表写入；若本次显式 --no-bitable-sync，可视为人工跳过",
                artifact=sync_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python -m ua_workflows.video_enhancer.sync --url \"$VIDEO_ENHANCER_BITABLE_URL\" --raw data/{pre}_raw.json --analysis data/video_analysis_{pre}_raw.json --suggestion-json data/ua_suggestion_{pre}.json --suggestion-md data/ua_suggestion_{pre}.md --sync-target raw --no-card",
                severity="soft",
            )
        )
    else:
        totals = sync.get("totals") if isinstance(sync.get("totals"), dict) else {}
        enabled = bool(sync.get("main_sync_enabled", True))
        status = "ok" if enabled else "skipped"
        nodes.append(
            _node(
                "bitable_sync",
                "多维表同步",
                status,
                f"候选 {totals.get('after_template_dedup', totals.get('after_hard_exclusion', 0))}，写入 {totals.get('synced_records', 0)}，硬拦 {totals.get('hard_excluded', 0)}",
                artifact=sync_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python -m ua_workflows.video_enhancer.sync --url \"$VIDEO_ENHANCER_BITABLE_URL\" --raw data/{pre}_raw.json --analysis data/video_analysis_{pre}_raw.json --suggestion-json data/ua_suggestion_{pre}.json --suggestion-md data/ua_suggestion_{pre}.md --sync-target raw --no-card",
            )
        )

    if review_path.is_file():
        nodes.append(
            _node(
                "review_dashboard",
                "筛选复核看板",
                "ok",
                f"已生成 {review_path.name}",
                artifact=review_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python -m ua_workflows.video_enhancer.review_dashboard --date {target_date}",
            )
        )
    else:
        nodes.append(
            _node(
                "review_dashboard",
                "筛选复核看板",
                "warn" if sync is not None else "pending",
                "未发现筛选复核 HTML；不阻塞主流程，但影响人工复核",
                artifact=review_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python -m ua_workflows.video_enhancer.review_dashboard --date {target_date}",
            )
        )

    if acceptance_path.is_file():
        nodes.append(
            _node(
                "acceptance",
                "验收报告",
                "ok",
                f"已生成 {acceptance_path.name}",
                artifact=acceptance_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python -m ua_workflows.video_enhancer.acceptance --date {target_date}",
            )
        )
    else:
        nodes.append(
            _node(
                "acceptance",
                "验收报告",
                "pending",
                "本次验收报告正在生成或尚未生成",
                artifact=acceptance_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python -m ua_workflows.video_enhancer.acceptance --date {target_date}",
            )
        )

    if flow_path.is_file():
        nodes.append(
            _node(
                "flow_report",
                "全流程漏斗报告",
                "ok",
                f"已生成 {flow_path.name}",
                artifact=flow_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python -m ua_workflows.video_enhancer.flow_report --date {target_date} --no-send",
            )
        )
    else:
        nodes.append(
            _node(
                "flow_report",
                "全流程漏斗报告",
                "pending",
                "全流程漏斗报告尚未生成；通常在验收报告之后生成",
                artifact=flow_path,
                restart=f"cd {PROJECT_ROOT} && .venv/bin/python -m ua_workflows.video_enhancer.flow_report --date {target_date} --no-send",
            )
        )

    counts = Counter(str(node.get("status") or "") for node in nodes)
    failed_nodes = [node for node in nodes if node.get("status") == "failed"]
    warn_nodes = [node for node in nodes if node.get("status") == "warn"]
    return {
        "target_date": target_date,
        "partial": bool(partial),
        "summary": {key: int(counts.get(key, 0)) for key in ("ok", "warn", "failed", "skipped", "pending")},
        "nodes": nodes,
        "failed_nodes": failed_nodes,
        "warn_nodes": warn_nodes,
    }


def render_node_markdown(report: Dict[str, Any] | None) -> str:
    if not isinstance(report, dict):
        return ""
    nodes = [node for node in (report.get("nodes") or []) if isinstance(node, dict)]
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    lines: List[str] = []
    lines.append("## 每日流程节点运行情况\n\n")
    lines.append(
        f"节点汇总：正常 {summary.get('ok', 0)}，提示 {summary.get('warn', 0)}，"
        f"失败 {summary.get('failed', 0)}，跳过 {summary.get('skipped', 0)}，未到达 {summary.get('pending', 0)}。\n\n"
    )
    lines.append("| 节点 | 状态 | 运行情况 | 证据 | 重启/复查 |\n")
    lines.append("| --- | --- | --- | --- | --- |\n")
    for node in nodes:
        status = str(node.get("status") or "")
        status_label = str(node.get("status_label") or STATUS_LABELS.get(status, status))
        message = str(node.get("message") or "-").replace("|", "/")
        artifact = Path(str(node.get("artifact") or "")).name if node.get("artifact") else "-"
        restart = str(node.get("restart") or "-").replace("|", "/")
        if len(restart) > 140:
            restart = restart[:137] + "..."
        lines.append(f"| {node.get('name') or node.get('id')} | {status_label} | {message} | {artifact} | `{restart}` |\n")
    lines.append("\n")
    failed = [node for node in nodes if node.get("status") == "failed"]
    if failed:
        lines.append("**失败节点：**\n")
        for node in failed:
            lines.append(f"- {node.get('name')}: {node.get('message')}\n")
        lines.append("\n")
    return "".join(lines)


def failed_node_lines(report: Dict[str, Any] | None, *, limit: int = 6) -> List[str]:
    if not isinstance(report, dict):
        return []
    failed = [node for node in (report.get("nodes") or []) if isinstance(node, dict) and node.get("status") == "failed"]
    lines: List[str] = []
    for node in failed[:limit]:
        lines.append(f"- **{node.get('name') or node.get('id')}**：{node.get('message') or '失败'}")
        restart = str(node.get("restart") or "").strip()
        if restart:
            lines.append(f"  重启/复查：`{restart}`")
    if len(failed) > limit:
        lines.append(f"- 还有 {len(failed) - limit} 个失败节点，见本地完整报告。")
    return lines


def compact_node_summary(report: Dict[str, Any] | None) -> str:
    if not isinstance(report, dict):
        return "节点状态未生成"
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    return (
        f"正常 {summary.get('ok', 0)} / 提示 {summary.get('warn', 0)} / "
        f"失败 {summary.get('failed', 0)} / 跳过 {summary.get('skipped', 0)} / 未到达 {summary.get('pending', 0)}"
    )
