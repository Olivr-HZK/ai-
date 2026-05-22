"""
Video Enhancer full-flow report.

Aggregates the daily pipeline artifacts into a per-product funnel report and
sends a Feishu card. It is intentionally read-only against workflow data so it
can be rerun after the fact:

    python -m ua_workflows.video_enhancer.flow_report --date 2026-05-18

Outputs:
  data/workflow_video_enhancer_{date}_flow_report.json
  reports/workflow_video_enhancer_{date}_flow_report.md
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests
from dotenv import load_dotenv

from ua_workflows.shared.config import DATA_DIR, PROJECT_ROOT, REPORTS_DIR

load_dotenv(PROJECT_ROOT / ".env", override=True)


REASON_LABELS: Dict[str, str] = {
    "advertiser_mismatch": "不是目标广告主",
    "non_target_date": "不是当天首次看到",
    "resume_advertising": "近期重复投放",
    "duplicate_ad_key": "同一广告重复",
    "per_product_truncated": "超过单产品数量上限",
    "cover_crossday_fingerprint": "历史已见过的封面/素材",
    "cover_clip_crossday": "封面像历史素材",
    "cover_clip_intraday": "当天相似封面",
    "analysis_dedup_intraday": "当天重复素材",
    "analysis_dedup_crossday_ad_key": "历史重复素材",
    "analysis_dedup_crossday_url": "历史重复素材",
    "analysis_dedup_crossday_ahash": "历史相似封面",
    "analysis_dedup_crossday_other": "历史重复素材",
    "analysis_cache_reused": "已分析过，复用结果",
    "analysis_ineligible": "素材信息不够分析",
    "analysis_failed": "AI 分析失败",
    "adult_content": "成人/色情风险",
    "ecommerce_effect": "电商/商品特效",
    "non_human_photo_effect": "非人物照片特效",
    "missing_human_photo_input": "非人物照片加工",
    "launched_effect": "我方已投过",
    "intraday_effect_duplicate": "当天同玩法",
    "old_effect_duplicate": "历史同玩法",
    "effect_embedding_duplicate": "看起来是同玩法",
    "exclude_from_bitable_other": "其他原因不入表",
    "same_template_demographic_or_gender_swap": "同模板只换人物",
    "same_play_non_representative": "同玩法非代表",
    "low_acceptance_priority": "低采纳优先级",
}

REPORT_COLUMNS = (
    "dom_cards",
    "clicked_detail_rows",
    "captured_materials",
    "target_date_hits",
    "kept_after_crawl_filter",
    "kept_after_cover_filter",
    "analysis_dedup_kept",
    "llm_queued",
    "analysis_success",
    "sync_candidate",
    "synced_records",
)

COLUMN_LABELS = {
    "dom_cards": "页面候选卡片",
    "clicked_detail_rows": "点开详情返回",
    "captured_materials": "读取到原始素材",
    "target_date_hits": "当天首见",
    "kept_after_crawl_filter": "进入 raw",
    "kept_after_cover_filter": "封面去重后",
    "analysis_dedup_kept": "待 AI 分析",
    "llm_queued": "新送 AI 分析",
    "analysis_success": "AI 分析完成",
    "sync_candidate": "待入多维表",
    "synced_records": "已入多维表",
}

BUSINESS_REASON_GROUPS: tuple[tuple[str, str, set[str]], ...] = (
    ("advertiser", "广告主不一致筛选", {"advertiser_mismatch"}),
    (
        "date_repeat",
        "非当天/重投筛选",
        {"non_target_date", "resume_advertising", "duplicate_ad_key", "per_product_truncated"},
    ),
    (
        "history_cover",
        "历史封面去重",
        {
            "cover_crossday_fingerprint",
            "cover_clip_crossday",
            "analysis_dedup_crossday_ad_key",
            "analysis_dedup_crossday_url",
            "analysis_dedup_crossday_ahash",
            "analysis_dedup_crossday_other",
        },
    ),
    (
        "same_day_cover",
        "日内封面去重",
        {"cover_clip_intraday", "analysis_dedup_intraday"},
    ),
    (
        "template_play",
        "同模板/同玩法合并",
        {
            "intraday_effect_duplicate",
            "old_effect_duplicate",
            "effect_embedding_duplicate",
            "same_template_demographic_or_gender_swap",
            "same_play_non_representative",
        },
    ),
    (
        "risk_sync",
        "风险/不入表筛选",
        {"adult_content", "launched_effect", "exclude_from_bitable_other", "low_acceptance_priority"},
    ),
    ("analysis", "AI 分析处理", {"analysis_cache_reused", "analysis_ineligible", "analysis_failed"}),
)


def _env_bool(key: str, default: str = "1") -> bool:
    v = (os.getenv(key) or default).strip().lower()
    return v in ("1", "true", "yes", "on")


def _env_int(key: str, default: str) -> int:
    try:
        return int((os.getenv(key) or default).strip())
    except ValueError:
        return int(default)


def _env_float(key: str, default: str) -> float:
    try:
        return float((os.getenv(key) or default).strip())
    except ValueError:
        return float(default)


def _prefix(target_date: str) -> str:
    return f"workflow_video_enhancer_{target_date}"


def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _product_from_item(item: Dict[str, Any]) -> str:
    creative = item.get("creative") if isinstance(item.get("creative"), dict) else {}
    return str(
        item.get("product")
        or creative.get("product")
        or creative.get("advertiser_name")
        or item.get("keyword")
        or "未知产品"
    ).strip() or "未知产品"


def _product_from_result(row: Dict[str, Any], ad_to_product: Dict[str, str]) -> str:
    ad_key = str(row.get("ad_key") or "").strip()
    return str(row.get("product") or ad_to_product.get(ad_key) or "未知产品").strip() or "未知产品"


def _ad_key_from_item(item: Dict[str, Any]) -> str:
    creative = item.get("creative") if isinstance(item.get("creative"), dict) else {}
    return str(creative.get("ad_key") or item.get("ad_key") or "").strip()


def _success_analysis(row: Dict[str, Any]) -> bool:
    text = str(row.get("analysis") or "").strip()
    return bool(text) and not text.startswith("[ERROR]")


def _new_row(product: str) -> Dict[str, Any]:
    row = {key: 0 for key in REPORT_COLUMNS}
    row.update(
        {
            "product": product,
            "analysis_cache_reused": 0,
            "analysis_ineligible": 0,
            "removed_total": 0,
            "removed_reasons": {},
            "removed_reason_labels": {},
        }
    )
    return row


def _row(rows: Dict[str, Dict[str, Any]], product: str) -> Dict[str, Any]:
    p = product or "未知产品"
    if p not in rows:
        rows[p] = _new_row(p)
    return rows[p]


def _add_reason(
    reason_by_product: Dict[str, Counter[str]],
    product: str,
    reason: str,
    count: int = 1,
) -> None:
    if count <= 0:
        return
    reason_by_product[product or "未知产品"][reason or "unknown"] += int(count)


def _count_items_by_product(items: Iterable[Any]) -> Counter[str]:
    out: Counter[str] = Counter()
    for item in items or []:
        if isinstance(item, dict):
            out[_product_from_item(item)] += 1
    return out


def _crossday_reason_key(reason: str) -> str:
    r = (reason or "").lower()
    if "ad_key" in r:
        return "analysis_dedup_crossday_ad_key"
    if "url" in r:
        return "analysis_dedup_crossday_url"
    if "ahash" in r:
        return "analysis_dedup_crossday_ahash"
    return "analysis_dedup_crossday_other"


def _hard_exclude_reason(row: Dict[str, Any]) -> str:
    if row.get("adult_content_filter_match"):
        return "adult_content"
    if row.get("human_photo_effect_filter_match"):
        match = row.get("human_photo_effect_filter_match") or {}
        if isinstance(match, dict):
            reason = str(match.get("reason") or "").strip()
            if reason:
                return reason
        return "non_human_photo_effect"
    if row.get("launched_effect_match"):
        return "launched_effect"
    if row.get("intraday_effect_match"):
        return "intraday_effect_duplicate"
    if row.get("old_effect_match"):
        return "old_effect_duplicate"
    if row.get("effect_embedding_duplicate_match"):
        return "effect_embedding_duplicate"
    return "exclude_from_bitable_other"


def _top_reason_text(reasons: Dict[str, int], *, limit: int = 4) -> str:
    if not reasons:
        return "-"
    pieces = []
    for key, value in sorted(reasons.items(), key=lambda kv: (-int(kv[1]), kv[0]))[:limit]:
        pieces.append(f"{REASON_LABELS.get(key, key)}={int(value)}")
    return "；".join(pieces) if pieces else "-"


def _business_reason_counts(reasons: Dict[str, int]) -> List[tuple[str, int]]:
    if not reasons:
        return []
    used: set[str] = set()
    grouped: List[tuple[str, int]] = []
    for _group_key, label, keys in BUSINESS_REASON_GROUPS:
        total = sum(_safe_int(reasons.get(key)) for key in keys)
        used.update(keys)
        if total > 0:
            grouped.append((label, total))
    other = sum(_safe_int(value) for key, value in reasons.items() if key not in used)
    if other > 0:
        grouped.append(("其他原因", other))
    return sorted(grouped, key=lambda item: (-item[1], item[0]))


def _business_reason_text(reasons: Dict[str, int], *, limit: int = 4) -> str:
    grouped = _business_reason_counts(reasons)
    if not grouped:
        return "-"
    return "；".join(f"{label}={count}" for label, count in grouped[:limit])


def _merge_reason_labels(rows: Dict[str, Dict[str, Any]], reason_by_product: Dict[str, Counter[str]]) -> None:
    for product, row in rows.items():
        reasons = {
            key: int(value)
            for key, value in sorted(reason_by_product.get(product, Counter()).items())
            if int(value) > 0
        }
        row["removed_reasons"] = reasons
        row["removed_reason_labels"] = {key: REASON_LABELS.get(key, key) for key in reasons}
        row["removed_total"] = int(sum(reasons.values()))


def _load_history_values(target_date: str, lookback_days: int) -> Dict[str, Dict[str, List[int]]]:
    history: Dict[str, Dict[str, List[int]]] = defaultdict(lambda: defaultdict(list))
    try:
        d0 = datetime.strptime(target_date, "%Y-%m-%d").date()
    except ValueError:
        return history

    for offset in range(1, lookback_days + 1):
        ds = (d0 - timedelta(days=offset)).isoformat()
        flow = _read_json(DATA_DIR / f"workflow_video_enhancer_{ds}_flow_report.json")
        if flow:
            rows = flow.get("per_product") or []
        else:
            rows = []
            crawl = _read_json(DATA_DIR / f"workflow_video_enhancer_{ds}_crawl_product_retention.json")
            sync = _read_json(DATA_DIR / f"workflow_video_enhancer_{ds}_sync_report.json")
            crawl_by_product = {
                str(r.get("product") or ""): r
                for r in ((crawl or {}).get("per_product") or [])
                if isinstance(r, dict)
            }
            sync_by_product = {
                str(r.get("product") or ""): r
                for r in ((sync or {}).get("per_product") or [])
                if isinstance(r, dict)
            }
            for product in sorted(set(crawl_by_product) | set(sync_by_product)):
                rows.append(
                    {
                        "product": product,
                        "kept_after_cover_filter": _safe_int(
                            crawl_by_product.get(product, {}).get("kept_after_cover_filter")
                        ),
                        "synced_records": _safe_int(sync_by_product.get(product, {}).get("synced_records")),
                    }
                )
        for row in rows:
            if not isinstance(row, dict):
                continue
            product = str(row.get("product") or "").strip()
            if not product:
                continue
            for metric in ("kept_after_cover_filter", "synced_records"):
                if metric in row:
                    history[product][metric].append(_safe_int(row.get(metric)))
    return history


def detect_product_volume_alerts(
    per_product: List[Dict[str, Any]],
    history: Dict[str, Dict[str, List[int]]],
    *,
    low_ratio: float = 0.5,
    min_baseline: int = 3,
    min_history_days: int = 2,
    target_date: str = "",
) -> List[Dict[str, Any]]:
    alerts: List[Dict[str, Any]] = []
    metric_names = {
        "kept_after_cover_filter": "封面后保留",
        "synced_records": "主表写入",
    }
    for row in per_product:
        product = str(row.get("product") or "").strip()
        if not product:
            continue
        for metric, label in metric_names.items():
            values = list(history.get(product, {}).get(metric) or [])
            if len(values) < min_history_days:
                continue
            baseline = sum(values) / len(values)
            today = _safe_int(row.get(metric))
            if baseline < min_baseline:
                continue
            if today < baseline * low_ratio:
                alerts.append(
                    {
                        "product": product,
                        "metric": metric,
                        "metric_label": label,
                        "today": today,
                        "history_values": values,
                        "history_mean": round(baseline, 2),
                        "ratio": round(today / baseline, 3) if baseline else 0,
                        "retry_command": _retry_command(target_date, product) if target_date else "",
                    }
                )
    return alerts


def _retry_command(target_date: str, product: str = "") -> str:
    quoted = product.replace('"', '\\"')
    cmd = f"cd {PROJECT_ROOT} && .venv/bin/python scripts/run_video_enhancer.py --date {target_date}"
    if quoted:
        cmd += f' --products "{quoted}"'
    return cmd


def build_flow_report(target_date: str, *, partial: bool = False) -> Dict[str, Any]:
    pre = _prefix(target_date)
    raw = _read_json(DATA_DIR / f"{pre}_raw.json") or {}
    crawl_report = _read_json(DATA_DIR / f"{pre}_crawl_product_retention.json")
    if not crawl_report and isinstance(raw, dict):
        crawl_report = raw.get("crawl_product_retention_report")
    if not isinstance(crawl_report, dict):
        crawl_report = {}
    queue_report = _read_json(DATA_DIR / f"{pre}_analysis_queue_report.json") or {}
    dedup_report = _read_json(DATA_DIR / f"{pre}_analysis_dedup_report.json") or {}
    pending = _read_json(DATA_DIR / f"{pre}_raw_pending_analysis.json") or {}
    analysis = _read_json(DATA_DIR / f"video_analysis_{pre}_raw.json") or {}
    failed = _read_json(DATA_DIR / f"{pre}_analysis_failed.json") or {}
    sync_report = _read_json(DATA_DIR / f"{pre}_sync_report.json") or {}
    acceptance = _read_json(DATA_DIR / f"{pre}_acceptance.json") or {}

    rows: Dict[str, Dict[str, Any]] = {}
    reason_by_product: Dict[str, Counter[str]] = defaultdict(Counter)

    raw_items = raw.get("items") if isinstance(raw, dict) else []
    if not isinstance(raw_items, list):
        raw_items = []
    ad_to_product = {
        _ad_key_from_item(item): _product_from_item(item)
        for item in raw_items
        if isinstance(item, dict) and _ad_key_from_item(item)
    }

    crawl_rows = crawl_report.get("per_product") or []
    if isinstance(crawl_rows, list) and crawl_rows:
        for crow in crawl_rows:
            if not isinstance(crow, dict):
                continue
            product = str(crow.get("product") or "未知产品")
            row = _row(rows, product)
            for key in (
                "dom_cards",
                "clicked_detail_rows",
                "captured_materials",
                "target_date_hits",
                "kept_after_crawl_filter",
                "kept_after_cover_filter",
            ):
                row[key] = _safe_int(crow.get(key))
            for reason, count in (crow.get("removed_reasons") or {}).items():
                _add_reason(reason_by_product, product, str(reason), _safe_int(count))
    else:
        for product, count in _count_items_by_product(raw_items).items():
            row = _row(rows, product)
            row["kept_after_cover_filter"] = count

    queue_rows = queue_report.get("per_product") or []
    if isinstance(queue_rows, list) and queue_rows:
        for qrow in queue_rows:
            if not isinstance(qrow, dict):
                continue
            product = str(qrow.get("product") or "未知产品")
            row = _row(rows, product)
            row["analysis_dedup_kept"] = _safe_int(qrow.get("after_dedup"))
            row["llm_queued"] = _safe_int(qrow.get("llm_queued"))
            row["analysis_cache_reused"] = _safe_int(qrow.get("cache_reused"))
            row["analysis_ineligible"] = _safe_int(qrow.get("ineligible_total"))
            if row["analysis_cache_reused"]:
                _add_reason(
                    reason_by_product,
                    product,
                    "analysis_cache_reused",
                    row["analysis_cache_reused"],
                )
            for reason, count in (qrow.get("ineligible_reasons") or {}).items():
                key = f"analysis_ineligible:{reason}"
                REASON_LABELS.setdefault(key, "素材信息不够分析")
                _add_reason(reason_by_product, product, key, _safe_int(count))
    else:
        dedup_removed: Counter[str] = Counter()
        for drow in dedup_report.get("intraday_removed") or []:
            if not isinstance(drow, dict):
                continue
            product = ad_to_product.get(str(drow.get("ad_key") or "").strip(), "未知产品")
            dedup_removed[product] += 1
        for drow in dedup_report.get("crossday_removed") or []:
            if not isinstance(drow, dict):
                continue
            product = str(drow.get("product") or "") or ad_to_product.get(
                str(drow.get("ad_key") or "").strip(), "未知产品"
            )
            dedup_removed[product] += 1
        for product, row in rows.items():
            row["analysis_dedup_kept"] = max(
                0,
                _safe_int(row.get("kept_after_cover_filter")) - int(dedup_removed.get(product, 0)),
            )
        for product, count in _count_items_by_product(pending.get("items") or []).items():
            _row(rows, product)["llm_queued"] = count

    if isinstance(dedup_report, dict):
        for drow in dedup_report.get("intraday_removed") or []:
            if not isinstance(drow, dict):
                continue
            product = ad_to_product.get(str(drow.get("ad_key") or "").strip(), "未知产品")
            _add_reason(reason_by_product, product, "analysis_dedup_intraday")
        for drow in dedup_report.get("crossday_removed") or []:
            if not isinstance(drow, dict):
                continue
            product = str(drow.get("product") or "") or ad_to_product.get(
                str(drow.get("ad_key") or "").strip(), "未知产品"
            )
            _add_reason(reason_by_product, product, _crossday_reason_key(str(drow.get("reason") or "")))

    for arow in analysis.get("results") or []:
        if not isinstance(arow, dict):
            continue
        product = _product_from_result(arow, ad_to_product)
        row = _row(rows, product)
        if _success_analysis(arow):
            row["analysis_success"] += 1
        elif str(arow.get("analysis") or "").startswith("[ERROR]"):
            _add_reason(reason_by_product, product, "analysis_failed")

    for frow in failed.get("failed") or []:
        if not isinstance(frow, dict):
            continue
        ad_key = str(frow.get("ad_key") or "").strip()
        product = ad_to_product.get(ad_key, "未知产品")
        _row(rows, product)
        _add_reason(reason_by_product, product, "analysis_failed")

    sync_rows = sync_report.get("per_product") or []
    if isinstance(sync_rows, list) and sync_rows:
        for srow in sync_rows:
            if not isinstance(srow, dict):
                continue
            product = str(srow.get("product") or "未知产品")
            row = _row(rows, product)
            row["sync_candidate"] = _safe_int(
                srow.get("after_template_dedup")
                if srow.get("after_template_dedup") is not None
                else srow.get("after_hard_exclusion")
            )
            row["synced_records"] = _safe_int(srow.get("synced_records"))
            for reason, count in (srow.get("removed_reasons") or {}).items():
                _add_reason(reason_by_product, product, str(reason), _safe_int(count))
    else:
        hard_removed: Counter[str] = Counter()
        for arow in analysis.get("results") or []:
            if not isinstance(arow, dict) or not _success_analysis(arow):
                continue
            if not arow.get("exclude_from_bitable"):
                continue
            product = _product_from_result(arow, ad_to_product)
            hard_removed[product] += 1
            _add_reason(reason_by_product, product, _hard_exclude_reason(arow))
        for product, row in rows.items():
            row["sync_candidate"] = max(0, _safe_int(row.get("analysis_success")) - int(hard_removed.get(product, 0)))
            row["synced_records"] = row["sync_candidate"]

    _merge_reason_labels(rows, reason_by_product)
    per_product = sorted(rows.values(), key=lambda r: (-_safe_int(r.get("kept_after_cover_filter")), str(r.get("product"))))

    totals = {key: sum(_safe_int(row.get(key)) for row in per_product) for key in REPORT_COLUMNS}
    totals["removed_total"] = sum(_safe_int(row.get("removed_total")) for row in per_product)
    summary_reasons: Counter[str] = Counter()
    for row in per_product:
        for key, value in (row.get("removed_reasons") or {}).items():
            summary_reasons[str(key)] += _safe_int(value)
    totals["removed_reasons"] = {
        key: int(value)
        for key, value in sorted(summary_reasons.items(), key=lambda kv: (-int(kv[1]), kv[0]))
        if int(value) > 0
    }
    totals["removed_reason_labels"] = {key: REASON_LABELS.get(key, key) for key in totals["removed_reasons"]}

    lookback_days = _env_int("VE_FLOW_REPORT_LOOKBACK_DAYS", "5")
    history = _load_history_values(target_date, lookback_days)
    alerts = detect_product_volume_alerts(
        per_product,
        history,
        low_ratio=_env_float("VE_FLOW_REPORT_LOW_RATIO", "0.5"),
        min_baseline=_env_int("VE_FLOW_REPORT_MIN_BASELINE", "3"),
        min_history_days=_env_int("VE_FLOW_REPORT_MIN_HISTORY_DAYS", "2"),
        target_date=target_date,
    )

    return {
        "target_date": target_date,
        "partial": bool(partial),
        "lookback_days": lookback_days,
        "status": "alert" if alerts else ("partial" if partial else "ok"),
        "alerts": alerts,
        "totals": totals,
        "per_product": per_product,
        "source_files": {
            "raw": str(DATA_DIR / f"{pre}_raw.json"),
            "crawl_product_retention": str(DATA_DIR / f"{pre}_crawl_product_retention.json"),
            "analysis_queue": str(DATA_DIR / f"{pre}_analysis_queue_report.json"),
            "analysis_dedup": str(DATA_DIR / f"{pre}_analysis_dedup_report.json"),
            "pending_analysis": str(DATA_DIR / f"{pre}_raw_pending_analysis.json"),
            "analysis": str(DATA_DIR / f"video_analysis_{pre}_raw.json"),
            "analysis_failed": str(DATA_DIR / f"{pre}_analysis_failed.json"),
            "sync": str(DATA_DIR / f"{pre}_sync_report.json"),
            "acceptance": str(DATA_DIR / f"{pre}_acceptance.json"),
        },
        "acceptance_status": acceptance.get("status") if isinstance(acceptance, dict) else None,
    }


def render_markdown(report: Dict[str, Any]) -> str:
    target_date = str(report.get("target_date") or "")
    totals = report.get("totals") or {}
    alerts = report.get("alerts") or []
    lines: List[str] = []
    lines.append(f"# VE 全流程报告（{target_date}）\n")
    if alerts:
        lines.append(f"**告警：{len(alerts)} 个产品/指标显著低于近 {report.get('lookback_days')} 天。**\n\n")
    elif report.get("partial"):
        lines.append("**流程提前结束：本报告按已产生的节点产物汇总。**\n\n")
    else:
        lines.append("**流程完成：关键节点已汇总。**\n\n")

    lines.append("## 汇总\n\n")
    lines.append("| 节点 | 数量 |\n| --- | ---: |\n")
    for key in REPORT_COLUMNS:
        lines.append(f"| {COLUMN_LABELS.get(key, key)} | {_safe_int(totals.get(key))} |\n")
    lines.append(f"| 累计筛掉/跳过原因计数 | {_safe_int(totals.get('removed_total'))} |\n")
    lines.append("\n")

    if alerts:
        lines.append("## 异常警报与重试建议\n\n")
        for alert in alerts[:12]:
            lines.append(
                f"- **{alert.get('product')}** {alert.get('metric_label')}: "
                f"今日 {alert.get('today')}，历史均值 {alert.get('history_mean')}，"
                f"比例 {alert.get('ratio')}。建议先人工看抓取日志/页面，再决定是否重试。\n"
            )
            cmd = str(alert.get("retry_command") or "").strip()
            if cmd:
                lines.append(f"  `重试命令：{cmd}`\n")
        lines.append("\n")

    lines.append("## 产品漏斗\n\n")
    header = "| 产品 | " + " | ".join(COLUMN_LABELS[k] for k in REPORT_COLUMNS) + " | 主要去掉原因 |\n"
    sep = "| --- | " + " | ".join("---:" for _ in REPORT_COLUMNS) + " | --- |\n"
    lines.append(header)
    lines.append(sep)
    for row in report.get("per_product") or []:
        if not isinstance(row, dict):
            continue
        cells = [str(row.get("product") or "未知产品")]
        cells.extend(str(_safe_int(row.get(k))) for k in REPORT_COLUMNS)
        cells.append(_top_reason_text(row.get("removed_reasons") or {}))
        lines.append("| " + " | ".join(c.replace("|", "/") for c in cells) + " |\n")

    lines.append("\n## 全局主要原因\n\n")
    reason_text = _top_reason_text(totals.get("removed_reasons") or {}, limit=12)
    lines.append(reason_text + "\n")
    lines.append(
        "\n> 说明：`主表写入`来自 sync 节点报告；如果该节点被跳过或提前失败，会退化为同步候选估算。"
        "飞书卡片只给确认与重试命令，不会自动触发重跑。\n"
    )
    return "".join(lines)


def _metric_chip(label: str, value: Any) -> str:
    return f"`{label} {_safe_int(value)}`"


def _card_markdown(content: str) -> Dict[str, str]:
    return {"tag": "markdown", "content": content[:12000]}


def _card_section(title: str, content: str) -> Dict[str, str]:
    return _card_markdown(f"**{title}**\n{content}".strip())


def _product_card_line(row: Dict[str, Any]) -> str:
    reasons = _business_reason_text(row.get("removed_reasons") or {}, limit=4)
    product = str(row.get("product") or "未知产品").strip()
    read_line = (
        f"原始页面：候选卡片 **{_safe_int(row.get('dom_cards'))}**，"
        f"详情返回 **{_safe_int(row.get('clicked_detail_rows'))}**，"
        f"读取到原始素材 **{_safe_int(row.get('captured_materials'))}**"
    )
    keep_line = (
        f"筛选去重：当天首见 **{_safe_int(row.get('target_date_hits'))}**，"
        f"进入 raw **{_safe_int(row.get('kept_after_crawl_filter'))}**，"
        f"封面去重后 **{_safe_int(row.get('kept_after_cover_filter'))}**，"
        f"待 AI 分析 **{_safe_int(row.get('analysis_dedup_kept'))}**"
    )
    final_line = (
        f"结果：新送 AI **{_safe_int(row.get('llm_queued'))}**，"
        f"AI 完成 **{_safe_int(row.get('analysis_success'))}**，"
        f"已入多维表 **{_safe_int(row.get('synced_records'))}**"
    )
    return f"**{product}**\n{read_line}\n{keep_line}\n{final_line}\n筛掉主要发生在：{reasons}"


def _build_feishu_card(report: Dict[str, Any]) -> Dict[str, Any]:
    target_date = str(report.get("target_date") or "")
    status = str(report.get("status") or "ok")
    totals = report.get("totals") or {}
    alerts = report.get("alerts") or []
    per_product = [row for row in (report.get("per_product") or []) if isinstance(row, dict)]

    template = "red" if status == "alert" else ("orange" if status == "partial" else "blue")
    status_text = "有告警" if status == "alert" else ("提前结束" if status == "partial" else "正常")
    title = f"VE 全流程报告｜{target_date}｜{status_text}"

    elements: List[Dict[str, Any]] = []
    summary_lines = [
        (
            f"原始页面：候选卡片 **{_safe_int(totals.get('dom_cards'))}**，"
            f"详情返回 **{_safe_int(totals.get('clicked_detail_rows'))}**，"
            f"读取到原始素材 **{_safe_int(totals.get('captured_materials'))}**"
        ),
        (
            f"筛选去重：当天首见 **{_safe_int(totals.get('target_date_hits'))}**，"
            f"进入 raw **{_safe_int(totals.get('kept_after_crawl_filter'))}**，"
            f"封面去重后 **{_safe_int(totals.get('kept_after_cover_filter'))}**，"
            f"待 AI 分析 **{_safe_int(totals.get('analysis_dedup_kept'))}**"
        ),
        (
            f"结果：新送 AI **{_safe_int(totals.get('llm_queued'))}**，"
            f"AI 完成 **{_safe_int(totals.get('analysis_success'))}**，"
            f"已入多维表 **{_safe_int(totals.get('synced_records'))}**"
        ),
        f"筛掉/跳过原因记录 **{_safe_int(totals.get('removed_total'))}** 条；告警 **{len(alerts)}** 个",
    ]
    elements.append(_card_section("今日概览", "\n".join(summary_lines)))
    elements.append(
        _card_section(
            "怎么看这张卡",
            "候选卡片 = 页面上看到的素材卡；详情返回 = 点卡接口返回的详情行，不等于点击次数；"
            "读取到原始素材 = DOM 卡片和详情合并后的原始素材，还没做广告主/日期筛选；"
            "当天首见 = 广告主一致且 first seen 是目标日期；进入 raw = 再去掉重投/重复后进入当天原始素材；"
            "封面去重后 = 去掉历史或当天看起来重复的封面；"
            "已入多维表 = 进入人工复核表。",
        )
    )

    if alerts:
        alert_lines: List[str] = []
        for alert in alerts[:6]:
            alert_lines.append(
                f"- **{alert.get('product')}**｜{alert.get('metric_label')} "
                f"今日 **{alert.get('today')}**，历史均值 **{alert.get('history_mean')}**，"
                f"比例 **{alert.get('ratio')}**"
            )
            cmd = str(alert.get("retry_command") or "").strip()
            if cmd:
                alert_lines.append(f"  重试命令：`{cmd}`")
        if len(alerts) > 6:
            alert_lines.append(f"- 还有 {len(alerts) - 6} 条告警，见本地完整报告。")
        elements.append(_card_section("异常警报｜人工决定是否重试", "\n".join(alert_lines)))

    product_chunks: List[str] = []
    for row in per_product[:12]:
        product_chunks.append(_product_card_line(row))
    if len(per_product) > 12:
        product_chunks.append(f"还有 {len(per_product) - 12} 个产品，见本地完整报告。")
    for i in range(0, len(product_chunks), 3):
        elements.append(_card_section("产品漏斗" if i == 0 else "产品漏斗（续）", "\n\n".join(product_chunks[i : i + 3])))

    global_reasons = _business_reason_text(totals.get("removed_reasons") or {}, limit=8)
    elements.append(_card_section("筛掉主要发生在", global_reasons))
    elements.append(
        _card_markdown(
            "说明：旧日期补发时，`已入表` 可能按已有分析结果估算；明天新流程会记录真实入表数。"
            "卡片只提供判断依据和重试命令，不会自动重跑。"
        )
    )

    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True, "enable_forward": True},
            "header": {"title": {"tag": "plain_text", "content": title}, "template": template},
            "elements": elements,
        },
    }


def _send_feishu_card(report: Dict[str, Any], webhook: str) -> None:
    if not webhook:
        print("[flow-report] 未配置 VE_FLOW_REPORT_FEISHU_WEBHOOK，跳过飞书推送。")
        return
    body = _build_feishu_card(report)
    resp = requests.post(webhook, json=body, timeout=15)
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}
    if resp.status_code != 200 or data.get("code") != 0:
        raise RuntimeError(f"飞书全流程报告推送失败: status={resp.status_code}, resp={data}")
    print("[flow-report] 飞书全流程报告推送成功。")


def run_flow_report_after_workflow(target_date: str, *, partial: bool = False) -> None:
    if not _env_bool("VE_FLOW_REPORT_ENABLED", "1"):
        print(f"[flow-report] 已关闭（VE_FLOW_REPORT_ENABLED=0），跳过 {target_date}")
        return
    report = build_flow_report(target_date, partial=partial)
    markdown = render_markdown(report)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_json = DATA_DIR / f"workflow_video_enhancer_{target_date}_flow_report.json"
    out_md = REPORTS_DIR / f"workflow_video_enhancer_{target_date}_flow_report.md"
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    out_md.write_text(markdown, encoding="utf-8")
    print(
        f"[flow-report] {target_date} status={report.get('status')} "
        f"alerts={len(report.get('alerts') or [])}; 已写 {out_json.name} 与 {out_md.name}"
    )
    if _env_bool("VE_FLOW_REPORT_FEISHU_ENABLED", "1"):
        webhook = (os.getenv("VE_FLOW_REPORT_FEISHU_WEBHOOK") or "").strip()
        try:
            _send_feishu_card(report, webhook)
        except Exception as e:
            print(f"[flow-report] 飞书推送失败: {e}")
            if _env_bool("VE_FLOW_REPORT_FEISHU_STRICT", "0"):
                raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Video Enhancer 全流程产品漏斗报告")
    parser.add_argument("--date", required=True, metavar="YYYY-MM-DD", help="target_date")
    parser.add_argument("--partial", action="store_true", help="按提前结束流程生成报告")
    parser.add_argument("--no-send", action="store_true", help="只写 JSON/Markdown，不推飞书")
    parser.add_argument("--feishu-webhook", default="", help="当次覆盖 VE_FLOW_REPORT_FEISHU_WEBHOOK")
    args = parser.parse_args()
    if args.no_send:
        os.environ["VE_FLOW_REPORT_FEISHU_ENABLED"] = "0"
    if args.feishu_webhook.strip():
        os.environ["VE_FLOW_REPORT_FEISHU_WEBHOOK"] = args.feishu_webhook.strip()
        os.environ["VE_FLOW_REPORT_FEISHU_ENABLED"] = "1"
    run_flow_report_after_workflow(args.date, partial=args.partial)


if __name__ == "__main__":
    main()
