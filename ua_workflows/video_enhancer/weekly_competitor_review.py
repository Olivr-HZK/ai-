"""Weekly VE competitor pruning and new-candidate review."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

import requests

from ua_workflows.shared.config import CONFIG_DIR, DATA_DIR, REPORTS_DIR, load_project_env
from ua_workflows.video_enhancer.competitor_list import load_competitors_from_bitable
from ua_workflows.video_enhancer.haopeng_ai_filter import fetch_rows_from_bitable

POSITIVE_STATUSES = {"采纳", "接受", "入素材库"}
NEGATIVE_STATUSES = {"不采纳", "删除", "拒绝", "重复抓取"}
DECISIVE_STATUSES = POSITIVE_STATUSES | NEGATIVE_STATUSES

DEFAULT_MATERIAL_THRESHOLD = 10
DEFAULT_ADOPTION_THRESHOLD = 0.5
DEFAULT_CANDIDATE_THRESHOLD = 5
DEFAULT_REVIEWER_FIELD = "浩鹏接受情况"
DEFAULT_CONFIG_GROUPS = ("video", "photo")


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_env_int(key: str, default: int) -> int:
    return _safe_int(os.getenv(key) if os.getenv(key) not in (None, "") else default)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, bool)):
        return str(value).strip()
    if isinstance(value, dict):
        for key in ("text", "name", "value"):
            text = _text(value.get(key))
            if text:
                return text
        return ""
    if isinstance(value, list):
        return "、".join(part for part in (_text(item) for item in value) if part)
    return str(value).strip()


def _today_shanghai() -> dt.date:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).date()


def _parse_date(value: str | dt.date | None) -> dt.date:
    if isinstance(value, dt.date):
        return value
    if value:
        return dt.date.fromisoformat(str(value))
    return _today_shanghai()


def previous_natural_week(run_date: str | dt.date | None = None) -> tuple[dt.date, dt.date]:
    """Return the Monday-Sunday week completed before run_date's week."""
    day = _parse_date(run_date)
    this_monday = day - dt.timedelta(days=day.weekday())
    start = this_monday - dt.timedelta(days=7)
    return start, start + dt.timedelta(days=6)


def _date_range(start: dt.date, end: dt.date) -> list[dt.date]:
    days = (end - start).days
    return [start + dt.timedelta(days=offset) for offset in range(max(0, days) + 1)]


def load_current_ve_competitors(
    config_path: Path = CONFIG_DIR / "ai_product.json",
    *,
    groups: Iterable[str] = DEFAULT_CONFIG_GROUPS,
) -> set[str]:
    data = _read_json(config_path)
    out: set[str] = set()
    for group in groups:
        section = data.get(group)
        if isinstance(section, dict):
            out.update(str(name).strip() for name in section if str(name).strip())
    return out


def load_current_ve_competitors_from_bitable_or_config(
    bitable_url: str = "",
    *,
    config_path: Path = CONFIG_DIR / "ai_product.json",
) -> set[str]:
    url = (bitable_url or os.getenv("VIDEO_ENHANCER_BITABLE_URL") or "").strip()
    if url:
        try:
            rows = load_competitors_from_bitable(url)
            products = {row.product for row in rows if row.product}
            if products:
                return products
        except Exception as exc:
            print(f"[weekly-competitor-review] 竞品list 读取失败，改用本地配置：{exc}")
    return load_current_ve_competitors(config_path=config_path)


def _crawl_report_path(data_dir: Path, day: dt.date) -> Path:
    return data_dir / f"workflow_video_enhancer_{day.isoformat()}_crawl_product_retention.json"


def _raw_path(data_dir: Path, day: dt.date) -> Path:
    return data_dir / f"workflow_video_enhancer_{day.isoformat()}_raw.json"


def _raw_item_product(item: Any) -> str:
    if not isinstance(item, dict):
        return ""
    creative = item.get("creative") if isinstance(item.get("creative"), dict) else {}
    return _text(item.get("product") or creative.get("advertiser_name") or item.get("keyword"))


def _load_day_counts(data_dir: Path, day: dt.date) -> tuple[dict[str, int], Path | None]:
    report_path = _crawl_report_path(data_dir, day)
    report = _read_json(report_path)
    if not report:
        raw = _read_json(_raw_path(data_dir, day))
        report = raw.get("crawl_product_retention_report") if isinstance(raw.get("crawl_product_retention_report"), dict) else {}
    else:
        raw = {}

    rows = report.get("per_product") if isinstance(report, dict) else []
    counts: dict[str, int] = {}
    if isinstance(rows, list) and rows:
        for row in rows:
            if not isinstance(row, dict):
                continue
            product = _text(row.get("product"))
            if product:
                counts[product] = _safe_int(row.get("kept_after_crawl_filter"))
        return counts, report_path if report_path.is_file() else _raw_path(data_dir, day)

    if not raw:
        raw = _read_json(_raw_path(data_dir, day))
    items = raw.get("items") if isinstance(raw, dict) else []
    if isinstance(items, list):
        for item in items:
            product = _raw_item_product(item)
            if product:
                counts[product] = counts.get(product, 0) + 1
    competitors = raw.get("competitors") if isinstance(raw, dict) else []
    if isinstance(competitors, list):
        for product in competitors:
            text = _text(product)
            if text:
                counts.setdefault(text, 0)
    source = _raw_path(data_dir, day) if _raw_path(data_dir, day).is_file() else None
    return counts, source


def _old_competitor_pool(data_dir: Path, week_start: dt.date, fallback: set[str]) -> list[str]:
    counts, _source = _load_day_counts(data_dir, week_start)
    products = [product for product in counts if product]
    if products:
        return sorted(products)
    return sorted(product for product in fallback if product)


def _row_date(row: dict[str, Any]) -> str:
    value = row.get("date")
    if value in (None, ""):
        value = row.get("抓取日期") or row.get("日期")
    text = _text(value)
    match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    return match.group(0) if match else text


def _row_product(row: dict[str, Any]) -> str:
    fields = row.get("fields") if isinstance(row.get("fields"), dict) else {}
    return _text(row.get("product") or fields.get("产品") or fields.get("product") or fields.get("广告主"))


def _row_status(row: dict[str, Any], reviewer_field: str) -> str:
    fields = row.get("fields") if isinstance(row.get("fields"), dict) else {}
    return _text(row.get("actual_hp") or fields.get(reviewer_field) or fields.get("接受情况"))


def summarize_adoption(
    rows: Iterable[dict[str, Any]],
    *,
    week_start: dt.date,
    week_end: dt.date,
    reviewer_field: str = DEFAULT_REVIEWER_FIELD,
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = defaultdict(lambda: {"positive": 0, "negative": 0, "decisive": 0})
    for row in rows:
        if not isinstance(row, dict):
            continue
        product = _row_product(row)
        if not product:
            continue
        try:
            row_day = dt.date.fromisoformat(_row_date(row))
        except ValueError:
            continue
        if row_day < week_start or row_day > week_end:
            continue
        status = _row_status(row, reviewer_field)
        if status in POSITIVE_STATUSES:
            out[product]["positive"] += 1
            out[product]["decisive"] += 1
        elif status in NEGATIVE_STATUSES:
            out[product]["negative"] += 1
            out[product]["decisive"] += 1

    for summary in out.values():
        decisive = int(summary["decisive"])
        summary["rate"] = (int(summary["positive"]) / decisive) if decisive else None
    return dict(out)


def _rate_text(rate: Any) -> str:
    if rate is None:
        return "反馈不足"
    return f"{round(float(rate) * 100):.0f}%"


def _classify_existing_competitor(
    *,
    product: str,
    weekly_materials: int,
    adoption: dict[str, Any],
    material_threshold: int,
    adoption_threshold: float,
) -> dict[str, Any]:
    positive = _safe_int(adoption.get("positive"))
    negative = _safe_int(adoption.get("negative"))
    decisive = _safe_int(adoption.get("decisive"))
    rate = adoption.get("rate")
    recommendation = "keep"
    reason = "素材量正常"
    if weekly_materials == 0:
        recommendation = "remove"
        reason = "一周素材 0 条"
    elif weekly_materials < material_threshold:
        if decisive > 0 and rate is not None and float(rate) < adoption_threshold:
            recommendation = "remove"
            reason = f"素材少于 {material_threshold} 条，浩鹏采纳低于 {round(adoption_threshold * 100):.0f}%"
        else:
            recommendation = "watch"
            reason = f"素材少于 {material_threshold} 条，浩鹏采纳 {_rate_text(rate)}"
    return {
        "product": product,
        "weekly_materials": int(weekly_materials),
        "adoption_positive": positive,
        "adoption_negative": negative,
        "adoption_total": decisive,
        "adoption_rate": rate,
        "recommendation": recommendation,
        "reason": reason,
    }


def _candidate_date_from_path(path: Path) -> dt.date | None:
    match = re.search(r"guangdada_new_charts_ai_tools_(\d{4}-\d{2}-\d{2})_raw\.json$", path.name)
    if not match:
        return None
    try:
        return dt.date.fromisoformat(match.group(1))
    except ValueError:
        return None


def discover_chart_paths(data_dir: Path, *, week_end: dt.date) -> list[Path]:
    latest_allowed = week_end + dt.timedelta(days=3)
    paths: list[Path] = []
    for path in sorted(data_dir.glob("guangdada_new_charts_ai_tools_*_raw.json")):
        path_day = _candidate_date_from_path(path)
        if path_day is None:
            continue
        if week_end <= path_day <= latest_allowed:
            paths.append(path)
    return paths


def _chart_creative(item: Any) -> dict[str, Any]:
    if isinstance(item, dict):
        creative = item.get("creative")
        if isinstance(creative, dict):
            return creative
        return item
    return {}


def _norm_name(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip()).casefold()


def collect_chart_candidates(
    chart_paths: Iterable[Path],
    *,
    current_competitors: set[str],
    candidate_threshold: int = DEFAULT_CANDIDATE_THRESHOLD,
) -> list[dict[str, Any]]:
    seen_by_advertiser: dict[str, set[str]] = defaultdict(set)
    category_counts: dict[str, Counter[str]] = defaultdict(Counter)
    source_files: dict[str, set[str]] = defaultdict(set)
    for path in chart_paths:
        payload = _read_json(Path(path))
        items = payload.get("items") if isinstance(payload, dict) else []
        if not isinstance(items, list):
            continue
        for idx, item in enumerate(items):
            creative = _chart_creative(item)
            advertiser = _text(
                creative.get("advertiser_name")
                or creative.get("app_name")
                or creative.get("product")
                or (item.get("product") if isinstance(item, dict) else "")
            )
            if not advertiser:
                continue
            key = _text(creative.get("ad_key") or creative.get("creative_id") or creative.get("id"))
            if not key:
                key = f"{path.name}:{idx}:{_text(creative.get('title'))}:{_text(creative.get('video_url'))}"
            seen_by_advertiser[advertiser].add(key)
            category = _text(creative.get("new_charts_category_label") or (item.get("product") if isinstance(item, dict) else ""))
            if category:
                category_counts[advertiser][category] += 1
            source_files[advertiser].add(path.name)

    current_norm = {_norm_name(name) for name in current_competitors}
    out: list[dict[str, Any]] = []
    for advertiser, keys in seen_by_advertiser.items():
        count = len(keys)
        if count <= candidate_threshold:
            continue
        status = "already_in_competitor_list" if _norm_name(advertiser) in current_norm else "new_candidate"
        out.append(
            {
                "advertiser": advertiser,
                "material_count": count,
                "status": status,
                "category_counts": dict(category_counts.get(advertiser, Counter())),
                "source_files": sorted(source_files.get(advertiser, set())),
            }
        )
    return sorted(out, key=lambda row: (-_safe_int(row.get("material_count")), str(row.get("advertiser"))))


def build_weekly_review(
    *,
    run_date: str | dt.date | None = None,
    week_start: str | dt.date | None = None,
    data_dir: Path = DATA_DIR,
    feedback_rows: Iterable[dict[str, Any]] = (),
    current_competitors: set[str] | None = None,
    chart_paths: Iterable[Path] | None = None,
    reviewer_field: str = DEFAULT_REVIEWER_FIELD,
    material_threshold: int = DEFAULT_MATERIAL_THRESHOLD,
    adoption_threshold: float = DEFAULT_ADOPTION_THRESHOLD,
    candidate_threshold: int = DEFAULT_CANDIDATE_THRESHOLD,
) -> dict[str, Any]:
    data_dir = Path(data_dir)
    if week_start is not None:
        start = _parse_date(week_start)
        end = start + dt.timedelta(days=6)
    else:
        start, end = previous_natural_week(run_date)
    current = current_competitors if current_competitors is not None else load_current_ve_competitors()
    old_pool = _old_competitor_pool(data_dir, start, current)

    weekly_counts: Counter[str] = Counter()
    source_files: list[str] = []
    daily_counts: dict[str, dict[str, int]] = {}
    for day in _date_range(start, end):
        counts, source = _load_day_counts(data_dir, day)
        daily_counts[day.isoformat()] = dict(counts)
        if source is not None:
            source_files.append(str(source))
        for product in old_pool:
            weekly_counts[product] += _safe_int(counts.get(product))

    adoption_by_product = summarize_adoption(
        feedback_rows,
        week_start=start,
        week_end=end,
        reviewer_field=reviewer_field,
    )
    existing = [
        _classify_existing_competitor(
            product=product,
            weekly_materials=_safe_int(weekly_counts.get(product)),
            adoption=adoption_by_product.get(product, {}),
            material_threshold=material_threshold,
            adoption_threshold=adoption_threshold,
        )
        for product in old_pool
    ]
    existing = sorted(existing, key=lambda row: (row["recommendation"] == "keep", -_safe_int(row["weekly_materials"]), row["product"]))
    remove = [row for row in existing if row.get("recommendation") == "remove"]
    watch = [row for row in existing if row.get("recommendation") == "watch"]
    keep = [row for row in existing if row.get("recommendation") == "keep"]

    paths = [Path(p) for p in chart_paths] if chart_paths is not None else discover_chart_paths(data_dir, week_end=end)
    candidates = collect_chart_candidates(
        paths,
        current_competitors=current,
        candidate_threshold=candidate_threshold,
    )
    return {
        "week_start": start.isoformat(),
        "week_end": end.isoformat(),
        "run_date": _parse_date(run_date).isoformat() if run_date is not None else _today_shanghai().isoformat(),
        "material_threshold": int(material_threshold),
        "adoption_threshold": float(adoption_threshold),
        "candidate_threshold": int(candidate_threshold),
        "old_competitor_pool": old_pool,
        "current_competitors": sorted(current),
        "existing_competitors": existing,
        "remove": remove,
        "watch": watch,
        "keep": keep,
        "new_competitor_candidates": candidates,
        "daily_counts": daily_counts,
        "source_files": {
            "crawl": source_files,
            "charts": [str(path) for path in paths],
        },
    }


def _card_markdown(text: str) -> dict[str, Any]:
    return {"tag": "div", "text": {"tag": "lark_md", "content": text}}


def _column(text: str) -> dict[str, Any]:
    return {
        "tag": "column",
        "width": "weighted",
        "weight": 1,
        "elements": [_card_markdown(text)],
    }


def _columns(elements: list[str]) -> dict[str, Any]:
    return {"tag": "column_set", "flex_mode": "bisect", "background_style": "default", "columns": [_column(text) for text in elements]}


def _existing_col(row: dict[str, Any]) -> str:
    rate = _rate_text(row.get("adoption_rate"))
    total = _safe_int(row.get("adoption_total"))
    suffix = f"{rate}（{_safe_int(row.get('adoption_positive'))}/{total}）" if total else rate
    return (
        f"**{row.get('product', '')}**\n"
        f"素材：**{_safe_int(row.get('weekly_materials'))}** 条\n"
        f"浩鹏：{suffix}\n"
        f"{row.get('reason', '')}"
    )


def _candidate_col(row: dict[str, Any]) -> str:
    status = "已在当前竞品 list" if row.get("status") == "already_in_competitor_list" else "新增候选"
    return f"**{row.get('advertiser', '')}**\n素材：**{_safe_int(row.get('material_count'))}** 条\n{status}"


def _add_column_section(
    elements: list[dict[str, Any]],
    title: str,
    rows: list[dict[str, Any]],
    formatter: Any,
    *,
    empty: str,
) -> None:
    elements.append(_card_markdown(f"### {title}"))
    if not rows:
        elements.append(_card_markdown(empty))
        return
    formatted = [formatter(row) for row in rows]
    for idx in range(0, len(formatted), 3):
        elements.append(_columns(formatted[idx : idx + 3]))


def build_feishu_card(report: dict[str, Any]) -> dict[str, Any]:
    week_start = str(report.get("week_start") or "")
    week_end = str(report.get("week_end") or "")
    remove = [row for row in (report.get("remove") or []) if isinstance(row, dict)]
    watch = [row for row in (report.get("watch") or []) if isinstance(row, dict)]
    candidates = [row for row in (report.get("new_competitor_candidates") or []) if isinstance(row, dict)]
    title = f"VE 竞品周检查｜{week_start} ~ {week_end}"
    elements: list[dict[str, Any]] = [
        _card_markdown(
            "建议剔除：0条；或少于10条且浩鹏采纳低于50%。\n"
            "低量观察：少于10条，但浩鹏采纳达到50%，或反馈不够。\n"
            "新竞品候选：AI图像 / AI视频周榜里，同广告主 >5条。"
        )
    ]
    _add_column_section(elements, "建议剔除", remove, _existing_col, empty="暂无")
    _add_column_section(elements, "低量观察", watch, _existing_col, empty="暂无")
    _add_column_section(elements, "新竞品候选", candidates, _candidate_col, empty="暂无")
    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True, "enable_forward": True},
            "header": {"title": {"tag": "plain_text", "content": title}, "template": "orange" if remove or watch else "blue"},
            "elements": elements,
        },
    }


def send_feishu_card(report: dict[str, Any], webhook: str) -> dict[str, Any]:
    if not webhook:
        print("[weekly-competitor-review] 未配置飞书 webhook，跳过推送。")
        return {}
    response = requests.post(webhook, json=build_feishu_card(report), timeout=20)
    try:
        data = response.json()
    except Exception:
        data = {"raw": response.text}
    ok = response.status_code == 200 and (data.get("code") == 0 or data.get("StatusCode") == 0)
    if not ok:
        raise RuntimeError(f"飞书竞品周检查推送失败: status={response.status_code}, resp={data}")
    print("[weekly-competitor-review] 飞书竞品周检查推送成功。")
    return data


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# VE 竞品周检查 {report.get('week_start')} ~ {report.get('week_end')}",
        "",
        "建议剔除：0条；或少于10条且浩鹏采纳低于50%。",
        "低量观察：少于10条，但浩鹏采纳达到50%，或反馈不够。",
        "新竞品候选：AI图像 / AI视频周榜里，同广告主 >5条。",
        "",
        "## 建议剔除",
    ]
    for row in report.get("remove") or []:
        lines.append(f"- {row.get('product')}：素材 {row.get('weekly_materials')} 条，浩鹏 {_rate_text(row.get('adoption_rate'))}，{row.get('reason')}")
    if not report.get("remove"):
        lines.append("- 暂无")
    lines.append("")
    lines.append("## 低量观察")
    for row in report.get("watch") or []:
        lines.append(f"- {row.get('product')}：素材 {row.get('weekly_materials')} 条，浩鹏 {_rate_text(row.get('adoption_rate'))}，{row.get('reason')}")
    if not report.get("watch"):
        lines.append("- 暂无")
    lines.append("")
    lines.append("## 新竞品候选")
    for row in report.get("new_competitor_candidates") or []:
        status = "已在当前竞品 list" if row.get("status") == "already_in_competitor_list" else "新增候选"
        lines.append(f"- {row.get('advertiser')}：素材 {row.get('material_count')} 条，{status}")
    if not report.get("new_competitor_candidates"):
        lines.append("- 暂无")
    return "\n".join(lines) + "\n"


def _env_bool(key: str, default: str = "1") -> bool:
    return (os.getenv(key) or default).strip().lower() in {"1", "true", "yes", "on"}


def _resolve_webhook(explicit: str = "") -> str:
    if explicit.strip():
        return explicit.strip()
    return (
        os.getenv("VE_WEEKLY_COMPETITOR_REVIEW_FEISHU_WEBHOOK")
        or os.getenv("FEISHU_BOT_WEBHOOK")
        or os.getenv("FEISHU_UA_WEBHOOK")
        or ""
    ).strip()


def run_weekly_review(
    *,
    run_date: str | dt.date | None = None,
    week_start: str | dt.date | None = None,
    data_dir: Path = DATA_DIR,
    chart_paths: Iterable[Path] | None = None,
    bitable_url: str = "",
    skip_bitable: bool = False,
    reviewer_field: str = DEFAULT_REVIEWER_FIELD,
    feishu_webhook: str = "",
    send: bool = True,
    material_threshold: int = DEFAULT_MATERIAL_THRESHOLD,
    adoption_threshold: float = DEFAULT_ADOPTION_THRESHOLD,
    candidate_threshold: int = DEFAULT_CANDIDATE_THRESHOLD,
) -> dict[str, Any]:
    load_project_env()
    bitable = (bitable_url or os.getenv("VIDEO_ENHANCER_BITABLE_URL") or "").strip()
    feedback_rows: list[dict[str, Any]] = []
    if skip_bitable:
        print("[weekly-competitor-review] 已指定 --no-bitable，浩鹏反馈按不足处理。")
    elif bitable:
        feedback_rows = fetch_rows_from_bitable(bitable, reviewer_field=reviewer_field)
    else:
        print("[weekly-competitor-review] 未配置 VIDEO_ENHANCER_BITABLE_URL，浩鹏反馈按不足处理。")
    report = build_weekly_review(
        run_date=run_date,
        week_start=week_start,
        data_dir=Path(data_dir),
        feedback_rows=feedback_rows,
        current_competitors=load_current_ve_competitors_from_bitable_or_config(bitable),
        chart_paths=chart_paths,
        reviewer_field=reviewer_field,
        material_threshold=material_threshold,
        adoption_threshold=adoption_threshold,
        candidate_threshold=candidate_threshold,
    )
    start = str(report.get("week_start"))
    end = str(report.get("week_end"))
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_json = DATA_DIR / f"workflow_video_enhancer_{start}_{end}_weekly_competitor_review.json"
    out_md = REPORTS_DIR / f"workflow_video_enhancer_{start}_{end}_weekly_competitor_review.md"
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    out_md.write_text(render_markdown(report), encoding="utf-8")
    print(f"[weekly-competitor-review] 已写 {out_json} 与 {out_md}")
    if send and _env_bool("VE_WEEKLY_COMPETITOR_REVIEW_FEISHU_ENABLED", "1"):
        send_feishu_card(report, _resolve_webhook(feishu_webhook))
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="VE 每周竞品检查：老竞品剔除/观察 + 新竞品候选")
    parser.add_argument("--run-date", default="", help="运行日期 YYYY-MM-DD，默认今天；用于计算上一自然周")
    parser.add_argument("--week-start", default="", help="直接指定自然周周一 YYYY-MM-DD")
    parser.add_argument("--data-dir", default=str(DATA_DIR), help="工作流 data 目录")
    parser.add_argument("--chart-raw", action="append", default=None, help="广大大 AI 工具周榜 raw，可重复传")
    parser.add_argument("--bitable-url", default="", help="覆盖 VIDEO_ENHANCER_BITABLE_URL")
    parser.add_argument("--no-bitable", action="store_true", help="不读取多维表浩鹏反馈，按反馈不足处理")
    parser.add_argument("--reviewer-field", default=DEFAULT_REVIEWER_FIELD, help="浩鹏反馈字段名")
    parser.add_argument("--feishu-webhook", default="", help="覆盖飞书机器人 webhook")
    parser.add_argument("--no-send", action="store_true", help="只写报告，不推飞书")
    parser.add_argument("--material-threshold", type=int, default=_safe_env_int("VE_WEEKLY_COMPETITOR_MATERIAL_THRESHOLD", DEFAULT_MATERIAL_THRESHOLD))
    parser.add_argument("--adoption-threshold", type=float, default=_safe_float(os.getenv("VE_WEEKLY_COMPETITOR_ADOPTION_THRESHOLD"), DEFAULT_ADOPTION_THRESHOLD))
    parser.add_argument("--candidate-threshold", type=int, default=_safe_env_int("VE_WEEKLY_COMPETITOR_CANDIDATE_THRESHOLD", DEFAULT_CANDIDATE_THRESHOLD))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_project_env()
    args = parse_args(argv)
    run_weekly_review(
        run_date=args.run_date or None,
        week_start=args.week_start or None,
        data_dir=Path(args.data_dir),
        chart_paths=[Path(path) for path in args.chart_raw] if args.chart_raw else None,
        bitable_url=args.bitable_url,
        skip_bitable=args.no_bitable,
        reviewer_field=args.reviewer_field,
        feishu_webhook=args.feishu_webhook,
        send=not args.no_send,
        material_threshold=max(0, int(args.material_threshold)),
        adoption_threshold=max(0.0, min(1.0, float(args.adoption_threshold))),
        candidate_threshold=max(0, int(args.candidate_threshold)),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
