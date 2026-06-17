#!/usr/bin/env python3
"""Generate Haopeng second-pass AI Top-N recommendation reports."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from ua_workflows.shared.config import DATA_DIR, load_project_env
from ua_workflows.shared.llm.client import call_text, flush_usage
from ua_workflows.video_enhancer.feedback_training import cell_to_text, fetch_bitable_records
from ua_workflows.video_enhancer.feedback_rating import (
    RATING_LABELS,
    legacy_status_to_rating,
    normalize_rating_value,
    resolve_feedback_rating,
)


DEFAULT_MODEL = "qwen/qwen3.7-max"
DEFAULT_HISTORY_START_DATE = "2026-05-25"
DEFAULT_OUTPUT_DIR = DATA_DIR / "haopeng_topn_experiments"
POSITIVE_STATUSES = {"采纳", "接受", "入素材库"}
NEGATIVE_STATUSES = {"不采纳", "删除", "拒绝", "重复抓取"}
DECISIVE_STATUSES = POSITIVE_STATUSES | NEGATIVE_STATUSES
GENERIC_PLAY_LABELS = {"", "未命中", "unmatched_play", "新玩法候选"}
EXCLUDED_TOPN_PLATFORMS = {"admob", "youtube"}


def today_shanghai() -> str:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).strftime("%Y-%m-%d")


def yesterday_shanghai() -> str:
    today = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).date()
    return (today - dt.timedelta(days=1)).isoformat()


def previous_date(value: str) -> str:
    return (dt.date.fromisoformat(value) - dt.timedelta(days=1)).isoformat()


def clamp(value: Any, limit: int) -> str:
    text = str(value or "").replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "..."


def _first_text(fields: dict[str, Any], names: tuple[str, ...]) -> str:
    for name in names:
        text = cell_to_text(fields.get(name)).strip()
        if text:
            return text
    return ""


def _normalize_date_value(value: Any) -> str:
    text = cell_to_text(value).strip()
    if not text:
        return ""
    match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    if match:
        return match.group(0)
    if re.fullmatch(r"\d{11,13}", text):
        try:
            stamp = int(text)
            if stamp > 10_000_000_000:
                stamp = stamp // 1000
            return dt.datetime.fromtimestamp(stamp, dt.timezone(dt.timedelta(hours=8))).date().isoformat()
        except (OverflowError, OSError, ValueError):
            return text
    return text


def _normalize_platform(value: Any) -> str:
    return str(value or "").strip().lower()


def is_excluded_topn_platform(value: Any) -> bool:
    return _normalize_platform(value) in EXCLUDED_TOPN_PLATFORMS


def _rating_field_for_reviewer_field(reviewer_field: str) -> str:
    text = str(reviewer_field or "").strip()
    if text.endswith("接受情况") and text != "接受情况":
        return text[: -len("接受情况")] + "评分"
    return "浩鹏评分"


def _row_rating(row: dict[str, Any]) -> int | None:
    rating = normalize_rating_value(row.get("rating"))
    if rating is not None:
        return rating
    rating = normalize_rating_value(row.get("rating_label"))
    if rating is not None:
        return rating
    return legacy_status_to_rating(row.get("actual_hp"))


def _with_rating(row: dict[str, Any]) -> dict[str, Any]:
    rating = _row_rating(row)
    out = dict(row)
    if rating is not None:
        out["rating"] = rating
        out["rating_label"] = out.get("rating_label") or RATING_LABELS.get(rating, "")
    return out


def normalize_bitable_record(record: dict[str, Any], *, reviewer_field: str = "浩鹏接受情况") -> dict[str, Any]:
    fields = record.get("fields") or {}
    if not isinstance(fields, dict):
        fields = {}
    feedback = resolve_feedback_rating(
        fields,
        reviewer="haopeng",
        rating_field=_rating_field_for_reviewer_field(reviewer_field),
        status_field=reviewer_field,
    )
    ad_key = _first_text(fields, ("广告ID", "ad_key")) or str(record.get("record_id") or record.get("id") or "")
    date_value = fields.get("抓取日期")
    if date_value in (None, ""):
        date_value = fields.get("日期")
    row = {
        "record_id": str(record.get("record_id") or record.get("id") or ""),
        "ad_key": ad_key,
        "product": _first_text(fields, ("产品", "product", "广告主")),
        "platform": _first_text(fields, ("平台", "platform")),
        "date": _normalize_date_value(date_value),
        "core": _first_text(fields, ("核心卖点", "玩法指纹", "AI分析结果", "标题")),
        "play_label": _first_text(fields, ("玩法", "玩法资产", "玩法指纹")),
        "actual_hp": _first_text(fields, (reviewer_field, "接受情况")),
        "rating": feedback.rating,
        "rating_label": feedback.rating_label,
        "rating_source_field": feedback.source_field,
        "rating_source_value": feedback.source_value,
        "rating_invalid": feedback.invalid_rating,
        "video_url": _first_text(fields, ("视频链接", "视频")),
        "cover_url": _first_text(fields, ("封面图链接", "封面图")),
        "title": _first_text(fields, ("标题", "素材标题")),
        "hook": _first_text(fields, ("Hook解析", "开场钩子")),
        "script_or_voiceover": _first_text(fields, ("脚本/口播", "旁白")),
        "material_tags": _first_text(fields, ("素材标签",)),
        "risk_level": _first_text(fields, ("风险等级",)),
    }
    if not row["product"]:
        row["product"] = "未知产品"
    return row


def fetch_rows_from_bitable(bitable_url: str, *, reviewer_field: str = "浩鹏接受情况") -> list[dict[str, Any]]:
    return [
        normalize_bitable_record(record, reviewer_field=reviewer_field)
        for record in fetch_bitable_records(bitable_url)
    ]


def _history_payload(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "ad_key": row.get("ad_key", ""),
            "date": row.get("date", ""),
            "status": row.get("actual_hp", ""),
            "rating": row.get("rating"),
            "rating_label": row.get("rating_label", ""),
            "platform": row.get("platform", ""),
            "core": clamp(row.get("core"), 140),
            "play_label": row.get("play_label", ""),
            "hook": clamp(row.get("hook"), 100),
        }
        for row in rows
    ]


def _candidate_payload(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "ad_key": row.get("ad_key", ""),
            "platform": row.get("platform", ""),
            "core": clamp(row.get("core"), 180),
            "play_label": row.get("play_label", ""),
            "cover_url": row.get("cover_url", ""),
            "video_url": row.get("video_url", ""),
            "hook": clamp(row.get("hook"), 120),
            "script_or_voiceover": clamp(row.get("script_or_voiceover"), 120),
            "material_tags": clamp(row.get("material_tags"), 120),
            "risk_level": row.get("risk_level", ""),
        }
        for row in rows
    ]


def build_ai_prompt(
    *,
    product: str,
    target_date: str,
    history: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> str:
    return f"""你是 Video Enhancer 竞品素材的二次筛选助手。你的任务不是拦截主流程，而是从当天素材中挑出最可能被浩鹏采纳的推荐素材。

浩鹏评分定义：
- 5星=强烈值得复刻/制作；APP 内部还未上线该类型内容，或上线到 APP 模板后对付费意愿提高有明显价值、好玩、可能产生付费。
- 4星=有明显参考价值；可优先作为素材库或变体方向观察。
- 3星=中等参考价值；方向可观察，但不等同于强正样本。
- 2星=低优先级；价值有限或适配度较弱。
- 1星=低价值或重复/不适配；App 端内已有、之前制作素材投放过、重复抓取，或与 App 受众完全不符。
- 待定/空值不作为正负样本。

判断要求：
- 只根据输入中的核心卖点、玩法标签、Hook/脚本、素材标签、封面/视频链接和历史浩鹏评分偏好判断。
- 优先推荐与历史 5 星相似但不重复、且具有新模板画面或新片段价值的素材。
- 历史 4 星是较强参考，历史 3 星只说明方向可观察，不能当作强正样本；历史 1 星相似素材要降分。
- 命中已有明确玩法标签时，不要标成纯新玩法，应视为老玩法新变体或重复低价值。
- 没有明确玩法标签、且历史没有同款具体玩法时，可以标成新玩法候选。
- 与历史 1 星高度同款、受众不符、或无新模板/新场景/新机制时，降分。
- 生产推送不展示回测字段，reason 要短、能解释为什么值得推。

产品：{product}
目标日期：{target_date}
历史浩鹏评分偏好：{json.dumps(history, ensure_ascii=False)}
当天候选素材：{json.dumps(candidates, ensure_ascii=False)}

只输出 JSON 数组，每个候选一个对象：
{{"ad_key":"...","accept_score":0-100,"confidence":"high|medium|low","recommend":"push|hold","matched_play_label":"新玩法候选或已有玩法标签","play_name":"短玩法名","reason":"推荐或不推荐理由"}}"""


def _extract_json_array(text: str) -> list[Any]:
    raw = str(text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw).strip()
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("[")
        end = raw.rfind("]")
        if start < 0 or end <= start:
            raise
        value = json.loads(raw[start : end + 1])
    if isinstance(value, list):
        return value
    if isinstance(value, dict) and isinstance(value.get("results"), list):
        return list(value["results"])
    raise ValueError("AI response is not a JSON array")


def call_ai_json(
    *,
    product: str,
    target_date: str,
    history: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    model: str,
) -> list[dict[str, Any]]:
    content = call_text(
        "你只输出合法 JSON，不要输出 Markdown 或解释。",
        build_ai_prompt(
            product=product,
            target_date=target_date,
            history=history,
            candidates=candidates,
        ),
        models=[model],
    )
    parsed = _extract_json_array(content)
    return [x for x in parsed if isinstance(x, dict)]


def _normalize_score(value: Any) -> int:
    try:
        score = int(round(float(value)))
    except (TypeError, ValueError):
        score = 50
    return max(0, min(100, score))


def _normalize_decisions(candidates: list[dict[str, Any]], raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key = {
        str(item.get("ad_key") or "").strip(): item
        for item in raw
        if str(item.get("ad_key") or "").strip()
    }
    out: list[dict[str, Any]] = []
    for row in candidates:
        ad_key = str(row.get("ad_key") or "").strip()
        item = by_key.get(ad_key) or {}
        play_label = str(row.get("play_label") or "").strip()
        matched = str(item.get("matched_play_label") or "").strip()
        if not matched:
            matched = play_label if play_label else "新玩法候选"
        if matched in GENERIC_PLAY_LABELS and play_label:
            matched = play_label
        recommend = str(item.get("recommend") or "").strip().lower()
        score = _normalize_score(item.get("accept_score"))
        if recommend not in {"push", "hold"}:
            recommend = "push" if score >= 70 else "hold"
        out.append(
            {
                **row,
                "accept_score": score,
                "confidence": str(item.get("confidence") or "medium").strip() or "medium",
                "recommend": recommend,
                "matched_play_label": matched,
                "play_name": str(item.get("play_name") or matched or play_label or "待确认").strip(),
                "reason": str(item.get("reason") or "模型未返回理由，按中性分保留排序。").strip(),
            }
        )
    return out


def summarize_topn(rows: list[dict[str, Any]], *, top_n: int = 10) -> dict[str, Any]:
    top = rows[:top_n]
    actual_counts = Counter(str(row.get("actual_hp") or "<空>") for row in top)
    accepted = sum(1 for row in top if str(row.get("actual_hp") or "") in {"采纳", "接受"})
    library = sum(1 for row in top if str(row.get("actual_hp") or "") == "入素材库")
    return {
        "candidate_count": len(rows),
        "top_n": top_n,
        "accepted": accepted,
        "accepted_or_library": accepted + library,
        "actual_counts": dict(actual_counts),
    }


def build_report_from_rows(
    rows: list[dict[str, Any]],
    *,
    target_date: str,
    model: str = DEFAULT_MODEL,
    history_start_date: str = DEFAULT_HISTORY_START_DATE,
    reviewer_field: str = "浩鹏接受情况",
) -> dict[str, Any]:
    history_end = previous_date(target_date)
    history = [
        _with_rating(row)
        for row in rows
        if history_start_date <= str(row.get("date") or "") <= history_end
        and _row_rating(row) in {1, 2, 3, 4, 5}
        and str(row.get("core") or "").strip()
    ]
    target_rows = [
        row
        for row in rows
        if str(row.get("date") or "") == target_date
        and str(row.get("core") or "").strip()
        and str(row.get("ad_key") or "").strip()
    ]
    excluded_platform_counts = Counter(
        _normalize_platform(row.get("platform"))
        for row in target_rows
        if is_excluded_topn_platform(row.get("platform"))
    )
    candidates = [
        row
        for row in target_rows
        if not is_excluded_topn_platform(row.get("platform"))
    ]

    grouped: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: {"history": [], "candidates": []})
    for row in history:
        grouped[str(row.get("product") or "未知产品")]["history"].append(row)
    for row in candidates:
        grouped[str(row.get("product") or "未知产品")]["candidates"].append(row)

    results: list[dict[str, Any]] = []
    for product in sorted(grouped):
        product_candidates = grouped[product]["candidates"]
        if not product_candidates:
            continue
        product_history = grouped[product]["history"]
        if product_history:
            raw = call_ai_json(
                product=product,
                target_date=target_date,
                history=_history_payload(product_history),
                candidates=_candidate_payload(product_candidates),
                model=model,
            )
            results.extend(_normalize_decisions(product_candidates, raw))
        else:
            results.extend(
                _normalize_decisions(
                    product_candidates,
                    [
                        {
                            "ad_key": row.get("ad_key"),
                            "accept_score": 75,
                            "confidence": "medium",
                            "recommend": "push",
                            "matched_play_label": str(row.get("play_label") or "新玩法候选"),
                            "play_name": str(row.get("play_label") or "新玩法候选"),
                            "reason": "同产品历史有效反馈为空，先作为候选推送观察。",
                        }
                        for row in product_candidates
                    ],
                )
            )

    results.sort(
        key=lambda row: (
            1 if str(row.get("recommend") or "") == "push" else 0,
            int(row.get("accept_score") or 0),
            str(row.get("ad_key") or ""),
        ),
        reverse=True,
    )
    status_counts = Counter(str(row.get("actual_hp") or "<空>") for row in history)
    rating_counts = Counter(str(row.get("rating_label") or "<未评分>") for row in history)
    report = {
        "name": "label_prior",
        "payload_kind": "label_prior",
        "target_date": target_date,
        "history_window": f"{history_start_date}..{history_end}",
        "history_start_date": history_start_date,
        "history_end_date": history_end,
        "reviewer_field": reviewer_field,
        "model": model,
        "history_effective_count": len(history),
        "history_rating_counts": dict(rating_counts),
        "target_candidate_count_before_platform_filter": len(target_rows),
        "target_candidate_count": len(candidates),
        "excluded_platforms": sorted(EXCLUDED_TOPN_PLATFORMS),
        "excluded_platform_counts": dict(sorted(excluded_platform_counts.items())),
        "history_status_counts": dict(status_counts),
        "summary": {
            "top10": summarize_topn(results, top_n=10),
            "top20": summarize_topn(results, top_n=20),
        },
        "results": results,
    }
    return report


def write_report(report: dict[str, Any], *, output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    target_date = str(report.get("target_date") or today_shanghai())
    path = output_dir / f"{target_date}_label_prior.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def generate_report_from_bitable(
    *,
    bitable_url: str,
    target_date: str,
    model: str = DEFAULT_MODEL,
    history_start_date: str = DEFAULT_HISTORY_START_DATE,
    reviewer_field: str = "浩鹏接受情况",
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> tuple[dict[str, Any], Path]:
    rows = fetch_rows_from_bitable(bitable_url, reviewer_field=reviewer_field)
    report = build_report_from_rows(
        rows,
        target_date=target_date,
        model=model,
        history_start_date=history_start_date,
        reviewer_field=reviewer_field,
    )
    path = write_report(report, output_dir=output_dir)
    try:
        flush_usage(target_date)
    except Exception as exc:
        print(f"[haopeng-ai-filter] flush_usage skipped: {exc}")
    return report, path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="生成 VE 浩鹏二次 AI 筛选 TopN JSON")
    parser.add_argument("--date", default=yesterday_shanghai(), help="目标日期 YYYY-MM-DD，默认昨天")
    parser.add_argument("--bitable-url", default="", help="VE 主多维表 URL，默认 VIDEO_ENHANCER_BITABLE_URL")
    parser.add_argument("--reviewer-field", default="浩鹏接受情况")
    parser.add_argument("--model", default=(os.getenv("VE_HAOPENG_FILTER_MODEL") or DEFAULT_MODEL).strip())
    parser.add_argument(
        "--history-start-date",
        default=(os.getenv("VE_HAOPENG_HISTORY_START_DATE") or DEFAULT_HISTORY_START_DATE).strip(),
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser.parse_args()


def main() -> None:
    load_project_env()
    args = parse_args()
    bitable_url = (args.bitable_url or os.getenv("VIDEO_ENHANCER_BITABLE_URL") or "").strip()
    if not bitable_url:
        raise SystemExit("请配置 VIDEO_ENHANCER_BITABLE_URL 或传入 --bitable-url")
    report, path = generate_report_from_bitable(
        bitable_url=bitable_url,
        target_date=args.date,
        model=args.model,
        history_start_date=args.history_start_date,
        reviewer_field=args.reviewer_field,
        output_dir=Path(args.output_dir),
    )
    print(
        f"[haopeng-ai-filter] wrote {path} candidates={report.get('target_candidate_count', 0)} "
        f"history={report.get('history_effective_count', 0)}"
    )


if __name__ == "__main__":
    main()
