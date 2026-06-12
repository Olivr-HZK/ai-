#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import html
import json
import sqlite3
import re
from copy import deepcopy
from datetime import date
from pathlib import Path
from typing import Any

from ua_workflows.shared.config import DATA_DIR, REPORTS_DIR, load_project_env
from ua_workflows.shared.llm.client import call_text, flush_usage, resolve_text_model
from ua_workflows.video_enhancer.content_filters import (
    apply_adult_content_filter,
    apply_human_photo_effect_filter,
)
from ua_workflows.video_enhancer.video_content_backfill import (
    fetch_missing_video_content_by_adkeys,
    material_video_content_from_creative,
)

from ua_workflows.shared.db import video_enhancer as ve_db


DEFAULT_OUTPUT_JSON = DATA_DIR / "ve_video_content_filter_eval.json"
DEFAULT_REPORT_HTML = REPORTS_DIR / "ve_video_content_filter_eval_2026-06-08_2026-06-10.html"

NON_PERSON_REASONS = {
    "ecommerce_effect",
    "non_human_photo_effect",
    "missing_human_photo_input",
}
LLM_LABELS = {
    "adult",
    "ecommerce_effect",
    "non_human_photo_effect",
    "missing_human_photo_input",
}


def _coerce_json_like(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return t
    t = re.sub(r"```json", "", t, flags=re.IGNORECASE)
    t = t.replace("```", "")
    return t.strip()


def _snippet(text: Any, max_len: int = 180) -> str:
    value = str(text or "").replace("\n", " ").strip()
    if len(value) <= max_len:
        return value
    return value[: max_len - 3].rstrip() + "..."


def _coerce_text(value: Any) -> str:
    return str(value or "").strip()


def _coerce_material_tags(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return []
        if isinstance(parsed, list):
            return [str(v).strip() for v in parsed if str(v).strip()]
    return []


def _normalize_llm_labels(value: Any) -> list[str]:
    if isinstance(value, str):
        value = [part.strip() for part in value.split(",") if part.strip()]
    elif isinstance(value, (set, tuple)):
        value = list(value)

    labels: list[str] = []
    if not isinstance(value, list):
        return labels

    for item in value:
        label = str(item).strip().lower()
        label = label.replace(" ", "")
        if label in LLM_LABELS:
            if label == "adult":
                labels.append("adult")
            elif label == "ecommerce_effect":
                labels.append("ecommerce_effect")
            elif label == "non_human_photo_effect":
                labels.append("non_human_photo_effect")
            elif label == "missing_human_photo_input":
                labels.append("missing_human_photo_input")
    # dedupe preserve order
    return list(dict.fromkeys(labels))


def _labels_from_llm_reason(reason: str) -> list[str]:
    text = str(reason or "").strip().lower()
    labels: list[str] = []
    if not text:
        return labels
    if any(token in text for token in ("色情", "成人", "性内容", "sexual", "adult")):
        labels.append("adult")
    if any(token in text for token in ("商品", "带货", "电商", "店铺", "ecommerce")):
        labels.append("ecommerce_effect")
    if any(
        token in text
        for token in (
            "非人物",
            "物体",
            "场景",
            "宠物",
            "动物",
            "房间",
            "风景",
            "车辆",
            "食物",
            "纯文字",
            "logo",
        )
    ):
        labels.append("non_human_photo_effect")
    if any(
        token in text
        for token in (
            "未体现用户上传人物照片",
            "未体现上传人物照片",
            "未体现用户上传",
            "未涉及人物照片",
            "缺失人物照片",
        )
    ):
        labels.append("missing_human_photo_input")
    return list(dict.fromkeys(labels))


def _coerce_llm_result(value: Any) -> dict[str, Any]:
    cleaned = _coerce_json_like(str(value or ""))
    if not cleaned:
        return {"is_hard_filtered": False}

    def _attempt(payload: str) -> Any:
        try:
            return json.loads(payload)
        except Exception:
            return None

    parsed = _attempt(cleaned)
    if parsed is None:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end != -1 and end > start:
            parsed = _attempt(cleaned[start : end + 1])
    if parsed is None:
        return {"is_hard_filtered": False, "parse_error": "invalid_json", "raw": cleaned}
    if not isinstance(parsed, dict):
        return {"is_hard_filtered": False, "parse_error": "invalid_json_type", "raw": cleaned}

    reason = str(parsed.get("reason") or "")
    labels = _normalize_llm_labels(parsed.get("labels") or [])
    checks = parsed.get("checks")
    if not isinstance(checks, dict):
        checks = {}
    for label in sorted(LLM_LABELS):
        raw = parsed.get(label)
        if raw is None:
            raw = checks.get(label)
        hit = False
        if isinstance(raw, dict):
            hit = bool(raw.get("hit") or raw.get("is_hit") or raw.get("value"))
        elif isinstance(raw, bool):
            hit = raw
        if hit:
            labels.append(label)
    labels = _normalize_llm_labels(labels)
    reason_fallback_labels: list[str] = []
    if not labels and reason:
        reason_fallback_labels = _labels_from_llm_reason(reason)
        labels = _normalize_llm_labels(reason_fallback_labels)
    payload = {
        "is_hard_filtered": bool(parsed.get("is_hard_filtered") or labels),
        "reason": reason,
        "labels": labels,
        "reason_fallback_labels": reason_fallback_labels,
    }
    payload["raw"] = parsed
    return payload


def _build_llm_user_text(row: dict[str, Any]) -> str:
    video_text = _coerce_text(row.get("guangdada_video_content"))
    if len(video_text) > 3000:
        video_text = f"{video_text[:2990]}..."
    return f"素材ad_key: {_coerce_text(row.get('ad_key'))}\n视频文本:\n{video_text}"


def _classify_with_llm(
    row: dict[str, Any],
    *,
    llm_models: list[str] | None = None,
    llm_timeout: float | None = None,
) -> tuple[set[str], dict[str, Any]]:
    ad_key = str(row.get("ad_key") or "")
    if not ad_key:
        return set(), {}
    system_prompt = (
        "你是 VE 素材业务硬拦截判定助手。只根据给定文本判断是否需要硬拦截。"
        "adult、ecommerce_effect、non_human_photo_effect、missing_human_photo_input 任一命中都必须判为硬拦截。"
        "只返回纯 JSON。"
    )
    user_prompt = (
        "请判断该素材文本是否属于以下四类硬拦截之一。注意：这不是泛安全审核，"
        "missing_human_photo_input 也是业务硬拦截，不要因为不色情/不带货就返回 clean。\n"
        "1. adult：成人/色情/性内容；\n"
        "2. ecommerce_effect：商品/带货/电商导向；\n"
        "3. non_human_photo_effect：非人物素材（宠物/房景/风景/食物/车辆/纯文字/logo/海报等）；\n"
        "4. missing_human_photo_input：未体现用户上传人物照片参与加工。\n"
        "只有文本能明确看出素材是用户上传/使用人物照片、自拍、人像照片进行 AI 加工，且不命中 adult/ecommerce/non_human 时，才返回 clean。\n"
        "请先分别判断四个字段的 true/false，再给最终结果。\n"
        "输出格式：{\"adult\": {\"hit\": bool, \"reason\": \"短句\"}, \"ecommerce_effect\": {\"hit\": bool, \"reason\": \"短句\"}, "
        "\"non_human_photo_effect\": {\"hit\": bool, \"reason\": \"短句\"}, \"missing_human_photo_input\": {\"hit\": bool, \"reason\": \"短句\"}, "
        "\"is_hard_filtered\": bool, \"labels\": [\"adult\"/\"ecommerce_effect\"/\"non_human_photo_effect\"/\"missing_human_photo_input\"], \"reason\": \"中文短句\"}\n"
        "如果四个字段均为 false，才允许 is_hard_filtered=false，labels=[]。"
        "仅输出上面 JSON。\n\n"
        f"{_build_llm_user_text(row)}"
    )
    try:
        raw = call_text(
            system_prompt,
            user_prompt,
            models=llm_models or [resolve_text_model()],
            timeout=llm_timeout,
        )
    except Exception as e:
        return set(), {"ad_key": ad_key, "parse_error": f"llm_call_failed:{e}", "reason": ""}

    parsed = _coerce_llm_result(raw)
    if parsed.get("parse_error"):
        return set(), {"ad_key": ad_key, **parsed, "parse_error": parsed.get("parse_error")}

    if parsed.get("is_hard_filtered"):
        labels = set(parsed.get("labels") or [])
    else:
        labels = set()
    labels = set(_normalize_llm_labels(labels))
    return labels, {
        "ad_key": ad_key,
        "product": str(row.get("product") or ""),
        "labels": sorted(labels),
        "is_hard_filtered": bool(parsed.get("is_hard_filtered")),
        "reason": str(parsed.get("reason") or ""),
        "raw": parsed.get("raw"),
    }
def _load_filter_input_rows(
    *,
    target_dates: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    product: str = "",
    only_excluded: bool = False,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    ve_db.init_db()
    conn = sqlite3.connect(ve_db.DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        where: list[str] = []
        args: list[Any] = []
        if target_dates:
            placeholders = ",".join("?" for _ in target_dates)
            where.append(f"target_date IN ({placeholders})")
            args.extend(target_dates)
        if start_date:
            where.append("target_date >= ?")
            args.append(start_date)
        if end_date:
            where.append("target_date <= ?")
            args.append(end_date)
        if product:
            where.append("product = ?")
            args.append(product)
        if only_excluded:
            where.append("COALESCE(exclude_from_bitable, 0) = 1")
        if not where:
            where.append("1=1")

        sql = f"""
            SELECT target_date, product, appid, ad_key, video_url, preview_img_url,
                   exclude_from_bitable,
                   insight_analysis, raw_json, material_tags, style_filter_match_summary,
                   effect_one_liner, ad_one_liner, play_fingerprint, differentiator,
                   template_fingerprint, play_asset_name, play_asset_classification_reason,
                   guangdada_video_content, heat, impression, all_exposure_value
            FROM daily_creative_insights
            WHERE {' AND '.join(where)}
            ORDER BY target_date DESC, all_exposure_value DESC, ad_key ASC
        """
        if isinstance(limit, int) and limit > 0:
            sql += f" LIMIT {limit}"
        rows = conn.execute(sql, args).fetchall()

        out: list[dict[str, Any]] = []
        for row in rows:
            raw_payload: dict[str, Any] = {}
            raw_json = row["raw_json"]
            if raw_json:
                try:
                    parsed = json.loads(str(raw_json))
                    if isinstance(parsed, dict):
                        raw_payload = parsed
                except Exception:
                    raw_payload = {}

            video_content = _coerce_text(row["guangdada_video_content"])
            video_content_source = "db_guangdada_video_content" if video_content else ""
            if not video_content and raw_payload:
                video_content, video_content_source = material_video_content_from_creative(raw_payload)
                video_content = _coerce_text(video_content)
                video_content_source = _coerce_text(video_content_source)

            out.append(
                {
                    "target_date": _coerce_text(row["target_date"]),
                    "product": _coerce_text(row["product"]),
                    "appid": _coerce_text(row["appid"]),
                    "ad_key": _coerce_text(row["ad_key"]),
                    "video_url": _coerce_text(row["video_url"]),
                    "preview_img_url": _coerce_text(row["preview_img_url"]),
                    "analysis": _coerce_text(row["insight_analysis"]),
                    "style_filter_match_summary": _coerce_text(row["style_filter_match_summary"]),
                    "title": _coerce_text(raw_payload.get("title")),
                    "body": _coerce_text(raw_payload.get("body")),
                    "effect_one_liner": _coerce_text(row["effect_one_liner"]),
                    "ad_one_liner": _coerce_text(row["ad_one_liner"]),
                    "play_fingerprint": _coerce_text(row["play_fingerprint"]),
                    "differentiator": _coerce_text(row["differentiator"]),
                    "template_fingerprint": _coerce_text(row["template_fingerprint"]),
                    "play_asset_name": _coerce_text(row["play_asset_name"]),
                    "play_asset_classification_reason": _coerce_text(row["play_asset_classification_reason"]),
                    "material_tags": _coerce_material_tags(row["material_tags"]),
                    "guangdada_video_content": video_content,
                    "guangdada_video_content_source": video_content_source,
                    "exclude_from_bitable": bool(int(row["exclude_from_bitable"] or 0) == 1),
                    "raw_json": raw_payload,
                    "heat": int(row["heat"] or 0),
                    "impression": int(row["impression"] or 0),
                    "all_exposure_value": int(row["all_exposure_value"] or 0),
                }
            )
        return out
    finally:
        conn.close()


def _ensure_video_content_for_rows(
    rows: list[dict[str, Any]],
    *,
    retries: int = 3,
    debug: bool = False,
    max_scroll_rounds: int = 1,
    direct_first: bool = True,
    skip_search: bool = False,
) -> dict[str, Any]:
    if not rows:
        return {
            "requested": 0,
            "updated": 0,
            "direct": {"requested": 0, "updated": 0, "attempts": 0},
            "search": {"requested": 0, "updated": 0, "attempts": 0},
        }

    missing = [row for row in rows if not str(row.get("guangdada_video_content") or "").strip()]
    if not missing:
        return {
            "requested": 0,
            "updated": 0,
            "direct": {"requested": 0, "updated": 0, "attempts": 0},
            "search": {"requested": 0, "updated": 0, "attempts": 0},
        }

    fetch_rows: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("guangdada_video_content") or "").strip():
            continue
        raw_item = {}
        raw_payload = row.get("raw_json")
        if isinstance(raw_payload, dict):
            raw_item = deepcopy(raw_payload)
            if not isinstance(raw_item.get("creative"), dict):
                raw_item["creative"] = {}
            creative = raw_item.get("creative")
            if not isinstance(creative, dict):
                creative = {}
            raw_item["creative"] = creative
        row.setdefault("raw_item", raw_item)
        raw_item = row.get("raw_item")
        if not isinstance(raw_item, dict):
            raw_item = {}
        creative = raw_item.get("creative")
        if not isinstance(creative, dict):
            creative = {}
        if not isinstance(raw_payload, dict):
            for key in ("app_type", "search_flag", "ads_type"):
                if row.get(key):
                    creative[key] = row.get(key)
        creative["ad_key"] = str(row.get("ad_key") or "")
        if row.get("appid"):
            creative["appid"] = row.get("appid")
        raw_item["creative"] = creative
        row["raw_item"] = raw_item
        if not str(row.get("ad_key") or "").strip():
            continue
        fetch_rows.append(row)

    try:
        summary = asyncio.run(
            fetch_missing_video_content_by_adkeys(
                fetch_rows,
                retries=max(1, int(retries)),
                debug=bool(debug),
                date_range=None,
                max_scroll_rounds=max(1, int(max_scroll_rounds)),
                use_direct=bool(direct_first),
                setup_search=not bool(skip_search),
                skip_time_filter=True,
            )
        )
    except Exception as exc:
        return {
            "requested": 0,
            "updated": 0,
            "error": str(exc),
            "direct": {"requested": 0, "updated": 0, "attempts": 0},
            "search": {"requested": 0, "updated": 0, "attempts": 0},
        }

    direct = summary.get("direct", {}) if isinstance(summary, dict) else {}
    search = summary.get("search", {}) if isinstance(summary, dict) else {}
    return {
        "requested": int(direct.get("requested", 0)) + int(search.get("requested", 0)),
        "updated": int(direct.get("updated", 0)) + int(search.get("updated", 0)),
        "direct": {
            "requested": int(direct.get("requested", 0)),
            "updated": int(direct.get("updated", 0)),
            "attempts": int(direct.get("attempts", 0)),
        },
        "search": {
            "requested": int(search.get("requested", 0)),
            "updated": int(search.get("updated", 0)),
            "attempts": int(search.get("attempts", 0)),
        },
    }


def _run_filter(rows: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    items = [deepcopy(r) for r in rows]
    _, adult_details = apply_adult_content_filter(items)
    _, human_details = apply_human_photo_effect_filter(items)

    adult_by_adkey = {}
    for item in adult_details:
        ad_key = str(item.get("ad_key") or "")
        if ad_key:
            adult_by_adkey[ad_key] = dict(item)
    human_by_adkey = {}
    for item in human_details:
        ad_key = str(item.get("ad_key") or "")
        if ad_key:
            human_by_adkey[ad_key] = dict(item)
    return adult_by_adkey, human_by_adkey


def _run_filter_with_llm(
    rows: list[dict[str, Any]],
    *,
    llm_models: list[str] | None = None,
    llm_timeout: float | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], list[dict[str, Any]], int]:
    adult_by_adkey: dict[str, dict[str, Any]] = {}
    non_person_by_adkey: dict[str, dict[str, Any]] = {}
    details: list[dict[str, Any]] = []
    failures = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        labels, detail = _classify_with_llm(
            deepcopy(row), llm_models=llm_models, llm_timeout=llm_timeout
        )
        ad_key = str(row.get("ad_key") or "")
        if detail.get("parse_error"):
            failures += 1
        if not ad_key or not detail:
            continue
        details.append(detail)
        if "adult" in labels:
            adult_by_adkey[ad_key] = {
                "ad_key": ad_key,
                "product": str(row.get("product") or ""),
                "reason": "adult",
                "is_hard_filtered": True,
            }
        for label in ("ecommerce_effect", "non_human_photo_effect", "missing_human_photo_input"):
            if label in labels:
                non_person_by_adkey[ad_key] = {
                    "ad_key": ad_key,
                    "product": str(row.get("product") or ""),
                    "reason": label,
                    "is_hard_filtered": True,
                }
    return adult_by_adkey, non_person_by_adkey, details, failures


def _inject_video_text(row: dict[str, Any], *, use_full_concat: bool) -> dict[str, Any]:
    out = deepcopy(row)
    video_text = str(row.get("guangdada_video_content") or "").strip()
    if not video_text:
        return out

    out["analysis"] = video_text
    if not use_full_concat:
        return out

    for key in (
        "title",
        "body",
        "style_filter_match_summary",
        "effect_one_liner",
        "ad_one_liner",
        "play_fingerprint",
        "differentiator",
        "template_fingerprint",
        "play_asset_name",
        "play_asset_classification_reason",
    ):
        out[key] = f"{_coerce_text(out.get(key))}\n{video_text}".strip()
    return out


def evaluate_filter_coverage(
    rows: list[dict[str, Any]],
    *,
    with_video_required: bool = True,
    use_full_concat: bool = False,
    use_llm: bool = False,
    llm_model: str | None = None,
    llm_timeout: float | None = None,
    sample_size: int = 20,
) -> dict[str, Any]:
    source_rows = [r for r in rows if not with_video_required or str(r.get("guangdada_video_content") or "").strip()]
    if not source_rows:
        return {
            "summary": {
                "total_rows": len(rows),
                "evaluated_rows": 0,
            },
            "row_details": [],
            "examples": {},
        }

    base_adult, base_human = _run_filter(source_rows)
    video_rows = [_inject_video_text(row, use_full_concat=use_full_concat) for row in source_rows]
    video_llm_failures = 0
    llm_detail_by_ad_key: dict[str, dict[str, Any]] = {}
    if use_llm:
        llm_models = [llm_model] if llm_model else [resolve_text_model()]
        video_adult, video_human, llm_details, video_llm_failures = _run_filter_with_llm(
            video_rows, llm_models=llm_models, llm_timeout=llm_timeout
        )
        for item in llm_details:
            key = str(item.get("ad_key") or "")
            if key:
                llm_detail_by_ad_key[key] = item
    else:
        video_adult, video_human = _run_filter(video_rows)

    base_adult_hits = set(base_adult)
    video_adult_hits = set(video_adult)
    video_human_hits = set(video_human)

    def _reason(details: dict[str, dict[str, Any]], ad_key: str) -> str:
        row = details.get(ad_key, {})
        reason = str(row.get("reason") or "")
        if not reason and "pattern" in row:
            return "pattern"
        return reason

    def _metrics(base_set: set[str], video_set: set[str], *, filter_name: str = "") -> dict[str, Any]:
        tp = sorted(base_set & video_set)
        fn = sorted(base_set - video_set)
        fp = sorted(video_set - base_set)
        precision = len(tp) / len(video_set) if video_set else 0.0
        recall = len(tp) / len(base_set) if base_set else 1.0
        return {
            "filter": filter_name,
            "base_hit": len(base_set),
            "video_hit": len(video_set),
            "true_positive": len(tp),
            "false_negative": len(fn),
            "false_positive": len(fp),
            "precision": round(precision, 6),
            "recall": round(recall, 6),
            "tp": tp[:sample_size],
            "fn": fn[:sample_size],
            "fp": fp[:sample_size],
        }

    base_non_person = {
        k: detail for k, detail in base_human.items() if _reason(base_human, k) in NON_PERSON_REASONS
    }
    video_non_person = {
        k: detail for k, detail in video_human.items() if _reason(video_human, k) in NON_PERSON_REASONS
    }

    row_details: list[dict[str, Any]] = []
    for row in source_rows:
        ad_key = str(row.get("ad_key") or "")
        if not ad_key:
            continue
        video_detail = llm_detail_by_ad_key.get(ad_key, {})
        base_non_person_hit = ad_key in base_non_person
        video_non_person_hit = ad_key in video_non_person

        base_adult_hit = ad_key in base_adult
        video_adult_hit = ad_key in video_adult

        row_details.append(
            {
                "target_date": row.get("target_date"),
                "product": row.get("product"),
                "appid": row.get("appid"),
                "ad_key": ad_key,
                "video_content": row.get("guangdada_video_content") or "",
                "base_adult_hit": base_adult_hit,
                "video_adult_hit": video_adult_hit,
                "base_adult_reason": _reason(base_adult, ad_key),
                "video_adult_reason": _reason(video_adult, ad_key),
                "base_human_reason": _reason(base_human, ad_key),
                "video_human_reason": _reason(video_human, ad_key),
                "base_non_person_hit": base_non_person_hit,
                "video_non_person_hit": video_non_person_hit,
                "base_any_hit": base_adult_hit or base_non_person_hit,
                "video_any_hit": video_adult_hit or video_non_person_hit,
                "video_hard_filters": video_detail.get("labels"),
                "video_hard_parse_error": bool(video_detail.get("parse_error")),
                "video_llm_reason": video_detail.get("reason") or "",
                "video_content_source": row.get("guangdada_video_content_source") or "",
                "analysis_snippet": _snippet(row.get("analysis"), 140),
                "video_snippet": _snippet(row.get("guangdada_video_content"), 140),
                "row_is_excluded": bool(row.get("exclude_from_bitable")),
                "has_video_content": bool(str(row.get("guangdada_video_content") or "").strip()),
            }
        )

    row_details.sort(
        key=lambda item: (
            0 if (item["base_any_hit"] and not item["video_any_hit"]) else 1,
            0 if (not item["base_any_hit"] and item["video_any_hit"]) else 1,
            str(item["target_date"]),
            str(item["ad_key"]),
        )
    )

    adult_metrics = _metrics(base_adult_hits, video_adult_hits, filter_name="adult_content")
    non_person_metrics = _metrics(set(base_non_person), set(video_non_person), filter_name="non_person")
    any_metrics = _metrics(
        base_adult_hits | set(base_non_person),
        video_adult_hits | set(video_non_person),
        filter_name="adult_or_non_person",
    )

    def _by_reason(rows_map: dict[str, dict[str, Any]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for detail in rows_map.values():
            reason = str(detail.get("reason") or "unknown")
            counts[reason] = counts.get(reason, 0) + 1
        return counts

    summary = {
        "total_rows": len(rows),
        "evaluated_rows": len(source_rows),
        "inject_mode": "full_concat" if use_full_concat else "analysis_only",
        "video_filter_mode": "llm" if use_llm else "keyword",
        "video_filter_model": llm_model or resolve_text_model() if use_llm else "",
        "with_video_required": with_video_required,
        "video_llm_failures": video_llm_failures,
        "adult": adult_metrics,
        "non_person": non_person_metrics,
        "adult_or_non_person": any_metrics,
        "base_human_reason_counts": _by_reason(base_human),
        "video_human_reason_counts": _by_reason(video_human),
        "base_non_person_reason_counts": _by_reason(base_non_person),
        "video_non_person_reason_counts": _by_reason(video_non_person),
    }
    return {
        "summary": summary,
        "row_details": row_details,
    }


def render_report(payload: dict[str, Any], *, title: str, generated_at: str = "") -> str:
    summary = payload.get("summary") or {}
    rows = payload.get("row_details") or []

    def esc(value: Any) -> str:
        return html.escape(str(value if value is not None else ""))

    def render_metric_rows(metrics: dict[str, Any], title_text: str) -> str:
        return (
            "<tr>"
            f"<th>{esc(title_text)}</th>"
            f"<td>{esc(metrics.get('base_hit', 0))}</td>"
            f"<td>{esc(metrics.get('video_hit', 0))}</td>"
            f"<td>{esc(metrics.get('true_positive', 0))}</td>"
            f"<td>{esc(metrics.get('false_negative', 0))}</td>"
            f"<td>{esc(metrics.get('false_positive', 0))}</td>"
            f"<td>{metrics.get('recall', 0):.2%}</td>"
            f"<td>{metrics.get('precision', 0):.2%}</td>"
            "</tr>"
        )

    adult = summary.get("adult") or {}
    non_person = summary.get("non_person") or {}
    combo = summary.get("adult_or_non_person") or {}

    row_html = []
    for row in rows:
        if not row:
            continue
        filters = row.get("video_hard_filters") or []
        if isinstance(filters, list):
            filters = ", ".join([str(x) for x in filters if x])
        else:
            filters = str(filters) if filters else ""
        if row.get("video_hard_parse_error"):
            filters = f"{filters} [parse_error]".strip()
        row_html.append(
            "<tr>"
            f"<td>{esc(row.get('target_date'))}</td>"
            f"<td><code>{esc(row.get('ad_key'))}</code></td>"
            f"<td>{'✅' if row.get('row_is_excluded') else '—'}</td>"
            f"<td>{'✅' if row.get('has_video_content') else '—'}</td>"
            f"<td>{'✅' if row.get('base_any_hit') else '—'}</td>"
            f"<td>{'✅' if row.get('video_any_hit') else '—'}</td>"
            f"<td>{esc(row.get('video_content_source'))}</td>"
            f"<td>{esc(row.get('base_human_reason') or row.get('base_adult_reason'))}</td>"
            f"<td>{esc(row.get('video_human_reason') or row.get('video_adult_reason'))}</td>"
            f"<td>{esc(filters)}</td>"
            f"<td>{esc(row.get('video_llm_reason'))}</td>"
            f"<td>{esc(row.get('analysis_snippet'))}</td>"
            f"<td>{esc(row.get('video_content'))}</td>"
            "</tr>"
        )

    return f"""<!doctype html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>{esc(title)}</title>
  <style>
  body {{font-family:-apple-system,BlinkMacSystemFont,\"Segoe UI\",sans-serif; margin:0; color:#111827; background:#f8fafc;}}
  header {{padding:18px 24px; position:sticky; top:0; background:#fff; border-bottom:1px solid #e5e7eb;}}
  main {{padding:20px 24px 40px;}}
  h1 {{margin:0 0 10px; font-size:20px;}}
  .meta {{color:#475569; font-size:13px; margin-bottom:10px;}}
  table {{width:100%; border-collapse:collapse; background:#fff; margin-top:12px; font-size:12px;}}
  th, td {{border:1px solid #e5e7eb; padding:8px; text-align:left; vertical-align:top;}}
  th {{background:#f1f5f9;}}
  code {{background:#f1f5f9; padding:1px 5px; border-radius:5px;}}
  </style>
</head>
<body>
  <header>
    <h1>{esc(title)}</h1>
    <div class=\"meta\">基于 {esc(summary.get('evaluated_rows', 0))} 条有视频内容样本 / 总计 {esc(summary.get('total_rows', 0))}</div>
    <div class=\"meta\">注：video_content 注入模式 {esc(summary.get('inject_mode', 'analysis_only'))}，过滤范围包含 6.1+人像异常规则</div>
    <div class=\"meta\">视频判断模式：{esc(summary.get('video_filter_mode', 'keyword'))}，模型 {esc(summary.get('video_filter_model', ''))}</div>
    <div class=\"meta\">模型解析失败/异常：{esc(summary.get('video_llm_failures', 0))}</div>
    <div class=\"meta\">生成时间：{esc(generated_at)}</div>
  </header>
  <main>
    <section>
      <h2>指标对比</h2>
      <table>
        <thead>
          <tr><th>规则</th><th>旧逻辑命中</th><th>视频提示命中</th><th>交并命中</th><th>漏报</th><th>误报</th><th>召回率</th><th>精确率</th></tr>
        </thead>
        <tbody>
          {render_metric_rows(adult, "成人/色情")}
          {render_metric_rows(non_person, "商品/非人")}
          {render_metric_rows(combo, "成人或商品/非人")}
        </tbody>
      </table>
    </section>
    <section>
      <h2>逐条对比（按漏报/误报优先）</h2>
      <table>
        <thead>
          <tr><th>日期</th><th>ad_key</th><th>旧拦截标记</th><th>视频内容</th><th>旧逻辑拦截</th><th>视频提示拦截</th><th>视频来源</th><th>旧原因</th><th>视频原因</th><th>视频标签</th><th>模型说明</th><th>analysis片段</th><th>video_content</th></tr>
        </thead>
        <tbody>{"".join(row_html)}</tbody>
      </table>
    </section>
  </main>
</body>
</html>"""


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="评估 video_content 对照现网内容过滤拦截规则的覆盖率。")
    today = date.today().isoformat()
    parser.add_argument(
        "--target-date",
        action="append",
        default=[],
        help="指定 target_date（可重复）；不传则按区间或最新记录检索。",
    )
    parser.add_argument("--start-date", help="按 start_date 过滤（含）。")
    parser.add_argument("--end-date", help="按 end_date 过滤（含）。")
    parser.add_argument("--product", default="", help="可选，按 product 过滤。")
    parser.add_argument("--limit", type=int, default=1000, help="读取样本上限（默认 1000）。")
    parser.add_argument(
        "--only-excluded",
        action="store_true",
        help="只抽取本地标记 exclude_from_bitable=1 的素材。",
    )
    parser.add_argument(
        "--fill-missing-video",
        action="store_true",
        help="先尝试回填缺失的 guangdada_video_content（可用于只抽 exclude 的样本）。",
    )
    parser.add_argument(
        "--fill-retries",
        type=int,
        default=3,
        help="回填缺失视频文本最大重试次数（默认 3）。",
    )
    parser.add_argument(
        "--fill-max-scroll-rounds",
        type=int,
        default=1,
        help="回填时单个关键词最大滚动次数（默认 1）。",
    )
    parser.add_argument(
        "--fill-debug",
        action="store_true",
        help="回填视频文本时使用有头浏览器。",
    )
    parser.add_argument(
        "--fill-skip-search",
        action="store_true",
        help="回填时不走关键词搜索兜底。",
    )
    parser.add_argument(
        "--fill-no-direct",
        action="store_true",
        help="回填时不走 direct material-script-analysis，仅走关键词搜索。",
    )
    parser.add_argument(
        "--no-video-only",
        action="store_true",
        help="默认只评估有 guangdada_video_content 的素材，关闭后会包含空 video_content。",
    )
    parser.add_argument(
        "--full-concat",
        action="store_true",
        help="将视频内容拼接到 title/body/effect 等字段；关闭时只覆盖 analysis。",
    )
    parser.add_argument(
        "--use-llm",
        action="store_true",
        help="改为调用大模型判断硬拦截，而非纯规则词匹配。",
    )
    parser.add_argument(
        "--llm-model",
        default="",
        help="可选：指定大模型 model 名称（默认取 OPENROUTER_TEXT_FALLBACK_MODEL）。",
    )
    parser.add_argument(
        "--llm-timeout",
        type=float,
        default=30.0,
        help="LLM 单请求超时（秒），默认 30。",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=20,
        help="报告中每类示例最大展示条数（默认 20）。",
    )
    parser.add_argument(
        "--output-json",
        default=str(DEFAULT_OUTPUT_JSON.with_name(f"ve_video_content_filter_eval_{today}.json")),
        help="JSON 输出路径。",
    )
    parser.add_argument(
        "--report-html",
        default=str(
            DEFAULT_REPORT_HTML.with_name(f"ve_video_content_filter_eval_{today}.html")
        ),
        help="HTML 报告路径。",
    )
    return parser


def run(args: argparse.Namespace) -> dict[str, Any]:
    load_project_env(override=True)
    if args.fill_skip_search and args.fill_no_direct:
        raise SystemExit("fill_skip_search 与 fill_no_direct 不能同时开启（至少保留一种抓取路径）。")
    target_dates = [str(v).strip() for v in (args.target_date or []) if str(v).strip()]
    rows = _load_filter_input_rows(
        target_dates=target_dates,
        start_date=str(args.start_date or "").strip() or None,
        end_date=str(args.end_date or "").strip() or None,
        product=str(args.product).strip(),
        only_excluded=bool(args.only_excluded),
        limit=max(0, int(args.limit or 0)),
    )
    if not rows:
        raise SystemExit("[结果] 未检索到素材，检查日期/产品条件。")

    backfill_summary: dict[str, Any] = {}
    if args.fill_missing_video:
        backfill_summary = _ensure_video_content_for_rows(
            rows,
            retries=max(1, int(args.fill_retries or 0)),
            debug=bool(args.fill_debug),
            max_scroll_rounds=max(1, int(args.fill_max_scroll_rounds or 0)),
            direct_first=not bool(args.fill_no_direct),
            skip_search=bool(args.fill_skip_search),
        )

    payload = evaluate_filter_coverage(
        rows,
        with_video_required=False if args.fill_missing_video else not bool(args.no_video_only),
        use_full_concat=bool(args.full_concat),
        use_llm=bool(args.use_llm),
        llm_model=str(args.llm_model or "").strip() or None,
        llm_timeout=max(1.0, float(args.llm_timeout)) if args.llm_timeout else None,
        sample_size=max(1, int(args.sample_size or 20)),
    )
    payload["scope"] = {
        "target_dates": sorted(set(target_dates)),
        "start_date": str(args.start_date or ""),
        "end_date": str(args.end_date or ""),
        "product": str(args.product or ""),
        "limit": max(0, int(args.limit or 0)),
        "only_excluded": bool(args.only_excluded),
        "fill_missing_video": bool(args.fill_missing_video),
    }
    if backfill_summary:
        payload["backfill_summary"] = backfill_summary

    Path(args.output_json).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output_json).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    title_parts = ["视频内容提示拦截评估"]
    if payload["scope"]["product"]:
        title_parts.append(payload["scope"]["product"])
    if target_dates:
        title_parts.append(" / ".join(target_dates))
    elif args.start_date or args.end_date:
        title_parts.append(f"{args.start_date or ''}~{args.end_date or ''}".strip("~"))

    report_html = render_report(
        payload,
        title=" | ".join(title_parts),
        generated_at=date.today().isoformat(),
    )
    Path(args.report_html).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report_html).write_text(report_html, encoding="utf-8")

    payload["output_json"] = str(args.output_json)
    payload["report_html"] = str(args.report_html)
    if args.use_llm:
        flush_usage(date.today().isoformat())
    return payload


def main() -> None:
    args = build_arg_parser().parse_args()
    payload = run(args)
    print(json.dumps({
        "summary": payload.get("summary"),
        "output_json": payload.get("output_json"),
        "report_html": payload.get("report_html"),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
