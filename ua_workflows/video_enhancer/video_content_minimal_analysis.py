"""Minimal VE analysis rows from Guangdada video-content text.

This is the lightweight replacement for the old multimodal analysis when the
main table only needs a successful non-empty analysis plus a readable selling
point. Dedupe and hard-block decisions should already be applied before this
module runs.
"""

from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable

from ua_workflows.shared.llm.client import call_text
from ua_workflows.video_enhancer.video_content_backfill import material_video_content_from_creative


def _coerce_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _clean_json_text(text: str) -> str:
    cleaned = _coerce_text(text)
    cleaned = re.sub(r"```json", "", cleaned, flags=re.IGNORECASE)
    return cleaned.replace("```", "").strip()


def _parse_json_object(text: str) -> dict[str, Any]:
    cleaned = _clean_json_text(text)
    candidates = [cleaned]
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start >= 0 and end > start:
        candidates.append(cleaned[start : end + 1])
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except Exception:
            continue
        if isinstance(parsed, dict):
            return parsed
    return {}


def _clip(value: Any, limit: int = 1200) -> str:
    text = _coerce_text(value).replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _fallback_effect(row: dict[str, Any]) -> str:
    for value in (
        row.get("guangdada_video_content"),
        row.get("analysis"),
        row.get("title"),
        row.get("body"),
    ):
        text = _coerce_text(value)
        if text:
            text = re.split(r"[。！？!?；;\n]", text, maxsplit=1)[0].strip()
            return text[:36]
    return "视频内容待人工复核"


def _fallback_analysis(row: dict[str, Any], effect: str = "") -> str:
    video_content = _coerce_text(row.get("guangdada_video_content"))
    title = _coerce_text(row.get("title"))
    body = _coerce_text(row.get("body"))
    parts = []
    if effect:
        parts.append(f"核心卖点：{effect}")
    if video_content:
        parts.append(f"视频内容：{video_content}")
    if title:
        parts.append(f"标题：{title}")
    if body:
        parts.append(f"正文：{body}")
    if not parts:
        parts.append("视频内容：待人工复核")
    return "\n".join(parts)


def _excluded_analysis(row: dict[str, Any]) -> str:
    match = row.get("llm_video_content_filter_match")
    if not isinstance(match, dict):
        match = {}
    decision = _coerce_text(match.get("final_decision")) or "已排除"
    reason = (
        _coerce_text(match.get("business_reason"))
        or _coerce_text(match.get("duplicate_reason"))
        or _coerce_text(row.get("style_filter_match_summary"))
        or "命中同步前筛选"
    )
    match_key = _coerce_text(match.get("match_ad_key"))
    match_date = _coerce_text(match.get("match_date"))
    tail = f"；命中素材：{match_date} {match_key}".strip() if match_key else ""
    return f"LLM视频内容筛选：{decision}；原因：{reason}{tail}"


def seed_analysis_results_from_raw(raw_payload: dict[str, Any], *, target_date: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in raw_payload.get("items") or []:
        if not isinstance(item, dict):
            continue
        creative = item.get("creative") or {}
        if not isinstance(creative, dict):
            continue
        ad_key = _coerce_text(creative.get("ad_key"))
        if not ad_key:
            continue
        video_content, _source = material_video_content_from_creative(creative)
        rows.append(
            {
                "category": item.get("category"),
                "product": item.get("product"),
                "appid": item.get("appid"),
                "ad_key": ad_key,
                "creative_type": "video" if _coerce_text(creative.get("video_url")) else "image",
                "platform": creative.get("platform"),
                "video_duration": creative.get("video_duration"),
                "all_exposure_value": creative.get("all_exposure_value"),
                "heat": creative.get("heat"),
                "impression": creative.get("impression"),
                "video_url": creative.get("video_url") or "",
                "image_url": creative.get("image_url") or "",
                "preview_img_url": creative.get("preview_img_url") or "",
                "title": creative.get("title") or "",
                "body": creative.get("body") or "",
                "target_date": target_date,
                "pipeline_tags": creative.get("pipeline_tags")
                if isinstance(creative.get("pipeline_tags"), list)
                else [],
                "guangdada_video_content": video_content,
                "analysis": "",
                "inspiration_enrichment": "video_content_minimal",
                "style_filter_match_summary": "",
                "material_tags": [],
                "ad_one_liner": "",
                "play_one_liner": "",
                "hook_one_liner": "",
                "voiceover_script": "",
                "ad_breakdown": {},
                "risk_level": "",
                "effect_one_liner": "",
                "play_fingerprint": "",
                "differentiator": "",
                "template_fingerprint": "",
                "play_asset_id": "",
                "play_asset_name": "",
                "play_asset_subtag_ids": "",
                "play_asset_subtag_names": "",
                "play_asset_novelty_label": "",
                "play_asset_classification_reason": "",
                "play_asset_match_source": "",
                "exclude_from_bitable": False,
                "exclude_from_cluster": False,
            }
        )
    return rows


def _system_prompt() -> str:
    return (
        "你是 VE 多维表主表字段整理助手。只根据给定的广告标题、正文和视频内容文本，"
        "生成主表可读的极简分析字段。不要做去重判断，不要输出投放建议。只返回纯 JSON。"
    )


def _user_prompt(row: dict[str, Any]) -> str:
    return f"""
请为这条素材生成多维表主表所需的极简字段。

素材：
- 产品：{row.get('product') or ''}
- ad_key：{row.get('ad_key') or ''}
- 标题：{_clip(row.get('title'), 260) or '无'}
- 正文：{_clip(row.get('body'), 420) or '无'}
- 视频内容：{_clip(row.get('guangdada_video_content'), 1200) or '无'}

输出 JSON：
{{
  "analysis": "2-4行中文。第一行以「核心卖点：」开头；后续可写「视频内容：」「可复核点：」。不要编造看不到的画面。",
  "effect_one_liner": "一句10-24字中文，概括素材最值得借鉴的效果或展现形式",
  "risk_level": "低风险|中风险|高风险"
}}
""".strip()


def generate_minimal_analysis(row: dict[str, Any], *, model: str, timeout: float) -> dict[str, Any]:
    raw = call_text(
        _system_prompt(),
        _user_prompt(row),
        models=[model],
        timeout=timeout,
    )
    parsed = _parse_json_object(raw)
    effect = _coerce_text(parsed.get("effect_one_liner")) or _fallback_effect(row)
    analysis = _coerce_text(parsed.get("analysis")) or _fallback_analysis(row, effect)
    risk = _coerce_text(parsed.get("risk_level"))
    if risk not in {"低风险", "中风险", "高风险"}:
        risk = "低风险"
    return {
        "analysis": analysis,
        "effect_one_liner": effect[:80],
        "risk_level": risk,
    }


GenerateFn = Callable[..., dict[str, Any]]


def _apply_generated(row: dict[str, Any], generated: dict[str, Any]) -> bool:
    effect = _coerce_text(generated.get("effect_one_liner")) or _fallback_effect(row)
    analysis = _coerce_text(generated.get("analysis")) or _fallback_analysis(row, effect)
    row["effect_one_liner"] = effect[:80]
    row["analysis"] = analysis
    risk = _coerce_text(generated.get("risk_level"))
    if risk in {"低风险", "中风险", "高风险"}:
        row["risk_level"] = risk
    elif not _coerce_text(row.get("risk_level")):
        row["risk_level"] = "低风险"
    row["inspiration_enrichment"] = "video_content_minimal"
    return True


def apply_minimal_analysis_to_results(
    rows: list[dict[str, Any]],
    *,
    model: str,
    timeout: float,
    max_workers: int,
    generate_fn: GenerateFn = generate_minimal_analysis,
) -> dict[str, int]:
    summary = {
        "total": len(rows),
        "excluded": 0,
        "requested": 0,
        "generated": 0,
        "fallback": 0,
        "failed": 0,
    }
    kept: list[dict[str, Any]] = []
    for row in rows:
        if row.get("exclude_from_bitable"):
            summary["excluded"] += 1
            row["analysis"] = _excluded_analysis(row)
            row["effect_one_liner"] = ""
            row["inspiration_enrichment"] = "llm_video_content_filtered"
            continue
        kept.append(row)

    if not kept:
        return summary

    workers = max(1, min(int(max_workers or 1), len(kept)))
    summary["requested"] = len(kept)
    if workers == 1:
        for row in kept:
            try:
                generated = generate_fn(row, model=model, timeout=timeout)
                _apply_generated(row, generated)
                summary["generated"] += 1
            except Exception:
                _apply_generated(row, {
                    "analysis": _fallback_analysis(row, _fallback_effect(row)),
                    "effect_one_liner": _fallback_effect(row),
                    "risk_level": "低风险",
                })
                summary["fallback"] += 1
        return summary

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(generate_fn, row, model=model, timeout=timeout): row
            for row in kept
        }
        for future in as_completed(futures):
            row = futures[future]
            try:
                generated = future.result()
                _apply_generated(row, generated)
                summary["generated"] += 1
            except Exception:
                _apply_generated(row, {
                    "analysis": _fallback_analysis(row, _fallback_effect(row)),
                    "effect_one_liner": _fallback_effect(row),
                    "risk_level": "低风险",
                })
                summary["fallback"] += 1
    return summary
