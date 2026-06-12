"""LLM filtering from Guangdada video-content text for VE materials.

This stage is intentionally downstream of cover dedupe: it spends model calls
only on materials that still have a chance to enter the main bitable.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from ua_workflows.shared.config import DATA_DIR
from ua_workflows.shared.db.video_enhancer import DB_PATH, init_db
from ua_workflows.shared.llm.client import call_text
from ua_workflows.video_enhancer.video_content_backfill import material_video_content_from_creative


BUSINESS_LABELS = {
    "adult",
    "ecommerce_effect",
    "non_human_photo_effect",
    "missing_human_photo_input",
}

FINAL_ORDER = ["业务硬拦", "历史重复", "日内重复", "保留"]


def _coerce_text(value: Any) -> str:
    return str(value or "").strip()


def _clip(value: Any, limit: int = 700) -> str:
    text = _coerce_text(value).replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _loads_json(value: Any) -> Any:
    if not value:
        return {}
    try:
        return json.loads(str(value))
    except Exception:
        return {}


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(_coerce_text(value))
    except ValueError:
        return None


def _window_start(target_date: str, history_days: int) -> str:
    parsed = _parse_date(target_date)
    if not parsed:
        return target_date
    return (parsed - timedelta(days=max(1, int(history_days)))).isoformat()


def _creative_from_raw(raw_payload: Any) -> dict[str, Any]:
    if not isinstance(raw_payload, dict):
        return {}
    creative = raw_payload.get("creative")
    if isinstance(creative, dict):
        return creative
    return raw_payload


def _advertiser_id_from_raw(raw_payload: Any, fallback: str = "") -> str:
    raw = raw_payload if isinstance(raw_payload, dict) else {}
    creative = _creative_from_raw(raw)
    return _coerce_text(
        raw.get("advertiser_id")
        or creative.get("advertiser_id")
        or raw.get("advertiser_name")
        or creative.get("advertiser_name")
        or fallback
    )


def _advertiser_name_from_raw(raw_payload: Any, fallback: str = "") -> str:
    raw = raw_payload if isinstance(raw_payload, dict) else {}
    creative = _creative_from_raw(raw)
    return _coerce_text(
        raw.get("advertiser_name")
        or creative.get("advertiser_name")
        or fallback
    )


def _record_for_prompt(row: dict[str, Any], *, prefix: str, index: int) -> dict[str, Any]:
    return {
        "id": f"{prefix}{index}",
        "date": row.get("target_date"),
        "ad_key": row.get("ad_key"),
        "exposure": row.get("all_exposure_value"),
        "title": _clip(row.get("title"), 90),
        "body": _clip(row.get("body"), 120),
        "video_content": _clip(row.get("video_content") or row.get("analysis"), 420),
    }


def _clean_json_text(text: str) -> str:
    cleaned = _coerce_text(text)
    cleaned = re.sub(r"```json", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.replace("```", "").strip()
    return cleaned


def _parse_results(text: str) -> list[dict[str, Any]]:
    cleaned = _clean_json_text(text)
    if not cleaned:
        return []
    candidates = [cleaned]
    start_obj = cleaned.find("{")
    end_obj = cleaned.rfind("}")
    if start_obj >= 0 and end_obj > start_obj:
        candidates.append(cleaned[start_obj : end_obj + 1])
    start_arr = cleaned.find("[")
    end_arr = cleaned.rfind("]")
    if start_arr >= 0 and end_arr > start_arr:
        candidates.append(cleaned[start_arr : end_arr + 1])

    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except Exception:
            continue
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
        if isinstance(parsed, dict):
            results = parsed.get("results")
            if isinstance(results, list):
                return [item for item in results if isinstance(item, dict)]
            if parsed.get("ad_key"):
                return [parsed]
    return []


def _normalize_labels(value: Any) -> list[str]:
    if isinstance(value, str):
        value = [part.strip() for part in value.split(",") if part.strip()]
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        label = _coerce_text(item).replace(" ", "").lower()
        if label in BUSINESS_LABELS and label not in out:
            out.append(label)
    return out


def _normalize_result(item: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]:
    labels = _normalize_labels(item.get("business_labels") or item.get("hard_labels") or item.get("labels"))
    hard = bool(item.get("business_hard_block") or item.get("hard_filter") or item.get("is_hard_filtered") or labels)

    duplicate_type = _coerce_text(item.get("duplicate_type") or "none").lower()
    if duplicate_type in {"cross_day", "history_duplicate", "historical"}:
        duplicate_type = "history"
    if duplicate_type in {"same_day", "same-day", "intraday_duplicate"}:
        duplicate_type = "intraday"
    if duplicate_type not in {"history", "intraday"}:
        duplicate_type = "none"

    confidence = _coerce_text(item.get("duplicate_confidence") or item.get("confidence") or "").lower()
    if confidence not in {"high", "medium", "low"}:
        confidence = "none" if duplicate_type == "none" else "medium"

    return {
        "ad_key": _coerce_text(item.get("ad_key") or target.get("ad_key")),
        "business_hard_block": hard,
        "business_labels": labels,
        "business_reason": _coerce_text(item.get("business_reason") or item.get("hard_reason") or item.get("reason")),
        "is_duplicate": bool(item.get("is_duplicate") or duplicate_type in {"history", "intraday"}),
        "duplicate_type": duplicate_type,
        "match_ad_key": _coerce_text(item.get("match_ad_key") or item.get("duplicate_ad_key")),
        "match_date": _coerce_text(item.get("match_date") or item.get("duplicate_date")),
        "duplicate_confidence": confidence,
        "duplicate_reason": _coerce_text(item.get("duplicate_reason")),
        "llm_raw": item,
    }


def _default_result(target: dict[str, Any], *, parse_error: str = "") -> dict[str, Any]:
    return {
        "ad_key": target.get("ad_key"),
        "business_hard_block": False,
        "business_labels": [],
        "business_reason": "",
        "is_duplicate": False,
        "duplicate_type": "none",
        "match_ad_key": "",
        "match_date": "",
        "duplicate_confidence": "none",
        "duplicate_reason": "",
        "llm_parse_error": parse_error,
    }


def _system_prompt(target_date: str) -> str:
    return (
        "你是 VE 素材筛选助手。你只根据给定的视频内容文本判断目标日素材是否应被筛掉。"
        f"目标日是 {target_date}。筛选类型只有两类：业务硬拦、视频内容重复。"
        "必须保守，证据不足就保留。只返回纯 JSON。"
    )


def _user_prompt(
    *,
    advertiser_name: str,
    target_date: str,
    history_records: list[dict[str, Any]],
    intraday_prior_records: list[dict[str, Any]],
    target_records: list[dict[str, Any]],
) -> str:
    history_json = json.dumps(history_records, ensure_ascii=False, indent=2)
    intraday_json = json.dumps(intraday_prior_records, ensure_ascii=False, indent=2)
    targets_json = json.dumps(target_records, ensure_ascii=False, indent=2)
    return f"""
请筛选同一个广告主在 {target_date} 的素材。

广告主：{advertiser_name}

输入分三组：
1. history_records：同广告主、日期早于 {target_date} 的历史候选。只能用于“历史重复”判断。
2. intraday_prior_records：同广告主、{target_date} 当天排在当前批次之前的素材。只能用于“日内重复”判断。
3. target_records：需要判断的 {target_date} 素材。请按 target_records 的顺序处理；同一批里排在前面的 target 也可以作为后面 target 的日内候选。

业务硬拦规则：
- adult：成人、色情、性暗示、脱衣、亲密性内容。
- ecommerce_effect：商品图、带货、电商卖点、实物商品/店铺/购物强导向。
- non_human_photo_effect：核心效果不是人物照片加工，例如宠物、动物、房间装修、风景、食物、车辆、纯文字、logo、海报、物体动画。
- missing_human_photo_input：明确不是用户上传人物照片/自拍/人像照片参与加工，或只是在演示通用文生视频/图生视频/APP工具流程。不要仅因为文本没写“上传”两个字就判定；如果结果清楚是人像/自拍/真人照片特效，可以不命中这个标签。

重复判断规则：
- 重复是指用户看到的广告创意实质同款：核心玩法、画面模板、故事脚本、主体结果高度一致。
- 只同属“AI视频工具”“上传照片”“足球/写真/舞蹈”等泛类别不算重复。
- APP界面、下载教程、CTA、品牌名、通用口播相同不算重复。
- 只有高置信重复才 is_duplicate=true；可疑但不确定请保留。
- match_ad_key 必须来自 history_records、intraday_prior_records 或更早的 target_records。

最终输出纯 JSON，格式如下：
{{
  "results": [
    {{
      "ad_key": "目标素材ad_key",
      "business_hard_block": true/false,
      "business_labels": ["adult"|"ecommerce_effect"|"non_human_photo_effect"|"missing_human_photo_input"],
      "business_reason": "中文短句",
      "is_duplicate": true/false,
      "duplicate_type": "history"|"intraday"|"none",
      "match_ad_key": "命中的历史或日内候选ad_key，没有则空字符串",
      "match_date": "命中素材日期，没有则空字符串",
      "duplicate_confidence": "high"|"medium"|"low"|"none",
      "duplicate_reason": "中文短句"
    }}
  ]
}}

history_records:
{history_json}

intraday_prior_records:
{intraday_json}

target_records:
{targets_json}
""".strip()


def _call_llm_for_targets(
    *,
    advertiser_name: str,
    target_date: str,
    history_rows: list[dict[str, Any]],
    intraday_prior_rows: list[dict[str, Any]],
    target_rows: list[dict[str, Any]],
    model: str,
    timeout: float,
) -> list[dict[str, Any]]:
    if not target_rows:
        return []
    raw = call_text(
        _system_prompt(target_date),
        _user_prompt(
            advertiser_name=advertiser_name,
            target_date=target_date,
            history_records=[_record_for_prompt(row, prefix="H", index=i + 1) for i, row in enumerate(history_rows)],
            intraday_prior_records=[
                _record_for_prompt(row, prefix="P", index=i + 1) for i, row in enumerate(intraday_prior_rows)
            ],
            target_records=[_record_for_prompt(row, prefix="T", index=i + 1) for i, row in enumerate(target_rows)],
        ),
        models=[model],
        timeout=timeout,
    )
    parsed = _parse_results(raw)
    by_key: dict[str, dict[str, Any]] = {}
    for item in parsed:
        key = _coerce_text(item.get("ad_key"))
        if key and key not in by_key:
            by_key[key] = item

    results: list[dict[str, Any]] = []
    for row in target_rows:
        key = _coerce_text(row.get("ad_key"))
        if key in by_key:
            results.append(_normalize_result(by_key[key], row))
        else:
            results.append(_default_result(row, parse_error="missing_from_llm_output"))
    return results


def _valid_duplicate(
    result: dict[str, Any],
    *,
    allowed_history: dict[str, dict[str, Any]],
    allowed_intraday: dict[str, dict[str, Any]],
) -> tuple[bool, str, dict[str, Any] | None]:
    if not result.get("is_duplicate"):
        return False, "none", None
    match_key = _coerce_text(result.get("match_ad_key"))
    duplicate_type = _coerce_text(result.get("duplicate_type")).lower()
    if duplicate_type == "history" and match_key in allowed_history:
        return True, "history", allowed_history[match_key]
    if duplicate_type == "intraday" and match_key in allowed_intraday:
        return True, "intraday", allowed_intraday[match_key]
    if match_key in allowed_history:
        return True, "history", allowed_history[match_key]
    if match_key in allowed_intraday:
        return True, "intraday", allowed_intraday[match_key]
    return False, "none", None


LlmRunner = Callable[..., list[dict[str, Any]]]


def _process_advertiser_group(
    *,
    group_rows: list[dict[str, Any]],
    target_date: str,
    model: str,
    timeout: float,
    chunk_size: int,
    llm_runner: LlmRunner,
) -> tuple[list[dict[str, Any]], int]:
    group_rows = sorted(
        group_rows,
        key=lambda row: (
            _coerce_text(row.get("target_date")),
            -_safe_int(row.get("all_exposure_value")),
            -_safe_int(row.get("heat")),
            _coerce_text(row.get("ad_key")),
        ),
    )
    history_rows = [row for row in group_rows if _coerce_text(row.get("target_date")) < target_date]
    target_rows = [
        row
        for row in group_rows
        if _coerce_text(row.get("target_date")) == target_date
        and _coerce_text(row.get("video_content") or row.get("analysis"))
    ]
    if not target_rows:
        return [], 0

    advertiser_name = _coerce_text(target_rows[0].get("advertiser_name") or target_rows[0].get("product") or "未知广告主")
    intraday_prior_rows: list[dict[str, Any]] = []
    group_results: list[dict[str, Any]] = []
    llm_failures = 0
    step = max(1, int(chunk_size or 1))
    for start in range(0, len(target_rows), step):
        chunk = target_rows[start : start + step]
        try:
            chunk_results = llm_runner(
                advertiser_name=advertiser_name,
                target_date=target_date,
                history_rows=history_rows,
                intraday_prior_rows=intraday_prior_rows,
                target_rows=chunk,
                model=model,
                timeout=timeout,
            )
        except Exception as exc:
            llm_failures += len(chunk)
            chunk_results = [_default_result(row, parse_error=f"llm_call_failed:{exc}") for row in chunk]
        group_results.extend(chunk_results)
        intraday_prior_rows.extend(chunk)

    result_by_key = {_coerce_text(item.get("ad_key")): item for item in group_results}
    seen_intraday: dict[str, dict[str, Any]] = {}
    history_map = {_coerce_text(row.get("ad_key")): row for row in history_rows}
    group_out: list[dict[str, Any]] = []
    for row in target_rows:
        key = _coerce_text(row.get("ad_key"))
        result = deepcopy(result_by_key.get(key) or _default_result(row, parse_error="no_result"))
        valid_dup, dup_type, matched = _valid_duplicate(
            result,
            allowed_history=history_map,
            allowed_intraday=seen_intraday,
        )
        if valid_dup:
            result["duplicate_type"] = dup_type
            result["match_date"] = _coerce_text(matched.get("target_date") if matched else result.get("match_date"))
        else:
            result["is_duplicate"] = False
            result["duplicate_type"] = "none"
            result["match_ad_key"] = ""
            result["match_date"] = ""

        hard = bool(result.get("business_hard_block"))
        situations: list[str] = []
        if hard:
            situations.append("业务硬拦")
        if valid_dup and dup_type == "history":
            situations.append("历史重复")
        if valid_dup and dup_type == "intraday":
            situations.append("日内重复")
        if not situations:
            situations.append("保留")

        if hard:
            final_decision = "业务硬拦"
        elif valid_dup and dup_type == "history":
            final_decision = "历史重复"
        elif valid_dup and dup_type == "intraday":
            final_decision = "日内重复"
        else:
            final_decision = "保留"

        merged = deepcopy(row)
        merged.update(result)
        merged["situations"] = situations
        merged["final_decision"] = final_decision
        merged["matched_record"] = matched or {}
        group_out.append(merged)
        seen_intraday[key] = row
    return group_out, llm_failures


def run_llm_video_content_filter(
    *,
    rows: list[dict[str, Any]],
    target_date: str,
    model: str,
    timeout: float,
    chunk_size: int,
    max_workers: int,
    llm_runner: LlmRunner | None = None,
) -> dict[str, Any]:
    runner = llm_runner or _call_llm_for_targets
    rows_by_advertiser: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        row_date = _coerce_text(row.get("target_date"))
        if row_date <= target_date:
            advertiser_id = _coerce_text(row.get("advertiser_id") or row.get("advertiser_name") or row.get("appid"))
            rows_by_advertiser[advertiser_id].append(row)

    advertiser_groups = [group for _key, group in sorted(rows_by_advertiser.items())]
    workers = max(1, min(int(max_workers or 1), len(advertiser_groups) or 1))
    target_out: list[dict[str, Any]] = []
    llm_failures = 0
    if workers == 1:
        for group_rows in advertiser_groups:
            group_out, group_failures = _process_advertiser_group(
                group_rows=group_rows,
                target_date=target_date,
                model=model,
                timeout=timeout,
                chunk_size=chunk_size,
                llm_runner=runner,
            )
            target_out.extend(group_out)
            llm_failures += group_failures
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(
                    _process_advertiser_group,
                    group_rows=group_rows,
                    target_date=target_date,
                    model=model,
                    timeout=timeout,
                    chunk_size=chunk_size,
                    llm_runner=runner,
                )
                for group_rows in advertiser_groups
            ]
            for future in as_completed(futures):
                group_out, group_failures = future.result()
                target_out.extend(group_out)
                llm_failures += group_failures

    target_out.sort(
        key=lambda row: (
            _coerce_text(row.get("advertiser_name")),
            _coerce_text(row.get("target_date")),
            -_safe_int(row.get("all_exposure_value")),
            _coerce_text(row.get("ad_key")),
        )
    )
    final_counts = Counter(_coerce_text(row.get("final_decision")) for row in target_out)
    situation_counts: Counter[str] = Counter()
    advertiser_counts: dict[str, dict[str, int]] = {}
    for row in target_out:
        for situation in row.get("situations") or []:
            situation_counts[_coerce_text(situation)] += 1
        advertiser = _coerce_text(row.get("advertiser_name") or row.get("advertiser_id"))
        decision = _coerce_text(row.get("final_decision"))
        advertiser_counts.setdefault(advertiser, {})
        advertiser_counts[decision] = advertiser_counts[advertiser].get(decision, 0) + 1

    return {
        "summary": {
            "target_date": target_date,
            "target_records": len(target_out),
            "model": model,
            "llm_failures": llm_failures,
            "max_workers": workers,
            "final_counts": {key: int(final_counts.get(key, 0)) for key in FINAL_ORDER},
            "situation_counts": {key: int(situation_counts.get(key, 0)) for key in FINAL_ORDER},
            "advertiser_counts": advertiser_counts,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "filter_order": FINAL_ORDER,
        },
        "records": target_out,
    }


def build_current_rows(
    *,
    target_date: str,
    raw_payload: dict[str, Any],
    analysis_results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    analysis_by_ad = {
        _coerce_text(row.get("ad_key")): row
        for row in analysis_results
        if isinstance(row, dict) and _coerce_text(row.get("ad_key"))
    }
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
        analysis = analysis_by_ad.get(ad_key) or {}
        row = {
            "target_date": target_date,
            "product": _coerce_text(item.get("product") or creative.get("product")),
            "appid": _coerce_text(item.get("appid") or creative.get("appid")),
            "ad_key": ad_key,
            "video_url": _coerce_text(creative.get("video_url")),
            "preview_img_url": _coerce_text(creative.get("preview_img_url") or creative.get("image_url")),
            "raw_payload": creative,
            "title": _coerce_text(creative.get("title")),
            "body": _coerce_text(creative.get("body")),
            "analysis": _coerce_text(analysis.get("analysis")),
            "video_content": video_content,
            "heat": _safe_int(creative.get("heat")),
            "impression": _safe_int(creative.get("impression")),
            "all_exposure_value": _safe_int(creative.get("all_exposure_value")),
        }
        row["advertiser_id"] = _advertiser_id_from_raw(creative, row["appid"] or row["product"])
        row["advertiser_name"] = _advertiser_name_from_raw(creative, row["product"] or row["appid"])
        rows.append(row)
    return rows


def load_history_rows(
    *,
    target_date: str,
    history_days: int,
    advertiser_ids: set[str] | None = None,
    db_path: Path = DB_PATH,
) -> list[dict[str, Any]]:
    init_db()
    start_date = _window_start(target_date, history_days)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT target_date, product, appid, ad_key, video_url, preview_img_url,
                   raw_json, insight_analysis, guangdada_video_content,
                   heat, impression, all_exposure_value
            FROM daily_creative_insights
            WHERE target_date >= ? AND target_date < ?
            ORDER BY target_date ASC, all_exposure_value DESC, heat DESC, ad_key ASC
            """,
            (start_date, target_date),
        ).fetchall()
    finally:
        conn.close()

    out: list[dict[str, Any]] = []
    for row in rows:
        raw_payload = _loads_json(row["raw_json"])
        if not isinstance(raw_payload, dict):
            raw_payload = {}
        advertiser_id = _advertiser_id_from_raw(raw_payload, _coerce_text(row["appid"] or row["product"]))
        if advertiser_ids and advertiser_id not in advertiser_ids:
            continue
        video_content = _coerce_text(row["guangdada_video_content"])
        analysis = _coerce_text(row["insight_analysis"])
        if not video_content and not analysis:
            continue
        item = {
            "target_date": _coerce_text(row["target_date"]),
            "product": _coerce_text(row["product"]),
            "appid": _coerce_text(row["appid"]),
            "ad_key": _coerce_text(row["ad_key"]),
            "video_url": _coerce_text(row["video_url"]),
            "preview_img_url": _coerce_text(row["preview_img_url"]),
            "raw_payload": raw_payload,
            "title": _coerce_text(raw_payload.get("title")),
            "body": _coerce_text(raw_payload.get("body")),
            "analysis": analysis,
            "video_content": video_content,
            "heat": _safe_int(row["heat"]),
            "impression": _safe_int(row["impression"]),
            "all_exposure_value": _safe_int(row["all_exposure_value"]),
            "advertiser_id": advertiser_id,
            "advertiser_name": _advertiser_name_from_raw(raw_payload, _coerce_text(row["product"] or row["appid"])),
        }
        out.append(item)
    return out


def _append_unique_tags(row: dict[str, Any], tags: list[str]) -> None:
    current = row.get("material_tags")
    if not isinstance(current, list):
        current = []
    existing = {_coerce_text(tag) for tag in current if _coerce_text(tag)}
    for tag in tags:
        text = _coerce_text(tag)
        if text and text not in existing:
            current.append(text)
            existing.add(text)
    row["material_tags"] = current


def _compact_match(record: dict[str, Any]) -> dict[str, Any]:
    keep = [
        "final_decision",
        "situations",
        "business_hard_block",
        "business_labels",
        "business_reason",
        "is_duplicate",
        "duplicate_type",
        "match_ad_key",
        "match_date",
        "duplicate_confidence",
        "duplicate_reason",
        "llm_parse_error",
    ]
    return {key: record.get(key) for key in keep if key in record}


def apply_llm_filter_results(
    analysis_results: list[dict[str, Any]],
    payload: dict[str, Any],
) -> dict[str, Any]:
    by_key = {
        _coerce_text(record.get("ad_key")): record
        for record in payload.get("records") or []
        if isinstance(record, dict) and _coerce_text(record.get("ad_key"))
    }
    summary = {
        "applied": 0,
        "business_hard_block": 0,
        "history_duplicate": 0,
        "intraday_duplicate": 0,
    }
    for row in analysis_results:
        if not isinstance(row, dict):
            continue
        record = by_key.get(_coerce_text(row.get("ad_key")))
        if not record:
            continue
        row["llm_video_content_filter_match"] = _compact_match(record)
        decision = _coerce_text(record.get("final_decision"))
        if decision == "保留":
            continue

        row["exclude_from_bitable"] = True
        row["exclude_from_cluster"] = True
        summary["applied"] += 1
        if decision == "业务硬拦":
            labels = [label for label in record.get("business_labels") or [] if _coerce_text(label)]
            _append_unique_tags(row, ["大模型业务硬拦"] + [f"业务硬拦:{label}" for label in labels])
            summary["business_hard_block"] += 1
        elif decision == "历史重复":
            _append_unique_tags(row, ["大模型历史重复"])
            summary["history_duplicate"] += 1
        elif decision == "日内重复":
            _append_unique_tags(row, ["大模型日内重复"])
            summary["intraday_duplicate"] += 1
    return summary


def run_pipeline_llm_video_content_filter(
    *,
    target_date: str,
    raw_payload: dict[str, Any],
    analysis_results: list[dict[str, Any]],
    output_prefix: str,
    model: str,
    timeout: float,
    chunk_size: int,
    max_workers: int,
    history_days: int,
    write_report: bool = True,
) -> dict[str, Any]:
    current_rows = build_current_rows(
        target_date=target_date,
        raw_payload=raw_payload,
        analysis_results=analysis_results,
    )
    advertiser_ids = {_coerce_text(row.get("advertiser_id")) for row in current_rows if _coerce_text(row.get("advertiser_id"))}
    history_rows = load_history_rows(
        target_date=target_date,
        history_days=history_days,
        advertiser_ids=advertiser_ids,
    )
    rows = history_rows + current_rows
    payload = run_llm_video_content_filter(
        rows=rows,
        target_date=target_date,
        model=model,
        timeout=timeout,
        chunk_size=chunk_size,
        max_workers=max_workers,
    )
    payload["summary"]["history_days"] = int(history_days)
    payload["summary"]["history_records"] = len(history_rows)
    payload["summary"]["current_rows"] = len(current_rows)
    payload["apply_summary"] = apply_llm_filter_results(analysis_results, payload)
    if write_report:
        path = DATA_DIR / f"{output_prefix}_llm_video_content_filter.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        payload["summary"]["report_path"] = str(path)
    return payload
