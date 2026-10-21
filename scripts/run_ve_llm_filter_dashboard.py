#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html
import json
import re
import sqlite3
import sys
import urllib.parse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ua_workflows.shared.config import DATA_DIR, PROJECT_ROOT, REPORTS_DIR, load_project_env
from ua_workflows.shared.llm.client import call_text, flush_usage, resolve_text_model


DEFAULT_DB_PATH = PROJECT_ROOT / "data/remote_snapshots/ve/data/video_enhancer_pipeline.db"
DEFAULT_KEY_FILE = DATA_DIR / "ve_bitable_view_vewd67ZK4J_adkeys_from_2026-06-07_unique.txt"

BUSINESS_LABELS = {
    "adult",
    "ecommerce_effect",
    "non_human_photo_effect",
    "missing_human_photo_input",
}


def _read_keys(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


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


def _advertiser_id(row: dict[str, Any]) -> str:
    raw = row.get("raw_payload")
    if not isinstance(raw, dict):
        raw = {}
    creative = raw.get("creative")
    if not isinstance(creative, dict):
        creative = {}
    return _coerce_text(
        raw.get("advertiser_id")
        or creative.get("advertiser_id")
        or raw.get("advertiser_name")
        or creative.get("advertiser_name")
        or row.get("appid")
        or row.get("product")
    )


def _advertiser_name(row: dict[str, Any]) -> str:
    raw = row.get("raw_payload")
    if not isinstance(raw, dict):
        raw = {}
    creative = raw.get("creative")
    if not isinstance(creative, dict):
        creative = {}
    return _coerce_text(
        raw.get("advertiser_name")
        or creative.get("advertiser_name")
        or row.get("product")
        or row.get("appid")
    )


def _load_rows(db_path: Path, ad_keys: list[str]) -> list[dict[str, Any]]:
    if not ad_keys:
        return []
    placeholders = ",".join("?" for _ in ad_keys)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"""
            SELECT target_date, product, appid, ad_key, video_url, preview_img_url,
                   raw_json, insight_analysis, guangdada_video_content,
                   heat, impression, all_exposure_value, exclude_from_bitable,
                   material_tags
            FROM daily_creative_insights
            WHERE ad_key IN ({placeholders})
            ORDER BY target_date ASC, all_exposure_value DESC, heat DESC, ad_key ASC
            """,
            ad_keys,
        ).fetchall()
    finally:
        conn.close()

    out: list[dict[str, Any]] = []
    for row in rows:
        raw_payload = _loads_json(row["raw_json"])
        if not isinstance(raw_payload, dict):
            raw_payload = {}
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
            "analysis": _coerce_text(row["insight_analysis"]),
            "video_content": _coerce_text(row["guangdada_video_content"]),
            "heat": int(row["heat"] or 0),
            "impression": int(row["impression"] or 0),
            "all_exposure_value": int(row["all_exposure_value"] or 0),
            "exclude_from_bitable": bool(int(row["exclude_from_bitable"] or 0)),
            "material_tags": _loads_json(row["material_tags"]) if row["material_tags"] else [],
        }
        item["advertiser_id"] = _advertiser_id(item)
        item["advertiser_name"] = _advertiser_name(item)
        out.append(item)
    return out


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


def _system_prompt() -> str:
    return (
        "你是 VE 素材筛选助手。你只根据给定的视频内容文本判断 6.11 当日素材是否应被筛掉。"
        "筛选类型只有两类：业务硬拦、视频内容重复。必须保守，证据不足就保留。只返回纯 JSON。"
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
    prefix: str = "",
) -> list[dict[str, Any]]:
    if not target_rows:
        return []

    history_records = [_record_for_prompt(row, prefix="H", index=i + 1) for i, row in enumerate(history_rows)]
    prior_records = [_record_for_prompt(row, prefix="P", index=i + 1) for i, row in enumerate(intraday_prior_rows)]
    target_records = [_record_for_prompt(row, prefix="T", index=i + 1) for i, row in enumerate(target_rows)]

    raw = call_text(
        _system_prompt(),
        _user_prompt(
            advertiser_name=advertiser_name,
            target_date=target_date,
            history_records=history_records,
            intraday_prior_records=prior_records,
            target_records=target_records,
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

    missing = [row for row in target_rows if _coerce_text(row.get("ad_key")) not in by_key]
    if missing and len(target_rows) > 1:
        print(
            f"[llm-filter] {prefix}{advertiser_name}: parsed={len(by_key)}/{len(target_rows)}, split missing={len(missing)}"
        )
        results: list[dict[str, Any]] = []
        already_prior = list(intraday_prior_rows)
        for row in target_rows:
            key = _coerce_text(row.get("ad_key"))
            if key in by_key:
                results.append(_normalize_result(by_key[key], row))
            else:
                sub = _call_llm_for_targets(
                    advertiser_name=advertiser_name,
                    target_date=target_date,
                    history_rows=history_rows,
                    intraday_prior_rows=already_prior,
                    target_rows=[row],
                    model=model,
                    timeout=timeout,
                    prefix=f"{prefix}single:",
                )
                results.extend(sub or [_default_result(row, parse_error="missing_after_split")])
            already_prior.append(row)
        return results

    results = []
    for row in target_rows:
        key = _coerce_text(row.get("ad_key"))
        if key in by_key:
            results.append(_normalize_result(by_key[key], row))
        else:
            results.append(_default_result(row, parse_error="missing_from_llm_output"))
    return results


def _valid_duplicate(
    result: dict[str, Any],
    target: dict[str, Any],
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


def _process_advertiser_group(
    *,
    group_rows: list[dict[str, Any]],
    target_date: str,
    model: str,
    timeout: float,
    chunk_size: int,
) -> tuple[list[dict[str, Any]], int]:
    group_rows = sorted(
        group_rows,
        key=lambda row: (
            _coerce_text(row.get("target_date")),
            -int(row.get("all_exposure_value") or 0),
            -int(row.get("heat") or 0),
            _coerce_text(row.get("ad_key")),
        ),
    )
    history_rows = [r for r in group_rows if _coerce_text(r.get("target_date")) < target_date]
    target_rows = [
        r
        for r in group_rows
        if _coerce_text(r.get("target_date")) == target_date
        and _coerce_text(r.get("video_content") or r.get("analysis"))
    ]
    if not target_rows:
        return [], 0

    advertiser_name = _advertiser_name(target_rows[0])
    print(
        f"[llm-filter] advertiser={advertiser_name} target={len(target_rows)} history={len(history_rows)}"
    )
    intraday_prior_rows: list[dict[str, Any]] = []
    group_results: list[dict[str, Any]] = []
    llm_failures = 0
    for start in range(0, len(target_rows), max(1, chunk_size)):
        chunk = target_rows[start : start + max(1, chunk_size)]
        print(
            f"[llm-filter] {advertiser_name} chunk {start + 1}-{start + len(chunk)} / {len(target_rows)}"
        )
        try:
            chunk_results = _call_llm_for_targets(
                advertiser_name=advertiser_name,
                target_date=target_date,
                history_rows=history_rows,
                intraday_prior_rows=intraday_prior_rows,
                target_rows=chunk,
                model=model,
                timeout=timeout,
                prefix=f"{advertiser_name}:",
            )
        except Exception as exc:
            llm_failures += len(chunk)
            print(f"[llm-filter] advertiser={advertiser_name} chunk failed: {exc}")
            chunk_results = [
                _default_result(row, parse_error=f"llm_call_failed:{exc}") for row in chunk
            ]
        group_results.extend(chunk_results)
        intraday_prior_rows.extend(chunk)
        print(
            f"[llm-filter] {advertiser_name} chunk done {start + 1}-{start + len(chunk)} / {len(target_rows)}"
        )

    group_out: list[dict[str, Any]] = []
    result_by_key = {_coerce_text(item.get("ad_key")): item for item in group_results}
    seen_intraday: dict[str, dict[str, Any]] = {}
    history_map = {_coerce_text(row.get("ad_key")): row for row in history_rows}
    for row in target_rows:
        key = _coerce_text(row.get("ad_key"))
        result = deepcopy(result_by_key.get(key) or _default_result(row, parse_error="no_result"))
        valid_dup, dup_type, matched = _valid_duplicate(
            result,
            row,
            allowed_history=history_map,
            allowed_intraday=seen_intraday,
        )
        if not valid_dup:
            result["is_duplicate"] = False
            result["duplicate_type"] = "none"
            result["match_ad_key"] = ""
            result["match_date"] = ""
        else:
            result["duplicate_type"] = dup_type
            result["match_date"] = _coerce_text(
                matched.get("target_date") if matched else result.get("match_date")
            )
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


def run_llm_filter(
    *,
    rows: list[dict[str, Any]],
    target_date: str,
    model: str,
    timeout: float,
    chunk_size: int,
    max_workers: int,
) -> dict[str, Any]:
    rows_by_advertiser: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if _coerce_text(row.get("target_date")) <= target_date:
            rows_by_advertiser[_coerce_text(row.get("advertiser_id"))].append(row)

    target_out: list[dict[str, Any]] = []
    llm_failures = 0
    advertiser_groups = [
        group_rows for _advertiser_id, group_rows in sorted(rows_by_advertiser.items())
    ]
    workers = max(1, min(int(max_workers or 1), len(advertiser_groups) or 1))
    print(f"[llm-filter] parallel advertisers={len(advertiser_groups)} workers={workers}")
    if workers == 1:
        for group_rows in advertiser_groups:
            group_out, group_failures = _process_advertiser_group(
                group_rows=group_rows,
                target_date=target_date,
                model=model,
                timeout=timeout,
                chunk_size=chunk_size,
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
            -int(row.get("all_exposure_value") or 0),
            _coerce_text(row.get("ad_key")),
        )
    )

    final_counts = Counter(row.get("final_decision") for row in target_out)
    situation_counts = Counter()
    for row in target_out:
        for situation in row.get("situations") or []:
            situation_counts[str(situation)] += 1
    advertiser_counts: dict[str, dict[str, int]] = {}
    for row in target_out:
        name = _coerce_text(row.get("advertiser_name"))
        advertiser_counts.setdefault(name, {})
        advertiser_counts[name][str(row.get("final_decision"))] = advertiser_counts[name].get(
            str(row.get("final_decision")), 0
        ) + 1

    return {
        "summary": {
            "target_date": target_date,
            "target_records": len(target_out),
            "model": model,
            "llm_failures": llm_failures,
            "max_workers": workers,
            "final_counts": dict(final_counts),
            "situation_counts": dict(situation_counts),
            "advertiser_counts": advertiser_counts,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "filter_order": ["业务硬拦", "历史重复", "日内重复", "保留"],
        },
        "records": target_out,
    }


def _proxy_url(url: str) -> str:
    if not url:
        return ""
    parsed = urllib.parse.urlparse(url)
    if parsed.netloc == "sp2cdn-idea-global.zingfront.com":
        return "/__media_proxy?u=" + urllib.parse.quote(url, safe="")
    return url


def _media_html(record: dict[str, Any], *, mini: bool = False) -> str:
    video = _proxy_url(_coerce_text(record.get("video_url")))
    poster = _proxy_url(_coerce_text(record.get("preview_img_url")))
    cls = "mini-media" if mini else "media"
    if video:
        return (
            f'<video class="{cls}" controls muted playsinline preload="metadata" '
            f'poster="{html.escape(poster)}" src="{html.escape(video)}"></video>'
        )
    if poster:
        return f'<img class="{cls}" src="{html.escape(poster)}" alt="">'
    return f'<div class="{cls} empty">No media</div>'


def _badge(text: str, cls: str = "") -> str:
    return f'<span class="badge {html.escape(cls)}">{html.escape(text)}</span>'


def _row_tags(row: dict[str, Any]) -> str:
    tags = [_badge(_coerce_text(row.get("final_decision")), _decision_class(row.get("final_decision")))]
    for situation in row.get("situations") or []:
        if situation != row.get("final_decision"):
            tags.append(_badge(_coerce_text(situation), _decision_class(situation)))
    labels = row.get("business_labels") or []
    if labels:
        tags.append(_badge(",".join(labels), "hard"))
    confidence = _coerce_text(row.get("duplicate_confidence"))
    if confidence and confidence != "none":
        tags.append(_badge(f"重复置信:{confidence}", "dup"))
    if row.get("llm_parse_error"):
        tags.append(_badge("模型解析异常", "error"))
    return " ".join(tags)


def _decision_class(value: Any) -> str:
    text = _coerce_text(value)
    return {
        "业务硬拦": "hard",
        "历史重复": "history",
        "日内重复": "intraday",
        "保留": "keep",
    }.get(text, "")


def render_dashboard(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    records = payload.get("records") or []

    def esc(value: Any) -> str:
        return html.escape(str(value if value is not None else ""))

    final_counts = summary.get("final_counts") or {}
    situation_counts = summary.get("situation_counts") or {}
    order = ["业务硬拦", "历史重复", "日内重复", "保留"]
    pills = [
        ("6.11素材", summary.get("target_records", 0), ""),
        ("业务硬拦", final_counts.get("业务硬拦", 0), "hard"),
        ("历史重复", final_counts.get("历史重复", 0), "history"),
        ("日内重复", final_counts.get("日内重复", 0), "intraday"),
        ("最终保留", final_counts.get("保留", 0), "keep"),
        ("硬拦+重复重叠", sum(1 for r in records if "业务硬拦" in (r.get("situations") or []) and ("历史重复" in (r.get("situations") or []) or "日内重复" in (r.get("situations") or []))), "overlap"),
    ]
    pill_html = "".join(
        f'<div class="pill {esc(cls)}"><b>{esc(value)}</b><span>{esc(label)}</span></div>'
        for label, value, cls in pills
    )

    advertiser_rows = []
    for advertiser, counts in sorted((summary.get("advertiser_counts") or {}).items()):
        advertiser_rows.append(
            "<tr>"
            f"<td>{esc(advertiser)}</td>"
            + "".join(f"<td>{esc((counts or {}).get(name, 0))}</td>" for name in order)
            + "</tr>"
        )

    situation_rows = []
    for name in ["业务硬拦", "历史重复", "日内重复", "保留"]:
        situation_rows.append(
            f"<tr><td>{esc(name)}</td><td>{esc(situation_counts.get(name, 0))}</td><td>{esc(final_counts.get(name, 0))}</td></tr>"
        )

    def card(row: dict[str, Any]) -> str:
        matched = row.get("matched_record")
        if not isinstance(matched, dict):
            matched = {}
        matched_html = ""
        if matched:
            matched_html = (
                '<div class="match">'
                f'{_media_html(matched, mini=True)}'
                '<div>'
                f'<div>{_badge("命中候选", "dup")} <b>{esc(matched.get("target_date"))}</b> '
                f'<code>{esc(matched.get("ad_key"))}</code></div>'
                f'<div class="muted">{esc(matched.get("advertiser_name") or matched.get("product"))}</div>'
                f'<p>{esc(_clip(matched.get("video_content") or matched.get("analysis"), 260))}</p>'
                '</div></div>'
            )
        else:
            matched_html = '<div class="muted">没有命中重复候选。</div>'
        return (
            '<article class="card">'
            f'<div>{_media_html(row)}</div>'
            '<div class="body">'
            f'<div class="tags">{_row_tags(row)}</div>'
            f'<h3>{esc(row.get("advertiser_name") or row.get("product"))}</h3>'
            f'<div class="meta"><b>{esc(row.get("target_date"))}</b> · <code>{esc(row.get("ad_key"))}</code> · 曝光值 {esc(row.get("all_exposure_value"))}</div>'
            f'<p class="reason"><b>业务：</b>{esc(row.get("business_reason")) or "未命中硬拦"}</p>'
            f'<p class="reason"><b>去重：</b>{esc(row.get("duplicate_reason")) or "未命中重复"}</p>'
            f'<p>{esc(_clip(row.get("video_content") or row.get("analysis"), 520))}</p>'
            f'{matched_html}'
            '</div></article>'
        )

    sections = []
    for decision in order:
        section_records = [row for row in records if row.get("final_decision") == decision]
        section_records.sort(
            key=lambda row: (
                _coerce_text(row.get("advertiser_name")),
                -int(row.get("all_exposure_value") or 0),
                _coerce_text(row.get("ad_key")),
            )
        )
        cards = "".join(card(row) for row in section_records)
        if not cards:
            cards = '<div class="empty-section">没有素材。</div>'
        sections.append(
            f'<section><h2>{esc(decision)} <span>{len(section_records)} 条</span></h2><div class="grid">{cards}</div></section>'
        )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>VE 6.11 大模型筛选看板</title>
<style>
body{{margin:0;background:#f7f8fb;color:#111827;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}}
header{{position:sticky;top:0;background:#fff;border-bottom:1px solid #e5e7eb;padding:18px 24px;z-index:3}}
h1{{font-size:22px;margin:0 0 10px}}h2{{font-size:18px;margin:24px 0 12px}}h2 span{{color:#64748b;font-size:13px}}h3{{font-size:16px;margin:8px 0 4px}}
main{{padding:20px 24px 50px}}.summary{{display:flex;gap:10px;flex-wrap:wrap}}.pill{{border:1px solid #cbd5e1;background:#fff;border-radius:8px;padding:8px 12px;min-width:92px}}.pill b{{display:block;font-size:22px}}.pill span{{font-size:12px;color:#475569}}
.pill.hard{{border-color:#fecaca;background:#fff1f2}}.pill.history{{border-color:#fed7aa;background:#fff7ed}}.pill.intraday{{border-color:#bfdbfe;background:#eff6ff}}.pill.keep{{border-color:#bbf7d0;background:#f0fdf4}}.pill.overlap{{border-color:#ddd6fe;background:#f5f3ff}}
.note{{color:#475569;font-size:13px;margin-top:8px}}table{{width:100%;border-collapse:collapse;background:#fff;margin:10px 0 18px}}th,td{{border:1px solid #e5e7eb;padding:8px;text-align:left;vertical-align:top;font-size:12px}}th{{background:#f1f5f9}}
.grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(620px,1fr));gap:14px}}.card{{display:grid;grid-template-columns:178px minmax(0,1fr);gap:14px;background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:12px}}
.media{{width:178px;aspect-ratio:9/16;object-fit:cover;background:#111827;border-radius:7px;border:1px solid #d1d5db}}.mini-media{{width:72px;aspect-ratio:9/16;object-fit:cover;background:#111827;border-radius:6px;border:1px solid #d1d5db;flex:0 0 auto}}
.empty{{display:flex;align-items:center;justify-content:center;color:#94a3b8;font-size:12px}}.body p{{line-height:1.5;margin:8px 0}}.meta,.muted{{color:#64748b;font-size:12px}}code{{background:#f1f5f9;border-radius:5px;padding:2px 5px;word-break:break-all}}
.badge{{display:inline-block;border-radius:5px;padding:3px 7px;font-size:12px;background:#e2e8f0;color:#334155;margin:0 5px 5px 0}}.badge.hard{{background:#dc2626;color:white}}.badge.history{{background:#f97316;color:white}}.badge.intraday{{background:#2563eb;color:white}}.badge.keep{{background:#059669;color:white}}.badge.dup{{background:#7c3aed;color:white}}.badge.error{{background:#111827;color:white}}
.reason{{background:#f8fafc;border-left:3px solid #cbd5e1;padding:7px 9px}}.match{{display:flex;gap:10px;border-top:1px solid #eef2f7;margin-top:10px;padding-top:10px}}.empty-section{{background:#fff;border:1px dashed #cbd5e1;border-radius:8px;padding:18px;color:#64748b}}
@media(max-width:760px){{.grid{{grid-template-columns:1fr}}.card{{grid-template-columns:1fr}}.media{{width:100%;max-height:360px}}}}
</style>
</head>
<body>
<header>
  <h1>VE 6.11 大模型筛选看板</h1>
  <div class="summary">{pill_html}</div>
  <div class="note">筛选顺序：业务硬拦 → 历史重复 → 日内重复 → 保留。去重和业务硬拦均由大模型基于视频内容文本判断；仅比较同广告主，6.11 只和历史以及当天前序素材比较。</div>
  <div class="note">模型：{esc(summary.get("model"))} · 并行广告主 workers：{esc(summary.get("max_workers", 1))} · 生成：{esc(summary.get("generated_at"))} · 模型异常：{esc(summary.get("llm_failures", 0))}</div>
</header>
<main>
<section>
  <h2>不同情况汇总</h2>
  <table><thead><tr><th>情况</th><th>命中该情况</th><th>作为最终筛选原因</th></tr></thead><tbody>{"".join(situation_rows)}</tbody></table>
  <table><thead><tr><th>广告主</th>{"".join(f"<th>{esc(name)}</th>" for name in order)}</tr></thead><tbody>{"".join(advertiser_rows)}</tbody></table>
</section>
{"".join(sections)}
</main>
</body>
</html>"""


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run VE video-content LLM hard filter and dedupe dashboard.")
    parser.add_argument("--target-date", default="2026-06-11")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--key-file", default=str(DEFAULT_KEY_FILE))
    parser.add_argument("--llm-model", default="")
    parser.add_argument("--llm-timeout", type=float, default=90.0)
    parser.add_argument("--chunk-size", type=int, default=8)
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="并行处理的广告主数量；广告主内部仍按日内顺序处理。",
    )
    parser.add_argument(
        "--output-json",
        default=str(DATA_DIR / "ve_llm_filter_dashboard_2026-06-11.json"),
    )
    parser.add_argument(
        "--report-html",
        default=str(REPORTS_DIR / "ve_llm_filter_dashboard_2026-06-11.html"),
    )
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    load_project_env(override=True)
    target_date = _coerce_text(args.target_date)
    model = _coerce_text(args.llm_model) or resolve_text_model()
    key_file = Path(args.key_file)
    db_path = Path(args.db_path)
    ad_keys = _read_keys(key_file)
    rows = _load_rows(db_path, ad_keys)
    if not rows:
        raise SystemExit("No rows loaded.")
    payload = run_llm_filter(
        rows=rows,
        target_date=target_date,
        model=model,
        timeout=max(1.0, float(args.llm_timeout or 90.0)),
        chunk_size=max(1, int(args.chunk_size or 8)),
        max_workers=max(1, int(args.max_workers or 1)),
    )
    payload["scope"] = {
        "target_date": target_date,
        "db_path": str(db_path),
        "key_file": str(key_file),
        "loaded_rows": len(rows),
        "loaded_ad_keys": len(ad_keys),
        "max_workers": max(1, int(args.max_workers or 1)),
    }
    out_json = Path(args.output_json)
    out_html = Path(args.report_html)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_html.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_html.write_text(render_dashboard(payload), encoding="utf-8")
    flush_usage(target_date)
    print(
        json.dumps(
            {
                "summary": payload.get("summary"),
                "output_json": str(out_json),
                "report_html": str(out_html),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
