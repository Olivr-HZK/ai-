"""Daily per-advertiser selection with cover diversity as the primary value signal."""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from ua_workflows.shared.db.video_enhancer import load_cover_embedding_blob_map_by_ad_keys
from ua_workflows.shared.llm.client import bytes_to_embedding, cosine_similarity


def _creative(item: dict[str, Any]) -> dict[str, Any]:
    value = item.get("creative") or {}
    return value if isinstance(value, dict) else {}


def _ad_key(item: dict[str, Any]) -> str:
    return str(_creative(item).get("ad_key") or "").strip()


def _appid(item: dict[str, Any]) -> str:
    return str(item.get("appid") or _creative(item).get("advertiser_id") or "").strip()


def _exposure(item: dict[str, Any]) -> int:
    creative = _creative(item)
    return int(creative.get("all_exposure_value") or creative.get("impression") or 0)


def _normalise_vector(vector: Iterable[float]) -> list[float] | None:
    values = [float(value) for value in vector]
    norm = math.sqrt(sum(value * value for value in values))
    if not values or norm <= 0:
        return None
    return [value / norm for value in values]


def _distance(left: list[float], right: list[float]) -> float:
    similarity = max(-1.0, min(1.0, float(cosine_similarity(left, right))))
    return 1.0 - similarity


def _exposure_scores(items: list[dict[str, Any]]) -> dict[str, float]:
    logged = { _ad_key(item): math.log1p(max(0, _exposure(item))) for item in items }
    if not logged:
        return {}
    low, high = min(logged.values()), max(logged.values())
    if high <= low:
        return {key: 0.0 for key in logged}
    return {key: (value - low) / (high - low) for key, value in logged.items()}


def _select_group(
    items: list[dict[str, Any]],
    *,
    limit: int,
    embedding_by_ad_key: dict[str, Iterable[float]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    vectors: dict[str, list[float]] = {}
    for item in items:
        key = _ad_key(item)
        raw_vector = embedding_by_ad_key.get(key)
        if key and raw_vector is not None:
            vector = _normalise_vector(raw_vector)
            if vector is not None:
                vectors[key] = vector

    exposure_scores = _exposure_scores(items)
    vector_items = [item for item in items if _ad_key(item) in vectors]
    fallback_items = [item for item in items if _ad_key(item) not in vectors]
    selected: list[dict[str, Any]] = []
    selected_keys: set[str] = set()
    score_by_key: dict[str, tuple[float | None, float, float]] = {}

    while vector_items and len(selected) < limit:
        candidates: list[tuple[float, float, int, str, dict[str, Any]]] = []
        for item in vector_items:
            key = _ad_key(item)
            vector = vectors[key]
            if selected_keys:
                difference = min(_distance(vector, vectors[chosen]) for chosen in selected_keys)
            else:
                others = [vectors[_ad_key(other)] for other in vector_items if _ad_key(other) != key]
                difference = (
                    sum(_distance(vector, other) for other in others) / len(others)
                    if others
                    else 1.0
                )
            exposure_score = exposure_scores.get(key, 0.0)
            combined = 0.9 * difference + 0.1 * exposure_score
            candidates.append((combined, difference, _exposure(item), key, item))
        combined, difference, _raw_exposure, key, winner = max(
            candidates,
            key=lambda row: (row[0], row[1], row[2], row[3]),
        )
        selected.append(winner)
        selected_keys.add(key)
        score_by_key[key] = (difference, exposure_scores.get(key, 0.0), combined)
        vector_items = [item for item in vector_items if _ad_key(item) != key]

    if len(selected) < limit:
        fallback_items.sort(key=lambda item: (_exposure(item), _ad_key(item)), reverse=True)
        for item in fallback_items[: limit - len(selected)]:
            key = _ad_key(item)
            selected.append(item)
            selected_keys.add(key)
            exposure_score = exposure_scores.get(key, 0.0)
            score_by_key[key] = (None, exposure_score, 0.1 * exposure_score)

    selected_rows: list[dict[str, Any]] = []
    for rank, item in enumerate(selected, start=1):
        key = _ad_key(item)
        difference, exposure_score, combined = score_by_key[key]
        row = dict(item)
        row["daily_value_selection"] = {
            "selected": True,
            "rank": rank,
            "limit": limit,
            "priority": "cover_difference_then_exposure",
            "difference_weight": 0.9,
            "exposure_weight": 0.1,
            "difference_score": round(difference, 6) if difference is not None else None,
            "exposure_score": round(exposure_score, 6),
            "selection_score": round(combined, 6),
        }
        selected_rows.append(row)

    omitted = [
        {
            "ad_key": _ad_key(item),
            "product": str(item.get("product") or ""),
            "all_exposure_value": _exposure(item),
            "reason": "daily_advertiser_limit",
        }
        for item in items
        if _ad_key(item) not in selected_keys
    ]
    return selected_rows, omitted


def select_diverse_top_per_advertiser(
    items: list[dict[str, Any]],
    *,
    limit: int = 10,
    embedding_by_ad_key: dict[str, Iterable[float]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    limit = max(1, int(limit))
    embedding_by_ad_key = embedding_by_ad_key or {}
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        if isinstance(item, dict):
            groups[_appid(item)].append(item)

    selected_all: list[dict[str, Any]] = []
    per_appid: list[dict[str, Any]] = []
    for appid, bucket in groups.items():
        selected, omitted = _select_group(
            bucket,
            limit=limit,
            embedding_by_ad_key=embedding_by_ad_key,
        )
        selected_all.extend(selected)
        per_appid.append(
            {
                "appid": appid,
                "product": str(bucket[0].get("product") or "") if bucket else "",
                "input_count": len(bucket),
                "output_count": len(selected),
                "removed_count": len(omitted),
                "embedding_count": sum(1 for item in bucket if _ad_key(item) in embedding_by_ad_key),
                "selected_ad_keys": [_ad_key(item) for item in selected],
                "omitted": omitted,
            }
        )

    return selected_all, {
        "skipped": False,
        "limit_per_advertiser_per_day": limit,
        "selection_priority": "cover_difference_then_exposure",
        "difference_weight": 0.9,
        "exposure_weight": 0.1,
        "input_count": len(items),
        "output_count": len(selected_all),
        "removed_count": len(items) - len(selected_all),
        "per_appid": per_appid,
    }


def apply_daily_advertiser_diversity_limit(
    items: list[dict[str, Any]],
    *,
    limit: int = 10,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    ad_keys = [_ad_key(item) for item in items if _ad_key(item)]
    blobs = load_cover_embedding_blob_map_by_ad_keys(ad_keys)
    embeddings: dict[str, list[float]] = {}
    for ad_key, blob in blobs.items():
        try:
            embeddings[ad_key] = bytes_to_embedding(blob)
        except Exception:
            continue
    return select_diverse_top_per_advertiser(
        items,
        limit=limit,
        embedding_by_ad_key=embeddings,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="按封面差异度精选每个广告主每日 TopN")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()

    source = Path(args.input)
    payload = json.loads(source.read_text(encoding="utf-8"))
    original = [item for item in (payload.get("items") or []) if isinstance(item, dict)]
    selected, report = apply_daily_advertiser_diversity_limit(original, limit=args.limit)
    payload["items"] = selected
    payload["total_before_daily_advertiser_limit"] = len(original)
    payload["total"] = len(selected)
    payload["daily_advertiser_selection_report"] = report
    Path(args.output).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    Path(args.report).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"output": args.output, "report": args.report, **{k: report[k] for k in ("input_count", "output_count", "removed_count")}}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
