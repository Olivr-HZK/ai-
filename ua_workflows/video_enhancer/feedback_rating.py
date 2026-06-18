"""Shared reviewer rating parsing for VE feedback fields."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


REVIEWER_FIELDS = {
    "haopeng": ("浩鹏评分", "浩鹏接受情况"),
    "浩鹏": ("浩鹏评分", "浩鹏接受情况"),
    "weilan": ("尉蓝评分", "尉蓝接受情况"),
    "尉蓝": ("尉蓝评分", "尉蓝接受情况"),
}

LEGACY_STATUS_FIELD = "接受情况"
RATING_LABELS = {rating: f"{rating}星" for rating in range(1, 4)}

LEGACY_STATUS_TO_RATING = {
    "采纳": 3,
    "接受": 3,
    "accept": 3,
    "accepted": 3,
    "yes": 3,
    "入素材库": 3,
    "不采纳": 1,
    "删除": 1,
    "拒绝": 1,
    "重复抓取": 1,
    "reject": 1,
    "rejected": 1,
    "no": 1,
}


@dataclass(frozen=True)
class FeedbackRating:
    rating: int | None
    rating_label: str
    source_field: str
    source_value: str
    legacy_status: str
    legacy_field: str
    invalid_rating: bool = False
    from_legacy: bool = False


def cell_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else f"{value:g}"
    if isinstance(value, list):
        return " ".join(part for part in (cell_to_text(v) for v in value) if part).strip()
    if isinstance(value, dict):
        for key in ("text", "name", "value", "link", "url", "file_token"):
            text = cell_to_text(value.get(key))
            if text:
                return text
        for key in ("segments", "elements", "attrs"):
            text = cell_to_text(value.get(key))
            if text:
                return text
    return str(value).strip()


def reviewer_rating_field(reviewer: str = "haopeng") -> str:
    return REVIEWER_FIELDS.get(str(reviewer or "").strip().lower(), REVIEWER_FIELDS["haopeng"])[0]


def reviewer_status_field(reviewer: str = "haopeng") -> str:
    return REVIEWER_FIELDS.get(str(reviewer or "").strip().lower(), REVIEWER_FIELDS["haopeng"])[1]


def normalize_rating_value(value: Any) -> int | None:
    text = cell_to_text(value)
    if not text:
        return None
    compact = re.sub(r"\s+", "", text).lower()
    zh_digits = {"一": 1, "二": 2, "两": 2, "三": 3}
    for zh, num in zh_digits.items():
        if compact.startswith(zh) and ("星" in compact or "颗" in compact):
            return num
    match = re.fullmatch(r"([1-3])(?:\.0+)?(?:颗?星|star|stars)?", compact)
    if match:
        return int(match.group(1))
    return None


def normalize_legacy_status(value: Any) -> str:
    return re.sub(r"\s+", "", cell_to_text(value))


def legacy_status_to_rating(status: Any) -> int | None:
    normalized = normalize_legacy_status(status)
    if not normalized:
        return None
    return LEGACY_STATUS_TO_RATING.get(normalized) or LEGACY_STATUS_TO_RATING.get(normalized.lower())


def _first_status(fields: dict[str, Any], names: tuple[str, ...]) -> tuple[str, str]:
    for name in names:
        text = normalize_legacy_status(fields.get(name))
        if text:
            return name, text
    return "", ""


def resolve_feedback_rating(
    fields: dict[str, Any],
    *,
    reviewer: str = "haopeng",
    rating_field: str | None = None,
    status_field: str | None = None,
    legacy_field: str = LEGACY_STATUS_FIELD,
) -> FeedbackRating:
    if not isinstance(fields, dict):
        fields = {}
    r_field = rating_field or reviewer_rating_field(reviewer)
    s_field = status_field or reviewer_status_field(reviewer)
    legacy_field_name, legacy_status = _first_status(fields, (s_field, legacy_field))

    raw_rating = fields.get(r_field)
    rating_text = cell_to_text(raw_rating)
    if rating_text:
        rating = normalize_rating_value(raw_rating)
        return FeedbackRating(
            rating=rating,
            rating_label=RATING_LABELS.get(rating, ""),
            source_field=r_field,
            source_value=rating_text,
            legacy_status=legacy_status,
            legacy_field=legacy_field_name,
            invalid_rating=rating is None,
            from_legacy=False,
        )

    rating = legacy_status_to_rating(legacy_status)
    return FeedbackRating(
        rating=rating,
        rating_label=RATING_LABELS.get(rating, ""),
        source_field=legacy_field_name,
        source_value=legacy_status,
        legacy_status=legacy_status,
        legacy_field=legacy_field_name,
        invalid_rating=False,
        from_legacy=rating is not None,
    )
