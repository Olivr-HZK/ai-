"""Content safety filters shared by the Video Enhancer pipeline and sync step."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Tuple


ADULT_FILTER_TAG = "色情/成人风险"

_ADULT_PATTERNS = [
    r"黄片",
    r"色情",
    r"成人视频",
    r"成人影片",
    r"低俗色情",
    r"淫秽",
    r"裸体",
    r"裸照",
    r"露点",
    r"性行为",
    r"性爱",
    r"\bporn\w*\b",
    r"\bxxx\b",
    r"\bnsfw\b",
    r"\bnude\w*\b",
    r"\bnaked\b",
    r"\bsexual\s+content\b",
]

_SOFT_ADULT_PATTERNS: list[str] = []

_NEGATION_RE = re.compile(r"(未|无|不|非|不是|没有|并无|并非|未发现|未涉及|未观察到|不涉及).{0,10}$")
_AFTER_NEGATION_RE = re.compile(r"^.{0,8}(未|无|不|非|不是|没有|并无|并非|未发现|未涉及|未观察到|不涉及).{0,14}(风险|高风险|内容|明显)")
_INTENSIFIER_RE = re.compile(r"(明显|严重|高风险|强|大量|大尺度|色情|低俗|裸露|成人|黄片)")


def _iter_text_parts(row: Dict[str, Any]) -> Iterable[str]:
    for key in (
        "analysis",
        "title",
        "body",
        "effect_one_liner",
        "ad_one_liner",
        "style_filter_match_summary",
    ):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            yield value

    tags = row.get("material_tags")
    if isinstance(tags, list):
        for tag in tags:
            if isinstance(tag, str) and tag.strip():
                yield tag


def _is_negated(text: str, start: int, end: int) -> bool:
    before = text[max(0, start - 16):start]
    after = text[end:min(len(text), end + 16)]
    return bool(
        _NEGATION_RE.search(before)
        or _AFTER_NEGATION_RE.match(after)
        or re.match(r".{0,6}(风险较低|风险不高|低风险)", after)
    )


def adult_content_match(row: Dict[str, Any]) -> Dict[str, Any] | None:
    """Return a high-confidence adult-content match for a row, if present."""
    text = "\n".join(_iter_text_parts(row))
    if not text.strip():
        return None

    lowered = text.lower()
    for pattern in _ADULT_PATTERNS:
        for m in re.finditer(pattern, lowered, flags=re.I):
            if _is_negated(lowered, m.start(), m.end()):
                continue
            snippet = text[max(0, m.start() - 24):min(len(text), m.end() + 24)].replace("\n", " ")
            return {"pattern": pattern, "snippet": snippet.strip()}

    for pattern in _SOFT_ADULT_PATTERNS:
        for m in re.finditer(pattern, lowered, flags=re.I):
            if _is_negated(lowered, m.start(), m.end()):
                continue
            window = lowered[max(0, m.start() - 24):min(len(lowered), m.end() + 24)]
            if not _INTENSIFIER_RE.search(window):
                continue
            snippet = text[max(0, m.start() - 24):min(len(text), m.end() + 24)].replace("\n", " ")
            return {"pattern": pattern, "snippet": snippet.strip()}

    return None


def _append_tag(row: Dict[str, Any], tag: str) -> None:
    tags = row.get("material_tags")
    if not isinstance(tags, list):
        tags = []
    if tag not in tags:
        tags.append(tag)
    row["material_tags"] = tags


def apply_adult_content_filter(rows: List[Dict[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Mark adult/pornographic material before it reaches Bitable or direction cards.

    The matcher intentionally reads multiple fields so a risky material is still caught
    when the short effect/core-selling-point line misses the adult signal.
    """
    details: List[Dict[str, Any]] = []
    newly_marked = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        match = adult_content_match(row)
        if not match:
            continue
        was_excluded = bool(row.get("exclude_from_bitable"))
        row["exclude_from_bitable"] = True
        row["exclude_from_cluster"] = True
        row["adult_content_filter_match"] = match
        _append_tag(row, ADULT_FILTER_TAG)
        if not was_excluded:
            newly_marked += 1
        details.append(
            {
                "ad_key": str(row.get("ad_key") or ""),
                "product": str(row.get("product") or ""),
                "pattern": match.get("pattern"),
                "snippet": match.get("snippet"),
                "newly_marked": not was_excluded,
            }
        )
    return newly_marked, details
