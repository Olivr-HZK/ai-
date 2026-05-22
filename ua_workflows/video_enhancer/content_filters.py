"""Content safety filters shared by the Video Enhancer pipeline and sync step."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Tuple


ADULT_FILTER_TAG = "色情/成人风险"
HUMAN_PHOTO_EFFECT_FILTER_TAG = "非人物照片加工特效"

_ADULT_PATTERNS = [
    r"黄片",
    r"色情",
    r"成人视频",
    r"成人影片",
    r"成人视频聊天",
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
    r"\bvideo\s*chat\b",
    r"\bvideochat\b",
    r"\bchat\s+for\s+free\b",
    r"\bno\s+ghosting\b",
    r"\bdating\b",
    r"\bbusty\b",
    r"免费聊天",
    r"视频聊天",
    r"附近.{0,8}(嫂子|美女|女性)",
    r"交友.{0,12}(下载|聊天|约会|附近|视频)",
    r"约会.{0,12}(下载|聊天|附近|视频)",
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
        "play_fingerprint",
        "differentiator",
        "template_fingerprint",
        "play_asset_name",
        "play_asset_variant_name",
        "play_asset_classification_reason",
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
        row["risk_level"] = "高风险"
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


_ECOMMERCE_PATTERNS = [
    r"商品",
    r"产品图",
    r"商品图",
    r"产品照片",
    r"电商",
    r"带货",
    r"购物",
    r"促销",
    r"折扣",
    r"开箱",
    r"sku",
    r"amazon",
    r"tiktok\s*shop",
    r"shopify",
    r"e-?commerce",
    r"\bproduct\s+(photo|image|ad|ads|video|showcase)\b",
]

_NON_HUMAN_SUBJECT_PATTERNS = [
    r"宠物",
    r"猫",
    r"狗",
    r"动物",
    r"房间",
    r"家装",
    r"装修",
    r"室内设计",
    r"空房",
    r"风景",
    r"食物",
    r"餐食",
    r"车辆",
    r"汽车",
    r"纯文字",
    r"logo",
    r"图标",
    r"海报",
    r"\bpet\b",
    r"\bcat\b",
    r"\bdog\b",
    r"\banimal\b",
    r"\broom\b",
    r"\binterior\b",
    r"\bfood\b",
    r"\bcar\b",
]

_HUMAN_PHOTO_PATTERNS = [
    r"用户上传.{0,12}(人|真人|人物|人像|自拍|面部|脸)",
    r"(上传|选择|导入).{0,10}(自拍|人像|真人|人物|面部|脸|单人照片|双人照片|合影)",
    r"自拍",
    r"人像",
    r"真人",
    r"人物",
    r"人脸",
    r"面部",
    r"脸部",
    r"单人照片",
    r"双人照片",
    r"合影",
    r"肖像",
    r"头像",
    r"写真",
    r"换脸",
    r"换装",
    r"变身",
    r"变老",
    r"变年轻",
    r"性别转换",
    r"发型",
    r"妆容",
    r"身体",
    r"\bselfie\b",
    r"\bportrait\b",
    r"\bface\b",
    r"\bphoto\s+of\s+(a\s+)?(person|people|man|woman)\b",
]


def _first_pattern_match(text: str, patterns: list[str]) -> Dict[str, Any] | None:
    lowered = text.lower()
    for pattern in patterns:
        for m in re.finditer(pattern, lowered, flags=re.I):
            if _is_negated(lowered, m.start(), m.end()):
                continue
            snippet = text[max(0, m.start() - 24):min(len(text), m.end() + 24)].replace("\n", " ")
            return {"pattern": pattern, "snippet": snippet.strip()}
    return None


def human_photo_effect_mismatch(row: Dict[str, Any]) -> Dict[str, Any] | None:
    """Return a mismatch when the effect is not based on editing a user's human photo."""
    text = "\n".join(_iter_text_parts(row))
    if not text.strip():
        return None

    ecommerce = _first_pattern_match(text, _ECOMMERCE_PATTERNS)
    if ecommerce:
        return {"reason": "ecommerce_effect", **ecommerce}

    non_human = _first_pattern_match(text, _NON_HUMAN_SUBJECT_PATTERNS)
    human = _first_pattern_match(text, _HUMAN_PHOTO_PATTERNS)
    if non_human and not human:
        return {"reason": "non_human_photo_effect", **non_human}
    if not human:
        return {
            "reason": "missing_human_photo_input",
            "pattern": "",
            "snippet": text[:80].replace("\n", " ").strip(),
        }
    return None


def apply_human_photo_effect_filter(rows: List[Dict[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Keep only effects that start from a user-uploaded human photo.

    VE no longer wants ecommerce/product, pet, room, scenery, text/logo, or other
    non-person effects in Bitable or daily push output.
    """
    details: List[Dict[str, Any]] = []
    newly_marked = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        match = human_photo_effect_mismatch(row)
        if not match:
            continue
        was_excluded = bool(row.get("exclude_from_bitable"))
        row["exclude_from_bitable"] = True
        row["exclude_from_cluster"] = True
        row["human_photo_effect_filter_match"] = match
        _append_tag(row, HUMAN_PHOTO_EFFECT_FILTER_TAG)
        if not was_excluded:
            newly_marked += 1
        details.append(
            {
                "ad_key": str(row.get("ad_key") or ""),
                "product": str(row.get("product") or ""),
                "reason": match.get("reason"),
                "pattern": match.get("pattern"),
                "snippet": match.get("snippet"),
                "newly_marked": not was_excluded,
            }
        )
    return newly_marked, details
