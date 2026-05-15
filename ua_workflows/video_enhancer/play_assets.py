"""Human-curated VE play asset library helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ua_workflows.shared.config import DATA_DIR, PROJECT_ROOT
from ua_workflows.shared.db.video_enhancer import normalize_effect_one_liner

DEFAULT_PLAY_ASSET_PATH = PROJECT_ROOT / "config" / "ve_play_assets.json"
LEGACY_PLAY_ASSET_PATH = DATA_DIR / "ve_play_assets.json"


def load_play_assets(path: Path | None = None) -> list[dict[str, Any]]:
    asset_path = path or DEFAULT_PLAY_ASSET_PATH
    if path is None and not asset_path.exists() and LEGACY_PLAY_ASSET_PATH.exists():
        asset_path = LEGACY_PLAY_ASSET_PATH
    try:
        payload = json.loads(asset_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    assets = payload.get("assets") if isinstance(payload, dict) else []
    if not isinstance(assets, list):
        return []
    return [asset for asset in assets if isinstance(asset, dict) and asset.get("asset_id")]


def format_play_asset_catalog_for_prompt(
    *,
    assets: list[dict[str, Any]] | None = None,
    max_aliases: int = 8,
    max_chars: int = 16000,
) -> str:
    """Render a compact asset catalog for the analysis prompt.

    The model only needs stable IDs, names, definitions, aliases, and subtag IDs;
    detailed keyword lists stay in JSON for deterministic fallback matching.
    """
    assets = assets if assets is not None else load_play_assets()
    lines: list[str] = []
    for asset in assets:
        if str(asset.get("status") or "active").strip().lower() not in ("active", ""):
            continue
        asset_id = str(asset.get("asset_id") or "").strip()
        name = str(asset.get("name") or "").strip()
        definition = str(asset.get("definition") or "").strip()
        aliases = [str(x).strip() for x in asset.get("aliases") or [] if str(x).strip()]
        alias_text = "、".join(aliases[:max_aliases])
        subtags = []
        for subtag in asset.get("subtags") or []:
            if not isinstance(subtag, dict):
                continue
            tag_id = str(subtag.get("tag_id") or "").strip()
            tag_name = str(subtag.get("name") or "").strip()
            if tag_id and tag_name:
                subtags.append(f"{tag_id}={tag_name}")
        subtag_text = "；".join(subtags) if subtags else "无"
        line = f"- {asset_id} | {name} | 定义：{definition} | 别名：{alias_text or '-'} | 变种：{subtag_text}"
        lines.append(line)

    text = "\n".join(lines)
    if len(text) > max_chars:
        return text[: max(1, max_chars - 20)].rstrip() + "\n- ...（资产库过长，已截断）"
    return text


def play_asset_text(row: dict[str, Any], evidence: dict[str, Any] | None = None) -> str:
    evidence = evidence or {}
    fields = (
        "product",
        "effect_one_liner",
        "play_fingerprint",
        "differentiator",
        "title",
        "body",
        "ad_one_liner",
    )
    parts: list[str] = []
    for source in (row, evidence):
        for field in fields:
            value = str(source.get(field) or "").strip()
            if value:
                parts.append(value)
    return " ".join(parts)


def _token_matches(raw_text: str, compact_text: str, tokens: list[Any]) -> list[str]:
    hits: list[str] = []
    raw_lower = raw_text.lower()
    for token0 in tokens:
        token = str(token0 or "").strip()
        if not token:
            continue
        compact_token = normalize_effect_one_liner(token)
        if token.lower() in raw_lower or (compact_token and compact_token in compact_text):
            hits.append(token)
    return hits


def _match_subtags(asset: dict[str, Any], raw_text: str, compact_text: str) -> list[dict[str, Any]]:
    matched: list[dict[str, Any]] = []
    for subtag in asset.get("subtags") or []:
        if not isinstance(subtag, dict):
            continue
        keywords = list(subtag.get("keywords") or [])
        hits = _token_matches(raw_text, compact_text, keywords)
        if not hits:
            continue
        try:
            min_score = int(subtag.get("min_score") or 1)
        except (TypeError, ValueError):
            min_score = 1
        if len(set(hits)) < min_score:
            continue
        matched.append(
            {
                "tag_id": str(subtag.get("tag_id") or ""),
                "name": str(subtag.get("name") or ""),
                "category": str(subtag.get("category") or ""),
                "matched_keywords": sorted(set(hits)),
            }
        )
    matched.sort(key=lambda item: (item.get("category") or "", item.get("name") or ""))
    return matched


def match_play_asset(
    row: dict[str, Any],
    *,
    assets: list[dict[str, Any]] | None = None,
    evidence: dict[str, Any] | None = None,
    min_score: int = 2,
) -> dict[str, Any] | None:
    assets = assets if assets is not None else load_play_assets()
    text = play_asset_text(row, evidence)
    compact = normalize_effect_one_liner(text)
    if not compact:
        return None

    best: dict[str, Any] | None = None
    for asset in assets:
        exclude_hits = _token_matches(text, compact, list(asset.get("exclude_keywords") or []))
        include_hits = _token_matches(
            text,
            compact,
            [
                *(asset.get("include_keywords") or []),
                *(asset.get("aliases") or []),
                asset.get("name") or "",
            ],
        )
        example_hits = _token_matches(text, compact, list(asset.get("example_effects") or []))
        score = len(set(include_hits)) + min(2, len(set(example_hits)))
        if exclude_hits:
            score -= max(2, len(set(exclude_hits)))
        try:
            asset_min_score = int(asset.get("min_score") or min_score)
        except (TypeError, ValueError):
            asset_min_score = min_score
        if score < asset_min_score:
            continue
        candidate = {
            "asset_id": str(asset.get("asset_id") or ""),
            "name": str(asset.get("name") or ""),
            "score": score,
            "confidence": "高" if score >= 4 else "中",
            "matched_keywords": sorted(set(include_hits + example_hits)),
            "exclude_hits": sorted(set(exclude_hits)),
            "matched_subtags": _match_subtags(asset, text, compact),
        }
        if best is None or int(candidate["score"]) > int(best["score"]):
            best = candidate
    return best
