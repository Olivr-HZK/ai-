"""Daily reporting helpers for the VE play asset library."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
import re
from typing import Any

from ua_workflows.shared.db.video_enhancer import (
    _get_conn,
    init_db,
    load_daily_material_report,
    normalize_effect_one_liner,
)
from ua_workflows.video_enhancer.play_assets import load_play_assets, match_play_asset


REPORTABLE_NARROW_LABELS = {"新玩法", "老玩法新迭代"}


def _item_score(item: dict[str, Any]) -> tuple[int, int, int, str]:
    def as_int(key: str) -> int:
        try:
            return int(float(item.get(key) or 0))
        except (TypeError, ValueError):
            return 0

    return (
        as_int("best_all_exposure_value") or as_int("all_exposure_value"),
        as_int("best_impression") or as_int("impression"),
        as_int("best_heat") or as_int("heat"),
        str(item.get("ad_key") or ""),
    )


def _fallback_variant_key(item: dict[str, Any]) -> str:
    text = " ".join(
        str(item.get(key) or "").strip()
        for key in (
            "play_fingerprint",
            "effect_one_liner",
            "differentiator",
            "title",
            "body",
        )
        if str(item.get(key) or "").strip()
    )
    compact = normalize_effect_one_liner(text)
    if compact:
        return f"unmatched::{compact[:80]}"
    return f"unmatched::{str(item.get('ad_key') or '')[:16]}"


def _split_label_list(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    return [x.strip() for x in re.split(r"[、,，;/；\s]+", text) if x.strip()]


def _asset_lookup(assets: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(asset.get("asset_id") or "").strip(): asset for asset in assets if asset.get("asset_id")}


def _subtag_lookup(asset: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for subtag in asset.get("subtags") or []:
        if isinstance(subtag, dict) and subtag.get("tag_id"):
            rows[str(subtag.get("tag_id") or "").strip()] = subtag
    return rows


def _new_variant_key(item: dict[str, Any], asset_id: str, variant_name: str) -> str:
    compact = normalize_effect_one_liner(
        " ".join(
            str(item.get(key) or "").strip()
            for key in ("play_fingerprint", "effect_one_liner", "differentiator")
            if str(item.get(key) or "").strip()
        )
        or variant_name
    )
    suffix = compact[:80] if compact else str(item.get("ad_key") or "")[:16]
    return f"{asset_id}::new_variant::{suffix}"


def _normalized_key(text: Any, max_chars: int = 140) -> str:
    compact = normalize_effect_one_liner(str(text or ""))
    return compact[:max_chars] if compact else ""


def _stable_play_key(item: dict[str, Any]) -> str:
    asset_id = str(item.get("play_asset_id") or "").strip()
    if asset_id:
        return f"asset::{asset_id}"
    text = " ".join(
        str(item.get(key) or "").strip()
        for key in ("play_asset_name", "play_fingerprint", "effect_one_liner")
        if str(item.get(key) or "").strip()
    )
    compact = _normalized_key(text, 120)
    if compact:
        return f"text::{compact}"
    variant_key = str(item.get("play_asset_variant_key") or "").strip()
    if variant_key.startswith("new_play::") or variant_key.startswith("unmatched::"):
        return variant_key[:160]
    return ""


def _template_key(item: dict[str, Any]) -> str:
    template = str(item.get("template_fingerprint") or "").strip()
    if template and template not in {"无", "None", "none", "-"}:
        compact = _normalized_key(template)
        if compact:
            return compact
    return ""


def _apply_ai_asset_choice(item: dict[str, Any], assets: list[dict[str, Any]]) -> bool:
    raw_asset_id = str(item.get("play_asset_id") or "").strip()
    raw_label = str(item.get("play_asset_novelty_label") or "").strip()
    if not raw_asset_id and not raw_label:
        return False

    normalized_asset_id = raw_asset_id.lower()
    if normalized_asset_id in {"new_play", "新玩法", "待沉淀", "none", "null", "-"} or raw_label == "新玩法":
        variant_key = _fallback_variant_key(item)
        proposed_name = str(item.get("play_asset_name") or "").strip()
        proposed_variant = str(item.get("play_asset_subtag_names") or proposed_name or "待沉淀变种").strip()
        if proposed_name or proposed_variant:
            compact = normalize_effect_one_liner(
                " ".join(
                    x
                    for x in (
                        proposed_name,
                        proposed_variant,
                        str(item.get("play_fingerprint") or ""),
                        str(item.get("effect_one_liner") or ""),
                    )
                    if x
                )
            )
            if compact:
                variant_key = f"new_play::{compact[:80]}"
        item["play_asset_id"] = ""
        item["play_asset_name"] = proposed_name or "待沉淀"
        item["play_asset_confidence"] = "AI"
        item["play_asset_matched_keywords"] = ""
        item["play_asset_subtag_ids"] = ""
        item["play_asset_subtag_names"] = proposed_variant
        item["play_asset_variant_key"] = variant_key
        item["play_asset_variant_name"] = proposed_variant
        item["play_asset_match_source"] = "ai"
        return True

    assets_by_id = _asset_lookup(assets)
    asset = assets_by_id.get(raw_asset_id)
    if not asset:
        return False

    subtag_by_id = _subtag_lookup(asset)
    raw_subtag_ids = _split_label_list(item.get("play_asset_subtag_ids"))
    known_subtag_ids = [tag_id for tag_id in raw_subtag_ids if tag_id in subtag_by_id]
    new_variant_requested = any(tag.lower() in {"new_variant", "新变种", "待沉淀变种"} for tag in raw_subtag_ids) or raw_label == "新变种"

    subtag_names = [
        str(subtag_by_id[tag_id].get("name") or "").strip()
        for tag_id in known_subtag_ids
        if str(subtag_by_id[tag_id].get("name") or "").strip()
    ]
    ai_names = _split_label_list(item.get("play_asset_subtag_names"))
    variant_name = "、".join(subtag_names) if subtag_names else "、".join(ai_names)
    if not variant_name:
        variant_name = "待沉淀变种" if new_variant_requested else "基础变体"

    if known_subtag_ids:
        variant_key = f"{raw_asset_id}::{','.join(known_subtag_ids)}"
    elif new_variant_requested:
        variant_key = _new_variant_key(item, raw_asset_id, variant_name)
    else:
        variant_key = f"{raw_asset_id}::base"

    item["play_asset_id"] = raw_asset_id
    item["play_asset_name"] = str(item.get("play_asset_name") or asset.get("name") or "").strip()
    item["play_asset_confidence"] = "AI"
    item["play_asset_matched_keywords"] = ""
    item["play_asset_subtag_ids"] = "、".join(known_subtag_ids)
    item["play_asset_subtag_names"] = variant_name if known_subtag_ids else ("" if variant_name == "基础变体" else variant_name)
    item["play_asset_variant_key"] = variant_key
    item["play_asset_variant_name"] = variant_name
    item["play_asset_match_source"] = "ai"
    return True


def _apply_asset_match(item: dict[str, Any], match: dict[str, Any] | None) -> None:
    if not match:
        variant_key = _fallback_variant_key(item)
        item["play_asset_id"] = ""
        item["play_asset_name"] = "待沉淀"
        item["play_asset_confidence"] = ""
        item["play_asset_matched_keywords"] = ""
        item["play_asset_subtag_ids"] = ""
        item["play_asset_subtag_names"] = ""
        item["play_asset_variant_key"] = variant_key
        item["play_asset_variant_name"] = "待沉淀变种"
        return

    subtag_rows = [
        subtag
        for subtag in (match.get("matched_subtags") or [])
        if isinstance(subtag, dict) and str(subtag.get("tag_id") or "").strip()
    ]
    subtag_ids = [str(subtag.get("tag_id") or "").strip() for subtag in subtag_rows]
    subtag_names = [str(subtag.get("name") or "").strip() for subtag in subtag_rows if str(subtag.get("name") or "").strip()]
    asset_id = str(match.get("asset_id") or "").strip()
    variant_suffix = ",".join(subtag_ids) if subtag_ids else "base"

    item["play_asset_id"] = asset_id
    item["play_asset_name"] = str(match.get("name") or "").strip()
    item["play_asset_confidence"] = str(match.get("confidence") or "").strip()
    item["play_asset_matched_keywords"] = "、".join(str(x) for x in match.get("matched_keywords") or [] if x)
    item["play_asset_subtag_ids"] = "、".join(subtag_ids)
    item["play_asset_subtag_names"] = "、".join(subtag_names)
    item["play_asset_variant_key"] = f"{asset_id}::{variant_suffix}" if asset_id else _fallback_variant_key(item)
    item["play_asset_variant_name"] = "、".join(subtag_names) if subtag_names else "基础变体"


def annotate_items_with_play_assets(
    items: list[dict[str, Any]],
    *,
    assets: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    assets = assets if assets is not None else load_play_assets()
    for item in items:
        if not isinstance(item, dict):
            continue
        if _apply_ai_asset_choice(item, assets):
            continue
        _apply_asset_match(item, match_play_asset(item, assets=assets, evidence=item))
    return items


def _historical_rows(
    target_date: str,
    *,
    history_lookback_days: int = 0,
) -> list[dict[str, Any]]:
    if not target_date:
        return []

    params: list[Any] = [target_date]
    lower_clause = ""
    if history_lookback_days > 0:
        try:
            lower_date = (date.fromisoformat(target_date) - timedelta(days=history_lookback_days)).isoformat()
        except ValueError:
            lower_date = ""
        if lower_date:
            lower_clause = "AND d.target_date >= ?"
            params.append(lower_date)

    init_db()
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT d.target_date, d.ad_key,
                   COALESCE(NULLIF(d.product, ''), cl.product, '') AS product,
                   COALESCE(NULLIF(d.appid, ''), cl.appid, '') AS appid,
                   COALESCE(cl.title, '') AS title,
                   COALESCE(cl.body, '') AS body,
                   d.effect_one_liner, d.play_fingerprint, d.differentiator, d.template_fingerprint,
                   d.play_asset_id, d.play_asset_name, d.play_asset_subtag_ids, d.play_asset_subtag_names,
                   d.play_asset_novelty_label, d.play_asset_match_source, d.play_asset_classification_reason,
                   d.material_tags
            FROM daily_creative_insights d
            LEFT JOIN creative_library cl ON cl.ad_key = d.ad_key
            WHERE d.target_date < ?
              {lower_clause}
              AND (
                COALESCE(TRIM(d.effect_one_liner), '') <> ''
                OR COALESCE(TRIM(d.play_fingerprint), '') <> ''
                OR COALESCE(TRIM(d.differentiator), '') <> ''
                OR COALESCE(TRIM(d.template_fingerprint), '') <> ''
              )
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def _annotate_narrow_novelty(
    items: list[dict[str, Any]],
    history: list[dict[str, Any]],
    target_date: str,
) -> None:
    play_first_seen: dict[str, str] = {}
    templates_by_play: dict[str, set[str]] = defaultdict(set)

    for row in history:
        if not isinstance(row, dict):
            continue
        play_key = _stable_play_key(row)
        if not play_key:
            continue
        seen_date = str(row.get("target_date") or "").strip()
        if seen_date and (play_key not in play_first_seen or seen_date < play_first_seen[play_key]):
            play_first_seen[play_key] = seen_date
        template_key = _template_key(row)
        if template_key:
            templates_by_play[play_key].add(template_key)

    for item in items:
        if not isinstance(item, dict):
            continue
        play_key = _stable_play_key(item)
        template_key = _template_key(item)
        has_play_history = bool(play_key and play_key in play_first_seen)
        historical_templates = templates_by_play.get(play_key, set()) if play_key else set()
        asset_is_new = bool(item.get("play_asset_is_new"))
        variant_is_new = bool(item.get("play_asset_variant_is_new"))
        broad_label = str(item.get("asset_variant_novelty_label") or item.get("play_asset_novelty_label") or "").strip()

        if asset_is_new or (not has_play_history and (variant_is_new or broad_label == "新玩法")):
            label = "新玩法"
            reason = "稳定玩法在历史库中未出现，按狭义新玩法处理。"
        elif template_key and template_key not in historical_templates:
            label = "老玩法新迭代"
            reason = "稳定玩法已出现，但模板指纹首次出现；同模板仅换人种/性别不计为新。"
        elif template_key and template_key in historical_templates:
            label = "老玩法换皮"
            reason = "稳定玩法和模板指纹均已出现；人物属性或主体替换不计为新。"
        elif variant_is_new:
            label = "老玩法新迭代"
            reason = "稳定玩法已出现，变种键首次出现；模板指纹缺失，暂按老玩法新迭代。"
        elif has_play_history:
            label = "已沉淀玩法"
            reason = "稳定玩法已沉淀，且缺少新的模板结构证据。"
        else:
            label = "玩法待复核"
            reason = "缺少稳定玩法或模板指纹，无法自动判断狭义新旧。"

        item["stable_play_key"] = play_key
        item["template_fingerprint_key"] = template_key
        item["narrow_novelty_label"] = label
        item["narrow_novelty_reason"] = reason
        item["narrow_novelty_is_reportable"] = label in REPORTABLE_NARROW_LABELS
        item["play_asset_novelty_label"] = label


def annotate_daily_play_asset_novelty(
    items: list[dict[str, Any]],
    target_date: str,
    *,
    history_lookback_days: int = 0,
    assets: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    assets = assets if assets is not None else load_play_assets()
    annotate_items_with_play_assets(items, assets=assets)

    history = _historical_rows(target_date, history_lookback_days=history_lookback_days)
    annotate_items_with_play_assets(history, assets=assets)

    asset_first_seen: dict[str, str] = {}
    variant_first_seen: dict[str, str] = {}
    for row in history:
        seen_date = str(row.get("target_date") or "").strip()
        asset_id = str(row.get("play_asset_id") or "").strip()
        variant_key = str(row.get("play_asset_variant_key") or "").strip()
        if asset_id and (asset_id not in asset_first_seen or seen_date < asset_first_seen[asset_id]):
            asset_first_seen[asset_id] = seen_date
        if variant_key and (variant_key not in variant_first_seen or seen_date < variant_first_seen[variant_key]):
            variant_first_seen[variant_key] = seen_date

    for item in items:
        asset_id = str(item.get("play_asset_id") or "").strip()
        variant_key = str(item.get("play_asset_variant_key") or "").strip()
        asset_is_new = bool(asset_id) and asset_id not in asset_first_seen
        variant_is_new = bool(variant_key) and variant_key not in variant_first_seen
        if not asset_id and variant_key:
            asset_is_new = variant_is_new

        item["play_asset_is_new"] = asset_is_new
        item["play_asset_variant_is_new"] = variant_is_new
        item["play_asset_first_seen_date"] = target_date if asset_is_new else asset_first_seen.get(asset_id, "")
        item["play_asset_variant_first_seen_date"] = target_date if variant_is_new else variant_first_seen.get(variant_key, "")
        if asset_is_new:
            broad_label = "新玩法"
        elif variant_is_new:
            broad_label = "新变种"
        else:
            broad_label = "已沉淀"
        item["asset_variant_novelty_label"] = broad_label
        item["play_asset_novelty_label"] = broad_label
    _annotate_narrow_novelty(items, history, target_date)
    return items


def _cluster_new_asset_variant_items(items: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        if not isinstance(item, dict):
            continue
        label = str(item.get("narrow_novelty_label") or item.get("play_asset_novelty_label") or "").strip()
        if label not in REPORTABLE_NARROW_LABELS:
            continue
        play_key = str(item.get("stable_play_key") or _stable_play_key(item) or "").strip()
        template_key = str(item.get("template_fingerprint_key") or _template_key(item) or "").strip()
        if label == "新玩法":
            group_key = f"narrow_play::{play_key or item.get('play_asset_variant_key') or item.get('ad_key')}"
        else:
            group_key = f"narrow_template::{play_key or 'unknown'}::{template_key or item.get('play_asset_variant_key') or item.get('ad_key')}"
        grouped[group_key].append(item)

    representatives: list[dict[str, Any]] = []
    clusters: list[dict[str, Any]] = []
    for group_key, rows in grouped.items():
        rows_sorted = sorted(rows, key=_item_score, reverse=True)
        representative = dict(rows_sorted[0])
        label = str(representative.get("narrow_novelty_label") or representative.get("play_asset_novelty_label") or "老玩法新迭代")
        representative["daily_asset_variant_cluster_key"] = group_key
        representative["cluster_material_count"] = len(rows_sorted)
        representative["play_asset_novelty_label"] = label
        representative["narrow_novelty_label"] = label
        representative["asset_variant_cluster_members"] = [
            {
                "ad_key": str(row.get("ad_key") or ""),
                "product": str(row.get("product") or ""),
                "play_asset_name": str(row.get("play_asset_name") or ""),
                "play_asset_variant_name": str(row.get("play_asset_variant_name") or ""),
                "effect_one_liner": str(row.get("effect_one_liner") or ""),
                "play_fingerprint": str(row.get("play_fingerprint") or ""),
                "template_fingerprint": str(row.get("template_fingerprint") or ""),
                "narrow_novelty_label": str(row.get("narrow_novelty_label") or ""),
                "best_impression": int(row.get("best_impression") or row.get("impression") or 0),
                "best_all_exposure_value": int(row.get("best_all_exposure_value") or row.get("all_exposure_value") or 0),
                "is_representative": str(row.get("ad_key") or "") == str(representative.get("ad_key") or ""),
            }
            for row in rows_sorted
        ]
        representatives.append(representative)
        clusters.append(
            {
                "cluster_key": group_key,
                "novelty_label": label,
                "play_asset_id": representative.get("play_asset_id") or "",
                "play_asset_name": representative.get("play_asset_name") or "",
                "play_asset_variant_key": representative.get("play_asset_variant_key") or "",
                "play_asset_variant_name": representative.get("play_asset_variant_name") or "",
                "template_fingerprint": representative.get("template_fingerprint") or "",
                "narrow_novelty_reason": representative.get("narrow_novelty_reason") or "",
                "representative_ad_key": representative.get("ad_key") or "",
                "material_count": len(rows_sorted),
                "members": representative["asset_variant_cluster_members"],
            }
        )

    representatives.sort(key=_item_score, reverse=True)
    clusters.sort(
        key=lambda cluster: (
            int(max((member.get("best_all_exposure_value") or 0) for member in cluster.get("members", [])) or 0),
            int(max((member.get("best_impression") or 0) for member in cluster.get("members", [])) or 0),
            str(cluster.get("cluster_key") or ""),
        ),
        reverse=True,
    )
    return representatives, clusters


def build_daily_asset_variant_report(
    target_date: str,
    *,
    lookback_days: int = 7,
    history_lookback_days: int = 0,
) -> dict[str, Any]:
    report = load_daily_material_report(target_date, lookback_days=lookback_days)
    items = [item for item in report.get("candidate_items") or [] if isinstance(item, dict)]
    annotate_daily_play_asset_novelty(
        items,
        target_date,
        history_lookback_days=history_lookback_days,
    )
    representatives, clusters = _cluster_new_asset_variant_items(items)

    new_asset_ids = {
        str(item.get("play_asset_id") or "")
        for item in items
        if item.get("play_asset_is_new") and str(item.get("play_asset_id") or "").strip()
    }
    new_variant_keys = {
        str(item.get("play_asset_variant_key") or "")
        for item in items
        if item.get("play_asset_variant_is_new")
        and not item.get("play_asset_is_new")
        and str(item.get("play_asset_variant_key") or "").strip()
    }
    narrow_new_play_clusters = {
        str(cluster.get("cluster_key") or "")
        for cluster in clusters
        if str(cluster.get("novelty_label") or "") == "新玩法"
    }
    old_play_iteration_clusters = {
        str(cluster.get("cluster_key") or "")
        for cluster in clusters
        if str(cluster.get("novelty_label") or "") == "老玩法新迭代"
    }

    summary = dict(report.get("summary") or {})
    summary.update(
        {
            "new_play_asset_count": len(new_asset_ids),
            "new_play_variant_count": len(new_variant_keys),
            "new_asset_variant_representative_count": len(representatives),
            "narrow_new_play_count": len(narrow_new_play_clusters),
            "old_play_new_iteration_count": len(old_play_iteration_clusters),
            "narrow_new_representative_count": len(representatives),
            "old_play_reskin_material_count": sum(
                1 for item in items if str(item.get("narrow_novelty_label") or "") == "老玩法换皮"
            ),
            "narrow_reportable_material_count": sum(
                1 for item in items if item.get("narrow_novelty_is_reportable")
            ),
            "asset_variant_material_count": len(items),
            "known_asset_variant_material_count": sum(
                1
                for item in items
                if not item.get("play_asset_is_new") and not item.get("play_asset_variant_is_new")
            ),
        }
    )
    report["candidate_items"] = items
    report["asset_variant_items"] = items
    report["new_asset_variant_items"] = representatives
    report["new_asset_variant_clusters"] = clusters
    report["narrow_new_items"] = representatives
    report["narrow_new_clusters"] = clusters
    report["summary"] = summary
    return report
