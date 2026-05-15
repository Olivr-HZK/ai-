"""Daily reporting helpers for the VE play asset library."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from typing import Any

from ua_workflows.shared.db.video_enhancer import (
    _get_conn,
    init_db,
    load_daily_material_report,
    normalize_effect_one_liner,
)
from ua_workflows.video_enhancer.play_assets import load_play_assets, match_play_asset


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
                   d.effect_one_liner, d.play_fingerprint, d.differentiator,
                   d.material_tags
            FROM daily_creative_insights d
            LEFT JOIN creative_library cl ON cl.ad_key = d.ad_key
            WHERE d.target_date < ?
              {lower_clause}
              AND (
                COALESCE(TRIM(d.effect_one_liner), '') <> ''
                OR COALESCE(TRIM(d.play_fingerprint), '') <> ''
                OR COALESCE(TRIM(d.differentiator), '') <> ''
              )
            """,
            params,
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


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
            item["play_asset_novelty_label"] = "新玩法"
        elif variant_is_new:
            item["play_asset_novelty_label"] = "新变种"
        else:
            item["play_asset_novelty_label"] = "已沉淀"
    return items


def _cluster_new_asset_variant_items(items: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        if not isinstance(item, dict):
            continue
        asset_new = bool(item.get("play_asset_is_new"))
        variant_new = bool(item.get("play_asset_variant_is_new"))
        if not asset_new and not variant_new:
            continue
        if asset_new and str(item.get("play_asset_id") or "").strip():
            group_key = f"asset::{item.get('play_asset_id')}"
        else:
            group_key = f"variant::{item.get('play_asset_variant_key') or item.get('ad_key')}"
        grouped[group_key].append(item)

    representatives: list[dict[str, Any]] = []
    clusters: list[dict[str, Any]] = []
    for group_key, rows in grouped.items():
        rows_sorted = sorted(rows, key=_item_score, reverse=True)
        representative = dict(rows_sorted[0])
        label = "新玩法" if representative.get("play_asset_is_new") else "新变种"
        representative["daily_asset_variant_cluster_key"] = group_key
        representative["cluster_material_count"] = len(rows_sorted)
        representative["play_asset_novelty_label"] = label
        representative["asset_variant_cluster_members"] = [
            {
                "ad_key": str(row.get("ad_key") or ""),
                "product": str(row.get("product") or ""),
                "play_asset_name": str(row.get("play_asset_name") or ""),
                "play_asset_variant_name": str(row.get("play_asset_variant_name") or ""),
                "effect_one_liner": str(row.get("effect_one_liner") or ""),
                "play_fingerprint": str(row.get("play_fingerprint") or ""),
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

    summary = dict(report.get("summary") or {})
    summary.update(
        {
            "new_play_asset_count": len(new_asset_ids),
            "new_play_variant_count": len(new_variant_keys),
            "new_asset_variant_representative_count": len(representatives),
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
    report["summary"] = summary
    return report
