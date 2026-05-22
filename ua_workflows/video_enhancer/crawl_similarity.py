from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Dict, List
from urllib.parse import urlparse, urlunparse


def _ad_key(item: Dict[str, Any]) -> str:
    creative = item.get("creative") or {}
    if not isinstance(creative, dict):
        return ""
    return str(creative.get("ad_key") or "").strip()


def _scope(item: Dict[str, Any]) -> str:
    creative = item.get("creative") or {}
    if not isinstance(creative, dict):
        creative = {}
    return str(
        item.get("appid")
        or creative.get("appid")
        or item.get("product")
        or creative.get("advertiser_name")
        or ""
    ).strip()


def _canonical_url(value: Any) -> str:
    url = str(value or "").strip()
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        path = re.sub(r"\.image$", ".jpg", parsed.path, flags=re.I)
        return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))
    except Exception:
        return url


def _first_video_url(creative: Dict[str, Any]) -> str:
    if creative.get("video_url"):
        return str(creative.get("video_url") or "").strip()
    for row in creative.get("resource_urls") or []:
        if isinstance(row, dict) and row.get("video_url"):
            return str(row.get("video_url") or "").strip()
    return ""


def _crawl_signature(item: Dict[str, Any]) -> str:
    creative = item.get("creative") or {}
    if not isinstance(creative, dict):
        return ""
    scope = _scope(item)
    if not scope:
        return ""
    ahash = str(creative.get("image_ahash_md5") or "").strip().lower()
    if ahash and ahash not in {"none", "null", "-"}:
        return f"{scope}::ahash::{ahash}"
    cover = _canonical_url(creative.get("preview_img_url"))
    if cover:
        return f"{scope}::cover::{cover}"
    video = _canonical_url(_first_video_url(creative))
    if video:
        return f"{scope}::video::{video}"
    return ""


def _as_count(value: Any) -> int:
    try:
        return max(1, int(value or 1))
    except Exception:
        return 1


def _set_count(item: Dict[str, Any], count: int) -> None:
    n = _as_count(count)
    item["crawl_similarity_count"] = n
    creative = item.get("creative")
    if isinstance(creative, dict):
        creative["crawl_similarity_count"] = n


def _item_count(item: Dict[str, Any]) -> int:
    creative = item.get("creative") or {}
    if not isinstance(creative, dict):
        creative = {}
    return max(
        _as_count(item.get("crawl_similarity_count")),
        _as_count(creative.get("crawl_similarity_count")),
        _as_count(creative.get("daily_similarity_count")),
    )


def _materialize_count_map(raw_payload: Dict[str, Any]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for item in raw_payload.get("items") or []:
        if not isinstance(item, dict):
            continue
        ad_key = _ad_key(item)
        if not ad_key:
            continue
        count = _item_count(item)
        _set_count(item, count)
        out[ad_key] = count
    raw_payload["crawl_similarity_count_by_ad"] = out
    return out


def annotate_crawl_similarity_counts(raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Add first-pass same-day similarity counts to the raw crawl payload.

    This stage only uses crawl-time signals available before analysis: same
    product/app scope plus exact cover ahash, cover URL, or video URL. Later
    stages can raise the count with CLIP clusters or template dedupe evidence.
    """
    items = [x for x in (raw_payload.get("items") or []) if isinstance(x, dict)]
    counts: Dict[str, int] = {}
    groups: Dict[str, List[str]] = defaultdict(list)
    for item in items:
        ad_key = _ad_key(item)
        if not ad_key:
            continue
        counts[ad_key] = max(1, _item_count(item))
        sig = _crawl_signature(item)
        if sig:
            groups[sig].append(ad_key)

    signature_groups: List[Dict[str, Any]] = []
    for sig, ad_keys in groups.items():
        unique = sorted(set(ad_keys))
        if len(unique) <= 1:
            continue
        for ad_key in unique:
            counts[ad_key] = max(counts.get(ad_key, 1), len(unique))
        signature_groups.append(
            {
                "source": "crawl_signature",
                "signature": sig[:240],
                "member_count": len(unique),
                "member_ad_keys": unique,
            }
        )

    for item in items:
        ad_key = _ad_key(item)
        if ad_key:
            _set_count(item, counts.get(ad_key, 1))

    raw_payload["crawl_similarity_count_by_ad"] = {
        ad_key: int(counts.get(ad_key, 1)) for ad_key in sorted(counts)
    }
    raw_payload["crawl_similarity_report"] = {
        "source": "crawl",
        "item_count": len(items),
        "max_similarity_count": max(counts.values()) if counts else 0,
        "groups": signature_groups,
    }
    return raw_payload


def merge_cover_similarity_counts(raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fold same-day CLIP cover clusters into crawl similarity counts.

    Cross-day cover matches intentionally do not increase same-day similarity
    count; they are old-material evidence, not today's batch size.
    """
    annotate_crawl_similarity_counts(raw_payload)
    counts = _materialize_count_map(raw_payload)
    cover_report = raw_payload.get("cover_style_intraday_report") or {}
    if not isinstance(cover_report, dict):
        return raw_payload

    grouped: Dict[str, set[str]] = defaultdict(set)
    per_appid = cover_report.get("per_appid") or []
    buckets = per_appid.values() if isinstance(per_appid, dict) else per_appid
    if not isinstance(buckets, list) and not hasattr(buckets, "__iter__"):
        buckets = []
    for bucket in buckets:
        if not isinstance(bucket, dict):
            continue
        for row in bucket.get("removed") or []:
            if not isinstance(row, dict):
                continue
            if str(row.get("reason") or "").strip() not in (
                "cover_style_cluster",
                "cover_style_cluster_history_refresh",
            ):
                continue
            kept = str(row.get("kept_ad_key") or "").strip()
            removed = str(row.get("ad_key") or "").strip()
            if not kept:
                continue
            grouped[kept].add(kept)
            if removed:
                grouped[kept].add(removed)

    clip_groups: List[Dict[str, Any]] = []
    for kept, members in sorted(grouped.items()):
        counts[kept] = max(int(counts.get(kept, 1)), len(members))
        clip_groups.append(
            {
                "source": "cover_style_intraday",
                "representative_ad_key": kept,
                "member_count": len(members),
                "member_ad_keys": sorted(members),
            }
        )

    for item in raw_payload.get("items") or []:
        if not isinstance(item, dict):
            continue
        ad_key = _ad_key(item)
        if ad_key:
            _set_count(item, counts.get(ad_key, 1))

    raw_payload["crawl_similarity_count_by_ad"] = {
        ad_key: int(counts.get(ad_key, 1)) for ad_key in sorted(counts)
    }
    report = raw_payload.get("crawl_similarity_report")
    if not isinstance(report, dict):
        report = {}
    existing_groups = [g for g in (report.get("groups") or []) if isinstance(g, dict)]
    report.update(
        {
            "source": "crawl_and_cover",
            "item_count": len([x for x in (raw_payload.get("items") or []) if isinstance(x, dict)]),
            "max_similarity_count": max(counts.values()) if counts else 0,
            "groups": existing_groups + clip_groups,
        }
    )
    raw_payload["crawl_similarity_report"] = report
    return raw_payload


def build_crawl_similarity_count_map(
    raw_payload: Dict[str, Any],
    items: List[Dict[str, Any]],
) -> Dict[str, int]:
    raw_map = raw_payload.get("crawl_similarity_count_by_ad")
    count_by_ad = raw_map if isinstance(raw_map, dict) else {}
    out: Dict[str, int] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        ad_key = _ad_key(item)
        if not ad_key:
            continue
        out[ad_key] = max(_as_count(count_by_ad.get(ad_key)), _item_count(item))
    return out
