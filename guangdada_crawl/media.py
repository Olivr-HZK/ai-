"""Local video artifacts for Guangdada crawl jobs."""

from __future__ import annotations

import json
import mimetypes
import re
import shutil
import zipfile
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from guangdada_crawl.jobs import CrawlJob
from ua_workflows.shared.media.resolve import normalize_video_url_for_consumption

Downloader = Callable[[str, Path], int]


def _first_string(obj: dict[str, Any], keys: Iterable[str]) -> str:
    for key in keys:
        value = obj.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _iter_resource_dicts(creative: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for key in ("resource_urls", "resourceUrls", "resources", "materials", "creative_resources"):
        value = creative.get(key)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    yield item


def _iter_string_list(creative: dict[str, Any], key: str) -> Iterable[str]:
    value = creative.get(key)
    if isinstance(value, str) and value.strip():
        yield value.strip()
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, str) and item.strip():
                yield item.strip()


def _is_videoish_url(url: str) -> bool:
    low = url.lower()
    if re.search(r"\.(mp4|mov|webm|m3u8|webp)(?:[?#]|$)", low):
        return True
    return "zingfront.com" in low and "/sp_opera/" in low


def pick_video_url_from_item(item: dict[str, Any]) -> str:
    """Return the best video URL from a raw item or nested creative row."""
    creative = item.get("creative") if isinstance(item.get("creative"), dict) else item
    if not isinstance(creative, dict):
        return ""

    direct = _first_string(
        creative,
        ("video_url", "videoUrl", "video", "play_url", "playUrl", "download_url", "downloadUrl"),
    )
    if direct:
        return normalize_video_url_for_consumption(direct)

    for resource in _iter_resource_dicts(creative):
        url = _first_string(resource, ("video_url", "videoUrl", "play_url", "playUrl", "download_url", "downloadUrl", "url"))
        if url and (_is_videoish_url(url) or resource.get("type") in (2, "2", "video")):
            return normalize_video_url_for_consumption(url)

    for key in ("cdn_url", "cdnUrl"):
        for url in _iter_string_list(creative, key):
            if _is_videoish_url(url):
                return normalize_video_url_for_consumption(url)

    top_direct = _first_string(
        item,
        ("video_url", "videoUrl", "video", "play_url", "playUrl", "download_url", "downloadUrl"),
    )
    if top_direct:
        return normalize_video_url_for_consumption(top_direct)
    return ""


def _creative_key(item: dict[str, Any], index: int) -> str:
    creative = item.get("creative") if isinstance(item.get("creative"), dict) else item
    if not isinstance(creative, dict):
        creative = {}
    for key in ("ad_key", "creative_id", "creativeId", "material_id", "id"):
        value = creative.get(key) or item.get(key)
        if value not in (None, ""):
            return str(value)
    return f"item-{index:03d}"


def _safe_slug(value: str, fallback: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "").strip()).strip(".-_")
    return (slug or fallback)[:80]


def _extension_from_url(url: str) -> str:
    try:
        path = urlparse(url).path
    except Exception:
        path = ""
    suffix = Path(path).suffix.lower()
    if suffix in {".mp4", ".mov", ".webm", ".m3u8"}:
        return suffix
    if suffix == ".webp":
        return ".mp4"
    guessed = mimetypes.guess_extension(mimetypes.guess_type(path)[0] or "")
    return guessed if guessed in {".mp4", ".mov", ".webm"} else ".mp4"


def download_video(url: str, target: Path) -> int:
    target.parent.mkdir(parents=True, exist_ok=True)
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "video/*,*/*;q=0.8",
        },
    )
    with urlopen(request, timeout=120) as response:
        with target.open("wb") as fh:
            shutil.copyfileobj(response, fh)
    return target.stat().st_size


def materialize_video_artifacts(
    job: CrawlJob,
    payload: dict[str, Any],
    *,
    downloader: Downloader | None = None,
) -> dict[str, Any]:
    """Download crawl videos into a job media directory and bundle a zip."""
    downloader = downloader or download_video
    media_dir = job.path / "media"
    manifest_path = job.path / "media_manifest.json"
    zip_path = job.path / "videos.zip"
    items = [item for item in (payload.get("items") or []) if isinstance(item, dict)]
    entries: list[dict[str, Any]] = []
    downloaded_paths: list[Path] = []
    failed = 0
    skipped = 0

    for index, item in enumerate(items, start=1):
        ad_key = _creative_key(item, index)
        source_url = pick_video_url_from_item(item)
        entry: dict[str, Any] = {
            "index": index,
            "ad_key": ad_key,
            "product": item.get("product"),
            "source_url": source_url,
            "status": "pending",
        }
        if not source_url:
            skipped += 1
            entry["status"] = "skipped_no_video_url"
            entries.append(entry)
            continue

        filename = f"{index:03d}_{_safe_slug(ad_key, f'item-{index:03d}')}{_extension_from_url(source_url)}"
        target = media_dir / filename
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            size = int(downloader(source_url, target))
        except Exception as exc:
            failed += 1
            entry["status"] = "failed"
            entry["error"] = str(exc)
            entry["target_path"] = str(target)
            entry["relative_path"] = str(target.relative_to(job.path))
            entries.append(entry)
            continue

        relative_path = str(target.relative_to(job.path))
        downloaded_paths.append(target)
        item["local_video_path"] = str(target)
        item["local_video_relative_path"] = relative_path
        item["source_video_url"] = source_url
        entry.update(
            {
                "status": "downloaded",
                "path": str(target),
                "relative_path": relative_path,
                "size_bytes": size,
            }
        )
        entries.append(entry)

    if downloaded_paths:
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in downloaded_paths:
                archive.write(path, arcname=str(path.relative_to(job.path)))

    manifest = {
        "job_id": job.job_id,
        "status": "completed",
        "video_count": len(entries),
        "downloaded_video_count": len(downloaded_paths),
        "video_download_failed_count": failed,
        "skipped_without_video_count": skipped,
        "media_dir": str(media_dir),
        "media_zip_path": str(zip_path) if downloaded_paths else None,
        "videos": entries,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {
        "media_artifacts_status": "completed",
        "media_manifest_path": str(manifest_path),
        "media_zip_path": str(zip_path) if downloaded_paths else None,
        "downloaded_video_count": len(downloaded_paths),
        "video_download_failed_count": failed,
        "skipped_without_video_count": skipped,
    }


def disabled_video_artifacts(job: CrawlJob) -> dict[str, Any]:
    return {
        "media_artifacts_status": "disabled_by_param",
        "media_manifest_path": str(job.path / "media_manifest.json"),
        "media_zip_path": None,
        "downloaded_video_count": 0,
        "video_download_failed_count": 0,
        "skipped_without_video_count": 0,
    }
