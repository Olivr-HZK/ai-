"""Job artifact storage for Guangdada crawl runs."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from guangdada_crawl.auth import default_home_dir


def default_jobs_dir() -> Path:
    root = os.getenv("GUANGDADA_CRAWL_HOME")
    if root:
        return Path(root).expanduser() / "jobs"
    return default_home_dir() / "jobs"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _json_safe(data: object) -> object:
    if isinstance(data, Path):
        return str(data)
    if isinstance(data, dict):
        return {str(k): _json_safe(v) for k, v in data.items()}
    if isinstance(data, (list, tuple)):
        return [_json_safe(v) for v in data]
    return data


@dataclass(frozen=True)
class CrawlJob:
    job_id: str
    path: Path
    manifest_path: Path


class JobStore:
    def __init__(self, root: Path | str | None = None) -> None:
        self.root = Path(root).expanduser() if root is not None else default_jobs_dir()
        self.root.mkdir(parents=True, exist_ok=True)

    def create_job(self, *, kind: str, params: dict[str, object], dry_run: bool = False) -> CrawlJob:
        safe_kind = "".join(ch if ch.isalnum() or ch in ("-", "_") else "-" for ch in kind).strip("-") or "crawl"
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        job_id = f"{stamp}_{safe_kind}_{uuid4().hex[:8]}"
        path = self.root / job_id
        path.mkdir(parents=True, exist_ok=False)
        job = CrawlJob(job_id=job_id, path=path, manifest_path=path / "manifest.json")
        manifest = {
            "job_id": job_id,
            "kind": kind,
            "status": "created",
            "dry_run": bool(dry_run),
            "params": _json_safe(params),
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
            "paths": {
                "job_dir": str(path),
                "manifest": str(path / "manifest.json"),
                "summary": str(path / "summary.json"),
                "raw": str(path / "raw.json"),
                "log": str(path / "run.log"),
            },
        }
        self._write_json(job.manifest_path, manifest)
        (path / "run.log").write_text("", encoding="utf-8")
        return job

    def read_manifest(self, job_id: str) -> dict[str, object]:
        return self._read_json(self._job_path(job_id) / "manifest.json")

    def read_summary(self, job_id: str) -> dict[str, object]:
        path = self._job_path(job_id) / "summary.json"
        return self._read_json(path) if path.exists() else {}

    def list_jobs(self, *, limit: int = 20) -> list[dict[str, object]]:
        jobs: list[dict[str, object]] = []
        if not self.root.exists():
            return jobs
        for manifest_path in self.root.glob("*/manifest.json"):
            try:
                manifest = self._read_json(manifest_path)
            except Exception:
                continue
            job_id = str(manifest.get("job_id") or manifest_path.parent.name)
            summary = {}
            try:
                summary = self.read_summary(job_id)
            except Exception:
                summary = {}
            paths = manifest.get("paths") if isinstance(manifest.get("paths"), dict) else {}
            sort_mtime_ns = manifest_path.stat().st_mtime_ns
            jobs.append(
                {
                    "job_id": job_id,
                    "kind": manifest.get("kind"),
                    "status": manifest.get("status"),
                    "dry_run": manifest.get("dry_run"),
                    "created_at": manifest.get("created_at"),
                    "updated_at": manifest.get("updated_at"),
                    "manifest_path": str(manifest_path),
                    "summary_path": str(paths.get("summary") or manifest_path.parent / "summary.json"),
                    "raw_path": str(paths.get("raw") or manifest_path.parent / "raw.json"),
                    "log_path": str(paths.get("log") or manifest_path.parent / "run.log"),
                    "summary": summary,
                    "_sort_mtime_ns": sort_mtime_ns,
                }
            )
        jobs.sort(
            key=lambda item: (
                str(item.get("updated_at") or item.get("created_at") or ""),
                int(item.get("_sort_mtime_ns") or 0),
                str(item.get("job_id") or ""),
            ),
            reverse=True,
        )
        for item in jobs:
            item.pop("_sort_mtime_ns", None)
        return jobs[: max(0, int(limit))]

    def artifacts(self, job_id: str) -> dict[str, object]:
        manifest = self.read_manifest(job_id)
        paths = manifest.get("paths") if isinstance(manifest.get("paths"), dict) else {}
        job_path = self._job_path(job_id)
        resolved = {
            "manifest": Path(str(paths.get("manifest") or job_path / "manifest.json")),
            "summary": Path(str(paths.get("summary") or job_path / "summary.json")),
            "raw": Path(str(paths.get("raw") or job_path / "raw.json")),
            "log": Path(str(paths.get("log") or job_path / "run.log")),
            "media_manifest": job_path / "media_manifest.json",
            "videos_zip": job_path / "videos.zip",
        }
        return {
            name: {
                "path": str(path),
                "exists": path.exists(),
                "size_bytes": path.stat().st_size if path.exists() else 0,
            }
            for name, path in resolved.items()
        }

    def write_summary(self, job_id: str, summary: dict[str, object]) -> Path:
        path = self._job_path(job_id) / "summary.json"
        self._write_json(path, summary)
        return path

    def write_raw(self, job_id: str, raw: dict[str, object]) -> Path:
        path = self._job_path(job_id) / "raw.json"
        self._write_json(path, raw)
        return path

    def append_log(self, job_id: str, message: str) -> None:
        log_path = self._job_path(job_id) / "run.log"
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(message.rstrip("\n") + "\n")

    def finish_job(self, job_id: str, *, status: str, error: str | None = None) -> dict[str, object]:
        manifest_path = self._job_path(job_id) / "manifest.json"
        manifest = self._read_json(manifest_path)
        manifest["status"] = status
        manifest["updated_at"] = _now_iso()
        if error:
            manifest["error"] = error
        self._write_json(manifest_path, manifest)
        return manifest

    def _job_path(self, job_id: str) -> Path:
        if not job_id or "/" in job_id or ".." in job_id:
            raise ValueError(f"Invalid job id: {job_id!r}")
        path = self.root / job_id
        if not path.exists():
            raise FileNotFoundError(f"Job not found: {job_id}")
        return path

    @staticmethod
    def _read_json(path: Path) -> dict[str, object]:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError(f"JSON object expected: {path}")
        return data

    @staticmethod
    def _write_json(path: Path, data: dict[str, object]) -> None:
        path.write_text(json.dumps(_json_safe(data), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
