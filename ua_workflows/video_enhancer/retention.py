"""Retention helpers for VE Bitable rows and local generated artifacts.

The first version is deliberately conservative:
- Feishu/Bitable rows are marked as archived, never deleted.
- SQLite history tables are reported only; `creative_library` is not pruned.
- Local artifact deletion is opt-in and excludes database files.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse

import requests

from ua_workflows.shared.config import DATA_DIR, PROJECT_ROOT, REPORTS_DIR, load_project_env
from ua_workflows.video_enhancer.feedback_training import cell_to_text


BITABLE_ARCHIVE_STATUS_FIELD = "生命周期状态"
BITABLE_ARCHIVE_DATE_FIELD = "归档日期"
BITABLE_ARCHIVE_REASON_FIELD = "归档原因"

DEFAULT_FEEDBACK_FIELDS = (
    "接受情况",
    "浩鹏接受情况",
    "尉蓝接受情况",
)
ACCEPTED_STATUS_VALUES = ("接受", "采纳", "入素材库", "accepted", "accept", "yes")
REJECTED_STATUS_VALUES = ("删除", "拒绝", "不采纳", "重复抓取", "rejected", "reject", "no")
PENDING_STATUS_VALUES = ("待定", "pending")
ARCHIVED_STATUS_VALUES = ("已归档", "归档", "archived")
PROTECTED_TAGS = ("历史簇持续发力",)
LOCAL_ARTIFACT_SUFFIXES = (".json", ".jsonl", ".md", ".html", ".png", ".jpg", ".jpeg", ".webp")
LOCAL_DB_SUFFIXES = (".db", ".sqlite", ".sqlite3")


@dataclass(frozen=True)
class ArchivePolicy:
    reference_date: date
    archive_after_days: int = 45
    feedback_fields: tuple[str, ...] = DEFAULT_FEEDBACK_FIELDS
    accepted_values: tuple[str, ...] = ACCEPTED_STATUS_VALUES
    rejected_values: tuple[str, ...] = REJECTED_STATUS_VALUES
    pending_values: tuple[str, ...] = PENDING_STATUS_VALUES
    protected_tags: tuple[str, ...] = PROTECTED_TAGS


@dataclass(frozen=True)
class ArchiveDecision:
    record_id: str
    ad_key: str
    crawl_date: str
    age_days: int | None
    action: str
    reason: str
    detail: str


@dataclass(frozen=True)
class LocalRetentionPolicy:
    reference_now: datetime
    artifact_keep_days: int = 30
    keep_remote_snapshots: int = 3


@dataclass(frozen=True)
class LocalArtifactCandidate:
    path: Path
    reason: str
    age_days: int
    size_bytes: int


def _date_from_cell(value: Any) -> date | None:
    text = cell_to_text(value)
    if text:
        for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(text[:10], fmt).date()
            except ValueError:
                pass
    if isinstance(value, (int, float)) and value > 100000000000:
        try:
            return datetime.fromtimestamp(float(value) / 1000).date()
        except Exception:
            return None
    return None


def _status_tokens(fields: dict[str, Any], policy: ArchivePolicy) -> list[str]:
    out: list[str] = []
    for name in policy.feedback_fields:
        value = cell_to_text(fields.get(name))
        if value:
            out.append(value.strip())
    return out


def _contains_any(texts: Iterable[str], needles: Iterable[str]) -> bool:
    lowered_needles = [n.lower() for n in needles]
    for text in texts:
        low = text.lower()
        if any(n and n in low for n in lowered_needles):
            return True
    return False


def _split_status_values(text: str) -> set[str]:
    return {part.strip().lower() for part in re.split(r"[,，/、;；\s]+", text or "") if part.strip()}


def _status_equals_any(statuses: Iterable[str], needles: Iterable[str]) -> bool:
    needle_set = {n.lower() for n in needles if n}
    for status in statuses:
        if _split_status_values(status) & needle_set:
            return True
    return False


def classify_bitable_record(record: dict[str, Any], policy: ArchivePolicy) -> ArchiveDecision:
    fields = record.get("fields") if isinstance(record.get("fields"), dict) else {}
    record_id = str(record.get("record_id") or record.get("id") or "")
    ad_key = cell_to_text(fields.get("广告ID"))
    status = cell_to_text(fields.get(BITABLE_ARCHIVE_STATUS_FIELD))
    if _status_equals_any([status], ARCHIVED_STATUS_VALUES):
        return ArchiveDecision(record_id, ad_key, "", None, "keep", "already_archived", "已归档记录不重复处理")

    crawl = _date_from_cell(fields.get("抓取日期"))
    crawl_date = crawl.isoformat() if crawl else ""
    if crawl is None:
        return ArchiveDecision(record_id, ad_key, crawl_date, None, "keep", "missing_crawl_date", "缺少抓取日期")

    age_days = (policy.reference_date - crawl).days
    statuses = _status_tokens(fields, policy)
    if _status_equals_any(statuses, policy.accepted_values):
        return ArchiveDecision(record_id, ad_key, crawl_date, age_days, "keep", "accepted_feedback", "已有采纳/入库反馈")
    if _status_equals_any(statuses, policy.pending_values):
        return ArchiveDecision(record_id, ad_key, crawl_date, age_days, "keep", "pending_feedback", "仍为待定反馈")

    material_tags = cell_to_text(fields.get("素材标签"))
    if _contains_any([material_tags], policy.protected_tags):
        return ArchiveDecision(record_id, ad_key, crawl_date, age_days, "keep", "history_refresh", "历史簇持续发力素材保留")

    if age_days < policy.archive_after_days:
        return ArchiveDecision(record_id, ad_key, crawl_date, age_days, "keep", "recent_record", "未超过归档保留天数")

    detail = f"超过 {policy.archive_after_days} 天且无保留信号"
    return ArchiveDecision(record_id, ad_key, crawl_date, age_days, "archive", f"older_than_{policy.archive_after_days}_days", detail)


def _iter_local_artifact_files(root: Path) -> Iterable[Path]:
    for dirname in ("data", "reports"):
        base = root / dirname
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() in LOCAL_DB_SUFFIXES:
                continue
            if path.suffix.lower() in LOCAL_ARTIFACT_SUFFIXES:
                yield path


def _remote_snapshot_files(root: Path, keep_latest: int) -> set[Path]:
    base = root / "data" / "remote_snapshots" / "ve"
    if not base.exists():
        return set()
    files = [
        p
        for p in base.rglob("*")
        if p.is_file()
        and p.suffix.lower() in LOCAL_ARTIFACT_SUFFIXES
        and p.suffix.lower() not in LOCAL_DB_SUFFIXES
    ]
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return set(files[keep_latest:])


def select_local_artifact_candidates(root: Path, policy: LocalRetentionPolicy) -> list[LocalArtifactCandidate]:
    cutoff_seconds = policy.artifact_keep_days * 86400
    out: dict[Path, LocalArtifactCandidate] = {}
    snapshot_delete_set = _remote_snapshot_files(root, policy.keep_remote_snapshots)
    for path in snapshot_delete_set:
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        age_days = int((policy.reference_now.timestamp() - stat.st_mtime) // 86400)
        out[path] = LocalArtifactCandidate(path, "remote_snapshot_keep_latest", max(age_days, 0), stat.st_size)
    for path in _iter_local_artifact_files(root):
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        age_days = int((policy.reference_now.timestamp() - stat.st_mtime) // 86400)
        if policy.reference_now.timestamp() - stat.st_mtime >= cutoff_seconds:
            out[path] = LocalArtifactCandidate(path, "artifact_age", max(age_days, 0), stat.st_size)
    return sorted(out.values(), key=lambda c: str(c.path))


def delete_local_artifact_candidates(candidates: Iterable[LocalArtifactCandidate]) -> dict[str, int]:
    deleted = 0
    bytes_deleted = 0
    for candidate in candidates:
        try:
            size = candidate.path.stat().st_size
            candidate.path.unlink()
            deleted += 1
            bytes_deleted += size
        except FileNotFoundError:
            continue
    return {"deleted": deleted, "bytes_deleted": bytes_deleted}


def _tenant_access_token() -> str:
    load_project_env()
    app_id = os.getenv("FEISHU_APP_ID", "")
    app_secret = os.getenv("FEISHU_APP_SECRET", "")
    if not app_id or not app_secret:
        raise RuntimeError("请在 .env 配置 FEISHU_APP_ID / FEISHU_APP_SECRET")
    resp = requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": app_id, "app_secret": app_secret},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"get tenant_access_token failed: {data}")
    return str(data["tenant_access_token"])


def parse_bitable_ref(bitable_url: str) -> tuple[str, str, str]:
    parsed = urlparse(bitable_url.strip())
    parts = [p for p in parsed.path.split("/") if p]
    app_token = parts[1] if len(parts) >= 2 and parts[0] == "base" else ""
    qs = parse_qs(parsed.query or "")
    table_id = (qs.get("table") or [""])[0]
    view_id = (qs.get("view") or [""])[0]
    if not app_token or not table_id:
        raise RuntimeError(f"无法从链接解析 app_token/table_id: {bitable_url}")
    return app_token, table_id, view_id


def list_bitable_records(
    bitable_url: str,
    *,
    access_token: str | None = None,
    page_size: int = 500,
) -> list[dict[str, Any]]:
    app_token, table_id, view_id = parse_bitable_ref(bitable_url)
    token = access_token or _tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}
    page_token = ""
    out: list[dict[str, Any]] = []
    while True:
        params: dict[str, Any] = {"page_size": page_size}
        if page_token:
            params["page_token"] = page_token
        if view_id:
            params["view_id"] = view_id
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"list bitable records failed: {data}")
        payload = data.get("data") or {}
        out.extend(payload.get("items") or payload.get("records") or [])
        if not payload.get("has_more"):
            break
        page_token = str(payload.get("page_token") or "")
        if not page_token:
            break
    return out


def update_bitable_archive_fields(
    access_token: str,
    app_token: str,
    table_id: str,
    decisions: Iterable[ArchiveDecision],
    *,
    archive_date: str,
    batch_size: int = 200,
) -> dict[str, int]:
    records = [
        {
            "record_id": d.record_id,
            "fields": {
                BITABLE_ARCHIVE_STATUS_FIELD: "已归档",
                BITABLE_ARCHIVE_DATE_FIELD: archive_date,
                BITABLE_ARCHIVE_REASON_FIELD: d.detail,
            },
        }
        for d in decisions
        if d.action == "archive" and d.record_id
    ]
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json; charset=utf-8"}
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update"
    updated = 0
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        resp = requests.post(url, headers=headers, json={"records": batch}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"batch_update archive fields failed: {data}")
        updated += len(batch)
    return {"updated": updated}


def ensure_bitable_archive_fields(access_token: str, app_token: str, table_id: str) -> dict[str, int]:
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json; charset=utf-8"}
    existing: set[str] = set()
    page_token = ""
    while True:
        params: dict[str, Any] = {}
        if page_token:
            params["page_token"] = page_token
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"list fields failed: {data}")
        payload = data.get("data") or {}
        for item in payload.get("items") or payload.get("fields") or []:
            name = item.get("field_name")
            if name:
                existing.add(str(name))
        if not payload.get("has_more"):
            break
        page_token = str(payload.get("page_token") or "")
        if not page_token:
            break

    created = 0
    for field_name in (BITABLE_ARCHIVE_STATUS_FIELD, BITABLE_ARCHIVE_DATE_FIELD, BITABLE_ARCHIVE_REASON_FIELD):
        if field_name in existing:
            continue
        resp = requests.post(
            url,
            headers=headers,
            json={"field_name": field_name, "type": 1},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"create archive field failed: field={field_name} data={data}")
        created += 1
    return {"created": created}


def write_retention_report(
    path: Path,
    *,
    bitable_decisions: list[ArchiveDecision],
    local_candidates: list[LocalArtifactCandidate],
    applied_bitable: bool,
    applied_local: bool,
    local_delete_result: dict[str, int] | None = None,
) -> dict[str, Any]:
    archive_count = sum(1 for d in bitable_decisions if d.action == "archive")
    keep_count = sum(1 for d in bitable_decisions if d.action != "archive")
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "applied": {"bitable": applied_bitable, "local": applied_local},
        "summary": {
            "bitable_total_records": len(bitable_decisions),
            "bitable_archive_candidates": archive_count,
            "bitable_keep_records": keep_count,
            "local_delete_candidates": len(local_candidates),
            "local_delete_bytes": sum(c.size_bytes for c in local_candidates),
        },
        "bitable": [asdict(d) for d in bitable_decisions],
        "local_artifacts": [
            {
                "path": str(c.path),
                "reason": c.reason,
                "age_days": c.age_days,
                "size_bytes": c.size_bytes,
            }
            for c in local_candidates
        ],
        "local_delete_result": local_delete_result or {"deleted": 0, "bytes_deleted": 0},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def _safe_rmtree_empty_dirs(root: Path) -> int:
    removed = 0
    for path in sorted(root.rglob("*"), key=lambda p: len(p.parts), reverse=True):
        if path.is_dir():
            try:
                path.rmdir()
                removed += 1
            except OSError:
                pass
    return removed


def cleanup_empty_snapshot_dirs(root: Path) -> int:
    base = root / "data" / "remote_snapshots" / "ve"
    if not base.exists():
        return 0
    return _safe_rmtree_empty_dirs(base)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="VE 多维表归档标记与本地产物清理 dry-run/执行入口")
    parser.add_argument("--bitable-url", default="", help="VE 主多维表 URL；默认 VIDEO_ENHANCER_BITABLE_URL")
    parser.add_argument("--archive-after-days", type=int, default=45, help="超过多少天且无保留信号后标记归档")
    parser.add_argument("--artifact-keep-days", type=int, default=30, help="本地 JSON/报告等产物保留天数")
    parser.add_argument("--keep-remote-snapshots", type=int, default=3, help="远端 VE 快照保留最新文件数")
    parser.add_argument("--output", default="", help="报告输出路径，默认 reports/ve_retention_{date}.json")
    parser.add_argument("--skip-bitable", action="store_true", help="跳过多维表扫描，只处理本地产物候选")
    parser.add_argument("--skip-local", action="store_true", help="跳过本地产物扫描，只处理多维表归档候选")
    parser.add_argument("--apply-bitable", action="store_true", help="实际写入多维表归档标记；默认只 dry-run")
    parser.add_argument("--apply-local", action="store_true", help="实际删除本地产物候选；默认只 dry-run")
    return parser


def run(args: argparse.Namespace) -> dict[str, Any]:
    load_project_env()
    today = date.today()
    now = datetime.now()
    output = Path(args.output) if args.output else REPORTS_DIR / f"ve_retention_{today.isoformat()}.json"
    bitable_decisions: list[ArchiveDecision] = []
    local_candidates: list[LocalArtifactCandidate] = []
    local_delete_result: dict[str, int] | None = None

    if not args.skip_bitable:
        bitable_url = (args.bitable_url or os.getenv("VIDEO_ENHANCER_BITABLE_URL") or "").strip()
        if not bitable_url:
            raise RuntimeError("请配置 VIDEO_ENHANCER_BITABLE_URL，或传入 --bitable-url，或使用 --skip-bitable")
        token = _tenant_access_token()
        records = list_bitable_records(bitable_url, access_token=token)
        policy = ArchivePolicy(reference_date=today, archive_after_days=int(args.archive_after_days))
        bitable_decisions = [classify_bitable_record(record, policy) for record in records]
        if args.apply_bitable:
            app_token, table_id, _ = parse_bitable_ref(bitable_url)
            ensure_bitable_archive_fields(token, app_token, table_id)
            update_bitable_archive_fields(
                token,
                app_token,
                table_id,
                bitable_decisions,
                archive_date=today.isoformat(),
            )

    if not args.skip_local:
        local_policy = LocalRetentionPolicy(
            reference_now=now,
            artifact_keep_days=int(args.artifact_keep_days),
            keep_remote_snapshots=int(args.keep_remote_snapshots),
        )
        local_candidates = select_local_artifact_candidates(PROJECT_ROOT, local_policy)
        if args.apply_local:
            local_delete_result = delete_local_artifact_candidates(local_candidates)
            cleanup_empty_snapshot_dirs(PROJECT_ROOT)

    return write_retention_report(
        output,
        bitable_decisions=bitable_decisions,
        local_candidates=local_candidates,
        applied_bitable=bool(args.apply_bitable),
        applied_local=bool(args.apply_local),
        local_delete_result=local_delete_result,
    )


def main(argv: list[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    report = run(args)
    summary = report["summary"]
    print(
        "[ve-retention] "
        f"多维表归档候选 {summary['bitable_archive_candidates']} / {summary['bitable_total_records']}；"
        f"本地产物候选 {summary['local_delete_candidates']} 个；"
        f"报告已写入 {args.output or REPORTS_DIR / ('ve_retention_' + date.today().isoformat() + '.json')}"
    )
    if not args.apply_bitable and not args.skip_bitable:
        print("[ve-retention] dry-run：未写入多维表；如需执行加 --apply-bitable")
    if not args.apply_local and not args.skip_local:
        print("[ve-retention] dry-run：未删除本地产物；如需执行加 --apply-local")


if __name__ == "__main__":
    main()
