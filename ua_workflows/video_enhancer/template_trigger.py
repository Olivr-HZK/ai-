"""Click-trigger helpers for VE high-rated template copy jobs."""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import subprocess
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests

from ua_workflows.shared.config import DATA_DIR, load_project_env
from ua_workflows.video_enhancer.feedback_rating import cell_to_text
from ua_workflows.video_enhancer.feedback_training import _tenant_access_token, fetch_bitable_records, parse_bitable_ref
from ua_workflows.video_enhancer.template_recognition import build_template_task_from_record


DEFAULT_JOB_DIR = DATA_DIR / "ve_template_copy_jobs"
TRIGGER_LINK_FIELD = "模板复刻触发链接"
TRIGGER_STATUS_FIELD = "模板复刻状态"
TRIGGER_JOB_ID_FIELD = "模板复刻任务ID"
RECOGNITION_TYPE_FIELD = "模板识别类型"
RECOGNITION_REASON_FIELD = "模板识别理由"
RECOGNITION_SCREENSHOT_FIELD = "模板参考截图"
RECOGNITION_VIDEO_SEGMENT_FIELD = "模板参考视频片段"


class TemplateTriggerError(RuntimeError):
    """Expected trigger failure with an API-friendly code."""

    def __init__(self, code: str, message: str, *, status_code: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


def _now_shanghai() -> str:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")


def _slug(value: Any, *, fallback: str = "job") -> str:
    text = str(value or "").strip()
    text = re.sub(r"[^0-9A-Za-z._-]+", "_", text).strip("._-")
    return text[:80] or fallback


def discover_aigc_template_copy_skill() -> dict[str, Any]:
    """Return local availability for the packaged aigc-template-copy skill."""
    candidates: list[Path] = []
    env_path = os.getenv("AIGC_TEMPLATE_COPY_SKILL_PATH", "").strip()
    if env_path:
        candidates.append(Path(env_path))
    home = Path.home()
    candidates.extend(
        [
            home / ".codex" / "skills" / "aigc-template-copy" / "SKILL.md",
            home / ".agents" / "skills" / "aigc-template-copy" / "SKILL.md",
        ]
    )
    for candidate in candidates:
        path = candidate if candidate.name == "SKILL.md" else candidate / "SKILL.md"
        if path.exists():
            return {"installed": True, "path": str(path)}
    return {"installed": False, "path": ""}


def _record_ad_key(record: dict[str, Any]) -> str:
    fields = record.get("fields") if isinstance(record, dict) else {}
    if not isinstance(fields, dict):
        fields = {}
    return cell_to_text(fields.get("广告ID") or fields.get("ad_key"))


def _record_id(record: dict[str, Any]) -> str:
    return str(record.get("record_id") or record.get("id") or "").strip()


def _record_crawl_date(record: dict[str, Any]) -> str:
    fields = record.get("fields") if isinstance(record, dict) else {}
    if not isinstance(fields, dict):
        fields = {}
    text = cell_to_text(fields.get("抓取日期") or fields.get("日期"))
    match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    return match.group(0) if match else text.strip()


def find_record(
    records: list[dict[str, Any]],
    *,
    record_id: str = "",
    ad_key: str = "",
) -> dict[str, Any]:
    record_id = str(record_id or "").strip()
    ad_key = str(ad_key or "").strip()
    if not record_id and not ad_key:
        raise TemplateTriggerError("missing_record_locator", "缺少 record_id 或 ad_key")
    for record in records:
        rid = str(record.get("record_id") or record.get("id") or "").strip()
        if record_id and rid == record_id:
            return record
        if ad_key and _record_ad_key(record) == ad_key:
            return record
    raise TemplateTriggerError("record_not_found", "没有找到对应的多维表记录", status_code=404)


def build_template_copy_job(
    record: dict[str, Any],
    *,
    reviewer: str = "haopeng",
    include_legacy: bool = False,
    trigger_source: str = "click",
    job_id: str = "",
    created_at: str = "",
) -> dict[str, Any]:
    task = build_template_task_from_record(
        record,
        reviewer=reviewer,
        include_legacy=include_legacy,
    )
    if task is None:
        raise TemplateTriggerError(
            "not_top_rating",
            "该记录评分低于 3 星，未触发模板复刻任务",
            status_code=409,
        )
    record_id = str(task.get("record_id") or "")
    ad_key = str(task.get("ad_key") or record_id or "material")
    created_at = created_at or _now_shanghai()
    if not job_id:
        job_id = "{date}_{ad_key}_{suffix}".format(
            date=str(task.get("crawl_date") or "unknown"),
            ad_key=_slug(ad_key, fallback="material"),
            suffix=uuid.uuid4().hex[:8],
        )
    return {
        "job_id": job_id,
        "status": "queued",
        "created_at": created_at,
        "trigger_source": trigger_source,
        "reviewer": reviewer,
        "record_id": record_id,
        "ad_key": ad_key,
        "task": task,
        "runner": {
            "type": "aigc-template-copy",
            "mode": "skill_handoff",
            "state": "pending",
            "skill": discover_aigc_template_copy_skill(),
        },
    }


def write_template_copy_job(job: dict[str, Any], *, job_dir: Path = DEFAULT_JOB_DIR) -> Path:
    job_dir.mkdir(parents=True, exist_ok=True)
    job_id = _slug(job.get("job_id"), fallback=uuid.uuid4().hex)
    path = job_dir / f"{job_id}.json"
    path.write_text(json.dumps(job, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def trigger_template_copy_job(
    records: list[dict[str, Any]],
    *,
    record_id: str = "",
    ad_key: str = "",
    reviewer: str = "haopeng",
    include_legacy: bool = False,
    trigger_source: str = "click",
    job_dir: Path = DEFAULT_JOB_DIR,
) -> tuple[dict[str, Any], Path]:
    record = find_record(records, record_id=record_id, ad_key=ad_key)
    job = build_template_copy_job(
        record,
        reviewer=reviewer,
        include_legacy=include_legacy,
        trigger_source=trigger_source,
    )
    return job, write_template_copy_job(job, job_dir=job_dir)


def trigger_template_copy_from_bitable(
    *,
    bitable_url: str,
    record_id: str = "",
    ad_key: str = "",
    reviewer: str = "haopeng",
    include_legacy: bool = False,
    trigger_source: str = "click",
    job_dir: Path = DEFAULT_JOB_DIR,
) -> tuple[dict[str, Any], Path]:
    load_project_env()
    if record_id:
        records = [fetch_bitable_record(bitable_url, record_id=record_id)]
    else:
        records = fetch_bitable_records(bitable_url)
    return trigger_template_copy_job(
        records,
        record_id=record_id,
        ad_key=ad_key,
        reviewer=reviewer,
        include_legacy=include_legacy,
        trigger_source=trigger_source,
        job_dir=job_dir,
    )


def fetch_bitable_record(
    bitable_url: str,
    *,
    record_id: str,
    access_token: str | None = None,
) -> dict[str, Any]:
    ref = parse_bitable_ref(bitable_url)
    token = access_token or _tenant_access_token()
    url = (
        f"https://open.feishu.cn/open-apis/bitable/v1/apps/{ref.app_token}"
        f"/tables/{ref.table_id}/records/{record_id}"
    )
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise TemplateTriggerError("record_not_found", f"读取多维表记录失败: {data}", status_code=404)
    record = (data.get("data") or {}).get("record") or data.get("data") or {}
    if not isinstance(record, dict) or not record:
        raise TemplateTriggerError("record_not_found", "没有找到对应的多维表记录", status_code=404)
    return record


def build_trigger_url(
    trigger_url: str,
    *,
    record_id: str = "",
    ad_key: str = "",
    token: str = "",
) -> str:
    trigger_url = str(trigger_url or "").strip()
    if not trigger_url:
        raise ValueError("trigger_url is required")
    params: dict[str, str] = {}
    if record_id:
        params["record_id"] = record_id
    if ad_key:
        params["ad_key"] = ad_key
    if token:
        params["token"] = token
    if not params:
        raise ValueError("record_id or ad_key is required")
    separator = "&" if "?" in trigger_url else "?"
    return f"{trigger_url}{separator}{urlencode(params)}"


def build_trigger_link_cell(trigger_url: str) -> dict[str, str]:
    """Return the raw Feishu Base URL-field CellValue."""
    return {"text": "触发模板复刻", "link": trigger_url}


def build_trigger_link_updates(
    records: list[dict[str, Any]],
    *,
    trigger_url: str,
    target_date: str = "",
    reviewer: str = "haopeng",
    include_legacy: bool = False,
    token: str = "",
    include_all_records: bool = False,
    link_only: bool = False,
) -> list[dict[str, Any]]:
    updates: list[dict[str, Any]] = []
    for record in records:
        task = build_template_task_from_record(
            record,
            reviewer=reviewer,
            include_legacy=include_legacy,
        )
        if not task and not include_all_records:
            continue
        crawl_date = str((task or {}).get("crawl_date") or _record_crawl_date(record))
        if target_date and crawl_date != target_date:
            continue
        record_id = str((task or {}).get("record_id") or _record_id(record))
        ad_key = str((task or {}).get("ad_key") or _record_ad_key(record))
        if not record_id:
            continue
        fields: dict[str, Any] = {
            TRIGGER_LINK_FIELD: build_trigger_link_cell(
                build_trigger_url(
                    trigger_url,
                    record_id=record_id,
                    token=token,
                )
            )
        }
        if task and not link_only:
            fields[TRIGGER_STATUS_FIELD] = "待触发"
            fields[TRIGGER_JOB_ID_FIELD] = ""
        updates.append(
            {
                "record_id": record_id,
                "fields": fields,
                "ad_key": ad_key,
            }
        )
    return updates


def update_template_trigger_links(
    *,
    bitable_url: str,
    trigger_url: str,
    target_date: str = "",
    reviewer: str = "haopeng",
    include_legacy: bool = False,
    token: str = "",
    include_all_records: bool = False,
    link_only: bool = False,
    dry_run: bool = False,
    batch_size: int = 200,
) -> dict[str, Any]:
    load_project_env()
    records = fetch_bitable_records(bitable_url)
    updates = build_trigger_link_updates(
        records,
        trigger_url=trigger_url,
        target_date=target_date,
        reviewer=reviewer,
        include_legacy=include_legacy,
        token=token,
        include_all_records=include_all_records,
        link_only=link_only,
    )
    if dry_run:
        return {"matched": len(updates), "updated": 0, "updates": updates[:20]}
    if not updates:
        return {"matched": 0, "updated": 0, "updates": []}
    ref = parse_bitable_ref(bitable_url)
    access_token = _tenant_access_token()
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json; charset=utf-8"}
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{ref.app_token}/tables/{ref.table_id}/records/batch_update"
    updated = 0
    for index in range(0, len(updates), batch_size):
        batch = updates[index : index + batch_size]
        records_payload = [{"record_id": item["record_id"], "fields": item["fields"]} for item in batch]
        resp = requests.post(url, headers=headers, json={"records": records_payload}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"batch_update template trigger links failed: {data}")
        updated += len(records_payload)
    return {"matched": len(updates), "updated": updated, "updates": []}


def update_template_trigger_link(
    *,
    bitable_url: str,
    record_id: str,
    trigger_url: str,
) -> dict[str, Any]:
    """Write one clickable template-trigger link to one Base record."""
    load_project_env()
    record_id = str(record_id or "").strip()
    if not record_id:
        raise ValueError("record_id is required")
    ref = parse_bitable_ref(bitable_url)
    access_token = _tenant_access_token()
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json; charset=utf-8"}
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{ref.app_token}/tables/{ref.table_id}/records/batch_update"
    fields = {TRIGGER_LINK_FIELD: build_trigger_link_cell(trigger_url)}
    resp = requests.post(
        url,
        headers=headers,
        json={"records": [{"record_id": record_id, "fields": fields}]},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"update template trigger link failed: {data}")
    return {"updated": True, "record_id": record_id, "fields": fields}


def ensure_template_trigger_link_from_bitable(
    *,
    bitable_url: str,
    record_id: str,
    trigger_url: str,
    reviewer: str = "haopeng",
    include_legacy: bool = False,
    token: str = "",
) -> dict[str, Any]:
    """Write the trigger link for one record if its current rating is 3 stars or higher."""
    load_project_env()
    record_id = str(record_id or "").strip()
    if not record_id:
        raise TemplateTriggerError("missing_record_locator", "缺少 record_id")
    record = fetch_bitable_record(bitable_url, record_id=record_id)
    task = build_template_task_from_record(
        record,
        reviewer=reviewer,
        include_legacy=include_legacy,
    )
    if not task:
        return {
            "eligible": False,
            "updated": False,
            "code": "not_top_rating",
            "message": "该记录评分低于 3 星，未写入模板复刻触发链接",
            "record_id": record_id,
        }
    resolved_record_id = str(task.get("record_id") or record_id)
    final_trigger_url = build_trigger_url(
        trigger_url,
        record_id=resolved_record_id,
        token=token,
    )
    update_result = update_template_trigger_link(
        bitable_url=bitable_url,
        record_id=resolved_record_id,
        trigger_url=final_trigger_url,
    )
    return {
        "eligible": True,
        "updated": bool(update_result.get("updated")),
        "record_id": resolved_record_id,
        "ad_key": str(task.get("ad_key") or ""),
        "trigger_link_written": True,
        "bitable_update": update_result,
    }


def update_template_copy_status(
    *,
    bitable_url: str,
    record_id: str,
    status: str,
    job_id: str = "",
) -> dict[str, Any]:
    """Write template-copy trigger status back to the source Base record."""
    load_project_env()
    record_id = str(record_id or "").strip()
    if not record_id:
        raise ValueError("record_id is required")
    fields: dict[str, Any] = {TRIGGER_STATUS_FIELD: status}
    if job_id:
        fields[TRIGGER_JOB_ID_FIELD] = str(job_id)
    ref = parse_bitable_ref(bitable_url)
    access_token = _tenant_access_token()
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json; charset=utf-8"}
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{ref.app_token}/tables/{ref.table_id}/records/batch_update"
    resp = requests.post(
        url,
        headers=headers,
        json={"records": [{"record_id": record_id, "fields": fields}]},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"update template copy status failed: {data}")
    return {"updated": True, "record_id": record_id, "fields": fields}


def _batch_update_record_fields(
    *,
    bitable_url: str,
    record_id: str,
    fields: dict[str, Any],
) -> dict[str, Any]:
    ref = parse_bitable_ref(bitable_url)
    access_token = _tenant_access_token()
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json; charset=utf-8"}
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{ref.app_token}/tables/{ref.table_id}/records/batch_update"
    resp = requests.post(
        url,
        headers=headers,
        json={"records": [{"record_id": record_id, "fields": fields}]},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"update bitable fields failed: {data}")
    return {"updated": True, "record_id": record_id, "fields": fields}


def _upload_attachments_with_lark_cli(
    *,
    bitable_url: str,
    record_id: str,
    field_name: str,
    files: list[str],
) -> dict[str, Any]:
    paths = [str(Path(path)) for path in files if str(path or "").strip()]
    if not paths:
        return {"uploaded": 0, "field": field_name, "files": []}
    ref = parse_bitable_ref(bitable_url)
    cmd = [
        "lark-cli",
        "base",
        "+record-upload-attachment",
        "--base-token",
        ref.app_token,
        "--table-id",
        ref.table_id,
        "--record-id",
        record_id,
        "--field-id",
        field_name,
        "--as",
        "user",
        "--format",
        "json",
    ]
    for path in paths:
        cmd.extend(["--file", path])
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        detail = (proc.stdout or proc.stderr or "").strip()
        raise RuntimeError(f"upload attachments failed for {field_name}: {detail}")
    return {"uploaded": len(paths), "field": field_name, "files": paths}


def update_template_recognition_result(
    *,
    bitable_url: str,
    record_id: str,
    recognition: dict[str, Any],
) -> dict[str, Any]:
    """Write recognition-only result fields and upload reference artifacts to Base."""
    load_project_env()
    record_id = str(record_id or "").strip()
    if not record_id:
        raise ValueError("record_id is required")
    info = recognition.get("recognition") if isinstance(recognition.get("recognition"), dict) else {}
    fields = {
        RECOGNITION_TYPE_FIELD: str(info.get("template_type") or ""),
        RECOGNITION_REASON_FIELD: str(info.get("reason") or ""),
    }
    field_update = _batch_update_record_fields(
        bitable_url=bitable_url,
        record_id=record_id,
        fields=fields,
    )
    screenshots = [str(path) for path in recognition.get("reference_screenshots") or []]
    video_segments = [str(path) for path in recognition.get("reference_video_segments") or []]
    screenshot_upload = _upload_attachments_with_lark_cli(
        bitable_url=bitable_url,
        record_id=record_id,
        field_name=RECOGNITION_SCREENSHOT_FIELD,
        files=screenshots,
    )
    video_upload = _upload_attachments_with_lark_cli(
        bitable_url=bitable_url,
        record_id=record_id,
        field_name=RECOGNITION_VIDEO_SEGMENT_FIELD,
        files=video_segments,
    )
    return {
        "updated": True,
        "record_id": record_id,
        "fields": field_update["fields"],
        "attachments": {
            "screenshots": screenshot_upload.get("uploaded", 0),
            "video_segments": video_upload.get("uploaded", 0),
        },
    }


def build_button_workflow_payload(
    *,
    trigger_url: str,
    table_name: str,
    token: str = "",
    title: str = "VE 模板复刻按钮触发",
    client_token: str = "",
) -> dict[str, Any]:
    """Build a Feishu Base Workflow body: ButtonTrigger -> HTTPClientAction."""
    if not trigger_url:
        raise ValueError("trigger_url is required")
    if not table_name:
        raise ValueError("table_name is required")
    client_token = client_token or f"ve-template-copy-{uuid.uuid4().hex}"
    raw_body: list[dict[str, Any]] = [
        {"value_type": "text", "value": '{"record_id":"'},
        {"value_type": "ref", "value": "$.step_button_trigger.recordId"},
        {"value_type": "text", "value": '","source":"lark_base_button"'},
    ]
    if token:
        raw_body.extend(
            [
                {"value_type": "text", "value": ',"token":"'},
                {"value_type": "text", "value": token},
                {"value_type": "text", "value": '"'},
            ]
        )
    raw_body.append({"value_type": "text", "value": "}"})
    return {
        "client_token": client_token,
        "title": title,
        "steps": [
            {
                "id": "step_button_trigger",
                "type": "ButtonTrigger",
                "title": "点击模板复刻按钮时触发",
                "next": "step_call_template_trigger",
                "data": {
                    "button_type": "buttonField",
                    "table_name": table_name,
                },
            },
            {
                "id": "step_call_template_trigger",
                "type": "HTTPClientAction",
                "title": "提交 VE 模板复刻任务",
                "next": None,
                "data": {
                    "method": "POST",
                    "url": [{"value_type": "text", "value": trigger_url}],
                    "headers": [
                        {
                            "key": "Content-Type",
                            "value": [{"value_type": "text", "value": "application/json"}],
                        }
                    ],
                    "body_type": "raw",
                    "raw_body": raw_body,
                    "response_type": "json",
                    "response_value": json.dumps(
                        {"success": True, "message": "queued", "job_id": "example"},
                        ensure_ascii=False,
                    ),
                },
            },
        ],
    }


def build_rating_link_workflow_payload(
    *,
    ensure_link_url: str,
    table_name: str,
    rating_field_name: str = "浩鹏评分",
    reviewer: str = "haopeng",
    token: str = "",
    title: str = "VE 模板复刻评分链接写入",
    client_token: str = "",
    min_rating: int = 3,
) -> dict[str, Any]:
    """Build a Base Workflow body: rating SetRecordTrigger -> POST /ensure-link."""
    if not ensure_link_url:
        raise ValueError("ensure_link_url is required")
    if not table_name:
        raise ValueError("table_name is required")
    if not rating_field_name:
        raise ValueError("rating_field_name is required")
    client_token = client_token or f"ve-template-rating-link-{uuid.uuid4().hex}"
    raw_body: list[dict[str, Any]] = [
        {"value_type": "text", "value": '{"record_id":"'},
        {"value_type": "ref", "value": "$.step_rating_trigger.recordId"},
        {"value_type": "text", "value": '",'},
        {"value_type": "text", "value": '"source":"lark_base_rating_update"'},
        {"value_type": "text", "value": ',"reviewer":"'},
        {"value_type": "text", "value": reviewer},
        {"value_type": "text", "value": '"'},
    ]
    if token:
        raw_body.extend(
            [
                {"value_type": "text", "value": ',"token":"'},
                {"value_type": "text", "value": token},
                {"value_type": "text", "value": '"'},
            ]
        )
    raw_body.append({"value_type": "text", "value": "}"})
    return {
        "client_token": client_token,
        "title": title,
        "steps": [
            {
                "id": "step_rating_trigger",
                "type": "SetRecordTrigger",
                "title": f"{rating_field_name} 达到 {min_rating} 星时触发",
                "next": "step_ensure_template_link",
                "data": {
                    "table_name": table_name,
                    "record_watch_conjunction": "and",
                    "record_watch_info": [],
                    "field_watch_info": [
                        {
                            "field_name": rating_field_name,
                            "operator": "isGreaterEqual",
                            "value": [{"value_type": "number", "value": min_rating}],
                        }
                    ],
                    "trigger_control_list": [],
                    "condition_list": None,
                },
            },
            {
                "id": "step_ensure_template_link",
                "type": "HTTPClientAction",
                "title": "写入模板复刻触发链接",
                "next": None,
                "data": {
                    "method": "POST",
                    "url": [{"value_type": "text", "value": ensure_link_url}],
                    "headers": [
                        {
                            "key": "Content-Type",
                            "value": [{"value_type": "text", "value": "application/json"}],
                        }
                    ],
                    "body_type": "raw",
                    "raw_body": raw_body,
                    "response_type": "json",
                    "response_value": json.dumps(
                        {"success": True, "message": "link_written", "record_id": "example"},
                        ensure_ascii=False,
                    ),
                },
            },
        ],
    }
