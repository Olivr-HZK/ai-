#!/usr/bin/env python3
"""Create or update the Feishu Base rating-trigger workflow for VE template recognition."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ua_workflows.shared.config import load_project_env
from ua_workflows.video_enhancer.feedback_training import parse_bitable_ref
from ua_workflows.video_enhancer.template_trigger import build_rating_trigger_workflow_payload


DEFAULT_TABLE_NAME = "ai工具video photo爬取表"
DEFAULT_RATING_FIELD_NAME = "浩鹏评分"


def _run_json(cmd: list[str]) -> dict[str, Any]:
    result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        detail = (result.stdout or result.stderr or "").strip()
        raise RuntimeError(f"command failed ({result.returncode}): {' '.join(cmd)}\n{detail}")
    return json.loads(result.stdout)


def _find_workflow(base_token: str, title: str, identity: str) -> str:
    data = _run_json(
        [
            "lark-cli",
            "base",
            "+workflow-list",
            "--base-token",
            base_token,
            "--as",
            identity,
            "--format",
            "json",
        ]
    )
    for item in (data.get("data") or {}).get("items") or []:
        if str(item.get("title") or "") == title:
            return str(item.get("workflow_id") or "")
    return ""


def _write_payload_file(payload: dict[str, Any]) -> str:
    target_dir = ROOT / "data"
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"ve_template_rating_trigger_workflow_payload.{os.getpid()}.tmp.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return f"data/{path.name}"


def _normalize_trigger_url(value: str) -> str:
    url = str(value or "").strip().rstrip("/")
    if not url:
        return ""
    if url.endswith("/trigger"):
        return url
    if url.endswith("/ensure-link"):
        return f"{url[: -len('/ensure-link')]}/trigger"
    return f"{url}/trigger"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="创建或更新 VE 评分自动触发模板识别 Workflow")
    parser.add_argument("--trigger-url", default="", help="公网可访问的 https://.../trigger")
    parser.add_argument("--public-url", default="", help="公网根 URL 或 /trigger URL，会自动转换为 /trigger")
    parser.add_argument("--bitable-url", default="", help="默认 VIDEO_ENHANCER_BITABLE_URL")
    parser.add_argument("--table-name", default=DEFAULT_TABLE_NAME)
    parser.add_argument("--rating-field-name", default=DEFAULT_RATING_FIELD_NAME)
    parser.add_argument("--reviewer", default="haopeng", choices=["haopeng", "weilan", "浩鹏", "尉蓝"])
    parser.add_argument("--min-rating", type=int, default=3)
    parser.add_argument("--title", default="", help="默认 VE 模板识别评分触发（评分字段名）")
    parser.add_argument("--token", default=os.getenv("VE_TEMPLATE_TRIGGER_TOKEN", ""))
    parser.add_argument("--as", dest="identity", default="user", choices=["user", "bot"])
    parser.add_argument("--enable", action="store_true", default=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_project_env()
    args = parse_args(argv)
    bitable_url = (args.bitable_url or os.getenv("VIDEO_ENHANCER_BITABLE_URL") or "").strip()
    if not bitable_url:
        raise SystemExit("请配置 VIDEO_ENHANCER_BITABLE_URL 或传入 --bitable-url")
    trigger_url = _normalize_trigger_url(
        args.trigger_url
        or args.public_url
        or os.getenv("VE_TEMPLATE_PUBLIC_TRIGGER_URL", "")
        or os.getenv("VE_TEMPLATE_TRIGGER_URL", "")
    )
    if not trigger_url:
        raise SystemExit("请传入 --trigger-url 或 --public-url")
    base_token = parse_bitable_ref(bitable_url).app_token
    title = args.title or f"VE 模板识别评分触发（{args.rating_field_name}）"
    payload = build_rating_trigger_workflow_payload(
        trigger_url=trigger_url,
        table_name=args.table_name,
        rating_field_name=args.rating_field_name,
        reviewer=args.reviewer,
        token=args.token,
        title=title,
        min_rating=args.min_rating,
    )
    workflow_id = _find_workflow(base_token, title, args.identity)
    if args.dry_run:
        print(
            json.dumps(
                {"action": "update" if workflow_id else "create", "workflow_id": workflow_id, "payload": payload},
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    payload_path = _write_payload_file(payload)
    try:
        if workflow_id:
            result = _run_json(
                [
                    "lark-cli",
                    "base",
                    "+workflow-update",
                    "--base-token",
                    base_token,
                    "--workflow-id",
                    workflow_id,
                    "--as",
                    args.identity,
                    "--json",
                    f"@{payload_path}",
                    "--format",
                    "json",
                ]
            )
            action = "updated"
        else:
            result = _run_json(
                [
                    "lark-cli",
                    "base",
                    "+workflow-create",
                    "--base-token",
                    base_token,
                    "--as",
                    args.identity,
                    "--json",
                    f"@{payload_path}",
                    "--format",
                    "json",
                ]
            )
            action = "created"
            workflow_id = str(((result.get("data") or {}).get("workflow") or {}).get("workflow_id") or "")
            if not workflow_id:
                workflow_id = str((result.get("data") or {}).get("workflow_id") or "")
        enabled = False
        if args.enable and workflow_id:
            _run_json(
                [
                    "lark-cli",
                    "base",
                    "+workflow-enable",
                    "--base-token",
                    base_token,
                    "--workflow-id",
                    workflow_id,
                    "--as",
                    args.identity,
                    "--format",
                    "json",
                ]
            )
            enabled = True
        print(json.dumps({"action": action, "workflow_id": workflow_id, "enabled": enabled}, ensure_ascii=False))
    finally:
        (ROOT / payload_path).unlink(missing_ok=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
