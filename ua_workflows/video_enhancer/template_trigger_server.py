"""Local HTTP server for click-triggered VE template copy jobs."""

from __future__ import annotations

import argparse
import html
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from ua_workflows.shared.config import DATA_DIR, load_project_env
from ua_workflows.video_enhancer.template_trigger import (
    DEFAULT_JOB_DIR,
    TemplateTriggerError,
    ensure_template_trigger_link_from_bitable,
    trigger_template_copy_from_bitable,
    update_template_copy_status,
)
from ua_workflows.video_enhancer.template_copy_worker import (
    DEFAULT_MODEL_REF_DIR,
    DEFAULT_WORK_DIR,
    TemplateCopyWorkerError,
    execute_prepared_template_copy_job,
    prepare_template_copy_job,
)


def _bool_env(name: str, *, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _json_response(handler: BaseHTTPRequestHandler, payload: dict[str, Any], status: int = 200) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _html_response(handler: BaseHTTPRequestHandler, payload: dict[str, Any], status: int = 200) -> None:
    title = "VE 模板复刻任务"
    if payload.get("success"):
        message = f"已提交任务：{payload.get('job_id', '')}"
        detail = str(payload.get("job_path") or "")
    else:
        message = str(payload.get("message") or "触发失败")
        detail = str(payload.get("code") or "")
    body = (
        "<!doctype html><html><head><meta charset=\"utf-8\">"
        f"<title>{html.escape(title)}</title></head><body>"
        f"<h1>{html.escape(message)}</h1>"
        f"<p>{html.escape(detail)}</p>"
        "</body></html>"
    ).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _first(values: dict[str, list[str]], name: str) -> str:
    return str((values.get(name) or [""])[0] or "").strip()


def run_optional_worker(
    job_path: Path,
    *,
    auto_prepare: bool,
    auto_execute_codex: bool = False,
    codex_bin: str = "",
    codex_model: str = "",
    model_ref_dir: Path = DEFAULT_MODEL_REF_DIR,
    work_dir: Path = DEFAULT_WORK_DIR,
) -> dict[str, Any]:
    if not auto_prepare:
        return {"skipped": True}
    prepared = prepare_template_copy_job(
        job_path,
        model_ref_dir=model_ref_dir,
        work_dir=work_dir,
        download=True,
        dry_run=not auto_execute_codex,
    )
    if not auto_execute_codex:
        return prepared
    return execute_prepared_template_copy_job(job_path, codex_bin=codex_bin, codex_model=codex_model)


class TemplateTriggerHandler(BaseHTTPRequestHandler):
    server_version = "VETemplateTrigger/0.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        print("[ve-template-trigger]", fmt % args)

    @property
    def trigger_token(self) -> str:
        return str(getattr(self.server, "trigger_token", "") or "")

    @property
    def bitable_url(self) -> str:
        return str(getattr(self.server, "bitable_url", "") or "")

    @property
    def reviewer(self) -> str:
        return str(getattr(self.server, "reviewer", "") or "haopeng")

    @property
    def include_legacy(self) -> bool:
        return bool(getattr(self.server, "include_legacy", False))

    @property
    def job_dir(self) -> Path:
        return Path(getattr(self.server, "job_dir", DEFAULT_JOB_DIR))

    @property
    def auto_prepare(self) -> bool:
        return bool(getattr(self.server, "auto_prepare", False))

    @property
    def auto_execute_codex(self) -> bool:
        return bool(getattr(self.server, "auto_execute_codex", False))

    @property
    def codex_bin(self) -> str:
        return str(getattr(self.server, "codex_bin", "") or "")

    @property
    def codex_model(self) -> str:
        return str(getattr(self.server, "codex_model", "") or "")

    @property
    def model_ref_dir(self) -> Path:
        return Path(getattr(self.server, "model_ref_dir", DEFAULT_MODEL_REF_DIR))

    @property
    def work_dir(self) -> Path:
        return Path(getattr(self.server, "work_dir", DEFAULT_WORK_DIR))

    @property
    def public_trigger_url(self) -> str:
        return str(getattr(self.server, "public_trigger_url", "") or "").strip()

    def _resolved_public_trigger_url(self) -> str:
        configured = self.public_trigger_url
        if configured:
            return configured if configured.endswith("/trigger") else f"{configured.rstrip('/')}/trigger"
        host = str(self.headers.get("X-Forwarded-Host") or self.headers.get("Host") or "").strip()
        if host:
            proto = str(self.headers.get("X-Forwarded-Proto") or "").strip()
            if not proto:
                proto = "https" if "trycloudflare.com" in host else "http"
            return f"{proto}://{host}/trigger"
        host, port = getattr(self.server, "server_address", ("127.0.0.1", 8765))
        return f"http://{host}:{port}/trigger"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            _json_response(self, {"success": True, "message": "ok"})
            return
        if parsed.path not in {"/trigger", "/ensure-link"}:
            _json_response(self, {"success": False, "message": "not found"}, status=404)
            return
        params = parse_qs(parsed.query or "")
        payload = {
            "record_id": _first(params, "record_id"),
            "ad_key": _first(params, "ad_key"),
            "token": _first(params, "token"),
            "source": _first(params, "source") or "local_link_click",
        }
        if parsed.path == "/ensure-link":
            result, status = self._ensure_link(payload)
            _json_response(self, result, status=status)
            return
        result, status = self._trigger(payload)
        _html_response(self, result, status=status)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path not in {"/trigger", "/ensure-link"}:
            _json_response(self, {"success": False, "message": "not found"}, status=404)
            return
        length = int(self.headers.get("Content-Length") or "0")
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            _json_response(self, {"success": False, "code": "invalid_json", "message": "请求体不是合法 JSON"}, 400)
            return
        if not isinstance(payload, dict):
            _json_response(self, {"success": False, "code": "invalid_json", "message": "请求体必须是 JSON 对象"}, 400)
            return
        if parsed.path == "/ensure-link":
            result, status = self._ensure_link(payload)
            _json_response(self, result, status=status)
            return
        result, status = self._trigger(payload)
        _json_response(self, result, status=status)

    def _authorize_payload(self, payload: dict[str, Any]) -> tuple[dict[str, Any] | None, int]:
        incoming_token = str(payload.get("token") or self.headers.get("X-VE-Template-Token") or "").strip()
        if self.trigger_token and incoming_token != self.trigger_token:
            return {"success": False, "code": "unauthorized", "message": "触发 token 不正确"}, 401
        return None, 200

    def _trigger(self, payload: dict[str, Any]) -> tuple[dict[str, Any], int]:
        auth_error, auth_status = self._authorize_payload(payload)
        if auth_error:
            return auth_error, auth_status
        record_id = str(payload.get("record_id") or "").strip()
        ad_key = str(payload.get("ad_key") or "").strip()
        source = str(payload.get("source") or "click").strip()
        if not self.bitable_url:
            return {"success": False, "code": "missing_bitable_url", "message": "未配置 VIDEO_ENHANCER_BITABLE_URL"}, 500
        try:
            job, path = trigger_template_copy_from_bitable(
                bitable_url=self.bitable_url,
                record_id=record_id,
                ad_key=ad_key,
                reviewer=self.reviewer,
                include_legacy=self.include_legacy,
                trigger_source=source,
                job_dir=self.job_dir,
            )
        except TemplateTriggerError as exc:
            return {"success": False, "code": exc.code, "message": exc.message}, exc.status_code
        except Exception as exc:
            return {
                "success": False,
                "code": "trigger_failed",
                "message": f"{type(exc).__name__}: {exc}",
            }, 500
        try:
            worker = run_optional_worker(
                path,
                auto_prepare=self.auto_prepare,
                auto_execute_codex=self.auto_execute_codex,
                codex_bin=self.codex_bin,
                codex_model=self.codex_model,
                model_ref_dir=self.model_ref_dir,
                work_dir=self.work_dir,
            )
        except TemplateCopyWorkerError as exc:
            bitable_update = self._update_status(record_id=str(job.get("record_id") or record_id), status="失败", job_id=str(job.get("job_id") or ""))
            return {
                "success": False,
                "code": exc.code,
                "message": exc.message,
                "job_id": job.get("job_id"),
                "bitable_update": bitable_update,
            }, 409
        except Exception as exc:
            bitable_update = self._update_status(record_id=str(job.get("record_id") or record_id), status="失败", job_id=str(job.get("job_id") or ""))
            return {
                "success": False,
                "code": "worker_prepare_failed",
                "message": f"{type(exc).__name__}: {exc}",
                "job_id": job.get("job_id"),
                "bitable_update": bitable_update,
            }, 500
        bitable_status = str(worker.get("bitable_status") or "已提交")
        job_status = str(worker.get("status") or "queued")
        bitable_update = self._update_status(record_id=str(job.get("record_id") or record_id), status=bitable_status, job_id=str(job.get("job_id") or ""))
        return {
            "success": True,
            "message": "queued",
            "job_id": job.get("job_id"),
            "job_status": job_status,
            "job_path": str(path),
            "worker": worker,
            "bitable_update": bitable_update,
        }, 200

    def _ensure_link(self, payload: dict[str, Any]) -> tuple[dict[str, Any], int]:
        auth_error, auth_status = self._authorize_payload(payload)
        if auth_error:
            return auth_error, auth_status
        record_id = str(payload.get("record_id") or "").strip()
        if not record_id:
            return {"success": False, "code": "missing_record_locator", "message": "缺少 record_id"}, 400
        if not self.bitable_url:
            return {"success": False, "code": "missing_bitable_url", "message": "未配置 VIDEO_ENHANCER_BITABLE_URL"}, 500
        reviewer = str(payload.get("reviewer") or self.reviewer).strip()
        try:
            result = ensure_template_trigger_link_from_bitable(
                bitable_url=self.bitable_url,
                record_id=record_id,
                trigger_url=self._resolved_public_trigger_url(),
                reviewer=reviewer,
                include_legacy=self.include_legacy,
                token=self.trigger_token,
            )
        except TemplateTriggerError as exc:
            return {"success": False, "code": exc.code, "message": exc.message}, exc.status_code
        except Exception as exc:
            return {
                "success": False,
                "code": "ensure_link_failed",
                "message": f"{type(exc).__name__}: {exc}",
            }, 500
        if not result.get("eligible"):
            return {
                "success": True,
                "message": "not_eligible",
                **result,
            }, 200
        return {
            "success": True,
            "message": "link_written",
            **result,
        }, 200

    def _update_status(self, *, record_id: str, status: str, job_id: str) -> dict[str, Any]:
        try:
            return update_template_copy_status(
                bitable_url=self.bitable_url,
                record_id=record_id,
                status=status,
                job_id=job_id,
            )
        except Exception as exc:
            return {
                "updated": False,
                "error": f"{type(exc).__name__}: {exc}",
                "record_id": record_id,
                "fields": {"模板复刻状态": status, "模板复刻任务ID": job_id},
            }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="启动 VE 模板复刻点击触发服务")
    parser.add_argument("--host", default=os.getenv("VE_TEMPLATE_TRIGGER_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("VE_TEMPLATE_TRIGGER_PORT", "8765")))
    parser.add_argument("--bitable-url", default=os.getenv("VIDEO_ENHANCER_BITABLE_URL", ""))
    parser.add_argument("--reviewer", default=os.getenv("VE_TEMPLATE_TRIGGER_REVIEWER", "haopeng"))
    parser.add_argument("--include-legacy", action="store_true", default=_bool_env("VE_TEMPLATE_TRIGGER_INCLUDE_LEGACY"))
    parser.add_argument("--token", default=os.getenv("VE_TEMPLATE_TRIGGER_TOKEN", ""))
    parser.add_argument("--job-dir", default=os.getenv("VE_TEMPLATE_COPY_JOB_DIR", str(DEFAULT_JOB_DIR)))
    parser.add_argument("--auto-prepare", action="store_true", default=_bool_env("VE_TEMPLATE_COPY_AUTO_PREPARE"))
    parser.add_argument("--auto-execute-codex", action="store_true", default=_bool_env("VE_TEMPLATE_COPY_AUTO_EXECUTE_CODEX"))
    parser.add_argument("--codex-bin", default=os.getenv("VE_TEMPLATE_COPY_CODEX_BIN", ""))
    parser.add_argument("--codex-model", default=os.getenv("VE_TEMPLATE_COPY_CODEX_MODEL", ""))
    parser.add_argument(
        "--public-trigger-url",
        default=os.getenv("VE_TEMPLATE_PUBLIC_TRIGGER_URL", os.getenv("VE_TEMPLATE_TRIGGER_URL", "")),
        help="写回多维表的公网 /trigger URL；为空时从请求 Host 推断",
    )
    parser.add_argument("--model-ref-dir", default=os.getenv("VE_TEMPLATE_COPY_MODEL_REF_DIR", str(DEFAULT_MODEL_REF_DIR)))
    parser.add_argument("--work-dir", default=os.getenv("VE_TEMPLATE_COPY_WORK_DIR", str(DEFAULT_WORK_DIR)))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_project_env()
    args = parse_args(argv)
    server = ThreadingHTTPServer((args.host, args.port), TemplateTriggerHandler)
    server.bitable_url = (args.bitable_url or os.getenv("VIDEO_ENHANCER_BITABLE_URL") or "").strip()
    server.reviewer = args.reviewer
    server.include_legacy = bool(args.include_legacy)
    server.trigger_token = str(args.token or "").strip()
    server.job_dir = Path(args.job_dir or DATA_DIR / "ve_template_copy_jobs")
    server.auto_prepare = bool(args.auto_prepare)
    server.auto_execute_codex = bool(args.auto_execute_codex)
    server.codex_bin = str(args.codex_bin or "").strip()
    server.codex_model = str(args.codex_model or "").strip()
    server.public_trigger_url = str(args.public_trigger_url or "").strip()
    server.model_ref_dir = Path(args.model_ref_dir or DEFAULT_MODEL_REF_DIR)
    server.work_dir = Path(args.work_dir or DEFAULT_WORK_DIR)
    print(f"[ve-template-trigger] listening on http://{args.host}:{args.port}")
    print(f"[ve-template-trigger] jobs -> {server.job_dir}")
    print(
        "[ve-template-trigger] "
        f"auto_prepare={server.auto_prepare} auto_execute_codex={server.auto_execute_codex} "
        f"model_refs={server.model_ref_dir}"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[ve-template-trigger] stopped")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
