"""Environment diagnostics for the Guangdada crawler CLI."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

from guangdada_crawl import __version__
from guangdada_crawl.auth import AuthStore


def _importable(module: str) -> bool:
    try:
        return importlib.util.find_spec(module) is not None
    except ModuleNotFoundError:
        return False


def _core_root() -> Path | None:
    spec = importlib.util.find_spec("ua_workflows.shared.config")
    if not spec or not spec.origin:
        return None
    path = Path(spec.origin).resolve()
    try:
        return path.parents[2]
    except IndexError:
        return None


def build_doctor_report(*, home: Path, jobs_dir: Path, cwd: Path | None = None) -> dict[str, Any]:
    auth = AuthStore.from_value(home).status()
    core_root = _core_root()
    checks = {
        "guangdada_crawl_importable": _importable("guangdada_crawl"),
        "playwright_importable": _importable("playwright.async_api"),
        "core_search_importable": _importable("ua_workflows.shared.guangdada.search"),
        "core_chart_importable": _importable("ua_workflows.shared.guangdada.new_charts_ai_tools"),
        "mcp_importable": _importable("mcp.server.fastmcp"),
        "operation_config_exists": bool(core_root and (core_root / "config" / "operation.json").exists()),
    }
    ready_for_browser_crawl = bool(auth["has_auth_state"] or auth["has_credentials"])
    required_imports_ok = bool(
        checks["guangdada_crawl_importable"]
        and checks["playwright_importable"]
        and checks["core_search_importable"]
        and checks["core_chart_importable"]
    )
    notes: list[str] = []
    if not ready_for_browser_crawl:
        notes.append("Run `guangdada auth login --headed` or set GUANGDADA_EMAIL/GUANGDADA_PASSWORD before real browser crawls.")
    if not checks["operation_config_exists"]:
        notes.append("config/operation.json was not found; the shared crawler will rely on built-in selector fallbacks.")
    if not checks["mcp_importable"]:
        notes.append("Install `guangdada-crawl[mcp]` before running guangdada-mcp.")
    return {
        "command": "doctor",
        "status": "ok" if ready_for_browser_crawl and required_imports_ok else "setup_needed",
        "version": __version__,
        "cwd": str(cwd or Path.cwd()),
        "home": str(home),
        "jobs_dir": str(jobs_dir),
        "auth": auth,
        "checks": checks,
        "core_root": str(core_root) if core_root else None,
        "ready_for_browser_crawl": ready_for_browser_crawl,
        "notes": notes,
    }
