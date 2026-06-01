"""Project paths shared by all workflows."""

from __future__ import annotations

from pathlib import Path

from dotenv import dotenv_values, load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"
DOWNLOADS_DIR = PROJECT_ROOT / "ua_downloads"

DATA_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def config_path(name: str) -> Path:
    return CONFIG_DIR / name


def data_path(name: str) -> Path:
    return DATA_DIR / name


def report_path(name: str) -> Path:
    return REPORTS_DIR / name


def project_env_path() -> Path:
    return PROJECT_ROOT / ".env"


def load_project_env(*, override: bool = True) -> bool:
    """Load the repo .env, preferring it over stale shell/cron values by default."""
    return load_dotenv(project_env_path(), override=override)


def project_env_values() -> dict[str, str | None]:
    return dict(dotenv_values(project_env_path()))
