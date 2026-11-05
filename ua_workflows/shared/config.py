"""Project paths shared by all workflows."""

from __future__ import annotations

from pathlib import Path
import os

from dotenv import dotenv_values, load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"
def persistent_root(environ=None):
    """Opt in explicitly; a typo must never create an empty history store."""
    value = (os.environ if environ is None else environ).get('UA_STATE_ROOT')
    if value is None:
        return PROJECT_ROOT
    root = Path(value).expanduser()
    if not value or not root.is_absolute() or not root.is_dir():
        raise ValueError('UA_STATE_ROOT must be an existing absolute directory')
    root = root.resolve()
    if not all((root / name).is_dir() for name in ('data', 'reports')):
        raise ValueError('Persistent data/reports must be provisioned before activation')
    return root


def validate_persistent_layout(code_root, state_root):
    """Keep hardcoded legacy paths on the same store as the explicit API."""
    code, state = Path(code_root), Path(state_root)
    for name in ('data', 'reports', '.env', '.venv'):
        old, actual = code / name, state / name
        if not old.is_symlink() or not actual.exists() or actual.is_symlink() or old.resolve() != actual.resolve():
            raise ValueError(f'Persistent compatibility link missing or incorrect: {name}')
    if (state / '.env').stat().st_mode & 0o077:
        raise ValueError('Persistent application environment must be private')


STATE_ROOT = persistent_root()
if STATE_ROOT != PROJECT_ROOT:
    validate_persistent_layout(PROJECT_ROOT, STATE_ROOT)
DATA_DIR = STATE_ROOT / "data"
REPORTS_DIR = STATE_ROOT / "reports"
DOWNLOADS_DIR = STATE_ROOT / "ua_downloads"

DATA_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def config_path(name: str) -> Path:
    return CONFIG_DIR / name


def data_path(name: str) -> Path:
    return DATA_DIR / name


def report_path(name: str) -> Path:
    return REPORTS_DIR / name


def project_env_path() -> Path:
    return STATE_ROOT / ".env"


def project_path(value) -> Path:
    """Resolve configured relative state paths; explicit absolute paths stay explicit."""
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    if '..' in path.parts:
        raise ValueError('Relative project paths cannot escape their root')
    base = STATE_ROOT if path.parts and path.parts[0] in ('data', 'reports', 'ua_downloads', '.env') else PROJECT_ROOT
    return base / path


def load_project_env(*, override: bool = True) -> bool:
    """Load the repo .env, preferring it over stale shell/cron values by default."""
    return load_dotenv(project_env_path(), override=override)


def project_env_values() -> dict[str, str | None]:
    return dict(dotenv_values(project_env_path()))
