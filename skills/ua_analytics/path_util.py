from pathlib import Path

# skills 目录：.../skills/ua_analytics
SKILL_ROOT = Path(__file__).resolve().parent
# 项目根目录：skills/ua_analytics 的上上级（在项目内运行时）
PROJECT_ROOT = SKILL_ROOT.parent.parent if len(SKILL_ROOT.parents) >= 2 else SKILL_ROOT


def _pick_dir(preferred: Path, fallback: Path) -> Path:
    """
    优先使用项目根目录下的路径；若不存在则退回到 skills/ua_analytics 下的本地目录。
    这样既支持在项目根运行，也支持把整个 skills 目录单独拿出去跑。
    """
    if preferred.exists():
        return preferred
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


# 配置与输入
_project_config_dir = PROJECT_ROOT / "config"
_project_data_dir = PROJECT_ROOT / "data"
_project_reports_dir = PROJECT_ROOT / "reports"
_project_downloads_dir = PROJECT_ROOT / "ua_downloads"

_skill_config_dir = SKILL_ROOT / "config"
_skill_data_dir = SKILL_ROOT / "data"
_skill_reports_dir = SKILL_ROOT / "reports"
_skill_downloads_dir = SKILL_ROOT / "ua_downloads"

CONFIG_DIR = _pick_dir(_project_config_dir, _skill_config_dir)
DATA_DIR = _pick_dir(_project_data_dir, _skill_data_dir)
REPORTS_DIR = _pick_dir(_project_reports_dir, _skill_reports_dir)
DOWNLOADS_DIR = _pick_dir(_project_downloads_dir, _skill_downloads_dir)

# 确保输出目录存在
DATA_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def config_path(name: str) -> Path:
    return CONFIG_DIR / name


def data_path(name: str) -> Path:
    return DATA_DIR / name


def report_path(name: str) -> Path:
    return REPORTS_DIR / name

