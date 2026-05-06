"""
项目路径统一配置。所有脚本通过此模块获取路径，便于维护与重构。
"""
from pathlib import Path

# 项目根目录（脚本位于 scripts/ 下时，parent.parent 为项目根）
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# 配置与输入
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"
DOWNLOADS_DIR = PROJECT_ROOT / "ua_downloads"

# 确保输出目录存在（首次运行或 clone 后）
DATA_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def config_path(name: str) -> Path:
    return CONFIG_DIR / name


def data_path(name: str) -> Path:
    return DATA_DIR / name


def report_path(name: str) -> Path:
    return REPORTS_DIR / name
