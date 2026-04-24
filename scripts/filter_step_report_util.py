"""
工作流分步「过滤」落盘 JSON，便于对账（封面聚类、我方已投放库等）。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from path_util import DATA_DIR as _DEFAULT_DATA
except Exception:  # pragma: no cover
    _DEFAULT_DATA = Path("data")  # type: ignore[assignment,misc]


def _write(
    data_dir: Path,
    name: str,
    payload: Dict[str, Any],
) -> Path:
    p = (data_dir if data_dir is not None else _DEFAULT_DATA) / name
    p.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return p


def write_cover_filter_step_json(
    data_dir: Path,
    output_prefix: str,
    target_date: str,
    cover_report: Dict[str, Any],
) -> Path:
    return _write(
        data_dir,
        f"{output_prefix}_filter_step3_cover.json",
        {
            "target_date": target_date,
            "step": "cover_style_intraday",
            "report": cover_report,
        },
    )


def write_cover_filter_step_json_skipped(
    data_dir: Path,
    output_prefix: str,
    target_date: str,
    reason: str,
) -> Path:
    return _write(
        data_dir,
        f"{output_prefix}_filter_step3_cover.json",
        {
            "target_date": target_date,
            "step": "cover_style_intraday",
            "skipped": True,
            "reason": reason,
        },
    )


def write_launched_filter_step_json(
    data_dir: Path,
    output_prefix: str,
    target_date: str,
    details: List[Dict[str, Any]],
    *,
    marked_count: int = 0,
) -> Path:
    return _write(
        data_dir,
        f"{output_prefix}_filter_step_launched_effects.json",
        {
            "target_date": target_date,
            "step": "launched_effects",
            "marked_count": int(marked_count),
            "details": details,
        },
    )
