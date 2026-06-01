"""Helpers for crawl-only smoke test entrypoints."""

from __future__ import annotations

import os
from collections.abc import Mapping, MutableMapping


def build_video_enhancer_crawl_args(
    *,
    date: str,
    product: str,
    output_prefix: str,
) -> list[str]:
    args = ["--target-date", date, "--products", product]
    if output_prefix:
        args.extend(["--output-prefix", output_prefix])
    return args


def build_arrow2_crawl_args(
    *,
    date: str,
    product: str,
    pull_only: str,
    output_prefix: str,
) -> list[str]:
    args = ["--date", date, "--products", product, "--no-pause", "--pull-only", pull_only]
    if output_prefix:
        args.extend(["--output-prefix", output_prefix])
    return args


def apply_browser_mode(
    env: Mapping[str, str] | None = None,
    *,
    headed: bool,
    headless: bool,
) -> dict[str, str]:
    next_env = dict(env or os.environ)
    if headed:
        next_env["DEBUG"] = "1"
    elif headless:
        next_env.pop("DEBUG", None)
    return next_env


def update_process_browser_mode(*, headed: bool, headless: bool) -> None:
    next_env = apply_browser_mode(os.environ, headed=headed, headless=headless)
    _replace_debug(os.environ, next_env)


def exposure_attempt_succeeded(output: str, returncode: int = 0) -> bool:
    return (
        returncode == 0
        and "Top 创意下拉 ✓" in output
        and "Top 创意下拉 ✗" not in output
        and "Traceback" not in output
    )


def _replace_debug(target: MutableMapping[str, str], source: Mapping[str, str]) -> None:
    if "DEBUG" in source:
        target["DEBUG"] = source["DEBUG"]
    else:
        target.pop("DEBUG", None)
