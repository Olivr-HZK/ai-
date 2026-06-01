#!/usr/bin/env python3
"""
根据 data/launched_effects_cache.json 重算预处理，输出简短对照：
  飞书「说明」→ 参与语义相似度匹配时用的文本（canonical_text）

生成：data/launched_effects_match_fields_preview.md

用法：在项目根目录
  PYTHONPATH=scripts .venv/bin/python3 scripts/preview_launched_effects_match_fields.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

import launched_effects_db as m  # noqa: E402


def _feishu_head(desc: str, max_len: int = 96) -> str:
    line = (desc or "").split("\n")[0].strip().replace("|", "｜")
    if len(line) > max_len:
        return line[: max_len - 1] + "…"
    return line or "（空）"


def main() -> None:
    cache_path = PROJECT_ROOT / "data" / "launched_effects_cache.json"
    if not cache_path.exists():
        print(f"缺少 {cache_path}", file=sys.stderr)
        sys.exit(1)

    cache = json.loads(cache_path.read_text(encoding="utf-8"))
    raw_rows = cache.get("effects", [])

    parsed: list[dict] = []
    for e in raw_rows:
        desc = str(e.get("description") or "").strip()
        if not desc:
            continue
        excluded = m._should_exclude_from_match(desc)
        primary = m._primary_block(desc)
        canonical_text = "" if excluded else m._canonical_text_for_embedding(desc)
        keywords = [] if excluded else m._build_keywords_for_effect(desc, primary)
        parsed.append(
            {
                "description": desc,
                "keywords": keywords,
                "canonical_text": canonical_text,
                "excluded_from_match": excluded,
            }
        )

    effects, _n_merge = m._dedupe_effects_by_canonical(parsed)

    lines: list[str] = []
    lines.append("# 飞书说明 → 语义匹配用文本\n\n")
    n = sum(
        1
        for e in effects
        if not e.get("excluded_from_match") and (e.get("canonical_text") or "").strip()
    )
    lines.append(f"（共 **{n}** 条参与语义层；运维说明类已剔除）\n\n")
    lines.append("| 飞书「说明」（首行） | 语义匹配用的字符串 |")
    lines.append("\n| --- | --- |")

    for e in effects:
        if e.get("excluded_from_match"):
            continue
        ct = (e.get("canonical_text") or "").strip()
        if not ct:
            continue
        head = _feishu_head(e.get("description") or "")
        cell_ct = ct.replace("|", "｜").replace("\n", " ")
        lines.append(f"\n| {head} | {cell_ct} |")

    out_path = PROJECT_ROOT / "data" / "launched_effects_match_fields_preview.md"
    out_path.write_text("".join(lines), encoding="utf-8")
    print(f"已写入 {out_path}（{n} 行）")


if __name__ == "__main__":
    main()
