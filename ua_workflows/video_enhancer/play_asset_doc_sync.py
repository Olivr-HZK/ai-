"""Sync the VE play asset library with a Lark cloud document.

The Lark document is the human-editable surface. The local JSON remains the
runtime format used by matching, dashboards, and daily pushes.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from datetime import date
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from ua_workflows.shared.config import PROJECT_ROOT
from ua_workflows.video_enhancer.play_assets import DEFAULT_PLAY_ASSET_PATH

DEFAULT_PLAY_ASSET_DOC_URL = "https://www.feishu.cn/docx/HrxAdmiN6o7S4BxSNpXcT8h2n1n"
SYNC_START = "<!-- VE_PLAY_ASSET_DOC_SYNC:START -->"
SYNC_END = "<!-- VE_PLAY_ASSET_DOC_SYNC:END -->"


def _split_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    text = str(value or "").strip()
    if not text or text == "-":
        return []
    return [x.strip() for x in re.split(r"[、,，]\s*", text) if x.strip()]


def _yaml_load(text: str) -> dict[str, Any]:
    try:
        data = yaml.safe_load(text) or {}
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _asset_yaml(asset: dict[str, Any]) -> str:
    keep_keys = [
        "asset_id",
        "name",
        "status",
        "definition",
        "aliases",
        "min_score",
        "include_keywords",
        "exclude_keywords",
        "variant_dimensions",
        "subtags",
        "representative_ad_keys",
        "example_effects",
        "source_dates",
        "notes",
    ]
    payload = {key: asset.get(key) for key in keep_keys if asset.get(key) not in (None, "", [])}
    return yaml.safe_dump(payload, allow_unicode=True, sort_keys=False).strip()


def render_play_asset_doc(payload: dict[str, Any]) -> str:
    assets = [asset for asset in payload.get("assets") or [] if isinstance(asset, dict)]
    internal_source = payload.get("internal_launch_source")
    internal_source = internal_source if isinstance(internal_source, dict) else {}
    lines: list[str] = [
        "# VE 玩法资产库（项目联动版）",
        "",
        "> 这是项目读取的玩法资产源。同事可以直接编辑每个玩法块里的 YAML 字段；项目会把这些字段同步回 `config/ve_play_assets.json`。",
        "> 建议只改 `name`、`definition`、`aliases`、`include_keywords`、`exclude_keywords`、`subtags`、`representative_ad_keys`、`example_effects`、`notes`。`asset_id` 和 `tag_id` 尽量保持稳定。",
        "",
        f"- 当前资产数：{len(assets)}",
        f"- 本地更新时间：{payload.get('updated_at') or date.today().isoformat()}",
        f"- 历史范围：{' ~ '.join(str(x) for x in payload.get('source_date_range') or []) or '-'}",
    ]
    if internal_source:
        source_title = str(internal_source.get("title") or "内部特效上线记录")
        source_sheet = str(internal_source.get("sheet") or "")
        source_name = f"{source_title} / {source_sheet}" if source_sheet else source_title
        launch_range = " ~ ".join(str(x) for x in internal_source.get("date_range") or []) or "-"
        lines.extend(
            [
                f"- 内部上线记录：{source_name}，{internal_source.get('record_count') or '-'} 条，{launch_range}",
                f"- 内部表入口：{internal_source.get('url') or '-'}",
            ]
        )
    lines.extend(["", SYNC_START, ""])
    for idx, asset in enumerate(assets, 1):
        lines.extend(
            [
                f"## {idx}. {asset.get('name') or asset.get('asset_id') or '未命名玩法'}",
                "",
                str(asset.get("definition") or "").strip(),
                "",
                "```yaml",
                _asset_yaml(asset),
                "```",
                "",
            ]
        )
        reps = [str(x) for x in asset.get("representative_ad_keys") or [] if x]
        if reps:
            lines.append("**代表素材**")
            lines.append("")
            for ad_key in reps:
                lines.append(f"- `{ad_key}`")
            lines.append("")
    lines.extend([SYNC_END, ""])
    return "\n".join(lines)


def parse_play_asset_doc(markdown: str, *, base_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    base = dict(base_payload or {})
    source = markdown
    if SYNC_START in source and SYNC_END in source:
        source = source.split(SYNC_START, 1)[1].split(SYNC_END, 1)[0]

    assets: list[dict[str, Any]] = []
    for match in re.finditer(r"^##\s+\d+\.\s+(.+?)\s*$", source, flags=re.M):
        start = match.end()
        next_match = re.search(r"^##\s+\d+\.\s+.+?\s*$", source[start:], flags=re.M)
        end = start + next_match.start() if next_match else len(source)
        section = source[start:end]
        yaml_match = re.search(r"```yaml\s*(.*?)```", section, flags=re.S)
        asset = _yaml_load(yaml_match.group(1)) if yaml_match else {}
        if not asset:
            continue
        title = match.group(1).strip()
        asset.setdefault("name", title)
        asset["aliases"] = _split_list(asset.get("aliases"))
        asset["include_keywords"] = _split_list(asset.get("include_keywords"))
        asset["exclude_keywords"] = _split_list(asset.get("exclude_keywords"))
        asset["variant_dimensions"] = _split_list(asset.get("variant_dimensions"))
        asset["representative_ad_keys"] = _split_list(asset.get("representative_ad_keys"))
        asset["example_effects"] = _split_list(asset.get("example_effects"))
        asset["source_dates"] = _split_list(asset.get("source_dates"))
        subtags: list[dict[str, Any]] = []
        for subtag in asset.get("subtags") or []:
            if isinstance(subtag, dict) and subtag.get("tag_id"):
                fixed = dict(subtag)
                fixed["keywords"] = _split_list(fixed.get("keywords"))
                subtags.append(fixed)
        asset["subtags"] = subtags
        assets.append(asset)

    base["schema_version"] = int(base.get("schema_version") or 1)
    base["updated_at"] = date.today().isoformat()
    base["assets"] = assets
    return base


def _read_local_payload(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"schema_version": 1, "assets": []}


def fetch_lark_doc_markdown(doc_url: str, *, identity: str = "bot") -> str:
    cmd = [
        "lark-cli",
        "docs",
        "+fetch",
        "--as",
        identity,
        "--doc",
        doc_url,
        "--format",
        "json",
    ]
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
    if res.returncode != 0:
        raise RuntimeError((res.stderr or res.stdout).strip() or "lark-cli docs +fetch failed")
    payload = json.loads(res.stdout)
    data = payload.get("data") if isinstance(payload, dict) else {}
    markdown = data.get("markdown") if isinstance(data, dict) else ""
    if not markdown:
        raise RuntimeError("Lark doc fetch returned empty markdown")
    return str(markdown)


def update_lark_doc(doc_url: str, markdown: str, *, identity: str = "bot") -> None:
    cmd = [
        "lark-cli",
        "docs",
        "+update",
        "--as",
        identity,
        "--doc",
        doc_url,
        "--mode",
        "overwrite",
        "--markdown",
        "-",
        "--new-title",
        "VE 玩法资产库（项目联动版）",
    ]
    res = subprocess.run(cmd, input=markdown, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
    if res.returncode != 0:
        raise RuntimeError((res.stderr or res.stdout).strip() or "lark-cli docs +update failed")


def pull_doc_to_json(
    *,
    doc_url: str,
    json_path: Path = DEFAULT_PLAY_ASSET_PATH,
    identity: str = "bot",
) -> Path:
    base = _read_local_payload(json_path)
    markdown = fetch_lark_doc_markdown(doc_url, identity=identity)
    payload = parse_play_asset_doc(markdown, base_payload=base)
    if not payload.get("assets"):
        raise RuntimeError("No play assets parsed from Lark doc; local JSON not changed")
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return json_path


def push_json_to_doc(
    *,
    doc_url: str,
    json_path: Path = DEFAULT_PLAY_ASSET_PATH,
    identity: str = "bot",
) -> None:
    payload = _read_local_payload(json_path)
    markdown = render_play_asset_doc(payload)
    update_lark_doc(doc_url, markdown, identity=identity)


def append_draft_assets_to_doc(
    *,
    doc_url: str,
    draft_items: list[dict[str, Any]],
    identity: str = "bot",
) -> int:
    if not draft_items:
        return 0
    today = date.today().isoformat()
    lines = [f"## 待沉淀新玩法草稿 {today}", ""]
    for idx, item in enumerate(draft_items, 1):
        title = str(item.get("play_fingerprint") or item.get("effect_one_liner") or f"待命名玩法 {idx}").strip()
        draft_id = re.sub(r"[^a-z0-9_]+", "_", title.lower())[:48].strip("_") or f"draft_{today}_{idx}"
        asset = {
            "asset_id": f"draft_{draft_id}",
            "name": title,
            "status": "draft",
            "definition": str(item.get("effect_one_liner") or title),
            "aliases": [],
            "min_score": 1,
            "include_keywords": _split_list(title),
            "exclude_keywords": [],
            "variant_dimensions": [],
            "subtags": [],
            "representative_ad_keys": [str(item.get("ad_key") or "")],
            "example_effects": [str(item.get("effect_one_liner") or title)],
            "source_dates": [str(item.get("target_date") or today)],
            "notes": "自动追加草稿，请人工补充定义、关键词和子标签后再改为 active。",
        }
        lines.extend([f"### {idx}. {title}", "", "```yaml", _asset_yaml(asset), "```", ""])
    cmd = [
        "lark-cli",
        "docs",
        "+update",
        "--as",
        identity,
        "--doc",
        doc_url,
        "--mode",
        "append",
        "--markdown",
        "\n".join(lines),
    ]
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
    if res.returncode != 0:
        raise RuntimeError((res.stderr or res.stdout).strip() or "lark-cli docs +update append failed")
    return len(draft_items)


def draft_assets_for_date(target_date: str) -> list[dict[str, Any]]:
    from ua_workflows.video_enhancer.play_asset_report import build_daily_asset_variant_report

    report = build_daily_asset_variant_report(target_date)
    drafts: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in report.get("asset_variant_items") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("play_asset_id") or "").strip():
            continue
        key = str(item.get("play_asset_variant_key") or item.get("play_fingerprint") or item.get("ad_key") or "")
        if key in seen:
            continue
        seen.add(key)
        drafts.append(item)
    return drafts


def maybe_pull_play_asset_doc() -> bool:
    load_dotenv(PROJECT_ROOT / ".env", override=True)
    enabled = (os.getenv("VE_PLAY_ASSET_DOC_SYNC_ENABLED") or "1").strip().lower()
    if enabled in ("0", "false", "no", "off", ""):
        return False
    doc_url = (os.getenv("VE_PLAY_ASSET_DOC_URL") or DEFAULT_PLAY_ASSET_DOC_URL).strip()
    identity = (os.getenv("VE_PLAY_ASSET_DOC_SYNC_IDENTITY") or "bot").strip()
    try:
        pull_doc_to_json(doc_url=doc_url, identity=identity)
        return True
    except Exception as exc:
        print(f"[play-assets-doc] 云文档同步失败，继续使用本地玩法库：{exc}")
        return False


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env", override=True)
    parser = argparse.ArgumentParser(description="同步 VE 玩法资产库与飞书云文档")
    parser.add_argument(
        "mode",
        choices=["render", "push-doc", "pull-doc", "append-drafts"],
        help="render=只输出 markdown；push-doc=本地 JSON 覆盖云文档；pull-doc=云文档覆盖本地 JSON；append-drafts=把某日待沉淀新玩法草稿追加到云文档",
    )
    parser.add_argument("--doc", default=os.getenv("VE_PLAY_ASSET_DOC_URL") or DEFAULT_PLAY_ASSET_DOC_URL)
    parser.add_argument("--json", default=str(DEFAULT_PLAY_ASSET_PATH))
    parser.add_argument("--identity", default=os.getenv("VE_PLAY_ASSET_DOC_SYNC_IDENTITY") or "bot")
    parser.add_argument("--output", default="")
    parser.add_argument("--date", default="", help="append-drafts 使用的目标日期 YYYY-MM-DD")
    args = parser.parse_args()

    json_path = Path(args.json)
    if args.mode == "render":
        markdown = render_play_asset_doc(_read_local_payload(json_path))
        if args.output:
            Path(args.output).write_text(markdown, encoding="utf-8")
            print(f"[play-assets-doc] 已写 {args.output}")
        else:
            print(markdown)
    elif args.mode == "push-doc":
        push_json_to_doc(doc_url=args.doc, json_path=json_path, identity=args.identity)
        print(f"[play-assets-doc] 已将 {json_path} 推送到云文档")
    elif args.mode == "pull-doc":
        out = pull_doc_to_json(doc_url=args.doc, json_path=json_path, identity=args.identity)
        print(f"[play-assets-doc] 已从云文档同步到 {out}")
    elif args.mode == "append-drafts":
        if not args.date:
            raise SystemExit("append-drafts 需要 --date YYYY-MM-DD")
        drafts = draft_assets_for_date(args.date)
        count = append_draft_assets_to_doc(doc_url=args.doc, draft_items=drafts, identity=args.identity)
        print(f"[play-assets-doc] 已追加待沉淀草稿 {count} 个")


if __name__ == "__main__":
    main()
