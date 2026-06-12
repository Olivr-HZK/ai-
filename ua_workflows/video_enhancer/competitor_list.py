"""Sync the VE competitor list from the Feishu Bitable ``竞品list`` table."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse

import requests

from ua_workflows.shared.config import CONFIG_DIR, load_project_env

DEFAULT_CONFIG_PATH = CONFIG_DIR / "ai_product.json"
DEFAULT_TABLE_NAME = "竞品list"
DEFAULT_PRODUCT_FIELD = "广告主名"
DEFAULT_APPID_FIELD = "appid"
DEFAULT_TARGET_GROUPS = ("video", "photo")
DEFAULT_GROUP = "video"


@dataclass(frozen=True)
class CompetitorListRow:
    product: str
    appid: str


@dataclass(frozen=True)
class CompetitorSyncResult:
    ok: bool
    competitor_count: int = 0
    source_table_id: str = ""
    source_table_name: str = DEFAULT_TABLE_NAME
    config_path: str = ""
    dry_run: bool = False
    error: str = ""


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, bool)):
        return str(value).strip()
    if isinstance(value, dict):
        for key in ("text", "name", "value"):
            text = _text(value.get(key))
            if text:
                return text
        return ""
    if isinstance(value, list):
        return "、".join(part for part in (_text(item) for item in value) if part)
    return str(value).strip()


def _parse_base_app_token(bitable_url: str) -> str:
    parsed = urlparse(str(bitable_url or "").strip())
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) >= 2 and parts[0] == "base":
        return parts[1]
    raise RuntimeError(f"无法从多维表链接解析 app_token: {bitable_url}")


def _tenant_access_token() -> str:
    app_id = (os.getenv("FEISHU_APP_ID") or "").strip()
    app_secret = (os.getenv("FEISHU_APP_SECRET") or "").strip()
    if not app_id or not app_secret:
        raise RuntimeError("请在 .env 配置 FEISHU_APP_ID / FEISHU_APP_SECRET")
    resp = requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": app_id, "app_secret": app_secret},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"get tenant_access_token failed: {data}")
    return str(data["tenant_access_token"])


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}


def _list_tables(token: str, app_token: str) -> list[dict[str, Any]]:
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"
    page_token = ""
    out: list[dict[str, Any]] = []
    while True:
        params: dict[str, Any] = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token
        resp = requests.get(url, headers=_headers(token), params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"list tables failed: {data}")
        payload = data.get("data") or {}
        rows = payload.get("items") or payload.get("tables") or []
        if isinstance(rows, list):
            out.extend(row for row in rows if isinstance(row, dict))
        if not payload.get("has_more"):
            break
        page_token = str(payload.get("page_token") or "")
        if not page_token:
            break
    return out


def resolve_competitor_table_id(
    token: str,
    app_token: str,
    *,
    table_id: str = "",
    table_name: str = DEFAULT_TABLE_NAME,
) -> str:
    explicit = str(table_id or "").strip()
    if explicit:
        return explicit
    wanted = str(table_name or DEFAULT_TABLE_NAME).strip() or DEFAULT_TABLE_NAME
    for table in _list_tables(token, app_token):
        name = str(table.get("name") or "").strip()
        candidate_id = str(table.get("table_id") or table.get("id") or "").strip()
        if name == wanted and candidate_id:
            return candidate_id
    raise RuntimeError(f"未在多维表中找到表：{wanted}")


def _list_records(token: str, app_token: str, table_id: str) -> list[dict[str, Any]]:
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    page_token = ""
    out: list[dict[str, Any]] = []
    while True:
        params: dict[str, Any] = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token
        resp = requests.get(url, headers=_headers(token), params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"list competitor records failed: {data}")
        payload = data.get("data") or {}
        rows = payload.get("items") or payload.get("records") or []
        if isinstance(rows, list):
            out.extend(row for row in rows if isinstance(row, dict))
        if not payload.get("has_more"):
            break
        page_token = str(payload.get("page_token") or "")
        if not page_token:
            break
    return out


def normalize_competitor_records(
    records: Iterable[dict[str, Any]],
    *,
    product_field: str = DEFAULT_PRODUCT_FIELD,
    appid_field: str = DEFAULT_APPID_FIELD,
) -> list[CompetitorListRow]:
    out: list[CompetitorListRow] = []
    seen: set[tuple[str, str]] = set()
    for record in records:
        if not isinstance(record, dict):
            continue
        fields = record.get("fields") if isinstance(record.get("fields"), dict) else record
        product = _text(fields.get(product_field))
        appid = _text(fields.get(appid_field))
        if not product or not appid:
            continue
        key = (product.casefold(), appid.casefold())
        if key in seen:
            continue
        seen.add(key)
        out.append(CompetitorListRow(product=product, appid=appid))
    return out


def load_competitors_from_bitable(
    bitable_url: str,
    *,
    table_id: str = "",
    table_name: str = DEFAULT_TABLE_NAME,
    product_field: str = DEFAULT_PRODUCT_FIELD,
    appid_field: str = DEFAULT_APPID_FIELD,
) -> list[CompetitorListRow]:
    load_project_env()
    app_token = _parse_base_app_token(bitable_url)
    token = _tenant_access_token()
    resolved_table_id = resolve_competitor_table_id(
        token,
        app_token,
        table_id=table_id,
        table_name=table_name,
    )
    return normalize_competitor_records(
        _list_records(token, app_token, resolved_table_id),
        product_field=product_field,
        appid_field=appid_field,
    )


def _read_ai_product_config(config_path: Path) -> dict[str, Any]:
    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if not isinstance(data, dict):
        raise RuntimeError(f"竞品配置不是 JSON object: {config_path}")
    return data


def merge_competitors_into_ai_product_config(
    existing_config: dict[str, Any],
    rows: Iterable[CompetitorListRow],
    *,
    target_groups: Iterable[str] = DEFAULT_TARGET_GROUPS,
    default_group: str = DEFAULT_GROUP,
) -> dict[str, Any]:
    groups = tuple(str(group) for group in target_groups if str(group).strip())
    if not groups:
        groups = DEFAULT_TARGET_GROUPS
    default = default_group if default_group in groups else groups[0]

    known_group_by_product: dict[str, str] = {}
    for group in groups:
        section = existing_config.get(group)
        if isinstance(section, dict):
            for product in section:
                known_group_by_product[str(product)] = group

    replacement: dict[str, dict[str, str]] = {group: {} for group in groups}
    for row in rows:
        product = str(row.product or "").strip()
        appid = str(row.appid or "").strip()
        if not product or not appid:
            continue
        group = known_group_by_product.get(product, default)
        replacement.setdefault(group, {})[product] = appid

    merged: dict[str, Any] = {}
    emitted_groups: set[str] = set()
    for key, value in existing_config.items():
        if key in replacement:
            merged[key] = replacement[key]
            emitted_groups.add(key)
        else:
            merged[key] = value
    for group in groups:
        if group not in emitted_groups:
            merged[group] = replacement.get(group, {})
    return merged


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def sync_competitor_config_from_bitable(
    *,
    config_path: Path = DEFAULT_CONFIG_PATH,
    bitable_url: str = "",
    table_id: str = "",
    table_name: str = "",
    product_field: str = "",
    appid_field: str = "",
    dry_run: bool = False,
) -> CompetitorSyncResult:
    load_project_env()
    url = (bitable_url or os.getenv("VIDEO_ENHANCER_BITABLE_URL") or "").strip()
    resolved_table_id = (
        table_id
        or os.getenv("VE_COMPETITOR_LIST_TABLE_ID")
        or _table_id_from_url(os.getenv("VE_COMPETITOR_LIST_BITABLE_URL") or "")
        or ""
    ).strip()
    resolved_table_name = (table_name or os.getenv("VE_COMPETITOR_LIST_TABLE_NAME") or DEFAULT_TABLE_NAME).strip()
    resolved_product_field = (product_field or os.getenv("VE_COMPETITOR_LIST_PRODUCT_FIELD") or DEFAULT_PRODUCT_FIELD).strip()
    resolved_appid_field = (appid_field or os.getenv("VE_COMPETITOR_LIST_APPID_FIELD") or DEFAULT_APPID_FIELD).strip()
    if not url:
        return CompetitorSyncResult(
            ok=False,
            config_path=str(config_path),
            dry_run=dry_run,
            error="未配置 VIDEO_ENHANCER_BITABLE_URL",
        )
    try:
        rows = load_competitors_from_bitable(
            url,
            table_id=resolved_table_id,
            table_name=resolved_table_name,
            product_field=resolved_product_field,
            appid_field=resolved_appid_field,
        )
        if not rows:
            return CompetitorSyncResult(
                ok=False,
                source_table_id=resolved_table_id,
                source_table_name=resolved_table_name,
                config_path=str(config_path),
                dry_run=dry_run,
                error="竞品list 表为空，未覆盖本地配置",
            )
        existing = _read_ai_product_config(config_path)
        merged = merge_competitors_into_ai_product_config(existing, rows)
        if not dry_run:
            _write_json_atomic(config_path, merged)
        return CompetitorSyncResult(
            ok=True,
            competitor_count=len(rows),
            source_table_id=resolved_table_id,
            source_table_name=resolved_table_name,
            config_path=str(config_path),
            dry_run=dry_run,
        )
    except Exception as exc:
        return CompetitorSyncResult(
            ok=False,
            source_table_id=resolved_table_id,
            source_table_name=resolved_table_name,
            config_path=str(config_path),
            dry_run=dry_run,
            error=str(exc),
        )


def _table_id_from_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    query = parse_qs(parsed.query or "")
    return str((query.get("table") or [""])[0]).strip()


def _env_enabled(name: str, default: str = "1") -> bool:
    value = (os.getenv(name) or default).strip().lower()
    return value in {"1", "true", "yes", "on"}


def sync_competitor_config_if_enabled(*, bitable_url: str = "", config_path: Path = DEFAULT_CONFIG_PATH) -> CompetitorSyncResult:
    if not _env_enabled("VE_COMPETITOR_LIST_SYNC_ENABLED", "1"):
        return CompetitorSyncResult(
            ok=False,
            config_path=str(config_path),
            error="VE_COMPETITOR_LIST_SYNC_ENABLED=0",
        )
    return sync_competitor_config_from_bitable(config_path=config_path, bitable_url=bitable_url)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="从多维表竞品list同步 VE 竞品配置")
    parser.add_argument("--bitable-url", default="", help="Base 链接；默认 VIDEO_ENHANCER_BITABLE_URL")
    parser.add_argument("--config-path", default=str(DEFAULT_CONFIG_PATH), help="ai_product.json 路径")
    parser.add_argument("--table-id", default="", help="竞品list 表 ID；默认按表名查找")
    parser.add_argument("--table-name", default="", help=f"竞品list 表名；默认 {DEFAULT_TABLE_NAME}")
    parser.add_argument("--product-field", default="", help=f"广告主字段名；默认 {DEFAULT_PRODUCT_FIELD}")
    parser.add_argument("--appid-field", default="", help=f"appid 字段名；默认 {DEFAULT_APPID_FIELD}")
    parser.add_argument("--dry-run", action="store_true", help="只读取和合并，不写配置")
    parser.add_argument("--strict", action="store_true", help="同步失败时以非 0 退出")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_project_env()
    args = parse_args(argv)
    result = sync_competitor_config_from_bitable(
        config_path=Path(args.config_path),
        bitable_url=args.bitable_url,
        table_id=args.table_id,
        table_name=args.table_name,
        product_field=args.product_field,
        appid_field=args.appid_field,
        dry_run=bool(args.dry_run),
    )
    if result.ok:
        mode = "dry-run 读取" if result.dry_run else "已同步"
        print(f"[competitor-list] {mode} {result.competitor_count} 个正式竞品 -> {result.config_path}")
        return 0
    print(f"[competitor-list] 同步失败，沿用本地配置：{result.error}")
    return 1 if args.strict else 0


if __name__ == "__main__":
    raise SystemExit(main())
