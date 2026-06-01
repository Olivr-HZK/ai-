"""Load VE play labels from the Feishu Bitable review table.

The user-facing play library now lives in the Bitable field named ``玩法``.
This module converts those labels into the asset-like shape expected by the
existing analysis and novelty code, without using the old curated JSON library.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests

from ua_workflows.shared.config import DATA_DIR, load_project_env

load_project_env()

PLAY_LABEL_FIELD_ENV = "VE_PLAY_LABEL_FIELD_NAME"
PLAY_LABEL_URL_ENV = "VE_PLAY_LABEL_BITABLE_URL"
DEFAULT_PLAY_LABEL_FIELD = "玩法"
PLAY_LABEL_CACHE_PATH = DATA_DIR / "ve_bitable_play_labels.json"


def _env_enabled(name: str, default: str = "1") -> bool:
    value = (os.getenv(name) or default).strip().lower()
    return value not in {"", "0", "false", "no", "off"}


def current_play_label_field_name() -> str:
    return (os.getenv(PLAY_LABEL_FIELD_ENV) or DEFAULT_PLAY_LABEL_FIELD).strip() or DEFAULT_PLAY_LABEL_FIELD


def _parse_bitable_url(url: str) -> tuple[str, str, str]:
    parsed = urlparse(url.strip())
    parts = [p for p in parsed.path.split("/") if p]
    app_token = parts[1] if len(parts) >= 2 and parts[0] == "base" else ""
    query = parse_qs(parsed.query or "")
    table_id = (query.get("table") or [""])[0]
    view_id = (query.get("view") or [""])[0]
    if not app_token or not table_id:
        raise RuntimeError(f"无法从多维表链接解析 app_token/table_id: {url}")
    return app_token, table_id, view_id


def _play_label_bitable_url() -> str:
    return (
        os.getenv(PLAY_LABEL_URL_ENV)
        or os.getenv("VIDEO_ENHANCER_BITABLE_URL")
        or ""
    ).strip()


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


def _extract_option_names(field: dict[str, Any]) -> list[str]:
    options: list[Any] = []
    prop = field.get("property")
    if isinstance(prop, dict):
        raw_options = prop.get("options")
        if isinstance(raw_options, list):
            options.extend(raw_options)
    raw_options = field.get("options")
    if isinstance(raw_options, list):
        options.extend(raw_options)

    out: list[str] = []
    for option in options:
        if isinstance(option, dict):
            name = str(option.get("name") or option.get("text") or option.get("value") or "").strip()
        else:
            name = str(option or "").strip()
        if name:
            out.append(name)
    return out


def _field_by_name(token: str, app_token: str, table_id: str, field_name: str) -> dict[str, Any] | None:
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    page_token = ""
    while True:
        params: dict[str, Any] = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token
        resp = requests.get(url, headers=_headers(token), params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"list fields failed: {data}")
        payload = data.get("data") or {}
        for field in payload.get("items") or payload.get("fields") or []:
            if isinstance(field, dict) and str(field.get("field_name") or "").strip() == field_name:
                return field
        if not payload.get("has_more"):
            return None
        page_token = str(payload.get("page_token") or "")
        if not page_token:
            return None


def _coerce_label_values(value: Any) -> list[str]:
    out: list[str] = []
    if value is None:
        return out
    if isinstance(value, str):
        candidates = re.split(r"[、,，\n/]+", value)
        out.extend(x.strip() for x in candidates if x.strip())
    elif isinstance(value, dict):
        text = str(value.get("text") or value.get("name") or value.get("value") or "").strip()
        if text:
            out.append(text)
    elif isinstance(value, list):
        for row in value:
            out.extend(_coerce_label_values(row))
    else:
        text = str(value).strip()
        if text:
            out.append(text)
    return out


def _labels_from_records(
    token: str,
    app_token: str,
    table_id: str,
    *,
    view_id: str = "",
    field_name: str,
    max_records: int = 2000,
) -> list[str]:
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    page_token = ""
    labels: list[str] = []
    seen: set[str] = set()
    fetched = 0
    while fetched < max_records:
        params: dict[str, Any] = {"page_size": min(500, max_records - fetched)}
        if page_token:
            params["page_token"] = page_token
        if view_id:
            params["view_id"] = view_id
        resp = requests.get(url, headers=_headers(token), params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"list records failed: {data}")
        payload = data.get("data") or {}
        rows = payload.get("items") or payload.get("records") or []
        fetched += len(rows)
        for row in rows:
            if not isinstance(row, dict):
                continue
            fields = row.get("fields") or {}
            if not isinstance(fields, dict):
                continue
            for label in _coerce_label_values(fields.get(field_name)):
                if label and label not in seen:
                    seen.add(label)
                    labels.append(label)
        if not payload.get("has_more"):
            break
        page_token = str(payload.get("page_token") or "")
        if not page_token:
            break
    return labels


def _manual_labels_from_env() -> list[str]:
    raw = os.getenv("VE_PLAY_LABELS") or ""
    labels: list[str] = []
    seen: set[str] = set()
    for label in re.split(r"[、,，\n]+", raw):
        text = label.strip()
        if text and text not in seen:
            seen.add(text)
            labels.append(text)
    return labels


def _read_cache() -> list[str]:
    try:
        payload = json.loads(PLAY_LABEL_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    labels = payload.get("labels") if isinstance(payload, dict) else payload
    if not isinstance(labels, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for label in labels:
        text = str(label or "").strip()
        if text and text not in seen:
            seen.add(text)
            out.append(text)
    return out


def _write_cache(labels: list[str], *, source: str, field_name: str) -> None:
    if not labels:
        return
    payload = {
        "source": source,
        "field_name": field_name,
        "labels": labels,
    }
    PLAY_LABEL_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    PLAY_LABEL_CACHE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


@lru_cache(maxsize=8)
def load_bitable_play_labels() -> tuple[str, ...]:
    """Return unique play labels from env, Bitable options/records, then cache."""
    manual = _manual_labels_from_env()
    if manual:
        return tuple(manual)

    field_name = current_play_label_field_name()
    bitable_url = _play_label_bitable_url()
    if bitable_url:
        try:
            app_token, table_id, view_id = _parse_bitable_url(bitable_url)
            token = _tenant_access_token()
            labels: list[str] = []
            field = None
            try:
                field = _field_by_name(token, app_token, table_id, field_name)
            except Exception as field_error:
                if _env_enabled("VE_PLAY_LABEL_WARN_ON_FETCH_FAIL", "1"):
                    print(f"[play-labels] 读取玩法字段选项失败，继续尝试扫描记录: {field_error}", flush=True)
            if field:
                labels.extend(_extract_option_names(field))
            if not labels:
                labels.extend(
                    _labels_from_records(
                        token,
                        app_token,
                        table_id,
                        view_id=view_id,
                        field_name=field_name,
                    )
                )
            if labels:
                deduped = list(dict.fromkeys(x for x in labels if x))
                _write_cache(deduped, source=bitable_url, field_name=field_name)
                return tuple(deduped)
        except Exception as e:
            if _env_enabled("VE_PLAY_LABEL_WARN_ON_FETCH_FAIL", "1"):
                print(f"[play-labels] 读取多维表玩法标签失败，改用缓存/空列表: {e}", flush=True)

    cached = _read_cache()
    return tuple(cached)


def play_label_asset_id(label: str) -> str:
    text = str(label or "").strip()
    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]
    return f"play_{digest}"


def labels_to_play_assets(labels: list[str] | tuple[str, ...]) -> list[dict[str, Any]]:
    assets: list[dict[str, Any]] = []
    for label in labels:
        name = str(label or "").strip()
        if not name:
            continue
        assets.append(
            {
                "asset_id": play_label_asset_id(name),
                "name": name,
                "definition": f"多维表格「{current_play_label_field_name()}」字段中的玩法标签",
                "aliases": [name],
                "keywords": [name],
                "subtags": [],
                "source": "bitable_play_label",
            }
        )
    return assets


def load_bitable_play_assets() -> list[dict[str, Any]]:
    return labels_to_play_assets(load_bitable_play_labels())
