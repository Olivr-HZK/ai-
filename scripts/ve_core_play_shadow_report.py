#!/usr/bin/env python3
"""VE core-selling-point shadow dedupe report.

This script is intentionally report-only. It does not mutate the Bitable or
change the main VE sync path.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sqlite3
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests
from dotenv import load_dotenv

from path_util import PROJECT_ROOT


POSITIVE_STATUSES = {"采纳", "入素材库"}
NEGATIVE_STATUSES = {"不采纳", "重复抓取"}
DECISIVE_STATUSES = POSITIVE_STATUSES | NEGATIVE_STATUSES
DEFAULT_MODEL = "qwen/qwen3.6-plus"
DEFAULT_REVIEWER_FIELD = "浩鹏接受情况"


load_dotenv(PROJECT_ROOT / ".env")


def today_shanghai() -> str:
    return dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).strftime("%Y-%m-%d")


def default_workers() -> int:
    raw = (os.getenv("VE_CORE_PLAY_DEDUPE_WORKERS") or "4").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 4


def default_label_batch_size() -> int:
    raw = (os.getenv("VE_CORE_PLAY_LABEL_BATCH_SIZE") or "8").strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return 8


def flatten_field_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return " ".join(s for item in value if (s := flatten_field_value(item))).strip()
    if isinstance(value, dict):
        for key in ("text", "name", "value", "link"):
            if key in value:
                return flatten_field_value(value[key])
        return json.dumps(value, ensure_ascii=False)
    return str(value).strip()


def extract_label_from_material_tags(material_tags: str) -> str:
    text = str(material_tags or "")
    match = re.search(r"玩法资产[:：]\s*([^、,，;；\s]+)", text)
    return match.group(1).strip() if match else ""


def extract_play_label(row: dict[str, Any]) -> str:
    for key in ("play_label", "play", "play_asset_name"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    tag_label = extract_label_from_material_tags(str(row.get("material_tags") or ""))
    if tag_label:
        return tag_label
    value = str(row.get("play_asset_subtag_names") or "").strip()
    return value


def date_to_ymd(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(
            float(value) / 1000,
            dt.timezone(dt.timedelta(hours=8)),
        ).strftime("%Y-%m-%d")
    text = str(value)
    m = re.search(r"20\d\d-\d\d-\d\d", text)
    return m.group(0) if m else text[:10]


def parse_bitable_url(url: str) -> tuple[str, str]:
    parsed = urlparse((url or "").strip())
    parts = [p for p in parsed.path.split("/") if p]
    app_token = parts[1] if len(parts) >= 2 and parts[0] == "base" else ""
    table_id = (parse_qs(parsed.query or "").get("table") or [""])[0]
    if not app_token or not table_id:
        raise RuntimeError(f"无法从链接解析 app_token/table_id: {url}")
    return app_token, table_id


def get_tenant_access_token() -> str:
    app_id = (os.getenv("FEISHU_APP_ID") or "").strip()
    app_secret = (os.getenv("FEISHU_APP_SECRET") or "").strip()
    if not app_id or not app_secret:
        raise RuntimeError("请在 .env 配置 FEISHU_APP_ID / FEISHU_APP_SECRET")
    resp = requests.post(
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        json={"app_id": app_id, "app_secret": app_secret},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"get tenant_access_token failed: {data}")
    return str(data["tenant_access_token"])


def fetch_bitable_rows(
    *,
    bitable_url: str,
    reviewer_field: str,
) -> list[dict[str, Any]]:
    app_token, table_id = parse_bitable_url(bitable_url)
    token = get_tenant_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    rows: list[dict[str, Any]] = []
    page_token: str | None = None
    while True:
        params: dict[str, Any] = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token
        resp = requests.get(
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records",
            headers=headers,
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"list records failed: {data}")
        obj = data.get("data") or {}
        for item in obj.get("items") or []:
            fields = item.get("fields") or {}
            ad_key = flatten_field_value(fields.get("广告ID"))
            if not ad_key:
                continue
            rows.append(
                {
                    "record_id": item.get("record_id") or "",
                    "date": date_to_ymd(fields.get("抓取日期")),
                    "actual_hp": flatten_field_value(fields.get(reviewer_field)),
                    "ad_key": ad_key,
                    "product": flatten_field_value(fields.get("产品")),
                    "core": flatten_field_value(fields.get("核心卖点")),
                    "material_tags": flatten_field_value(fields.get("素材标签")),
                    "play": flatten_field_value(fields.get("玩法")),
                    "play_fingerprint": flatten_field_value(fields.get("玩法指纹")),
                    "title": flatten_field_value(fields.get("标题")),
                    "video_url": flatten_field_value(fields.get("视频链接")),
                    "cover_url": flatten_field_value(fields.get("封面图链接")),
                }
            )
        if not obj.get("has_more"):
            break
        page_token = obj.get("page_token")
    return rows


def default_db_path() -> Path:
    local = PROJECT_ROOT / "data" / "video_enhancer_pipeline.db"
    if local.exists():
        return local
    return PROJECT_ROOT / "data" / "remote_snapshots" / "ve" / "data" / "video_enhancer_pipeline.db"


def load_local_fallbacks(db_path: Path) -> dict[str, dict[str, str]]:
    if not db_path.exists():
        return {}
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(daily_creative_insights)")
        }

        def col(name: str, alias: str | None = None) -> str:
            if name in columns:
                return f"d.{name}" if alias is None else f"d.{name} AS {alias}"
            return f"'' AS {alias or name}"

        out: dict[str, dict[str, str]] = {}
        for row in conn.execute(
            f"""
            SELECT d.ad_key,
                   {col('product')},
                   {col('effect_one_liner')},
                   {col('video_url')},
                   {col('preview_img_url')},
                   {col('material_tags')},
                   {col('play_asset_name')},
                   {col('play_asset_subtag_names')},
                   {col('play_fingerprint')},
                   json_extract(d.raw_json, '$.title') AS title
            FROM daily_creative_insights d
            """
        ):
            out[str(row["ad_key"])] = {
                "product": str(row["product"] or ""),
                "core": str(row["effect_one_liner"] or ""),
                "video_url": str(row["video_url"] or ""),
                "cover_url": str(row["preview_img_url"] or ""),
                "title": str(row["title"] or ""),
                "material_tags": str(row["material_tags"] or ""),
                "play_asset_name": str(row["play_asset_name"] or ""),
                "play_asset_subtag_names": str(row["play_asset_subtag_names"] or ""),
                "play_fingerprint": str(row["play_fingerprint"] or ""),
            }
        return out
    finally:
        conn.close()


def apply_local_fallbacks(rows: list[dict[str, Any]], local: dict[str, dict[str, str]]) -> None:
    for row in rows:
        fallback = local.get(str(row.get("ad_key") or "")) or {}
        for key in (
            "product",
            "core",
            "video_url",
            "cover_url",
            "title",
            "material_tags",
            "play_asset_name",
            "play_asset_subtag_names",
            "play_fingerprint",
        ):
            if not str(row.get(key) or "").strip() and fallback.get(key):
                row[key] = fallback[key]
        row["play_label"] = extract_play_label(row)


def build_dedupe_prompt(
    *,
    product: str,
    target_date: str,
    history: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> str:
    return f"""你是“新玩法/重复玩法”判断助手，不是素材价值评审。只允许从“核心卖点 core”提炼关键词。输入已经按同产品分组；你需要把当前素材与近 7 天同产品中有明确接受情况的历史素材比较，判断当前素材是否与历史几乎同款。

核心原则：
- 历史中任意有效反馈（采纳、入素材库、不采纳、重复抓取）都表示该核心玩法已经被处理过。
- 历史采纳/入素材库不是放行理由，而是“已收录/已看过”的证据。
- 当前素材如果与历史已处理素材的核心卖点几乎同款，就应该视为重复玩法。
- 当前素材如果只是同主题延展，或有新增主题/机制/使用场景，不要硬拦，降为 maybe_block 或 allow。

你要自己判断，不要使用固定数值阈值。判断时先提炼 1-3 个具体关键词，再比较“具体玩法是否几乎同款”。

不要把这些泛词单独当作重复依据：照片、自拍、AI生成、动态视频、写真、手绘、跳舞、机甲、生日、风格、特效、视频、模板、热门、趋势。

决策：
- block：与历史已处理素材的具体核心玩法几乎同款，只是换措辞、换主体、换轻微场景；当前不算新玩法，不建议同步。
- maybe_block：与历史已处理素材明显相关，但属于同主题延展，存在新增主题/机制/场景；仍可同步但打标观察。
- allow：核心玩法与历史不同，或只是泛词重合，算新玩法，可以同步。

注意：如果历史相似素材状态是“采纳/入素材库”，当前几乎同款也要按重复处理，不要因此放行。但如果只是同一大主题的新变体，不要直接 block。

产品：{product}
测试日期：{target_date}
历史已处理素材：{json.dumps(history, ensure_ascii=False)}
当前候选：{json.dumps(candidates, ensure_ascii=False)}

只输出 JSON 数组，每个候选一个对象：{{"idx":0,"ad_key":"...","current_keywords":["..."],"decision":"allow|maybe_block|block","is_new_play":true,"matched_history_ad_keys":["..."],"matched_history_statuses":["..."],"matched_keywords":["..."],"reason":"..."}}"""


def build_missing_label_prompt(
    *,
    product: str,
    rows: list[dict[str, Any]],
) -> str:
    return f"""你是素材玩法标签标注助手。请只根据核心卖点 core 给每条素材打一个简短、稳定的玩法标签。

要求：
- 只根据核心卖点 core，不看封面、视频、热度、接受情况。
- 标签用 2-8 个中文字符，尽量像“机甲变身”“手绘拼贴”“赛场转播”“星座画像”“微缩小人”“AI分身”。
- 同一底层玩法要使用同一个标签，不要因为轻微场景变化频繁造新词。
- 不要输出泛词：照片、自拍、AI生成、动态视频、写真、风格、特效、模板。

产品：{product}
待标注素材：{json.dumps(rows, ensure_ascii=False)}

只输出 JSON 数组：{{"idx":0,"ad_key":"...","play_label":"..."}}"""


def parse_json_payload(text: str) -> Any:
    stripped = (text or "").strip()
    fence = re.search(r"```(?:json)?\s*(.*?)```", stripped, re.S)
    if fence:
        stripped = fence.group(1).strip()
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        start_obj = stripped.find("{")
        start_arr = stripped.find("[")
        starts = [x for x in (start_obj, start_arr) if x >= 0]
        if not starts:
            raise
        start = min(starts)
        end = max(stripped.rfind("}"), stripped.rfind("]"))
        if end <= start:
            raise
        return json.loads(stripped[start : end + 1])


def build_json_repair_prompt(raw_text: str) -> str:
    return f"""请把下面模型输出修复为合法 JSON。

要求：
- 只输出修复后的 JSON。
- 不要增加解释、Markdown 或代码块。
- 保留原有字段和值，必要时只修正引号、逗号、括号等 JSON 语法问题。

待修复内容：
{raw_text}"""


def call_json(prompt: str, *, model: str) -> Any:
    from llm_client import call_text

    content = call_text(
        "你只输出合法 JSON，不输出解释。",
        prompt,
        models=[model],
    )
    try:
        return parse_json_payload(content)
    except json.JSONDecodeError:
        repaired = call_text(
            "你是 JSON 修复器，只输出合法 JSON。",
            build_json_repair_prompt(content),
            models=[model],
        )
        return parse_json_payload(repaired)


def history_window(target_date: str, lookback_days: int) -> tuple[str, str]:
    target = dt.date.fromisoformat(target_date)
    return (
        (target - dt.timedelta(days=lookback_days)).isoformat(),
        (target - dt.timedelta(days=1)).isoformat(),
    )


def summarize_decision_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    decisive = [r for r in rows if str(r.get("actual_hp") or "") in DECISIVE_STATUSES]
    decision_counts = dict(Counter(str(r.get("decision") or "allow") for r in decisive))
    status_counts = dict(Counter(str(r.get("actual_hp") or "") for r in decisive))

    def block_metrics(block_values: set[str]) -> dict[str, Any]:
        tp = fp = tn = fn = 0
        for row in decisive:
            actual_negative = str(row.get("actual_hp") or "") in NEGATIVE_STATUSES
            predicted_block = str(row.get("decision") or "allow") in block_values
            if predicted_block and actual_negative:
                tp += 1
            elif predicted_block and not actual_negative:
                fp += 1
            elif not predicted_block and actual_negative:
                fn += 1
            else:
                tn += 1
        blocked = tp + fp
        return {
            "TP": tp,
            "FP": fp,
            "FN": fn,
            "TN": tn,
            "precision": round(tp / blocked, 3) if blocked else None,
            "recall": round(tp / (tp + fn), 3) if tp + fn else 0,
            "false_block_rate_good": round(fp / (fp + tn), 3) if fp + tn else 0,
            "sync_rate": round((len(decisive) - blocked) / len(decisive), 3) if decisive else 0,
        }

    return {
        "decisive_count": len(decisive),
        "status_counts": status_counts,
        "decision_counts": decision_counts,
        "block_only": block_metrics({"block"}),
        "block_or_maybe": block_metrics({"block", "maybe_block"}),
    }


def normalize_model_decisions(
    *,
    product: str,
    candidates: list[dict[str, Any]],
    raw_decisions: Any,
) -> list[dict[str, Any]]:
    if not isinstance(raw_decisions, list):
        raw_decisions = []
    by_idx: dict[int, dict[str, Any]] = {}
    by_ad_key: dict[str, dict[str, Any]] = {}
    for item in raw_decisions:
        if not isinstance(item, dict):
            continue
        try:
            by_idx[int(item.get("idx"))] = item
        except (TypeError, ValueError):
            pass
        ad_key = str(item.get("ad_key") or "")
        if ad_key:
            by_ad_key[ad_key] = item

    out: list[dict[str, Any]] = []
    for idx, cand in enumerate(candidates):
        item = by_idx.get(idx) or by_ad_key.get(str(cand.get("ad_key") or "")) or {}
        decision = str(item.get("decision") or "allow")
        if decision not in {"allow", "maybe_block", "block"}:
            decision = "allow"
        out.append(
            {
                **cand,
                "product": product,
                "decision": decision,
                "is_new_play": item.get("is_new_play", decision == "allow"),
                "keywords": item.get("current_keywords") or [],
                "matched_history_ad_keys": item.get("matched_history_ad_keys") or [],
                "matched_history_statuses": item.get("matched_history_statuses") or [],
                "matched_keywords": item.get("matched_keywords") or [],
                "reason": str(item.get("reason") or "模型未返回该素材，默认 allow"),
            }
        )
    return out


def normalize_label_decisions(
    *,
    rows: list[dict[str, Any]],
    raw_labels: Any,
) -> None:
    if not isinstance(raw_labels, list):
        raw_labels = []
    by_idx: dict[int, dict[str, Any]] = {}
    by_ad_key: dict[str, dict[str, Any]] = {}
    for item in raw_labels:
        if not isinstance(item, dict):
            continue
        try:
            by_idx[int(item.get("idx"))] = item
        except (TypeError, ValueError):
            pass
        ad_key = str(item.get("ad_key") or "")
        if ad_key:
            by_ad_key[ad_key] = item

    for idx, row in enumerate(rows):
        item = by_idx.get(idx) or by_ad_key.get(str(row.get("ad_key") or "")) or {}
        label = str(item.get("play_label") or "").strip()
        if label:
            row["play_label"] = label
            row["play_label_source"] = "ai_core"


def ensure_play_labels(
    *,
    rows: list[dict[str, Any]],
    model: str,
    workers: int,
    batch_size: int | None = None,
) -> None:
    if batch_size is None:
        batch_size = default_label_batch_size()
    missing_by_product: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        label = extract_play_label(row)
        if label:
            row["play_label"] = label
            row.setdefault("play_label_source", "existing")
        elif str(row.get("core") or "").strip():
            missing_by_product[str(row.get("product") or "")].append(row)

    def label_batch(product: str, batch_no: int, product_rows: list[dict[str, Any]]) -> tuple[str, int]:
        payload = [
            {"idx": idx, "ad_key": row.get("ad_key", ""), "core": row.get("core", "")}
            for idx, row in enumerate(product_rows)
        ]
        print(
            f"[core-play] label {product} batch={batch_no}: missing={len(product_rows)} calling {model}",
            flush=True,
        )
        raw = call_json(build_missing_label_prompt(product=product, rows=payload), model=model)
        normalize_label_decisions(rows=product_rows, raw_labels=raw)
        print(
            f"[core-play] label {product} batch={batch_no}: returned={len(product_rows)}",
            flush=True,
        )
        return product, batch_no

    groups: list[tuple[str, int, list[dict[str, Any]]]] = []
    for product, product_rows in sorted(missing_by_product.items()):
        for offset in range(0, len(product_rows), batch_size):
            groups.append((product, offset // batch_size + 1, product_rows[offset : offset + batch_size]))
    if not groups:
        return
    if workers <= 1:
        for product, batch_no, product_rows in groups:
            label_batch(product, batch_no, product_rows)
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(label_batch, product, batch_no, product_rows)
                for product, batch_no, product_rows in groups
            ]
            for future in as_completed(futures):
                future.result()

    for row in rows:
        if not str(row.get("play_label") or "").strip():
            row["play_label"] = "未归类"
            row["play_label_source"] = "fallback"


def run_dedupe(
    *,
    rows: list[dict[str, Any]],
    target_date: str,
    lookback_days: int,
    model: str,
    workers: int = 1,
) -> list[dict[str, Any]]:
    start, end = history_window(target_date, lookback_days)
    history = [
        r
        for r in rows
        if start <= str(r.get("date") or "") <= end
        and str(r.get("actual_hp") or "") in DECISIVE_STATUSES
        and str(r.get("core") or "").strip()
    ]
    candidates = [
        r
        for r in rows
        if str(r.get("date") or "") == target_date and str(r.get("core") or "").strip()
    ]
    ensure_play_labels(rows=history + candidates, model=model, workers=workers)

    grouped: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: {"history": [], "candidates": []})
    for row in history:
        key = str(row.get("product") or "")
        grouped[key]["history"].append(row)
    for row in candidates:
        key = str(row.get("product") or "")
        grouped[key]["candidates"].append(row)

    def run_product(product: str) -> list[dict[str, Any]]:
        product_history = grouped[product]["history"]
        product_candidates = grouped[product]["candidates"]
        if not product_candidates:
            return []
        if not product_history:
            print(
                f"[core-play] {product}: history=0 candidates={len(product_candidates)} skip LLM",
                flush=True,
            )
            return [
                {
                    **cand,
                    "decision": "allow",
                    "is_new_play": True,
                    "keywords": [],
                    "matched_history_ad_keys": [],
                    "matched_history_statuses": [],
                    "matched_keywords": [],
                    "reason": "近7天同产品无有效反馈历史",
                }
                for cand in product_candidates
            ]

        history_payload = [
            {
                "hidx": idx,
                "ad_key": h.get("ad_key", ""),
                "date": h.get("date", ""),
                "hp": h.get("actual_hp", ""),
                "core": h.get("core", ""),
            }
            for idx, h in enumerate(product_history)
        ]
        candidate_payload = [
            {
                "idx": idx,
                "ad_key": c.get("ad_key", ""),
                "core": c.get("core", ""),
            }
            for idx, c in enumerate(product_candidates)
        ]
        print(
            f"[core-play] {product}: history={len(product_history)} candidates={len(product_candidates)} calling {model}",
            flush=True,
        )
        raw = call_json(
            build_dedupe_prompt(
                product=product,
                target_date=target_date,
                history=history_payload,
                candidates=candidate_payload,
            ),
            model=model,
        )
        product_results = normalize_model_decisions(
            product=product,
            candidates=product_candidates,
            raw_decisions=raw,
        )
        print(f"[core-play] {product}: returned={len(product_results)}", flush=True)
        return product_results

    product_order = sorted(grouped)
    if workers <= 1:
        results: list[dict[str, Any]] = []
        for product in product_order:
            results.extend(run_product(product))
        return results

    print(f"[core-play] product LLM workers={workers}", flush=True)
    by_product: dict[str, list[dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(run_product, product): product for product in product_order}
        for future in as_completed(futures):
            product = futures[future]
            try:
                by_product[product] = future.result()
            except Exception as exc:
                raise RuntimeError(f"product group failed: {product}") from exc
    results = []
    for product in product_order:
        results.extend(by_product.get(product, []))
    return results


def md_escape(value: Any) -> str:
    return str(value or "").replace("\n", "<br>").replace("|", "\\|")


def render_markdown(report: dict[str, Any]) -> str:
    metrics = report.get("metrics") or {}
    block_only = metrics.get("block_only") or {}
    rows = report.get("results") or []
    lines = [
        f"# VE 核心卖点去重 Shadow 报告 - {report.get('target_date')}",
        "",
        f"- 模型：`{report.get('model')}`",
        f"- 口径：先看同产品近 {report.get('lookback_days')} 天有明确接受情况的历史 `核心卖点`；历史任意浩鹏有效反馈都视为已处理玩法；只有几乎同款才 `block`。`玩法标签` 仅用于展示/补齐，不作为第一层硬分桶。本报告只输出去重评估，不生成玩法聚合结果。",
        f"- 有效反馈样本：{metrics.get('decisive_count', 0)}",
        f"- 决策分布：{json.dumps(metrics.get('decision_counts', {}), ensure_ascii=False)}",
        f"- 只拦 `block`：TP={block_only.get('TP', 0)} / FP={block_only.get('FP', 0)} / FN={block_only.get('FN', 0)} / TN={block_only.get('TN', 0)}，同步率={block_only.get('sync_rate', 0)}，误杀率={block_only.get('false_block_rate_good', 0)}",
        "",
    ]
    lines.extend(["## Block / Maybe 明细", ""])
    non_allow = [r for r in rows if str(r.get("decision") or "") != "allow"]
    if non_allow:
        lines.append("| 决策 | 浩鹏实际 | 产品 | 玩法标签 | 核心卖点 | 命中历史 | 理由 |")
        lines.append("|---|---|---|---|---|---|---|")
        for row in non_allow:
            lines.append(
                "| "
                + " | ".join(
                    [
                        md_escape(row.get("decision")),
                        md_escape(row.get("actual_hp")),
                        md_escape(row.get("product")),
                        md_escape(row.get("play_label")),
                        md_escape(row.get("core")),
                        md_escape("、".join(row.get("matched_history_ad_keys") or [])),
                        md_escape(row.get("reason")),
                    ]
                )
                + " |"
            )
    else:
        lines.append("无 `block` / `maybe_block`。")

    lines.extend(["", "## 全量素材明细", ""])
    lines.append("| 决策 | 浩鹏实际 | 产品 | 广告ID | 玩法标签 | 核心卖点 | 理由 |")
    lines.append("|---|---|---|---|---|---|---|")
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    md_escape(row.get("decision")),
                    md_escape(row.get("actual_hp")),
                    md_escape(row.get("product")),
                    md_escape(row.get("ad_key")),
                    md_escape(row.get("play_label")),
                    md_escape(row.get("core")),
                    md_escape(row.get("reason")),
                ]
            )
            + " |"
        )
    lines.append("")
    return "\n".join(lines)


def write_report(report: dict[str, Any], output_dir: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    date = str(report.get("target_date") or "unknown")
    json_path = output_dir / f"ve_core_play_shadow_report_{date}.json"
    md_path = output_dir / f"ve_core_play_shadow_report_{date}.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    return json_path, md_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="VE 核心卖点新玩法/重复玩法 shadow 报告")
    parser.add_argument("--date", default=today_shanghai(), help="目标抓取日期 YYYY-MM-DD")
    parser.add_argument("--lookback-days", type=int, default=7, help="历史反馈回看天数")
    parser.add_argument("--reviewer-field", default=DEFAULT_REVIEWER_FIELD, help="浩鹏接受情况字段名")
    parser.add_argument(
        "--model",
        default=(os.getenv("VE_CORE_PLAY_DEDUPE_MODEL") or DEFAULT_MODEL).strip(),
        help="OpenRouter 模型，默认 qwen/qwen3.6-plus",
    )
    parser.add_argument(
        "--bitable-url",
        default=(os.getenv("VIDEO_ENHANCER_BITABLE_URL") or "").strip(),
        help="VE 主多维表 URL，默认 VIDEO_ENHANCER_BITABLE_URL",
    )
    parser.add_argument("--db-path", default=str(default_db_path()), help="本地 VE SQLite，用于补空字段")
    parser.add_argument("--output-dir", default=str(PROJECT_ROOT / "data"), help="报告输出目录")
    parser.add_argument("--workers", type=int, default=default_workers(), help="按产品并行调用 LLM 的线程数，默认 4")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.bitable_url:
        raise SystemExit("请配置 VIDEO_ENHANCER_BITABLE_URL 或传入 --bitable-url")
    rows = fetch_bitable_rows(bitable_url=args.bitable_url, reviewer_field=args.reviewer_field)
    apply_local_fallbacks(rows, load_local_fallbacks(Path(args.db_path)))

    start, end = history_window(args.date, args.lookback_days)
    history_count = sum(
        1
        for r in rows
        if start <= str(r.get("date") or "") <= end
        and str(r.get("actual_hp") or "") in DECISIVE_STATUSES
        and str(r.get("core") or "").strip()
    )
    target_count = sum(1 for r in rows if str(r.get("date") or "") == args.date and str(r.get("core") or "").strip())
    print(
        f"[core-play] date={args.date} history_window={start}..{end} "
        f"history_effective={history_count} target_candidates={target_count}"
        ,
        flush=True,
    )

    results = run_dedupe(
        rows=rows,
        target_date=args.date,
        lookback_days=args.lookback_days,
        model=args.model,
        workers=max(1, int(args.workers)),
    )
    report = {
        "target_date": args.date,
        "lookback_days": args.lookback_days,
        "reviewer_field": args.reviewer_field,
        "model": args.model,
        "workers": max(1, int(args.workers)),
        "history_window": {"start": start, "end": end, "effective_count": history_count},
        "target_candidate_count": target_count,
        "metrics": summarize_decision_metrics(results),
        "results": results,
    }
    json_path, md_path = write_report(report, Path(args.output_dir))
    try:
        from llm_client import flush_usage

        flush_usage(args.date)
    except Exception as exc:
        print(f"[core-play] flush_usage skipped: {exc}", flush=True)
    print(f"[core-play] wrote {json_path}", flush=True)
    print(f"[core-play] wrote {md_path}", flush=True)


if __name__ == "__main__":
    main()
