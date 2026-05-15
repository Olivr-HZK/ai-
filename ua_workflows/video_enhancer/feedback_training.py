"""Pull VE Bitable feedback and build a separate preference-training dataset.

This module is intentionally outside the normal Video Enhancer production
pipeline. It reads the review Bitable directly, records human feedback, exports
material-only training samples, and trains a small dependency-free baseline
model for quick calibration.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse

import requests

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - dotenv is available in prod, optional in tests.
    load_dotenv = None  # type: ignore[assignment]

from ua_workflows.shared.config import DATA_DIR, REPORTS_DIR


DEFAULT_BITABLE_URL = (
    "https://scnmrtumk0zm.feishu.cn/base/CivwbJ2HkazcKTsKnbGclA5RnWc"
    "?table=tblrZZvVuFcjL0kE&view=vewJtPixtM"
)

DB_PATH = DATA_DIR / "ve_feedback_training.db"
MODEL_DIR = DATA_DIR / "models"

LABEL_FIELD = "接受情况"
LABEL_ACCEPTED = 1
LABEL_REJECTED = 0
LABEL_VALUE_MAP = {
    "接受": LABEL_ACCEPTED,
    "采纳": LABEL_ACCEPTED,
    "入素材库": LABEL_ACCEPTED,
    "accept": LABEL_ACCEPTED,
    "accepted": LABEL_ACCEPTED,
    "yes": LABEL_ACCEPTED,
    "删除": LABEL_REJECTED,
    "拒绝": LABEL_REJECTED,
    "不采纳": LABEL_REJECTED,
    "reject": LABEL_REJECTED,
    "rejected": LABEL_REJECTED,
    "no": LABEL_REJECTED,
}

# These are the only fields used as training features. Operational metadata such
# as product, advertiser, dates, exposure, heat, geo, and source rows are stored
# for auditing but are intentionally excluded from `feature_text`.
MATERIAL_FEATURE_FIELDS: dict[str, str] = {
    "title": "标题",
    "body_zh": "正文（中文）",
    "video_url": "视频链接",
    "cover_url": "封面图链接",
    "core_selling_point": "核心卖点",
    "hook": "Hook解析",
    "script_or_voiceover": "脚本/口播",
    "play_asset": "玩法资产",
    "play_variant": "玩法变种",
    "play_asset_id": "玩法资产ID",
    "play_variant_id": "玩法变种ID",
    "play_judgement_reason": "玩法判断理由",
    "play_fingerprint": "玩法指纹",
    "differentiator": "差异点",
    "risk_level": "风险等级",
    "ai_analysis": "AI分析结果",
    "material_tags": "素材标签",
}

MEDIA_METADATA_FIELDS: dict[str, str] = {
    "cover_attachment": "封面图",
    "video_attachment": "视频附件",
}

AUDIT_FIELDS: dict[str, str] = {
    "ad_key": "广告ID",
    "product": "产品",
    "advertiser": "广告主",
    "category": "类目",
    "platform": "平台",
    "crawl_date": "抓取日期",
    "created_time": "创建时间",
    "updated_time": "更新时间",
    "video_duration": "视频时长",
    "play_newness": "玩法新旧",
    "play_judgement_source": "玩法判断来源",
    "own_product": "我方产品",
}

TEXT_FEATURE_KEYS = [
    "title",
    "body_zh",
    "core_selling_point",
    "hook",
    "script_or_voiceover",
    "play_asset",
    "play_variant",
    "play_judgement_reason",
    "play_fingerprint",
    "differentiator",
    "risk_level",
    "ai_analysis",
    "material_tags",
]

COMPLETENESS_PROFILES: dict[str, tuple[str, ...]] = {
    "any": tuple(),
    "all": tuple(MATERIAL_FEATURE_FIELDS.keys()),
    "core": (
        "title",
        "video_url",
        "cover_url",
        "core_selling_point",
        "hook",
        "script_or_voiceover",
        "risk_level",
        "ai_analysis",
    ),
    "core_play": (
        "title",
        "video_url",
        "cover_url",
        "core_selling_point",
        "hook",
        "script_or_voiceover",
        "play_asset",
        "play_variant",
        "play_asset_id",
        "play_variant_id",
        "play_fingerprint",
        "differentiator",
        "risk_level",
        "ai_analysis",
    ),
    "play": (
        "play_asset",
        "play_variant",
        "play_asset_id",
        "play_variant_id",
        "play_fingerprint",
        "differentiator",
    ),
}


@dataclass(frozen=True)
class BitableRef:
    app_token: str
    table_id: str
    view_id: str


@dataclass
class FeedbackSample:
    record_id: str
    ad_key: str
    accept_status: str
    label: int | None
    feature: dict[str, Any]
    media: dict[str, Any]
    audit: dict[str, Any]
    feature_text: str
    raw_fields: dict[str, Any]


def _now_local() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _run_date(value: str | None = None) -> str:
    return value or datetime.now().strftime("%Y-%m-%d")


def parse_bitable_ref(url: str) -> BitableRef:
    parsed = urlparse(url.strip())
    parts = [p for p in parsed.path.split("/") if p]
    app_token = parts[1] if len(parts) >= 2 and parts[0] == "base" else ""
    qs = parse_qs(parsed.query or "")
    table_id = (qs.get("table") or [""])[0]
    view_id = (qs.get("view") or [""])[0]
    if not app_token or not table_id:
        raise RuntimeError(f"无法从链接解析 app_token/table_id: {url}")
    return BitableRef(app_token=app_token, table_id=table_id, view_id=view_id)


def _load_env() -> None:
    if load_dotenv is not None:
        load_dotenv()


def _tenant_access_token() -> str:
    app_id = os.getenv("FEISHU_APP_ID", "")
    app_secret = os.getenv("FEISHU_APP_SECRET", "")
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


def fetch_bitable_records(
    bitable_url: str,
    *,
    access_token: str | None = None,
    page_size: int = 500,
) -> list[dict[str, Any]]:
    ref = parse_bitable_ref(bitable_url)
    token = access_token or _tenant_access_token()
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{ref.app_token}/tables/{ref.table_id}/records"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}
    page_token = ""
    out: list[dict[str, Any]] = []
    while True:
        params: dict[str, Any] = {"page_size": page_size}
        if page_token:
            params["page_token"] = page_token
        if ref.view_id:
            params["view_id"] = ref.view_id
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"list bitable records failed: {data}")
        payload = data.get("data") or {}
        items = payload.get("items") or []
        out.extend(items)
        if not payload.get("has_more"):
            break
        page_token = str(payload.get("page_token") or "")
        if not page_token:
            break
    return out


def _primitive_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:g}"
    return ""


def cell_to_text(value: Any) -> str:
    primitive = _primitive_to_text(value)
    if primitive:
        return primitive
    if isinstance(value, list):
        parts = [cell_to_text(v) for v in value]
        return " ".join(p for p in parts if p).strip()
    if isinstance(value, dict):
        for key in ("text", "name", "value", "link", "url", "file_token"):
            text = _primitive_to_text(value.get(key))
            if text:
                return text
        if "segments" in value:
            return cell_to_text(value.get("segments"))
        if "elements" in value:
            return cell_to_text(value.get("elements"))
        if "attrs" in value:
            return cell_to_text(value.get("attrs"))
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return ""


def _cell_to_jsonable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [_cell_to_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _cell_to_jsonable(v) for k, v in value.items()}
    return str(value)


def normalize_accept_status(value: Any) -> str:
    text = cell_to_text(value).strip()
    if not text:
        return ""
    return re.sub(r"\s+", "", text)


def label_from_accept_status(status: str) -> int | None:
    normalized = (status or "").strip().lower()
    return LABEL_VALUE_MAP.get(normalized)


def build_feature_text(feature: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in TEXT_FEATURE_KEYS:
        value = cell_to_text(feature.get(key))
        if value:
            field_name = MATERIAL_FEATURE_FIELDS.get(key, key)
            parts.append(f"【{field_name}】{value}")
    return "\n".join(parts).strip()


def completeness_required_field_names(profile: str) -> list[str]:
    keys = COMPLETENESS_PROFILES.get(profile)
    if keys is None:
        raise ValueError(f"未知 complete profile: {profile}")
    return [MATERIAL_FEATURE_FIELDS[k] for k in keys]


def sample_matches_completeness(sample: FeedbackSample, profile: str) -> bool:
    keys = COMPLETENESS_PROFILES.get(profile)
    if keys is None:
        raise ValueError(f"未知 complete profile: {profile}")
    return all(cell_to_text(sample.feature.get(k)).strip() for k in keys)


def filter_samples_by_completeness(samples: list[FeedbackSample], profile: str) -> list[FeedbackSample]:
    if profile == "any":
        return samples
    return [s for s in samples if sample_matches_completeness(s, profile)]


def normalize_record(record: dict[str, Any]) -> FeedbackSample:
    fields = record.get("fields") or {}
    record_id = str(record.get("record_id") or record.get("id") or "")
    feature = {key: cell_to_text(fields.get(field_name)) for key, field_name in MATERIAL_FEATURE_FIELDS.items()}
    media = {key: _cell_to_jsonable(fields.get(field_name)) for key, field_name in MEDIA_METADATA_FIELDS.items()}
    audit = {key: cell_to_text(fields.get(field_name)) for key, field_name in AUDIT_FIELDS.items()}
    accept_status = normalize_accept_status(fields.get(LABEL_FIELD))
    label = label_from_accept_status(accept_status)
    ad_key = audit.get("ad_key") or cell_to_text(fields.get("广告ID")) or record_id
    return FeedbackSample(
        record_id=record_id,
        ad_key=ad_key,
        accept_status=accept_status,
        label=label,
        feature=feature,
        media=media,
        audit=audit,
        feature_text=build_feature_text(feature),
        raw_fields={str(k): _cell_to_jsonable(v) for k, v in fields.items()},
    )


def init_db(db_path: Path = DB_PATH) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ve_feedback_records (
                record_id TEXT PRIMARY KEY,
                ad_key TEXT,
                accept_status TEXT,
                label INTEGER,
                feature_text TEXT,
                feature_json TEXT NOT NULL,
                media_json TEXT NOT NULL,
                audit_json TEXT NOT NULL,
                raw_fields_json TEXT NOT NULL,
                source_app_token TEXT,
                source_table_id TEXT,
                source_view_id TEXT,
                first_pulled_at TEXT NOT NULL,
                last_pulled_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ve_feedback_training_runs (
                run_id TEXT PRIMARY KEY,
                run_date TEXT NOT NULL,
                pulled_count INTEGER NOT NULL,
                labeled_count INTEGER NOT NULL,
                accepted_count INTEGER NOT NULL,
                rejected_count INTEGER NOT NULL,
                pending_count INTEGER NOT NULL,
                dataset_path TEXT,
                model_path TEXT,
                report_path TEXT,
                metrics_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ve_feedback_label ON ve_feedback_records(label)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ve_feedback_ad_key ON ve_feedback_records(ad_key)")


def upsert_samples(
    samples: Iterable[FeedbackSample],
    *,
    source: BitableRef,
    db_path: Path = DB_PATH,
) -> dict[str, Any]:
    init_db(db_path)
    now = _now_local()
    status_counts: Counter[str] = Counter()
    stats: dict[str, Any] = {"total": 0, "accepted": 0, "rejected": 0, "pending": 0}
    with sqlite3.connect(db_path) as conn:
        for sample in samples:
            stats["total"] += 1
            status_counts[sample.accept_status or "<empty>"] += 1
            if sample.label == LABEL_ACCEPTED:
                stats["accepted"] += 1
            elif sample.label == LABEL_REJECTED:
                stats["rejected"] += 1
            else:
                stats["pending"] += 1
            conn.execute(
                """
                INSERT INTO ve_feedback_records (
                    record_id, ad_key, accept_status, label, feature_text,
                    feature_json, media_json, audit_json, raw_fields_json,
                    source_app_token, source_table_id, source_view_id,
                    first_pulled_at, last_pulled_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(record_id) DO UPDATE SET
                    ad_key=excluded.ad_key,
                    accept_status=excluded.accept_status,
                    label=excluded.label,
                    feature_text=excluded.feature_text,
                    feature_json=excluded.feature_json,
                    media_json=excluded.media_json,
                    audit_json=excluded.audit_json,
                    raw_fields_json=excluded.raw_fields_json,
                    source_app_token=excluded.source_app_token,
                    source_table_id=excluded.source_table_id,
                    source_view_id=excluded.source_view_id,
                    last_pulled_at=excluded.last_pulled_at
                """,
                (
                    sample.record_id,
                    sample.ad_key,
                    sample.accept_status,
                    sample.label,
                    sample.feature_text,
                    json.dumps(sample.feature, ensure_ascii=False, sort_keys=True),
                    json.dumps(sample.media, ensure_ascii=False, sort_keys=True),
                    json.dumps(sample.audit, ensure_ascii=False, sort_keys=True),
                    json.dumps(sample.raw_fields, ensure_ascii=False, sort_keys=True),
                    source.app_token,
                    source.table_id,
                    source.view_id,
                    now,
                    now,
                ),
            )
    stats["status_counts"] = dict(status_counts.most_common())
    return stats


def pull_feedback(
    bitable_url: str,
    *,
    db_path: Path = DB_PATH,
) -> tuple[list[FeedbackSample], dict[str, Any]]:
    ref = parse_bitable_ref(bitable_url)
    records = fetch_bitable_records(bitable_url)
    samples = [normalize_record(r) for r in records]
    stats = upsert_samples(samples, source=ref, db_path=db_path)
    return samples, stats


def load_labeled_samples(db_path: Path = DB_PATH) -> list[FeedbackSample]:
    init_db(db_path)
    out: list[FeedbackSample] = []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT record_id, ad_key, accept_status, label, feature_text,
                   feature_json, media_json, audit_json, raw_fields_json
            FROM ve_feedback_records
            WHERE label IN (0, 1)
            ORDER BY record_id
            """
        ).fetchall()
    for row in rows:
        out.append(
            FeedbackSample(
                record_id=str(row["record_id"]),
                ad_key=str(row["ad_key"] or ""),
                accept_status=str(row["accept_status"] or ""),
                label=int(row["label"]),
                feature=json.loads(row["feature_json"] or "{}"),
                media=json.loads(row["media_json"] or "{}"),
                audit=json.loads(row["audit_json"] or "{}"),
                feature_text=str(row["feature_text"] or ""),
                raw_fields=json.loads(row["raw_fields_json"] or "{}"),
            )
        )
    return out


def export_dataset(
    samples: list[FeedbackSample],
    *,
    run_date: str,
    output_path: Path | None = None,
) -> Path:
    out_path = output_path or (DATA_DIR / f"ve_feedback_training_dataset_{run_date}.jsonl")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        for sample in samples:
            if sample.label not in (LABEL_ACCEPTED, LABEL_REJECTED):
                continue
            row = {
                "record_id": sample.record_id,
                "ad_key": sample.ad_key,
                "label": sample.label,
                "label_name": "accepted" if sample.label == LABEL_ACCEPTED else "rejected",
                "accept_status": sample.accept_status,
                "feature": sample.feature,
                "feature_text": sample.feature_text,
            }
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    return out_path


def output_path_with_profile(base_dir: Path, stem: str, run_date: str, suffix: str, profile: str) -> Path:
    profile_suffix = "" if profile == "any" else f"_{profile}"
    return base_dir / f"{stem}_{run_date}{profile_suffix}.{suffix}"


ASCII_WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9_+\-.]{1,}|[0-9]+(?:\.[0-9]+)?")


def tokenize_feature_text(text: str) -> list[str]:
    words = [m.group(0).lower() for m in ASCII_WORD_RE.finditer(text or "")]
    chinese_chars = [ch for ch in text if "\u4e00" <= ch <= "\u9fff"]
    chinese_bigrams = [chinese_chars[i] + chinese_chars[i + 1] for i in range(len(chinese_chars) - 1)]
    return words + chinese_bigrams


def _stable_bucket(value: str, modulo: int = 5) -> int:
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % modulo


def split_train_validation(samples: list[FeedbackSample]) -> tuple[list[FeedbackSample], list[FeedbackSample]]:
    labels = Counter(s.label for s in samples)
    if len(samples) < 10 or min(labels.get(LABEL_ACCEPTED, 0), labels.get(LABEL_REJECTED, 0)) < 3:
        return samples, []
    validation = [s for s in samples if _stable_bucket(s.record_id or s.ad_key) == 0]
    train = [s for s in samples if s not in validation]
    train_labels = Counter(s.label for s in train)
    validation_labels = Counter(s.label for s in validation)
    if (
        not validation
        or min(train_labels.get(LABEL_ACCEPTED, 0), train_labels.get(LABEL_REJECTED, 0)) < 1
        or min(validation_labels.get(LABEL_ACCEPTED, 0), validation_labels.get(LABEL_REJECTED, 0)) < 1
    ):
        return samples, []
    return train, validation


def _fit_nb(samples: list[FeedbackSample], *, alpha: float = 1.0) -> dict[str, Any]:
    label_doc_counts: Counter[int] = Counter()
    token_counts: dict[int, Counter[str]] = {LABEL_REJECTED: Counter(), LABEL_ACCEPTED: Counter()}
    vocabulary: set[str] = set()
    for sample in samples:
        if sample.label not in (LABEL_REJECTED, LABEL_ACCEPTED):
            continue
        label = int(sample.label)
        label_doc_counts[label] += 1
        tokens = tokenize_feature_text(sample.feature_text)
        token_counts[label].update(tokens)
        vocabulary.update(tokens)
    total_docs = sum(label_doc_counts.values())
    label_log_prior: dict[str, float] = {}
    for label in (LABEL_REJECTED, LABEL_ACCEPTED):
        label_log_prior[str(label)] = math.log((label_doc_counts[label] + alpha) / (total_docs + 2 * alpha))
    vocab = sorted(vocabulary)
    vocab_size = len(vocab)
    label_token_totals = {label: sum(token_counts[label].values()) for label in (LABEL_REJECTED, LABEL_ACCEPTED)}
    token_log_prob: dict[str, dict[str, float]] = {"0": {}, "1": {}}
    for label in (LABEL_REJECTED, LABEL_ACCEPTED):
        denom = label_token_totals[label] + alpha * max(vocab_size, 1)
        for token in vocab:
            token_log_prob[str(label)][token] = math.log((token_counts[label][token] + alpha) / denom)
    return {
        "model_type": "multinomial_nb_text_baseline",
        "created_at": _now_local(),
        "label_names": {"0": "rejected", "1": "accepted"},
        "feature_fields": MATERIAL_FEATURE_FIELDS,
        "text_feature_keys": TEXT_FEATURE_KEYS,
        "alpha": alpha,
        "doc_counts": {str(k): int(v) for k, v in label_doc_counts.items()},
        "token_totals": {str(k): int(v) for k, v in label_token_totals.items()},
        "vocabulary_size": vocab_size,
        "label_log_prior": label_log_prior,
        "token_log_prob": token_log_prob,
    }


def predict_accept_probability(model: dict[str, Any], feature_text: str) -> float:
    token_log_prob = model.get("token_log_prob") or {}
    label_log_prior = model.get("label_log_prior") or {}
    scores: dict[int, float] = {}
    for label in (LABEL_REJECTED, LABEL_ACCEPTED):
        key = str(label)
        score = float(label_log_prior.get(key, math.log(0.5)))
        probs = token_log_prob.get(key) or {}
        for token in tokenize_feature_text(feature_text):
            if token in probs:
                score += float(probs[token])
        scores[label] = score
    max_score = max(scores.values())
    exp0 = math.exp(scores[LABEL_REJECTED] - max_score)
    exp1 = math.exp(scores[LABEL_ACCEPTED] - max_score)
    return exp1 / (exp0 + exp1)


def evaluate_model(model: dict[str, Any], samples: list[FeedbackSample]) -> dict[str, Any]:
    if not samples:
        return {"validation_count": 0}
    tp = fp = tn = fn = 0
    rows: list[dict[str, Any]] = []
    for sample in samples:
        probability = predict_accept_probability(model, sample.feature_text)
        pred = LABEL_ACCEPTED if probability >= 0.5 else LABEL_REJECTED
        label = int(sample.label or 0)
        if pred == LABEL_ACCEPTED and label == LABEL_ACCEPTED:
            tp += 1
        elif pred == LABEL_ACCEPTED and label == LABEL_REJECTED:
            fp += 1
        elif pred == LABEL_REJECTED and label == LABEL_REJECTED:
            tn += 1
        else:
            fn += 1
        rows.append(
            {
                "record_id": sample.record_id,
                "ad_key": sample.ad_key,
                "label": label,
                "pred": pred,
                "accept_probability": round(probability, 4),
            }
        )
    total = tp + fp + tn + fn
    precision = tp / (tp + fp) if tp + fp else None
    recall = tp / (tp + fn) if tp + fn else None
    return {
        "validation_count": total,
        "accuracy": (tp + tn) / total if total else None,
        "precision_accept": precision,
        "recall_accept": recall,
        "confusion": {"tp": tp, "fp": fp, "tn": tn, "fn": fn},
        "examples": rows[:20],
    }


def train_baseline_model(
    samples: list[FeedbackSample],
    *,
    run_date: str,
    model_path: Path | None = None,
) -> tuple[Path | None, dict[str, Any]]:
    labeled = [s for s in samples if s.label in (LABEL_ACCEPTED, LABEL_REJECTED) and s.feature_text]
    label_counts = Counter(s.label for s in labeled)
    metrics: dict[str, Any] = {
        "labeled_count": len(labeled),
        "accepted_count": int(label_counts.get(LABEL_ACCEPTED, 0)),
        "rejected_count": int(label_counts.get(LABEL_REJECTED, 0)),
    }
    if len(labeled) < 2 or not label_counts.get(LABEL_ACCEPTED) or not label_counts.get(LABEL_REJECTED):
        metrics["status"] = "insufficient_labeled_data"
        return None, metrics
    train_samples, validation_samples = split_train_validation(labeled)
    model = _fit_nb(train_samples)
    model["run_date"] = run_date
    model["train_count"] = len(train_samples)
    model["validation_count"] = len(validation_samples)
    validation_metrics = evaluate_model(model, validation_samples)
    metrics["status"] = "trained"
    metrics["train_count"] = len(train_samples)
    metrics.update(validation_metrics)
    out_path = model_path or (MODEL_DIR / f"ve_feedback_preference_nb_{run_date}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(model, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return out_path, metrics


def _count_pending(samples: list[FeedbackSample]) -> int:
    return sum(1 for s in samples if s.label not in (LABEL_ACCEPTED, LABEL_REJECTED))


def write_report(
    *,
    run_date: str,
    pulled_count: int,
    labeled_samples: list[FeedbackSample],
    dataset_path: Path,
    model_path: Path | None,
    metrics: dict[str, Any],
    status_counts: dict[str, int] | None = None,
    complete_profile: str = "any",
    output_path: Path | None = None,
) -> Path:
    out_path = output_path or (REPORTS_DIR / f"ve_feedback_training_{run_date}.md")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    label_counts = Counter(s.label for s in labeled_samples)
    lines = [
        f"# VE 反馈训练日报 {run_date}",
        "",
        "该报告来自独立反馈训练链路，不属于 VE 正常抓取/分析/推送主流程。",
        "",
        "## 样本",
        "",
        f"- 多维表拉取记录：{pulled_count}",
        f"- 可训练样本：{len(labeled_samples)}",
        f"- 接受：{label_counts.get(LABEL_ACCEPTED, 0)}",
        f"- 删除：{label_counts.get(LABEL_REJECTED, 0)}",
        f"- 模型状态：{metrics.get('status', 'unknown')}",
    ]
    if complete_profile != "any":
        lines.extend(
            [
                f"- 完整度口径：`{complete_profile}`",
                f"- 必填字段：{'、'.join(completeness_required_field_names(complete_profile))}",
            ]
        )
        source_labeled_count = metrics.get("source_labeled_count")
        if source_labeled_count:
            lines.append(f"- 完整度筛选前可训练样本：{source_labeled_count}")
            lines.append(f"- 完整度筛选后可训练样本：{len(labeled_samples)}")
    lines.append("")
    if status_counts:
        lines.extend(["## 接受情况分布", ""])
        for status, count in status_counts.items():
            lines.append(f"- {status}: {count}")
    lines.append("")
    lines.extend(
        [
            "## 产物",
            "",
            f"- 训练集：`{dataset_path}`",
            f"- 模型：`{model_path}`" if model_path else "- 模型：未训练（有效正负样本不足）",
            "",
            "## 验证",
            "",
        ]
    )
    if metrics.get("validation_count"):
        lines.extend(
            [
                f"- 验证样本：{metrics.get('validation_count')}",
                f"- Accuracy：{metrics.get('accuracy'):.3f}" if metrics.get("accuracy") is not None else "- Accuracy：N/A",
                (
                    f"- Precision(接受)：{metrics.get('precision_accept'):.3f}"
                    if metrics.get("precision_accept") is not None
                    else "- Precision(接受)：N/A"
                ),
                (
                    f"- Recall(接受)：{metrics.get('recall_accept'):.3f}"
                    if metrics.get("recall_accept") is not None
                    else "- Recall(接受)：N/A"
                ),
            ]
        )
    else:
        lines.append("- 当前样本量较小，未切验证集；后续反馈量上来后会自动评估。")
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out_path


def record_training_run(
    *,
    run_id: str,
    run_date: str,
    pulled_count: int,
    labeled_count: int,
    accepted_count: int,
    rejected_count: int,
    pending_count: int,
    dataset_path: Path,
    model_path: Path | None,
    report_path: Path,
    metrics: dict[str, Any],
    db_path: Path = DB_PATH,
) -> None:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO ve_feedback_training_runs (
                run_id, run_date, pulled_count, labeled_count, accepted_count,
                rejected_count, pending_count, dataset_path, model_path,
                report_path, metrics_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                run_date,
                pulled_count,
                labeled_count,
                accepted_count,
                rejected_count,
                pending_count,
                str(dataset_path),
                str(model_path) if model_path else "",
                str(report_path),
                json.dumps(metrics, ensure_ascii=False, sort_keys=True),
                _now_local(),
            ),
        )


def run_feedback_training(
    *,
    bitable_url: str,
    run_date: str,
    db_path: Path = DB_PATH,
    complete_profile: str = "any",
) -> dict[str, Any]:
    _load_env()
    pulled_samples, pull_stats = pull_feedback(bitable_url, db_path=db_path)
    source_labeled_samples = load_labeled_samples(db_path)
    labeled_samples = filter_samples_by_completeness(source_labeled_samples, complete_profile)
    dataset_path = export_dataset(
        labeled_samples,
        run_date=run_date,
        output_path=output_path_with_profile(DATA_DIR, "ve_feedback_training_dataset", run_date, "jsonl", complete_profile),
    )
    requested_model_path = output_path_with_profile(
        MODEL_DIR,
        "ve_feedback_preference_nb",
        run_date,
        "json",
        complete_profile,
    )
    model_path, metrics = train_baseline_model(labeled_samples, run_date=run_date, model_path=requested_model_path)
    metrics["complete_profile"] = complete_profile
    metrics["source_labeled_count"] = len(source_labeled_samples)
    report_path = write_report(
        run_date=run_date,
        pulled_count=pull_stats["total"],
        labeled_samples=labeled_samples,
        dataset_path=dataset_path,
        model_path=model_path,
        metrics=metrics,
        status_counts=pull_stats.get("status_counts") or {},
        complete_profile=complete_profile,
        output_path=output_path_with_profile(REPORTS_DIR, "ve_feedback_training", run_date, "md", complete_profile),
    )
    label_counts = Counter(s.label for s in labeled_samples)
    record_training_run(
        run_id=f"{run_date}-{datetime.now().strftime('%H%M%S')}",
        run_date=run_date,
        pulled_count=pull_stats["total"],
        labeled_count=len(labeled_samples),
        accepted_count=int(label_counts.get(LABEL_ACCEPTED, 0)),
        rejected_count=int(label_counts.get(LABEL_REJECTED, 0)),
        pending_count=_count_pending(pulled_samples),
        dataset_path=dataset_path,
        model_path=model_path,
        report_path=report_path,
        metrics=metrics,
        db_path=db_path,
    )
    return {
        "pulled": pull_stats,
        "labeled_count": len(labeled_samples),
        "dataset_path": str(dataset_path),
        "model_path": str(model_path) if model_path else "",
        "report_path": str(report_path),
        "metrics": metrics,
    }


def _resolve_url(arg_url: str) -> str:
    return arg_url or os.getenv("VE_FEEDBACK_BITABLE_URL", "") or DEFAULT_BITABLE_URL


def _compact_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    out = dict(metrics)
    examples = out.pop("examples", None)
    if examples is not None:
        out["example_count"] = len(examples)
    return out


def cmd_pull(args: argparse.Namespace) -> int:
    _load_env()
    url = _resolve_url(args.url)
    samples, stats = pull_feedback(url, db_path=Path(args.db))
    print(
        f"[ve-feedback] pulled={stats['total']} accepted={stats['accepted']} "
        f"rejected={stats['rejected']} pending={stats['pending']} db={args.db}"
    )
    print(f"[ve-feedback] status_counts={json.dumps(stats.get('status_counts', {}), ensure_ascii=False)}")
    if samples:
        first = samples[0]
        last = samples[-1]
        print(f"[ve-feedback] first={first.ad_key} status={first.accept_status or '-'}")
        print(f"[ve-feedback] last={last.ad_key} status={last.accept_status or '-'}")
    return 0


def cmd_train(args: argparse.Namespace) -> int:
    run_date = _run_date(args.date)
    source_samples = load_labeled_samples(Path(args.db))
    samples = filter_samples_by_completeness(source_samples, args.complete_profile)
    dataset_path = export_dataset(
        samples,
        run_date=run_date,
        output_path=output_path_with_profile(
            DATA_DIR,
            "ve_feedback_training_dataset",
            run_date,
            "jsonl",
            args.complete_profile,
        ),
    )
    model_path, metrics = train_baseline_model(
        samples,
        run_date=run_date,
        model_path=output_path_with_profile(
            MODEL_DIR,
            "ve_feedback_preference_nb",
            run_date,
            "json",
            args.complete_profile,
        ),
    )
    metrics["complete_profile"] = args.complete_profile
    metrics["source_labeled_count"] = len(source_samples)
    report_path = write_report(
        run_date=run_date,
        pulled_count=0,
        labeled_samples=samples,
        dataset_path=dataset_path,
        model_path=model_path,
        metrics=metrics,
        complete_profile=args.complete_profile,
        output_path=output_path_with_profile(
            REPORTS_DIR,
            "ve_feedback_training",
            run_date,
            "md",
            args.complete_profile,
        ),
    )
    print(
        f"[ve-feedback] labeled={len(samples)} dataset={dataset_path} "
        f"model={model_path or '-'} report={report_path}"
    )
    print(json.dumps(_compact_metrics(metrics), ensure_ascii=False, sort_keys=True))
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    run_date = _run_date(args.date)
    samples = filter_samples_by_completeness(load_labeled_samples(Path(args.db)), args.complete_profile)
    dataset_path = export_dataset(
        samples,
        run_date=run_date,
        output_path=Path(args.output)
        if args.output
        else output_path_with_profile(
            DATA_DIR,
            "ve_feedback_training_dataset",
            run_date,
            "jsonl",
            args.complete_profile,
        ),
    )
    print(f"[ve-feedback] exported={dataset_path} labeled={len(samples)}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    result = run_feedback_training(
        bitable_url=_resolve_url(args.url),
        run_date=_run_date(args.date),
        db_path=Path(args.db),
        complete_profile=args.complete_profile,
    )
    print(
        f"[ve-feedback] pulled={result['pulled']['total']} labeled={result['labeled_count']} "
        f"dataset={result['dataset_path']} model={result['model_path'] or '-'} report={result['report_path']}"
    )
    print(f"[ve-feedback] status_counts={json.dumps(result['pulled'].get('status_counts', {}), ensure_ascii=False)}")
    print(json.dumps(_compact_metrics(result["metrics"]), ensure_ascii=False, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="独立拉取 VE 多维表反馈并训练素材偏好模型")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--url", default="", help="飞书多维表 URL；默认读 VE_FEEDBACK_BITABLE_URL 或内置反馈表")
    common.add_argument("--date", default="", help="产物日期，默认今天")
    common.add_argument("--db", default=str(DB_PATH), help="独立反馈训练 SQLite 路径")
    common.add_argument(
        "--complete-profile",
        choices=sorted(COMPLETENESS_PROFILES.keys()),
        default="any",
        help="训练/导出前的字段完整度过滤：any=不过滤，core=核心素材字段齐全，core_play=核心+玩法字段齐全，all=全部字段齐全",
    )
    sub = parser.add_subparsers(dest="command")

    pull = sub.add_parser("pull", parents=[common], help="只拉取多维表并写入独立反馈库")
    pull.set_defaults(func=cmd_pull)

    train = sub.add_parser("train", parents=[common], help="基于本地反馈库导出训练集并训练 baseline")
    train.set_defaults(func=cmd_train)

    export = sub.add_parser("export", parents=[common], help="只导出 JSONL 训练集")
    export.add_argument("--output", default="", help="训练集输出路径")
    export.set_defaults(func=cmd_export)

    run = sub.add_parser("run", parents=[common], help="拉取 + 入库 + 导出训练集 + 训练 baseline")
    run.set_defaults(func=cmd_run)
    return parser


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    commands = {"pull", "train", "export", "run"}
    if not argv:
        argv = ["run"]
    elif argv[0] not in commands and argv[0] not in {"-h", "--help"}:
        argv = ["run", *argv]
    args = parser.parse_args(argv)
    func = getattr(args, "func", cmd_run)
    return int(func(args))


if __name__ == "__main__":
    raise SystemExit(main())
