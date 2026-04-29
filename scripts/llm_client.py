"""
统一 LLM 调用层：model fallback chain、circuit breaker、usage 追踪、embedding。

所有脚本的 LLM 调用统一经由此模块，不再各自重复 try/except 降级逻辑。

用法示例::

    from llm_client import call_text, call_vision, call_embedding, flush_usage

    # 纯文本
    result = call_text("你是 UA 专家", "分析这条素材...")

    # 视觉（视频/图片），自动降级到文本模型
    result = call_vision(
        system="...", user_text="...",
        media_url="https://...", media_type="image",
        text_fallback_system="...",  # 降级到纯文本时的 system msg
    )

    # 嵌入向量
    vec = call_embedding("这是一段分析文本")

    # 脚本结束时刷入用量表
    flush_usage("2026-04-02")
"""
from __future__ import annotations

import os
import struct
import threading
from typing import Any, Dict, List, Literal, Optional, Sequence

from dotenv import load_dotenv
from openai import OpenAI

from path_util import PROJECT_ROOT
from ua_crawl_db import accumulate_usage_tokens, merge_llm_usage_daily

load_dotenv(PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# 默认模型
# ---------------------------------------------------------------------------
DEFAULT_TEXT_FALLBACK = "qwen/qwen3.5-397b-a17b"

# ---------------------------------------------------------------------------
# Module-level state (进程内共享)
# ---------------------------------------------------------------------------
_blocked_models: set[str] = set()
_blocked_lock = threading.Lock()
_usage_patch: Dict[str, Dict[str, int]] = {}
_usage_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Usage tracking
# ---------------------------------------------------------------------------
def get_usage() -> Dict[str, Dict[str, int]]:
    with _usage_lock:
        return dict(_usage_patch)


def flush_usage(target_date: str) -> None:
    """将本进程内累计的 token 用量合并写入 ai_llm_usage_daily。"""
    if not target_date or not _usage_patch:
        return
    try:
        merge_llm_usage_daily(target_date, _usage_patch)
        tot = sum(int(v.get("total_tokens", 0) or 0) for v in _usage_patch.values())
        print(f"[llm-client] usage flushed date={target_date} total_tokens≈{tot}")
    except Exception as e:
        print(f"[llm-client] flush_usage failed: {e}")


def _accumulate(provider: str, model: str, usage: Any) -> None:
    with _usage_lock:
        accumulate_usage_tokens(_usage_patch, provider, model, usage)


# ---------------------------------------------------------------------------
# Circuit breaker — 区域受限模型一次 403 后整个进程不再重试
# ---------------------------------------------------------------------------
def _is_region_block(e: BaseException) -> bool:
    s = str(e).lower()
    return "403" in s and ("region" in s or "not available in your region" in s)


def _block_model(model: str, reason: str) -> None:
    with _blocked_lock:
        if model not in _blocked_models:
            _blocked_models.add(model)
            print(f"[llm-client] model blocked for this process: {model} ({reason})")


def _is_blocked(model: str) -> bool:
    with _blocked_lock:
        return model in _blocked_models


# ---------------------------------------------------------------------------
# Model resolution helpers
# ---------------------------------------------------------------------------
def _or_key() -> str:
    return os.getenv("OPENROUTER_API_KEY", "").strip()


def _oa_key() -> str:
    return os.getenv("OPENAI_API_KEY", "").strip()


def resolve_vision_models() -> List[str]:
    primary = os.getenv("OPENROUTER_VIDEO_MODEL", "").strip()
    fallback = os.getenv("OPENROUTER_VISION_FALLBACK_MODEL", DEFAULT_TEXT_FALLBACK).strip()
    out: List[str] = []
    if primary:
        out.append(primary)
    if fallback and fallback not in out:
        out.append(fallback)
    return out


def resolve_text_model() -> str:
    return os.getenv("OPENROUTER_TEXT_FALLBACK_MODEL", DEFAULT_TEXT_FALLBACK).strip()


def resolve_cluster_models() -> List[str]:
    """
    聚类 / 方向卡片用纯文本 chat。默认同**多模态脚本的 Qwen 侧**：
    首选 `OPENROUTER_CLUSTER_MODEL`；否则 = `resolve_text_model()`（即 `OPENROUTER_TEXT_FALLBACK_MODEL`，
    默认 `qwen/qwen3.5-397b-a17b`）。后续依次尝试 `OPENROUTER_CLUSTER_FALLBACK_MODEL`、
    `OPENROUTER_VISION_FALLBACK_MODEL`（与视觉主链的兜底一致），最后为 `OPENROUTER_MODEL`（历史默认 gemini）。
    """
    primary = (os.getenv("OPENROUTER_CLUSTER_MODEL") or "").strip() or resolve_text_model()
    out: List[str] = [primary]
    for m in (
        (os.getenv("OPENROUTER_CLUSTER_FALLBACK_MODEL") or "").strip(),
        (os.getenv("OPENROUTER_VISION_FALLBACK_MODEL") or DEFAULT_TEXT_FALLBACK).strip(),
        (os.getenv("OPENROUTER_MODEL") or "google/gemini-2.5-flash").strip(),
    ):
        if m and m not in out:
            out.append(m)
    return out


# ---------------------------------------------------------------------------
# Core: text
# ---------------------------------------------------------------------------
def call_text(
    system: str,
    user: str,
    *,
    models: Sequence[str] | None = None,
) -> str:
    """纯文本 LLM 调用，按 models 列表依次尝试。"""
    key = _or_key()
    if models is None:
        models = [resolve_text_model()]

    last_err: Exception | None = None

    if key:
        for model in models:
            if _is_blocked(model):
                continue
            try:
                client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=key)
                r = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                )
                _accumulate("openrouter", model, getattr(r, "usage", None))
                choices = getattr(r, "choices", None) or []
                if not choices or not getattr(choices[0], "message", None):
                    raise RuntimeError(f"LLM empty response (text, model={model})")
                return (choices[0].message.content or "").strip()
            except Exception as e:
                last_err = e
                if _is_region_block(e):
                    _block_model(model, "region")
                elif len(models) > 1:
                    print(f"[llm-client] text model={model} failed: {e}")
                continue

    oa = _oa_key()
    if oa:
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        try:
            client = OpenAI(api_key=oa, base_url=os.getenv("OPENAI_API_BASE") or None)
            r = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            )
            _accumulate("openai", model, getattr(r, "usage", None))
            choices = getattr(r, "choices", None) or []
            if not choices or not getattr(choices[0], "message", None):
                raise RuntimeError(f"LLM empty (text-openai, model={model})")
            return (choices[0].message.content or "").strip()
        except Exception as e:
            last_err = e

    raise RuntimeError(f"All text models failed: {last_err}")


# ---------------------------------------------------------------------------
# Core: vision (video / image) with automatic text fallback
# ---------------------------------------------------------------------------
def call_vision(
    system: str,
    user_text: str,
    media_url: str,
    media_type: Literal["video", "image"],
    *,
    vision_models: Sequence[str] | None = None,
    text_fallback_system: str | None = None,
    text_fallback_models: Sequence[str] | None = None,
    quiet: bool = False,
) -> str:
    """
    多模态调用：依次尝试 vision_models（含媒体附件），
    全部失败后降级到 text_fallback_system + call_text（纯文本）。
    """
    key = _or_key()
    if vision_models is None:
        vision_models = resolve_vision_models()

    media_block = (
        {"type": "video_url", "video_url": {"url": str(media_url)}}
        if media_type == "video"
        else {"type": "image_url", "image_url": {"url": str(media_url)}}
    )

    last_err: Exception | None = None
    if key:
        for model in vision_models:
            if _is_blocked(model):
                continue
            try:
                client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=key)
                r = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system},
                        {
                            "role": "user",
                            "content": [
                                {"type": "text", "text": user_text},
                                media_block,
                            ],
                        },
                    ],
                )
                _accumulate("openrouter", model, getattr(r, "usage", None))
                choices = getattr(r, "choices", None) or []
                if not choices or not getattr(choices[0], "message", None):
                    raise RuntimeError(f"LLM empty ({media_type}, model={model})")
                return (choices[0].message.content or "").strip()
            except Exception as e:
                last_err = e
                if _is_region_block(e):
                    _block_model(model, "region")
                if not quiet:
                    print(f"[llm-client] vision model={model} failed: {e}")
                continue

    fb_sys = text_fallback_system or system
    fb_models = list(text_fallback_models or [resolve_text_model()])
    if not quiet:
        print(f"[llm-client] all vision models exhausted, text fallback → {fb_models[0]}")
    return call_text(fb_sys, user_text, models=fb_models)


# ---------------------------------------------------------------------------
# Embedding
# ---------------------------------------------------------------------------
EMBEDDING_DIM = 1536  # text-embedding-3-small default


def call_embedding(
    text: str,
    *,
    model: str | None = None,
) -> List[float]:
    """生成文本嵌入向量。

    优先级：
    1. 本地 sentence-transformers（bge-small-zh-v1.5，512维，零API成本）
    2. OpenRouter / OpenAI API（降级）

    环境变量 EMBEDDING_PROVIDER=api 可强制走 API。
    """
    provider = (os.getenv("EMBEDDING_PROVIDER") or "").strip().lower()
    if provider != "api":
        vec = _call_local_embedding(text)
        if vec is not None:
            return vec
    # API 降级
    key = _or_key()
    if key:
        em = model or os.getenv("EMBEDDING_MODEL", "openai/text-embedding-3-small").strip()
        try:
            client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=key)
            r = client.embeddings.create(model=em, input=[text])
            _accumulate("openrouter", em, getattr(r, "usage", None))
            return r.data[0].embedding
        except Exception:
            pass  # API 失败，继续尝试本地
    oa = _oa_key()
    if oa:
        em = model or "text-embedding-3-small"
        try:
            client = OpenAI(api_key=oa, base_url=os.getenv("OPENAI_API_BASE") or None)
            r = client.embeddings.create(model=em, input=[text])
            _accumulate("openai", em, getattr(r, "usage", None))
            return r.data[0].embedding
        except Exception:
            pass
    # 最后尝试本地
    vec = _call_local_embedding(text)
    if vec is not None:
        return vec
    raise RuntimeError("所有 embedding 方式均失败（本地模型不可用 + API 403/不可用）")


_local_embedding_model = None


def _call_local_embedding(text: str) -> List[float] | None:
    """本地 sentence-transformers embedding（进程内单例）。"""
    global _local_embedding_model
    model_name = os.getenv("LOCAL_EMBEDDING_MODEL", "BAAI/bge-small-zh-v1.5").strip()
    try:
        if _local_embedding_model is None:
            from sentence_transformers import SentenceTransformer
            _local_embedding_model = SentenceTransformer(model_name)
        vec = _local_embedding_model.encode([text])[0]
        return list(vec)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Embedding utilities (pure Python, no numpy)
# ---------------------------------------------------------------------------
def cosine_similarity(a: List[float], b: List[float]) -> float:
    if len(a) != len(b) or not a:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(x * x for x in b) ** 0.5
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def embedding_to_bytes(vec: List[float]) -> bytes:
    return struct.pack(f"{len(vec)}f", *vec)


def bytes_to_embedding(data: bytes) -> List[float]:
    n = len(data) // 4
    return list(struct.unpack(f"{n}f", data))


# ---------------------------------------------------------------------------
# OpenRouter key meter（主流程起止可选打印用量，与 openrouter_key_snapshot.sh 同源 API）
# ---------------------------------------------------------------------------
def print_openrouter_key_meter(label: str = "") -> None:
    """
    GET https://openrouter.ai/api/v1/key，在日志里对比工作流前/后用量。
    默认关闭；设 OPENROUTER_METER=1 / true 开启。需 .env 中 OPENROUTER_API_KEY。
    """
    v = (os.getenv("OPENROUTER_METER") or "0").strip().lower()
    if v in ("0", "false", "no", "off", ""):
        return
    key = _or_key()
    if not key:
        return
    try:
        import json as _json
        import urllib.request

        req = urllib.request.Request(
            "https://openrouter.ai/api/v1/key",
            headers={"Authorization": f"Bearer {key}"},
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=45) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        try:
            obj = _json.loads(raw)
            pretty = _json.dumps(obj, ensure_ascii=False, indent=2)
        except _json.JSONDecodeError:
            pretty = raw
        pre = f"{label} " if label else ""
        print(f"[openrouter-meter] {pre}\n{pretty}", flush=True)
    except Exception as e:
        print(f"[openrouter-meter] {label} 请求失败: {e}", flush=True)
