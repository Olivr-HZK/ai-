"""Enterprise WeChat robot helpers."""

from __future__ import annotations

from typing import Any, Dict, List

import requests

WECOM_MAX_TEXT_BYTES = 3800


def split_by_utf8_bytes(text: str, max_bytes: int = WECOM_MAX_TEXT_BYTES) -> List[str]:
    chunks: List[str] = []
    cur_chars: List[str] = []
    cur_bytes = 0
    for ch in text:
        b = len(ch.encode("utf-8"))
        if cur_bytes + b > max_bytes and cur_chars:
            chunks.append("".join(cur_chars))
            cur_chars = [ch]
            cur_bytes = b
        else:
            cur_chars.append(ch)
            cur_bytes += b
    if cur_chars:
        chunks.append("".join(cur_chars))
    return chunks


def post_wecom(webhook: str, payload: Dict[str, Any], *, timeout: int = 10) -> Dict[str, Any]:
    resp = requests.post(webhook, json=payload, timeout=timeout)
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}
    if resp.status_code != 200 or data.get("errcode") != 0:
        raise RuntimeError(f"[wecom] 推送失败: status={resp.status_code}, resp={data}")
    return data


def push_wecom_markdown(webhook: str, text: str, *, max_bytes: int = WECOM_MAX_TEXT_BYTES) -> None:
    """Push Enterprise WeChat markdown and split long content by UTF-8 bytes."""
    if not webhook:
        print("[wecom] 未配置 webhook，跳过。")
        return

    chunks = split_by_utf8_bytes(text, max_bytes=max_bytes)
    total = len(chunks)
    for i, part in enumerate(chunks, start=1):
        content = f"[{i}/{total}]\n{part}" if total > 1 else part
        payload = {"msgtype": "markdown", "markdown": {"content": content}}
        post_wecom(webhook, payload)
    print(f"[wecom] markdown 推送完成，共 {total} 段。")
