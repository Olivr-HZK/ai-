#!/usr/bin/env python3
"""
对比 OpenRouter 在同一份提示词下：
  A) 纯文本 user 消息
  B) 多模态 user：同一段 text + video_url

用于验证「带视频」时 API 返回的 usage（prompt_tokens 等）是否明显大于纯文本。
若两者几乎相同，可能是上游未把视频侧 token 计入 usage（需以 OpenRouter 文档/账单为准）。

用法（项目根目录，已配置 .env 的 OPENROUTER_API_KEY）：

  source .venv/bin/activate
  python scripts/test_openrouter_video_vs_text_tokens.py

不传 --video-url 时，会从 data/video_enhancer_pipeline.db 的 daily_creative_insights
中任意一条有 video_url 的记录取链接。

可选：

  --video-url  显式指定 URL
  --model      指定模型（默认用 Qwen，避免 Gemini 区域不可用）
  --prompt     用户提示词片段

默认模型：qwen/qwen3.5-397b-a17b。可用 --model 或环境变量 TEST_OPENROUTER_MODEL 覆盖。
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from openai import OpenAI

from path_util import DATA_DIR, PROJECT_ROOT

load_dotenv(PROJECT_ROOT / ".env")

VE_DB = DATA_DIR / "video_enhancer_pipeline.db"

# 与 analyze_video_from_raw_json 中 OPENROUTER_VISION_FALLBACK_MODEL 默认一致；测试脚本优先用 Qwen，避免调 Gemini。
DEFAULT_TEST_MODEL = "qwen/qwen3.5-397b-a17b"


def _pick_video_url_from_db() -> Optional[str]:
    """从 daily_creative_insights 取一条非空 video_url；必要时从 raw_json 解析。"""
    if not VE_DB.exists():
        return None
    conn = sqlite3.connect(VE_DB)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT video_url FROM daily_creative_insights
            WHERE COALESCE(TRIM(video_url), '') <> ''
            ORDER BY RANDOM()
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if row and row[0]:
            return str(row[0]).strip()
        cur.execute(
            """
            SELECT raw_json FROM daily_creative_insights
            WHERE COALESCE(TRIM(raw_json), '') <> ''
            ORDER BY RANDOM()
            LIMIT 50
            """
        )
        for (raw_json,) in cur.fetchall() or []:
            try:
                c = json.loads(raw_json or "{}")
            except Exception:
                continue
            if c.get("video_url"):
                return str(c["video_url"]).strip()
            for r in c.get("resource_urls") or []:
                if isinstance(r, dict) and r.get("video_url"):
                    return str(r["video_url"]).strip()
        return None
    finally:
        conn.close()


def _usage_to_dict(usage: Any) -> Dict[str, Any]:
    if usage is None:
        return {}
    if hasattr(usage, "model_dump"):
        try:
            return dict(usage.model_dump())
        except Exception:
            pass
    return {
        "prompt_tokens": getattr(usage, "prompt_tokens", None),
        "completion_tokens": getattr(usage, "completion_tokens", None),
        "total_tokens": getattr(usage, "total_tokens", None),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="对比同提示词下纯文本 vs 视频 URL 的 token usage")
    p.add_argument(
        "--video-url",
        default=os.getenv("TEST_VIDEO_URL", "").strip(),
        help="公网可访问的视频 URL；不传则从 video_enhancer_pipeline.db 随机取一条。",
    )
    p.add_argument(
        "--model",
        default="",
        help=f"OpenRouter 模型 id；默认 {DEFAULT_TEST_MODEL}；也可用环境变量 TEST_OPENROUTER_MODEL。",
    )
    p.add_argument(
        "--prompt",
        default="请用两句话概括这条素材可能的广告意图与受众。",
        help="用户侧固定提示词（两种调用共用）。",
    )
    args = p.parse_args()

    api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    if not api_key:
        print("错误：未配置 OPENROUTER_API_KEY", file=sys.stderr)
        sys.exit(1)

    model = (
        (args.model or "").strip()
        or os.getenv("TEST_OPENROUTER_MODEL", "").strip()
        or DEFAULT_TEST_MODEL
    )

    explicit_url = (args.video_url or os.getenv("TEST_VIDEO_URL", "").strip() or "").strip()
    video_url = explicit_url or (_pick_video_url_from_db() or "")
    if not video_url:
        print(
            "错误：库中无可用 video_url，请传入 --video-url 或设置 TEST_VIDEO_URL",
            file=sys.stderr,
        )
        sys.exit(1)
    src = "参数/TEST_VIDEO_URL" if explicit_url else "数据库 daily_creative_insights"
    print(f"测试视频 URL（{src}）: {video_url[:120]}{'...' if len(video_url) > 120 else ''}")

    system = (
        "你是 UA 视频创意分析助手。请严格用简体中文回答，保持简短。"
    )
    user_text = args.prompt.strip()

    client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=api_key)

    # A) 纯文本：user 为单条字符串（与主流程「仅文本」一致）
    print(f"模型: {model}")
    print("---")
    print("[A] 纯文本 user（无 video_url）")
    r_a = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user_text},
        ],
    )
    u_a = _usage_to_dict(getattr(r_a, "usage", None))
    print("usage:", json.dumps(u_a, ensure_ascii=False, indent=2))
    preview_a = (r_a.choices[0].message.content or "")[:200].replace("\n", " ")
    print("回复预览:", preview_a, "..." if len(preview_a) >= 200 else "")

    # B) 多模态：与 analyze_video_from_raw_json._call_llm_video 相同结构
    print("---")
    print("[B] 多模态 user（text + video_url）")
    r_b = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_text},
                    {"type": "video_url", "video_url": {"url": video_url}},
                ],
            },
        ],
    )
    u_b = _usage_to_dict(getattr(r_b, "usage", None))
    print("usage:", json.dumps(u_b, ensure_ascii=False, indent=2))
    preview_b = (r_b.choices[0].message.content or "")[:200].replace("\n", " ")
    print("回复预览:", preview_b, "..." if len(preview_b) >= 200 else "")

    print("---")
    print("对比（仅 API 返回的 usage，非账单最终计费）：")
    pt_a = int(u_a.get("prompt_tokens") or 0)
    pt_b = int(u_b.get("prompt_tokens") or 0)
    tt_a = int(u_a.get("total_tokens") or 0)
    tt_b = int(u_b.get("total_tokens") or 0)
    print(f"  prompt_tokens:  A={pt_a}  B={pt_b}  (B-A)={pt_b - pt_a}")
    print(f"  total_tokens:   A={tt_a}  B={tt_b}  (B-A)={tt_b - tt_a}")
    if pt_b <= pt_a and tt_b <= tt_a:
        print(
            "  提示：B 的 prompt/total 未高于 A，可能说明当前接口返回的 usage 未单独体现视频输入侧，"
            "或视频按固定方式计费；请以 OpenRouter 控制台/文档为准。"
        )


if __name__ == "__main__":
    main()
