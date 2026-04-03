#!/usr/bin/env python3
"""
实测 OpenRouter 上 Qwen3.6 Plus Preview 是否接受多模态（image_url）请求。

官网说明（2026-04 抓取 https://openrouter.ai/qwen/qwen3.6-plus-preview ）：
  文案强调「推理、agent、编码、前端」等，未写 vision / multimodal；
  同页其它 Qwen 多模态模型（如 Qwen3.5-397B）会明确写 vision-language。

API 实测（带 image_url 时）：
  错误信息示例：No endpoints found that support image input
  → 在 OpenRouter 上该模型 **不提供图像输入端点**，即不按多模态（图）使用。

模型 id：完整名为 `qwen/qwen3.6-plus-preview`；免费预览常用 `qwen/qwen3.6-plus-preview:free`
（无 `:free` 时若账号无对应路由可能 404: No endpoints found for ...）。

用法（项目根目录，已配置 .env 的 OPENROUTER_API_KEY）：

  source .venv/bin/activate
  python scripts/test_openrouter_qwen36plus_multimodal.py

  # 指定模型或图片 URL
  python scripts/test_openrouter_qwen36plus_multimodal.py --model qwen/qwen3.6-plus-preview:free
  python scripts/test_openrouter_qwen36plus_multimodal.py --image-url 'https://...'
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Dict

from dotenv import load_dotenv
from openai import OpenAI

from path_util import PROJECT_ROOT

load_dotenv(PROJECT_ROOT / ".env")

# OpenRouter 完整 id（口语 qwen3.6plus-preview）；免费预览加 :free
DEFAULT_MODEL = "qwen/qwen3.6-plus-preview:free"

# 小图，公网可访问，便于复现
DEFAULT_IMAGE_URL = (
    "https://upload.wikimedia.org/wikipedia/commons/thumb/a/a7/"
    "Camponotus_flavomarginatus_ant.jpg/320px-Camponotus_flavomarginatus_ant.jpg"
)


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
    p = argparse.ArgumentParser(description="测试 Qwen3.6 Plus Preview 多模态 image_url")
    p.add_argument(
        "--model",
        default=os.getenv("TEST_QWEN36_MODEL", "").strip() or DEFAULT_MODEL,
        help=f"OpenRouter 模型 id，默认 {DEFAULT_MODEL}",
    )
    p.add_argument(
        "--image-url",
        default=os.getenv("TEST_IMAGE_URL", "").strip() or DEFAULT_IMAGE_URL,
        help="公网图片 URL（HTTPS）",
    )
    p.add_argument(
        "--prompt",
        default="请用一句话回答：图里主要是什么？只输出中文短语。",
        help="多模态 user 中的文字部分",
    )
    args = p.parse_args()

    api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    if not api_key:
        print("错误：未配置 OPENROUTER_API_KEY", file=sys.stderr)
        sys.exit(1)

    model = args.model.strip()
    image_url = args.image_url.strip()
    user_text = args.prompt.strip()

    client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=api_key)

    system = "你是助手。请按要求简短回答。"

    print(f"模型: {model}")
    print(f"图片 URL: {image_url[:100]}{'...' if len(image_url) > 100 else ''}")
    print("---")

    # 1) 纯文本
    print("[1] 纯文本 chat")
    try:
        r1 = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": "1+1等于几？只输出数字。"},
            ],
        )
        print("usage:", json.dumps(_usage_to_dict(getattr(r1, "usage", None)), ensure_ascii=False))
        c1 = (r1.choices[0].message.content or "").strip().replace("\n", " ")
        print("回复:", c1[:300])
    except Exception as e:
        print("失败:", repr(e))
        sys.exit(1)

    print("---")

    # 2) 多模态：OpenAI-compatible image_url
    print("[2] 多模态 user（text + image_url）")
    try:
        r2 = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": user_text},
                        {"type": "image_url", "image_url": {"url": image_url}},
                    ],
                },
            ],
        )
        print("usage:", json.dumps(_usage_to_dict(getattr(r2, "usage", None)), ensure_ascii=False))
        c2 = (r2.choices[0].message.content or "").strip().replace("\n", " ")
        print("回复:", c2[:500])
        print("\n结论: 该模型在当前请求下 **接受** image_url 多模态。")
    except Exception as e:
        print("失败:", repr(e))
        msg = str(e)
        no_image = "support image" in msg.lower() or "image input" in msg.lower()
        print(
            "\n结论: 多模态请求 **不被支持**（OpenRouter 对该模型无图像输入端点）。"
            "图片/视频理解请改用标明 VL / vision-language 的模型，例如 "
            "`qwen/qwen3.5-397b-a17b` 或 `qwen/qwen3-vl-8b-instruct` 等。"
        )
        # 已得到明确结论时仍算「测试脚本成功跑完」
        sys.exit(0 if no_image else 2)


if __name__ == "__main__":
    main()
