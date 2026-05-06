"""
使用 Google AI Studio 的 API Key，直接调用 Gemini 视频模型做一次视频分析测试。

支持两种用法：
1）手动指定视频 URL：
    python scripts/test_gemini_video_google.py --video-url "https://example.com/your_video.mp4"
2）从「每日灵感表」中自动挑选一条最近的带视频素材：
    python scripts/test_gemini_video_google.py

注意：
- 需要先在 .env 中配置 GOOGLE_API_KEY（或 GEMINI_API_KEY，均视为 Google AI Studio Key）。
- 该脚本使用 google-generativeai 官方 SDK，需提前安装：
    pip install google-generativeai

运行完成后，会在 data/ 目录下生成 test_gemini_video_google.json，
其中包含本次调用的输入信息和 Gemini 返回的完整分析结果。
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv

from path_util import DATA_DIR
from ua_crawl_db import DB_PATH as UA_DB_PATH


load_dotenv()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "使用 Google AI Studio 的 Key 调用 Gemini 视频模型做一次测试分析。\n"
            "若不指定 --video-url，则会自动从 ad_creative_analysis 中挑选一条最近的带视频素材。"
        )
    )
    p.add_argument(
        "--video-url",
        required=False,
        help="待分析的视频 URL（建议为可公网访问的 mp4 链接）。不传则自动从灵感表中选择。",
    )
    return p.parse_args()


def pick_one_video_creative() -> Dict[str, Any] | None:
    """
    从 ad_creative_analysis 中随机取一条有 video_url 的素材（优先按最近 updated_at 排序）。
    """
    conn = sqlite3.connect(UA_DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              ad_key,
              crawl_date,
              category,
              product,
              advertiser_name,
              COALESCE(title_zh, title) AS title,
              COALESCE(body_zh, body) AS body,
              platform,
              video_url,
              video_duration,
              preview_img_url
            FROM ad_creative_analysis
            WHERE video_url IS NOT NULL
              AND TRIM(video_url) != ''
            ORDER BY datetime(updated_at) DESC
            LIMIT 20
            """
        )
        rows = cur.fetchall()
        if not rows:
            return None
        row = random.choice(rows)
        return {
            "ad_key": row["ad_key"],
            "crawl_date": row["crawl_date"],
            "category": row["category"],
            "product": row["product"],
            "advertiser_name": row["advertiser_name"],
            "title": row["title"],
            "body": row["body"],
            "platform": row["platform"],
            "video_url": row["video_url"],
            "video_duration": row["video_duration"],
            "preview_img_url": row["preview_img_url"],
        }
    finally:
        conn.close()


def main() -> None:
    api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("错误: 请在 .env 中配置 GOOGLE_API_KEY 或 GEMINI_API_KEY 后再运行本脚本。")
        return

    try:
        import google.generativeai as genai  # type: ignore
    except ImportError:
        print(
            "错误: 未安装 google-generativeai。\n"
            "请先运行: pip install google-generativeai"
        )
        return

    args = parse_args()
    video_url_arg = (args.video_url or "").strip()

    # 若未指定 URL，则自动从灵感表中选一条带视频素材
    if video_url_arg:
        video_url = video_url_arg
        picked = None
    else:
        picked = pick_one_video_creative()
        if not picked:
            print("未在 ad_creative_analysis 中找到任何带 video_url 的素材，且未指定 --video-url，无法测试。")
            return
        video_url = str(picked["video_url"])

    genai.configure(api_key=api_key)

    # 模型名称可通过环境变量覆盖，默认使用 1.5 Pro
    model_name = os.getenv("GOOGLE_GEMINI_VIDEO_MODEL", "gemini-1.5-pro")
    model = genai.GenerativeModel(model_name)

    prompt = (
        "你是资深 UA 视频创意分析专家，擅长从视频内容中拆解玩法与转化逻辑。\n\n"
        "请基于以下视频，输出一个结构化的中文分析，包含：\n"
        "1）画面与剧情概述；\n"
        "2）玩法与交互路径（从用户视角）；\n"
        "3）前几秒的 Hook 与关键镜头；\n"
        "4）可复用的 UA 素材建议（给出清晰结构和可执行要点）。\n"
        "请使用 Markdown，分段清晰。"
    )

    print(f"正在调用 Gemini 模型 {model_name} 分析视频：{video_url}")

    try:
        response = model.generate_content(
            [
                prompt,
                {
                    "video_url": {
                        "url": video_url,
                    }
                },
            ]
        )
    except Exception as e:
        print(f"调用 Gemini 视频模型失败: {e}")
        return

    text = ""
    try:
        text = response.text or ""
    except Exception:
        pass

    # 用量统计（usage_metadata）
    usage_info: Dict[str, Any] = {}
    try:
        usage = getattr(response, "usage_metadata", None)
        if usage is not None:
            prompt_tokens = getattr(usage, "prompt_token_count", None)
            completion_tokens = getattr(usage, "candidates_token_count", None)
            total_tokens = getattr(usage, "total_token_count", None)
            usage_info = {
                "prompt_token_count": prompt_tokens,
                "candidates_token_count": completion_tokens,
                "total_token_count": total_tokens,
            }
            print("【用量统计】")
            print(f"输入 Token (Prompt): {prompt_tokens}")
            print(f"输出 Token (Completion): {completion_tokens}")
            print(f"总计 Token: {total_tokens}")
    except Exception:
        pass

    out: Dict[str, Any] = {
        "tested_at": datetime.now().isoformat(timespec="seconds"),
        "model": model_name,
        "video_url": video_url,
        "prompt": prompt,
        "analysis": text,
    }
    if picked:
        out["picked_creative"] = picked
    if usage_info:
        out["usage_metadata"] = usage_info
    try:
        # 部分 SDK 响应对象支持 to_dict
        raw = getattr(response, "to_dict", lambda: None)()
        if raw is None:
            raw = str(response)
        out["raw_response"] = raw
    except Exception:
        out["raw_response"] = str(response)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATA_DIR / "test_gemini_video_google.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"已将调用输入与返回结果写入: {out_path}")


if __name__ == "__main__":
    main()

