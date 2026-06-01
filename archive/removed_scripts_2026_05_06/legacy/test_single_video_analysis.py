"""
从数据库中随机取一条带视频的素材，使用当前的「视频分析」逻辑跑一次，
并将输入信息 + LLM 返回结果写入一个临时 JSON 文件，便于你检查是否真的做了视频分析。

用法（项目根目录）：
  source .venv/bin/activate
  python scripts/test_single_video_analysis.py
"""

from __future__ import annotations

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
from analyze_creatives_with_llm import _call_llm_video, build_prompt  # type: ignore


load_dotenv()


def pick_one_video_creative(conn: sqlite3.Connection) -> Dict[str, Any] | None:
    """
    从 ad_creative_analysis 中随机取一条有 video_url 的素材（优先按最近 updated_at 排序）。
    """
    conn.row_factory = sqlite3.Row
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
          preview_img_url,
          selected_json
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
    # 从 selected_json 里再补一些指标（如 heat / all_exposure_value / call_to_action）
    heat = 0
    all_exp = 0
    cta = ""
    try:
        sel = json.loads(row["selected_json"] or "{}")
        heat = int(sel.get("heat") or 0)
        all_exp = int(sel.get("all_exposure_value") or 0)
        cta = (sel.get("call_to_action") or "").strip()
    except Exception:
        pass

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
        "heat": heat,
        "all_exposure_value": all_exp,
        "call_to_action": cta,
    }


def build_test_prompt(item: Dict[str, Any]) -> str:
    """
    使用「每日工作流广告创意分析」中的 build_prompt 提示词风格，
    构造一条与实际 daily UA job 完全一致的文本 prompt。
    """
    # 构造一个最小 selected dict，供 build_prompt 使用
    selected = {
        "advertiser_name": item.get("advertiser_name") or "",
        "title": item.get("title") or "",
        "body": item.get("body") or "",
        "platform": item.get("platform") or "",
        "video_duration": item.get("video_duration") or 0,
        "preview_img_url": item.get("preview_img_url") or "",
        "all_exposure_value": item.get("all_exposure_value") or 0,
        "heat": item.get("heat") or 0,
        "call_to_action": item.get("call_to_action") or "",
        "resource_urls": [
            {"video_url": item.get("video_url") or ""}
        ],
    }
    # daily UA job 中 build_prompt 的用法：用中文标题/正文优先覆盖
    prompt = build_prompt(
        category=item.get("category") or "",
        product=item.get("product") or "",
        selected=selected,
        title_zh=item.get("title") or "",
        body_zh=item.get("body") or "",
    )
    return prompt


def main() -> None:
    if not os.getenv("OPENROUTER_API_KEY") or not os.getenv("OPENROUTER_VIDEO_MODEL"):
        print("错误: 请在 .env 中配置 OPENROUTER_API_KEY 和 OPENROUTER_VIDEO_MODEL 后再运行本测试脚本。")
        return

    conn = sqlite3.connect(UA_DB_PATH)
    try:
        item = pick_one_video_creative(conn)
    finally:
        conn.close()

    if not item:
        print("未在 ad_creative_analysis 中找到任何带 video_url 的素材，无法测试。")
        return

    print("选中的测试素材：")
    print(
        f"- ad_key={item['ad_key'][:12]}..., "
        f"category={item['category']}, product={item['product']}, "
        f"video_url={item['video_url']}"
    )

    prompt = build_test_prompt(item)
    print("\n正在调用视频分析模型进行测试...")
    analysis = _call_llm_video(prompt, str(item.get("video_url") or ""))

    out = {
        "tested_at": datetime.now().isoformat(timespec="seconds"),
        "model": os.getenv("OPENROUTER_VIDEO_MODEL"),
        "input": item,
        "prompt": prompt,
        "analysis": analysis,
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATA_DIR / "test_video_analysis_single.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"\n已将测试输入 + LLM 返回写入: {out_path}")


if __name__ == "__main__":
    main()

