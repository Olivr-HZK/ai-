"""
仅基于指定 raw JSON 做视频创意分析（不读库、不入库）。

默认输入：
  data/test_video_enhancer_2_2026-03-18_raw.json

输出：
  data/video_analysis_test_video_enhancer_2_2026-03-18_raw.json
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List

from openai import OpenAI
from dotenv import load_dotenv

from path_util import DATA_DIR

load_dotenv()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="对 raw JSON 中素材做视频分析（仅文件模式）")
    p.add_argument(
        "--input",
        default=str(DATA_DIR / "test_video_enhancer_2_2026-03-18_raw.json"),
        help="输入 raw JSON 文件路径",
    )
    p.add_argument(
        "--output",
        default="",
        help="输出文件路径（默认自动生成到 data/ 下）",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=0,
        help="最多分析多少条（0=不限制）",
    )
    return p.parse_args()


def _pick_video_url(creative: Dict[str, Any]) -> str:
    if creative.get("video_url"):
        return str(creative["video_url"])
    for r in creative.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("video_url"):
            return str(r["video_url"])
    return ""


def _call_llm_text(system: str, user_content: str) -> str:
    # 1) OpenRouter
    or_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    if or_key:
        client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=or_key)
        model = os.getenv("OPENROUTER_MODEL", "google/gemini-2.5-flash")
        r = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_content},
            ],
        )
        choices = getattr(r, "choices", None) or []
        if not choices or not getattr(choices[0], "message", None):
            raise RuntimeError(f"LLM 返回为空（text, model={model}）")
        return (choices[0].message.content or "").strip()

    # 2) OpenAI
    oa_key = os.getenv("OPENAI_API_KEY", "").strip()
    if oa_key:
        client = OpenAI(api_key=oa_key, base_url=os.getenv("OPENAI_API_BASE") or None)
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        r = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_content},
            ],
        )
        choices = getattr(r, "choices", None) or []
        if not choices or not getattr(choices[0], "message", None):
            raise RuntimeError(f"LLM 返回为空（text, model={model}）")
        return (choices[0].message.content or "").strip()

    raise RuntimeError("未配置 OPENROUTER_API_KEY 或 OPENAI_API_KEY")


def _call_llm_video(user_content: str, video_url: str) -> str:
    or_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    video_model = os.getenv("OPENROUTER_VIDEO_MODEL", "").strip()
    if or_key and video_model:
        client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=or_key)
        r = client.chat.completions.create(
            model=video_model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "你是资深 UA 视频创意分析专家，擅长从视频画面、镜头、节奏拆解转化逻辑。"
                        "请用简体中文输出结构化结论。"
                    ),
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": user_content},
                        {"type": "video_url", "video_url": {"url": str(video_url)}},
                    ],
                },
            ],
        )
        choices = getattr(r, "choices", None) or []
        if not choices or not getattr(choices[0], "message", None):
            raise RuntimeError(f"LLM 返回为空（video, model={video_model}, video_url={video_url[:120]}）")
        return (choices[0].message.content or "").strip()

    # 无视频模型时回退文本
    return _call_llm_text(
        "你是 UA 创意分析专家。即使无法直接看视频，也请根据给定信息输出可执行分析。",
        user_content,
    )


def _format_pipeline_tags(creative: Dict[str, Any]) -> str:
    t = creative.get("pipeline_tags")
    if isinstance(t, list) and t:
        return "、".join(str(x) for x in t if x)
    return "无"


def _build_prompt(item: Dict[str, Any], creative: Dict[str, Any], video_url: str) -> str:
    return f"""
以下是一条竞品 UA 素材：
- 分类/产品: {item.get('category', '')} / {item.get('product', '')}
- AppID: {item.get('appid', '')}
- 广告主: {creative.get('advertiser_name', '')}
- 平台: {creative.get('platform', '')}
- 标题: {creative.get('title', '') or '无'}
- 文案: {creative.get('body', '') or '无'}
- 视频时长: {creative.get('video_duration', 0)} 秒
- 视频链接: {video_url or '无'}
- 展示估值: {creative.get('all_exposure_value', 0)}
- 热度: {creative.get('heat', 0)}
- 人气值: {creative.get('impression', 0)}
- 素材标签（系统）: {_format_pipeline_tags(creative)}

请输出：
1) 广告创意拆解
2) Hook（前几秒抓人点）
3) 情感基调
4) 可复用观察（仅总结素材表现与创意机制，不输出 UA 投放建议）
""".strip()


def main() -> None:
    args = parse_args()
    in_path = Path(args.input)
    if not in_path.exists():
        raise FileNotFoundError(f"输入文件不存在：{in_path}")

    data = json.loads(in_path.read_text(encoding="utf-8"))
    items = data.get("items") or []
    if not isinstance(items, list):
        raise RuntimeError("输入 JSON 格式不正确：缺少 items 列表")

    if args.limit and args.limit > 0:
        items = items[: args.limit]

    results: List[Dict[str, Any]] = []
    total = len(items)
    for idx, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        creative = item.get("creative") or {}
        if not isinstance(creative, dict):
            continue
        ad_key = str(creative.get("ad_key") or "")
        video_url = _pick_video_url(creative)
        if not video_url:
            continue

        print(f"[{idx}/{total}] analyzing ad_key={ad_key[:12]}...")
        prompt = _build_prompt(item, creative, video_url)
        try:
            analysis = _call_llm_video(prompt, video_url)
        except Exception as e:
            analysis = f"[ERROR] {e}"

        results.append(
            {
                "category": item.get("category"),
                "product": item.get("product"),
                "appid": item.get("appid"),
                "ad_key": ad_key,
                "platform": creative.get("platform"),
                "video_duration": creative.get("video_duration"),
                "all_exposure_value": creative.get("all_exposure_value"),
                "heat": creative.get("heat"),
                "impression": creative.get("impression"),
                "video_url": video_url,
                "pipeline_tags": creative.get("pipeline_tags")
                if isinstance(creative.get("pipeline_tags"), list)
                else [],
                "analysis": analysis,
            }
        )

    if args.output:
        out_path = Path(args.output)
    else:
        out_path = DATA_DIR / f"video_analysis_{in_path.stem}.json"
    out_payload = {
        "input_file": str(in_path),
        "total_items": len(items),
        "analyzed_items": len(results),
        "results": results,
    }
    out_path.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"完成：{len(results)} 条，输出 {out_path}")


if __name__ == "__main__":
    main()

