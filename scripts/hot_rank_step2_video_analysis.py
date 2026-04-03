"""
热门榜工作流 · 第 2 步：多模态视频分析（Gemini）

目标：
- 从第 1 步写入的 competitor_hot_creatives_daily 中，找出还有视频、但尚未做「视频内容分析」的素材；
- 调用 Gemini 多模态模型（通过 OpenRouter，推荐 gemini-3-pro，当下可先用 2.5 系列占位），
  对每条素材的视频链接做深入解析（以 UA 视角），生成结构化文本；
- 结果仅按「ad_key → video_analysis」的映射写入 competitor_hot_video_analysis 表。

注意：
- 本脚本**不做聚类**，只生成单条素材的视频分析结果；
- 聚类 + 深度解析在第 3 步单独脚本中完成，基于本表的 video_analysis 文本及其他标签。

用法（项目根目录，已配置 OPENROUTER_API_KEY 或 Gemini 官方 API）：

  source .venv/bin/activate
  python scripts/hot_rank_step2_video_analysis.py --date 2026-03-13

参数：
  --date YYYY-MM-DD   仅分析指定 crawl_date 的素材（推荐与第 1 步保持一致）；
  --limit N           最多分析 N 条素材（默认 50，避免一次调用过多）。
"""

from __future__ import annotations

import argparse
import os
from typing import Any, Dict, List

from dotenv import load_dotenv

from competitor_hot_db import fetch_videos_without_analysis, upsert_video_analysis
from ua_crawl_db import merge_llm_usage_daily  # 复用文本 usage 统计表（合并写入，不覆盖同日其它业务）

load_dotenv()

_VIDEO_USAGE: dict[str, dict[str, int]] = {}


def _accumulate_video_usage(model: str, usage) -> None:
    """
    累加视频模型的 usage（按 model 维度），写入 ai_llm_usage_daily 时用 key: "openrouter-video:<model>"。
    """
    if usage is None:
        return
    try:
        prompt = int(getattr(usage, "prompt_tokens", 0) or 0)
        completion = int(getattr(usage, "completion_tokens", 0) or 0)
        total = int(getattr(usage, "total_tokens", prompt + completion) or 0)
    except Exception:
        return
    key = model
    stat = _VIDEO_USAGE.setdefault(
        key,
        {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    )
    stat["prompt_tokens"] += prompt
    stat["completion_tokens"] += completion
    stat["total_tokens"] += total


def _call_gemini_video(system: str, user_content: str) -> str:
    """
    通过 OpenRouter 调用 Gemini 多模态模型（文本接口占位）：
    - 默认模型可通过环境变量 OPENROUTER_VIDEO_MODEL 配置；
    - 推荐设置为 gemini 3 Pro 正式可用的模型名，例如：
      OPENROUTER_VIDEO_MODEL=google/gemini-3.0-pro
    在模型暂未正式发布前，可先用 2.5 系列替代：
      google/gemini-2.5-pro-exp 或 google/gemini-2.5-flash
    """
    from openai import OpenAI

    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError(
            "请在 .env 中配置 OPENROUTER_API_KEY，用于调用 Gemini 多模态模型。"
        )

    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
    )
    model = os.getenv("OPENROUTER_VIDEO_MODEL", "google/gemini-2.5-pro-exp")

    r = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user_content},
        ],
    )
    try:
        _accumulate_video_usage(model, getattr(r, "usage", None))
    except Exception:
        pass
    return (r.choices[0].message.content or "").strip()


def build_video_prompt(
    ad_key: str,
    category: str,
    product: str,
    video_url: str,
) -> str:
    """
    构造单条素材的视频分析 Prompt。
    由于我们目前只传入视频 URL，模型是否真正加载视频取决于后端实现；
    Prompt 中会明确要求从「视频内容 + 标题/文案（后续可补）」角度进行 UA 向分析。
    """
    return f"""
你是服务 Guru 的 UA 团队的视频解析顾问和产品对标分析师。

现在有一条竞品 UA 视频素材，来源于广大大 Top 创意：Top1% 榜单。

基础信息：
- 分类（category）: {category}
- 竞品名称（product）: {product}
- ad_key: {ad_key}
- 视频 URL: {video_url}

请你仅基于视频内容本身（画面、镜头语言、动作、节奏、构图、转场等），从 UA 视角进行深入解析。

输出要求：
1. 使用简体中文，结构分为 4 个部分，以 Markdown 二级标题 `##` 分隔：

## 1. 画面与剧情概述
- 用 3–6 句概括视频里发生了什么：人物/角色是谁、在做什么、出现了哪些关键场景或转场。
- 指出视频的主叙事线（例如“自拍变装”“老照片修复前后对比”“AI 聊天机器人陪伴情绪”等）。

## 2. 玩法与交互方式
- 从“用户视角”说明这条视频体现的是怎样的体验路径：
  - 用户大概需要上传几张什么类型的图，或提供什么输入（文字、语音等）；
  - 系统如何把这些输入变成现在视频中看到的效果。
- 强调这是可以在 1–2 张图、简短操作下完成的 UA 玩法。

## 3. 视觉钩子与情绪节奏
- 分析前 3 秒的视觉钩子（第一幕是什么、有什么强对比或好奇点）。
- 指出 1–2 个主要情绪节奏变化（例如：平静→惊喜、怀旧→感动、焦虑→安心等）。
- 指出 1–2 个关键镜头或转场（例如“大特写→远景”“静态图→动态人像”等）为什么对转化有效。

## 4. 可复用的 UA 素材建议
- 针对这条视频，总结 2–3 条可以直接复用到 Guru 产品 UA 素材中的建议：
  - 推荐的视频结构（多少秒、几幕、每一幕大致画面/文案）。
  - 对“用户上传的图片/输入”的具体建议。
  - 对收尾 CTA 的具体建议（文字 + 画面）。

2. 不要输出其他额外说明，不要解释你作为模型的限制。
3. 如果无法访问视频 URL，请根据你能推断到的一般 UA 视频套路，给出一个合理的、可执行的通用分析（但不要编造具体品牌/人物信息）。
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="热门榜工作流 · 第 2 步：对有视频但未分析的素材使用 Gemini 多模态模型生成视频分析。"
    )
    p.add_argument(
        "--date",
        type=str,
        default=None,
        help="仅分析指定 crawl_date 的素材（格式 YYYY-MM-DD）。不传则不限日期，仅按 limit 截断。",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=50,
        help="本次最多分析多少条素材（默认 50）。",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    pending = fetch_videos_without_analysis(
        crawl_date=args.date,
        limit=max(1, args.limit),
    )
    if not pending:
        scope = f"crawl_date={args.date}" if args.date else "全部日期"
        print(f"[info] 在 {scope} 中未找到需要视频分析的素材。")
        return

    scope = f"crawl_date={args.date}" if args.date else "最近若干条"
    print(f"[1/2] 在 {scope} 中找到 {len(pending)} 条待分析的视频素材。")

    sys_msg = (
        "你是资深 UA 视频创意分析专家，擅长从视频内容中拆解玩法与转化逻辑。"
        "请严格按照用户给出的结构化输出要求，用简体中文给出可执行的建议。"
    )

    for i, item in enumerate(pending, 1):
        ad_key = item["ad_key"]
        cat = item["category"]
        prod = item["product"]
        video_url = item["video_url"]
        crawl_date = item["crawl_date"]

        print(f"[2/2] ({i}/{len(pending)}) ad_key={ad_key[:12]}..., category={cat}, product={prod}")
        try:
            prompt = build_video_prompt(
                ad_key=ad_key,
                category=cat,
                product=prod,
                video_url=video_url,
            )
            analysis = _call_gemini_video(sys_msg, prompt)
            upsert_video_analysis(
                ad_key=ad_key,
                crawl_date=crawl_date,
                video_url=video_url,
                video_analysis=analysis,
            )
            print("  ✓ 已写入视频分析（competitor_hot_video_analysis）")
        except Exception as e:
            print(f"  ✗ 视频分析失败：{e}")

    # 写入视频模型 usage 统计（按日期维度），与文本 usage 共用一张表
    if _VIDEO_USAGE:
        from datetime import date as _date
        import json as _json

        usage_date = args.date or _date.today().isoformat()
        usage_for_db: dict[str, dict[str, int]] = {}
        print("\n视频模型 LLM usage 统计：")
        for model, stat in _VIDEO_USAGE.items():
            key = f"openrouter-video:{model}"
            usage_for_db[key] = {
                "prompt_tokens": stat.get("prompt_tokens", 0),
                "completion_tokens": stat.get("completion_tokens", 0),
                "total_tokens": stat.get("total_tokens", 0),
            }
            print(
                f"  - {key}: "
                f"prompt={stat.get('prompt_tokens', 0)}, "
                f"completion={stat.get('completion_tokens', 0)}, "
                f"total={stat.get('total_tokens', 0)}"
            )
        try:
            merge_llm_usage_daily(usage_date, usage_for_db)
        except Exception as e:
            print(f"（忽略）写入视频 LLM usage 统计失败: {e}")

    print("完成（第 2 步：多模态视频分析）。")


if __name__ == "__main__":
    main()

