"""
根据已生成的灵感分析结果，产出「我方 video enhancer 产品线」统一 UA 建议。
不针对单个产品，输出“方向卡片”风格（精简版）。

默认输入：
  data/video_analysis_test_video_enhancer_2_2026-03-18_raw.json

输出：
  data/video_enhancer_ua_suggestion_from_analysis.json
  data/video_enhancer_ua_suggestion_from_analysis.md
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv
from openai import OpenAI

from path_util import CONFIG_DIR, DATA_DIR

load_dotenv()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="从灵感分析结果生成统一 UA 建议（video enhancer 产品线）")
    p.add_argument(
        "--input",
        default=str(DATA_DIR / "video_analysis_test_video_enhancer_2_2026-03-18_raw.json"),
        help="输入分析结果 JSON 文件路径",
    )
    p.add_argument(
        "--output-json",
        default=str(DATA_DIR / "video_enhancer_ua_suggestion_from_analysis.json"),
        help="输出 JSON 路径",
    )
    p.add_argument(
        "--output-md",
        default=str(DATA_DIR / "video_enhancer_ua_suggestion_from_analysis.md"),
        help="输出 Markdown 路径",
    )
    return p.parse_args()


def _call_llm(system: str, user_content: str) -> str:
    """
    只走 OpenRouter，避免直连 OpenAI 在本地区域受限导致 403。
    若未配置 OPENROUTER_API_KEY，则直接报错提示。
    """
    or_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    if not or_key:
        raise RuntimeError("未配置 OPENROUTER_API_KEY，无法生成 UA 建议（请在 .env 中填入 OpenRouter Key）")

    client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=or_key)
    model = os.getenv("OPENROUTER_MODEL", "google/gemini-2.5-flash")
    r = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user_content},
        ],
    )
    return (r.choices[0].message.content or "").strip()


def _load_video_enhancer_products() -> List[Dict[str, str]]:
    """
    从产品手册中提取我方 video enhancer 相关产品，作为建议约束上下文。
    规则：
    - 分类包含「图像」或「视频」
    - 名称/内部名称/描述中包含 enhancer、retake、video、photo 等关键词
    """
    csv_path = CONFIG_DIR / "产品手册_AI工具类_表格 2.csv"
    if not csv_path.exists():
        return []

    keywords = ("enhancer", "retake", "video", "photo", "image", "修复", "重拍", "图像", "视频")
    out: List[Dict[str, str]] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("名称") or "").strip()
            internal = (row.get("内部名称") or "").strip()
            category = (row.get("分类") or "").strip()
            desc = (row.get("产品描述") or "").strip()
            text = f"{name} {internal} {category} {desc}".lower()
            if ("图像" in category or "视频" in category) and any(k in text for k in keywords):
                out.append(
                    {
                        "name": name,
                        "internal_name": internal,
                        "category": category,
                        "desc": desc,
                    }
                )
    return out


def _build_prompt(results: List[Dict[str, Any]], products: List[Dict[str, str]]) -> str:
    source_by_adkey: Dict[str, Dict[str, Any]] = {}
    for r in results:
        if not isinstance(r, dict):
            continue
        ad_key = str(r.get("ad_key") or "").strip()
        if not ad_key:
            continue
        source_by_adkey[ad_key] = {
            "product": r.get("product", ""),
            "platform": r.get("platform", ""),
            "video_duration": r.get("video_duration", 0),
            "all_exposure_value": r.get("all_exposure_value", 0),
            "heat": r.get("heat", 0),
            "impression": r.get("impression", 0),
            "video_url": r.get("video_url", ""),
            "analysis": r.get("analysis", ""),
        }
    insight_text = json.dumps(source_by_adkey, ensure_ascii=False, indent=2)

    pm_lines = []
    for p in products:
        pm_lines.append(
            f"- {p.get('internal_name') or p.get('name')} | 分类:{p.get('category')} | 描述:{p.get('desc')}"
        )
    pm_text = "\n".join(pm_lines) if pm_lines else "（未提供产品手册约束）"

    return f"""
下面是竞品素材灵感分析（JSON，key 为 ad_key）：
{insight_text}

下面是我方 video enhancer 相关产品信息（用于约束建议）：
{pm_text}

请基于以上信息，输出「方向卡片」格式的统一 UA 建议。
要求：
1) 方向卡片数量不设上限，由你根据素材自动归类，确保分类充分且不过度合并。
2) 不要按单个我方产品拆开写，按产品线通用策略写。
3) 内容要精简，严格控制字数（避免大段冗长）：
   - 背景：80~160字
   - UA建议：120~220字
   - 产品对标点：80~160字
   - 风险提示：<=80字
4) 输出 JSON，结构必须如下：
{{
  "方向卡片": [
    {{
      "方向名称": "",
      "背景": "",
      "UA建议": "",
      "产品对标点": "",
      "风险提示": "",
      "参考链接": ["", "", "", "", ""]
    }}
  ],
  "共性执行建议": ["", ""]
}}
5) “参考链接”必须严格来自上文素材：
   - 只能填写上文 JSON 中出现过的 ad_key；
   - 不允许填写 URL，不允许编造任何新 ad_key；
   - 每个方向建议 1~5 个 ad_key，不足可留空。
""".strip()


def _to_markdown(data: Dict[str, Any]) -> str:
    md: List[str] = ["# Video Enhancer 方向卡片（精简版）", ""]
    cards = data.get("方向卡片") or []
    if isinstance(cards, list):
        for card in cards:
            if not isinstance(card, dict):
                continue
            direction = str(card.get("方向名称") or "未命名方向")
            md.append(f"## [video enhancer 方向] {direction}")
            md.append(f"🎬 背景：{card.get('背景', '')}")
            md.append(f"🎯 UA建议：{card.get('UA建议', '')}")
            md.append(f"🧩 产品对标点：{card.get('产品对标点', '')}")
            md.append(f"⚠️ 风险提示：{card.get('风险提示', '')}")
            links = card.get("参考链接") or []
            if isinstance(links, list) and links:
                md.append(f"🔗 参考链接：{'；'.join([str(x) for x in links if x])}")
            else:
                md.append("🔗 参考链接：")
            md.append("")

    common = data.get("共性执行建议") or []
    md.append("## 共性执行建议")
    if isinstance(common, list):
        for x in common:
            md.append(f"- {x}")
    else:
        md.append(f"- {common}")
    md.append("")
    return "\n".join(md)


def main() -> None:
    args = parse_args()
    in_path = Path(args.input)
    if not in_path.exists():
        raise FileNotFoundError(f"输入文件不存在：{in_path}")

    raw = json.loads(in_path.read_text(encoding="utf-8"))
    results = raw.get("results") or []
    if not isinstance(results, list):
        results = []

    out_json = Path(args.output_json)
    out_md = Path(args.output_md)

    if not results:
        print("警告：输入分析结果为空（results 为空），跳过 LLM，输出占位文件。")
        suggestion: Dict[str, Any] = {
            "方向卡片": [],
            "共性执行建议": [
                "今日无命中素材或分析结果为空，无法生成方向卡片；请检查抓取日期筛选（UTC+8）与广告主过滤。"
            ],
        }
        payload = {
            "input_file": str(in_path),
            "source_count": 0,
            "products_context_count": len(_load_video_enhancer_products()),
            "suggestion": suggestion,
            "skipped_llm": True,
        }
        out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        out_md.write_text(_to_markdown(suggestion), encoding="utf-8")
        print(f"完成（空结果占位）：\n- {out_json}\n- {out_md}")
        return

    products = _load_video_enhancer_products()
    system = (
        "你是资深 UA 策略负责人。"
        "你输出的内容必须短、准、可执行，严格按用户给定JSON结构与字数约束返回。"
    )
    prompt = _build_prompt(results, products)

    empty_suggestion: Dict[str, Any] = {
        "方向卡片": [],
        "共性执行建议": [],
    }

    try:
        out = _call_llm(system, prompt)
    except Exception as e:
        payload = {
            "input_file": str(in_path),
            "source_count": len(results),
            "products_context_count": len(products),
            "suggestion": empty_suggestion,
            "llm_error": str(e),
            "skipped_llm": True,
        }
        out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        out_md.write_text(_to_markdown(empty_suggestion), encoding="utf-8")
        print(f"警告：LLM 调用失败，已写空结果。error={e}")
        print(f"完成：已输出\n- {out_json}\n- {out_md}")
        return

    # 清理 markdown 代码块
    cleaned = "\n".join([ln for ln in out.splitlines() if not ln.strip().startswith("```")]).strip()
    try:
        suggestion = json.loads(cleaned)
    except Exception:
        payload = {
            "input_file": str(in_path),
            "source_count": len(results),
            "products_context_count": len(products),
            "suggestion": empty_suggestion,
            "llm_error": "invalid_json_output",
            "raw_output": out,
            "skipped_llm": True,
        }
        out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        out_md.write_text(_to_markdown(empty_suggestion), encoding="utf-8")
        print("警告：LLM 返回非 JSON，已写空结果。")
        print(f"完成：已输出\n- {out_json}\n- {out_md}")
        return

    payload = {
        "input_file": str(in_path),
        "source_count": len(results),
        "products_context_count": len(products),
        "suggestion": suggestion,
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_md.write_text(_to_markdown(suggestion), encoding="utf-8")

    print(f"完成：已输出\n- {out_json}\n- {out_md}")


if __name__ == "__main__":
    main()

