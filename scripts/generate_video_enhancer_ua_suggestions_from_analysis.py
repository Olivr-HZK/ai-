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
import re
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv

from path_util import CONFIG_DIR, DATA_DIR

load_dotenv()

import llm_client  # noqa: E402
from video_enhancer_pipeline_db import (  # noqa: E402
    compute_trend_signals,
    load_recent_direction_cards,
)


def _target_date_from_analysis_path(in_path: Path) -> str:
    m = re.search(r"workflow_video_enhancer_(\d{4}-\d{2}-\d{2})", in_path.name)
    return m.group(1) if m else ""


def _flush_generate_usage(in_path: Path) -> None:
    ud = _target_date_from_analysis_path(in_path)
    if ud:
        llm_client.flush_usage(ud)


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
    return llm_client.call_text(system, user_content, models=llm_client.resolve_cluster_models())


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


def _build_prompt(
    results: List[Dict[str, Any]],
    products: List[Dict[str, str]],
    *,
    target_date: str = "",
) -> str:
    source_by_adkey: Dict[str, Dict[str, Any]] = {}
    for r in results:
        if not isinstance(r, dict):
            continue
        ad_key = str(r.get("ad_key") or "").strip()
        if not ad_key:
            continue
        vu0 = str(r.get("video_url") or "").strip()
        iu0 = str(r.get("image_url") or "").strip()
        ct = r.get("creative_type") or ("image" if (not vu0 and iu0) else "video")
        entry: Dict[str, Any] = {
            "product": r.get("product", ""),
            "platform": r.get("platform", ""),
            "creative_type": ct,
            "video_duration": r.get("video_duration", 0),
            "all_exposure_value": r.get("all_exposure_value", 0),
            "heat": r.get("heat", 0),
            "impression": r.get("impression", 0),
            "analysis": r.get("analysis", ""),
            "video_url": str(r.get("video_url") or "").strip(),
            "image_url": str(r.get("image_url") or "").strip(),
        }
        prev = str(r.get("preview_img_url") or "").strip()
        if prev:
            entry["preview_img_url"] = prev
        source_by_adkey[ad_key] = entry
    insight_text = json.dumps(source_by_adkey, ensure_ascii=False, indent=2)

    pm_lines = []
    for p in products:
        pm_lines.append(
            f"- {p.get('internal_name') or p.get('name')} | 分类:{p.get('category')} | 描述:{p.get('desc')}"
        )
    pm_text = "\n".join(pm_lines) if pm_lines else "（未提供产品手册约束）"

    # ---- 历史方向卡片上下文 ----
    hist_section = ""
    if target_date:
        try:
            hist_cards = load_recent_direction_cards(target_date, n_days=3)
        except Exception:
            hist_cards = []
        if hist_cards:
            h_lines = []
            for hc in hist_cards:
                h_lines.append(f"- [{hc['target_date']}] {hc['direction_name']}：{hc['background'][:80]}")
            hist_section = (
                "\n## 近期已推送的方向卡片（最近 3 天）\n"
                + "\n".join(h_lines)
                + "\n若某方向与上述历史高度重叠，请标注「持续推荐」并说明今日新增洞察；优先输出与历史不同的新方向。\n"
            )

    # ---- 趋势信号 ----
    trend_section = ""
    if target_date:
        try:
            trend = compute_trend_signals(target_date)
        except Exception:
            trend = {}
        pp = trend.get("per_product") or {}
        ov = trend.get("overall") or {}
        if pp:
            t_lines = []
            for prod, info in sorted(pp.items()):
                tw = info.get("this_week_new", 0)
                pw = info.get("prev_week_new", 0)
                tr = info.get("trend", "stable")
                arrow = {"rising": "↑", "declining": "↓"}.get(tr, "→")
                t_lines.append(f"  - {prod}: 本周新素材 {tw} 条 vs 上周 {pw} 条 {arrow}")
            trend_section = (
                "\n## 竞品素材趋势信号\n"
                + "\n".join(t_lines)
                + f"\n  整体趋势：{ov.get('trend', 'stable')}"
                + "\n请在方向卡片中适当引用趋势走向（如「该类素材本周明显增多/减少」），帮助 UA 团队把握节奏。\n"
            )

    return f"""
下面是竞品素材灵感分析（JSON，key 为 ad_key）：
{insight_text}

下面是我方 video enhancer 相关产品信息（用于约束建议）：
{pm_text}
{hist_section}{trend_section}
请基于以上信息，输出「方向卡片」格式的统一 UA 建议。
要求：
0) 语言：JSON 内所有文案字段（方向名称、背景、UA建议、产品对标点、风险提示、共性执行建议）仅使用汉字、英文字母与常规标点数字；不得出现阿拉伯文、泰文、西里尔文等非中英文字符。若上游「analysis」中含此类文字，请改写为中文表述（可保留必要英文产品名/功能词），勿照抄原文。
1) 方向卡片最多 3 个（1~3 个均可）。默认优先输出 3 个主方向；可少不可硬凑。
   - 若两个方向在核心受众/创意机制/转化动作上差异不足，请合并；若差异显著，请分开，避免牵强归并。
2) 不要按单个我方产品拆开写，按产品线通用策略写。
3) 内容要精简，严格控制字数（避免大段冗长）：
   - 背景：80~160字
   - UA建议：120~220字
   - 产品对标点：80~160字
   - 风险提示：<=80字；必须结合各条素材「分析」正文中已提到的画面/文案信息，点明该方向是否存在露肤、性暗示、擦边博眼球等合规隐患；若有，写清审核与定向上的注意点；若无或整体低风险，可写「常规注意各平台素材政策」；不得鼓励制作违规或低俗素材
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
   - 每个方向严格最多 5 个 ad_key（不得超过）；不足可留空。
   - 对不属于这 1~3 个主方向的零散素材可以不纳入参考链接，不需要强制覆盖全部素材。
   - 可在主方向内混合视频与图片素材；不要求为凑覆盖而硬塞不相关素材。
6) 文风：以中文为主、可自然夹杂英文术语；与第 0) 条语言约束同时满足。
""".strip()


def _to_markdown(data: Dict[str, Any]) -> str:
    md: List[str] = ["# Video Enhancer 方向卡片（精简版）", ""]
    cards = data.get("方向卡片") or []
    if isinstance(cards, list):
        for card in cards:
            if not isinstance(card, dict):
                continue
            direction = str(card.get("方向名称") or "未命名方向")
            ua_text = card.get("UA建议", "")
            if not ua_text:
                ua_text = card.get("UA 建议", "")
            md.append(f"## [video enhancer 方向] {direction}")
            md.append(f"🎬 背景：{card.get('背景', '')}")
            md.append(f"🎯 UA建议：{ua_text}")
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


def _normalize_suggestion_fields(suggestion: Dict[str, Any]) -> Dict[str, Any]:
    """
    兼容模型偶发的字段变体（如“UA 建议”）。
    统一落盘为规范键名，避免下游渲染丢字段。
    """
    if not isinstance(suggestion, dict):
        return suggestion
    cards = suggestion.get("方向卡片")
    if not isinstance(cards, list):
        return suggestion
    for card in cards:
        if not isinstance(card, dict):
            continue
        if (not card.get("UA建议")) and card.get("UA 建议"):
            card["UA建议"] = card.get("UA 建议")
    return suggestion


def _extract_json_block(text: str) -> str:
    """
    尝试从模型输出中抽取最外层 JSON 块（兼容前后夹带解释文本）。
    """
    t = (text or "").strip()
    if not t:
        return t
    start = t.find("{")
    end = t.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return t
    return t[start : end + 1]


def _repair_unescaped_quotes_in_json(raw: str) -> str:
    """
    修复常见坏格式：JSON 字符串值中出现未转义的英文双引号。
    仅在「字符串内部」将可疑双引号转义，不改结构性引号。
    """
    if not raw:
        return raw

    out: List[str] = []
    i = 0
    n = len(raw)
    in_string = False
    escape = False
    while i < n:
        ch = raw[i]
        if not in_string:
            out.append(ch)
            if ch == '"':
                in_string = True
            i += 1
            continue

        # in_string == True
        if escape:
            out.append(ch)
            escape = False
            i += 1
            continue

        if ch == "\\":
            out.append(ch)
            escape = True
            i += 1
            continue

        if ch == '"':
            # 看看后续是否像字符串结束（后面跟逗号/右括号/右中括号/换行+这些）
            j = i + 1
            while j < n and raw[j] in " \t\r\n":
                j += 1
            if j < n and raw[j] in [",", "}", "]", ":"]:
                out.append(ch)  # 结构性结束引号
                in_string = False
            else:
                out.append('\\"')  # 视作值内引号，转义
            i += 1
            continue

        out.append(ch)
        i += 1

    return "".join(out)


def _parse_json_with_repair(model_output: str) -> Dict[str, Any]:
    """
    LLM 输出 JSON 解析兜底：
    1) 直接 parse
    2) 抽取最外层 JSON 块
    3) 修复未转义双引号后再 parse
    """
    cleaned = "\n".join([ln for ln in (model_output or "").splitlines() if not ln.strip().startswith("```")]).strip()
    if not cleaned:
        raise ValueError("empty output")

    try:
        return json.loads(cleaned)
    except Exception:
        pass

    block = _extract_json_block(cleaned)
    try:
        return json.loads(block)
    except Exception:
        pass

    repaired = _repair_unescaped_quotes_in_json(block)
    # 再做一层轻量清洗：去掉末尾多余逗号
    repaired = re.sub(r",(\s*[}\]])", r"\1", repaired)
    return json.loads(repaired)


def main() -> None:
    args = parse_args()
    in_path = Path(args.input)
    if not in_path.exists():
        raise FileNotFoundError(f"输入文件不存在：{in_path}")

    raw = json.loads(in_path.read_text(encoding="utf-8"))
    results_all = raw.get("results") or []
    if not isinstance(results_all, list):
        results_all = []
    results = [
        r
        for r in results_all
        if isinstance(r, dict) and not r.get("exclude_from_cluster")
    ]
    cluster_excluded = (
        len([r for r in results_all if isinstance(r, dict) and r.get("exclude_from_cluster")])
        if results_all
        else 0
    )

    out_json = Path(args.output_json)
    out_md = Path(args.output_md)

    if not results:
        print(
            "警告：输入分析结果为空（results 为空或全部 exclude_from_cluster），跳过 LLM，输出占位文件。"
            + (f" 本批聚类排除 {cluster_excluded} 条（我方已投套路）。" if cluster_excluded else "")
        )
        suggestion: Dict[str, Any] = {
            "方向卡片": [],
            "共性执行建议": [
                "今日无命中素材或分析结果为空，无法生成方向卡片；请检查抓取日期筛选（UTC+8）与广告主过滤。"
            ],
        }
        payload = {
            "input_file": str(in_path),
            "source_count": 0,
            "cluster_excluded_count": cluster_excluded,
            "products_context_count": len(_load_video_enhancer_products()),
            "suggestion": suggestion,
            "skipped_llm": True,
        }
        out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        out_md.write_text(_to_markdown(suggestion), encoding="utf-8")
        print(f"完成（空结果占位）：\n- {out_json}\n- {out_md}")
        return

    products = _load_video_enhancer_products()
    td = _target_date_from_analysis_path(in_path) or ""
    system = (
        "你是资深 UA 策略负责人。"
        "你输出的内容必须短、准、可执行，严格按用户给定JSON结构与字数约束返回。"
        "方向卡片中的「风险提示」须如实反映素材分析里体现的合规尺度（露肤/擦边/性暗示等），优先服务审核与品牌安全决策。"
        "所有可见文案仅允许中文与英文：遇多语言素材时意译为中文表述，禁止输出阿拉伯文等非中英字符。"
    )
    prompt = _build_prompt(results, products, target_date=td)
    if cluster_excluded:
        print(
            f"[cluster] 方向卡片输入已排除「我方已投套路」素材 {cluster_excluded} 条，"
            f"实际参与聚类 {len(results)} 条。"
        )

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
            "cluster_excluded_count": cluster_excluded,
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

    try:
        suggestion = _parse_json_with_repair(out)
    except Exception as e:
        _flush_generate_usage(in_path)
        payload = {
            "input_file": str(in_path),
            "source_count": len(results),
            "cluster_excluded_count": cluster_excluded,
            "products_context_count": len(products),
            "suggestion": empty_suggestion,
            "llm_error": "invalid_json_output",
            "llm_error_detail": str(e),
            "raw_output": out,
            "skipped_llm": True,
        }
        out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        out_md.write_text(_to_markdown(empty_suggestion), encoding="utf-8")
        print("警告：LLM 返回非 JSON，已写空结果。")
        print(f"完成：已输出\n- {out_json}\n- {out_md}")
        return

    # 后处理：强制限制方向数与每方向参考链接数量，避免模型超出字段/格式限制
    if isinstance(suggestion, dict):
        suggestion = _normalize_suggestion_fields(suggestion)
        cards = suggestion.get("方向卡片")
        if isinstance(cards, list):
            # 最多 3 个方向
            suggestion["方向卡片"] = cards[:3]
            for c in suggestion["方向卡片"]:
                if not isinstance(c, dict):
                    continue
                links = c.get("参考链接")
                if isinstance(links, list):
                    c["参考链接"] = links[:5]

    payload = {
        "input_file": str(in_path),
        "source_count": len(results),
        "cluster_excluded_count": cluster_excluded,
        "products_context_count": len(products),
        "suggestion": suggestion,
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_md.write_text(_to_markdown(suggestion), encoding="utf-8")

    _flush_generate_usage(in_path)
    print(f"完成：已输出\n- {out_json}\n- {out_md}")


if __name__ == "__main__":
    main()

