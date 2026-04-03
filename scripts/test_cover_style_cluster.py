"""
实验：用与主流程一致的多模态链路（OPENROUTER_VIDEO_MODEL → 视觉回退 → 文本回退）
逐张读取广告「封面/缩略图」，抽取风格标签；再对当日批次做风格聚类（文本 LLM）。

用法示例：
  cd 项目根 && source .venv/bin/activate && PYTHONPATH=scripts \\
    python scripts/test_cover_style_cluster.py \\
    --input data/workflow_video_enhancer_2026-03-30_raw.json \\
    --appid app.getglam \\
    --output data/cover_style_cluster_test_2026-03-30_glam.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

from path_util import DATA_DIR, PROJECT_ROOT

load_dotenv(PROJECT_ROOT / ".env")

from analyze_video_from_raw_json import (  # noqa: E402
    _call_llm_image,
    _call_llm_text,
    _pick_image_url,
)


def _pick_cover_url(creative: Dict[str, Any]) -> str:
    """视频用 preview 作封面；纯图素材用 _pick_image_url。"""
    pu = str(creative.get("preview_img_url") or "").strip()
    if pu:
        return pu
    return str(_pick_image_url(creative) or "").strip()


def _strip_json_fence(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        t = re.sub(r"^```(?:json)?\s*", "", t, flags=re.IGNORECASE)
        t = re.sub(r"\s*```$", "", t)
    return t.strip()


def _parse_json_object(text: str) -> Optional[Dict[str, Any]]:
    t = _strip_json_fence(text)
    try:
        obj = json.loads(t)
        return obj if isinstance(obj, dict) else None
    except json.JSONDecodeError:
        m = re.search(r"\{[\s\S]*\}", t)
        if m:
            try:
                obj = json.loads(m.group(0))
                return obj if isinstance(obj, dict) else None
            except json.JSONDecodeError:
                pass
    return None


def _cover_style_user_prompt(item: Dict[str, Any], creative: Dict[str, Any]) -> str:
    return (
        "你正在看的是一条广告素材的**封面/缩略图**（不是整支视频）。\n"
        f"- 产品: {item.get('product', '')}\n"
        f"- 标题: {creative.get('title') or '无'}\n\n"
        "请只根据画面与版式，输出**仅一段 JSON 对象**（不要 markdown 代码块），字段如下：\n"
        '  "style_type": 字符串，用 4~12 个字概括封面视觉风格类型（如：黑白时尚大片风、高饱和促销贴片等）\n'
        '  "style_tags": 字符串数组，3~6 个极短中文标签\n'
        '  "one_line": 一句话中文概括该封面的视觉策略\n'
        "要求：简体中文；禁止输出除 JSON 以外的任何字符。"
    )


def _cluster_user_prompt(target_date: str, rows: List[Dict[str, Any]]) -> str:
    lines = []
    for r in rows:
        lines.append(
            json.dumps(
                {
                    "ad_key": r.get("ad_key"),
                    "appid": r.get("appid"),
                    "product": r.get("product"),
                    "style_type": (r.get("style_json") or {}).get("style_type"),
                    "style_tags": (r.get("style_json") or {}).get("style_tags"),
                    "one_line": (r.get("style_json") or {}).get("one_line"),
                },
                ensure_ascii=False,
            )
        )
    blob = "\n".join(lines)
    return (
        f"以下是同一自然日（{target_date}）内多条广告素材的「封面风格」结构化描述（每行一个 JSON）。\n"
        "请根据 style_type / style_tags / one_line 的语义，把**视觉风格属于同一类**的素材归为一簇（允许一簇只有 1 条）。\n"
        "不同 appid 也可以进同一簇，只要封面视觉策略一致。\n\n"
        f"{blob}\n\n"
        "输出**仅一段** JSON 对象（不要 markdown），格式：\n"
        '{\n'
        '  "clusters": [\n'
        '    { "cluster_id": 1, "label": "簇的简短中文名", "ad_keys": ["...", "..."] }\n'
        "  ],\n"
        '  "notes": "可选说明"\n'
        "}\n"
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="封面风格抽取 + 日内聚类（实验）")
    p.add_argument(
        "--input",
        default=str(DATA_DIR / "workflow_video_enhancer_2026-03-30_raw.json"),
        help="raw JSON 路径",
    )
    p.add_argument("--appid", default="", help="只保留该 appid（如 app.getglam），空=不过滤")
    p.add_argument("--limit", type=int, default=0, help="最多处理多少条（0=不限制）")
    p.add_argument("--output", default="", help="输出 JSON 路径")
    p.add_argument(
        "--skip-cluster",
        action="store_true",
        help="只做逐张风格抽取，跳过第二步聚类",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    in_path = Path(args.input)
    if not in_path.exists():
        raise FileNotFoundError(f"输入不存在：{in_path}")

    data = json.loads(in_path.read_text(encoding="utf-8"))
    items = data.get("items") or []
    target_date = str(data.get("target_date") or in_path.stem.split("_")[-1] or "").strip() or "unknown"

    vision_model = os.getenv("OPENROUTER_VIDEO_MODEL", "").strip() or os.getenv(
        "OPENROUTER_VISION_FALLBACK_MODEL", ""
    ).strip()
    print(
        f"[config] OPENROUTER_VIDEO_MODEL={os.getenv('OPENROUTER_VIDEO_MODEL', '')!r} "
        f"(封面走与 analyze_video_from_raw_json._call_llm_image 相同链路)"
    )

    filtered: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        if args.appid and str(it.get("appid") or "").strip() != args.appid.strip():
            continue
        filtered.append(it)
    if args.limit and args.limit > 0:
        filtered = filtered[: args.limit]

    per_item: List[Dict[str, Any]] = []
    for idx, item in enumerate(filtered, start=1):
        c = item.get("creative") or {}
        if not isinstance(c, dict):
            continue
        ad_key = str(c.get("ad_key") or "").strip()
        cover = _pick_cover_url(c)
        if not cover:
            print(f"[{idx}/{len(filtered)}] skip ad_key={ad_key[:12]} (no cover url)")
            per_item.append(
                {
                    "ad_key": ad_key,
                    "appid": item.get("appid"),
                    "product": item.get("product"),
                    "cover_url": "",
                    "error": "no_cover_url",
                    "style_json": None,
                    "raw_response": "",
                }
            )
            continue

        print(f"[{idx}/{len(filtered)}] cover ad_key={ad_key[:16]}... model_chain=~{vision_model!r}")
        user_prompt = _cover_style_user_prompt(item, c)
        try:
            raw = _call_llm_image(user_prompt, cover)
        except Exception as e:
            raw = f"[ERROR] {e}"
            print(f"  -> ERROR {e}")

        sj = _parse_json_object(raw) if raw and not str(raw).startswith("[ERROR]") else None
        if not sj:
            per_item.append(
                {
                    "ad_key": ad_key,
                    "appid": item.get("appid"),
                    "product": item.get("product"),
                    "cover_url": cover[:200],
                    "error": "parse_failed_or_llm_error",
                    "style_json": sj,
                    "raw_response": raw[:4000] if isinstance(raw, str) else str(raw),
                }
            )
        else:
            per_item.append(
                {
                    "ad_key": ad_key,
                    "appid": item.get("appid"),
                    "product": item.get("product"),
                    "cover_url": cover[:200],
                    "error": "",
                    "style_json": sj,
                    "raw_response": raw[:2000] if isinstance(raw, str) else "",
                }
            )

    cluster_out: Optional[Dict[str, Any]] = None
    cluster_raw = ""
    ok_rows = [x for x in per_item if x.get("style_json")]
    if not args.skip_cluster and ok_rows:
        print(f"[cluster] 文本聚类，输入 {len(ok_rows)} 条（_call_llm_text）...")
        cluster_prompt = _cluster_user_prompt(target_date, ok_rows)
        cluster_raw = _call_llm_text(
            "你是素材策略与视觉归类助手，只输出合法 JSON，不要多余解释。",
            cluster_prompt,
        )
        cluster_out = _parse_json_object(cluster_raw)
        if cluster_out is None:
            cluster_out = {"parse_error": True, "raw": cluster_raw[:8000]}

    out_path = Path(args.output) if args.output else DATA_DIR / f"cover_style_cluster_test_{target_date}.json"
    out_payload = {
        "target_date": target_date,
        "input_file": str(in_path),
        "filter_appid": args.appid or None,
        "vision_model_env": os.getenv("OPENROUTER_VIDEO_MODEL", ""),
        "per_item": per_item,
        "cluster": cluster_out,
        "cluster_raw_response": cluster_raw[:12000] if cluster_raw else "",
    }
    out_path.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"完成，写入 {out_path}")


if __name__ == "__main__":
    main()
