"""
基于指定 raw JSON 做视频/图片创意分析（默认不写库，仅输出 JSON）。

若输入 JSON 含 target_date（工作流 pending 文件），默认每成功分析一条即写入
data/video_enhancer_pipeline.db 的 daily_creative_insights；可用 --no-db 或环境变量
VIDEO_ENHANCER_ANALYSIS_NO_DB=1 关闭。

默认在**同一次**多模态灵感分析中要求模型输出 JSON：`analysis` 正文 + `flower_background` / `bw_blockbuster`（花卉背景、黑白大片套路）；
命中任一则打标「我方已经投过」并设置 exclude_from_bitable / exclude_from_cluster，且跳过单条 UA 建议。
环境变量 VIDEO_ANALYSIS_STYLE_FILTER_DISABLED=1 可关闭套路字段与 JSON 输出（恢复纯文本分析）。

并发：环境变量 VIDEO_ANALYSIS_WORKERS（默认 3）或命令行 --workers，多条素材并行调用多模态（缩短总墙钟时间；注意 API 限流）。

默认输入：
  data/test_video_enhancer_2_2026-03-18_raw.json

输出：
  data/video_analysis_test_video_enhancer_2_2026-03-18_raw.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

from path_util import CONFIG_DIR, DATA_DIR
from video_enhancer_pipeline_db import upsert_single_daily_creative_insight

load_dotenv()

import llm_client  # noqa: E402  — 统一 LLM 调用层

_DB_WRITE_LOCK = threading.Lock()

# ---------------------------------------------------------------------------
# 配置化套路过滤（从 config/style_filters.json 读取，支持热更新）
# ---------------------------------------------------------------------------
_style_filters_cache: List[Dict[str, Any]] | None = None


def _load_style_filters() -> List[Dict[str, Any]]:
    """加载启用的套路过滤规则；配置文件不存在时使用内置默认值。"""
    global _style_filters_cache
    if _style_filters_cache is not None:
        return _style_filters_cache
    path = CONFIG_DIR / "style_filters.json"
    if path.exists():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, list):
                _style_filters_cache = [f for f in raw if isinstance(f, dict) and f.get("enabled", True)]
                return _style_filters_cache
        except Exception as e:
            print(f"[WARN] style_filters.json 解析失败，使用内置默认: {e}")
    _style_filters_cache = [
        {"id": "flower_background", "label": "花卉背景",
         "description": "主体或背景为大面积花卉/花墙/花丛/植物棚等装饰性花卉场景（非风景里偶然一朵花）"},
        {"id": "bw_blockbuster", "label": "黑白大片",
         "description": "整体以黑白灰影调为主、偏电影感/大片质感（非偶尔单色滤镜）"},
    ]
    return _style_filters_cache


def _style_filter_ids() -> List[str]:
    return [f["id"] for f in _load_style_filters()]


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
    p.add_argument(
        "--no-db",
        action="store_true",
        help="关闭逐条入库（即使输入含 target_date）。也可用环境变量 VIDEO_ENHANCER_ANALYSIS_NO_DB=1。",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=0,
        help="并行分析条数（0=读环境变量 VIDEO_ANALYSIS_WORKERS，默认 3；设为 1 则完全串行）",
    )
    return p.parse_args()


def _resolve_analysis_workers(args: argparse.Namespace) -> int:
    w = int(getattr(args, "workers", 0) or 0)
    if w > 0:
        return max(1, w)
    raw = os.getenv("VIDEO_ANALYSIS_WORKERS", "3").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 3
    return max(1, n)


def _should_incremental_db(args: argparse.Namespace, target_date: str) -> bool:
    if not (target_date or "").strip():
        return False
    if getattr(args, "no_db", False):
        return False
    if os.getenv("VIDEO_ENHANCER_ANALYSIS_NO_DB", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        return False
    return True


def _pick_video_url(creative: Dict[str, Any]) -> str:
    if creative.get("video_url"):
        return str(creative["video_url"])
    for r in creative.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("video_url"):
            return str(r["video_url"])
    return ""


def _pick_image_url(creative: Dict[str, Any]) -> str:
    """提取图片 URL：优先 resource_urls 中的 image_url，其次 preview_img_url。"""
    for r in creative.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("image_url") and not r.get("video_url"):
            return str(r["image_url"])
    if creative.get("preview_img_url"):
        return str(creative["preview_img_url"])
    return ""


def _call_llm_text(system: str, user_content: str) -> str:
    return llm_client.call_text(system, user_content)


def _json_output_constraint(filter_ids: List[str]) -> str:
    """根据当前启用的套路过滤 ID 列表，拼出 JSON 输出约束。"""
    fields = "analysis（字符串，完整灵感分析正文）"
    for fid in filter_ids:
        fields += f"、{fid}（布尔）"
    return (
        f"\n\n【输出约束】用户要求你**只输出一个合法 JSON 对象**（不要使用 markdown 代码块），"
        f"字段为：{fields}。"
        "不要在 JSON 前后添加任何说明文字。"
    )


def _video_system_message(json_merged_output: bool) -> str:
    base = (
        "你是资深 UA 视频创意分析专家，擅长从视频画面、镜头、节奏拆解转化逻辑。"
        "请用简体中文输出结构化结论；可夹杂必要英文（品牌名、功能词、CTA 等）。"
        "若标题、文案、口播或画面文字为非中文/非英文（如阿拉伯语、泰语等），须在分析中用中文说明含义与语气，"
        "禁止在输出中整段照抄或堆叠非中英文字符。"
        "须客观评估合规相关画面：如大面积露肤、性暗示、擦边博眼球、低俗梗等；"
        "只做投放侧风险提示（是否涉及、大致程度、审核/定向需注意），禁止色情描写或煽动性表述。"
    )
    if json_merged_output:
        base += _json_output_constraint(_style_filter_ids())
    return base


def _text_fallback_system(media_kind: str, json_merged_output: bool) -> str:
    """视觉模型全部失败后的纯文本 system message。"""
    word = "视频" if media_kind == "video" else "图片"
    base = (
        f"你是 UA 创意分析专家。即使无法直接看{word}，也请根据给定信息输出可执行分析；"
        "若标题/文案暗示擦边或性暗示，须在结论中单独点出合规风险，表述克制。"
        "正文以简体中文为主，可含必要英文；非中英素材内容请用中文意译说明，勿直接粘贴阿语等非中英原文。"
    )
    if json_merged_output:
        base += _json_output_constraint(_style_filter_ids()).replace("\n\n", "")
    return base


def _call_llm_video(user_content: str, video_url: str, *, json_merged_output: bool = False) -> str:
    sys_video = _video_system_message(json_merged_output)
    text_fb = _text_fallback_system("video", json_merged_output)
    return llm_client.call_vision(
        sys_video, user_content, video_url, "video",
        text_fallback_system=text_fb,
    )


def _image_system_message(json_merged_output: bool) -> str:
    base = (
        "你是资深 UA 图片创意分析专家，擅长从图片构图、视觉元素、文案拆解转化逻辑。"
        "请用简体中文输出结构化结论；可夹杂必要英文（品牌名、功能词等）。"
        "若标题、文案或图中文字为非中文/非英文，须在分析中用中文说明含义，禁止在输出中整段照抄非中英文字符。"
        "须客观评估合规相关画面：如大面积露肤、性暗示、擦边博眼球、低俗梗等；"
        "只做投放侧风险提示（是否涉及、大致程度、审核/定向需注意），禁止色情描写或煽动性表述。"
    )
    if json_merged_output:
        base += _json_output_constraint(_style_filter_ids())
    return base


def _call_llm_image(
    user_content: str,
    image_url: str,
    *,
    json_merged_output: bool = False,
    quiet: bool = False,
) -> str:
    """用视觉模型分析图片素材，自动降级到纯文本。quiet=True 抑制降级日志。"""
    sys_img = _image_system_message(json_merged_output)
    text_fb = _text_fallback_system("image", json_merged_output)
    return llm_client.call_vision(
        sys_img, user_content, image_url, "image",
        text_fallback_system=text_fb, quiet=quiet,
    )


def _format_pipeline_tags(creative: Dict[str, Any]) -> str:
    t = creative.get("pipeline_tags")
    if isinstance(t, list) and t:
        return "、".join(str(x) for x in t if x)
    return "无"


def _style_filter_prompt_section(scope: str, basis: str) -> str:
    """动态生成套路筛选的 prompt 段落（基于 config/style_filters.json）。"""
    filters = _load_style_filters()
    if not filters:
        return ""
    bullet_lines = "\n".join(
        f"- {f['label']}：{f['description']}" for f in filters
    )
    json_keys = '\n'.join(
        f'- "{f["id"]}"：布尔。' for f in filters
    )
    return f"""

## 套路筛选（与上方灵感分析同一次完成）
请根据**{scope}**的{basis}，判断是否符合以下「我方已大量投放过的视觉套路」：
{bullet_lines}

## 输出格式（仅输出一个 JSON 对象，不要使用 markdown 代码块）
必须包含以下键：
- "analysis"：字符串，即上面「请输出」要求的完整灵感分析正文（可含换行）。
{json_keys}
任一为 true 表示命中该套路。除该 JSON 外不要输出任何其他文字。
"""


def _build_video_prompt(
    item: Dict[str, Any],
    creative: Dict[str, Any],
    video_url: str,
    *,
    merge_style: bool = True,
) -> str:
    body = f"""
以下是一条竞品 UA 视频素材：
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
5) 合规与风险提示：是否涉及明显露肤、性暗示、擦边博眼球或易触发审核的画面/文案；若无则写「未观察到明显高风险」；若有则简述程度与投放侧注意点（平台审核、年龄定向、素材尺度），禁止色情细节描写
6) 语言：全文仅使用汉字、英文字母与常规标点数字；遇外语口播/字幕/标题时用中文概括含义，勿整段保留阿拉伯文等非中英原文
""".strip()
    if not merge_style:
        return body
    return (body + _style_filter_prompt_section("整支视频", "画面与节奏")).strip()


def _build_image_prompt(
    item: Dict[str, Any],
    creative: Dict[str, Any],
    image_url: str,
    *,
    merge_style: bool = True,
) -> str:
    body = f"""
以下是一条竞品 UA 图片素材：
- 分类/产品: {item.get('category', '')} / {item.get('product', '')}
- AppID: {item.get('appid', '')}
- 广告主: {creative.get('advertiser_name', '')}
- 平台: {creative.get('platform', '')}
- 标题: {creative.get('title', '') or '无'}
- 文案: {creative.get('body', '') or '无'}
- 图片链接: {image_url or '无'}
- 展示估值: {creative.get('all_exposure_value', 0)}
- 热度: {creative.get('heat', 0)}
- 人气值: {creative.get('impression', 0)}
- 素材标签（系统）: {_format_pipeline_tags(creative)}

请结合图片画面与文案/标题，输出：
1) 广告创意拆解（构图、视觉焦点、Before/After 对比、文字排版等）
2) 视觉钩子（第一眼抓人的核心元素）
3) 情感基调
4) 可复用观察（仅总结素材表现与创意机制，不输出 UA 投放建议）
5) 合规与风险提示：是否涉及明显露肤、性暗示、擦边博眼球或易触发审核的画面/文案；若无则写「未观察到明显高风险」；若有则简述程度与投放侧注意点（平台审核、年龄定向、素材尺度），禁止色情细节描写
6) 语言：全文仅使用汉字、英文字母与常规标点数字；遇外语标题/画中字时用中文概括含义，勿整段保留阿拉伯文等非中英原文
""".strip()
    if not merge_style:
        return body
    return (body + _style_filter_prompt_section("整张图片", "画面")).strip()


def _style_filter_disabled() -> bool:
    return os.getenv("VIDEO_ANALYSIS_STYLE_FILTER_DISABLED", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _try_parse_json_object(text: str) -> Any:
    t = (text or "").strip()
    if not t:
        return None
    t = re.sub(r"^```(?:json)?\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s*```$", "", t)
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        m = re.search(r"\{[\s\S]*\}", t)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                return None
    return None


def _parse_inspiration_json_response(
    raw: str,
    expect_json: bool,
) -> Tuple[str, Dict[str, bool], str]:
    """
    从单次 LLM 回复中拆出 analysis 与套路命中结果。

    返回 (analysis_text, filter_hits, raw_json_str)。
    filter_hits: {filter_id: bool} — 只含当前启用的过滤器 ID。
    """
    fids = _style_filter_ids()
    empty_hits: Dict[str, bool] = {k: False for k in fids}
    if not expect_json:
        return (raw or "").strip(), empty_hits, ""
    t = (raw or "").strip()
    obj = _try_parse_json_object(t)
    if isinstance(obj, dict) and "analysis" in obj:
        a = str(obj.get("analysis") or "").strip()
        hits = {k: bool(obj.get(k)) for k in fids}
        return a, hits, t
    print(
        "[WARN] 灵感分析未返回含 analysis 的合法 JSON，已整段作为 analysis，套路筛选视为未命中。",
        flush=True,
    )
    return t, empty_hits, t


def _build_single_ua_suggestion(analysis_text: str, creative_type: str) -> str:
    """
    基于单条素材分析结果，按方向卡片格式输出单条 UA 可执行建议（用于爬取表展示）。
    各字段内容比聚类分析更精简。
    """
    if not analysis_text or analysis_text.startswith("[ERROR]"):
        return ""
    prompt = (
        "你是一名 UA 创意优化顾问。请基于下面这条素材分析，按方向卡片格式输出单条 UA 建议。\n"
        f"素材类型：{creative_type}\n"
        "格式要求（严格按以下结构输出，不要 JSON，直接输出可读文本）：\n"
        "背景：（一句话概括素材背景，20~40 字）\n"
        "UA建议：（聚焦可执行动作，素材改法/文案/首屏/节奏/审核规避，60~120 字）\n"
        "风险提示：（是否有露肤/擦边/性暗示/低俗等合规隐患；若无或低风险写「常规注意各平台素材政策」，不超过 40 字）\n"
        "语言：仅使用中文与必要英文术语，遇多语言素材意译为中文，禁止输出阿拉伯文等非中英字符。\n\n"
        f"素材分析如下：\n{analysis_text}"
    )
    return _call_llm_text("你是资深UA增长专家，擅长把分析提炼为精简可执行的卡片格式。", prompt)


def _analyze_one_item(
    idx: int,
    total: int,
    item: Dict[str, Any],
    *,
    target_date: str,
    crawl_date: Any,
    incremental_db: bool,
    merge_style: bool,
) -> Dict[str, Any]:
    """分析单条素材（含多模态 + 单条 UA 建议 + 可选入库）。供串行与线程池共用。"""
    creative = item.get("creative") or {}
    if not isinstance(creative, dict):
        creative = {}
    ad_key = str(creative.get("ad_key") or "")
    video_url = _pick_video_url(creative)
    image_url = _pick_image_url(creative) if not video_url else ""
    creative_type = "video" if video_url else "image"
    prod = str(item.get("product") or "")[:48]
    print(
        f"[{idx}/{total}] 开始 [{creative_type}] ad_key={ad_key[:12]}..."
        + (f" product={prod}" if prod else ""),
        flush=True,
    )

    t_inspiration = time.perf_counter()
    try:
        if video_url:
            prompt = _build_video_prompt(
                item, creative, video_url, merge_style=merge_style
            )
            raw_out = _call_llm_video(
                prompt, video_url, json_merged_output=merge_style
            )
        else:
            prompt = _build_image_prompt(
                item, creative, image_url, merge_style=merge_style
            )
            raw_out = _call_llm_image(
                prompt, image_url, json_merged_output=merge_style
            )
    except Exception as e:
        print(
            f"[ERROR] 灵感分析失败 ad_key={ad_key[:12]} reason={e}",
            flush=True,
        )
        raw_out = f"[ERROR] {e}"
    inspiration_sec = time.perf_counter() - t_inspiration
    print(
        f"[{idx}/{total}] 灵感多模态耗时 {inspiration_sec:.1f}s · [{creative_type}] ad_key={ad_key[:12]}…",
        flush=True,
    )

    preview_img = str(creative.get("preview_img_url") or "").strip()
    filter_hits: Dict[str, bool] = {}
    style_filter_raw = ""
    analysis = ""
    exclude_from_bitable = False
    exclude_from_cluster = False
    material_tags: List[str] = []
    if raw_out and not str(raw_out).startswith("[ERROR]"):
        analysis, filter_hits, style_filter_raw = _parse_inspiration_json_response(
            raw_out,
            merge_style,
        )
        if any(filter_hits.values()):
            exclude_from_bitable = True
            exclude_from_cluster = True
            material_tags = ["我方已经投过"]
            filters_map = {f["id"]: f["label"] for f in _load_style_filters()}
            hit_labels = [filters_map.get(k, k) for k, v in filter_hits.items() if v]
            print(
                f"[style-filter] ad_key={ad_key[:12]} 命中 {'/'.join(hit_labels)} → "
                f"标签「我方已经投过」，不参与多维表与聚类方向",
                flush=True,
            )
    else:
        analysis = str(raw_out or "")

    ua_suggestion_single = ""
    if (
        analysis
        and not str(analysis).startswith("[ERROR]")
        and not exclude_from_bitable
    ):
        try:
            ua_suggestion_single = _build_single_ua_suggestion(analysis, creative_type)
        except Exception as e:
            print(
                f"[WARN] 单条UA建议生成失败 ad_key={ad_key[:12]} reason={e}",
                flush=True,
            )
            ua_suggestion_single = ""

    row = {
        "category": item.get("category"),
        "product": item.get("product"),
        "appid": item.get("appid"),
        "ad_key": ad_key,
        "creative_type": creative_type,
        "platform": creative.get("platform"),
        "video_duration": creative.get("video_duration"),
        "all_exposure_value": creative.get("all_exposure_value"),
        "heat": creative.get("heat"),
        "impression": creative.get("impression"),
        "video_url": video_url,
        "image_url": image_url,
        "preview_img_url": preview_img,
        "title": creative.get("title") or "",
        "body": creative.get("body") or "",
        "pipeline_tags": creative.get("pipeline_tags")
        if isinstance(creative.get("pipeline_tags"), list)
        else [],
        "analysis": analysis,
        "ua_suggestion_single": ua_suggestion_single,
        **filter_hits,
        "style_filter_raw": style_filter_raw,
        "material_tags": material_tags,
        "exclude_from_bitable": exclude_from_bitable,
        "exclude_from_cluster": exclude_from_cluster,
    }
    err = str(analysis).startswith("[ERROR]")
    ua_ok = bool(ua_suggestion_single) and not err
    print(
        f"[{idx}/{total}] 完成 [{creative_type}] ad_key={ad_key[:12]}"
        + (f" product={prod}" if prod else "")
        + (" | 分析失败" if err else "")
        + (" | 我方已投套路(跳过表/聚类)" if exclude_from_bitable else "")
        + (" | UA建议已生成" if ua_ok else (" | 无UA建议" if not err else "")),
        flush=True,
    )

    if incremental_db and not err and str(analysis).strip():
        try:
            with _DB_WRITE_LOCK:
                ok = upsert_single_daily_creative_insight(
                    target_date,
                    crawl_date,
                    item,
                    {
                        "analysis": analysis,
                        "ua_suggestion_single": ua_suggestion_single,
                    },
                )
            if ok:
                print(
                    f"[DB] 已入库 insight ad_key={ad_key[:12]}...",
                    flush=True,
                )
        except Exception as e:
            print(
                f"[WARN] 单条入库失败 ad_key={ad_key[:12]} reason={e}",
                flush=True,
            )

    return row


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

    target_date = str(data.get("target_date") or "").strip()
    crawl_date = data.get("crawl_date")
    incremental_db = _should_incremental_db(args, target_date)
    if incremental_db:
        print(
            f"[DB] 逐条入库已开启 target_date={target_date}（SQLite daily_creative_insights）",
            flush=True,
        )

    merge_style = not _style_filter_disabled()
    workers = _resolve_analysis_workers(args)
    if workers > 1:
        print(
            f"[parallel] 灵感分析并发 workers={workers}（VIDEO_ANALYSIS_WORKERS / --workers）",
            flush=True,
        )

    results: List[Dict[str, Any]] = []
    skipped = 0
    total = len(items)
    work: List[Tuple[int, Dict[str, Any]]] = []
    for idx, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        creative = item.get("creative") or {}
        if not isinstance(creative, dict):
            continue
        ad_key = str(creative.get("ad_key") or "")
        video_url = _pick_video_url(creative)
        image_url = _pick_image_url(creative) if not video_url else ""
        if not video_url and not image_url:
            print(
                f"[{idx}/{total}] skip ad_key={ad_key[:12]} (no video or image)",
                flush=True,
            )
            skipped += 1
            continue
        work.append((idx, item))

    if workers <= 1 or len(work) <= 1:
        for idx, item in work:
            results.append(
                _analyze_one_item(
                    idx,
                    total,
                    item,
                    target_date=target_date,
                    crawl_date=crawl_date,
                    incremental_db=incremental_db,
                    merge_style=merge_style,
                )
            )
    else:
        by_idx: Dict[int, Dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=min(workers, len(work))) as ex:
            futs = {
                ex.submit(
                    _analyze_one_item,
                    idx,
                    total,
                    item,
                    target_date=target_date,
                    crawl_date=crawl_date,
                    incremental_db=incremental_db,
                    merge_style=merge_style,
                ): idx
                for idx, item in work
            }
            for fut in as_completed(futs):
                idx = futs[fut]
                try:
                    by_idx[idx] = fut.result()
                except Exception as e:
                    print(
                        f"[ERROR] 并行任务异常 idx={idx} reason={e}",
                        flush=True,
                    )
        for idx in sorted(by_idx):
            results.append(by_idx[idx])

    if args.output:
        out_path = Path(args.output)
    else:
        out_path = DATA_DIR / f"video_analysis_{in_path.stem}.json"
    video_count = sum(1 for r in results if r.get("creative_type") == "video")
    image_count = sum(1 for r in results if r.get("creative_type") == "image")
    style_excluded = sum(1 for r in results if isinstance(r, dict) and r.get("exclude_from_bitable"))
    out_payload = {
        "input_file": str(in_path),
        "total_items": len(items),
        "analyzed_items": len(results),
        "video_analyzed": video_count,
        "image_analyzed": image_count,
        "skipped": skipped,
        "style_filter_excluded": style_excluded,
        "results": results,
    }
    out_path.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if target_date:
        llm_client.flush_usage(target_date)
    print(
        f"完成：{len(results)} 条（视频 {video_count} / 图片 {image_count} / 跳过 {skipped}），输出 {out_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()

