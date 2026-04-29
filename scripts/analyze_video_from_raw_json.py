"""
基于指定 raw JSON 做视频/图片创意分析（默认不写库，仅输出 JSON）。

若输入 JSON 含 target_date（工作流 pending 文件），默认每成功分析一条即写入
data/video_enhancer_pipeline.db 的 daily_creative_insights；可用 --no-db 或环境变量
VIDEO_ENHANCER_ANALYSIS_NO_DB=1 关闭。

灵感分析为**纯文本结论**（或模型若输出 JSON，仅取其中 `analysis` 字段）。「我方已投」类标签由主流程中
`launched_effects_db` 等在分析**之后**统一打标，本脚本不再做套路筛选。

并发：环境变量 VIDEO_ANALYSIS_WORKERS（默认 3）或命令行 --workers，多条素材并行调用多模态（缩短总墙钟时间；注意 API 限流）。

多模态可调用成功但**正文解析为空**时（或模型只返回空 JSON/空 analysis）：
- `VIDEO_ANALYSIS_VISION_RETRY_ON_EMPTY=1`：同一条再请求一次多模态（加「禁止空」提示）；默认 0
- `VIDEO_ANALYSIS_EMPTY_ENRICH=1`（默认 1）：再调**纯文本**大模型，仅根据标题/文案/指标写补充分析（并带前缀说明置信度有限）
- 若仍无正文：输出 `inspiration_enrichment=minimal_stub` 的元数据摘记
- 若返回像 JSON 但**无法解析**或 **`analysis` 无效/过短**：`VIDEO_ANALYSIS_MULTIMODAL_FORMAT_RETRIES=3`（默认 3，0=关闭）对**同一条依次多模态重试**（不依赖纯文本收束，避免长文被「格式修复」截断/改写丢失）。仍失败且 `VIDEO_ANALYSIS_JSON_REPAIR=1` 时，才用**纯文本**做最后兜底（默认关闭；`inspiration_enrichment=json_repair`）
- `VIDEO_ANALYSIS_PARALLEL_SHARDS=1`（默认 1，最大 5）：**首次**多模态可并发 N 路；**格式重试**为串行单路，不叠加 fanout

灵感准入：纯图 / mp4(等)直链 / 真实 TikTok 外链（需 TIKTOK_YTDLP_RESOLVE=1 + yt-dlp 预处理）；tiktok.com/@test/ 为假链，仅视频路径不可分析（有封面可走图）。详见 scripts/tiktok_video_resolve.py。

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

from path_util import DATA_DIR
from video_enhancer_pipeline_db import upsert_single_daily_creative_insight

try:
    from arrow2_pipeline_db import upsert_arrow2_daily_insight_full
except ImportError:
    upsert_arrow2_daily_insight_full = None  # type: ignore[misc, assignment]

from tiktok_video_resolve import (
    display_video_link_for_prompt,
    is_creative_analyzable_with_resolve,
    is_playable_ads_creative,
    pick_playable_html_url,
    preprocess_video_for_vision,
    tiktok_ytdlp_resolve_enabled,
)

load_dotenv()

import llm_client  # noqa: E402  — 统一 LLM 调用层

_DB_WRITE_LOCK = threading.Lock()
_ARROW2_DB_WRITE_LOCK = threading.Lock()


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
    p.add_argument(
        "--arrow2",
        action="store_true",
        help="Arrow2 工作流：游戏素材三类标签输出 + 入库 arrow2_daily_insights（也可用 raw JSON workflow=arrow2_competitor）",
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


def _json_repair_enabled() -> bool:
    v = (os.getenv("VIDEO_ANALYSIS_JSON_REPAIR") or "0").strip().lower()
    return v not in ("0", "false", "no", "off")


def _multimodal_format_retry_max() -> int:
    """解析失败/过短时，串行多模态重试次数（不含首次调用）。"""
    raw = (os.getenv("VIDEO_ANALYSIS_MULTIMODAL_FORMAT_RETRIES") or "3").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 3
    return max(0, min(8, n))


def _vision_call_retry_max() -> int:
    """多模态调用失败（403/超时/空结果）时的重试次数，每次间隔2秒。"""
    raw = (os.getenv("VIDEO_ANALYSIS_CALL_RETRIES") or "3").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 3
    return max(0, min(5, n))


def _vision_parallel_shards() -> int:
    raw = (os.getenv("VIDEO_ANALYSIS_PARALLEL_SHARDS") or "1").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 1
    return max(1, min(5, n))


def _substantial_inspiration_body(s: str) -> bool:
    t = (s or "").strip()
    if not t or t.startswith("[ERROR]"):
        return False
    return len(t) >= 30


def _format_retry_user_note(attempt: int) -> str:
    return (
        f"\n\n【重要·多模态重试 第{attempt}次】上一条**无法稳定解析**为有效长正文（如 JSON 不合法、"
        "缺 `analysis` 或过短、杂糅等）。请按原题**原样打满**各小节与编号要求；"
        "若用 JSON 包装则须**完整合法**且 `analysis` 为**完整长文**（不少于约 30 字），"
        "禁止空字段、禁止用省略/摘要代替要点。"
    )


def _needs_json_or_format_repair(raw: str, parsed: str) -> bool:
    """多模态成功返回但正文过短/像坏 JSON/含 analysis 却无效时，需再投多模态或（可选）文本兜底。"""
    if (raw or "").strip().startswith("[ERROR]"):
        return False
    if _substantial_inspiration_body(parsed):
        return False
    r = raw or ""
    if "{" in r or "```" in r:
        return True
    if re.search(r'["\']analysis["\']', r, re.I):
        return True
    obj = _try_parse_json_object(r)
    if isinstance(obj, dict) and "analysis" in obj:
        v = obj.get("analysis")
        s = (str(v) if v is not None else "").strip()
        if len(s) < 20:
            return True
    return False


def _repair_inspiration_raw_with_text_llm(raw: str) -> str:
    """纯文本再投：把破碎 JSON / 杂糅输出收成合法 JSON 或抽出长分析。"""
    sys_t = (
        "你是数据格式整理助手。用户会给你模型原始回复，"
        "可能不是合法 JSON、或 JSON 中 analysis 为空/过短。请**只**输出一个 JSON 对象，"
        "有且仅有一个键 `analysis`（字符串），值为完整、结构化的中文素材分析长文，"
        "与日常灵感分析 1～6 点类似；不要 markdown 代码围栏、不要其他键。"
    )
    u = "模型原始输出如下，请恢复或合理补全为上述 JSON（须保留与合并全部实质信息，禁止无故删减长文）：\n\n" + (raw or "")
    return _call_llm_text(sys_t, u)


def _call_llm_video_fanout(
    build_prompt,  # () -> str
    vision_url: str,
    *,
    n: int,
) -> str:
    if n <= 1:
        return _call_llm_video(build_prompt(), vision_url)
    prompt_text = build_prompt()

    def job() -> str:
        return _call_llm_video(prompt_text, vision_url)

    with ThreadPoolExecutor(max_workers=n) as ex:
        futs = [ex.submit(job) for _ in range(n)]
        for f in as_completed(futs):
            try:
                out = f.result()
                if not out or str(out).strip().startswith("[ERROR]"):
                    continue
                p = _parse_inspiration_response(str(out))
                if _substantial_inspiration_body(p):
                    return str(out)
            except Exception:
                continue
        for f in futs:
            try:
                out = f.result()
                if out and not str(out).strip().startswith("[ERROR]"):
                    return str(out)
            except Exception:
                pass
    return f"[ERROR] {n} 路多模态均未返回可解析内容"


def _call_llm_image_fanout(
    build_prompt,  # () -> str
    image_url: str,
    *,
    n: int,
) -> str:
    if n <= 1:
        return _call_llm_image(build_prompt(), image_url)
    prompt_text = build_prompt()

    def job() -> str:
        return _call_llm_image(prompt_text, image_url, quiet=True)

    with ThreadPoolExecutor(max_workers=n) as ex:
        futs = [ex.submit(job) for _ in range(n)]
        for f in as_completed(futs):
            try:
                out = f.result()
                if not out or str(out).strip().startswith("[ERROR]"):
                    continue
                p = _parse_inspiration_response(str(out))
                if _substantial_inspiration_body(p):
                    return str(out)
            except Exception:
                continue
        for f in futs:
            try:
                out = f.result()
                if out and not str(out).strip().startswith("[ERROR]"):
                    return str(out)
            except Exception:
                pass
    return f"[ERROR] {n} 路多模态均未返回可解析内容"


# Arrow2：三类创意分类（与 pipeline_tags 如「重投」合并入 material_tags）
ARROW2_CATEGORY_LABELS: tuple[str, ...] = (
    "录屏素材（纯箭头点击飞出）",
    "解救类素材",
    "创意玩法素材",
)


def _normalize_arrow2_category(raw: str) -> str:
    """将模型输出的【素材类型】行归一为三类之一。"""
    s = (raw or "").strip()
    if not s:
        return ""
    for label in ARROW2_CATEGORY_LABELS:
        if label in s or s in label:
            return label
    if "录屏" in s and ("箭头" in s or "点击" in s or "飞出" in s):
        return ARROW2_CATEGORY_LABELS[0]
    if "解救" in s:
        return ARROW2_CATEGORY_LABELS[1]
    if any(
        k in s
        for k in ("创意", "玩法", "爆金币", "填色", "新玩法", "关卡")
    ):
        return ARROW2_CATEGORY_LABELS[2]
    return ""


def _arrow2_fixed_footer() -> str:
    return (
        "\n7) 在全文最后**必须**追加三行（固定格式，便于系统解析，每行独立一行）：\n"
        "【素材类型】只选下面**一类**的完整名称写入（勿加解释、勿多选）："
        "录屏素材（纯箭头点击飞出）、解救类素材、创意玩法素材。\n"
        "    （「创意玩法素材」含爆金币、填色、合成类新玩法等与录屏/解救不同的花样玩法。）\n"
        "【一句话说明】用约**10个汉字**一句话概括广告在播什么；无换行。\n"
        "【系统标签】若上方「素材标签（系统）」已含 pipeline 信息（如重投），照实写「重投」等；否则写「无」。"
    )


def _ve_fixed_footer() -> str:
    """VE（视频增强）流程的 footer：特效玩法 + 一句话说明。"""
    return (
        "\n7) 在全文最后**必须**追加两行（固定格式，便于系统解析，每行独立一行）：\n"
        "【特效玩法】用一句中文（约10~20字）概括这条素材的核心特效/玩法/创意卖点；"
        "参考风格：「圣诞华服换脸」「AI肌肉编辑」「老照片修复转动态」「黑白线稿漫画」「巨型猫咪特效」"
        "等——先写中文玩法名，必要时加英文原名；禁止写投放建议，只描述素材本身做了什么。\n"
        "【一句话说明】用约**10个汉字**一句话概括广告在播什么；无换行。"
    )


def _strip_arrow2_footer_lines(text: str) -> tuple[str, list[str], str, str, str]:
    """从分析正文移除末段固定行；解析素材类型、一句话说明、特效玩法；旧版【游戏素材标签】仍兼容为 llm_tags。

    返回 (cleaned_text, llm_tags, category, one_liner, effect_one_liner)。
    """
    raw = (text or "").strip()
    if not raw:
        return "", [], "", "", ""
    lines = raw.splitlines()
    n = len(lines)
    legacy_tags: list[str] = []
    category_raw = ""
    one_liner = ""
    effect_one_liner = ""
    remove_idx: set[int] = set()

    def _read_block(start: int, prefix: str) -> tuple[str, int]:
        """从 start 行开始读取 prefix 行的内容（含同行或下一行续行），返回 (rest_text, next_i)。"""
        s = lines[start].strip()
        rest = s.replace(prefix, "", 1).strip()
        remove_idx.add(start)
        if not rest:
            j = start + 1
            buf: list[str] = []
            while j < n:
                nx = lines[j].strip()
                if nx.startswith("【"):
                    break
                remove_idx.add(j)
                if nx:
                    buf.append(nx)
                j += 1
            rest = " ".join(buf).strip()
            return rest, j
        return rest, start + 1

    i = 0
    while i < n:
        s = lines[i].strip()
        if s.startswith("【素材类型】"):
            rest, i = _read_block(i, "【素材类型】")
            category_raw = rest
            continue
        if s.startswith("【一句话说明】"):
            rest, i = _read_block(i, "【一句话说明】")
            one_liner = rest[:40]
            continue
        if s.startswith("【特效玩法】"):
            rest, i = _read_block(i, "【特效玩法】")
            effect_one_liner = rest[:60]
            continue
        if s.startswith("【游戏素材标签】"):
            rest = s.replace("【游戏素材标签】", "", 1).strip()
            remove_idx.add(i)
            for part in rest.replace("、", ",").split(","):
                t = part.strip()
                if t and t not in ("无", "未分类"):
                    legacy_tags.append(t)
            i += 1
            continue
        if s.startswith("【系统标签】"):
            remove_idx.add(i)
            i += 1
            continue
        i += 1

    cat = _normalize_arrow2_category(category_raw)
    if cat:
        llm_tags = [cat]
    else:
        # 旧版多标签：逐条尝试归一为三类；无法归一的丢弃（避免「玩法录屏」「真人UGC」等非规范碎片进库）
        llm_tags = []
        seen: set[str] = set()
        for t in legacy_tags:
            nt = _normalize_arrow2_category(t)
            if nt and nt not in seen:
                seen.add(nt)
                llm_tags.append(nt)
        if not cat and llm_tags:
            cat = llm_tags[0]

    out_lines = [lines[k] for k in range(n) if k not in remove_idx]
    return "\n".join(out_lines).strip(), llm_tags, cat, one_liner.strip(), effect_one_liner.strip()


def _merge_material_tags_arrow2(creative: Dict[str, Any], llm_tags: list[str]) -> list[str]:
    out: list[str] = []
    pt = creative.get("pipeline_tags")
    if isinstance(pt, list):
        out.extend(str(x).strip() for x in pt if str(x).strip())
    for t in llm_tags:
        if t and t not in out:
            out.append(t)
    return out


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


def is_creative_analyzable(creative: Dict[str, Any]) -> bool:
    """
    是否与主流程「灵感分析准入」一致（见 tiktok_video_resolve.is_eligible_for_inspiration_analysis）。
    Arrow2 试玩 ads_type=7 由单独分支处理，不经多模态。
    """
    return is_creative_analyzable_with_resolve(creative)


def _call_llm_text(system: str, user_content: str) -> str:
    return llm_client.call_text(system, user_content)


def _video_system_message() -> str:
    return (
        "你是资深 UA 视频创意分析专家，擅长从视频画面、镜头、节奏拆解转化逻辑。"
        "请用简体中文输出结构化结论；可夹杂必要英文（品牌名、功能词、CTA 等）。"
        "若标题、文案、口播或画面文字为非中文/非英文（如阿拉伯语、泰语等），须在分析中用中文说明含义与语气，"
        "禁止在输出中整段照抄或堆叠非中英文字符。"
        "须客观评估合规相关画面：如大面积露肤、性暗示、擦边博眼球、低俗梗等；"
        "只做投放侧风险提示（是否涉及、大致程度、审核/定向需注意），禁止色情描写或煽动性表述。"
    )


def _text_fallback_system(media_kind: str) -> str:
    """视觉模型全部失败后的纯文本 system message。"""
    word = "视频" if media_kind == "video" else "图片"
    return (
        f"你是 UA 创意分析专家。即使无法直接看{word}，也请根据给定信息输出可执行分析；"
        "若标题/文案暗示擦边或性暗示，须在结论中单独点出合规风险，表述克制。"
        "正文以简体中文为主，可含必要英文；非中英素材内容请用中文意译说明，勿直接粘贴阿语等非中英原文。"
    )


def _call_llm_video(user_content: str, video_url: str) -> str:
    sys_video = _video_system_message()
    text_fb = _text_fallback_system("video")
    return llm_client.call_vision(
        sys_video, user_content, video_url, "video",
        text_fallback_system=text_fb,
    )


def _image_system_message() -> str:
    return (
        "你是资深 UA 图片创意分析专家，擅长从图片构图、视觉元素、文案拆解转化逻辑。"
        "请用简体中文输出结构化结论；可夹杂必要英文（品牌名、功能词等）。"
        "若标题、文案或图中文字为非中文/非英文，须在分析中用中文说明含义，禁止在输出中整段照抄非中英文字符。"
        "须客观评估合规相关画面：如大面积露肤、性暗示、擦边博眼球、低俗梗等；"
        "只做投放侧风险提示（是否涉及、大致程度、审核/定向需注意），禁止色情描写或煽动性表述。"
    )


def _call_llm_image(
    user_content: str,
    image_url: str,
    *,
    quiet: bool = False,
) -> str:
    """用视觉模型分析图片素材，自动降级到纯文本。quiet=True 抑制降级日志。"""
    sys_img = _image_system_message()
    text_fb = _text_fallback_system("image")
    return llm_client.call_vision(
        sys_img, user_content, image_url, "image",
        text_fallback_system=text_fb, quiet=quiet,
    )


def _format_pipeline_tags(creative: Dict[str, Any]) -> str:
    t = creative.get("pipeline_tags")
    if isinstance(t, list) and t:
        return "、".join(str(x) for x in t if x)
    return "无"


def _build_video_prompt(
    item: Dict[str, Any],
    creative: Dict[str, Any],
    video_url: str,
    *,
    arrow2: bool = False,
) -> str:
    foot = _arrow2_fixed_footer() if arrow2 else _ve_fixed_footer()
    return f"""
以下是一条竞品 UA 视频素材：
- 分类/产品: {item.get('category', '')} / {item.get('product', '')}
- AppID: {item.get('appid', '')}
- 广告主: {creative.get('advertiser_name', '')}
- 平台: {creative.get('platform', '')}
- 标题: {creative.get('title', '') or '无'}
- 文案: {creative.get('body', '') or '无'}
- 视频时长: {creative.get('video_duration', 0)} 秒
- 视频链接: {video_url or '无'}
- 展示估值（字段 impression）: {creative.get('impression', 0)}
- 人气（字段 all_exposure_value）: {creative.get('all_exposure_value', 0)}
- 热度: {creative.get('heat', 0)}
- 素材标签（系统）: {_format_pipeline_tags(creative)}

请输出：
1) 广告创意拆解
2) Hook（前几秒抓人点）
3) 情感基调
4) 可复用观察（仅总结素材表现与创意机制，不输出 UA 投放建议）
5) 合规与风险提示：是否涉及明显露肤、性暗示、擦边博眼球或易触发审核的画面/文案；若无则写「未观察到明显高风险」；若有则简述程度与投放侧注意点（平台审核、年龄定向、素材尺度），禁止色情细节描写
6) 语言：全文仅使用汉字、英文字母与常规标点数字；遇外语口播/字幕/标题时用中文概括含义，勿整段保留阿拉伯文等非中英原文
{foot}
""".strip()


def _build_image_prompt(
    item: Dict[str, Any],
    creative: Dict[str, Any],
    image_url: str,
    *,
    arrow2: bool = False,
) -> str:
    foot = _arrow2_fixed_footer() if arrow2 else _ve_fixed_footer()
    return f"""
以下是一条竞品 UA 图片素材：
- 分类/产品: {item.get('category', '')} / {item.get('product', '')}
- AppID: {item.get('appid', '')}
- 广告主: {creative.get('advertiser_name', '')}
- 平台: {creative.get('platform', '')}
- 标题: {creative.get('title', '') or '无'}
- 文案: {creative.get('body', '') or '无'}
- 图片链接: {image_url or '无'}
- 展示估值（字段 impression）: {creative.get('impression', 0)}
- 人气（字段 all_exposure_value）: {creative.get('all_exposure_value', 0)}
- 热度: {creative.get('heat', 0)}
- 素材标签（系统）: {_format_pipeline_tags(creative)}

请结合图片画面与文案/标题，输出：
1) 广告创意拆解（构图、视觉焦点、Before/After 对比、文字排版等）
2) 视觉钩子（第一眼抓人的核心元素）
3) 情感基调
4) 可复用观察（仅总结素材表现与创意机制，不输出 UA 投放建议）
5) 合规与风险提示：是否涉及明显露肤、性暗示、擦边博眼球或易触发审核的画面/文案；若无则写「未观察到明显高风险」；若有则简述程度与投放侧注意点（平台审核、年龄定向、素材尺度），禁止色情细节描写
6) 语言：全文仅使用汉字、英文字母与常规标点数字；遇外语标题/画中字时用中文概括含义，勿整段保留阿拉伯文等非中英原文
{foot}
""".strip()


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


def _parse_inspiration_response(raw: str) -> str:
    """
    从单次 LLM 回复中取分析正文：若为 JSON 且含 analysis 则取该字段，否则整段作为正文。
    """
    t = (raw or "").strip()
    if not t:
        return ""
    obj = _try_parse_json_object(t)
    if isinstance(obj, dict) and "analysis" in obj:
        return str(obj.get("analysis") or "").strip()
    return t


def _vision_retry_on_empty_enabled() -> bool:
    v = (os.getenv("VIDEO_ANALYSIS_VISION_RETRY_ON_EMPTY") or "0").strip().lower()
    return v in ("1", "true", "yes", "on")


def _empty_text_enrich_enabled() -> bool:
    v = (os.getenv("VIDEO_ANALYSIS_EMPTY_ENRICH") or "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def _build_text_only_inspiration_prompt(
    item: Dict[str, Any],
    creative: Dict[str, Any],
    creative_type: str,
    display_link: str,
    image_url: str,
    *,
    arrow2: bool = False,
) -> str:
    foot = _arrow2_fixed_footer() if arrow2 else _ve_fixed_footer()
    if creative_type == "video":
        return f"""
多模态接口未返回可解析的有效分析。请**仅根据**以下元数据作合理推断（首句须点明：因未见到画面，以下为基于文案/数据的推测）；
按常规视频分析**编号 1～6 节**写全，**每节至少 1～2 句中文**，**总字数不少于 200 字**。

- 分类/产品: {item.get('category', '')} / {item.get('product', '')}
- 平台: {creative.get('platform', '')}
- 标题: {creative.get('title', '') or '无'}
- 文案: {creative.get('body', '') or '无'}
- 视频时长: {creative.get('video_duration', 0)} 秒
- 展示/参考链: {display_link or '无'}
- 展示估值/人气/热度: {creative.get('impression', 0)} / {creative.get('all_exposure_value', 0)} / {creative.get('heat', 0)}
- 素材标签: {_format_pipeline_tags(creative)}

禁止只输出空内容、只输出「无」或空 JSON。{foot}
""".strip()
    return f"""
多模态接口未返回可解析的有效分析。请**仅根据**以下元数据对「图片类」素材作合理推断；按常规**图片分析编号结构**写全，**每节至少 1～2 句中文**，**总字数不少于 150 字**。

- 分类/产品: {item.get('category', '')} / {item.get('product', '')}
- 平台: {creative.get('platform', '')}
- 标题: {creative.get('title', '') or '无'}
- 文案: {creative.get('body', '') or '无'}
- 参考图/封面: {image_url or '无'}
- 展示估值/人气: {creative.get('impression', 0)} / {creative.get('all_exposure_value', 0)}

禁止只输出空内容。{foot}
""".strip()


def _minimal_inspiration_stub(item: Dict[str, Any], creative: Dict[str, Any]) -> str:
    return (
        "【系统补记】多模态与文本补全均未返回可解析正文，以下为元数据摘要做人工审核依据：\n"
        f"- 标题: {str(creative.get('title') or '无')[:500]}\n"
        f"- 文案: {str(creative.get('body') or '无')[:800]}\n"
        f"- 产品/平台: {item.get('product', '')} / {creative.get('platform', '')}\n"
        f"- impression: {creative.get('impression', 0)}"
    )


def _apply_empty_multimodal_enrichment(
    analysis: str,
    raw_out: str,
    item: Dict[str, Any],
    creative: Dict[str, Any],
    creative_type: str,
    display_video_link: str,
    image_url: str,
    vision_url: str,
    *,
    idx: int,
    total: int,
    ad_key: str,
    arrow2: bool,
) -> tuple[str, str]:
    """
    若多模态未报 [ERROR] 但解析后无正文，则：可选多模态重试 -> 纯文本补全 -> 元数据硬兜底。
    返回 (新 analysis, inspiration_enrichment 标签)。
    """
    st = (analysis or "").strip()
    if st.startswith("[ERROR]"):
        return analysis, "none"
    if st:
        return analysis, "none"
    ro = str(raw_out or "")
    if ro.strip().startswith("[ERROR]"):
        return analysis, "none"

    vision_note = (
        "\n\n【重要】上一条未产生**有效可解析**的分析正文。请按原要求写满各小节，"
        "每节有实质内容；禁止空响应、禁止仅输出 {} 或空 JSON、禁止 analysis 字段为空。"
    )

    if _vision_retry_on_empty_enabled():
        try:
            if vision_url:
                p2 = _build_video_prompt(
                    item, creative, display_video_link, arrow2=arrow2
                ) + vision_note
                raw2 = _call_llm_video(p2, vision_url)
                if raw2 and not str(raw2).strip().startswith("[ERROR]"):
                    a2 = _parse_inspiration_response(str(raw2))
                    if (a2 or "").strip() and not (a2 or "").strip().startswith("[ERROR]"):
                        print(
                            f"[{idx}/{total}] [enrich] vision_retry ok ad_key={ad_key[:12]}…",
                            flush=True,
                        )
                        return a2, "vision_retry"
            if (image_url or "").strip() and not vision_url:
                p2 = _build_image_prompt(
                    item, creative, image_url, arrow2=arrow2
                ) + vision_note
                raw2 = _call_llm_image(p2, image_url, quiet=True)
                if raw2 and not str(raw2).strip().startswith("[ERROR]"):
                    a2 = _parse_inspiration_response(str(raw2))
                    if (a2 or "").strip() and not (a2 or "").strip().startswith("[ERROR]"):
                        print(
                            f"[{idx}/{total}] [enrich] image_retry ok ad_key={ad_key[:12]}…",
                            flush=True,
                        )
                        return a2, "vision_retry"
        except Exception as e:
            print(
                f"[{idx}/{total}] [enrich] 多模态重试异常 ad_key={ad_key[:12]}: {e}",
                flush=True,
            )

    if _empty_text_enrich_enabled():
        try:
            sys_t = _text_fallback_system("video" if creative_type == "video" else "image")
            u_t = _build_text_only_inspiration_prompt(
                item, creative, creative_type, display_video_link, image_url, arrow2=arrow2
            )
            filled = _call_llm_text(sys_t, u_t)
            if (filled or "").strip():
                a2 = (
                    "【多模态未返回有效分析；以下为据标题/文案/指标的补充推断，置信度有限】\n"
                    + filled.strip()
                )
                print(
                    f"[{idx}/{total}] [enrich] text_only ok ad_key={ad_key[:12]}…",
                    flush=True,
                )
                return a2, "text_only"
        except Exception as e:
            print(
                f"[{idx}/{total}] [enrich] 文本补全失败 ad_key={ad_key[:12]}: {e}",
                flush=True,
            )
    return _minimal_inspiration_stub(item, creative), "minimal_stub"



def _analyze_one_item(
    idx: int,
    total: int,
    item: Dict[str, Any],
    *,
    target_date: str,
    crawl_date: Any,
    incremental_db: bool,
    arrow2: bool = False,
    incremental_arrow2: bool = False,
) -> Dict[str, Any]:
    """分析单条素材（含多模态 + 单条 UA 建议 + 可选入库）。供串行与线程池共用。"""
    creative = item.get("creative") or {}
    if not isinstance(creative, dict):
        creative = {}
    ad_key = str(creative.get("ad_key") or "")
    vision_url, vision_src = preprocess_video_for_vision(creative)
    display_video_link = display_video_link_for_prompt(creative, vision_url)
    direct_video_ref = _pick_video_url(creative)
    image_url = _pick_image_url(creative) if not vision_url else ""
    creative_type = "video" if vision_url else "image"
    prod = str(item.get("product") or "")[:48]
    print(
        f"[{idx}/{total}] 开始 [{creative_type}] ad_key={ad_key[:12]}..."
        + (f" product={prod}" if prod else ""),
        flush=True,
    )

    n_shards = _vision_parallel_shards()
    if n_shards > 1:
        print(
            f"[{idx}/{total}] 多模态并发 {n_shards} 路（VIDEO_ANALYSIS_PARALLEL_SHARDS）"
            f" ad_key={ad_key[:12]}…",
            flush=True,
        )
    t_inspiration = time.perf_counter()
    try:
        if vision_url:

            def _build_vp() -> str:
                return _build_video_prompt(
                    item, creative, display_video_link, arrow2=arrow2
                )

            if n_shards > 1:
                raw_out = _call_llm_video_fanout(_build_vp, vision_url, n=n_shards)
            else:
                raw_out = _call_llm_video(_build_vp(), vision_url)
        elif (image_url or "").strip():

            def _build_ip() -> str:
                return _build_image_prompt(
                    item, creative, image_url, arrow2=arrow2
                )

            if n_shards > 1:
                raw_out = _call_llm_image_fanout(_build_ip, image_url, n=n_shards)
            else:
                raw_out = _call_llm_image(_build_ip(), image_url)
        else:
            raw_out = (
                "[ERROR] 无可用媒体：TikTok/YouTube 直链解析失败且无封面图；"
                "可检查 yt-dlp 或关闭 TIKTOK_YTDLP_RESOLVE / YOUTUBE_YTDLP_RESOLVE 仅用图分析"
            )
    except Exception as e:
        print(
            f"[ERROR] 灵感分析失败 ad_key={ad_key[:12]} reason={e}",
            flush=True,
        )
        raw_out = f"[ERROR] {e}"

    # ── 多模态调用失败/空结果重试（最多3次，每次间隔2秒） ──
    _call_retry_max = _vision_call_retry_max()
    _call_retry_used = 0
    while (
        _call_retry_max > 0
        and _call_retry_used < _call_retry_max
        and (not raw_out or str(raw_out).startswith("[ERROR]"))
        and (vision_url or image_url)
    ):
        _call_retry_used += 1
        print(
            f"[{idx}/{total}] [call_retry] 多模态调用失败，重试 {_call_retry_used}/{_call_retry_max} "
            f"ad_key={ad_key[:12]}…",
            flush=True,
        )
        time.sleep(2)
        try:
            if vision_url:
                raw_out = _call_llm_video(_build_vp() if vision_url else "", vision_url)
            elif image_url:
                raw_out = _call_llm_image(_build_ip(), image_url)
        except Exception as e2:
            print(
                f"[{idx}/{total}] [call_retry] 重试异常 ad_key={ad_key[:12]} reason={e2}",
                flush=True,
            )
            raw_out = f"[ERROR] {e2}"
        if raw_out and not str(raw_out).startswith("[ERROR]"):
            print(
                f"[{idx}/{total}] [call_retry] 重试成功 ad_key={ad_key[:12]}…",
                flush=True,
            )
            break

    inspiration_sec = time.perf_counter() - t_inspiration
    print(
        f"[{idx}/{total}] 灵感多模态耗时 {inspiration_sec:.1f}s · [{creative_type}] ad_key={ad_key[:12]}…",
        flush=True,
    )

    preview_img = str(creative.get("preview_img_url") or "").strip()
    style_filter_match_summary = ""
    exclude_from_bitable = False
    exclude_from_cluster = False
    material_tags: List[str] = []
    arrow2_material_category = ""
    ad_one_liner = ""
    effect_one_liner = ""
    inspiration_enrich: str = "none"
    json_repair_applied = False
    work: str = str(raw_out or "")
    mm_fmt_used = 0

    if work and not work.startswith("[ERROR]"):
        p0 = _parse_inspiration_response(work)
        max_fr = _multimodal_format_retry_max()
        can_mm = bool(vision_url) or bool((image_url or "").strip())
        while (
            can_mm
            and max_fr > 0
            and mm_fmt_used < max_fr
            and _needs_json_or_format_repair(work, p0)
        ):
            mm_fmt_used += 1
            note = _format_retry_user_note(mm_fmt_used)
            try:
                if vision_url:
                    p2 = _build_video_prompt(
                        item, creative, display_video_link, arrow2=arrow2
                    ) + note
                    w2 = _call_llm_video(p2, vision_url)
                else:
                    p2 = _build_image_prompt(
                        item, creative, image_url, arrow2=arrow2
                    ) + note
                    w2 = _call_llm_image(p2, image_url, quiet=True)
            except Exception as e:
                w2 = ""
                print(
                    f"[{idx}/{total}] [format_retry] 多模态重试异常 ad_key={ad_key[:12]}: {e}",
                    flush=True,
                )
            if (w2 or "").strip() and not str(w2).strip().startswith("[ERROR]"):
                work = str(w2)
                print(
                    f"[{idx}/{total}] [format_retry] 多模态 {mm_fmt_used}/{max_fr} "
                    f"ad_key={ad_key[:12]}…",
                    flush=True,
                )
            p0 = _parse_inspiration_response(work)
            if not _needs_json_or_format_repair(work, p0):
                break
        if _json_repair_enabled() and _needs_json_or_format_repair(work, p0):
            try:
                r2 = _repair_inspiration_raw_with_text_llm(work)
            except Exception as e:
                r2 = ""
                print(
                    f"[{idx}/{total}] [json_repair] 失败 ad_key={ad_key[:12]}: {e}",
                    flush=True,
                )
            if (r2 or "").strip() and not str(r2).strip().startswith("[ERROR]"):
                work = str(r2)
                json_repair_applied = True
                print(
                    f"[{idx}/{total}] [json_repair] 已用纯文本兜底 ad_key={ad_key[:12]}…",
                    flush=True,
                )
        analysis = _parse_inspiration_response(work)
        if arrow2:
            analysis, llm_tags, arrow2_material_category, ad_one_liner, _effect = _strip_arrow2_footer_lines(
                analysis
            )
            material_tags = _merge_material_tags_arrow2(creative, llm_tags)
        else:
            analysis, _, _, ad_one_liner, effect_one_liner = _strip_arrow2_footer_lines(
                analysis
            )
        analysis, inspiration_enrich = _apply_empty_multimodal_enrichment(
            analysis,
            work,
            item,
            creative,
            creative_type,
            display_video_link,
            image_url,
            vision_url,
            idx=idx,
            total=total,
            ad_key=ad_key,
            arrow2=arrow2,
        )
        if (
            not json_repair_applied
            and mm_fmt_used
            and inspiration_enrich == "none"
            and _substantial_inspiration_body(analysis)
        ):
            inspiration_enrich = "multimodal_format_retry"
        if (
            json_repair_applied
            and inspiration_enrich == "none"
            and _substantial_inspiration_body(analysis)
        ):
            inspiration_enrich = "json_repair"
        if inspiration_enrich != "none" and arrow2:
            analysis, llm_tags, arrow2_material_category, ad_one_liner, _effect = _strip_arrow2_footer_lines(
                analysis
            )
            material_tags = _merge_material_tags_arrow2(creative, llm_tags)
        if inspiration_enrich != "none" and not arrow2:
            analysis, _, _, ad_one_liner, effect_one_liner = _strip_arrow2_footer_lines(
                analysis
            )
    else:
        analysis = work

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
        "video_url": direct_video_ref,
        "tiktok_ytdlp_used": vision_src == "tiktok_resolved",
        "youtube_ytdlp_used": vision_src == "youtube_resolved",
        "image_url": image_url,
        "preview_img_url": preview_img,
        "title": creative.get("title") or "",
        "body": creative.get("body") or "",
        "pipeline_tags": creative.get("pipeline_tags")
        if isinstance(creative.get("pipeline_tags"), list)
        else [],
        "analysis": analysis,
        "inspiration_enrichment": inspiration_enrich,
        "style_filter_match_summary": style_filter_match_summary,
        "material_tags": material_tags,
        "arrow2_material_category": arrow2_material_category,
        "effect_one_liner": effect_one_liner,
        "exclude_from_bitable": exclude_from_bitable,
        "exclude_from_cluster": exclude_from_cluster,
        "_orig_idx": idx,
    }
    err = str(analysis).startswith("[ERROR]")
    print(
        f"[{idx}/{total}] 完成 [{creative_type}] ad_key={ad_key[:12]}"
        + (f" product={prod}" if prod else "")
        + (" | 分析失败" if err else ""),
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
                        "material_tags": material_tags,
                        "exclude_from_bitable": exclude_from_bitable,
                        "exclude_from_cluster": exclude_from_cluster,
                        "style_filter_match_summary": style_filter_match_summary,
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

    if incremental_arrow2 and not err and str(analysis).strip() and upsert_arrow2_daily_insight_full:
        try:
            with _ARROW2_DB_WRITE_LOCK:
                ok = upsert_arrow2_daily_insight_full(
                    target_date,
                    crawl_date,
                    item,
                    {
                        "analysis": analysis,
                        "ua_suggestion_single": ua_suggestion_single,
                        "material_tags": material_tags,
                        "arrow2_material_category": arrow2_material_category,
                        "ad_one_liner": ad_one_liner,
                    },
                )
            if ok:
                print(
                    f"[DB] 已写入 arrow2_daily_insights ad_key={ad_key[:12]}...",
                    flush=True,
                )
        except Exception as e:
            print(
                f"[WARN] Arrow2 入库失败 ad_key={ad_key[:12]} reason={e}",
                flush=True,
            )

    return row


def _analyze_arrow2_playable_item(
    idx: int,
    total: int,
    item: Dict[str, Any],
    *,
    target_date: str,
    crawl_date: Any,
    incremental_db: bool,
    incremental_arrow2: bool,
) -> Dict[str, Any]:
    """ads_type=7 试玩 HTML：不写多模态；结果里 video_url=试玩链，供飞书「视频链接」列。"""
    creative = item.get("creative") or {}
    if not isinstance(creative, dict):
        creative = {}
    ad_key = str(creative.get("ad_key") or "")
    html = pick_playable_html_url(creative)
    prod = str(item.get("product") or "")[:48]
    print(
        f"[{idx}/{total}] 试玩 HTML（跳过灵感多模态）ad_key={ad_key[:12]}..."
        + (f" product={prod}" if prod else ""),
        flush=True,
    )
    analysis = "【试玩】未做多模态灵感分析；试玩 HTML 见多维表「试玩链接」列。"
    material_tags = ["试玩", "Playable"]
    preview_img = str(creative.get("preview_img_url") or "").strip()
    row: Dict[str, Any] = {
        "category": item.get("category"),
        "product": item.get("product"),
        "appid": item.get("appid"),
        "ad_key": ad_key,
        "creative_type": "playable",
        "platform": creative.get("platform"),
        "video_duration": creative.get("video_duration"),
        "all_exposure_value": creative.get("all_exposure_value"),
        "heat": creative.get("heat"),
        "impression": creative.get("impression"),
        "video_url": "",
        "tiktok_ytdlp_used": False,
        "youtube_ytdlp_used": False,
        "image_url": "",
        "preview_img_url": preview_img,
        "title": creative.get("title") or "",
        "body": creative.get("body") or "",
        "pipeline_tags": creative.get("pipeline_tags")
        if isinstance(creative.get("pipeline_tags"), list)
        else [],
        "analysis": analysis if html else "[ERROR] 试玩素材但缺少 html_url/cdn_url",
        "inspiration_enrichment": "none",
        "ua_suggestion_single": "",
        "style_filter_match_summary": "",
        "material_tags": material_tags,
        "arrow2_material_category": "",
        "ad_one_liner": "",
        "effect_one_liner": "",
        "exclude_from_bitable": False,
        "exclude_from_cluster": False,
    }
    err = not bool(html)
    print(
        f"[{idx}/{total}] 完成 [playable] ad_key={ad_key[:12]}"
        + (f" product={prod}" if prod else "")
        + (" | 缺少试玩链" if err else " | 试玩链已写入结果"),
        flush=True,
    )
    if incremental_arrow2 and html and upsert_arrow2_daily_insight_full:
        try:
            with _ARROW2_DB_WRITE_LOCK:
                ok = upsert_arrow2_daily_insight_full(
                    target_date,
                    crawl_date,
                    item,
                    {
                        "analysis": row["analysis"],
                        "ua_suggestion_single": "",
                        "material_tags": material_tags,
                        "arrow2_material_category": "",
                        "ad_one_liner": "",
                    },
                )
            if ok:
                print(
                    f"[DB] 已写入 arrow2_daily_insights ad_key={ad_key[:12]}...",
                    flush=True,
                )
        except Exception as e:
            print(
                f"[WARN] Arrow2 入库失败 ad_key={ad_key[:12]} reason={e}",
                flush=True,
            )
    elif incremental_arrow2 and err:
        print(
            f"[WARN] 试玩无链，跳过 Arrow2 入库 ad_key={ad_key[:12]}",
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
    arrow2 = bool(getattr(args, "arrow2", False)) or str(data.get("workflow") or "").strip() == "arrow2_competitor"
    incremental_db = _should_incremental_db(args, target_date) and not arrow2
    incremental_arrow2 = arrow2 and _should_incremental_db(args, target_date) and upsert_arrow2_daily_insight_full is not None
    if incremental_db:
        print(
            f"[DB] 逐条入库已开启 target_date={target_date}（SQLite daily_creative_insights）",
            flush=True,
        )
    if incremental_arrow2:
        try:
            from arrow2_pipeline_db import init_db as init_arrow2_db

            init_arrow2_db()
        except Exception as e:
            print(f"[WARN] Arrow2 init_db: {e}", flush=True)
        print(
            f"[DB] Arrow2 逐条入库 target_date={target_date}（SQLite arrow2_daily_insights）",
            flush=True,
        )
    if arrow2:
        print(
            "[arrow2] ads_type=7 试玩：跳过多模态与单条 UA；飞书「试玩链接」列写 HTML（非「视频链接」）",
            flush=True,
        )

    if tiktok_ytdlp_resolve_enabled():
        print(
            "[tiktok-resolve] TIKTOK_YTDLP_RESOLVE=1：TikTok 落地页将通过 yt-dlp 解析直链（需已安装 yt-dlp）",
            flush=True,
        )
    workers = _resolve_analysis_workers(args)
    if workers > 1:
        print(
            f"[parallel] 灵感分析并发 workers={workers}（VIDEO_ANALYSIS_WORKERS / --workers）",
            flush=True,
        )

    skipped = 0
    total = len(items)
    work: List[Tuple[int, Dict[str, Any]]] = []
    playable_work: List[Tuple[int, Dict[str, Any]]] = []
    for idx, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        creative = item.get("creative") or {}
        if not isinstance(creative, dict):
            continue
        ad_key = str(creative.get("ad_key") or "")
        if arrow2 and is_playable_ads_creative(creative):
            if not pick_playable_html_url(creative):
                print(
                    f"[{idx}/{total}] skip ad_key={ad_key[:12]} (试玩 ads_type=7 但无 html_url/cdn_url)",
                    flush=True,
                )
                skipped += 1
                continue
            playable_work.append((idx, item))
            continue
        if not is_creative_analyzable(creative):
            print(
                f"[{idx}/{total}] skip ad_key={ad_key[:12]} (no video/image URL for analysis)",
                flush=True,
            )
            skipped += 1
            continue
        work.append((idx, item))

    by_idx: Dict[int, Dict[str, Any]] = {}
    for idx, item in playable_work:
        by_idx[idx] = _analyze_arrow2_playable_item(
            idx,
            total,
            item,
            target_date=target_date,
            crawl_date=crawl_date,
            incremental_db=incremental_db,
            incremental_arrow2=incremental_arrow2,
        )

    if workers <= 1 or len(work) <= 1:
        for idx, item in work:
            by_idx[idx] = _analyze_one_item(
                idx,
                total,
                item,
                target_date=target_date,
                crawl_date=crawl_date,
                incremental_db=incremental_db,
                arrow2=arrow2,
                incremental_arrow2=incremental_arrow2,
            )
    else:
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
                    arrow2=arrow2,
                    incremental_arrow2=incremental_arrow2,
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
    results = [by_idx[i] for i in sorted(by_idx)]

    # ── 二轮重试：对一轮中分析失败/为空的条目再跑一次 ──
    failed_indices = [
        i for i, r in enumerate(results)
        if not r.get("analysis") or str(r.get("analysis", "")).startswith("[ERROR]")
    ]
    if failed_indices:
        n_fail = len(failed_indices)
        print(f"\n[retry-round2] 一轮分析完成，{n_fail} 条失败/为空，5秒后统一重试...", flush=True)
        time.sleep(5)
        retry_ok = 0
        retry_still_fail = 0
        for ri in failed_indices:
            orig_idx = results[ri].get("_orig_idx", ri)
            # 从原始 items 找回 item
            item = None
            for _idx, _item in work:
                if _idx == orig_idx:
                    item = _item
                    break
            if item is None:
                retry_still_fail += 1
                continue
            try:
                new_row = _analyze_one_item(
                    orig_idx,
                    total,
                    item,
                    target_date=target_date,
                    crawl_date=crawl_date,
                    incremental_db=incremental_db,
                    arrow2=arrow2,
                    incremental_arrow2=incremental_arrow2,
                )
                new_analysis = new_row.get("analysis", "")
                if new_analysis and not str(new_analysis).startswith("[ERROR]"):
                    results[ri] = new_row
                    retry_ok += 1
                    print(
                        f"[retry-round2] 重试成功 ad_key={new_row.get('ad_key','')[:12]}…",
                        flush=True,
                    )
                else:
                    retry_still_fail += 1
            except Exception as e3:
                retry_still_fail += 1
                print(
                    f"[retry-round2] 重试异常 ad_key={results[ri].get('ad_key','')[:12]} reason={e3}",
                    flush=True,
                )
        print(
            f"[retry-round2] 二轮重试完成：成功 {retry_ok}，仍失败 {retry_still_fail}",
            flush=True,
        )

    if args.output:
        out_path = Path(args.output)
    else:
        out_path = DATA_DIR / f"video_analysis_{in_path.stem}.json"
    video_count = sum(1 for r in results if r.get("creative_type") == "video")
    image_count = sum(1 for r in results if r.get("creative_type") == "image")
    playable_count = sum(1 for r in results if r.get("creative_type") == "playable")
    out_payload = {
        "input_file": str(in_path),
        "workflow": "arrow2_competitor" if arrow2 else "",
        "total_items": len(items),
        "analyzed_items": len(results),
        "video_analyzed": video_count,
        "image_analyzed": image_count,
        "playable_no_llm": playable_count,
        "skipped": skipped,
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

