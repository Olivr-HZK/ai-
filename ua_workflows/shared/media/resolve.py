"""
TikTok / YouTube / 外链视频：准入判定、预处理（yt-dlp 取直链）、与多模态 vision URL 解析。

环境变量：
  TIKTOK_YTDLP_RESOLVE=1   真实 TikTok 落地页走 yt-dlp（默认关闭）
  YOUTUBE_YTDLP_RESOLVE    YouTube 观看页 / youtu.be：默认 **开启**（用 yt-dlp -g 取直链喂多模态，不落盘）；设为 0/false/off 可关
  YTDLP_PATH / YTDLP_TIMEOUT_SEC

灵感分析准入（与主流程一致，预检不调用 yt-dlp）：
  - 纯图：有可用封面/resource 图且无合格视频直链，或仅有假链时仍可走图；
  - 视频：须为「直链文件」（.mp4/.webm 等或广大大 CDN 形态），或「真实 TikTok / YouTube 页」且开启对应解析；
  - 广大大脱敏 tiktok.com/@test/… 不视为真实外链，不进入视频分析（有图可走图）。
"""

from __future__ import annotations

import os
import re
import subprocess
import threading
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

_CACHE: Dict[str, str] = {}
_CACHE_LOCK = threading.Lock()


def tiktok_ytdlp_resolve_enabled() -> bool:
    return os.getenv("TIKTOK_YTDLP_RESOLVE", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def ytdlp_youtube_resolve_enabled() -> bool:
    """
    YouTube 页是否用 yt-dlp 解析为直链（子进程 `yt-dlp -g`，取 HTTPS 流地址给 vision；不下载整文件到本地）。
    未设置 YOUTUBE_YTDLP_RESOLVE 时默认开启；显式 0/false/off 关闭。
    """
    raw = os.getenv("YOUTUBE_YTDLP_RESOLVE")
    if raw is not None and str(raw).strip() != "":
        s = str(raw).strip().lower()
        if s in ("0", "false", "no", "off"):
            return False
        if s in ("1", "true", "yes", "on"):
            return True
        return False
    return True


def ytdlp_bin() -> str:
    return (os.getenv("YTDLP_PATH") or "yt-dlp").strip() or "yt-dlp"


def ytdlp_timeout_sec() -> int:
    raw = (os.getenv("YTDLP_TIMEOUT_SEC") or "120").strip()
    try:
        return max(30, min(600, int(raw)))
    except ValueError:
        return 120


def is_tiktok_landing_url(url: str) -> bool:
    u = (url or "").strip()
    if not u:
        return False
    try:
        host = urlparse(u).netloc.lower()
    except Exception:
        return False
    return "tiktok.com" in host


def is_youtube_page_url(url: str) -> bool:
    """YouTube 观看页 / Shorts / youtu.be（非直链 mp4）。"""
    u = (url or "").strip()
    if not u:
        return False
    try:
        host = urlparse(u).netloc.lower()
    except Exception:
        return False
    return "youtube.com" in host or "youtu.be" in host


# 广大大等来源的 TikTok 脱敏：用户名固定为 @test
_FAKE_TIKTOK_PLACEHOLDER = re.compile(r"tiktok\.com/@test(?:/|$|\?)", re.I)


def is_fake_tiktok_placeholder_url(url: str) -> bool:
    u = (url or "").strip()
    if not u:
        return False
    return bool(_FAKE_TIKTOK_PLACEHOLDER.search(u))


_VIDEO_FILE_SUFFIX = re.compile(r"\.(mp4|webm|m3u8)(?:\?|#|$)", re.I)


def is_direct_video_file_url(url: str) -> bool:
    """
    可直喂多模态的「文件型」视频 URL（非 TikTok / YouTube 网页落地页）。
    含：路径以 .mp4/.webm/.m3u8 结尾、或广大大 zingfront sp_opera CDN 形态。
    """
    u = (url or "").strip()
    if not u or is_tiktok_landing_url(u) or is_youtube_page_url(u):
        return False
    low = u.lower()
    try:
        path = urlparse(u).path.lower()
    except Exception:
        path = ""
    if path.endswith((".mp4", ".webm", ".m3u8")):
        return True
    if _VIDEO_FILE_SUFFIX.search(u):
        return True
    if "zingfront.com" in low and "/sp_opera/" in low:
        return True
    return False


def pick_video_url_direct(creative: Dict[str, Any]) -> str:
    if not isinstance(creative, dict):
        return ""
    if creative.get("video_url"):
        return str(creative["video_url"]).strip()
    for r in creative.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("video_url"):
            return str(r["video_url"]).strip()
    return ""


def pick_image_url_direct(creative: Dict[str, Any]) -> str:
    if not isinstance(creative, dict):
        return ""
    for r in creative.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("image_url") and not r.get("video_url"):
            return str(r["image_url"]).strip()
    if creative.get("preview_img_url"):
        return str(creative["preview_img_url"]).strip()
    return ""


def is_eligible_for_inspiration_analysis(creative: Dict[str, Any]) -> bool:
    """
    是否可进入灵感分析队列（不调用 yt-dlp）。
    纯图 / mp4(等)直链 / 真实 TikTok 且开启解析。
    """
    if not isinstance(creative, dict):
        return False
    direct = pick_video_url_direct(creative)
    img = pick_image_url_direct(creative)

    if direct:
        if is_fake_tiktok_placeholder_url(direct):
            return bool(img.strip())
        if is_direct_video_file_url(direct):
            return True
        if is_tiktok_landing_url(direct):
            return bool(tiktok_ytdlp_resolve_enabled() or img.strip())
        if is_youtube_page_url(direct):
            return bool(ytdlp_youtube_resolve_enabled() or img.strip())
        return False

    if img:
        return True

    for key in ("source_url", "landing_url", "page_url"):
        u = str(creative.get(key) or "").strip()
        if u and is_tiktok_landing_url(u) and not is_fake_tiktok_placeholder_url(u):
            return tiktok_ytdlp_resolve_enabled()
        if u and is_youtube_page_url(u):
            return ytdlp_youtube_resolve_enabled()
    return False


INELIGIBLE_REASON_LABEL_CN: Dict[str, str] = {
    "source_fake_tiktok_no_image": "外链含 @test 占位且无封面图",
    "direct_fake_tiktok_no_image": "直链为 @test 且无图",
    "real_tiktok_page_ytdlp_off": "真实 TikTok 页但未开 TIKTOK_YTDLP_RESOLVE",
    "direct_tiktok_page_ytdlp_off": "直链为 TikTok 页但未开解析",
    "real_youtube_page_ytdlp_off": "YouTube 页但已关 YOUTUBE_YTDLP_RESOLVE（=0）",
    "direct_youtube_page_ytdlp_off": "直链为 YouTube 页但已关解析",
    "no_video_no_image": "无视频直链且无图、无可用外链",
    "direct_not_mp4_or_cdn_file": "直链非 mp4/CDN 文件形态",
    "unexpected_ineligible_with_image": "异常：有图却判不可分析",
    "other": "其他",
    "eligible": "（不应出现）",
}


def ineligible_reason_label_cn(reason: str) -> str:
    return INELIGIBLE_REASON_LABEL_CN.get(reason, reason)


def classify_ineligible_reason(creative: Dict[str, Any]) -> str:
    """不可进灵感分析时的原因标签（用于日志与 filter_report）。"""
    if is_eligible_for_inspiration_analysis(creative):
        return "eligible"
    direct = pick_video_url_direct(creative)
    img = pick_image_url_direct(creative)
    if img:
        return "unexpected_ineligible_with_image"
    if not direct:
        for key in ("source_url", "landing_url", "page_url"):
            u = str(creative.get(key) or "").strip()
            if u and is_fake_tiktok_placeholder_url(u):
                return "source_fake_tiktok_no_image"
            if u and is_tiktok_landing_url(u) and not is_fake_tiktok_placeholder_url(u):
                if not tiktok_ytdlp_resolve_enabled():
                    return "real_tiktok_page_ytdlp_off"
            if u and is_youtube_page_url(u):
                if not ytdlp_youtube_resolve_enabled():
                    return "real_youtube_page_ytdlp_off"
        return "no_video_no_image"
    if is_fake_tiktok_placeholder_url(direct):
        return "direct_fake_tiktok_no_image"
    if is_tiktok_landing_url(direct) and not tiktok_ytdlp_resolve_enabled():
        return "direct_tiktok_page_ytdlp_off"
    if is_youtube_page_url(direct) and not ytdlp_youtube_resolve_enabled():
        return "direct_youtube_page_ytdlp_off"
    if not is_direct_video_file_url(direct):
        return "direct_not_mp4_or_cdn_file"
    return "other"


def collect_detailed_inspiration_stats(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    全量 items 媒体与准入明细（不删条）。
    字段：item_count、无直链视频、有 mp4 型直链、任意外链含 @test 占位、已 DOM 补全标记、
    eligible/ineligible、ineligible_reasons。
    """
    items = raw.get("items") or []
    out: Dict[str, Any] = {
        "item_count": 0,
        "no_direct_video_url": 0,
        "has_direct_mp4_like": 0,
        "direct_is_tiktok_landing": 0,
        "direct_is_youtube_landing": 0,
        "direct_is_fake_tiktok": 0,
        "any_field_fake_placeholder": 0,
        "dom_detail_enriched_true": 0,
        "eligible": 0,
        "ineligible": 0,
        "ineligible_reasons": {},
    }
    reason_counts: Dict[str, int] = {}
    if not isinstance(items, list):
        return out

    for it in items:
        if not isinstance(it, dict):
            continue
        c = it.get("creative") or {}
        if not isinstance(c, dict):
            continue
        out["item_count"] += 1
        direct = pick_video_url_direct(c)
        if not direct:
            out["no_direct_video_url"] += 1
        else:
            if is_direct_video_file_url(direct):
                out["has_direct_mp4_like"] += 1
            if is_tiktok_landing_url(direct):
                out["direct_is_tiktok_landing"] += 1
            if is_youtube_page_url(direct):
                out["direct_is_youtube_landing"] += 1
            if is_fake_tiktok_placeholder_url(direct):
                out["direct_is_fake_tiktok"] += 1
        fake_any = False
        for key in ("video_url", "source_url", "landing_url", "page_url"):
            u = str(c.get(key) or "").strip()
            if u and is_fake_tiktok_placeholder_url(u):
                fake_any = True
                break
        if fake_any:
            out["any_field_fake_placeholder"] += 1
        if c.get("_dom_detail_enriched") is True:
            out["dom_detail_enriched_true"] += 1
        if is_eligible_for_inspiration_analysis(c):
            out["eligible"] += 1
        else:
            out["ineligible"] += 1
            r = classify_ineligible_reason(c)
            reason_counts[r] = reason_counts.get(r, 0) + 1
    out["ineligible_reasons"] = reason_counts
    return out


def format_inspiration_detail_lines(detail: Dict[str, Any]) -> List[str]:
    """供终端打印的多行说明。"""
    lines = [
        f"  · 总条数: {detail.get('item_count', 0)}",
        f"  · 无 video_url/resource 直链: {detail.get('no_direct_video_url', 0)}",
        f"  · 有 mp4/类 CDN 直链: {detail.get('has_direct_mp4_like', 0)}",
        f"  · 直链为 TikTok 落地页: {detail.get('direct_is_tiktok_landing', 0)}",
        f"  · 直链为 YouTube 落地页: {detail.get('direct_is_youtube_landing', 0)}",
        f"  · 直链为 @test 假链: {detail.get('direct_is_fake_tiktok', 0)}",
        f"  · 任意外链字段含 @test 占位: {detail.get('any_field_fake_placeholder', 0)}",
        f"  · 已标 DOM 详情补全(_dom_detail_enriched): {detail.get('dom_detail_enriched_true', 0)}",
        f"  · 灵感准入「可分析」: {detail.get('eligible', 0)}，「不可」: {detail.get('ineligible', 0)}",
    ]
    reasons = detail.get("ineligible_reasons") or {}
    if isinstance(reasons, dict) and reasons:
        lines.append("  · 不可分析原因分布:")
        order = (
            "source_fake_tiktok_no_image",
            "direct_fake_tiktok_no_image",
            "real_tiktok_page_ytdlp_off",
            "direct_tiktok_page_ytdlp_off",
            "real_youtube_page_ytdlp_off",
            "direct_youtube_page_ytdlp_off",
            "no_video_no_image",
            "direct_not_mp4_or_cdn_file",
            "other",
            "unexpected_ineligible_with_image",
        )
        seen = set(reasons.keys())
        for k in order:
            if k in reasons and reasons[k]:
                lab = ineligible_reason_label_cn(k)
                lines.append(f"      - {k} ({lab}): {reasons[k]}")
                seen.discard(k)
        for k in sorted(seen):
            if reasons.get(k):
                lab = ineligible_reason_label_cn(k)
                lines.append(f"      - {k} ({lab}): {reasons[k]}")
    return lines


def merge_inspiration_filter_stats(raw: Dict[str, Any]) -> Tuple[Dict[str, Any], int, int, int]:
    """写入 filter_report 灵感准入统计 + inspiration_detail，不删改 items。"""
    items = raw.get("items") or []
    if not isinstance(items, list):
        return raw, 0, 0, 0
    total = len(items)
    eligible = 0
    for it in items:
        if not isinstance(it, dict):
            continue
        c = it.get("creative") or {}
        if isinstance(c, dict) and is_eligible_for_inspiration_analysis(c):
            eligible += 1
    skipped = total - eligible
    detail = collect_detailed_inspiration_stats(raw)
    out = dict(raw)
    fr = dict(raw.get("filter_report") or {}) if isinstance(raw.get("filter_report"), dict) else {}
    fr["inspiration_total_items"] = total
    fr["inspiration_eligible_items"] = eligible
    fr["inspiration_skipped_items"] = skipped
    fr["inspiration_detail"] = detail
    out["filter_report"] = fr
    return out, total, eligible, skipped


def _first_real_tiktok_page_from_creative(creative: Dict[str, Any]) -> str:
    """source/landing/page 中第一条非 @test 的 TikTok 页。"""
    if not isinstance(creative, dict):
        return ""
    for key in ("source_url", "landing_url", "page_url"):
        v = str(creative.get(key) or "").strip()
        if v and is_tiktok_landing_url(v) and not is_fake_tiktok_placeholder_url(v):
            return v
    return ""


def _first_youtube_page_from_creative(creative: Dict[str, Any]) -> str:
    """source/landing/page 中第一条 YouTube 页。"""
    if not isinstance(creative, dict):
        return ""
    for key in ("source_url", "landing_url", "page_url"):
        v = str(creative.get(key) or "").strip()
        if v and is_youtube_page_url(v):
            return v
    return ""


def resolve_video_page_to_direct_url(page_url: str) -> str:
    """
    用 yt-dlp -g 将流媒体页解析为可直喂多模态的 URL（TikTok / YouTube 等）。
    """
    page_url = (page_url or "").strip()
    if not page_url:
        return ""

    with _CACHE_LOCK:
        if page_url in _CACHE:
            return _CACHE[page_url]

    bin_path = ytdlp_bin()
    timeout = ytdlp_timeout_sec()
    cmd = [
        bin_path,
        "--no-warnings",
        "--no-playlist",
        "--no-check-certificates",
        "-f",
        "best[ext=mp4]/best[ext=webm]/best",
        "-g",
        page_url,
    ]
    try:
        p = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, "PYTHONUTF8": "1"},
        )
    except FileNotFoundError:
        print(
            "[ytdlp-resolve] 未找到 yt-dlp 可执行文件，请 pip install yt-dlp 或设置 YTDLP_PATH",
            flush=True,
        )
        with _CACHE_LOCK:
            _CACHE[page_url] = ""
        return ""
    except subprocess.TimeoutExpired:
        print(f"[ytdlp-resolve] 超时 ({timeout}s): {page_url[:64]}...", flush=True)
        with _CACHE_LOCK:
            _CACHE[page_url] = ""
        return ""

    if p.returncode != 0:
        err = (p.stderr or p.stdout or "")[:300]
        print(
            f"[ytdlp-resolve] 失败 rc={p.returncode} url={page_url[:64]}... err={err!r}",
            flush=True,
        )
        with _CACHE_LOCK:
            _CACHE[page_url] = ""
        return ""

    lines = [ln.strip() for ln in (p.stdout or "").splitlines() if ln.strip()]
    if not lines:
        with _CACHE_LOCK:
            _CACHE[page_url] = ""
        return ""

    direct = lines[0]
    if not re.match(r"^https?://", direct, re.I):
        with _CACHE_LOCK:
            _CACHE[page_url] = ""
        return ""

    with _CACHE_LOCK:
        _CACHE[page_url] = direct
    return direct


def resolve_tiktok_page_to_direct_url(page_url: str) -> str:
    page_url = (page_url or "").strip()
    if not page_url or not is_tiktok_landing_url(page_url):
        return ""
    return resolve_video_page_to_direct_url(page_url)


def preprocess_video_for_vision(creative: Dict[str, Any]) -> Tuple[str, str]:
    """
    外链预处理：得到供 call_vision 使用的视频直链。
    返回 (vision_url, 来源: cdn | tiktok_resolved | youtube_resolved | empty)。
    """
    if not isinstance(creative, dict):
        return "", "empty"

    direct = pick_video_url_direct(creative)
    if direct:
        if is_fake_tiktok_placeholder_url(direct):
            return "", "empty"
        if is_tiktok_landing_url(direct):
            if tiktok_ytdlp_resolve_enabled():
                r = resolve_tiktok_page_to_direct_url(direct)
                return (r, "tiktok_resolved") if r else ("", "empty")
            return "", "empty"
        if is_youtube_page_url(direct):
            if ytdlp_youtube_resolve_enabled():
                r = resolve_video_page_to_direct_url(direct)
                return (r, "youtube_resolved") if r else ("", "empty")
            return "", "empty"
        if is_direct_video_file_url(direct):
            return direct, "cdn"
        return "", "empty"

    if tiktok_ytdlp_resolve_enabled():
        page = _first_real_tiktok_page_from_creative(creative)
        if page:
            r = resolve_tiktok_page_to_direct_url(page)
            return (r, "tiktok_resolved") if r else ("", "empty")

    if ytdlp_youtube_resolve_enabled():
        page = _first_youtube_page_from_creative(creative)
        if page:
            r = resolve_video_page_to_direct_url(page)
            return (r, "youtube_resolved") if r else ("", "empty")

    return "", "empty"


def display_video_link_for_prompt(creative: Dict[str, Any], vision_url: str) -> str:
    d = pick_video_url_direct(creative)
    if d and not is_tiktok_landing_url(d):
        return d
    page = _first_real_tiktok_page_from_creative(creative)
    if page:
        return page
    page = _first_youtube_page_from_creative(creative)
    if page:
        return page
    return vision_url or ""


# 兼容旧名
effective_video_url_for_vision = preprocess_video_for_vision


def is_creative_analyzable_with_resolve(creative: Dict[str, Any]) -> bool:
    """analyze / 工作流入队：与 is_eligible_for_inspiration_analysis 一致。"""
    return is_eligible_for_inspiration_analysis(creative)


def is_playable_ads_creative(creative: Dict[str, Any]) -> bool:
    """广大大 creative.ads_type=7：试玩 HTML 壳，无 mp4 直链。"""
    if not isinstance(creative, dict):
        return False
    try:
        return int(creative.get("ads_type") or 0) == 7
    except (TypeError, ValueError):
        return False


def pick_playable_html_url(creative: Dict[str, Any]) -> str:
    """试玩：resource_urls[].html_url，或 cdn_url 中的 .html / sp_opera 链接。"""
    if not isinstance(creative, dict):
        return ""
    for r in creative.get("resource_urls") or []:
        if isinstance(r, dict) and str(r.get("html_url") or "").strip():
            return str(r["html_url"]).strip()
    cu = creative.get("cdn_url")
    if isinstance(cu, list):
        for x in cu:
            s = str(x).strip()
            if s and (s.lower().endswith(".html") or "sp_opera" in s.lower()):
                return s
    elif isinstance(cu, str) and cu.strip():
        return cu.strip()
    return ""
