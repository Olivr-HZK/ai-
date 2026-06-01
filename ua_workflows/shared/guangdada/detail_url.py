"""
广大大 detail 页 SPA 链接构建与 ad_key 解析。

URL 格式：
  https://guangdada.net/modules/creative/display-ads/detail?id={ad_key}&type={1|2}&created_at=…&fb_merge=false&search_flag=…

- type=1：Arrow2 竞品 Tab 使用的链接
- type=2：Video Enhancer 主表使用的链接
- 需在已登录广大大的浏览器中打开
"""

from __future__ import annotations

from typing import Any


_BASE = "https://guangdada.net/modules/creative/display-ads/detail"


def try_build_url_spa(
    creative: dict[str, Any],
    *,
    creative_type: int = 1,
) -> str:
    """
    根据素材字典构建广大大 SPA detail 页链接。

    Args:
        creative: 素材 dict，需含 ad_key，可选 created_at / fb_merge_channel / search_flag。
        creative_type: URL 参数 type，1=Arrow2，2=VE 主表。

    Returns:
        拼接好的 URL；ad_key 为空时返回空串。
    """
    ad_key = str(creative.get("ad_key") or "").strip()
    if not ad_key:
        return ""

    parts = [f"id={ad_key}", f"type={creative_type}"]

    created_at = creative.get("created_at")
    if created_at is not None:
        try:
            parts.append(f"created_at={int(created_at)}")
        except (ValueError, TypeError):
            pass

    fb_merge = creative.get("fb_merge_channel")
    if isinstance(fb_merge, list) and fb_merge:
        fb = "true" if fb_merge else "false"
        parts.append(f"fb_merge={fb}")
    else:
        parts.append("fb_merge=false")

    search_flag = creative.get("search_flag")
    if search_flag is not None:
        try:
            parts.append(f"search_flag={int(search_flag)}")
        except (ValueError, TypeError):
            pass

    return f"{_BASE}?{'&'.join(parts)}"


def resolve_ad_key_for_napi(creative: dict[str, Any]) -> str:
    """
    从素材 dict 中提取 ad_key（供 napi 查询等场景使用）。
    优先级：ad_key > creative_id > id > creativeId。
    """
    for key in ("ad_key", "creative_id", "id", "creativeId"):
        v = str(creative.get(key) or "").strip()
        if v:
            return v
    return ""
