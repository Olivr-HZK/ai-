"""
测试：广大大「工具 → 素材 → 最新创意」下，用 DOM 点击卡片详情接口，
能否拿到列表 napi 中 video2pic=1、无 video_url 的素材的「外链视频」等字段。

默认仅跑 AI Mirror（appid = com.ai.polyverse.mirror），与主工作流一致。

用法（项目根目录）：
  .venv/bin/python scripts/test_dom_video_url_ai_mirror.py
  DEBUG=1 .venv/bin/python scripts/test_dom_video_url_ai_mirror.py --limit 5

输出：
  data/test_dom_ai_mirror_<timestamp>.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from playwright.async_api import Page, async_playwright

load_dotenv()

# 脚本在 scripts/ 下运行
sys.path.insert(0, str(Path(__file__).resolve().parent))

from guangdada_login import login
from path_util import DATA_DIR
from proxy_util import prepare_playwright_proxy_for_crawl

from run_search_workflow import (
    _all_creatives_from_batches,
    _do_setup,
    _extract_creative_lists,
    _search_one_keyword,
)

APPID_AI_MIRROR = "com.ai.polyverse.mirror"


def _pick_video_url(creative: dict[str, Any]) -> str:
    if creative.get("video_url"):
        return str(creative["video_url"])
    for r in creative.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("video_url"):
            return str(r["video_url"])
    return ""


def _extract_urls_for_report(c: dict[str, Any]) -> dict[str, Any]:
    """便于对比：视频直链、资源数组、常见外链字段。"""
    ru = c.get("resource_urls") or []
    flat_v = []
    flat_i = []
    if isinstance(ru, list):
        for r in ru:
            if not isinstance(r, dict):
                continue
            if r.get("video_url"):
                flat_v.append(str(r["video_url"]))
            if r.get("image_url"):
                flat_i.append(str(r["image_url"]))
    return {
        "ad_key": str(c.get("ad_key") or ""),
        "video2pic": c.get("video2pic"),
        "video_duration": c.get("video_duration"),
        "video_url_top": str(c.get("video_url") or ""),
        "resource_urls_video": flat_v,
        "resource_urls_image": flat_i[:3],
        "preview_img_url": (str(c.get("preview_img_url") or ""))[:120],
        "source_url": str(c.get("source_url") or ""),
        "landing_url": str(c.get("landing_url") or ""),
        "page_url": str(c.get("page_url") or ""),
        "has_playable_video_field": bool(_pick_video_url(c)),
    }


async def _click_card_detail_raw(
    page: Page,
    dom_idx: int,
    timeout_ms: int = 15000,
    target_ad_key: str | None = None,
) -> dict[str, Any] | None:
    """
    点击第 dom_idx 张卡片，从 guangdada JSON 响应里解析 creative。
    若提供 target_ad_key，优先返回与该 ad_key 一致的一条（避免混入其它接口的列表）。
    """
    detail_holder: list[dict[str, Any]] = []

    async def _on_resp(response):
        url = response.url or ""
        if response.status != 200:
            return
        if "guangdada" not in url.lower():
            return
        try:
            body = await response.json()
        except Exception:
            return
        for lst in _extract_creative_lists(body):
            if isinstance(lst, list):
                for item in lst:
                    if isinstance(item, dict) and item.get("ad_key"):
                        detail_holder.append(item)
        if isinstance(body, dict):
            if body.get("ad_key"):
                detail_holder.append(body)
            for key in ("creative", "data", "result", "info", "item"):
                sub = body.get(key)
                if isinstance(sub, dict) and sub.get("ad_key"):
                    detail_holder.append(sub)
                if isinstance(sub, dict):
                    inner = sub.get("creative") or sub.get("detail")
                    if isinstance(inner, dict) and inner.get("ad_key"):
                        detail_holder.append(inner)

    page.on("response", _on_resp)
    try:
        # 优先点播放区（与页面交互一致），否则点整张卡
        clicked = await page.evaluate(
            f"""
() => {{
  const cards = document.querySelectorAll('.shadow-common-light.bg-white');
  const card = cards[{dom_idx}];
  if (!card) return false;
  const play = card.querySelector('[class*="play-simple"]') || card.querySelector('[class*="play"]');
  (play || card).click();
  return true;
}}
"""
        )
        if not clicked:
            return None
        await page.wait_for_timeout(400)
        waited = 0
        while waited < timeout_ms:
            if detail_holder:
                break
            await page.wait_for_timeout(200)
            waited += 200
        try:
            close = page.locator('button[aria-label="Close"], .ant-modal-close').first
            if await close.count() > 0:
                await close.click(timeout=800)
            else:
                await page.keyboard.press("Escape")
        except Exception:
            await page.keyboard.press("Escape")
        await page.wait_for_timeout(400)
        if not detail_holder:
            return None
        if target_ad_key:
            for item in detail_holder:
                if str(item.get("ad_key") or "") == str(target_ad_key):
                    return item
            # 详情结构可能与列表 ad_key 不一致时仍返回首条供对比
            return detail_holder[0]
        return detail_holder[0]
    finally:
        page.remove_listener("response", _on_resp)


async def _find_dom_idx_by_ad_key(
    page: Page,
    ad_key: str,
    allowed_indices: list[int] | None = None,
) -> tuple[int | None, str]:
    """
    在列表卡片上定位 ad_key。
    1) 优先：任意节点 **属性值** 与 ad_key 完全一致（32 位 hex，不区分大小写）。
    2) 否则：outerHTML 包含该 ad_key 子串。

    allowed_indices：只在这些卡片下标内查找（例如已按列表「日期区间」标签筛过）。

    返回 (dom_idx 或 None, 'attr' | 'outer_html' | '')。
    """
    ak = str(ad_key or "").strip().lower()
    if len(ak) < 16:
        return None, ""
    raw = await page.evaluate(
        """({ target, allowed }) => {
  const t = (target || '').toLowerCase();
  if (!t) return [-1, ''];
  const cards = document.querySelectorAll('.shadow-common-light.bg-white');
  const allowSet = Array.isArray(allowed) && allowed.length
    ? new Set(allowed.map(Number))
    : null;
  const inAllow = (i) => !allowSet || allowSet.has(i);

  for (let i = 0; i < cards.length; i++) {
    if (!inAllow(i)) continue;
    const card = cards[i];
    const stack = [card, ...card.querySelectorAll('*')];
    for (const el of stack) {
      if (!el || !el.attributes) continue;
      for (const a of el.attributes) {
        const v = (a.value || '').trim().toLowerCase();
        if (v === t) return [i, 'attr'];
      }
    }
  }
  for (let i = 0; i < cards.length; i++) {
    if (!inAllow(i)) continue;
    if (cards[i].outerHTML.toLowerCase().includes(t)) return [i, 'outer_html'];
  }
  return [-1, ''];
}""",
        {"target": ak, "allowed": allowed_indices},
    )
    if isinstance(raw, list) and len(raw) >= 2:
        idx, how = raw[0], str(raw[1] or "")
        if isinstance(idx, int) and idx >= 0:
            return idx, how
    return None, ""


async def _list_dom_preview_by_index(page: Page) -> list[tuple[int, str]]:
    """每张卡片对应 preview（sp_opera 优先）。"""
    raw = await page.evaluate(
        r"""() => {
  const cards = document.querySelectorAll('.shadow-common-light.bg-white');
  const out = [];
  for (let i = 0; i < cards.length; i++) {
    const imgs = cards[i].querySelectorAll('img');
    let src = '';
    const sp = Array.from(imgs).find(img => img.src && img.src.includes('sp_opera'));
    if (sp) src = sp.src.split('?')[0];
    else {
      const lazy = Array.from(imgs).find(img => img.dataset.src || img.currentSrc);
      if (lazy) src = (lazy.dataset.src || lazy.currentSrc || '').split('?')[0];
    }
    out.push([i, src]);
  }
  return out;
}"""
    )
    out: list[tuple[int, str]] = []
    if isinstance(raw, list):
        for row in raw:
            if isinstance(row, list) and len(row) >= 2:
                out.append((int(row[0]), str(row[1] or "")))
    return out


def _norm_preview(u: str) -> str:
    """列表与 DOM 可能分别为 .image / .png，只比路径主体。"""
    u = str(u or "").split("?")[0].strip()
    if u.endswith(".image"):
        u = u[: -len(".image")]
    elif u.endswith(".png"):
        u = u[: -len(".png")]
    return u


async def run(args: argparse.Namespace) -> None:
    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not email or not password:
        print("[错误] 请配置 .env 中 GUANGDADA_EMAIL / GUANGDADA_PASSWORD", file=sys.stderr)
        sys.exit(1)

    keyword = (args.appid or APPID_AI_MIRROR).strip()
    batches_ref: list = []
    capture_state: dict[str, Any] = {"enabled": False}

    async def on_response(response):
        if not capture_state.get("enabled"):
            return
        url = response.url or ""
        if "guangdada.net/napi" not in url or response.status != 200:
            return
        try:
            body = await response.json()
        except Exception:
            return
        for lst in _extract_creative_lists(body):
            if isinstance(lst, list) and lst:
                batches_ref.append(lst)
        if len(batches_ref) > 80:
            batches_ref.pop(0)

    playwright_proxy = prepare_playwright_proxy_for_crawl()
    debug = bool(os.environ.get("DEBUG"))

    results_out: dict[str, Any] = {
        "keyword": keyword,
        "test": "dom_detail_vs_list_api_video_fields",
        "samples": [],
    }

    async with async_playwright() as p:
        launch_kw: dict[str, Any] = {"headless": not debug}
        if playwright_proxy:
            launch_kw["proxy"] = playwright_proxy
        browser = await p.chromium.launch(**launch_kw)
        context = await browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = await context.new_page()
        page.on("response", on_response)

        try:
            print("[1/3] 登录...")
            if not await login(page, email, password):
                print("[失败] 登录失败", file=sys.stderr)
                sys.exit(2)
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            await page.wait_for_timeout(3000)

            print("[2/3] 筛选：工具 / 7天 / 素材 / 最新创意")
            await _do_setup(page, is_tool=True, log_prefix="  ", order_by="latest", use_popularity_top1=False)

            print(f"[3/3] 搜索关键词: {keyword}")
            await _search_one_keyword(
                page,
                keyword,
                batches_ref,
                capture_state,
                order_by="latest",
                log_prefix="    ",
            )

            napi = _all_creatives_from_batches(batches_ref)
            print(f"    [napi] 合并去重后约 {len(napi)} 条素材")

            # 目标：video2pic=1 且列表层无 video_url
            targets: list[dict[str, Any]] = []
            for c in napi:
                if not isinstance(c, dict):
                    continue
                if c.get("video2pic") != 1:
                    continue
                if _pick_video_url(c):
                    continue
                targets.append(c)
            if not targets:
                print("    [提示] 未找到 video2pic=1 且无 video_url 的条目，退化为：无 video_url 的前几条")
                for c in napi:
                    if isinstance(c, dict) and not _pick_video_url(c):
                        targets.append(c)
                    if len(targets) >= args.limit * 2:
                        break

            dom_map = await _list_dom_preview_by_index(page)
            prev_to_idx: dict[str, int] = {}
            for idx, src in dom_map:
                if src:
                    prev_to_idx[_norm_preview(src)] = idx

            tested = 0
            for c in targets:
                if tested >= args.limit:
                    break
                prev = _norm_preview(str(c.get("preview_img_url") or ""))
                if not prev:
                    print(f"    [跳过] ad_key={c.get('ad_key')} 无 preview_img_url")
                    continue
                dom_idx = prev_to_idx.get(prev)
                if dom_idx is None:
                    # 模糊：只比路径尾段
                    tail = prev.split("/")[-1][:20]
                    for idx, src in dom_map:
                        if tail and tail in src:
                            dom_idx = idx
                            break
                if dom_idx is None:
                    print(f"    [跳过] 无法在 DOM 中匹配 preview: {prev[:80]}...")
                    continue

                list_report = _extract_urls_for_report(c)
                print(f"    [点击] dom_idx={dom_idx} ad_key={c.get('ad_key')}")

                detail = await _click_card_detail_raw(
                    page, dom_idx, target_ad_key=str(c.get("ad_key") or "")
                )
                if not detail:
                    results_out["samples"].append(
                        {
                            "dom_idx": dom_idx,
                            "list_api": list_report,
                            "detail_api": None,
                            "note": "详情响应未解析到 creative",
                        }
                    )
                    tested += 1
                    continue

                detail_report = _extract_urls_for_report(detail)
                gained_video = (not list_report["has_playable_video_field"]) and detail_report[
                    "has_playable_video_field"
                ]
                results_out["samples"].append(
                    {
                        "dom_idx": dom_idx,
                        "list_api": list_report,
                        "detail_api": detail_report,
                        "detail_gained_playable_video": gained_video,
                        "same_ad_key": str(c.get("ad_key")) == str(detail.get("ad_key")),
                    }
                )
                tested += 1
                if gained_video:
                    print(f"      → 详情接口补全了可播放 video_url ✓")
                else:
                    print(f"      → 详情仍无 video_url（与列表一致或仅有外链字段）")

        finally:
            await browser.close()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = DATA_DIR / f"test_dom_ai_mirror_{ts}.json"
    out_path.write_text(json.dumps(results_out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[输出] {out_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description="DOM 点击详情 vs 列表 API — AI Mirror 视频外链探测")
    ap.add_argument("--appid", default=APPID_AI_MIRROR, help="搜索关键词（默认 AI Mirror appid）")
    ap.add_argument("--limit", type=int, default=5, help="最多测试几条素材")
    args = ap.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
