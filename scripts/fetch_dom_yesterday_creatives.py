"""
纯 DOM 工作流：不依赖 napi 响应，直接从页面 DOM 读取广告卡片，
筛选「投放首日 = 昨天」的卡片，逐一点击获取详情，写入 JSON。

流程：
  登录 → 搜索 appid → 设置（工具/7天/素材/最新） → 滚动加载全部卡片
  → 从 DOM 提取所有卡片 → 按首日期过滤昨天 → 点击每张卡片收集详情响应
  → 写入 data/dom_yesterday_<product>_<date>.json

用法：
  python scripts/fetch_dom_yesterday_creatives.py --product "UpFoto - AI Photo Enhancer"
  python scripts/fetch_dom_yesterday_creatives.py --product "Remini - AI Photo Enhancer" --date 2026-03-22
  DEBUG=1 python scripts/fetch_dom_yesterday_creatives.py --product "UpFoto - AI Photo Enhancer"
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from path_util import CONFIG_DIR, DATA_DIR
from guangdada_login import login
from proxy_util import prepare_playwright_proxy_for_crawl
from run_search_workflow import _do_setup, _extract_creative_lists

CONFIG_FILE = CONFIG_DIR / "ai_product.json"


# ─── 工具函数 ──────────────────────────────────────────────────────────────────

def _load_product(product_name: str) -> tuple[str, str]:
    """返回 (category, appid)"""
    data = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    for cat, items in (data.items() if isinstance(data, dict) else []):
        if not isinstance(items, dict):
            continue
        for name, appid in items.items():
            if name == product_name and str(appid or "").strip():
                return str(cat), str(appid)
    raise ValueError(f"找不到产品 '{product_name}'，请检查 config/ai_product.json")


def _safe_name(s: str) -> str:
    s = re.sub(r"\s+", "_", s.strip())
    return re.sub(r"[^a-zA-Z0-9_\-]", "", s)[:40]


def _yesterday(ref: str | None = None) -> str:
    if ref:
        return (date.fromisoformat(ref) - timedelta(days=1)).isoformat()
    return (date.today() - timedelta(days=1)).isoformat()


# ─── DOM 提取（所有卡片） ──────────────────────────────────────────────────────

async def _extract_all_dom_cards(page) -> list[dict]:
    """从页面提取所有可见创意卡片的基础信息（不依赖 napi）。"""
    try:
        return await page.evaluate(r"""
() => {
  const results = [];
  const cardEls = document.querySelectorAll('.shadow-common-light.bg-white');
  cardEls.forEach((card, idx) => {
    try {
      // 预览图
      const imgs = Array.from(card.querySelectorAll('img'));
      const spImg = imgs.find(img => img.src && img.src.includes('sp_opera'));
      let previewSrc = spImg ? spImg.src.split('?')[0] : '';
      if (!previewSrc) {
        const lazy = imgs.find(img => !img.src.includes('appcdn-global') && (img.dataset.src || img.currentSrc));
        if (lazy) previewSrc = (lazy.dataset.src || lazy.currentSrc || '').split('?')[0];
      }

      // 广告主
      const advEl = card.querySelector('.leading-\\[18px\\] span span');
      const advertiserName = advEl ? advEl.textContent.trim() : '';

      // 平台
      const isYouTube = !!card.querySelector('.net-icon-youtube');
      const platform = isYouTube ? 'youtube' : 'admob';

      // 视频时长
      let videoDuration = null;
      const playArea = card.querySelector('[class*="play-simple"]');
      if (playArea) {
        const txt = (playArea.parentElement || playArea).textContent.trim();
        const m = txt.match(/(\d+)s/);
        videoDuration = m ? parseInt(m[1]) : 0;
      }

      // 标签（重投 + 日期区间）
      const tagEls = Array.from(card.querySelectorAll('.ant-tag'));
      const tags = tagEls.map(t => t.textContent.trim());
      const dateRange = tags.find(t => t.includes('~')) || '';
      const isRelaunch = tags.some(t => t === '重投');

      // 指标
      const bolds = Array.from(card.querySelectorAll('.font-semibold')).map(el => el.textContent.trim());
      const impression = bolds[0] ? parseInt(bolds[0].replace(/[^0-9]/g, '')) || 0 : 0;

      let heat = 0, allExposure = 0;
      Array.from(card.querySelectorAll('.rounded-full')).forEach(el => {
        const t = el.textContent.trim();
        const hm = t.match(/热度[:：]\s*([\d.]+)([KkMm]?)/);
        if (hm) {
          const v = parseFloat(hm[1]), u = hm[2].toUpperCase();
          heat = u === 'K' ? Math.round(v*1000) : u === 'M' ? Math.round(v*1000000) : v;
        }
        const em = t.match(/展示估值[:：]\s*([\d.]+)([KkMm]?)/);
        if (em) {
          const v = parseFloat(em[1]), u = em[2].toUpperCase();
          allExposure = u === 'K' ? Math.round(v*1000) : u === 'M' ? Math.round(v*1000000) : v;
        }
      });

      // 标题（下方广告主行，有时显示广告文案）
      const bottomAdvEls = card.querySelectorAll('.text-xs .whitespace-nowrap span span');
      const pageTitle = bottomAdvEls.length > 0
        ? bottomAdvEls[bottomAdvEls.length - 1].textContent.trim()
        : advertiserName;

      results.push({
        _dom_idx: idx,
        preview_img_url: previewSrc,
        advertiser_name: advertiserName || pageTitle,
        page_name: pageTitle,
        platform: platform,
        video_duration: videoDuration,
        heat: heat,
        all_exposure_value: allExposure,
        impression: impression,
        resume_advertising_flag: isRelaunch,
        date_range_text: dateRange,
      });
    } catch(e) {}
  });
  return results;
}
""") or []
    except Exception as e:
        print(f"[DOM提取] 失败: {e}", file=sys.stderr)
        return []


# ─── 过滤首日期 = target_date 的卡片 ──────────────────────────────────────────

def _filter_by_start_date(cards: list[dict], target_date: str) -> list[dict]:
    """
    date_range_text 格式: "YYYY-MM-DD~YYYY-MM-DD"
    只保留首日期（~左边）== target_date 的卡片。
    """
    matched = []
    for c in cards:
        dr = str(c.get("date_range_text") or "")
        start = dr.split("~")[0].strip()
        if start == target_date:
            matched.append(c)
    return matched


# ─── 点击卡片获取详情 ──────────────────────────────────────────────────────────

async def _click_card_for_detail(page, dom_idx: int, timeout_ms: int = 5000) -> dict | None:
    """
    点击第 dom_idx 张卡片，监听所有 guangdada.net 响应，
    返回第一个包含完整 creative 数据的响应体（有 ad_key）；失败返回 None。
    """
    detail_holder: list[dict] = []

    async def _on_resp(response):
        url = response.url or ""
        if "guangdada" not in url or response.status != 200:
            return
        try:
            body = await response.json()
        except Exception:
            return
        # 先用现有的递归查找逻辑
        lists = _extract_creative_lists(body)
        for lst in lists:
            for item in lst:
                if isinstance(item, dict) and item.get("ad_key"):
                    detail_holder.append(item)
        # 兜底：如果顶层就是一个带 ad_key 的 dict
        if not detail_holder and isinstance(body, dict):
            if body.get("ad_key"):
                detail_holder.append(body)
            # 或者在 data/result 字段里
            for key in ("data", "result", "creative", "info"):
                sub = body.get(key)
                if isinstance(sub, dict) and sub.get("ad_key"):
                    detail_holder.append(sub)
                    break

    page.on("response", _on_resp)
    try:
        clicked = await page.evaluate(f"""
() => {{
  const cards = document.querySelectorAll('.shadow-common-light.bg-white');
  const card = cards[{dom_idx}];
  if (!card) return false;
  card.click();
  return true;
}}
""")
        if not clicked:
            return None

        # 等待响应（最多 timeout_ms）
        waited = 0
        while waited < timeout_ms:
            if detail_holder:
                break
            await page.wait_for_timeout(200)
            waited += 200

        # 关闭弹窗
        try:
            close = page.locator('button[aria-label="Close"], .ant-modal-close').first
            if await close.count() > 0:
                await close.click(timeout=800)
            else:
                await page.keyboard.press("Escape")
        except Exception:
            await page.keyboard.press("Escape")
        await page.wait_for_timeout(400)

        return detail_holder[0] if detail_holder else None

    finally:
        page.remove_listener("response", _on_resp)


# ─── 搜索关键词并等待页面加载 ──────────────────────────────────────────────────

async def _search_and_load(page, keyword: str) -> None:
    """在已设置好筛选的页面里输入关键词并搜索，然后滚动加载全部卡片。"""
    search_selectors = [
        "#display-search-input-container input#rc_select_1",
        "#display-search-input-container input[role='combobox']",
        "#display-search-input-container input.ant-select-selection-search-input",
        "input#display-search-input",
        "input[role='combobox']",
    ]
    inp = None
    for sel in search_selectors:
        loc = page.locator(sel).first
        if await loc.count() > 0:
            inp = loc
            print(f"[搜索] 找到输入框: {sel}")
            break
    if inp is None:
        raise RuntimeError("未找到搜索输入框")

    await inp.click()
    await inp.fill("")
    await page.wait_for_timeout(300)
    await inp.fill(keyword)
    await page.wait_for_timeout(500)
    await page.keyboard.press("Enter")
    await page.wait_for_timeout(2500)

    # 选「最新创意」
    for sel in ["text=最新创意", "li:has-text('最新创意')", "div:has-text('最新创意')"]:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                await loc.click(timeout=2000)
                print("[搜索] 已选最新创意 ✓")
                await page.wait_for_timeout(1500)
                break
        except Exception:
            continue

    # 滚动加载全部卡片
    print("[加载] 滚动加载全部卡片...")
    idle = 0
    last_count = 0
    for _ in range(20):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(1200)
        cur = await page.evaluate(
            "() => document.querySelectorAll('.shadow-common-light.bg-white').length"
        )
        if cur == last_count:
            idle += 1
            if idle >= 3:
                break
        else:
            idle = 0
            last_count = cur
    print(f"[加载] 页面卡片总数: {last_count}")


# ─── 主流程 ───────────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(description="纯 DOM 工作流：抓取首日期=昨天的广告卡片详情")
    parser.add_argument("--product", required=True, help="产品名（需与 config/ai_product.json 完全一致）")
    parser.add_argument("--date", default=None, metavar="YYYY-MM-DD",
                        help="目标日期（默认昨天）；卡片首日期需等于此日期")
    args = parser.parse_args()

    target_date = args.date or _yesterday()
    category, appid = _load_product(args.product)

    print(f"[产品] {args.product}  appid={appid}")
    print(f"[目标] 首日期 = {target_date}（纯 DOM，不依赖 napi）")

    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not email or not password:
        print("[错误] 请在 .env 设置 GUANGDADA_EMAIL 和 GUANGDADA_PASSWORD", file=sys.stderr)
        sys.exit(1)

    proxy = prepare_playwright_proxy_for_crawl()
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        launch_kw: dict = {"headless": not bool(os.environ.get("DEBUG"))}
        if proxy:
            launch_kw["proxy"] = proxy
        browser = await p.chromium.launch(**launch_kw)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = await context.new_page()

        try:
            # 1) 登录
            print("[1] 登录...")
            ok = await login(page, email, password)
            if not ok:
                print("[错误] 登录失败", file=sys.stderr)
                sys.exit(1)
            print("[1] 登录成功 ✓")

            # 2) 设置筛选（工具/7天/素材）
            print("[2] 设置筛选...")
            await _do_setup(page, is_tool=True, log_prefix="  ", order_by="latest")
            print("[2] 设置完成 ✓")

            # 3) 搜索 + 滚动加载
            print(f"[3] 搜索 {appid}...")
            await _search_and_load(page, appid)

            # 4) DOM 提取全部卡片
            all_cards = await _extract_all_dom_cards(page)
            print(f"[4] DOM 提取卡片总数: {len(all_cards)}")

            # 5) 按首日期过滤
            matched = _filter_by_start_date(all_cards, target_date)
            print(f"[5] 首日期={target_date} 命中: {len(matched)} 张")
            for i, c in enumerate(matched, 1):
                print(f"    [{i:02d}] dom_idx={c['_dom_idx']}  platform={c['platform']}  "
                      f"热度={c['heat']}  估值={c['all_exposure_value']}  "
                      f"date_range={c['date_range_text']}  重投={c['resume_advertising_flag']}")

            if not matched:
                print("[完成] 没有命中卡片，不写入文件。")
                return

            # 6) 逐一点击获取详情
            print(f"[6] 逐一点击 {len(matched)} 张卡片获取详情...")
            results = []
            for i, card in enumerate(matched, 1):
                dom_idx = card["_dom_idx"]
                print(f"    [{i}/{len(matched)}] 点击 dom_idx={dom_idx}  date_range={card['date_range_text']}", end="  ")
                detail = await _click_card_for_detail(page, dom_idx)
                if detail:
                    # 用 detail 覆盖 dom_basic，保留 dom 补充字段
                    merged = {**card, **detail, "_source": "dom_detail"}
                    results.append(merged)
                    print(f"✓ ad_key={str(detail.get('ad_key',''))[:16]}  "
                          f"first_seen={detail.get('first_seen','-')}")
                else:
                    card["_source"] = "dom_basic"
                    results.append(card)
                    print("✗ 未获取到详情（保留 dom_basic）")

            # 7) 写入 JSON
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            out_path = DATA_DIR / f"dom_yesterday_{_safe_name(args.product)}_{target_date}.json"
            out_path.write_text(
                json.dumps({
                    "target_date": target_date,
                    "product": args.product,
                    "category": category,
                    "appid": appid,
                    "total": len(results),
                    "dom_detail_count": sum(1 for r in results if r.get("_source") == "dom_detail"),
                    "dom_basic_count": sum(1 for r in results if r.get("_source") == "dom_basic"),
                    "creatives": results,
                }, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"\n[输出] {out_path.name}（共 {len(results)} 条，"
                  f"详情={sum(1 for r in results if r.get('_source')=='dom_detail')}，"
                  f"仅基础={sum(1 for r in results if r.get('_source')=='dom_basic')}）")

        finally:
            await browser.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("中断。", file=sys.stderr)
