"""
广大大 - 创意榜单 → 每周热门榜 → 游戏分类选益智 → 获取周榜全部素材数据

流程：
1. 登录
2. 点击「创意灵感」展开下拉 → 点击「每周热门榜」（或直接打开 hot-charts 页面）
3. 在榜单页找到「游戏分类」，勾选「益智」
4. 监听 napi 接口，收集全部周榜创意数据（含分页/滚动加载）
5. 保存为 hot_charts_yizhi_creatives.json

每条创意中：投放天数 = 字段 days_count（单位：天）。
"""

import asyncio
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page

load_dotenv()

GUANGDADA_BASE_URL = "https://www.guangdada.net"
HOT_CHARTS_URL = f"{GUANGDADA_BASE_URL}/modules/creative/charts/hot-charts"

from guangdada_login import login
from path_util import DATA_DIR


async def _safe_click(page: Page, selector: str, desc: str = "", timeout: int = 8000) -> bool:
    """安全点击，找不到元素不报错"""
    try:
        loc = page.locator(selector)
        if await loc.count() > 0:
            await loc.first.scroll_into_view_if_needed(timeout=timeout)
            await loc.first.click(timeout=timeout)
            print(f"  ✓ 点击: {desc or selector}")
            return True
    except Exception as e:
        print(f"  ✗ 点击失败 {desc or selector}: {e}")
    return False


async def run_hot_charts_yizhi(page: Page, out_dir: Path) -> list:
    """
    执行：创意榜单 → 每周热门榜 → 游戏分类选益智，并采集周榜全部素材。
    仅在勾选「益智」之后才开始采集，确保拿到的是益智周榜数据。
    返回采集到的创意列表（可能来自 creative_list 或 charts 接口）。
    """
    all_creatives = []
    seen_ids = set()
    capture_enabled = False  # 仅在选择益智后为 True 时才采集

    def _append_creatives(lst: list) -> int:
        added = 0
        for item in lst:
            if not isinstance(item, dict):
                continue
            cid = item.get("creative_id") or item.get("id") or str(item)
            if cid in seen_ids:
                continue
            seen_ids.add(cid)
            all_creatives.append(item)
            added += 1
        return added

    async def on_response(response):
        if not capture_enabled:
            return
        url = response.url
        if response.status != 200 or "guangdada.net/napi" not in url:
            return
        try:
            body = await response.json()
            data = body.get("data")
            if not data:
                return
            # 创意列表接口：data.creative_list
            lst = data.get("creative_list") if isinstance(data, dict) else None
            if lst and isinstance(lst, list):
                n = _append_creatives(lst)
                if n:
                    print(f"  [采集-益智榜] +{n} 条 (当前共 {len(all_creatives)} 条)")
                return
            # 榜单接口可能直接返回 list 或 data.list / data.creatives
            if isinstance(data, list):
                n = _append_creatives(data)
                if n:
                    print(f"  [采集-益智榜] +{n} 条 (当前共 {len(all_creatives)} 条)")
                return
            for key in ("list", "creatives", "items", "rank_list"):
                lst = data.get(key) if isinstance(data, dict) else None
                if lst and isinstance(lst, list):
                    n = _append_creatives(lst)
                    if n:
                        print(f"  [采集-益智榜] +{n} 条 [{key}] (当前共 {len(all_creatives)} 条)")
                    return
        except Exception:
            pass

    page.on("response", on_response)

    # 1) 进入创意灵感（先到 creative 再通过菜单进榜单，或直接打开 hot-charts）
    print("  [1/5] 进入创意榜单 - 每周热门榜...")
    await page.goto(GUANGDADA_BASE_URL + "/modules/creative", wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(2000)
    try:
        await page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass

    # 点击「创意灵感」展开下拉（group with 创意灵感 text）
    await page.locator('div.group:has(div:has-text("创意灵感"))').first.hover()
    await page.wait_for_timeout(600)
    # 点击「每周热门榜」链接
    clicked = await _safe_click(
        page,
        'a[href="/modules/creative/charts/hot-charts"], a:has-text("每周热门榜")',
        "每周热门榜",
    )
    if not clicked:
        # 若菜单没点到，直接跳转
        await page.goto(HOT_CHARTS_URL, wait_until="domcontentloaded", timeout=30000)
        print("  ✓ 直接打开每周热门榜页面")
    await page.wait_for_timeout(3000)
    try:
        await page.wait_for_load_state("networkidle", timeout=12000)
    except Exception:
        pass

    # 2) 等待「游戏分类」区域出现
    print("  [2/5] 等待游戏分类区域...")
    try:
        await page.wait_for_selector('text=游戏分类', timeout=15000)
    except Exception:
        pass
    await page.wait_for_timeout(1500)

    # 3) 勾选「益智」—— 只有在此之后才开始采集榜单数据，确保是益智周榜
    print("  [3/5] 选择游戏分类 - 益智...")
    yizhi_sel = 'label.ant-checkbox-wrapper:has(span:has-text("益智")), label:has-text("益智")'
    yizhi_clicked = await _safe_click(page, yizhi_sel, "益智")
    if not yizhi_clicked:
        yizhi_clicked = await _safe_click(page, 'span:has-text("益智")', "益智(span)")
    if yizhi_clicked:
        capture_enabled = True
        print("  ✓ 已开启采集：仅收集益智周榜数据")
    else:
        capture_enabled = True
        print("  ⚠ 未找到益智选项，仍开启采集（可能为默认榜单）")
    await page.wait_for_timeout(3500)
    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
    except Exception:
        pass

    # 4) 滚动加载更多（周榜可能分页或无限滚动）
    print("  [4/5] 尝试滚动加载更多...")
    for _ in range(8):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(2000)
        # 若存在「加载更多」按钮可点击
        load_more = page.locator('button:has-text("加载更多"), [role="button"]:has-text("更多")')
        if await load_more.count() > 0:
            try:
                await load_more.first.click(timeout=3000)
                await page.wait_for_timeout(2500)
            except Exception:
                break

    # 5) 再等一次网络
    print("  [5/5] 等待数据稳定...")
    await page.wait_for_timeout(2000)
    try:
        await page.wait_for_load_state("networkidle", timeout=8000)
    except Exception:
        pass

    return all_creatives


async def main():
    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not email or not password:
        print("错误: 请在 .env 中设置 GUANGDADA_EMAIL 和 GUANGDADA_PASSWORD", file=sys.stderr)
        sys.exit(1)

    out_dir = DATA_DIR
    debug = os.getenv("DEBUG", "").lower() in ("1", "true", "yes")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not debug)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = await context.new_page()
        try:
            print("[1] 登录...")
            if not await login(page, email, password):
                print("登录失败", file=sys.stderr)
                sys.exit(2)
            print("[2] 执行创意榜单 → 每周热门榜 → 益智 并采集...")
            creatives = await run_hot_charts_yizhi(page, out_dir)
            out_file = out_dir / "hot_charts_yizhi_creatives.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(creatives, f, ensure_ascii=False, indent=2)
            print(f"\n已保存 {len(creatives)} 条周榜素材 → {out_file.name}")
            print("  投放天数见每条创意的 days_count 字段（单位：天）")
        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
