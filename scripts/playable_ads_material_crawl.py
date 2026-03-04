"""
广大大 - 试玩广告素材爬虫

流程（遵循现有爬虫逻辑）：
1. 登录（guangdada_login）
2. 侧边栏点击「试玩广告」（参考 sidebar.html）
3. 分类中点击「益智」（参考 category.html）
4. 日期中点击「七天」（参考 date.html）
5. 筛选点击「素材」（参考 素材.html）
6. 点击列表中的卡片，获取卡片上的信息及试玩 URL（参考 素材卡片.html 结构）

结果保存到 data/playable_ads_material_cards.json
"""

import asyncio
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page

from guangdada_login import login
from path_util import DATA_DIR

load_dotenv()

GUANGDADA_BASE_URL = "https://www.guangdada.net"
PLAYABLE_ADS_URL = f"{GUANGDADA_BASE_URL}/modules/creative/playable-ads"


async def _safe_click(page: Page, selector: str, desc: str = "", timeout: int = 8000) -> bool:
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


async def run_playable_material_crawl(page: Page, out_path: Path, max_cards: int = 50) -> list:
    """
    执行：登录后 → 试玩广告 → 益智 → 7天 → 素材 → 点击卡片并采集信息与试玩 URL。
    """
    results = []

    # 1) 进入试玩广告页（侧边栏「试玩广告」）
    print("  [1/6] 打开试玩广告页...")
    await page.goto(PLAYABLE_ADS_URL, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(2000)
    try:
        await page.wait_for_load_state("networkidle", timeout=12000)
    except Exception:
        pass

    # 若当前不在试玩广告，点击侧边栏「试玩广告」（参考 sidebar.html：li 内 span.ant-menu-title-content）
    menu_clicked = await _safe_click(
        page,
        'li.ant-menu-item:has(span.ant-menu-title-content:has-text("试玩广告"))',
        "侧边栏-试玩广告",
    )
    if not menu_clicked:
        await _safe_click(page, 'li:has(span:has-text("试玩广告"))', "侧边栏-试玩广告(备用)")
    await page.wait_for_timeout(2500)
    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
    except Exception:
        pass

    # 2) 分类中点击「益智」（参考 category.html：checkbox-wrapper 内 span 益智）
    print("  [2/6] 选择分类 - 益智...")
    yizhi_sel = 'label.ant-checkbox-wrapper:has(span:has-text("益智")), label:has-text("益智")'
    await _safe_click(page, yizhi_sel, "益智")
    await page.wait_for_timeout(2000)

    # 3) 日期中点击「7天」（参考 date.html：radio value="7"）
    print("  [3/6] 选择日期 - 7天...")
    date_sel = 'label.ant-radio-button-wrapper:has(input[value="7"]), label:has(span:has-text("7天"))'
    await _safe_click(page, date_sel, "7天")
    await page.wait_for_timeout(2000)

    # 4) 筛选点击「素材」（参考 素材.html：filter_duplicate_removal 下 value="1" 的 radio，文案「素材」）
    print("  [4/6] 选择类型 - 素材...")
    material_sel = (
        '#filter_duplicate_removal label.ant-radio-button-wrapper:has(span:has-text("素材")), '
        '#filter_duplicate_removal label:has(input[value="1"])'
    )
    await _safe_click(page, material_sel, "素材")
    await page.wait_for_timeout(3000)
    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
    except Exception:
        pass

    # 5) 等待卡片列表出现（参考 素材卡片.html：带 shadow-common-light、cursor-pointer 的卡片）
    print("  [5/6] 等待素材卡片列表，并滚动加载一整页...")
    # 注意：这里不再强制要求卡片内部一定有 iframe，避免只命中极少数卡片
    card_outer_selector = 'div.shadow-common-light.cursor-pointer'
    try:
        await page.wait_for_selector(card_outer_selector, timeout=15000)
    except Exception:
        card_outer_selector = 'div[class*="shadow-common-light"][class*="cursor-pointer"]'
        try:
            await page.wait_for_selector(card_outer_selector, timeout=8000)
        except Exception:
            print("  ⚠ 未检测到卡片列表，尝试滚动并继续")
    await page.wait_for_timeout(2000)

    # 尝试向下滚动多次，确保当前这一页的卡片都加载出来
    try:
        cards_loc_for_scroll = page.locator(card_outer_selector)
        last_count = await cards_loc_for_scroll.count()
        for _ in range(10):
            await page.evaluate("window.scrollBy(0, window.innerHeight);")
            await page.wait_for_timeout(1200)
            cur_count = await cards_loc_for_scroll.count()
            # 若数量不再增加，则认为这一页已经加载完
            if cur_count <= last_count:
                break
            last_count = cur_count
    except Exception:
        pass

    # 6) 获取所有卡片并逐个点击、提取信息与试玩 URL
    print("  [6/6] 点击卡片并采集信息与试玩 URL...")
    cards_loc = page.locator(card_outer_selector)
    n_cards = await cards_loc.count()
    if n_cards == 0:
        # 兜底：只要是 cursor-pointer 的卡片都尝试
        cards_loc = page.locator('div[class*="cursor-pointer"]')
        n_cards = await cards_loc.count()

    print(f"  → 当前 DOM 中检测到卡片元素数量: {n_cards}")
    if n_cards == 0:
        print("  ⚠ 未检测到任何卡片 DOM，请确认筛选条件下页面是否真的有卡片显示，以及列表是否在其他滚动容器内。")
        return results

    n_cards = min(n_cards, max_cards)
    print(f"  → 实际将遍历的卡片数量（受 MAX_PLAYABLE_CARDS 限制）: {n_cards}")

    for i in range(n_cards):
        card = cards_loc.nth(i)
        try:
            await card.scroll_into_view_if_needed(timeout=5000)
            await page.wait_for_timeout(500)
            # 尝试点击卡片（便于展开/选中，并确保 iframe 可能加载）
            # 若点击失败，不再中断该卡片的解析，仅记录日志后直接读取当前 DOM 信息
            try:
                await card.click(timeout=5000)
                await page.wait_for_timeout(800)
            except Exception as e_click:
                print(f"    卡片 {i+1}/{n_cards} 点击失败（继续解析当前卡片 DOM）: {e_click}")
        except Exception as e:
            print(f"    卡片 {i+1}/{n_cards} 滚动/点击阶段异常（继续尝试解析）: {e}")

        try:
            # 从当前卡片 DOM 提取信息（参考 素材卡片.html 结构）
            # 标题：头部第一个长文本（游戏/应用名）
            title = ""
            for sel in ('span.hover\\:text-primary span', 'div.whitespace-nowrap.overflow-hidden span', 'div.mr-7 span'):
                el = card.locator(sel).first
                if await el.count() > 0:
                    t = (await el.text_content() or "").strip()
                    if t and len(t) > 1 and len(t) < 120:
                        title = t
                        break

            # 开发者：头部区域图标后的小字文本（.text-xs 或 .ml-1 内）
            developer = ""
            for sel in ('div.text-xs span', 'div.ml-1 span', 'div.flex-1.w-0.ml-1 span'):
                el = card.locator(sel).first
                if await el.count() > 0:
                    t = (await el.text_content() or "").strip()
                    if t and t != title and len(t) < 80:
                        developer = t
                        break

            # 试玩 URL：卡片内 iframe 的 src（.html）
            playable_url = ""
            iframe = card.locator('iframe[src*=".html"]').first
            if await iframe.count() > 0:
                playable_url = (await iframe.get_attribute("src")) or ""

            # 人气值、投放天数、最后看见：底部 grid 三列
            pop_value = ""
            days_value = ""
            last_seen = ""
            grid = card.locator('div.grid.grid-cols-3 div.font-semibold')
            if await grid.count() >= 3:
                pop_value = (await grid.nth(0).text_content() or "").strip()
                days_value = (await grid.nth(1).text_content() or "").strip()
                last_seen = (await grid.nth(2).text_content() or "").strip()

            item = {
                "title": title,
                "developer": developer,
                "playable_url": playable_url,
                "人气值": pop_value,
                "投放天数": days_value,
                "最后看见": last_seen,
                "index": i + 1,
            }
            results.append(item)
            print(f"    [{i+1}/{n_cards}] {title or '(无标题)'} | 试玩: {playable_url[:50] + '...' if len(playable_url) > 50 else playable_url or '-'}")

        except Exception as e:
            print(f"    卡片 {i+1}/{n_cards} 解析失败: {e}")
            results.append({"index": i + 1, "error": str(e), "title": "", "developer": "", "playable_url": ""})

    return results


async def main():
    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not email or not password:
        print("错误: 请在 .env 中设置 GUANGDADA_EMAIL 和 GUANGDADA_PASSWORD", file=sys.stderr)
        sys.exit(1)

    out_path = DATA_DIR / "playable_ads_material_cards.json"
    debug = os.getenv("DEBUG", "").strip().lower() in ("1", "true", "yes")
    max_cards = int(os.getenv("MAX_PLAYABLE_CARDS", "50"))

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
            print("[2] 试玩广告 → 益智 → 7天 → 素材 → 点击卡片采集...")
            results = await run_playable_material_crawl(page, out_path, max_cards=max_cards)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print(f"\n已保存 {len(results)} 条卡片信息与试玩 URL → {out_path.name}")
        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
