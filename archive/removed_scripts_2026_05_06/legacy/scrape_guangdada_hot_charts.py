"""
广大大 - 创意榜单 → 每周热门榜 → 工具 → 标签选「生成式AI / AI图像生成」→ 获取周榜全部素材数据

流程：
1. 登录
2. 点击「创意灵感」展开下拉 → 点击「每周热门榜」（或直接打开 hot-charts 页面）
3. 在榜单页点击「工具」→「每周热门榜」，在工具分类检索框输入「AI 图像生成」并回车
4. 监听 napi 接口，收集全部周榜创意数据（含分页/滚动加载）
5. 保存为 hot_charts_yizhi_creatives.json（文件名保留原有命名，含义已变更为工具-生成式AI-图像生成周榜）

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


async def _close_any_modal(page: Page) -> None:
    """关闭可能挡住点击的 ant-modal（如引导、提示弹窗），避免 pointer 被拦截。"""
    try:
        modal = page.locator(".ant-modal-wrap")
        if await modal.count() > 0:
            # 优先点关闭按钮，否则按 Esc
            close_btn = page.locator(".ant-modal .ant-modal-close").first
            if await close_btn.count() > 0 and await close_btn.is_visible():
                await close_btn.click(timeout=1000)
            else:
                await page.keyboard.press("Escape")
            await page.wait_for_timeout(500)
    except Exception:
        pass


# 排行榜类型：与页面上 Tab 文案一致
CHART_TYPE_HOT = "每周热门榜"
CHART_TYPE_SURGE = "每周飙升榜"
CHART_TYPE_NEW = "新创意榜"


async def prepare_hot_charts_page(
    page: Page,
    chart_type: str = CHART_TYPE_HOT,
    date_range_index: int | None = None,
) -> None:
    """
    准备榜单页面：
    1. 进入创意榜单页
    2. 选择榜单
    3. 切换到工具
    4. 选择日期范围
    这些步骤每个榜单只需执行一次。
    """
    print("  [2.0] 进入创意榜单页...")
    await page.goto(GUANGDADA_BASE_URL + "/modules/creative", wait_until="domcontentloaded", timeout=20000)
    await page.wait_for_timeout(800)
    try:
        await page.wait_for_load_state("networkidle", timeout=6000)
    except Exception:
        pass

    try:
        await page.locator('div.group:has(div:has-text("创意灵感"))').first.hover(timeout=3000)
        await page.wait_for_timeout(300)
        clicked = await _safe_click(
            page,
            'a[href="/modules/creative/charts/hot-charts"], a:has-text("每周热门榜")',
            "每周热门榜",
            timeout=3000,
        )
        if not clicked:
            raise RuntimeError("menu click failed")
    except Exception:
        await page.goto(HOT_CHARTS_URL, wait_until="domcontentloaded", timeout=15000)
        print("  ✓ 直接打开每周热门榜页面（菜单/hover 失败时兜底）")
    await page.wait_for_timeout(800)

    print(f"  [2.1] 选择榜单：{chart_type}")
    try:
        await page.wait_for_selector(f'div[role="tab"]:has-text("{chart_type}")', timeout=5000)
        await _safe_click(page, f'div[role="tab"]:has-text("{chart_type}")', f"排行榜Tab-{chart_type}", timeout=3000)
    except Exception as e:
        raise RuntimeError(f"选择榜单失败：{chart_type}，{e}")
    await page.wait_for_timeout(250)

    print("  [2.2] 切换到工具")
    try:
        await page.wait_for_selector('div[role="tab"]:has-text("工具")', timeout=5000)
        clicked = await _safe_click(page, 'div[role="tab"]:has-text("工具")', "游戏/工具Tab-工具", timeout=3000)
        if not clicked:
            raise RuntimeError("工具Tab未点击成功")
    except Exception as e:
        raise RuntimeError(f"切换工具失败：{e}")
    await page.wait_for_timeout(250)

    if date_range_index is not None:
        print(f"  [2.3] 选择日期范围：第 {date_range_index + 1} 项")
        date_clicked = False
        for date_sel in [
            ".ant-select-selector:has(#creative_charts_filter_date)",
            'div.ant-select:has(#creative_charts_filter_date) .ant-select-selector',
            "#creative_charts_filter_date",
            "input#creative_charts_filter_date",
            '[id="creative_charts_filter_date"]',
        ]:
            try:
                loc = page.locator(date_sel).first
                if await loc.count() > 0 and await loc.is_visible():
                    clicked_selector = date_sel
                    try:
                        await loc.click(timeout=3000)
                    except Exception:
                        # 某些情况下 ant-select selector 被浮层/动画影响，强制点击更稳
                        await loc.click(timeout=3000, force=True)
                    await page.wait_for_timeout(500)
                    print(f"    - 日期选择器命中: {clicked_selector}")

                    # 优先使用日期 input 的 aria-controls 锁定对应 listbox，避免误点其他下拉
                    date_input = page.locator("#creative_charts_filter_date").first
                    listbox_id = None
                    if await date_input.count() > 0:
                        listbox_id = await date_input.get_attribute("aria-controls")

                    selectors = []
                    if listbox_id:
                        selectors.append(f"#{listbox_id} .ant-select-item")
                        selectors.append(f"div[id='{listbox_id}'] .ant-select-item")
                    selectors.extend(
                        [
                            "[id*='creative_charts_filter_date_list'] .ant-select-item",
                            ".ant-select-dropdown .ant-select-item",
                            "div[role='listbox'] .ant-select-item",
                        ]
                    )

                    for opt_sel in selectors:
                        all_opts = page.locator(opt_sel)
                        count = await all_opts.count()
                        print(f"      - 日期下拉候选 {opt_sel} count={count}")
                        if count > date_range_index:
                            opt_loc = all_opts.nth(date_range_index)
                            try:
                                await opt_loc.wait_for(state="visible", timeout=3000)
                            except Exception:
                                pass
                            try:
                                await opt_loc.click(timeout=3000)
                            except Exception:
                                await opt_loc.click(timeout=3000, force=True)
                            date_clicked = True
                            print(f"    - 日期下拉命中: {opt_sel}")
                            break
                    break
            except Exception:
                continue
        if not date_clicked:
            raise RuntimeError(f"选择日期范围第 {date_range_index + 1} 项失败")
        await page.wait_for_timeout(300)


async def collect_hot_charts_category(page: Page, keyword: str, step_label: str = "[3]") -> list:
    """在当前已准备好的榜单页面中，搜索单个分类并采集结果。"""
    all_creatives = []
    seen_ids = set()
    capture_enabled = False

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
                    print(f"  [采集] +{n} 条 (当前共 {len(all_creatives)} 条)")
                return
            # 榜单接口可能直接返回 list 或 data.list / data.creatives
            if isinstance(data, list):
                n = _append_creatives(data)
                if n:
                    print(f"  [采集] +{n} 条 (当前共 {len(all_creatives)} 条)")
                return
            for key in ("list", "creatives", "items", "rank_list"):
                lst = data.get(key) if isinstance(data, dict) else None
                if lst and isinstance(lst, list):
                    n = _append_creatives(lst)
                    if n:
                        print(f"  [采集] +{n} 条 [{key}] (当前共 {len(all_creatives)} 条)")
                    return
        except Exception:
            pass

    page.on("response", on_response)
    print(f"  {step_label} 搜索分类「{keyword}」并选择第一个结果...")

    # 先等「工具分类」出现（工具 Tab 下的面板可能稍晚渲染）
    try:
        await page.wait_for_selector('text=工具分类', state="visible", timeout=4000)
        await page.wait_for_timeout(200)
    except Exception:
        page.remove_listener("response", on_response)
        raise RuntimeError("未找到工具分类区域")

    search_ok = False
    inp = None
    for desc, loc in [
        ("工具分类同行内的检索框", page.get_by_text("工具分类").locator("xpath=ancestor::div[contains(@class,\"flex\")][1]").locator("input.ant-select-selection-search-input")),
        ("含「工具分类」的块内 input", page.locator('div:has(div:has-text("工具分类")) input.ant-select-selection-search-input')),
        ("任意 ant-select 检索框", page.locator('input.ant-select-selection-search-input[role="combobox"]')),
        ("placeholder 检索", page.get_by_placeholder("快速检索一级或二级分类")),
    ]:
        try:
            if await loc.count() > 0:
                inp = loc.first
                await inp.scroll_into_view_if_needed(timeout=3000)
                if await inp.is_visible():
                    break
        except Exception:
            continue
    if inp and await inp.count() > 0:
        try:
            # 若有弹窗挡住点击，先尝试关闭
            await _close_any_modal(page)
            await inp.click(timeout=1500)
            await page.wait_for_timeout(100)
            await inp.fill(keyword)
            await page.wait_for_timeout(150)

            # 直接用键盘选择下拉第一项：ArrowDown + Enter
            # 严格只针对当前搜索框，避免误操作其他下拉（如日期）
            listbox_id = await inp.get_attribute("aria-controls")
            if not listbox_id:
                print("    - 搜索下拉 aria-controls 为空，无法锁定对应下拉列表，尝试直接键盘选择")
            else:
                listbox_sel = f"#{listbox_id}"
                print(f"    - 绑定搜索下拉 listbox: {listbox_sel}")
            try:
                await page.keyboard.press("ArrowDown")
                await page.keyboard.press("Enter")
                search_ok = True
                print("    - 通过键盘 ArrowDown+Enter 选中第一条候选")
            except Exception as e:
                print(f"    - 键盘选择下拉第一项失败: {e}")
        except Exception as e:
            page.remove_listener("response", on_response)
            raise RuntimeError(f"检索框/下拉选择失败: {e}")
    else:
        page.remove_listener("response", on_response)
        raise RuntimeError("未找到工具分类检索输入框")

    if search_ok:
        capture_enabled = True
    else:
        page.remove_listener("response", on_response)
        raise RuntimeError(f"搜索分类「{keyword}」后未找到可点击的下拉第一项")

    await page.wait_for_timeout(1200)
    try:
        await page.wait_for_load_state("networkidle", timeout=5000)
    except Exception:
        pass

    # 4) 滚动加载更多（周榜可能分页或无限滚动）
    print(f"  {step_label} 开始采集...")
    for _ in range(8):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(1200)
        # 若存在「加载更多」按钮可点击
        load_more = page.locator('button:has-text("加载更多"), [role="button"]:has-text("更多")')
        if await load_more.count() > 0:
            try:
                await load_more.first.click(timeout=2000)
                await page.wait_for_timeout(1200)
            except Exception:
                break

    # 再等一次网络
    await page.wait_for_timeout(1000)
    try:
        await page.wait_for_load_state("networkidle", timeout=4000)
    except Exception:
        pass
    page.remove_listener("response", on_response)
    print(f"  {step_label} 采集完成，共 {len(all_creatives)} 条")

    return all_creatives


async def run_hot_charts_for_category(
    page: Page,
    keyword: str,
    chart_type: str = CHART_TYPE_HOT,
    date_range_index: int | None = None,
    prepare: bool = True,
) -> list:
    """
    兼容旧接口：
    - prepare=True 时：先做榜单页面准备，再采集当前分类
    - prepare=False 时：只在当前已准备好的页面里搜索并采集
    """
    if prepare:
        await prepare_hot_charts_page(page, chart_type=chart_type, date_range_index=date_range_index)
    return await collect_hot_charts_category(page, keyword)


async def run_hot_charts_yizhi(page: Page, out_dir: Path, chart_type: str = CHART_TYPE_HOT) -> list:
    """兼容旧入口：采集「AI 图像生成」榜单，可指定 chart_type（每周热门榜/每周飙升榜/新创意榜）。"""
    return await run_hot_charts_for_category(page, "AI 图像生成", chart_type=chart_type)


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
            print("[2] 执行创意榜单 → 每周热门榜 → 工具 → 生成式AI → AI图像生成 并采集...")
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
