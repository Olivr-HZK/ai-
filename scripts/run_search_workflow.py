"""
根据 operation.json 中的元素执行搜索工作流

流程：登录 → 搜索 → 时间框选7天 → 广告素材框选素材 → 筛选框选展示估值
结果：从素材内容中选 天数最新 且 展示估值最高 的素材

登录后跳转的页面即有搜索框，用 operation.json 中的 HTML 匹配元素。
"""
import argparse
import asyncio
import json
import os
import re
import sys

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

from guangdada_login import login

from path_util import CONFIG_DIR, DATA_DIR

OP_FILE = CONFIG_DIR / "operation.json"
OUT_DIR = DATA_DIR


def _html_to_selectors(html: str) -> list[str]:
    """从 operation.json 的 html 字符串解析出 CSS 选择器列表"""
    if not html or not html.strip().startswith("<"):
        return []
    sel_list = []
    # 提取 tag
    m = re.search(r"<(\w+)", html)
    tag = m.group(1).lower() if m else "*"
    # 提取 class（取稳定部分，不含 css-xxx 之类动态 hash）
    classes = re.findall(r"class=['\"]([^'\"]+)['\"]", html)
    for cls in classes:
        for c in cls.split():
            if c and not re.match(r"css-[a-z0-9]+", c):
                sel_list.append(f"{tag}.{c}".replace(" ", "."))
                break
        if sel_list:
            break
    # 提取 type
    m = re.search(r"type=['\"]([^'\"]+)['\"]", html)
    if m:
        sel_list.append(f'{tag}[type="{m.group(1)}"]')
    # 提取 role
    m = re.search(r"role=['\"]([^'\"]+)['\"]", html)
    if m:
        sel_list.append(f'{tag}[role="{m.group(1)}"]')
    # 组合 type+role
    if "type=" in html and "role=" in html:
        t = re.search(r"type=['\"]([^'\"]+)['\"]", html)
        r = re.search(r"role=['\"]([^'\"]+)['\"]", html)
        if t and r:
            sel_list.append(f'{tag}[type="{t.group(1)}"][role="{r.group(1)}"]')
    # 提取 value（如 7天 的 input[value='7']）
    m = re.search(r"value=['\"]([^'\"]+)['\"]", html)
    if m and m.group(1).isdigit():
        sel_list.append(f'{tag}[value="{m.group(1)}"]')
    return sel_list


def _load_selectors():
    """从 operation.json 加载，用 HTML 解析选择器"""
    sel_map = {}
    if OP_FILE.exists():
        try:
            data = json.load(open(OP_FILE, encoding="utf-8"))
            for item in data.get("data", []):
                name, html = item.get("element"), item.get("html", "")
                if name and html:
                    sels = _html_to_selectors(html)
                    if sels:
                        sel_map[name] = sels
        except Exception:
            pass
    sel_map["搜索框_容器"] = ["#display-search-input-container", "[id='display-search-input-container']"]
    # operation.json: 时间=7/30/90天, 广告素材=广告/素材/广告主, 筛选=最新创意/最后看见/展示估值
    defaults = {
        "时间": [".filter-search-radio-group_new", "#filter-search-radio-group_new", ".ant-radio-group-solid"],
        "七天": ["label:has-text('7天')", "input[value='7']", ".ant-radio-group-solid label:has-text('7天')"],
        "广告素材": ["#filter_duplicate_removal", ".ant-radio-group-outline"],
        "素材": ["#filter_duplicate_removal label:has-text('素材')", "label:has-text('素材')", "input[value='1']"],
        "筛选": [".flex.items-center.gap-x-3", "div:has(span:has-text('展示估值'))", "div:has(span:has-text('最新创意'))"],
        "展示估值": ["span.text-sm:has-text('展示估值')", "div:has(span:has-text('展示估值'))"],
        "最新创意": ["span.text-sm:has-text('最新创意')", "div:has(span:has-text('最新创意'))"],
        "素材内容": [".shadow-common-light", ".grid.grid-cols-4 div.shadow-common-light"],
    }
    for k, v in defaults.items():
        if k not in sel_map:
            sel_map[k] = v
        else:
            sel_map[k] = sel_map[k] + v
    return sel_map


SELECTORS = _load_selectors()


async def _click(page, keys: list, timeout: int = 5000) -> bool:
    for key in keys:
        sels = SELECTORS.get(key, [key] if isinstance(key, str) else [])
        if isinstance(sels, str):
            sels = [s.strip() for s in sels.split(",")]
        for sel in sels:
            try:
                loc = page.locator(sel)
                if await loc.count() > 0:
                    first = loc.first
                    await first.scroll_into_view_if_needed()
                    await first.click(timeout=timeout)
                    return True
            except Exception:
                pass
        try:
            if await page.locator(f"text={key}").count() > 0:
                await page.locator(f"text={key}").first.click(timeout=timeout)
                return True
        except Exception:
            pass
    return False


async def run(keyword: str, debug: bool = False, is_tool: bool = False):
    batches = []
    after_search = False

    async def on_response(response):
        url = response.url
        if "napi/v1/creative/list" not in url or response.status != 200:
            return
        if not after_search:
            return
        try:
            body = await response.json()
            lst = body.get("data", {}).get("creative_list", [])
            if lst:
                batches.append(lst)
                if len(batches) > 6:
                    batches.pop(0)
        except Exception:
            pass

    print(f"关键词: {keyword}")
    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not email or not password:
        print("[错误] 请在 .env 中设置 GUANGDADA_EMAIL 和 GUANGDADA_PASSWORD", file=sys.stderr)
        sys.exit(1)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not debug)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = await context.new_page()
        page.on("response", on_response)

        try:
            print("[1/8] 正在登录...")
            if not await login(page, email, password):
                print("[失败] 登录失败", file=sys.stderr)
                sys.exit(2)
            print("[1/8] 登录成功 ✓")

            print("[2/8] 等待登录后跳转页面就绪...")
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            await page.wait_for_timeout(4000)

            print(f"[3/8] 从搜索框容器找到输入框并输入: {keyword}")
            search_ok = False
            # 1. 先找搜索框容器（operation.json: id=display-search-input-container）
            container = None
            for sel in SELECTORS.get("搜索框_容器", ["#display-search-input-container"]):
                try:
                    loc = page.locator(sel).first
                    await loc.wait_for(state="visible", timeout=5000)
                    container = loc
                    print(f"    找到搜索框容器: {sel}")
                    break
                except Exception:
                    continue
            # 2. 在容器内找输入框：ant-select-show-search 里的 input（排除 readonly 的 filter_position）
            inp = None
            if container:
                for inp_sel in [
                    ".ant-select-show-search input.ant-select-selection-search-input",
                    "input.ant-select-selection-search-input:not([readonly])",
                    ".ant-select-auto-complete input.ant-select-selection-search-input",
                ]:
                    try:
                        loc = container.locator(inp_sel).first
                        if await loc.count() > 0:
                            await loc.wait_for(state="visible", timeout=3000)
                            inp = loc
                            print(f"    找到输入框: {inp_sel}")
                            break
                    except Exception:
                        continue
            if inp is None:
                try:
                    inp = page.locator("#display-search-input-container input.ant-select-selection-search-input:not([readonly])").first
                    await inp.wait_for(state="visible", timeout=2000)
                except Exception:
                    inp = page.locator("input.ant-select-selection-search-input:not([readonly])").first
            if inp:
                try:
                    await inp.scroll_into_view_if_needed()
                    # 点 ant-select 展开后输入
                    parent = page.locator("#display-search-input-container .ant-select-show-search, #display-search-input-container .ant-select-auto-complete").first
                    if await parent.count() > 0:
                        await parent.click(timeout=3000)
                        await page.wait_for_timeout(400)
                    await inp.click(timeout=2000)
                    await page.wait_for_timeout(200)
                    await inp.evaluate(
                        """(el, v) => {
                            el.focus();
                            const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                            setter.call(el, v);
                            el.dispatchEvent(new Event('input', { bubbles: true }));
                        }""",
                        keyword,
                    )
                    await page.wait_for_timeout(600)
                    # 点搜索按钮或按 Enter
                    btn = page.locator("#display-search-input-container button.bg-primary").first
                    if await btn.count() > 0:
                        await btn.click(timeout=2000)
                    else:
                        await page.keyboard.press("Enter")
                    search_ok = True
                except Exception as e:
                    print(f"    输入失败: {e}，尝试键盘...")
                    try:
                        await inp.click()
                        await page.keyboard.type(keyword, delay=80)
                        await page.wait_for_timeout(300)
                        btn = page.locator("#display-search-input-container button.bg-primary").first
                        if await btn.count() > 0:
                            await btn.click()
                        else:
                            await page.keyboard.press("Enter")
                        search_ok = True
                    except Exception:
                        pass
            if not search_ok:
                debug_path = OUT_DIR / "search_debug.png"
                await page.screenshot(path=str(debug_path))
                print(f"[调试] 未找到搜索框，截图已保存 → {debug_path}")
            if search_ok:
                print(f"[3/8] 已搜索: {keyword} ✓")
            else:
                print("[3/8] 未找到搜索框或输入失败 ✗")
            await page.wait_for_timeout(800)
            after_search = True
            await page.wait_for_timeout(2500)

            # 如果需要切换到工具标签
            if is_tool:
                print("[3.5/8] 切换到「工具」标签...")
                tool_ok = False
                # 根据 HTML 结构：<div class="flex items-center justify-center gap-x-12 text-base">
                #   <div class="pb-[6px] border-b-2 text-primary border-primary cursor-pointer">游戏</div>
                #   <div class="pb-[6px] border-b-2 border-transparent cursor-pointer">工具</div>
                # </div>
                tool_selectors = [
                    # 方法1: 在父容器内找包含"工具"文本且不是选中状态（没有text-primary）的div
                    "div.flex.items-center.justify-center.gap-x-12.text-base div:has-text('工具'):not(:has(.text-primary))",
                    # 方法2: 找包含"工具"且有border-transparent的div（未选中状态）
                    "div.border-transparent.cursor-pointer:has-text('工具')",
                    # 方法3: 在父容器内直接找"工具"文本
                    "div.flex.items-center.justify-center.gap-x-12.text-base >> text=工具",
                    # 方法4: 通用文本匹配
                    "div:has-text('工具').cursor-pointer:not(:has(.text-primary))",
                    "text=工具",
                ]
                for sel in tool_selectors:
                    try:
                        loc = page.locator(sel).first
                        if await loc.count() > 0:
                            await loc.scroll_into_view_if_needed()
                            await loc.click(timeout=3000)
                            await page.wait_for_timeout(1500)
                            tool_ok = True
                            print(f"    [3.5/8] 已切换到「工具」标签 ✓ (使用选择器: {sel})")
                            break
                    except Exception as e:
                        continue
                if not tool_ok:
                    # 如果还是找不到，尝试截图调试
                    debug_path = OUT_DIR / "tool_tab_debug.png"
                    await page.screenshot(path=str(debug_path))
                    print(f"    [3.5/8] 未找到「工具」标签，截图已保存 → {debug_path} ✗")
                await page.wait_for_timeout(1000)

            print("[4/8] 匹配时间框，选择 7天...")
            ok = await _click(page, ["七天", "时间"])
            print(f"[4/8] 7天 {'✓' if ok else '✗'}")
            await page.wait_for_timeout(2000)

            print("[5/8] 匹配广告素材框，选择 素材（多次尝试）...")
            ok = False
            for attempt in range(1, 6):  # 最多尝试 5 次
                ok = await _click(page, ["素材", "广告素材"])
                if ok:
                    print(f"[5/8] 素材 ✓ (第 {attempt} 次尝试成功)")
                    break
                print(f"    第 {attempt} 次未点到素材，等待后重试...")
                await page.wait_for_timeout(800)
            if not ok:
                print(f"[5/8] 素材 ✗ (5 次尝试均失败)")
            await page.wait_for_timeout(2500)

            print("[6/8] 匹配筛选框，选择 展示估值...")
            ok = await _click(page, ["展示估值", "筛选"])
            print(f"[6/8] 展示估值 {'✓' if ok else '✗'}")
            await page.wait_for_timeout(3500)

            print("[7/8] 等待素材内容加载...")
            await page.wait_for_timeout(2000)

            print("[8/8] 将从 API 数据中选取天数最新且展示估值最高的素材（无需点击卡片）")

        finally:
            await browser.close()

    print(f"\n[数据] 共捕获 {len(batches)} 批 creative/list 响应")
    all_creatives = []
    for b in batches:
        all_creatives.extend(b)

    # 从素材内容中选 天数最新(days_count 最小) 且 展示估值最高(all_exposure_value 最大)
    # 排序：投放天数升序，展示估值降序
    def sort_key(c):
        days = c.get("days_count", 999999)
        exp = c.get("all_exposure_value", 0) or c.get("impression", 0)
        return (days, -exp)

    best_creative = min(all_creatives, key=sort_key) if all_creatives else None

    result = {
        "keyword": keyword,
        "selected": best_creative,
        "total_captured": len(all_creatives),
    }
    out_file = OUT_DIR / "keyword_result.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"[结果] 已写入 {out_file.name}")
    if best_creative:
        name = best_creative.get("title") or best_creative.get("app_name") or "N/A"
        days = best_creative.get("days_count", "?")
        exp = best_creative.get("all_exposure_value") or best_creative.get("impression") or "?"
        print(f"  - 天数最新且展示估值最高: {name[:50]}")
        print(f"    投放天数: {days} 天, 展示估值: {exp}")
    else:
        print("  - 未捕获到素材")
    summary = {"keyword": result["keyword"], "total_captured": result["total_captured"], "selected_title": best_creative.get("title") if best_creative else None}
    print("\n" + json.dumps(summary, ensure_ascii=False))
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("keyword", nargs="*", help="搜索关键词（支持空格，如: puzzle game）")
    parser.add_argument("--debug", action="store_true", help="显示浏览器")
    parser.add_argument("--tool", action="store_true", help="切换到工具标签（用于搜索工具类产品）")
    args = parser.parse_args()
    keyword = " ".join(args.keyword).strip() if args.keyword else input("请输入关键词: ").strip()
    if not keyword:
        print("错误: 需要关键词", file=sys.stderr)
        sys.exit(1)
    asyncio.run(run(keyword, debug=args.debug, is_tool=args.tool))


if __name__ == "__main__":
    main()
