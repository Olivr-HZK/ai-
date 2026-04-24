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
from datetime import datetime, timedelta, timezone
from typing import Any

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

from guangdada_login import login
from proxy_util import prepare_playwright_proxy_for_crawl

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


def _extract_creative_lists(obj) -> list[list]:
    """
    从响应 JSON 中递归查找可能的创意列表：
    - 只要是 list 且元素为 dict，且包含 ad_key/creative_id/creativeId/id 等字段，就认为是创意列表。
    返回所有匹配到的列表。
    """
    results: list[list] = []

    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            sample = obj[0]
            keys = sample.keys()
            if any(
                k in keys
                for k in ("ad_key", "creative_id", "creativeId", "id")
            ):
                results.append(obj)
        # 继续深入子元素
        for item in obj:
            results.extend(_extract_creative_lists(item))
    elif isinstance(obj, dict):
        # 跳过我们内部的 meta 哨兵
        if "__meta__" in obj:
            return results
        for v in obj.values():
            results.extend(_extract_creative_lists(v))
    return results


async def _click_day_span(page, day_span: str, log_prefix: str) -> bool:
    """点击时间窗：7 / 30 / 90 天（无法匹配时回退 7 天并打印提醒）。"""
    s = str(day_span or "7").strip()
    if s in ("7", "7天"):
        ok = await _click(page, ["七天", "时间"])
        print(f"{log_prefix}7天 {'✓' if ok else '✗'}")
        return bool(ok)
    if s in ("30", "30天"):
        for keys in (["label:has-text('30天')", "30天", "时间"], ["input[value='30']"], ["三十天", "时间"]):
            if await _click(page, keys):
                print(f"{log_prefix}30天 ✓")
                return True
        print(f"{log_prefix}[提醒] 未找到 30 天选项，回退 7 天", file=sys.stderr)
    elif s in ("90", "90天"):
        for keys in (["label:has-text('90天')", "90天", "时间"], ["input[value='90']"], ["九十天", "时间"]):
            if await _click(page, keys):
                print(f"{log_prefix}90天 ✓")
                return True
        print(f"{log_prefix}[提醒] 未找到 90 天选项，回退 7 天", file=sys.stderr)
    ok = await _click(page, ["七天", "时间"])
    print(f"{log_prefix}7天（回退） {'✓' if ok else '✗'}")
    return bool(ok)


async def _select_top_popularity_option(
    page, popularity_option_text: str | None, use_first_fallback: bool, log_prefix: str
) -> bool:
    """
    展开「Top 创意 / 人气」类下拉，优先按 `popularity_option_text` 子串匹配某一项，否则用首项或放弃。
    """
    ok = False
    try:
        selector = page.locator("div.ant-select:has(#filter_popularity_tag) .ant-select-selector").first
        if await selector.count() > 0:
            await selector.scroll_into_view_if_needed()
            try:
                await selector.click(timeout=3000)
            except Exception:
                await selector.click(timeout=3000, force=True)
            await page.wait_for_timeout(400)
            for opt_sel in [
                "#filter_popularity_tag_list .ant-select-item",
                "div[id='filter_popularity_tag_list'] .ant-select-item",
                "div[role='listbox'] .ant-select-item",
            ]:
                cand = page.locator(opt_sel)
                count = await cand.count()
                if count == 0:
                    continue
                want = (popularity_option_text or "").strip()
                for i in range(min(count, 30)):
                    opt = cand.nth(i)
                    try:
                        txt = (await opt.inner_text() or "").strip()
                    except Exception:
                        continue
                    if want and want in txt:
                        try:
                            await opt.click(timeout=3000)
                        except Exception:
                            await opt.click(timeout=3000, force=True)
                        print(f"{log_prefix}  已点 Top 创意 → {txt[:32]}…")
                        ok = True
                        break
                    if not want and use_first_fallback and i == 0:
                        try:
                            await opt.click(timeout=3000)
                        except Exception:
                            await opt.click(timeout=3000, force=True)
                        print(f"{log_prefix}  已点 Top 创意首项: {txt[:32]}")
                        ok = True
                        break
                if ok:
                    break
    except Exception:
        ok = False
    if popularity_option_text and not ok:
        print(f"{log_prefix}Top 创意下拉拉取 {popularity_option_text!r} 失败，请人工核对页面", file=sys.stderr)
    return ok


async def _try_click_search_tab(page, search_tab: str) -> None:
    """登录后切到 游戏/工具/试玩（失败仅打印，不中断）。"""
    st = (search_tab or "game").strip().lower()
    if st in ("", "game"):
        for sel in (
            "div.flex:has-text('游戏') >> text=游戏",
            "div.cursor-pointer:has-text('游戏'):not(:has(.text-primary))",
        ):
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    await loc.click(timeout=2500)
                    print("[arrow2] 已点「游戏」页签")
                    await page.wait_for_timeout(800)
                    return
            except Exception:
                continue
        return
    if st == "tool":
        for sel in ("text=工具", "div:has-text('工具').cursor-pointer"):
            try:
                if await page.locator(sel).first.count() > 0:
                    await page.locator(sel).first.click(timeout=2500)
                    print("[arrow2] 已点「工具」页签")
                    await page.wait_for_timeout(800)
                    return
            except Exception:
                continue
        return
    if st in ("playable", "playable_ads"):
        for sel in ("text=试玩广告", "a:has-text('试玩')", "div:has-text('试玩广告')"):
            try:
                if await page.locator(sel).first.count() > 0:
                    await page.locator(sel).first.click(timeout=2500)
                    print("[arrow2] 已点「试玩广告」")
                    await page.wait_for_timeout(1200)
                    return
            except Exception:
                continue


def _beijing_ymd_from_first_seen(ts: object) -> str | None:
    try:
        t = int(ts)  # type: ignore[arg-type]
    except Exception:
        return None
    try:
        tz = timezone(timedelta(hours=8))
        return datetime.fromtimestamp(t, tz=tz).date().isoformat()
    except Exception:
        return None


def _filter_creatives_first_seen_day(creatives: list, ymd: str) -> list:
    ymd = (ymd or "").strip()[:10]
    if not ymd or not isinstance(creatives, list):
        return list(creatives) if isinstance(creatives, list) else []
    out: list = []
    for c in creatives:
        if not isinstance(c, dict):
            continue
        d = _beijing_ymd_from_first_seen(c.get("first_seen"))
        if d == ymd:
            out.append(c)
    return out


def _arrow2_apply_post_filters(
    all_creatives: list[dict],
    spec: dict[str, Any],
    keyword: str,
    keyword_product: dict[str, str] | None,
    first_seen_ymd: str,
) -> list[dict]:
    """仅 napi 路径：拉取后按 first_seen 日、广告主、max_c 截断。"""
    c = [x for x in all_creatives if isinstance(x, dict)]
    if spec.get("filter_yesterday_only") and first_seen_ymd:
        c = _filter_creatives_first_seen_day(c, first_seen_ymd)
    prod = (keyword_product or {}).get(keyword) or (keyword_product or {}).get((keyword or "").strip())
    if prod and str(prod).strip():
        from workflow_guangdada_competitor_yesterday_creatives import advertiser_matches_product  # noqa: PLC0415

        p2 = str(prod).strip()
        c = [
            x
            for x in c
            if advertiser_matches_product(str(x.get("advertiser_name") or x.get("page_name") or ""), p2)
        ]
    mdef = 10**6
    try:
        mc = int(spec.get("max_creatives_per_keyword") or os.getenv("ARROW2_MAX_CREATIVES_PER_KEYWORD") or 0) or 0
    except Exception:
        mc = 0
    if mc and mc < mdef:
        c = c[:mc]
    return c


def _env_truthy(name: str, default: bool) -> bool:
    v = (os.environ.get(name) or "").strip().lower()
    if v == "" and name not in os.environ:
        return default
    return v not in ("0", "false", "no", "off")


def _print_debug_step_cards(keyword: str, all_c: list, max_n: int) -> None:
    n = min(max_n, len(all_c))
    print(f"\n[debug-step] 词={keyword!r} 共 {len(all_c)} 条，示前 {n} 条：")
    for i, c in enumerate(all_c[:n], 1):
        if not isinstance(c, dict):
            continue
        title = (c.get("title") or c.get("body") or "")[:60]
        ak = c.get("ad_key") or c.get("id")
        im = c.get("impression")
        aev = c.get("all_exposure_value")
        fs = c.get("first_seen")
        print(f"  {i}. ad_key={ak} 展示估值/人气≈{aev!r}/{im!r} first_seen={fs!r} 标题={title!r}")


def _print_pause_yesterday_summary(all_c: list) -> None:
    print(f"\n[arrow2] 本词「仅昨日 first_seen」后共 {len(all_c)} 条")
    n = min(5, len(all_c))
    for i, c in enumerate(all_c[:n], 1):
        if not isinstance(c, dict):
            continue
        aev = c.get("all_exposure_value", "?")
        im = c.get("impression", "?")
        heat = c.get("heat", "?")
        print(f"  {i} 展示估值={aev} 人气/热度={im}/{heat} ad_key={c.get('ad_key')!r}")


async def _do_setup(
    page,
    is_tool: bool,
    log_prefix: str = "",
    order_by: str = "exposure",
    use_popularity_top1: bool = False,
    *,
    day_span: str = "7",
    popularity_option_text: str | None = None,
) -> None:
    """
    在已登录的页面上做一次性的筛选设置：工具标签（可选）→ 时间窗 → 素材 → Top 创意(可选) → 排序在搜索后点。
    order_by: 供调用方在搜索后使用（本函数内不点排序）。
    day_span: "7" / "30" / "90" 等。
    popularity_option_text: 若设，在 Top 创意下拉中尽量匹配子串；否则 `use_popularity_top1` 为真时点首项。
    """
    if is_tool:
        print(f"{log_prefix}切换到「工具」标签...")
        tool_ok = False
        tool_selectors = [
            "div.flex.items-center.justify-center.gap-x-12.text-base div:has-text('工具'):not(:has(.text-primary))",
            "div.border-transparent.cursor-pointer:has-text('工具')",
            "div.flex.items-center.justify-center.gap-x-12.text-base >> text=工具",
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
                    print(f"{log_prefix}已切换到「工具」标签 ✓")
                    break
            except Exception:
                continue
        if not tool_ok:
            print(f"{log_prefix}未找到「工具」标签 ✗")
        await page.wait_for_timeout(1000)

    print(f"{log_prefix}选择时间窗: {day_span!r} 天")
    await _click_day_span(page, day_span, log_prefix)
    await page.wait_for_timeout(2000)

    print(f"{log_prefix}选择 素材...")
    ok = False
    for attempt in range(1, 6):
        ok = await _click(page, ["素材", "广告素材"])
        if ok:
            print(f"{log_prefix}素材 ✓ (第 {attempt} 次)")
            break
        await page.wait_for_timeout(800)
    if not ok:
        print(f"{log_prefix}素材 ✗")
    await page.wait_for_timeout(2500)
    # ⚠️ 注意：不要在这里点「最新创意/展示估值」排序。
    # 实测在搜索框输入/回车后页面会自动切回「相关性」，
    # 所以排序必须在“每次触发搜索之后”再点一次（见 _search_one_keyword）。

    if popularity_option_text or use_popularity_top1:
        print(f"{log_prefix}选择 Top 创意 下拉（option_text={popularity_option_text!r} 首项回退={use_popularity_top1}）")
        p_ok = await _select_top_popularity_option(
            page, popularity_option_text, use_popularity_top1, log_prefix
        )
        print(f"{log_prefix}Top 创意下拉 {'✓' if p_ok else '✗'}")
        await page.wait_for_timeout(2500)


async def _search_one_keyword(
    page,
    keyword: str,
    batches_ref: list,
    capture_state: dict,
    order_by: str = "exposure",
    log_prefix: str = "",
    max_scroll_rounds: int = 16,
) -> None:
    """
    在当前已设置好筛选的页面上：清空 batches_ref，清空搜索框再填新关键字并搜索，
    等待 creative/list 接口返回（轮询 batches_ref 或最长 8 秒）。
    """
    # 重要：先关闭采集，避免把“搜索触发的相关性/其他请求”混进来
    capture_state["enabled"] = False
    batches_ref.clear()
    # 找搜索框（针对工具 Tab，根据你提供的 HTML 优先锁定 #rc_select_1）
    inp = None
    candidates = [
        "#display-search-input-container input#rc_select_1",
        "#display-search-input-container input[role='combobox']",
        "#display-search-input-container input.ant-select-selection-search-input",
        "input#display-search-input",
        "input[role='combobox']",
        "input.ant-select-selection-search-input",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel).first
            count = await loc.count()
            if count > 0:
                inp = loc
                print(f"{log_prefix}命中搜索输入选择器: {sel} (count={count})")
                break
        except Exception:
            continue
    if inp is None:
        raise RuntimeError("未找到搜索输入框（combobox/ant-select-selection-search-input）")

    try:
        await inp.scroll_into_view_if_needed(timeout=2000)
    except Exception:
        pass

    # 有些情况下 input 被包在 ant-select 内部，需要点父级以激活
    try:
        parent = page.locator(
            "#display-search-input-container .ant-select-show-search, "
            "#display-search-input-container .ant-select-auto-complete"
        ).first
        if await parent.count() > 0:
            try:
                await parent.click(timeout=2000)
            except Exception:
                await parent.click(timeout=2000, force=True)
            await page.wait_for_timeout(300)
    except Exception:
        pass

    try:
        await inp.click(timeout=2000)
    except Exception:
        await inp.click(timeout=2000, force=True)
    await page.wait_for_timeout(200)
    # 先清空再填新关键词，优先用 Playwright 原生 fill/type，确保界面上可见输入
    try:
        await inp.fill("")
    except Exception:
        # 某些 rc-select 受控组件可能不允许 fill 清空，忽略错误
        pass
    await page.wait_for_timeout(200)
    try:
        await inp.fill(keyword)
    except Exception:
        # 回退到 type，逐字输入
        await inp.type(keyword, delay=50)
    await page.wait_for_timeout(500)
    btn = page.locator("#display-search-input-container button.bg-primary").first
    if await btn.count() > 0:
        await btn.click(timeout=2000)
    else:
        await page.keyboard.press("Enter")
    await page.wait_for_timeout(1000)

    # 搜索触发后，页面通常会自动切回「相关性」。
    # 因此必须在每次搜索后重新点击一次排序（最新创意/展示估值），并且“点完排序后再开启采集”。
    async def _is_order_selected(name: str) -> bool:
        """
        通过 operation.json 里记录的选中态 class（text-blue-600）判断排序是否选中。
        命中即认为“操作正确”，否则即使 click 成功也可能没生效（如被遮挡/点到别的元素）。
        """
        try:
            # 选中态：父 div 同时含 text-blue-600 且内部 span.text-sm 含对应文本
            loc = page.locator(
                f'div.flex.items-center.cursor-pointer:has(span.text-sm:has-text("{name}")).text-blue-600'
            ).first
            if await loc.count() > 0 and await loc.is_visible():
                return True
        except Exception:
            pass
        # 兜底：仅检测文本节点是否出现且周围有 text-blue-600
        try:
            loc2 = page.locator(f'text={name}').first
            if await loc2.count() > 0:
                # 往上找包含 text-blue-600 的祖先
                anc = loc2.locator("xpath=ancestor::div[contains(@class,'text-blue-600')][1]")
                if await anc.count() > 0:
                    return True
        except Exception:
            pass
        return False

    async def _click_order_once() -> bool:
        if order_by == "exposure":
            print(f"{log_prefix}（搜索后）选择 展示估值...")
            ok = await _click(page, ["展示估值", "筛选"])
            print(f"{log_prefix}（搜索后）展示估值 {'✓' if ok else '✗'}")
            return ok
        if order_by == "latest":
            print(f"{log_prefix}（搜索后）选择 最新创意...")
            ok = await _click(page, ["最新创意", "筛选"])
            print(f"{log_prefix}（搜索后）最新创意 {'✓' if ok else '✗'}")
            return ok
        return True

    # 关键：必须保证“排序触发的请求”被我们监听到。
    # 经验：请求可能在 click 的瞬间就发出，因此应该先开启采集，再 click 排序。
    ok = True
    if order_by in ("exposure", "latest"):
        # 恢复为：先开启采集，再点击排序，不额外长时间等待
        capture_state["enabled"] = True
        batches_ref.clear()
        ok = await _click_order_once()
        await page.wait_for_timeout(1200)

        # 校验排序确实已选中（避免“点了但没生效”）
        expected_name = "最新创意" if order_by == "latest" else "展示估值"
        for _ in range(6):
            if await _is_order_selected(expected_name):
                break
            await page.wait_for_timeout(300)

        # 等待接口返回：轮询直到 batches_ref 有数据或超时（最多约 12 秒）
        for _ in range(24):  # 12s
            if len(batches_ref) > 0:
                break
            await page.wait_for_timeout(500)

        # 若仍为空，再强制重试点击一次排序并延长等待
        if len(batches_ref) == 0:
            print(f"{log_prefix}[提醒] 排序后仍未捕获到新返回，将重试点击一次排序...")
            capture_state["enabled"] = False
            await _click_order_once()
            await page.wait_for_timeout(1500)
            capture_state["enabled"] = True
            batches_ref.clear()
            for _ in range(30):  # 15s
                if len(batches_ref) > 0:
                    break
                await page.wait_for_timeout(500)
        await page.wait_for_timeout(400)
    else:
        # 未指定排序时：搜索后立即开启采集（仍然避免采集“搜索前的请求”）
        batches_ref.clear()
        capture_state["enabled"] = True
        for _ in range(20):
            if len(batches_ref) > 0:
                break
            await page.wait_for_timeout(500)
        await page.wait_for_timeout(500)

    # 恢复滚动加载逻辑：向下滚动若干轮，尽量拿到更多素材
    idle_rounds = 0
    last_batch_count = len(batches_ref)
    for _ in range(max(1, int(max_scroll_rounds))):
        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            break
        await page.wait_for_timeout(1200)
        try:
            await page.wait_for_load_state("networkidle", timeout=3500)
        except Exception:
            pass
        await page.wait_for_timeout(400)

        if len(batches_ref) > last_batch_count:
            last_batch_count = len(batches_ref)
            idle_rounds = 0
        else:
            idle_rounds += 1
            if idle_rounds >= 3:
                break

    # 完成本关键词采集后，立刻关闭采集，避免泄漏到下一个关键词或页面的其他请求
    capture_state["enabled"] = False

    # 将回退信息塞进 batches_ref 的第 0 个元素上层不方便，这里用一个哨兵 dict 记录（不影响解析创意列表）
    # 仅用于 run_batch 内部调试/返回值标记
    try:
        batches_ref.append({"__meta__": {"order_by": order_by, "order_clicked": bool(ok)}})
    except Exception:
        pass


async def _extract_dom_cards(page) -> list[dict]:
    """
    从页面 DOM 中提取所有可见创意卡片的基础信息，作为 napi 捕获的补充。
    对于 napi 未返回的卡片（如 YouTube 0s 外链素材），生成一个 _source="dom" 的 partial 对象。
    用 preview_img_url 去重；napi 已有的优先，DOM 只补充缺失的。
    """
    try:
        cards = await page.evaluate(r"""
() => {
  const results = [];
  const cardEls = document.querySelectorAll('.shadow-common-light.bg-white');
  cardEls.forEach((card, cardIdx) => {
    try {
      // 预览图：取所有 img，优先 sp_opera CDN，其次任意非 logo 图
      const allImgs = Array.from(card.querySelectorAll('img'));
      let previewSrc = '';
      const spImg = allImgs.find(img => img.src && img.src.includes('sp_opera'));
      if (spImg) {
        previewSrc = spImg.src.split('?')[0];
      } else {
        // 懒加载尚未触发时 src 为空，尝试 data-src 或 currentSrc
        const lazyImg = allImgs.find(img =>
          !img.src.includes('appcdn-global') && (img.dataset.src || img.currentSrc)
        );
        if (lazyImg) previewSrc = (lazyImg.dataset.src || lazyImg.currentSrc || '').split('?')[0];
      }

      // 广告主名称
      const advEl = card.querySelector('.leading-\\[18px\\] span span');
      const advertiserName = advEl ? advEl.textContent.trim() : '';

      // 平台
      const isYouTube = !!card.querySelector('.net-icon-youtube');
      const platform = isYouTube ? 'youtube' : 'admob';

      // 视频时长：找 "Xs" 文本（如 "0s" "15s"）
      let videoDuration = null;
      const playArea = card.querySelector('[class*="play-simple"]');
      if (playArea) {
        const txt = (playArea.parentElement || playArea).textContent.trim();
        const m = txt.match(/(\d+)s/);
        videoDuration = m ? parseInt(m[1]) : 0;
      }

      // 标签
      const tagEls = Array.from(card.querySelectorAll('.ant-tag'));
      const tags = tagEls.map(t => t.textContent.trim());
      const dateRange = tags.find(t => t.includes('~')) || '';
      const isRelaunch = tags.some(t => t === '重投');

      // 指标值（人气值/投放天数/最后看见）
      const metricBolds = Array.from(card.querySelectorAll('.font-semibold')).map(el => el.textContent.trim());
      const impression = metricBolds[0] ? parseInt(metricBolds[0].replace(/[^0-9]/g, '')) || 0 : 0;

      // 展示估值/热度标签
      const smallTags = Array.from(card.querySelectorAll('.rounded-full')).map(el => el.textContent.trim());
      let heat = 0, allExposure = 0;
      smallTags.forEach(t => {
        const hm = t.match(/热度[:：]\s*([\d.]+)([KkMm万]?)/);
        if (hm) {
          const v = parseFloat(hm[1]);
          const u = hm[2].toUpperCase();
          heat = u === 'K' ? Math.round(v * 1000) : u === 'M' ? Math.round(v * 1000000) : v;
        }
        const em = t.match(/展示估值[:：]\s*([\d.]+)([KkMm万]?)/);
        if (em) {
          const v = parseFloat(em[1]);
          const u = em[2].toUpperCase();
          allExposure = u === 'K' ? Math.round(v * 1000) : u === 'M' ? Math.round(v * 1000000) : v;
        }
      });

      // 页面广告主（卡片下方的广告主文本行，可能与顶部不同）
      const bottomAdvEls = card.querySelectorAll('.text-xs .whitespace-nowrap span span');
      const bottomAdv = bottomAdvEls.length > 0 ? bottomAdvEls[bottomAdvEls.length - 1].textContent.trim() : advertiserName;

      // 始终推入——用 _dom_idx 保证即使 preview 为空也能区分每张卡片
      results.push({
        _source: 'dom',
        _dom_idx: cardIdx,
        preview_img_url: previewSrc,
        advertiser_name: advertiserName || bottomAdv,
        page_name: bottomAdv,
        platform: platform,
        video_duration: videoDuration,
        heat: heat,
        all_exposure_value: allExposure,
        impression: impression,
        resume_advertising_flag: isRelaunch,
        date_range_text: dateRange,
        resource_urls: [],
      });
    } catch (e) {}
  });
  return results;
}
""")
        return cards if isinstance(cards, list) else []
    except Exception as e:
        print(f"    [DOM补充] 提取失败: {e}")
        return []


async def _click_cards_for_details(
    page,
    known_ad_keys: set,
    max_cards: int = 80,
    target_previews: set | None = None,
) -> list[dict]:
    """
    逐一点击页面上的创意卡片，拦截详情响应，提取完整的 creative 数据。
    target_previews: 若指定，只点击 preview_img_url 匹配的卡片；否则点击所有。
    """
    enriched: list[dict] = []
    detail_holder: list[dict] = []

    async def _on_detail_response(response):
        url = response.url or ""
        if "guangdada" not in url:
            return
        if response.status != 200:
            return
        # 调试：打印所有 guangdada 请求
        print(f"      [detail请求] {url[:120]}")
        try:
            body = await response.json()
        except Exception:
            return
        lists = _extract_creative_lists(body)
        for lst in lists:
            if isinstance(lst, list):
                for c in lst:
                    if isinstance(c, dict) and c.get("ad_key"):
                        detail_holder.append(c)

    page.on("response", _on_detail_response)

    try:
        # 获取所有卡片 index（只取前 max_cards 张）
        card_count = await page.evaluate(
            "() => document.querySelectorAll('.shadow-common-light.bg-white').length"
        )
        card_count = min(int(card_count or 0), max_cards)
        print(f"    [点击详情] 页面卡片总数={card_count}，最多点击 {card_count} 张")

        for idx in range(card_count):
            detail_holder.clear()
            try:
                # 若指定了 target_previews，先检查该卡片的 preview 是否在目标集内
                if target_previews:
                    card_preview = await page.evaluate(f"""
() => {{
  const cards = document.querySelectorAll('.shadow-common-light.bg-white');
  const card = cards[{idx}];
  if (!card) return '';
  const imgs = card.querySelectorAll('img');
  const preview = Array.from(imgs).find(img => img.src && img.src.includes('sp_opera'));
  return preview ? preview.src.split('?')[0] : '';
}}
""")
                    if not card_preview or card_preview not in target_previews:
                        continue

                # 重新查询防止 DOM 变动导致 stale
                clicked = await page.evaluate(f"""
() => {{
  const cards = document.querySelectorAll('.shadow-common-light.bg-white');
  const card = cards[{idx}];
  if (!card) return false;
  card.click();
  return true;
}}
""")
                if not clicked:
                    continue

                # 等待详情响应（最多 4s）
                for _ in range(16):
                    if detail_holder:
                        break
                    await page.wait_for_timeout(250)

                # 把新的 creative 收集起来
                for c in detail_holder:
                    ak = str(c.get("ad_key") or "")
                    if ak and ak not in known_ad_keys:
                        known_ad_keys.add(ak)
                        c["_source"] = "dom_detail"
                        enriched.append(c)

                # 关闭详情弹窗（Escape 或点关闭按钮）
                try:
                    close_btn = page.locator(
                        'button[aria-label="Close"], .ant-modal-close, [class*="close"]'
                    ).first
                    if await close_btn.count() > 0:
                        await close_btn.click(timeout=800)
                    else:
                        await page.keyboard.press("Escape")
                except Exception:
                    await page.keyboard.press("Escape")

                await page.wait_for_timeout(400)

            except Exception as e:
                # 单张失败不中断整体
                await page.keyboard.press("Escape")
                await page.wait_for_timeout(300)
                continue

    finally:
        page.remove_listener("response", _on_detail_response)

    print(f"    [点击详情] 新增完整素材 {len(enriched)} 条（_source=dom_detail）")
    return enriched


def _merge_dom_into_napi(napi_creatives: list[dict], dom_cards: list[dict]) -> list[dict]:
    """
    将 DOM 卡片补充进 napi 结果：
    - 以 preview_img_url 作为去重 key
    - napi 已有的跳过（napi 数据更完整）
    - DOM-only 的卡片追加在末尾，带 _source="dom" 标记
    """
    napi_imgs = {
        str(c.get("preview_img_url") or "").split("?")[0]
        for c in napi_creatives
        if c.get("preview_img_url")
    }
    added = 0
    merged = list(napi_creatives)
    for card in dom_cards:
        img = str(card.get("preview_img_url") or "").split("?")[0]
        if img and img in napi_imgs:
            continue
        merged.append(card)
        if img:
            napi_imgs.add(img)
        added += 1
    if added:
        print(f"    [DOM补充] 新增 {added} 条（napi 未返回）")
    return merged


def _top_creatives_from_batches(batches: list) -> tuple:
    """
    从多批 creative/list 合并去重后，直接取「前 3 条」作为 top_creatives。

    说明：
    - 你希望 top3 与网页展示顺序一致，因此不再按 heat/days 做人为排序。
    - 在取 top3 前会过滤掉明显的赞助广告（原始文案含 Sponsored/赞助 等）。
    返回 (top_creatives, total_count)。
    """
    all_creatives = _all_creatives_from_batches(batches)
    top_creatives = all_creatives[:3] if all_creatives else []
    return top_creatives, len(all_creatives)


def _all_creatives_from_batches(batches: list) -> list:
    """从多批 creative/list 合并去重，返回完整素材列表。"""
    all_creatives = []
    seen_ids = set()

    def is_sponsored(c: dict) -> bool:
        try:
            # 1) 文案/标题中出现 Sponsored/赞助：最强信号，直接过滤
            def _norm(s):
                return str(s or "").strip().lower()

            txt = " ".join(
                [
                    _norm(c.get("title")),
                    _norm(c.get("body")),
                    _norm(c.get("message")),
                    _norm(c.get("page_name")),
                ]
            )
            if "sponsored" in txt or "赞助广告" in txt or "赞助" in txt:
                return True

            # 2) 兼容一些可能的结构字段（作为补充信号）
            ads_type = c.get("ads_type")
            if str(ads_type) == "1":
                return True
            if c.get("is_sponsored") in (1, True, "1", "true", "True"):
                return True
            if c.get("sponsored") in (1, True, "1", "true", "True"):
                return True
            if c.get("is_promoted") in (1, True, "1", "true", "True"):
                return True
            if c.get("promoted") in (1, True, "1", "true", "True"):
                return True
        except Exception:
            return False
        return False

    for b in batches:
        # 跳过 meta 哨兵
        if isinstance(b, dict) and b.get("__meta__"):
            continue
        for c in b:
            if not isinstance(c, dict) or is_sponsored(c):
                continue
            cid = (
                c.get("ad_key")
                or c.get("creative_id")
                or c.get("id")
                or c.get("creativeId")
            )
            if cid:
                if cid in seen_ids:
                    continue
                seen_ids.add(cid)
            all_creatives.append(c)
    return all_creatives


def _sort_creatives_latest_first(creatives: list[dict]) -> list[dict]:
    """
    将素材按“最新”倒序排序（更贴近页面上的「最新创意」）。
    优先 first_seen，其次 created_at；两者均缺失则排在最后。
    """

    def _ts(c: dict) -> int:
        v = c.get("first_seen")
        if v is None:
            v = c.get("created_at")
        try:
            return int(v) if v is not None else 0
        except Exception:
            return 0

    # Python sort 稳定：相同时间戳保持原顺序
    return sorted(creatives, key=_ts, reverse=True)


async def _collect_keyword_crawl_result(
    page,
    keyword: str,
    batches_ref: list,
    capture_state: dict,
    order_by: str = "exposure",
    log_prefix: str = "    ",
    max_scroll_rounds: int = 16,
    enable_dom_track: bool = False,
) -> dict:
    """单关键词：搜索、滚动、合并 napi 结果（与 `run_batch` 单轮逻辑一致，供 Arrow2 复用）。"""
    result_for_kw: dict | None = None
    for attempt in range(1, 3):
        try:
            await _search_one_keyword(
                page,
                keyword,
                batches_ref,
                capture_state,
                order_by=order_by,
                log_prefix=log_prefix,
                max_scroll_rounds=max_scroll_rounds,
            )
            top_creatives, total = _top_creatives_from_batches(batches_ref)
            napi_creatives = _all_creatives_from_batches(batches_ref)
            all_creatives = list(napi_creatives)

            dom_creatives: list[dict] = []
            if enable_dom_track:
                dom_cards = await _extract_dom_cards(page)
                napi_preview_set = {
                    str(c.get("preview_img_url") or "").split("?")[0]
                    for c in napi_creatives if c.get("preview_img_url")
                }
                napi_preview_set_nonempty = {
                    str(c.get("preview_img_url") or "").split("?")[0]
                    for c in napi_creatives if c.get("preview_img_url")
                }
                dom_only_cards: list[dict] = []
                for c in dom_cards:
                    img = str(c.get("preview_img_url") or "").split("?")[0]
                    if img and img in napi_preview_set_nonempty:
                        continue
                    dom_only_cards.append(c)

                known_keys = {str(c.get("ad_key") or "") for c in napi_creatives if c.get("ad_key")}
                dom_preview_set = {
                    str(c.get("preview_img_url") or "").split("?")[0]
                    for c in dom_only_cards if c.get("preview_img_url")
                }
                detail_creatives = await _click_cards_for_details(
                    page, known_keys, max_cards=len(dom_only_cards) + 5, target_previews=dom_preview_set or None
                )
                detail_by_preview = {
                    str(c.get("preview_img_url") or "").split("?")[0]: c
                    for c in detail_creatives
                    if c.get("preview_img_url")
                }
                seen_dom: set[str] = set()
                for card in dom_only_cards:
                    img = str(card.get("preview_img_url") or "").split("?")[0]
                    key = img if img else f"_idx_{card.get('_dom_idx', id(card))}"
                    if key in seen_dom:
                        continue
                    seen_dom.add(key)
                    dom_creatives.append(detail_by_preview.get(img, card) if img else card)
                for c in detail_creatives:
                    img = str(c.get("preview_img_url") or "").split("?")[0]
                    if img and img not in seen_dom:
                        dom_creatives.append(c)
                        seen_dom.add(img)
                print(
                    f"{log_prefix}[DOM track] dom_basic={len(dom_only_cards)}  "
                    f"dom_detail={len(detail_creatives)}  最终={len(dom_creatives)}"
                )

            if order_by == "latest" and all_creatives:
                all_creatives = _sort_creatives_latest_first(all_creatives)
            try:
                tz = timezone(timedelta(hours=8))

                def _ts2(c: dict) -> int | None:
                    v = c.get("first_seen")
                    if v is None:
                        v = c.get("created_at")
                    try:
                        return int(v) if v is not None else None
                    except Exception:
                        return None

                head = all_creatives[:3] if isinstance(all_creatives, list) else []
                times: list[str] = []
                for c in head:
                    if not isinstance(c, dict):
                        continue
                    tsv = _ts2(c)
                    if tsv is None:
                        continue
                    times.append(datetime.fromtimestamp(tsv, tz=tz).strftime("%Y-%m-%d %H:%M:%S"))
                if times:
                    print(f"{log_prefix}[校验] 前3条时间(UTC+8): {times}")
            except Exception:
                pass
            best = top_creatives[0] if top_creatives else None
            result_for_kw = {
                "keyword": keyword,
                "selected": best,
                "top_creatives": top_creatives,
                "all_creatives": all_creatives,
                "napi_creatives": napi_creatives,
                "dom_creatives": dom_creatives,
                "total_captured": total,
            }
            if all_creatives:
                break
            if attempt == 1:
                print("    [提示] 当前未捕获到素材（all_creatives 为空），准备重试一次...", file=sys.stderr)
                continue
        except Exception as e:
            if attempt == 1:
                print(f"    [失败] {e}，准备重试一次...", file=sys.stderr)
                continue
            print(f"    [失败] {e}", file=sys.stderr)
            break
    if result_for_kw is None:
        return {
            "keyword": keyword,
            "selected": None,
            "top_creatives": [],
            "all_creatives": [],
            "napi_creatives": [],
            "dom_creatives": [],
            "total_captured": 0,
        }
    if not result_for_kw.get("all_creatives"):
        print(
            "    [提醒] 两次尝试后仍未捕获到素材（all_creatives 为空），"
            "请检查页面结构或筛选条件是否变化。",
            file=sys.stderr,
        )
    return result_for_kw


async def run_batch(
    keywords: list,
    debug: bool = False,
    is_tool: bool = False,
    order_by: str = "exposure",
    use_popularity_top1: bool = False,
    enable_dom_track: bool = False,
) -> list:
    """
    登录一次、界面设置一次（工具/7天/素材/排序方式），然后对每个关键词只做「填关键字 → 搜索 → 取结果」。
    返回与 run() 相同结构的列表，每项对应一个关键词：
      {
        "keyword", "selected", "top_creatives", "all_creatives", "total_captured"
      }。
    order_by: "exposure"（展示估值）或 "latest"（最新创意）。
    enable_dom_track: 是否启用 DOM 补充 + 点击详情（默认关闭，仅供 fetch_competitor_raw 等调试脚本使用）。
    """
    if not keywords:
        return []
    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not email or not password:
        print("[错误] 请在 .env 中设置 GUANGDADA_EMAIL 和 GUANGDADA_PASSWORD", file=sys.stderr)
        sys.exit(1)

    playwright_proxy = prepare_playwright_proxy_for_crawl()

    batches_ref = []
    capture_state = {"enabled": False}

    async def on_response(response):
        """
        监听创意列表接口，把最近几次返回的 creative_list 追加到 batches_ref。
        Top创意 / 人气值Top1% 下，接口路径和 data 结构可能有变化，因此这里只要是 guangdada 的 napi，
        就在整个 JSON 里递归查找「包含 ad_key/creative_id 等字段的列表」，作为创意列表。
        """
        if not capture_state.get("enabled"):
            return
        url = response.url
        if "guangdada.net/napi" not in url or response.status != 200:
            return
        try:
            body = await response.json()
        except Exception:
            return
        # 在整个 JSON 中递归查找「创意列表」
        lists = _extract_creative_lists(body)
        if not lists:
            return

        # 可能一次响应里包含多个“创意列表”（不同模块/组件），这里全部收集，后续再去重
        for lst in lists:
            if isinstance(lst, list) and lst:
                batches_ref.append(lst)
        # 只保留最近几批，避免无限增长
        # 这里上限不要太小，否则滚动加载会被截断，只剩很少条素材
        if len(batches_ref) > 80:
            batches_ref.pop(0)

    async with async_playwright() as p:
        launch_kw: dict = {"headless": not debug}
        if playwright_proxy:
            launch_kw["proxy"] = playwright_proxy
        browser = await p.chromium.launch(**launch_kw)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = await context.new_page()
        page.on("response", on_response)

        try:
            print("[1/4] 正在登录...")
            if not await login(page, email, password):
                print("[失败] 登录失败", file=sys.stderr)
                sys.exit(2)
            print("[1/4] 登录成功 ✓")
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            await page.wait_for_timeout(4000)

            print("[2/4] 一次性设置筛选（7天 / 素材 / 排序方式 / 可选人气值Top1%）...")
            await _do_setup(
                page,
                is_tool,
                log_prefix="  ",
                order_by=order_by,
                use_popularity_top1=use_popularity_top1,
            )
            print("[2/4] 设置完成 ✓")

            print("[3/4] 按关键词依次搜索并拉取数据...")
            results = []
            for i, keyword in enumerate(keywords, 1):
                print(f"  [{i}/{len(keywords)}] {keyword}")
                result_for_kw = await _collect_keyword_crawl_result(
                    page,
                    keyword,
                    batches_ref,
                    capture_state,
                    order_by=order_by,
                    log_prefix="    ",
                    max_scroll_rounds=16,
                    enable_dom_track=enable_dom_track,
                )
                results.append(result_for_kw)
            print("[3/4] 全部关键词搜索完成 ✓")
            print("[4/4] 关闭浏览器")
            return results
        finally:
            await browser.close()


async def run(keyword: str, debug: bool = False, is_tool: bool = False):
    """单关键词入口：内部走 run_batch([keyword])，保持返回格式与 keyword_result.json 兼容。"""
    print(f"关键词: {keyword}")
    results = await run_batch([keyword], debug=debug, is_tool=is_tool)
    result = results[0] if results else {
        "keyword": keyword,
        "selected": None,
        "top_creatives": [],
        "total_captured": 0,
    }
    out_file = OUT_DIR / "keyword_result.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"[结果] 已写入 {out_file.name}")
    best_creative = result.get("selected")
    top_creatives = result.get("top_creatives") or []
    if best_creative:
        name = best_creative.get("title") or best_creative.get("app_name") or "N/A"
        days = best_creative.get("days_count", "?")
        heat = best_creative.get("heat", "?")
        print(f"  - 热度最高的素材: {name[:50]}")
        print(f"    投放天数: {days} 天, 热度: {heat}")
        if len(top_creatives) > 1:
            print(f"  - 共选出热度前 {len(top_creatives)} 条素材")
    else:
        print("  - 未捕获到素材")
    print("\n" + json.dumps({
        "keyword": result["keyword"],
        "total_captured": result["total_captured"],
        "selected_title": best_creative.get("title") if best_creative else None,
    }, ensure_ascii=False))
    return result


async def run_arrow2_batch(  # noqa: PLR0912,PLR0915
    keywords: list,
    debug: bool = False,
    is_tool: bool = False,
    is_game: bool = True,
    day_spans: list | None = None,
    order_modes: list | None = None,
    popularity_option_text: str | None = None,
    ad_channel_labels: list | None = None,
    country_codes: list | None = None,
    pull_specs: list | None = None,
    pull_spec_defaults: dict | None = None,
    search_tab: str = "game",
    keyword_appid: dict | None = None,  # noqa: ARG001
    debug_step_per_product: bool = False,
    keyword_product: dict[str, str] | None = None,
    target_date_first_seen: str | None = None,
    debug_pause_per_product: bool = False,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    """
    Arrow2 竞品拉取：按 pull_specs（或 day_spans×order_modes）循环设置筛选，再对每词搜索、napi 合批；
    支持仅昨日 `first_seen` 过滤、Top 创意文案匹配、多轮滚动（latest_yesterday 默认更深滚动）。

    注：与 AGENTS 所述「全 DOM+detail-v2+地区 napi 补全」的 2k 行版相比，本实现为 **napi 主路径**；
    渠道/国家未自动点选（与旧 `run_batch` 一致），可后续再接 UI。
    """
    _ = (
        is_game,
        keyword_appid,
        kwargs,
    )  # 保留与调用方签名兼容
    if ad_channel_labels or country_codes:
        print(
            "[arrow2] 渠道/国家筛选：当前未在 Playwright 里逐一点选，结果依赖页面已选或默认。",
            file=sys.stderr,
        )
    if not keywords:
        return []
    _email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    _password = os.getenv("GUANGDADA_PASSWORD")
    if not _email or not _password:
        print("[错误] 请在 .env 中设置 GUANGDADA_EMAIL 和 GUANGDADA_PASSWORD", file=sys.stderr)
        sys.exit(1)

    defaults: dict[str, Any] = dict(pull_spec_defaults) if pull_spec_defaults else {}
    base_specs: list[dict[str, Any]] = []
    if pull_specs and len(pull_specs) > 0:
        for p in pull_specs:
            if isinstance(p, dict):
                merged: dict[str, Any] = {**defaults, **p}
                base_specs.append(merged)
    if not base_specs:
        dlist = [str(d) for d in (day_spans or ["7"])]
        olist = [str(x) for x in (order_modes or ["exposure"])]
        for ds in dlist:
            for ob in olist:
                base_specs.append(
                    {
                        **defaults,
                        "id": f"matrix_{ds}_{ob}",
                        "day_span": ds,
                        "order_by": ob,
                        "popularity_option_text": popularity_option_text,
                    }
                )
    if not base_specs:
        base_specs = [
            {**defaults, "id": "default", "day_span": "7", "order_by": "exposure", "popularity_option_text": popularity_option_text}
        ]

    tz8 = timezone(timedelta(hours=8))
    first_seen_ymd = (target_date_first_seen or (os.environ.get("TARGET_DATE") or "").strip()[:10] or None)
    if not first_seen_ymd:
        first_seen_ymd = (datetime.now(tz8).date() - timedelta(days=1)).isoformat()

    playwright_proxy = prepare_playwright_proxy_for_crawl()
    out: list[dict[str, Any]] = []

    async with async_playwright() as p:
        launch_kw: dict = {"headless": not debug}
        if playwright_proxy:
            launch_kw["proxy"] = playwright_proxy
        browser = await p.chromium.launch(**launch_kw)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = await context.new_page()
        batches_ref: list = []
        capture_state: dict = {"enabled": False}

        async def on_response(response):
            if not capture_state.get("enabled"):
                return
            if "guangdada.net/napi" not in response.url or response.status != 200:
                return
            try:
                body = await response.json()
            except Exception:
                return
            lists = _extract_creative_lists(body)
            for lst in lists:
                if isinstance(lst, list) and lst:
                    batches_ref.append(lst)
            if len(batches_ref) > 80:
                batches_ref.pop(0)

        page.on("response", on_response)

        try:
            print("[arrow2 1/4] 正在登录…")
            if not await login(page, _email, _password):
                print("[失败] 登录失败", file=sys.stderr)
                sys.exit(2)
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            await page.wait_for_timeout(4000)
            await _try_click_search_tab(page, search_tab)
            is_tool_effective = bool(is_tool)

            for spec in base_specs:
                ds = str(spec.get("day_span") or "7")
                ob = str(spec.get("order_by") or "exposure")
                popt = spec.get("popularity_option_text")
                if popt is None and isinstance(spec.get("popularity_option_text"), str) is False:
                    popt = None
                if popt is None and popularity_option_text:
                    popt = popularity_option_text
                use_pfirst = bool(spec.get("popularity_pick_first", False))
                need_pop = bool(popt) or use_pfirst
                await _do_setup(
                    page,
                    is_tool_effective,
                    log_prefix="  [arrow2] ",
                    order_by=ob,
                    use_popularity_top1=use_pfirst and not popt,
                    day_span=ds,
                    popularity_option_text=(str(popt).strip() if popt else None) if popt else None,
                )
                for ki, keyword in enumerate(keywords, 1):
                    if not (keyword and str(keyword).strip()):
                        continue
                    kw = str(keyword).strip()
                    print(f"  [arrow2 pull={spec.get('id')!r} {ki}/{len(keywords)}] {kw}")
                    do_scroll = bool(
                        spec.get("filter_yesterday_only")
                        and spec.get("scroll_until_past_target_date", True) is not False
                    )
                    max_rounds = 56 if do_scroll else 16
                    r = await _collect_keyword_crawl_result(
                        page,
                        kw,
                        batches_ref,
                        capture_state,
                        order_by=ob,
                        log_prefix="    ",
                        max_scroll_rounds=max_rounds,
                        enable_dom_track=False,
                    )
                    raw_all: list[dict] = [x for x in (r.get("all_creatives") or []) if isinstance(x, dict)]
                    filtered = _arrow2_apply_post_filters(
                        raw_all, spec, kw, keyword_product, first_seen_ymd
                    )
                    if ob == "latest" and filtered and not spec.get("filter_yesterday_only"):
                        filtered = _sort_creatives_latest_first(filtered)
                    f_top = filtered[:3]
                    f_best = f_top[0] if f_top else None
                    r["all_creatives"] = filtered
                    r["top_creatives"] = f_top
                    r["selected"] = f_best
                    r["total_captured"] = len(filtered)
                    r["day_span"] = ds
                    r["order_by"] = ob
                    r["pull_id"] = spec.get("id")
                    r["pull_spec"] = spec
                    r["list_source"] = "napi"
                    if debug_step_per_product:
                        try:
                            n_show = int(os.getenv("ARROW2_DEBUG_STEP_CARDS") or "10")
                        except Exception:
                            n_show = 10
                        _print_debug_step_cards(kw, filtered, n_show)
                        if os.environ.get("ARROW2_DEBUG_STEP_AUTO_ENTER", "").lower() in (
                            "1",
                            "true",
                            "y",
                        ):
                            pass
                        else:
                            _ = input("[debug-step] 按 Enter 进入下一词… ")
                    if debug_pause_per_product:
                        _print_pause_yesterday_summary(filtered)
                        _ = input("[arrow2] 按 Enter 进入下一产品… ")

                    out.append(r)
            if debug and _env_truthy(
                "ARROW2_DEBUG_PAUSE_AT_END", default=(not debug_step_per_product)
            ):
                _ = input("[arrow2] 关浏览器前暂停，按 Enter 继续… ")
        finally:
            await browser.close()

    return out


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
