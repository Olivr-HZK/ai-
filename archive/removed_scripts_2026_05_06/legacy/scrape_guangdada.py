"""
广大大网站爬取测试脚本
使用 Playwright 模拟登录、点击操作、爬取数据

支持：
- 模拟登录（弹窗表单）
- 点击导航、筛选、搜索
- 可选：监听并打印 API 请求（便于后续抓包分析）
"""

import asyncio
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page

# 加载环境变量（将账号密码放在 .env 中，勿提交到 git）
load_dotenv()

# 广大大配置
GUANGDADA_BASE_URL = "https://www.guangdada.net"

from guangdada_login import login
from path_util import DATA_DIR


async def _safe_click(page: Page, selector: str, desc: str = "") -> bool:
    """安全点击，找不到元素不报错"""
    try:
        loc = page.locator(selector)
        if await loc.count() > 0:
            await loc.first.click(timeout=5000)
            print(f"  ✓ 点击: {desc or selector}")
            return True
    except Exception as e:
        print(f"  ✗ 点击失败 {desc or selector}: {e}")
    return False


# 从 session_log 提取的操作序列
TAG_GOODS_SORT = 4007  # Goods Sort 分类的 tag_id


async def auto_run_filter_flow(page: Page, context):
    """
    自动执行你记录的操作：展示广告 → 已订阅广告主 → Goods Sort → 7天 → 展示估值
    并采集创意数据。
    """
    creative_list_data = []
    subscribed_advertisers = []
    out_dir = DATA_DIR
    filters_applied = False

    async def on_response(response):
        nonlocal filters_applied
        url = response.url
        if "subscription/query-subscribed-advertiser" in url and response.status == 200:
            filters_applied = True
            try:
                body = await response.json()
                data = body.get("data", [])
                if isinstance(data, list) and data:
                    subscribed_advertisers.extend(data)
            except Exception:
                pass
        elif "napi/v1/creative/list" in url and response.status == 200 and filters_applied:
            try:
                body = await response.json()
                lst = body.get("data", {}).get("creative_list", [])
                if lst:
                    creative_list_data.clear()
                    creative_list_data.extend(lst)
                    print(f"  [采集] {len(lst)} 条创意")
            except Exception:
                pass

    page.on("response", on_response)

    # 1. 进入创意灵感
    await page.goto(f"{GUANGDADA_BASE_URL}/creative", wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_load_state("networkidle", timeout=20000)
    await page.wait_for_timeout(2000)
    print("  ✓ 进入创意灵感")

    # 2. 点击 展示广告
    await _safe_click(page, "text=展示广告", "展示广告")
    await page.wait_for_timeout(1500)

    # 3. 勾选 已订阅广告主
    for sel in ['label:has-text("已订阅广告主")', 'span:has-text("已订阅广告主")', '[class*="subscri"]:has-text("已订阅广告主")']:
        if await _safe_click(page, sel, "已订阅广告主"):
            break
    await page.wait_for_timeout(2500)

    # 4. 选择 Goods Sort（tag_id=4007）
    for sel in ['text="Goods Sort"', 'text="Good Sort"', f'[data-id="{TAG_GOODS_SORT}"]']:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.first.click(timeout=3000)
                print(f"  ✓ 点击: Goods Sort")
                break
        except Exception:
            pass
    await page.wait_for_timeout(2000)

    # 5. 选择 7天
    await _safe_click(page, 'button:has-text("7天"), [role="button"]:has-text("7天"), text="7天"', "7天")
    await page.wait_for_timeout(2500)

    # 6. 选择 展示估值
    for sel in ['[class*="sort"]:has-text("排序")', 'span:has-text("排序")', 'text=排序']:
        if await _safe_click(page, sel, "排序下拉"):
            await page.wait_for_timeout(800)
            break
    await _safe_click(page, 'text=展示估值', "展示估值")
    await page.wait_for_timeout(3000)
    await page.wait_for_load_state("networkidle", timeout=15000)

    # 保存
    if creative_list_data:
        with open(out_dir / "creative_data.json", "w", encoding="utf-8") as f:
            json.dump(creative_list_data, f, ensure_ascii=False, indent=2)
        print(f"\n已保存 {len(creative_list_data)} 条创意 → creative_data.json")
    if subscribed_advertisers:
        with open(out_dir / "subscribed_advertisers.json", "w", encoding="utf-8") as f:
            json.dump(subscribed_advertisers, f, ensure_ascii=False, indent=2)
        print(f"已保存 {len(subscribed_advertisers)} 个订阅广告主")


async def record_full_session(page: Page, context):
    """
    纯记录模式：跳过自动登录，完整记录从登录到筛选的全部操作。
    你手动：登录 → 创意灵感 → 展示广告 → 已订阅广告主 → good sort → 7天 → 展示估值
    脚本会记录所有相关 API 请求及响应。
    """
    import time
    creative_list_data = []
    subscribed_advertisers = []
    out_dir = DATA_DIR
    filters_applied = False
    # 完整会话记录：按时间顺序记录所有 napi 请求
    session_log = []

    async def on_response(response):
        nonlocal filters_applied
        url = response.url
        if "guangdada.net/napi" not in url:
            return
        entry = {"url": url, "status": response.status, "ts": round(time.time(), 2)}
        try:
            if response.status == 200 and "json" in response.headers.get("content-type", ""):
                body = await response.json()
                if "subscription/query-subscribed-advertiser" in url:
                    filters_applied = True
                    data = body.get("data", [])
                    if isinstance(data, list) and data:
                        subscribed_advertisers.extend(data)
                        entry["data"] = body
                        print(f"  [记录] 订阅广告主 {len(data)} 个")
                elif "napi/v1/creative/list" in url:
                    lst = body.get("data", {}).get("creative_list", [])
                    entry["creative_count"] = len(lst)
                    if filters_applied and lst:
                        creative_list_data.clear()
                        creative_list_data.extend(lst)
                        print(f"  [记录] 创意列表 {len(lst)} 条 (已筛选)")
                    elif lst:
                        print(f"  [记录] 创意列表 {len(lst)} 条 (未筛选，跳过)")
                elif "user" in url or "jwt" in url or "nbs-info" in url:
                    entry["data_preview"] = str(body)[:500]
                    print(f"  [记录] {url.split('/')[-1].split('?')[0]}")
                else:
                    entry["data_preview"] = str(body)[:300]
            session_log.append(entry)
        except Exception as e:
            entry["error"] = str(e)
            session_log.append(entry)

    page.on("response", on_response)

    await page.goto(f"{GUANGDADA_BASE_URL}/creative", wait_until="domcontentloaded", timeout=30000)
    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
    except Exception:
        pass
    await page.wait_for_timeout(2000)

    print("\n" + "=" * 55)
    print("  已登录，请手动执行筛选：")
    print("=" * 55)
    print("  1. 创意灵感 → 展示广告")
    print("  2. 勾选 已订阅广告主 → 选择 good sort")
    print("  3. 选择 7天 → 选择 展示估值")
    print("=" * 55)
    print("  完成后回到终端按 Enter 保存并退出\n")

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, lambda: input())

    # 保存
    with open(out_dir / "session_log.json", "w", encoding="utf-8") as f:
        json.dump(session_log, f, ensure_ascii=False, indent=2)
    print(f"\n已保存会话记录 {len(session_log)} 条 → session_log.json")

    if subscribed_advertisers:
        with open(out_dir / "subscribed_advertisers.json", "w", encoding="utf-8") as f:
            json.dump(subscribed_advertisers, f, ensure_ascii=False, indent=2)
        print(f"已保存订阅广告主 {len(subscribed_advertisers)} 个")

    if creative_list_data:
        with open(out_dir / "creative_data.json", "w", encoding="utf-8") as f:
            json.dump(creative_list_data, f, ensure_ascii=False, indent=2)
        print(f"已保存创意 {len(creative_list_data)} 条")


async def main():
    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not email or not password:
        print("错误: 请在 .env 中设置 GUANGDADA_EMAIL 和 GUANGDADA_PASSWORD", file=sys.stderr)
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = await context.new_page()
        try:
            if not await login(page, email, password):
                print("登录失败")
                return
            record_mode = os.getenv("RECORD_MODE", "").lower() in ("1", "true", "yes")
            if record_mode:
                await record_full_session(page, context)
            else:
                await auto_run_filter_flow(page, context)
        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
