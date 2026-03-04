"""
自动执行关键词工作流（无界面）
输入: keyword
输出: 两个素材
  1. 展示量最高 第 1 条
  2. 最新 素材中展示量最高的 1 条

用法: python run_keyword_workflow.py <keyword>
例:   python run_keyword_workflow.py puzzle
"""
import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

from guangdada_login import GUANGDADA_BASE_URL, login

from path_util import DATA_DIR

OUT_DIR = DATA_DIR


async def _safe_click(page, selector: str, timeout: int = 5000) -> bool:
    try:
        loc = page.locator(selector)
        if await loc.count() > 0:
            await loc.first.click(timeout=timeout)
            return True
    except Exception:
        pass
    return False


async def run(keyword: str, debug: bool = False):
    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not email or not password:
        print("错误: 请在 .env 中设置 GUANGDADA_EMAIL 和 GUANGDADA_PASSWORD", file=sys.stderr)
        sys.exit(1)

    batches = []
    after_search = False  # 搜索后才收集，避免初始无筛选数据

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

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not debug)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = await context.new_page()
        page.on("response", on_response)

        try:
            if not await login(page, email, password):
                print("登录失败", file=sys.stderr)
                sys.exit(2)
            await page.goto(f"{GUANGDADA_BASE_URL}/creative", wait_until="domcontentloaded", timeout=30000)
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass
            await page.wait_for_timeout(2000)
            await page.reload(wait_until="domcontentloaded")
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass
            await page.wait_for_timeout(2000)

            search = page.locator('input[placeholder*="搜索"], input[placeholder*="关键词"]').first
            if await search.count() == 0:
                search = page.locator('input[type="text"]').first
            if await search.count() > 0:
                await search.fill(keyword)
            await page.wait_for_timeout(500)

            await _safe_click(page, 'button:has-text("搜索"), [class*="search"] button, button[type="button"]')
            after_search = True
            await page.wait_for_timeout(3000)

            await _safe_click(page, 'text=素材内容', 3000)
            await _safe_click(page, 'text=素材', 3000)
            await page.wait_for_timeout(2500)

            await _safe_click(page, 'text=最新', 3000)
            await page.wait_for_timeout(3500)

            await _safe_click(page, 'text=展示估值, text=展示量最高', 3000)
            await page.wait_for_timeout(3500)

        finally:
            await browser.close()

    if len(batches) < 2:
        print(f"[提示] 仅捕获 {len(batches)} 批数据，可能未登录或选择器未匹配。可加 --debug 查看浏览器", file=sys.stderr)

    latest_batch = batches[-2] if len(batches) >= 2 else []
    top_exposure_batch = batches[-1] if batches else []

    creative1 = top_exposure_batch[0] if top_exposure_batch else None
    creative2 = None
    if latest_batch:
        creative2 = max(
            latest_batch,
            key=lambda x: x.get("all_exposure_value", 0) or x.get("impression", 0),
        )

    result = {
        "keyword": keyword,
        "top_by_exposure": creative1,
        "latest_top_by_exposure": creative2,
    }

    out_file = OUT_DIR / "keyword_result.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return result


def main():
    parser = argparse.ArgumentParser(description="关键词搜索，获取展示量最高 + 最新中展示量最高 两个素材")
    parser.add_argument("keyword", nargs="?", default="", help="搜索关键词")
    parser.add_argument("--debug", action="store_true", help="显示浏览器窗口便于调试")
    args = parser.parse_args()
    keyword = args.keyword.strip() or input("请输入关键词: ").strip()
    if not keyword:
        print("错误: 需要提供关键词", file=sys.stderr)
        sys.exit(1)
    asyncio.run(run(keyword, debug=args.debug))


if __name__ == "__main__":
    main()
