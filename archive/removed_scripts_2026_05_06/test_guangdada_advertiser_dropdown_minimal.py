"""
极简：登录 → 打开页 → 在搜索框**只输入词、不点选**；把联想结果里解析出的
**展示名 + appid** 打到终端。

若你用的是顶栏**综合**搜索（下拉里是表格：列头含「近90天创意」「上月下载」等），
与旧版「搜索广告主 ant-select」DOM 不同；脚本会读 config/ guangdada_advertiser_suggest_markers.json
 与 page.evaluate 在页面里按表头特征定位浮层，再抽 com.，逻辑同 operation.json/手工录制
  那套「用页面内稳定文案找节点」。

  .venv/bin/python scripts/test_guangdada_advertiser_dropdown_minimal.py
  .venv/bin/python scripts/test_guangdada_advertiser_dropdown_minimal.py -q "com.arrow"

`--url` 需是你页面里能出现「搜索广告主」的地址。默认有头，结束按 Enter 关浏览器。
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import traceback

from dotenv import load_dotenv
from playwright.async_api import async_playwright

from guangdada_login import GUANGDADA_BASE_URL, login
from path_util import PROJECT_ROOT
from proxy_util import prepare_playwright_proxy_for_crawl
from run_search_workflow import (
    _collect_advertiser_select_options_for_debug,
    _fill_main_advertiser_search_keyword,
)

load_dotenv(PROJECT_ROOT / ".env")

_DEFAULT_PAGE = f"{GUANGDADA_BASE_URL.rstrip('/')}/modules/creative/display-ads"


async def _main_async(*, start_url: str, query: str, wait_ms: int) -> int:
    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not email or not password:
        print("请配置 .env：GUANGDADA_EMAIL、GUANGDADA_PASSWORD", file=sys.stderr)
        return 1

    proxy = prepare_playwright_proxy_for_crawl()
    launch: dict = {"headless": False}
    if proxy:
        launch["proxy"] = proxy

    n_options = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(**launch)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
        )
        page = await context.new_page()
        try:
            if not await login(page, email, password):
                return 2
            try:
                await page.wait_for_load_state("networkidle", timeout=20000)
            except Exception:
                pass
            await page.wait_for_timeout(2000)
            print(f"打开: {start_url}")
            await page.goto(
                start_url,
                wait_until="domcontentloaded",
                timeout=90000,
            )
            try:
                await page.wait_for_load_state("networkidle", timeout=25000)
            except Exception:
                pass
            await page.wait_for_timeout(2000)

            log_prefix = "  "
            print(f"输入: {query!r}（不点候选项，只读下拉里）")
            await _fill_main_advertiser_search_keyword(page, query, log_prefix)
            rows = await _collect_advertiser_select_options_for_debug(
                page, log_prefix, max_wait_ms=wait_ms
            )
            n_options = len(rows)
            if not rows:
                print(
                    f"未读到任何下拉里候选项（约 {wait_ms // 1000}s 内无可见项，"
                    f"或页面上没有「搜索广告主」）。"
                )
            else:
                print(f"下拉里共 {n_options} 条（去重后）：")
                for i, r in enumerate(rows, 1):
                    print(
                        f"  [{i}] 展示名: {r.get('product') or '-'}  "
                        f"appid: {r.get('appid') or '-'}"
                    )
                    raw = (r.get("raw") or "").replace("\n", " / ")
                    if raw:
                        print(f"        原文: {raw[:300]!r}")
        except Exception as e:
            print(f"异常: {e}", file=sys.stderr)
            traceback.print_exc()
        finally:
            try:
                input("按 Enter 关闭浏览器…")
            except (EOFError, KeyboardInterrupt):
                pass
            await browser.close()
    return 0 if n_options else 3


def main() -> None:
    ap = argparse.ArgumentParser(
        description="只测：搜索框输入后打印下拉里展示名+appid（不点击）"
    )
    ap.add_argument(
        "-q",
        "--query",
        default="com.arrow",
        help="填入「搜索广告主」的文本（默认: com.arrow）",
    )
    ap.add_argument(
        "--url",
        default=_DEFAULT_PAGE,
        help="需含「搜索广告主」的页面 URL",
    )
    ap.add_argument(
        "--wait-ms",
        type=int,
        default=20_000,
        help="轮询下拉里候选项的最长毫秒（默认 20000）",
    )
    args = ap.parse_args()
    q = (args.query or "").strip()
    if not q:
        print("-q 不能为空", file=sys.stderr)
        sys.exit(1)

    code = asyncio.run(
        _main_async(
            start_url=args.url.strip(),
            query=q,
            wait_ms=max(3_000, int(args.wait_ms)),
        )
    )
    sys.exit(code)


if __name__ == "__main__":
    main()
