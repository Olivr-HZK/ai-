"""
单独测试广大大登录（不跑搜索、不写库）。

有头模式（便于观察）：
  DEBUG=1 python scripts/test_guangdada_login.py

无头快速测：
  python scripts/test_guangdada_login.py

依赖 .env：GUANGDADA_EMAIL、GUANGDADA_PASSWORD；可选 PLAYWRIGHT_PROXY_SERVER。
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright

from guangdada_login import login
from proxy_util import prepare_playwright_proxy_for_crawl

load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def _truthy_debug(s: str | None) -> bool:
    return str(s or "").strip().lower() in ("1", "true", "yes", "on")


async def _run(*, debug: bool) -> int:
    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not email or not password:
        print("[test] 请在 .env 中设置 GUANGDADA_EMAIL 与 GUANGDADA_PASSWORD", file=sys.stderr)
        return 1

    playwright_proxy = prepare_playwright_proxy_for_crawl()
    launch_kw: dict = {"headless": not debug}
    if playwright_proxy:
        launch_kw["proxy"] = playwright_proxy
        print("[test] Playwright 代理:", playwright_proxy)
    else:
        print("[test] 未配置 PLAYWRIGHT_PROXY_SERVER，直连广大大")

    if debug:
        print("[test] DEBUG：有头浏览器（headless=False），可观察登录页与跳转")

    async with async_playwright() as p:
        browser = await p.chromium.launch(**launch_kw)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = await context.new_page()
        ok = False
        try:
            print("[test] 开始登录…")
            ok = await login(page, email, password)
        finally:
            if debug and sys.stdin.isatty():
                try:
                    input("[test] 按回车关闭浏览器…")
                except EOFError:
                    await page.wait_for_timeout(5000)
            elif debug:
                await page.wait_for_timeout(8000)
            await browser.close()

    if ok:
        print("[test] 结果: 登录成功")
        return 0
    print("[test] 结果: 登录失败", file=sys.stderr)
    return 2


def main() -> None:
    ap = argparse.ArgumentParser(description="单独测试广大大登录")
    ap.add_argument(
        "--debug",
        action="store_true",
        help="有头浏览器（等价于 DEBUG=1）",
    )
    args = ap.parse_args()
    debug = args.debug or _truthy_debug(os.getenv("DEBUG"))
    raise SystemExit(asyncio.run(_run(debug=debug)))


if __name__ == "__main__":
    main()
