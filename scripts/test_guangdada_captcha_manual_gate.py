from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from playwright.async_api import Page, async_playwright  # noqa: E402

from ua_workflows.shared.config import load_project_env  # noqa: E402
from ua_workflows.shared.guangdada.login import login  # noqa: E402
from ua_workflows.shared.guangdada.interrupt_alerts import (  # noqa: E402
    DEFAULT_GUIDE_IMAGE_PATH,
    start_feishu_confirmation_waiter,
)
from ua_workflows.shared.guangdada.proxy import prepare_playwright_proxy_for_crawl  # noqa: E402
from ua_workflows.shared.guangdada.search import _send_guangdada_security_verification_alert  # noqa: E402


DEFAULT_URL = "https://www.guangdada.net/modules/creative/charts/new-charts"
CAPTCHA_SELECTORS = [
    ".ant-modal-wrap:visible .ant-modal-title:has-text('安全验证')",
    ".ant-modal-content:has-text('请按住滑块')",
    ".captcha-modal-content:has-text('您的操作过于频繁')",
    "#modal-captcha-container",
    "#aliyunCaptcha-sliding-slider",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "广大大安全验证飞书回调闸口测试。脚本只检测滑块弹窗并等待飞书确认，"
            "不会自动拖动或绕过验证码。"
        )
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="登录后打开的目标页")
    parser.add_argument("--detect-timeout-sec", type=int, default=45, help="等待安全验证出现的秒数")
    parser.add_argument("--solve-timeout-sec", type=int, default=900, help="等待飞书「已完成」回调的秒数")
    parser.add_argument("--require-captcha", action="store_true", help="未检测到验证码时以失败退出")
    parser.add_argument("--skip-login", action="store_true", help="跳过账号密码登录，直接打开 URL")
    parser.add_argument("--callback-port", type=int, default=0, help="本地飞书确认回调端口；默认随机端口")
    parser.add_argument("--keep-open", action="store_true", help="结束前保持浏览器打开 30 秒便于观察")
    return parser.parse_args()


async def captcha_visible(page: Page) -> bool:
    for selector in CAPTCHA_SELECTORS:
        try:
            loc = page.locator(selector)
            if await loc.count() > 0 and await loc.first.is_visible(timeout=1000):
                return True
        except Exception:
            continue
    try:
        text = await page.locator("body").inner_text(timeout=1500)
    except Exception:
        return False
    return "安全验证" in text and "请按住滑块" in text


async def wait_for_captcha(page: Page, timeout_sec: int) -> bool:
    deadline = max(1, timeout_sec) * 1000
    step_ms = 1000
    waited = 0
    while waited <= deadline:
        if await captcha_visible(page):
            return True
        await page.wait_for_timeout(step_ms)
        waited += step_ms
    return False


async def run() -> int:
    args = parse_args()
    load_project_env(override=True)

    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not args.skip_login and (not email or not password):
        print("[captcha-test] 缺少 GUANGDADA_EMAIL/GUANGDADA_USERNAME 或 GUANGDADA_PASSWORD", file=sys.stderr)
        return 2

    launch_kw: dict = {"headless": False}
    proxy = prepare_playwright_proxy_for_crawl()
    if proxy:
        launch_kw["proxy"] = proxy

    async with async_playwright() as p:
        browser = await p.chromium.launch(**launch_kw)
        context = await browser.new_context(viewport={"width": 1280, "height": 900})
        page = await context.new_page()
        try:
            if not args.skip_login:
                print("[captcha-test] 正在登录广大大...")
                if not await login(page, str(email), str(password)):
                    print("[captcha-test] 登录失败", file=sys.stderr)
                    return 3

            print(f"[captcha-test] 打开目标页: {args.url}")
            await page.goto(args.url, wait_until="domcontentloaded", timeout=90000)
            try:
                await page.wait_for_load_state("networkidle", timeout=6000)
            except Exception:
                pass
            await page.wait_for_timeout(1500)

            print(f"[captcha-test] 等待安全验证出现，timeout={args.detect_timeout_sec}s")
            detected = await wait_for_captcha(page, args.detect_timeout_sec)
            if not detected:
                print("[captcha-test] 未检测到安全验证弹窗")
                if args.keep_open:
                    await page.wait_for_timeout(30000)
                return 5 if args.require_captcha else 0

            print("[captcha-test] 已检测到安全验证，启动飞书确认回调")
            waiter = await start_feishu_confirmation_waiter(port=max(0, int(args.callback_port or 0)))
            try:
                try:
                    sent = _send_guangdada_security_verification_alert(
                        page.url,
                        DEFAULT_GUIDE_IMAGE_PATH,
                        waiter.confirm_url,
                        workflow="广大大安全验证测试",
                        step="测试脚本检测到弹窗",
                    )
                except Exception as exc:
                    strict = (os.getenv("GUANGDADA_INTERRUPT_FEISHU_STRICT", "0") or "0").strip().lower()
                    if strict in {"1", "true", "yes", "on"}:
                        raise
                    print(f"[captcha-test] 飞书告警发送失败: {exc}", file=sys.stderr)
                    sent = False
                if not sent:
                    print("[captcha-test] 飞书告警未发送，无法等待回调", file=sys.stderr)
                    return 6

                print(f"[captcha-test] 等待飞书「已完成」回调: {waiter.confirm_url}")
                result = await waiter.wait(timeout_sec=args.solve_timeout_sec)
                if result.confirmed:
                    print("[captcha-test] 已收到飞书确认回调")
                    if args.keep_open:
                        await page.wait_for_timeout(30000)
                    return 0
            finally:
                await waiter.close()

            print("[captcha-test] 等待飞书确认超时", file=sys.stderr)
            if args.keep_open:
                await page.wait_for_timeout(30000)
            return 4
        finally:
            await context.close()
            await browser.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
