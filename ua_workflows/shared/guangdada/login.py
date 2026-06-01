"""
广大大登录（邮箱+密码）
每次运行都重新登录，不保存登录态。
"""
import os
from playwright.async_api import Page

GUANGDADA_BASE_URL = "https://www.guangdada.net"
# 2026-02 实际站点登录入口已改为 /user/login，旧的 /modules/auth/login 会导致 about:blank 或无法正常渲染
LOGIN_URL = f"{GUANGDADA_BASE_URL}/user/login"


def _login_goto_timeout_ms() -> int:
    """默认 90s；30s 在弱网/未走代理时易触发 Page.goto Timeout。"""
    raw = (os.getenv("GUANGDADA_LOGIN_GOTO_TIMEOUT_MS") or "").strip()
    if raw.isdigit():
        return max(5000, int(raw))
    return 90000


def _login_goto_wait_until() -> str:
    v = (os.getenv("GUANGDADA_LOGIN_GOTO_WAIT_UNTIL") or "domcontentloaded").strip().lower()
    if v in ("commit", "domcontentloaded", "load", "networkidle"):
        return v
    return "domcontentloaded"


async def _goto_login_url(page: Page) -> None:
    """打开登录页；domcontentloaded 超时时可自动改用 load 再试一次。"""
    timeout = _login_goto_timeout_ms()
    wait_until = _login_goto_wait_until()
    try:
        await page.goto(LOGIN_URL, wait_until=wait_until, timeout=timeout)
    except Exception as e:
        err = str(e).lower()
        is_timeout = "timeout" in err or type(e).__name__ == "TimeoutError"
        if is_timeout and wait_until == "domcontentloaded":
            print(
                "  [登录] 首次打开登录页超时，改用 wait_until=load 重试一次…",
                flush=True,
            )
            await page.goto(LOGIN_URL, wait_until="load", timeout=timeout)
        else:
            raise


async def login(page: Page, email: str, password: str) -> bool:
    """使用邮箱密码登录，返回是否成功。每次从 LOGIN_URL 开始"""
    try:
        await _goto_login_url(page)
        try:
            await page.wait_for_load_state("networkidle", timeout=20000)
        except Exception:
            pass
        # 等待登录表单出现（SPA 可能稍晚渲染）
        email_selectors = [
            'input[type="email"]',
            'input[name="email"]',
            'input[placeholder*="邮箱"]',
            'input[placeholder*="邮件"]',
            'input[placeholder*="email"]',
            'input[autocomplete="email"]',
            'input[id*="email"]',
        ]
        email_loc = None
        for sel in email_selectors:
            loc = page.locator(sel)
            try:
                await loc.first.wait_for(state="visible", timeout=5000)
                if await loc.count() > 0:
                    email_loc = loc.first
                    break
            except Exception:
                continue
        if not email_loc:
            await page.wait_for_timeout(3000)
            for sel in email_selectors:
                loc = page.locator(sel)
                if await loc.count() > 0:
                    email_loc = loc.first
                    break
        if not email_loc:
            print("  登录失败: 未找到邮箱输入框，请检查登录页是否正常打开")
            return False

        await email_loc.fill("")
        await email_loc.fill(email)
        await page.wait_for_timeout(300)

        pwd_selectors = ['input[type="password"]', 'input[name="password"]', 'input[placeholder*="密码"]', 'input[autocomplete="current-password"]']
        pwd_loc = None
        for sel in pwd_selectors:
            loc = page.locator(sel)
            if await loc.count() > 0:
                pwd_loc = loc.first
                break
        if not pwd_loc:
            print("  登录失败: 未找到密码输入框")
            return False
        await pwd_loc.fill("")
        await pwd_loc.fill(password)
        await page.wait_for_timeout(300)

        btn_selectors = [
            'button:has-text("登录")',
            'button:has-text("Login")',
            'button[type="submit"]',
            'input[type="submit"]',
            '[type="submit"]',
            'a:has-text("登录")',
        ]
        for sel in btn_selectors:
            loc = page.locator(sel)
            if await loc.count() > 0:
                try:
                    await loc.first.click(timeout=5000)
                    break
                except Exception:
                    continue

        await page.wait_for_timeout(5000)
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        # 若密码框仍可见，多半还在登录页
        if await page.locator("input[type='password']").is_visible():
            print("  登录失败: 仍在登录页（可能账号/密码错误或需验证）")
            return False
        print("登录成功")
        return True
    except Exception as e:
        print(f"登录失败: {e}")
        return False
