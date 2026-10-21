"""Authentication state helpers for the Guangdada crawl CLI."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def default_home_dir() -> Path:
    root = os.getenv("GUANGDADA_CRAWL_HOME")
    if root:
        return Path(root).expanduser()
    return Path.home() / ".guangdada-crawl"


def has_env_credentials() -> bool:
    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    return bool(str(email or "").strip() and str(password or "").strip())


@dataclass(frozen=True)
class AuthStore:
    home: Path

    @classmethod
    def from_value(cls, value: str | Path | None = None) -> "AuthStore":
        return cls(Path(value).expanduser() if value is not None else default_home_dir())

    @property
    def auth_dir(self) -> Path:
        return self.home / "auth"

    @property
    def auth_state_path(self) -> Path:
        return self.auth_dir / "state.json"

    def status(self) -> dict[str, object]:
        return {
            "home": str(self.home),
            "auth_state_path": str(self.auth_state_path),
            "has_auth_state": self.auth_state_path.exists(),
            "has_credentials": has_env_credentials(),
        }

    async def login(self, *, headed: bool = False, auth_mode: str = "env-login") -> dict[str, object]:
        from playwright.async_api import async_playwright

        from ua_workflows.shared.guangdada.login import LOGIN_URL, login
        from ua_workflows.shared.guangdada.proxy import prepare_playwright_proxy_for_crawl

        self.auth_dir.mkdir(parents=True, exist_ok=True)
        email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
        password = os.getenv("GUANGDADA_PASSWORD")
        if auth_mode == "env-login" and not has_env_credentials():
            raise RuntimeError("Set GUANGDADA_EMAIL/GUANGDADA_USERNAME and GUANGDADA_PASSWORD before env-login")

        proxy = prepare_playwright_proxy_for_crawl()
        async with async_playwright() as playwright:
            launch_kw: dict[str, object] = {"headless": not headed}
            if proxy:
                launch_kw["proxy"] = proxy
            browser = await playwright.chromium.launch(**launch_kw)
            context = await browser.new_context(
                viewport={"width": 1440, "height": 960},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            )
            page = await context.new_page()
            try:
                if auth_mode == "manual":
                    await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=90000)
                    print("Complete Guangdada login in the browser, then press Enter here.", flush=True)
                    input()
                else:
                    ok = await login(page, str(email), str(password))
                    if not ok:
                        raise RuntimeError("Guangdada env-login failed")
                await context.storage_state(path=str(self.auth_state_path))
            finally:
                await context.close()
                await browser.close()
        payload = self.status()
        payload["status"] = "completed"
        return payload
