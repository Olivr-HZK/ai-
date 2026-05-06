"""
广大大 - 试玩广告流程：登录 → 试玩广告 → 监听接口。

- DEBUG=1（记录模式）：用 Playwright 打开浏览器（可观测每一步操作），登录 → 点击「试玩广告」→ 记录你的操作。
  - 默认会加 slow_mo，方便在浏览器里看清脚本执行了啥；也可设 PWDEBUG=1 用 Playwright 官方调试器。
- 非 DEBUG：根据已保存的 session 直接解析接口数据，不打开浏览器。
"""

import asyncio
import json
import os
import sys
import time
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page

from guangdada_login import login
from path_util import DATA_DIR

load_dotenv()

GUANGDADA_BASE_URL = "https://www.guangdada.net"
PLAYABLE_ADS_URL = f"{GUANGDADA_BASE_URL}/modules/creative/playable-ads"


async def _safe_click(page: Page, selector: str, desc: str = "", timeout: int = 8000) -> bool:
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


async def run_debug_record():
    """Debug 模式：登录 → 点击试玩广告 → 监听你的操作，记录所有 napi 请求与响应。"""
    print("提示: 使用 PWDEBUG=1 可启用 Playwright 官方调试器（逐步执行、查看选择器等）。\n")
    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not email or not password:
        print("错误: 请在 .env 中设置 GUANGDADA_EMAIL 和 GUANGDADA_PASSWORD", file=sys.stderr)
        sys.exit(1)

    session = []  # [{ type, ts, url, method?, post_data?, status?, body? }]
    # 方便在浏览器里看清脚本每一步：慢速执行（毫秒），可通过环境变量 PLAYWRIGHT_SLOW_MO 覆盖
    slow_mo = int(os.getenv("PLAYWRIGHT_SLOW_MO", "600"))

    async with async_playwright() as p:
        # DEBUG 下不无头、加慢速，便于观察「脚本都执行了什么」；PWDEBUG=1 时用官方调试器
        browser = await p.chromium.launch(
            headless=False,
            slow_mo=slow_mo,
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = await context.new_page()

        async def on_request(request):
            url = request.url
            if "guangdada.net/napi" not in url:
                return
            post_data = None
            try:
                post_data = request.post_data
            except Exception:
                pass
            session.append({
                "type": "request",
                "ts": round(time.time(), 2),
                "method": request.method,
                "url": url,
                "post_data": post_data,
            })
            print(f"  [请求] {request.method} {url[:80]}{'...' if len(url) > 80 else ''}")

        async def on_response(response):
            url = response.url
            if "guangdada.net/napi" not in url or response.status != 200:
                return
            try:
                body = await response.json()
            except Exception:
                body = None
            session.append({
                "type": "response",
                "ts": round(time.time(), 2),
                "url": url,
                "status": response.status,
                "body": body,
            })
            # 简短打印
            if "creative/list" in url and isinstance(body, dict):
                data = body.get("data") or {}
                lst = data.get("creative_list") if isinstance(data, dict) else []
                n = len(lst) if isinstance(lst, list) else 0
                print(f"  [响应] list -> {n} 条")
            elif "creative/detail-v2" in url:
                print(f"  [响应] detail-v2")
            else:
                print(f"  [响应] {url.split('?')[0].split('/')[-1]}")

        page.on("request", on_request)
        page.on("response", on_response)

        try:
            print("\n[Playwright Debug] 以下操作会在浏览器中慢速执行，便于观察。\n")
            print("[步骤 1/3] 打开登录页并执行登录...")
            if not await login(page, email, password):
                print("登录失败", file=sys.stderr)
                sys.exit(2)
            print("  → 登录完成\n")

            print("[步骤 2/3] 打开试玩广告页...")
            await page.goto(PLAYABLE_ADS_URL, wait_until="domcontentloaded", timeout=30000)
            try:
                await page.wait_for_load_state("networkidle", timeout=12000)
            except Exception:
                pass
            print("  → 页面加载完成\n")

            print("[步骤 3/3] 点击侧边栏「试玩广告」...")
            await _safe_click(
                page,
                'li:has(span:has-text("试玩广告")), a[href="/modules/creative/playable-ads"]',
                "试玩广告",
            )
            await page.wait_for_timeout(2000)
            print("  → 已进入试玩广告，接下来由你在页面操作。\n")

            print("=" * 60)
            print("  Debug 记录中：请你在页面里按你的习惯操作（筛选、点广告等）。")
            print("  脚本会监听所有 napi 请求与响应，完成后按 Enter 保存并退出。")
            print("=" * 60 + "\n")

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: input())
        finally:
            out = DATA_DIR / "playable_session_debug.json"
            with open(out, "w", encoding="utf-8") as f:
                json.dump(session, f, ensure_ascii=False, indent=2)
            print(f"\n已保存 {len(session)} 条请求/响应 → {out.name}")
            print("后续可直接用该文件复现接口调用。")
            await browser.close()


async def main():
    debug = os.getenv("DEBUG", "").strip().lower() in ("1", "true", "yes")
    if debug:
        await run_debug_record()
    else:
        # 非 Debug：可在此根据 data/playable_session_debug.json 直接请求接口
        session_path = DATA_DIR / "playable_session_debug.json"
        if not session_path.exists():
            print("未找到记录文件。请先设置 DEBUG=1 运行一次，在页面操作后保存。", file=sys.stderr)
            print("  DEBUG=1 python scripts/capture_playable_click.py", file=sys.stderr)
            sys.exit(1)
        print("读取已保存的 session，直接复现接口操作...")
        with open(session_path, "r", encoding="utf-8") as f:
            session = json.load(f)
        requests = [e for e in session if e.get("type") == "request"]
        responses = [e for e in session if e.get("type") == "response"]
        print(f"  请求 {len(requests)} 条，响应 {len(responses)} 条")
        # 这里可解析 response 中的 creative/list、detail-v2 等，直接产出试玩广告列表与详情
        # 保存为 playable_click_ads.json 等（与之前格式兼容）
        list_bodies = [e["body"] for e in responses if e.get("url") and "creative/list" in e["url"] and e.get("body")]
        detail_bodies = [e["body"] for e in responses if e.get("url") and "creative/detail-v2" in e["url"] and e.get("body")]
        print(f"  其中 creative/list 响应 {len(list_bodies)} 次，detail-v2 响应 {len(detail_bodies)} 次")
        # 简单产出：从 list 取 creative_list，从 detail 取详情，合并写 playable_click_ads.json
        ads = []
        seen_keys = set()
        def pick(d, keys):
            if not isinstance(d, dict):
                return None
            for k in keys:
                v = d.get(k)
                if v not in (None, "", []):
                    return v
            return None
        for body in list_bodies:
            data = body.get("data") if isinstance(body, dict) else {} or {}
            lst = data.get("creative_list") if isinstance(data, dict) else []
            if not isinstance(lst, list):
                continue
            for item in lst:
                if not isinstance(item, dict):
                    continue
                ad_key = item.get("ad_key")
                if not ad_key or ad_key in seen_keys:
                    continue
                seen_keys.add(ad_key)
                playable_url = item.get("playable_url") or item.get("playable_link")
                if not playable_url and isinstance(item.get("resource_urls"), list):
                    for r in item["resource_urls"]:
                        if isinstance(r, dict) and ".html" in str(r.get("url", "")):
                            playable_url = r.get("url")
                            break
                ads.append({
                    "ad_key": ad_key,
                    "game_name": pick(item, ["game_name", "app_name", "title", "name"]),
                    "developer": pick(item, ["developer", "advertiser_name", "publisher_name", "company_name"]),
                    "display_value": pick(item, ["display_value", "show_value", "estimate_value", "estimate", "all_exposure_value", "new_week_exposure_value"]),
                    "playable_url": playable_url,
                })
        for body in detail_bodies:
            data = body.get("data") if isinstance(body, dict) else {}
            if not isinstance(data, dict):
                continue
            ad_key = data.get("ad_key")
            if not ad_key:
                continue
            app = data.get("app") or {}
            adv = data.get("advertiser") or {}
            if ad_key in seen_keys:
                for a in ads:
                    if a.get("ad_key") == ad_key:
                        a["game_name"] = a.get("game_name") or pick(data, ["game_name", "app_name", "name"]) or pick(app, ["game_name", "app_name", "name"])
                        a["developer"] = a.get("developer") or pick(data, ["developer", "advertiser_name"]) or pick(adv, ["name", "developer", "company_name"])
                        a["display_value"] = a.get("display_value") or pick(data, ["display_value", "show_value", "estimate_value", "estimate"])
                        break
            else:
                seen_keys.add(ad_key)
                ads.append({
                    "ad_key": ad_key,
                    "game_name": pick(data, ["game_name", "app_name", "name"]) or pick(app, ["game_name", "app_name", "name"]),
                    "developer": pick(data, ["developer", "advertiser_name"]) or pick(adv, ["name", "developer", "company_name"]),
                    "display_value": pick(data, ["display_value", "show_value", "estimate_value", "estimate"]),
                    "playable_url": None,
                })
        out_ads = DATA_DIR / "playable_click_ads.json"
        with open(out_ads, "w", encoding="utf-8") as f:
            json.dump(ads, f, ensure_ascii=False, indent=2)
        print(f"已根据 session 产出 {len(ads)} 条广告 → {out_ads.name}")


if __name__ == "__main__":
    asyncio.run(main())
