"""
工作流：关键词搜索 → 素材 → 最新 / 展示量最高
每次重新登录（邮箱+密码），然后记录你的操作。

用法: python workflow_keyword_search.py
"""
import asyncio
import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

from guangdada_login import GUANGDADA_BASE_URL, login

from path_util import DATA_DIR

OUT_DIR = DATA_DIR


async def run():
    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not email or not password:
        print("错误: 请在 .env 中设置 GUANGDADA_EMAIL 和 GUANGDADA_PASSWORD", file=sys.stderr)
        sys.exit(1)

    creative_batches = []
    session_log = []

    async def on_response(response):
        url = response.url
        if "guangdada.net/napi" in url:
            entry = {"url": url, "status": response.status, "ts": round(time.time(), 2)}
            try:
                if response.status == 200 and "json" in response.headers.get("content-type", ""):
                    body = await response.json()
                    if "napi/v1/creative/list" in url:
                        lst = body.get("data", {}).get("creative_list", [])
                        if lst:
                            creative_batches.append({"lst": lst, "ts": time.time()})
                            if len(creative_batches) > 5:
                                creative_batches.pop(0)
                            entry["creative_count"] = len(lst)
                            print(f"  [监听] 创意列表 {len(lst)} 条")
                    else:
                        entry["data_preview"] = str(body)[:300]
            except Exception as e:
                entry["error"] = str(e)
            session_log.append(entry)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = await context.new_page()
        page.on("response", on_response)

        # 1. 每次重新登录
        if not await login(page, email, password):
            print("登录失败，请检查 .env 中的账号密码", file=sys.stderr)
            await browser.close()
            sys.exit(2)

        # 2. 进入创意页，等待你操作
        await page.goto(f"{GUANGDADA_BASE_URL}/creative", wait_until="domcontentloaded", timeout=30000)
        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
        await page.wait_for_timeout(2000)

        print("\n" + "=" * 60)
        print("  已登录，请手动执行你的操作：")
        print("=" * 60)
        print("  1. 在搜索框输入关键词并搜索")
        print("  2. 点击「素材」标签")
        print("  3. 先点击「最新」，等列表加载完")
        print("  4. 再点击「展示量最高」，等列表加载完")
        print("=" * 60)
        print("  完成后回到终端按 Enter\n")

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: input())

        await browser.close()

    # 解析
    latest_creatives = creative_batches[-2]["lst"] if len(creative_batches) >= 2 else []
    top_exposure_creatives = creative_batches[-1]["lst"] if creative_batches else []
    latest_top_by_exposure = None
    if latest_creatives:
        latest_top_by_exposure = max(
            latest_creatives,
            key=lambda x: x.get("all_exposure_value", 0) or x.get("impression", 0),
        )

    result = {
        "top_exposure_list": top_exposure_creatives,
        "latest_list": latest_creatives,
        "latest_top_by_exposure": latest_top_by_exposure,
    }
    with open(OUT_DIR / "workflow_result.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    with open(OUT_DIR / "session_log.json", "w", encoding="utf-8") as f:
        json.dump(session_log, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 60)
    print("已保存 workflow_result.json, session_log.json")
    print("=" * 60)
    print(f"展示量最高: {len(top_exposure_creatives)} 条, 最新: {len(latest_creatives)} 条")


if __name__ == "__main__":
    asyncio.run(run())
