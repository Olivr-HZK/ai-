"""
从 Arrow2 SQLite（arrow2_daily_insights.raw_json）按 ad_key 补全国家/地区。

说明（重要）：
- 「DOM 点卡」只对**当前搜索结果页**上出现的卡片有效；库里成千上万条历史行无法逐条点卡。
- 本脚本与主流程一致：在**已登录**浏览器里用页面 fetch 请求 `GET .../creative/detail-v2`
  （与 DOM 拦截的**同一接口**），按 ad_key + search_flag 拉取 data.countries 等，写回 raw_json。

依赖：.env 中 GUANGDADA_EMAIL / GUANGDADA_PASSWORD；库默认 data/arrow2_pipeline.db。

用法（项目根目录，建议虚拟环境）：
  .venv/bin/python scripts/backfill_arrow2_country_from_db.py --dry-run
  .venv/bin/python scripts/backfill_arrow2_country_from_db.py --limit 50
  .venv/bin/python scripts/backfill_arrow2_country_from_db.py --target-date 2026-04-14 --limit 20

  ARROW2_ENRICH_DETAIL_COUNTRY_VERBOSE=1 .venv/bin/python scripts/backfill_arrow2_country_from_db.py --limit 5
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

from dotenv import load_dotenv
from playwright.async_api import async_playwright

from arrow2_pipeline_db import (
    load_arrow2_daily_insights_for_country_backfill,
    update_arrow2_daily_insights_raw_json,
)
from guangdada_login import login
from proxy_util import prepare_playwright_proxy_for_crawl
from run_search_workflow import (
    _arrow2_geo_still_empty,
    _arrow2_enrich_detail_verbose,
    _fetch_detail_v2_best_attempt,
    _merge_detail_v2_geo_into_creative,
)
from guangdada_detail_url import resolve_ad_key_for_napi


async def _run() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Arrow2 DB：按 ad_key 用 detail-v2 补全国家并写回 raw_json",
    )
    parser.add_argument(
        "--target-date",
        default="",
        help="只处理该 target_date（YYYY-MM-DD）；默认扫全部（受 scan-limit 限制）",
    )
    parser.add_argument(
        "--scan-limit",
        type=int,
        default=3000,
        help="从 DB 最多读取多少行 raw_json（默认 3000）；0 表示不限制",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="本批最多处理多少条「仍缺地区」的素材（默认 100）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只统计与试请求，不写库",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="有头浏览器",
    )
    args = parser.parse_args()

    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not email or not password:
        print("[错误] 请在 .env 中设置 GUANGDADA_EMAIL 和 GUANGDADA_PASSWORD", file=sys.stderr)
        sys.exit(1)

    rows = load_arrow2_daily_insights_for_country_backfill(
        target_date=(args.target_date or "").strip() or None,
        scan_limit=max(0, int(args.scan_limit)),
    )
    need: list[dict] = []
    for r in rows:
        c = r.get("creative")
        if not isinstance(c, dict):
            continue
        if not _arrow2_geo_still_empty(c):
            continue
        need.append(r)

    missing_count = len(need)
    proc_limit = max(0, int(args.limit))
    if proc_limit > 0:
        need = need[:proc_limit]

    print(
        f"[1] DB 扫描载入 {len(rows)} 行，其中仍缺地区 {missing_count} 条；"
        f"本批将处理 {len(need)} 条"
    )
    if not need:
        print("-- 无需处理或 scan-limit 内无缺地区行。")
        return

    if args.dry_run:
        for i, r in enumerate(need[:10], 1):
            ak = resolve_ad_key_for_napi(r["creative"])
            sf = r["creative"].get("search_flag")
            print(f"  [dry-run] {i} target_date={r['target_date']} ad_key={ak[:16]}… search_flag={sf!r}")
        if len(need) > 10:
            print(f"  … 共 {len(need)} 条（仅展示前 10）")
        print("-- dry-run 结束，未登录、未写库。")
        return

    verbose = _arrow2_enrich_detail_verbose()
    try:
        delay_ms = max(0, int((os.environ.get("ARROW2_ENRICH_DETAIL_COUNTRY_MS") or "180").strip()))
    except Exception:
        delay_ms = 180

    playwright_proxy = prepare_playwright_proxy_for_crawl()
    ok_n = 0
    fail_n = 0
    skip_n = 0

    async with async_playwright() as p:
        launch_kw: dict = {"headless": not args.debug}
        if playwright_proxy:
            launch_kw["proxy"] = playwright_proxy
        browser = await p.chromium.launch(**launch_kw)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = await context.new_page()
        try:
            print("[2] 登录广大大…")
            if not await login(page, email, password):
                print("[失败] 登录失败", file=sys.stderr)
                sys.exit(2)
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            await page.wait_for_timeout(2000)

            for i, r in enumerate(need):
                td = r["target_date"]
                c = dict(r["creative"])
                ak = resolve_ad_key_for_napi(c)
                if not ak:
                    skip_n += 1
                    continue
                if c.get("search_flag") is None:
                    if verbose:
                        print(f"    [skip] 无 search_flag ad_key={ak[:16]}…")
                    skip_n += 1
                    continue

                body = await _fetch_detail_v2_best_attempt(page, c)
                if not body:
                    fail_n += 1
                    if verbose:
                        print(f"    [fail] detail-v2 无可用响应 ad_key={ak[:16]}…")
                    if delay_ms:
                        await page.wait_for_timeout(delay_ms)
                    continue

                if _merge_detail_v2_geo_into_creative(c, body):
                    c["_country_source"] = "backfill_detail_v2"
                    if update_arrow2_daily_insights_raw_json(td, ak, c):
                        ok_n += 1
                    else:
                        fail_n += 1
                else:
                    fail_n += 1
                    if verbose:
                        print(
                            f"    [fail] 合并无地区字段 ad_key={ak[:16]}… "
                            f"id={body.get('id')!r}"
                        )

                if delay_ms and i + 1 < len(need):
                    await page.wait_for_timeout(delay_ms)

            print(f"[3] 完成：写入成功 {ok_n}，失败 {fail_n}，跳过 {skip_n}")
        finally:
            await browser.close()


def main() -> None:
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        print("中断。", file=sys.stderr)


if __name__ == "__main__":
    main()
