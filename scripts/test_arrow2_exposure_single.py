"""
Arrow2 展示估值 单产品测试脚本 —— 复用 test_arrow2_first_card_fields.py 的 detail-v2 逐张点击逻辑。

与完整 exposure_top10 流程的区别：
  - 仅跑配置中第一个产品
  - 每词最多点击 60 张卡片（而非全量）
  - 默认有头浏览器 + 每产品暂停
  - 不写 raw JSON（仅打印结果摘要）

用法（项目根目录）：
  .venv/bin/python scripts/test_arrow2_exposure_single.py
  .venv/bin/python scripts/test_arrow2_exposure_single.py --date 2026-04-27
  .venv/bin/python scripts/test_arrow2_exposure_single.py --products "Arrows – Puzzle Escape"
  .venv/bin/python scripts/test_arrow2_exposure_single.py --max-cards 30
  .venv/bin/python scripts/test_arrow2_exposure_single.py --no-pause
  .venv/bin/python scripts/test_arrow2_exposure_single.py --output
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

load_dotenv()

from path_util import CONFIG_DIR, DATA_DIR, PROJECT_ROOT
from test_arrow2_first_card_fields import (
    PULL_SPEC_EXPOSURE_TOP10,
    _build_raw_payload,
    _load_entries,
    _run_one_product,
)
from run_search_workflow import (
    _await_post_login_shell,
    _do_setup,
    _try_click_search_tab,
)
from guangdada_login import login
from proxy_util import prepare_playwright_proxy_for_crawl
from playwright.async_api import async_playwright

CONFIG_FILE = CONFIG_DIR / "arrow2_competitor.json"
CST = timezone(timedelta(hours=8))


def _beijing_today_iso() -> str:
    return datetime.now(CST).date().isoformat()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Arrow2 展示估值 单产品测试（detail-v2 逐张点击，最多20张卡片）"
    )
    parser.add_argument("--date", default="", help="目标日期 YYYY-MM-DD（默认昨日）")
    parser.add_argument("--products", default="", help="指定产品名，逗号分隔（默认配置中第一个）")
    parser.add_argument("--max-cards", type=int, default=20, help="每词最多点击卡片数（默认20）")
    parser.add_argument("--no-pause", action="store_true", help="产品完成后不暂停")
    parser.add_argument("--output", action="store_true", help="同时输出 raw JSON 到 data/")
    parser.add_argument("--headless", action="store_true", help="无头浏览器（默认有头）")
    args = parser.parse_args()

    debug = not args.headless
    target_date = (args.date or "").strip()[:10] or (
        (datetime.now(CST).date() - timedelta(days=1)).isoformat()
    )
    if (os.getenv("TARGET_DATE") or "").strip() and not (args.date or "").strip():
        target_date = os.getenv("TARGET_DATE").strip()[:10]

    crawl_date = _beijing_today_iso()

    # ─── pull_spec：exposure_top10，覆盖 max_creatives ───
    pull_spec = {**PULL_SPEC_EXPOSURE_TOP10, "max_creatives_per_keyword": args.max_cards}
    pull_id = pull_spec["id"]
    order_by = pull_spec["order_by"]
    day_span = pull_spec["day_span"]
    popularity_option = pull_spec.get("popularity_option_text") or None

    # ─── 加载配置 ───
    cfg = json.load(CONFIG_FILE.open("r", encoding="utf-8"))
    names = [x.strip() for x in (args.products or "").split(",") if x.strip()]
    entries = _load_entries(cfg, names=names if names else None, all_products=False)

    if not entries:
        print("[错误] 没有匹配的产品配置", file=sys.stderr)
        sys.exit(1)

    search_tab = str(cfg.get("search_tab") or "game").strip().lower()
    is_tool = search_tab == "tool"

    filters = cfg.get("filters") or {}
    ch_normal = filters.get("ad_channels") or []
    ch_playable = filters.get("ad_channels_playable") or []
    if search_tab in ("playable", "playable_ads"):
        channels = ch_playable if ch_playable else ["Admob", "UnityAds", "AppLovin"]
    else:
        channels = ch_normal if ch_normal else ["Facebook系", "Google系", "UnityAds", "AppLovin"]
    countries = filters.get("countries") or []

    _email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    _password = os.getenv("GUANGDADA_PASSWORD")
    if not _email or not _password:
        print("[错误] 请在 .env 中设置 GUANGDADA_EMAIL 和 GUANGDADA_PASSWORD", file=sys.stderr)
        sys.exit(1)

    print("=" * 80)
    print(f"  Arrow2 展示估值 单产品测试")
    print("=" * 80)
    print(f"目标日期: {target_date}")
    print(f"pull_id: {pull_id}  排序: {order_by}  天数: {day_span}  Top创意: {popularity_option or '(无)'}")
    print(f"每词最多点击: {args.max_cards} 张卡片")
    print(f"测试产品: {', '.join(e['match'] for e in entries)}")
    print(f"有头浏览器: {'是' if debug else '否'}")
    print()

    playwright_proxy = prepare_playwright_proxy_for_crawl()

    async with async_playwright() as p:
        launch_kw: dict = {"headless": not debug}
        if playwright_proxy:
            launch_kw["proxy"] = playwright_proxy
        browser = await p.chromium.launch(**launch_kw)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = await context.new_page()

        try:
            # ─── 登录 ───
            print("[全局] 正在登录…")
            if not await login(page, _email, _password):
                print("[失败] 登录失败", file=sys.stderr)
                sys.exit(2)
            print("[全局] 登录成功")
            await _await_post_login_shell(page)
            await _try_click_search_tab(page, search_tab)

            # ─── 设置筛选 ───
            setup_desc = f"{day_span}天 / 素材 / {'最新创意' if order_by == 'latest' else '展示估值'}"
            if popularity_option:
                setup_desc += f" / {popularity_option}"
            print(f"[全局] 设置筛选（{setup_desc}）…")
            await _do_setup(
                page,
                is_tool=bool(is_tool),
                order_by=order_by,
                day_span=day_span,
                popularity_option_text=popularity_option,
                ad_channel_labels=channels if channels else None,
                country_codes=countries or None,
                log_quiet=False,
            )
            print("[全局] 筛选设置完成\n")

            # ─── 逐产品爬取 ───
            results = []
            for i, entry in enumerate(entries):
                result = await _run_one_product(
                    page, entry, target_date, pull_spec, channels, countries, is_tool
                )
                results.append(result)

                # 保存单产品结果
                DATA_DIR.mkdir(parents=True, exist_ok=True)
                safe_name = entry["match"].replace("/", "_").replace(" ", "_")[:30]
                out_path = DATA_DIR / f"test_arrow2_exposure_single_{safe_name}_{target_date}.json"
                out_path.write_text(
                    json.dumps(result, ensure_ascii=False, indent=2, default=str),
                    encoding="utf-8",
                )
                print(f"\n  [保存] {result['matched_count']} 条 → {out_path}")

                if not args.no_pause:
                    print(f"\n{'*' * 80}")
                    print(f"  [暂停] 产品 '{entry['match']}' 已完成，请检查上方输出。")
                    print(f"         按 Enter 继续，或 Ctrl+C 退出…")
                    print(f"{'*' * 80}")
                    _ = input()

            # ─── 汇总 ───
            print(f"\n{'=' * 80}")
            print(f"  汇总")
            print(f"{'=' * 80}")
            for r in results:
                print(f"  {r['product']}: detail={r['total_detail']}  "
                      f"匹配={r['matched_count']}  "
                      f"广告主不匹配={r['adv_filtered_out']}  "
                      f"早停={'是' if r['early_stop'] else '否'}")

            # ─── 可选：输出 raw JSON ───
            if args.output:
                prefix = f"test_arrow2_exposure_single_{target_date}"
                raw_payload = _build_raw_payload(
                    results, target_date, crawl_date, cfg, search_tab, channels, countries, entries, pull_spec
                )
                raw_path = DATA_DIR / f"{prefix}_raw.json"
                raw_path.write_text(
                    json.dumps(raw_payload, ensure_ascii=False, indent=2, default=str),
                    encoding="utf-8",
                )
                ds = raw_payload["dedupe_stats"]
                print(
                    f"\n[raw] {raw_path.name}（广告主匹配 {len(raw_payload['items'])} 条，"
                    f"全局 ad_key 去重后 {ds['rows_after_dedupe']} 条，"
                    f"合并重复 {ds['duplicate_rows_merged']} 条）"
                )

        finally:
            await browser.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("中断。", file=sys.stderr)
