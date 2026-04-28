"""
Arrow2 detail-v2 全量采集 —— 多产品顺序爬取 + 广告主筛选 + 输出 raw JSON

流程（每个产品）：
  登录（仅首次）→ 设置筛选（时间/渠道/国家/Top创意）→ 搜索 → 滚动加载
  → 逐张点击卡片拦截 detail-v2 → 广告主匹配过滤 → 暂停等 check

只使用 detail-v2 一种数据源，不使用 napi 列表接口。

支持两种 pull_spec 模式：
  - latest_yesterday：7天+最新创意+滚动到 first_seen<target_date 停止+仅保留 first_seen=昨日
  - exposure_top10：30天+展示估值排序+Top10%+不限 first_seen+每词最多 N 条

输出格式与 test_arrow2_competitors.py 一致（items + items_deduped_by_ad_key），
可供 workflow_arrow2_full_pipeline.py 直接消费。入库时按 ad_key 去重。

用法（项目根目录）：
  .venv/bin/python scripts/test_arrow2_first_card_fields.py --date 2026-04-26
  .venv/bin/python scripts/test_arrow2_first_card_fields.py --date 2026-04-26 --all-products
  .venv/bin/python scripts/test_arrow2_first_card_fields.py --date 2026-04-26 --products "Arrows – Puzzle Escape"
  .venv/bin/python scripts/test_arrow2_first_card_fields.py --date 2026-04-26 --pull-only exposure_top10
  .venv/bin/python scripts/test_arrow2_first_card_fields.py --date 2026-04-26 --all-products --no-pause
  DEBUG=1 .venv/bin/python scripts/test_arrow2_first_card_fields.py --date 2026-04-26 --all-products
  .venv/bin/python scripts/test_arrow2_first_card_fields.py --date 2026-04-26 --all-products --no-pause --output-prefix workflow_arrow2_2026-04-26
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
from run_search_workflow import (
    _await_post_login_shell,
    _beijing_dt_from_unix_sec,
    _beijing_ymd_from_first_seen,
    _do_setup,
    _extract_creative_lists,
    _search_one_keyword,
    _select_top_popularity_option,
    _try_click_search_tab,
)
from workflow_guangdada_competitor_yesterday_creatives import advertiser_matches_product
from arrow2_pipeline_db import dedupe_arrow2_raw_items_by_ad_key
from guangdada_login import login
from proxy_util import prepare_playwright_proxy_for_crawl
from playwright.async_api import async_playwright

CONFIG_FILE = CONFIG_DIR / "arrow2_competitor.json"
CST = timezone(timedelta(hours=8))

PULL_SPEC_LATEST_YESTERDAY = {
    "id": "latest_yesterday",
    "day_span": "7",
    "order_by": "latest",
    "filter_yesterday_only": True,
    "scroll_until_past_target_date": True,
}

PULL_SPEC_EXPOSURE_TOP10 = {
    "id": "exposure_top10",
    "day_span": "30",
    "order_by": "exposure",
    "popularity_option_text": "Top10%",
    "max_creatives_per_keyword": 30,
}

PULL_SPECS = {
    "latest_yesterday": PULL_SPEC_LATEST_YESTERDAY,
    "exposure_top10": PULL_SPEC_EXPOSURE_TOP10,
}


def _beijing_today_iso() -> str:
    return datetime.now(CST).date().isoformat()


def _beijing_ymd_from_unix(ts: object) -> str | None:
    try:
        v = int(ts)
    except Exception:
        return None
    try:
        return datetime.fromtimestamp(v, tz=CST).date().isoformat()
    except Exception:
        return None


def _load_entries(cfg: dict, names: list[str] | None = None, all_products: bool = False):
    """从配置加载产品条目，支持按名筛选。"""
    raw = cfg.get("products") or []
    entries = []
    for x in raw:
        if not isinstance(x, dict):
            continue
        keyword = str(x.get("keyword") or "").strip()
        match = str(x.get("match") or x.get("product") or keyword).strip()
        appid = str(x.get("appid") or "").strip()
        if not keyword:
            continue
        entries.append({"keyword": keyword, "match": match, "appid": appid})

    if all_products:
        return entries

    if names:
        wanted = {n.strip().lower() for n in names if n.strip()}
        filtered = []
        for e in entries:
            hit = (
                e["keyword"].lower() in wanted
                or e["match"].lower() in wanted
                or (e["appid"] and e["appid"].lower() in wanted)
            )
            if hit:
                filtered.append(e)
        return filtered

    # 默认只跑第一个
    return entries[:1] if entries else []


async def _close_modal(page) -> None:
    """关闭弹窗：按 Escape 或点击关闭按钮"""
    try:
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(500)
        has_modal = await page.evaluate(
            "() => !!document.querySelector('.ant-modal-body')"
        )
        if has_modal:
            await page.evaluate("""
            () => {
              const btn = document.querySelector('.ant-modal-close');
              if (btn) btn.click();
            }
            """)
            await page.wait_for_timeout(500)
    except Exception:
        pass


async def _run_one_product(
    page,
    entry: dict,
    target_date: str,
    pull_spec: dict,
    channels: list[str],
    countries: list[str],
    is_tool: bool,
) -> dict:
    """爬取单个产品：搜索 → 滚动 → 逐张点击 → detail-v2 拦截 → 广告主筛选。"""
    keyword = entry["keyword"]
    match_name = entry["match"]
    appid = entry["appid"]
    search_query = appid if appid else keyword

    pull_id = str(pull_spec.get("id") or "")
    order_by = str(pull_spec.get("order_by") or "latest")
    day_span = str(pull_spec.get("day_span") or "7")
    filter_yesterday = bool(pull_spec.get("filter_yesterday_only"))
    max_creatives = int(pull_spec.get("max_creatives_per_keyword") or 0)

    print(f"\n{'#' * 80}")
    print(f"  产品: {match_name!r}  搜索词: {search_query!r}  appid: {appid!r}")
    print(f"  目标日期: {target_date}  pull_id: {pull_id}  排序: {order_by}")
    print(f"{'#' * 80}")

    batches_ref: list = []
    capture_state: dict = {"enabled": False}

    # ─── detail-v2 拦截器 ───
    detail_holder: list[dict] = []

    async def _on_detail_response(response):
        url = response.url or ""
        if "detail-v2" not in url:
            return
        if response.status != 200:
            return
        try:
            body = await response.json()
        except Exception:
            return
        data = body.get("data") if isinstance(body, dict) else body
        if isinstance(data, dict) and data.get("ad_key"):
            detail_holder.append(data)

    page.on("response", _on_detail_response)

    # ─── napi 列表拦截器（供 _search_one_keyword 的 batches_ref 填充） ───
    async def _on_napi_response(response):
        if not capture_state.get("enabled"):
            return
        url = response.url or ""
        if "guangdada.net/napi" not in url or response.status != 200:
            return
        try:
            body = await response.json()
        except Exception:
            return
        lists = _extract_creative_lists(body)
        for lst in lists:
            if isinstance(lst, list) and lst:
                batches_ref.append(lst)
        if len(batches_ref) > 80:
            batches_ref.pop(0)

    page.on("response", _on_napi_response)

    try:
        # ─── 确保页面滚回顶部 ───
        try:
            await page.evaluate("window.scrollTo(0, 0)")
            await page.wait_for_timeout(500)
        except Exception:
            pass

        # ─── 搜索 + 滚动 ───
        stop_ymd = target_date if filter_yesterday else None
        if stop_ymd:
            print(f"  [1/3] 搜索关键词: {search_query!r}，滚动到出现 first_seen < {target_date}…")
        else:
            print(f"  [1/3] 搜索关键词: {search_query!r}，滚动加载中…")

        await _search_one_keyword(
            page,
            search_query,
            batches_ref,
            capture_state,
            order_by=order_by,
            log_prefix="    ",
            max_scroll_rounds=48,
            log_quiet=False,
            stop_scroll_if_oldest_first_seen_before_ymd=stop_ymd,
        )
        await page.wait_for_timeout(2000)

        card_count = await page.evaluate(
            "() => document.querySelectorAll('.shadow-common-light.bg-white').length"
        )
        print(f"  [1/3] 搜索完成，页面卡片数: {card_count}")

        if card_count == 0:
            print("  [终止] 页面上无卡片")
            return {
                "product": match_name,
                "keyword": keyword,
                "target_date": target_date,
                "search_query": search_query,
                "appid": appid,
                "pull_id": pull_id,
                "total_detail": 0,
                "fs_filtered_out": 0,
                "adv_filtered_out": 0,
                "matched_count": 0,
                "early_stop": False,
                "matched": [],
            }

        # ─── 逐张点击卡片，拦截 detail-v2 ───
        if filter_yesterday:
            print(f"  [2/3] 逐张点击卡片，拦截 detail-v2（目标日={target_date}）…")
        else:
            print(f"  [2/3] 逐张点击卡片，拦截 detail-v2…")

        all_details: list[dict] = []
        early_stop = False
        seen_ad_keys: set[str] = set()

        for idx in range(card_count):
            if early_stop:
                break

            # exposure_top10：达到 max_creatives 就停
            if max_creatives > 0 and len(all_details) >= max_creatives:
                print(f"    [{idx + 1}] 已达每词上限 {max_creatives} 条，停止点击")
                break

            detail_holder.clear()

            # 点击第 idx 张卡片
            clicked = await page.evaluate(f"""
            () => {{
              const cards = document.querySelectorAll('.shadow-common-light.bg-white');
              const card = cards[{idx}];
              if (!card) return false;
              card.click();
              return true;
            }}
            """)
            if not clicked:
                continue

            # 等待 detail-v2 响应（最多 6s）
            for _ in range(24):
                if detail_holder:
                    break
                await page.wait_for_timeout(250)

            if not detail_holder:
                # 尝试重试一次
                detail_holder.clear()
                await page.evaluate(f"""
                () => {{
                  const cards = document.querySelectorAll('.shadow-common-light.bg-white');
                  const card = cards[{idx}];
                  if (!card) return;
                  card.click();
                }}
                """)
                for _ in range(16):
                    if detail_holder:
                        break
                    await page.wait_for_timeout(250)

            if not detail_holder:
                await _close_modal(page)
                continue

            detail = detail_holder[0]
            ad_key = str(detail.get("ad_key") or "")
            first_seen = detail.get("first_seen")
            fs_ymd = _beijing_ymd_from_unix(first_seen) or "?"

            # 去重
            if ad_key and ad_key in seen_ad_keys:
                await _close_modal(page)
                await page.wait_for_timeout(300)
                continue
            if ad_key:
                seen_ad_keys.add(ad_key)

            # latest_yesterday 早停：first_seen 早于 target_date
            if filter_yesterday and first_seen is not None:
                fs_ymd_str = _beijing_ymd_from_unix(first_seen)
                if fs_ymd_str and fs_ymd_str < target_date:
                    adv = str(detail.get("advertiser_name") or detail.get("page_name") or "")[:30]
                    print(f"    [{idx + 1}] first_seen={fs_ymd} < {target_date}（广告主={adv!r}），停止点击")
                    early_stop = True
                    await _close_modal(page)
                    break

            # 收集
            all_details.append(detail)
            adv = str(detail.get("advertiser_name") or detail.get("page_name") or "")[:30]
            print(f"    [{idx + 1}] ad_key={ad_key[:20]}… first_seen={fs_ymd} 广告主={adv!r}")

            # 关闭弹窗
            await _close_modal(page)
            await page.wait_for_timeout(300)

        print(f"  [2/3] 点击完成，共获取 {len(all_details)} 条 detail-v2 素材")

        # ─── 筛选 ───
        matched: list[dict] = []
        fs_filtered_out = 0
        adv_filtered_out = 0

        for c in all_details:
            # latest_yesterday：first_seen 筛选
            if filter_yesterday:
                fs_ymd = _beijing_ymd_from_unix(c.get("first_seen"))
                if fs_ymd != target_date:
                    fs_filtered_out += 1
                    continue

            # 广告主筛选
            adv_name = str(c.get("advertiser_name") or c.get("page_name") or "")
            if not advertiser_matches_product(adv_name, match_name):
                adv_filtered_out += 1
                continue

            if filter_yesterday:
                c["_first_seen_ymd"] = _beijing_ymd_from_unix(c.get("first_seen")) or "(无)"
                c["_first_seen_utc8"] = _beijing_dt_from_unix_sec(c.get("first_seen")) or "(无)"
            matched.append(c)

        if filter_yesterday:
            print(f"  [3/3] first_seen 不匹配: {fs_filtered_out} 条，广告主不匹配: {adv_filtered_out} 条")
        else:
            print(f"  [3/3] 广告主不匹配: {adv_filtered_out} 条")
        print(f"  [3/3] 最终匹配: {len(matched)} 条")

        # 打印匹配素材摘要
        if matched:
            print(f"\n  {'#':>3}  {'ad_key':22s}  {'first_seen':20s}  {'人气':>8}  {'估值':>8}  {'热度':>5}  {'天数':>4}  {'平台':8s}  {'广告主'}")
            print(f"  {'─'*3}  {'─'*22}  {'─'*20}  {'─'*8}  {'─'*8}  {'─'*5}  {'─'*4}  {'─'*8}  {'─'*20}")
            for i, d in enumerate(matched, 1):
                ak = str(d.get("ad_key") or "")[:22]
                fs = d.get("_first_seen_utc8", _beijing_ymd_from_unix(d.get("first_seen")) or "?")
                exp = d.get("all_exposure_value", 0)
                imp = d.get("impression", 0)
                ht = d.get("heat", 0)
                dc = d.get("days_count", 0)
                pf = str(d.get("platform") or "")[:8]
                adv = str(d.get("advertiser_name") or d.get("page_name") or "")[:20]
                print(f"  {i:>3}  {ak:22s}  {str(fs):20s}  {exp:>8}  {imp:>8}  {ht:>5}  {dc:>4}  {pf:8s}  {adv}")

            # 字段完整性检查（取第 1 条做样本）
            print(f"\n  字段完整性检查（第 1 条样本）:")
            sample = matched[0]
            important_fields = [
                "ad_key", "advertiser_id", "advertiser_name", "app_developer",
                "platform", "video_duration", "first_seen", "created_at", "last_seen",
                "days_count", "heat", "all_exposure_value", "impression",
                "image_ahash_md5", "preview_img_url", "cdn_url", "resource_urls",
                "title", "body", "countries", "store_url", "search_flag",
                "material_ai_tag", "material_ai_search_word_new",
                "resume_advertising_flag", "video2pic", "ads_format",
                "new_week_exposure_value", "material_id",
            ]
            for f in important_fields:
                v = sample.get(f)
                tag = "✓" if v is not None and v != "" and v != [] else "❌"
                v_str = str(v)
                if len(v_str) > 80:
                    v_str = v_str[:80] + "..."
                print(f"    {tag}  {f:35s} = {v_str!r}")

        return {
            "product": match_name,
            "keyword": keyword,
            "target_date": target_date,
            "search_query": search_query,
            "appid": appid,
            "pull_id": pull_id,
            "total_detail": len(all_details),
            "fs_filtered_out": fs_filtered_out,
            "adv_filtered_out": adv_filtered_out,
            "matched_count": len(matched),
            "early_stop": early_stop,
            "matched": matched,
        }

    finally:
        page.remove_listener("response", _on_detail_response)
        page.remove_listener("response", _on_napi_response)


def _build_raw_payload(
    results: list[dict],
    target_date: str,
    crawl_date: str,
    cfg: dict,
    search_tab: str,
    channels: list[str],
    countries: list[str],
    entries: list[dict],
    pull_spec: dict,
) -> dict:
    """将爬取结果展平为与 test_arrow2_competitors.py 一致的 raw_payload 格式。"""
    pull_id = str(pull_spec.get("id") or "")
    day_span = str(pull_spec.get("day_span") or "7")
    order_by = str(pull_spec.get("order_by") or "latest")

    raw_items: list[dict] = []
    for result in results:
        product = result.get("product", "")
        kw = result.get("keyword", product)
        search_query = result.get("search_query", "")
        appid = result.get("appid", "")
        for detail in result.get("matched", []):
            raw_items.append({
                "product": product,
                "keyword": kw,
                "search_query": search_query,
                "appid": appid,
                "day_span": day_span,
                "order_by": order_by,
                "pull_id": pull_id,
                "pull_spec": pull_spec,
                "creative": detail,
            })

    items_deduped, dedupe_stats = dedupe_arrow2_raw_items_by_ad_key(raw_items)

    filters = cfg.get("filters") or {}
    ch_normal = filters.get("ad_channels") or []
    ch_playable = filters.get("ad_channels_playable") or []

    return {
        "workflow": "arrow2_competitor",
        "target_date": target_date,
        "crawl_date": crawl_date,
        "config_file": str(CONFIG_FILE),
        "search_tab": search_tab,
        "source_script": "test_arrow2_first_card_fields",
        "pull_specs": [pull_spec],
        "filters": {
            "ad_channels": ch_normal,
            "ad_channels_playable": ch_playable,
            "ad_channels_used": channels,
            "countries": countries,
        },
        "competitors": [
            {"keyword": e["keyword"], "product": e["match"], "appid": e["appid"]}
            for e in entries
        ],
        "total_items_matched_before_dedupe": len(raw_items),
        "total_unique_by_ad_key": dedupe_stats["rows_after_dedupe"],
        "dedupe_stats": dedupe_stats,
        "items": raw_items,
        "items_deduped_by_ad_key": items_deduped,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Arrow2 detail-v2 全量采集 —— 多产品顺序爬取 + 广告主筛选 + 输出 raw JSON"
    )
    parser.add_argument("--date", default="", help="目标日期 YYYY-MM-DD（默认昨日）")
    parser.add_argument("--products", default="", help="指定产品，逗号分隔")
    parser.add_argument("--all-products", action="store_true", help="跑配置中所有产品")
    parser.add_argument("--no-pause", action="store_true", help="每产品结束后不暂停")
    parser.add_argument(
        "--output-prefix",
        default="",
        help="输出文件前缀（默认 workflow_arrow2_<date>）；与 test_arrow2_competitors --output-prefix 一致",
    )
    parser.add_argument(
        "--pull-only",
        default="latest_yesterday",
        help="pull_spec id：latest_yesterday（7天+最新+仅昨日）或 exposure_top10（30天+展示估值+Top10%%）；默认 latest_yesterday",
    )
    args = parser.parse_args()

    debug = bool(os.environ.get("DEBUG"))
    target_date = (args.date or "").strip()[:10] or (
        (datetime.now(CST).date() - timedelta(days=1)).isoformat()
    )
    if (os.getenv("TARGET_DATE") or "").strip() and not (args.date or "").strip():
        target_date = os.getenv("TARGET_DATE").strip()[:10]

    crawl_date = _beijing_today_iso()
    prefix = (args.output_prefix or "").strip() or f"workflow_arrow2_{target_date}"

    # ─── 解析 pull_spec ───
    pull_id = (args.pull_only or "").strip() or "latest_yesterday"
    pull_spec = PULL_SPECS.get(pull_id)
    if not pull_spec:
        print(f"[错误] 不支持的 pull_id: {pull_id!r}，可选: {', '.join(PULL_SPECS.keys())}", file=sys.stderr)
        sys.exit(1)

    # 读取配置中的 max_creatives_per_keyword 覆盖默认值
    cfg = json.load(CONFIG_FILE.open("r", encoding="utf-8"))
    for ps in cfg.get("pull_specs") or []:
        if str(ps.get("id") or "") == pull_id and ps.get("max_creatives_per_keyword"):
            pull_spec = {**pull_spec, "max_creatives_per_keyword": int(ps["max_creatives_per_keyword"])}

    order_by = str(pull_spec.get("order_by") or "latest")
    day_span = str(pull_spec.get("day_span") or "7")
    popularity_option = str(pull_spec.get("popularity_option_text") or "").strip() or None
    filter_yesterday = bool(pull_spec.get("filter_yesterday_only"))

    # ─── 加载配置 ───
    names = [x.strip() for x in (args.products or "").split(",") if x.strip()]
    entries = _load_entries(cfg, names=names if names else None, all_products=args.all_products)

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
    print(f"  Arrow2 detail-v2 全量采集 —— 多产品顺序爬取")
    print("=" * 80)
    print(f"目标日期: {target_date}")
    print(f"pull_id: {pull_id}  排序: {order_by}  天数: {day_span}  Top创意: {popularity_option or '(无)'}")
    print(f"输出前缀: {prefix}")
    print(f"产品列表: {', '.join(e['match'] for e in entries)}")
    print(f"每产品暂停: {'否' if args.no_pause else '是'}")
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
            # ─── 登录（仅一次） ───
            print("[全局] 正在登录…")
            if not await login(page, _email, _password):
                print("[失败] 登录失败", file=sys.stderr)
                sys.exit(2)
            print("[全局] 登录成功")
            await _await_post_login_shell(page)
            await _try_click_search_tab(page, search_tab)

            # ─── 一次性设置筛选 ───
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
                out_path = DATA_DIR / f"test_arrow2_first_card_fields_{safe_name}_{target_date}.json"
                out_path.write_text(
                    json.dumps(result, ensure_ascii=False, indent=2, default=str),
                    encoding="utf-8",
                )
                print(f"\n  [保存] {result['matched_count']} 条 → {out_path}")

                # 每产品结束后暂停（最后一个产品也暂停，方便 check）
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

            summary_path = DATA_DIR / f"test_arrow2_first_card_fields_{target_date}.json"
            summary_path.write_text(
                json.dumps(
                    {"target_date": target_date, "pull_id": pull_id, "products": results},
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                ),
                encoding="utf-8",
            )
            print(f"\n[保存] 汇总文件: {summary_path}")

            # ─── 输出 raw JSON（与 test_arrow2_competitors.py 格式一致） ───
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
