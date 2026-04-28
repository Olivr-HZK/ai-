"""
根据 operation.json 中的元素执行搜索工作流

流程：登录 → 搜索 → 时间框选7天 → 广告素材框选素材 → 筛选框选展示估值
结果：从素材内容中选 天数最新 且 展示估值最高 的素材

登录后跳转的页面即有搜索框，用 operation.json 中的 HTML 匹配元素。
"""
import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

from guangdada_login import login
from proxy_util import prepare_playwright_proxy_for_crawl

from path_util import CONFIG_DIR, DATA_DIR

OP_FILE = CONFIG_DIR / "operation.json"
OUT_DIR = DATA_DIR


def _html_to_selectors(html: str) -> list[str]:
    """从 operation.json 的 html 字符串解析出 CSS 选择器列表"""
    if not html or not html.strip().startswith("<"):
        return []
    sel_list = []
    # 提取 tag
    m = re.search(r"<(\w+)", html)
    tag = m.group(1).lower() if m else "*"
    # 提取 class（取稳定部分，不含 css-xxx 之类动态 hash）
    classes = re.findall(r"class=['\"]([^'\"]+)['\"]", html)
    for cls in classes:
        for c in cls.split():
            if c and not re.match(r"css-[a-z0-9]+", c):
                sel_list.append(f"{tag}.{c}".replace(" ", "."))
                break
        if sel_list:
            break
    # 提取 type
    m = re.search(r"type=['\"]([^'\"]+)['\"]", html)
    if m:
        sel_list.append(f'{tag}[type="{m.group(1)}"]')
    # 提取 role
    m = re.search(r"role=['\"]([^'\"]+)['\"]", html)
    if m:
        sel_list.append(f'{tag}[role="{m.group(1)}"]')
    # 组合 type+role
    if "type=" in html and "role=" in html:
        t = re.search(r"type=['\"]([^'\"]+)['\"]", html)
        r = re.search(r"role=['\"]([^'\"]+)['\"]", html)
        if t and r:
            sel_list.append(f'{tag}[type="{t.group(1)}"][role="{r.group(1)}"]')
    # 提取 value（如 7天 的 input[value='7']）
    m = re.search(r"value=['\"]([^'\"]+)['\"]", html)
    if m and m.group(1).isdigit():
        sel_list.append(f'{tag}[value="{m.group(1)}"]')
    return sel_list


def _load_selectors():
    """从 operation.json 加载，用 HTML 解析选择器"""
    sel_map = {}
    if OP_FILE.exists():
        try:
            data = json.load(open(OP_FILE, encoding="utf-8"))
            for item in data.get("data", []):
                name, html = item.get("element"), item.get("html", "")
                if name and html:
                    sels = _html_to_selectors(html)
                    if sels:
                        sel_map[name] = sels
        except Exception:
            pass
    sel_map["搜索框_容器"] = ["#display-search-input-container", "[id='display-search-input-container']"]
    # operation.json: 时间=7/30/90天, 广告素材=广告/素材/广告主, 筛选=最新创意/最后看见/展示估值
    defaults = {
        "时间": [".filter-search-radio-group_new", "#filter-search-radio-group_new", ".ant-radio-group-solid"],
        "七天": ["label:has-text('7天')", "input[value='7']", ".ant-radio-group-solid label:has-text('7天')"],
        "广告素材": ["#filter_duplicate_removal", ".ant-radio-group-outline"],
        "素材": ["#filter_duplicate_removal label:has-text('素材')", "label:has-text('素材')", "input[value='1']"],
        "筛选": [".flex.items-center.gap-x-3", "div:has(span:has-text('展示估值'))", "div:has(span:has-text('最新创意'))"],
        "展示估值": ["span.text-sm:has-text('展示估值')", "div:has(span:has-text('展示估值'))"],
        "最新创意": ["span.text-sm:has-text('最新创意')", "div:has(span:has-text('最新创意'))"],
        "素材内容": [".shadow-common-light", ".grid.grid-cols-4 div.shadow-common-light"],
    }
    for k, v in defaults.items():
        if k not in sel_map:
            sel_map[k] = v
        else:
            sel_map[k] = sel_map[k] + v
    return sel_map


SELECTORS = _load_selectors()


async def _click(page, keys: list, timeout: int = 5000) -> bool:
    for key in keys:
        sels = SELECTORS.get(key, [key] if isinstance(key, str) else [])
        if isinstance(sels, str):
            sels = [s.strip() for s in sels.split(",")]
        for sel in sels:
            try:
                loc = page.locator(sel)
                if await loc.count() > 0:
                    first = loc.first
                    await first.scroll_into_view_if_needed()
                    await first.click(timeout=timeout)
                    return True
            except Exception:
                pass
        try:
            if await page.locator(f"text={key}").count() > 0:
                await page.locator(f"text={key}").first.click(timeout=timeout)
                return True
        except Exception:
            pass
    return False


def _extract_creative_lists(obj) -> list[list]:
    """
    从响应 JSON 中递归查找可能的创意列表：
    - 只要是 list 且元素为 dict，且包含 ad_key/creative_id/creativeId/id 等字段，就认为是创意列表。
    返回所有匹配到的列表。
    """
    results: list[list] = []

    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            sample = obj[0]
            keys = sample.keys()
            if any(
                k in keys
                for k in ("ad_key", "creative_id", "creativeId", "id")
            ):
                results.append(obj)
        # 继续深入子元素
        for item in obj:
            results.extend(_extract_creative_lists(item))
    elif isinstance(obj, dict):
        # 跳过我们内部的 meta 哨兵
        if "__meta__" in obj:
            return results
        for v in obj.values():
            results.extend(_extract_creative_lists(v))
    return results


async def _click_day_span(page, day_span: str, log_prefix: str) -> bool:
    """点击时间窗：7 / 30 / 90 天（无法匹配时回退 7 天并打印提醒）。"""
    s = str(day_span or "7").strip()
    if s in ("7", "7天"):
        ok = await _click(page, ["七天", "时间"])
        print(f"{log_prefix}7天 {'✓' if ok else '✗'}")
        return bool(ok)
    if s in ("30", "30天"):
        for keys in (["label:has-text('30天')", "30天", "时间"], ["input[value='30']"], ["三十天", "时间"]):
            if await _click(page, keys):
                print(f"{log_prefix}30天 ✓")
                return True
        print(f"{log_prefix}[提醒] 未找到 30 天选项，回退 7 天", file=sys.stderr)
    elif s in ("90", "90天"):
        for keys in (["label:has-text('90天')", "90天", "时间"], ["input[value='90']"], ["九十天", "时间"]):
            if await _click(page, keys):
                print(f"{log_prefix}90天 ✓")
                return True
        print(f"{log_prefix}[提醒] 未找到 90 天选项，回退 7 天", file=sys.stderr)
    ok = await _click(page, ["七天", "时间"])
    print(f"{log_prefix}7天（回退） {'✓' if ok else '✗'}")
    return bool(ok)


async def _select_top_popularity_option(
    page,
    popularity_option_text: str | None,
    use_first_fallback: bool,
    log_prefix: str,
    *,
    log_quiet: bool = False,
    max_retries: int = 3,
) -> bool:
    """
    展开「Top 创意 / 人气」类下拉，优先按 `popularity_option_text` 子串匹配某一项，否则用首项或放弃。
    失败时重试最多 max_retries 次。
    """
    for attempt in range(max_retries):
        ok = False
        try:
            selector = page.locator("div.ant-select:has(#filter_popularity_tag) .ant-select-selector").first
            if await selector.count() > 0:
                await selector.scroll_into_view_if_needed()
                try:
                    await selector.click(timeout=3000)
                except Exception:
                    await selector.click(timeout=3000, force=True)
                await page.wait_for_timeout(400)
                for opt_sel in [
                    "#filter_popularity_tag_list .ant-select-item",
                    "div[id='filter_popularity_tag_list'] .ant-select-item",
                    "div[role='listbox'] .ant-select-item",
                ]:
                    cand = page.locator(opt_sel)
                    count = await cand.count()
                    if count == 0:
                        continue
                    want = (popularity_option_text or "").strip()
                    for i in range(min(count, 30)):
                        opt = cand.nth(i)
                        try:
                            txt = (await opt.inner_text() or "").strip()
                        except Exception:
                            continue
                        if want and want in txt:
                            try:
                                await opt.click(timeout=3000)
                            except Exception:
                                await opt.click(timeout=3000, force=True)
                            _p(f"{log_prefix}  已点 Top 创意 → {txt[:32]}…", log_quiet=log_quiet)
                            ok = True
                            break
                        if not want and use_first_fallback and i == 0:
                            try:
                                await opt.click(timeout=3000)
                            except Exception:
                                await opt.click(timeout=3000, force=True)
                            _p(f"{log_prefix}  已点 Top 创意首项: {txt[:32]}", log_quiet=log_quiet)
                            ok = True
                            break
                    if ok:
                        break
        except Exception:
            ok = False
        if ok:
            return True
        if attempt < max_retries - 1:
            _p(f"{log_prefix}Top 创意下拉第 {attempt + 1} 次失败，重试…", log_quiet=log_quiet)
            await page.wait_for_timeout(1000)

    if popularity_option_text and not log_quiet:
        print(
            f"{log_prefix}Top 创意下拉拉取 {popularity_option_text!r} 失败（已重试 {max_retries} 次），请人工核对页面",
            file=sys.stderr,
        )
    return ok


async def _try_click_search_tab(
    page, search_tab: str, *, log_quiet: bool = False
) -> None:
    """登录后切到 游戏/工具/试玩（失败仅打印，不中断）。"""
    st = (search_tab or "game").strip().lower()
    if st in ("", "game"):
        for sel in (
            "div.flex:has-text('游戏') >> text=游戏",
            "div.cursor-pointer:has-text('游戏'):not(:has(.text-primary))",
        ):
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    await loc.click(timeout=2500)
                    _p("[arrow2] 已点「游戏」页签", log_quiet=log_quiet)
                    await page.wait_for_timeout(800)
                    return
            except Exception:
                continue
        return
    if st == "tool":
        for sel in ("text=工具", "div:has-text('工具').cursor-pointer"):
            try:
                if await page.locator(sel).first.count() > 0:
                    await page.locator(sel).first.click(timeout=2500)
                    _p("[arrow2] 已点「工具」页签", log_quiet=log_quiet)
                    await page.wait_for_timeout(800)
                    return
            except Exception:
                continue
        return
    if st in ("playable", "playable_ads"):
        for sel in ("text=试玩广告", "a:has-text('试玩')", "div:has-text('试玩广告')"):
            try:
                if await page.locator(sel).first.count() > 0:
                    await page.locator(sel).first.click(timeout=2500)
                    _p("[arrow2] 已点「试玩广告」", log_quiet=log_quiet)
                    await page.wait_for_timeout(1200)
                    return
            except Exception:
                continue


def _beijing_ymd_from_first_seen(ts: object) -> str | None:
    try:
        t = int(ts)  # type: ignore[arg-type]
    except Exception:
        return None
    try:
        tz = timezone(timedelta(hours=8))
        return datetime.fromtimestamp(t, tz=tz).date().isoformat()
    except Exception:
        return None


def _beijing_dt_from_unix_sec(ts: object) -> str | None:
    try:
        t = int(ts)  # type: ignore[arg-type]
    except Exception:
        return None
    try:
        tz = timezone(timedelta(hours=8))
        return datetime.fromtimestamp(t, tz=tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _beijing_unix_sec_from_ymd(ymd: str, *, end_of_day: bool = False) -> int | None:
    s = (ymd or "").strip()[:10]
    if not s:
        return None
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return None
    tz = timezone(timedelta(hours=8))
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59, tzinfo=tz)
    else:
        dt = dt.replace(hour=0, minute=0, second=0, tzinfo=tz)
    return int(dt.timestamp())


def _parse_dom_date_range_text(date_range_text: object) -> tuple[str | None, str | None]:
    raw = str(date_range_text or "").strip()
    if not raw:
        return None, None
    parts = re.split(r"\s*[~～]\s*", raw, maxsplit=1)
    if len(parts) != 2:
        return None, None

    def _norm_one(s: str) -> str | None:
        s2 = re.sub(r"[^\d\-/\.]", "", s.strip())[:10]
        if not s2:
            return None
        s2 = s2.replace("/", "-").replace(".", "-")
        try:
            return datetime.strptime(s2, "%Y-%m-%d").date().isoformat()
        except Exception:
            return None

    return _norm_one(parts[0]), _norm_one(parts[1])


def _oldest_first_seen_ymd_among_creatives(creatives: list) -> str | None:
    """合并列表中每条 first_seen 的北京自然日，取最早（最旧）一日；用于「最新」列表已滚过目标日边界时提前停滚。"""
    ymds: list[str] = []
    for c in creatives:
        if not isinstance(c, dict):
            continue
        d = _beijing_ymd_from_first_seen(c.get("first_seen"))
        if d:
            ymds.append(d)
    if not ymds:
        return None
    return min(ymds)


def _filter_creatives_first_seen_day(creatives: list, ymd: str) -> list:
    ymd = (ymd or "").strip()[:10]
    if not ymd or not isinstance(creatives, list):
        return list(creatives) if isinstance(creatives, list) else []
    out: list = []
    for c in creatives:
        if not isinstance(c, dict):
            continue
        d = _beijing_ymd_from_first_seen(c.get("first_seen"))
        if d == ymd:
            out.append(c)
    return out


def _arrow2_apply_post_filters(
    all_creatives: list[dict],
    spec: dict[str, Any],
    keyword: str,
    keyword_product: dict[str, str] | None,
    first_seen_ymd: str,
) -> list[dict]:
    """仅 napi 路径：拉取后先按广告主，再按 first_seen 日，最后 max_c 截断。"""
    c = [x for x in all_creatives if isinstance(x, dict)]
    prod = (keyword_product or {}).get(keyword) or (keyword_product or {}).get((keyword or "").strip())
    if prod and str(prod).strip():
        from workflow_guangdada_competitor_yesterday_creatives import advertiser_matches_product  # noqa: PLC0415

        p2 = str(prod).strip()
        c = [
            x
            for x in c
            if advertiser_matches_product(str(x.get("advertiser_name") or x.get("page_name") or ""), p2)
        ]
    if spec.get("filter_yesterday_only") and first_seen_ymd:
        c = _filter_creatives_first_seen_day(c, first_seen_ymd)
    mdef = 10**6
    try:
        mc = int(spec.get("max_creatives_per_keyword") or os.getenv("ARROW2_MAX_CREATIVES_PER_KEYWORD") or 0) or 0
    except Exception:
        mc = 0
    if mc and mc < mdef:
        c = c[:mc]
    return c


def _arrow2_filter_stage_views(
    all_creatives: list[dict],
    spec: dict[str, Any],
    keyword: str,
    keyword_product: dict[str, str] | None,
    first_seen_ymd: str,
) -> tuple[list[dict], list[dict]]:
    """返回两段视图：先广告主匹配，再在其基础上按 first_seen 日过滤。"""
    base = [x for x in all_creatives if isinstance(x, dict)]
    prod = (keyword_product or {}).get(keyword) or (keyword_product or {}).get((keyword or "").strip())
    adv_matched = list(base)
    if prod and str(prod).strip():
        from workflow_guangdada_competitor_yesterday_creatives import advertiser_matches_product  # noqa: PLC0415

        p2 = str(prod).strip()
        adv_matched = [
            x
            for x in adv_matched
            if advertiser_matches_product(str(x.get("advertiser_name") or x.get("page_name") or ""), p2)
        ]
    time_filtered = list(adv_matched)
    if spec.get("filter_yesterday_only") and first_seen_ymd:
        time_filtered = _filter_creatives_first_seen_day(time_filtered, first_seen_ymd)
    return adv_matched, time_filtered


def print_arrow2_filter_stage_creatives(
    keyword: str,
    label: str,
    creatives: list[dict],
    *,
    max_items: int = 60,
) -> None:
    if not isinstance(creatives, list):
        return
    n = len(creatives)
    print(f"[arrow2] {label}: 词={keyword!r} 条数={n}（见下，最多列 {min(max_items, n)}/{n} 条）")
    for i, c in enumerate(creatives[: max(0, int(max_items))], 1):
        if not isinstance(c, dict):
            continue
        ak = c.get("ad_key") or ""
        fs = c.get("first_seen")
        fs_utc8 = _beijing_dt_from_unix_sec(fs) or "?"
        adv = (c.get("advertiser_name") or c.get("page_name") or "")[:48]
        t = (c.get("title") or c.get("body") or "")[:48]
        print(
            f"  {i:>2}. ad_key={str(ak)[:24]} first_seen={fs} utc+8={fs_utc8!r} "
            f"广告主={adv!r} 标题={t!r}"
        )


def _env_truthy(name: str, default: bool) -> bool:
    v = (os.environ.get(name) or "").strip().lower()
    if v == "" and name not in os.environ:
        return default
    return v not in ("0", "false", "no", "off")


def _p(
    msg: str,
    log_quiet: bool = False,
    *,
    file: object | None = None,
    end: str | None = None,
) -> None:
    if log_quiet:
        return
    print(msg, file=file, end=end or "\n")


def _arrow2_enrich_detail_verbose() -> bool:
    return _env_truthy("ARROW2_ENRICH_DETAIL_VERBOSE", default=False)


def _arrow2_geo_still_empty(creative: dict[str, Any]) -> bool:
    if not isinstance(creative, dict):
        return True
    for key in ("countries", "country", "country_code", "country_codes", "areas", "area"):
        v = creative.get(key)
        if isinstance(v, list) and v:
            return False
        if isinstance(v, dict) and v:
            return False
        if str(v or "").strip():
            return False
    return True


def _detail_rows_from_body(body: object) -> list[dict]:
    out: list[dict] = []
    seen_obj_ids: set[int] = set()
    if isinstance(body, dict):
        if body.get("ad_key"):
            out.append(body)
            seen_obj_ids.add(id(body))
        for key in ("data", "result", "creative", "info", "item", "detail"):
            sub = body.get(key)
            if isinstance(sub, dict) and sub.get("ad_key") and id(sub) not in seen_obj_ids:
                out.append(sub)
                seen_obj_ids.add(id(sub))
    for lst in _extract_creative_lists(body):
        if not isinstance(lst, list):
            continue
        for item in lst:
            if isinstance(item, dict) and item.get("ad_key") and id(item) not in seen_obj_ids:
                out.append(item)
                seen_obj_ids.add(id(item))
    return out


def _pick_best_detail_row(detail_rows: list[dict], creative: dict[str, Any]) -> dict | None:
    if not detail_rows:
        return None
    ad_key = str(creative.get("ad_key") or creative.get("creative_id") or creative.get("id") or "").strip()
    if ad_key:
        for row in detail_rows:
            if str(row.get("ad_key") or "").strip() == ad_key:
                return row
    preview = str(creative.get("preview_img_url") or "").split("?")[0]
    if preview:
        for row in detail_rows:
            prev2 = str(row.get("preview_img_url") or "").split("?")[0]
            if prev2 and prev2 == preview:
                return row
    return detail_rows[0]


async def _fetch_detail_v2(page, creative: dict[str, Any], *, app_type: int) -> dict | None:
    ad_key = str(
        creative.get("ad_key") or creative.get("creative_id") or creative.get("id") or creative.get("creativeId") or ""
    ).strip()
    search_flag = creative.get("search_flag")
    if not ad_key or search_flag in (None, ""):
        return None
    try:
        sf = int(search_flag)
    except Exception:
        return None
    try:
        at = int(app_type)
    except Exception:
        return None
    url = f"/napi/v1/creative/detail-v2?ad_key={ad_key}&app_type={at}&search_flag={sf}"
    try:
        body = await page.evaluate(
            """
async ({ url }) => {
  const resp = await fetch(url, {
    method: 'GET',
    credentials: 'include',
    headers: {
      'accept': 'application/json, text/plain, */*'
    }
  });
  let data = null;
  try {
    data = await resp.json();
  } catch (e) {
    data = null;
  }
  return { ok: resp.ok, status: resp.status, url: resp.url, body: data };
}
""",
            {"url": url},
        )
    except Exception:
        return None
    if not isinstance(body, dict):
        return None
    if not body.get("ok"):
        return None
    return body


async def _fetch_detail_v2_best_attempt(page, creative: dict[str, Any]) -> dict | None:
    if not isinstance(creative, dict):
        return None
    candidates: list[int] = []
    raw_app_type = creative.get("app_type")
    try:
        if raw_app_type not in (None, ""):
            candidates.append(int(raw_app_type))
    except Exception:
        pass
    for v in (1, 2):
        if v not in candidates:
            candidates.append(v)
    for app_type in candidates:
        body = await _fetch_detail_v2(page, creative, app_type=app_type)
        if isinstance(body, dict):
            return body
    return None


def _merge_detail_v2_geo_into_creative(creative: dict[str, Any], body: dict[str, Any]) -> bool:
    if not isinstance(creative, dict) or not isinstance(body, dict):
        return False
    row = _pick_best_detail_row(_detail_rows_from_body(body.get("body") if "body" in body else body), creative)
    if not isinstance(row, dict):
        return False
    changed = False
    for key in ("countries", "country", "country_code", "country_codes", "areas", "area"):
        v = row.get(key)
        if v in (None, "", [], {}):
            continue
        creative[key] = v
        changed = True
    return changed


def _print_dom_detail_probe(idx: int, meta: dict[str, str], detail_rows: list[dict]) -> None:
    """调试：只点少量卡片时，打印 detail 原始字段，便于人工对齐字段语义。"""
    print(
        f"    [DOM探针] 第 {idx + 1} 张卡片"
        f" 广告主={str(meta.get('advertiser') or '')[:50]!r}"
        f" 标题={str(meta.get('title') or '')[:80]!r}",
        flush=True,
    )
    if not detail_rows:
        print("    [DOM探针] 本张未拿到 detail JSON", flush=True)
        return

    preferred_time_keys = [
        "first_seen",
        "created_at",
        "last_seen",
        "start_time",
        "end_time",
        "create_time",
        "update_time",
        "launch_time",
        "online_time",
        "online_at",
    ]
    for j, row in enumerate(detail_rows[:3], 1):
        if not isinstance(row, dict):
            continue
        keys = sorted(str(k) for k in row.keys())
        time_fields: dict[str, object] = {}
        for k in preferred_time_keys:
            if k in row:
                time_fields[k] = row.get(k)
        print(
            f"    [DOM探针] detail#{j} ad_key={str(row.get('ad_key') or '')[:32]!r} "
            f"keys={keys[:80]}{' ...' if len(keys) > 80 else ''}",
            flush=True,
        )
        print(
            f"    [DOM探针] detail#{j} time_fields={json.dumps(time_fields, ensure_ascii=False)}",
            flush=True,
        )
        sample = {
            "ad_key": row.get("ad_key"),
            "advertiser_name": row.get("advertiser_name"),
            "page_name": row.get("page_name"),
            "title": row.get("title"),
            "body": row.get("body"),
            "first_seen": row.get("first_seen"),
            "created_at": row.get("created_at"),
            "last_seen": row.get("last_seen"),
            "days_count": row.get("days_count"),
            "heat": row.get("heat"),
            "all_exposure_value": row.get("all_exposure_value"),
            "impression": row.get("impression"),
            "platform": row.get("platform"),
            "preview_img_url": row.get("preview_img_url"),
            "resource_urls": row.get("resource_urls"),
        }
        print(
            f"    [DOM探针] detail#{j} sample={json.dumps(sample, ensure_ascii=False, default=str)}",
            flush=True,
        )


async def _await_post_login_shell(page) -> None:
    """
    登录后进入主站。勿长时间「仅等 networkidle」：广大大类 SPA 常有轮询/长连，
    `wait_for_load_state("networkidle")` 可卡满 15s 或长期达不到，终端像死机。

    顺序：`load` → 短等 `networkidle`（不成就放弃）→ 短 sleep，供 Tab/筛选项渲染。
    """
    try:
        await page.wait_for_load_state("load", timeout=15000)
    except Exception:
        pass
    try:
        await page.wait_for_load_state("networkidle", timeout=4000)
    except Exception:
        pass
    await page.wait_for_timeout(1500)


def print_arrow2_matched_creatives(keyword: str, filtered: list, max_items: int = 30) -> None:
    """拉取+过滤后仅打印最终匹配条（与 DB / 落盘一致口径）。"""
    if not isinstance(filtered, list):
        return
    n = len(filtered)
    print(f"[arrow2] 已匹配: 词={keyword!r} 条数={n}（见下，最多列 {min(max_items, n)}/{n} 条）")
    for i, c in enumerate(filtered[: max(0, int(max_items))], 1):
        if not isinstance(c, dict):
            continue
        ak = c.get("ad_key") or ""
        src = c.get("_source", "")
        fs = c.get("first_seen")
        fs_utc8 = _beijing_dt_from_unix_sec(fs) or "?"
        adv = (c.get("advertiser_name") or c.get("page_name") or "")[:48]
        t = (c.get("title") or c.get("body") or "")[:48]
        imp = c.get("impression", "?")
        exp = c.get("all_exposure_value", "?")
        heat = c.get("heat", "?")
        days = c.get("days_count", "?")
        platform = c.get("platform", "?")
        duration = c.get("video_duration", "?")
        print(
            f"  {i:>2}. ad_key={str(ak)[:24]} first_seen={fs} utc+8={fs_utc8!r} src={src!r} "
            f"平台={platform!r} 天数={days!r} 时长={duration!r} "
            f"人气={exp!r} 估值={imp!r} 热度={heat!r} 广告主={adv!r} 标题={t!r}"
        )


def _print_debug_step_cards(keyword: str, all_c: list, max_n: int) -> None:
    n = min(max_n, len(all_c))
    print(f"\n[debug-step] 词={keyword!r} 共 {len(all_c)} 条，示前 {n} 条：")
    for i, c in enumerate(all_c[:n], 1):
        if not isinstance(c, dict):
            continue
        title = (c.get("title") or c.get("body") or "")[:60]
        ak = c.get("ad_key") or c.get("id")
        im = c.get("impression")
        aev = c.get("all_exposure_value")
        heat = c.get("heat")
        fs = c.get("first_seen")
        fs_utc8 = _beijing_dt_from_unix_sec(fs) or "?"
        print(
            f"  {i}. ad_key={ak} 人气={aev!r} 估值={im!r} 热度={heat!r} "
            f"first_seen={fs!r} utc+8={fs_utc8!r} 标题={title!r}"
        )


def _print_pause_yesterday_summary(all_c: list) -> None:
    print(f"\n[arrow2] 本词「仅昨日 first_seen」后共 {len(all_c)} 条")
    n = min(5, len(all_c))
    for i, c in enumerate(all_c[:n], 1):
        if not isinstance(c, dict):
            continue
        exp = c.get("all_exposure_value", "?")
        imp = c.get("impression", "?")
        heat = c.get("heat", "?")
        fs = c.get("first_seen", "?")
        fs_utc8 = _beijing_dt_from_unix_sec(fs) or "?"
        print(
            f"  {i} ad_key={c.get('ad_key')!r} first_seen={fs!r} utc+8={fs_utc8!r} "
            f"人气={exp} 估值={imp} 热度={heat}"
        )


_ISO3_ZH_MAP: dict[str, str] | None = None


def _get_iso3_zh_map() -> dict[str, str]:
    global _ISO3_ZH_MAP
    if _ISO3_ZH_MAP is not None:
        return _ISO3_ZH_MAP
    p = CONFIG_DIR / "iso3166_alpha3_zh.json"
    if p.is_file():
        try:
            with open(p, encoding="utf-8") as f:
                _ISO3_ZH_MAP = {str(k).upper(): str(v) for k, v in json.load(f).items()}
        except Exception:
            _ISO3_ZH_MAP = {}
    else:
        _ISO3_ZH_MAP = {}
    return _ISO3_ZH_MAP


def _display_labels_for_iso3(code: str) -> list[str]:
    c = (code or "").strip().upper()[:3]
    m = _get_iso3_zh_map()
    zh = m.get(c)
    out: list[str] = []
    if zh:
        out.append(zh)
    out.append(c)
    if c == "USA":
        out.extend(["美国", "United States", "U.S."])
    elif c == "GBR":
        out.extend(["英国", "United Kingdom", "U.K."])
    uniq: list[str] = []
    s: set[str] = set()
    for x in out:
        if x and x not in s:
            s.add(x)
            uniq.append(x)
    return uniq


async def _scroll_to_country_input(page) -> None:
    """将国家/地区筛选项滚进视口，避免在筛选横条下方时点不到。"""
    for sel in (
        'input[placeholder="国家/地区"]',
        'input[placeholder*="国家"]',
        "input[placeholder*='地区']",
    ):
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                await loc.scroll_into_view_if_needed()
                return
        except Exception:
            continue


async def _open_country_region_picker(page, log_prefix: str, *, log_quiet: bool = False) -> bool:
    """
    广大大：国家/地区为只读 `input[placeholder=国家/地区].action-input`，点击后弹出面板再选国。
    """
    await _scroll_to_country_input(page)
    for sel in (
        'input[placeholder="国家/地区"]',
        'input.action-input[placeholder="国家/地区"]',
        "input.cursor-pointer[placeholder='国家/地区']",
        "input.ant-input[placeholder='国家/地区']",
        'input[placeholder*="国家/地区"]',
    ):
        try:
            inp = page.locator(sel).first
            if await inp.count() == 0:
                continue
            await inp.scroll_into_view_if_needed()
            await inp.click(timeout=3000, force=True)
            _p(f"{log_prefix}  已点选「国家/地区」展开 ({sel})", log_quiet=log_quiet)
            await page.wait_for_timeout(500)
            return True
        except Exception:
            continue
    for opener in (
        "span:has-text('国家/地区')",
        "text=国家/地区",
    ):
        try:
            loc = page.locator(opener).first
            if await loc.count() > 0:
                await loc.scroll_into_view_if_needed()
                await loc.click(timeout=2000, force=True)
                await page.wait_for_timeout(600)
                return True
        except Exception:
            continue
    return False


async def _select_ad_channel_checkboxes(
    page, ad_channel_labels: list[str] | None, log_prefix: str, *, log_quiet: bool = False
) -> int:
    """
    勾选「Facebook系 / Google系 / UnityAds / AppLovin」等：`label.ant-checkbox-wrapper` 内为渠道名 + net-icon。
    设 `ARROW2_SKIP_AD_CHANNEL_FILTER=1` 跳过。
    """
    if not ad_channel_labels or _env_truthy("ARROW2_SKIP_AD_CHANNEL_FILTER", default=False):
        return 0
    n = 0
    _p(f"{log_prefix}广告渠道: 尝试勾选 {len(ad_channel_labels)} 项", log_quiet=log_quiet)
    for raw in ad_channel_labels:
        name = str(raw or "").strip()
        if not name:
            continue
        ok = False
        try:
            lab = page.locator("label.ant-checkbox-wrapper").filter(has_text=name).first
            if await lab.count() > 0:
                await lab.scroll_into_view_if_needed()
                await lab.click(timeout=3000, force=True)
                _p(f"{log_prefix}  ✓ 渠道 {name!r}", log_quiet=log_quiet)
                n += 1
                ok = True
                await page.wait_for_timeout(250)
        except Exception as e:
            if not log_quiet:
                print(f"{log_prefix}  ✗ 渠道 {name!r} {e}", file=sys.stderr)
        if not ok and not log_quiet:
            print(
                f"{log_prefix}  ✗ 未找到 label.ant-checkbox-wrapper: {name!r}",
                file=sys.stderr,
            )
    await page.wait_for_timeout(400)
    return n


async def _open_country_region_fallback_clicks(page) -> bool:
    """备用：点「国家/地区」文案展开。"""
    for opener in (
        "span:has-text('国家/地区')",
        "text=国家/地区",
    ):
        try:
            loc = page.locator(opener).first
            if await loc.count() > 0:
                await loc.scroll_into_view_if_needed()
                await loc.click(timeout=2000, force=True)
                await page.wait_for_timeout(500)
                return True
        except Exception:
            continue
    return False


async def _has_any_visible_filter_dropdown(page) -> bool:
    """筛选区下拉面是否已展开（任一类），用于避免「下拉面已开却又点一次国家」把层关掉。"""
    for cls in (".ant-cascader-dropdown", ".ant-select-dropdown", ".ant-tree-select-dropdown"):
        try:
            loc = page.locator(cls)
            n = await loc.count()
        except Exception:
            continue
        for i in range(n):
            try:
                w = loc.nth(i)
                if await w.is_visible():
                    return True
            except Exception:
                continue
    return False


async def _fill_country_dropdown_search(page, needle: str) -> bool:
    """
    在「已展开」的筛选下拉面内搜国家/地区，**禁止**用页面顶部的创意/关键词 combobox。
    只向 **当前可见** 的 dropdown 里填，避免用 .last 指到其它隐藏层/错误筛选。
    """
    inners = [
        "input[placeholder*='搜索']",
        "input[placeholder*='搜索国家']",
        ".ant-cascader-picker-search input",
        ".ant-select-selection-search input",
        "input[type='search']",
        "input.ant-select-selection-search-input",
    ]
    for cls in (".ant-cascader-dropdown", ".ant-select-dropdown", ".ant-tree-select-dropdown"):
        try:
            loc = page.locator(cls)
            n = await loc.count()
        except Exception:
            continue
        for i in range(n):
            try:
                wrap = loc.nth(i)
                if not await wrap.is_visible():
                    continue
            except Exception:
                continue
            for inner in inners:
                try:
                    sbox = wrap.locator(inner).first
                    if await sbox.count() == 0:
                        continue
                    if not await sbox.is_visible():
                        continue
                    await sbox.fill("")
                    await sbox.fill(needle)
                    await page.wait_for_timeout(450)
                    return True
                except Exception:
                    continue
    return False


async def _click_country_region_confirm(
    page, log_prefix: str, *, log_quiet: bool = False
) -> bool:
    """
    广大大国家/地区多选：选完后在 **ant-popover-inner** 底栏点主按钮（`.ant-btn-primary`，文案常见为「确 定」
    带空格）后筛选才生效。优先点 Popover 内主按钮，避免误点页面其他「确定」。
    """
    # 1) 同一只国家 Popover 里：取「确 定」主按钮（与「取 消」的 default 区分）
    for sel in (
        "div.ant-popover-inner:visible button.ant-btn-primary",
        "div.ant-popover:visible .ant-btn-primary",
    ):
        try:
            b = page.locator(sel).last
            if await b.count() == 0:
                continue
            if await b.is_visible():
                await b.scroll_into_view_if_needed()
                await b.click(timeout=3000, force=True)
                _p(
                    f"{log_prefix}  已点国家/地区弹层主按钮（确定/确 定）",
                    log_quiet=log_quiet,
                )
                await page.wait_for_timeout(600)
                return True
        except Exception:
            continue
    js = r"""
    () => {
      const want = ['确认', '确定', '应用', '完成', '好'];
      function norm(t) { return (t || '').replace(/\s/g, ''); }
      function vis(el) {
        if (!el) return false;
        const st = getComputedStyle(el);
        if (st.visibility === 'hidden' || st.display === 'none') return false;
        if (parseFloat(st.opacity || '1') < 0.01) return false;
        const r = el.getBoundingClientRect();
        if (r.width < 1 || r.height < 1) return false;
        if (r.bottom < 0 || r.top > window.innerHeight) return false;
        if (r.right < 0 || r.left > window.innerWidth) return false;
        return true;
      }
      function tryClick(el) {
        const t = norm(el.innerText || el.textContent || '');
        for (const w of want) {
          if (t === w) { el.click(); return w; }
        }
        return '';
      }
      const roots = [
        'div[role="dialog"]', 'div[role="presentation"]',
        '.ant-popover', '.ant-popover-content', '.ant-modal', '.ant-modal-content', '.ant-modal-wrap',
        '.ant-drawer', '.ant-drawer-content', '.ant-cascader-dropdown', '.ant-cascader-menus',
        '.ant-select-dropdown', '.ant-picker-dropdown', 'footer', '[class*="cascader"]', '[class*="Cascader"]',
      ];
      for (const rs of roots) {
        for (const root of document.querySelectorAll(rs)) {
          if (!vis(root)) continue;
          for (const el of root.querySelectorAll(
            'button, a, [role="button"], .ant-btn, .ant-btn-primary, span.ant-btn, div.ant-btn'
          )) {
            if (!vis(el)) continue;
            const g = tryClick(el);
            if (g) return g;
            for (const ch of el.querySelectorAll('span, div, p')) {
              const t = norm(ch.textContent || '');
              for (const w of want) {
                if (t === w) { el.click(); return w; }
              }
            }
          }
        }
      }
      for (const el of document.querySelectorAll('button, .ant-btn, [role="button"]')) {
        if (!vis(el)) continue;
        const g = tryClick(el);
        if (g) return g;
      }
      return '';
    }
    """
    try:
        hit: str = await page.evaluate(js)
    except Exception:
        hit = ""
    if hit:
        _p(
            f"{log_prefix}  已点「{hit}」应用国家/地区（页面脚本）",
            log_quiet=log_quiet,
        )
        await page.wait_for_timeout(600)
        return True

    for lab in ("确认", "确定", "应用", "完成", "好"):
        try:
            btn = page.get_by_role("button", name=lab, exact=False)
            n = await btn.count()
        except Exception:
            n = 0
        for i in range(n - 1, -1, -1):
            try:
                t = btn.nth(i)
                if not await t.is_visible():
                    continue
                await t.scroll_into_view_if_needed()
                await t.click(timeout=3000, force=True)
                _p(f"{log_prefix}  已点「{lab}」应用国家/地区筛选", log_quiet=log_quiet)
                await page.wait_for_timeout(600)
                return True
            except Exception:
                continue

    for lab in ("确认", "确定", "应用", "完成", "好"):
        try:
            m = page.locator("button, .ant-btn, [role='button'], .ant-btn-primary, span").filter(
                has_text=lab
            )
            c = await m.count()
        except Exception:
            c = 0
        for i in range(c - 1, -1, -1):
            try:
                el = m.nth(i)
                if not await el.is_visible():
                    continue
                raw = _norm_cn_btn(await el.inner_text() or "")
                if raw not in ("确认", "确定", "应用", "完成", "好"):
                    continue
                await el.scroll_into_view_if_needed()
                await el.click(timeout=3000, force=True)
                _p(
                    f"{log_prefix}  已点「{raw}」应用国家/地区筛选",
                    log_quiet=log_quiet,
                )
                await page.wait_for_timeout(600)
                return True
            except Exception:
                continue

    print(
        f"{log_prefix}  ✗ 未点到「确认/确定」等按钮，本次国家筛选可能未应用（请有头看 DOM 或反馈按钮文案）",
        file=sys.stderr,
    )
    return False


def _norm_cn_btn(s: str) -> str:
    return "".join((s or "").split())


def _country_filter_popover(page):
    """
    广大大国家/地区为 Popover，内含「快速检索国家与地区」与 `input[value=ISO3]` 的 checkbox。
    """
    p1 = page.locator("div.ant-popover-inner:visible").filter(
        has=page.locator('input[placeholder*="快速检索国家"]')
    )
    p2 = page.locator("div.ant-popover-inner:visible").filter(
        has=page.locator("label.popover-checkbox input.ant-checkbox-input")
    )
    return p1, p2


async def _tick_country_popover_checkboxes(
    page,
    pop,
    flat: list[str],
    iso_zh: dict,
    log_prefix: str,
    *,
    log_quiet: bool,
) -> int:
    """
    在 .ant-popover-inner 内用 `input.ant-checkbox-input[value=ISO3]` 勾选
    （与广大大 data 一致）；可配合「快速检索国家与地区」按中文名过滤。
    """
    n_ok = 0
    q = pop.locator('input[placeholder*="快速检索国家"]').first
    for code in flat:
        if not code:
            continue
        zh = (iso_zh or {}).get(code) or ""
        inp = pop.locator(
            f'input.ant-checkbox-input[type="checkbox"][value="{code}"]',
        ).first
        if await inp.count() == 0 and zh and await q.count() > 0:
            await q.fill("")
            await q.fill(zh)
            await page.wait_for_timeout(500)
            inp = pop.locator(
                f'input.ant-checkbox-input[type="checkbox"][value="{code}"]',
            ).first
        if await inp.count() == 0:
            if not log_quiet:
                print(
                    f"{log_prefix}  ✗ Popover 内无 value={code}（{zh}）",
                    file=sys.stderr,
                )
            continue
        try:
            lab = pop.locator(
                f'label.popover-checkbox:has(input.ant-checkbox-input[value="{code}"])',
            ).or_(
                pop.locator(
                    f'label:has(input.ant-checkbox-input[value="{code}"])',
                )
            ).first
            to_click = lab if await lab.count() > 0 else inp
            await to_click.scroll_into_view_if_needed(timeout=5000)
            if await inp.is_checked():
                n_ok += 1
                _p(
                    f"{log_prefix}  ✓(Popover) {code} 已勾选",
                    log_quiet=log_quiet,
                )
                continue
            await to_click.click(timeout=3000, force=True)
            n_ok += 1
            _p(
                f"{log_prefix}  ✓(Popover) 已勾选 {code}（{zh!r}）",
                log_quiet=log_quiet,
            )
            await page.wait_for_timeout(200)
        except Exception as e:
            if not log_quiet:
                print(
                    f"{log_prefix}  ✗ 勾选 {code} 失败: {e}",
                    file=sys.stderr,
                )
    return n_ok


async def _try_click_country_label_in_ui(page, label: str) -> bool:
    """尝试点击某一国家/地区展示名（多路径）。"""
    for exact in (True, False):
        try:
            opt = page.get_by_role("option", name=label, exact=exact)
            if await opt.count() > 0:
                o = opt.first
                if await o.is_visible():
                    await o.scroll_into_view_if_needed()
                    await o.click(timeout=2500, force=True)
                    return True
        except Exception:
            pass
    for rname in ("menuitem", "menuitemcheckbox", "treeitem"):
        try:
            n = page.get_by_role(rname, name=label, exact=False)
            if await n.count() > 0:
                c = n.first
                if await c.is_visible():
                    await c.scroll_into_view_if_needed()
                    await c.click(timeout=2000, force=True)
                    return True
        except Exception:
            pass
    # CSS 回退：避免 label 里含引号时串选择器
    for sel in (".ant-select-item", ".ant-cascader-menu-item", "label.ant-checkbox-wrapper"):
        try:
            el = page.locator(sel).filter(has_text=label).first
            if await el.count() == 0:
                continue
            if not await el.is_visible():
                continue
            await el.scroll_into_view_if_needed()
            await el.click(timeout=2000, force=True)
            return True
        except Exception:
            continue
    return False


async def _select_countries_on_page(
    page, country_codes: list[str] | None, log_prefix: str, *, log_quiet: bool = False
) -> int:
    """
    登录后、时间/素材之前：在筛选区多选国家/地区。ISO-3166 alpha-3 与 `iso3166_alpha3_zh.json` 对应用中文名点选；失败不中断。
    设 `ARROW2_SKIP_COUNTRY_FILTER=1` 可整段跳过。

    当前页（广大大）：`input[placeholder=国家/地区]` 打开的是 **ant-popover-inner**，
    内为 `input[value=ISO3]` 的 `popover-checkbox` 多选，底栏为「取 消」「确 定」主按钮（`.ant-btn-primary`）；
    与旧版级联/虚拟列表无关时走下方兼容路径。

    同一弹层内勾完全部目标国家后再点 **确 定**；勿依赖 Esc 当应用。
    """
    if not country_codes or _env_truthy("ARROW2_SKIP_COUNTRY_FILTER", default=False):
        return 0
    n_ok = 0
    m = _get_iso3_zh_map()
    expected = len([x for x in country_codes if str(x).strip()])
    _p(f"{log_prefix}国家/地区: 尝试多选 {expected} 项", log_quiet=log_quiet)

    flat: list[str] = [str(x).strip().upper()[:3] for x in country_codes if str(x).strip()]
    if not flat:
        return 0

    await _scroll_to_country_input(page)
    opened = await _open_country_region_picker(page, log_prefix, log_quiet=log_quiet) or await _open_country_region_fallback_clicks(
        page
    )
    if not opened:
        print(
            f"{log_prefix}  ✗ 无法展开「国家/地区」，已跳过国别多选（0/{expected}）。",
            file=sys.stderr,
        )
        return 0
    await page.wait_for_timeout(600)

    p1, p2 = _country_filter_popover(page)
    pop: Any = None
    if await p1.count() > 0:
        pop = p1.first
    elif await p2.count() > 0:
        pop = p2.first

    if pop is not None:
        n_ok = await _tick_country_popover_checkboxes(
            page, pop, flat, m, log_prefix, log_quiet=log_quiet
        )
    else:
        for idx, code in enumerate(flat):
            if idx > 0 and not (await _has_any_visible_filter_dropdown(page)):
                await _scroll_to_country_input(page)
                opened2 = await _open_country_region_picker(
                    page, log_prefix, log_quiet=log_quiet
                ) or await _open_country_region_fallback_clicks(page)
                if not opened2:
                    print(
                        f"{log_prefix}  ✗ 下拉面已关且无法重开，跳过 {code}（{m.get(code) or code}）",
                        file=sys.stderr,
                    )
                    continue
                await page.wait_for_timeout(400)
            labels = _display_labels_for_iso3(code)
            zh0 = m.get(code) or labels[0]
            picked = False

            for label in (zh0, *labels[1:]):
                if not label:
                    continue
                if await _try_click_country_label_in_ui(page, label):
                    _p(
                        f"{log_prefix}  ✓ 已选 {code} → {label!r}",
                        log_quiet=log_quiet,
                    )
                    n_ok += 1
                    picked = True
                    await page.wait_for_timeout(300)
                    break
            if not picked:
                for needle in (zh0, *labels[1:]):
                    if not needle:
                        continue
                    try:
                        ok_fill = await _fill_country_dropdown_search(page, str(needle))
                        if not ok_fill:
                            continue
                        el = page.get_by_role(
                            "option", name=str(needle), exact=False
                        ).first
                        if await el.count() == 0:
                            el = page.locator("div[role=option]").filter(
                                has_text=needle
                            ).first
                        if await el.count() == 0:
                            continue
                        if not await el.is_visible():
                            continue
                        await el.click(timeout=2000, force=True)
                        _p(
                            f"{log_prefix}  ✓(搜索) 已选 {code} → {needle!r}",
                            log_quiet=log_quiet,
                        )
                        n_ok += 1
                        picked = True
                        break
                    except Exception:
                        continue
            if not picked and not log_quiet:
                print(
                    f"{log_prefix}  ✗ 未选上 {code}（{zh0}）请手工核对筛选区",
                    file=sys.stderr,
                )
    if expected and n_ok < expected:
        # 安静模式也打一行，避免「完全没选上」还误以为成功
        print(
            f"{log_prefix}[警告] 国家/地区只成功 {n_ok}/{expected} 个；可 DEBUG=1 或 ARROW2_VERBOSE=1 看过程；"
            f"可设 ARROW2_SKIP_COUNTRY_FILTER=1 先跳过。",
            file=sys.stderr,
        )
    # 与手操一致：在面板里勾完再点「确认/确定」才写入筛选
    await _click_country_region_confirm(page, log_prefix, log_quiet=log_quiet)
    pop_still = await page.locator("div.ant-popover:visible").count()
    if await _has_any_visible_filter_dropdown(page) or pop_still > 0:
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass
        await page.wait_for_timeout(400)
    await page.wait_for_timeout(400)
    return n_ok


async def _do_setup(
    page,
    is_tool: bool,
    log_prefix: str = "",
    order_by: str = "exposure",
    use_popularity_top1: bool = False,
    *,
    day_span: str = "7",
    popularity_option_text: str | None = None,
    ad_channel_labels: list[str] | None = None,
    country_codes: list[str] | None = None,
    log_quiet: bool = False,
) -> None:
    """
    在已登录的页面上做一次性的筛选设置：工具标签（可选）→ 广告渠道多选（可选）→
    国家/地区（只读 input 点开后选，可选）→ 时间窗 → 素材 → Top 创意(可选) → 排序在搜索后点。
    order_by: 供调用方在搜索后使用（本函数内不点排序）。
    day_span: "7" / "30" / "90" 等。
    popularity_option_text: 若设，在 Top 创意下拉中尽量匹配子串；否则 `use_popularity_top1` 为真时点首项。
    ad_channel_labels: 如 Facebook系、Google系、UnityAds、AppLovin，对应 `label.ant-checkbox-wrapper`。
    country_codes: ISO3，点 `input[placeholder=国家/地区]` 打开面板后再点选/搜索。
    log_quiet: True 时少打筛选过程日志（run_arrow2 用）。
    """
    if is_tool:
        _p(f"{log_prefix}切换到「工具」标签…", log_quiet=log_quiet)
        tool_ok = False
        tool_selectors = [
            "div.flex.items-center.justify-center.gap-x-12.text-base div:has-text('工具'):not(:has(.text-primary))",
            "div.border-transparent.cursor-pointer:has-text('工具')",
            "div.flex.items-center.justify-center.gap-x-12.text-base >> text=工具",
            "div:has-text('工具').cursor-pointer:not(:has(.text-primary))",
            "text=工具",
        ]
        for sel in tool_selectors:
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    await loc.scroll_into_view_if_needed()
                    await loc.click(timeout=3000)
                    await page.wait_for_timeout(1500)
                    tool_ok = True
                    _p(f"{log_prefix}已切换到「工具」标签 ✓", log_quiet=log_quiet)
                    break
            except Exception:
                continue
        if not tool_ok:
            _p(f"{log_prefix}未找到「工具」标签 ✗", log_quiet=log_quiet)
        await page.wait_for_timeout(1000)

    if ad_channel_labels:
        await _select_ad_channel_checkboxes(
            page, ad_channel_labels, log_prefix, log_quiet=log_quiet
        )
    if country_codes:
        await _select_countries_on_page(page, country_codes, log_prefix, log_quiet=log_quiet)

    _p(f"{log_prefix}选择时间窗: {day_span!r} 天", log_quiet=log_quiet)
    await _click_day_span(page, day_span, log_prefix)
    await page.wait_for_timeout(2000)

    _p(f"{log_prefix}选择 素材…", log_quiet=log_quiet)
    ok = False
    for attempt in range(1, 6):
        ok = await _click(page, ["素材", "广告素材"])
        if ok:
            _p(f"{log_prefix}素材 ✓ (第 {attempt} 次)", log_quiet=log_quiet)
            break
        await page.wait_for_timeout(800)
    if not ok:
        _p(f"{log_prefix}素材 ✗", log_quiet=log_quiet)
    await page.wait_for_timeout(2500)
    # ⚠️ 注意：不要在这里点「最新创意/展示估值」排序。
    # 实测在搜索框输入/回车后页面会自动切回「相关性」，
    # 所以排序必须在“每次触发搜索之后”再点一次（见 _search_one_keyword）。

    if popularity_option_text or use_popularity_top1:
        _p(
            f"{log_prefix}选择 Top 创意 下拉（option_text={popularity_option_text!r} 首项回退={use_popularity_top1}）",
            log_quiet=log_quiet,
        )
        p_ok = await _select_top_popularity_option(
            page, popularity_option_text, use_popularity_top1, log_prefix, log_quiet=log_quiet
        )
        _p(f"{log_prefix}Top 创意下拉 {'✓' if p_ok else '✗'}", log_quiet=log_quiet)
        await page.wait_for_timeout(2500)


async def _search_one_keyword(
    page,
    keyword: str,
    batches_ref: list,
    capture_state: dict,
    order_by: str = "exposure",
    log_prefix: str = "",
    max_scroll_rounds: int = 16,
    log_quiet: bool = False,
    *,
    stop_scroll_if_oldest_first_seen_before_ymd: str | None = None,
) -> None:
    """
    在当前已设置好筛选的页面上：清空 batches_ref，清空搜索框再填新关键字并搜索，
    等待 creative/list 接口返回（轮询 batches_ref 或最长 8 秒）。
    log_quiet: True 时少打搜索/排序过程日志（Arrow2 安静模式）。
    stop_scroll_if_oldest_first_seen_before_ymd: 若已合并的 napi 中最早 first_seen 北京日 **严格早于**
    该 ISO 日（通常为 filter 目标日），则不再向下滚动（更旧数据对「仅某日」无增量）。
    """
    # 重要：先关闭采集，避免把“搜索触发的相关性/其他请求”混进来
    capture_state["enabled"] = False
    batches_ref.clear()
    # 找搜索框（针对工具 Tab，根据你提供的 HTML 优先锁定 #rc_select_1）
    inp = None
    candidates = [
        "#display-search-input-container input#rc_select_1",
        "#display-search-input-container input[role='combobox']",
        "#display-search-input-container input.ant-select-selection-search-input",
        "input#display-search-input",
        "input[role='combobox']",
        "input.ant-select-selection-search-input",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel).first
            count = await loc.count()
            if count > 0:
                inp = loc
                _p(f"{log_prefix}命中搜索输入选择器: {sel} (count={count})", log_quiet=log_quiet)
                break
        except Exception:
            continue
    if inp is None:
        raise RuntimeError("未找到搜索输入框（combobox/ant-select-selection-search-input）")

    try:
        await inp.scroll_into_view_if_needed(timeout=2000)
    except Exception:
        pass

    # 有些情况下 input 被包在 ant-select 内部，需要点父级以激活
    try:
        parent = page.locator(
            "#display-search-input-container .ant-select-show-search, "
            "#display-search-input-container .ant-select-auto-complete"
        ).first
        if await parent.count() > 0:
            try:
                await parent.click(timeout=2000)
            except Exception:
                await parent.click(timeout=2000, force=True)
            await page.wait_for_timeout(300)
    except Exception:
        pass

    try:
        await inp.click(timeout=2000)
    except Exception:
        await inp.click(timeout=2000, force=True)
    await page.wait_for_timeout(200)
    # 先清空再填新关键词，优先用 Playwright 原生 fill/type，确保界面上可见输入
    try:
        await inp.fill("")
    except Exception:
        # 某些 rc-select 受控组件可能不允许 fill 清空，忽略错误
        pass
    await page.wait_for_timeout(200)
    try:
        await inp.fill(keyword)
    except Exception:
        # 回退到 type，逐字输入
        await inp.type(keyword, delay=50)
    await page.wait_for_timeout(500)
    btn = page.locator("#display-search-input-container button.bg-primary").first
    if await btn.count() > 0:
        await btn.click(timeout=2000)
    else:
        await page.keyboard.press("Enter")
    await page.wait_for_timeout(1000)

    # 搜索触发后，页面通常会自动切回「相关性」。
    # 因此必须在每次搜索后重新点击一次排序（最新创意/展示估值），并且“点完排序后再开启采集”。
    async def _is_order_selected(name: str) -> bool:
        """
        通过 operation.json 里记录的选中态 class（text-blue-600）判断排序是否选中。
        命中即认为“操作正确”，否则即使 click 成功也可能没生效（如被遮挡/点到别的元素）。
        """
        try:
            # 选中态：父 div 同时含 text-blue-600 且内部 span.text-sm 含对应文本
            loc = page.locator(
                f'div.flex.items-center.cursor-pointer:has(span.text-sm:has-text("{name}")).text-blue-600'
            ).first
            if await loc.count() > 0 and await loc.is_visible():
                return True
        except Exception:
            pass
        # 兜底：仅检测文本节点是否出现且周围有 text-blue-600
        try:
            loc2 = page.locator(f'text={name}').first
            if await loc2.count() > 0:
                # 往上找包含 text-blue-600 的祖先
                anc = loc2.locator("xpath=ancestor::div[contains(@class,'text-blue-600')][1]")
                if await anc.count() > 0:
                    return True
        except Exception:
            pass
        return False

    async def _click_order_once() -> bool:
        if order_by == "exposure":
            _p(f"{log_prefix}（搜索后）选择 展示估值…", log_quiet=log_quiet)
            ok = await _click(page, ["展示估值", "筛选"])
            _p(f"{log_prefix}（搜索后）展示估值 {'✓' if ok else '✗'}", log_quiet=log_quiet)
            return ok
        if order_by == "latest":
            _p(f"{log_prefix}（搜索后）选择 最新创意…", log_quiet=log_quiet)
            ok = await _click(page, ["最新创意", "筛选"])
            _p(f"{log_prefix}（搜索后）最新创意 {'✓' if ok else '✗'}", log_quiet=log_quiet)
            return ok
        return True

    # 关键：必须保证“排序触发的请求”被我们监听到。
    # 经验：请求可能在 click 的瞬间就发出，因此应该先开启采集，再 click 排序。
    ok = True
    if order_by in ("exposure", "latest"):
        # 恢复为：先开启采集，再点击排序，不额外长时间等待
        capture_state["enabled"] = True
        batches_ref.clear()
        ok = await _click_order_once()
        await page.wait_for_timeout(1200)

        # 校验排序确实已选中（避免“点了但没生效”）
        expected_name = "最新创意" if order_by == "latest" else "展示估值"
        for _ in range(6):
            if await _is_order_selected(expected_name):
                break
            await page.wait_for_timeout(300)

        # 等待接口返回：轮询直到 batches_ref 有数据或超时（最多约 12 秒）
        for _ in range(24):  # 12s
            if len(batches_ref) > 0:
                break
            await page.wait_for_timeout(500)

        # 若仍为空，再强制重试点击一次排序并延长等待
        if len(batches_ref) == 0:
            _p(
                f"{log_prefix}[提醒] 排序后仍未捕获到新返回，将重试点击一次排序…",
                log_quiet=log_quiet,
            )
            capture_state["enabled"] = False
            await _click_order_once()
            await page.wait_for_timeout(1500)
            capture_state["enabled"] = True
            batches_ref.clear()
            for _ in range(30):  # 15s
                if len(batches_ref) > 0:
                    break
                await page.wait_for_timeout(500)
        await page.wait_for_timeout(400)
    else:
        # 未指定排序时：搜索后立即开启采集（仍然避免采集“搜索前的请求”）
        batches_ref.clear()
        capture_state["enabled"] = True
        for _ in range(20):
            if len(batches_ref) > 0:
                break
            await page.wait_for_timeout(500)
        await page.wait_for_timeout(500)

    # 恢复滚动加载逻辑：向下滚动若干轮，尽量拿到更多素材
    thr = (stop_scroll_if_oldest_first_seen_before_ymd or "").strip()[:10]

    def _scroll_reached_before_target_day() -> tuple[bool, str | None]:
        if not thr:
            return False, None
        merged = [x for x in _all_creatives_from_batches(batches_ref) if isinstance(x, dict)]
        oldest = _oldest_first_seen_ymd_among_creatives(merged)
        if oldest and oldest < thr:
            return True, oldest
        return False, oldest

    hit_past, oldest0 = _scroll_reached_before_target_day()
    if hit_past:
        _p(
            f"{log_prefix}napi 中已出现 first_seen 早于目标日 {thr} 的素材（oldest={oldest0}），跳过向下滚动",
            log_quiet=log_quiet,
        )
    else:
        idle_rounds = 0
        last_batch_count = len(batches_ref)
        for _ in range(max(1, int(max_scroll_rounds))):
            try:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            except Exception:
                break
            await page.wait_for_timeout(1200)
            try:
                await page.wait_for_load_state("networkidle", timeout=3500)
            except Exception:
                pass
            await page.wait_for_timeout(400)

            if len(batches_ref) > last_batch_count:
                last_batch_count = len(batches_ref)
                idle_rounds = 0
            else:
                idle_rounds += 1
                if idle_rounds >= 3:
                    break

            if thr:
                hit_past2, oldest2 = _scroll_reached_before_target_day()
                if hit_past2:
                    _p(
                        f"{log_prefix}已滚到 first_seen 早于目标日 {thr} 的区间（oldest={oldest2}），提前结束滚动",
                        log_quiet=log_quiet,
                    )
                    break

    # 完成本关键词采集后，立刻关闭采集，避免泄漏到下一个关键词或页面的其他请求
    capture_state["enabled"] = False

    # 将回退信息塞进 batches_ref 的第 0 个元素上层不方便，这里用一个哨兵 dict 记录（不影响解析创意列表）
    # 仅用于 run_batch 内部调试/返回值标记
    try:
        batches_ref.append({"__meta__": {"order_by": order_by, "order_clicked": bool(ok)}})
    except Exception:
        pass


async def _extract_dom_cards(page, log_quiet: bool = False) -> list[dict]:
    """
    从页面 DOM 中提取所有可见创意卡片的基础信息。
    这些字段尽量与 Arrow2 当前入库口径对齐：`preview_img_url / advertiser_name /
    page_name / platform / video_duration / first_seen / created_at / last_seen /
    days_count / heat / all_exposure_value / impression / title / body / resource_urls`。
    若列表卡片只有日期区间标签，则先按北京自然日推导首末次时间；点开详情后再由 detail 精确覆盖。
    """
    try:
        cards = await page.evaluate(r"""
() => {
  const results = [];
  const toInt = (txt) => {
    const n = String(txt || '').replace(/[^0-9]/g, '');
    return n ? parseInt(n, 10) : 0;
  };
  const cardEls = document.querySelectorAll('.shadow-common-light.bg-white');
  cardEls.forEach((card, cardIdx) => {
    try {
      // 预览图：取所有 img，优先 sp_opera CDN，其次任意非 logo 图
      const allImgs = Array.from(card.querySelectorAll('img'));
      let previewSrc = '';
      const spImg = allImgs.find(img => img.src && img.src.includes('sp_opera'));
      if (spImg) {
        previewSrc = spImg.src.split('?')[0];
      } else {
        // 懒加载尚未触发时 src 为空，尝试 data-src 或 currentSrc
        const lazyImg = allImgs.find(img =>
          !img.src.includes('appcdn-global') && (img.dataset.src || img.currentSrc)
        );
        if (lazyImg) previewSrc = (lazyImg.dataset.src || lazyImg.currentSrc || '').split('?')[0];
      }

      // 广告主名称
      const advEl = card.querySelector('.leading-\\[18px\\] span span');
      const advertiserName = advEl ? advEl.textContent.trim() : '';

      // 平台
      const isYouTube = !!card.querySelector('.net-icon-youtube');
      const platform = isYouTube ? 'youtube' : 'admob';

      // 视频时长：找 "Xs" 文本（如 "0s" "15s"）
      let videoDuration = null;
      const playArea = card.querySelector('[class*="play-simple"]');
      if (playArea) {
        const txt = (playArea.parentElement || playArea).textContent.trim();
        const m = txt.match(/(\d+)s/);
        videoDuration = m ? parseInt(m[1]) : 0;
      }

      // 标签
      const tagEls = Array.from(card.querySelectorAll('.ant-tag'));
      const tags = tagEls.map(t => t.textContent.trim());
      const dateRange = tags.find(t => t.includes('~') || t.includes('～')) || '';
      const isRelaunch = tags.some(t => t === '重投');

      // 标题/文案：列表层只取可见文本，详情再覆盖
      const titleLike =
        (card.querySelector('.line-clamp-2')?.textContent || '').trim()
        || (card.querySelector('.line-clamp-1')?.textContent || '').trim()
        || '';

      // 指标值（列表通常为 估值/投放天数/最后看见）
      const metricBolds = Array.from(card.querySelectorAll('.font-semibold')).map(el => el.textContent.trim());
      const impression = metricBolds[0] ? toInt(metricBolds[0]) : 0;
      const daysCount = metricBolds[1] ? toInt(metricBolds[1]) : 0;

      // 展示估值/热度标签
      const smallTags = Array.from(card.querySelectorAll('.rounded-full')).map(el => el.textContent.trim());
      let heat = 0, allExposure = 0;
      smallTags.forEach(t => {
        const hm = t.match(/热度[:：]\s*([\d.]+)([KkMm万]?)/);
        if (hm) {
          const v = parseFloat(hm[1]);
          const u = hm[2].toUpperCase();
          heat = u === 'K' ? Math.round(v * 1000) : u === 'M' ? Math.round(v * 1000000) : v;
        }
        const em = t.match(/展示估值[:：]\s*([\d.]+)([KkMm万]?)/);
        if (em) {
          const v = parseFloat(em[1]);
          const u = em[2].toUpperCase();
          allExposure = u === 'K' ? Math.round(v * 1000) : u === 'M' ? Math.round(v * 1000000) : v;
        }
      });

      // 页面广告主（卡片下方的广告主文本行，可能与顶部不同）
      const bottomAdvEls = card.querySelectorAll('.text-xs .whitespace-nowrap span span');
      const bottomAdv = bottomAdvEls.length > 0 ? bottomAdvEls[bottomAdvEls.length - 1].textContent.trim() : advertiserName;

      // 始终推入——用 _dom_idx 保证即使 preview 为空也能区分每张卡片
      results.push({
        _source: 'dom',
        _dom_idx: cardIdx,
        preview_img_url: previewSrc,
        advertiser_name: advertiserName || bottomAdv,
        page_name: bottomAdv,
        platform: platform,
        title: titleLike,
        body: '',
        video_duration: videoDuration,
        days_count: daysCount,
        heat: heat,
        all_exposure_value: allExposure,
        impression: impression,
        resume_advertising_flag: isRelaunch,
        date_range_text: dateRange,
        resource_urls: [],
      });
    } catch (e) {}
  });
  return results;
}
""")
        if not isinstance(cards, list):
            return []
        out: list[dict] = []
        for raw in cards:
            if not isinstance(raw, dict):
                continue
            item = dict(raw)
            start_ymd, end_ymd = _parse_dom_date_range_text(item.get("date_range_text"))
            fs = item.get("first_seen")
            cr = item.get("created_at")
            ls = item.get("last_seen")
            if fs is None and start_ymd:
                item["first_seen"] = _beijing_unix_sec_from_ymd(start_ymd)
                item["_dom_first_seen_derived"] = True
            if cr is None and start_ymd:
                item["created_at"] = _beijing_unix_sec_from_ymd(start_ymd)
            if ls is None and end_ymd:
                item["last_seen"] = _beijing_unix_sec_from_ymd(end_ymd, end_of_day=True)
                item["_dom_last_seen_derived"] = True
            item.setdefault("resource_urls", [])
            item.setdefault("title", "")
            item.setdefault("body", "")
            item.setdefault("video_duration", 0)
            item.setdefault("days_count", 0)
            out.append(item)
        return out
    except Exception as e:
        if not log_quiet:
            print(f"    [DOM补充] 提取失败: {e}", file=sys.stderr)
        return []


async def _click_cards_for_details(
    page,
    known_ad_keys: set,
    max_cards: int = 80,
    target_previews: set | None = None,
    napi_rows_by_preview: dict[str, list[dict]] | None = None,
    *,
    log_quiet: bool = False,
    stop_after_detail_first_seen_before_ymd: str | None = None,
) -> list[dict]:
    """
    逐一点击页面上的创意卡片，拦截详情响应，提取完整的 creative 数据。
    target_previews: 若指定，只点击 preview_img_url 匹配的卡片；否则点击所有。
    stop_after_detail_first_seen_before_ymd: 当详情 JSON 中 first_seen 的北京日**严格早于**该 ISO
    日时（与「仅 target 日」一致），在关闭弹层后**不再**点击后续卡片（从页首向下逐张点，命中更旧即停）。
    """
    enriched: list[dict] = []
    detail_holder: list[dict] = []
    try:
        probe_first = int((os.environ.get("ARROW2_DEBUG_DOM_PROBE_FIRST") or "").strip() or "0")
    except Exception:
        probe_first = 0
    probe_only = _env_truthy("ARROW2_DEBUG_DOM_PROBE_ONLY", default=False)
    probe_mode = probe_first > 0
    log_each_click = (
        probe_mode
        or _env_truthy("ARROW2_LOG_EACH_CLICK", default=False)
        or _env_truthy("DEBUG", default=False)
    )

    async def _card_debug_meta(idx: int) -> dict[str, str]:
        try:
            meta = await page.evaluate(f"""
() => {{
  const cards = document.querySelectorAll('.shadow-common-light.bg-white');
  const card = cards[{idx}];
  if (!card) return {{}};
  const txt = (sel) => {{
    const el = card.querySelector(sel);
    return el ? (el.textContent || '').trim() : '';
  }};
  const imgs = Array.from(card.querySelectorAll('img'));
  const preview = imgs.find(img => img.src && img.src.includes('sp_opera'));
  const titleLike = txt('.line-clamp-2') || txt('.line-clamp-1') || txt('.font-semibold') || '';
  return {{
    advertiser: txt('.leading-\\\\[18px\\\\] span span') || txt('.text-xs .whitespace-nowrap span span'),
    title: titleLike,
    preview: preview ? preview.src.split('?')[0] : '',
  }};
}}
""")
            return meta if isinstance(meta, dict) else {}
        except Exception:
            return {}

    async def _on_detail_response(response):
        url = response.url or ""
        if "guangdada" not in url:
            return
        if response.status != 200:
            return
        _p(f"      [detail请求] {url[:120]}", log_quiet=log_quiet)
        try:
            body = await response.json()
        except Exception:
            return
        lists = _extract_creative_lists(body)
        for lst in lists:
            if isinstance(lst, list):
                for c in lst:
                    if isinstance(c, dict) and c.get("ad_key"):
                        detail_holder.append(c)

    page.on("response", _on_detail_response)

    try:
        # 获取所有卡片 index（只取前 max_cards 张）
        card_count = await page.evaluate(
            "() => document.querySelectorAll('.shadow-common-light.bg-white').length"
        )
        card_count = min(int(card_count or 0), max_cards)
        if probe_first > 0:
            card_count = min(card_count, probe_first)
        _p(
            f"    [点击详情] 页面卡片总数={card_count}，最多点击 {card_count} 张",
            log_quiet=log_quiet,
        )

        for idx in range(card_count):
            detail_holder.clear()
            try:
                meta = await _card_debug_meta(idx) if log_each_click else {}
                # 若指定了 target_previews，先检查该卡片的 preview 是否在目标集内
                if target_previews:
                    card_preview = await page.evaluate(f"""
() => {{
  const cards = document.querySelectorAll('.shadow-common-light.bg-white');
  const card = cards[{idx}];
  if (!card) return '';
  const imgs = card.querySelectorAll('img');
  const preview = Array.from(imgs).find(img => img.src && img.src.includes('sp_opera'));
  return preview ? preview.src.split('?')[0] : '';
}}
""")
                    if not card_preview or card_preview not in target_previews:
                        continue
                else:
                    card_preview = str(meta.get("preview") or "")

                # 优先走主动 detail-v2：若当前卡片可用 preview 对到列表 napi 的 ad_key/search_flag，
                # 直接请求 detail-v2，不再依赖 card.click() 是否恰好触发详情接口。
                active_detail_rows: list[dict] = []
                if napi_rows_by_preview and card_preview:
                    cand_rows = napi_rows_by_preview.get(card_preview) or []
                    if cand_rows:
                        for cand in cand_rows[:3]:
                            body = await _fetch_detail_v2_best_attempt(page, cand)
                            if not isinstance(body, dict):
                                continue
                            got = _detail_rows_from_body(body.get("body") if "body" in body else body)
                            best = _pick_best_detail_row(got, cand)
                            if isinstance(best, dict):
                                active_detail_rows.append(best)
                        if active_detail_rows:
                            detail_holder[:] = active_detail_rows
                            if log_each_click:
                                rows = []
                                for c0 in active_detail_rows:
                                    ak0 = str(c0.get("ad_key") or "")[:16]
                                    d0 = _beijing_ymd_from_first_seen(c0.get("first_seen")) or "?"
                                    dt0 = _beijing_dt_from_unix_sec(c0.get("first_seen")) or "?"
                                    adv0 = str(c0.get("advertiser_name") or c0.get("page_name") or "")[:40]
                                    rows.append(f"{ak0}@{d0} {dt0}:{adv0}")
                                print(
                                    f"    [detail-v2] 第 {idx + 1}/{card_count} 张主动拉取成功: {rows if rows else '[空]'}",
                                    flush=True,
                                )
                            if probe_first > 0:
                                _print_dom_detail_probe(
                                    idx,
                                    meta,
                                    [x for x in active_detail_rows if isinstance(x, dict)],
                                )

                # 重新查询防止 DOM 变动导致 stale；单张最多重试 2 次，避免“页面上有但本次没点开”。
                clicked = False
                for click_attempt in range(1, 3):
                    if detail_holder:
                        break
                    detail_holder.clear()
                    if log_each_click:
                        adv = str(meta.get("advertiser") or "")[:50]
                        title = str(meta.get("title") or "")[:60]
                        print(
                            f"    [点击详情] 第 {idx + 1}/{card_count} 张，第 {click_attempt} 次 "
                            f"广告主={adv!r} 标题={title!r}",
                            flush=True,
                        )
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
                        break

                    # 等待详情响应（最多 4s）
                    for _ in range(16):
                        if detail_holder:
                            break
                        await page.wait_for_timeout(250)
                    if detail_holder:
                        if log_each_click:
                            rows = []
                            for c0 in detail_holder:
                                if not isinstance(c0, dict):
                                    continue
                                ak0 = str(c0.get("ad_key") or "")[:16]
                                d0 = _beijing_ymd_from_first_seen(c0.get("first_seen")) or "?"
                                dt0 = _beijing_dt_from_unix_sec(c0.get("first_seen")) or "?"
                                adv0 = str(c0.get("advertiser_name") or c0.get("page_name") or "")[:40]
                                rows.append(f"{ak0}@{d0} {dt0}:{adv0}")
                            print(
                                f"    [点击详情] 第 {idx + 1}/{card_count} 张拿到 detail: {rows if rows else '[空]'}",
                                flush=True,
                            )
                        if probe_first > 0:
                            _print_dom_detail_probe(idx, meta, [x for x in detail_holder if isinstance(x, dict)])
                        break
                    if click_attempt < 2:
                        _p(
                            f"    [点击详情] 第 {idx + 1} 张首击未拿到 detail，重试一次…",
                            log_quiet=log_quiet,
                        )
                        await page.wait_for_timeout(300)
                if detail_holder:
                    clicked = True
                if not clicked:
                    if log_each_click:
                        print(f"    [点击详情] 第 {idx + 1}/{card_count} 张 click 失败，跳过", flush=True)
                    if probe_mode:
                        _print_dom_detail_probe(idx, meta, [])
                    continue
                if clicked and not detail_holder:
                    if log_each_click:
                        print(f"    [点击详情] 第 {idx + 1}/{card_count} 张两次都没拿到 detail", flush=True)
                    if probe_mode:
                        _print_dom_detail_probe(idx, meta, [])

                # 把新的 creative 收集起来
                for c in detail_holder:
                    ak = str(c.get("ad_key") or "")
                    if ak and ak not in known_ad_keys:
                        known_ad_keys.add(ak)
                        c["_source"] = "dom_detail"
                        enriched.append(c)

                # 关闭详情弹窗（Escape 或点关闭按钮）
                try:
                    close_btn = page.locator(
                        'button[aria-label="Close"], .ant-modal-close, [class*="close"]'
                    ).first
                    if await close_btn.count() > 0:
                        await close_btn.click(timeout=800)
                    else:
                        await page.keyboard.press("Escape")
                except Exception:
                    await page.keyboard.press("Escape")

                await page.wait_for_timeout(400)

                if stop_after_detail_first_seen_before_ymd:
                    st0 = (stop_after_detail_first_seen_before_ymd or "").strip()[:10]
                    if st0:
                        for c0 in detail_holder:
                            d0 = _beijing_ymd_from_first_seen(c0.get("first_seen"))
                            if d0 and d0 < st0:
                                _p(
                                    f"    [点击详情] 本张 first_seen={d0} 早于目标日 {st0}，不再继续点卡",
                                    log_quiet=log_quiet,
                                )
                                return enriched
                if probe_only and probe_first > 0:
                    print("    [DOM探针] probe_only=1，本次只点设定的探针卡片后返回", flush=True)
                    return enriched

            except Exception as e:
                # 单张失败不中断整体
                await page.keyboard.press("Escape")
                await page.wait_for_timeout(300)
                if log_each_click:
                    print(f"    [点击详情] 第 {idx + 1}/{card_count} 张异常跳过: {e}", flush=True)
                if probe_mode:
                    _print_dom_detail_probe(idx, meta if 'meta' in locals() else {}, [])
                continue

    finally:
        page.remove_listener("response", _on_detail_response)

    _p(
        f"    [点击详情] 新增完整素材 {len(enriched)} 条（_source=dom_detail）",
        log_quiet=log_quiet,
    )
    return enriched


def _merge_dom_into_napi(napi_creatives: list[dict], dom_cards: list[dict]) -> list[dict]:
    """
    将 DOM 卡片补充进 napi 结果：
    - 以 preview_img_url 作为去重 key
    - napi 已有的跳过（napi 数据更完整）
    - DOM-only 的卡片追加在末尾，带 _source="dom" 标记
    """
    napi_imgs = {
        str(c.get("preview_img_url") or "").split("?")[0]
        for c in napi_creatives
        if c.get("preview_img_url")
    }
    added = 0
    merged = list(napi_creatives)
    for card in dom_cards:
        img = str(card.get("preview_img_url") or "").split("?")[0]
        if img and img in napi_imgs:
            continue
        merged.append(card)
        if img:
            napi_imgs.add(img)
        added += 1
    if added:
        print(f"    [DOM补充] 新增 {added} 条（napi 未返回）")
    return merged


def _top_creatives_from_batches(batches: list) -> tuple:
    """
    从多批 creative/list 合并去重后，直接取「前 3 条」作为 top_creatives。

    说明：
    - 你希望 top3 与网页展示顺序一致，因此不再按 heat/days 做人为排序。
    - 在取 top3 前会过滤掉明显的赞助广告（原始文案含 Sponsored/赞助 等）。
    返回 (top_creatives, total_count)。
    """
    all_creatives = _all_creatives_from_batches(batches)
    top_creatives = all_creatives[:3] if all_creatives else []
    return top_creatives, len(all_creatives)


def _all_creatives_from_batches(batches: list) -> list:
    """从多批 creative/list 合并去重，返回完整素材列表。"""
    all_creatives = []
    seen_ids = set()

    def is_sponsored(c: dict) -> bool:
        try:
            # 1) 文案/标题中出现 Sponsored/赞助：最强信号，直接过滤
            def _norm(s):
                return str(s or "").strip().lower()

            txt = " ".join(
                [
                    _norm(c.get("title")),
                    _norm(c.get("body")),
                    _norm(c.get("message")),
                    _norm(c.get("page_name")),
                ]
            )
            if "sponsored" in txt or "赞助广告" in txt or "赞助" in txt:
                return True

            # 2) 兼容一些可能的结构字段（作为补充信号）
            ads_type = c.get("ads_type")
            if str(ads_type) == "1":
                return True
            if c.get("is_sponsored") in (1, True, "1", "true", "True"):
                return True
            if c.get("sponsored") in (1, True, "1", "true", "True"):
                return True
            if c.get("is_promoted") in (1, True, "1", "true", "True"):
                return True
            if c.get("promoted") in (1, True, "1", "true", "True"):
                return True
        except Exception:
            return False
        return False

    for b in batches:
        # 跳过 meta 哨兵
        if isinstance(b, dict) and b.get("__meta__"):
            continue
        for c in b:
            if not isinstance(c, dict) or is_sponsored(c):
                continue
            cid = (
                c.get("ad_key")
                or c.get("creative_id")
                or c.get("id")
                or c.get("creativeId")
            )
            if cid:
                if cid in seen_ids:
                    continue
                seen_ids.add(cid)
            all_creatives.append(c)
    return all_creatives


def _sort_creatives_latest_first(creatives: list[dict]) -> list[dict]:
    """
    将素材按“最新”倒序排序（更贴近页面上的「最新创意」）。
    优先 first_seen，其次 created_at；两者均缺失则排在最后。
    """

    def _ts(c: dict) -> int:
        v = c.get("first_seen")
        if v is None:
            v = c.get("created_at")
        try:
            return int(v) if v is not None else 0
        except Exception:
            return 0

    # Python sort 稳定：相同时间戳保持原顺序
    return sorted(creatives, key=_ts, reverse=True)


async def _collect_keyword_crawl_result(
    page,
    keyword: str,
    batches_ref: list,
    capture_state: dict,
    order_by: str = "exposure",
    log_prefix: str = "    ",
    max_scroll_rounds: int = 16,
    enable_dom_track: bool = False,
    log_quiet: bool = False,
) -> dict:
    """单关键词：搜索、滚动、合并 napi 结果（与 `run_batch` 单轮逻辑一致，供 Arrow2 复用）。"""
    result_for_kw: dict | None = None
    for attempt in range(1, 3):
        try:
            await _search_one_keyword(
                page,
                keyword,
                batches_ref,
                capture_state,
                order_by=order_by,
                log_prefix=log_prefix,
                max_scroll_rounds=max_scroll_rounds,
                log_quiet=log_quiet,
            )
            top_creatives, total = _top_creatives_from_batches(batches_ref)
            napi_creatives = _all_creatives_from_batches(batches_ref)
            all_creatives = list(napi_creatives)

            dom_creatives: list[dict] = []
            if enable_dom_track:
                dom_cards = await _extract_dom_cards(page, log_quiet=log_quiet)
                napi_preview_set = {
                    str(c.get("preview_img_url") or "").split("?")[0]
                    for c in napi_creatives if c.get("preview_img_url")
                }
                napi_preview_set_nonempty = {
                    str(c.get("preview_img_url") or "").split("?")[0]
                    for c in napi_creatives if c.get("preview_img_url")
                }
                dom_only_cards: list[dict] = []
                for c in dom_cards:
                    img = str(c.get("preview_img_url") or "").split("?")[0]
                    if img and img in napi_preview_set_nonempty:
                        continue
                    dom_only_cards.append(c)

                known_keys = {str(c.get("ad_key") or "") for c in napi_creatives if c.get("ad_key")}
                dom_preview_set = {
                    str(c.get("preview_img_url") or "").split("?")[0]
                    for c in dom_only_cards if c.get("preview_img_url")
                }
                detail_creatives = await _click_cards_for_details(
                    page,
                    known_keys,
                    max_cards=len(dom_only_cards) + 5,
                    target_previews=dom_preview_set or None,
                    log_quiet=log_quiet,
                )
                detail_by_preview = {
                    str(c.get("preview_img_url") or "").split("?")[0]: c
                    for c in detail_creatives
                    if c.get("preview_img_url")
                }
                seen_dom: set[str] = set()
                for card in dom_only_cards:
                    img = str(card.get("preview_img_url") or "").split("?")[0]
                    key = img if img else f"_idx_{card.get('_dom_idx', id(card))}"
                    if key in seen_dom:
                        continue
                    seen_dom.add(key)
                    dom_creatives.append(detail_by_preview.get(img, card) if img else card)
                for c in detail_creatives:
                    img = str(c.get("preview_img_url") or "").split("?")[0]
                    if img and img not in seen_dom:
                        dom_creatives.append(c)
                        seen_dom.add(img)
                _p(
                    f"{log_prefix}[DOM track] dom_basic={len(dom_only_cards)}  "
                    f"dom_detail={len(detail_creatives)}  最终={len(dom_creatives)}",
                    log_quiet=log_quiet,
                )

            if order_by == "latest" and all_creatives:
                all_creatives = _sort_creatives_latest_first(all_creatives)
            try:
                tz = timezone(timedelta(hours=8))

                def _ts2(c: dict) -> int | None:
                    v = c.get("first_seen")
                    if v is None:
                        v = c.get("created_at")
                    try:
                        return int(v) if v is not None else None
                    except Exception:
                        return None

                head = all_creatives[:3] if isinstance(all_creatives, list) else []
                times: list[str] = []
                for c in head:
                    if not isinstance(c, dict):
                        continue
                    tsv = _ts2(c)
                    if tsv is None:
                        continue
                    times.append(datetime.fromtimestamp(tsv, tz=tz).strftime("%Y-%m-%d %H:%M:%S"))
                if times:
                    _p(f"{log_prefix}[校验] 前3条时间(UTC+8): {times}", log_quiet=log_quiet)
            except Exception:
                pass
            best = top_creatives[0] if top_creatives else None
            result_for_kw = {
                "keyword": keyword,
                "selected": best,
                "top_creatives": top_creatives,
                "all_creatives": all_creatives,
                "napi_creatives": napi_creatives,
                "dom_creatives": dom_creatives,
                "total_captured": total,
            }
            if all_creatives:
                break
            if attempt == 1:
                if not log_quiet:
                    print(
                        "    [提示] 当前未捕获到素材（all_creatives 为空），准备重试一次...",
                        file=sys.stderr,
                    )
                continue
        except Exception as e:
            if attempt == 1:
                if not log_quiet:
                    print(f"    [失败] {e}，准备重试一次...", file=sys.stderr)
                continue
            print(f"    [失败] {e}", file=sys.stderr)
            break
    if result_for_kw is None:
        return {
            "keyword": keyword,
            "selected": None,
            "top_creatives": [],
            "all_creatives": [],
            "napi_creatives": [],
            "dom_creatives": [],
            "total_captured": 0,
        }
    if not result_for_kw.get("all_creatives") and not log_quiet:
        print(
            "    [提醒] 两次尝试后仍未捕获到素材（all_creatives 为空），"
            "请检查页面结构或筛选条件是否变化。",
            file=sys.stderr,
        )
    return result_for_kw


def _merge_prefer_dom_detail(
    napi_creatives: list[dict], detail_rows: list[dict]
) -> list[dict]:
    """以 detail 补全同 ad_key 的 napi 行；表现指标口径与展示估值工作流保持一致。"""

    def _to_int_or_none(v: object) -> int | None:
        try:
            return int(v)  # type: ignore[arg-type]
        except Exception:
            return None

    def _merge_detail_with_fallback(detail_row: dict, napi_row: dict | None) -> dict:
        merged = dict(napi_row or {})
        merged.update(detail_row or {})
        merged.setdefault("_source", "dom_detail")

        # 与 exposure_top10/展示估值工作流入库口径对齐：
        # 表现类指标优先取列表 napi 行（该链路一直以 napi creative 直写 DB）。
        numeric_fields = (
            "impression",
            "all_exposure_value",
            "heat",
            "days_count",
            "new_week_exposure_value",
        )
        for field in numeric_fields:
            nv = _to_int_or_none((napi_row or {}).get(field)) if napi_row else None
            dv = _to_int_or_none((detail_row or {}).get(field))
            if nv is not None:
                merged[field] = nv
            elif dv is not None:
                merged[field] = dv

        for field in (
            "preview_img_url",
            "platform",
            "advertiser_name",
            "page_name",
            "title",
            "body",
            "resource_urls",
            "video_duration",
        ):
            if (not merged.get(field)) and napi_row and napi_row.get(field):
                merged[field] = napi_row.get(field)
        return merged

    by_ak: dict[str, dict] = {}
    for c in napi_creatives:
        if not isinstance(c, dict):
            continue
        ak = str(c.get("ad_key") or "")
        if ak:
            by_ak[ak] = c
    for d in detail_rows:
        if not isinstance(d, dict):
            continue
        ak = str(d.get("ad_key") or "")
        if ak:
            by_ak[ak] = _merge_detail_with_fallback(d, by_ak.get(ak))
    return list(by_ak.values()) if by_ak else [x for x in napi_creatives if isinstance(x, dict)]


def _merge_dom_cards_with_details(dom_cards: list[dict], detail_rows: list[dict]) -> list[dict]:
    """以当前页面 DOM 卡片为底，按 preview_img_url 尽量用 detail 行覆盖；不以 napi 作为主结果源。"""
    detail_by_preview: dict[str, dict] = {}
    extra_details: list[dict] = []
    for d in detail_rows:
        if not isinstance(d, dict):
            continue
        prev = str(d.get("preview_img_url") or "").split("?")[0]
        if prev:
            detail_by_preview[prev] = d
        else:
            extra_details.append(d)

    merged: list[dict] = []
    seen_keys: set[str] = set()
    for card in dom_cards:
        if not isinstance(card, dict):
            continue
        prev = str(card.get("preview_img_url") or "").split("?")[0]
        key = prev or f"_dom_idx_{card.get('_dom_idx', id(card))}"
        if key in seen_keys:
            continue
        seen_keys.add(key)
        detail = detail_by_preview.get(prev) if prev else None
        if detail:
            item = dict(card)
            item.update(detail)
            item["_source"] = "dom_detail"
            merged.append(item)
        else:
            item = dict(card)
            item.setdefault("_source", "dom")
            merged.append(item)

    seen_ad_keys = {
        str(x.get("ad_key") or "")
        for x in merged
        if isinstance(x, dict) and x.get("ad_key")
    }
    for d in detail_rows:
        if not isinstance(d, dict):
            continue
        ak = str(d.get("ad_key") or "")
        if ak and ak in seen_ad_keys:
            continue
        prev = str(d.get("preview_img_url") or "").split("?")[0]
        if prev and prev in seen_keys:
            continue
        d2 = dict(d)
        d2.setdefault("_source", "dom_detail")
        merged.append(d2)
        if ak:
            seen_ad_keys.add(ak)
        if prev:
            seen_keys.add(prev)
    return merged


async def _collect_keyword_crawl_result_arrow2_latest_dom(
    page,
    keyword: str,
    batches_ref: list,
    capture_state: dict,
    log_prefix: str = "    ",
    max_scroll_rounds: int = 48,
    log_quiet: bool = False,
    *,
    first_seen_target_ymd: str | None = None,
) -> dict:
    """
    Arrow2 每日「最新」主路径：输入 appid → 点「最新创意」→ 滚动 napi 加载 →
    在页面上能看到的卡片上逐张点击，以 detail 接口全量补全；最终结果以 DOM 卡片/详情为主，
    不以 napi creative_list 作为主结果源。若点卡 0 条，则回退为纯 napi 列表。
    first_seen_target_ymd: 用于点卡阶段判断“本张是否早于目标日”；命中后停止继续点后续卡片。
    """
    ymd_for_stop: str | None = None
    if first_seen_target_ymd and _env_truthy("ARROW2_FIRST_SEEN_EARLY_STOP", default=True):
        y = (str(first_seen_target_ymd).strip() or "")[:10]
        ymd_for_stop = y if y else None

    result_for_kw: dict | None = None
    for attempt in range(1, 3):
        try:
            await _search_one_keyword(
                page,
                keyword,
                batches_ref,
                capture_state,
                order_by="latest",
                log_prefix=log_prefix,
                max_scroll_rounds=max_scroll_rounds,
                log_quiet=log_quiet,
                stop_scroll_if_oldest_first_seen_before_ymd=None,
            )
            napi_creatives = [x for x in _all_creatives_from_batches(batches_ref) if isinstance(x, dict)]
            dom_cards = await _extract_dom_cards(page, log_quiet=log_quiet)
            napi_rows_by_preview: dict[str, list[dict]] = {}
            for row in napi_creatives:
                if not isinstance(row, dict):
                    continue
                prev = str(row.get("preview_img_url") or "").split("?")[0]
                if not prev:
                    continue
                napi_rows_by_preview.setdefault(prev, []).append(row)
            try:
                raw_max_dc = (os.environ.get("ARROW2_DOM_CLICK_MAX_CARDS") or "").strip()
                max_dc = int(raw_max_dc) if raw_max_dc else 0
            except Exception:
                max_dc = 0
            known: set = set()
            details = await _click_cards_for_details(
                page,
                known,
                # 未显式设 cap 时，默认把当前 DOM 中已加载的卡片都点完；
                # 需要限条时再通过 ARROW2_DOM_CLICK_MAX_CARDS 传正整数。
                max_cards=(max(1, max_dc) if max_dc > 0 else 10**9),
                target_previews=None,
                napi_rows_by_preview=napi_rows_by_preview or None,
                log_quiet=log_quiet,
                stop_after_detail_first_seen_before_ymd=ymd_for_stop,
            )
            merged = _merge_dom_cards_with_details(
                [x for x in dom_cards if isinstance(x, dict)],
                [x for x in details if isinstance(x, dict)],
            )
            if not merged and napi_creatives:
                merged = napi_creatives
            merged = _sort_creatives_latest_first(merged) if merged else []
            top_creatives, total = (merged[:3] if merged else []), len(merged)
            best = top_creatives[0] if top_creatives else None
            result_for_kw = {
                "keyword": keyword,
                "selected": best,
                "top_creatives": top_creatives,
                "all_creatives": merged,
                "napi_creatives": napi_creatives,
                "dom_cards": dom_cards,
                "dom_creatives": [d for d in (details or []) if isinstance(d, dict)],
                "total_captured": total,
            }
            if merged:
                break
            if attempt == 1 and not log_quiet:
                print(
                    f"{log_prefix}[arrow2] 点卡+napi 全空，1 次重试…",
                    file=sys.stderr,
                )
        except Exception as e:
            if attempt == 1:
                if not log_quiet:
                    print(f"{log_prefix}[失败] {e}，重试…", file=sys.stderr)
                continue
            print(f"{log_prefix}[失败] {e}", file=sys.stderr)
            break
    if result_for_kw is None:
        return {
            "keyword": keyword,
            "selected": None,
            "top_creatives": [],
            "all_creatives": [],
            "napi_creatives": [],
            "dom_creatives": [],
            "total_captured": 0,
        }
    if not (result_for_kw.get("all_creatives") or []) and not log_quiet:
        print(f"{log_prefix}[提醒] 本词 napi 抓取后仍无素材。", file=sys.stderr)
    return result_for_kw


async def run_batch(
    keywords: list,
    debug: bool = False,
    is_tool: bool = False,
    order_by: str = "exposure",
    use_popularity_top1: bool = False,
    enable_dom_track: bool = False,
) -> list:
    """
    登录一次、界面设置一次（工具/7天/素材/排序方式），然后对每个关键词只做「填关键字 → 搜索 → 取结果」。
    返回与 run() 相同结构的列表，每项对应一个关键词：
      {
        "keyword", "selected", "top_creatives", "all_creatives", "total_captured"
      }。
    order_by: "exposure"（展示估值）或 "latest"（最新创意）。
    enable_dom_track: 是否启用 DOM 补充 + 点击详情（默认关闭，仅供 fetch_competitor_raw 等调试脚本使用）。
    """
    if not keywords:
        return []
    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not email or not password:
        print("[错误] 请在 .env 中设置 GUANGDADA_EMAIL 和 GUANGDADA_PASSWORD", file=sys.stderr)
        sys.exit(1)

    playwright_proxy = prepare_playwright_proxy_for_crawl()

    batches_ref = []
    capture_state = {"enabled": False}

    async def on_response(response):
        """
        监听创意列表接口，把最近几次返回的 creative_list 追加到 batches_ref。
        Top创意 / 人气值Top1% 下，接口路径和 data 结构可能有变化，因此这里只要是 guangdada 的 napi，
        就在整个 JSON 里递归查找「包含 ad_key/creative_id 等字段的列表」，作为创意列表。
        """
        if not capture_state.get("enabled"):
            return
        url = response.url
        if "guangdada.net/napi" not in url or response.status != 200:
            return
        try:
            body = await response.json()
        except Exception:
            return
        # 在整个 JSON 中递归查找「创意列表」
        lists = _extract_creative_lists(body)
        if not lists:
            return

        # 可能一次响应里包含多个“创意列表”（不同模块/组件），这里全部收集，后续再去重
        for lst in lists:
            if isinstance(lst, list) and lst:
                batches_ref.append(lst)
        # 只保留最近几批，避免无限增长
        # 这里上限不要太小，否则滚动加载会被截断，只剩很少条素材
        if len(batches_ref) > 80:
            batches_ref.pop(0)

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
        page.on("response", on_response)

        try:
            print("[1/4] 正在登录...")
            if not await login(page, email, password):
                print("[失败] 登录失败", file=sys.stderr)
                sys.exit(2)
            print("[1/4] 登录成功 ✓")
            print("[1/4] 主站稳定中（SPA 可能不触发 networkidle，数秒内继续）…", flush=True)
            await _await_post_login_shell(page)

            print("[2/4] 一次性设置筛选（7天 / 素材 / 排序方式 / 可选人气值Top1%）...")
            await _do_setup(
                page,
                is_tool,
                log_prefix="  ",
                order_by=order_by,
                use_popularity_top1=use_popularity_top1,
            )
            print("[2/4] 设置完成 ✓")

            print("[3/4] 按关键词依次搜索并拉取数据...")
            results = []
            for i, keyword in enumerate(keywords, 1):
                print(f"  [{i}/{len(keywords)}] {keyword}")
                result_for_kw = await _collect_keyword_crawl_result(
                    page,
                    keyword,
                    batches_ref,
                    capture_state,
                    order_by=order_by,
                    log_prefix="    ",
                    max_scroll_rounds=16,
                    enable_dom_track=enable_dom_track,
                )
                results.append(result_for_kw)
            print("[3/4] 全部关键词搜索完成 ✓")
            print("[4/4] 关闭浏览器")
            return results
        finally:
            await browser.close()


async def run(keyword: str, debug: bool = False, is_tool: bool = False):
    """单关键词入口：内部走 run_batch([keyword])，保持返回格式与 keyword_result.json 兼容。"""
    print(f"关键词: {keyword}")
    results = await run_batch([keyword], debug=debug, is_tool=is_tool)
    result = results[0] if results else {
        "keyword": keyword,
        "selected": None,
        "top_creatives": [],
        "total_captured": 0,
    }
    out_file = OUT_DIR / "keyword_result.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"[结果] 已写入 {out_file.name}")
    best_creative = result.get("selected")
    top_creatives = result.get("top_creatives") or []
    if best_creative:
        name = best_creative.get("title") or best_creative.get("app_name") or "N/A"
        days = best_creative.get("days_count", "?")
        heat = best_creative.get("heat", "?")
        print(f"  - 热度最高的素材: {name[:50]}")
        print(f"    投放天数: {days} 天, 热度: {heat}")
        if len(top_creatives) > 1:
            print(f"  - 共选出热度前 {len(top_creatives)} 条素材")
    else:
        print("  - 未捕获到素材")
    print("\n" + json.dumps({
        "keyword": result["keyword"],
        "total_captured": result["total_captured"],
        "selected_title": best_creative.get("title") if best_creative else None,
    }, ensure_ascii=False))
    return result


async def run_arrow2_batch(  # noqa: PLR0912,PLR0915
    keywords: list,
    debug: bool = False,
    is_tool: bool = False,
    is_game: bool = True,
    day_spans: list | None = None,
    order_modes: list | None = None,
    popularity_option_text: str | None = None,
    ad_channel_labels: list | None = None,
    country_codes: list | None = None,
    pull_specs: list | None = None,
    pull_spec_defaults: dict | None = None,
    search_tab: str = "game",
    keyword_appid: dict | None = None,  # noqa: ARG001
    debug_step_per_product: bool = False,
    keyword_product: dict[str, str] | None = None,
    target_date_first_seen: str | None = None,
    debug_pause_per_product: bool = False,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    """
    Arrow2 竞品拉取：按 pull_specs 设置筛选，再对每词拉取结果。

    - `latest_yesterday`（及「latest + 仅 first_seen 日」）走 **国家 → 7 天 → 素材 → 搜 appid → 最新创意 →
      滚列表 → 当前页面卡片逐张点击拿 detail**，再后处理：仅保留 `first_seen` 为指定日且广告主与产品一致。
    - 其它类（如展示估值+Top%）走 napi 滚动为主；`_do_setup` 会尝试勾选 **Facebook系 / Google系** 等渠道并点选国家。

    终端输出：默认 **安静**（少打搜索/排序日志）；latest+点卡时会打印一行 DOM 点卡摘要，随后打印 `print_arrow2_matched_creatives`。
    设环境变量 **`ARROW2_VERBOSE=1`** 可恢复详细日志（与旧行为接近）。
    """
    _ = (
        is_game,
        keyword_appid,
        kwargs,
    )  # 保留与调用方签名兼容
    if not keywords:
        return []
    _email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    _password = os.getenv("GUANGDADA_PASSWORD")
    if not _email or not _password:
        print("[错误] 请在 .env 中设置 GUANGDADA_EMAIL 和 GUANGDADA_PASSWORD", file=sys.stderr)
        sys.exit(1)

    defaults: dict[str, Any] = dict(pull_spec_defaults) if pull_spec_defaults else {}
    base_specs: list[dict[str, Any]] = []
    if pull_specs and len(pull_specs) > 0:
        for p in pull_specs:
            if isinstance(p, dict):
                merged: dict[str, Any] = {**defaults, **p}
                base_specs.append(merged)
    if not base_specs:
        dlist = [str(d) for d in (day_spans or ["7"])]
        olist = [str(x) for x in (order_modes or ["exposure"])]
        for ds in dlist:
            for ob in olist:
                base_specs.append(
                    {
                        **defaults,
                        "id": f"matrix_{ds}_{ob}",
                        "day_span": ds,
                        "order_by": ob,
                        "popularity_option_text": popularity_option_text,
                    }
                )
    if not base_specs:
        base_specs = [
            {**defaults, "id": "default", "day_span": "7", "order_by": "exposure", "popularity_option_text": popularity_option_text}
        ]

    tz8 = timezone(timedelta(hours=8))
    first_seen_ymd = (target_date_first_seen or (os.environ.get("TARGET_DATE") or "").strip()[:10] or None)
    if not first_seen_ymd:
        first_seen_ymd = (datetime.now(tz8).date() - timedelta(days=1)).isoformat()

    playwright_proxy = prepare_playwright_proxy_for_crawl()
    out: list[dict[str, Any]] = []
    log_quiet = not _env_truthy("ARROW2_VERBOSE", default=False)

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
        batches_ref: list = []
        capture_state: dict = {"enabled": False}

        async def on_response(response):
            if not capture_state.get("enabled"):
                return
            if "guangdada.net/napi" not in response.url or response.status != 200:
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

        page.on("response", on_response)

        try:
            _p("[arrow2 1/4] 正在登录…", log_quiet=log_quiet)
            if not await login(page, _email, _password):
                print("[失败] 登录失败", file=sys.stderr)
                sys.exit(2)
            print(
                "[arrow2] 主站稳定中，即将切 Tab/设国家与筛选项（若久无新行，多为网络idle在等，已缩短等待）…",
                flush=True,
            )
            await _await_post_login_shell(page)
            await _try_click_search_tab(page, search_tab, log_quiet=log_quiet)
            is_tool_effective = bool(is_tool)

            for spec in base_specs:
                ds = str(spec.get("day_span") or "7")
                ob = str(spec.get("order_by") or "exposure")
                popt = spec.get("popularity_option_text")
                if popt is None and popularity_option_text:
                    popt = popularity_option_text
                use_pfirst = bool(spec.get("popularity_pick_first", False))
                use_latest_dom = (str(spec.get("id") or "") == "latest_yesterday") or (
                    str(ob) == "latest" and bool(spec.get("filter_yesterday_only"))
                )
                await _do_setup(
                    page,
                    is_tool_effective,
                    log_prefix="  [arrow2] ",
                    order_by=ob,
                    use_popularity_top1=use_pfirst and not popt,
                    day_span=ds,
                    popularity_option_text=(str(popt).strip() if popt else None) if popt else None,
                    ad_channel_labels=ad_channel_labels,
                    country_codes=country_codes,
                    log_quiet=log_quiet,
                )
                for ki, keyword in enumerate(keywords, 1):
                    if not (keyword and str(keyword).strip()):
                        continue
                    kw = str(keyword).strip()
                    _p(
                        f"  [arrow2 pull={spec.get('id')!r} {ki}/{len(keywords)}] {kw}",
                        log_quiet=log_quiet,
                    )
                    do_scroll = bool(
                        spec.get("filter_yesterday_only")
                        and spec.get("scroll_until_past_target_date", True) is not False
                    )
                    max_rounds = 56 if do_scroll else 16
                    if use_latest_dom and ob == "latest":
                        fs_stop = first_seen_ymd if spec.get("filter_yesterday_only") else None
                        r = await _collect_keyword_crawl_result_arrow2_latest_dom(
                            page,
                            kw,
                            batches_ref,
                            capture_state,
                            log_prefix="    ",
                            max_scroll_rounds=max_rounds,
                            log_quiet=log_quiet,
                            first_seen_target_ymd=fs_stop,
                        )
                        list_tag = "dom_detail+dom"
                    else:
                        r = await _collect_keyword_crawl_result(
                            page,
                            kw,
                            batches_ref,
                            capture_state,
                            order_by=ob,
                            log_prefix="    ",
                            max_scroll_rounds=max_rounds,
                            enable_dom_track=False,
                            log_quiet=log_quiet,
                        )
                        list_tag = "napi"
                    raw_all: list[dict] = [x for x in (r.get("all_creatives") or []) if isinstance(x, dict)]
                    debug_stage = _env_truthy("DEBUG", default=False) and bool(spec.get("filter_yesterday_only"))
                    if debug_stage:
                        adv_stage, time_stage = _arrow2_filter_stage_views(
                            raw_all, spec, kw, keyword_product, first_seen_ymd
                        )
                        print_arrow2_filter_stage_creatives(
                            kw,
                            "广告主匹配后（未按时间筛）",
                            adv_stage,
                        )
                        print_arrow2_filter_stage_creatives(
                            kw,
                            f"按 first_seen={first_seen_ymd} 筛后",
                            time_stage,
                        )
                    filtered = _arrow2_apply_post_filters(
                        raw_all, spec, kw, keyword_product, first_seen_ymd
                    )
                    if ob == "latest" and filtered and not spec.get("filter_yesterday_only"):
                        filtered = _sort_creatives_latest_first(filtered)
                    f_top = filtered[:3]
                    f_best = f_top[0] if f_top else None
                    r["all_creatives"] = filtered
                    r["top_creatives"] = f_top
                    r["selected"] = f_best
                    r["total_captured"] = len(filtered)
                    r["day_span"] = ds
                    r["order_by"] = ob
                    r["pull_id"] = spec.get("id")
                    r["pull_spec"] = spec
                    r["list_source"] = list_tag
                    if log_quiet and use_latest_dom and ob == "latest":
                        n_dom = len(r.get("dom_creatives") or [])
                        print(
                            f"[arrow2] DOM: 已对页面卡片逐张 click，"
                            f"详情接口补全 {n_dom} 条（_source=dom_detail）"
                        )
                    print_arrow2_matched_creatives(kw, filtered)
                    if debug_step_per_product:
                        try:
                            n_show = int(os.getenv("ARROW2_DEBUG_STEP_CARDS") or "10")
                        except Exception:
                            n_show = 10
                        _print_debug_step_cards(kw, filtered, n_show)
                        if os.environ.get("ARROW2_DEBUG_STEP_AUTO_ENTER", "").lower() in (
                            "1",
                            "true",
                            "y",
                        ):
                            pass
                        else:
                            _ = input("[debug-step] 按 Enter 进入下一词… ")
                    if debug_pause_per_product:
                        _print_pause_yesterday_summary(filtered)
                        _ = input("[arrow2] 按 Enter 进入下一产品… ")

                    out.append(r)
            if debug and _env_truthy(
                "ARROW2_DEBUG_PAUSE_AT_END", default=(not debug_step_per_product)
            ):
                _ = input("[arrow2] 关浏览器前暂停，按 Enter 继续… ")
        finally:
            await browser.close()

    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("keyword", nargs="*", help="搜索关键词（支持空格，如: puzzle game）")
    parser.add_argument("--debug", action="store_true", help="显示浏览器")
    parser.add_argument("--tool", action="store_true", help="切换到工具标签（用于搜索工具类产品）")
    args = parser.parse_args()
    keyword = " ".join(args.keyword).strip() if args.keyword else input("请输入关键词: ").strip()
    if not keyword:
        print("错误: 需要关键词", file=sys.stderr)
        sys.exit(1)
    asyncio.run(run(keyword, debug=args.debug, is_tool=args.tool))


if __name__ == "__main__":
    main()
