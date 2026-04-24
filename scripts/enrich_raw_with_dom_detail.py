"""
对 raw JSON 中「列表层无 video_url」的素材，按 appid 打开广大大素材页，
通过 DOM 点击详情接口，把详情里的 video_url / resource_urls / source_url 等合并回 creative。

典型场景：video2pic=1，列表无直链，详情可补 TikTok 等 source_url（或偶发补全 video_url）。

匹配策略：
- 列表卡片刻有 **日期区间**（`.ant-tag` 如 `2026-04-01 ~ 2026-04-07`，无需点开详情），与 raw 的 `target_date` 对齐时优先缩小候选。
- 再尝试 **ad_key**：先 **属性值完全等于** ad_key，再 **outerHTML 子串**。
- 最后 **preview_img_url** 与卡片预览图对齐。之后滚动、翻页（最多 8 页）。

用法（项目根）：
  .venv/bin/python scripts/enrich_raw_with_dom_detail.py --date 2026-04-07
  .venv/bin/python scripts/enrich_raw_with_dom_detail.py --raw data/workflow_video_enhancer_2026-04-07_raw.json

输出：
  data/<prefix>_raw_dom_enriched.json
  data/<prefix>_dom_enrich_report.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

sys.path.insert(0, str(Path(__file__).resolve().parent))

from analyze_video_from_raw_json import _pick_video_url
from guangdada_login import login
from path_util import DATA_DIR
from proxy_util import prepare_playwright_proxy_for_crawl

from run_search_workflow import (
    _do_setup,
    _extract_creative_lists,
    _extract_dom_cards,
    _search_one_keyword,
)
from test_dom_video_url_ai_mirror import (
    _click_card_detail_raw,
    _find_dom_idx_by_ad_key,
    _norm_preview,
)


async def _count_dom_cards(page) -> int:
    try:
        n = await page.evaluate(
            "() => document.querySelectorAll('.shadow-common-light.bg-white').length"
        )
        return int(n) if n is not None else 0
    except Exception:
        return 0


async def _scroll_page_and_list_parent(page) -> None:
    """窗口滚到底 + 尝试滚动卡片所在的可滚动父容器（部分布局下列表不在 window 上滚）。"""
    try:
        await page.evaluate(
            r"""() => {
  const card = document.querySelector('.shadow-common-light.bg-white');
  if (card) {
    let el = card.parentElement;
    for (let i = 0; i < 14 && el; i++) {
      const st = window.getComputedStyle(el);
      const oy = st.overflowY;
      if ((oy === 'auto' || oy === 'scroll') && el.scrollHeight > el.clientHeight + 20) {
        el.scrollTo(0, el.scrollHeight);
        break;
      }
      el = el.parentElement;
    }
  }
  window.scrollTo(0, document.body.scrollHeight);
  const last = document.querySelectorAll('.shadow-common-light.bg-white');
  if (last.length) last[last.length - 1].scrollIntoView({ block: 'end' });
}"""
        )
    except Exception:
        try:
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            pass


async def _scroll_dom_until_stable(
    page,
    *,
    max_rounds: int = 48,
    idle_needed: int = 4,
    log_prefix: str = "",
) -> int:
    """
    仅根据 DOM 卡片数量判断是否还有新卡加载（与 napi batches 无关）。
    返回最终卡片数。
    """
    idle = 0
    last = await _count_dom_cards(page)
    for r in range(max_rounds):
        await _scroll_page_and_list_parent(page)
        await page.wait_for_timeout(900)
        try:
            await page.wait_for_load_state("networkidle", timeout=2800)
        except Exception:
            pass
        await page.wait_for_timeout(350)
        cur = await _count_dom_cards(page)
        if cur > last:
            if log_prefix:
                print(f"{log_prefix}[滚动加载] 卡片 {last} → {cur}")
            last = cur
            idle = 0
        else:
            idle += 1
            if idle >= idle_needed:
                break
    return last


def _resolve_target_date_str(payload: dict[str, Any], args: argparse.Namespace) -> str:
    t = str(payload.get("target_date") or "").strip()
    if t:
        return t[:10]
    d = getattr(args, "date", None)
    if d:
        return str(d)[:10]
    return ""


def _parse_range_dates(date_range_text: str) -> tuple[date | None, date | None]:
    """解析列表标签里的 `YYYY-MM-DD ~ YYYY-MM-DD`。"""
    if not date_range_text or "~" not in date_range_text and "～" not in date_range_text:
        return None, None
    parts = re.split(r"[~～]", date_range_text, maxsplit=1)
    if len(parts) < 2:
        return None, None

    def _one(s: str) -> date | None:
        s = re.sub(r"[^\d\-/\.]", "", s.strip())[:10]
        if not s:
            return None
        s = s.replace("/", "-").replace(".", "-")
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except Exception:
            return None

    a, b = _one(parts[0]), _one(parts[1])
    return a, b


def _allowed_indices_for_target_date(
    dom_cards: list[dict[str, Any]], target_date_str: str
) -> list[int] | None:
    """
    缩小候选：target_date 落在卡片标签区间内的卡；**无日期标签/无法解析**的卡也保留（避免误杀）。
    若至少有一张卡解析出区间且**全部**不含 target，返回 None（退回全表）。
    """
    if not target_date_str:
        return None
    try:
        td = datetime.strptime(target_date_str[:10], "%Y-%m-%d").date()
    except Exception:
        return None
    in_range: list[int] = []
    unknown: list[int] = []
    parsed_but_miss: list[int] = []
    for c in dom_cards:
        idx = int(c.get("_dom_idx", -1))
        if idx < 0:
            continue
        dr = str(c.get("date_range_text") or "").strip()
        lo, hi = _parse_range_dates(dr)
        if lo is None or hi is None:
            unknown.append(idx)
            continue
        a, b = (lo, hi) if lo <= hi else (hi, lo)
        if a <= td <= b:
            in_range.append(idx)
        else:
            parsed_but_miss.append(idx)
    if in_range:
        return sorted(set(in_range + unknown))
    if parsed_but_miss and not unknown:
        return None
    return None


def _dom_cards_to_preview_map(dom_cards: list[dict[str, Any]]) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    for c in dom_cards:
        idx = int(c.get("_dom_idx", -1))
        if idx < 0:
            continue
        out.append((idx, str(c.get("preview_img_url") or "")))
    return sorted(out, key=lambda x: x[0])


def _filter_dom_cards_by_indices(
    dom_cards: list[dict[str, Any]], indices: list[int] | None
) -> list[dict[str, Any]]:
    if not indices:
        return dom_cards
    allow = set(indices)
    return [c for c in dom_cards if int(c.get("_dom_idx", -1)) in allow]


def _match_preview_to_dom_idx(
    dom_map: list[tuple[int, str]], norm_prev: str, tail: str
) -> int | None:
    prev_to_idx: dict[str, int] = {}
    for idx, src in dom_map:
        if src:
            prev_to_idx[_norm_preview(src)] = idx
    dom_idx = prev_to_idx.get(norm_prev)
    if dom_idx is None and tail:
        for idx, src in dom_map:
            if tail and tail in (src or ""):
                dom_idx = idx
                break
    return dom_idx


async def _try_click_next_page(page) -> bool:
    """Ant Design 分页「下一页」；已禁用则返回 False。"""
    selectors = [
        "li.ant-pagination-next:not(.ant-pagination-disabled)",
        ".ant-pagination-next:not(.ant-pagination-disabled) .ant-pagination-item-link",
        "button[aria-label='Next Page']",
        "button[title='Next Page']",
        ".ant-pagination .ant-pagination-next:not(.ant-pagination-disabled) button",
    ]
    for sel in selectors:
        loc = page.locator(sel).first
        try:
            if await loc.count() == 0:
                continue
            cls = await loc.get_attribute("class") or ""
            aria_d = await loc.get_attribute("aria-disabled") or ""
            if "disabled" in cls or aria_d == "true":
                continue
            await loc.scroll_into_view_if_needed(timeout=2000)
            await loc.click(timeout=2500)
            await page.wait_for_timeout(1200)
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass
            await page.wait_for_timeout(400)
            return True
        except Exception:
            continue
    return False


def _needs_dom_enrich(creative: dict[str, Any]) -> bool:
    if (_pick_video_url(creative) or "").strip():
        return False
    if creative.get("_dom_detail_enriched"):
        return False
    return True


def _merge_detail_into_creative(creative: dict[str, Any], detail: dict[str, Any] | None) -> list[str]:
    """将详情字段合并入列表 creative（不覆盖已有非空直链）。返回本次写入的字段名列表。"""
    added: list[str] = []
    if not detail:
        return added

    if not str(creative.get("video_url") or "").strip():
        dv = str(detail.get("video_url") or "").strip()
        if dv:
            creative["video_url"] = dv
            added.append("video_url")

    ru = creative.get("resource_urls")
    if not ru and detail.get("resource_urls"):
        creative["resource_urls"] = detail["resource_urls"]
        added.append("resource_urls")

    if not str(creative.get("source_url") or "").strip():
        su = str(detail.get("source_url") or "").strip()
        if su:
            creative["source_url"] = su
            added.append("source_url")

    if not str(creative.get("landing_url") or "").strip():
        lu = str(detail.get("landing_url") or "").strip()
        if lu:
            creative["landing_url"] = lu
            added.append("landing_url")

    if not str(creative.get("page_url") or "").strip():
        pu = str(detail.get("page_url") or "").strip()
        if pu:
            creative["page_url"] = pu
            added.append("page_url")

    creative["_dom_detail_enriched"] = True
    creative["_dom_enriched_at"] = datetime.now().isoformat(timespec="seconds")
    added.append("_dom_detail_enriched")
    return added


def _write_dom_enrich_outputs(
    raw_path: Path,
    payload: dict[str, Any],
    items: list[dict[str, Any]],
    target_date_str: str,
    report: dict[str, Any],
    meta_note: str | None = None,
) -> None:
    stem = raw_path.stem
    out_prefix = stem[: -len("_raw")] if stem.endswith("_raw") else stem
    out_raw = DATA_DIR / f"{out_prefix}_raw_dom_enriched.json"
    out_report = DATA_DIR / f"{out_prefix}_dom_enrich_report.json"
    payload["items"] = items
    note = meta_note or "无 video_url：列表日期标签→ad_key(属性优先)→预览图；再滚动翻页"
    payload["dom_enrich_meta"] = {
        "enriched_at": datetime.now().isoformat(timespec="seconds"),
        "source_file": raw_path.name,
        "target_date": target_date_str,
        "note": note,
    }
    out_raw.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    out_report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[输出] {out_raw.name}")
    print(f"[输出] {out_report.name}")


async def _run(args: argparse.Namespace) -> None:
    if args.raw:
        raw_path = Path(args.raw)
    else:
        raw_path = DATA_DIR / f"workflow_video_enhancer_{args.date}_raw.json"
    if not raw_path.exists():
        print(f"[错误] 找不到 raw: {raw_path}", file=sys.stderr)
        sys.exit(1)

    payload = json.loads(raw_path.read_text(encoding="utf-8"))
    items: list[dict[str, Any]] = list(payload.get("items") or [])
    target_date_str = _resolve_target_date_str(payload, args)

    pending: list[dict[str, Any]] = []
    for it in items:
        c = it.get("creative") if isinstance(it, dict) else None
        if not isinstance(c, dict):
            continue
        if _needs_dom_enrich(c):
            pending.append(it)

    if args.limit > 0:
        pending = pending[: args.limit]

    by_app: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for it in pending:
        aid = str((it.get("appid") or "")).strip()
        if aid:
            by_app[aid].append(it)

    print(
        f"[计划] raw={raw_path.name} target_date={target_date_str or '(未指定)'} "
        f"待补全（无 video_url）共 {len(pending)} 条，"
        f"涉及 {len(by_app)} 个 appid：{sorted(by_app.keys())}"
    )
    if args.dry_run:
        print("[dry-run] 不启动浏览器")
        return

    if not pending:
        print("[dom-enrich] 无待补全项（均有 video_url 或已标 _dom_detail_enriched），不启动浏览器")
        _write_dom_enrich_outputs(
            raw_path,
            payload,
            items,
            target_date_str,
            {
                "source_raw": str(raw_path),
                "target_date": target_date_str,
                "pending_total": 0,
                "results": [],
                "note": "nothing_to_enrich",
            },
            meta_note="无待补全项，raw 原样写出供主流程合并",
        )
        return

    email = os.getenv("GUANGDADA_EMAIL") or os.getenv("GUANGDADA_USERNAME")
    password = os.getenv("GUANGDADA_PASSWORD")
    if not email or not password:
        print("[错误] 请配置 GUANGDADA_EMAIL / GUANGDADA_PASSWORD", file=sys.stderr)
        sys.exit(1)

    report: dict[str, Any] = {
        "source_raw": str(raw_path),
        "target_date": target_date_str,
        "pending_total": len(pending),
        "results": [],
    }

    batches_ref: list = []
    capture_state: dict[str, Any] = {"enabled": False}

    async def on_response(response):
        if not capture_state.get("enabled"):
            return
        url = response.url or ""
        if "guangdada.net/napi" not in url or response.status != 200:
            return
        try:
            body = await response.json()
        except Exception:
            return
        for lst in _extract_creative_lists(body):
            if isinstance(lst, list) and lst:
                batches_ref.append(lst)
        if len(batches_ref) > 80:
            batches_ref.pop(0)

    playwright_proxy = prepare_playwright_proxy_for_crawl()
    debug = bool(os.environ.get("DEBUG"))

    async with async_playwright() as p:
        launch_kw: dict[str, Any] = {"headless": not debug}
        if playwright_proxy:
            launch_kw["proxy"] = playwright_proxy
        browser = await p.chromium.launch(**launch_kw)
        context = await browser.new_context(
            viewport={"width": 1400, "height": 900},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = await context.new_page()
        page.on("response", on_response)

        try:
            print("[1/2] 登录...")
            if not await login(page, email, password):
                print("[失败] 登录", file=sys.stderr)
                sys.exit(2)
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            await page.wait_for_timeout(2500)

            print("[2/2] 筛选：工具 / 7天 / 素材 / 最新创意")
            await _do_setup(page, is_tool=True, log_prefix="  ", order_by="latest", use_popularity_top1=False)

            for appid in sorted(by_app.keys()):
                group = by_app[appid]
                print(f"\n[appid] {appid} 待处理 {len(group)} 条")
                await _search_one_keyword(
                    page,
                    appid,
                    batches_ref,
                    capture_state,
                    order_by="latest",
                    log_prefix="    ",
                    search_select_appid=appid,
                )
                # 搜索后仍以 DOM 卡片数为准再滚一轮（napi 停止加载 ≠ 列表已滚完）
                await _scroll_dom_until_stable(page, log_prefix="    ")

                remaining: list[dict[str, Any]] = list(group)
                max_page_turns = 8
                page_turn = 0

                while remaining and page_turn < max_page_turns:
                    await _scroll_dom_until_stable(page, log_prefix="    ")
                    scroll_idle = 0
                    last_n = await _count_dom_cards(page)

                    while True:
                        dom_cards = await _extract_dom_cards(page)
                        allowed = _allowed_indices_for_target_date(dom_cards, target_date_str)

                        for it in list(remaining):
                            c = it.get("creative") or {}
                            ak = str(c.get("ad_key") or "").strip()
                            prev = _norm_preview(str(c.get("preview_img_url") or ""))
                            tail = prev.split("/")[-1][:24] if prev else ""
                            rec: dict[str, Any] = {
                                "ad_key": ak,
                                "appid": appid,
                                "status": "fail",
                                "added": [],
                            }

                            if not ak:
                                rec["reason"] = "no_ad_key"
                                report["results"].append(rec)
                                print(f"    [跳过] 无 ad_key")
                                remaining.remove(it)
                                continue

                            if allowed is None:
                                dom_idx, ak_how = await _find_dom_idx_by_ad_key(
                                    page, ak, allowed_indices=None
                                )
                            else:
                                dom_idx, ak_how = await _find_dom_idx_by_ad_key(
                                    page, ak, allowed_indices=allowed
                                )
                                if dom_idx is None:
                                    dom_idx, ak_how = await _find_dom_idx_by_ad_key(
                                        page, ak, allowed_indices=None
                                    )

                            match_note = ""
                            if dom_idx is None and prev:
                                pool = (
                                    _filter_dom_cards_by_indices(dom_cards, allowed)
                                    if allowed
                                    else dom_cards
                                )
                                dom_map = _dom_cards_to_preview_map(pool)
                                dom_idx = _match_preview_to_dom_idx(dom_map, prev, tail)
                                if dom_idx is not None:
                                    match_note = "preview"
                                if dom_idx is None and allowed:
                                    dom_idx = _match_preview_to_dom_idx(
                                        _dom_cards_to_preview_map(dom_cards), prev, tail
                                    )
                                    if dom_idx is not None:
                                        match_note = "preview_fullpage"

                            if dom_idx is None:
                                continue

                            tag_parts: list[str] = []
                            if ak_how:
                                tag_parts.append(f"ad_key={ak_how}")
                            if match_note:
                                tag_parts.append(match_note)
                            if allowed is not None:
                                tag_parts.append(f"date_idx={len(allowed)}")
                            tag = " ".join(tag_parts)
                            print(f"    [点击] dom_idx={dom_idx} ad_key={ak[:12]}… {tag}".strip())
                            detail = await _click_card_detail_raw(page, dom_idx, target_ad_key=ak)
                            if not detail:
                                rec["reason"] = "no_detail_json"
                                report["results"].append(rec)
                                print("      → 未拿到详情 JSON")
                                remaining.remove(it)
                                continue

                            added = _merge_detail_into_creative(c, detail)
                            rec["status"] = "ok"
                            rec["added"] = added
                            report["results"].append(rec)
                            print(f"      → 已合并字段: {added}")
                            remaining.remove(it)

                        if not remaining:
                            break

                        await _scroll_page_and_list_parent(page)
                        await page.wait_for_timeout(700)
                        try:
                            await page.wait_for_load_state("networkidle", timeout=2200)
                        except Exception:
                            pass
                        await page.wait_for_timeout(300)
                        cur_n = await _count_dom_cards(page)
                        if cur_n > last_n:
                            last_n = cur_n
                            scroll_idle = 0
                            continue
                        scroll_idle += 1
                        if scroll_idle >= 4:
                            break

                    if not remaining:
                        break

                    if not await _try_click_next_page(page):
                        print("    [翻页] 无下一页或已到底，停止翻页")
                        break
                    page_turn += 1
                    print(f"    [翻页] 已切到第 {page_turn + 1} 页，剩余 {len(remaining)} 条待匹配")

                for it in remaining:
                    c = it.get("creative") or {}
                    ak = str(c.get("ad_key") or "").strip()
                    prev = _norm_preview(str(c.get("preview_img_url") or ""))
                    rec: dict[str, Any] = {"ad_key": ak, "appid": appid, "status": "fail", "added": []}
                    if not ak:
                        rec["reason"] = "no_ad_key"
                    else:
                        rec["reason"] = "card_not_on_dom"
                    report["results"].append(rec)
                    hint = "无 ad_key" if not ak else "未匹配（ad_key 属性/HTML、日期范围、预览图）"
                    print(f"    [跳过] {ak[:12] if ak else '?'}… {hint}（已滚动+翻页）")

        finally:
            await browser.close()

    _write_dom_enrich_outputs(raw_path, payload, items, target_date_str, report)


def main() -> None:
    ap = argparse.ArgumentParser(description="DOM 详情补全 raw 中无 video_url 的 creative")
    ap.add_argument("--date", default=date.today().isoformat(), help="与 workflow 文件名中的日期一致")
    ap.add_argument("--raw", default="", help="直接指定 raw JSON 路径（优先于 --date）")
    ap.add_argument("--limit", type=int, default=0, help="最多处理几条（0=不限制）")
    ap.add_argument("--dry-run", action="store_true", help="只统计待处理条数，不跑浏览器")
    args = ap.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
