"""
Arrow2 竞品追踪：搜索框优先填「包名 appid」；无 appid 时填 keyword。

若配置含 pull_specs：按「类」顺序拉取（默认两类：7 天最新+仅昨日 first_seen；30 天展示估值+Top10%），
可在每条 pull_spec 或配置根设 max_creatives_per_keyword，限制每个搜索词保留条数（展示估值类常用）；环境变量 ARROW2_MAX_CREATIVES_PER_KEYWORD 可作全局默认。
不再做 30×90× 四排序全矩阵。若无 pull_specs，则回退为 day_spans × order_modes 旧矩阵。

多轮结果写入 items；dedupe **全局按 ad_key**（同一素材跨轮次/跨搜索词/跨 pull 只留一行，合并 seen_in_runs）；
无 ad_key 的以「__no_ad_key__:下标」为键、互不去重。

配置：config/arrow2_competitor.json
- search_tab：`game`、`tool`，或 `playable` / `playable_ads`（先点侧栏「试玩广告」再按游戏侧筛选）
- products：keyword / match / appid
- pull_specs（推荐）：每类含 day_span、order_by、popularity_option_text 或 popularity_pick_first、filter_yesterday_only
- filters：ad_channels（常规视频/图片）、ad_channels_playable（试玩）、国家（Top 创意由 pull_specs 分轮控制）
- 旧版：day_spans、order_modes、filters.popularity_option_text

用法（项目根目录，建议虚拟环境）：
  ./scripts/daily_arrow2_workflow.sh all
  ./scripts/daily_arrow2_workflow.sh latest_yesterday
  ./scripts/daily_arrow2_workflow.sh exposure_top10
  DEBUG=1 .venv/bin/python scripts/test_arrow2_competitors.py --pull-only latest_yesterday
  .venv/bin/python scripts/test_arrow2_competitors.py --debug --pull-only latest_yesterday
  .venv/bin/python scripts/test_arrow2_competitors.py --debug-step-products --pull-only exposure_top10
    # 每产品（搜索词）10 条（可调 ARROW2_DEBUG_STEP_CARDS）打印摘要后 Enter 下一词；与 workflow 同一爬取逻辑
  DEBUG=1 .venv/bin/python scripts/test_arrow2_competitors.py --products "Arrow Flow" --search-tab playable --pull-only exposure_top10
  .venv/bin/python scripts/test_arrow2_competitors.py --products "Arrow Flow"

可视化流程：`--debug` 或 `DEBUG=1`（有头浏览器）；地区补全前默认不暂停，需见 `ARROW2_DEBUG_PAUSE=1`；
补全后会打印 [地区快照]（含 appid 与返回 JSON 的 country），再可暂停见 `ARROW2_DEBUG_PAUSE_AFTER_GEO`；
结束前会再等一次 Enter 才关浏览器见 `ARROW2_DEBUG_PAUSE_AT_END`（默认开）。
多行命令时注释单独成行，勿在 `\\` 续行紧下一行写 `#`，否则 zsh 会报 `command not found: #`。
地区 DOM 未全满时会多轮点卡直到补全或达 `ARROW2_ENRICH_DETAIL_COUNTRY_DOM_MAX_ROUNDS`。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from path_util import CONFIG_DIR, DATA_DIR
from arrow2_pipeline_db import arrow2_creative_ad_key, dedupe_arrow2_raw_items_by_ad_key
from run_search_workflow import run_arrow2_batch
from workflow_guangdada_competitor_yesterday_creatives import advertiser_matches_product


CONFIG_FILE = CONFIG_DIR / "arrow2_competitor.json"


def _arrow2_search_tab_label(tab: str) -> str:
    t = (tab or "").strip().lower()
    if t in ("playable", "playable_ads"):
        return "试玩广告"
    if t == "game":
        return "游戏"
    if t == "tool":
        return "工具"
    return t or "默认"


def _beijing_today_iso() -> str:
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz).date().isoformat()


def _target_date_from_env() -> str:
    v = (os.environ.get("TARGET_DATE") or "").strip()
    if v:
        return v[:10]
    tz = timezone(timedelta(hours=8))
    return (datetime.now(tz).date() - timedelta(days=1)).isoformat()


@dataclass(frozen=True)
class Arrow2Entry:
    """keyword：产品展示名/搜索词兜底；product：广告主模糊匹配用名；appid：包名（非空时优先填入搜索框）。"""
    keyword: str
    product: str
    appid: str = ""


def _arrow2_search_query(e: Arrow2Entry) -> str:
    """广大大搜索框实际输入：有 appid 用包名，否则用 keyword。"""
    a = (e.appid or "").strip()
    return a if a else (e.keyword or "").strip()


def _load_config() -> dict[str, Any]:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"未找到配置：{CONFIG_FILE}")
    return json.load(CONFIG_FILE.open("r", encoding="utf-8"))


def _load_entries(cfg: dict[str, Any], names: list[str] | None) -> list[Arrow2Entry]:
    wanted = {n.strip().lower() for n in names} if names else None
    out: list[Arrow2Entry] = []
    raw = cfg.get("products")

    if isinstance(raw, list):
        for x in raw:
            if isinstance(x, dict):
                k = str(x.get("keyword") or "").strip()
                m = str(x.get("match") or x.get("product") or k).strip()
                aid = str(x.get("appid") or "").strip()
            else:
                k = str(x).strip()
                m = k
                aid = ""
            if not k:
                continue
            if wanted is not None:
                aid_l = aid.lower() if aid else ""
                hit = (
                    k.lower() in wanted
                    or m.lower() in wanted
                    or (aid_l and aid_l in wanted)
                )
                if not hit:
                    continue
            out.append(Arrow2Entry(keyword=k, product=m, appid=aid))
    elif isinstance(raw, dict):
        for k, v in raw.items():
            name = str(k).strip()
            if not name:
                continue
            aid = ""
            if isinstance(v, dict):
                aid = str(v.get("appid") or "").strip()
            aid_l = aid.lower() if aid else ""
            if wanted is not None:
                hit = name.lower() in wanted or (aid_l and aid_l in wanted)
                if not hit:
                    continue
            out.append(Arrow2Entry(keyword=name, product=name, appid=aid))
    return out


def _reduce_creative(creative: dict) -> dict:
    def _pick_media_link(c: dict) -> str:
        for r in c.get("resource_urls") or []:
            if not isinstance(r, dict):
                continue
            if r.get("video_url"):
                return str(r["video_url"])
            if r.get("image_url"):
                return str(r["image_url"])
        if c.get("video_url"):
            return str(c["video_url"])
        return ""

    tags = creative.get("pipeline_tags")
    tag_list = list(tags) if isinstance(tags, list) else []
    return {
        "展示估值": creative.get("impression") or 0,
        "人气值": creative.get("all_exposure_value") or 0,
        "热度": creative.get("heat") or 0,
        "视频长度": creative.get("video_duration") or 0,
        "素材链接": _pick_media_link(creative),
        "标签": tag_list,
        "广告主": str(creative.get("advertiser_name") or creative.get("page_name") or ""),
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="Arrow2：游戏 Tab + 30/90 天 × 4 排序 + 渠道 + 国家")
    parser.add_argument(
        "--products",
        default="",
        help="只跑这些产品（逗号分隔）：与 config 中 keyword、match/product 或 appid 任一匹配；"
        "搜索框优先填 appid（包名），无 appid 则用 keyword",
    )
    parser.add_argument(
        "--output-prefix",
        default="",
        help="输出文件前缀（默认 workflow_arrow2_sample）",
    )
    parser.add_argument(
        "--pull-only",
        default="",
        help="仅执行 pull_specs 里指定 id（逗号分隔），如 latest_yesterday 或 exposure_top10；默认跑配置中的全部类",
    )
    parser.add_argument(
        "--wipe-db",
        action="store_true",
        help="开始前清空当前 ARROW2_SQLITE_PATH 库中两表全部行（默认 data/arrow2_pipeline.db；测试可设 ARROW2_SQLITE_PATH 指向测试库）",
    )
    parser.add_argument(
        "--all-products",
        action="store_true",
        help="跑 config 中全部竞品；未加本参数且未传 --products 时只跑配置中第一个产品。",
    )
    parser.add_argument(
        "--search-tab",
        default="",
        help="覆盖配置 search_tab：game / tool / playable（或 playable_ads）。也可用环境变量 ARROW2_SEARCH_TAB",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="有头浏览器（可目视登录与筛选）；并打印 napi 中与地区/国家相关的键路径（与 DEBUG=1 相同）",
    )
    parser.add_argument(
        "--debug-step-products",
        action="store_true",
        help="逐步核对：每个搜索词在截断（默认 10 条，见 ARROW2_DEBUG_STEP_CARDS）与地区补全后，"
        "终端打印本批素材摘要，有头浏览器保持当前结果页，按 Enter 进入下一搜索词；"
        "自动等同开启 --debug；与 workflow 共用 run_arrow2_batch（建议 --pull-only exposure_top10 与日常一致）",
    )
    parser.add_argument(
        "--debug-dom-probe",
        type=int,
        default=0,
        metavar="N",
        help="地区 DOM 探针：点列表前 N 张卡、终端打印「列表+detail-v2 摘要」后暂停；常配合 --debug。"
        "也支持环境变量 ARROW2_DEBUG_DOM_PROBE_FIRST 不设本参数；"
        "仅探针不继续多点时可加下方 --debug-dom-probe-only，或设环境变量 ARROW2_DEBUG_DOM_PROBE_ONLY=1",
    )
    parser.add_argument(
        "--debug-dom-probe-only",
        action="store_true",
        help="与 --debug-dom-probe 同用：设 ARROW2_DEBUG_DOM_PROBE_ONLY=1，探针暂停后不再多轮/单条点卡",
    )
    args = parser.parse_args()

    if int(getattr(args, "debug_dom_probe", 0) or 0) > 0:
        os.environ["ARROW2_DEBUG_DOM_PROBE_FIRST"] = str(int(args.debug_dom_probe))
    if getattr(args, "debug_dom_probe_only", False):
        os.environ["ARROW2_DEBUG_DOM_PROBE_ONLY"] = "1"

    if args.wipe_db:
        from arrow2_pipeline_db import wipe_arrow2_sqlite_all_rows

        wiped = wipe_arrow2_sqlite_all_rows()
        print(f"[0] 已清空 Arrow2 SQLite：{wiped}")

    cfg = _load_config()
    names = [x.strip() for x in (args.products or "").split(",") if x.strip()]
    if names:
        entries = _load_entries(cfg, names)
    elif args.all_products:
        entries = _load_entries(cfg, None)
    else:
        all_e = _load_entries(cfg, None)
        if not all_e:
            entries = []
        else:
            entries = [all_e[0]]
            if len(all_e) > 1:
                print(
                    f"[1] 未传 --products 且未加 --all-products：仅跑配置中第一个产品 "
                    f"「{all_e[0].keyword!r}」；全量请加 --all-products 或传 --products",
                    flush=True,
                )
    if not entries:
        print(
            "[终止] 无可用条目：请在 config/arrow2_competitor.json 的 products 中填写至少一项，"
            "或用 --products 指定 keyword / match / appid。",
            file=sys.stderr,
        )
        return

    extra_kw = cfg.get("extra_keywords") or []
    if not isinstance(extra_kw, list):
        extra_kw = []
    extra_kw = [str(x).strip() for x in extra_kw if str(x).strip()]

    tab_env = (os.environ.get("ARROW2_SEARCH_TAB") or "").strip().lower()
    tab_cli = (args.search_tab or "").strip().lower()
    tab_cfg = str(cfg.get("search_tab") or "game").strip().lower()
    tab = tab_cli or tab_env or tab_cfg
    is_game = tab in ("game", "playable", "playable_ads")
    is_tool = tab == "tool"

    filters = cfg.get("filters") or {}
    pop = str(filters.get("popularity_option_text") or "Top10%").strip()
    ch_normal = filters.get("ad_channels")
    if not isinstance(ch_normal, list):
        ch_normal = []
    ch_playable = filters.get("ad_channels_playable")
    if not isinstance(ch_playable, list):
        ch_playable = []
    if tab in ("playable", "playable_ads"):
        channels = ch_playable if ch_playable else ["Admob", "UnityAds", "AppLovin"]
    else:
        channels = ch_normal if ch_normal else ["Facebook系", "Google系", "UnityAds", "AppLovin"]
    countries = filters.get("countries")
    if not isinstance(countries, list):
        countries = []

    pull_specs_raw = cfg.get("pull_specs")
    pull_specs: list[dict[str, Any]] | None = None
    if isinstance(pull_specs_raw, list) and pull_specs_raw:
        pull_specs = [x for x in pull_specs_raw if isinstance(x, dict)]
    want_pull_ids = [x.strip() for x in (args.pull_only or "").split(",") if x.strip()]
    if want_pull_ids:
        if not pull_specs:
            print(
                "[终止] 配置中无 pull_specs，无法使用 --pull-only",
                file=sys.stderr,
            )
            return
        allowed = {str(p.get("id") or "").strip() for p in pull_specs}
        unknown = [x for x in want_pull_ids if x not in allowed]
        if unknown:
            print(
                f"[终止] --pull-only 含未知 id: {unknown}；当前有: {sorted(allowed)}",
                file=sys.stderr,
            )
            return
        pull_specs = [p for p in pull_specs if str(p.get("id") or "").strip() in want_pull_ids]
        if not pull_specs:
            print("[终止] --pull-only 过滤后 pull_specs 为空", file=sys.stderr)
            return

    day_spans = cfg.get("day_spans")
    if not isinstance(day_spans, list) or not day_spans:
        day_spans = ["30", "90"]
    else:
        day_spans = [str(x).strip() for x in day_spans if str(x).strip()]

    order_modes = cfg.get("order_modes")
    if not isinstance(order_modes, list) or not order_modes:
        order_modes = ["exposure", "heat", "latest", "relevance"]
    else:
        order_modes = [str(x).strip() for x in order_modes if str(x).strip()]

    keywords: list[str] = [_arrow2_search_query(e) for e in entries]
    keywords.extend(extra_kw)

    comp_map = {_arrow2_search_query(e): e for e in entries}

    n_kw = len([e for e in entries])
    if pull_specs:
        n_runs = n_kw * len(pull_specs)
    else:
        n_runs = n_kw * len(day_spans) * len(order_modes)
    print(
        "[1] Arrow2 产品: "
        + "; ".join(
            f"{e.keyword!r} → 搜索框={_arrow2_search_query(e)!r}" for e in entries
        )
    )
    print(f"[1a] 搜索页签: {_arrow2_search_tab_label(tab)} (raw={tab!r})")
    if pull_specs:
        print(
            f"[1b] 拉取：pull_specs {len(pull_specs)} 类 × 每竞品 {n_kw} 词 = 共约 {n_runs} 次（无 90 天全矩阵）"
        )
        for ps in pull_specs:
            cap = ps.get("max_creatives_per_keyword")
            if cap is None:
                cap = ps.get("max_per_keyword")
            cap_s = f" 每词≤{cap}条" if cap is not None else ""
            print(f"      - {ps.get('id')!r}: 天={ps.get('day_span')} 排序={ps.get('order_by')} "
                  f"Top创意={ps.get('popularity_option_text') or ('首项' if ps.get('popularity_pick_first') else '默认')}"
                  f"{' 仅昨日first_seen' if ps.get('filter_yesterday_only') else ''}{cap_s}")
    else:
        print(
            f"[1b] 拉取矩阵: {len(day_spans)} 档天数 × {len(order_modes)} 种排序 = "
            f"每竞品 {len(day_spans)*len(order_modes)} 次（共约 {n_runs} 次）"
        )
    if extra_kw:
        print(f"[1c] extra_keywords: {extra_kw}")
    ch_route = "试玩 ad_channels_playable" if tab in ("playable", "playable_ads") else "常规 ad_channels"
    ch_msg = f"{ch_route}: {channels}" if channels else "（未启用）"
    if pull_specs:
        print(f"[2] 筛选: pull_specs 分轮 Top创意 | 渠道={ch_msg} | 国家={countries}")
    else:
        print(f"[2] 筛选: Top创意≈{pop!r} | 渠道={ch_msg} | 国家={countries}")
    print(f"[3] 搜索关键词: {keywords}")

    pull_spec_defaults: dict[str, Any] = {}
    if cfg.get("max_creatives_per_keyword") is not None:
        pull_spec_defaults["max_creatives_per_keyword"] = cfg.get("max_creatives_per_keyword")

    debug_run = bool(args.debug or os.environ.get("DEBUG") or args.debug_step_products)
    if args.debug_step_products:
        os.environ.setdefault("ARROW2_DEBUG_PAUSE", "0")
        os.environ.setdefault("ARROW2_DEBUG_PAUSE_AFTER_GEO", "0")
    if debug_run:
        print(
            "[debug] 有头浏览器 + napi 地区线索；地区补全前默认不暂停（ARROW2_DEBUG_PAUSE=1 才停）；"
            "补全后会打印 [地区快照]（appid + 返回 JSON 的 country），再暂停（ARROW2_DEBUG_PAUSE_AFTER_GEO=0 可跳过）；"
            "关浏览器前再暂停（ARROW2_DEBUG_PAUSE_AT_END=0 则立即关）。"
            "可加 ARROW2_DEBUG_NAPI_TRACE=1 打印每条 napi 请求 URL（约前 150 条）。"
        )
        if args.debug_step_products:
            print(
                "[debug-step] 每搜索词完成后打印本批素材摘要并按 Enter 进入下一词；"
                "条数=min(配置上限, ARROW2_DEBUG_STEP_CARDS，默认 10)；全流程结束时不额外「关浏览器前」暂停。"
            )

    keyword_appid = {_arrow2_search_query(e): (e.appid or "").strip() for e in entries}

    results = await run_arrow2_batch(
        keywords=keywords,
        debug=debug_run,
        is_tool=is_tool and not is_game,
        is_game=is_game,
        day_spans=day_spans,
        order_modes=order_modes,
        popularity_option_text=None if pull_specs else (pop or None),
        ad_channel_labels=channels if channels else None,
        country_codes=countries or None,
        pull_specs=pull_specs,
        pull_spec_defaults=pull_spec_defaults if pull_spec_defaults else None,
        search_tab=tab,
        keyword_appid=keyword_appid,
        debug_step_per_product=args.debug_step_products,
    )

    raw_items: list[dict[str, Any]] = []

    for r in results:
        kw = str(r.get("keyword") or "")
        all_creatives = r.get("all_creatives") or []
        day_span = r.get("day_span")
        order_by = str(r.get("order_by") or "")
        if not isinstance(all_creatives, list):
            continue
        ent = comp_map.get(kw)
        if not ent:
            continue
        filtered = [
            c
            for c in all_creatives
            if isinstance(c, dict)
            and advertiser_matches_product(
                str(c.get("advertiser_name") or c.get("page_name") or ""),
                ent.product,
            )
        ]
        for c in filtered:
            row = {
                "product": ent.product,
                "keyword": ent.keyword,
                "search_query": kw,
                "appid": ent.appid,
                "day_span": day_span,
                "order_by": order_by,
                "pull_id": r.get("pull_id"),
                "pull_spec": r.get("pull_spec"),
                "creative": c,
            }
            raw_items.append(row)

    items_deduped, dedupe_stats = dedupe_arrow2_raw_items_by_ad_key(raw_items)

    by_product_reduce: dict[str, list[dict]] = {}
    for row in items_deduped:
        product = str(row.get("product") or "")
        c = row.get("creative")
        if not isinstance(c, dict):
            c = {}
        ak = str(row.get("ad_key") or "").strip() or arrow2_creative_ad_key(c)
        by_product_reduce.setdefault(product, []).append(
            {
                **_reduce_creative(c),
                "ad_key": ak,
                "keyword": row.get("keyword") or "",
                "search_query": row.get("search_query") or "",
                "appid": row.get("appid") or "",
                "pull_id": row.get("pull_id"),
                "pull_spec": row.get("pull_spec"),
                "day_span": row.get("day_span"),
                "order_by": row.get("order_by"),
                "seen_in_runs": row.get("seen_in_runs"),
                "cross_check": row.get("cross_check"),
            }
        )

    base = (args.output_prefix or "").strip() or "workflow_arrow2_sample"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_path = DATA_DIR / f"{base}_raw.json"
    raw_payload = {
        "workflow": "arrow2_competitor",
        "target_date": _target_date_from_env(),
        "crawl_date": _beijing_today_iso(),
        "config_file": str(CONFIG_FILE),
        "search_tab": tab,
        "day_spans": day_spans,
        "order_modes": order_modes,
        "filters": {
            "popularity_option_text": pop,
            "ad_channels": ch_normal,
            "ad_channels_playable": ch_playable,
            "ad_channels_used": channels,
            "countries": countries,
        },
        "competitors": [
            {"keyword": e.keyword, "product": e.product, "appid": e.appid} for e in entries
        ],
        "extra_keywords": extra_kw,
        "pull_specs": pull_specs if pull_specs else None,
        "day_spans": day_spans if not pull_specs else None,
        "order_modes": order_modes if not pull_specs else None,
        "total_items_matched_before_dedupe": len(raw_items),
        "total_unique_by_ad_key": dedupe_stats["rows_after_dedupe"],
        "dedupe_stats": dedupe_stats,
        "api_runs": len(results),
        "items": raw_items,
        "items_deduped_by_ad_key": items_deduped,
    }
    raw_path.write_text(json.dumps(raw_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        f"[4] 原始 JSON: {raw_path.name}（广告主匹配 {len(raw_items)} 条，"
        f"全局 ad_key 去重后 {dedupe_stats['rows_after_dedupe']} 条，合并重复 "
        f"{dedupe_stats['duplicate_rows_merged']} 条；接口轮次 {len(results)}）"
    )

    reduce_path = DATA_DIR / f"{base}_reduce.json"
    reduce_path.write_text(
        json.dumps(
            {
                "workflow": "arrow2_competitor",
                "total_items_matched_before_dedupe": len(raw_items),
                "total_unique_by_ad_key": dedupe_stats["rows_after_dedupe"],
                "dedupe_stats": dedupe_stats,
                "by_product": by_product_reduce,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"[5] 提炼 JSON: {reduce_path.name}（全局 ad_key 去重后写入 by_product）")
    print("完成。--debug 或 DEBUG=1 可核对：游戏 Tab、筛选、排序、渠道、国家；napi 地区键名见终端 [arrow2 DEBUG·napi 地区线索]。")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("中断。", file=sys.stderr)
