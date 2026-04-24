"""
Arrow2 每日「仅昨日 first_seen」竞品：有头浏览器、**配置中全部产品**、只跑 pull id=latest_yesterday。

- 与 run_arrow2 一致：appid 下拉里匹配 → 搜索；napi 与 DOM 点卡均在 **first_seen 早于目标日** 时早停
  （见 run_search_workflow.arrow2_enrich_dom_creatives_detail_v2_yesterday_window）。

- 每个产品跑完**本轮全部 pull**（此处仅 1 类）后：终端打印**昨日素材**的
  展示估值 / 人气 / 热度，**按 Enter** 再下一产品（需环境变量
  `ARROW2_PAUSE_PRODUCT_SUMMARY=yesterday_metrics`，本脚本已设）。

  cd 项目根
  PYTHONUNBUFFERED=1 .venv/bin/python scripts/test_arrow2_yesterday_all_products_headed.py
  # 指定 first_seen 目标日（默认北京「昨天」）
  TARGET_DATE=2026-04-18 .venv/bin/python scripts/test_arrow2_yesterday_all_products_headed.py
"""

from __future__ import annotations

import asyncio
import os
import sys

from dotenv import load_dotenv

from path_util import PROJECT_ROOT, CONFIG_DIR
from test_arrow2_competitors import (  # noqa: E402
    _arrow2_search_query,
    _load_config,
    _load_entries,
)
from run_search_workflow import run_arrow2_batch  # noqa: E402

load_dotenv(PROJECT_ROOT / ".env")

CONFIG_FILE = CONFIG_DIR / "arrow2_competitor.json"


def _beijing_yesterday_iso() -> str:
    from datetime import datetime, timedelta, timezone

    tz = timezone(timedelta(hours=8))
    return (datetime.now(tz).date() - timedelta(days=1)).isoformat()


def main() -> int:
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    # 有头、每产品暂停时只打昨日三指标
    os.environ["DEBUG"] = "1"
    os.environ["ARROW2_PAUSE_PRODUCT_SUMMARY"] = "yesterday_metrics"
    os.environ.setdefault("ARROW2_DEBUG_PAUSE_AT_END", "0")
    os.environ.setdefault("ARROW2_DEBUG_PAUSE", "0")

    td = (os.environ.get("TARGET_DATE") or "").strip()[:10]
    if not td:
        td = _beijing_yesterday_iso()
    os.environ["TARGET_DATE"] = td
    print(f"[配置] first_seen 目标日（仅昨日）= {td!r}（可设 TARGET_DATE 覆盖）\n")

    cfg = _load_config()
    entries = _load_entries(cfg, None)
    if not entries:
        print(f"[终止] {CONFIG_FILE} 无产品", file=sys.stderr)
        return 1

    pull_specs_raw = cfg.get("pull_specs")
    if not isinstance(pull_specs_raw, list):
        print("[终止] 配置缺少 pull_specs 列表", file=sys.stderr)
        return 1
    only = [p for p in pull_specs_raw if isinstance(p, dict) and p.get("id") == "latest_yesterday"]
    if not only:
        print(
            "[终止] pull_specs 中需含 id=latest_yesterday 的一项",
            file=sys.stderr,
        )
        return 1

    keywords = [_arrow2_search_query(e) for e in entries]
    keyword_appid = {_arrow2_search_query(e): (e.appid or "").strip() for e in entries}
    keyword_product = {
        _arrow2_search_query(e): (e.product or e.keyword or "").strip() for e in entries
    }

    tab = str(cfg.get("search_tab") or "game").strip().lower()
    is_game = tab in ("game", "playable", "playable_ads")
    is_tool = tab == "tool"
    filters = cfg.get("filters") or {}
    ch_normal = filters.get("ad_channels")
    if not isinstance(ch_normal, list):
        ch_normal = []
    ch_playable = filters.get("ad_channels_playable")
    if not isinstance(ch_playable, list):
        ch_playable = []
    if tab in ("playable", "playable_ads"):
        channels = ch_playable if ch_playable else ["Admob", "UnityAds", "AppLovin"]
    else:
        channels = ch_normal if ch_normal else [
            "Facebook系",
            "Google系",
            "UnityAds",
            "AppLovin",
        ]
    countries = filters.get("countries")
    if not isinstance(countries, list):
        countries = []

    print(
        f"[计划] 共 {len(keywords)} 个竞品；pull=latest_yesterday ；"
        f"每产品结束打印昨日素材(展示估值/人气/热度) 后按 Enter 继续\n"
    )

    async def _run() -> None:
        await run_arrow2_batch(
            keywords=keywords,
            debug=True,
            is_tool=is_tool and not is_game,
            is_game=is_game,
            ad_channel_labels=channels if channels else None,
            country_codes=countries or None,
            pull_specs=only,
            pull_spec_defaults=None,
            search_tab=tab,
            keyword_appid=keyword_appid,
            keyword_product=keyword_product,
            target_date_first_seen=td,
            debug_step_per_product=False,
            debug_pause_per_product=True,
        )

    asyncio.run(_run())
    print("\n[结束] 全部产品已跑完。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
