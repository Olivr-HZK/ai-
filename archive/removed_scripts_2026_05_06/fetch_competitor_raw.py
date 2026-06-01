"""
通用竞品原始素材抓取：从 config/ai_product.json 读取 appid，
按主工作流逻辑（工具→7天→素材→最新创意）抓取，不做任何过滤直接输出。

用法：
  python scripts/fetch_competitor_raw.py --product "Remini - AI Photo Enhancer"
  python scripts/fetch_competitor_raw.py --product "UpFoto - AI Photo Enhancer"
  python scripts/fetch_competitor_raw.py --list          # 列出所有可用竞品
  DEBUG=1 python scripts/fetch_competitor_raw.py --product "Remini - AI Photo Enhancer"
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from path_util import CONFIG_DIR, DATA_DIR
from run_search_workflow import run_batch

CONFIG_FILE = CONFIG_DIR / "ai_product.json"


def _load_all_products() -> dict[str, tuple[str, str]]:
    """返回 {product_name: (category, appid)}"""
    data = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    result: dict[str, tuple[str, str]] = {}
    for cat, items in (data.items() if isinstance(data, dict) else []):
        if not isinstance(items, dict):
            continue
        for product, appid in items.items():
            if product and str(appid or "").strip():
                result[str(product)] = (str(cat), str(appid))
    return result


def _ts_to_utc8(ts) -> str:
    try:
        tz8 = timezone(timedelta(hours=8))
        return datetime.fromtimestamp(int(ts), tz=tz8).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def _pick_video_url(c: dict) -> str:
    if c.get("video_url"):
        return str(c["video_url"])
    for r in c.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("video_url"):
            return str(r["video_url"])
    return ""


def _safe_name(s: str) -> str:
    s = re.sub(r"\s+", "_", s.strip())
    return re.sub(r"[^a-zA-Z0-9_\-]", "", s)[:50]


async def main() -> None:
    all_products = _load_all_products()

    parser = argparse.ArgumentParser(description="抓取指定竞品的 7 天窗口原始素材，不做任何过滤")
    parser.add_argument("--product", default="", help="竞品产品名（需与 config/ai_product.json 完全一致）")
    parser.add_argument("--list", action="store_true", help="列出所有可用竞品名称后退出")
    args = parser.parse_args()

    if args.list:
        print("可用竞品列表：")
        for name, (cat, appid) in sorted(all_products.items(), key=lambda x: x[1][0]):
            print(f"  [{cat}]  {name}  ({appid})")
        return

    if not args.product.strip():
        parser.error("请通过 --product 指定竞品名，或用 --list 查看所有竞品")

    product = args.product.strip()
    if product not in all_products:
        print(f"[错误] 找不到产品 '{product}'，可用列表：", file=sys.stderr)
        for name in sorted(all_products):
            print(f"  {name}", file=sys.stderr)
        sys.exit(1)

    category, appid = all_products[product]
    today = datetime.now(tz=timezone(timedelta(hours=8))).strftime("%Y-%m-%d")

    print(f"[产品] {product}")
    print(f"[分类] {category}")
    print(f"[AppID] {appid}  ← 作为搜索关键词")
    print(f"[模式] 工具→7天→素材→最新创意，不做任何过滤")

    results = await run_batch(
        keywords=[appid],
        debug=bool(os.environ.get("DEBUG")),
        is_tool=True,
        order_by="latest",
        use_popularity_top1=False,
        enable_dom_track=True,  # 仅此脚本启用 DOM 补充，主工作流不受影响
    )

    napi_creatives: list[dict] = []
    dom_creatives: list[dict] = []
    for r in results:
        napi_creatives.extend(c for c in (r.get("napi_creatives") or r.get("all_creatives") or []) if isinstance(c, dict))
        dom_creatives.extend(c for c in (r.get("dom_creatives") or []) if isinstance(c, dict))

    print(f"\n[结果] napi={len(napi_creatives)} 条  dom={len(dom_creatives)} 条")

    # napi 摘要
    print(f"\n{'='*72}  [NAPI]")
    for i, c in enumerate(napi_creatives, 1):
        fs = _ts_to_utc8(c.get("first_seen")) if c.get("first_seen") else "-"
        resume = c.get("resume_advertising_flag", False)
        video_url = _pick_video_url(c)
        print(
            f"[{i:03d}] first_seen={fs}  重投={resume}  "
            f"热度={c.get('heat',0)}  估值={c.get('all_exposure_value',0)}  "
            f"platform={c.get('platform','')}  dur={c.get('video_duration','?')}s"
        )
        if video_url:
            print(f"       video: {video_url[:90]}")

    # dom 摘要
    if dom_creatives:
        print(f"\n{'='*72}  [DOM]")
        for i, c in enumerate(dom_creatives, 1):
            src = c.get("_source", "dom")
            fs = _ts_to_utc8(c.get("first_seen")) if c.get("first_seen") else "-"
            resume = c.get("resume_advertising_flag", False)
            print(
                f"[{i:03d}][{src}] first_seen={fs}  重投={resume}  "
                f"热度={c.get('heat',0)}  估值={c.get('all_exposure_value',0)}  "
                f"platform={c.get('platform','')}  dur={c.get('video_duration','?')}s"
            )
            preview = c.get("preview_img_url", "")
            if preview:
                print(f"       preview: {preview[:90]}")

    # 保存两份文件
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    safe = _safe_name(product)

    napi_path = DATA_DIR / f"raw_{safe}_{today}_napi.json"
    napi_path.write_text(
        json.dumps({"crawl_date": today, "product": product, "category": category,
                    "appid": appid, "total": len(napi_creatives), "creatives": napi_creatives},
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    dom_path = DATA_DIR / f"raw_{safe}_{today}_dom.json"
    dom_path.write_text(
        json.dumps({"crawl_date": today, "product": product, "category": category,
                    "appid": appid, "total": len(dom_creatives), "creatives": dom_creatives},
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"\n[输出] {napi_path.name}（{len(napi_creatives)} 条）")
    print(f"[输出] {dom_path.name}（{len(dom_creatives)} 条）")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("中断。", file=sys.stderr)
