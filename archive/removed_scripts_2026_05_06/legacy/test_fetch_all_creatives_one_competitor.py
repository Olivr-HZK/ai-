"""
测试脚本：按当前 run_search_workflow 逻辑，抓取「单个竞品关键词」的整页素材（all_creatives），
并检查是否存在「同一 ad_key，但平台/文案/素材链接不同」的情况。

说明：
- 使用 run_search_workflow.run_batch（已包含：过滤 Sponsored/赞助、搜索后再点排序、抓取 all_creatives）
- 本脚本不会写入数据库，只输出检查结果并保存 JSON。

用法（项目根目录）：
  .venv/bin/python3 scripts/test_fetch_all_creatives_one_competitor.py "ChatOn - AI Chat Bot Assistant" --order-by latest --debug
  .venv/bin/python3 scripts/test_fetch_all_creatives_one_competitor.py "Remini - AI Photo Enhancer" --order-by latest
  .venv/bin/python3 scripts/test_fetch_all_creatives_one_competitor.py "some keyword" --tool

输出：
- 控制台：总数、去重后 ad_key 数、重复 ad_key 数、以及“同 ad_key 多变体”的样例
- data/test_all_creatives_<safe_keyword>.json：完整 all_creatives（便于人工核对）
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

from path_util import DATA_DIR
from run_search_workflow import run_batch


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="抓取单个竞品的整页素材并检查同 ad_key 多变体。")
    p.add_argument("keyword", nargs="+", help="搜索关键词（可含空格）")
    p.add_argument("--debug", action="store_true", help="显示浏览器")
    p.add_argument("--tool", action="store_true", help="切换到工具标签")
    p.add_argument(
        "--order-by",
        choices=["exposure", "latest"],
        default="latest",
        help="页面排序方式：exposure=展示估值，latest=最新创意（默认 latest）",
    )
    p.add_argument(
        "--show",
        type=int,
        default=10,
        help="最多展示多少个“同 ad_key 多变体”的样例（默认 10）",
    )
    return p.parse_args()


def _safe_name(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^a-zA-Z0-9_\u4e00-\u9fff-]+", "", s)
    return s[:80] or "keyword"


def _brief(s: str | None, n: int = 70) -> str:
    s = (s or "").strip().replace("\n", " ").replace("\r", " ")
    return s if len(s) <= n else s[:n] + "..."


def _video_url_from_creative(c: Dict[str, Any]) -> str:
    if c.get("video_url"):
        return str(c.get("video_url") or "")
    for r in c.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("video_url"):
            return str(r.get("video_url") or "")
    return ""


def _signature(c: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
    """
    用于判断“同 ad_key 是否出现不同变体”的签名：
    (platform, title, body, preview_img_url, video_url)
    """
    platform = str(c.get("platform") or "")
    title = str(c.get("title") or "")
    body = str(c.get("body") or c.get("message") or "")
    preview_img_url = str(c.get("preview_img_url") or "")
    video_url = _video_url_from_creative(c)
    return (platform.strip(), title.strip(), body.strip(), preview_img_url.strip(), str(video_url).strip())


async def main() -> None:
    args = parse_args()
    keyword = " ".join(args.keyword).strip()
    if not keyword:
        raise SystemExit("keyword 不能为空")

    results = await run_batch(
        [keyword],
        debug=bool(args.debug),
        is_tool=bool(args.tool),
        order_by=str(args.order_by),
    )
    r = results[0] if results else {}
    all_creatives: List[Dict[str, Any]] = r.get("all_creatives") or []

    # group by ad_key
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for c in all_creatives:
        if not isinstance(c, dict):
            continue
        ad_key = (
            c.get("ad_key")
            or c.get("creative_id")
            or c.get("creativeId")
            or c.get("id")
            or ""
        )
        ad_key = str(ad_key).strip()
        if not ad_key:
            continue
        groups[ad_key].append(c)

    total = len(all_creatives)
    uniq_ad = len(groups)
    dup_ad_keys = [k for k, v in groups.items() if len(v) > 1]

    multi_variant: List[Tuple[str, int, int, List[Tuple[str, str, str, str, str]]]] = []
    for k in dup_ad_keys:
        sigs = list({_signature(c) for c in groups[k]})
        if len(sigs) > 1:
            multi_variant.append((k, len(groups[k]), len(sigs), sigs))

    # 输出概要
    print("")
    print(f"[test_all_creatives] keyword={keyword}")
    print(f"[test_all_creatives] order_by={args.order_by}, is_tool={bool(args.tool)}")
    print(f"[test_all_creatives] total(all_creatives)={total}")
    print(f"[test_all_creatives] unique_ad_key={uniq_ad}")
    print(f"[test_all_creatives] dup_ad_key_count={len(dup_ad_keys)}")
    print(f"[test_all_creatives] multi_variant_ad_key_count={len(multi_variant)}")
    print("")

    # 按“变体数”降序展示
    multi_variant.sort(key=lambda x: (x[2], x[1]), reverse=True)
    show_n = max(0, int(args.show))
    if show_n and multi_variant:
        print("[test_all_creatives] 样例（同 ad_key 多变体）：")
        for idx, (ad_key, rows_n, sig_n, sigs) in enumerate(multi_variant[:show_n], 1):
            print(f"  #{idx} ad_key={ad_key} rows={rows_n} variants={sig_n}")
            for j, (platform, title, body, preview_img_url, video_url) in enumerate(sigs[:3], 1):
                print(f"    - v{j} platform={platform or '∅'}")
                print(f"      title={_brief(title)}")
                print(f"      body ={_brief(body)}")
                print(f"      img  ={preview_img_url or '∅'}")
                print(f"      video={video_url or '∅'}")
            if sig_n > 3:
                print(f"    ... 还有 {sig_n-3} 个变体未展示")
        print("")

    # 保存 JSON
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path: Path = DATA_DIR / f"test_all_creatives_{_safe_name(keyword)}.json"
    payload = {
        "tested_at": datetime.now().isoformat(timespec="seconds"),
        "keyword": keyword,
        "order_by": args.order_by,
        "is_tool": bool(args.tool),
        "total_captured": r.get("total_captured"),
        "total_all_creatives": total,
        "unique_ad_key": uniq_ad,
        "dup_ad_key_count": len(dup_ad_keys),
        "multi_variant_ad_key_count": len(multi_variant),
        "multi_variant_ad_keys": [x[0] for x in multi_variant],
        "all_creatives": all_creatives,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"[test_all_creatives] 已写入: {out_path}")


if __name__ == "__main__":
    asyncio.run(main())

