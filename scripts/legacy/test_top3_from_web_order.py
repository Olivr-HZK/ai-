"""
测试：只取「网页展示顺序」前三条素材（已去重 + 已过滤 Sponsored/赞助广告）。

用途：
- 你想验证：脚本选出来的 top3 是否与网页展示的前三条一致。

用法（项目根目录）：
  .venv/bin/python3 scripts/test_top3_from_web_order.py "Remini - AI Photo Enhancer"
  .venv/bin/python3 scripts/test_top3_from_web_order.py "ChatOn - AI Chat Bot Assistant" --debug
  .venv/bin/python3 scripts/test_top3_from_web_order.py "some keyword" --order-by latest
  .venv/bin/python3 scripts/test_top3_from_web_order.py "some keyword" --tool

输出：
- 终端打印 top3 摘要（ad_key/title/body 前 80 字）
- 保存完整结果到 data/test_top3_result.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime

from path_util import DATA_DIR
from run_search_workflow import run_batch


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="测试：按网页顺序取前三条素材。")
    p.add_argument("keyword", nargs="+", help="搜索关键词（可含空格）")
    p.add_argument("--debug", action="store_true", help="显示浏览器")
    p.add_argument("--tool", action="store_true", help="切换到工具标签")
    p.add_argument(
        "--order-by",
        choices=["exposure", "latest"],
        default="latest",
        help="页面排序方式：exposure=展示估值，latest=最新创意（默认 latest）",
    )
    return p.parse_args()


def _brief(s: str | None, n: int = 80) -> str:
    s = (s or "").strip().replace("\n", " ").replace("\r", " ")
    if len(s) <= n:
        return s
    return s[:n] + "..."


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
    all_creatives = r.get("all_creatives") or []
    top3 = r.get("top_creatives") or []

    print("")
    print(f"[test_top3] keyword={keyword}")
    print(f"[test_top3] captured(all_creatives)={len(all_creatives)}")
    print(f"[test_top3] top3={len(top3)}")
    print("")

    for i, c in enumerate(top3, 1):
        ad_key = c.get("ad_key") or c.get("creative_id") or c.get("id") or ""
        title = c.get("title") or ""
        body = c.get("body") or c.get("message") or ""
        print(f"Top{i}: ad_key={ad_key}")
        print(f"  title: {_brief(title)}")
        print(f"  body : {_brief(body)}")
        print("")

    out = {
        "tested_at": datetime.now().isoformat(timespec="seconds"),
        "keyword": keyword,
        "order_by": args.order_by,
        "is_tool": bool(args.tool),
        "total_captured": r.get("total_captured"),
        "top_creatives": top3,
    }
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATA_DIR / "test_top3_result.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"[test_top3] 已写入: {out_path}")


if __name__ == "__main__":
    asyncio.run(main())

