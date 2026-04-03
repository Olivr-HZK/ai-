"""
将 data/ 下所有 `guangdada_creatives_target_*.json` 重写成：
1) 按 product 名称分组：by_product
2) 每条素材只保留你关心的字段（字段名保持中文，便于直接核对）

输出结构：
{
  "crawl_date": "...",
  "target_date": "...",
  "by_product": {
     "<product>": [
        {
          "展示估值": ...,
          "人气值": ...,
          "热度": ...,
          "视频长度": ...,
          "素材链接": "...",
          "投放时间_utc8": "YYYY-MM-DD HH:MM:SS"
        },
        ...
     ]
  }
}
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from path_util import DATA_DIR


def _utc8(dt: int) -> str:
    tz8 = timezone(timedelta(hours=8))
    return datetime.fromtimestamp(int(dt), tz=tz8).strftime("%Y-%m-%d %H:%M:%S")


def _pick_media_links(creative: dict) -> tuple[Optional[str], Optional[str]]:
    """
    返回 (video_url, image_url)；没有对应类型则为 None。
    """
    video_url = None
    image_url = None
    for r in creative.get("resource_urls") or []:
        if not isinstance(r, dict):
            continue
        if not video_url and r.get("video_url"):
            video_url = str(r["video_url"])
        if not image_url and r.get("image_url"):
            image_url = str(r["image_url"])
    # 有的 creative 也可能直接带 video_url
    if not video_url and creative.get("video_url"):
        video_url = str(creative["video_url"])
    return video_url, image_url


def _pick_ts(creative: dict) -> Optional[int]:
    """
    投放时间优先用 created_at，其次 first_seen。
    """
    for k in ("created_at", "first_seen"):
        v = creative.get(k)
        if v is None:
            continue
        try:
            return int(v)
        except Exception:
            continue
    return None


def _reduce_item(item: dict) -> dict:
    creative = item.get("creative") or {}
    if not isinstance(creative, dict):
        creative = {}

    video_url, image_url = _pick_media_links(creative)
    media_link = video_url or image_url or ""

    ts = _pick_ts(creative)
    time_utc8 = _utc8(ts) if ts is not None else ""

    # 字段映射：人气值使用 impression（素材列表里常见字段），热度使用 heat
    return {
        "展示估值": creative.get("all_exposure_value") or 0,
        "人气值": creative.get("impression") or 0,
        "热度": creative.get("heat") or 0,
        "视频长度": creative.get("video_duration") or 0,
        "素材链接": media_link,
        "投放时间_utc8": time_utc8,
    }


def main() -> None:
    pattern = "guangdada_creatives_target_*.json"
    files = sorted(DATA_DIR.glob(pattern))
    if not files:
        print(f"[reduce] 未找到匹配文件：{pattern}")
        return

    print(f"[reduce] 将处理 {len(files)} 个文件")
    for f in files:
        data = json.load(f.open("r", encoding="utf-8"))
        crawl_date = data.get("crawl_date")
        target_date = data.get("target_date")
        items = data.get("items") or []
        if not isinstance(items, list) or not items:
            # 兼容：上一次已转换成 by_advertiser 结构
            by_adv = data.get("by_advertiser") or {}
            if isinstance(by_adv, dict) and by_adv:
                flat: list[Any] = []
                for _, lst in by_adv.items():
                    if isinstance(lst, list):
                        flat.extend(lst)
                items = flat
            if not isinstance(items, list) or not items:
                print(f"[reduce] {f.name} 无可用 items/by_advertiser 数据，跳过。")
                continue

        by_product: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for item in items:
            if not isinstance(item, dict):
                continue
            product = str(item.get("product") or "").strip()
            if not product:
                product = "(unknown)"
            by_product[product].append(_reduce_item(item))

        # 可选：按每个 product 的数量降序排序 key
        product_keys = sorted(by_product.keys(), key=lambda k: len(by_product[k]), reverse=True)
        by_product_sorted: Dict[str, List[Dict[str, Any]]] = {k: by_product[k] for k in product_keys}

        new_data = {
            "crawl_date": crawl_date,
            "target_date": target_date,
            "by_product": by_product_sorted,
        }

        bak = f.with_suffix(f.suffix + ".bak_by_product_reduce")
        if not bak.exists():
            bak.write_bytes(f.read_bytes())
        f.write_text(json.dumps(new_data, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[reduce] 已重写 {f.name}（bak: {bak.name}，by_product={len(by_product_sorted)}）")

    print("[reduce] 全部完成。")


if __name__ == "__main__":
    main()

