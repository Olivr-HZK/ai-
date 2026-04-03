"""
将 data/ 下所有以 `guangdada_creatives_target_*.json` 开头的文件，
从：
  {crawl_date, target_date, items:[{..., creative:{advertiser_name,...}}]}
转换为：
  {crawl_date, target_date, by_advertiser:{advertiser_name:[items...]}}

会对原文件生成同目录 .bak 备份，并覆盖写入。
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

from path_util import DATA_DIR


def load_json(path: Path) -> dict[str, Any]:
    return json.load(path.open("r", encoding="utf-8"))


def save_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    pattern = "guangdada_creatives_target_*.json"
    files = sorted(DATA_DIR.glob(pattern))
    if not files:
        print(f"[transform] 未找到匹配文件：{DATA_DIR / pattern}")
        return

    print(f"[transform] 将转换 {len(files)} 个文件：")
    for f in files:
        print(f"  - {f.name}")

    for f in files:
        data = load_json(f)
        crawl_date = data.get("crawl_date")
        target_date = data.get("target_date")
        items = data.get("items") or []
        if not isinstance(items, list):
            items = []

        by_adv: dict[str, list[Any]] = defaultdict(list)
        for item in items:
            if not isinstance(item, dict):
                continue
            adv = ""
            creative = item.get("creative") or {}
            if isinstance(creative, dict):
                adv = creative.get("advertiser_name") or ""
            by_adv[str(adv)].append(item)

        # 按数量降序组织键，方便肉眼查看
        adv_keys = sorted(by_adv.keys(), key=lambda k: len(by_adv[k]), reverse=True)
        by_adv_sorted: dict[str, list[Any]] = {k: by_adv[k] for k in adv_keys}

        new_data = {
            "crawl_date": crawl_date,
            "target_date": target_date,
            "by_advertiser": by_adv_sorted,
        }

        bak = f.with_suffix(f.suffix + ".bak")
        if not bak.exists():
            bak.write_bytes(f.read_bytes())

        save_json(f, new_data)
        print(f"[transform] 已写入：{f.name}（bak: {bak.name}）")

    print("[transform] 全部完成。")


if __name__ == "__main__":
    main()

