"""
从 batch_ua_results.json 下载 UA 素材（视频、图片）。

用法:
  python download_ua_assets.py           # 下载全部
  python download_ua_assets.py --test    # 仅测试下载 1 个素材
"""
import argparse
import json
import re
import sys
from pathlib import Path
from typing import List, Tuple
from urllib.request import urlopen, Request

from path_util import DATA_DIR, DOWNLOADS_DIR

INPUT_FILE = DATA_DIR / "batch_ua_results.json"
OUTPUT_DIR = DOWNLOADS_DIR


def safe_filename(s: str) -> str:
    return re.sub(r'[^\w\-.]', '_', s)[:80]


def collect_urls(data: dict) -> List[Tuple[str, str, str]]:
    """返回 [(url, 公司_游戏_类型, ext), ...]"""
    items = []
    for g in data.get("games", []):
        company = safe_filename(g.get("company", "unknown"))
        game = safe_filename(g.get("game", "unknown"))
        sel = g.get("selected")
        if not sel:
            continue
        ad_key = sel.get("ad_key", "")[:12]
        base = f"{company}_{game}_{ad_key}"
        # resource_urls
        for r in sel.get("resource_urls", []):
            if r.get("image_url"):
                url = r["image_url"]
                ext = Path(url.split("?")[0]).suffix or ".jpg"
                items.append((url, f"{base}_cover", ext))
            if r.get("video_url"):
                url = r["video_url"]
                ext = Path(url.split("?")[0]).suffix or ".mp4"
                items.append((url, f"{base}_video", ext))
        # preview_img_url, logo_url（去重，可能与 resource 重复）
        for key, name in [("preview_img_url", "preview"), ("logo_url", "logo")]:
            url = sel.get(key)
            if url and not any(u == url for u, _, _ in items):
                ext = Path(url.split("?")[0]).suffix or ".jpg"
                items.append((url, f"{base}_{name}", ext))
    return items


def download_one(url: str, out_path: Path) -> bool:
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=30) as r:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, "wb") as f:
                f.write(r.read())
        return True
    except Exception as e:
        print(f"  下载失败 {url[:60]}...: {e}")
        return False


def main(test: bool = False):
    if not INPUT_FILE.exists():
        print(f"[错误] 未找到 {INPUT_FILE.name}", file=sys.stderr)
        sys.exit(1)

    with open(INPUT_FILE, encoding="utf-8") as f:
        data = json.load(f)

    urls = collect_urls(data)
    if not urls:
        print("[错误] 未找到可下载的素材", file=sys.stderr)
        sys.exit(1)

    print(f"共 {len(urls)} 个素材待下载")
    if test:
        # 测试：下载第 1 个（图）和第 2 个（若有视频）
        urls = urls[:2]
        print(f"[测试模式] 仅下载 {len(urls)} 个")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ok = 0
    for url, name, ext in urls:
        out = OUTPUT_DIR / f"{name}{ext}"
        if out.exists():
            print(f"  已存在: {out.name}")
            ok += 1
        elif download_one(url, out):
            print(f"  ✓ {out.name}")
            ok += 1

    print(f"\n完成: {ok}/{len(urls)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="仅测试下载 1 个")
    args = parser.parse_args()
    main(test=args.test)
