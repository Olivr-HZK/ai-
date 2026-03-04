"""
从 twitter_input.json 解析所有游戏名，批量调用 run_search_workflow 获取各游戏的 UA 素材。

用法: python batch_fetch_ua.py [--debug]
"""
import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import List, Tuple

from path_util import CONFIG_DIR, DATA_DIR

INPUT_FILE = CONFIG_DIR / "twitter_input.json"
OUTPUT_FILE = DATA_DIR / "batch_ua_results.json"


def extract_game_names(data: dict) -> List[Tuple[str, str]]:
    """返回 [(公司名, 游戏名), ...]"""
    games = []
    for comp in data.get("competitors", []):
        company = comp.get("name", "")
        for g in comp.get("games", []):
            name = g.get("name", "").strip()
            if name:
                games.append((company, name))
    return games


async def main(debug: bool = False):
    if not INPUT_FILE.exists():
        print(f"[错误] 未找到 {INPUT_FILE.name}", file=sys.stderr)
        sys.exit(1)

    with open(INPUT_FILE, encoding="utf-8") as f:
        data = json.load(f)

    game_list = extract_game_names(data)
    if not game_list:
        print("[错误] 未解析到任何游戏", file=sys.stderr)
        sys.exit(1)

    print(f"共 {len(game_list)} 个游戏待获取 UA 素材:")
    for company, name in game_list:
        print(f"  - {company} / {name}")

    from run_search_workflow import run

    results = []
    for i, (company, game_name) in enumerate(game_list, 1):
        print(f"\n{'='*50}\n[{i}/{len(game_list)}] {company} - {game_name}\n{'='*50}")
        try:
            r = await run(keyword=game_name, debug=debug)
            results.append({
                "company": company,
                "game": game_name,
                "keyword": r.get("keyword"),
                "selected": r.get("selected"),
                "total_captured": r.get("total_captured", 0),
            })
        except Exception as e:
            print(f"[失败] {game_name}: {e}", file=sys.stderr)
            results.append({
                "company": company,
                "game": game_name,
                "error": str(e),
            })

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump({"games": results}, f, ensure_ascii=False, indent=2)

    print(f"\n[完成] 结果已写入 {OUTPUT_FILE.name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="显示浏览器")
    args = parser.parse_args()
    asyncio.run(main(debug=args.debug))
