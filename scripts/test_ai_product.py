"""
测试脚本：只测试一个 AI 产品，使用 debug 模式查看是否成功切换到工具标签

用法: python test_ai_product.py [--debug]
"""
import argparse
import asyncio
import json
import sys
from pathlib import Path

from path_util import CONFIG_DIR

INPUT_FILE = CONFIG_DIR / "ai_product.json"


async def main(debug: bool = True):
    """测试单个产品"""
    if not INPUT_FILE.exists():
        print(f"[错误] 未找到 {INPUT_FILE.name}", file=sys.stderr)
        sys.exit(1)

    with open(INPUT_FILE, encoding="utf-8") as f:
        data = json.load(f)

    # 选择第一个产品进行测试
    first_category = list(data.keys())[0]
    first_product = list(data[first_category].keys())[0]
    
    print(f"测试产品: [{first_category}] {first_product}")
    print(f"使用 debug 模式: {debug}")
    print("=" * 60)

    from run_search_workflow import run

    try:
        result = await run(keyword=first_product, debug=debug, is_tool=True)
        print("\n" + "=" * 60)
        print("测试结果:")
        print(f"  关键词: {result.get('keyword')}")
        print(f"  捕获素材数: {result.get('total_captured', 0)}")
        if result.get("selected"):
            sel = result["selected"]
            print(f"  选中素材:")
            print(f"    - 标题: {sel.get('title', 'N/A')}")
            print(f"    - 投放天数: {sel.get('days_count', 'N/A')} 天")
            print(f"    - 展示估值: {sel.get('all_exposure_value', 'N/A')}")
        else:
            print("  未找到素材")
        print("=" * 60)
    except Exception as e:
        print(f"\n[错误] 测试失败: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", default=True, help="显示浏览器（默认开启）")
    parser.add_argument("--no-debug", dest="debug", action="store_false", help="不显示浏览器")
    args = parser.parse_args()
    asyncio.run(main(debug=args.debug))
