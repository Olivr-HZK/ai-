"""
从竞品手册 CSV 文件的"竞品"列中提取竞品信息，添加到 ai_product.json

用法: python scripts/extract_competitors_from_csv.py
"""
import csv
import json
import sys
from pathlib import Path
from collections import defaultdict

from path_util import CONFIG_DIR

CSV_FILE = CONFIG_DIR / "产品手册_AI工具类_表格 2.csv"
JSON_FILE = CONFIG_DIR / "ai_product.json"


def parse_csv() -> dict:
    """解析 CSV 文件，从"竞品"列提取竞品，按产品分类组织"""
    competitors_by_category = defaultdict(set)
    
    if not CSV_FILE.exists():
        print(f"[错误] 未找到 {CSV_FILE.name}", file=sys.stderr)
        sys.exit(1)
    
    with open(CSV_FILE, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            category = row.get("分类", "").strip()
            competitors_str = row.get("竞品", "").strip()
            
            # 跳过空行或无效数据
            if not category or not competitors_str or category == "无匹配类别":
                continue
            
            # 解析竞品列表（用逗号分隔）
            competitors = [c.strip() for c in competitors_str.split(",") if c.strip()]
            
            # 添加到对应分类
            for competitor in competitors:
                competitors_by_category[category].add(competitor)
                print(f"[发现] [{category}] 竞品: {competitor}")
    
    # 转换为字典格式
    result = {}
    for category, competitors in competitors_by_category.items():
        result[category] = sorted(list(competitors))
    
    return result


def merge_with_existing(new_competitors: dict) -> dict:
    """合并新竞品和现有产品数据"""
    # 读取现有文件
    if JSON_FILE.exists():
        with open(JSON_FILE, encoding="utf-8") as f:
            existing = json.load(f)
    else:
        existing = {}
    
    # 对于每个分类，将竞品添加到现有数据中
    # 注意：竞品只有名称，没有包名，所以我们需要用空字符串或让用户后续补充
    for category, competitors in new_competitors.items():
        if category not in existing:
            existing[category] = {}
        
        # 添加竞品（如果没有包名，先用空字符串占位）
        for competitor in competitors:
            if competitor not in existing[category]:
                existing[category][competitor] = ""  # 包名待补充
                print(f"[添加] [{category}] {competitor} (包名待补充)")
    
    return existing


def main():
    print("=" * 60)
    print("从 CSV 文件的「竞品」列提取竞品信息")
    print("=" * 60)
    
    # 解析 CSV
    print(f"\n[1/3] 解析 CSV 文件: {CSV_FILE.name}")
    new_competitors = parse_csv()
    
    if not new_competitors:
        print("[警告] 未从 CSV 中解析到任何竞品", file=sys.stderr)
        return
    
    print(f"\n[2/3] 合并现有数据...")
    merged = merge_with_existing(new_competitors)
    
    # 保存更新后的 JSON
    print(f"\n[3/3] 保存到 {JSON_FILE.name}...")
    JSON_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    
    # 打印统计
    print("\n" + "=" * 60)
    print("更新完成！")
    print("=" * 60)
    print("\n注意：部分竞品没有包名（显示为空字符串），需要后续手动补充。")
    print("\n各分类统计：")
    for category, products in merged.items():
        products_with_package = sum(1 for pkg in products.values() if pkg)
        products_without_package = sum(1 for pkg in products.values() if not pkg)
        print(f"  [{category}]: {len(products)} 个产品")
        print(f"    - 有包名: {products_with_package} 个")
        print(f"    - 无包名: {products_without_package} 个")
    print("=" * 60)


if __name__ == "__main__":
    main()
