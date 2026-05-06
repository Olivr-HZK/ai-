"""
从竞品手册 CSV 文件中读取分类和产品信息，更新 ai_product.json

用法: python scripts/update_ai_products_from_csv.py
"""
import csv
import json
import re
import sys
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from path_util import CONFIG_DIR

CSV_FILE = CONFIG_DIR / "产品手册_AI工具类_表格 2.csv"
JSON_FILE = CONFIG_DIR / "ai_product.json"


def extract_package_name(google_play_url: str) -> str:
    """从 Google Play 链接中提取包名"""
    if not google_play_url or google_play_url == "无":
        return ""
    try:
        # 解析 URL，提取 id 参数
        parsed = urlparse(google_play_url)
        params = parse_qs(parsed.query)
        if "id" in params:
            return params["id"][0]
        # 如果没有查询参数，尝试从路径中提取
        # 例如：/store/apps/details?id=com.example.app
        match = re.search(r'[?&]id=([^&]+)', google_play_url)
        if match:
            return match.group(1)
    except Exception:
        pass
    return ""


def parse_csv() -> dict:
    """解析 CSV 文件，返回按分类组织的产品字典"""
    products_by_category = {}
    
    if not CSV_FILE.exists():
        print(f"[错误] 未找到 {CSV_FILE.name}", file=sys.stderr)
        sys.exit(1)
    
    with open(CSV_FILE, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # 获取分类和产品名称
            category = row.get("分类", "").strip()
            product_name = row.get("Google Play", "").strip() or row.get("名称", "").strip()
            google_play_url = row.get("GooglePlay链接", "").strip()
            
            # 跳过空行或无效数据
            if not category or not product_name or category == "无匹配类别":
                continue
            
            # 提取包名
            package_name = extract_package_name(google_play_url)
            
            # 如果没有包名，跳过（因为 ai_product.json 需要包名）
            if not package_name:
                print(f"[跳过] {product_name} - 无 Google Play 包名")
                continue
            
            # 按分类组织
            if category not in products_by_category:
                products_by_category[category] = {}
            
            # 添加到对应分类
            products_by_category[category][product_name] = package_name
            print(f"[添加] [{category}] {product_name} -> {package_name}")
    
    return products_by_category


def merge_with_existing(new_products: dict) -> dict:
    """合并新产品和现有产品"""
    # 读取现有文件
    if JSON_FILE.exists():
        with open(JSON_FILE, encoding="utf-8") as f:
            existing = json.load(f)
    else:
        existing = {}
    
    # 合并：新产品的分类会覆盖或补充现有分类
    for category, products in new_products.items():
        if category not in existing:
            existing[category] = {}
        # 合并产品（新产品会覆盖同名产品）
        existing[category].update(products)
    
    return existing


def main():
    print("=" * 60)
    print("从 CSV 文件更新 ai_product.json")
    print("=" * 60)
    
    # 解析 CSV
    print(f"\n[1/3] 解析 CSV 文件: {CSV_FILE.name}")
    new_products = parse_csv()
    
    if not new_products:
        print("[错误] 未从 CSV 中解析到任何产品", file=sys.stderr)
        sys.exit(1)
    
    print(f"\n[2/3] 合并现有数据...")
    merged = merge_with_existing(new_products)
    
    # 保存更新后的 JSON
    print(f"\n[3/3] 保存到 {JSON_FILE.name}...")
    JSON_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    
    # 打印统计
    print("\n" + "=" * 60)
    print("更新完成！")
    print("=" * 60)
    for category, products in merged.items():
        print(f"  [{category}]: {len(products)} 个产品")
    print("=" * 60)


if __name__ == "__main__":
    main()
