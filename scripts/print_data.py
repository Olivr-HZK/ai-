"""
打印订阅广告主列表（名称 + 可点击链接）
"""
import json

from path_util import DATA_DIR

GUANGDADA_BASE_URL = "https://www.guangdada.net"


def build_advertiser_url(adv: dict) -> str:
    """构建广告主链接"""
    # 尝试多种可能的 ID 字段
    adv_id = (
        adv.get("advertiser_id")
        or adv.get("id")
        or adv.get("advertiser_id_cn")
        or adv.get("ecom_advertiser_id")
    )
    
    if adv_id:
        # 假设链接格式为 /advertiser/{id} 或 /advertiser/detail/{id}
        return f"{GUANGDADA_BASE_URL}/advertiser/{adv_id}"
    
    # 如果没有 ID，尝试使用名称（URL 编码）
    name = adv.get("advertiser_name") or adv.get("name") or adv.get("advertiser_name_cn") or ""
    if name:
        from urllib.parse import quote
        return f"{GUANGDADA_BASE_URL}/advertiser/{quote(name)}"
    
    return ""


def main():
    print("=" * 60)
    print("【订阅广告主列表】")
    print("=" * 60)

    sub_file = DATA_DIR / "subscribed_advertisers.json"
    if not sub_file.exists():
        print("  未找到 data/subscribed_advertisers.json，请先运行 scrape_guangdada 或 workflow 捕获")
        return

    with open(sub_file, encoding="utf-8") as f:
        sub_list = json.load(f)
    
    if not sub_list:
        print("  订阅广告主列表为空")
        return

    print(f"\n(来自 data/subscribed_advertisers.json, 共 {len(sub_list)} 个)\n")
    
    # 打印所有广告主（Markdown 格式，方便点击）
    for i, adv in enumerate(sub_list, 1):
        name = (
            adv.get("advertiser_name")
            or adv.get("name")
            or adv.get("advertiser_name_cn")
            or str(adv)[:50]
        )
        url = build_advertiser_url(adv)
        
        if url:
            # Markdown 格式： [名称](链接)
            print(f"{i:3}. [{name}]({url})")
        else:
            # 如果没有链接，只打印名称
            print(f"{i:3}. {name}")


if __name__ == "__main__":
    main()
