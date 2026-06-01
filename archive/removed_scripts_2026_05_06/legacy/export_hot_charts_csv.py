"""
从 hot_charts_yizhi_creatives.json 导出：游戏名、开发者、平台、热度、展示估值、本周人气、投放天数、图片URL、视频URL
保存为 CSV：每周益智人气榜.csv
"""

import csv
import json


def main():
    from path_util import DATA_DIR
    json_path = DATA_DIR / "hot_charts_yizhi_creatives.json"
    csv_path = DATA_DIR / "每周益智人气榜.csv"

    if not json_path.exists():
        print(f"未找到 {json_path.name}，请先运行 scrape_guangdada_hot_charts.py 获取数据")
        return

    with open(json_path, "r", encoding="utf-8") as f:
        creatives = json.load(f)

    if not creatives:
        print("创意列表为空")
        return

    # 表头与字段映射
    headers = [
        "游戏名",
        "开发者",
        "平台",
        "热度",
        "展示估值",
        "本周人气",
        "投放天数",
        "图片URL",
        "视频URL",
    ]

    def image_url(c):
        urls = c.get("resource_urls") or []
        if urls and isinstance(urls[0], dict) and urls[0].get("image_url"):
            return urls[0].get("image_url", "")
        return c.get("preview_img_url") or ""

    def video_url(c):
        urls = c.get("resource_urls") or []
        if urls and isinstance(urls[0], dict):
            return urls[0].get("video_url", "")
        return ""

    rows = []
    for c in creatives:
        rows.append([
            c.get("advertiser_name") or c.get("page_name") or "",
            c.get("app_developer") or "",
            c.get("platform") or "",
            c.get("heat") or 0,
            c.get("all_exposure_value") or 0,
            c.get("new_week_exposure_value") or 0,
            c.get("days_count") or 0,
            image_url(c),
            video_url(c),
        ])

    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)

    print(f"已导出 {len(rows)} 条 → {csv_path.name}")


if __name__ == "__main__":
    main()
