"""
根据 ai_products_ua_results.json 调用大模型生成 AI 产品 UA 素材日报。

日报内容：
1. 各分类各产品的 UA 素材介绍
2. 我方可用的 UA 素材灵感分析

用法: python generate_ai_products_report.py
优先使用 OpenRouter 中转（需 OPENROUTER_API_KEY），也可用 OPENAI_API_KEY
"""
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from path_util import DATA_DIR, REPORTS_DIR, DOWNLOADS_DIR

INPUT_FILE = DATA_DIR / "ai_products_ua_results.json"
OUTPUT_FILE = REPORTS_DIR / "ai_products_report_daily.md"


def safe_filename(s: str) -> str:
    """生成安全的文件名"""
    return re.sub(r'[^\w\-.]', '_', s)[:80]


def find_downloaded_files(category: str, product: str, ad_key: str) -> dict:
    """查找已下载的素材文件"""
    safe_category = safe_filename(category)
    safe_product = safe_filename(product)
    ad_key_short = ad_key[:12] if ad_key else ""
    files = {"cover": None, "logo": None, "video": None}
    if DOWNLOADS_DIR.exists():
        # 匹配模式：分类_产品_adkey_类型.扩展名
        patterns = [
            f"{safe_category}_{safe_product}_{ad_key_short}_cover.*",
            f"{safe_category}_{safe_product}_{ad_key_short}_logo.*",
            f"{safe_category}_{safe_product}_{ad_key_short}_video.*",
        ]
        for pattern in patterns:
            matches = list(DOWNLOADS_DIR.glob(pattern))
            if matches:
                f = matches[0]
                # 报告在 reports/ 下，图片路径相对报告为 ../ua_downloads/
                rel = f"../ua_downloads/{f.name}"
                if "_cover" in f.name:
                    files["cover"] = rel
                elif "_logo" in f.name:
                    files["logo"] = rel
                elif "_video" in f.name:
                    files["video"] = rel
    return files


def build_summary(data: dict) -> tuple[str, dict]:
    """构建发给 LLM 的素材摘要，返回 (摘要文本, 素材映射)"""
    lines = []
    assets_map = {}
    for p in data.get("products", []):
        category = p.get("category", "")
        product = p.get("product", "")
        sel = p.get("selected") or {}
        ad_key = sel.get("ad_key", "")[:12] if sel.get("ad_key") else ""
        files = find_downloaded_files(category, product, ad_key)
        video_url = None
        for r in sel.get("resource_urls", []):
            if r.get("video_url"):
                video_url = r["video_url"]
                break
        assets_map[f"{category}_{product}"] = {
            "files": files,
            "video_url": video_url,
            "ad_key": ad_key,
        }
        lines.append(f"""
【{category} - {product}】
- 广告主: {sel.get('advertiser_name', '')}
- 素材类型: {'视频' if sel.get('video_duration') else '图片'}
- 视频时长: {sel.get('video_duration', 0)}秒
- 投放天数: {sel.get('days_count', 0)}天
- 展示估值: {sel.get('all_exposure_value', 0)}
- 文案/卖点: {sel.get('body', '') or sel.get('title', '') or '无'}
- CTA: {sel.get('call_to_action', '')}
- 投放平台: {sel.get('platform', '')}
- 热度: {sel.get('heat', 0)}
- 视频URL: {video_url or '无'}
- 已下载素材: 封面图={files['cover'] is not None}, Logo={files['logo'] is not None}, 视频={files['video'] is not None}
""")
    return "\n".join(lines).strip(), assets_map


def call_llm(prompt: str) -> str:
    """调用大模型 API，优先 OpenRouter"""
    from openai import OpenAI

    # 1. OpenRouter 中转（性价比高）
    api_key = os.getenv("OPENROUTER_API_KEY")
    if api_key:
        try:
            client = OpenAI(
                base_url="https://openrouter.ai/api/v1",
                api_key=api_key,
            )
            # 性价比模型：gemini-2.5-flash 速度快、成本低
            model = os.getenv(
                "OPENROUTER_MODEL",
                "google/gemini-2.5-flash",
            )
            r = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "你是 UA 素材分析专家，擅长解读竞品广告素材并提炼可复用的灵感。输出使用中文，结构清晰。"},
                    {"role": "user", "content": prompt},
                ],
            )
            return r.choices[0].message.content or ""
        except Exception as e:
            raise RuntimeError(f"OpenRouter API 调用失败: {e}")

    # 2. 直连 OpenAI
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        try:
            client = OpenAI(
                api_key=api_key,
                base_url=os.getenv("OPENAI_API_BASE") or None,
            )
            r = client.chat.completions.create(
                model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
                messages=[
                    {"role": "system", "content": "你是 UA 素材分析专家，擅长解读竞品广告素材并提炼可复用的灵感。输出使用中文，结构清晰。"},
                    {"role": "user", "content": prompt},
                ],
            )
            return r.choices[0].message.content or ""
        except Exception as e:
            raise RuntimeError(f"OpenAI API 调用失败: {e}")

    raise RuntimeError(
        "请设置 OPENROUTER_API_KEY 或 OPENAI_API_KEY。"
        "推荐 OpenRouter: 在 .env 中添加 OPENROUTER_API_KEY=sk-or-xxx"
    )


def main():
    if not INPUT_FILE.exists():
        print(f"[错误] 未找到 {INPUT_FILE.name}", file=sys.stderr)
        sys.exit(1)

    with open(INPUT_FILE, encoding="utf-8") as f:
        data = json.load(f)

    summary, assets_map = build_summary(data)
    date_str = datetime.now().strftime("%Y-%m-%d")

    prompt = f"""请根据以下竞品 AI 产品 UA 素材数据，生成一份**AI 产品 UA 素材日报**（日期：{date_str}）。

## 素材数据
{summary}

## 日报要求
请按以下结构输出（使用 Markdown 格式）：

### 一、各分类 AI 产品 UA 素材概览
按分类分组，每个产品一段，介绍：
- 素材形式（视频/图片）与时长
- 核心卖点与文案
- 投放平台与表现（展示估值、热度）
- 亮点与特色
- **如果有视频URL，请在介绍中直接包含可点击的视频链接**（格式：[视频链接](视频URL)）
- **在介绍每个产品时，请使用 Markdown 图片语法引用已下载的封面图**（格式：![产品名封面](../ua_downloads/分类_产品_adkey_cover.jpg)）

### 二、我方可用的 UA 素材灵感分析
从以上竞品素材中提炼：
1. **可借鉴的创意方向**（至少 3 条）
2. **可复用的文案/卖点句式**
3. **投放策略建议**（平台、时长、节奏）
4. **避坑提示**（需注意的风险或失效套路）

请直接输出日报正文，不要输出其他说明。"""

    provider = "OpenRouter" if os.getenv("OPENROUTER_API_KEY") else "OpenAI"
    print(f"正在通过 {provider} 调用大模型生成日报...")
    report = call_llm(prompt)

    # 构建素材链接部分（只保留视频URL，移除封面图和Logo）
    assets_section = "\n\n---\n\n## 三、素材资源\n\n"
    for p in data.get("products", []):
        category = p.get("category", "")
        product = p.get("product", "")
        key = f"{category}_{product}"
        assets = assets_map.get(key, {})
        files = assets.get("files", {})
        video_url = assets.get("video_url")
        ad_key = assets.get("ad_key", "")
        
        assets_section += f"### {category} - {product}\n\n"
        if video_url:
            assets_section += f"**视频URL**: [{video_url}]({video_url})\n\n"
        if files.get("video"):
            assets_section += f"**已下载视频**: `{files['video']}`\n\n"
        assets_section += "---\n\n"

    header = f"""# AI 产品 UA 素材日报
**日期**: {date_str}  
**素材来源**: 广大大（{len(data.get('products', []))} 款竞品 AI 产品）

---

"""
    full = header + report + assets_section

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(full)

    print(f"日报已生成 → {OUTPUT_FILE.name}")


if __name__ == "__main__":
    main()
