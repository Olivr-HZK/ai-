"""
基于当前数据库 ad_creative_analysis 表中已有的素材数据，重新跑一遍「灵感分析」，
并在同一条记录上补充/覆盖：
1) llm_analysis（创意拆解 + Hook + 情感）
2) our_products（适用我方产品列表，JSON 数组）
3) our_ua_suggestions（针对各产品的完整 UA 建议，Markdown 文本）

用法（在项目根目录，且已激活虚拟环境）：
  python scripts/reanalyze_creatives_from_db.py
"""

import argparse
import json
import os
import sys
from typing import List

from dotenv import load_dotenv

load_dotenv()

from path_util import CONFIG_DIR
from ua_crawl_db import (
    get_conn,
    init_db,
    update_creative_llm_analysis,
    update_creative_product_suggestions,
    insert_product_suggestions,
)


def _call_llm(system: str, user_content: str) -> str:
    """与 analyze_creatives_with_llm 保持一致的多模型调用逻辑。"""
    from openai import OpenAI

    # 1. OpenRouter 多模态链：gemini → kimi → qwen-vl
    api_key = os.getenv("OPENROUTER_API_KEY")
    if api_key:
        client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=api_key)
        primary = os.getenv("OPENROUTER_MODEL", "google/gemini-2.5-flash")
        fallback_kimi = "moonshotai/kimi-k2.5"
        fallback_qwen = "qwen/qwen2.5-vl-32b-instruct"

        last_err: Exception | None = None
        for m in (primary, fallback_kimi, fallback_qwen):
            try:
                r = client.chat.completions.create(
                    model=m,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user_content},
                    ],
                )
                return (r.choices[0].message.content or "").strip()
            except Exception as e:  # 尝试下一个模型
                last_err = e
                continue
        if last_err is not None:
            raise RuntimeError(f"OpenRouter 多模型调用失败: {last_err}")

    # 2. 回退到直连 OpenAI
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        try:
            client = OpenAI(api_key=api_key, base_url=os.getenv("OPENAI_API_BASE") or None)
            model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
            r = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user_content},
                ],
            )
            return (r.choices[0].message.content or "").strip()
        except Exception as e:
            raise RuntimeError(f"OpenAI 调用失败: {e}")

    raise RuntimeError("请设置 OPENROUTER_API_KEY 或 OPENAI_API_KEY")


def _load_product_manual() -> List[dict]:
    """读取产品手册 CSV，返回产品列表。"""
    import csv

    csv_path = CONFIG_DIR / "产品手册_AI工具类_表格 2.csv"
    if not csv_path.exists():
        return []
    products: List[dict] = []
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("名称") or "").strip()
            internal = (row.get("内部名称") or "").strip()
            if not name and not internal:
                continue
            products.append(
                {
                    "name": name,
                    "internal_name": internal,
                    "category": (row.get("分类") or "").strip(),
                    "desc": (row.get("产品描述") or "").strip(),
                    "competitors": (row.get("竞品") or "").strip(),
                }
            )
    return products


def _build_creative_prompt(row: dict) -> str:
    """基于 ad_creative_analysis 一行，构造创意拆解 prompt。"""
    title_zh = (row.get("title_zh") or "").strip()
    body_zh = (row.get("body_zh") or "").strip()
    title = title_zh or (row.get("title") or "")
    body = body_zh or (row.get("body") or "")

    video_url = row.get("video_url") or "无"
    return f"""请基于以下 UA 广告素材信息，输出「广告创意拆解」「Hook」「情感」三部分（中文）：

- 分类/产品: {row.get('category', '')} / {row.get('product', '')}
- 广告主: {row.get('advertiser_name', '')}
- 标题（中文）: {title or '无'}
- 文案/描述（中文）: {body or '无'}
- 投放平台: {row.get('platform', '')}
- 视频时长: {row.get('video_duration', 0)} 秒
- 视频链接: {video_url}
- CTA: {row.get('call_to_action', '')}
- 展示估值: {row.get('all_exposure_value', 0)}
- 热度: {row.get('heat', 0)}

请直接输出分析内容，不要多余说明。"""


def _build_product_sugg_prompt(row: dict, product_manual: List[dict]) -> str:
    """构造对单条素材做产品级 UA 建议的 prompt。"""
    pm_lines = []
    for p in product_manual:
        pm_lines.append(
            f"- 内部名称: {p['internal_name'] or p['name']} | 名称: {p['name']} | 分类: {p['category']} | 描述: {p['desc']} | 竞品: {p['competitors']}"
        )
    pm_text = "\n".join(pm_lines)

    title_zh = (row.get("title_zh") or "").strip()
    body_zh = (row.get("body_zh") or "").strip()
    title = title_zh or (row.get("title") or "")
    body = body_zh or (row.get("body") or "")
    video_url = row.get("video_url") or "无"

    return f"""
以下是我方产品手册摘要（每行为一款产品）：
{pm_text}

下面是一条竞品 UA 素材（已翻译成中文）：
- 分类/产品: {row.get('category', '')} / {row.get('product', '')}
- 广告主: {row.get('advertiser_name', '')}
- 标题（中文）: {title or '无'}
- 文案/描述（中文）: {body or '无'}
- 投放平台: {row.get('platform', '')}
- 视频时长: {row.get('video_duration', 0)} 秒
- 视频链接: {video_url}
- CTA: {row.get('call_to_action', '')}
- 展示估值: {row.get('all_exposure_value', 0)}
- 热度: {row.get('heat', 0)}

请思考：这条素材适合我方哪些产品使用？针对每个适合的产品，给出完整 UA 建议。

【输出要求】
1. 只输出一个 JSON 字符串，结构如下（注意键名不要改）：
{{
  "products": [
    {{
      "our_product": "我方产品的内部名称或名称（优先内部名称）",
      "ua_suggestion": "一整段建议，内部可分点：创意方向 / 可复用文案 / 投放策略 / 避坑提示"
    }}
  ]
}}
2. 如果没有任何适合的产品，输出 {{\"products\": []}}。
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="最多处理多少条素材（默认全部）",
    )
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="只处理还没有 our_products/our_ua_suggestions 的记录",
    )
    args = parser.parse_args()

    init_db()
    conn = get_conn()
    try:
        where = "WHERE 1=1"
        if args.only_missing:
            where += " AND (our_products IS NULL OR our_products = '' OR our_ua_suggestions IS NULL OR our_ua_suggestions = '')"
        sql = f"""
        SELECT
          ad_key,
          crawl_date,
          category,
          product,
          advertiser_name,
          title,
          body,
          title_zh,
          body_zh,
          platform,
          video_url,
          video_duration,
          preview_img_url,
          selected_json,
          llm_analysis,
          our_products,
          our_ua_suggestions
        FROM ad_creative_analysis
        {where}
        ORDER BY crawl_date, ad_key
        """
        cur = conn.execute(sql)
        rows = cur.fetchall()
    finally:
        conn.close()

    total = len(rows)
    if args.limit is not None:
        rows = rows[: args.limit]
    print(f"共 {total} 条记录，实际将处理 {len(rows)} 条。")
    if not rows:
        return

    sys_analysis = (
        "你是广告创意分析专家。根据提供的广告素材信息，用中文输出："
        "1) 广告创意拆解 2) Hook（前几秒/前几句的抓人点）3) 情感基调。结构清晰、分点列出。"
    )
    sys_prod_sugg = (
        "你是 UA 策略和创意专家。"
        "根据提供的广告素材信息和我方产品手册，为这条素材找出适合的我方产品，"
        "并针对每个产品输出完整 UA 建议。你最终只输出 JSON，不要多余说明。"
    )

    product_manual = _load_product_manual()
    if not product_manual:
        print("警告：未找到产品手册 CSV，将只重算 llm_analysis，不写入产品建议。")

    for idx, r in enumerate(rows, 1):
        row = dict(r)
        ad_key = row.get("ad_key")
        if not ad_key:
            continue
        print(f"[{idx}/{len(rows)}] {ad_key[:12]}... {row.get('product', '')}")
        try:
            # 1) 重新生成创意拆解 / Hook / 情感
            prompt_creative = _build_creative_prompt(row)
            creative_content = _call_llm(sys_analysis, prompt_creative)
            update_creative_llm_analysis(ad_key, creative_content)
            print("  ✓ 已更新 llm_analysis")

            # 2) 生成并写入产品级 UA 建议
            if product_manual:
                try:
                    prompt_sugg = _build_product_sugg_prompt(row, product_manual)
                    raw = _call_llm(sys_prod_sugg, prompt_sugg)
                    # 去掉 Markdown 代码块包裹（```json ... ```），保留纯 JSON
                    cleaned = "\n".join(
                        line for line in raw.splitlines() if not line.strip().startswith("```")
                    ).strip()
                    try:
                        data = json.loads(cleaned)
                    except Exception:
                        # 打印原始返回，方便排查 JSON 解析失败原因
                        print("  原始模型返回内容（JSON 解析失败）:")
                        print("  ---")
                        for line in raw.splitlines():
                            print("  ", line)
                        print("  ---")
                        raise
                    items = data.get("products") or []
                    our_products: List[str] = []
                    md_parts: List[str] = []
                    sugg_rows: List[dict] = []
                    for item in items:
                        name = (item.get("our_product") or "").strip()
                        sugg = (item.get("ua_suggestion") or "").strip()
                        if not name or not sugg:
                            continue
                        our_products.append(name)
                        md_parts.append(f"### {name}\n\n{sugg}\n")
                        sugg_rows.append(
                            {
                                "our_product": name,
                                "ua_suggestion": sugg,
                            }
                        )
                    if our_products and md_parts:
                        # 写入独立建议表（逐产品一行）
                        insert_product_suggestions(ad_key, sugg_rows)
                        # 同时在主表上更新汇总字段（便于报表/多维表使用）
                        update_creative_product_suggestions(
                            ad_key,
                            our_products=our_products,
                            our_ua_suggestions="\n".join(md_parts),
                        )
                        print("  ✓ 已写入产品建议:", ", ".join(our_products))
                    else:
                        print("  - 模型未返回任何适用产品（products 为空）")
                except Exception as e:
                    print(f"  （忽略）产品级 UA 建议生成失败: {e}", file=sys.stderr)
        except Exception as e:
            print(f"  ✗ 处理失败: {e}", file=sys.stderr)

    print("全部完成。")


if __name__ == "__main__":
    main()

