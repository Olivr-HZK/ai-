"""
每周获取到的广告中，对有视频的条目调用大模型分析：
1) 广告创意拆解 + Hook + 情感（写入 ad_creative_analysis.llm_analysis）
2) 基于产品手册的、按素材的我方产品 UA 建议（写入 ad_creative_analysis.our_products / our_ua_suggestions）

数据来源：ai_products_crawl 表（按爬取日期或近 N 天）
用法:
  python scripts/analyze_creatives_with_llm.py --date 2026-02-26
  python scripts/analyze_creatives_with_llm.py --days 7
"""
import argparse
import json
import os
import sys

from dotenv import load_dotenv

load_dotenv()

from path_util import CONFIG_DIR
from ua_crawl_db import (
    get_conn,
    init_db,
    query_by_date,
    update_creative_llm_analysis,
    update_creative_product_suggestions,
    insert_product_suggestions,
    upsert_creative,
    upsert_llm_usage,
    touch_creative_updated_at,
)


def _has_video(selected: dict) -> bool:
    if not selected:
        return False
    if selected.get("video_duration") and selected.get("video_duration") > 0:
        return True
    for r in selected.get("resource_urls") or []:
        if r.get("video_url"):
            return True
    return False


_USAGE_STATS: dict[tuple[str, str], dict[str, int]] = {}


def _accumulate_usage(provider: str, model: str, usage) -> None:
    """
    累加一次 LLM 调用的 usage（prompt/completion/total tokens）。
    若返回体中不含 usage，则直接忽略。
    """
    if usage is None:
        return
    try:
        prompt = int(getattr(usage, "prompt_tokens", 0) or 0)
        completion = int(getattr(usage, "completion_tokens", 0) or 0)
        total = int(getattr(usage, "total_tokens", prompt + completion) or 0)
    except Exception:
        return
    key = (provider, model)
    stat = _USAGE_STATS.setdefault(
        key,
        {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    )
    stat["prompt_tokens"] += prompt
    stat["completion_tokens"] += completion
    stat["total_tokens"] += total


def _call_llm(system: str, user_content: str) -> str:
    """优先走 OpenRouter: gemini → kimi → qwen，多模态模型级联；失败再回退到直连 OpenAI。"""
    from openai import OpenAI

    # 1. OpenRouter 多模态链：gemini → kimi → qwen-vl
    api_key = os.getenv("OPENROUTER_API_KEY")
    if api_key:
        client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=api_key)
        # 主模型：Gemini 2.5 flash
        primary = os.getenv("OPENROUTER_MODEL", "google/gemini-2.5-flash")
        # 回退1：Kimi 2.5（多模态）
        fallback_kimi = "moonshotai/kimi-k2.5"
        # 回退2：Qwen2.5 VL 32B Instruct（多模态）
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
                # 累加 usage，不影响正常逻辑
                try:
                    _accumulate_usage("openrouter", m, getattr(r, "usage", None))
                except Exception:
                    pass
                return (r.choices[0].message.content or "").strip()
            except Exception as e:  # 尝试下一个模型
                last_err = e
                continue
        if last_err is not None:
            raise RuntimeError(f"OpenRouter 多模型调用失败: {last_err}")

    # 2. 回退到直连 OpenAI（纯文本，但足够做翻译和分析）
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
            try:
                _accumulate_usage("openai", model, getattr(r, "usage", None))
            except Exception:
                pass
            return (r.choices[0].message.content or "").strip()
        except Exception as e:
            raise RuntimeError(f"OpenAI 调用失败: {e}")

    raise RuntimeError("请设置 OPENROUTER_API_KEY 或 OPENAI_API_KEY")


def _video_url_from_selected(selected: dict) -> str | None:
    """从 selected 中提取视频 URL（若有）。"""
    if not isinstance(selected, dict):
        return None
    if selected.get("video_url"):
        return str(selected["video_url"])
    for r in selected.get("resource_urls") or []:
        if r.get("video_url"):
            return str(r["video_url"])
    return None


def _call_llm_video(user_content: str, video_url: str) -> str:
    """
    使用 OpenRouter 的视频模型（OPENROUTER_VIDEO_MODEL）进行视频向分析。
    若未配置 OPENROUTER_VIDEO_MODEL，则回退到文本模型。
    """
    from openai import OpenAI

    api_key = os.getenv("OPENROUTER_API_KEY")
    video_model = os.getenv("OPENROUTER_VIDEO_MODEL")
    if not api_key or not video_model:
        # 未配置视频模型时，回退到文本模型
        return _call_llm(
            "你是 UA 视频创意分析专家。虽然你可能无法直接看到视频，但请根据给定信息尽量给出合理分析。",
            user_content,
        )

    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
    )
    system = (
        "你是资深 UA 视频创意分析专家，擅长从视频内容本身（画面/镜头/节奏）和提供的文字信息中拆解玩法与转化逻辑。"
        "请严格按照用户给出的结构化输出要求，用简体中文给出可执行的建议。"
    )
    r = client.chat.completions.create(
        model=video_model,
        messages=[
            {"role": "system", "content": system},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_content},
                    {
                        "type": "video_url",
                        "video_url": {
                            "url": str(video_url),
                        },
                    },
                ],
            },
        ],
    )
    try:
        _accumulate_usage("openrouter-video", video_model, getattr(r, "usage", None))
    except Exception:
        pass
    return (r.choices[0].message.content or "").strip()


def _load_product_manual() -> list[dict]:
    """
    读取产品手册 CSV（产品手册_AI工具类_表格 2.csv），返回产品列表。
    关心字段：名称 / 内部名称 / 分类 / 产品描述 / 竞品。
    """
    import csv

    csv_path = CONFIG_DIR / "产品手册_AI工具类_表格 2.csv"
    if not csv_path.exists():
        return []
    products: list[dict] = []
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


def _translate_title_body(title: str, body: str) -> tuple[str, str]:
    """将广告标题和正文翻译成中文，返回 (title_zh, body_zh)。"""
    title = (title or "").strip()
    body = (body or "").strip()
    if not title and not body:
        return "", ""
    prompt = f"""请将以下广告文案翻译成简体中文。若已是中文则做适当润色即可。
只输出两行：第一行以「标题：」开头写标题中文；第二行以「正文：」开头写正文中文。不要其他解释。

标题原文：{title or '（无）'}
正文原文：{body or '（无）'}"""
    system = "你只输出两行：标题：xxx 和 正文：xxx，不要其他内容。"
    out = _call_llm(system, prompt)
    title_zh = ""
    body_zh = ""
    for line in out.split("\n"):
        line = line.strip()
        if line.startswith("标题：") or line.startswith("标题:"):
            title_zh = line.split("：", 1)[-1].split(":", 1)[-1].strip()
        elif line.startswith("正文：") or line.startswith("正文:"):
            body_zh = line.split("：", 1)[-1].split(":", 1)[-1].strip()
    return title_zh, body_zh


def build_prompt(
    category: str,
    product: str,
    selected: dict,
    title_zh: str = "",
    body_zh: str = "",
) -> str:
    video_url = None
    for r in selected.get("resource_urls") or []:
        if r.get("video_url"):
            video_url = r["video_url"]
            break
    title_display = title_zh or selected.get("title", "") or "无"
    body_display = body_zh or selected.get("body", "") or "无"
    return f"""请基于以下 UA 广告素材信息，输出「广告创意拆解」「Hook」「情感」三部分（中文）：

- 分类/产品: {category} / {product}
- 广告主: {selected.get('advertiser_name', '')}
- 标题（中文）: {title_display}
- 文案/描述（中文）: {body_display}
- 投放平台: {selected.get('platform', '')}
- 视频时长: {selected.get('video_duration', 0)} 秒
- 视频链接: {video_url or '无'}
- CTA: {selected.get('call_to_action', '')}
- 展示/热度: 展示估值 {selected.get('all_exposure_value', 0)}，热度 {selected.get('heat', 0)}

请直接输出分析内容，不要多余说明。"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="爬取日期 YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=None, help="最近 N 天的爬取数据")
    parser.add_argument("--dry-run", action="store_true", help="只列出待分析条数，不调 LLM")
    parser.add_argument("--ours", action="store_true", help="仅分析我方产品素材：只做广告拆解，不做 UA 建议")
    args = parser.parse_args()

    if not args.date and args.days is None:
        args.days = 7

    init_db()
    conn = get_conn()
    try:
        if args.date:
            dates = [args.date]
        else:
            if args.ours:
                cur = conn.execute(
                    "SELECT DISTINCT crawl_date FROM ai_products_crawl WHERE is_our_product = 1 ORDER BY crawl_date DESC LIMIT ?",
                    (max(1, args.days),),
                )
            else:
                cur = conn.execute(
                    "SELECT DISTINCT crawl_date FROM ai_products_crawl WHERE COALESCE(is_our_product, 0) = 0 ORDER BY crawl_date DESC LIMIT ?",
                    (max(1, args.days),),
                )
            dates = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    if not dates:
        print("没有找到爬取数据")
        return

    # 仅对「之前从未进入灵感分析表的素材」做分析：
    # 通过 ad_creative_analysis 是否已有该 ad_key 来判断，避免重复跑历史素材。
    existing_ad_keys: set[str] = set()
    conn = get_conn()
    try:
        cur = conn.execute("SELECT ad_key FROM ad_creative_analysis")
        for r in cur.fetchall():
            if r[0]:
                existing_ad_keys.add(str(r[0]))
    finally:
        conn.close()

    is_our = 1 if args.ours else 0
    rows_to_analyze = []
    video_count = 0
    image_only_count = 0
    skipped_existing = 0
    for d in dates:
        for row in query_by_date(d, is_our_product=is_our):
            sel = row.get("selected")
            if not sel:
                continue
            ad_key = (sel.get("ad_key") or "").strip()
            if not ad_key:
                continue
            if ad_key in existing_ad_keys:
                # 已存在灵感分析表的素材：不重跑 LLM，但刷新 updated_at 以标记今日已处理
                try:
                    touch_creative_updated_at(ad_key)
                except Exception:
                    pass
                skipped_existing += 1
                continue
            rows_to_analyze.append((d, row))
            existing_ad_keys.add(ad_key)
            if _has_video(sel):
                video_count += 1
            else:
                image_only_count += 1

    label = "我方" if args.ours else "竞品"
    print(
        f"共找到 {len(rows_to_analyze) + skipped_existing} 条原始素材（{label}，日期: {dates}）；"
        f"其中已存在灵感分析表中的素材 {skipped_existing} 条，本次将新分析 {len(rows_to_analyze)} 条："
        f"视频素材 {video_count} 条，纯图片/其他 {image_only_count} 条。"
    )
    if args.dry_run:
        return

    sys_analysis = "你是广告创意分析专家。根据提供的广告素材信息，用中文输出：1) 广告创意拆解 2) Hook（前几秒/前几句的抓人点）3) 情感基调。结构清晰、分点列出。"
    sys_prod_sugg = (
        "你是 UA 策略和创意专家。"
        "根据提供的广告素材信息和我方产品手册，为这条素材找出适合的我方产品，"
        "并针对每个产品输出完整 UA 建议。你最终只输出 JSON，不要多余说明。"
    )

    product_manual = _load_product_manual()

    for i, (crawl_date, row) in enumerate(rows_to_analyze, 1):
        category = row.get("category", "")
        product = row.get("product", "")
        selected = row.get("selected") or {}
        ad_key = selected.get("ad_key")
        if not ad_key:
            continue
        print(f"[{i}/{len(rows_to_analyze)}] {ad_key[:12]}... {product}")
        try:
            # 1) 标题、正文翻译成中文
            title_zh, body_zh = _translate_title_body(
                selected.get("title") or "",
                selected.get("body") or "",
            )
            if title_zh or body_zh:
                print(f"  翻译: 标题_zh={title_zh[:40]}..." if len(title_zh) > 40 else f"  翻译: 标题_zh={title_zh}")
            # 2) 入库（含中文翻译）
            upsert_creative(
                ad_key=ad_key,
                crawl_date=crawl_date,
                category=category,
                product=product,
                selected=selected,
                llm_analysis=None,
                title_zh=title_zh or None,
                body_zh=body_zh or None,
            )
            # 3) 用原视频 + 中文标题/正文做创意分析：
            #    - 若有 video_url，则优先走视频模型（OPENROUTER_VIDEO_MODEL）
            #    - 若无，则回退到文本模型（sys_analysis）
            video_url = _video_url_from_selected(selected)
            if video_url:
                video_prompt = f"""
以下是一条竞品 UA 视频素材的关键信息：
- 分类/产品: {category} / {product}
- 广告主: {selected.get('advertiser_name', '')}
- 视频链接: {video_url}
- 标题（中文）: {title_zh or selected.get('title') or '无'}
- 文案/描述（中文）: {body_zh or selected.get('body') or '无'}
- 投放平台: {selected.get('platform', '')}
- 视频时长: {selected.get('video_duration', 0)} 秒
- CTA: {selected.get('call_to_action', '')}
- 展示估值: {selected.get('all_exposure_value', 0)}
- 热度: {selected.get('heat', 0)}

请你仅基于「视频内容本身 + 以上文字信息」，从 UA 视角进行深入解析，输出：
1) 广告创意拆解
2) Hook（前几秒/前几句的抓人点）
3) 情感基调
4) 可复用的 UA 素材建议（给出清晰结构和可执行要点）

使用简体中文，结构清晰、分点列出。
"""
                content = _call_llm_video(video_prompt, video_url)
            else:
                # 无视频链接时，仍使用原有文本分析逻辑
                prompt = build_prompt(
                    category,
                    product,
                    selected,
                    title_zh=title_zh,
                    body_zh=body_zh,
                )
                content = _call_llm(sys_analysis, prompt)

            update_creative_llm_analysis(ad_key, content)
            print(f"  ✓ 已写入 title_zh/body_zh + llm_analysis")

            # 4) 仅竞品素材做 UA 建议；我方素材只做拆解，不做建议
            if args.ours:
                continue
            if product_manual:
                try:
                    # 构造产品手册摘要文本
                    pm_lines = []
                    for p in product_manual:
                        pm_lines.append(
                            f"- 内部名称: {p['internal_name'] or p['name']} | 名称: {p['name']} | 分类: {p['category']} | 描述: {p['desc']} | 竞品: {p['competitors']}"
                        )
                    pm_text = "\n".join(pm_lines)

                    video_url = None
                    for r in selected.get("resource_urls") or []:
                        if r.get("video_url"):
                            video_url = r["video_url"]
                            break

                    sugg_prompt = f"""
以下是我方产品手册摘要（每行为一款产品）：
{pm_text}

下面是一条竞品 UA 素材（已翻译成中文）：
- 分类/产品: {category} / {product}
- 广告主: {selected.get('advertiser_name', '')}
- 标题（中文）: {title_zh or selected.get('title') or '无'}
- 文案/描述（中文）: {body_zh or selected.get('body') or '无'}
- 投放平台: {selected.get('platform', '')}
- 视频时长: {selected.get('video_duration', 0)} 秒
- 视频链接: {video_url or '无'}
- CTA: {selected.get('call_to_action', '')}
- 展示估值: {selected.get('all_exposure_value', 0)}
- 热度: {selected.get('heat', 0)}

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
                    raw = _call_llm(sys_prod_sugg, sugg_prompt)
                    # 去掉 Markdown 代码块包裹（```json ... ```），保留纯 JSON
                    cleaned = "\n".join(
                        line for line in raw.splitlines() if not line.strip().startswith("```")
                    ).strip()
                    data = json.loads(cleaned)
                    items = data.get("products") or []
                    our_products: list[str] = []
                    md_parts: list[str] = []
                    sugg_rows: list[dict] = []
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
                        print(f"  ✓ 已写入我方产品 UA 建议: {', '.join(our_products)}")
                except Exception as e:
                    print(f"  （忽略）产品级 UA 建议生成失败: {e}", file=sys.stderr)
        except Exception as e:
            print(f"  ✗ {e}", file=sys.stderr)

    # LLM 分析完成后，输出并记录本次 run 的 usage 统计（按模型/提供方聚合）
    if _USAGE_STATS:
        print("\nLLM usage 统计（按提供方/模型）：")
        usage_for_db: dict[str, dict[str, int]] = {}
        for (provider, model), stat in _USAGE_STATS.items():
            key = f"{provider}:{model}"
            usage_for_db[key] = {
                "prompt_tokens": stat.get("prompt_tokens", 0),
                "completion_tokens": stat.get("completion_tokens", 0),
                "total_tokens": stat.get("total_tokens", 0),
            }
            print(
                f"  - {key}: "
                f"prompt={stat.get('prompt_tokens', 0)}, "
                f"completion={stat.get('completion_tokens', 0)}, "
                f"total={stat.get('total_tokens', 0)}"
            )
        # 选择一个代表日期写入 usage 表：若指定 --date，则用该日期；否则用处理到的最新日期
        if args.date:
            usage_date = args.date
        else:
            usage_date = dates[0]
        try:
            upsert_llm_usage(usage_date, json.dumps(usage_for_db, ensure_ascii=False))
        except Exception as e:
            print(f"（忽略）写入 LLM usage 统计失败: {e}", file=sys.stderr)

    print("完成。")


if __name__ == "__main__":
    main()
