"""
对 ai_products_crawl 中带视频的广告做翻译 + 创意分析，将结果写入 ad_creative_analysis。

用法:
  python skills/ua_analytics/analyze_creatives_with_llm.py --date 2026-02-26
  python skills/ua_analytics/analyze_creatives_with_llm.py --days 7
"""
import argparse
import os
import sys

from pathlib import Path

from dotenv import load_dotenv

_SKILL_DIR = Path(__file__).resolve().parent
load_dotenv(_SKILL_DIR / ".env")
load_dotenv()

from ua_crawl_db import (  # type: ignore
    get_conn,
    init_db,
    query_by_date,
    update_creative_llm_analysis,
    upsert_creative,
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


def _call_llm(system: str, user_content: str) -> str:
    """优先走 OpenRouter: gemini → kimi → qwen，多模态链；失败再回退到 OpenAI。"""
    from openai import OpenAI

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
            except Exception as e:
                last_err = e
                continue
        if last_err is not None:
            raise RuntimeError(f"OpenRouter 多模型调用失败: {last_err}")

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
    args = parser.parse_args()

    if not args.date and args.days is None:
        args.days = 7

    init_db()
    conn = get_conn()
    try:
        if args.date:
            dates = [args.date]
        else:
            cur = conn.execute(
                "SELECT DISTINCT crawl_date FROM ai_products_crawl ORDER BY crawl_date DESC LIMIT ?",
                (max(1, args.days),),
            )
            dates = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    if not dates:
        print("没有找到爬取数据")
        return

    # 按 ad_key 去重，避免同一广告重复分析
    rows_with_video = []
    seen_keys = set()
    for d in dates:
        for row in query_by_date(d):
            sel = row.get("selected")
            ad_key = (sel or {}).get("ad_key")
            if sel and _has_video(sel) and ad_key and ad_key not in seen_keys:
                seen_keys.add(ad_key)
                rows_with_video.append((d, row))

    print(f"共 {len(rows_with_video)} 条带视频的广告待分析（按 ad_key 去重，日期: {dates}）")
    if args.dry_run:
        return

    sys_analysis = (
        "你是广告创意分析专家。根据提供的广告素材信息，用中文输出："
        "1) 广告创意拆解 2) Hook（前几秒/前几句的抓人点）3) 情感基调。结构清晰、分点列出。"
    )

    for i, (crawl_date, row) in enumerate(rows_with_video, 1):
        category = row.get("category", "")
        product = row.get("product", "")
        selected = row.get("selected") or {}
        ad_key = selected.get("ad_key")
        if not ad_key:
            continue
        print(f"[{i}/{len(rows_with_video)}] {ad_key[:12]}... {product}")
        try:
            title_zh, body_zh = _translate_title_body(
                selected.get("title") or "",
                selected.get("body") or "",
            )
            if title_zh or body_zh:
                msg = title_zh if len(title_zh) <= 40 else title_zh[:40] + "..."
                print(f"  翻译: 标题_zh={msg}")

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

            prompt = build_prompt(category, product, selected, title_zh=title_zh, body_zh=body_zh)
            content = _call_llm(sys_analysis, prompt)
            update_creative_llm_analysis(ad_key, content)
            print("  ✓ 已写入 title_zh/body_zh + llm_analysis")
        except Exception as e:
            print(f"  ✗ {e}", file=sys.stderr)

    print("完成。")


if __name__ == "__main__":
    main()

