"""
竞品热门榜（workflow B）：

- 数据来源：广大大搜索页，开启「7天 / 素材 / 人气值Top1%」过滤条件，
  对一批竞品关键词逐一搜索，拉取最近 7 天内「人气值Top1%」的热门素材。
- 范围：仅关注 config/ai_product.json 中定义的两类标签：seek / video enhancer。
- 分析：将各类的热门素材列表传给大模型做「相似度聚类 + 深度解析」，输出结构化 JSON，
  字段直接兼容后续“消息卡片推送模板”的需要。

用法（在项目根目录，已配置 OPENROUTER_API_KEY 或 OPENAI_API_KEY，且 .env 有广大大账号）：

  # 使用 config/ai_product.json 中全部竞品作为关键词
  python scripts/workflow_competitor_hot_rank.py

  # 仅使用前 9 个关键词（用于测试）
  python scripts/workflow_competitor_hot_rank.py --limit-keywords 9

当前脚本只负责：
1) 通过搜索工作流，开启「人气值Top1%」过滤并拉取热门素材
2) 调 LLM 输出聚类+深度解析 JSON
3) 打印一份“热门榜周报（热门榜）”文本到 stdout

工作流 A（每日新晋榜）将单独实现，二者仅在聚类/解析层面共享逻辑。
"""

from __future__ import annotations

import argparse
import json
import os
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv

from path_util import DATA_DIR, CONFIG_DIR

load_dotenv()
DEFAULT_CLUSTER_FALLBACK_MODEL = "qwen/qwen3.5-397b-a17b"


from run_search_workflow import run_batch
from competitor_hot_db import insert_hot_creatives, insert_latest_creatives


@dataclass
class CreativeItem:
    """用于聚类/分析的精简素材结构。"""

    crawl_date: str
    category: str
    product: str
    ad_key: str
    advertiser_name: str
    title: str
    body: str
    platform: str
    video_url: str
    preview_img_url: str
    heat: int
    all_exposure_value: int
    days_count: int
    raw_json: Dict[str, Any]


def _parse_date(s: str | None) -> dt.date:
    if not s:
        return dt.date.today()
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def _load_competitors_from_config() -> List[Dict[str, Any]]:
    """
    从 config/ai_product.json 解析竞品列表。
    返回元素形如：
      {
        "category": "seek",
        "product": "AI Chatbot - Nova",
        "android_appid": "com.scaleup.chatai",
      }
    """
    cfg_path = CONFIG_DIR / "ai_product.json"
    if not cfg_path.exists():
        raise FileNotFoundError(f"未找到配置文件: {cfg_path}")
    with open(cfg_path, encoding="utf-8") as f:
        data = json.load(f)
    items: List[Dict[str, Any]] = []
    for category, products in data.items():
        if not isinstance(products, dict):
            continue
        for name, pkg in products.items():
            if not name:
                continue
            items.append(
                {
                    "category": category,
                    "product": name,
                    "android_appid": pkg,
                }
            )
    return items


def build_hot_candidates_from_search(
    competitors: List[Dict[str, Any]],
    search_results: List[Dict[str, Any]],
) -> Dict[str, List[CreativeItem]]:
    """
    根据 run_search_workflow.run_batch 的结果构建热门候选集合。
    假设调用时已开启「7天 / 素材 / 最新创意」排序（不使用 Top创意 / 人气值Top1% 过滤）。

    返回：
      {
        "seek": [CreativeItem, ...],
        "video enhancer": [CreativeItem, ...],
      }
    """
    dedup: Dict[Tuple[str, str, str], CreativeItem] = {}
    for meta, result in zip(competitors, search_results):
        category = meta["category"]
        product = meta["product"]
        all_creatives = result.get("all_creatives") or []
        for c in all_creatives:
            ad_key = (
                c.get("ad_key")
                or c.get("creative_id")
                or c.get("id")
                or c.get("creativeId")
                or ""
            )
            if not ad_key:
                continue
            key = (category, product, ad_key)
            heat = int(c.get("heat") or 0)
            if key in dedup and heat <= dedup[key].heat:
                continue
            item = CreativeItem(
                crawl_date=dt.date.today().isoformat(),
                category=category,
                product=product,
                ad_key=ad_key,
                advertiser_name=c.get("advertiser_name") or c.get("page_name") or "",
                title=c.get("title") or "",
                body=c.get("body") or "",
                platform=c.get("platform") or "",
                video_url="",
                preview_img_url=c.get("preview_img_url") or "",
                heat=heat,
                all_exposure_value=int(c.get("all_exposure_value") or 0),
                days_count=int(c.get("days_count") or 0),
                raw_json=c,
            )
            # 提取视频 URL
            for r in c.get("resource_urls") or []:
                if r.get("video_url"):
                    item.video_url = r["video_url"]
                    break
            dedup[key] = item

    by_category: Dict[str, List[CreativeItem]] = {"seek": [], "video enhancer": []}
    for item in dedup.values():
        if item.category in by_category:
            by_category[item.category].append(item)

    # 为了后续阅读友好，按 heat 降序
    for cat in by_category:
        by_category[cat].sort(key=lambda x: x.heat, reverse=True)
    return by_category


def _call_llm(system: str, user_content: str) -> str:
    """
    调用大模型：优先 OpenRouter，其次 OpenAI。
    逻辑参考 scripts/analyze_creatives_with_llm.py 中的 _call_llm。
    """
    from openai import OpenAI

    # 1. OpenRouter 多模型链
    api_key = os.getenv("OPENROUTER_API_KEY")
    if api_key:
        client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=api_key)
        primary = os.getenv("OPENROUTER_MODEL", "google/gemini-2.5-flash").strip()
        fallback_kimi = "moonshotai/kimi-k2.5"
        fallback_qwen = os.getenv("OPENROUTER_CLUSTER_FALLBACK_MODEL", DEFAULT_CLUSTER_FALLBACK_MODEL).strip()

        last_err: Exception | None = None
        for model in (primary, fallback_kimi, fallback_qwen):
            try:
                r = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user_content},
                    ],
                )
                return (r.choices[0].message.content or "").strip()
            except Exception as e:  # pragma: no cover - 网络/配额错误路径
                last_err = e
                print(f"[WARN] 聚类模型调用失败。model={model}, reason={e}")
                continue
        if last_err is not None:
            raise RuntimeError(f"OpenRouter 调用失败: {last_err}")

    # 2. 回退到直连 OpenAI
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        client = OpenAI(
            api_key=api_key,
            base_url=os.getenv("OPENAI_API_BASE") or None,
        )
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        r = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_content},
            ],
        )
        return (r.choices[0].message.content or "").strip()

    raise RuntimeError("请在 .env 中配置 OPENROUTER_API_KEY 或 OPENAI_API_KEY。")


def build_cluster_prompt(
    board_title: str,
    category: str,
    items: List[CreativeItem],
    window_desc: str,
) -> str:
    """
    构造聚类 + 深度解析 Prompt。
    要求大模型输出 JSON，字段直接对应需求中 4.2 和 5 的结构。
    """
    lines: List[str] = []
    for idx, it in enumerate(items, 1):
        # 尽量包含标题/文案/标签等，标签从 raw_json 中常见字段里兜底
        tags = it.raw_json.get("tags") or it.raw_json.get("labels") or []
        if isinstance(tags, list):
            tags_str = ", ".join(str(t) for t in tags)
        else:
            tags_str = str(tags)
        # 优先使用视频分析文本作为内容描述，其次是原始描述字段
        desc = (
            it.raw_json.get("video_analysis")
            or it.raw_json.get("description")
            or ""
        )
        lines.append(
            f"""[{idx}]
- ad_key: {it.ad_key}
- 分类: {it.category}
- 竞品: {it.product}
- 广告主: {it.advertiser_name}
- 标题: {it.title or '（无）'}
- 文案/描述: {it.body or desc or '（无）'}
- 标签: {tags_str or '（无）'}
- 投放平台: {it.platform}
- 视频链接: {it.video_url or '（无）'}
- 预览图: {it.preview_img_url or '（无）'}
- 热度 heat: {it.heat}
- 展示估值 all_exposure_value: {it.all_exposure_value}
- 投放天数 days_count: {it.days_count}
- 来源日期 crawl_date: {it.crawl_date}
"""
        )

    items_block = "\n".join(lines)

    user = f"""
你是服务 Guru 的 UA 团队的视频解析顾问和产品对标分析师。

现在给你的是「{board_title}」中的一批 TikTok / 移动广告平台上的 AI 玩法相关热点视频素材，
监控窗口为：{window_desc}，分类为「{category}」。

下面是素材列表（每条以编号 [n] 开头）：

{items_block}

请基于这些素材，按下面要求输出**严格的 JSON**（不要多余说明，不要 Markdown，不要代码块）：

1. 先基于「视频内容总结 + 标题 + 文案/描述 + 标签」计算相似度、进行聚类。
2. 对每个聚类，计算聚类下素材的代表条数、最高播放/热度等指标。
3. 按热度/代表性排序，选出前 2–4 个「关键聚类」。
4. 对每个关键聚类，按以下 5 个部分做**深度解析**：

## 1. 背景（热点内容）
- 用 2–4 句说明：人设/角色 + 行为、使用了什么 AI 特效或工具、画面/情绪/剧情的主要爆点、为什么会成为热点。

## 2. UA 建议
- 围绕「用户上传 1–2 张图即可落地」的玩法，说明：
  - 建议的用户输入（上传几张什么类型的图，如本人自拍/情侣照/宠物等）。
  - 生成的是一张图还是一段视频，一句话概括玩法。
  - 开头 3 秒的画面和钩子文案建议。
  - 生成后的画面与动作示例。
  - 收尾 CTA 建议。

## 3. 产品对标点
- 只说明 Guru 当前能力下**可以直接做**的具体玩法。
- 明确这个玩法生成的结果是什么人物/动作/关系/场景。
- 说明玩法的用户吸引点/传播点。

输出格式（JSON 模式）：

{{
  "category": "{category}",
  "window_desc": "{window_desc}",
  "clusters": [
    {{
      "cluster_id": "字符串，聚类的短标识，例如 '{category}-1'",
      "cluster_title": "该聚类的中文标题，概括玩法/场景",
      "repr_count": 代表视频条数（整数）,
      "max_play": 代表视频的最高播放量或热度（整数，若未知可用 heat 估算）,
      "representative_ad_keys": ["来自上述列表的 ad_key 若干条，作为代表"],
      "analysis": {{
        "background": "1. 背景（热点内容）的完整中文描述",
        "ua_suggestion": "2. UA 建议的完整中文描述",
        "product_points": "3. 产品对标点的完整中文描述",
        "risk": "4. 风险提示：一句话说明版权/合规/品牌等风险；若风险较低，写“当前玩法风险较低，注意常规版权与隐私提示即可”。",
        "trend_label": "5. 趋势阶段判断标签：在 `24H 突发·爆发期`、`3日持续上升`、`持续长红`、`已过峰值`、`24H突发·节日驱动` 中选一个原文输出。",
        "trend_reason": "5. 趋势阶段判断的依据，一句话说明（例如“过去 7 天内同类玩法热度持续上升，并在最近 48 小时集中爆发”）。"
      }}
    }}
  ]
}}

严格要求：
- 只能输出一个 JSON 对象，键名必须与上述格式一致。
- 不要使用 Markdown 代码块（不要 ```json）。
- 字段中的中文描述要具体、可执行，便于直接用到日报与消息卡片中。
"""
    return user


def analyze_clusters_for_category(
    board_title: str,
    category: str,
    items: List[CreativeItem],
    window_desc: str,
) -> Dict[str, Any]:
    if not items:
        return {
            "category": category,
            "window_desc": window_desc,
            "clusters": [],
        }

    prompt = build_cluster_prompt(board_title, category, items, window_desc)
    system = "你是资深 UA 视频创意顾问和产品对标分析师，只输出要求的 JSON 结果。"
    raw = _call_llm(system, prompt)

    # 去掉可能的 ```json 包裹
    cleaned_lines = []
    for line in raw.splitlines():
        t = line.strip()
        if t.startswith("```"):
            continue
        cleaned_lines.append(line)
    cleaned = "\n".join(cleaned_lines).strip()

    try:
        data = json.loads(cleaned)
    except Exception as e:
        raise RuntimeError(f"LLM 返回的 JSON 解析失败: {e}\n原始内容:\n{raw}")
    return data


def build_weekly_hot_report_text(
    end_date: dt.date,
    window_days: int,
    clusters_by_category: Dict[str, Dict[str, Any]],
) -> str:
    """
    根据聚类+深度解析结果，生成「广大大热度监控周报（热门榜）」文本。
    结构严格按照需求说明中的热门榜模版。
    """
    start_date = end_date - dt.timedelta(days=window_days - 1)
    start_str = start_date.strftime("%Y/%m/%d")
    end_str = end_date.strftime("%Y/%m/%d")
    gen_time = dt.datetime.now().strftime("%Y-%m-%d %H:%M")

    # 统计有效高优爆款条数：按聚类数计，或按代表视频计数
    total_clusters = 0
    for data in clusters_by_category.values():
        total_clusters += len(data.get("clusters") or [])

    header_lines = [
        "广大大热度监控周报（热门榜）",
        "",
        "【Metadata】",
        f"- 监控平台：广大大",
        f"- 监控周期：过去 7 天 ({start_str} - {end_str})",
        f"- 有效高优爆款：{total_clusters} 条",
        f"- 生成时间：{gen_time}",
        "",
    ]

    # 趋势概览：对 seek / video enhancer 做简要总结
    # 这里先按照聚类数量和代表视频数做一个 rule-based 概述；如需更复杂可再接 LLM。
    def _summary_for_cat(cat_key: str, display: str) -> str:
        data = clusters_by_category.get(cat_key) or {}
        clusters = data.get("clusters") or []
        if not clusters:
            return f"{display}：本周未监测到明显的高优爆款聚类。"
        total_repr = sum(int(c.get("repr_count") or 0) for c in clusters)
        hot_titles = [c.get("cluster_title") for c in clusters if c.get("cluster_title")]
        hot_titles_str = "、".join(hot_titles[:3])
        return (
            f"{display}：本周共识别 {len(clusters)} 个关键聚类，覆盖代表视频约 {total_repr} 条，"
            f"其中较突出的玩法包括【{hot_titles_str}】等。"
        )

    trend_lines = [
        "【趋势概览】",
        _summary_for_cat("seek", "seek 方向"),
        _summary_for_cat("video enhancer", "video enhancer 方向"),
        "",
    ]

    # 深度分析部分
    analysis_lines = ["【深度分析】", ""]
    for cat_key, display in (("seek", "seek"), ("video enhancer", "video enhancer")):
        data = clusters_by_category.get(cat_key) or {}
        clusters = data.get("clusters") or []
        for c in clusters:
            tag = display
            title = c.get("cluster_title") or "未命名聚类"
            analysis = c.get("analysis") or {}
            repr_count = c.get("repr_count") or 0
            max_play = c.get("max_play") or 0
            bg = analysis.get("background") or ""
            ua = analysis.get("ua_suggestion") or ""
            prod_pts = analysis.get("product_points") or ""
            risk = analysis.get("risk") or ""
            trend_label = analysis.get("trend_label") or ""
            trend_reason = analysis.get("trend_reason") or ""
            ref_keys = c.get("representative_ad_keys") or []
            ref_links: List[str] = []
            # 尝试从 raw_json 中恢复代表视频的链接（best-effort）
            # 这里我们无法直接映射，后续可以在 JSON 中追加映射表；暂以 ad_key 作为引用占位。
            for ak in ref_keys:
                ref_links.append(f"ad_key={ak}")

            analysis_lines.extend(
                [
                    f"- [{tag}] {title}",
                    f"  - 核心数据摘要：代表视频 {repr_count} 条，最高播放/热度约 {max_play}",
                    f"  - 背景：{bg}",
                    f"  - UA 建议：{ua}",
                    f"  - 产品对标点：{prod_pts}",
                    f"  - 风险提示：{risk}",
                    f"  - 趋势阶段判断：{trend_label}（{trend_reason}）",
                    f"  - 参考链接：{'; '.join(ref_links) if ref_links else '（后续可补充素材链接）'}",
                    "",
                ]
            )

    return "\n".join(header_lines + trend_lines + analysis_lines)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="基于广大大搜索页「7天 / 素材 / 最新创意」生成竞品热门榜周报（不使用 Top创意 过滤）。"
    )
    p.add_argument(
        "--date",
        type=str,
        default=None,
        help="统计日期（用于报表展示），格式 YYYY-MM-DD，默认今天。",
    )
    p.add_argument(
        "--limit-keywords",
        type=int,
        default=None,
        help="可选，仅使用前 N 个关键词（用于测试）。默认使用全部竞品。",
    )
    p.add_argument(
        "--output-json",
        type=str,
        default=None,
        help="可选，聚类+深度解析结果 JSON 输出路径，默认 data/competitor_hot_rank_<date>.json。",
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help="以 debug 模式运行浏览器（Playwright 显示窗口，便于检查筛选是否正确）。",
    )
    p.add_argument(
        "--with-llm",
        action="store_true",
        help="是否在爬取+入库之后执行聚类与深度解析（默认只做爬取和入库）。",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    report_date = _parse_date(args.date)

    competitors = _load_competitors_from_config()
    if args.limit_keywords is not None:
        competitors = competitors[: max(1, args.limit_keywords)]
    if not competitors:
        print("配置中未解析到任何竞品。")
        return

    print(f"[1/3] 将对 {len(competitors)} 个竞品执行「7天 / 素材 / 最新创意」搜索（不使用 Top创意 过滤）：")
    for c in competitors:
        print(f"  - [{c['category']}] {c['product']}")

    keywords = [c["product"] for c in competitors]

    # 通过 run_search_workflow 执行实际 Playwright 流程
    print("[2/3] 调用搜索工作流（按「最新创意」排序，不使用 Top创意 过滤）拉取热门素材...")
    # 为简化，这里采用同步入口，内部使用 asyncio.run
    import asyncio

    search_results = asyncio.run(
        run_batch(
            keywords,
            debug=args.debug,
            is_tool=True,
            order_by="latest",
            use_popularity_top1=False,
        )
    )

    # 先只做爬取 + 入库：写入新的 competitor_hot_rank.db
    items_for_db: List[Dict[str, Any]] = []
    for meta, result in zip(competitors, search_results):
        category = meta["category"]
        product = meta["product"]
        android_appid = meta.get("android_appid")
        creatives = result.get("all_creatives") or []
        for c in creatives:
            items_for_db.append(
                {
                    "category": category,
                    "product": product,
                    "android_appid": android_appid,
                    "creative": c,
                }
            )
    inserted = insert_latest_creatives(report_date.isoformat(), items_for_db)
    print(
        f"[2/3] 已将 {inserted} 条按「最新创意」排序的素材写入 data/competitor_hot_rank.db 的 competitor_latest_creatives_daily 表。"
    )

    # 如未指定 --with-llm，则本次仅做爬取+入库，直接结束
    if not args.with_llm:
        print("[完成] 当前运行仅测试爬取和入库，未执行聚类与深度解析。")
        return

    # 以下为可选的聚类 + 深度解析流程（需显式传入 --with-llm）
    hot_by_category = build_hot_candidates_from_search(competitors, search_results)
    total_hot = sum(len(v) for v in hot_by_category.values())
    print(
        f"[2/3] 已在「7天 / 素材 / 最新创意」排序下筛出热门候选共 {total_hot} 条："
        + ", ".join(f"{k}={len(v)}" for k, v in hot_by_category.items())
    )

    window_days = 7
    window_desc = f"{(report_date - dt.timedelta(days=window_days - 1)).isoformat()} ~ {report_date.isoformat()}"
    board_title = "广大大热度监控周报（热门榜）"

    clusters_by_category: Dict[str, Dict[str, Any]] = {}
    for cat in ("seek", "video enhancer"):
        items = hot_by_category.get(cat) or []
        print(f"  - 正在对 {cat} 分类的 {len(items)} 条热门素材做聚类与深度解析...")
        data = analyze_clusters_for_category(
            board_title=board_title,
            category=cat,
            items=items,
            window_desc=window_desc,
        )
        clusters_by_category[cat] = data

    # 写 JSON
    out_path = Path(args.output_json) if args.output_json else DATA_DIR / f"competitor_hot_rank_{report_date.isoformat()}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(clusters_by_category, f, ensure_ascii=False, indent=2)
    print(f"[2/3] 聚类+深度解析结果已写入 {out_path}")

    # 生成周报文本
    report_text = build_weekly_hot_report_text(
        end_date=report_date,
        window_days=window_days,
        clusters_by_category=clusters_by_category,
    )
    print("\n[3/3] 热门榜周报文本如下：\n")
    print(report_text)


if __name__ == "__main__":
    main()

