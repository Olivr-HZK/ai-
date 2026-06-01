"""
Arrow2 竞品素材周趋势总结 & 飞书卡片推送。

从 arrow2_daily_insights 汇总指定日期范围内所有素材的一句话描述（ad_one_liner），
让 LLM 生成趋势分析报告，并推送飞书 interactive 卡片。

用法：
  # 总结本周（默认周一~今天）
  python scripts/arrow2_weekly_trend.py

  # 指定日期范围
  python scripts/arrow2_weekly_trend.py --start 2026-04-21 --end 2026-04-27

  # 自定义 webhook
  python scripts/arrow2_weekly_trend.py --webhook https://open.feishu.cn/open-apis/bot/v2/hook/xxx

  # 不推送，只打印
  python scripts/arrow2_weekly_trend.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests

# ── 项目路径 ──────────────────────────────────────────
_PROJECT = Path(__file__).resolve().parent.parent
if str(_Project := _PROJECT) not in sys.path:
    sys.path.insert(0, str(_Project))
if str(_Project / "scripts") not in sys.path:
    sys.path.insert(0, str(_Project / "scripts"))

from dotenv import load_dotenv

load_dotenv(_Project / ".env")

from arrow2_pipeline_db import init_db, _conn
from guangdada_detail_url import try_build_url_spa
from llm_client import call_text
from path_util import PROJECT_ROOT as _ROOT

UTC8 = timezone(timedelta(hours=8))

# ── 默认 webhook ──────────────────────────────────────
_DEFAULT_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/75d19d62-75f9-46ac-9637-254b09629c2a"


# ── 工具函数 ──────────────────────────────────────────
def _beijing_now() -> datetime:
    return datetime.now(UTC8)


def _this_monday() -> str:
    """本周一（北京时区），如果今天是周一则返回今天。"""
    now = _beijing_now()
    monday = now - timedelta(days=now.weekday())
    return monday.strftime("%Y-%m-%d")


def _yesterday() -> str:
    return (_beijing_now() - timedelta(days=1)).strftime("%Y-%m-%d")


# ── 数据查询 ──────────────────────────────────────────
def load_creatives_in_range(
    start_date: str,
    end_date: str,
) -> List[Dict[str, Any]]:
    """从 arrow2_daily_insights 加载日期范围内的素材（含 one_liner + 构建链接）。"""
    init_db()
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT target_date, product, appid, ad_key, platform,
               video_url, preview_img_url, video_duration,
               heat, all_exposure_value, impression, days_count,
               ad_one_liner, insight_analysis, crawl_workflow,
               raw_json
        FROM arrow2_daily_insights
        WHERE target_date >= ? AND target_date <= ?
        ORDER BY target_date, product, ad_key
        """,
        (start_date, end_date),
    )
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()

    results: List[Dict[str, Any]] = []
    for row in rows:
        raw_json_str = row.pop("raw_json") or "{}"
        try:
            creative = json.loads(raw_json_str)
        except (json.JSONDecodeError, TypeError):
            creative = {}

        # 构建广大大链接
        detail_url = try_build_url_spa(creative, creative_type=1)

        # 优先用 ad_one_liner，否则从 insight_analysis 截取首行
        one_liner = str(row.get("ad_one_liner") or "").strip()
        if not one_liner:
            analysis = str(row.get("insight_analysis") or "")
            for line in analysis.split("\n"):
                line = line.strip().lstrip("-•*0-9. ")
                if line and len(line) > 5:
                    one_liner = line[:80]
                    break

        results.append(
            {
                "target_date": row.get("target_date", ""),
                "product": row.get("product", ""),
                "ad_key": row.get("ad_key", ""),
                "platform": row.get("platform", ""),
                "one_liner": one_liner,
                "detail_url": detail_url,
                "heat": int(row.get("heat") or 0),
                "impression": int(row.get("impression") or 0),
                "all_exposure_value": int(row.get("all_exposure_value") or 0),
                "video_duration": int(row.get("video_duration") or 0),
                "crawl_workflow": row.get("crawl_workflow", ""),
            }
        )
    return results


# ── LLM 趋势生成 ──────────────────────────────────────
_WORKFLOW_LABELS = {
    "最新创意": {"short": "新素材", "icon": "🆕", "card_color": "green"},
    "展示估值": {"short": "展示估值", "icon": "📈", "card_color": "orange"},
}


def generate_trend_report(
    creatives: List[Dict[str, Any]],
    start_date: str,
    end_date: str,
    workflow_label: str = "",
) -> str:
    """将素材一句话描述汇总后让 LLM 生成趋势分析。"""
    if not creatives:
        return f"⚠️ {start_date}~{end_date} 无竞品素材数据。"

    # 按产品分组汇总
    by_product: Dict[str, List[Dict[str, Any]]] = {}
    for c in creatives:
        p = c["product"] or "未知"
        by_product.setdefault(p, []).append(c)

    sections: List[str] = []
    for product, items in sorted(by_product.items()):
        sections.append(f"### {product}（{len(items)} 条素材）")
        for it in items:
            tag = "📷" if it["video_duration"] == 0 else "🎬"
            line = f"- {tag} [{it['target_date']}] {it['one_liner']}"
            if it["detail_url"]:
                line += f"  [链接]({it['detail_url']})"
            if it["heat"] > 0:
                line += f"  热度={it['heat']}"
            if it["all_exposure_value"] > 0:
                line += f"  估值={it['all_exposure_value']}"
            sections.append(line)
        sections.append("")

    material_summary = "\n".join(sections)
    total = len(creatives)
    n_products = len(by_product)

    # 根据工作流类型定制 prompt
    if workflow_label == "展示估值":
        focus_hint = textwrap.dedent("""\
            本次分析的是「展示估值 Top 素材」，即各产品曝光量最高的素材。
            请重点分析：
            1. 高曝光素材的共性特征（视觉风格、钩子类型、叙事结构）
            2. 哪些创意方向获得了最高曝光，为什么
            3. 对我方产品如何获取高曝光的建议
        """)
    else:
        focus_hint = textwrap.dedent("""\
            本次分析的是「最新创意素材」，即各产品最近新投放的素材。
            请重点分析：
            1. 新素材的创意方向和素材套路变化
            2. 哪些新创意方向值得关注和借鉴
            3. 对我方产品创意方向的建议
        """)

    system_prompt = textwrap.dedent(f"""\
        你是一位资深移动游戏 UA（用户获取）素材分析师，专注于箭头/解谜类游戏的竞品广告创意研究。
        请根据下方素材清单，写一份**简洁但洞察深刻**的周度趋势报告。

        {focus_hint}

        通用要求：
        1. 开头用 2-3 句话总结本周整体趋势
        2. 按产品分别总结创意方向和素材特点（每产品 2-4 句）
        3. 提炼出 2-3 个值得我方借鉴的创意方向或套路
        4. 风格：简洁、专业、有数据支撑
        5. 使用 Markdown 格式，适当使用加粗
        6. 素材链接用 [查看素材](url) 格式，保留原始链接
        7. 不要列出每一条素材，而是归纳总结
    """)

    user_prompt = textwrap.dedent(f"""\
        日期范围：{start_date} ~ {end_date}
        素材总数：{total} 条，覆盖 {n_products} 个竞品产品
        标记说明：🎬=视频素材 📷=图片素材

        {material_summary}

        请生成周度趋势报告。
    """)

    try:
        report = call_text(system_prompt, user_prompt)
        return report
    except Exception as e:
        return f"⚠️ LLM 生成失败: {e}\n\n原始素材清单:\n{material_summary}"


# ── 飞书卡片推送 ──────────────────────────────────────
def push_feishu_card(webhook: str, title: str, md_text: str, template: str = "blue") -> None:
    """发送飞书 interactive 卡片。template 可选: blue/green/orange/red 等。"""
    card = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True, "enable_forward": True},
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": template,
            },
            "elements": [{"tag": "markdown", "content": md_text[:28000]}],
        },
    }
    try:
        resp = requests.post(webhook, json=card, timeout=15)
        print(f"[arrow2-trend] 飞书推送结果: HTTP {resp.status_code} | {resp.text[:200]}")
    except Exception as exc:
        print(f"[arrow2-trend] 飞书推送失败: {exc}")


def _push_one_workflow(
    creatives: List[Dict[str, Any]],
    workflow_key: str,
    start_date: str,
    end_date: str,
    webhook: str,
    dry_run: bool,
) -> None:
    """为单个工作流生成趋势报告并推送卡片。"""
    label = _WORKFLOW_LABELS.get(workflow_key, {"short": workflow_key, "icon": "📋", "card_color": "blue"})
    filtered = [c for c in creatives if c["crawl_workflow"] == workflow_key]
    if not filtered:
        print(f"[arrow2-trend] {workflow_key}: 无素材，跳过。")
        return

    by_product: Dict[str, int] = {}
    for c in filtered:
        by_product[c["product"]] = by_product.get(c["product"], 0) + 1

    print(f"[arrow2-trend] {workflow_key}: {len(filtered)} 条，{len(by_product)} 个产品")

    # LLM 生成
    print(f"[arrow2-trend] {workflow_key}: 正在生成趋势报告...")
    report = generate_trend_report(filtered, start_date, end_date, workflow_label=workflow_key)
    print(f"[arrow2-trend] {workflow_key}: 报告生成完成，长度={len(report)}")

    summary_line = (
        f"**日期**：{start_date} ~ {end_date}  |  "
        f"**素材数**：{len(filtered)}  |  "
        f"**产品数**：{len(by_product)}"
    )
    card_md = f"{summary_line}\n\n---\n\n{report}"

    if dry_run:
        print(f"\n{'=' * 60}")
        print(f"【dry-run】{workflow_key} 卡片内容预览：")
        print("=" * 60)
        print(card_md)
        print("=" * 60)
        return

    title = f"{label['icon']} Arrow2 {label['short']}趋势 {start_date}~{end_date}"
    push_feishu_card(webhook, title, card_md, template=label["card_color"])


# ── 主流程 ─────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Arrow2 竞品素材周趋势总结 & 飞书推送")
    p.add_argument("--start", default="", help="起始日期 YYYY-MM-DD（默认本周一）")
    p.add_argument("--end", default="", help="截止日期 YYYY-MM-DD（默认昨天）")
    p.add_argument(
        "--workflow",
        default="",
        help="指定工作流：最新创意 / 展示估值；不指定则两个都推",
    )
    p.add_argument("--webhook", default="", help="飞书 webhook URL")
    p.add_argument("--dry-run", action="store_true", help="只打印不推送")
    return p.parse_args()


def main() -> None:
    load_dotenv(_ROOT / ".env")
    args = parse_args()

    start_date = (args.start or "").strip() or _this_monday()
    end_date = (args.end or "").strip() or _yesterday()

    print(f"[arrow2-trend] 日期范围: {start_date} ~ {end_date}")

    # 1. 加载素材
    creatives = load_creatives_in_range(start_date, end_date)
    if not creatives:
        print("[arrow2-trend] 该日期范围无素材数据。")
        return

    # 统计
    by_workflow: Dict[str, int] = {}
    for c in creatives:
        wf = c["crawl_workflow"] or "未知"
        by_workflow[wf] = by_workflow.get(wf, 0) + 1
    print(f"[arrow2-trend] 素材总数: {len(creatives)}，按工作流: {by_workflow}")

    # 2. 确定 webhook
    webhook = (args.webhook or "").strip()
    if not webhook:
        webhook = _DEFAULT_WEBHOOK

    # 3. 按工作流拆分生成 & 推送
    workflow_arg = (args.workflow or "").strip()
    if workflow_arg:
        # 只推指定工作流
        _push_one_workflow(creatives, workflow_arg, start_date, end_date, webhook, args.dry_run)
    else:
        # 两个都推
        for wf_key in ("最新创意", "展示估值"):
            _push_one_workflow(creatives, wf_key, start_date, end_date, webhook, args.dry_run)


if __name__ == "__main__":
    main()
