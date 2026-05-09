"""Arrow2 weekly new-play and new-hook Enterprise WeChat push."""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List

from dotenv import load_dotenv

from ua_workflows.shared.config import PROJECT_ROOT
from ua_workflows.shared.db.arrow2 import init_db, _conn
from ua_workflows.shared.guangdada.detail_url import try_build_url_spa
from ua_workflows.shared.llm import client as llm_client
from ua_workflows.shared.push.wecom import push_wecom_markdown

UTC8 = timezone(timedelta(hours=8))

WORKFLOW_ALIASES = {
    "latest": "最新创意",
    "latest_yesterday": "最新创意",
    "新素材": "最新创意",
    "最新创意": "最新创意",
    "exposure": "展示估值",
    "exposure_top10": "展示估值",
    "展示估值": "展示估值",
    "all": "",
    "全部": "",
}


def _yesterday_utc8() -> date:
    return datetime.now(UTC8).date() - timedelta(days=1)


def _default_start_end() -> tuple[str, str]:
    end = _yesterday_utc8()
    start = end - timedelta(days=6)
    return start.isoformat(), end.isoformat()


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _normalize_workflow(raw: str) -> str:
    s = (raw or "").strip()
    return WORKFLOW_ALIASES.get(s, s)


def _similarity_threshold() -> float:
    raw = (os.getenv("ARROW2_WEEKLY_TEXT_SIMILARITY_THRESHOLD") or "0.82").strip()
    try:
        return max(0.0, min(1.0, float(raw)))
    except ValueError:
        return 0.82


def _norm_text(text: str) -> str:
    out: list[str] = []
    for ch in (text or "").lower():
        if ch.isspace():
            continue
        if ch.isalnum() or "\u4e00" <= ch <= "\u9fff":
            out.append(ch)
    return "".join(out)


def _is_similar(a: str, b: str, threshold: float) -> bool:
    na = _norm_text(a)
    nb = _norm_text(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    if min(len(na), len(nb)) >= 4 and (na in nb or nb in na):
        return True
    return SequenceMatcher(None, na, nb).ratio() >= threshold


def _fallback_line(row: Dict[str, Any]) -> str:
    return str(row.get("ad_one_liner") or "").strip()


def _derive_hook_from_row(row: Dict[str, Any]) -> str:
    """Best-effort historical fallback before hook_one_liner exists in analyzed rows."""
    one = _fallback_line(row)
    creative = row.get("creative") if isinstance(row.get("creative"), dict) else {}
    title = str(creative.get("title") or "").strip()
    body = str(creative.get("body") or "").strip()
    text = f"{one} {title} {body}".lower()
    rules = [
        (("倒计时", "限时", "time", "timer", "countdown"), "倒计时制造紧迫感"),
        (("求救", "解救", "救援", "被困", "困住", "rescue", "save", "help"), "角色被困求救"),
        (("金币", "金钱", "钞票", "奖励", "coin", "coins", "money", "cash", "reward"), "金币奖励即时反馈"),
        (("失败", "差一步", "fail", "failed", "wrong", "mistake"), "失败差一步反差"),
        (("美女", "女孩", "女友", "公主", "girl", "woman", "princess"), "角色处境引发好奇"),
        (("怪物", "敌人", "boss", "monster", "enemy"), "敌人威胁制造压力"),
        (("聪明", "智商", "iq", "brain", "only", "can you"), "智力挑战激发胜负欲"),
        (("填色", "涂色", "color", "paint"), "填色变化视觉反馈"),
        (("抽针", "拉针", "pin"), "抽针结果制造悬念"),
        (("迷宫", "路径", "路线", "箭头", "arrow", "path", "maze"), "路径解谜即时可懂"),
    ]
    for needles, label in rules:
        if any(n in text for n in needles):
            return label
    return ""


def _field_value(row: Dict[str, Any], field: str) -> str:
    v = str(row.get(field) or "").strip()
    if v:
        return v
    if field == "play_one_liner":
        return _fallback_line(row)
    if field == "hook_one_liner" and _derive_missing_hooks_enabled():
        return _derive_hook_from_row(row)
    return ""


def _derive_missing_hooks_enabled() -> bool:
    raw = (os.getenv("ARROW2_WEEKLY_DERIVE_MISSING_HOOKS") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _load_rows(start_date: str, end_date: str, workflow: str = "") -> List[Dict[str, Any]]:
    init_db()
    conn = _conn()
    try:
        cur = conn.cursor()
        params: list[Any] = [start_date, end_date]
        workflow_clause = ""
        if workflow:
            workflow_clause = " AND COALESCE(crawl_workflow, '') LIKE ?"
            params.append(f"%{workflow}%")
        cur.execute(
            f"""
            SELECT target_date, product, appid, ad_key, platform,
                   video_url, preview_img_url, video_duration,
                   heat, all_exposure_value, impression, days_count,
                   ad_one_liner, play_one_liner, hook_one_liner,
                   insight_material_category, crawl_workflow, raw_json
            FROM arrow2_daily_insights
            WHERE target_date >= ? AND target_date <= ?
              AND COALESCE(TRIM(ad_key), '') != ''
              {workflow_clause}
            ORDER BY target_date, product, ad_key
            """,
            tuple(params),
        )
        rows = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()

    for row in rows:
        raw = str(row.get("raw_json") or "").strip()
        creative: Dict[str, Any] = {}
        if raw:
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    creative = obj
            except Exception:
                creative = {}
        row["creative"] = creative
        row["detail_url"] = try_build_url_spa(creative, creative_type=1)
    return rows


def _history_by_appid(rows: Iterable[Dict[str, Any]], field: str) -> Dict[str, List[str]]:
    hist: Dict[str, List[str]] = defaultdict(list)
    for row in rows:
        appid = str(row.get("appid") or "").strip()
        value = _field_value(row, field)
        if appid and value:
            hist[appid].append(value)
    return hist


def _is_new_for_app(
    value: str,
    appid: str,
    history: Dict[str, List[str]],
    threshold: float,
) -> bool:
    if not value or not appid:
        return False
    for old in history.get(appid, []):
        if _is_similar(value, old, threshold):
            return False
    return True


def _group_new_items(
    rows: List[Dict[str, Any]],
    history_rows: List[Dict[str, Any]],
    *,
    field: str,
    threshold: float,
    max_representatives: int = 3,
) -> List[Dict[str, Any]]:
    history = _history_by_appid(history_rows, field)
    buckets: Dict[tuple[str, str], Dict[str, Any]] = {}
    for row in rows:
        appid = str(row.get("appid") or "").strip()
        value = _field_value(row, field)
        if not _is_new_for_app(value, appid, history, threshold):
            continue
        key = (appid, _norm_text(value))
        if key not in buckets:
            buckets[key] = {
                "appid": appid,
                "phrase": value,
                "products": set(),
                "workflows": set(),
                "dates": set(),
                "rows": [],
                "max_impression": 0,
                "max_all_exposure_value": 0,
                "max_heat": 0,
            }
        b = buckets[key]
        b["products"].add(str(row.get("product") or "").strip() or "未知产品")
        b["workflows"].add(str(row.get("crawl_workflow") or "").strip() or "未知")
        b["dates"].add(str(row.get("target_date") or "").strip())
        b["rows"].append(row)
        b["max_impression"] = max(int(b["max_impression"]), int(row.get("impression") or 0))
        b["max_all_exposure_value"] = max(
            int(b["max_all_exposure_value"]),
            int(row.get("all_exposure_value") or 0),
        )
        b["max_heat"] = max(int(b["max_heat"]), int(row.get("heat") or 0))

    groups = list(buckets.values())
    for g in groups:
        rows2 = sorted(
            g["rows"],
            key=lambda r: (
                int(r.get("impression") or 0),
                int(r.get("all_exposure_value") or 0),
                int(r.get("heat") or 0),
            ),
            reverse=True,
        )
        g["rows"] = rows2
        g["representatives"] = rows2[:max_representatives]
        g["products"] = sorted(g["products"])
        g["workflows"] = sorted(g["workflows"])
        g["dates"] = sorted(x for x in g["dates"] if x)
        g["material_count"] = len(rows2)

    groups.sort(
        key=lambda g: (
            int(g["material_count"]),
            int(g["max_impression"]),
            int(g["max_all_exposure_value"]),
            int(g["max_heat"]),
        ),
        reverse=True,
    )
    return groups


def build_weekly_report(
    start_date: str,
    end_date: str,
    *,
    workflow: str,
    lookback_days: int,
    max_items: int,
    use_llm: bool,
) -> Dict[str, Any]:
    workflow_label = _normalize_workflow(workflow)
    rows = _load_rows(start_date, end_date, workflow_label)
    hist_start = (_parse_date(start_date) - timedelta(days=lookback_days)).isoformat()
    hist_end = (_parse_date(start_date) - timedelta(days=1)).isoformat()
    history_rows = _load_rows(hist_start, hist_end, "")
    threshold = _similarity_threshold()

    new_plays = _group_new_items(
        rows,
        history_rows,
        field="play_one_liner",
        threshold=threshold,
    )
    new_hooks = _group_new_items(
        rows,
        history_rows,
        field="hook_one_liner",
        threshold=threshold,
    )
    llm_takeaways = ""
    if use_llm and rows:
        llm_takeaways = _generate_llm_takeaways(
            start_date,
            end_date,
            new_plays[:max_items],
            new_hooks[:max_items],
        )
    return {
        "start_date": start_date,
        "end_date": end_date,
        "workflow": workflow_label or "全部",
        "lookback_days": lookback_days,
        "rows_count": len(rows),
        "history_rows_count": len(history_rows),
        "product_count": len({str(r.get("product") or "") for r in rows if str(r.get("product") or "")}),
        "new_plays": new_plays,
        "new_hooks": new_hooks,
        "llm_takeaways": llm_takeaways,
    }


def _group_lines(groups: List[Dict[str, Any]], *, max_items: int) -> str:
    if not groups:
        return "- 暂无历史窗口外的新项。"
    lines: list[str] = []
    for idx, g in enumerate(groups[:max_items], start=1):
        products = "、".join(g["products"][:3])
        if len(g["products"]) > 3:
            products += f" 等{len(g['products'])}个"
        dates = f"{g['dates'][0]}~{g['dates'][-1]}" if g["dates"] else ""
        lines.append(
            f"{idx}. **{g['phrase']}**"
            f"｜{products}｜素材 {g['material_count']} 条"
            f"｜最高展示估值 {int(g['max_impression'])}"
            + (f"｜{dates}" if dates else "")
        )
        for row in g["representatives"]:
            one = str(row.get("ad_one_liner") or "").strip()
            url = str(row.get("detail_url") or "").strip()
            prefix = f"   - {row.get('target_date')} {row.get('product')}"
            if one:
                prefix += f"：{one}"
            if url:
                prefix += f" [查看素材]({url})"
            lines.append(prefix)
    return "\n".join(lines)


def _generate_llm_takeaways(
    start_date: str,
    end_date: str,
    new_plays: List[Dict[str, Any]],
    new_hooks: List[Dict[str, Any]],
) -> str:
    def compact(groups: List[Dict[str, Any]]) -> str:
        out: list[str] = []
        for g in groups[:12]:
            out.append(
                f"- {g['phrase']}｜产品={','.join(g['products'][:3])}"
                f"｜素材数={g['material_count']}｜最高展示估值={g['max_impression']}"
            )
        return "\n".join(out) or "无"

    system = (
        "你是移动游戏 UA 素材分析师。请只基于给定的 Arrow2 竞品新玩法和新 Hook，"
        "输出简短周报洞察，不要虚构未给出的素材事实。"
    )
    user = f"""
日期：{start_date} ~ {end_date}

本周新玩法：
{compact(new_plays)}

本周新 Hook：
{compact(new_hooks)}

请输出：
1. 本周最值得关注的 2-3 个变化
2. 我方下周优先验证的 2-3 个素材方向

要求：Markdown；控制在 220 字内；直接给结论。
""".strip()
    try:
        return llm_client.call_text(system, user).strip()
    except Exception as exc:
        return f"LLM 洞察生成失败：{exc}"


def render_markdown(report: Dict[str, Any], *, max_items: int) -> str:
    start_date = report["start_date"]
    end_date = report["end_date"]
    title = f"# Arrow2 周报：新玩法 / 新 Hook\n{start_date} ~ {end_date}"
    summary = (
        f"> 工作流：{report['workflow']}｜素材 {report['rows_count']} 条"
        f"｜产品 {report['product_count']} 个｜历史对比 {report['lookback_days']} 天\n"
        f"> 新玩法 {len(report['new_plays'])} 个｜新 Hook {len(report['new_hooks'])} 个"
    )
    parts = [
        title,
        summary,
        "## 本周新玩法",
        _group_lines(report["new_plays"], max_items=max_items),
        "## 本周新 Hook",
        _group_lines(report["new_hooks"], max_items=max_items),
    ]
    if str(report.get("llm_takeaways") or "").strip():
        parts.extend(["## 值得跟进", str(report["llm_takeaways"]).strip()])
    return "\n\n".join(parts).strip()


def parse_args() -> argparse.Namespace:
    start, end = _default_start_end()
    p = argparse.ArgumentParser(description="Arrow2 每周新玩法 / 新 Hook 企业微信推送")
    p.add_argument("--start", default=start, help="起始日期 YYYY-MM-DD，默认昨天往前 7 天")
    p.add_argument("--end", default=end, help="截止日期 YYYY-MM-DD，默认昨天")
    p.add_argument(
        "--workflow",
        default="最新创意",
        help="最新创意 / 展示估值 / all；默认最新创意",
    )
    p.add_argument("--lookback-days", type=int, default=28, help="历史对比窗口，默认 28 天")
    p.add_argument("--max-items", type=int, default=8, help="每个小节最多展示条数，默认 8")
    p.add_argument("--webhook", default="", help="企业微信机器人 webhook；默认读 ARROW2_WECOM_BOT_WEBHOOK/WECOM_BOT_WEBHOOK")
    p.add_argument("--dry-run", action="store_true", help="只打印 markdown，不推送")
    p.add_argument("--no-llm", action="store_true", help="不生成 LLM 跟进建议")
    return p.parse_args()


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()
    start_date = (args.start or "").strip()
    end_date = (args.end or "").strip()
    _parse_date(start_date)
    _parse_date(end_date)
    if start_date > end_date:
        raise SystemExit(f"--start 不能晚于 --end：{start_date} > {end_date}")

    report = build_weekly_report(
        start_date,
        end_date,
        workflow=args.workflow,
        lookback_days=max(1, int(args.lookback_days or 28)),
        max_items=max(1, int(args.max_items or 8)),
        use_llm=not args.no_llm,
    )
    text = render_markdown(report, max_items=max(1, int(args.max_items or 8)))

    if args.dry_run:
        print(text)
        return

    webhook = (
        (args.webhook or "").strip()
        or os.getenv("ARROW2_WECOM_BOT_WEBHOOK", "").strip()
        or os.getenv("WECOM_BOT_WEBHOOK", "").strip()
    )
    if not webhook:
        print("[arrow2-weekly] 未配置 ARROW2_WECOM_BOT_WEBHOOK/WECOM_BOT_WEBHOOK，跳过推送。")
        print(text)
        return
    push_wecom_markdown(webhook, text)


if __name__ == "__main__":
    main()
