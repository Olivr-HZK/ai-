"""
Video Enhancer 工作流验收：检查当日产物文件、分析成功率、方向卡片、推送表与历史波动，
写出 JSON + Markdown；可选飞书群通知、失败时非 0 退出码。

由 `workflow_video_enhancer_full_pipeline.py` 在成功/部分失败结尾调用：

    run_acceptance_after_workflow(target_date, partial=False|True)

环境变量（均为可选，默认见下）：
  ACCEPTANCE_ENABLED=1          关闭则 no-op
  ACCEPTANCE_BLOCK_ON_FAIL=0    为 1 且最终 status=fail 时 sys.exit(2)
  ACCEPTANCE_MIN_SUCCESS_RATE=0.0  新分析尝试数>0 时，成功率低于此值记一条 soft（0~1）
  ACCEPTANCE_COVER_REMOVAL_WARN=30 封面环节剔除比例超过此百分数记一条 warn
  ACCEPTANCE_LOOKBACK_DAYS=7     历史截断后条数对比窗口
  ACCEPTANCE_LOW_VS_MEAN=0.5     今日 post_total 低于 lookback 均值比例时 warn
  ACCEPTANCE_HIGH_VS_MEAN=2.0    今日 post_total 高于 lookback 均值比例时 warn
  ACCEPTANCE_EXIT_ON_SOFT=0      为 1 时 soft 问题也 exit(2)（与 BLOCK 配合）
  ACCEPTANCE_STRICT=0            为 1 时部分「仅提示」升级为 soft
  ACCEPTANCE_FEISHU_ENABLED=0    为 1 且配置了 webhook 时发飞书卡片
  ACCEPTANCE_FEISHU_WEBHOOK=     默认同环境或专用；也可用 CLI --feishu-webhook
  ACCEPTANCE_FEISHU_STRICT=0     为 1 时飞书发送失败抛错

输出：
  data/workflow_video_enhancer_{date}_acceptance.json
  reports/workflow_video_enhancer_{date}_acceptance.md
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

from path_util import DATA_DIR, PROJECT_ROOT, REPORTS_DIR
from video_enhancer_pipeline_db import (
    DB_PATH,
    init_db,
    should_persist_suggestion_to_push_table,
)

load_dotenv(PROJECT_ROOT / ".env")


def _env_bool(key: str, default: str = "0") -> bool:
    v = (os.getenv(key) or default).strip().lower()
    return v in ("1", "true", "yes", "on")


def _env_float(key: str, default: str) -> float:
    try:
        return float((os.getenv(key) or default).strip())
    except ValueError:
        return float(default)


def _env_int(key: str, default: str) -> int:
    try:
        return int((os.getenv(key) or default).strip())
    except ValueError:
        return int(default)


def _prefix(target_date: str) -> str:
    return f"workflow_video_enhancer_{target_date}"


def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _count_push_rows(target_date: str) -> int:
    init_db()
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM daily_ua_push_content WHERE target_date = ?",
            (target_date,),
        )
        row = cur.fetchone()
        conn.close()
        return int(row[0]) if row else 0
    except Exception:
        return 0


def _filter_log_after_total(target_date: str) -> Optional[int]:
    """daily_video_enhancer_filter_log 里 __TOTAL__ 的 after_cnt。"""
    init_db()
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT after_cnt FROM daily_video_enhancer_filter_log "
            "WHERE target_date = ? AND product = '__TOTAL__' LIMIT 1",
            (target_date,),
        )
        r = cur.fetchone()
        conn.close()
        if r:
            return int(r["after_cnt"])
    except Exception:
        pass
    return None


def _history_post_totals(end_date: str, lookback_days: int) -> List[Tuple[str, int]]:
    """最近 lookback_days 个日历日（不含 end_date）的截断后条数，来自各日 raw.filter_report。"""
    out: List[Tuple[str, int]] = []
    try:
        d0 = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        return out
    for i in range(1, lookback_days + 1):
        d = d0 - timedelta(days=i)
        ds = d.isoformat()
        p = DATA_DIR / f"workflow_video_enhancer_{ds}_raw.json"
        raw = _read_json(p)
        if not raw:
            continue
        fr = raw.get("filter_report")
        if isinstance(fr, dict):
            post = int(fr.get("post_truncation_total") or 0)
        else:
            post = 0
        if post:
            out.append((ds, post))
    return list(reversed(out))


def _build_stages(target_date: str, partial: bool) -> Dict[str, Any]:
    pre = _prefix(target_date)
    raw_path = DATA_DIR / f"{pre}_raw.json"
    analysis_path = DATA_DIR / f"video_analysis_{pre}_raw.json"
    sugg_path = DATA_DIR / f"ua_suggestion_{pre}.json"
    cover_rep_path = DATA_DIR / f"{pre}_cover_style_intraday.json"
    launched_path = DATA_DIR / f"{pre}_filter_step_launched_effects.json"
    dedup_path = DATA_DIR / f"{pre}_analysis_dedup_report.json"
    log_path = PROJECT_ROOT / "logs" / f"daily_video_enhancer_workflow_{target_date}.log"

    raw = _read_json(raw_path)
    analysis = _read_json(analysis_path)
    sugg = _read_json(sugg_path)
    cover_rep = _read_json(cover_rep_path)
    launched = _read_json(launched_path)
    dedup = _read_json(dedup_path)

    fr = raw.get("filter_report") if isinstance(raw, dict) else None
    if not isinstance(fr, dict):
        fr = {}
    pre_t = int(fr.get("pre_truncation_total") or 0)
    post_t = int(fr.get("post_truncation_total") or 0)
    if raw and not post_t:
        post_t = len(raw.get("items") or [])

    items = raw.get("items") or [] if raw else []
    item_count = len(items) if isinstance(items, list) else 0

    fl_after = _filter_log_after_total(target_date)

    an_results = (analysis or {}).get("results") or []
    if not isinstance(an_results, list):
        an_results = []
    ad_keys = {str(x.get("ad_key") or "") for x in an_results if isinstance(x, dict)}
    ad_key_count = len({k for k in ad_keys if k})

    ex_sem = sum(
        1
        for x in an_results
        if isinstance(x, dict)
        and (
            x.get("semantic_dedup_matched")
            or x.get("semantic_dedup_similarity") is not None
        )
    )
    ex_launch = 0
    for x in an_results:
        if not isinstance(x, dict):
            continue
        if x.get("launched_effect_match"):
            ex_launch += 1
            continue
        mtags = x.get("material_tags")
        if isinstance(mtags, list) and any("我方已投" in str(t) for t in mtags):
            ex_launch += 1

    new_success = int((analysis or {}).get("new_success") or 0)
    new_failed = int((analysis or {}).get("new_failed") or 0)
    attempted_new = new_success + new_failed
    analyzed_items = int((analysis or {}).get("analyzed_items") or len(an_results))
    pipeline_items = int((analysis or {}).get("pipeline_items") or item_count or 0)

    skipped_llm = bool((sugg or {}).get("skipped_llm")) if sugg else False
    llm_err = (sugg or {}).get("llm_error")
    s_inner = (sugg or {}).get("suggestion") or sugg
    cards: List[Any] = []
    if isinstance(s_inner, dict):
        cards = s_inner.get("方向卡片") or []
    if not isinstance(cards, list):
        cards = []
    card_count = len([c for c in cards if isinstance(c, dict) and c])

    # 封面 step：以 cover_style_intraday 报告为准；无文件则视为本环节未跑（skipped）
    f3_in = item_count
    f3_out = item_count
    f3_skip = True
    if isinstance(cover_rep, dict):
        f3_skip = bool(cover_rep.get("skipped", True))
        f3_in = int(cover_rep.get("input_count") or item_count)
        f3_out = int(cover_rep.get("output_count") or f3_in)

    f4_marked = int((launched or {}).get("marked_count") or 0) if launched else 0
    if not f4_marked and isinstance(launched, dict):
        det = launched.get("details")
        if isinstance(det, list):
            f4_marked = len(det)

    log_exists = log_path.is_file()
    sync_note = ""
    sync_mentioned = False
    if log_exists:
        try:
            tail = log_path.read_text(encoding="utf-8", errors="replace")[-8000:]
            sync_mentioned = "sync" in tail.lower() or "多维" in tail or "bitable" in tail.lower()
        except OSError:
            pass
    if not log_exists:
        sync_note = (
            "该文件通常只有跑 `scripts/daily_video_enhancer_workflow.sh`（内部 tee）才会出现；"
            "若只手动执行 `python scripts/workflow_video_enhancer_full_pipeline.py`，可能无此日志，不算失败。"
        )

    should_persist = should_persist_suggestion_to_push_table(sugg) if sugg else False
    push_rows = _count_push_rows(target_date)

    lb = _env_int("ACCEPTANCE_LOOKBACK_DAYS", "7")
    hist_pairs = _history_post_totals(target_date, lb)
    mean_after = (
        sum(p[1] for p in hist_pairs) / len(hist_pairs) if hist_pairs else 0.0
    )
    filter_step3 = {"input": f3_in, "output": f3_out, "skipped": f3_skip}

    stages: Dict[str, Any] = {
        "raw": {
            "path": str(raw_path.resolve()),
            "exists": raw_path.is_file() and raw is not None,
            "item_count": item_count,
            "filter_post_total": post_t,
            "filter_pre_total": pre_t,
            "filter_log_after_total": fl_after,
        },
        "analysis": {
            "path": str(analysis_path.resolve()),
            "exists": analysis_path.is_file() and analysis is not None,
            "analyzed_items": analyzed_items,
            "new_success": new_success,
            "new_failed": new_failed,
            "pipeline_items": pipeline_items,
            "attempted_new": attempted_new,
            "ad_key_count": ad_key_count,
            "exclude_cluster_semantic_count": ex_sem,
            "exclude_cluster_launched_hint": ex_launch,
        },
        "cluster": {
            "path": str(sugg_path.resolve()),
            "exists": sugg_path.is_file() and sugg is not None,
            "skipped_llm": skipped_llm,
            "llm_error": llm_err,
            "card_count": card_count,
        },
        "filters": {
            "filter_step3": filter_step3,
            "filter_step4": {"marked_count": f4_marked},
        },
        "dedup_report": {
            "path": str(dedup_path.resolve()),
            "exists": dedup_path.is_file() and dedup is not None,
        },
        "sync": {
            "log_path": str(log_path.resolve()),
            "exists": log_exists,
            "sync_mentioned": sync_mentioned if log_exists else None,
            "note": sync_note or None,
        },
        "push": {
            "daily_ua_push_rows": push_rows,
            "should_persist_push_table": should_persist,
        },
        "history": {
            "lookback_days": lb,
            "history_pairs": [[a, b] for a, b in hist_pairs],
            "today_post_total": post_t,
            "history_mean_after_cnt": round(mean_after, 2),
        },
    }
    if partial:
        stages["_partial"] = True
    return stages


def _issue(
    issues: List[Dict[str, Any]],
    severity: str,
    code: str,
    message: str,
) -> None:
    issues.append({"severity": severity, "code": code, "message": message})


def _collect_issues(
    target_date: str,
    stages: Dict[str, Any],
    partial: bool,
) -> List[Dict[str, Any]]:
    issues: List[Dict[str, Any]] = []
    strict = _env_bool("ACCEPTANCE_STRICT", "0")
    w_cat = "soft" if strict else "warn"

    raw = stages.get("raw") or {}
    an = stages.get("analysis") or {}
    cl = stages.get("cluster") or {}
    push = stages.get("push") or {}
    hist = stages.get("history") or {}
    f3 = (stages.get("filters") or {}).get("filter_step3") or {}

    if not raw.get("exists") and not partial:
        _issue(issues, "hard", "missing_raw", f"缺少 raw 文件或无法解析: {raw.get('path')}")
    if not an.get("exists") and not partial:
        _issue(issues, "hard", "missing_analysis", f"缺少 analysis 文件或无法解析: {an.get('path')}")

    if an.get("exists") and int(an.get("new_failed") or 0) > 0:
        _issue(
            issues,
            "soft" if not strict else "soft",
            "analysis_failures",
            f"本轮新分析失败 {an.get('new_failed')} 条，见 analysis_failed 清单。",
        )

    min_rate = _env_float("ACCEPTANCE_MIN_SUCCESS_RATE", "0")
    att = int(an.get("attempted_new") or 0)
    if att > 0 and min_rate > 0:
        succ = int(an.get("new_success") or 0)
        rate = succ / att
        if rate < min_rate:
            _issue(
                issues,
                "soft",
                "low_success_rate",
                f"新分析成功率 {rate:.1%} 低于阈值 {min_rate:.1%}（成功 {succ}/{att}）。",
            )

    if not partial and an.get("exists") and not cl.get("exists"):
        _issue(issues, "soft", "missing_suggestion", "缺少 ua_suggestion JSON（方向卡片未生成或路径不对）。")

    if cl.get("exists") and cl.get("skipped_llm"):
        _issue(issues, w_cat, "cluster_skipped", "方向卡片阶段 skipped_llm=True。")
    if cl.get("llm_error"):
        _issue(issues, "soft", "cluster_llm_error", f"方向卡片 LLM 错误: {cl.get('llm_error')}")

    should_p = bool(push.get("should_persist_push_table"))
    pr = int(push.get("daily_ua_push_rows") or 0)
    if should_p and pr == 0 and not partial:
        _issue(issues, "soft", "empty_push_table", "按规则应写入 daily_ua_push_content，但当天行数为 0。")
    if not should_p and pr > 0:
        _issue(issues, "warn", "unexpected_push_rows", f"按规则不必写入推送表，但存在 {pr} 行，请核对。")

    # 封面剔除比例
    if not f3.get("skipped") and f3.get("input") and f3.get("output") is not None:
        fin = int(f3["input"])
        fout = int(f3["output"])
        if fin > 0 and fout < fin:
            pct = (fin - fout) * 100.0 / fin
            cap = _env_float("ACCEPTANCE_COVER_REMOVAL_WARN", "30")
            if pct > cap:
                _issue(
                    issues,
                    "warn",
                    "high_cover_removal",
                    f"封面环节剔除 {pct:.1f}%（{fout}/{fin}），超过提示阈值 {cap:.0f}%。",
                )

    # 历史截断量波动
    today_p = int(raw.get("filter_post_total") or 0)
    mean = float(hist.get("history_mean_after_cnt") or 0)
    low = _env_float("ACCEPTANCE_LOW_VS_MEAN", "0.5")
    high = _env_float("ACCEPTANCE_HIGH_VS_MEAN", "2.0")
    if mean > 0 and today_p > 0:
        r = today_p / mean
        if r < low:
            _issue(
                issues,
                "warn",
                "low_volume_vs_mean",
                f"今日截断后 {today_p} 条，低于近 {hist.get('lookback_days')} 日均 {mean:.1f} 的 {low:.0%} 以下。",
            )
        if r > high:
            _issue(
                issues,
                "warn",
                "high_volume_vs_mean",
                f"今日截断后 {today_p} 条，高于近 {hist.get('lookback_days')} 日均 {mean:.1f} 的 {high:.0%} 以上。",
            )

    r_exist = float(raw.get("filter_post_total") or 0)
    flt = raw.get("filter_log_after_total")
    if flt is not None and r_exist and int(flt) != int(r_exist):
        _issue(
            issues,
            "warn",
            "filter_log_mismatch",
            f"raw.filter_post_total={int(r_exist)} 与库 filter_log __TOTAL__ after={int(flt)} 不一致。",
        )

    return issues


def _score(issues: List[Dict[str, Any]]) -> Tuple[str, int, Dict[str, int]]:
    h = s = w = 0
    for it in issues:
        sev = (it.get("severity") or "").lower()
        if sev == "hard":
            h += 1
        elif sev == "soft":
            s += 1
        else:
            w += 1
    score = 100 - 25 * h - 10 * s - 3 * w
    score = max(0, min(100, score))
    if h > 0:
        status = "fail"
    elif s > 0:
        status = "warn"
    else:
        status = "pass"
    return status, score, {"hard": h, "soft": s, "warn": w}


def _render_markdown(
    target_date: str,
    status: str,
    score: int,
    counts: Dict[str, int],
    issues: List[Dict[str, Any]],
    stages: Dict[str, Any],
) -> str:
    lines: List[str] = []
    lines.append(f"# Video Enhancer 验收报告（{target_date}）\n")
    lines.append("## 一句话结论\n")
    if status == "pass":
        lines.append("本日验收**通过**：关键检查未发现需立即处理的问题。")
    elif status == "warn":
        lines.append("本日验收**有提示项**：无严重问题，请查看下表与待办。")
    else:
        lines.append("本日验收**未通过**：存在严重项，请优先修复后再采信当日产物。")
    lines.append("\n## 汇总\n\n")
    lines.append("| 项目 | 值 |\n| --- | --- |\n")
    st_zh = {"pass": "通过", "warn": "有提示", "fail": "未通过"}.get(status, status)
    lines.append(f"| 结论 | **{st_zh}** |\n")
    lines.append(f"| 健康分 | **{score}** / 100 |\n")
    lines.append(
        f"| 严重 / 需关注 / 提示 | {counts.get('hard', 0)} / {counts.get('soft', 0)} / {counts.get('warn', 0)} |\n"
    )
    lines.append(
        "\n- **严重**：缺关键文件或无法解析，当日结果不可靠。\n"
        "- **需关注**：成功率、推送表、方向卡片等异常。\n"
        "- **提示**：量级波动、日志缺失等，建议人工看一眼。\n"
    )
    lines.append("\n## 待办清单\n\n")
    if not issues:
        lines.append("*本日无待办项。*\n")
    else:
        for it in issues:
            lines.append(
                f"- [{it.get('severity')}] {it.get('code')}: {it.get('message')}\n"
            )
    lines.append("\n## 阶段摘要（机器可读见 JSON）\n\n")
    lines.append("```json\n")
    lines.append(json.dumps(stages, ensure_ascii=False, indent=2)[:20000])
    if len(json.dumps(stages, ensure_ascii=False)) > 20000:
        lines.append("\n…（已截断，见 data 下完整 JSON）\n")
    lines.append("\n```\n")
    return "".join(lines)


def _feishu_send_card(
    target_date: str,
    status: str,
    score: int,
    counts: Dict[str, int],
    issues: List[Dict[str, Any]],
) -> None:
    if not _env_bool("ACCEPTANCE_FEISHU_ENABLED", "0"):
        return
    url = (os.getenv("ACCEPTANCE_FEISHU_WEBHOOK") or "").strip()
    if not url:
        return
    one_line = f"{target_date} | {status} | 分{score} | 严重{counts.get('hard')}/关注{counts.get('soft')}/提示{counts.get('warn')}"
    if issues:
        one_line += " | " + issues[0].get("message", "")[:80]
    body = {
        "msg_type": "text",
        "content": {"text": f"Video Enhancer 验收\n{one_line}"},
    }
    r = requests.post(url, json=body, timeout=15)
    if r.status_code != 200:
        if _env_bool("ACCEPTANCE_FEISHU_STRICT", "0"):
            raise RuntimeError(f"Feishu webhook HTTP {r.status_code}: {r.text[:200]}")
    else:
        j = r.json() if r.text else {}
        if isinstance(j, dict) and j.get("code") not in (0, None) and j.get("code") != 0:
            if _env_bool("ACCEPTANCE_FEISHU_STRICT", "0"):
                raise RuntimeError(f"Feishu: {j}")


def run_acceptance_after_workflow(target_date: str, partial: bool = False) -> None:
    if not _env_bool("ACCEPTANCE_ENABLED", "1"):
        print(f"[acceptance] 已关闭（ACCEPTANCE_ENABLED=0），跳过 {target_date}")
        return

    stages = _build_stages(target_date, partial=partial)
    issues = _collect_issues(target_date, stages, partial=partial)
    status, score, counts = _score(issues)

    out_json = DATA_DIR / f"workflow_video_enhancer_{target_date}_acceptance.json"
    out_md = REPORTS_DIR / f"workflow_video_enhancer_{target_date}_acceptance.md"
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "target_date": target_date,
        "partial": partial,
        "status": status,
        "score": score,
        "counts": counts,
        "issues": issues,
        "stages": stages,
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    out_md.write_text(
        _render_markdown(target_date, status, score, counts, issues, stages),
        encoding="utf-8",
    )
    print(
        f"[acceptance] {target_date} status={status} score={score} "
        f"hard={counts['hard']} soft={counts['soft']} warn={counts['warn']}"
    )
    print(f"[acceptance] 已写 {out_json.name} 与 {out_md.name}")

    try:
        _feishu_send_card(target_date, status, score, counts, issues)
    except Exception as e:
        print(f"[acceptance] 飞书通知失败: {e}")
        if _env_bool("ACCEPTANCE_FEISHU_STRICT", "0"):
            raise

    block = _env_bool("ACCEPTANCE_BLOCK_ON_FAIL", "0")
    if block and status == "fail":
        sys.exit(2)
    if _env_bool("ACCEPTANCE_EXIT_ON_SOFT", "0") and (counts.get("soft", 0) or 0) > 0:
        sys.exit(2)


def main() -> None:
    p = argparse.ArgumentParser(description="Video Enhancer 工作流验收（可单独跑）")
    p.add_argument("--date", required=True, metavar="YYYY-MM-DD", help="target_date")
    p.add_argument(
        "--partial",
        action="store_true",
        help="与流水线「分析失败提前 return」一致：放宽对方向卡片的要求",
    )
    p.add_argument(
        "--feishu-webhook",
        default="",
        help="若传则当次启用飞书 text 通知并写入 ACCEPTANCE_FEISHU_WEBHOOK 等效",
    )
    args = p.parse_args()
    if (args.feishu_webhook or "").strip():
        os.environ["ACCEPTANCE_FEISHU_WEBHOOK"] = args.feishu_webhook.strip()
        os.environ["ACCEPTANCE_FEISHU_ENABLED"] = "1"
    run_acceptance_after_workflow(args.date, partial=args.partial)


if __name__ == "__main__":
    main()
