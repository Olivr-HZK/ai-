"""Generate template-recognition tasks from high-rated VE materials."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
from pathlib import Path
from typing import Any

from ua_workflows.shared.config import DATA_DIR, REPORTS_DIR, load_project_env
from ua_workflows.video_enhancer.feedback_rating import cell_to_text, resolve_feedback_rating
from ua_workflows.video_enhancer.feedback_training import fetch_bitable_records


DYNAMIC_TEMPLATE_RE = re.compile(
    r"转身|移动|镜头|表情|身体|手势|动作|跳舞|运动|变身|转场|推近|拉近|摇摆|挥手|走动|motion|move|camera|zoom",
    re.IGNORECASE,
)
TEMPLATE_RECOGNITION_MIN_RATING = 3


def yesterday_shanghai() -> str:
    today = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).date()
    return (today - dt.timedelta(days=1)).isoformat()


def _rating_field_for_reviewer(reviewer: str) -> tuple[str, str]:
    key = str(reviewer or "").strip().lower()
    if key in {"weilan", "尉蓝"}:
        return "尉蓝评分", "尉蓝接受情况"
    return "浩鹏评分", "浩鹏接受情况"


def _normalize_date_value(value: Any) -> str:
    text = cell_to_text(value)
    if not text:
        return ""
    match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    if match:
        return match.group(0)
    if re.fullmatch(r"\d{11,13}", text):
        try:
            stamp = int(text)
            if stamp > 10_000_000_000:
                stamp = stamp // 1000
            return dt.datetime.fromtimestamp(stamp, dt.timezone(dt.timedelta(hours=8))).date().isoformat()
        except (OSError, OverflowError, ValueError):
            return text
    return text


def _duration_seconds(value: Any) -> int:
    text = cell_to_text(value)
    if not text:
        return 0
    try:
        return int(float(text))
    except ValueError:
        return 0


def _suggest_template_kind(fields: dict[str, Any]) -> tuple[str, str]:
    video_url = cell_to_text(fields.get("视频链接") or fields.get("视频"))
    cover_url = cell_to_text(fields.get("封面图链接") or fields.get("封面图"))
    duration = _duration_seconds(fields.get("视频时长"))
    combined = " ".join(
        cell_to_text(fields.get(name))
        for name in ("脚本/口播", "模板指纹", "Hook解析", "核心卖点", "素材标签")
    )
    if video_url and duration > 0 and DYNAMIC_TEMPLATE_RE.search(combined):
        return "video_template_candidate", "素材有视频链接，且文本信号包含动态动作/镜头/转场。"
    if video_url or cover_url:
        return "image_template_candidate", "素材更像静态画面、海报、拼贴、卡片包装或缺少明确动态信号。"
    return "needs_manual_review", "缺少视频和封面媒体链接，需人工补齐来源。"


def _is_template_eligible_rating(rating: int | None) -> bool:
    return rating is not None and rating >= TEMPLATE_RECOGNITION_MIN_RATING


def _task_from_record(
    record: dict[str, Any],
    *,
    reviewer: str,
    include_legacy: bool,
) -> dict[str, Any] | None:
    fields = record.get("fields") if isinstance(record, dict) else {}
    if not isinstance(fields, dict):
        return None
    rating_field, status_field = _rating_field_for_reviewer(reviewer)
    feedback = resolve_feedback_rating(
        fields,
        reviewer=reviewer,
        rating_field=rating_field,
        status_field=status_field,
    )
    if not _is_template_eligible_rating(feedback.rating):
        return None
    if feedback.from_legacy and not include_legacy:
        return None

    kind, reason = _suggest_template_kind(fields)
    video_url = cell_to_text(fields.get("视频链接") or fields.get("视频"))
    cover_url = cell_to_text(fields.get("封面图链接") or fields.get("封面图"))
    source_url = video_url or cover_url
    source_kind = "video" if source_url == video_url and video_url else "image"
    record_id = str(record.get("record_id") or record.get("id") or "")
    ad_key = cell_to_text(fields.get("广告ID") or fields.get("ad_key")) or record_id
    crawl_date = _normalize_date_value(fields.get("抓取日期") or fields.get("日期"))
    task = {
        "record_id": record_id,
        "ad_key": ad_key,
        "product": cell_to_text(fields.get("产品") or fields.get("广告主")),
        "platform": cell_to_text(fields.get("平台")),
        "crawl_date": crawl_date,
        "rating": feedback.rating,
        "rating_label": feedback.rating_label,
        "rating_source_field": feedback.source_field,
        "rating_source_value": feedback.source_value,
        "video_url": video_url,
        "cover_url": cover_url,
        "title": cell_to_text(fields.get("标题") or fields.get("素材标题")),
        "core": cell_to_text(fields.get("核心卖点")),
        "hook": cell_to_text(fields.get("Hook解析") or fields.get("开场钩子")),
        "script_or_voiceover": cell_to_text(fields.get("脚本/口播") or fields.get("旁白")),
        "play_label": cell_to_text(fields.get("玩法") or fields.get("玩法资产") or fields.get("玩法指纹")),
        "template_fingerprint": cell_to_text(fields.get("模板指纹")),
        "material_tags": cell_to_text(fields.get("素材标签")),
        "suggested_template_kind": kind,
        "template_reason": reason,
    }
    task["aigc_template_copy_input"] = {
        "source_url": source_url,
        "source_kind": source_kind,
        "record_id": record_id,
        "ad_key": ad_key,
        "product": task["product"],
        "suggested_template_kind": kind,
    }
    return task


def build_template_task_from_record(
    record: dict[str, Any],
    *,
    reviewer: str = "haopeng",
    include_legacy: bool = False,
) -> dict[str, Any] | None:
    """Return a template-recognition task for one high-rated record."""
    return _task_from_record(record, reviewer=reviewer, include_legacy=include_legacy)


def build_template_tasks(
    records: list[dict[str, Any]],
    *,
    target_date: str,
    reviewer: str = "haopeng",
    include_legacy: bool = False,
) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    for record in records:
        fields = record.get("fields") if isinstance(record, dict) else {}
        if not isinstance(fields, dict):
            continue
        crawl_date = _normalize_date_value(fields.get("抓取日期") or fields.get("日期"))
        if crawl_date != target_date:
            continue
        task = _task_from_record(record, reviewer=reviewer, include_legacy=include_legacy)
        if task:
            tasks.append(task)
    return tasks


def write_artifacts(
    tasks: list[dict[str, Any]],
    *,
    target_date: str,
    data_dir: Path = DATA_DIR,
    reports_dir: Path = REPORTS_DIR,
) -> tuple[Path, Path]:
    data_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "name": "ve_template_recognition_tasks",
        "target_date": target_date,
        "task_count": len(tasks),
        "tasks": tasks,
    }
    data_path = data_dir / f"ve_template_recognition_tasks_{target_date}.json"
    report_path = reports_dir / f"ve_template_recognition_tasks_{target_date}.md"
    data_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        f"# VE 3星及以上模板识别任务 {target_date}",
        "",
        f"- 任务数：{len(tasks)}",
        "- 说明：该报告只整理 3 星及以上素材作为模板识别输入，不自动调用 Eagle、Video Lab 或 aigc-template-copy。",
        "",
        "| 广告ID | 产品 | 评分 | 类型 | 来源 |",
        "| --- | --- | --- | --- | --- |",
    ]
    for task in tasks:
        source = (task.get("aigc_template_copy_input") or {}).get("source_url") or ""
        lines.append(
            "| {ad_key} | {product} | {rating} | {kind} | {source} |".format(
                ad_key=str(task.get("ad_key") or ""),
                product=str(task.get("product") or ""),
                rating=str(task.get("rating_label") or task.get("rating") or ""),
                kind=str(task.get("suggested_template_kind") or ""),
                source=str(source),
            )
        )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return data_path, report_path


def generate_template_tasks_from_bitable(
    *,
    bitable_url: str,
    target_date: str,
    reviewer: str = "haopeng",
    include_legacy: bool = False,
    data_dir: Path = DATA_DIR,
    reports_dir: Path = REPORTS_DIR,
) -> tuple[list[dict[str, Any]], Path, Path]:
    records = fetch_bitable_records(bitable_url)
    tasks = build_template_tasks(
        records,
        target_date=target_date,
        reviewer=reviewer,
        include_legacy=include_legacy,
    )
    data_path, report_path = write_artifacts(
        tasks,
        target_date=target_date,
        data_dir=data_dir,
        reports_dir=reports_dir,
    )
    return tasks, data_path, report_path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="生成 VE 3星及以上素材模板识别任务")
    parser.add_argument("--date", default=yesterday_shanghai(), help="目标日期 YYYY-MM-DD，默认昨天")
    parser.add_argument("--reviewer", default="haopeng", choices=["haopeng", "weilan", "浩鹏", "尉蓝"])
    parser.add_argument("--bitable-url", default="", help="VE 主多维表 URL，默认 VIDEO_ENHANCER_BITABLE_URL")
    parser.add_argument("--include-legacy", action="store_true", help="评分为空时纳入旧字段为采纳/接受的记录")
    parser.add_argument("--data-dir", default=str(DATA_DIR))
    parser.add_argument("--reports-dir", default=str(REPORTS_DIR))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    load_project_env()
    args = parse_args(argv)
    bitable_url = (args.bitable_url or os.getenv("VIDEO_ENHANCER_BITABLE_URL") or "").strip()
    if not bitable_url:
        raise SystemExit("请配置 VIDEO_ENHANCER_BITABLE_URL 或传入 --bitable-url")
    tasks, data_path, report_path = generate_template_tasks_from_bitable(
        bitable_url=bitable_url,
        target_date=args.date,
        reviewer=args.reviewer,
        include_legacy=bool(args.include_legacy),
        data_dir=Path(args.data_dir),
        reports_dir=Path(args.reports_dir),
    )
    print(f"[ve-template-recognition] tasks={len(tasks)} json={data_path} report={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
