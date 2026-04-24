"""
Arrow2 全流程：爬取 → 写入 arrow2 主库 → 封面 CLIP 去重 →（可选）灵感分析 → 飞书多维表同步。
默认**不跑**灵感分析，仅按 raw 同步多维表（含素材类型：视频/图片/试玩广告）；需 LLM 分析时加 --analyze。
无聚类、无 Video Enhancer daily_creative_insights、无「我方已投」筛选。

用法（项目根目录）：
  ./scripts/arrow2_exposure_workflow.sh
  ./scripts/daily_arrow2_workflow.sh all
  .venv/bin/python scripts/workflow_arrow2_full_pipeline.py
    # 默认仅 exposure_top10（展示估值）；--all-pull-specs 可跑满 config 全部类
  .venv/bin/python scripts/workflow_arrow2_full_pipeline.py --analyze
  .venv/bin/python scripts/workflow_arrow2_full_pipeline.py --date 2026-04-14 --pull-only latest_yesterday
  TARGET_DATE=2026-04-14 .venv/bin/python scripts/workflow_arrow2_full_pipeline.py --skip-sync
  .venv/bin/python scripts/workflow_arrow2_full_pipeline.py --test-db --wipe-db --products com.arrow.out --pull-only exposure_top10
  .venv/bin/python scripts/workflow_arrow2_full_pipeline.py --debug --debug-dom-probe 5 --debug-dom-probe-only --skip-sync
  # --test-db：写入 data/arrow2_pipeline_test.db，不碰默认 arrow2_pipeline.db
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from path_util import DATA_DIR, PROJECT_ROOT

# 与 .env.example 一致；未设置环境变量时使用（仍可在 .env 中覆盖）
DEFAULT_ARROW2_BITABLE_URL = (
    "https://scnmrtumk0zm.feishu.cn/base/W8QMbUR1vaiUGUskOF2cwnXenBe"
    "?table=tblQYmtjrgcS21xO&view=vewaeIFfng"
)

PY = sys.executable


def _beijing_yesterday_iso() -> str:
    tz = timezone(timedelta(hours=8))
    return (datetime.now(tz).date() - timedelta(days=1)).isoformat()


def _beijing_today_iso() -> str:
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz).date().isoformat()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Arrow2 全流程")
    p.add_argument("--date", default="", help="target_date（YYYY-MM-DD），默认昨日 UTC+8")
    p.add_argument(
        "--output-prefix",
        default="",
        help="与 test_arrow2_competitors --output-prefix 一致；默认 workflow_arrow2_<date>",
    )
    p.add_argument(
        "--pull-only",
        default="exposure_top10",
        help="pull_specs 的 id，逗号分隔；默认 exposure_top10（30 天+展示估值+Top10%%）。"
        "多类示例：latest_yesterday,exposure_top10",
    )
    p.add_argument(
        "--all-pull-specs",
        action="store_true",
        help="不按 --pull-only 过滤，按 config 中全部 pull_specs 执行（含 latest_yesterday）",
    )
    p.add_argument(
        "--products",
        default="",
        help="传给 test_arrow2_competitors：逗号分隔，只跑这些产品（与 config 中 keyword / match / appid 匹配）",
    )
    p.add_argument(
        "--skip-products",
        default="",
        help="传给 test_arrow2_competitors --skip-products；未传时读环境变量 ARROW2_SKIP_PRODUCTS",
    )
    p.add_argument("--skip-cover", action="store_true", help="跳过封面 CLIP 去重")
    p.add_argument(
        "--analyze",
        action="store_true",
        help="执行 Step4 灵感分析；默认跳过，仅同步 raw 到多维表",
    )
    p.add_argument("--skip-sync", action="store_true", help="跳过飞书多维表")
    p.add_argument(
        "--wipe-db",
        action="store_true",
        help="Step1 爬取前传给 test_arrow2_competitors：清空 arrow2 SQLite 两表全部行",
    )
    p.add_argument(
        "--test-db",
        action="store_true",
        help="使用独立测试库 data/arrow2_pipeline_test.db（不写入默认 data/arrow2_pipeline.db）；"
        "可覆盖环境变量 ARROW2_TEST_SQLITE_PATH 为绝对路径或其它相对路径",
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help="有头浏览器：传给 test_arrow2_competitors --debug",
    )
    p.add_argument(
        "--debug-dom-probe",
        type=int,
        default=0,
        metavar="N",
        help="地区 DOM 探针：点当前列表前 N 张卡、打印并暂停；同 ARROW2_DEBUG_DOM_PROBE_FIRST",
    )
    p.add_argument(
        "--debug-dom-probe-only",
        action="store_true",
        help="与 --debug-dom-probe 同用：多轮点卡前即结束（探针后返回）",
    )
    return p.parse_args()


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()
    if args.test_db:
        override = (os.getenv("ARROW2_TEST_SQLITE_PATH") or "").strip()
        if override:
            p = Path(override)
            test_sqlite = p if p.is_absolute() else (PROJECT_ROOT / p)
        else:
            test_sqlite = PROJECT_ROOT / "data" / "arrow2_pipeline_test.db"
        os.environ["ARROW2_SQLITE_PATH"] = str(test_sqlite)
        print(f"[arrow2-pipeline] 测试库 ARROW2_SQLITE_PATH={test_sqlite}")
    td = (args.date or os.getenv("TARGET_DATE") or "").strip()
    if not td:
        td = _beijing_yesterday_iso()
    os.environ["TARGET_DATE"] = td

    prefix = (args.output_prefix or "").strip() or f"workflow_arrow2_{td}"
    raw_path = DATA_DIR / f"{prefix}_raw.json"

    cmd = [PY, str(PROJECT_ROOT / "scripts" / "test_arrow2_competitors.py"), "--output-prefix", prefix]
    if args.all_pull_specs:
        pass
    elif (args.pull_only or "").strip():
        cmd.extend(["--pull-only", args.pull_only.strip()])
    if (args.products or "").strip():
        cmd.extend(["--products", args.products.strip()])
    _skip = (args.skip_products or os.getenv("ARROW2_SKIP_PRODUCTS") or "").strip()
    if _skip:
        cmd.extend(["--skip-products", _skip])
    if args.wipe_db:
        cmd.append("--wipe-db")
    if getattr(args, "debug", False):
        cmd.append("--debug")
    ddp = int(getattr(args, "debug_dom_probe", 0) or 0)
    if ddp > 0:
        cmd.extend(["--debug-dom-probe", str(ddp)])
    if getattr(args, "debug_dom_probe_only", False):
        os.environ["ARROW2_DEBUG_DOM_PROBE_ONLY"] = "1"

    print(f"[arrow2-pipeline] Step1 爬取: {' '.join(cmd)}")
    r = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    if r.returncode != 0:
        raise SystemExit(r.returncode)

    if not raw_path.exists():
        raise SystemExit(f"未找到 raw 文件：{raw_path}")

    raw = json.loads(raw_path.read_text(encoding="utf-8"))
    raw["target_date"] = td
    raw["crawl_date"] = _beijing_today_iso()
    raw["workflow"] = "arrow2_competitor"

    from arrow2_pipeline_db import (
        get_arrow2_pipeline_items_from_raw_payload,
        init_db as init_a2,
        prune_arrow2_daily_insights_not_in_raw,
        upsert_arrow2_creative_library_batch,
    )

    items: list[dict[str, Any]] = get_arrow2_pipeline_items_from_raw_payload(raw)
    if not items:
        print("[arrow2-pipeline] items 为空，结束。")
        raw_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")
        return

    init_a2()
    n_lib = upsert_arrow2_creative_library_batch(td, items)
    print(f"[arrow2-pipeline] Step2 arrow2_creative_library 写入/更新 {n_lib} 条")

    if not args.skip_cover:
        from arrow2_cover_style_intraday import apply_arrow2_cover_style_dedupe

        items2, cov_rep = apply_arrow2_cover_style_dedupe(items, td, raw.get("crawl_date"))
        raw["items"] = items2
        raw["items_deduped_by_ad_key"] = items2
        raw["cover_style_report"] = cov_rep
        prune_arrow2_daily_insights_not_in_raw(td, raw)
        cov_path = DATA_DIR / f"{prefix}_cover_style_intraday.json"
        cov_path.write_text(json.dumps(cov_rep, ensure_ascii=False, indent=2), encoding="utf-8")
        print(
            f"[arrow2-pipeline] Step3 封面去重：{cov_rep.get('input_count')} → {cov_rep.get('output_count')} 条；报告 {cov_path.name}"
        )
    else:
        raw["items"] = items
        raw["items_deduped_by_ad_key"] = items

    raw_path.write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")

    analysis_path = DATA_DIR / f"video_analysis_{prefix}_raw.json"
    if args.analyze:
        acmd = [
            PY,
            str(PROJECT_ROOT / "scripts" / "analyze_video_from_raw_json.py"),
            "--input",
            str(raw_path),
            "--output",
            str(analysis_path),
            "--arrow2",
        ]
        print(f"[arrow2-pipeline] Step4 灵感分析: {' '.join(acmd)}")
        r2 = subprocess.run(acmd, cwd=str(PROJECT_ROOT))
        if r2.returncode != 0:
            raise SystemExit(r2.returncode)
    else:
        print("[arrow2-pipeline] 跳过灵感分析（默认）；写入占位分析 JSON 供仅 raw 同步")
        stub = {
            "input_file": str(raw_path),
            "workflow": "arrow2_competitor",
            "sync_from_raw_only": True,
            "results": [],
        }
        analysis_path.write_text(json.dumps(stub, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.skip_sync:
        print("[arrow2-pipeline] 跳过多维表同步")
        return

    if not analysis_path.exists():
        print("[arrow2-pipeline] 未找到分析占位/结果 JSON，跳过多维表同步")
        return

    bitable = (os.getenv("ARROW2_BITABLE_URL") or "").strip() or DEFAULT_ARROW2_BITABLE_URL
    if bitable == DEFAULT_ARROW2_BITABLE_URL and not (os.getenv("ARROW2_BITABLE_URL") or "").strip():
        print("[arrow2-pipeline] 使用默认 ARROW2_BITABLE_URL（可在项目根 .env 中设置 ARROW2_BITABLE_URL 覆盖）")

    scmd = [
        PY,
        str(PROJECT_ROOT / "scripts" / "sync_arrow2_to_bitable.py"),
        "--url",
        bitable,
        "--raw",
        str(raw_path),
        "--analysis",
        str(analysis_path),
    ]
    print(f"[arrow2-pipeline] Step5 同步: {' '.join(scmd)}")
    r3 = subprocess.run(scmd, cwd=str(PROJECT_ROOT))
    if r3.returncode != 0:
        raise SystemExit(r3.returncode)
    print("[arrow2-pipeline] 完成。")


if __name__ == "__main__":
    main()
