from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ua_workflows.arrow2.crawl import CONFIG_FILE, _load_entries  # noqa: E402
from ua_workflows.shared.crawl_smoke import (  # noqa: E402
    apply_browser_mode,
    build_arrow2_crawl_args,
    exposure_attempt_succeeded,
)


def _default_date() -> str:
    return (datetime.now(timezone(timedelta(hours=8))).date() - timedelta(days=1)).isoformat()


def _default_product() -> str:
    cfg = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    entries = _load_entries(cfg)
    if not entries:
        raise SystemExit("未找到 Arrow2 可测试产品，请检查 config/arrow2_competitor.json")
    return entries[0]["match"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Arrow2 exposure_top10 爬取冒烟测试：只跑一个产品，只落 raw JSON，不入库/不同步。",
        epilog=(
            "示例:\n"
            "  .venv/bin/python scripts/test_arrow2_exposure_crawl.py --headless\n"
            "  .venv/bin/python scripts/test_arrow2_exposure_crawl.py --headed --product \"Arrows - Puzzle Escape\""
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--date", default=_default_date(), help="目标日期 YYYY-MM-DD，默认昨日 UTC+8")
    parser.add_argument("--product", default="", help="只验证一个产品；默认 config 中第一个 Arrow2 产品")
    parser.add_argument("--output-prefix", default="", help="输出前缀，默认 smoke_arrow2_exposure_<date>")
    parser.add_argument("--attempts", type=int, default=3, help="无头模式最多重试次数，默认 3")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--headed", action="store_true", help="有头浏览器运行")
    mode.add_argument("--headless", action="store_true", help="无头浏览器运行（默认）")
    return parser.parse_args()


def _run_crawl(forward_args: list[str], *, headed: bool) -> tuple[int, str]:
    env = apply_browser_mode(os.environ, headed=headed, headless=not headed)
    env["PYTHONUNBUFFERED"] = "1"
    cmd = [sys.executable, "-m", "ua_workflows.arrow2.crawl", *forward_args]
    proc = subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    chunks: list[str] = []
    assert proc.stdout is not None
    for line in proc.stdout:
        print(line, end="")
        chunks.append(line)
    return proc.wait(), "".join(chunks)


def _run_crawl_interactive(forward_args: list[str]) -> int:
    env = apply_browser_mode(os.environ, headed=True, headless=False)
    env["PYTHONUNBUFFERED"] = "1"
    cmd = [sys.executable, "-m", "ua_workflows.arrow2.crawl", *forward_args]
    return subprocess.call(cmd, cwd=str(ROOT), env=env)


def main() -> None:
    args = parse_args()
    date = args.date.strip()
    product = (args.product or "").strip() or _default_product()
    output_prefix = (args.output_prefix or "").strip() or f"smoke_arrow2_exposure_{date}"
    attempts = max(1, int(args.attempts or 1))

    print(f"[smoke] Arrow2 exposure crawl-only product={product!r} date={date} output_prefix={output_prefix}")
    if args.headed:
        forward_args = build_arrow2_crawl_args(
            date=date,
            product=product,
            pull_only="exposure_top10",
            output_prefix=output_prefix,
        )
        code, _ = _run_crawl(forward_args, headed=True)
        raise SystemExit(code)

    last_code = 1
    for attempt in range(1, attempts + 1):
        attempt_prefix = f"{output_prefix}_attempt{attempt}" if attempts > 1 else output_prefix
        print(f"\n[smoke] exposure headless attempt {attempt}/{attempts} output_prefix={attempt_prefix}")
        forward_args = build_arrow2_crawl_args(
            date=date,
            product=product,
            pull_only="exposure_top10",
            output_prefix=attempt_prefix,
        )
        code, output = _run_crawl(forward_args, headed=False)
        last_code = code
        if exposure_attempt_succeeded(output, code):
            print(f"[smoke] exposure attempt {attempt}/{attempts} succeeded with Top10% selected.")
            return
        print(f"[smoke] exposure attempt {attempt}/{attempts} failed Top10% selection or crawl status.")

    headed_prefix = f"{output_prefix}_headed"
    print(
        f"\n[smoke] exposure 无头 {attempts} 次均未成功选中 Top10%，"
        "现在启动有头浏览器，请在页面上人工查看。"
    )
    forward_args = build_arrow2_crawl_args(
        date=date,
        product=product,
        pull_only="exposure_top10",
        output_prefix=headed_prefix,
    )
    forward_args.append("--pause-after-setup")
    code = _run_crawl_interactive(forward_args)
    raise SystemExit(code if code != 0 else last_code)


if __name__ == "__main__":
    main()
