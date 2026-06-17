#!/usr/bin/env python3
"""Build a Feishu Base Workflow body for VE template-copy button triggering."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ua_workflows.video_enhancer.template_trigger import build_button_workflow_payload


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="生成 VE 模板复刻按钮 Workflow JSON")
    parser.add_argument("--trigger-url", required=True, help="Feishu Workflow 可访问的 POST /trigger URL")
    parser.add_argument("--table-name", required=True, help="按钮所在数据表名称")
    parser.add_argument("--token", default="", help="可选触发 token，会写入 Workflow 请求体")
    parser.add_argument("--title", default="VE 模板复刻按钮触发")
    parser.add_argument("--client-token", default="")
    parser.add_argument("--output", default="", help="输出 JSON 文件；默认打印到 stdout")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = build_button_workflow_payload(
        trigger_url=args.trigger_url,
        table_name=args.table_name,
        token=args.token,
        title=args.title,
        client_token=args.client_token,
    )
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
