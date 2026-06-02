#!/usr/bin/env bash
# Video Enhancer：昨日（UTC+8）全流程，含分析与飞书/企业微信等多维表后续（与 run_video_enhancer.py 默认一致）。
# 建议 crontab（北京时间由 TZ 固定，不依赖本机系统时区）：
#   20 5 * * * /path/to/ai-/scripts/cron_ai_video_enhancer_daily.sh >> /path/to/ai-/logs/cron_video_enhancer.log 2>&1
set -euo pipefail
export TZ="${TZ:-Asia/Shanghai}"
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:${PATH:-}"
export PYTHONUNBUFFERED="${PYTHONUNBUFFERED:-1}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
mkdir -p "$ROOT/logs"

if [[ ! -x "$ROOT/.venv/bin/python" ]]; then
  echo "[cron] 未找到 $ROOT/.venv/bin/python" >&2
  exit 1
fi

echo "======== $(date '+%F %T %Z') cron_ai_video_enhancer_daily start ========"
"$ROOT/.venv/bin/python" "$ROOT/scripts/run_video_enhancer.py"

DAILY_PUSH_CHAT_ID="$("$ROOT/.venv/bin/python" - <<'PY'
import os

from ua_workflows.shared.config import load_project_env

load_project_env()
print((os.getenv("FEISHU_DAILY_PUSH_CHAT_ID") or "").strip())
PY
)"

if [[ -z "$DAILY_PUSH_CHAT_ID" ]]; then
  echo "[cron] 未配置 FEISHU_DAILY_PUSH_CHAT_ID，跳过浩鹏 TopN 二次筛选 IM 推送。"
  exit 0
fi

echo "======== $(date '+%F %T %Z') cron_ai_video_enhancer_daily haopeng_topn start ========"
"$ROOT/.venv/bin/python" "$ROOT/scripts/run_ve_haopeng_topn_push.py" \
  --top-n 10 \
  --send-mode im \
  --chat-id "$DAILY_PUSH_CHAT_ID"

echo "======== $(date '+%F %T %Z') cron_ai_video_enhancer_daily done ========"
