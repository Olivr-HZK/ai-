#!/usr/bin/env bash
# Arrow2 latest_yesterday：昨日首见，含分析 + 飞书同步（--analyze）。
# 建议 crontab：
#   10 11 * * * /path/to/ai-/scripts/cron_ai_arrow2_latest_daily.sh >> /path/to/ai-/logs/cron_arrow2_latest.log 2>&1
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

echo "======== $(date '+%F %T %Z') cron_ai_arrow2_latest_daily start ========"
exec "$ROOT/.venv/bin/python" "$ROOT/scripts/run_arrow2_latest.py" --analyze
