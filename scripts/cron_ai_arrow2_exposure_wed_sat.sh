#!/usr/bin/env bash
# Arrow2 exposure_top10（展示估值）：与每日两条叠加；仅周三、六由 crontab 调度。
# 建议 crontab：
#   20 14 * * 3,6 /path/to/ai-/scripts/cron_ai_arrow2_exposure_wed_sat.sh >> /path/to/ai-/logs/cron_arrow2_exposure.log 2>&1
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

echo "======== $(date '+%F %T %Z') cron_ai_arrow2_exposure_wed_sat start ========"
exec "$ROOT/.venv/bin/python" "$ROOT/scripts/run_arrow2_exposure.py" --analyze
