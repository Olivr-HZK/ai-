#!/usr/bin/env bash
# VE 反馈训练：每天从审核多维表拉取「接受情况」，独立落库并训练素材偏好 baseline。
# 建议 crontab（北京时间由 TZ 固定，不依赖本机系统时区）：
#   40 9 * * * /path/to/repo/scripts/cron_ve_feedback_training_daily.sh >> /path/to/repo/logs/cron_ve_feedback_training.log 2>&1
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

RUN_DATE="${1:-$(date '+%F')}"
BITABLE_URL="${VE_FEEDBACK_BITABLE_URL:-https://scnmrtumk0zm.feishu.cn/base/CivwbJ2HkazcKTsKnbGclA5RnWc?table=tblrZZvVuFcjL0kE&view=vewJtPixtM}"

echo "======== $(date '+%F %T %Z') cron_ve_feedback_training_daily start date=${RUN_DATE} ========"
exec "$ROOT/.venv/bin/python" "$ROOT/scripts/run_ve_feedback_training.py" run \
  --date "$RUN_DATE" \
  --url "$BITABLE_URL"
