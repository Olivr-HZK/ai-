#!/usr/bin/env bash
# Video Enhancer：低风险留存维护。默认 dry-run，只写报告，不改多维表、不删本地产物。
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

echo "======== $(date '+%F %T %Z') cron_ve_retention_weekly start ========"
EXTRA_ARGS=()
if [[ -n "${VE_RETENTION_EXTRA_ARGS:-}" ]]; then
  # shellcheck disable=SC2206
  EXTRA_ARGS=(${VE_RETENTION_EXTRA_ARGS})
fi
"$ROOT/.venv/bin/python" "$ROOT/scripts/run_ve_retention.py" \
  --archive-after-days "${VE_RETENTION_ARCHIVE_AFTER_DAYS:-45}" \
  --artifact-keep-days "${VE_RETENTION_ARTIFACT_KEEP_DAYS:-30}" \
  --keep-remote-snapshots "${VE_RETENTION_KEEP_REMOTE_SNAPSHOTS:-3}" \
  "${EXTRA_ARGS[@]}"
echo "======== $(date '+%F %T %Z') cron_ve_retention_weekly done ========"
