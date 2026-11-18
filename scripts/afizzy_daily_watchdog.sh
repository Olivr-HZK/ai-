#!/usr/bin/env bash
# Hourly stall check for the daily pipeline. Exit 2 means missing/stalled/failed.
# Schedule independently of the 05:00 collection so a stop between stages is visible.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="${AFIZZY_PYTHON:-$ROOT/scripts/afizzy-python}"
TARGET="${AFIZZY_TARGET_DATE:-$("$PY" -c 'from datetime import datetime,timedelta,timezone;print((datetime.now(timezone(timedelta(hours=8)))-timedelta(days=1)).date().isoformat())')}"
RUN_DIR="${AFIZZY_RUN_DIR:-$ROOT/data/afizzy_daily/$TARGET}"
exec "$PY" -m ua_workflows.afizzy_daily watchdog --date "$TARGET" --run-dir "$RUN_DIR" \
  --max-age "${AFIZZY_STALL_MINUTES:-120}" ${AFIZZY_WATCHDOG_ARGS:-}
