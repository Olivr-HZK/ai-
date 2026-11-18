#!/usr/bin/env bash
# 05:00 scheduled entry: collection is pure CLI, no agent needed.
# Codex only reads `check` / `watchdog` output to decide whether to continue.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="${AFIZZY_PYTHON:-$ROOT/scripts/afizzy-python}"
TARGET="${AFIZZY_TARGET_DATE:-$("$PY" -c 'from datetime import datetime,timedelta,timezone;print((datetime.now(timezone(timedelta(hours=8)))-timedelta(days=1)).date().isoformat())')}"
RUN_DIR="${AFIZZY_RUN_DIR:-$ROOT/data/afizzy_daily/$TARGET}"
echo "[afizzy-0500] target=$TARGET run_dir=$RUN_DIR"
"$PY" -m ua_workflows.afizzy_daily daily --date "$TARGET" --run-dir "$RUN_DIR"
# Report only; never blocks the collection result on alert delivery.
"$PY" -m ua_workflows.afizzy_daily watchdog --date "$TARGET" --run-dir "$RUN_DIR" ${AFIZZY_WATCHDOG_ARGS:-} || true
