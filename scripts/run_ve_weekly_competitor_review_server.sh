#!/usr/bin/env bash
# VE weekly competitor review server runner.
# Run this after the Monday VE crawl/sync/push finishes. It does not install or edit crontab.
set -euo pipefail

export TZ="${TZ:-Asia/Shanghai}"
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:${PATH:-}"
export PYTHONUNBUFFERED="${PYTHONUNBUFFERED:-1}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
mkdir -p "$ROOT/logs"

PYTHON="$ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  echo "[weekly-competitor-review] 未找到 $PYTHON" >&2
  exit 1
fi

RUN_DATE="${VE_WEEKLY_COMPETITOR_REVIEW_RUN_DATE:-$(date '+%F')}"
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-date)
      RUN_DATE="$2"
      shift 2
      ;;
    --no-send|--no-bitable)
      EXTRA_ARGS+=("$1")
      shift
      ;;
    --week-start|--data-dir|--bitable-url|--reviewer-field|--feishu-webhook|--material-threshold|--adoption-threshold|--candidate-threshold|--chart-raw)
      EXTRA_ARGS+=("$1" "$2")
      shift 2
      ;;
    *)
      EXTRA_ARGS+=("$1")
      shift
      ;;
  esac
done

CHART_RAW="$ROOT/data/guangdada_new_charts_ai_tools_${RUN_DATE}_raw.json"

echo "======== $(date '+%F %T %Z') weekly_competitor_review_server start ========"
echo "[weekly-competitor-review] run_date=$RUN_DATE"

if [[ "${VE_WEEKLY_COMPETITOR_CHART_ENABLED:-1}" != "0" ]]; then
  echo "======== $(date '+%F %T %Z') weekly_new_charts start ========"
  "$PYTHON" "$ROOT/scripts/run_new_charts_ai_tools.py" \
    --date "$RUN_DATE" \
    --category AI图像生成 \
    --category AI视频 \
    --limit "${VE_WEEKLY_COMPETITOR_CHART_LIMIT:-100}" \
    --category-mode combined
fi

REVIEW_ARGS=(--run-date "$RUN_DATE")
if [[ -f "$CHART_RAW" ]]; then
  REVIEW_ARGS+=(--chart-raw "$CHART_RAW")
fi
REVIEW_ARGS+=("${EXTRA_ARGS[@]}")

echo "======== $(date '+%F %T %Z') weekly_competitor_review card start ========"
"$PYTHON" "$ROOT/scripts/run_ve_weekly_competitor_review.py" "${REVIEW_ARGS[@]}"

echo "======== $(date '+%F %T %Z') weekly_competitor_review_server done ========"
