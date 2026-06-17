#!/usr/bin/env bash
# VE 大盘周榜：周一抓取上一完整自然周的三张广大大榜单并推送飞书。
set -euo pipefail

export TZ="${TZ:-Asia/Shanghai}"
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:${PATH:-}"
export PYTHONUNBUFFERED="${PYTHONUNBUFFERED:-1}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
mkdir -p "$ROOT/logs"

PYTHON="$ROOT/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  echo "[market-weekly-charts] 未找到 $PYTHON" >&2
  exit 1
fi

if [[ -n "${VE_MARKET_WEEKLY_CHARTS_WEEK_END:-}" ]]; then
  WEEK_END="$VE_MARKET_WEEKLY_CHARTS_WEEK_END"
  WEEK_START="${VE_MARKET_WEEKLY_CHARTS_WEEK_START:-$("$PYTHON" - "$WEEK_END" <<'PY'
from datetime import date, timedelta
import sys
week_end = date.fromisoformat(sys.argv[1])
print((week_end - timedelta(days=6)).isoformat())
PY
)}"
else
  read -r WEEK_START WEEK_END < <("$PYTHON" <<'PY'
from datetime import date, timedelta

today = date.today()
days_since_sunday = (today.weekday() + 1) % 7
if days_since_sunday == 0:
    days_since_sunday = 7
week_end = today - timedelta(days=days_since_sunday)
week_start = week_end - timedelta(days=6)
print(week_start.isoformat(), week_end.isoformat())
PY
)
fi

LIMIT="${VE_MARKET_WEEKLY_CHARTS_LIMIT:-80}"
AUTH_MODE="${VE_MARKET_WEEKLY_CHARTS_AUTH_MODE:-env-login}"
CATEGORY_MODE="${VE_MARKET_WEEKLY_CHARTS_CATEGORY_MODE:-combined}"
FOCUS_TOP_N="${VE_MARKET_WEEKLY_CHARTS_FOCUS_TOP_N:-10}"
PER_CHART_TOP_N="${VE_MARKET_WEEKLY_CHARTS_PER_CHART_TOP_N:-10}"
SCROLL_ROUNDS="${VE_MARKET_WEEKLY_CHARTS_SCROLL_ROUNDS:-32}"
WAIT_AFTER_FILTER_MS="${VE_MARKET_WEEKLY_CHARTS_WAIT_AFTER_FILTER_MS:-3000}"

CRAWL_EXTRA_ARGS=()
if [[ -n "${VE_MARKET_WEEKLY_CHARTS_CRAWL_EXTRA_ARGS:-}" ]]; then
  # shellcheck disable=SC2206
  CRAWL_EXTRA_ARGS=(${VE_MARKET_WEEKLY_CHARTS_CRAWL_EXTRA_ARGS})
fi

PUSH_EXTRA_ARGS=()
if [[ -n "${VE_MARKET_WEEKLY_CHARTS_PUSH_EXTRA_ARGS:-}" ]]; then
  # shellcheck disable=SC2206
  PUSH_EXTRA_ARGS=(${VE_MARKET_WEEKLY_CHARTS_PUSH_EXTRA_ARGS})
fi

echo "======== $(date '+%F %T %Z') cron_ve_market_weekly_charts start ========"
echo "[market-weekly-charts] week_start=$WEEK_START week_end=$WEEK_END limit=$LIMIT auth_mode=$AUTH_MODE"

CHART_RAW_PATHS=()
for chart_type in new hot surge; do
  output_prefix="guangdada_market_weekly_${chart_type}_${WEEK_START}_${WEEK_END}"
  echo "======== $(date '+%F %T %Z') market_weekly crawl chart=${chart_type} ========"
  "$PYTHON" "$ROOT/scripts/run_new_charts_ai_tools.py" \
    --chart-type "$chart_type" \
    --date "$WEEK_END" \
    --auth-mode "$AUTH_MODE" \
    --limit "$LIMIT" \
    --category-mode "$CATEGORY_MODE" \
    --scroll-rounds "$SCROLL_ROUNDS" \
    --wait-after-filter-ms "$WAIT_AFTER_FILTER_MS" \
    --output-prefix "$output_prefix" \
    "${CRAWL_EXTRA_ARGS[@]}"
  CHART_RAW_PATHS+=("$ROOT/data/${output_prefix}_raw.json")
done

PUSH_ARGS=(
  --week-start "$WEEK_START"
  --week-end "$WEEK_END"
  --focus-top-n "$FOCUS_TOP_N"
  --per-chart-top-n "$PER_CHART_TOP_N"
  --output-md "$ROOT/reports/ve_market_weekly_charts_${WEEK_START}_${WEEK_END}.md"
)
for path in "${CHART_RAW_PATHS[@]}"; do
  PUSH_ARGS+=(--chart-raw "$path")
done
if [[ "${VE_MARKET_WEEKLY_CHARTS_DRY_RUN:-0}" == "1" ]]; then
  PUSH_ARGS+=(--dry-run)
fi
if [[ ${#PUSH_EXTRA_ARGS[@]} -gt 0 ]]; then
  PUSH_ARGS+=("${PUSH_EXTRA_ARGS[@]}")
fi

echo "======== $(date '+%F %T %Z') market_weekly push ========"
"$PYTHON" "$ROOT/scripts/run_ve_market_weekly_charts_push.py" "${PUSH_ARGS[@]}"

echo "======== $(date '+%F %T %Z') cron_ve_market_weekly_charts done ========"
