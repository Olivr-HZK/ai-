#!/usr/bin/env bash

# 旧版每日任务（全量竞品 batch_crawl + analyze + 飞书等）
# 若仍需跑旧流程，请单独在 crontab 里调用本脚本，或手动执行。
# 默认每日入口已改为 Video Enhancer Pipeline：见 daily_ua_job.sh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "$LOG_DIR"

PYTHON="${ROOT_DIR}/.venv/bin/python3"
if [ ! -x "$PYTHON" ]; then
  echo "[$(date '+%F %T')] 找不到可执行 .venv/bin/python3" >> "${LOG_DIR}/daily_ua_job_legacy.log"
  exit 1
fi
LOG_FILE="${LOG_DIR}/daily_ua_job_legacy.log"
export PYTHONUNBUFFERED=1

{
  echo ""
  echo "========== $(date '+%F %T') [LEGACY] 本次任务开始 =========="
  echo "[$(date '+%F %T')] 工作目录: $ROOT_DIR"
  RUN_DATE="$(date +%F)"
  echo "[$(date '+%F %T')] 爬取日期: $RUN_DATE"

  echo "[$(date '+%F %T')] ---------- 步骤 1/5: 竞品批量爬取 ----------"
  MAX_CRAWL_RETRIES=3
  RETRY_DELAY=60
  crawl_ok=0
  for attempt in $(seq 1 $MAX_CRAWL_RETRIES); do
    if "$PYTHON" scripts/batch_crawl_ai_products_dated.py; then
      crawl_ok=1
      break
    fi
    echo "[$(date '+%F %T')] [步骤 1] 第 ${attempt}/${MAX_CRAWL_RETRIES} 次失败"
    if [ "$attempt" -lt "$MAX_CRAWL_RETRIES" ]; then
      sleep $RETRY_DELAY
    fi
  done
  if [ "$crawl_ok" -eq 0 ]; then
    echo "[$(date '+%F %T')] [失败] 步骤 1 在 ${MAX_CRAWL_RETRIES} 次尝试后仍失败"
    exit 1
  fi

  echo "[$(date '+%F %T')] ---------- 步骤 2/5: 竞品广告创意分析 ----------"
  "$PYTHON" scripts/analyze_creatives_with_llm.py --date "$RUN_DATE"

  echo "[$(date '+%F %T')] ---------- 步骤 3/5: 同步基础创意多维表 ----------"
  "$PYTHON" scripts/sync_ad_creative_basic_by_date.py --date "$RUN_DATE"

  echo "[$(date '+%F %T')] ---------- 步骤 4/5: 同步灵感多维表 ----------"
  "$PYTHON" scripts/daily_sync_latest_creative_to_bitable.py

  echo "[$(date '+%F %T')] ---------- 步骤 5/5: 飞书日报 ----------"
  "$PYTHON" scripts/push_ai_weekly_to_feishu.py

  echo "[$(date '+%F %T')] ========== [LEGACY] 本次任务全部完成 =========="
  echo ""
} >> "$LOG_FILE" 2>&1
