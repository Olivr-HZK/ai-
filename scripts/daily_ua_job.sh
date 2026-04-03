#!/usr/bin/env bash

# 每日默认任务：Video Enhancer Pipeline（爬取→分析→UA建议→飞书多维表→企业微信/Sheet 等）
# 与 daily_video_enhancer_workflow.sh 等价；保留本文件名以便已有 crontab 无需改路径。
#
# crontab 示例（每天 10:30）：
#   30 10 * * * /bin/bash /Users/oliver/guru/ua素材/scripts/daily_ua_job.sh
#
# 旧版「全量竞品」任务见：scripts/daily_ua_job_legacy.sh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/daily_ua_job.log"

{
  echo ""
  echo "========== $(date '+%F %T') 主监控流 daily_ua_job.sh 开始 =========="
  exec /bin/bash "${ROOT_DIR}/scripts/daily_video_enhancer_workflow.sh"
} >> "$LOG_FILE" 2>&1
