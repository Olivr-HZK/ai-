#!/usr/bin/env bash

# 每日任务：批量获取 AI 产品 UA 素材 + 入库 + 广告分析 + 同步多维表格
# 建议通过 crontab 在每天早上 10:30 调用本脚本。
# crontab 示例: 30 10 * * * /bin/bash /Users/oliver/guru/ua素材/scripts/daily_ua_job.sh

set -euo pipefail

# 脚本所在目录的上级 = 项目根目录
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "$LOG_DIR"

# 使用虚拟环境里的 Python（cron 下 PATH 可能没有 venv）
PYTHON="${ROOT_DIR}/.venv/bin/python3"
if [ ! -x "$PYTHON" ]; then
  echo "[$(date '+%F %T')] 找不到可执行 .venv/bin/python3" >> "${LOG_DIR}/weekly_ua_job.log"
  exit 1
fi
LOG_FILE="${LOG_DIR}/weekly_ua_job.log"
# 让 Python 立即输出到日志，不缓冲
export PYTHONUNBUFFERED=1

{
  echo ""
  echo "========== $(date '+%F %T') 本次任务开始 =========="
  echo "[$(date '+%F %T')] 工作目录: $ROOT_DIR"
  RUN_DATE="$(date +%F)"
  echo "[$(date '+%F %T')] 爬取日期: $RUN_DATE"

  # 步骤 1: 批量爬取
  echo "[$(date '+%F %T')] ---------- 步骤 1/3: 批量爬取 AI 产品 UA 素材 ----------"
  if ! "$PYTHON" scripts/batch_crawl_ai_products_dated.py; then
    echo "[$(date '+%F %T')] [失败] 步骤 1 批量爬取脚本退出非 0"
    exit 1
  fi
  echo "[$(date '+%F %T')] [完成] 步骤 1/3 批量爬取结束"

  # 步骤 2: 广告创意分析
  echo "[$(date '+%F %T')] ---------- 步骤 2/3: 广告创意分析（翻译+拆解） ----------"
  if ! "$PYTHON" scripts/analyze_creatives_with_llm.py --date "$RUN_DATE"; then
    echo "[$(date '+%F %T')] [失败] 步骤 2 广告分析脚本退出非 0"
    exit 1
  fi
  echo "[$(date '+%F %T')] [完成] 步骤 2/3 广告分析结束"

  # 步骤 3: 同步最新广告创意到飞书多维表格
  echo "[$(date '+%F %T')] ---------- 步骤 3/3: 同步最新广告创意到飞书多维表格 ----------"
  if ! "$PYTHON" scripts/daily_sync_latest_creative_to_bitable.py; then
    echo "[$(date '+%F %T')] [失败] 步骤 3 同步多维表格脚本失败"
    exit 1
  fi
  echo "[$(date '+%F %T')] [完成] 步骤 3/3 同步多维表格结束"

  echo "[$(date '+%F %T')] ========== 本次任务全部完成 =========="
  echo ""
} >> "$LOG_FILE" 2>&1

