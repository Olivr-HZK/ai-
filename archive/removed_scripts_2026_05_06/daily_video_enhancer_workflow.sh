#!/usr/bin/env bash

# 每日定时：Video Enhancer 全流程
# - 爬取（video enhancer 全部产品，含 AI Mirror，按昨天日期）
# - 视频灵感分析
# - 统一 UA 建议（方向卡片）
# - 同步多维表 + 飞书卡片推送
#
# 建议 crontab（每天 10:30）：
# 30 10 * * * /bin/bash /Users/oliver/guru/ua素材/scripts/daily_video_enhancer_workflow.sh

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# cron / 管道到 tee 时无 TTY，Python 默认块缓冲 stdout，日志长时间不刷易被误认为「卡住」；
# 命令行交互终端多为行缓冲，故体感差异大。见 workflow_video_enhancer_full_pipeline 中子进程 env。
export PYTHONUNBUFFERED=1

LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "$LOG_DIR"
LOG_FILE_BASE="${LOG_DIR}/daily_video_enhancer_workflow.log"

PYTHON="${ROOT_DIR}/.venv/bin/python3"
if [ ! -x "$PYTHON" ]; then
  echo "[$(date '+%F %T')] 找不到可执行 .venv/bin/python3" >> "$LOG_FILE_BASE"
  exit 1
fi

# 默认为“昨天”
TARGET_DATE="$(date -v-1d +%F 2>/dev/null || "$PYTHON" - <<'PY'
from datetime import date, timedelta
print((date.today() - timedelta(days=1)).isoformat())
PY
)"

LOG_FILE_RUN="${LOG_DIR}/daily_video_enhancer_workflow_${TARGET_DATE}.log"

# 飞书多维表 URL：在项目根 .env 中配置 VIDEO_ENHANCER_BITABLE_URL（Python 会自动 load_dotenv）

{
  echo ""
  echo "========== $(date '+%F %T') Video Enhancer 工作流开始 =========="
  echo "[$(date '+%F %T')] 工作目录: $ROOT_DIR"
  echo "[$(date '+%F %T')] 目标日期: $TARGET_DATE"

  # 多模态封面日内聚类去重（同 appid 内相似封面风格只保留展示估值最高一条）；设为 0 可关闭
  export COVER_STYLE_INTRADAY_ENABLED="${COVER_STYLE_INTRADAY_ENABLED:-1}"

  # OpenRouter：启动前 / 结束后各查一次 Key（便于对比单次 run 消耗）。需 .env 中 OPENROUTER_API_KEY；设 OPENROUTER_METER=1 开启
  if [ -f "${ROOT_DIR}/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    . "${ROOT_DIR}/.env"
    set +a
  fi
  _openrouter_meter_curl() {
    local phase="$1"
    if [ "${OPENROUTER_METER:-}" != "1" ]; then
      return 0
    fi
    if [ -z "${OPENROUTER_API_KEY:-}" ]; then
      echo "[$(date '+%F %T')] [openrouter] ${phase}: 未设置 OPENROUTER_API_KEY，跳过"
      return 0
    fi
    echo "[$(date '+%F %T')] [openrouter] ${phase} GET https://openrouter.ai/api/v1/key"
    curl -sS -i "https://openrouter.ai/api/v1/key" \
      -H "Authorization: Bearer ${OPENROUTER_API_KEY}" \
      || echo "[$(date '+%F %T')] [openrouter] ${phase}: curl 失败（不影响主流程）"
    echo ""
  }

  _openrouter_meter_curl "启动前"

  "$PYTHON" scripts/workflow_video_enhancer_full_pipeline.py \
    --date "$TARGET_DATE"

  _openrouter_meter_curl "结束后"

  echo "[$(date '+%F %T')] ========== Video Enhancer 工作流完成 =========="
  echo ""
} 2>&1 | tee -a "$LOG_FILE_RUN" >> "$LOG_FILE_BASE"

