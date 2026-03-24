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

LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/daily_video_enhancer_workflow.log"

PYTHON="${ROOT_DIR}/.venv/bin/python3"
if [ ! -x "$PYTHON" ]; then
  echo "[$(date '+%F %T')] 找不到可执行 .venv/bin/python3" >> "$LOG_FILE"
  exit 1
fi

# 默认为“昨天”
TARGET_DATE="$(date -v-1d +%F 2>/dev/null || python3 - <<'PY'
from datetime import date, timedelta
print((date.today() - timedelta(days=1)).isoformat())
PY
)"

# 飞书多维表 URL：在项目根 .env 中配置 VIDEO_ENHANCER_BITABLE_URL（Python 会自动 load_dotenv）

{
  echo ""
  echo "========== $(date '+%F %T') Video Enhancer 工作流开始 =========="
  echo "[$(date '+%F %T')] 工作目录: $ROOT_DIR"
  echo "[$(date '+%F %T')] 目标日期: $TARGET_DATE"

  "$PYTHON" scripts/workflow_video_enhancer_full_pipeline.py \
    --date "$TARGET_DATE"

  echo "[$(date '+%F %T')] ========== Video Enhancer 工作流完成 =========="
  echo ""
} >> "$LOG_FILE" 2>&1

