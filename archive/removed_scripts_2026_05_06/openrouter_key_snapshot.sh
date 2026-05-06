#!/usr/bin/env bash
# 查询 OpenRouter Key 信息（含用量相关字段），用于与「跑流程前后」对比。
# 依赖项目根 .env 中的 OPENROUTER_API_KEY。
#
# 用法：
#   ./scripts/openrouter_key_snapshot.sh
#   ./scripts/openrouter_key_snapshot.sh | tee /tmp/or_before.txt
#
# 与全流程对比示例：
#   ./scripts/openrouter_key_snapshot.sh | tee logs/openrouter_before.log
#   ./.venv/bin/python3 scripts/workflow_video_enhancer_full_pipeline.py --date 2026-04-01
#   ./scripts/openrouter_key_snapshot.sh | tee logs/openrouter_after.log

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ -f "${ROOT_DIR}/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "${ROOT_DIR}/.env"
  set +a
fi

if [ -z "${OPENROUTER_API_KEY:-}" ]; then
  echo "[openrouter] 未设置 OPENROUTER_API_KEY（请在 .env 配置）" >&2
  exit 1
fi

echo "========== $(date '+%F %T') OpenRouter GET /api/v1/key =========="
curl -sS -i "https://openrouter.ai/api/v1/key" \
  -H "Authorization: Bearer ${OPENROUTER_API_KEY}"
echo ""
