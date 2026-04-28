#!/usr/bin/env bash
# Arrow2「展示估值」日流程（封装入口）
# ——————————————————————————————————————————————————————————————
# 对应 config/arrow2_competitor.json 中 pull_spec id = exposure_top10：
#   30 天 + 展示估值排序 + Top10% 人气标签；每词条数上限见该 pull_spec 或 ARROW2_MAX_CREATIVES_PER_KEYWORD。
# 流水线：爬取（test_arrow2_first_card_fields，detail-v2 逐张点击 + ad_key 去重）→ data/arrow2_pipeline.db
#        → 封面 CLIP 去重 →（默认跳过 LLM）→ 飞书多维表 sync_arrow2_to_bitable。
# 灵感分析需显式：在下方命令后附加 --analyze（会跑 analyze_video_from_raw_json --arrow2）。
#
# 用法（项目根目录）：
#   chmod +x scripts/arrow2_exposure_workflow.sh
#   ./scripts/arrow2_exposure_workflow.sh
#   TARGET_DATE=2026-04-21 ./scripts/arrow2_exposure_workflow.sh
#   ./scripts/arrow2_exposure_workflow.sh --analyze
#   ./scripts/arrow2_exposure_workflow.sh --debug
#   ./scripts/arrow2_exposure_workflow.sh --skip-sync          # 仅爬取+入库，不同步飞书
#   ARROW2_WIPE_DB=1 ./scripts/arrow2_exposure_workflow.sh     # 爬取前清空 Arrow2 两表（慎用）
#
# 环境变量：与 workflow_arrow2_full_pipeline.py 一致，见 config/arrow2_exposure_workflow.md
# ——————————————————————————————————————————————————————————————
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export PYTHONUNBUFFERED=1

PY="${ROOT}/.venv/bin/python3"
if [[ ! -x "$PY" ]]; then PY="python3"; fi

TD="${TARGET_DATE:-}"
export TARGET_DATE="${TD}"

PREFIX="${ARROW2_OUTPUT_PREFIX:-workflow_arrow2_${TD:-$(date +%Y-%m-%d)}}"

WFLAG=()
if [[ "${ARROW2_WIPE_DB:-}" == "1" ]]; then
  WFLAG+=(--wipe-db)
fi

exec "$PY" "${ROOT}/scripts/workflow_arrow2_full_pipeline.py" \
  ${TD:+--date "$TD"} \
  --output-prefix "$PREFIX" \
  --pull-only exposure_top10 \
  "${WFLAG[@]}" \
  "$@"
