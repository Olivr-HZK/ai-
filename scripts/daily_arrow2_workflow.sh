#!/usr/bin/env bash
# Arrow2 日流程：爬取 → arrow2 主库 → 封面 CLIP 去重 →（默认跳过灵感分析）→ 飞书多维表（含素材类型）。
# 统一使用 test_arrow2_first_card_fields.py（detail-v2 逐张点击 + ad_key 去重入库）。
# 展示估值（exposure_top10）推荐用封装脚本：scripts/arrow2_exposure_workflow.sh（与下方 all 等价）。
# 需灵感分析：见 arrow2_exposure_workflow.sh 或本脚本 all 后附 --analyze；需 .env：FEISHU_APP_ID/SECRET、ARROW2_BITABLE_URL 等。
# 配置说明：config/arrow2_exposure_workflow.md
#
# 用法（项目根目录）：
#   chmod +x scripts/daily_arrow2_workflow.sh
#   ./scripts/arrow2_exposure_workflow.sh              # 推荐：同下「all」
#   ./scripts/daily_arrow2_workflow.sh              # 同 all：默认仅 exposure_top10（展示估值+Top10%），不跑 latest_yesterday
#   ./scripts/daily_arrow2_workflow.sh all
#   ./scripts/daily_arrow2_workflow.sh all --analyze
#   ./scripts/daily_arrow2_workflow.sh latest_yesterday   # 仅 7 天最新+仅昨日 first_seen
#   ./scripts/daily_arrow2_workflow.sh exposure_top10     # 显式仅展示估值（与 all 等价，输出前缀带 _exposure_top10）
#   ./scripts/daily_arrow2_workflow.sh crawl-only         # 仅爬取+落盘：默认 latest_yesterday（7天最新创意+仅昨日 first_seen）
#   ./scripts/daily_arrow2_workflow.sh crawl-only exposure_top10   # 仅爬取+落盘，改用展示估值那一组
#   仅爬 + 有头浏览器 + 每产品暂停（**默认 latest_yesterday**；脚本已带 --debug）：
#   ./scripts/daily_arrow2_workflow.sh crawl-pause
#   若要「展示估值」那组再写：crawl-pause exposure_top10
#   自组 crawl-only 时务必加 --debug 或环境变量 DEBUG=1，否则无窗口可看
#   TARGET_DATE=2026-04-14 ./scripts/daily_arrow2_workflow.sh all
#
# 可选环境变量：
#   DEBUG=1
#   TARGET_DATE  业务日（默认昨日 UTC+8，未设则 workflow 内默认昨日）
#   ARROW2_OUTPUT_PREFIX              输出前缀（默认 workflow_arrow2_<date>）
#   ARROW2_ENSURE_FIELDS=0            同步时跳过自动建列，仅写表中已有列（默认会尝试建列）
#   ARROW2_WIPE_DB=1                  与 exposure_top10 联用：爬取前清空 arrow2 SQLite 两表（慎用）

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY="${ROOT}/.venv/bin/python3"
if [[ ! -x "$PY" ]]; then PY="python3"; fi
export PYTHONUNBUFFERED=1

MODE="${1:-all}"
TD="${TARGET_DATE:-}"
export TARGET_DATE="${TD}"

PREFIX="${ARROW2_OUTPUT_PREFIX:-workflow_arrow2_${TD:-$(date +%Y-%m-%d)}}"

case "$MODE" in
  all)
    if [[ "${1:-}" == "all" ]]; then shift; fi
    exec "$ROOT/scripts/arrow2_exposure_workflow.sh" "$@"
    ;;
  latest_yesterday)
    shift
    exec "$PY" scripts/workflow_arrow2_full_pipeline.py \
      ${TD:+--date "$TD"} \
      --output-prefix "${PREFIX}_latest_yesterday" \
      --pull-only latest_yesterday \
      "$@"
    ;;
  exposure_top10|exposure)
    shift
    WFLAG=()
    if [[ "${ARROW2_WIPE_DB:-}" == "1" ]]; then WFLAG+=(--wipe-db); fi
    exec "$PY" scripts/workflow_arrow2_full_pipeline.py \
      ${TD:+--date "$TD"} \
      --output-prefix "${PREFIX}_exposure_top10" \
      --pull-only exposure_top10 \
      "${WFLAG[@]}" \
      "$@"
    ;;
  crawl-only)
    shift
    PULL_SUB="latest_yesterday"
    if [[ "${1:-}" == "exposure_top10" || "${1:-}" == "exposure" ]]; then
      PULL_SUB="exposure_top10"
      shift
    elif [[ "${1:-}" == "latest_yesterday" ]]; then
      shift
    fi
    exec "$PY" scripts/test_arrow2_first_card_fields.py \
      ${TD:+--date "$TD"} \
      --all-products \
      --no-pause \
      --output-prefix "${PREFIX}_${PULL_SUB}" \
      --pull-only "$PULL_SUB" \
      "$@"
    ;;
  crawl-pause|crawl-pause-per-product|crawl-interactive)
    shift
    PULL_SUB="latest_yesterday"
    if [[ "${1:-}" == "exposure_top10" || "${1:-}" == "exposure" ]]; then
      PULL_SUB="exposure_top10"
      shift
    elif [[ "${1:-}" == "latest_yesterday" ]]; then
      shift
    fi
    export DEBUG=1
    exec "$PY" scripts/test_arrow2_first_card_fields.py \
      ${TD:+--date "$TD"} \
      --all-products \
      --output-prefix "${PREFIX}_${PULL_SUB}" \
      --pull-only "$PULL_SUB" \
      "$@"
    ;;
  *)
    echo "用法: $0 [all | latest_yesterday | exposure_top10 | crawl-only [exposure_top10|latest_yesterday] | crawl-pause [exposure_top10] …]（默认 all=展示估值；crawl-only/crawl-pause 无参时=latest_yesterday）" >&2
    exit 1
    ;;
esac
