#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

REMOTE_HOST="${VE_REMOTE_HOST:-ggbond@10.125.46.30}"
REMOTE_ROOT="${VE_REMOTE_ROOT:-/Users/ggbond/oliver/ai-}"
LOCAL_DEST="${VE_REMOTE_SNAPSHOT_DIR:-$ROOT/data/remote_snapshots/ve}"
REMOTE_TMP="${VE_REMOTE_TMP_DIR:-/tmp/ve_remote_snapshot_$(date +%Y%m%d_%H%M%S)}"
SSH_OPTS="${VE_REMOTE_SSH_OPTS:-}"

DB_REL="data/video_enhancer_pipeline.db"
LOCAL_DB="$LOCAL_DEST/$DB_REL"

mkdir -p "$LOCAL_DEST"

echo "[sync-ve] remote=${REMOTE_HOST}:${REMOTE_ROOT}"
echo "[sync-ve] local_dest=${LOCAL_DEST}"

ssh ${SSH_OPTS} "$REMOTE_HOST" "REMOTE_ROOT='$REMOTE_ROOT' REMOTE_TMP='$REMOTE_TMP' bash -s" <<'REMOTE_SCRIPT'
set -euo pipefail

cd "$REMOTE_ROOT"
if [ ! -f "data/video_enhancer_pipeline.db" ]; then
  echo "[sync-ve][remote] missing data/video_enhancer_pipeline.db under $REMOTE_ROOT" >&2
  exit 2
fi

rm -rf "$REMOTE_TMP"
mkdir -p "$REMOTE_TMP/data" "$REMOTE_TMP/reports" "$REMOTE_TMP/logs"

sqlite3 "data/video_enhancer_pipeline.db" ".backup '$REMOTE_TMP/data/video_enhancer_pipeline.db'"

find data -maxdepth 1 -type f \( \
  -name 'workflow_video_enhancer_*.json' -o \
  -name 'video_analysis_workflow_video_enhancer_*.json' -o \
  -name 'ua_suggestion_workflow_video_enhancer_*.json' -o \
  -name 'ua_suggestion_workflow_video_enhancer_*.md' -o \
  -name 've_feedback_training_*.db' -o \
  -name 've_feedback_training_dataset_*.jsonl' \
\) -exec cp -p {} "$REMOTE_TMP/data/" \; 2>/dev/null || true

find reports -maxdepth 1 -type f \( \
  -name 'workflow_video_enhancer_*.md' -o \
  -name 've_feedback_training_*.md' -o \
  -name 've_*dedupe*.md' -o \
  -name 've_*cluster*.md' \
\) -exec cp -p {} "$REMOTE_TMP/reports/" \; 2>/dev/null || true

find logs -maxdepth 1 -type f \( \
  -name 'cron_video_enhancer.log' -o \
  -name 'daily_video_enhancer_workflow*.log' \
\) -exec cp -p {} "$REMOTE_TMP/logs/" \; 2>/dev/null || true

echo "[sync-ve][remote] snapshot ready at $REMOTE_TMP"
REMOTE_SCRIPT

if command -v rsync >/dev/null 2>&1; then
  rsync -a --delete \
    --exclude 'arrow2_pipeline.db' \
    --exclude 'arrow2_*' \
    "$REMOTE_HOST:$REMOTE_TMP/" "$LOCAL_DEST/"
else
  echo "[sync-ve] rsync not found, falling back to scp"
  rm -rf "$LOCAL_DEST"
  mkdir -p "$LOCAL_DEST"
  scp -r ${SSH_OPTS} "$REMOTE_HOST:$REMOTE_TMP/." "$LOCAL_DEST/"
fi

ssh ${SSH_OPTS} "$REMOTE_HOST" "rm -rf '$REMOTE_TMP'" >/dev/null 2>&1 || true

if [ ! -f "$LOCAL_DB" ]; then
  echo "[sync-ve] missing local db after sync: $LOCAL_DB" >&2
  exit 3
fi

echo "[sync-ve] verifying $LOCAL_DB"
sqlite3 "$LOCAL_DB" 'PRAGMA quick_check;'
sqlite3 "$LOCAL_DB" "SELECT COUNT(*), COALESCE(MAX(target_date), '') FROM daily_creative_insights;"

echo "[sync-ve] Synced VE data to $LOCAL_DEST"
