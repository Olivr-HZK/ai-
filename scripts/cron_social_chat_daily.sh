#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PY="$ROOT/.venv/bin/python"
exec "$PY" "$ROOT/scripts/run_social_chat_daily.py" "$@"
