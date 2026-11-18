#!/bin/zsh
# 需求池·换语言监听服务：只处理「替换语言」需求，独立于换人服务。
set -u
cd "$(dirname "$0")/.." || exit 1

mkdir -p data/replica-pool-translate

pkill -f "replica_pool_run.py --auth-mode browser --run-dir data/replica-pool-translate" 2>/dev/null
sleep 1

while true; do
  .venv/bin/python scripts/replica_pool_run.py --auth-mode browser \
      --types 替换语言 --run-dir data/replica-pool-translate
  sleep 120
done
