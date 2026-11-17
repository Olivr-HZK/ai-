#!/bin/zsh
# 换人监听服务包装：只读监听 + 换人执行器，两个循环各自守护；由 launchd 保活。
set -u
cd "$(dirname "$0")/.." || exit 1

mkdir -p data/replica-pool-watch data/replica-pool-run

# 重启时先清掉上一次可能遗留的子进程，避免抢锁
pkill -f "scripts/replica_pool_watch.py" 2>/dev/null
pkill -f "scripts/replica_pool_run.py" 2>/dev/null
sleep 1

( while true; do
    .venv/bin/python scripts/replica_pool_watch.py --run-dir data/replica-pool-watch --interval 15
    sleep 10
  done ) &
WATCH=$!

( while true; do
    .venv/bin/python scripts/replica_pool_run.py --auth-mode browser --run-dir data/replica-pool-run
    sleep 30
  done ) &
RUN=$!

trap 'kill $WATCH $RUN 2>/dev/null' EXIT INT TERM
wait
