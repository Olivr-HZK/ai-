"""User-session supervisor with separate code and persistent state roots.

Uses the existing dedicated login bridge and original checkpoints. It never
retries failed rows or creates a new business request itself.
"""
from pathlib import Path
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ua_workflows.shared.config import PROJECT_ROOT, DATA_DIR
from ua_workflows.replica_auth_bridge import SOCKET, bridge_status, start_login
from ua_workflows.replica_studio_api import save_private


def main(role):
    if role not in ('bridge', 'watcher'):
        raise ValueError('Unknown service role')
    dep = DATA_DIR / 'deployment'
    run = DATA_DIR / 'replica-main-table-worker'
    setting = dep / 'replica-main-table.json'
    expected = json.loads(setting.read_text()).get('expected_account')
    if not expected:
        raise ValueError('An explicit expected account is required')
    status_file = dep / (role + '-supervisor.json')
    while True:
        try:
            status = bridge_status()
            if role == 'bridge' and not SOCKET.exists():
                save_private(status_file, {'phase': 'starting_owned_bridge', 'pid': os.getpid(), 'at': time.time()})
                status = start_login(timeout=60)
            authenticated = bool(status and status.get('authenticated') and status.get('account_id') == expected)
            paused = (run / 'pause-requested').exists()
            if role == 'watcher' and authenticated and not paused:
                save_private(status_file, {'phase': 'watching', 'pid': os.getpid(), 'at': time.time()})
                result = subprocess.run([sys.executable, '-u', '-m', 'ua_workflows.replica_main_table', 'watch',
                                         '--config', str(setting), '--run-dir', str(run), '--poll-interval', '15'],
                                        cwd=PROJECT_ROOT)
                save_private(status_file, {'phase': 'watcher_exited', 'exit_code': result.returncode,
                                          'pid': os.getpid(), 'at': time.time()})
                time.sleep(60)
                continue
            phase = ('authenticated' if authenticated else 'waiting_for_normal_login') if role == 'bridge' else (
                'paused' if paused else 'waiting_for_normal_login')
            save_private(status_file, {'phase': phase, 'pid': os.getpid(), 'at': time.time()})
        except Exception as error:
            save_private(status_file, {'phase': 'waiting_after_error', 'error': getattr(error, 'code', type(error).__name__),
                                      'pid': os.getpid(), 'at': time.time()})
        time.sleep(30)


if __name__ == '__main__':
    main(sys.argv[1])
