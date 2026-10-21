"""Renewable local wake lease for browser-tool workflows; never fetches remote data."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from uuid import uuid4

from ua_workflows.collection_runtime import collection_phase, configured_secrets, redact, stamp, write

TTL = 900


def load(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def atomic(path, value):
    temp = path.with_name(path.name + '.' + uuid4().hex + '.tmp')
    write(temp, value)
    temp.replace(path)


def status(base):
    folder = Path(base)/'runtime/browser'
    state, control = load(folder/'state.json'), load(folder/'control.json')
    active = (state.get('status') == 'active' and state.get('token') == control.get('token')
              and control.get('action') == 'run' and control.get('deadline', 0) > time.monotonic()
              and 0 <= time.monotonic()-state.get('heartbeat', 0) < 10)
    return {**state, 'active': active, 'remaining_seconds': max(0, round(control.get('deadline', 0)-time.monotonic()))}


def serve(base, token):
    folder = Path(base)/'runtime/browser'
    with (folder/'worker.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        state = {'token': token, 'started_at': stamp(), 'pid': os.getpid()}
        try:
            with collection_phase(base, 'browser-lease'):
                if sys.platform != 'darwin':
                    raise RuntimeError('Browser wake lease currently requires macOS')
                while True:
                    control = load(folder/'control.json')
                    if control.get('token') != token or control.get('action') != 'run':
                        state['status'] = 'stopped'
                        break
                    if time.monotonic() >= control.get('deadline', 0):
                        state['status'] = 'expired'
                        break
                    state.update(status='active', heartbeat=time.monotonic())
                    atomic(folder/'state.json', state)
                    time.sleep(0.25)
        except BaseException:
            state['status'] = 'failed'
            raise
        finally:
            state['finished_at'] = stamp()
            atomic(folder/'state.json', state)


def control(base, action):
    base = Path(base).resolve()
    folder = base/'runtime/browser'
    folder.mkdir(parents=True, exist_ok=True)
    with (folder/'command.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        current = status(base)
        if action == 'status':
            return current
        if action == 'renew' and not current['active']:
            raise RuntimeError('Wake lease expired or unavailable; start and verify before continuing')
        if action == 'stop':
            old = load(folder/'control.json')
            atomic(folder/'control.json', {**old, 'action': 'stop'})
            # The worker publishes its terminal state only after releasing the assertion.
            for _ in range(40):
                state = status(base)
                if state.get('status') != 'active':
                    return state
                time.sleep(0.1)
            raise RuntimeError('Stop requested but worker release is not yet verified')
        if current['active']:
            old = load(folder/'control.json')
            atomic(folder/'control.json', {**old, 'deadline': time.monotonic()+TTL})
            return status(base)
        # Wait for an expired worker to relinquish its exclusive lock before restarting.
        with (folder/'worker.lock').open('a') as worker_lock:
            for attempt in range(40):
                try:
                    fcntl.flock(worker_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if attempt == 39:
                        raise RuntimeError('Prior wake worker still running; no duplicate started')
                    time.sleep(0.1)
        token = uuid4().hex
        atomic(folder/'control.json', {'token': token, 'action': 'run', 'deadline': time.monotonic()+TTL})
        process = subprocess.Popen([sys.executable, '-m', 'ua_workflows.browser_collection_runtime', '_serve', '--run-dir', str(base), '--token', token],
                                   cwd=Path(__file__).resolve().parents[1], start_new_session=True,
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(60):
            state = status(base)
            if state['active'] and state.get('token') == token:
                return state
            if process.poll() is not None:
                break
            time.sleep(0.1)
        atomic(folder/'control.json', {'token': token, 'action': 'stop'})
        raise RuntimeError('Wake lease did not become active; do not begin browser collection')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['start', 'renew', 'status', 'stop', 'error', '_serve'])
    parser.add_argument('--run-dir', required=True, type=Path)
    parser.add_argument('--token')
    args = parser.parse_args()
    if args.action == '_serve':
        serve(args.run_dir, args.token)
    elif args.action == 'error':
        # Tool errors may only have a message, not a Python traceback. Preserve that distinction.
        source = json.load(sys.stdin)
        allowed = {k: source[k] for k in ('stage', 'app_id', 'page', 'card', 'error_type', 'message', 'traceback', 'evidence_paths') if k in source}
        if not allowed.get('stage') or not allowed.get('message'):
            raise ValueError('Tool error requires stage and actual message')
        secrets = configured_secrets()
        def sanitize(value):
            if isinstance(value, str): return redact(value, secrets)
            if isinstance(value, list): return [sanitize(v) for v in value]
            if isinstance(value, dict): return {k: sanitize(v) for k, v in value.items()}
            return value
        clean = sanitize(allowed)
        path = args.run_dir/'runtime'/('browser-error-' + uuid4().hex + '.json')
        write(path, {'at': stamp(), 'source': 'browser_tool', **clean})
        print(json.dumps({'saved': str(path)}))
    else:
        result = control(args.run_dir, args.action)
        print(json.dumps(result, ensure_ascii=False))
        if args.action in ('start', 'renew', 'status') and not result['active']:
            raise SystemExit(1)


if __name__ == '__main__':
    main()
