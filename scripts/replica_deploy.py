"""Local, paused code release for the existing absolute-root Mac mini services.

Never resumes services, fetches, installs dependencies, or calls business APIs.
"""
from contextlib import contextmanager
import argparse
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid

TERMINAL = {'verified', 'failed', 'source_failed', 'needs_review', 'skipped_existing'}
LABELS = ('com.oliver.replica-watcher', 'com.oliver.replica-bridge')


def run(argv, cwd=None):
    return subprocess.run([str(v) for v in argv], cwd=cwd, check=True, capture_output=True, timeout=180)


def git(root, *args):
    return run(['git', '-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgsign=false', '-c', 'core.fsmonitor=false', '-c', 'protocol.allow=never', '-C', root, *args]).stdout.decode().strip()


def read(path):
    return json.loads(path.read_text())


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def stopped_services(root):
    require(platform.system() == 'Darwin', 'Deployment is restricted to the configured Mac')
    for label in LABELS:
        result = subprocess.run(['/bin/launchctl', 'print', f'gui/{os.getuid()}/{label}'], capture_output=True, text=True, timeout=10)
        require(result.returncode == 113 and 'Could not find service' in result.stderr,
                f'{label} must be booted out first (unknown launchctl results also block)')
    processes = run(['/bin/ps', '-axo', 'pid=,command=']).stdout.decode()
    for line in processes.splitlines():
        if str(root) in line and any(s in line for s in ('supervisor.py', 'replica_main_table', 'replica_auth_bridge')):
            require(int(line.strip().split(None, 1)[0]) == os.getpid(), 'Owned service/bridge process still running')


def paused_runtime(runtime):
    require((runtime / 'pause-requested').is_file(), 'Normal pause-requested marker required')
    scope = read(runtime / 'scope.json')
    require(scope.get('workflow') == 'main_table_replica' and isinstance(scope.get('items'), list), 'Unknown runtime scope')
    ids = [item['record_id'] for item in scope['items']]
    require(len(set(ids)) == len(ids), 'Duplicate runtime identities')
    paths = set()
    for scoped in scope['items']:
        rid = scoped['record_id']
        require(isinstance(rid, str) and re.fullmatch(r'[A-Za-z0-9_-]+', rid), 'Unsafe record identity')
        old_path = runtime / 'items' / (rid + '.json')
        if scoped.get('existing_logo'):
            execution = scoped.get('execution_id')
            require(isinstance(execution, str) and re.fullmatch(r'[A-Za-z0-9_-]+', execution), 'Unsafe execution identity')
            path = runtime / 'translation-followups' / execution / (rid + '.json')
            if old_path.exists():
                old = read(old_path)
                archives = (runtime / 'translation-followups' / 'history').glob(rid + '-*.json')
                require(any(read(p).get('state') == old for p in archives), 'Original checkpoint lacks a matching archive')
                paths.add(old_path)
        else:
            path = old_path
        item = read(path)  # Missing checkpoint blocks; never infer terminal from progress counts.
        require(item.get('record_id') == rid and item.get('stage') in TERMINAL, f'Active/unknown task: {rid}')
        if path == old_path:
            paths.add(path)
    require(set((runtime / 'items').glob('*.json')) == paths, 'Orphan checkpoints require review')
    worker = read(runtime / 'worker.json')
    require(worker.get('finished_at') is not None, 'Watcher has not confirmed a normal exit')
    pid = worker.get('pid')
    require(isinstance(pid, int) and pid > 0, 'Unknown watcher PID')
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        pass
    else:
        raise RuntimeError('Previous watcher PID is still alive or reused; review worker identity')


def snapshot(runtime):
    return {str(p.relative_to(runtime)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in runtime.rglob('*') if p.is_file() and p.name != 'watcher.lock'}


@contextmanager
def locked(path):
    # Never unlink a lock file: the watcher and publisher must use the same inode.
    with path.open('a+') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def health(root, python):
    # Fixed offline suite also checks config/skill/CLI contracts. No business plan/status calls.
    try:
        if (root / 'scripts/ci_offline.py').is_file():
            run([python, '-B', root / 'scripts/ci_offline.py'], cwd=root)
            return 'offline_contracts_and_regressions'
        # Bootstrap rollback: the prior release may predate this CI entrypoint.
        # Check its own sources and installed dependencies without importing business code.
        require((root / 'ua_workflows/replica_main_table.py').is_file(), 'Previous release lacks watcher module')
        script = ('import pathlib,sys,requests,dotenv,PIL,pytest; '
                  'root=pathlib.Path(sys.argv[1]); '
                  '[compile(p.read_bytes(),str(p),"exec") for p in (root/"ua_workflows").rglob("*.py")]')
        run([python, '-B', '-c', script, root], cwd=root)
        return 'legacy_source_syntax_and_dependencies_only'
    except subprocess.CalledProcessError as error:
        detail = ((error.stdout or b'') + (error.stderr or b'')).decode(errors='replace')[-20000:]
        raise RuntimeError('Offline health failed (bounded offline test output):\n' + detail) from error


def publish(config, commit, check_only=False):
    root = Path(config['repo']).resolve()
    runtime = Path(config['runtime']).resolve()
    receipts = Path(config['receipts']).resolve()
    python = Path(config['python']).absolute()
    require(runtime == (root / 'data/replica-main-table-worker').resolve(), 'Runtime must match the installed supervisor location')
    require(python == root / '.venv/bin/python', 'Interpreter must belong to the deployment checkout')
    require(not receipts.is_relative_to(runtime), 'Release receipts must be outside the runtime')
    require(socket.gethostname() == config['expected_host'], 'Host does not match deployment configuration')
    require(re.fullmatch(r'[0-9a-f]{40}', commit), 'Use a full, already fetched 40-character commit SHA')
    require(root.is_dir() and runtime.is_dir() and python.is_file(), 'Missing checkout/runtime/interpreter')
    require(Path(git(root, 'rev-parse', '--show-toplevel')).resolve() == root, 'Repo must be the deployment checkout root')
    require(not git(root, 'status', '--porcelain', '--untracked-files=no'), 'Tracked deployment files are dirty')
    require(not git(root, 'branch', '--show-current'), 'Deployment checkout must be detached')
    require(git(root, 'rev-parse', '--verify', commit + '^{commit}') == commit, 'Exact commit is not available locally')
    stopped_services(root)
    with locked(runtime / 'watcher.lock'):
        paused_runtime(runtime)
        before = snapshot(runtime)
        old = git(root, 'rev-parse', 'HEAD')
        # Export tracked sources only: no media, credentials, .env or untracked deployment state.
        with tempfile.TemporaryDirectory(prefix='replica-release-') as directory:
            candidate = Path(directory)
            archive = run(['git', '-c', 'core.hooksPath=/dev/null', '-C', root, 'archive', commit]).stdout
            with tarfile.open(fileobj=io.BytesIO(archive)) as stream:
                for member in stream.getmembers():
                    require(not member.issym() and not member.islnk() and not member.name.startswith('/')
                            and '..' not in Path(member.name).parts, 'Unsafe release archive entry')
                stream.extractall(candidate, filter='data')
            require((candidate / 'scripts/ci_offline.py').is_file(), 'Candidate must contain the offline CI gate')
            health(candidate, python)
        require(snapshot(runtime) == before, 'Runtime changed during candidate validation')
        stopped_services(root)
        if check_only:
            return {'status': 'checked_not_deployed', 'commit': commit, 'previous': old}
        receipts.mkdir(parents=True, exist_ok=True)
        release_id = time.strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex[:8]
        backup = 'refs/replica-releases/' + release_id
        git(root, 'update-ref', backup, old)
        receipt = {'release_id': release_id, 'commit': commit, 'previous': old, 'backup_ref': backup,
                   'status': 'intent', 'services_resumed': False, 'runtime': str(runtime), 'health_scope': 'offline_only_no_live_business_verification'}
        receipt_path = receipts / (release_id + '.json')
        def save():
            tmp = receipt_path.with_suffix('.tmp')
            with tmp.open('w') as output:
                json.dump(receipt, output, indent=2); output.flush(); os.fsync(output.fileno())
            tmp.replace(receipt_path)
        save()
        try:
            git(root, 'checkout', '--detach', commit)
            require(git(root, 'rev-parse', 'HEAD') == commit, 'Checkout version mismatch')
            receipt['health'] = health(root, python)
            require(snapshot(runtime) == before, 'Runtime changed after code switch')
            stopped_services(root)
            receipt['status'] = 'code_deployed_not_resumed'
        except Exception as error:
            receipt['error_type'] = type(error).__name__
            receipt['error_detail'] = str(error)[-22000:]
            try:
                git(root, 'checkout', '--detach', old)
                require(git(root, 'rev-parse', 'HEAD') == old, 'Rollback version mismatch')
                receipt['rollback_health'] = health(root, python)
                require(snapshot(runtime) == before, 'Runtime changed during rollback')
                stopped_services(root)
                receipt['status'] = 'rolled_back_not_resumed'
            except Exception as rollback_error:
                receipt.update(status='rollback_failed_keep_paused', rollback_error_type=type(rollback_error).__name__)
            save()
            raise RuntimeError(f"Release failed: {receipt['status']}; receipt {receipt_path}") from error
        save()
        return {**receipt, 'receipt': str(receipt_path)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True, type=Path)
    parser.add_argument('--commit', required=True)
    parser.add_argument('--check-only', action='store_true')
    args = parser.parse_args()
    try:
        print(json.dumps(publish(read(args.config), args.commit, args.check_only), indent=2))
    except Exception as error:
        print(f'Deployment blocked: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
