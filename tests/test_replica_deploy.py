"""Temporary Git repositories exercise real checkout, preflight and rollback."""
import fcntl
import json
import os
from pathlib import Path
import socket
from unittest.mock import patch

import pytest
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import replica_deploy as deploy


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


@pytest.fixture
def release(tmp_path):
    root = tmp_path / 'repo'; root.mkdir()
    deploy.git(root, 'init')
    deploy.git(root, 'config', 'user.name', 'Offline fixture')
    deploy.git(root, 'config', 'user.email', 'fixture@example.invalid')
    (root / 'scripts').mkdir(); (root / 'scripts/ci_offline.py').write_text('# fixture health gate')
    deploy.git(root, 'add', 'scripts/ci_offline.py')
    code = root / 'code.txt'; code.write_text('old')
    deploy.git(root, 'add', 'code.txt'); deploy.git(root, 'commit', '-m', 'old')
    old = deploy.git(root, 'rev-parse', 'HEAD')
    code.write_text('new'); deploy.git(root, 'add', 'code.txt'); deploy.git(root, 'commit', '-m', 'new')
    new = deploy.git(root, 'rev-parse', 'HEAD')
    deploy.git(root, 'checkout', '--detach', old)
    runtime = root / 'data/replica-main-table-worker'; runtime.mkdir(parents=True)
    interpreter = root / '.venv/bin/python'; interpreter.parent.mkdir(parents=True)
    interpreter.symlink_to(__import__('sys').executable)
    (runtime / 'pause-requested').write_text('{}')
    write(runtime / 'scope.json', {'workflow': 'main_table_replica', 'items': [{'record_id': 'recOne'}]})
    write(runtime / 'items/recOne.json', {'record_id': 'recOne', 'stage': 'verified'})
    write(runtime / 'worker.json', {'pid': 99999999, 'finished_at': 1})
    config = {'repo': str(root), 'runtime': str(runtime), 'receipts': str(tmp_path / 'receipts'),
              'python': str(interpreter), 'expected_host': socket.gethostname()}
    with patch.object(deploy, 'stopped_services'):
        yield config, root, runtime, old, new


def test_checked_candidate_switches_actual_checkout_without_runtime_changes(release):
    cfg, root, runtime, old, new = release
    before = deploy.snapshot(runtime)
    calls = []
    def health(candidate, python):
        calls.append(candidate)
        assert (candidate / 'code.txt').read_text() == 'new'
    with patch.object(deploy, 'health', side_effect=health):
        result = deploy.publish(cfg, new)
    assert result['status'] == 'code_deployed_not_resumed'
    assert not result['services_resumed']
    assert len(calls) == 2 and calls[-1] == root
    assert deploy.git(root, 'rev-parse', 'HEAD') == new
    assert deploy.git(root, 'rev-parse', result['backup_ref']) == old
    assert before == deploy.snapshot(runtime)
    assert json.loads(Path(result['receipt']).read_text())['status'] == result['status']


def test_post_switch_failure_rolls_back_and_checks_old_version(release):
    cfg, root, runtime, old, new = release
    calls = []
    def health(candidate, python):
        calls.append((candidate, (candidate / 'code.txt').read_text()))
        if candidate == root and calls[-1][1] == 'new':
            raise RuntimeError('New version health failure')
    with patch.object(deploy, 'health', side_effect=health), pytest.raises(RuntimeError, match='rolled_back_not_resumed'):
        deploy.publish(cfg, new)
    assert calls[-1] == (root, 'old')
    assert deploy.git(root, 'rev-parse', 'HEAD') == old
    receipt = json.loads(next(Path(cfg['receipts']).glob('*.json')).read_text())
    assert receipt['status'] == 'rolled_back_not_resumed'
    assert (runtime / 'pause-requested').exists()


def test_candidate_failure_never_switches(release):
    cfg, root, runtime, old, new = release
    with patch.object(deploy, 'health', side_effect=RuntimeError('Bad candidate')), pytest.raises(RuntimeError, match='Bad candidate'):
        deploy.publish(cfg, new)
    assert deploy.git(root, 'rev-parse', 'HEAD') == old
    assert not Path(cfg['receipts']).exists()


@pytest.mark.parametrize('stage', ['logo_accepted', 'write_intent', 'upload_unknown', 'queued'])
def test_active_and_unknown_tasks_block_release(release, stage):
    cfg, root, runtime, old, new = release
    write(runtime / 'items/recOne.json', {'record_id': 'recOne', 'stage': stage})
    with pytest.raises(RuntimeError, match='Active/unknown'):
        deploy.publish(cfg, new)
    assert deploy.git(root, 'rev-parse', 'HEAD') == old


def test_dirty_checkout_and_missing_pause_block(release):
    cfg, root, runtime, old, new = release
    (root / 'code.txt').write_text('local user edit')
    with pytest.raises(RuntimeError, match='dirty'):
        deploy.publish(cfg, new)
    (root / 'code.txt').write_text('old')
    (runtime / 'pause-requested').unlink()
    with pytest.raises(RuntimeError, match='pause-requested'):
        deploy.publish(cfg, new)


def test_check_only_keeps_version_and_does_not_create_receipt(release):
    cfg, root, runtime, old, new = release
    with patch.object(deploy, 'health'):
        assert deploy.publish(cfg, new, check_only=True)['status'] == 'checked_not_deployed'
    assert deploy.git(root, 'rev-parse', 'HEAD') == old
    assert not Path(cfg['receipts']).exists()


def test_held_watcher_lock_blocks_release(release):
    cfg, root, runtime, old, new = release
    with (runtime / 'watcher.lock').open('a+') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            deploy.publish(cfg, new)


def test_unknown_launchctl_output_blocks():
    with patch.object(deploy.platform, 'system', return_value='Darwin'), patch.object(deploy.subprocess, 'run',
        return_value=__import__('subprocess').CompletedProcess([], 1, '', 'Unknown error')):
        with pytest.raises(RuntimeError, match='booted out'):
            deploy.stopped_services(Path('/not-a-real-deployment'))


def test_wrong_runtime_cannot_validate_an_idle_directory(release):
    cfg, root, runtime, old, new = release
    cfg['runtime'] = str(root / 'some-idle-runtime')
    with pytest.raises(RuntimeError, match='installed supervisor'):
        deploy.publish(cfg, new)


def test_wrong_host_blocks_before_mutation(release):
    cfg, root, runtime, old, new = release
    cfg['expected_host'] = 'not-this-host'
    with pytest.raises(RuntimeError, match='Host does not match'):
        deploy.publish(cfg, new)
    assert deploy.git(root, 'rev-parse', 'HEAD') == old


def test_bootstrap_rollback_health_does_not_require_new_ci_script(tmp_path):
    source = tmp_path / 'ua_workflows/replica_main_table.py'
    source.parent.mkdir(); source.write_text('print("never imported")')
    with patch.object(deploy, 'run') as execute:
        assert deploy.health(tmp_path, Path('/fixture/python')) == 'legacy_source_syntax_and_dependencies_only'
    assert execute.call_args.args[0][2] == '-c'
    assert 'compile(' in execute.call_args.args[0][3]
