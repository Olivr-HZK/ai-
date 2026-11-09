import fcntl
import json
import runpy
from pathlib import Path
from unittest.mock import Mock, call, patch

import pytest

from ua_workflows import persistent_storage as storage
from ua_workflows.shared import config as paths


def setup(tmp_path):
    source = tmp_path / 'legacy/history'; source.mkdir(parents=True)
    (source / 'review.json').write_text('{"confirmed": true}')
    (source / 'media.mp4').write_bytes(b'original bytes')
    cfg = {'state_root': str(tmp_path / 'persistent'),
           'entries': [{'source': str(source), 'target': 'data/history'}]}
    return source, cfg, tmp_path / 'receipt.json'


def test_explicit_paths_preserve_code_root_and_fail_closed(tmp_path, monkeypatch):
    assert paths.persistent_root({}) == paths.PROJECT_ROOT
    for value in ['', 'relative', str(tmp_path / 'missing'), str(tmp_path)]:
        with pytest.raises(ValueError): paths.persistent_root({'UA_STATE_ROOT': value})
    (tmp_path / 'data').mkdir(); (tmp_path / 'reports').mkdir()
    assert paths.persistent_root({'UA_STATE_ROOT': str(tmp_path)}) == tmp_path.resolve()
    monkeypatch.setattr(paths, 'STATE_ROOT', tmp_path)
    assert paths.project_path('data/history') == tmp_path / 'data/history'
    assert paths.project_path('config/example.json') == paths.PROJECT_ROOT / 'config/example.json'
    assert paths.project_path('/explicit/legacy/file') == Path('/explicit/legacy/file')
    with pytest.raises(ValueError): paths.project_path('../escape')


def test_shadow_preserves_source_and_has_independent_bytes(tmp_path):
    source, cfg, receipt = setup(tmp_path)
    result = storage.stage(cfg, receipt)
    assert result['status'] == 'shadow_verified' and not result['activated']
    target = Path(cfg['state_root']) / 'data/history/media.mp4'
    assert target.read_bytes() == (source / 'media.mp4').read_bytes()
    assert target.stat().st_ino != (source / 'media.mp4').stat().st_ino
    assert storage.verify(cfg, receipt)['status'] == 'shadow_verified'
    (source / 'media.mp4').write_bytes(b'next capture')
    assert target.read_bytes() == b'original bytes'
    with pytest.raises(ValueError, match='changed'): storage.verify(cfg, receipt)
    storage.stage(cfg, receipt)
    assert target.read_bytes() == b'next capture'


def test_explicit_layout_cannot_split_legacy_and_new_writes(tmp_path):
    code, state = tmp_path / 'code', tmp_path / 'state'; code.mkdir(); state.mkdir()
    for name in ('data', 'reports', '.venv'): (state / name).mkdir()
    (code / 'data').mkdir(); (code / 'reports').mkdir()
    (state / '.env').write_bytes(b'fixture'); (state / '.env').chmod(0o600)
    with pytest.raises(ValueError, match='link'): paths.validate_persistent_layout(code, state)
    for name in ('.env', '.venv'):
        (code / name).symlink_to(state / name)
    for name in paths.PERSISTENT_DATA_ALIASES:
        (state / 'data' / name).mkdir()
        (code / 'data' / name).symlink_to(state / 'data' / name)
    (code / 'data/versioned-fixture.txt').write_text('tracked reference')
    paths.validate_persistent_layout(code, state)
    (code / 'data/afizzy_daily').unlink(); (code / 'data/afizzy_daily').mkdir()
    with pytest.raises(ValueError, match='afizzy_daily'): paths.validate_persistent_layout(code, state)


def test_added_file_corruption_and_changed_config_block_cutover(tmp_path):
    source, cfg, receipt = setup(tmp_path); storage.stage(cfg, receipt)
    (source / 'new.json').write_text('{}')
    with pytest.raises(ValueError, match='file set'): storage.verify(cfg, receipt)
    storage.stage(cfg, receipt)
    target = Path(cfg['state_root']) / 'data/history/media.mp4'; target.write_bytes(b'corrupt')
    with pytest.raises(ValueError, match='changed'): storage.verify(cfg, receipt)
    changed = {**cfg, 'guard_locks': ['/another-lock']}
    with pytest.raises(ValueError, match='configuration changed'): storage.stage(changed, receipt)


def test_symlinks_and_nonempty_foreign_store_are_rejected(tmp_path):
    source, cfg, receipt = setup(tmp_path)
    (source / 'outside').symlink_to(tmp_path / 'private')
    with pytest.raises(ValueError, match='Special'): storage.stage(cfg, receipt)
    (source / 'outside').unlink()
    root = tmp_path / 'other'; root.mkdir(); (root / 'user-file').touch()
    with pytest.raises(ValueError, match='not owned'):
        storage.stage({**cfg, 'state_root': str(root)}, receipt)


def test_no_activation_during_live_copy_and_lock_prevents_cutover(tmp_path):
    source, cfg, receipt = setup(tmp_path)
    lock = tmp_path / 'worker.lock'; lock.touch(); cfg['guard_locks'] = [str(lock)]
    storage.stage(cfg, receipt); journal = tmp_path / 'cutover.json'
    with pytest.raises(ValueError, match='quiesced'): storage.activate(cfg, receipt, journal)
    with lock.open('r+') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError): storage.activate(cfg, receipt, journal, quiesced=True)
    assert source.is_dir() and not source.is_symlink()


def test_cutover_keeps_legacy_paths_and_original_backup(tmp_path):
    source, cfg, receipt = setup(tmp_path); storage.stage(cfg, receipt)
    journal = tmp_path / 'cutover.json'
    assert storage.activate(cfg, receipt, journal, quiesced=True)['status'] == 'activated'
    assert source.is_symlink() and (source / 'media.mp4').read_bytes() == b'original bytes'
    assert source.with_name('history.before-persistent').is_dir()
    with pytest.raises(ValueError): storage.activate(cfg, receipt, journal, quiesced=True)


def test_link_failure_rolls_back_without_rewriting_history(tmp_path):
    source, cfg, receipt = setup(tmp_path); storage.stage(cfg, receipt)
    journal = tmp_path / 'cutover.json'
    with patch.object(Path, 'symlink_to', side_effect=OSError('simulated link failure')):
        with pytest.raises(OSError): storage.activate(cfg, receipt, journal, quiesced=True)
    assert not source.is_symlink() and (source / 'media.mp4').read_bytes() == b'original bytes'
    assert json.loads(journal.read_text())['status'] == 'rolled_back'


def test_versioned_reference_can_be_seeded_without_replacing_git_directory(tmp_path):
    source, cfg, receipt = setup(tmp_path)
    cfg['entries'][0]['activate'] = False
    storage.stage(cfg, receipt)
    result = storage.activate(cfg, receipt, tmp_path / 'cutover.json', quiesced=True)
    assert result['status'] == 'activated' and result['entries'] == []
    assert source.is_dir() and not source.is_symlink()


def test_live_profile_is_excluded_and_cannot_be_silently_omitted(tmp_path):
    source, cfg, receipt = setup(tmp_path)
    profile = source / 'profile'; profile.mkdir(); (profile / 'session').write_bytes(b'owned test profile')
    cfg['entries'][0]['excluded_top_level'] = ['profile']
    storage.stage(cfg, receipt); target = Path(cfg['state_root']) / 'data/history/profile'
    assert not target.exists() and profile.is_dir()
    with pytest.raises(ValueError, match='Excluded state'):
        storage.activate(cfg, receipt, tmp_path / 'cutover.json', quiesced=True)
    # Operator's final, owner-stopped rename retains bytes and legacy path.
    profile.rename(target); profile.symlink_to(target, target_is_directory=True)
    storage.activate(cfg, receipt, tmp_path / 'cutover.json', quiesced=True)
    assert (source / 'profile/session').read_bytes() == b'owned test profile'


def test_live_source_change_is_recorded_without_activation(tmp_path):
    source, cfg, receipt = setup(tmp_path); original = storage.digest; changed = False
    def changing(path):
        nonlocal changed
        value = original(path)
        if Path(path) == source / 'media.mp4' and not changed:
            changed = True; Path(path).write_bytes(b'capture still running')
        return value
    with patch.object(storage, 'digest', side_effect=changing):
        # Copy integrity also catches mutations occurring before the copy.
        with pytest.raises(ValueError, match='Copied bytes'): storage.stage(cfg, receipt)
    assert source.is_dir() and not source.is_symlink()


def test_supervisor_never_starts_a_paused_worker(tmp_path):
    module = runpy.run_path(str(paths.PROJECT_ROOT / 'scripts/replica_supervisor.py'))
    fn = module['main']; g = fn.__globals__
    data = tmp_path / 'state/data'; dep = data / 'deployment'; dep.mkdir(parents=True)
    (dep / 'replica-main-table.json').write_text(json.dumps({'expected_account': 'expected'}))
    run = data / 'replica-main-table-worker'; run.mkdir(); (run / 'pause-requested').touch()
    writes = []
    g.update(DATA_DIR=data, bridge_status=lambda: pytest.fail('Watcher must not query Replica login'),
             save_private=lambda path, value: writes.append(value))
    with patch.object(g['subprocess'], 'run', side_effect=AssertionError('No worker allowed')), \
            patch.object(g['time'], 'sleep', side_effect=SystemExit):
        with pytest.raises(SystemExit): fn('watcher')
    assert writes[-1]['phase'] == 'paused'


def test_supervisor_uses_code_checkout_and_persistent_checkpoint_separately(tmp_path):
    module = runpy.run_path(str(paths.PROJECT_ROOT / 'scripts/replica_supervisor.py'))
    fn = module['main']; g = fn.__globals__
    data = tmp_path / 'state/data'; dep = data / 'deployment'; dep.mkdir(parents=True)
    (dep / 'replica-main-table.json').write_text(json.dumps({'expected_account': 'expected'}))
    code = tmp_path / 'code'; code.mkdir()
    g.update(DATA_DIR=data, PROJECT_ROOT=code, bridge_status=lambda: pytest.fail('Watcher must not query Replica login'),
             save_private=lambda *a: None)
    with patch.object(g['subprocess'], 'run', side_effect=SystemExit) as execute:
        with pytest.raises(SystemExit): fn('watcher')
    args = execute.call_args
    assert args.kwargs['cwd'] == code
    assert str(data / 'replica-main-table-worker') in args.args[0]
    assert str(dep / 'replica-main-table.json') in args.args[0]


def test_live_bridge_reopens_sso_once_per_logout_and_reports_wrong_account(tmp_path):
    fn = runpy.run_path(str(paths.PROJECT_ROOT / 'scripts/replica_supervisor.py'))['main']
    g = fn.__globals__
    dep = tmp_path / 'deployment'; dep.mkdir()
    (dep / 'replica-main-table.json').write_text(json.dumps({'expected_account': 'expected'}))
    valid = {'authenticated': True, 'account_id': 'expected'}
    expired = {'authenticated': False}
    wrong = {'authenticated': True, 'account_id': 'another'}
    statuses = iter([valid, None, expired, expired, wrong, valid, expired])
    phases = []

    def poll(_):
        if len(phases) == 7:
            raise SystemExit

    socket = tmp_path / 'bridge.sock'; socket.touch()
    rpc = Mock()
    g.update(DATA_DIR=tmp_path, SOCKET=socket, bridge_status=lambda: next(statuses), rpc=rpc,
             start_login=lambda **_: pytest.fail('Live bridge must be reused'),
             save_private=lambda path, value: phases.append(value['phase']))
    with patch.object(g['time'], 'sleep', side_effect=poll), pytest.raises(SystemExit):
        fn('bridge')
    assert phases == ['authenticated', 'bridge_unreachable', 'waiting_for_normal_login',
                      'waiting_for_normal_login', 'account_mismatch', 'authenticated',
                      'waiting_for_normal_login']
    assert rpc.call_args_list == [call({'action': 'begin_login'}, timeout=60)] * 2
