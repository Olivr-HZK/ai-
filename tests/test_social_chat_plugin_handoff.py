import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import zipfile

import pytest

from ua_workflows import social_chat_browser_daily as daily


def archive(path, report=True, unsafe=False):
    with zipfile.ZipFile(path, 'w') as z:
        if report:
            z.writestr('下载报告.md', '# 下载报告\n日期下限2026-10-25\n')
        z.writestr('../escape.mp4' if unsafe else '媒体/video/01.mp4', b'not-decoded-by-intake')
    return path


def test_default_prepare_waits_for_package_without_browser_or_daily_fsm(tmp_path, monkeypatch):
    def forbidden():
        raise AssertionError('Browser FSM must not be initialized')
    monkeypatch.setattr(daily, 'fsm', forbidden)
    cfg = daily.load(daily.CONFIG)
    assert cfg['collection_mode'] == 'plugin-handoff-v1' and cfg['execution'] == 'manual'
    result = daily.prepare('2026-10-25', tmp_path, cfg)
    assert result['status'] == 'WAITING_PACKAGE' and result['browser_started'] is False
    scope = daily.load(tmp_path / 'scope.json')
    assert scope['ranking'] == {'mode': 'all'}
    assert scope['content_review_policy']['version'] == 'male-display-female-oriented-no-talking-head-v1'
    from ua_workflows.social_chat_plugin_review_plan import POLICY
    assert scope['processing_policy']==POLICY
    assert not (tmp_path / 'daily-machine').exists() and not (tmp_path / 'tasks').exists()
    assert not (tmp_path / 'browser-task.txt').exists()


def test_multi_zip_handoff_checks_crc_and_keeps_scope_and_duplicate_receipt(tmp_path):
    cfg = daily.load(daily.CONFIG)
    base = tmp_path / 'run'
    daily.prepare('2026-10-25', base, cfg)
    before = (base / 'scope.json').read_bytes()
    first = archive(tmp_path / 'one.zip')
    result = daily.prepare('2026-10-25', base, cfg, source_zips=[first, first])
    assert result['status'] == 'PACKAGE_RECEIVED' and result['packages'] == 1
    doc = daily.load(base / 'handoff.json')
    assert doc['packages'][0]['crc_verified'] is True
    assert doc['packages'][0]['date_metadata_and_download_ledger_checked'] is False
    assert doc['review_status'] == 'not_certified_by_intake'
    assert not doc['collection_complete_21_apps'] and not doc['feishu_written_by_intake']
    receipt = (base / 'handoff.json').read_bytes()
    changed = copy.deepcopy(cfg); changed['ranking']['count'] = 50
    daily.prepare('2026-10-25', base, changed, source_zips=[first])
    assert (base / 'scope.json').read_bytes() == before
    assert (base / 'handoff.json').read_bytes() == receipt
    second = tmp_path / 'two.zip'
    with zipfile.ZipFile(second, 'w') as z:
        z.writestr('下载报告.md', '# 第二包原始报告')
        z.writestr('媒体/image/01.jpg', b'original-image')
    assert daily.prepare('2026-10-25', base, cfg, source_zips=[second])['packages'] == 2
    assert (base / 'scope.json').read_bytes() == before
    with pytest.raises(ValueError, match='different scope'):
        daily.prepare('2026-10-24', base, cfg)
    first.write_bytes(b'changed')
    with pytest.raises(ValueError, match='ZIP changed'):
        daily.prepare('2026-10-25', base, cfg)


@pytest.mark.parametrize('problem', ['no_report', 'unsafe', 'not_zip', 'scheduled'])
def test_bad_or_scheduled_handoff_does_not_create_scope(tmp_path, problem):
    cfg = daily.load(daily.CONFIG); base = tmp_path / 'run'
    path = archive(tmp_path / 'input.zip', report=problem != 'no_report', unsafe=problem == 'unsafe')
    if problem == 'not_zip':
        path.write_bytes(b'not-a-zip')
    with pytest.raises(ValueError):
        daily.prepare('2026-10-25', base, cfg, source_zips=[path],
                      execution_origin='scheduled' if problem == 'scheduled' else 'manual')
    assert not (base / 'scope.json').exists()


def test_old_browser_scope_resumes_under_new_default_without_migration(tmp_path):
    cfg = daily.load(daily.CONFIG); old = copy.deepcopy(cfg)
    old['collection_mode'] = 'direct-browser-v1'
    daily.prepare('2026-10-25', tmp_path, old)
    files = [tmp_path / 'scope.json', tmp_path / 'daily-machine/state.json', tmp_path / 'browser-task.txt']
    before = [p.read_bytes() for p in files]
    assert daily.prepare('2026-10-25', tmp_path, cfg)['status'] == 'resume_existing'
    assert [p.read_bytes() for p in files] == before
    assert not (tmp_path / 'handoff.json').exists()


def test_cli_default_entry_accepts_plugin_zip_without_browser(tmp_path):
    path = archive(tmp_path / 'input.zip'); base = tmp_path / 'run'
    result = subprocess.run([sys.executable, 'scripts/run_social_chat_browser_daily.py', 'prepare',
        '--date', '2026-10-25', '--run-dir', str(base), '--source-zip', str(path)],
        cwd=daily.ROOT, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['status'] == 'PACKAGE_RECEIVED'
    assert not (base / 'daily-machine').exists()


def test_legacy_handoff_does_not_gain_new_processing_order(tmp_path):
    from ua_workflows.social_chat_plugin_handoff import prepare
    cfg=daily.load(daily.CONFIG)
    daily.prepare('2026-10-25',tmp_path,cfg)
    scope=daily.load(tmp_path/'scope.json');scope.pop('processing_policy')
    daily.save(tmp_path/'scope.json',scope)
    receipt=daily.load(tmp_path/'handoff.json')
    receipt['scope_sha256']=hashlib.sha256(json.dumps(scope,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
    daily.save(tmp_path/'handoff.json',receipt)
    before=(tmp_path/'scope.json').read_bytes()
    prepare('2026-10-25',tmp_path,cfg)
    assert (tmp_path/'scope.json').read_bytes()==before
    assert 'processing_policy' not in daily.load(tmp_path/'scope.json')


def test_cli_pre_review_requires_new_policy_and_received_zip(tmp_path):
    cfg=daily.load(daily.CONFIG);daily.prepare('2026-10-25',tmp_path,cfg)
    result=subprocess.run([sys.executable,'scripts/run_social_chat_browser_daily.py','pre-review','--run-dir',str(tmp_path)],
        cwd=daily.ROOT,capture_output=True,text=True,timeout=20)
    assert result.returncode==2 and 'Receive the original plugin ZIP' in result.stderr
    assert not (tmp_path/'pre-review/plan.json').exists()
