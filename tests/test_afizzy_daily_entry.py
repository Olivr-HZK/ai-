import json

from ua_workflows import afizzy_daily

DATE = '2026-11-17'


def test_daily_entry_records_heartbeat_and_stage_on_success(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(afizzy_daily, 'run_crawl', lambda a, mode='run': 0)
    assert afizzy_daily.main(['daily', '--date', DATE, '--run-dir', str(tmp_path / 'run')]) == 0
    run_dir = tmp_path / 'run'
    beat = json.loads((run_dir / 'heartbeat.json').read_text())
    control = json.loads((run_dir / 'execution-control.json').read_text())
    assert beat['status'] == 'done' and beat['target_date'] == DATE
    assert control['stages']['crawl']['status'] == 'done'


def test_daily_entry_records_failure_when_crawl_exits_nonzero(tmp_path, monkeypatch):
    monkeypatch.setattr(afizzy_daily, 'run_crawl', lambda a, mode='run': 3)
    assert afizzy_daily.main(['daily', '--date', DATE, '--run-dir', str(tmp_path / 'run')]) == 3
    run_dir = tmp_path / 'run'
    beat = json.loads((run_dir / 'heartbeat.json').read_text())
    control = json.loads((run_dir / 'execution-control.json').read_text())
    assert beat['status'] == 'failed' and beat['stage'] == 'crawl'
    assert 'crawl exit 3' in beat['note']
    assert control['stages']['crawl']['status'] == 'failed'


def test_watchdog_reports_missing_run_and_exits_two(tmp_path, capsys):
    assert afizzy_daily.main(['watchdog', '--date', DATE, '--run-dir', str(tmp_path / 'none')]) == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload['decision']['status'] == 'missing'
    assert '没有启动痕迹' in payload['alert']


def test_watchdog_backfill_only_when_nothing_started(tmp_path, monkeypatch):
    calls = []

    def fake_crawl(a, mode='run'):
        calls.append(a.date)
        return 0

    monkeypatch.setattr(afizzy_daily, 'run_crawl', fake_crawl)
    code = afizzy_daily.main(['watchdog', '--date', DATE, '--run-dir', str(tmp_path / 'run'), '--backfill'])
    assert code == 0 and calls == [DATE]
    beat = json.loads((tmp_path / 'run' / 'heartbeat.json').read_text())
    assert beat['status'] == 'started' and beat['stage'] == 'crawl'


def test_crawl_outcome_keeps_complete_batch_with_media_gaps(tmp_path, monkeypatch):
    summary = tmp_path / 'summary.json'
    monkeypatch.setattr(afizzy_daily, 'crawl_summary_path', lambda a: summary)
    summary.write_text(json.dumps({'complete': True, 'attention': [], 'media_failed': 3}))
    assert afizzy_daily.crawl_outcome(None, 2) == ('done_with_gaps', 'media_failed=3')
    summary.write_text(json.dumps({'complete': False, 'attention': ['tool3: 登录失效'], 'media_failed': 0}))
    assert afizzy_daily.crawl_outcome(None, 2)[0] == 'failed'
    summary.write_text(json.dumps({'complete': True, 'attention': [], 'media_failed': 0}))
    assert afizzy_daily.crawl_outcome(None, 0)[0] == 'done'


def test_daily_records_gaps_as_done_with_note(tmp_path, monkeypatch):
    summary = tmp_path / 'summary.json'
    summary.write_text(json.dumps({'complete': True, 'attention': [], 'media_failed': 4}))
    monkeypatch.setattr(afizzy_daily, 'crawl_summary_path', lambda a: summary)
    monkeypatch.setattr(afizzy_daily, 'run_crawl', lambda a, mode='run': 2)
    assert afizzy_daily.main(['daily', '--date', DATE, '--run-dir', str(tmp_path / 'run')]) == 0
    beat = json.loads((tmp_path / 'run' / 'heartbeat.json').read_text())
    control = json.loads((tmp_path / 'run' / 'execution-control.json').read_text())
    assert beat['status'] == 'done' and 'media_failed=4' in beat['note']
    assert control['stages']['crawl']['status'] == 'done'


def test_crawl_passes_persistent_state_dir_and_repo_python(tmp_path, monkeypatch):
    calls = {}

    class Result:
        returncode = 0

    def fake_run(argv, **kwargs):
        calls['argv'] = argv
        return Result()

    monkeypatch.setattr(afizzy_daily.subprocess, 'run', fake_run)
    monkeypatch.setattr(afizzy_daily, 'default_output_dir', lambda: tmp_path / 'raw')
    monkeypatch.setattr(afizzy_daily, 'default_state_dir', lambda: tmp_path / 'collector-state')
    assert afizzy_daily.main(['crawl', '--date', DATE]) == 0
    argv = calls['argv']
    assert '--state-dir' in argv and argv[argv.index('--state-dir') + 1] == str(tmp_path / 'collector-state')
    assert '--output' in argv and argv[argv.index('--output') + 1] == str(tmp_path / 'raw')
    assert 'guangdada-direct-0.2.1' not in argv[0]


def test_heartbeat_refreshes_during_long_collection(tmp_path, monkeypatch):
    import time
    monkeypatch.setenv('AFIZZY_HEARTBEAT_SECONDS', '1')
    monkeypatch.setattr(afizzy_daily, 'crawl_summary_path', lambda a: tmp_path / 'missing.json')
    run = tmp_path / 'run'
    seen = {}

    def slow_crawl(a, mode='run'):
        time.sleep(2.5)
        seen.update(json.loads((run / 'heartbeat.json').read_text()))
        return 0

    monkeypatch.setattr(afizzy_daily, 'run_crawl', slow_crawl)
    assert afizzy_daily.main(['daily', '--date', DATE, '--run-dir', str(run)]) == 0
    assert seen['status'] == 'started' and seen['note'] == 'collection_running', '采集中应持续刷新心跳'
    assert json.loads((run / 'heartbeat.json').read_text())['status'] == 'done'


def test_crawl_failure_is_not_overwritten_by_heartbeat_tick(tmp_path, monkeypatch):
    import time
    import pytest
    monkeypatch.setenv('AFIZZY_HEARTBEAT_SECONDS', '1')

    def failing_crawl(a, mode='run'):
        time.sleep(1.6)
        raise RuntimeError('browser crashed')

    monkeypatch.setattr(afizzy_daily, 'run_crawl', failing_crawl)
    run = tmp_path / 'run'
    with pytest.raises(RuntimeError):
        afizzy_daily.main(['daily', '--date', DATE, '--run-dir', str(run)])
    beat = json.loads((run / 'heartbeat.json').read_text())
    control = json.loads((run / 'execution-control.json').read_text())
    assert beat['status'] == 'failed' and beat['note'] == 'RuntimeError'
    assert control['stages']['crawl']['status'] == 'failed'


def test_corrupt_summary_keeps_crawl_failed_instead_of_crashing(tmp_path, monkeypatch):
    summary = tmp_path / 'summary.json'
    summary.write_text('{not json')
    monkeypatch.setattr(afizzy_daily, 'crawl_summary_path', lambda a: summary)
    assert afizzy_daily.crawl_outcome(None, 2) == ('failed', 'crawl exit 2')


def test_watchdog_backfill_then_reports_running_not_missing(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(afizzy_daily, 'run_crawl', lambda a, mode='run': 0)

    def backfill_writes_heartbeat(a, mode='run'):
        from ua_workflows.social_chat_run_state import heartbeat
        heartbeat(tmp_path / 'run', status='started', stage='crawl', target_date=a.date)
        return 0

    monkeypatch.setattr(afizzy_daily, 'run_crawl', backfill_writes_heartbeat)
    code = afizzy_daily.main(['watchdog', '--date', DATE, '--run-dir', str(tmp_path / 'run'), '--backfill'])
    payload = json.loads(capsys.readouterr().out)
    assert code == 0 and payload['decision']['status'] == 'running'
