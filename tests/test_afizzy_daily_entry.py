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
    assert code == 2 and calls == [DATE]
    beat = json.loads((tmp_path / 'run' / 'heartbeat.json').read_text())
    assert beat['status'] == 'started' and beat['stage'] == 'crawl'
