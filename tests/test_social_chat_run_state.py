import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from ua_workflows.social_chat_run_state import (check_daily, heartbeat, mark, notify, render_alert,
                                                stale)

DATE = '2026-11-17'


def at(minutes):
    return datetime.fromisoformat('2026-11-17T10:00:00+08:00') + timedelta(minutes=minutes)


def test_stage_markers_and_stale_detection(tmp_path):
    mark(tmp_path, 'crawl', 'done', now=at(0), target_date=DATE)
    mark(tmp_path, 'pre-review', 'running', now=at(1), target_date=DATE)
    assert stale(tmp_path, 30, now=at(10)) == []
    problems = stale(tmp_path, 30, now=at(45))
    assert [p['stage'] for p in problems] == ['pre-review']
    assert problems[0]['age_minutes'] == 44.0
    mark(tmp_path, 'pre-review', 'failed', note='缺向量缓存', now=at(46), target_date=DATE)
    assert stale(tmp_path, 30, now=at(47))[0]['status'] == 'failed'
    control = json.loads((tmp_path / 'execution-control.json').read_text())
    assert control['stages']['pre-review']['note'] == '缺向量缓存'


def test_check_daily_reports_missing_running_stalled_and_done(tmp_path):
    assert check_daily(tmp_path, target_date=DATE)['status'] == 'missing'
    heartbeat(tmp_path, status='started', stage='crawl', now=at(0), target_date=DATE)
    assert check_daily(tmp_path, max_age_minutes=30, now=at(5), target_date=DATE)['status'] == 'running'
    decision = check_daily(tmp_path, max_age_minutes=30, now=at(90), target_date=DATE)
    assert decision['status'] == 'stalled' and decision['heartbeat_age_minutes'] == 90.0
    heartbeat(tmp_path, status='failed', stage='pre-review', note='blocked', now=at(91), target_date=DATE)
    failed = check_daily(tmp_path, max_age_minutes=30, now=at(92), target_date=DATE)
    assert failed['status'] == 'failed'
    assert '失败' in render_alert(failed) and 'blocked' in render_alert(failed)
    heartbeat(tmp_path, status='done', stage='report', now=at(93), target_date=DATE)
    assert check_daily(tmp_path, max_age_minutes=30, now=at(94), target_date=DATE)['status'] == 'done'


def test_alert_is_disabled_without_configured_webhook():
    assert notify('x', values={}) == {'status': 'not_configured', 'sent': False}
    with pytest.raises(ValueError, match='Invalid configured alert webhook'):
        notify('x', values={'AFIZZY_ALERT_WECOM_WEBHOOK': 'https://example.com/hook?key=1'})


def test_alert_uses_only_configured_wecom_endpoint():
    sent = {}

    class Response:
        def json(self):
            return {'errcode': 0}

    def post(url, json=None, **kwargs):
        sent.update(url=url, body=json)
        return Response()

    result = notify('Afizzy 任务失败', post=post, values={
        'AFIZZY_ALERT_WECOM_WEBHOOK': 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=abc'})
    assert result['status'] == 'accepted' and result['sent'] is True
    assert sent['body']['msgtype'] == 'text' and 'Afizzy' in sent['body']['text']['content']


def test_cli_check_exit_codes_and_markers(tmp_path, capsys):
    from ua_workflows import afizzy_daily
    run = tmp_path / 'run'
    assert afizzy_daily.main(['check', '--date', DATE, '--run-dir', str(run)]) == 2
    assert json.loads(capsys.readouterr().out)['decision']['status'] == 'missing'
    assert afizzy_daily.main(['heartbeat', '--date', DATE, '--run-dir', str(run),
                              '--status', 'started', '--stage', 'crawl']) == 0
    capsys.readouterr()
    assert afizzy_daily.main(['check', '--date', DATE, '--run-dir', str(run), '--max-age', '60']) == 0
    assert json.loads(capsys.readouterr().out)['decision']['status'] == 'running'


def test_legacy_completed_run_without_heartbeat_is_done_not_missing(tmp_path):
    (tmp_path / 'daily-result.json').write_text(json.dumps({
        'target_date': DATE, 'status': 'partial_completed', 'workflow_finished': True}))
    decision = check_daily(tmp_path, target_date=DATE)
    assert decision['status'] == 'done' and decision['source'] == 'daily-result.json'


def test_unfinished_run_without_heartbeat_stays_missing(tmp_path):
    (tmp_path / 'daily-result.json').write_text(json.dumps({'target_date': DATE, 'status': 'running'}))
    assert check_daily(tmp_path, target_date=DATE)['status'] == 'missing'


def test_alert_uses_configured_feishu_chat():
    sent = {}

    class Client:
        def call(self, method, route, **kwargs):
            sent.update(method=method, route=route, body=kwargs.get('json'))
            return {'message_id': 'om_test', 'chat_id': 'oc_test'}

    result = notify('Afizzy 任务失败', client=Client(),
                    values={'AFIZZY_ALERT_FEISHU_CHAT_ID': 'oc_test',
                            'AFIZZY_ALERT_FEISHU_CHAT_NAME': '测试群'})
    assert result['status'] == 'accepted' and result['message_id'] == 'om_test'
    assert sent['route'] == 'im/v1/messages' and sent['body']['receive_id'] == 'oc_test'
    assert sent['body']['msg_type'] == 'text'
    assert 'Afizzy' in json.loads(sent['body']['content'])['text']


def test_alert_rejects_mismatched_feishu_response():
    class Client:
        def call(self, method, route, **kwargs):
            return {'message_id': 'om_test', 'chat_id': 'oc_other'}
    with pytest.raises(ValueError, match='identity mismatch'):
        notify('x', client=Client(), values={'AFIZZY_ALERT_FEISHU_CHAT_ID': 'oc_test'})


def test_watchdog_notifies_only_when_problem(tmp_path, monkeypatch, capsys):
    from ua_workflows import afizzy_daily
    calls = []
    monkeypatch.setattr('ua_workflows.social_chat_run_state.project_values',
                        lambda: {'AFIZZY_ALERT_FEISHU_CHAT_ID': 'oc_test'})
    monkeypatch.setattr('ua_workflows.social_chat_run_state.notify_feishu',
                        lambda text, **kwargs: calls.append(text) or {'status': 'accepted', 'sent': True})
    run = tmp_path / 'run'
    assert afizzy_daily.main(['watchdog', '--date', DATE, '--run-dir', str(run), '--notify']) == 2
    assert len(calls) == 1 and '没有启动痕迹' in calls[0]
    capsys.readouterr()
    run.mkdir(parents=True, exist_ok=True)
    (run / 'daily-result.json').write_text(json.dumps({'target_date': DATE, 'workflow_finished': True}))
    assert afizzy_daily.main(['watchdog', '--date', DATE, '--run-dir', str(run), '--notify']) == 0
    assert len(calls) == 1, '完成状态不应再发告警'
