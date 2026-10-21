from contextlib import contextmanager
import json
import importlib.util
import os
import stat
import subprocess
import sys
from unittest.mock import Mock

import pytest
import requests

from ua_workflows import collection_runtime as r


@contextmanager
def no_sleep_guard(*args, **kwargs):
    yield {}


@pytest.fixture
def isolated(monkeypatch):
    monkeypatch.setattr(r, 'keep_awake', no_sleep_guard)
    monkeypatch.setattr(requests.utils, 'get_environ_proxies', lambda _: {
        'https': 'http://proxy-user:proxy-password@127.0.0.1:7897', 'no': 'private-domain'})


def test_timeout_is_single_attempt_and_redacted_with_chained_trace(tmp_path, monkeypatch, isolated):
    def fail(*a, **kw):
        try:
            raise OSError('socket stalled https://proxy-user:proxy-password@localhost/path?token=signed-secret')
        except OSError as exc:
            raise requests.ReadTimeout('Bearer test-api-key read timeout=120') from exc
    post = Mock(side_effect=fail)
    monkeypatch.setattr(requests, 'post', post)
    path = tmp_path/'http.json'
    with pytest.raises(requests.ReadTimeout):
        r.post_json('https://api.everyinfra.com/api/v1/social', key='test-api-key', body={}, diagnostic_path=path)
    assert post.call_count == 1
    text = path.read_text()
    for secret in ('test-api-key', 'proxy-user', 'proxy-password', 'signed-secret', 'private-domain'):
        assert secret not in text
    data = json.loads(text)
    assert data['exception']['type'] == 'ReadTimeout'
    assert 'OSError' in data['exception']['traceback'] and 'test_collection_runtime.py' in data['exception']['traceback']
    assert data['billing'] == 'unknown' and data['stage'] == 'request_wait'
    assert data['finished_at'] and data['wall_elapsed_seconds'] >= 0
    assert data['monotonic_elapsed_seconds'] >= 0
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_response_ids_and_non_json_error(tmp_path, monkeypatch, isolated):
    response = requests.Response()
    response.status_code = 502
    response._content = b'not json'
    response.headers = {'X-Request-ID': 'req-123', 'Set-Cookie': 'private-cookie'}
    post = Mock(return_value=response)
    monkeypatch.setattr(requests, 'post', post)
    with pytest.raises(requests.exceptions.JSONDecodeError):
        r.post_json('https://example.com', key='secret', body={}, diagnostic_path=tmp_path/'http.json')
    data = json.loads((tmp_path/'http.json').read_text())
    assert data['http_status'] == 502 and data['request_ids'] == {'X-Request-ID': 'req-123'}
    assert data['stage'] == 'json_decode' and post.call_count == 1
    assert 'private-cookie' not in json.dumps(data)


def test_success_preserves_contract_and_timeout(tmp_path, monkeypatch, isolated):
    response = requests.Response(); response.status_code = 200; response._content = b'{"results": []}'
    post = Mock(return_value=response); monkeypatch.setattr(requests, 'post', post)
    actual, payload = r.post_json('https://example.com', key='secret', body={'action':'search'}, diagnostic_path=tmp_path/'http.json')
    assert actual is response and payload == {'results': []}
    assert post.call_args.kwargs['timeout'] == (15, 120)
    assert json.loads((tmp_path/'http.json').read_text())['stage'] == 'complete'


def test_assertion_failure_stops_before_post(tmp_path, monkeypatch, isolated):
    @contextmanager
    def broken(*a):
        raise RuntimeError('assertion failed')
        yield
    monkeypatch.setattr(r, 'keep_awake', broken)
    post = Mock(); monkeypatch.setattr(requests, 'post', post)
    with pytest.raises(RuntimeError):
        r.post_json('https://example.com', key='secret', body={}, diagnostic_path=tmp_path/'http.json')
    post.assert_not_called()
    assert json.loads((tmp_path/'http.json').read_text())['billing'] == 'not_sent'


@pytest.mark.skipif(sys.platform != 'darwin', reason='macOS power assertion')
def test_real_assertion_is_present_and_released_on_exception(tmp_path):
    path = tmp_path/'awake.json'
    with pytest.raises(ValueError, match='test cleanup'):
        with r.keep_awake(path) as state:
            pid = state['caffeinate_pid']
            text = subprocess.check_output(['/usr/bin/pmset','-g','assertions'], text=True)
            assert any(f'pid {pid}(' in line and 'PreventUserIdleSystemSleep' in line for line in text.splitlines())
            assert state['status'] == 'active_verified'
            raise ValueError('test cleanup')
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
    assert json.loads(path.read_text())['status'] == 'released'


def test_guard_start_failure_has_evidence_and_reaps_child(tmp_path, monkeypatch):
    monkeypatch.setattr(r.sys, 'platform', 'darwin')
    proc = Mock(pid=12345); proc.poll.return_value = 1
    monkeypatch.setattr(r.subprocess, 'Popen', Mock(return_value=proc))
    with pytest.raises(RuntimeError, match='not verified'):
        with r.keep_awake(tmp_path/'awake.json'):
            pytest.fail('must not enter')
    proc.wait.assert_called_once()
    assert json.loads((tmp_path/'awake.json').read_text())['status'] == 'failed'


def script(name):
    from pathlib import Path
    path = Path(__file__).resolve().parents[1]/'scripts'/name
    spec = importlib.util.spec_from_file_location(name.replace('.', '_'), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_keyword_entry_preserves_failure_directory(tmp_path, monkeypatch, isolated, capsys):
    from ua_workflows.shared import config
    module = script('test_douyin_keyword_search.py')
    monkeypatch.setattr(module, 'ROOT', tmp_path)
    monkeypatch.setattr(sys, 'argv', ['search', '--keyword', '乙女向'])
    monkeypatch.setattr(config, 'project_env_values', lambda: {'EVERYINFRA_API_KEY':'test-only'})
    post = Mock(side_effect=requests.ReadTimeout('simulated timeout'))
    monkeypatch.setattr(requests, 'post', post)
    assert module.main() == 1
    row = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert row['status'] == 'transport_error_billing_unknown'
    from pathlib import Path
    assert (Path(row['directory'])/'http-diagnostic.json').exists()
    assert post.call_count == 1


def test_batch_keeps_failed_keyword_evidence_without_retry(tmp_path, monkeypatch):
    module = script('test_douyin_all_keywords.py')
    monkeypatch.setattr(module, 'ROOT', tmp_path)
    monkeypatch.setattr(module, 'keep_awake', no_sleep_guard)
    (tmp_path/'config').mkdir()
    (tmp_path/'config/social_chat_douyin_daily.json').write_text('{"keywords":["乙女向"]}')
    monkeypatch.setattr(sys, 'argv', ['batch', '--run-dir', str(tmp_path/'run')])
    row = {'keyword':'乙女向', 'status':'transport_error_billing_unknown', 'directory':str(tmp_path/'raw')}
    run = Mock(return_value=subprocess.CompletedProcess([], 1, stdout=json.dumps(row), stderr=''))
    monkeypatch.setattr(module.subprocess, 'run', run)
    module.main()
    summary = json.loads((tmp_path/'run/summary.json').read_text())
    assert summary['keywords'][0]['directory'] == row['directory']
    assert summary['keywords'][0]['status'] == row['status']
    assert run.call_count == 1
