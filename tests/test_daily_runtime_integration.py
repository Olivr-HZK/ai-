from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
import sys
import time
from unittest.mock import Mock

import pytest
import requests

from ua_workflows import collection_runtime as runtime
from ua_workflows import browser_collection_runtime as browser


@contextmanager
def noop(*a, **kw):
    yield {}


def module(name):
    path=Path(__file__).resolve().parents[1]/'scripts'/name
    spec=importlib.util.spec_from_file_location('runtime_test_'+name.replace('.', '_'),path)
    result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result)
    return result


def test_xhs_timeout_never_retries_and_keeps_diagnostics(tmp_path, monkeypatch):
    from ua_workflows.shared import config
    m=module('run_xhs_keyword_search.py')
    monkeypatch.setattr(m,'ROOT',tmp_path)
    monkeypatch.setattr(runtime,'keep_awake',noop)
    monkeypatch.setattr(config,'project_env_values',lambda:{'EVERYINFRA_API_KEY':'fake-api-key'})
    (tmp_path/'config').mkdir()
    (tmp_path/'config/social_chat_xhs_daily.json').write_text(json.dumps({'api':{'endpoint':'https://api.everyinfra.com/api/v1/social'},'keywords':['乙女向']}))
    session=Mock();session.post.side_effect=requests.ReadTimeout('fake-api-key timeout')
    monkeypatch.setattr(requests,'Session',lambda:session)
    monkeypatch.setattr(sys,'argv',['search','--run-dir',str(tmp_path/'run')])
    assert m.main()==1
    assert session.post.call_count==1
    summary=json.loads((tmp_path/'run/summary.json').read_text())
    assert summary['status']=='incomplete'
    diagnostic=json.loads((tmp_path/'run/keywords/01/pages/0001.attempt-1.http.json').read_text())
    assert diagnostic['exception']['type']=='ReadTimeout'
    error=Path(summary['keywords'][0]['diagnostic']).read_text()
    assert 'fake-api-key' not in error and 'traceback' in error


def test_xhs_detail_timeout_intent_still_blocks_replay(tmp_path, monkeypatch):
    from ua_workflows import xhs_daily as xhs
    from ua_workflows.shared import config
    monkeypatch.setattr(config,'project_env_values',lambda:{'EVERYINFRA_API_KEY':'fake-api-key'})
    monkeypatch.setattr(runtime,'keep_awake',noop)
    post=Mock(side_effect=requests.ReadTimeout('test timeout'));monkeypatch.setattr(requests,'post',post)
    note={'id':'n','url':'https://www.xiaohongshu.com/explore/n'}
    cfg={'api':{'endpoint':'https://api.everyinfra.com/api/v1/social'}}
    with pytest.raises(requests.ReadTimeout):xhs.api_video(note,tmp_path,cfg)
    with pytest.raises(ValueError,match='uncertain'):xhs.api_video(note,tmp_path,cfg)
    assert post.call_count==1 and (tmp_path/'video-api-http.json').exists()


def test_gdd_stage_failure_is_logged_without_crawling(tmp_path, monkeypatch):
    from ua_workflows import social_chat_browser_daily as daily
    monkeypatch.setattr(runtime,'keep_awake',noop)
    monkeypatch.setattr(runtime,'configured_secrets',lambda:['fake-api-key'])
    monkeypatch.setattr(sys,'argv',['gdd','prepare','--run-dir',str(tmp_path)])
    monkeypatch.setattr(daily,'prepare',Mock(side_effect=ValueError('fake-api-key bad scope')))
    with pytest.raises(ValueError):daily.main()
    records=list((tmp_path/'runtime').glob('error-*.json'))
    assert len(records)==1
    content=records[0].read_text()
    assert 'fake-api-key' not in content and 'traceback' in content


def test_gdd_prepare_accepts_guard_evidence_but_not_unrelated_files(tmp_path, monkeypatch):
    from ua_workflows import social_chat_browser_daily as daily
    monkeypatch.setattr(sys,'argv',['gdd','prepare','--run-dir',str(tmp_path/'run')])
    assert daily.main()==0
    assert (tmp_path/'run/scope.json').exists()
    assert list((tmp_path/'run/runtime').glob('*.awake.json'))
    bad=tmp_path/'bad';bad.mkdir();(bad/'unrelated.json').write_text('{}')
    with pytest.raises(ValueError,match='empty'):
        daily.prepare('2026-10-20',bad,daily.load(daily.CONFIG))


@pytest.mark.skipif(sys.platform!='darwin', reason='macOS wake assertions')
def test_browser_lease_real_start_renew_duplicate_stop(tmp_path):
    try:
        first=browser.control(tmp_path,'start')
        assert first['active']
        repeated=browser.control(tmp_path,'start')
        renewed=browser.control(tmp_path,'renew')
        assert first['token']==repeated['token']==renewed['token']
        assert first['pid']==renewed['pid'] and renewed['remaining_seconds']>890
    finally:
        stopped=browser.control(tmp_path,'stop')
    assert stopped['status']=='stopped' and not stopped['active']
    awake=list((tmp_path/'runtime').glob('browser-lease-*.awake.json'))
    assert len(awake)==1 and json.loads(awake[0].read_text())['status']=='released'


@pytest.mark.skipif(sys.platform!='darwin', reason='macOS wake assertions')
def test_browser_lease_expiry_releases_and_requires_restart(tmp_path):
    try:
        first=browser.control(tmp_path,'start')
        control=tmp_path/'runtime/browser/control.json'
        data=browser.load(control);data['deadline']=time.monotonic()-1;browser.atomic(control,data)
        for _ in range(40):
            if browser.status(tmp_path).get('status')=='expired':break
            time.sleep(.1)
        assert browser.status(tmp_path)['status']=='expired'
        with pytest.raises(RuntimeError,match='expired'):browser.control(tmp_path,'renew')
        restarted=browser.control(tmp_path,'start')
        assert restarted['active'] and restarted['token']!=first['token']
    finally:
        browser.control(tmp_path,'stop')


def test_browser_tool_error_preserves_real_fields_and_redacts(tmp_path, monkeypatch):
    import io
    monkeypatch.setattr(runtime,'configured_secrets',lambda:['fake-api-key'])
    monkeypatch.setattr(browser,'configured_secrets',lambda:['fake-api-key'])
    monkeypatch.setattr(sys,'argv',['runtime','error','--run-dir',str(tmp_path)])
    monkeypatch.setattr(sys,'stdin',io.StringIO(json.dumps({'stage':'card','page':2,'message':'fake-api-key failed https://host/path?token=signed','evidence_paths':['/tmp/screenshot.png'],'cookies':'not allowed'})))
    browser.main()
    content=next((tmp_path/'runtime').glob('browser-error-*.json')).read_text()
    result=json.loads(content)
    assert 'fake-api-key' not in content and 'signed' not in content and 'cookies' not in content
    assert result['page']==2 and result['evidence_paths']==['/tmp/screenshot.png']
    assert 'traceback' not in result
