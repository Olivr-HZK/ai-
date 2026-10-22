from contextlib import contextmanager
import json
from unittest.mock import Mock

import pytest
import requests

from ua_workflows import collection_runtime as runtime, everyinfra_jobs as jobs


BODY={'platform':'douyin','action':'search','params':{'keyword':'乙女向','publish_time':'one_day','sort':'newest','limit':50}}


def test_battery_blocks_before_any_submission_intent(tmp_path,monkeypatch):
    monkeypatch.setattr(runtime,'power_snapshot',lambda:{'platform':'darwin','ac_power':False})
    post=Mock();monkeypatch.setattr(requests,'post',post)
    with pytest.raises(runtime.PowerPreflightError):jobs.execute(BODY,tmp_path,key='fake-key')
    post.assert_not_called();assert not (tmp_path/'job-intent.json').exists()
    assert json.loads((tmp_path/'power.json').read_text())['ac_power'] is False


def test_ac_change_during_preflight_blocks_and_releases(tmp_path,monkeypatch):
    readings=iter([{'platform':'darwin','ac_power':True},{'platform':'darwin','ac_power':False}]);released=[]
    monkeypatch.setattr(runtime,'power_snapshot',lambda:next(readings))
    @contextmanager
    def guard(*a,**kw):
        assert kw['require_system_sleep'] is True
        try:yield
        finally:released.append(True)
    monkeypatch.setattr(runtime,'keep_awake',guard)
    post=Mock();monkeypatch.setattr(requests,'post',post)
    with pytest.raises(runtime.PowerPreflightError):jobs.execute(BODY,tmp_path,key='fake-key')
    assert released==[True] and not (tmp_path/'job-intent.json').exists();post.assert_not_called()


def test_idle_assertion_alone_is_insufficient_for_new_paid_request(tmp_path,monkeypatch):
    monkeypatch.setattr(runtime.sys,'platform','darwin')
    process=Mock(pid=12345);process.poll.return_value=None
    monkeypatch.setattr(runtime.subprocess,'Popen',Mock(return_value=process))
    monkeypatch.setattr(runtime.subprocess,'run',lambda *a,**kw:Mock(stdout='pid 12345(caffeinate): PreventUserIdleSystemSleep'))
    monkeypatch.setattr(runtime.time,'sleep',lambda _:None)
    with pytest.raises(RuntimeError,match='not verified'):
        with runtime.keep_awake(tmp_path/'awake.json',require_system_sleep=True):pytest.fail('must not enter')
    process.terminate.assert_called_once();process.wait.assert_called_once()


def test_saved_job_can_be_polled_without_a_new_power_submission(tmp_path,monkeypatch):
    endpoint='https://api.everyinfra.com/api/v1/social'
    runtime.write(tmp_path/'job-intent.json',{'fingerprint':jobs.fingerprint({'endpoint':endpoint,'body':{**BODY,'mode':'async'}})})
    runtime.write(tmp_path/'submit-response.json',{'job_id':'job-saved','status':'running'})
    monkeypatch.setattr(runtime,'paid_request_power',Mock(side_effect=AssertionError('must not submit')))
    post=Mock();monkeypatch.setattr(requests,'post',post)
    response=Mock(ok=True,status_code=200)
    response.json.return_value={'job_id':'job-saved','status':'succeeded','platform':'douyin','action':'search','results':[]}
    monkeypatch.setattr(requests,'get',Mock(return_value=response))
    assert jobs.execute(BODY,tmp_path,key='fake-key')['status']=='succeeded'
    post.assert_not_called()


def test_review_pending_is_not_classified_as_network_timeout(tmp_path,monkeypatch):
    monkeypatch.setattr(runtime,'configured_secrets',lambda:[])
    review=runtime.record_failure(tmp_path,'review',ValueError('IMAGE_REVIEW_PENDING: n'))
    network=runtime.record_failure(tmp_path,'media',requests.ReadTimeout('timeout'))
    assert json.loads(open(review).read())['category']=='review_pending'
    assert json.loads(open(network).read())['category']=='transport_timeout'
