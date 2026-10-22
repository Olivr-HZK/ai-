from pathlib import Path
import pytest
from ua_workflows import social_chat_login_policy as login
from ua_workflows import social_chat_daily_state as state
from ua_workflows import social_chat_browser_daily as daily


def setup(base,origin):
    scope={'target_date':'2026-10-20','execution_origin':origin}
    state.write(base/'scope.json',scope);state.write(base/'run.json',{'scope_sha256':state.fingerprint(scope)})
    (base/'screen.png').write_bytes(b'observed screenshot')
    return scope


def proof(base,name):
    state.write(base/name,{'started_at':state.now(),'ended_at':state.now(),'authenticated':True,'evidence_paths':['screen.png']})


def test_scheduled_login_is_automatic_manual_is_not_and_captcha_always_stops(tmp_path):
    for origin,want in [('scheduled','auto_relogin'),('manual','ask_user')]:
        p=tmp_path/origin;p.mkdir();setup(p,origin)
        assert login.decision(p)['action']==want
        assert login.decision(p,captcha=True)['action']=='stop_waiting_human'


def test_finite_attempts_and_evidence_required(tmp_path):
    setup(tmp_path,'scheduled')
    with pytest.raises(ValueError):login.record(tmp_path,'success','missing.json')
    for n in range(2):
        name=f'attempt{n}.json';proof(tmp_path,name);login.record(tmp_path,'failed',name)
    assert login.decision(tmp_path)['action']=='stop_retry_exhausted'
    proof(tmp_path,'third.json')
    with pytest.raises(ValueError):login.record(tmp_path,'failed','third.json')


def test_captcha_latches_until_actual_user_confirmation_and_new_ui_evidence(tmp_path):
    scope=setup(tmp_path,'scheduled');proof(tmp_path,'attempt.json');login.record(tmp_path,'captcha','attempt.json')
    assert login.decision(tmp_path)['action']=='stop_waiting_human'
    state.write(tmp_path/'clear.json',{'scope_sha256':state.fingerprint(scope),'observed_at':state.now(),'captcha_visible':False,'evidence_paths':['screen.png']})
    with pytest.raises(ValueError):login.clear_captcha(tmp_path,'clear.json','')
    login.clear_captcha(tmp_path,'clear.json','验证完成')
    assert login.decision(tmp_path)['action']=='auto_relogin'


def test_manual_prepare_stays_manual_despite_scheduled_default_config(tmp_path):
    c=daily.load(daily.CONFIG)
    daily.prepare('2026-10-20',tmp_path/'manual',c)
    daily.prepare('2026-10-20',tmp_path/'scheduled',c,execution_origin='scheduled')
    assert login.decision(tmp_path/'manual')['action']=='ask_user'
    assert login.decision(tmp_path/'scheduled')['action']=='auto_relogin'
    daily.prepare('2026-10-20',tmp_path/'manual',c,execution_origin='scheduled')
    assert login.decision(tmp_path/'manual')['action']=='ask_user'
