import copy
from datetime import datetime,timezone,timedelta
from pathlib import Path
import pytest
from ua_workflows import social_chat_daily_state as fsm
from ua_workflows import social_chat_mixed_window as window


def setup(tmp_path):
    scope={'target_date':'2026-10-20','tasks':[{'id':'ios-01','app_name':'Example','app_id':'123','platform':'iOS'},{'id':'android-01','app_name':'Example','app_id':'app.test','platform':'Android'}], 'filters':{'category':'工具','dedupe':'素材'}}
    fsm.write(tmp_path/'scope.json',scope);fsm.write(tmp_path/'run.json',{'scope_sha256':fsm.fingerprint(scope)})
    fsm.init(tmp_path);return scope


def move(tmp_path,state):
    d=fsm.load(tmp_path);steps=fsm.plan(fsm.read(tmp_path/'scope.json'));d['cursor']=next(i for i,s in enumerate(steps) if s['state']==state);fsm.write(tmp_path/'daily-machine/state.json',d)


def test_prepare_is_recorded_and_scope_cannot_drift(tmp_path):
    scope=setup(tmp_path);s=fsm.status(tmp_path)
    assert s['state']=='START_NOTICE_PENDING' and s['completed_steps']==1
    a=fsm.load(tmp_path)['attempts'][0];assert a['state']=='PREPARE' and a['elapsed_seconds']>=0
    scope['target_date']='2026-10-21';fsm.write(tmp_path/'scope.json',scope)
    with pytest.raises(ValueError,match='Scope changed'):fsm.status(tmp_path)


def test_order_and_idempotent_init_no_automatic_stage_jump(tmp_path):
    setup(tmp_path);fsm.init(tmp_path)
    with pytest.raises(ValueError,match='Expected SYNC'):
        with fsm.tracked(tmp_path,'SYNC_PENDING'):pass
    assert len(fsm.load(tmp_path)['attempts'])==1
    fsm.begin(tmp_path)
    with pytest.raises(ValueError,match='not ready'):fsm.begin(tmp_path)
    with pytest.raises(ValueError):fsm.finish(tmp_path)
    assert fsm.status(tmp_path)['state']=='RETRY_REQUIRED'
    assert fsm.status(tmp_path)['resume_state']=='START_NOTICE_PENDING'


def test_retry_budget_and_pause_preserve_stage_and_exclude_wait(tmp_path):
    setup(tmp_path)
    for n in range(3):
        fsm.begin(tmp_path)
        with pytest.raises(ValueError):fsm.finish(tmp_path,'failed',error='real timeout')
        fsm.resume(tmp_path,'connection restored')
    with pytest.raises(ValueError,match='budget exhausted'):fsm.begin(tmp_path)
    fsm.hold(tmp_path,'PAUSED','user pause');fsm.resume(tmp_path,'user continue',extra_attempts=1)
    fsm.begin(tmp_path);fsm.hold(tmp_path,'WAITING_HUMAN','captcha')
    s=fsm.status(tmp_path);assert s['active_attempt'] is None and s['resume_state']=='START_NOTICE_PENDING'
    assert all(a['elapsed_seconds']>=0 for a in fsm.load(tmp_path)['attempts'])
    assert 'waiting_seconds' in fsm.load(tmp_path)['resumes'][0]


def proof(tmp_path,scope,action='readonly',before=None):
    expected={'category':'工具'};p=tmp_path/'capture.png';p.write_bytes(b'evidence')
    data={'task_id':'ios-01','scope_sha256':fsm.fingerprint(scope),'expected':expected,'before':expected if before is None else before,'after':expected,'action':action,'observed_at':fsm.now(),'images':{k:{'path':'capture.png','sha256':window.sha(p)} for k in ('before','after')}}
    fsm.write(tmp_path/'category.json',data);return data


def test_correct_filter_refuses_mutation_allows_readonly_and_hash_check(tmp_path):
    scope=setup(tmp_path);move(tmp_path,'CATEGORY_PENDING');fsm.begin(tmp_path)
    proof(tmp_path,scope,'repair')
    with pytest.raises(ValueError,match='readonly'):fsm.finish(tmp_path,proof='category.json')
    fsm.resume(tmp_path,'use readonly');fsm.begin(tmp_path);proof(tmp_path,scope)
    fsm.finish(tmp_path,proof='category.json');assert fsm.status(tmp_path)['state']=='FILTERS_PENDING'
    move(tmp_path,'CATEGORY_PENDING');fsm.begin(tmp_path);proof(tmp_path,scope);(tmp_path/'capture.png').write_bytes(b'changed')
    with pytest.raises(ValueError,match='hash mismatch'):fsm.finish(tmp_path,proof='category.json')


def test_filter_difference_allows_repair_but_stale_evidence_rejected(tmp_path):
    scope=setup(tmp_path);move(tmp_path,'CATEGORY_PENDING');p=proof(tmp_path,scope,'repair',{'category':'游戏'})
    p['observed_at']='2000-01-01T00:00:00+00:00';fsm.write(tmp_path/'category.json',p);fsm.begin(tmp_path)
    with pytest.raises(ValueError,match='fresh'):fsm.finish(tmp_path,proof='category.json')
    fsm.resume(tmp_path,'fresh observation');fsm.begin(tmp_path);proof(tmp_path,scope,'repair',{'category':'游戏'});fsm.finish(tmp_path,proof='category.json')
    assert fsm.status(tmp_path)['state']=='FILTERS_PENDING'


def test_graph_requires_app_seal_before_next_app_and_records_attempts(tmp_path,monkeypatch):
    scope=setup(tmp_path);monkeypatch.setattr(fsm,'validate',lambda *args,**kwargs:[tmp_path/'scope.json'])
    # Only this structural test bypasses gates; real filter gate tests above do not.
    monkeypatch.setattr(fsm,'APP_STATES',('COLLECTING','COLLECTION_SUMMARY_PENDING'))
    steps=fsm.plan(scope);order=[]
    while fsm.status(tmp_path)['state']!='COMPLETED':
        s=fsm.status(tmp_path);order.append((s['state'],s['task']));fsm.begin(tmp_path);fsm.finish(tmp_path)
    assert order.index(('COLLECTION_SUMMARY_PENDING','ios-01'))<order.index(('COLLECTING','android-01'))
    assert len(fsm.load(tmp_path)['attempts'])==len(steps)


def test_operation_log_batch_timing_retry_and_redundant_filter_refusal(tmp_path):
    setup(tmp_path);fsm.begin(tmp_path);(tmp_path/'proof.txt').write_text('real observation')
    t=fsm.now();e={'id':'a','name':'regions','action':'readonly','before':[],'after':[],'expected':[],'started_at':t,'ended_at':t,'evidence_paths':['proof.txt']}
    fsm.operations(tmp_path,[e]);e.update(id='b',error='timeout');fsm.operations(tmp_path,[e])
    rows=fsm.read(tmp_path/'daily-machine/operations.json');assert rows[1]['retry'] and rows[1]['operation_attempt']==2
    with pytest.raises(ValueError,match='Redundant'):fsm.operations(tmp_path,[{**e,'id':'c','action':'repair'}])
    assert len(fsm.read(tmp_path/'daily-machine/operations.json'))==2
    with pytest.raises(ValueError,match='duplicate'):fsm.operations(tmp_path,[e])


def test_tracked_command_records_failure_and_never_marks_passed(tmp_path):
    setup(tmp_path)
    with pytest.raises(RuntimeError,match='network'):
        with fsm.tracked(tmp_path,'START_NOTICE_PENDING'):raise RuntimeError('network')
    a=fsm.load(tmp_path)['attempts'][-1];assert a['outcome']=='failed' and a['error']=='network'
    assert fsm.status(tmp_path)['state']=='RETRY_REQUIRED'


def test_legacy_run_is_not_silently_migrated(tmp_path):
    with fsm.tracked(tmp_path,'SYNC_PENDING'):pass
    assert not (tmp_path/'daily-machine').exists()


def test_relative_run_dir_can_complete_without_path_mismatch(tmp_path,monkeypatch):
    root=tmp_path/'run';root.mkdir();scope=setup(root);move(root,'CATEGORY_PENDING');monkeypatch.chdir(tmp_path)
    fsm.begin(Path('run'));proof(root,scope);fsm.finish(Path('run'),proof='category.json')
    assert fsm.status(root)['state']=='FILTERS_PENDING'


def test_resume_app_stage_requires_fresh_identity_and_correct_filter_snapshot(tmp_path):
    scope=setup(tmp_path);move(tmp_path,'COLLECTING');fsm.begin(tmp_path);fsm.hold(tmp_path,'WAITING_HUMAN','captcha')
    with pytest.raises((TypeError,ValueError)):fsm.resume(tmp_path,'user completed')
    expected={'category':'工具','dedupe':'素材','date':{'start':'2026-10-20','end':'2026-10-20'},'media':'图片&视频','selected_types':[],'channels':[],'regions':[],'languages':[],'size':''}
    data=proof(tmp_path,scope);data.update(expected=expected,before=expected,after=expected,authenticated=True,app_id='wrong',platform='iOS',selected_app_ids=['123'],residual_keywords=[]);fsm.write(tmp_path/'resume.json',data)
    with pytest.raises(ValueError,match='identity'):fsm.resume(tmp_path,'user completed',proof='resume.json')
    data['app_id']='123';fsm.write(tmp_path/'resume.json',data);fsm.resume(tmp_path,'user completed',proof='resume.json')
    assert fsm.status(tmp_path)['state']=='COLLECTING'


def test_extra_retry_budget_is_scoped_to_current_stage(tmp_path):
    setup(tmp_path);fsm.begin(tmp_path);fsm.hold(tmp_path,'RETRY_REQUIRED','timeout');fsm.resume(tmp_path,'fixed',extra_attempts=2)
    doc=fsm.load(tmp_path);assert doc['max_attempts']==3
    assert doc['budgets']=={'START_NOTICE_PENDING/None':5}


@pytest.mark.parametrize('mode',['candidate','enter','query_only','wrong_platform'])
def test_daily_app_gate_requires_real_candidate_click_and_selected_identity(tmp_path,mode):
    from PIL import Image
    scope=setup(tmp_path);move(tmp_path,'APP_PENDING');fsm.begin(tmp_path)
    root=tmp_path/'tasks/ios-01';root.mkdir(parents=True)
    for name in ('before','after'):Image.new('RGB',(2,2)).save(root/f'{name}.png')
    chosen={'app_name':'Example','app_id':'123','platform':'iOS','selected_app_ids':['123'],'residual_keywords':[],'search_mode':'广告主'}
    images={n:{'path':f'{n}.png','sha256':window.sha(root/f'{n}.png')} for n in ('before','after')}
    def event(key,after,interaction=None):
        path='evidence/clicks/'+key+'.json'
        fsm.write(root/path,{'producer':'gdd-checked-click/v1','scope_sha256':fsm.fingerprint(scope),'task_id':'ios-01','event_id':key,'key':key,'kind':'click','performed_at':fsm.now(),'status':'passed','error':None,'expected':after,'after':after,'images':images,'interaction':interaction})
        return path
    query=event('app.query','123')
    candidate=event('app.candidate',chosen,{'action':'press','target':'Enter'} if mode=='enter' else {'action':'click','target':'advertiser_candidate'})
    proof={**chosen,'scope_sha256':fsm.fingerprint(scope),'observed_at':fsm.now(),'query_type':'app_id','query_app_id':'123','query_ui_check':query,'ui_check':query if mode=='query_only' else candidate}
    if mode=='wrong_platform':proof['platform']='Android'
    fsm.write(root/'selection-proof.json',proof)
    if mode=='candidate':
        fsm.finish(tmp_path,proof='tasks/ios-01/selection-proof.json')
        assert fsm.status(tmp_path)['state']=='SORT_PENDING'
        assert fsm.read(root/'app-selection.json')['app_id']=='123'
    else:
        with pytest.raises(ValueError):fsm.finish(tmp_path,proof='tasks/ios-01/selection-proof.json')
        assert fsm.status(tmp_path)['resume_state']=='APP_PENDING'

@pytest.mark.parametrize('mode',['valid','not_enter','extra_keyword','wrong_platform'])
def test_comprehensive_id_enter_requires_actual_submission(tmp_path,mode):
    from PIL import Image
    scope=setup(tmp_path);move(tmp_path,'APP_PENDING');fsm.begin(tmp_path)
    root=tmp_path/'tasks/ios-01';root.mkdir(parents=True)
    for name in ('before','after'):Image.new('RGB',(2,2)).save(root/f'{name}.png')
    submission={'search_mode':'综合','query_terms':['123'],'selected_app_ids':[],'residual_keywords':[]}
    if mode=='extra_keyword':submission['query_terms'].append('old')
    path='evidence/clicks/submitted.json'
    fsm.write(root/path,{'producer':'gdd-checked-click/v1','scope_sha256':fsm.fingerprint(scope),'task_id':'ios-01','event_id':'submitted','key':'app.query','kind':'click','performed_at':fsm.now(),'status':'passed','expected':'123','after':'123','error':None,'images':{n:{'path':f'{n}.png','sha256':window.sha(root/f'{n}.png')} for n in ('before','after')},'interaction':{'action':'press','target':'Enter' if mode!='not_enter' else 'Tab'},'submission':submission})
    proof={**submission,'app_name':'Example','app_id':'123','platform':'Android' if mode=='wrong_platform' else 'iOS','scope_sha256':fsm.fingerprint(scope),'observed_at':fsm.now(),'query_type':'app_id','query_app_id':'123','query_ui_check':path,'ui_check':path}
    fsm.write(root/'proof.json',proof)
    if mode=='valid':
        fsm.finish(tmp_path,proof='tasks/ios-01/proof.json')
        assert fsm.status(tmp_path)['state']=='SORT_PENDING'
    else:
        with pytest.raises(ValueError):fsm.finish(tmp_path,proof='tasks/ios-01/proof.json')
