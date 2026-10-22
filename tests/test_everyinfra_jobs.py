from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests

from ua_workflows import everyinfra_jobs as jobs, collection_runtime as runtime, social_search as search


@contextmanager
def noop(*a,**kw):yield {}


@pytest.fixture(autouse=True)
def offline_power(monkeypatch):
    monkeypatch.setattr(runtime,'keep_awake',noop)
    monkeypatch.setattr(runtime,'paid_request_power',noop)
    monkeypatch.setattr(runtime,'configured_secrets',lambda:['test-key'])


def response(body,status=200):
    r=requests.Response();r.status_code=status;r._content=json.dumps(body).encode();r.headers={};return r


BODY={'platform':'douyin','action':'search','params':{'keyword':'乙女向','publish_time':'one_day','sort':'newest','limit':50}}


class Session:
    def __init__(self,statuses=('succeeded',)):self.posts=0;self.gets=0;self.statuses=list(statuses)
    def post(self,url,**kw):
        self.posts+=1;assert kw['json']['mode']=='async'
        return response({'job_id':'job-one','status':'running','billing':{'charged_usd':.001}},202)
    def get(self,url,**kw):
        self.gets+=1;status=self.statuses.pop(0) if len(self.statuses)>1 else self.statuses[0]
        return response({'job_id':'job-one','status':status,'platform':'douyin','action':'search','results':[],'refund_state':'refunded'})


def test_pending_restart_and_completed_cache_never_resubmit(tmp_path):
    s=Session(['running','succeeded'])
    with pytest.raises(jobs.JobPending):jobs.execute(BODY,tmp_path,key='test-key',session=s,poll_budget=0)
    assert s.posts==s.gets==1
    assert jobs.execute(BODY,tmp_path,key='test-key',session=s)['status']=='succeeded'
    assert s.posts==1 and s.gets==2
    jobs.execute(BODY,tmp_path,key=None,session=s)
    assert s.posts==1 and s.gets==2 and jobs.net_charge(tmp_path)==0


def test_ambiguous_submission_timeout_cannot_replay(tmp_path):
    s=Session()
    def fail(*a,**kw):s.posts+=1;raise requests.ReadTimeout('test-key failed')
    s.post=fail
    with pytest.raises(requests.ReadTimeout):jobs.execute(BODY,tmp_path,key='test-key',session=s)
    with pytest.raises(jobs.JobBlocked,match='unknown'):jobs.execute(BODY,tmp_path,key='test-key',session=s)
    assert s.posts==1
    assert 'test-key' not in (tmp_path/'submission-error.json').read_text()


def test_crash_after_receipt_before_first_poll_uses_saved_job(tmp_path):
    s=Session()
    def interrupted(*a,**kw):raise KeyboardInterrupt()
    old=s.get;s.get=interrupted
    with pytest.raises(KeyboardInterrupt):jobs.execute(BODY,tmp_path,key='test-key',session=s)
    s.get=old;jobs.execute(BODY,tmp_path,key='test-key',session=s)
    assert s.posts==1
    with pytest.raises(ValueError,match='scope changed'):jobs.execute({**BODY,'params':{'keyword':'other'}},tmp_path,key='test-key',session=s)


def test_failed_refunded_job_never_reposts(tmp_path):
    s=Session(['failed'])
    for _ in range(2):
        with pytest.raises(jobs.JobBlocked,match='failed'):jobs.execute(BODY,tmp_path,key='test-key',session=s)
    assert s.posts==1


def test_unconfirmed_503_does_not_repeat_even_if_refunded(tmp_path):
    s=Session()
    def fail(*a,**kw):s.posts+=1;return response({'error':{'code':'timeout'},'billing':{'charged':False}},503)
    s.post=fail
    for _ in range(2):
        with pytest.raises(jobs.JobBlocked):jobs.execute(BODY,tmp_path,key='test-key',session=s)
    assert s.posts==1


def test_job_query_failure_only_repeats_get(tmp_path):
    s=Session();old=s.get
    s.get=lambda *a,**kw:response({'error':'temporarily unavailable'},503)
    with pytest.raises(jobs.JobPending):jobs.execute(BODY,tmp_path,key='test-key',session=s)
    s.get=old;jobs.execute(BODY,tmp_path,key='test-key',session=s)
    assert s.posts==1


def test_actual_local_http_contract(tmp_path):
    counts={'post':0,'get':0}
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*a):pass
        def reply(self,p,status):
            data=json.dumps(p).encode();self.send_response(status);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
        def do_POST(self):
            body=json.loads(self.rfile.read(int(self.headers['Content-Length'])));assert body['mode']=='async';counts['post']+=1
            self.reply({'job_id':'job-local','status':'running'},202)
        def do_GET(self):
            counts['get']+=1;assert self.path=='/api/v1/jobs/job-local'
            self.reply({'job_id':'job-local','status':'succeeded','platform':'douyin','action':'search','results':[]},200)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:
        with requests.Session() as s:
            s.trust_env=False;endpoint=f'http://127.0.0.1:{server.server_port}/api/v1/social'
            for _ in range(2):jobs.execute(BODY,tmp_path,key='test-key',endpoint=endpoint,session=s)
        assert counts=={'post':1,'get':1}
    finally:server.shutdown();server.server_close();thread.join()


def test_xhs_pages_resume_without_repaying_completed_page(tmp_path,monkeypatch):
    cfg={'keywords':['乙女向'],'api':{'endpoint':'https://api.everyinfra.com/api/v1/social'}};calls=[]
    now=(datetime.now(timezone.utc)-timedelta(hours=1)).isoformat()
    def execute(body,folder,**kw):
        token=body['params'].get('page_token');calls.append(token)
        if token and calls.count(token)==1:raise jobs.JobPending('pending job')
        return {'platform':'xiaohongshu','action':'search','results':[{'id':str(token),'like_count':1,'is_video':True,'posted_at':now}], 'next_page_token':None if token else 'page-two'}
    monkeypatch.setattr(search,'execute',execute)
    assert search.collect_xhs(tmp_path,cfg,key='test-key')['status']=='incomplete'
    result=search.collect_xhs(tmp_path,cfg,key='test-key')
    assert result['status']=='complete' and result['pages']==2 and result['unique_notes']==2
    assert calls==[None,'page-two','page-two']


def test_legacy_unknown_search_is_blocked_not_reissued(tmp_path,monkeypatch):
    folder=tmp_path/'search/01';runtime.write(folder/'request.json',{'body':BODY,'started_at':datetime.now(timezone.utc).isoformat()})
    def fail(*a,**kw):pytest.fail('must not call an existing ambiguous submission')
    monkeypatch.setattr(search,'execute',fail)
    result=search.collect_douyin(tmp_path,{'keywords':['乙女向'],'api':{'endpoint':'https://api.everyinfra.com/api/v1/social'}},key='test-key')
    assert result['keywords'][0]['status']=='incomplete'


def test_concurrent_worker_does_not_submit_a_second_job(tmp_path):
    entered=threading.Event();release=threading.Event();s=Session();old=s.post;errors=[]
    def waiting(*a,**kw):
        entered.set();release.wait(timeout=5);return old(*a,**kw)
    s.post=waiting
    def first():
        try:jobs.execute(BODY,tmp_path,key='test-key',session=s)
        except BaseException as exc:errors.append(exc)
    thread=threading.Thread(target=first);thread.start();assert entered.wait(timeout=5)
    try:
        with pytest.raises(BlockingIOError):jobs.execute(BODY,tmp_path,key='test-key',session=s)
    finally:release.set();thread.join(timeout=5)
    assert not errors and s.posts==1


def test_both_platforms_end_to_end_search_evidence_and_cached_rerun(tmp_path,monkeypatch):
    from ua_workflows import xhs_daily as xhs, douyin_daily as douyin
    from ua_workflows.collection_runtime import write
    posted=(datetime.now(timezone.utc)-timedelta(hours=1)).isoformat();submitted={};counts={'post':0,'get':0}
    def post(url,**kw):
        counts['post']+=1;ident='job-'+str(counts['post']);submitted[ident]=kw['json']
        return response({'job_id':ident,'status':'running','billing':{'charged_usd':.001}},202)
    def get(url,**kw):
        counts['get']+=1;ident=url.rsplit('/',1)[1];body=submitted[ident]
        row={'id':'one','like_count':5,'is_video':True,'posted_at':posted,'url':'https://www.douyin.com/video/one'}
        return response({'job_id':ident,'status':'succeeded','platform':body['platform'],'action':'search','results':[row],'next_page_token':None})
    monkeypatch.setattr(requests,'post',post);monkeypatch.setattr(requests,'get',get)
    cfg={'keywords':['乙女向'],'api':{'endpoint':'https://api.everyinfra.com/api/v1/social'}}
    config=tmp_path/'config.json';write(config,cfg);monkeypatch.setattr(xhs,'CONFIG',config)
    xb=tmp_path/'xhs';db=tmp_path/'douyin'
    assert search.collect_xhs(xb,cfg,key='test-key')['status']=='complete'
    assert xhs.select(xb)['selected']==1
    search.collect_douyin(db,cfg,key='test-key');assert douyin.select(db,cfg)['selected']==1
    for _ in range(2):search.collect_xhs(xb,cfg,key='test-key');search.collect_douyin(db,cfg,key='test-key')
    assert counts=={'post':2,'get':2}
