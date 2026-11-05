import copy
import json
import pytest
from ua_workflows import afizzy_wecom_report as report

ENDPOINT='https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=test-only-not-a-credential'
CONFIG={'daily_workflow':{'completion_broadcast':{'enabled':True,'wecom':{'enabled':True,'mention_all':False,'webhook_env':'AFIZZY_WECOM_WEBHOOK','first_target_date':'2026-11-03'}}},
        'feishu':{'base_url':'https://example.feishu.cn/base/test','table_id':'tbl_test'}}


def stats():
    return {'target_date':'2026-11-03','verified_records':2,'total':{'new':10,'selected':2},
            'products':[{'product':'Tizz','new':10,'selected':2}]}


@pytest.fixture
def base(tmp_path,monkeypatch):
    path=tmp_path/'2026-11-03';path.mkdir()
    monkeypatch.setattr(report,'verified_payload',lambda _: {'stats':stats()})
    return path


def test_minimal_wecom_content_has_only_totals_products_and_link():
    p=report.build_payload(stats(),'https://example.com')
    assert p['msgtype']=='markdown'
    t=p['markdown']['content'];assert '竞品上新：**10条**' in t and '去重后入表：**2条**' in t
    assert 'Tizz：上新 **10** → 入表 **2**' in t
    assert not any(s in t for s in ['各轮剔除','封面','模板','未取得','预设','口径'])


def test_successful_wecom_send_is_not_repeated_and_receipt_has_no_secret(base):
    calls=[]
    class Response:
        status_code=200
        def json(self):return {'errcode':0,'errmsg':'ok'}
    def post(url,**kwargs):calls.append(kwargs);return Response()
    first=report.send(base,CONFIG,post=post,credentials={'AFIZZY_WECOM_WEBHOOK':ENDPOINT})
    second=report.send(base,CONFIG,post=post,credentials={'AFIZZY_WECOM_WEBHOOK':ENDPOINT})
    assert first==second and len(calls)==1 and first['status']=='accepted_by_webhook'
    assert calls[0]['allow_redirects'] is False
    for path in base.rglob('*.json'):
        assert 'test-only-not-a-credential' not in path.read_text() and ENDPOINT not in path.read_text()


def test_unknown_network_submission_is_not_replayed_or_leaked(base):
    calls=[]
    def post(url,**kwargs):calls.append(1);raise TimeoutError(url)
    with pytest.raises(ValueError) as error:report.send(base,CONFIG,post=post,credentials={'AFIZZY_WECOM_WEBHOOK':ENDPOINT})
    assert ENDPOINT not in str(error.value)
    with pytest.raises(ValueError,match='do not replay'):report.send(base,CONFIG,post=post,credentials={'AFIZZY_WECOM_WEBHOOK':ENDPOINT})
    assert len(calls)==1


def test_nonzero_errcode_is_not_success(base):
    class Response:
        status_code=200
        def json(self):return {'errcode':40058,'errmsg':'invalid'}
    with pytest.raises(ValueError):report.send(base,CONFIG,post=lambda *a,**k:Response(),credentials={'AFIZZY_WECOM_WEBHOOK':ENDPOINT})
    assert report.load(base/'wecom-report-receipt.json')['status']=='rejected_or_unconfirmed'


def test_long_content_does_not_split_into_multiple_messages():
    data=stats();data['products']=[{'product':'长'*2000,'new':10,'selected':2}]
    with pytest.raises(ValueError,match='one message'):report.build_payload(data,'https://example.com')


def test_arbitrary_endpoint_is_rejected_before_post(base):
    with pytest.raises(ValueError,match='endpoint'):
        report.send(base,CONFIG,post=lambda *a,**k:pytest.fail('No POST'),credentials={'AFIZZY_WECOM_WEBHOOK':'https://other.invalid/hook?key=test'})


def test_parent_broadcast_disabled_blocks_wecom(base):
    config=copy.deepcopy(CONFIG);config['daily_workflow']['completion_broadcast']['enabled']=False
    with pytest.raises(ValueError,match='disabled'):
        report.send(base,config,post=lambda *a,**k:pytest.fail('No POST'),credentials={'AFIZZY_WECOM_WEBHOOK':ENDPOINT})


def test_current_report_cannot_use_stale_ingestion_count(tmp_path):
    base=tmp_path/'2026-11-03';(base/'report').mkdir(parents=True)
    report.save(base/'report/payload.json',{'stats':stats()})
    report.save(base/'sync-receipt.json',{'status':'complete','verified':1})
    with pytest.raises(ValueError,match='not verified'):report.verified_payload(base)
