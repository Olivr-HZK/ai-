import copy
import json
import pytest
from ua_workflows import afizzy_daily_report as report
from ua_workflows.afizzy_wecom_report import build_payload

GROUPS={'products':[{'name':'Emochi','aliases':['Emochi Android','EMOCHI World iOS']}]}


def rows():
    return [
        {'id':'R1','os':'Android','app':'Emochi Android','ad_id':'a','kind':'video','status':'selected'},
        {'id':'R2','os':'iOS','app':'EMOCHI World iOS','ad_id':'b','kind':'image','status':'clip_crossday_removed'},
        {'id':'R3','os':'Android','app':'Unmapped product','ad_id':'c','kind':'video','status':'download_failed'},
    ]


def payload():
    stats=report.aggregate(rows(),GROUPS)
    stats.update(target_date='2026-11-03',status='partial_completed',verified_records=1,
                 selected_media={'video':1},missing_history_dates=[],marker='AFIZZY-DAY-test')
    return {'stats':stats,'card':report.build_card(stats,'https://example.feishu.cn/base/test')}


def config():
    return {'daily_workflow':{'completion_broadcast':{'enabled':True,'mention_all':False,'destination':'crawl_completion_report'}},
            'crawl_completion_report':{'enabled':True,'mention_all':False,'chat_id':'oc_allowed','chat_name':'Configured group'}}


class Client:
    def __init__(self):
        self.posts=[];self.message=None;self.read_fails=False;self.post_unknown=False;self.wrong_group=False

    def call(self,method,route,**kwargs):
        if route=='im/v1/chats/oc_allowed':
            return {'name':'Other group' if self.wrong_group else 'Configured group','chat_mode':'group'}
        if method=='POST':
            self.posts.append(kwargs['json'])
            if self.post_unknown:raise TimeoutError('Submission result unknown')
            self.message={'message_id':'om_known','chat_id':'oc_allowed','mentions':[],
                          'body':{'content':kwargs['json']['content']}}
            return self.message
        assert route=='im/v1/messages/om_known'
        if self.read_fails:raise PermissionError('Readback not available')
        return {'items':[self.message]}


@pytest.fixture
def base(tmp_path,monkeypatch):
    def prepare(path,config):
        result=payload();report.save(path/'report/payload.json',result);return result
    monkeypatch.setattr(report,'prepare',prepare)
    return tmp_path


def test_merge_oses_keep_unknown_and_failures_separate():
    stats=report.aggregate(rows(),GROUPS)
    assert stats['total']['new']==3 and stats['total']['downloaded']==2
    assert stats['total']['unavailable']==1 and stats['total']['dedupe_removed']==1
    emochi=stats['products'][0]
    assert emochi['product']=='Emochi' and emochi['new']==2 and emochi['selected']==1
    assert emochi['oses']==['Android','iOS']
    assert stats['products'][1]['product']=='Unmapped product'


def test_missing_original_without_media_kind_still_counts_as_unavailable(tmp_path):
    source = [{'preset':'tool2','pageNumber':1,'cardIndex':1,'adId':'missing',
               'state':'missing','firstDiscovered':'2026-11-04',
               'metadata':{'fields':{'appName':{'value':'Unmapped product'},
                                     'platform':{'value':'Android'}}}}]
    files = {
        'source-ledger.json':source, 'all-downloaded-media-index.json':[],
        'excluded-landscape.json':{'items':[]}, 'relaunch-exclusions.json':[],
        'pre-review/plan.json':{'decisions':{},'counts':{}},
        'review_observations.json':{'items':[]},
        'selection.json':{'items':[],'target_date':'2026-11-04','selected_count':0},
        'previous-day-template-review.json':{'matches':{},'pending':{}},
        'template-groups.json':[],
    }
    for name, value in files.items():
        report.save(tmp_path/name,value)
    evidence = report.evidence_rows(tmp_path)
    assert evidence[0]['kind']=='unknown'
    stats = report.aggregate(evidence,GROUPS)['total']
    assert stats['new']==stats['unavailable']==1
    assert stats['selected']==stats['dedupe_removed']==0


def test_all_gate_counts_partition_new_and_product_totals():
    data=[{'id':str(i),'ad_id':str(i),'app':'Emochi Android' if i%2 else 'EMOCHI World iOS',
           'os':'Android' if i%2 else 'iOS','kind':'video','status':bucket}
          for i,bucket in enumerate(report.BUCKETS)]
    result=report.aggregate(data,GROUPS)
    assert result['total']['new']==len(report.BUCKETS)
    assert result['total']['dedupe_removed']==4 and result['total']['held']==2
    assert len(result['products'])==1
    assert all(result['products'][0][key]==value for key,value in result['total'].items())


@pytest.mark.parametrize('mutation',[
    lambda data:data.append(dict(data[0])),
    lambda data:data[0].update(status='not_reviewed'),
    lambda data:data[0].update(os='unknown'),
])
def test_invalid_data_is_not_reported_as_zero(mutation):
    data=rows();mutation(data)
    with pytest.raises(ValueError):report.aggregate(data,GROUPS)


def test_ambiguous_alias_is_blocked():
    groups=copy.deepcopy(GROUPS);groups['products'].append({'name':'Other','aliases':['Emochi Android']})
    with pytest.raises(ValueError,match='Ambiguous'):report.aggregate(rows(),groups)


def test_card_has_overall_and_product_funnel_and_no_mentions():
    card=payload()['card'];text=json.dumps(card,ensure_ascii=False)
    assert len(card['elements'][0]['columns'])==2
    assert 'Emochi**　上新 **2** → 入表 **1**' in text
    assert all(label in text for label in ['竞品上新','去重后入表'])
    detail=card['elements'][-3]['text']['content']
    assert all(word not in detail for word in ['封面','模板','主体','未取得'])
    assert all(label not in text for label in ['预设','口径','前7自然日','AFIZZY-DAY','复刻','模型指纹','各轮剔除','封面','模板','未取得','Playable','待核'])
    assert '<at' not in text


def test_both_channels_group_competitor_regions_and_preserve_legacy_payload():
    groups = report.load(report.ROOT / 'config/afizzy_report_products.json')
    data = rows()
    data[0]['app'] = 'com.scatterlab.messenger'
    data[1]['app'] = 'zeta: AI Chat, Story, Play'
    stats = report.aggregate(data, groups)
    stats.update(target_date='2026-11-11', verified_records=1)
    url = 'https://example.feishu.cn/base/test'
    card = report.build_card(stats, url)
    wecom = build_payload(stats, url)
    assert '**日韩竞品**\n**zeta**　上新 **2** → 入表 **1**' in card['elements'][-3]['text']['content']
    assert '**日韩竞品**\nzeta：上新 **2** → 入表 **1**' in wecom['markdown']['content']
    assert '**地区待确认**\nUnmapped product' in wecom['markdown']['content']

    dual = dict(stats['products'][0], product='PolyBuzz', region='日韩／欧美及巴西')
    dual_sections = report.product_sections([dual])
    assert [title for title, _ in dual_sections] == ['日韩竞品', '欧美及巴西竞品']
    assert all(members == [dual] for _, members in dual_sections)

    legacy_stats = copy.deepcopy(stats)
    for product in legacy_stats['products']:
        del product['region']
    legacy_text = build_payload(legacy_stats, url)['markdown']['content']
    assert '\nzeta：上新 **2** → 入表 **1**' in legacy_text
    assert '地区待确认' not in legacy_text


def test_known_send_is_only_read_back_on_repeat(base):
    client=Client();first=report.send(base,config(),client);second=report.send(base,config(),client)
    assert first['status']==second['status']=='verified'
    assert len(client.posts)==1 and first['message_id']==second['message_id']


def test_unknown_submission_is_persisted_and_never_replayed(base):
    client=Client();client.post_unknown=True
    with pytest.raises(TimeoutError):report.send(base,config(),client)
    assert report.load(base/'report-receipt.json')['status']=='send_unknown'
    with pytest.raises(ValueError,match='cannot be replayed'):report.send(base,config(),client)
    assert len(client.posts)==1


def test_readback_failure_resumes_with_get_only(base):
    client=Client();client.read_fails=True
    with pytest.raises(PermissionError):report.send(base,config(),client)
    assert report.load(base/'report-receipt.json')['message_id']=='om_known'
    client.read_fails=False
    assert report.send(base,config(),client)['status']=='verified'
    assert len(client.posts)==1


def test_group_change_blocks_before_send(base):
    client=Client();client.wrong_group=True
    with pytest.raises(ValueError,match='group identity'):report.send(base,config(),client)
    assert not client.posts


def test_modified_sent_payload_is_not_reposted(base):
    client=Client();report.send(base,config(),client)
    p=report.load(base/'report/payload.json');p['card']['header']['title']['content']='changed'
    report.save(base/'report/payload.json',p)
    with pytest.raises(ValueError,match='payload changed'):report.send(base,config(),client)
    assert len(client.posts)==1


def test_wrong_readback_counts_or_mentions_are_not_verified(base):
    p=payload();client=Client();report.send(base,config(),client)
    receipt=report.load(base/'report-receipt.json')
    bad=copy.deepcopy(client.message);bad['body']['content']=bad['body']['content'].replace('**3 条**','**9 条**')
    with pytest.raises(ValueError,match='content mismatch'):report.verify_message(bad,receipt,p['card'])
    bad=copy.deepcopy(client.message);bad['mentions']=[{'id':'all'}]
    with pytest.raises(ValueError,match='mentions'):report.verify_message(bad,receipt,p['card'])


def test_server_text_fragments_do_not_insert_schema_tag_names():
    card={'elements':[{'tag':'div','text':{'tag':'lark_md','content':'**竞品上新：3 条**　**实际入表：1 条**'}}]}
    message={'message_id':'om_test','chat_id':'oc_test','body':{'content':json.dumps({
        'elements':[[{'tag':'text','text':'竞品上新：3 条'}, {'tag':'text','text':'　'},
                     {'tag':'text','text':'实际入表：1 条'}]]})}}
    report.verify_message(message,{'message_id':'om_test','chat_id':'oc_test'},card)


def test_missing_history_permission_uses_real_send_response_without_replay(base):
    class LimitedClient(Client):
        def call(self,method,route,**kwargs):
            if route=='im/v1/messages/om_known':
                raise ValueError('code=99991672 missing im:message:readonly')
            return super().call(method,route,**kwargs)
    client=LimitedClient()
    first=report.send(base,config(),client)
    second=report.send(base,config(),client)
    assert first['status']==second['status']=='verified_by_send_response'
    assert first['readback_status']=='permission_denied' and len(client.posts)==1


def test_prepare_cannot_replace_a_sent_payload(tmp_path):
    report.save(tmp_path/'report-receipt.json',{'message_id':'om_already_sent'})
    with pytest.raises(ValueError,match='Preserve a sent'):
        report.prepare(tmp_path,{})
