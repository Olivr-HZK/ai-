import json
from pathlib import Path

import pytest

from ua_workflows import xhs_daily as x


def note(i='a', likes=5, posted='2026-10-19T10:00:00+00:00'):
    return {'id':i,'url':'https://www.xiaohongshu.com/explore/'+i,'title':'样本','text':'摘要',
            'posted_at':posted,'like_count':likes,'collect_count':0,'comment_count':2,'share_count':None,
            'is_video':True,'matched_keywords':['乙女向']}


def test_new_and_old_daily_batches_keep_their_original_table(tmp_path):
    config={'feishu':{'app_token':'base','base_url':'https://example.com/base','table_id':'new','view_id':'new-view','expected_app_id':'app'}}
    old={'target_date':'2026-10-26'}
    legacy=x.daily_destination(tmp_path,config,old)
    assert legacy['feishu']['table_id']=='tblwhAqPfQoBXK3Q' and not legacy['feishu']['view_id']
    pinned={**old,'feishu_target':{'app_token':'base','base_url':'https://example.com/base','table_id':'new','view_id':'new-view'}}
    assert x.daily_destination(tmp_path,config,pinned)['feishu']['table_id']=='new'
    x.save(tmp_path/'run.json',{'started_at':'2026-10-27T00:00:00+08:00'})
    x.save(tmp_path/'runtime/config-snapshot.json',{'target_date':old['target_date'],'run_sha256':x.sha(tmp_path/'run.json'),
           'config':{'feishu':{'app_token':'base','base_url':'https://example.com/base','table_id':'snapshot'}}})
    assert x.daily_destination(tmp_path,config,old)['feishu']['table_id']=='snapshot'
    x.save(tmp_path/'run.json',{'changed':True})
    with pytest.raises(ValueError,match='snapshot'):x.daily_destination(tmp_path,config,old)


def fixture_run(tmp_path, monkeypatch, notes):
    cfg=tmp_path/'config.json';x.save(cfg,{'keywords':['乙女向','女性向']});monkeypatch.setattr(x,'CONFIG',cfg)
    clean=[{k:v for k,v in n.items() if k!='matched_keywords'} for n in notes]
    for i,keyword in enumerate(['乙女向','女性向'],1):
        stem=tmp_path/f'keywords/{i:02}/pages/0001'
        x.save(stem.with_suffix('.request.json'),{'requested_at':'2026-10-19T19:00:00+08:00','body':{'params':{'keyword':keyword,'content_type':'all','date_range':'last_24_hours','sort':'newest'}}})
        x.save(stem.with_suffix('.response.json'),{'results':clean,'next_page_token':None})
    x.save(tmp_path/'notes.json',[{**n,'matched_keywords':['乙女向','女性向']} for n in notes])
    x.save(tmp_path/'summary.json',{'status':'complete','date_range':'last_24_hours','unique_notes':len(notes),'keywords':[{'keyword':k,'pages':1} for k in ['乙女向','女性向']]})
    x.save(tmp_path/'run.json',{'sort':'newest'})
    return tmp_path


def test_global_dedupe_decile_ties_and_raw_evidence(tmp_path,monkeypatch):
    notes=[note(str(i), 1) for i in range(11)]
    notes[1]=note('1',20,'2026-10-19T09:00:00+00:00');notes[2]=note('2',20)
    run=fixture_run(tmp_path,monkeypatch,notes);result=x.select(run)
    assert result['denominator']==11 and result['selected']==10
    assert [n['id'] for n in result['items'][:2]]==['2','1']
    merged=x.load(run/'notes.json');merged[0]['like_count']=999;x.save(run/'notes.json',merged)
    with pytest.raises(ValueError,match='raw evidence'):x.select(run)


def test_missing_likes_blocks_not_zero(tmp_path,monkeypatch):
    run=fixture_run(tmp_path,monkeypatch,[note(likes=None)])
    with pytest.raises(ValueError,match='likes'):x.select(run)


@pytest.mark.parametrize('total,expected',[(0,0),(1,1),(7,7),(9,9),(10,10),(11,10),(99,10),(100,10),(101,11),(111,12)])
def test_candidate_count_rule_after_global_dedupe(tmp_path,monkeypatch,total,expected):
    run=fixture_run(tmp_path,monkeypatch,[note(str(i),likes=i) for i in range(total)])
    result=x.select(run)
    assert result['denominator']==total and result['selected']==expected
    assert [n['id'] for n in result['items']]==[str(i) for i in range(total-1,total-expected-1,-1)]
    assert result['selection_mode']==('top_n' if total<100 else 'fraction')


def test_count_threshold_uses_content_filtered_candidates(tmp_path,monkeypatch):
    notes=[note(str(i),likes=i) for i in range(101)]
    notes[99]['is_video']=False;notes[100]['is_video']=False
    monkeypatch.setattr(x,'reviewed_images',lambda *a:{'files':[]})
    result=x.select(fixture_run(tmp_path,monkeypatch,notes))
    assert result['raw_unique_notes']==101 and result['denominator']==99
    assert result['selected']==10 and result['selection_mode']=='top_n'


def test_unfinished_cursor_blocks(tmp_path,monkeypatch):
    run=fixture_run(tmp_path,monkeypatch,[note()]);p=run/'keywords/01/pages/0001.response.json';r=x.load(p);r['next_page_token']='more';x.save(p,r)
    with pytest.raises(ValueError,match='pagination'):x.select(run)


def test_old_timestamp_blocks(tmp_path,monkeypatch):
    run=fixture_run(tmp_path,monkeypatch,[note(posted='2026-10-17T10:00:00+00:00')])
    with pytest.raises(ValueError,match='window'):x.select(run)


def pin_calendar_day(run):
    x.save(run/'daily-context.json',{'target_date':'2026-10-19','execution_started_at':'2026-10-20T14:00:00+08:00',
                                   'timezone':'Asia/Shanghai','date_filter_rule':x.CALENDAR_DAY_RULE})


def test_calendar_day_filters_shanghai_boundaries_without_rewriting_raw_data(tmp_path,monkeypatch):
    notes=[note('before',posted='2026-10-18T15:59:59+00:00'),
           note('start',posted='2026-10-18T16:00:00Z'),
           note('end',posted='2026-10-19T23:59:59+08:00'),
           note('today',posted='2026-10-19T16:00:00+00:00'),
           note('old',posted='2024-07-15T04:55:05+00:00')]
    run=fixture_run(tmp_path,monkeypatch,notes);pin_calendar_day(run)
    raw={str(p):p.read_bytes() for p in [run/'notes.json',*run.glob('keywords/*/pages/*.response.json')]}
    # The local day stays fixed even for a late or resumed run; request windows do not redefine it.
    for p in run.glob('keywords/*/pages/*.request.json'):
        r=x.load(p);r['requested_at']='2026-10-20T14:00:00+08:00';x.save(p,r)
    result=x.select(run)
    assert {n['id'] for n in result['items']}=={'start','end'}
    assert result['date_excluded_unique']==3 and result['date_excluded_hits']==6
    assert result['target_day_start']=='2026-10-19T00:00:00+08:00'
    assert result['target_day_end_exclusive']=='2026-10-20T00:00:00+08:00'
    assert all(p.read_bytes()==data for name,data in raw.items() for p in [Path(name)])
    assert len(x.load(run/'excluded-outside-target-day.json'))==6
    assert x.load(run/'date-validation.json')['status']=='passed_target_calendar_day_filter'


def test_calendar_day_requires_valid_target_and_aware_publication_timestamp(tmp_path,monkeypatch):
    run=fixture_run(tmp_path,monkeypatch,[note(posted='2026-10-19T10:00:00')]);pin_calendar_day(run)
    with pytest.raises(ValueError,match='timezone'):x.select(run)
    context=x.load(run/'daily-context.json');context['target_date']='2026-10-18';x.save(run/'daily-context.json',context)
    with pytest.raises(ValueError,match='day before'):x.select(run)


def test_calendar_day_excludes_irrelevant_missing_likes_without_filling_zero(tmp_path,monkeypatch):
    run=fixture_run(tmp_path,monkeypatch,[note('new'),{**note('old',likes=None,posted='2024-07-15T04:55:05Z'),'is_video':None}]);pin_calendar_day(run)
    original=(run/'notes.json').read_bytes();result=x.select(run)
    assert [n['id'] for n in result['items']]==['new'] and (run/'notes.json').read_bytes()==original
    p=run/'notes.json';ns=x.load(p);ns[0]['like_count']=None;x.save(p,ns)
    for p in run.glob('keywords/*/pages/*.response.json'):
        d=x.load(p);d['results'][0]['like_count']=None;x.save(p,d)
    with pytest.raises(ValueError,match='likes'):x.select(run)


def test_calendar_day_cannot_keep_an_old_merged_note_using_a_conflicting_observation(tmp_path,monkeypatch):
    run=fixture_run(tmp_path,monkeypatch,[note(posted='2024-07-15T04:55:05Z')]);pin_calendar_day(run)
    p=run/'keywords/02/pages/0001.response.json';d=x.load(p);d['results'][0]['posted_at']='2026-10-19T10:00:00Z';x.save(p,d)
    with pytest.raises(ValueError,match='Conflicting publication timestamps'):x.select(run)


def test_date_filter_precedes_image_download_without_per_batch_exception(tmp_path,monkeypatch):
    notes=[{**note('keep'), 'is_video':False},
           {**note('old',posted='2024-07-15T04:55:05Z'),'is_video':False}]
    run=fixture_run(tmp_path,monkeypatch,notes);pin_calendar_day(run);calls=[]
    monkeypatch.setattr(x,'media',lambda n,*a:(calls.append(n['id']) or {'files':[]}))
    monkeypatch.setattr(x,'reviewed_images',lambda *a:{'files':[]})
    assert x.prepare_image_review(run)['status']=='complete'
    assert calls==['keep'] and not (run/'date-policy.json').exists()


def test_completed_delivery_is_not_recomputed_after_configuration_change(tmp_path,monkeypatch,capsys):
    base=tmp_path/'historical';x.save(base/'daily-context.json',{'target_date':'2026-10-19'})
    x.save(base/'selection.json',{'original':'kept'});digest=x.sha(base/'selection.json')
    x.save(base/'daily-result.json',{'target_date':'2026-10-19','selection_sha256':digest,
                                   'collection_status':'partial','sync':{'status':'complete'},'report':{'message_id':'om_saved'}})
    x.save(base/'report-receipt.json',{'message_id':'om_saved'})
    cfg=tmp_path/'config.json';x.save(cfg,{'keywords':['changed'],'date_semantics':x.CALENDAR_DAY_RULE})
    monkeypatch.setattr(x,'ROOT',tmp_path);monkeypatch.setattr(x,'CONFIG',cfg)
    monkeypatch.setattr(x.sys,'argv',['xhs_daily','run','--run-dir',str(base)])
    def never(*a):raise AssertionError('Completed delivery must not search or prepare media')
    monkeypatch.setattr(x,'prepare_image_review',never);monkeypatch.setattr(x,'collection_phase',never)
    x.main();assert 'already_delivered' in capsys.readouterr().out
    assert x.sha(base/'selection.json')==digest


def authorize_old_exclusion(run):
    x.save(run/'daily-context.json',{'target_date':'2026-10-19'})
    x.save(run/'manual-authorization.json',{'exclude_old_hits':True,'answer':'explicitly approved'})
    x.save(run/'date-policy.json',{'version':'exclude_old_hits_v1','target_date':'2026-10-19',
           'run_sha256':x.sha(run/'run.json'),'authorization_file':'manual-authorization.json',
           'authorization_sha256':x.sha(run/'manual-authorization.json')})


def test_authorized_date_exclusion_preserves_raw_and_filtered_denominators(tmp_path,monkeypatch):
    run=fixture_run(tmp_path,monkeypatch,[note('old',999,'2026-10-17T10:00:00+00:00'),note('new')])
    before=(run/'notes.json').read_bytes();authorize_old_exclusion(run)
    result=x.select(run)
    assert result['raw_unique_notes']==2 and result['date_excluded_hits']==2
    assert result['date_excluded_unique']==1 and result['denominator']==1
    assert result['image_content_excluded']==0 and result['items'][0]['id']=='new'
    assert (run/'notes.json').read_bytes()==before
    assert len(x.load(run/'excluded-old-posts.json'))==2


def test_date_exclusion_authorization_rejects_changed_scope_or_approval(tmp_path,monkeypatch):
    run=fixture_run(tmp_path,monkeypatch,[note()]);authorize_old_exclusion(run)
    x.save(run/'manual-authorization.json',{'exclude_old_hits':True,'answer':'changed'})
    with pytest.raises(ValueError,match='authorization evidence'):x.select(run)


def test_authorized_old_exclusion_still_blocks_future_dates(tmp_path,monkeypatch):
    run=fixture_run(tmp_path,monkeypatch,[note(posted='2026-10-20T10:00:00+00:00')]);authorize_old_exclusion(run)
    with pytest.raises(ValueError,match='window'):x.select(run)


def test_partial_delivery_keeps_incomplete_search_status_and_binds_repeat_evidence(tmp_path,monkeypatch):
    run=fixture_run(tmp_path,monkeypatch,[note()]);authorize_old_exclusion(run)
    summary=x.load(run/'summary.json');summary['status']='incomplete'
    summary['keywords'][0]['status']='incomplete';summary['keywords'][1]['status']='complete'
    x.save(run/'summary.json',summary)
    p=run/'keywords/01/pages/0001.response.json';response=x.load(p);response['next_page_token']='real-next';x.save(p,response)
    duplicate=run/'keywords/01/pages/0002.response.json';x.save(duplicate,response)
    proof={'response_path':str(duplicate.relative_to(run)),'response_sha256':x.sha(duplicate),
           'duplicate_response_path':str(p.relative_to(run)),'duplicate_response_sha256':x.sha(p)}
    x.save(run/'repeated-page-validation.json',proof)
    x.save(run/'partial-delivery-authorization.json',{'allow_partial_delivery':True,'answer':'approved'})
    x.save(run/'partial-collection-policy.json',{'version':'authorized_available_results_v1',
           'target_date':'2026-10-19','run_sha256':x.sha(run/'run.json'),'summary_sha256':x.sha(run/'summary.json'),
           'authorization_sha256':x.sha(run/'partial-delivery-authorization.json'),
           'repeat_proof_sha256':x.sha(run/'repeated-page-validation.json'),
           'incomplete_keyword':'乙女向','accepted_pages':1})
    original=(run/'summary.json').read_bytes();result=x.select(run)
    assert result['collection_status']=='partial' and result['incomplete_keywords']==['乙女向']
    assert (run/'summary.json').read_bytes()==original and x.load(run/'summary.json')['status']=='incomplete'
    response['results']=[];x.save(duplicate,response)
    with pytest.raises(ValueError,match='Repeated page evidence'):x.select(run)


@pytest.mark.parametrize('collection_status',['partial','complete'])
def test_unavailable_media_needs_bound_explicit_approval_and_cannot_hide_available_file(tmp_path,monkeypatch,collection_status):
    run=fixture_run(tmp_path,monkeypatch,[note('bad')])
    monkeypatch.setattr(x,'validated_notes',lambda base:([note('bad')],{'collection_status':collection_status}))
    assert len(x.reviewable_notes(run)[0])==1
    x.save(run/'media/bad/failure.json',{'status':'failed','error_type':'ReadTimeout'})
    x.save(run/'media-unavailable-authorization.json',{'allow_unavailable_media':True,'answer':'explicitly approved'})
    x.save(run/'unavailable-media-policy.json',{'version':'authorized_unavailable_media_v1',
           'run_sha256':x.sha(run/'run.json'),'summary_sha256':x.sha(run/'summary.json'),
           'authorization_sha256':x.sha(run/'media-unavailable-authorization.json'),
           'failures':[{'note_id':'bad','evidence':[{'path':'media/bad/failure.json','sha256':x.sha(run/'media/bad/failure.json')}]}]})
    remaining,stats=x.reviewable_notes(run)
    assert not remaining and stats['media_unavailable_notes']==1
    assert stats['collection_status']==collection_status and stats['media_review_status']=='partial'
    assert not (run/'media/bad/audience-review.json').exists()
    x.save(run/'media/bad/manifest.json',{'note_id':'bad'})
    with pytest.raises(ValueError,match='Available media'):x.reviewable_notes(run)


def test_fields_preserve_null_and_do_not_invent_ad_data(tmp_path):
    row=x.row_fields(note(),{'files':[{'width':720,'height':954,'url':'https://sns-video.xhscdn.com/a.mp4'}]},'2026-10-19',tmp_path)
    assert row['分享数'] is None and row['收藏数']==0
    assert row['来源']=='每日最新' and row['平台']=='小红书' and row['素材类型']=='视频'
    assert 'App ID' not in row and 'appid' not in row and '展示估值' not in row and '广大大详情' not in row
    assert row['发布时间']=='2026-10-19T18:00:00+08:00'
    x.verify_row({'分享数':None,'点赞数':'5','视频附件':[{'file_token':'a','size':10}]},{'分享数':None,'点赞数':5,'视频附件':[{'file_token':'a'}]})
    with pytest.raises(ValueError):x.verify_row({'分享数':0},{'分享数':None})
    with pytest.raises(ValueError):x.verify_row({'视频附件':[{'file_token':'a','size':0}]},{'视频附件':[{'file_token':'a'}]})


def test_delivery_readback_rejects_truncation_duplicate_rows_and_local_changes(tmp_path,monkeypatch):
    path=tmp_path/'video.mp4';path.write_bytes(b'original-video-bytes')
    item={'path':'video.mp4','sha256':x.sha(path),'size':path.stat().st_size}
    monkeypatch.setattr(x,'media',lambda *args:{'files':[item]})
    fields={'唯一键':'key','笔记ID':'a','视频附件':[{'file_token':'token'}]}
    remote=[{'record_id':'record','fields':{**fields,'视频附件':[{'file_token':'token','size':item['size']}]}}]
    args=(tmp_path,{'items':[note()]},{},remote,[fields],{item['sha256']:'token'})
    assert x.verify_delivery_rows(*args)['verified_attachments']==1
    remote[0]['fields']['视频附件'][0]['size']-=1
    with pytest.raises(ValueError,match='exact attachment size'):x.verify_delivery_rows(*args)
    remote[0]['fields']['视频附件'][0]['size']=item['size'];remote.append(remote[0])
    with pytest.raises(ValueError,match='unique key'):x.verify_delivery_rows(*args)
    remote.pop();path.write_bytes(b'changed')
    with pytest.raises(ValueError,match='local media changed'):x.verify_delivery_rows(*args)


class MessageClient:
    def __init__(self):self.sent=[]
    def call(self,method,route,**kw):
        if method=='GET':return {'name':'霍振堃','chat_mode':'group'}
        self.sent.append(kw);return {'message_id':'om_test','chat_id':'oc_test'}


def test_calendar_day_report_separates_local_day_from_provider_search_window(tmp_path):
    x.save(tmp_path/'summary.json',{'raw_rows':3,'pages':1,'unique_notes':3,'video_notes':3,'image_notes':0})
    x.save(tmp_path/'run.json',{'started_at':'2026-10-20T00:01:00+08:00'})
    cfg={'feishu':{'base_url':'https://example.com/base','table_id':'table'},'crawl_completion_report':{'chat_id':'oc_test','chat_name':'霍振堃'}}
    client=MessageClient();selection={'selected':1,'denominator':1,'date_filter_rule':x.CALENDAR_DAY_RULE}
    x.report(tmp_path,selection,cfg,client,{'status':'complete','verified_rows':1,'video_success':1,'image_success':0,'failures':[]},'2026-10-19')
    text=json.loads(client.sent[0]['json']['content'])['elements'][0]['text']['content']
    assert '北京时间2026-10-19自然日' in text and '搜索接口仍按实际请求前24小时' in text
    assert '可能不能完整覆盖目标自然日' in text


def test_partial_report_and_idempotency(tmp_path):
    x.save(tmp_path/'summary.json',{'raw_rows':20,'pages':1,'unique_notes':20,'video_notes':8,'image_notes':12})
    x.save(tmp_path/'run.json',{'started_at':'2026-10-20T00:00:00+08:00'})
    cfg={'feishu':{'base_url':'https://example.com/base','table_id':'table'},'crawl_completion_report':{'chat_id':'oc_test','chat_name':'霍振堃'}}
    client=MessageClient();synced={'status':'partial','verified_rows':1,'video_success':1,'image_success':0,'failures':[{'note_id':'b'}]}
    x.report(tmp_path,{'selected':2,'denominator':20},cfg,client,synced,'2026-10-19');x.report(tmp_path,{'selected':2,'denominator':20},cfg,client,synced,'2026-10-19')
    assert len(client.sent)==1
    card=json.loads(client.sent[0]['json']['content']);assert '部分完成' in card['header']['title']['content']
    assert card['elements'][1]['actions'][0]['url']=='https://example.com/base?table=table'
    assert '展示估值' not in client.sent[0]['json']['content']


def test_complete_search_with_authorized_unreviewed_media_reports_partial_once(tmp_path):
    x.save(tmp_path/'summary.json',{'raw_rows':20,'pages':7,'unique_notes':20,'video_notes':10,'image_notes':10})
    x.save(tmp_path/'run.json',{'started_at':'2026-10-27T00:00:00+08:00'})
    cfg={'feishu':{'base_url':'https://example.com/base','table_id':'table'},'crawl_completion_report':{'chat_id':'oc_test','chat_name':'霍振堃'}}
    client=MessageClient();selection={'selected':10,'denominator':10,'collection_status':'complete','media_review_status':'partial','media_unavailable_notes':4}
    synced={'status':'complete','verified_rows':10,'video_success':5,'image_success':5,'failures':[]}
    x.report(tmp_path,selection,cfg,client,synced,'2026-10-26')
    x.report(tmp_path,selection,cfg,client,synced,'2026-10-26')
    assert len(client.sent)==1
    card=json.loads(client.sent[0]['json']['content']);text=card['elements'][0]['text']['content']
    assert card['header']['template']=='orange' and '部分完成' in card['header']['title']['content']
    assert '全部关键词已到接口末页' in text and '未审核：**4 篇**' in text
    assert '排名仅基于已取得并审核的素材' in text and '未到末页' not in text


def test_partial_collection_report_remains_partial_when_all_selected_rows_sync(tmp_path):
    x.save(tmp_path/'summary.json',{'raw_rows':489,'pages':25,'unique_notes':451,'video_notes':114,'image_notes':337,'prior_timeout_billing_unknown':True})
    x.save(tmp_path/'run.json',{'started_at':'2026-10-26T00:00:00+08:00'})
    cfg={'feishu':{'base_url':'https://example.com/base','table_id':'table'},'crawl_completion_report':{'chat_id':'oc_test','chat_name':'霍振堃'}}
    client=MessageClient();selection={'selected':16,'denominator':156,'collection_status':'partial','incomplete_keywords':['猫箱'],'media_unavailable_notes':6}
    x.report(tmp_path,selection,cfg,client,{'status':'complete','verified_rows':16,'video_success':11,'image_success':5,'failures':[]},'2026-10-25')
    card=json.loads(client.sent[0]['json']['content']);text=card['elements'][0]['text']['content']
    assert card['header']['template']=='orange' and '部分完成' in card['header']['title']['content']
    assert '猫箱未到末页' in text and '排名仅基于已获数据' in text
    assert '未审核：**6 篇**' in text and '不计入可审核排名分母' in text
    assert '全部关键词已到接口末页' not in text


def test_range_download_resumes_and_checks_full_size(tmp_path,monkeypatch):
    import requests
    data=b'x'*(1024*1024+123)
    calls=[]
    class Response:
        status_code=206
        def __init__(self,start,end):
            self.start,self.end=start,end;self.headers={'Content-Range':f'bytes {start}-{end}/{len(data)}'}
        def __enter__(self):return self
        def __exit__(self,*args):return False
        def raise_for_status(self):pass
        def iter_content(self,chunk_size):yield data[self.start:self.end+1]
    def get(url,headers,**kwargs):
        assert headers['Referer']=='https://www.xiaohongshu.com/' and headers['User-Agent']=='Mozilla/5.0'
        start,end=map(int,headers['Range'].split('=')[1].split('-'));calls.append((start,end));return Response(start,end)
    monkeypatch.setattr(requests,'get',get)
    dest=tmp_path/'video.mp4';partial=dest.with_suffix('.mp4.part');partial.write_bytes(data[:100])
    x.download('https://sns-video-v4.xhscdn.com/video.mp4',dest)
    assert dest.read_bytes()==data and not partial.exists()
    assert calls[0]==(0,0) and calls[1][0]==0
    assert any(p.read_bytes()==data[:100] for p in tmp_path.glob('*.part.saved-*'))


def test_range_download_rejects_wrong_content_range(tmp_path,monkeypatch):
    import requests
    class Response:
        status_code=206
        headers={'Content-Range':'bytes 0-0/100'}
        def iter_content(self,chunk_size):yield b'x'
        def __enter__(self):return self
        def __exit__(self,*args):return False
        def raise_for_status(self):pass
    monkeypatch.setattr(requests,'get',lambda *a,**kw:Response())
    with pytest.raises(ValueError,match='unexpected byte range'):
        x.download('https://sns-video-v4.xhscdn.com/video.mp4',tmp_path/'video.mp4')


def test_large_attachment_uses_blocks_and_reuses_receipt(tmp_path):
    import types
    path=tmp_path/'video.mp4';path.write_bytes(b'x'*(20*1024*1024+1))
    class Client:
        def __init__(self):self.parts=[];self.calls=[];self.session=types.SimpleNamespace(post=self.post)
        def call(self,method,route,**kw):
            self.calls.append(route)
            if route.endswith('upload_prepare'):
                assert kw['json']['parent_type']=='bitable_file'
                return {'upload_id':'up1','block_size':10*1024*1024,'block_num':3}
            assert route.endswith('upload_finish')
            return {'file_token':'file1'}
        def post(self,url,data,files,**kw):
            self.parts.append(int(data['seq']))
            assert int(data['size'])==len(files['file'][1])
            return types.SimpleNamespace(ok=True,json=lambda:{'code':0})
    client=Client();config={'feishu':{'app_token':'base'}}
    assert x.upload_attachment(client,config,path,'video.mp4')=='file1'
    assert client.parts==[0,1,2]
    assert x.upload_attachment(client,config,path,'video.mp4')=='file1'
    assert len(client.calls)==2


def reviewed_note(tmp_path, decisions):
    n=note('image');n['is_video']=False;n['images']=['https://a.xhscdn.com/'+str(i) for i in range(len(decisions))]
    folder=tmp_path/'media/image';folder.mkdir(parents=True,exist_ok=True);files=[];reviews=[]
    for i,decision in enumerate(decisions):
        path=folder/f'{i}.png';path.write_bytes(str(i).encode());item={'path':str(path.relative_to(tmp_path)),'sha256':x.sha(path),'url':n['images'][i]};files.append(item)
        reviews.append({**item,'decision':decision,'reason':'Actual visual decision','visually_inspected':True,'reviewer':'test fixture'})
    x.save(folder/'manifest.json',{'note_id':'image','is_video':False,'files':files})
    x.save(folder/'image-review.json',{'rule':x.IMAGE_RULE,'note_id':'image','images':reviews})
    return n,folder


def test_image_review_keeps_only_person_pages_and_rejects_changed_bytes(tmp_path):
    n,folder=reviewed_note(tmp_path,['keep','exclude'])
    assert len(x.reviewed_images(n,tmp_path)['files'])==1
    (folder/'0.png').write_bytes(b'changed')
    with pytest.raises(ValueError,match='hash mismatch'):x.reviewed_images(n,tmp_path)


def test_image_review_missing_or_incomplete_blocks(tmp_path):
    n,folder=reviewed_note(tmp_path,['keep','exclude']);review=x.load(folder/'image-review.json');review['images'].pop();x.save(folder/'image-review.json',review)
    with pytest.raises(ValueError,match='all original images'):x.reviewed_images(n,tmp_path)
    (folder/'image-review.json').unlink()
    with pytest.raises(ValueError,match='IMAGE_REVIEW_PENDING'):x.reviewed_images(n,tmp_path)


def test_text_only_note_excluded_before_decile(tmp_path,monkeypatch):
    n,folder=reviewed_note(tmp_path,['exclude'])
    fixture_run(tmp_path,monkeypatch,[n]+[note(str(i)) for i in range(10)])
    result=x.select(tmp_path)
    assert result['raw_unique_notes']==11 and result['denominator']==10 and result['selected']==10
    assert result['image_content_excluded']==1 and all(n['is_video'] for n in result['items'])


def test_male_subject_filter_excludes_female_and_mixed(tmp_path):
    n,folder=reviewed_note(tmp_path,['keep','keep']);manifest=x.reviewed_images(n,tmp_path)
    entries=[{**m,'presentation':p,'reason':'visual decision','visually_inspected':True,'reviewer':'fixture','evidence':'contact.jpg'} for m,p in zip(manifest['files'],['male_subject','female_subject'])]
    x.save(folder/'audience-review.json',{'rule':x.AUDIENCE_RULE,'note_id':n['id'],'files':entries})
    result=x.male_subject_media(n,tmp_path,manifest)
    assert [m['path'] for m in result['files']]==[manifest['files'][0]['path']]
    entries[0]['presentation']='mixed';x.save(folder/'audience-review.json',{'rule':x.AUDIENCE_RULE,'note_id':n['id'],'files':entries})
    assert not x.male_subject_media(n,tmp_path,manifest)['files']


def test_male_subject_pending_and_unknown_never_pass(tmp_path):
    n,folder=reviewed_note(tmp_path,['keep']);manifest=x.reviewed_images(n,tmp_path)
    with pytest.raises(ValueError,match='AUDIENCE_REVIEW_PENDING'):x.male_subject_media(n,tmp_path,manifest)
    entry={**manifest['files'][0],'presentation':'uncertain','reason':'uncertain','visually_inspected':True,'reviewer':'fixture','evidence':'contact.jpg'}
    x.save(folder/'audience-review.json',{'rule':x.AUDIENCE_RULE,'note_id':n['id'],'files':[entry]})
    assert not x.male_subject_media(n,tmp_path,manifest)['files']


def test_audience_filter_precedes_global_decile(tmp_path,monkeypatch):
    n,folder=reviewed_note(tmp_path,['keep']);n['like_count']=10000
    fixture_run(tmp_path,monkeypatch,[n]+[note(str(i)) for i in range(10)])
    cfg=x.load(x.CONFIG);cfg['audience_content_filter']={'enabled':True};x.save(x.CONFIG,cfg)
    with pytest.raises(ValueError,match='AUDIENCE_REVIEW_PENDING'):x.select(tmp_path)


def test_audience_frames_use_real_sparse_video_frames(tmp_path):
    import subprocess
    source=tmp_path/'sparse.mp4'
    subprocess.run(['ffmpeg','-v','error','-y','-f','lavfi','-i','color=c=blue:s=64x64:r=1:d=2','-f','lavfi','-i','anullsrc=r=8000:cl=mono','-t','5','-c:v','libx264','-c:a','aac',str(source)],check=True)
    samples=x.extract_audience_frames(source,tmp_path/'frames',tmp_path)
    assert len(samples)==6
    assert max(s['frame_index'] for s in samples)==1
    assert max(s['timestamp_seconds'] for s in samples)<=1
    assert all((tmp_path/s['path']).stat().st_size>0 for s in samples)


def test_six_actual_indices_decode_once_and_verified_cache_never_rewrites(tmp_path,monkeypatch):
    import subprocess
    from PIL import Image,ImageStat
    inputs=tmp_path/'inputs';inputs.mkdir();source=tmp_path/'sample.mp4'
    for i in range(12):Image.new('RGB',(64,48),(i*20,)*3).save(inputs/f'{i+1:02}.png')
    subprocess.run(['ffmpeg','-v','error','-y','-framerate','12','-i',str(inputs/'%02d.png'),'-frames:v','12','-c:v','libx264','-crf','0','-pix_fmt','yuv420p',str(source)],check=True)
    real_run=subprocess.run;calls=[]
    def traced(cmd,*a,**kw):calls.append(cmd[0]);return real_run(cmd,*a,**kw)
    monkeypatch.setattr(x.subprocess,'run',traced)
    samples=x.extract_audience_frames(source,tmp_path/'frames',tmp_path)
    assert calls.count('ffmpeg')==1 and calls.count('ffprobe')==1
    assert [s['frame_index'] for s in samples]==[0,2,4,7,9,11]
    for s in samples:
        with Image.open(tmp_path/s['path']) as im:mean=ImageStat.Stat(im.convert('RGB')).mean
        assert all(abs(v-s['frame_index']*20)<=3 for v in mean)
        assert abs(s['timestamp_seconds']-s['frame_index']/12)<0.001
    before={s['path']:x.sha(tmp_path/s['path']) for s in samples}
    assert x.extract_audience_frames(source,tmp_path/'frames',tmp_path)==samples
    assert len(calls)==2 and before=={s['path']:x.sha(tmp_path/s['path']) for s in samples}
    (tmp_path/samples[0]['path']).write_bytes(b'changed')
    with pytest.raises(ValueError,match='frame evidence changed'):x.extract_audience_frames(source,tmp_path/'frames',tmp_path)
    assert len(calls)==2


def test_media_download_transport_fallback_and_length_check(tmp_path,monkeypatch):
    import requests
    calls=[]
    class Response:
        status_code=200
        headers={'Content-Length':'4'}
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def raise_for_status(self):pass
        def iter_content(self,chunk_size):yield b'good'
    def get(url,direct=False,**kwargs):
        calls.append(direct)
        if not direct:raise requests.exceptions.ProxyError('fixture proxy failure')
        return Response()
    monkeypatch.setattr(x,'media_get',get)
    p=tmp_path/'image.image';x.download('https://a.xhscdn.com/image',p)
    assert calls==[False,True] and p.read_bytes()==b'good'
    with pytest.raises(ValueError,match='host'):x.download('https://evilxhscdn.com/image',tmp_path/'bad')


def test_incomplete_full_media_does_not_promote_partial(tmp_path,monkeypatch):
    import requests
    class Response:
        status_code=200
        headers={'Content-Length':'99'}
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def raise_for_status(self):pass
        def iter_content(self,chunk_size):yield b'short'
    monkeypatch.setattr(x,'media_get',lambda *a,**k:Response())
    p=tmp_path/'image.image'
    with pytest.raises(requests.RequestException):x.download('https://a.xhscdn.com/image',p)
    assert not p.exists()


def test_incomplete_video_range_retries_without_appending_bad_bytes(tmp_path,monkeypatch):
    calls=[]
    class Response:
        status_code=206
        def __init__(self,header,data):self.headers={'Content-Range':header};self.data=data
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def raise_for_status(self):pass
        def iter_content(self,chunk_size):yield self.data
    def get(url,direct=False,headers=None,**kwargs):
        calls.append((headers['Range'],direct))
        if headers['Range']=='bytes=0-0':return Response('bytes 0-0/4',b'g')
        return Response('bytes 0-3/4',b'good' if direct else b'bad')
    monkeypatch.setattr(x,'media_get',get)
    p=tmp_path/'video.mp4';x.download('https://a.xhscdn.com/video',p)
    assert p.read_bytes()==b'good'
    assert calls==[('bytes=0-0',False),('bytes=0-3',False),('bytes=0-3',True)]


def test_large_stalled_range_falls_back_to_exact_small_ranges(tmp_path,monkeypatch):
    import requests
    payload=b'v'*20000;calls=[]
    class Response:
        status_code=206
        def __init__(self,start,end):self.headers={'Content-Range':f'bytes {start}-{end}/{len(payload)}'};self.data=payload[start:end+1]
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def raise_for_status(self):pass
        def iter_content(self,chunk_size):yield self.data
    def get(url,direct=False,headers=None,**kwargs):
        start,end=map(int,headers['Range'].removeprefix('bytes=').split('-'));calls.append((start,end,direct))
        if end-start+1>8192:raise requests.exceptions.ReadTimeout('large response stalls')
        return Response(start,end)
    monkeypatch.setattr(x,'media_get',get)
    p=tmp_path/'video.mp4';x.download('https://a.xhscdn.com/video',p)
    assert p.read_bytes()==payload
    assert calls[-3:]==[(0,8191,False),(8192,16383,False),(16384,19999,False)]


def test_configured_top100_selects_after_dedupe(tmp_path,monkeypatch):
    run=fixture_run(tmp_path,monkeypatch,[note(str(i),likes=i) for i in range(111)])
    cfg=x.load(x.CONFIG);cfg['ranking']={'rule_version':'likes_top100_v1'};x.save(x.CONFIG,cfg)
    selected=x.select(run)
    assert selected['denominator']==111 and selected['selected']==100
    assert [n['id'] for n in selected['items']]==[str(i) for i in range(110,10,-1)]
