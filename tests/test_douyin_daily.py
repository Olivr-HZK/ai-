from pathlib import Path
import json
import subprocess
import pytest
from ua_workflows import douyin_daily as d


def fixture_run(tmp_path):
    cfg={'keywords':['乙女向','女性向']}
    rows=[{'id':str(i),'url':'https://www.douyin.com/video/'+str(i),'posted_at':'2026-10-19T10:00:00+00:00','like_count':i,'media_type':'image','text':'示例'} for i in range(11)]
    tasks=[]
    for i,key in enumerate(cfg['keywords']):
        p=tmp_path/f'raw/{i}';p.mkdir(parents=True)
        d.save(p/'request.json',{'started_at':'2026-10-19T19:00:00+08:00','body':{'platform':'douyin','action':'search','params':{'keyword':key,'publish_time':'one_day','sort':'newest','limit':50}}})
        d.save(p/'response.json',{'platform':'douyin','action':'search','results':rows})
        tasks.append({'keyword':key,'directory':str(p),'returned_notes':11,'status':'search_verified'})
    d.save(tmp_path/'summary.json',{'keywords':tasks,'total_returned':22})
    return cfg


def test_ranking_global_dedup_ceil_and_raw_validation(tmp_path):
    cfg=fixture_run(tmp_path);s=d.select(tmp_path,cfg)
    assert s['denominator']==11 and s['selected']==10
    assert [n['id'] for n in s['items']]==[str(i) for i in range(10,0,-1)]
    assert s['items'][0]['matched_keywords']==cfg['keywords']
    p=tmp_path/'raw/0/response.json';r=d.load(p);r['results'][0]['like_count']=None;d.save(p,r)
    with pytest.raises(ValueError,match='likes'):d.select(tmp_path,cfg)


def test_async_search_requires_matching_successful_job_evidence(tmp_path):
    cfg=fixture_run(tmp_path);folder=tmp_path/'raw/0'
    request=d.load(folder/'request.json');request['body']['mode']='async';d.save(folder/'request.json',request)
    response=d.load(folder/'response.json');response.update(status='succeeded',job_id='job-one');d.save(folder/'response.json',response)
    d.save(folder/'submit-response.json',{'job_id':'job-one','status':'running'})
    assert d.select(tmp_path,cfg)['denominator']==11
    response['job_id']='job-other';d.save(folder/'response.json',response)
    with pytest.raises(ValueError,match='job evidence mismatch'):d.select(tmp_path,cfg)
    response.update(job_id='job-one',status='running');d.save(folder/'response.json',response)
    with pytest.raises(ValueError,match='job evidence mismatch'):d.select(tmp_path,cfg)


@pytest.mark.parametrize('total,expected',[(0,0),(1,1),(7,7),(9,9),(10,10),(11,10),(99,10),(100,10),(101,11),(111,12)])
def test_count_rule_after_valid_window_and_deduplication(tmp_path,total,expected):
    rows=[{'id':str(i),'url':'https://www.douyin.com/video/'+str(i),'posted_at':'2026-10-19T10:00:00+00:00','like_count':i} for i in range(total)]
    keywords=[f'词{i}' for i in range(max(1,(total+49)//50))];cfg={'keywords':keywords};tasks=[]
    for i,key in enumerate(keywords):
        folder=tmp_path/str(i);batch=rows[i*50:(i+1)*50]
        d.save(folder/'request.json',{'started_at':'2026-10-19T19:00:00+08:00','body':{'platform':'douyin','action':'search','params':{'keyword':key,'publish_time':'one_day','sort':'newest','limit':50}}})
        d.save(folder/'response.json',{'platform':'douyin','action':'search','results':batch})
        tasks.append({'keyword':key,'directory':str(folder),'returned_notes':len(batch),'status':'search_verified'})
    d.save(tmp_path/'summary.json',{'keywords':tasks,'total_returned':total})
    result=d.select(tmp_path,cfg)
    assert result['denominator']==total and result['selected']==expected
    assert [n['id'] for n in result['items']]==[str(i) for i in range(total-1,total-expected-1,-1)]
    assert result['selection_mode']==('top_n' if total<100 else 'fraction')


def test_new_pagination_cannot_be_silently_ignored(tmp_path):
    cfg=fixture_run(tmp_path);p=tmp_path/'raw/0/response.json';r=d.load(p);r['next_page_token']='new';d.save(p,r)
    with pytest.raises(ValueError,match='pagination'):d.select(tmp_path,cfg)


def test_audio_not_accepted_as_video(tmp_path,monkeypatch):
    def download(url,path):path.write_bytes(b'audio')
    monkeypatch.setattr(d,'download',download)
    monkeypatch.setattr(d.subprocess,'run',lambda *a,**kw:subprocess.CompletedProcess(a,0,stdout=json.dumps({'streams':[{'codec_type':'audio'}],'format':{'duration':'19'}})))
    assert d.video_file('https://example.com/audio',tmp_path/'media.bin',tmp_path) is None
    assert not (tmp_path/'media.mp4').exists()


def test_actual_video_accepted_despite_reported_image(tmp_path,monkeypatch):
    def download(url,path):path.write_bytes(b'video')
    def run(args,**kw):return subprocess.CompletedProcess(args,0,stdout=json.dumps({'streams':[{'codec_type':'video','width':576,'height':1024}],'format':{'duration':'8.1'}}))
    monkeypatch.setattr(d,'download',download);monkeypatch.setattr(d.subprocess,'run',run)
    result=d.media({'id':'one','media_type':'image','video_url':'https://example.com/a'},tmp_path,{})
    assert result['is_video'] and result['actual_media_type']=='video' and result['reported_media_type']=='image'


def test_fields_mark_douyin_not_advertiser(tmp_path):
    n={'id':'a','url':'https://www.douyin.com/video/a','posted_at':'2026-10-19T10:00:00+00:00','like_count':5,'share_count':None,'matched_keywords':['乙女向']}
    manifest={'is_video':True,'files':[{'width':576,'height':1024,'url':'https://example.com/a.mp4'}]}
    row=d.fields(n,manifest,'2026-10-19',tmp_path)
    assert row['平台']=='抖音' and row['来源']=='每日最新' and row['分享数'] is None
    assert row['唯一键']=='抖音|2026-10-19|a'
    assert 'App ID' not in row and '广告主' not in row and '展示估值' not in row
    d.verify(row,row)
    with pytest.raises(ValueError,match='Platform'):d.verify({**row,'平台':'iOS'},row)


def test_partial_report_scoped_and_deduplicated(tmp_path):
    class Client:
        def __init__(self):self.sent=[]
        def call(self,method,route,**kw):
            if method=='GET':return {'name':'霍振堃','chat_mode':'group'}
            self.sent.append(kw);return {'message_id':'om_1','chat_id':'oc_1'}
    cfg={'keywords':['乙女向'],'feishu':{'base_url':'https://example.com/base','table_id':'t'},'crawl_completion_report':{'chat_name':'霍振堃','chat_id':'oc_1'}}
    d.save(tmp_path/'summary.json',{'cost_usd':0.1,'keywords':[]})
    selection={'raw_rows':64,'denominator':63,'selected':7}
    synced={'status':'partial','video_success':1,'image_success':2,'verified_rows':3,'failures':[{}]*4}
    client=Client();d.report(tmp_path,selection,cfg,client,synced,'2026-10-19');d.report(tmp_path,selection,cfg,client,synced,'2026-10-19')
    assert len(client.sent)==1
    text=client.sent[0]['json']['content'];assert '部分完成' in text and '没有翻页' in text and '不代表全站' in text and '展示估值' not in text


def test_old_posts_excluded_before_likes_and_denom(tmp_path):
    cfg=fixture_run(tmp_path)
    for i in range(2):
        p=tmp_path/f'raw/{i}/response.json';r=d.load(p)
        r['results'][0]['posted_at']='2026-10-03T10:00:00+00:00'
        r['results'][0]['like_count']=None
        d.save(p,r)
    p=tmp_path/'summary.json';r=d.load(p);r['keywords'][0]['status']='response_needs_review';d.save(p,r)
    s=d.select(tmp_path,cfg)
    assert s['denominator']==10 and s['selected']==10
    assert s['excluded_old_hits']==2 and s['excluded_old_unique']==1
    assert len(d.load(tmp_path/'excluded-old-posts.json'))==2


def test_future_timestamp_still_blocks(tmp_path):
    cfg=fixture_run(tmp_path);p=tmp_path/'raw/0/response.json';r=d.load(p)
    r['results'][0]['posted_at']='2026-10-21T10:00:00+00:00';d.save(p,r)
    with pytest.raises(ValueError,match='later than request'):d.select(tmp_path,cfg)


def test_failed_search_download_can_use_cached_original_images(tmp_path,monkeypatch):
    from PIL import Image
    folder=tmp_path/'media'/'n';folder.mkdir(parents=True)
    d.save(folder/'detail-response.json',{'results':{'id':'n','image_urls':['https://p5.douyinpic.com/original']}})
    def download(url,path):
        Image.new('RGB',(20,30),'blue').save(path,format='PNG')
    def video(*a):raise ValueError('Unusable search source')
    monkeypatch.setattr(d,'download',download);monkeypatch.setattr(d,'video_file',video)
    result=d.media({'id':'n','media_type':'image','url':'https://www.douyin.com/video/n','video_url':'https://example.com/search'},tmp_path,{})
    assert not result['is_video'] and len(result['files'])==1
    assert result['files'][0]['width']==20


def test_media_download_direct_fallback(tmp_path,monkeypatch):
 import requests
 from ua_workflows import douyin_daily as d
 calls=[]
 def stalled(*a,**kw):raise requests.ReadTimeout()
 class Response:
  status_code=200
  headers={'Content-Length':'12'}
  content=b'actual-media'
  def close(self):pass
  def __enter__(self):return self
  def __exit__(self,*a):self.close()
  def iter_content(self,chunk_size):yield self.content
  def raise_for_status(self):pass
 class Direct:
  trust_env=True
  def close(self):pass
  def __enter__(self):return self
  def __exit__(self,*args):pass
  def get(self,url,**kw):
   assert self.trust_env is False;calls.append(url);return Response()
 monkeypatch.setattr(requests,'get',stalled);monkeypatch.setattr(requests,'Session',Direct)
 p=tmp_path/'media';d.download('https://api-play.amemv.com/video',p)
 assert p.read_bytes()==b'actual-media' and len(calls)==1
