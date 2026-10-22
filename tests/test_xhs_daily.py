import json
from pathlib import Path

import pytest

from ua_workflows import xhs_daily as x


def note(i='a', likes=5, posted='2026-10-19T10:00:00+00:00'):
    return {'id':i,'url':'https://www.xiaohongshu.com/explore/'+i,'title':'样本','text':'摘要',
            'posted_at':posted,'like_count':likes,'collect_count':0,'comment_count':2,'share_count':None,
            'is_video':True,'matched_keywords':['乙女向']}


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


def test_fields_preserve_null_and_do_not_invent_ad_data(tmp_path):
    row=x.row_fields(note(),{'files':[{'width':720,'height':954,'url':'https://sns-video.xhscdn.com/a.mp4'}]},'2026-10-19',tmp_path)
    assert row['分享数'] is None and row['收藏数']==0
    assert row['来源']=='每日最新' and row['平台']=='小红书' and row['素材类型']=='视频'
    assert 'App ID' not in row and 'appid' not in row and '展示估值' not in row and '广大大详情' not in row
    assert row['发布时间']=='2026-10-19T18:00:00+08:00'
    x.verify_row({'分享数':None,'点赞数':'5','视频附件':[{'file_token':'a','size':10}]},{'分享数':None,'点赞数':5,'视频附件':[{'file_token':'a'}]})
    with pytest.raises(ValueError):x.verify_row({'分享数':0},{'分享数':None})
    with pytest.raises(ValueError):x.verify_row({'视频附件':[{'file_token':'a','size':0}]},{'视频附件':[{'file_token':'a'}]})


class MessageClient:
    def __init__(self):self.sent=[]
    def call(self,method,route,**kw):
        if method=='GET':return {'name':'霍振堃','chat_mode':'group'}
        self.sent.append(kw);return {'message_id':'om_test','chat_id':'oc_test'}


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
