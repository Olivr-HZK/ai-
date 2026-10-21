import json
import pytest
from ua_workflows import xhs_link_bot as b

S={'allowed_chat_id':'group','max_links_per_message':5}
def event(text,mention='bot',chat='group'):
 return {'sender':{'sender_type':'user'},'message':{'chat_id':chat,'chat_type':'group','message_type':'text','message_id':'om_test','mentions':[{'id':{'open_id':mention}}],'content':json.dumps({'text':text})}}

def test_only_allowlisted_group_and_bot_mention():
 url='https://www.xiaohongshu.com/explore/'+'a'*24+'?xsec_token=abc'
 assert b.links_from_message(event(url),'bot',S)==[url]
 assert b.links_from_message(event(url,chat='other'),'bot',S)==[]
 assert b.links_from_message(event(url,mention='other'),'bot',S)==[]
 assert b.links_from_message(event('https://evil.com/'),'bot',S)==[]

def test_host_validation_and_token_preserved():
 url='https://www.xiaohongshu.com/explore/'+'a'*24+'?xsec_token=abc&xsec_source=pc_search'
 assert b.resolve_url(url)==(url,'a'*24)
 for u in ['https://xiaohongshu.com.evil.com/x','https://evil.com','http://xhslink.com/abc','https://www.xiaohongshu.com@evil.com']:
  assert not b.valid_url(u)

def test_unsafe_shortlink_redirect_not_followed(monkeypatch):
 class Response:
  status_code=302;headers={'Location':'http://127.0.0.1/private'}
  def __enter__(self):return self
  def __exit__(self,*args):pass
 calls=[]
 def get(url,**kw):calls.append(url);return Response()
 monkeypatch.setattr(b.requests,'get',get)
 with pytest.raises(ValueError):b.resolve_url('https://xhslink.com/abc')
 assert len(calls)==1

def test_duplicate_event_enqueues_once(tmp_path):
 e=event('https://www.xiaohongshu.com/explore/'+'a'*24)
 b.enqueue(e,'bot',S,tmp_path);b.enqueue(e,'bot',S,tmp_path)
 assert len(list((tmp_path/'jobs').glob('*.json')))==1

def test_uncertain_supplier_request_never_replays(tmp_path):
 b.x.save(tmp_path/'note-intent.json',{'note_id':'a'})
 with pytest.raises(ValueError,match='不确定'):b.fetch_note('https://www.xiaohongshu.com/explore/'+'a'*24,'a',tmp_path,{'api':{'endpoint':'https://unused'}})

def test_manual_import_uses_own_key_and_verifies_readback(tmp_path,monkeypatch):
 note_id='a'*24;url='https://www.xiaohongshu.com/explore/'+note_id
 note={'id':note_id,'url':url,'is_video':False,'posted_at':'2026-10-19T00:00:00Z','matched_keywords':[],'title':'fixture'}
 manifest={'files':[{'path':'image.png','sha256':'digest','width':10,'height':10,'url':'https://a.xhscdn.com/img'}]}
 (tmp_path/'image.png').write_bytes(b'image')
 monkeypatch.setattr(b,'fetch_note',lambda *args:note)
 monkeypatch.setattr(b.x,'media',lambda *args:manifest)
 monkeypatch.setattr(b.x,'upload_attachment',lambda *args:'token')
 class Client:
  def __init__(self):self.rows=[];self.created=0
  def listing(self,route):return self.rows
  def call(self,method,route,**kw):
   if route.endswith('batch_create'):
    self.created+=1;self.rows=[{'record_id':'record','fields':kw['json']['records'][0]['fields']}]
    self.rows[0]['fields']['图片附件'][0]['size']=5
    return {'records':self.rows}
   if method=='GET':return {'record':self.rows[0]}
   raise AssertionError('Unexpected update/retirement')
 client=Client();config={'feishu':{'app_token':'app','table_id':'table'}}
 assert b.import_link(url,tmp_path,client,config)['status']=='verified'
 assert client.rows[0]['fields']['唯一键']=='小红书|手动|'+note_id
 assert b.import_link(url,tmp_path,client,config)['status']=='already_imported'
 assert client.created==1


def test_douyin_links_and_unsafe_redirect(monkeypatch):
 url='https://www.douyin.com/note/123456789'
 assert b.links_from_message(event(url),'bot',S)==[url]
 assert b.resolve_url(url)==('https://www.douyin.com/video/123456789','123456789')
 class Response:
  status_code=302;headers={'Location':'https://evil.com/video/123'}
  def __enter__(self):return self
  def __exit__(self,*args):pass
 calls=[]
 def get(url,**kw):calls.append(url);return Response()
 monkeypatch.setattr(b.requests,'get',get)
 with pytest.raises(ValueError):b.resolve_url('https://v.douyin.com/abc/')
 assert len(calls)==1


def test_douyin_detail_cached_once_and_identity(tmp_path,monkeypatch):
 calls=[]
 class Response:
  status_code=200;ok=True
  def json(self):return {'platform':'douyin','action':'video','results':{'id':'123','posted_at':'2026-10-19T00:00:00Z','image_urls':['https://example.invalid/img']}}
 monkeypatch.setattr(b,'project_env_values',lambda:{'EVERYINFRA_API_KEY':'fixture'})
 monkeypatch.setattr(b.requests,'post',lambda *a,**k:calls.append(k['json']) or Response())
 c={'api':{'endpoint':'https://unused'}}
 for _ in range(2):assert b.fetch_douyin_note('https://www.douyin.com/video/123','123',tmp_path,c)['id']=='123'
 assert len(calls)==1 and calls[0]['action']=='video'
 folder=tmp_path/'media'/'456';folder.mkdir();b.x.save(folder/'detail-intent.json',{})
 with pytest.raises(ValueError,match='不确定'):b.fetch_douyin_note('https://www.douyin.com/video/456','456',tmp_path,c)


def test_douyin_manual_import_separate_namespace(tmp_path,monkeypatch):
 note={'id':'123','url':'https://www.douyin.com/video/123','posted_at':'2026-10-19T00:00:00Z','matched_keywords':[]}
 manifest={'is_video':False,'files':[{'path':'image.png','sha256':'digest','width':10,'height':10,'url':'https://a.douyinpic.com/img'}]}
 monkeypatch.setattr(b,'fetch_douyin_note',lambda *a:note)
 monkeypatch.setattr(b.d,'media',lambda *a:manifest)
 monkeypatch.setattr(b.x,'upload_attachment',lambda *a:'token')
 class Client:
  def __init__(self):self.rows=[];self.created=0
  def listing(self,r):return self.rows
  def call(self,method,route,**kw):
   if route.endswith('batch_create'):
    self.created+=1;self.rows=[{'record_id':'record','fields':kw['json']['records'][0]['fields']}];self.rows[0]['fields']['图片附件'][0]['size']=5
    return {'records':self.rows}
   return {'record':self.rows[0]}
 c=Client();config={'feishu':{'app_token':'app','table_id':'table'}}
 assert b.import_link(note['url'],tmp_path,c,config)['status']=='verified'
 row=c.rows[0]['fields'];assert row['平台']=='抖音' and row['来源']=='手动增加' and row['唯一键']=='抖音|手动|123'
 assert b.import_link(note['url'],tmp_path,c,config)['status']=='already_imported'
 assert c.created==1


def test_douyin_official_share_redirect(monkeypatch):
 class Response:
  status_code=302;headers={'Location':'https://www.iesdouyin.com/share/video/7695307129971952939/?region=CN'}
  def __enter__(self):return self
  def __exit__(self,*args):pass
 monkeypatch.setattr(b.requests,'get',lambda *a,**kw:Response())
 assert b.resolve_url('https://v.douyin.com/abc/')==('https://www.douyin.com/video/7695307129971952939','7695307129971952939')
 assert not b.valid_url('https://www.iesdouyin.com.evil.com/share/video/123/')


def test_two_users_tasks_and_replies_stay_isolated(tmp_path,monkeypatch):
    import copy
    first=event('https://www.xiaohongshu.com/explore/'+'a'*24)
    second=event('https://www.xiaohongshu.com/explore/'+'b'*24)
    first['sender']['sender_id']={'open_id':'user_a'}
    second['sender']['sender_id']={'open_id':'user_b'}
    first['message']['message_id']='om_userA'
    second['message']['message_id']='om_userB'
    b.enqueue(first,'bot',S,tmp_path);b.enqueue(second,'bot',S,tmp_path)
    replies=[];imports=[]
    class Client:
        def call(self,method,route,**kwargs):
            replies.append((route,copy.deepcopy(kwargs['json'])))
            return {'message_id':'reply_'+str(len(replies))}
    monkeypatch.setattr(b.x,'authorized_client',lambda config:Client())
    def importer(url,*args):
        imports.append(url)
        if url.endswith('a'*24):raise ValueError('fixture failure for first user')
        return {'status':'verified','note_id':'b'*24}
    monkeypatch.setattr(b,'import_link',importer)
    class StopWorker(Exception):pass
    def stop(_):raise StopWorker()
    monkeypatch.setattr(b.time,'sleep',stop)
    with pytest.raises(StopWorker):b.work(tmp_path,S)
    a=b.x.load(tmp_path/'jobs/om_userA.json');z=b.x.load(tmp_path/'jobs/om_userB.json')
    assert a['status']=='blocked' and 'results' not in a
    assert z['status']=='complete' and z['results'][0]['note_id']=='b'*24
    assert imports==['https://www.xiaohongshu.com/explore/'+'a'*24,'https://www.xiaohongshu.com/explore/'+'b'*24]
    assert [r[0] for r in replies]==['im/v1/messages/om_userA/reply','im/v1/messages/om_userB/reply']
    assert '未完成' in json.loads(replies[0][1]['content'])['text']
    assert '完成：1篇' in json.loads(replies[1][1]['content'])['text']
    assert replies[0][1]['uuid']!=replies[1][1]['uuid']


def test_receipt_audit_distinguishes_filtered_messages_without_content(tmp_path):
    e=event('https://www.xiaohongshu.com/explore/'+'a'*24,chat='other')
    b.enqueue(e,'bot',S,tmp_path)
    audit=b.x.load(tmp_path/'received/om_test.json')
    assert audit['reason']=='other_chat' and not audit['allowed_chat']
    assert 'https://' not in json.dumps(audit)
    e=event('hello')
    b.enqueue(e,'bot',S,tmp_path)
    assert b.x.load(tmp_path/'received/om_test.json')['reason']=='no_supported_link'
    assert not (tmp_path/'jobs/om_test.json').exists()


def test_xhslink_cn_shares_and_redirect_validation(monkeypatch):
    url='https://xhslink.cn/o/APQURhVOu0R'
    assert b.links_from_message(event(url),'bot',S)==[url]
    target='https://www.xiaohongshu.com/explore/'+'a'*24+'?xsec_token=fixture'
    class Response:
        status_code=302;headers={'Location':target}
        def __enter__(self):return self
        def __exit__(self,*a):pass
    monkeypatch.setattr(b.requests,'get',lambda *a,**k:Response())
    assert b.resolve_url(url)==(target,'a'*24)
    assert not b.valid_url('https://xhslink.cn.evil.com/o/test')


@pytest.mark.parametrize('text',[
 '帮忙收一下： https://xhslink.cn/o/APQURhVOu0R 复制后打开小红书',
 '链接在这里\n（https://xhslink.cn/o/APQURhVOu0R）。谢谢',
 '[这个帖子](https://xhslink.cn/o/APQURhVOu0R)',
 '看看https://xhslink.cn/o/APQURhVOu0R这个视频',
 '分享：xhslink.cn/o/APQURhVOu0R',
])
def test_extracts_shared_url_from_freeform_copy(text):
 assert b.links_from_message(event(text),'bot',S)==['https://xhslink.cn/o/APQURhVOu0R']


def test_rich_post_html_query_and_deduplication():
 url='https://www.xiaohongshu.com/explore/'+'a'*24+'?xsec_token=Ab%2B12&xsec_source=pc_share'
 e=event('unused');e['message'].update(message_type='post',content=json.dumps({'zh_cn':{'title':'收录','content':[[{'tag':'a','href':url.replace('&','&amp;'),'text':'点这里'},{'tag':'text','text':url}]]}}))
 assert b.links_from_message(e,'bot',S)==[url]


@pytest.mark.parametrize('text',[
 'https://xhslink.cn.evil.com/o/abc', 'https://xhslink.cn@evil.com/o/abc',
 'https://evil.com/xhslink.cn/o/abc', 'evil.xhslink.cn/o/abc',
 'https://xhslink.cn:invalid/o/abc', 'ftp://xhslink.cn/o/abc',
])
def test_freeform_extraction_does_not_bypass_host_restrictions(text):
 assert b.links_from_message(event(text),'bot',S)==[]


def test_douyin_search_modal_and_adjacent_feishu_mention():
    raw='https://www.douyin.com/jingxuan/search/%E4%B9%99%E5%A5%B3%E5%90%91?aid=fixture&modal_id=7684954281401202538&type=general'
    links=b.links_from_message(event(raw+'@@_user_1'),'bot',S)
    assert links==[raw]
    assert b.resolve_url(links[0])==('https://www.douyin.com/video/7684954281401202538','7684954281401202538')
    with pytest.raises(ValueError):b.resolve_url('https://www.douyin.com/search/test?modal_id=not-a-work')
