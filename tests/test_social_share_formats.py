"""Share-copy contract tests. External redirects/API/Feishu use explicit fixtures."""
import json
from types import SimpleNamespace

import pytest

from ua_workflows import xhs_link_bot as b

XID='6acf2eb900000000140397fd'
DID='7684954281401202538'
TOKEN='Ab%2B12_-=&xsec_source=pc_share'
XURL=f'https://www.xiaohongshu.com/explore/{XID}?xsec_token={TOKEN}'
DURL=f'https://www.douyin.com/video/{DID}'
SETTING={'allowed_chat_id':'production','max_links_per_message':5}
FORMATS=[
 ('xhs_cn_o','https://xhslink.cn/o/APQURhVOu0R',XID),
 ('xhs_cn_a','https://xhslink.cn/a/FixtureShare',XID),
 ('xhs_com_a','https://xhslink.com/a/FixtureShare',XID),
 ('xhs_com_short','https://xhslink.com/FixtureShare',XID),
 ('xhs_explore',XURL,XID),
 ('xhs_discovery',f'https://www.xiaohongshu.com/discovery/item/{XID}?xsec_token={TOKEN}',XID),
 ('douyin_short','https://v.douyin.com/FixtureShare/',DID),
 ('douyin_video',DURL,DID),
 ('douyin_note',f'https://www.douyin.com/note/{DID}',DID),
 ('douyin_share_video',f'https://www.iesdouyin.com/share/video/{DID}/?region=CN',DID),
 ('douyin_share_note',f'https://www.iesdouyin.com/share/note/{DID}/',DID),
 ('douyin_search_modal',f'https://www.douyin.com/jingxuan/search/%E4%B9%99%E5%A5%B3%E5%90%91?modal_id={DID}&type=general',DID),
 ('douyin_discover_modal',f'https://www.douyin.com/discover?modal_id={DID}',DID),
 ('douyin_profile_modal',f'https://www.douyin.com/user/FixtureAuthor?modal_id={DID}',DID),
]
COPIES=[
 ('xhs_copy',lambda u:f'69 作者发布了一篇小红书笔记，快来看吧！ 😆 AbCd 😆 {u}，复制本条信息，打开【小红书】App查看精彩内容！'),
 ('douyin_copy',lambda u:f'7.89 AbC:/ X@Y.Z 10/20 #乙女向 #数字人 分享作品！ {u} 复制此链接，打开Dou音搜索，直接观看视频！'),
 ('lines',lambda u:f'@_user_1\n帮我收录一下\n{u}\n谢谢'),
 ('chinese',lambda u:f'看看{u}这个视频'),
 ('brackets',lambda u:f'（{u}）。'),
 ('markdown',lambda u:f'[打开作品]({u})'),
 ('adjacent_at',lambda u:f'{u}@@_user_1'),
 ('ascii_label',lambda u:f'URL:{u}'),
 ('angle_brackets',lambda u:f'链接：<{u}>'),
 ('html',lambda u:f'<a href="{u.replace("&","&amp;")}">查看作品</a>'),
]

def event(text,message_type='text'):
 content={'text':text} if message_type=='text' else {'zh_cn':{'title':'作品推荐','content':[[{'tag':'a','href':text,'text':'点这里'},{'tag':'at','user_id':'bot'}]]}}
 return {'sender':{'sender_type':'user'},'message':{'message_id':'om_fixture','chat_id':'production','chat_type':'group','message_type':message_type,'mentions':[{'id':{'open_id':'bot'}}],'content':json.dumps(content,ensure_ascii=False)}}


def redirects(monkeypatch):
 calls=[]
 class Response:
  status_code=302
  def __init__(self,url):self.headers={'Location':DURL if 'douyin.com' in url else XURL}
  def __enter__(self):return self
  def __exit__(self,*a):pass
 def get(url,**kwargs):
  assert kwargs['allow_redirects'] is False
  calls.append(url);return Response(url)
 monkeypatch.setattr(b.requests,'get',get)
 return calls


@pytest.mark.parametrize('name,url,note_id',FORMATS,ids=[f[0] for f in FORMATS])
@pytest.mark.parametrize('copy_name,make_copy',COPIES,ids=[c[0] for c in COPIES])
def test_whole_share_copy_extracts_and_resolves(monkeypatch,name,url,note_id,copy_name,make_copy):
 redirects(monkeypatch)
 links=b.links_from_message(event(make_copy(url)),'bot',SETTING)
 assert links==[url]
 resolved,actual=b.resolve_url(links[0]);assert actual==note_id
 if note_id==DID:assert resolved==DURL
 else:assert 'xsec_token='+TOKEN in resolved


@pytest.mark.parametrize('name,url,note_id',FORMATS,ids=[f[0] for f in FORMATS])
@pytest.mark.parametrize('kind',['post','http_copy','no_scheme'])
def test_rich_text_and_legacy_share_protocols(monkeypatch,name,url,note_id,kind):
 redirects(monkeypatch)
 raw=url if kind=='post' else url.replace('https://','http://',1) if kind=='http_copy' else url.removeprefix('https://')
 links=b.links_from_message(event(raw,'post' if kind=='post' else 'text'),'bot',SETTING)
 assert links==[url]  # HTTP upgraded before any network request; token not decoded.
 assert b.resolve_url(links[0])[1]==note_id


@pytest.mark.parametrize('name,url,note_id',FORMATS,ids=[f[0] for f in FORMATS])
def test_copy_to_detail_media_bitable_readback_and_duplicate(monkeypatch,tmp_path,name,url,note_id):
 redirects(monkeypatch);api_calls=[];uploads=[]
 def post(endpoint,**kwargs):
  body=kwargs['json'];api_calls.append(body)
  assert endpoint=='https://supplier.invalid/social'
  assert body['params']['url']==(DURL if note_id==DID else b.resolve_url(url)[0])
  platform='douyin' if note_id==DID else 'xiaohongshu'
  note={'id':note_id,'title':'fixture title','text':'fixture copy','posted_at':'2026-10-20T09:00:00+08:00','is_video':False,'media_type':'image','images':['https://fixture.invalid/image'],'image_urls':['https://fixture.invalid/image'],'like_count':10,'share_count':None}
  return SimpleNamespace(status_code=200,ok=True,json=lambda:{'platform':platform,'action':body['action'],'results':note})
 monkeypatch.setattr(b.requests,'post',post)
 monkeypatch.setattr(b,'project_env_values',lambda:{'EVERYINFRA_API_KEY':'test-only'})
 def media(note,root,config):
  path=root/'fixture.png';path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'fixture')
  return {'is_video':False,'files':[{'path':'fixture.png','sha256':b.x.sha(path),'size':7,'width':10,'height':10,'url':'https://fixture.invalid/image'}]}
 monkeypatch.setattr(b.x,'media',media);monkeypatch.setattr(b.d,'media',media)
 def upload(*args):uploads.append(1);return 'file-token'
 monkeypatch.setattr(b.x,'upload_attachment',upload)
 class Client:
  def __init__(self):self.rows=[];self.creates=0
  def listing(self,route):return self.rows
  def call(self,method,route,**kwargs):
   if method=='POST' and route.endswith('/batch_create'):
    self.creates+=1
    self.rows=[{'record_id':'record-fixture','fields':kwargs['json']['records'][0]['fields']}]
    self.rows[0]['fields']['图片附件'][0]['size']=7
    return {'records':self.rows}
   if method=='GET':return {'record':self.rows[0]}
   raise AssertionError('Unexpected mutation')
 c=Client();config={'api':{'endpoint':'https://supplier.invalid/social'},'feishu':{'app_token':'base','table_id':'table'}}
 links=b.links_from_message(event(COPIES[1][1](url)),'bot',SETTING)
 assert b.import_link(links[0],tmp_path,c,config)['status']=='verified'
 assert b.import_link(links[0],tmp_path,c,config)['status']=='already_imported'
 assert len(api_calls)==len(uploads)==c.creates==1
 row=c.rows[0]['fields'];platform='抖音' if note_id==DID else '小红书'
 assert row['平台']==platform and row['来源']=='手动增加'
 assert row['笔记ID']==note_id and row['唯一键']==platform+'|手动|'+note_id
 assert row['分享数'] is None and row['图片附件'][0]['file_token']=='file-token'


def test_two_platforms_multiple_links_punctuation_duplicates_and_limit():
 assert b.extract_links([f'{XURL},{DURL};{XURL}'])==[XURL,DURL]
 assert b.extract_links([f'{DURL}，{XURL}'])==[DURL,XURL]
 msg=event(' '.join(f'https://www.douyin.com/video/{100000+i}' for i in range(6)))
 with pytest.raises(ValueError,match='最多提交5'):b.links_from_message(msg,'bot',SETTING)


@pytest.mark.parametrize('raw',[
 'https://xhslink.cn.evil.invalid/o/a',
 'https://xhslink.cn@evil.invalid/o/a',
 'https://evil.invalid/?url=https://xhslink.cn/o/a',
 'https://v.douyin.com:444/a/',
 'https://v.douyin.com:broken/a/',
 'http://127.0.0.1/private',
 'https://v.douyin.com.evil.invalid/a',
 'javascript:alert(1)',
])
def test_attack_copy_never_passes_domain_filter(raw):
 assert b.links_from_message(event('请帮我收录：'+raw),'bot',SETTING)==[]


@pytest.mark.parametrize('start',['https://xhslink.cn/o/a','https://v.douyin.com/a/'])
@pytest.mark.parametrize('dest',['http://127.0.0.1/private','https://evil.invalid/video/123','https://xhslink.cn.evil.invalid/o/a'])
def test_redirect_cannot_escape_white_list(monkeypatch,start,dest):
 calls=[]
 class Response:
  status_code=302;headers={'Location':dest}
  def __enter__(self):return self
  def __exit__(self,*a):pass
 def get(url,**kwargs):calls.append(url);return Response()
 monkeypatch.setattr(b.requests,'get',get)
 with pytest.raises(ValueError):b.resolve_url(start)
 assert calls==[start]


@pytest.mark.parametrize('url',[
 'https://www.xiaohongshu.com/user/profile/fixture',
 'https://www.douyin.com/user/fixture',
 'https://www.douyin.com/search/test',
 'https://www.douyin.com/search/test?modal_id=abc',
 'https://www.douyin.com/search/test?modal_id=123&modal_id=456',
])
def test_no_actual_note_identity_no_detail_charge(monkeypatch,tmp_path,url):
 def no_network(*a,**k):raise AssertionError('Should reject before supplier or website calls')
 monkeypatch.setattr(b.requests,'post',no_network);monkeypatch.setattr(b.requests,'get',no_network)
 with pytest.raises(ValueError):b.import_link(url,tmp_path,None,{})
