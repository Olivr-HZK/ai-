import copy
import sys
from contextlib import nullcontext
from pathlib import Path

import pytest

from ua_workflows import xhs_daily as x
from ua_workflows import xhs_link_bot as bot

FIELD='竞品素材（图片/视频/Playable）去除男频'


def config():
    return {'feishu':{'app_token':'app','table_id':'new','base_url':'https://example.invalid/base',
                      'attachment_field_id':'fld-new','attachment_field_name':FIELD}}


def schema(name=FIELD,field_id='fld-new',kind=17):
    return [{'field_name':n,'type':t,'field_id':'id-'+n} for n,t in x.FIELDS if n not in ('视频附件','图片附件')]+[
        {'field_name':name,'field_id':field_id,'type':kind}]


def test_schema_uses_actual_name_by_id_and_never_recreates_old_columns():
    class Client:
        def __init__(self):self.writes=[]
        def listing(self,route):return schema(name='重命名后的素材')
        def call(self,*args,**kwargs):self.writes.append((args,kwargs));raise AssertionError('unexpected schema write')
    c=Client();cfg=config();route=x.provision(c,cfg)
    assert x.attachment_field_name(c,cfg,route)=='重命名后的素材' and not c.writes
    resolved=x.with_resolved_attachment(c,cfg,route)
    assert x.attachment_key({'is_video':True},resolved)=='重命名后的素材'
    assert x.attachment_key({'is_video':False},resolved)=='重命名后的素材'


@pytest.mark.parametrize('field_id,kind',[('wrong-id',17),('fld-new',1)])
def test_missing_or_wrong_type_target_blocks_before_field_creation(field_id,kind):
    class Client:
        def listing(self,route):return schema(field_id=field_id,kind=kind)
        def call(self,*args,**kw):raise AssertionError('must not create old fields')
    with pytest.raises(ValueError,match='attachment'):x.provision(Client(),config())


def test_old_table_cannot_inherit_new_table_field_id(tmp_path):
    c=config();context={'target_date':'2026-10-26'}
    old=x.daily_destination(tmp_path,c,context)
    assert old['feishu']['table_id']=='tblwhAqPfQoBXK3Q' and 'attachment_field_id' not in old['feishu']
    assert x.attachment_key({'is_video':True},old)=='视频附件'
    pinned={**context,'feishu_target':dict(c['feishu'])}
    new=x.daily_destination(tmp_path,c,pinned)
    assert new['feishu']['attachment_field_id']=='fld-new'
    assert x.attachment_key({'is_video':False},new)==FIELD


def test_new_daily_context_pins_unified_field_before_collection(tmp_path,monkeypatch):
    from ua_workflows import social_search
    cfg=config();path=tmp_path/'config.json';x.save(path,cfg)
    monkeypatch.setattr(x,'ROOT',tmp_path);monkeypatch.setattr(x,'CONFIG',path)
    monkeypatch.setattr(x,'collection_phase',lambda *a:nullcontext())
    monkeypatch.setattr(social_search,'collect_xhs',lambda *a,**kw:{'status':'incomplete'})
    monkeypatch.setattr(sys,'argv',['xhs_daily','run'])
    with pytest.raises(ValueError,match='SEARCH_INCOMPLETE'):x.main()
    contexts=list((tmp_path/'data/xhs_daily').glob('*/daily-context.json'))
    assert len(contexts)==1
    pinned=x.load(contexts[0])['feishu_target']
    assert pinned['table_id']=='new' and pinned['attachment_field_id']=='fld-new' and pinned['attachment_field_name']==FIELD


def test_daily_sync_unifies_both_media_and_preserves_other_records(tmp_path,monkeypatch):
    cfg=config();x.save(tmp_path/'run.json',{'started_at':'2026-10-28T00:00:00+08:00'})
    manifests={}
    for name,data in [('v.mp4',b'video'),('p.png',b'image')]:
        p=tmp_path/name;p.write_bytes(data);manifests[name]={'files':[{'path':name,'sha256':x.sha(p),'size':len(data)}]}
    notes=[{'id':'vid','is_video':True},{'id':'img','is_video':False}]
    protected={'record_id':'manual','fields':{'唯一键':'小红书|手动|existing','来源':'手动增加','平台':'小红书',FIELD:[{'file_token':'old','size':9}]}}
    class Client:
        def __init__(self):self.rows=[copy.deepcopy(protected)];self.created=[]
        def listing(self,route):return copy.deepcopy(schema() if route.endswith('/fields') else self.rows)
        def call(self,method,route,**kw):
            assert route.endswith('/batch_create') and method=='POST'
            fields=copy.deepcopy(kw['json']['records'][0]['fields']);self.created.append(copy.deepcopy(fields))
            for a in fields[FIELD]:a['size']=5
            row={'record_id':'r'+str(len(self.created)),'fields':fields};self.rows.append(row)
            return {'records':[row]}
    c=Client();monkeypatch.setattr(x,'authorized_client',lambda *a:c)
    monkeypatch.setattr(x,'eligible_media',lambda n,*a:manifests['v.mp4' if n['is_video'] else 'p.png'])
    monkeypatch.setattr(x,'upload_attachment',lambda client,cfg,path,name:'token-'+name)
    def fields(n,m,day,base):
        return {'唯一键':'小红书|'+day+'|'+n['id'],'笔记ID':n['id'],'平台':'小红书','来源':'每日最新',
                '目标日期':day,'素材类型':'视频' if n['is_video'] else '图片'}
    monkeypatch.setattr(x,'row_fields',fields)
    selection={'items':notes,'selected':2};cfg['audience_content_filter']={'enabled':True}
    _,result=x.sync(tmp_path,selection,cfg,'2026-10-27')
    assert result['status']=='complete' and result['verified_rows']==2
    assert all(FIELD in f and '视频附件' not in f and '图片附件' not in f for f in c.created)
    assert c.rows[0]==protected and x.load(tmp_path/'delivery-validation.json')['verified_attachments']==2
    c.rows[-1]['fields'][FIELD][0]['size']=4
    with pytest.raises(ValueError,match='exact attachment size'):
        x.verify_delivery_rows(tmp_path,selection,cfg,c.rows,x.load(tmp_path/'expected-rows.json'),x.load(tmp_path/'uploads.json'))


@pytest.mark.parametrize('video',[False,True])
def test_manual_import_uses_unified_field_and_retains_existing_attachments(tmp_path,monkeypatch,video):
    ident='a'*24;url='https://www.xiaohongshu.com/explore/'+ident;path=tmp_path/'media.bin';path.write_bytes(b'new media')
    note={'id':ident,'url':url,'is_video':video,'posted_at':'2026-10-19T00:00:00Z','matched_keywords':[]}
    manifest={'files':[{'path':'media.bin','sha256':x.sha(path),'size':path.stat().st_size,'width':10,'height':10,'url':'https://sns-video.xhscdn.com/a'}]}
    monkeypatch.setattr(bot,'fetch_note',lambda *a:note);monkeypatch.setattr(x,'media',lambda *a:manifest)
    monkeypatch.setattr(x,'upload_attachment',lambda *a:'new-token')
    def fields(n,m,day,base):return {'笔记ID':ident,'平台':'小红书','来源':'每日最新','素材类型':'视频' if video else '图片','目标日期':day}
    monkeypatch.setattr(x,'row_fields',fields)
    old={'record_id':'record','fields':{'唯一键':'小红书|手动|'+ident,'平台':'小红书','来源':'手动增加',
        FIELD:[{'file_token':'old-token','size':9}],'图片附件':[{'file_token':'legacy','size':7}]}}
    class Client:
        def __init__(self):self.row=copy.deepcopy(old);self.puts=0
        def listing(self,route):return schema() if route.endswith('/fields') else [copy.deepcopy(self.row)]
        def call(self,method,route,**kw):
            if method=='PUT':
                self.puts+=1;fields=copy.deepcopy(kw['json']['fields'])
                for a in fields[FIELD]:a['size']=9
                self.row['fields'].update(fields);return {}
            assert method=='GET';return {'record':copy.deepcopy(self.row)}
    c=Client();assert bot.import_link(url,tmp_path,c,config())['status']=='verified'
    assert [a['file_token'] for a in c.row['fields'][FIELD]]==['old-token','new-token']
    assert c.row['fields']['图片附件']==old['fields']['图片附件']
    assert bot.import_link(url,tmp_path,c,config())['status']=='already_imported' and c.puts==1


def test_manual_import_rejects_changed_file_before_any_upload_or_record_write(tmp_path,monkeypatch):
    ident='a'*24;url='https://www.xiaohongshu.com/explore/'+ident;path=tmp_path/'media.bin';path.write_bytes(b'changed')
    note={'id':ident,'url':url,'is_video':False,'posted_at':'2026-10-19T00:00:00Z','matched_keywords':[]}
    manifest={'files':[{'path':'media.bin','sha256':'not-the-real-hash','size':7,'width':10,'height':10,'url':'https://sns-img.xhscdn.com/a'}]}
    monkeypatch.setattr(bot,'fetch_note',lambda *a:note);monkeypatch.setattr(x,'media',lambda *a:manifest)
    monkeypatch.setattr(x,'upload_attachment',lambda *a:pytest.fail('must not upload changed content'))
    monkeypatch.setattr(x,'row_fields',lambda *a:{})
    class Client:
        def listing(self,route):return schema() if route.endswith('/fields') else []
        def call(self,*args,**kwargs):pytest.fail('must not write changed content')
    with pytest.raises(ValueError,match='local media changed'):bot.import_link(url,tmp_path,Client(),config())
