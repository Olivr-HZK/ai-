import copy
import hashlib
import json
from pathlib import Path
import subprocess
import shutil

import pytest
from PIL import Image
from ua_workflows import social_chat_browser_daily as daily
from ua_workflows import social_chat_daily_state as state
from ua_workflows import social_chat_direct as direct
from ua_workflows import social_chat_direct_media as media
from ua_workflows import social_chat_mixed_window as window
from ua_workflows import social_chat_mixed_delivery as delivery
from ua_workflows import social_chat_crawl_report as report


@pytest.fixture(scope="module")
def collected_base(tmp_path_factory):
    tmp_path=tmp_path_factory.mktemp("direct-e2e")
    cfg=daily.load(daily.CONFIG);cfg['collection_mode']=direct.MODE
    daily.prepare('2026-10-21',tmp_path,cfg)
    Image.new('RGB',(16,24),'blue').save(tmp_path/'fixture.png')
    daily.save(tmp_path/'crawl-notice-start.json',dict(message_id='fixture-message',chat_id=cfg['crawl_start_notice']['chat_id'],mention_all=True,status='verified_by_send_response'))
    state.begin(tmp_path);state.finish(tmp_path)
    scope=daily.load(tmp_path/'scope.json')
    state.begin(tmp_path)
    daily.save(tmp_path/'browser.json',dict(scope_sha256=state.fingerprint(scope),authenticated=True,browser_session='isolated-ui-fixture',observed_at=state.now(),images={'current':{'path':'fixture.png','sha256':window.sha(tmp_path/'fixture.png')}}))
    state.finish(tmp_path,proof='browser.json')
    r=subprocess.run(['node','tests/direct_session_fixture.mjs',str(tmp_path),str(daily.ROOT)],cwd=daily.ROOT,capture_output=True,text=True,timeout=60)
    assert r.returncode==0,r.stderr+r.stdout
    assert json.loads(r.stdout)['searches']==22
    assert state.status(tmp_path)['state']=='RELAUNCH_PENDING'
    return tmp_path,cfg


@pytest.fixture
def collected(tmp_path, collected_base):
    original,cfg=collected_base
    shutil.copytree(original,tmp_path,dirs_exist_ok=True)
    return tmp_path,cfg


def test_direct_session_accepts_all_apps_once_and_preserves_downstream(collected):
    base,cfg=collected
    _,scope,fp,cards,seals=window.collection(base,cfg)
    assert len(seals)==21 and len(cards)==20
    assert next(t for t in scope['tasks'] if t['id']=='ios-03')['category']=='游戏'
    assert len([a for a in state.load(base)['attempts'] if a['state']=='APP_COLLECTION_PENDING' and a['outcome']=='passed'])==21
    assert state.load(base)['resumes'][0]['from']=='WAITING_HUMAN'
    assert len(state.plan(scope))==31  # prepare + two globals + 21 Apps + seven post-processing states + complete cursor
    with pytest.raises(ValueError):direct.accept(base,'ios-01')
    with pytest.raises((ValueError,FileNotFoundError)):window.select(base,cfg)
    assert not (base/'selection.json').exists()


def test_direct_validation_rejects_tampering_and_wrong_identity(collected):
    base,cfg=collected;scope=daily.load(base/'scope.json');task=scope['tasks'][0];root=base/'tasks'/task['id']
    seal=daily.load(root/'direct/complete.json');cardpath=root/seal['cards'][0]['path'];original=cardpath.read_bytes();c=json.loads(original)
    c['app_id']='wrong';daily.save(cardpath,c)
    with pytest.raises(ValueError,match='hash mismatch'):direct.validate_task(base,scope,task)
    seal['cards'][0]['sha256']=window.sha(cardpath);daily.save(root/'direct/complete.json',seal)
    with pytest.raises(ValueError,match='identity mismatch'):direct.validate_task(base,scope,task)
    cardpath.write_bytes(original);seal['cards'][0]['sha256']=window.sha(cardpath);daily.save(root/'direct/complete.json',seal)
    image=root/json.loads(original)['evidence']['after']['path'];image.write_bytes(b'changed')
    with pytest.raises(ValueError,match='hash mismatch'):direct.validate_task(base,scope,task)


def test_direct_seal_refuses_missing_positions_and_false_zero(collected):
    base,cfg=collected;scope=daily.load(base/'scope.json');task=scope['tasks'][1];root=base/'tasks'/task['id'];seal=daily.load(root/'direct/complete.json')
    cpath=root/seal['cards'][0]['path'];c=daily.load(cpath);c['position']=3;daily.save(cpath,c);seal['cards'][0]['sha256']=window.sha(cpath);daily.save(root/'direct/complete.json',seal)
    with pytest.raises(ValueError,match='position'):direct.validate_task(base,scope,task)


def test_media_retry_budget_hash_cache_queue_lock_and_review_bridge(collected,monkeypatch):
    base,cfg=collected
    video=base/'fixture.mp4'
    subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','color=c=blue:s=32x48:r=25','-t','1','-pix_fmt','yuv420p',str(video)],check=True,capture_output=True)
    calls=[]
    def fetch(url,out):
        calls.append(url);out.write_bytes(video.read_bytes() if url.endswith('.mp4') else (base/'fixture.png').read_bytes())
    monkeypatch.setattr(media,'transfer',fetch)
    with media.queue_lock(base):
        with pytest.raises(ValueError,match='already active'):
            with media.queue_lock(base):pass
    result=media.prefetch(base);assert result['verified']==19  # 20 target cards minus the playable
    before=len(calls);assert media.prefetch(base)==result;assert len(calls)==before
    state.begin(base);state.finish(base)
    prepared=media.review_media(base);assert prepared['review_decisions_created']==0 and prepared['prepared']==18  # one relaunch excluded
    state.begin(base);state.finish(base);assert state.status(base)['state']=='REVIEW_PENDING'
    with pytest.raises((ValueError,FileNotFoundError)):window.select(base,cfg)
    board=base/'fixture.png';observations=[]
    for task in daily.load(base/'scope.json')['tasks']:
        for f in (base/'tasks'/task['id']/'cards').glob('*.json'):
            c=daily.load(f)
            if c['first_discovered_date']!='2026-10-21' or c['media_type']=='试玩' or daily.is_relaunch(c):continue
            root=f.parent.parent;kind=c['media_type'];manifest=root/f'evidence/{"images" if kind=="图片" else "frames"}/{c["ad_id"]}/manifest.json';m=daily.load(manifest)
            observations.append(dict(task=task['id'],ad_id=c['ad_id'],reason='Isolated fixture review',decision='keep',viewed_board='fixture.png',viewed_board_sha256=window.sha(board),media_manifest_sha256=window.sha(manifest),method='original_image' if kind=='图片' else 'sampled_frames',images_inspected=True,image_sha256s=[i['sha256'] for i in m.get('images',[])],frames_inspected=True,checks={'real_person_talking_head':False,'real_person_presence':'absent','primary_subject':'male'},review_policy='male-character-no-real-person-v1'))
    # Exercise the actual mixed selector with an image and a non-speaking video.
    changed=copy.deepcopy(observations)
    for method in ('original_image','sampled_frames'):
        row=next(r for r in changed if r['method']==method)
        row['decision']='exclude';row['checks']['real_person_presence']='present'
    daily.save(base/'review_observations.json',changed)
    filtered=window.select(base,cfg,persist=False)
    assert filtered['counts']['eligible']==16 and filtered['counts']['exclude']==2
    assert filtered['counts']['selected']==2
    female=copy.deepcopy(observations)
    for method in ('original_image','sampled_frames'):
        row=next(r for r in female if r['method']==method)
        row['decision']='exclude';row['checks']['primary_subject']='female'
    daily.save(base/'review_observations.json',female)
    assert window.select(base,cfg,persist=False)['counts']['eligible']==16
    female[0]['checks']['primary_subject']='mixed';female[0]['decision']='keep'
    daily.save(base/'review_observations.json',female)
    with pytest.raises(ValueError,match='subject'):
        window.select(base,cfg,persist=False)
    uncertain=copy.deepcopy(changed);uncertain[0]['checks']['real_person_presence']='uncertain'
    daily.save(base/'review_observations.json',uncertain)
    with pytest.raises(ValueError,match='uncertain'):
        window.select(base,cfg,persist=False)
    daily.save(base/'review_observations.json',observations);state.begin(base);state.finish(base);state.begin(base);state.finish(base)
    selected=daily.load(base/'selection.json');assert selected['counts']['eligible']==18 and selected['counts']['selected']==2
    with state.tracked(base,'DELIVERY_PENDING'):result=delivery.sync(base,cfg,apply=False)
    assert result['status']=='prepared' and state.status(base)['state']=='SYNC_PENDING'
    assert {r['来源'] for r in daily.load(base/'feishu-pending.json')['records']}=={'每日最新'}
    assert all(r['投放地区']==['日本','韩国'] and r['投放渠道']==['Facebook'] for r in daily.load(base/'feishu-pending.json')['records'])

    class MockFeishu:
        def __init__(self):
            self.rows=[{'record_id':'manual','fields':{'唯一键':'manual-fixture','平台':'广大大','来源':'手动增加','目标日期':'2026-10-21','当前入选':True}}]
            self.tokens={};self.sends=0
        def listing(self,route):
            if route.endswith('/fields'):return [{'field_name':n,'type':k} for n,k in daily.FIELDS+delivery.EXTRA_FIELDS]
            rows=copy.deepcopy(self.rows)
            for row in rows:
                for field in ('视频附件','图片附件'):
                    for item in row['fields'].get(field,[]):item['size']=self.tokens[item['file_token']]
            return rows
        def call(self,method,route,**kw):
            if method=='GET':return {'name':cfg['crawl_completion_report']['chat_name'],'chat_mode':'group'}
            if route=='im/v1/messages':
                self.sends+=1;return {'message_id':'fixture-report','chat_id':cfg['crawl_completion_report']['chat_id']}
            for row in kw['json']['records']:
                if route.endswith('/batch_create'):self.rows.append({'record_id':'fixture-'+str(len(self.rows)),'fields':copy.deepcopy(row['fields'])})
                elif route.endswith('/batch_update'):next(r for r in self.rows if r['record_id']==row['record_id'])['fields'].update(copy.deepcopy(row['fields']))
            return {}
    client=MockFeishu()
    def upload(mock,cfg,path,name):
        token=window.sha(path);mock.tokens[token]=path.stat().st_size;return token
    monkeypatch.setattr(delivery,'upload',upload)
    with state.tracked(base,'SYNC_PENDING'):synced=delivery.sync(base,cfg,client=client,apply=True)
    assert synced['verified_active']==2 and state.status(base)['state']=='REPORT_PENDING'
    again=delivery.sync(base,cfg,client=client,apply=True)
    assert again['created']==0 and len(client.rows)==3 and client.rows[0]['fields']['当前入选'] is True
    with state.tracked(base,'REPORT_PENDING'):sent=report.report(base,cfg,client)
    assert sent['stats']['selected']==2 and state.status(base)['state']=='COMPLETED'
    assert report.report(base,cfg,client)['message_id']==sent['message_id'] and client.sends==1


def test_failed_resources_are_not_retried_by_restarting_queue(tmp_path,monkeypatch):
    c={'task_id':'ios-01','ad_id':'a'*32,'media_type':'图片'};calls=[]
    def fail(url,p):calls.append(url);raise ValueError('HTTP 404')
    monkeypatch.setattr(media,'transfer',fail)
    first=media.download(tmp_path,c,1,'http','https://example.org/image.png')
    assert first['status']=='failed' and len(calls)==2
    assert media.download(tmp_path,c,1,'http','https://example.org/image.png')==first and len(calls)==2


def test_youtube_channel_and_truncated_links_never_download_a_playlist(tmp_path,monkeypatch):
    c={'task_id':'ios-01','ad_id':'b'*32,'media_type':'视频','observed_videos':[],'links':[{'href':'https://www.youtube.com/@fixture-channel'}]}
    assert media.source(c)[0][0]=='invalid_youtube'
    monkeypatch.setattr(media,'transfer',lambda *args:pytest.fail('Invalid YouTube must not make HTTP request'))
    result=media.download(tmp_path,c,1,*media.source(c)[0]);assert result['status']=='failed'
    c['links']=[{'href':'https://www.youtube.com/watch?v=abc'}];assert media.source(c)[0][0]=='invalid_youtube'
    c['links']=[{'href':'https://www.youtube.com/watch?v=abcdefghijk'}];assert media.source(c)==[('youtube',c['links'][0]['href'])]
