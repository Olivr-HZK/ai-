"""Offline state invariants; synthetic records do not claim a real browser crawl."""
import hashlib
import importlib.util
import json
import subprocess
import sys
import struct
import zlib
from datetime import datetime, timezone
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1]/'skills/guangdada-browser-crawl/scripts/state.py'
spec = importlib.util.spec_from_file_location('browser_skill_state', SCRIPT)
fsm = importlib.util.module_from_spec(spec);spec.loader.exec_module(fsm)


class StateTests(unittest.TestCase):
    def test_observed_name_alias_requires_explicit_exact_scoped_app_id(self):
        task = {'app_name': 'Store Full Name', 'platform': 'Android', 'app_id': 'io.example'}
        observed = {'app_name': 'Localized Site Full Name', 'canonical_app_name': 'Store Full Name',
                    'platform': 'Android', 'app_id': 'io.example'}
        fsm.identity(observed, task)
        for changed in [dict(observed, app_id='io.other'),
                        dict(observed, platform='iOS'),
                        dict(observed, canonical_app_name='Other Store Name'),
                        dict(observed, app_name='')]:
            with self.assertRaises(Exception):
                fsm.identity(changed, task)
        with self.assertRaises(Exception):
            fsm.identity(observed, {'app_name':'Store Full Name','platform':'Android'})

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.run = Path(self.tmp.name)/'run';self.run.mkdir()
        self.scope = {'filters': {'category':'工具','dedupe':'素材','sort':'展示估值','date':{'mode':'preset','preset':'30天'},'channels': ['Facebook'], 'regions': ['日本'], 'languages': [], 'aspect_ratio':'9:16'}, 'content_checks': {'male_character': True, 'real_person_talking_head':False}, 'tasks':[{'id':'app-ios','app_name':'Full App Name','platform':'iOS','app_id':'123'}]}
        self.fp = fsm.digest(self.scope);self.root = self.run/'tasks/app-ios';self.root.mkdir(parents=True)
        fsm.write(self.run/'scope.json', self.scope);fsm.write(self.run/'run.json', {'scope_sha256':self.fp})
        (self.run/'ui.txt').write_text('synthetic UI fixture, not production evidence')
        (self.root/'ui.txt').write_text('synthetic UI fixture, not production evidence')
        # Valid synthetic PNG; actual browser execution is covered separately by helper tests.
        def chunk(tag, data):
            return struct.pack('>I', len(data))+tag+data+struct.pack('>I', zlib.crc32(tag+data)&0xffffffff)
        self.png = b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR', struct.pack('>IIBBBBB', 1,1,8,2,0,0,0))+chunk(b'IDAT', zlib.compress(b'\x00\xff\x00\x00'))+chunk(b'IEND', b'')
        (self.root/'ui.png').write_bytes(self.png)
        (self.root/'ui-before.png').write_bytes(self.png)
        (self.root/'ui-after.png').write_bytes(self.png)
        self.put(self.run, 'browser.json', {'authenticated':True,'browser_session':'fixture'})
        self.put(self.root, 'filters.json', {'selected':self.scope['filters']})
        self.app = {'query_full_name':'Full App Name','app_name':'Full App Name','platform':'iOS','app_id':'123','selected_app_ids':['123'],'residual_keywords':[],'card_count':1,'count_basis':'素材'}
        self.put(self.root, 'app.json', self.app)
        self.card = {'page':1,'position':1,'detail_loaded':True,'ad_id':'ad1','app_name':'Full App Name','platform':'iOS','app_id':'123','detail_url':'https://example.test/detail/ad1','video_url':'https://example.test/ad1.mp4','channel':'FB News Feed','regions':['日本'],'language':'英语'}
        self.put(self.root, 'cards/0001-0001.json', self.card)
        self.put(self.root, 'probe.json', {'app_name':'Full App Name','platform':'iOS','app_id':'123','detail_loaded':True,'card_file':'cards/0001-0001.json'})
        self.page = {'page':1,'visible_count':1,'cards':['cards/0001-0001.json'],'has_next':False,'terminal_reason':'next_disabled'}
        self.put(self.root, 'pages/0001.json', self.page)
        self.review = {'ad_id':'ad1','decision':'keep','reason':'synthetic fixture','method':'sampled_frames','checks':{'male_character':True,'real_person_talking_head':False},'evidence':[f'evidence/frames/ad1/frame-{i:02d}.png' for i in range(1,7)]}
        self.put(self.root, 'reviews.json', {'items':[self.review]})
        (self.root/'delivery').mkdir();(self.root/'delivery/a.mp4').write_bytes(b'synthetic video fixture')
        (self.root/'delivery/index.html').write_text('<p>fixture</p>')
        self.download = {'ad_id':'ad1','path':'delivery/a.mp4','sha256':hashlib.sha256(b'synthetic video fixture').hexdigest(),'width':720,'height':1280,'duration_seconds':2,'decode_verified':True}
        self.put(self.root, 'downloads.json', {'items':[self.download]})
        self.put(self.root, 'frames.json', {'items':[{'ad_id':'ad1','manifest':'evidence/frames/ad1/manifest.json'}]})
        frame_dir=self.root/'evidence/frames/ad1';frame_dir.mkdir(parents=True,exist_ok=True)
        frames=[]
        for i,fraction in enumerate([0.03,0.15,0.3,0.5,0.7,0.9],1):
            image=frame_dir/f'frame-{i:02d}.png';image.write_bytes(self.png)
            frames.append({'path':str(image.relative_to(self.root)),'sha256':hashlib.sha256(self.png).hexdigest(),'width':1,'height':1,'fraction':fraction,'requested_timestamp_seconds':2*fraction})
        self.put(self.root, 'evidence/frames/ad1/manifest.json', {'producer':'gdd-frames/v1','ad_id':'ad1','source':{'path':'delivery/a.mp4','sha256':self.download['sha256'],'duration_seconds':2,'decode_verified':True},'frames':frames})
        self.put(self.root, 'delivery.json', {'included_ad_ids':['ad1'],'preview_verified':True,'portable_links_verified':True,'files':['delivery/index.html','delivery/a.mp4']})

    def event(self, key, expected):
        (self.root/'evidence/clicks').mkdir(parents=True,exist_ok=True)
        path='evidence/clicks/'+key.replace('.','-')+'-'+str(len(list((self.root/'evidence/clicks').glob('*.json'))))+'.json'
        event={'scope_sha256':self.fp,'producer':'gdd-checked-click/v1','task_id':'app-ios','event_id':Path(path).stem,'key':key,'kind':'click','performed_at':datetime.now(timezone.utc).isoformat(),'status':'passed','expected':expected,'after':expected,'before':None,'images':{stage:{'path':'ui-'+stage+'.png','sha256':hashlib.sha256(self.png).hexdigest()} for stage in ['before','after']},'error':None}
        fsm.write(self.root/path,event)
        return path

    def put(self, root, name, data):
        data=dict(data)
        if name in ['filters.json','resume-verification.json']:
            selected=dict(data.get('selected',{}))
            if name=='filters.json':selected.pop('sort',None)
            data['selected']=selected
            keys=sorted(selected,key=lambda k:(k!='category',k))
            data.setdefault('ui_checks',{key:self.event('filter.'+key,selected[key]) for key in keys})
        if name in ['app.json','resume-verification.json']:
            data.setdefault('ui_check',self.event('app',{k:data[k] for k in ['app_name','platform','app_id','selected_app_ids','residual_keywords']}))
        if name=='sort.json':
            data.setdefault('ui_check',self.event('filter.sort',data['selected']))
        if name.startswith('cards/'):
            data.setdefault('ui_check',self.event('card.open',{k:data[k] for k in ['ad_id','app_id','platform']}))
        if name.startswith('pages/') and data.get('page',0)>1:
            data.setdefault('ui_check',self.event('page.open',{'page':data['page']}))
        if name=='reviews.json':
            data['items']=[{'frames_inspected':True,**r} for r in data['items']]
        fsm.write(root/name, {'scope_sha256':self.fp,'evidence':['ui.txt'],**data})
        # Simulate sequential setup and a durable per-app checklist in this synthetic fixture.
        if name=='filters.json' and (root/'app.json').exists():
            existing=json.loads((root/'app.json').read_text());existing.pop('ui_check',None)
            self.put(root,'app.json',existing)
        if name=='app.json':
            self.put(root,'sort.json',{'selected':self.scope['filters']['sort']})
            for file in sorted((root/'cards').glob('*.json')):
                card=json.loads(file.read_text());card.pop('ui_check',None)
                self.put(root,str(file.relative_to(root)),card)
        if (root/'app.json').exists() and (root/'pages/0001.json').exists():
            pages=[json.loads(p.read_text()) for p in sorted((root/'pages').glob('*.json'))]
            try:
                cards=[json.loads((root/ref).read_text()) for p in pages for ref in p['cards']]
                app=json.loads((root/'app.json').read_text())
                summary={'producer':'gdd-collection-seal/v1','status':'completed','completed_at':datetime.now(timezone.utc).isoformat(),'collection':{'app_name':app['app_name'],'platform':app['platform'],'app_id':app['app_id'],'card_count':len({c['ad_id'] for c in cards}),'page_count':len(pages),'ad_ids':sorted({c['ad_id'] for c in cards}),'count_basis':self.scope['filters']['dedupe'],'sort':self.scope['filters']['sort']}}
                fsm.write(root/'collection_complete.json',{'scope_sha256':self.fp,'evidence':['ui.txt'],**summary})
            except (OSError,KeyError):pass

    def status(self):
        return fsm.inspect(self.run)['tasks'][0]

    def test_complete_and_file_corruption_invalidates(self):
        self.assertEqual(self.status()['state'], 'COMPLETED')
        (self.root/'delivery/a.mp4').write_bytes(b'corrupted')
        self.assertEqual(self.status()['state'], 'FRAMES_PENDING')

    def test_unrestricted_filter_readonly_accepts_empty_and_rejects_residue(self):
        filters=load_json(self.root/'filters.json');ref=filters['ui_checks']['languages']
        event=load_json(self.root/ref);event.update(producer='gdd-readonly-check/v1',kind='read',before=[])
        fsm.write(self.root/ref,event)
        self.assertEqual(self.status()['state'],'COMPLETED')
        event['before']=['英语'];fsm.write(self.root/ref,event)
        self.assertEqual(self.status()['state'],'FILTERS_PENDING')
        event.update(before=[],key='filter.channels',expected=['Facebook'],after=['Facebook'])
        fsm.write(self.root/ref,event)
        with self.assertRaises(fsm.GateError):fsm.click_check(self.root,ref,self.fp,'app-ios','filter.channels',['Facebook'])

    def test_correct_base_filters_can_be_read_only_but_not_app_or_sort(self):
        filters=load_json(self.root/'filters.json')
        for key,ref in filters['ui_checks'].items():
            event=load_json(self.root/ref)
            event.update(producer='gdd-readonly-check/v1',kind='read',before=event['expected'])
            fsm.write(self.root/ref,event)
        self.assertEqual(self.status()['state'],'COMPLETED')
        for name in ['app.json','sort.json']:
            value=load_json(self.root/name);ref=value['ui_check'];event=load_json(self.root/ref)
            event.update(producer='gdd-readonly-check/v1',kind='read',before=event['expected'])
            fsm.write(self.root/ref,event)
            self.assertEqual(self.status()['state'],'APP_PENDING' if name=='app.json' else 'SORT_PENDING')
            event.update(producer='gdd-checked-click/v1',kind='click');fsm.write(self.root/ref,event)

    def test_card_identity_event_requires_observed_ad_and_intended_app(self):
        expected={'ad_id':'a'*32,'app_id':'123','platform':'iOS'}
        ref=self.event('card.open',{'app_id':'123','platform':'iOS'})
        event=load_json(self.root/ref);event.update(producer='gdd-checked-card/v1',after=expected)
        fsm.write(self.root/ref,event)
        fsm.click_check(self.root,ref,self.fp,'app-ios','card.open',expected)
        event['expected']['app_id']='different-app';fsm.write(self.root/ref,event)
        with self.assertRaises(fsm.GateError):fsm.click_check(self.root,ref,self.fp,'app-ios','card.open',expected)
        event.update(expected={'app_id':'123','platform':'iOS'},after={**expected,'ad_id':'ad1'});fsm.write(self.root/ref,event)
        with self.assertRaises(fsm.GateError):fsm.click_check(self.root,ref,self.fp,'app-ios','card.open',{**expected,'ad_id':'ad1'})

    def test_explicit_per_app_limit_seals_without_claiming_site_terminal(self):
        self.scope['collection_limit_per_app']=1;self.rebase()
        self.put(self.root,'app.json',{**self.app,'card_count':2})
        self.put(self.root,'pages/0001.json',{**self.page,'visible_count':2,'has_next':False,'terminal_reason':'limit_reached','collection_limit':1})
        (self.root/'collection_complete.json').unlink()
        result=self.status();self.assertIn('CRAWL_COMPLETE',result['passed'])
        self.assertEqual(result['collection']['site_card_count'],2)
        sealed=subprocess.run([sys.executable,str(SCRIPT),'seal','--run-dir',str(self.run),'--task','app-ios'],capture_output=True,text=True)
        self.assertEqual(sealed.returncode,0,sealed.stdout+sealed.stderr)
        self.assertEqual(self.status()['state'],'COMPLETED')
        self.put(self.root,'pages/0001.json',{**self.page,'visible_count':2,'terminal_reason':'next_disabled'})
        self.assertEqual(self.status()['state'],'COLLECTING')
        self.scope.pop('collection_limit_per_app');self.rebase()
        self.put(self.root,'pages/0001.json',{**self.page,'visible_count':2,'terminal_reason':'limit_reached','collection_limit':1})
        self.assertEqual(self.status()['state'],'COLLECTING')

    def test_missing_card_cannot_pass_probe(self):
        (self.root/'cards/0001-0001.json').unlink()
        self.assertEqual(self.status()['state'], 'PROBE_PENDING')

    def test_mixed_platform_cannot_pass(self):
        self.put(self.root,'cards/0001-0001.json',{**self.card,'platform':'Android'})
        self.assertEqual(self.status()['state'], 'PROBE_PENDING')

    def test_old_advertiser_residue_is_blocked(self):
        self.put(self.root,'app.json',{**self.app,'selected_app_ids':['old-app','123']})
        self.assertEqual(self.status()['state'],'APP_PENDING')

    def test_keyword_residue_is_blocked(self):
        self.put(self.root,'app.json',{**self.app,'residual_keywords':['old']})
        self.assertEqual(self.status()['state'],'APP_PENDING')

    def test_unfinished_pagination(self):
        self.put(self.root,'pages/0001.json',{**self.page,'has_next':True})
        self.assertEqual(self.status()['state'],'COLLECTING')
        self.assertEqual(self.status()['checkpoint']['next'],{'page':2,'position':1})

    def test_count_mismatch_and_missing_terminal_evidence(self):
        self.put(self.root,'app.json',{**self.app,'card_count':2})
        self.assertEqual(self.status()['state'],'COLLECTING')
        self.put(self.root,'app.json',self.app)
        self.put(self.root,'pages/0001.json',{**self.page,'terminal_reason':None})
        self.assertEqual(self.status()['state'],'COLLECTING')

    def test_checkpoint_uses_saved_files_and_finds_gap(self):
        (self.root/'pages/0001.json').unlink()
        self.put(self.root,'cards/0001-0003.json',{**self.card,'position':3,'ad_id':'ad3'})
        self.assertEqual(self.status()['checkpoint']['next'],{'page':1,'position':2})

    def test_duplicate_ids_across_pages(self):
        self.put(self.root,'app.json',{**self.app,'card_count':2})
        self.put(self.root,'pages/0001.json',{**self.page,'has_next':True})
        self.put(self.root,'cards/0002-0001.json',{**self.card,'page':2})
        self.put(self.root,'pages/0002.json',{**self.page,'page':2,'cards':['cards/0002-0001.json']})
        self.assertIn('重复广告 ID', self.status()['gate_failure'])

    def test_false_content_and_wrong_channel_blocked(self):
        self.put(self.root,'reviews.json',{'items':[{**self.review,'checks':{'male_character':False,'real_person_talking_head':False}}]})
        self.assertEqual(self.status()['state'],'REVIEW_PENDING')
        self.put(self.root,'reviews.json',{'items':[self.review]})
        self.put(self.root,'cards/0001-0001.json',{**self.card,'channel':'Instagram'})
        self.assertEqual(self.status()['state'],'REVIEW_PENDING')

    def test_review_rows_are_not_delivered_as_keep(self):
        self.put(self.root,'reviews.json',{'items':[{**self.review,'decision':'review'}]})
        self.put(self.root,'downloads.json',{'items':[]})
        self.put(self.root,'delivery.json',{'included_ad_ids':[],'preview_verified':True,'portable_links_verified':True,'files':['delivery/index.html']})
        self.assertEqual(self.status()['state'],'COMPLETED_WITH_REVIEW')
        self.assertEqual(self.status()['counts']['keep'],0)

    def test_zero_results_require_correct_app_and_empty_page(self):
        self.put(self.root,'app.json',{**self.app,'card_count':0,'empty_result_verified':True})
        self.put(self.root,'pages/0001.json',{'page':1,'visible_count':0,'cards':[],'has_next':False,'terminal_reason':'explicit_no_results'})
        self.put(self.root,'frames.json',{'items':[]})
        self.put(self.root,'reviews.json',{'items':[]});self.put(self.root,'downloads.json',{'items':[]})
        self.put(self.root,'delivery.json',{'included_ad_ids':[],'preview_verified':True,'portable_links_verified':True,'files':['delivery/index.html']})
        self.assertEqual(self.status()['state'],'COMPLETED')
        self.put(self.root,'app.json',{**self.app,'card_count':0})
        self.assertEqual(self.status()['state'],'PROBE_PENDING')

    def test_scope_changes_rejected(self):
        self.scope['filters']['languages']=['日语'];fsm.write(self.run/'scope.json',self.scope)
        with self.assertRaises(fsm.GateError):fsm.inspect(self.run)

    def test_escape_paths_and_missing_evidence_rejected(self):
        self.put(self.root,'filters.json',{'selected':self.scope['filters'],'evidence':['../../browser.json']})
        self.assertEqual(self.status()['state'],'CATEGORY_PENDING')
        self.put(self.root,'filters.json',{'selected':self.scope['filters'],'evidence':[]})
        self.assertEqual(self.status()['state'],'CATEGORY_PENDING')

    def test_download_ratio_and_missing_assets_block_completion(self):
        self.put(self.root,'downloads.json',{'items':[{**self.download,'width':1280,'height':720}]})
        self.assertEqual(self.status()['state'],'MEDIA_PENDING')
        self.put(self.root,'downloads.json',{'items':[self.download]})
        (self.root/'delivery/index.html').unlink()
        self.assertEqual(self.status()['state'],'DELIVERY_PENDING')

    def test_human_hold_and_cli_resume(self):
        def cli(*args):
            return subprocess.run([sys.executable,str(SCRIPT),*args,'--run-dir',str(self.run)],capture_output=True,text=True)
        self.assertEqual(cli('hold','--task','app-ios','--reason','captcha','--cursor','page=1,next_position=2').returncode,0)
        self.assertEqual(self.status()['state'],'WAITING_HUMAN')
        self.assertEqual(cli('check','--require-complete').returncode,2)
        self.put(self.root,'resume-verification.json',{'authenticated':True,'selected':{},**self.app})
        self.assertEqual(cli('resume','--task','app-ios','--confirmation','synthetic user reply','--verification','resume-verification.json').returncode,1)
        self.put(self.root,'resume-verification.json',{'authenticated':True,'selected':self.scope['filters'],**self.app})
        self.assertEqual(cli('resume','--task','app-ios','--confirmation','synthetic user reply','--verification','resume-verification.json').returncode,0)
        self.assertEqual(self.status()['state'],'COMPLETED')
        self.assertTrue((self.root/'resumes.json').is_file())
        self.assertEqual(cli('check','--require-complete').returncode,0)

    def test_both_platforms_must_complete(self):
        other={**self.scope['tasks'][0],'id':'app-android','platform':'Android'}
        self.scope['tasks'].append(other)
        fsm.write(self.run/'scope.json',self.scope);fsm.write(self.run/'run.json',{'scope_sha256':fsm.digest(self.scope)})
        state=fsm.inspect(self.run)
        self.assertEqual(state['state'],'IN_PROGRESS')
        self.assertEqual(len(state['tasks']),2)


    def rebase(self):
        self.fp=fsm.digest(self.scope)
        fsm.write(self.run/'scope.json',self.scope)
        for path in self.run.rglob('*.json'):
            if path.name in ['scope.json','state.json']:
                continue
            value=json.loads(path.read_text())
            if isinstance(value,dict) and 'scope_sha256' in value:
                value['scope_sha256']=self.fp;fsm.write(path,value)
        self.put(self.root,'filters.json',{'selected':self.scope['filters']})

    def test_all_date_presets_and_custom_range(self):
        for preset in ['7天','30天','90天','1年']:
            self.scope['filters']['date']={'mode':'preset','preset':preset};self.rebase()
            self.assertEqual(self.status()['state'],'COMPLETED')
        self.scope['filters']['date']={'mode':'custom','start':'2026-09-01','end':'2026-09-30'};self.rebase()
        self.assertEqual(self.status()['state'],'COMPLETED')
        self.scope['filters']['date']['start']='2026-10-01'
        with self.assertRaises(fsm.GateError):fsm.validate_scope(self.scope)

    def test_games_and_ad_mode_use_matching_count_basis(self):
        self.scope['filters'].update(category='游戏',dedupe='广告');self.rebase()
        self.assertEqual(self.status()['state'],'APP_PENDING')
        self.put(self.root,'app.json',{**self.app,'count_basis':'广告'})
        self.assertEqual(self.status()['state'],'COMPLETED')

    def test_filters_need_script_clicks_not_only_declared_values(self):
        self.put(self.root,'filters.json',{'selected':self.scope['filters'],'ui_checks':{}})
        self.assertEqual(self.status()['state'],'CATEGORY_PENDING')

    def test_app_needs_script_click_check(self):
        self.put(self.root,'app.json',{**self.app,'ui_check':None})
        self.assertEqual(self.status()['state'],'APP_PENDING')

    def test_script_readback_mismatch_is_rejected(self):
        app=load_json(self.root/'app.json');event_path=self.root/app['ui_check']
        event=load_json(event_path);event['after']['platform']='Android';fsm.write(event_path,event)
        self.assertEqual(self.status()['state'],'APP_PENDING')

    def test_missing_click_screenshot_is_rejected(self):
        app=load_json(self.root/'app.json');event_path=self.root/app['ui_check']
        event=load_json(event_path);del event['images']['before'];fsm.write(event_path,event)
        self.assertEqual(self.status()['state'],'APP_PENDING')

    def test_missing_frames_or_modified_frame_block_review(self):
        frame=self.root/'evidence/frames/ad1/frame-03.png';frame.write_bytes(b'wrong bytes')
        self.assertEqual(self.status()['state'],'FRAMES_PENDING')
        frame.write_bytes(self.png)
        self.put(self.root,'frames.json',{'items':[]})
        self.assertEqual(self.status()['state'],'FRAMES_PENDING')

    def test_browser_jpeg_screenshots_are_accepted_but_frames_stay_png(self):
        jpeg=b'\xff\xd8\xff\xc0'+struct.pack('>H',17)+bytes([8,0,1,0,1,3,1,17,0,2,17,0,3,17,0])+b'\xff\xd9'
        for stage in ['before','after']:(self.root/('ui-'+stage+'.png')).write_bytes(jpeg)
        for path in (self.root/'evidence/clicks').glob('*.json'):
            event=load_json(path)
            for item in event['images'].values():item['sha256']=hashlib.sha256(jpeg).hexdigest()
            fsm.write(path,event)
        self.assertEqual(self.status()['state'],'COMPLETED')
        with self.assertRaises(fsm.GateError):fsm.png_image(self.root,{'path':'ui-before.png','sha256':hashlib.sha256(jpeg).hexdigest()})

    def test_country_readback_order_is_not_semantic_difference(self):
        self.scope['filters']['regions']=['日本','韩国'];self.rebase()
        doc=load_json(self.root/'filters.json');doc['selected']['regions']=['韩国','日本'];fsm.write(self.root/'filters.json',doc)
        path=self.root/doc['ui_checks']['regions'];event=load_json(path);event['after']=['韩国','日本'];fsm.write(path,event)
        self.assertEqual(self.status()['state'],'COMPLETED')

    def test_heat_sort_is_supported_after_app(self):
        self.scope['filters']['sort']='热度';self.rebase()
        self.assertEqual(self.status()['state'],'COMPLETED')

    def test_latest_sort_and_verified_app_id_query(self):
        self.scope['filters']['sort']='最新创意';self.rebase()
        app=load_json(self.root/'app.json')
        app.update(query_type='app_id',query_app_id='123')
        app.pop('query_full_name',None)
        fsm.write(self.root/'app.json',app)
        self.assertEqual(self.status()['state'],'COMPLETED')
        app['query_app_id']='different-id';fsm.write(self.root/'app.json',app)
        self.assertEqual(self.status()['state'],'APP_PENDING')

    def test_app_cannot_be_selected_before_base_filters(self):
        app=load_json(self.root/'app.json');event=load_json(self.root/app['ui_check'])
        event['performed_at']='2000-01-01T00:00:00+00:00';fsm.write(self.root/app['ui_check'],event)
        self.assertEqual(self.status()['state'],'APP_PENDING')

    def test_sort_must_happen_after_app(self):
        sort=load_json(self.root/'sort.json');event=load_json(self.root/sort['ui_check'])
        event['performed_at']='2000-01-01T00:00:00+00:00';fsm.write(self.root/sort['ui_check'],event)
        self.assertEqual(self.status()['state'],'SORT_PENDING')

    def test_card_cannot_be_clicked_before_sort(self):
        card=load_json(self.root/'cards/0001-0001.json');event=load_json(self.root/card['ui_check'])
        event['performed_at']='2000-01-01T00:00:00+00:00';fsm.write(self.root/card['ui_check'],event)
        self.assertEqual(self.status()['state'],'PROBE_PENDING')

    def test_app_collection_needs_seal_and_updates_checklist(self):
        (self.root/'collection_complete.json').unlink()
        self.assertEqual(self.status()['state'],'COLLECTION_SUMMARY_PENDING')
        self.assertEqual(fsm.inspect(self.run)['next_collection_task'],'app-ios')
        sealed=subprocess.run([sys.executable,str(SCRIPT),'seal','--run-dir',str(self.run),'--task','app-ios'],capture_output=True,text=True)
        self.assertEqual(sealed.returncode,0,sealed.stdout+sealed.stderr)
        state=json.loads(sealed.stdout)
        self.assertTrue(state['collection_complete'])
        self.assertIsNone(state['next_collection_task'])
        self.assertEqual(load_json(self.run/'app_checklist.json')['items'][0]['collection_status'],'completed')

    def test_next_app_waits_for_previous_collection_checklist(self):
        self.scope['tasks'].append({'id':'next-app','app_name':'Next App','platform':'Android'})
        self.rebase()
        (self.root/'collection_complete.json').unlink()
        tasks=fsm.inspect(self.run)['tasks']
        self.assertEqual(tasks[1]['state'],'PREVIOUS_APP_PENDING')
        sealed=subprocess.run([sys.executable,str(SCRIPT),'seal','--run-dir',str(self.run),'--task','app-ios'],capture_output=True,text=True)
        self.assertEqual(sealed.returncode,0,sealed.stdout)
        tasks=json.loads(sealed.stdout)['tasks']
        self.assertEqual(tasks[1]['state'],'CATEGORY_PENDING')
        self.assertEqual(json.loads(sealed.stdout)['next_collection_task'],'next-app')

    def test_facebook_family_ui_alias_only_keeps_news_feed(self):
        self.scope['filters']['channels']=['Facebook系']
        self.rebase()
        self.put(self.root,'filters.json',{'selected':self.scope['filters']})
        self.assertEqual(self.status()['state'],'COMPLETED')
        self.put(self.root,'cards/0001-0001.json',{**self.card,'channel':'Instagram'})
        self.assertEqual(self.status()['state'],'REVIEW_PENDING')

    def test_review_cannot_reference_unrelated_screenshots(self):
        self.put(self.root,'reviews.json',{'items':[{**self.review,'evidence':['ui.txt']}]})
        self.assertEqual(self.status()['state'],'REVIEW_PENDING')

    def test_frames_must_be_inspected_before_review_completes(self):
        self.put(self.root,'reviews.json',{'items':[{**self.review,'frames_inspected':False}]})
        self.assertEqual(self.status()['state'],'REVIEW_PENDING')

    def test_resume_cannot_reuse_pre_captcha_click_logs(self):
        self.put(self.root,'resume-verification.json',{'authenticated':True,'selected':self.scope['filters'],**self.app})
        self.assertEqual(subprocess.run([sys.executable,str(SCRIPT),'hold','--run-dir',str(self.run),'--task','app-ios','--reason','captcha','--cursor','page=1'],capture_output=True).returncode,0)
        resumed=subprocess.run([sys.executable,str(SCRIPT),'resume','--run-dir',str(self.run),'--task','app-ios','--confirmation','synthetic reply','--verification','resume-verification.json'],capture_output=True)
        self.assertEqual(resumed.returncode,1)
        self.assertEqual(self.status()['state'],'WAITING_HUMAN')


def load_json(path):
    return json.loads(path.read_text())


class FrameExtractorTest(unittest.TestCase):
    def test_real_ffmpeg_extraction_and_manifest_validation(self):
        import shutil
        if not shutil.which('ffmpeg') or not shutil.which('ffprobe'):
            self.skipTest('FFmpeg unavailable')
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);video=root/'test.mp4'
            subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','testsrc2=size=90x160:rate=10:duration=1','-c:v','mpeg4',str(video)],check=True,capture_output=True)
            extracted=subprocess.run([sys.executable,str(SCRIPT.with_name('frames.py')),'--video',str(video),'--task-root',str(root),'--output-dir',str(root/'frames'),'--scope-sha256','fixture','--ad-id','test'],capture_output=True,text=True)
            self.assertEqual(extracted.returncode,0,extracted.stdout+extracted.stderr)
            self.assertEqual(json.loads(extracted.stdout)['frames'],6)
            manifest=fsm.frame_check(root,'frames/manifest.json','fixture','test')
            self.assertEqual(manifest['source']['width'],90)
            self.assertEqual(manifest['source']['height'],160)
            self.assertEqual(len({f['sha256'] for f in manifest['frames']}),6)


if __name__ == '__main__':
    unittest.main()
