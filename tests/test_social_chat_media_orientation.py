import copy
import json
from pathlib import Path
import subprocess
from unittest.mock import Mock

from PIL import Image
import pytest
from ua_workflows import social_chat_media_orientation as orientation
from ua_workflows import social_chat_mixed_delivery as delivery
from ua_workflows import social_chat_plugin_review_plan as planner


def picture(path, size, exif=None):
    image=Image.new('RGB',size,'blue')
    image.save(path,**({'exif':exif} if exif else {}))
    return path


def movie(path,size=(24,16),sar=None):
    cmd=['ffmpeg','-v','error','-f','lavfi','-i',f'color=blue:s={size[0]}x{size[1]}:d=0.12']
    if sar:cmd+=['-vf','setsar='+sar]
    subprocess.run(cmd+['-c:v','libx264','-pix_fmt','yuv420p',str(path)],check=True,capture_output=True)
    return path


@pytest.mark.parametrize('size,status', [((24,16),'landscape'),((16,24),'eligible'),((16,16),'eligible')])
def test_original_images_classified_and_bound_to_hash(tmp_path,size,status):
    p=picture(tmp_path/'image.png',size)
    r=orientation.inspect_media(p)
    assert r['status']==status and (r['width'],r['height'])==size
    assert r['source_sha256']==orientation.sha(p)
    assert orientation.inspect_media(p,'0'*64)['status']=='unknown'


def test_image_exif_orientation_uses_display_geometry(tmp_path):
    exif=Image.Exif();exif[274]=6
    p=picture(tmp_path/'rotated.jpg',(24,16),exif)
    r=orientation.require_non_landscape(p)
    assert (r['encoded_width'],r['encoded_height'])==(24,16)
    assert (r['width'],r['height'])==(16,24)


def test_video_rotation_and_sample_aspect_ratio(tmp_path):
    original=movie(tmp_path/'raw.mp4')
    assert orientation.inspect_media(original)['status']=='landscape'
    rotated=tmp_path/'rotated.mp4'
    subprocess.run(['ffmpeg','-v','error','-display_rotation','90','-i',str(original),'-c','copy',str(rotated)],check=True,capture_output=True)
    r=orientation.require_non_landscape(rotated)
    assert (r['width'],r['height'])==(16,24)
    stretched=movie(tmp_path/'stretched.mp4',(16,24),'2/1')
    assert orientation.inspect_media(stretched)['status']=='landscape'


def test_unreadable_missing_or_non_media_is_never_eligible(tmp_path):
    p=tmp_path/'bad.mp4';p.write_bytes(b'not video')
    for path in [p,tmp_path/'absent.png',tmp_path/'unknown.html']:
        with pytest.raises(orientation.OrientationError,match='dimensions_unverified'):
            orientation.require_non_landscape(path)


def test_first_pass_excludes_before_embeddings_and_quarantines_partial_image_groups(tmp_path):
    portrait=picture(tmp_path/'portrait.png',(16,24));wide=picture(tmp_path/'wide.png',(24,16))
    def row(id,paths):return {'id':id,'day':'2026-11-03','kind':'图片','sources':[{'path':str(p),'sha256':orientation.sha(p)} for p in paths]}
    records=[row('wide',[wide]),row('mixed',[portrait,wide]),row('ok',[portrait]),row('missing',[]),dict(row('relaunch',[]),relaunch=True)]
    kept,d=orientation.filter_current('2026-11-03',records)
    assert {x['id'] for x in kept}=={'ok','relaunch'}
    assert d['wide']['status']==d['mixed']['status']=='landscape_excluded'
    assert d['missing']['status']=='dimensions_pending' and 'relaunch' not in d
    # Rejected rows have no identity, estimate, proxy, vector or six frames: none are required.
    index=tmp_path/'index.json';index.write_text(json.dumps(records[:2]+records[3:]))
    out=tmp_path/'plan.json';planner.prepare_queue('2026-11-03',index,tmp_path/'no-cache',out,orientation_policy=orientation.POLICY)
    result=json.loads(out.read_text())
    assert result['review_queue']==[] and result['model_fingerprint'] is None
    assert result['plugin_relaunch_excluded_ids']==['relaunch']
    assert result['media_orientation_filter']['counts']=={'landscape_excluded':2,'dimensions_pending':1}


def test_pre_ingestion_rechecks_even_when_a_token_was_cached(tmp_path,monkeypatch):
    p=picture(tmp_path/'landscape.png',(24,16));digest=orientation.sha(p)
    name='2026-11-03-001-App.png';cache={'table|'+digest+'|'+name:{'file_token':'old','sha256':digest,'size':p.stat().st_size,'name':name}}
    upload=Mock(side_effect=AssertionError('Must never upload a rejected original'))
    monkeypatch.setattr(delivery,'upload',upload)
    with pytest.raises(orientation.OrientationError,match='landscape_media'):
        delivery.upload_selected(None,{'feishu':{'app_token':'table'}},p,target_date='2026-11-03',sequence=1,advertiser='App',source_sha256=digest,cache=cache)
    upload.assert_not_called()


def test_whole_selection_gate_runs_before_any_feishu_call(tmp_path,monkeypatch):
    p=picture(tmp_path/'wide.png',(24,16))
    selection={'items':[{'ad_id':'a','media_type':'图片','source_collection':str(tmp_path),
                        'image_files':[{'path':p.name,'sha256':orientation.sha(p)}]}]}
    monkeypatch.setattr(delivery.window,'select',lambda *_:selection)
    remote=Mock();remote.listing.side_effect=AssertionError('Do not even provision rejected selection')
    with pytest.raises(ValueError,match='dimensions before ingestion'):
        delivery.sync(tmp_path,{},client=remote,apply=True)
    assert not remote.mock_calls
    assert json.loads((tmp_path/'orientation-before-ingestion.json').read_text())['items'][0]['evidence'][0]['status']=='landscape'


def test_new_replica_scope_pins_gate_but_old_scope_is_not_migrated(tmp_path):
    from ua_workflows import social_chat_replica as replica
    from test_social_chat_replica import Feishu,row
    client=Feishu([row()]);state=replica.prepare(tmp_path/'run','2026-10-29',client=client)
    assert state.scope['media_orientation_policy']==orientation.POLICY
    scope=state.scope;scope.pop('media_orientation_policy')
    replica.save_private(state.root/'scope.json',scope)
    assert 'media_orientation_policy' not in replica.prepare(state.root,'2026-10-29',client=client).scope


def test_replica_blocks_original_before_upload_and_keeps_evidence(tmp_path):
    from ua_workflows import social_chat_replica as replica
    from test_social_chat_replica import Feishu,row,SCHEMA
    path=movie(tmp_path/'source.mp4');r=row();r['fields']['原素材'][0]['size']=path.stat().st_size
    scope=replica.prepare_rows([r],SCHEMA,'2026-10-29');scope['media_orientation_policy']=copy.deepcopy(orientation.POLICY)
    replica.save_private(tmp_path/'scope.json',scope);state=replica.State(tmp_path)
    state.put('rec1',{'record_id':'rec1','stage':'source_ready','source_path':str(path),'source_sha256':orientation.sha(path)})
    api=Mock();api.authenticate.return_value={'account_id':'isolated'};api.upload.side_effect=AssertionError('No paid upload for landscape')
    runner=replica.StagedRunner(state,api_factory=lambda:api,feishu=Feishu([r]))
    runner.upload_all()
    api.upload.assert_not_called()
    value=state.get('rec1');assert value['stage']=='source_failed' and value['error']=='landscape_media'
    assert value['orientation_before_replication']['status']=='landscape'


def test_replica_result_is_checked_again_before_backfill(tmp_path):
    from ua_workflows import social_chat_replica as replica
    from test_social_chat_replica import row,SCHEMA
    path=movie(tmp_path/'output.mp4');scope=replica.prepare_rows([row()],SCHEMA,'2026-10-29')
    scope['media_orientation_policy']=copy.deepcopy(orientation.POLICY)
    replica.save_private(tmp_path/'scope.json',scope);state=replica.State(tmp_path)
    value={'record_id':'rec1','stage':'downloaded','output':{'path':str(path),'size':path.stat().st_size,
            'sha256':orientation.sha(path),'full_decode_passed':True}}
    remote=Mock()
    replica.backfill(state,[value],remote,'/unused')
    assert not remote.mock_calls
    assert state.get('rec1')['stage']=='needs_review'
    assert state.get('rec1')['error']=='landscape_media'


def test_new_handoff_pins_orientation_and_resume_keeps_original_rule(tmp_path):
    from ua_workflows import social_chat_browser_daily as daily
    cfg=daily.load(daily.CONFIG);daily.prepare('2026-11-03',tmp_path,cfg)
    paths=[tmp_path/x for x in ['scope.json','handoff.json','handoff-task.md']]
    before=[p.read_bytes() for p in paths]
    assert json.loads(before[0])['media_orientation_policy']==orientation.POLICY
    cfg=copy.deepcopy(cfg);cfg['plugin_handoff']['media_orientation_policy']['square']='exclude'
    daily.prepare('2026-11-03',tmp_path,cfg)
    assert before==[p.read_bytes() for p in paths]
    with pytest.raises(ValueError,match='orientation policy'):
        daily.prepare('2026-11-03',tmp_path/'new',cfg)
