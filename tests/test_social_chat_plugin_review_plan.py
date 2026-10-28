import copy
import hashlib
import json
import math
from pathlib import Path
import subprocess
import sys
import zipfile

import pytest

from ua_workflows.social_chat_plugin_review_plan import POLICY, LEGACY_POLICY, plan, prepare_queue


def vector(angle):
    return [math.cos(math.radians(angle)), math.sin(math.radians(angle))] + [0.0]*510


def row(key, day='2026-10-25', value=100, app='Product: Full Name', os='Android', **extra):
    return dict(id=key, day=day, value=value, app=app, os=os, kind='视频', **extra)


def test_dedupe_before_content_preserves_highest_even_if_old_review_excluded():
    records=[row('high', value=200, decision='exclude'),row('low', decision='keep'),row('new')]
    result=plan('2026-10-25',records,{'high':vector(0),'low':vector(2),'new':vector(90)})
    assert {r['id'] for r in result['review_queue']}=={'high','new'}
    assert result['decisions']['low']['status']=='intraday_removed'
    assert result['status']=='JOINT_REVIEW_PENDING' and not result['review_certified']
    assert result['input_before_content_review']==3


def test_plugin_positive_labels_are_first_pass_and_unknown_is_not_false():
    records=[row('positive',relaunch={'value':True,'label':'重投'}),row('unknown',relaunch={'value':None,'label':'未知'}),
             row('copy_only',raw_text='广告文案写重投，实际无标签')]
    result=plan('2026-10-25',records,{'unknown':vector(0),'copy_only':vector(90)})
    assert result['plugin_relaunch_excluded_ids']==['positive']
    assert result['target_media_before_relaunch']==3 and result['input_before_content_review']==2
    assert {r['id'] for r in result['review_queue']}=={'unknown','copy_only'}
    assert result['policy']['selection']=='all_qualified' and len(result['policy']['screening_passes'])==4


def test_legacy_processing_version_resumes_without_policy_migration():
    result=plan('2026-10-25',[row('one')],{'one':vector(0)},LEGACY_POLICY)
    assert result['version']=='dedupe-first-joint-review-v1'
    assert result['policy']==LEGACY_POLICY and 'rank_top100' in result['policy']['order']


def test_seven_calendar_day_window_excludes_17_and_includes_18():
    records=[row('today'),row('old','2026-10-17'),row('boundary','2026-10-18')]
    vectors={r['id']:vector(0) for r in records}
    result=plan('2026-10-25',records,vectors)
    assert result['counts']=={'crossday_removed':1}
    assert result['decisions']['today']['matched_date']=='2026-10-18'
    result=plan('2026-10-25',records[:2],vectors)
    assert result['counts']=={'review_pending':1}
    assert '2026-10-18' in result['missing_history_dates']


def test_identity_system_and_chain_clustering_tie_are_explicit():
    records=[row('a'),row('b'),row('c'),row('ios',os='iOS'),row('other',app='Another Product')]
    result=plan('2026-10-25',records,{'a':vector(0),'b':vector(20),'c':vector(40),'ios':vector(0),'other':vector(0)})
    assert {r['id'] for r in result['review_queue']}=={'a','ios','other'}
    assert result['decisions']['c']['similarity_to_representative']<.9
    assert result['decisions']['c']['representative']=='a'


@pytest.mark.parametrize('bad', [None, [1.0], [float('nan')]*512, [0.0]*512])
def test_missing_or_invalid_vectors_never_pass_through_as_new(bad):
    with pytest.raises(ValueError):plan('2026-10-25',[row('x')],{'x':bad})


def test_unidentified_target_blocks_but_history_is_explicitly_reported():
    with pytest.raises(ValueError,match='identity'):plan('2026-10-25',[row('today',os=None)],{'today':vector(0)})
    result=plan('2026-10-25',[row('today'),row('history','2026-10-24',os=None)],{'today':vector(0)})
    assert result['unidentified_history_ids']==['history']
    assert result['history_rows']==0 and '2026-10-24' in result['missing_history_dates']


def test_recent_history_component_behavior_matches_legacy_pure_cluster():
    from ua_workflows.video_enhancer.cover_dedupe import _cluster_clip_dedupe
    from ua_workflows.shared.llm.client import embedding_to_bytes
    records=[row('a',value=200),row('b'),row('h','2026-10-24')]
    vectors={'a':vector(0),'b':vector(70),'h':vector(10)}
    actual=plan('2026-10-25',records,vectors)
    daily,_,_=_cluster_clip_dedupe(threshold=.9,today_rows=[{'ad_key':r['id'],'exposure':r['value'],'vec':vectors[r['id']]} for r in records[:2]],history_hist=[],history_emb={})
    expected,_,_=_cluster_clip_dedupe(threshold=.9,today_rows=[{'ad_key':r['id'],'exposure':r['value'],'vec':vectors[r['id']]} for r in records[:2] if r['id'] in daily],
        history_hist=[{'ad_key':'h','target_date':'2026-10-24','exposure':100}],history_emb={'h':embedding_to_bytes(vectors['h'])},target_date='2026-10-25',hard_dedupe_days=7)
    assert {r['id'] for r in actual['review_queue']}==expected


def fixture_index(tmp_path):
    def evidence(name, data):
        path=tmp_path/name;path.write_bytes(data)
        return {'path':str(path),'sha256':hashlib.sha256(data).hexdigest()}
    source=evidence('source.bin',b'isolated fixture, not a decoded media claim')
    frames=[dict(evidence(f'frame-{n}.png',f'isolated frame {n}'.encode()),fraction=f) for n,f in enumerate(POLICY['frame_fractions'])]
    manifest=tmp_path/'manifest.json';manifest.write_text(json.dumps({'source':source,'frames':frames}))
    r=row('one', decision='exclude',review_reason='old reason',review={'decision':'exclude'})
    r.update(sources=[source],proxy=frames[0],frames=frames,manifest=str(manifest),manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest())
    index=tmp_path/'index.json';index.write_text(json.dumps([r]))
    cache=tmp_path/'cache';cache.mkdir();(cache/(frames[0]['sha256']+'.json')).write_text(json.dumps({'image_sha256':frames[0]['sha256'],'model_fingerprint':'isolated-model','vector':vector(0)}))
    return index,cache,r


def test_one_joint_queue_reuses_bound_frames_without_rewriting_or_inheriting_review(tmp_path):
    index,cache,r=fixture_index(tmp_path)
    before={f['path']:Path(f['path']).read_bytes() for f in r['frames']}
    output=tmp_path/'out/plan.json';prepare_queue('2026-10-25',index,cache,output)
    queue=json.loads(output.read_text())['review_queue']
    assert queue[0]['joint_review']=={'status':'pending','evidence_action':'reuse_verified_six_frames','content':None,'template':None,'style':None}
    assert not {'decision','review','review_reason'} & queue[0].keys()
    assert all(Path(path).read_bytes()==content for path,content in before.items())
    manifest=json.loads(Path(r['manifest']).read_text());manifest['source']['sha256']='wrong-source'
    Path(r['manifest']).write_text(json.dumps(manifest));r['manifest_sha256']=hashlib.sha256(Path(r['manifest']).read_bytes()).hexdigest();index.write_text(json.dumps([r]))
    with pytest.raises(ValueError,match='not bound'):prepare_queue('2026-10-25',index,cache,tmp_path/'bad.json')
    assert not (tmp_path/'bad.json').exists()


def test_actual_handoff_cli_pins_date_and_outputs_joint_queue(tmp_path):
    from ua_workflows import social_chat_browser_daily as daily
    index,cache,_=fixture_index(tmp_path)
    package=tmp_path/'original.zip'
    with zipfile.ZipFile(package,'w') as archive:
        archive.writestr('下载报告.md','# Isolated intake fixture')
        archive.writestr('媒体/video/01.mp4',b'not a completed review')
    run=tmp_path/'run'
    daily.prepare('2026-10-25',run,daily.load(daily.CONFIG),source_zips=[package])
    result=subprocess.run([sys.executable,'scripts/run_social_chat_browser_daily.py','pre-review',
        '--run-dir',str(run),'--media-index',str(index),'--embedding-cache',str(cache)],
        cwd=daily.ROOT,capture_output=True,text=True,timeout=20)
    assert result.returncode==0,result.stderr
    output=json.loads(result.stdout)
    assert output['target_date']=='2026-10-25' and output['status']=='JOINT_REVIEW_PENDING'
    assert output['counts']=={'review_pending':1} and not output['review_certified']
    assert not (run/'selection.json').exists() and not (run/'daily-machine').exists()
