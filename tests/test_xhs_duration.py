import json
from pathlib import Path
import shutil
import subprocess

import pytest

from ua_workflows import xhs_daily as x
from ua_workflows import xhs_duration as d
from ua_workflows import xhs_tolerance as t


def pin(base):
    x.save(base/'run.json',{'video_duration_filter':d.POLICY,'continuation_policy':t.POLICY,
                          'keywords':[str(i) for i in range(7)],'started_at':'2026-10-28T00:00:00+08:00'})


def note():
    return {'id':'a','is_video':True,'url':'https://www.xiaohongshu.com/explore/a',
            'video_url':'https://sns-video.xhscdn.com/a.mp4'}


@pytest.mark.parametrize('seconds,excluded',[(299.9,False),(300,False),(300.01,True)])
def test_actual_video_duration_boundary_and_hash_bound_evidence(tmp_path,seconds,excluded):
    pin(tmp_path);source=tmp_path/'media/a/video.mp4';source.parent.mkdir(parents=True);source.write_bytes(b'fixture-original-video')
    probe={'streams':[{'codec_type':'video','duration':str(seconds)}],'format':{'duration':'9000'}}
    if excluded:
        with pytest.raises(d.DurationExcluded):d.check_video(note(),tmp_path,source,probe)
        assert d.excluded_notes(tmp_path,[note()])[0]['duration_seconds']==seconds
    else:assert d.check_video(note(),tmp_path,source,probe)==seconds
    record=x.load(source.parent/'video-duration-check.json');assert record['source_sha256']==x.sha(source)
    record['duration_seconds']=1;x.save(source.parent/'video-duration-check.json',record)
    with pytest.raises(ValueError,match='decision'):d.check_video(note(),tmp_path,source)


def test_audio_or_container_length_cannot_exclude_short_video():
    probe={'streams':[{'codec_type':'audio','duration':'6000'},{'codec_type':'video','duration':'20'}],
           'format':{'duration':'6000'}}
    assert d.stream_seconds(probe)==20
    probe['streams'][1]={'codec_type':'video','duration_ts':900,'time_base':'1/30'}
    assert d.stream_seconds(probe)==30
    probe['streams'][1]={'codec_type':'video'}
    with pytest.raises(ValueError,match='unavailable'):d.stream_seconds(probe)


def test_live_config_does_not_retrofit_historical_run(tmp_path):
    x.save(tmp_path/'run.json',{})
    assert d.policy_for(tmp_path) is None
    assert d.check_video(note(),tmp_path,tmp_path/'nonexistent') is None


@pytest.mark.skipif(not shutil.which('ffmpeg') or not shutil.which('ffprobe'),reason='native media tools unavailable')
@pytest.mark.parametrize('seconds',[300,301])
def test_real_original_cutoff_before_full_decode_frames_and_retry(tmp_path,monkeypatch,seconds):
    pin(tmp_path);source=tmp_path/'fixture.mp4'
    subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i',f'color=black:s=16x16:r=1:d={seconds}',
                    '-an','-c:v','libx264','-pix_fmt','yuv420p',str(source)],check=True,capture_output=True)
    calls={'download':0,'decode':0};original=subprocess.run
    def download(url,path):calls['download']+=1;shutil.copyfile(source,path)
    def run(args,*a,**kw):
        if Path(args[0]).name=='ffmpeg':
            calls['decode']+=1
            assert seconds<=300,'long videos must be filtered before full decode'
        return original(args,*a,**kw)
    monkeypatch.setattr(x,'download',download);monkeypatch.setattr(x.subprocess,'run',run)
    cfg={'audience_content_filter':{'enabled':True}}
    if seconds>300:
        with pytest.raises(d.DurationExcluded):x.media(note(),tmp_path,cfg)
        with pytest.raises(d.DurationExcluded):x.media(note(),tmp_path,cfg)
        assert calls=={'download':1,'decode':0}
        assert not (tmp_path/'media/a/manifest.json').exists()
        assert not (tmp_path/'media/a/audience-review.json').exists()
        assert t.exhausted_notes(tmp_path,[note()])==[]
        monkeypatch.setattr(x,'validated_notes',lambda b:([note()],{'raw_unique_notes':1,'collection_status':'complete'}))
        config=tmp_path/'config.json';x.save(config,cfg);monkeypatch.setattr(x,'CONFIG',config)
        result=x.prepare_audience_review(tmp_path)
        assert result['status']=='complete' and result['video_duration_excluded']==1 and result['media_unavailable_notes']==0
        selected=x.select(tmp_path)
        assert selected['selected']==0 and selected['denominator']==0 and selected['video_duration_excluded']==1
        assert selected['collection_status']=='complete'
        (tmp_path/'media/a/video.mp4').write_bytes(b'changed-original')
        with pytest.raises(ValueError,match='evidence changed'):x.media(note(),tmp_path,cfg)
    else:
        manifest=x.media(note(),tmp_path,cfg)
        assert manifest['files'][0]['duration_seconds']==300 and manifest['files'][0]['decode_verified']
        assert calls=={'download':1,'decode':1}


def test_changed_original_cannot_be_hidden_as_duration_exclusion(tmp_path):
    pin(tmp_path);source=tmp_path/'media/a/video.mp4';source.parent.mkdir(parents=True);source.write_bytes(b'original')
    with pytest.raises(d.DurationExcluded):d.check_video(note(),tmp_path,source,{'streams':[{'codec_type':'video','duration':'301'}]})
    source.write_bytes(b'changed')
    with pytest.raises(ValueError,match='evidence changed'):d.excluded_notes(tmp_path,[note()])


def test_report_distinguishes_duration_filter_from_collection_failure(tmp_path):
    pin(tmp_path);x.save(tmp_path/'summary.json',{'raw_rows':3,'pages':7,'unique_notes':3,'video_notes':3,'image_notes':0})
    class Client:
        def __init__(self):self.sent=[]
        def call(self,method,route,**kw):
            if method=='GET':return {'name':'霍振堃','chat_mode':'group'}
            self.sent.append(kw);return {'message_id':'om_test','chat_id':'oc_test'}
    cfg={'feishu':{'base_url':'https://example.com/base','table_id':'table'},'crawl_completion_report':{'chat_id':'oc_test','chat_name':'霍振堃'}}
    client=Client();selection={'selected':1,'denominator':1,'video_duration_filter':d.POLICY,'video_duration_excluded':2}
    x.report(tmp_path,selection,cfg,client,{'status':'complete','verified_rows':1,'video_success':1,'image_success':0,'failures':[]},'2026-10-27')
    card=json.loads(client.sent[0]['json']['content']);text=card['elements'][0]['text']['content']
    assert '5分钟' in text and '超限排除：**2 篇**' in text
    assert card['header']['template']=='blue' and '部分完成' not in card['header']['title']['content']
