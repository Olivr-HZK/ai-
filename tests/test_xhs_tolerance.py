import json
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from ua_workflows import xhs_daily as x
from ua_workflows import xhs_tolerance as t


def setup_run(tmp_path, monkeypatch, completed=7):
    names=[f'word{i}' for i in range(7)]
    cfg={'keywords':names,'continuation_policy':t.POLICY,'api':{'endpoint':'https://api.everyinfra.com/api/v1/social'}}
    c=tmp_path/'config.json';x.save(c,cfg);monkeypatch.setattr(x,'CONFIG',c)
    x.save(tmp_path/'run.json',{'keywords':names,'sort':'newest','continuation_policy':t.POLICY})
    n={'id':'a','url':'https://www.xiaohongshu.com/explore/a','posted_at':'2026-10-19T10:00:00+00:00','like_count':5,'is_video':True}
    for i in range(completed):
        stem=tmp_path/f'keywords/{i+1:02}/pages/0001'
        x.save(stem.with_suffix('.request.json'),{'requested_at':'2026-10-19T19:00:00+08:00','body':{'params':{'keyword':names[i],'content_type':'all','date_range':'last_24_hours','sort':'newest'}}})
        x.save(stem.with_suffix('.response.json'),{'results':[n],'next_page_token':None})
    summary={'status':'complete' if completed==7 else 'incomplete','date_range':'last_24_hours','unique_notes':1,
             'completed_keywords':completed,'keywords':[{'keyword':k,'pages':int(i<completed),'status':'complete' if i<completed else 'incomplete'} for i,k in enumerate(names)]}
    x.save(tmp_path/'summary.json',summary)
    merged={**n,'matched_keywords':names[:completed]};x.save(tmp_path/'notes.json',[merged])
    return merged,cfg


@pytest.mark.parametrize('completed,allowed',[(3,False),(4,True),(6,True),(7,True)])
def test_majority_requires_four_terminal_keywords_and_preserves_partial(tmp_path,monkeypatch,completed,allowed):
    setup_run(tmp_path,monkeypatch,completed)
    raw=(tmp_path/'summary.json').read_bytes()
    if not allowed:
        with pytest.raises(ValueError,match='finish'):x.validated_notes(tmp_path)
    else:
        notes,stats=x.validated_notes(tmp_path)
        assert len(notes)==1 and stats['completed_keywords']==completed
        assert stats['collection_status']==('complete' if completed==7 else 'partial')
        assert len(stats['incomplete_keywords'])==7-completed
    assert (tmp_path/'summary.json').read_bytes()==raw


def test_majority_does_not_trust_fake_terminal_status_or_empty_proof(tmp_path,monkeypatch):
    setup_run(tmp_path,monkeypatch,4)
    p=tmp_path/'keywords/01/pages/0001.response.json';r=x.load(p);r['next_page_token']='still-more';x.save(p,r)
    with pytest.raises(ValueError,match='pagination'):x.validated_notes(tmp_path)
    s=x.load(tmp_path/'summary.json');s['keywords'][0]['pages']=0;x.save(tmp_path/'summary.json',s)
    with pytest.raises(ValueError,match='completeness'):x.validated_notes(tmp_path)


def test_failed_media_three_attempts_persist_and_never_fake_a_review(tmp_path,monkeypatch):
    n,c=setup_run(tmp_path,monkeypatch);calls=[]
    n['video_url']='https://sns-video.xhscdn.com/a.mp4'
    monkeypatch.setattr(x,'_media_once',lambda *args: (calls.append(1), (_ for _ in ()).throw(requests.ReadTimeout('truncated')))[1])
    with pytest.raises(t.MediaUnavailable):x.media(n,tmp_path,c)
    with pytest.raises(t.MediaUnavailable):x.media(n,tmp_path,c)
    assert len(calls)==3
    assert t.exhausted_notes(tmp_path,[n])[0]['attempts']==3
    assert not (tmp_path/'media/a/audience-review.json').exists()
    ledger=tmp_path/'media/a/retry-media/state.json';d=x.load(ledger)
    evidence=ledger.parent/d['attempts'][0]['diagnostic'];evidence.write_text('{}')
    with pytest.raises(ValueError,match='failure evidence'):t.exhausted_notes(tmp_path,[n])


def test_interrupted_attempt_consumes_budget_and_cached_success_is_free(tmp_path,monkeypatch):
    n,c=setup_run(tmp_path,monkeypatch);calls=[]
    def interrupted(slot):calls.append(1);raise KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):t.bounded_acquire(tmp_path,n,'detail',interrupted)
    def succeed(slot):calls.append(1);return 'actual-original-url'
    assert t.bounded_acquire(tmp_path,n,'detail',succeed)=='actual-original-url'
    assert t.bounded_acquire(tmp_path,n,'detail',interrupted)=='actual-original-url'
    assert len(calls)==2


def test_detail_retries_three_posts_then_skips_and_success_cache_needs_no_key(tmp_path,monkeypatch):
    from ua_workflows.shared import config as shared
    from ua_workflows import collection_runtime as runtime
    n,c=setup_run(tmp_path,monkeypatch);folder=tmp_path/'media/a';folder.mkdir(parents=True)
    monkeypatch.setattr(shared,'project_env_values',lambda:{'EVERYINFRA_API_KEY':'test'})
    monkeypatch.setattr(runtime,'paid_request_power',lambda *a:nullcontext())
    calls=[]
    def post(*args,**kw):
        calls.append(1)
        if len(calls)<3:raise requests.ReadTimeout('unknown outcome')
        return SimpleNamespace(ok=True,status_code=200),{'results':{'id':'a','video_url':'https://sns-video.xhscdn.com/a.mp4'}}
    monkeypatch.setattr(x,'post_json',post)
    assert x.api_video(n,folder,c).endswith('a.mp4') and len(calls)==3
    monkeypatch.setattr(shared,'project_env_values',lambda:{})
    assert x.api_video(n,folder,c).endswith('a.mp4') and len(calls)==3


def test_three_failed_details_are_auto_omitted_from_reviewable_cohort(tmp_path,monkeypatch):
    from ua_workflows.shared import config as shared
    from ua_workflows import collection_runtime as runtime
    n,c=setup_run(tmp_path,monkeypatch);folder=tmp_path/'media/a';folder.mkdir(parents=True)
    monkeypatch.setattr(shared,'project_env_values',lambda:{'EVERYINFRA_API_KEY':'test'})
    monkeypatch.setattr(runtime,'paid_request_power',lambda *a:nullcontext())
    calls=[]
    def post(*args,**kw):calls.append(1);raise requests.ReadTimeout('unknown outcome')
    monkeypatch.setattr(x,'post_json',post)
    with pytest.raises(t.MediaUnavailable):x.api_video(n,folder,c)
    assert len(calls)==3
    notes,stats=x.reviewable_notes(tmp_path)
    assert notes==[] and stats['media_unavailable_ids']==['a'] and stats['media_review_status']=='partial'
    assert stats['collection_status']=='complete'
    with pytest.raises(t.MediaUnavailable):x.api_video(n,folder,c)
    assert len(calls)==3


def test_old_run_does_not_inherit_live_config_retry_policy(tmp_path,monkeypatch):
    n,c=setup_run(tmp_path,monkeypatch);r=x.load(tmp_path/'run.json');r.pop('continuation_policy');x.save(tmp_path/'run.json',r)
    assert t.policy_for(tmp_path) is None


def test_image_download_three_failures_skip_without_visual_fabrication(tmp_path,monkeypatch):
    n,c=setup_run(tmp_path,monkeypatch);n.update(is_video=False,images=['https://sns-img.xhscdn.com/a.jpg'])
    monkeypatch.setattr(x,'validated_notes',lambda base:([n],{'raw_unique_notes':1,'collection_status':'complete'}))
    calls=[]
    def download(*args):calls.append(1);raise requests.ReadTimeout('image truncated')
    monkeypatch.setattr(x,'download',download)
    result=x.prepare_image_review(tmp_path)
    assert result['status']=='complete' and result['failures']==[] and len(calls)==3
    remaining,stats=x.reviewable_notes(tmp_path)
    assert remaining==[] and stats['media_unavailable_ids']==['a']
    assert not (tmp_path/'media/a/image-review.json').exists()
    x.prepare_image_review(tmp_path);assert len(calls)==3


def test_successful_detail_and_three_failed_downloads_have_independent_budgets(tmp_path,monkeypatch):
    from ua_workflows.shared import config as shared
    from ua_workflows import collection_runtime as runtime
    n,c=setup_run(tmp_path,monkeypatch)
    monkeypatch.setattr(shared,'project_env_values',lambda:{'EVERYINFRA_API_KEY':'test'})
    monkeypatch.setattr(runtime,'paid_request_power',lambda *a:nullcontext())
    calls={'post':0,'download':0}
    def post(*args,**kwargs):
        calls['post']+=1
        return SimpleNamespace(ok=True,status_code=200),{'results':{'id':'a','video_url':'https://sns-video.xhscdn.com/a.mp4'}}
    def download(*args):calls['download']+=1;raise requests.ReadTimeout('partial original')
    monkeypatch.setattr(x,'post_json',post);monkeypatch.setattr(x,'download',download)
    with pytest.raises(t.MediaUnavailable):x.media(n,tmp_path,c)
    assert calls=={'post':1,'download':3}
    assert t.exhausted_notes(tmp_path,[n])[0]['phase']=='media'
    with pytest.raises(t.MediaUnavailable):x.media(n,tmp_path,c)
    assert calls=={'post':1,'download':3}


def test_final_interrupted_detail_with_real_response_is_recovered_before_omission(tmp_path,monkeypatch):
    from ua_workflows.shared import config as shared
    n,c=setup_run(tmp_path,monkeypatch);calls=[]
    def op(slot):
        calls.append(1)
        if len(calls)<3:raise requests.ReadTimeout('unknown')
        x.save(slot/'video-api-result-meta.json',{'ok':True,'status_code':200})
        x.save(slot/'video-api-response.json',{'results':{'id':'a','video_url':'https://sns-video.xhscdn.com/a.mp4'}})
        raise KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):t.bounded_acquire(tmp_path,n,'detail',op)
    assert t.exhausted_notes(tmp_path,[n])==[]
    monkeypatch.setattr(shared,'project_env_values',lambda:{})
    monkeypatch.setattr(x,'post_json',lambda *a,**k:pytest.fail('recovery must not POST'))
    assert x.api_video(n,tmp_path/'media/a',c).endswith('a.mp4')
