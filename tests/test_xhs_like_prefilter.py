import json
from pathlib import Path
import ua_workflows.xhs_daily as x


def test_prefilter_before_media_and_equal_boundary(tmp_path, monkeypatch):
    x.save(tmp_path/'daily-context.json', {'like_filter':x.LIKE_FILTER})
    notes=[{'id':'low','like_count':99,'posted_at':'2026-10-27T01:00:00+08:00','is_video':False},
           {'id':'boundary','like_count':100,'posted_at':'2026-10-27T02:00:00+08:00','is_video':False},
           {'id':'high','like_count':150,'posted_at':'2026-10-27T03:00:00+08:00','is_video':True}]
    monkeypatch.setattr(x,'validated_notes',lambda _: (notes,{'raw_unique_notes':3}))
    monkeypatch.setattr(x,'duration_policy_for',lambda _:None)
    monkeypatch.setattr(x,'policy_for',lambda _:None)
    result,stats=x.reviewable_notes(tmp_path)
    assert [n['id'] for n in result]==['high','boundary']
    assert stats['like_filter_eligible']==2 and stats['like_filter_excluded']==1
    assert x.load(tmp_path/'excluded-below-likes.json')['notes']==[{'id':'low','like_count':99}]


def test_selection_all_qualifying_over_100(tmp_path, monkeypatch):
    x.save(tmp_path/'daily-context.json', {'like_filter':x.LIKE_FILTER})
    notes=[{'id':str(i),'like_count':100+i,'posted_at':'2026-10-27T01:00:00+08:00','is_video':True} for i in range(120)]
    monkeypatch.setattr(x,'reviewable_notes',lambda _: (notes,{'raw_unique_notes':120,'like_filter':x.LIKE_FILTER}))
    monkeypatch.setattr(x,'load',lambda p: {'audience_content_filter':{'enabled':True}} if Path(p)==x.CONFIG else json.loads(Path(p).read_text()))
    monkeypatch.setattr(x,'eligible_media',lambda *args: {'files':[{}]})
    result=x.select(tmp_path)
    assert result['selected']==120 and len(result['items'])==120
    assert result['selection_rule']=='minimum_likes_before_media_v1'


def test_old_batch_no_filter_and_invalid_rule_fails(tmp_path):
    assert x.like_filter_for(tmp_path) is None
    x.save(tmp_path/'daily-context.json', {'like_filter':{'rule':'minimum_likes_before_media_v1','minimum_likes':99}})
    import pytest
    with pytest.raises(ValueError,match='Unsupported'): x.like_filter_for(tmp_path)


def test_prepare_only_passes_filtered_images_and_preserves_old_sheet(tmp_path, monkeypatch):
    from PIL import Image
    x.save(tmp_path/'daily-context.json', {'like_filter':x.LIKE_FILTER})
    notes=[{'id':'low','like_count':99,'posted_at':'2026-10-27T01:00:00+08:00','is_video':False},
           {'id':'high','like_count':100,'posted_at':'2026-10-27T02:00:00+08:00','is_video':False}]
    monkeypatch.setattr(x,'validated_notes',lambda _: (notes,{'raw_unique_notes':2}))
    monkeypatch.setattr(x,'duration_policy_for',lambda _:None)
    monkeypatch.setattr(x,'policy_for',lambda _:None)
    old=tmp_path/'image-review-sheets/001.jpg';old.parent.mkdir();old.write_bytes(b'old evidence')
    calls=[]
    def media(note,base,config):
        calls.append(note['id']);path=base/'image.png';Image.new('RGB',(10,10)).save(path)
        return {'files':[{'path':'image.png','sha256':x.sha(path)}]}
    monkeypatch.setattr(x,'media',media)
    monkeypatch.setattr(x,'reviewed_images',lambda *args: (_ for _ in ()).throw(ValueError('pending')))
    result=x.prepare_image_review(tmp_path)
    assert result['status']=='IMAGE_REVIEW_PENDING' and calls==['high']
    assert old.read_bytes()==b'old evidence'
    assert len(list((tmp_path/'image-review-sheets').glob('*/001.jpg')))==1
