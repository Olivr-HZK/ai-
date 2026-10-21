import copy
import json
from pathlib import Path
import pytest
from PIL import Image
from ua_workflows import social_chat_browser_daily as daily
from ua_workflows import social_chat_mixed_window as window
from ua_workflows import social_chat_mixed_delivery as delivery


def sample(kind='图片'):
    return {'app_id':'app.test', 'app_name':'Example', 'platform':'Android', 'ad_id':'ad1', 'media_type':kind,
            'first_discovered_date':'2026-10-17', 'impressions_estimate':'1万', 'estimate_number':10000,
            'channel':'Facebook/TikTok','regions':['日本/韩国'], 'language':'日语/韩语', 'dimensions':'720 x 1280',
            'detail_url':'https://example.com/detail?id=ad1','review_reason':'实际查看', 'review_method':'original_image',
            'image_files':[{'url':'https://example.com/original.png'}],
            'links':[{'href':'https://www.youtube.com/watch?v=abcdefghijk'}]}


def test_payload_uses_real_image_fields_and_individual_options():
    s={'target_date':'2026-10-17','scope_sha256':'scope','generated_at':'now'}
    image=delivery.fields(s,sample())
    assert image['视频链接'] is None and image['视频附件']==[]
    assert image['图片链接']['link']=='https://example.com/original.png'
    assert image['投放地区']==['日本','韩国']
    assert image['投放渠道']==['Facebook','TikTok']
    assert image['非真人口播判断']=='不适用（静态图片）'
    video=delivery.fields(s,sample('视频'))
    assert video['视频链接']['link']=='https://www.youtube.com/watch?v=abcdefghijk'
    assert video['图片附件']==[] and video['图片链接'] is None


def test_readback_checks_attachment_tokens_sizes_and_media_type():
    desired={'唯一键':'key','素材类型':'图片','图片附件':[{'file_token':'img'}], '视频附件':[],
             '图片链接':{'link':'https://example.com/img'},'视频链接':None,'展示估值':10}
    actual={**desired,'展示估值':'10','图片附件':[{'file_token':'img','size':100}]}
    delivery.verify([{'fields':actual}],[desired])
    for name,value in [('素材类型','视频'),('图片附件',[{'file_token':'other','size':100}]),('图片附件',[{'file_token':'img','size':0}]),('视频链接',{'link':'https://wrong'})]:
        bad={**actual,name:value}
        with pytest.raises(ValueError,match='Remote mismatch'):delivery.verify([{'fields':bad}],[desired])


def test_original_image_hash_decode_and_all_sources(tmp_path):
    root=tmp_path;p=root/'image.png';Image.new('RGB',(12,16)).save(p)
    entry={'path':'image.png','url':'https://zingfront.com/sp_opera/image.png','sha256':window.sha(p),'width':12,'height':16,'format':'PNG','decode_verified':True}
    manifest={'ad_id':'ad1','scope_sha256':'scope','images':[entry]}
    daily.save(root/'evidence/images/ad1/manifest.json',manifest)
    card={'ad_id':'ad1','scope_sha256':'scope','observed_images':[{'cls':'ant-image-img','src':entry['url']}]}
    assert window.image_evidence(root,card)==[entry]
    card['observed_images'].append({'cls':'ant-image-img','src':'https://zingfront.com/sp_opera/missing.png'})
    with pytest.raises(ValueError,match='missing/substituted'):window.image_evidence(root,card)
    card['observed_images'].pop();p.write_bytes(b'tampered')
    with pytest.raises(ValueError,match='hash mismatch'):window.image_evidence(root,card)


def test_combined_decile_and_missing_media_fail_closed(tmp_path,monkeypatch):
    cards=[];observations=[]
    board=tmp_path/'board.jpg';board.write_bytes(b'actually viewed test board')
    for n in range(11):
        root=tmp_path/'tasks'/str(n);c={**sample('图片' if n%2 else '视频'),'task_id':str(n),'ad_id':str(n),'source_collection':str(root),'impressions_estimate':str(n),'scope_sha256':'scope'}
        directory='images' if n%2 else 'frames';p=root/f'evidence/{directory}/{n}/manifest.json';daily.save(p,{'test':'manifest'})
        r={'task':str(n),'ad_id':str(n),'reason':'viewed','decision':'keep','viewed_board':'board.jpg','viewed_board_sha256':window.sha(board),'media_manifest_sha256':window.sha(p),'method':'original_image' if n%2 else 'sampled_frames','images_inspected':True,'frames_inspected':True,'checks':{'real_person_talking_head':False},'image_sha256s':['image']}
        cards.append(c);observations.append(r)
    monkeypatch.setattr(window,'collection',lambda *a:(tmp_path,{'target_date':'2026-10-17'},'scope',copy.deepcopy(cards),[{}]*21))
    monkeypatch.setattr(window,'image_evidence',lambda *a:[{'sha256':'image'}])
    monkeypatch.setattr(window,'video_evidence',lambda *a:{'source':{'sha256':'video','path':'video.mp4'}})
    daily.save(tmp_path/'review_observations.json',observations)
    result=window.select(tmp_path,{})
    assert result['counts']['eligible']==11 and result['counts']['selected']==2
    assert [c['ad_id'] for c in result['items']]==['10','9']
    cards[-1]['raw_text']='创意\n重投\n首次发现\n(UTC+8) 2026-09-01'
    filtered=window.select(tmp_path,{})
    assert filtered['counts']['relaunch']==1 and filtered['counts']['eligible']==10
    assert [c['ad_id'] for c in filtered['items']]==['9']
    cards[-1].pop('raw_text')
    daily.save(tmp_path/'review_observations.json',observations[1:])
    with pytest.raises(ValueError,match='Unreviewed'):window.select(tmp_path,{})
    daily.save(tmp_path/'missing_media.json',[{'task_id':'0','ad_id':'0','reason':'failed','attempts':['board.jpg'],'failure':{'status':'failed','ad_id':'0'}}])
    with pytest.raises(ValueError,match='hide available'):window.select(tmp_path,{})


def test_page_readback_accepts_actual_scalar_or_object_not_boolean():
    assert window.page_number({'after':{'page':3}})==3
    assert window.page_number({'after':3})==3
    assert window.page_number({'after':True}) is None


def test_daily_sync_protects_manual_records_and_other_platforms():
    auto={'record_id':'a','fields':{'唯一键':'same','平台':'广大大','来源':'每日最新','目标日期':'2026-10-20','当前入选':True}}
    manual=copy.deepcopy(auto);manual['fields']['来源']='手动增加'
    external=copy.deepcopy(auto);external['fields']['平台']='小红书'
    assert delivery.is_daily_row(auto, '2026-10-20')
    assert not delivery.is_daily_row(auto, '2026-10-19')
    assert not delivery.is_daily_row(manual) and not delivery.is_daily_row(external)
    delivery.guard_manual_collisions({'same':auto}, [{'唯一键':'same'}])
    delivery.guard_manual_collisions({'other':manual}, [{'唯一键':'new'}])
    for protected in (manual, external):
        with pytest.raises(ValueError, match='protected manual/external'):
            delivery.guard_manual_collisions({'same':protected}, [{'唯一键':'same'}])
