import copy

import pytest

from ua_workflows.social_chat_attachments import DEFAULT_ATTACHMENT_FIELD, merge_plan, resolve_field, verify_merge
from ua_workflows import social_chat_mixed_delivery as delivery


def file(token, name):
    return {'file_token': token, 'name': name, 'size': 123}


def test_resolve_tracks_renamed_field_id_and_does_not_create_a_video_column():
    cfg={'feishu':{'attachment_field_id':'stable-id','attachment_field_name':'old-label'}}
    field={'field_id':'stable-id','field_name':'新附件列','type':17}
    assert resolve_field(cfg,[field]) is field
    assert ('新附件列',17) in delivery.extra_fields({'attachment_field':'新附件列'})
    assert not any(n in ('视频附件','图片附件') for n,_ in delivery.extra_fields({}))
    for fields in ([],[{**field,'type':1}],[{**field,'field_id':'other'}]):
        with pytest.raises(ValueError):resolve_field(cfg,fields)


def test_image_and_playable_union_keeps_existing_video_and_replication_fields():
    field={'field_id':'stable-id','field_name':DEFAULT_ATTACHMENT_FIELD,'type':17};target=field['field_name']
    old=file('old','existing.mp4');one=file('one','原图一.png');two=file('two','原图二.jpg');html=file('html','playable.html')
    rows=[{'record_id':'image','fields':{'素材类型':'图片',target:[old,one],'图片附件':[one,two],'复刻附件':[file('replica','copy.png')],'来源':'手动增加'}},
          {'record_id':'play','fields':{'素材类型':'Playable','Playable附件':[html]}},
          {'record_id':'video','fields':{'素材类型':'视频',target:[old],'图片附件':[two]}},
          {'record_id':'missing','fields':{'素材类型':'图片'}}]
    original=copy.deepcopy(rows);plan=merge_plan(rows,field)
    assert rows==original
    assert len(plan['updates'])==2 and plan['missing_record_ids']==['missing']
    image=plan['updates'][0]
    assert list(image['fields'])==[target]
    assert [i['file_token'] for i in image['fields'][target]]==['old','one','two']
    assert plan['updates'][1]['fields'][target]==[{'file_token':'html'}]
    actual=copy.deepcopy(rows)
    for r in actual:
        if r['record_id'] in plan['expected_attachments']:r['fields'][target]=copy.deepcopy(plan['expected_attachments'][r['record_id']])
    assert verify_merge(actual,plan)=={'verified_records':2,'verified_files':4}
    assert merge_plan(actual,field)['updates']==[]
    assert actual[0]['fields']['图片附件']==original[0]['fields']['图片附件']
    actual[0]['fields'][target][1]={**one,'size':1}
    with pytest.raises(ValueError,match='name/size'):verify_merge(actual,plan)


def test_only_unified_attachment_can_satisfy_named_readback():
    expected={'key':[file('t','2026-10-26-001-App.png')]}
    legacy=[{'fields':{'唯一键':'key','图片附件':expected['key']}}]
    with pytest.raises(ValueError):delivery.verify_attachment_names(legacy,expected)
    delivery.verify_attachment_names([{'fields':{'唯一键':'key',DEFAULT_ATTACHMENT_FIELD:expected['key']}}],expected)
    renamed=[{'fields':{'唯一键':'key','改名后的附件':expected['key']}}]
    delivery.verify(renamed,[{'唯一键':'key','改名后的附件':[{'file_token':'t'}]}],'改名后的附件')
    delivery.verify_attachment_names(renamed,expected,'改名后的附件')
