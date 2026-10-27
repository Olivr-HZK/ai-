import copy

import pytest

from ua_workflows import social_chat_browser_daily as daily
from ua_workflows import social_chat_mixed_delivery as delivery


URL='https://guangdada.net/modules/creative/display-ads/detail?id=observed-ad'


@pytest.mark.parametrize('value', [URL, {'text':'查看详情','link':URL},
    [{'text':'查看详情','type':'url','link':URL}], [{'text':URL,'type':'text'}]])
def test_source_link_verifies_actual_url_across_text_and_hyperlink_readback(value):
    assert daily.source_link_url(value)==URL
    delivery.verify([{'fields':{'唯一键':'key','链接':value}}],[{'唯一键':'key','链接':URL}])


@pytest.mark.parametrize('value', ['查看详情', '按广告ID查看详情', '', 'app.id', 'http://[', None,
    [{'link':URL},{'link':'https://example.com/another'}]])
def test_lost_or_ambiguous_url_does_not_pass_readback(value):
    assert daily.source_link_url(value)==''
    with pytest.raises(ValueError,match='链接'):
        delivery.verify([{'fields':{'唯一键':'key','链接':value}}],[{'唯一键':'key','链接':URL}])


def test_provision_does_not_recreate_deleted_old_columns():
    names=dict(daily.FIELDS)
    assert names['链接']==1 and names['App ID']==1
    assert 'appid' not in names and '广大大详情' not in names


def test_record_listing_requests_rich_text_and_normalizes_only_text_fields():
    row={'record_id':'r','fields':{'链接':[{'type':'url','text':'查看详情','link':URL}],
        'App ID':[{'type':'text','text':'app.id'}], '广告ID':[{'type':'text','text':'ad'}],
        '地区':['日本','韩国'], '附件':[{'file_token':'t','name':'file','type':'video/mp4'}]}}
    original=copy.deepcopy(row);calls=[]
    client=object.__new__(daily.Feishu)
    def call(method,route,**kwargs):
        calls.append(kwargs['params']);return {'items':[row], 'has_more':False}
    client.call=call
    got=client.listing('bitable/v1/apps/app/tables/table/records')[0]
    assert calls[0]['text_field_as_array']=='true'
    assert got['fields']['链接']==URL and got['fields']['App ID']=='app.id'
    assert got['fields']['广告ID']=='ad' and got['fields']['地区']==original['fields']['地区']
    assert got['fields']['附件']==original['fields']['附件'] and row==original
    calls.clear();client.listing('bitable/v1/apps/app/tables/table/fields')
    assert 'text_field_as_array' not in calls[0]


@pytest.mark.parametrize('field', [
    {'field_id':'old-appid','field_name':'链接','type':1},
    {'field_id':'source','field_name':'改名列','type':1},
    {'field_id':'source','field_name':'链接','type':15},
])
def test_changed_source_identity_blocks_provision_before_recreating_columns(field):
    class ReadOnlyClient:
        def listing(self, route):return [field]
        def call(self, *args, **kwargs):pytest.fail('Should not recreate deleted columns')
    cfg={'feishu':{'app_token':'fixture','table_id':'fixture','source_link_field_id':'source'}}
    with pytest.raises(ValueError,match='Source link primary'):
        daily.provision(cfg,ReadOnlyClient())
