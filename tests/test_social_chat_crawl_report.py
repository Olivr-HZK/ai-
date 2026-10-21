import copy
import pytest
from ua_workflows.social_chat_crawl_report import build_card


STATS = {'report_date': '2026-10-19', 'target_date': '2026-10-18', 'tasks': 21,
         'new': 107, 'video_success': 106, 'eligible': 94, 'excluded': 12,
         'missing': 1, 'selected': 10}


def test_report_distinguishes_discovered_downloaded_and_selected_and_links_table():
    url = 'https://example.feishu.cn/base/test?table=test'
    card = build_card(STATS, url, test=True)
    assert card['header']['title']['content'] == '【测试】广大大竞品素材日报｜2026-10-19'
    text = card['elements'][0]['text']['content']
    assert '2026-10-18' in text
    assert '竞品上新：**107 条**' in text
    assert '视频获取成功：**106 条**' in text
    assert '10 条已导入' in text
    assert card['elements'][1]['actions'][0]['url'] == url


@pytest.mark.parametrize('key,value', [('new', 108), ('video_success', 107), ('selected', 9)])
def test_report_blocks_inconsistent_counts(key, value):
    stats = copy.deepcopy(STATS)
    stats[key] = value
    with pytest.raises(ValueError):
        build_card(stats, 'https://example.com')


def test_mixed_media_report_counts_images_in_same_top_decile():
    stats = {**STATS, 'video_new': 80, 'image_new': 27,
             'video_success': 79, 'image_success': 27}
    text = build_card(stats, 'https://example.com')['elements'][0]['text']['content']
    assert '视频 80 条，图片 27 条' in text
    assert '图片获取成功：**27 条**' in text
    assert '10 条已导入' in text


def test_report_reconciles_post_collection_relaunch_exclusion():
    from ua_workflows.social_chat_crawl_report import build_card
    stats = {'report_date':'2026-10-20', 'target_date':'2026-10-19', 'tasks':21,
             'new':12, 'video_new':8, 'image_new':4, 'video_success':6, 'image_success':3,
             'eligible':8, 'excluded':1, 'missing':1, 'relaunch':2, 'selected':1}
    card = build_card(stats, 'https://example.com/base')
    assert '重投素材排除：**2 条**' in card['elements'][0]['text']['content']
    stats['relaunch'] = 1
    with pytest.raises(ValueError, match='reconcile'):
        build_card(stats, 'https://example.com/base')
