import copy

import pytest

from ua_workflows import social_chat_browser_daily as daily
from ua_workflows.social_chat_crawl_report import build_card
from ua_workflows.social_chat_ranking import count, rule, label

TOP100 = {'mode': 'top_n', 'count': 100}
ALL = {'mode': 'all'}


@pytest.mark.parametrize('eligible', [0, 1, 99, 100, 101, 332, 2000])
def test_all_qualified_has_no_hidden_cap(eligible):
    assert count(eligible,ALL)==eligible
    assert '全部审核合格项' in label(ALL)


def test_all_qualified_rejects_conflicting_limit():
    with pytest.raises(ValueError):rule({'mode':'all','count':100})


@pytest.mark.parametrize('eligible,wanted', [(0, 0), (1, 1), (99, 99), (100, 100), (101, 100), (332, 100), (2000, 100)])
def test_top100_caps_without_padding(eligible, wanted):
    assert count(eligible, TOP100) == wanted


@pytest.mark.parametrize('n', [0, -1, True, 100.5, '100', None])
def test_reject_invalid_limits(n):
    with pytest.raises(ValueError):
        rule({'mode': 'top_n', 'count': n})


def test_rank_before_limit_and_content_exclusions_do_not_fill_slots():
    cards = [dict(ad_id=str(i), app_id='test', app_name='Test', platform='Android',
                  raw_text='2026-10-24~2026-10-24', impressions_estimate=str(i)) for i in range(110)]
    reviews = {('Android', 'test', str(i)): dict(decision='keep', checks={'real_person_talking_head': False},
                reason='Isolated fixture', method='sampled_frames') for i in range(110)}
    reviews['Android', 'test', '109'].update(decision='exclude', checks={'real_person_talking_head': True})
    result = daily.rank(cards, reviews, '2026-10-24', TOP100)
    assert result['counts']['eligible'] == 109 and result['counts']['selected'] == 100
    assert [x['ad_id'] for x in result['items']] == [str(i) for i in range(108, 8, -1)]


def test_prepare_pins_new_ranking_but_resumes_old_scope_without_rewrite(tmp_path):
    cfg = daily.load(daily.CONFIG)
    cfg['collection_mode'] = 'direct-browser-v1'
    assert cfg['ranking']['mode'] == 'all' and 'count' not in cfg['ranking']
    daily.prepare('2026-10-24', tmp_path / 'new', cfg)
    assert daily.load(tmp_path / 'new/scope.json')['ranking'] == ALL
    prior_top100=copy.deepcopy(cfg);prior_top100['ranking']=TOP100
    daily.prepare('2026-10-24',tmp_path/'prior_top100',prior_top100)
    pinned=tmp_path/'prior_top100/scope.json';before_top100=pinned.read_bytes()
    daily.prepare('2026-10-24',tmp_path/'prior_top100',cfg)
    assert pinned.read_bytes()==before_top100
    old = copy.deepcopy(cfg); old['ranking'] = {'fraction': '0.10', 'rounding': 'ceil'}
    daily.prepare('2026-10-24', tmp_path / 'old', old)
    p = tmp_path / 'old/scope.json'; before = p.read_bytes()
    assert daily.prepare('2026-10-24', tmp_path / 'old', cfg)['status'] == 'resume_existing'
    assert p.read_bytes() == before
    # Older scopes had no ranking key; their established top-decile rule remains.
    value = daily.load(p); value.pop('ranking'); daily.save(p, value); before = p.read_bytes()
    assert daily.prepare('2026-10-24', tmp_path / 'old', cfg)['status'] == 'resume_existing'
    assert p.read_bytes() == before and count(332) == 34


@pytest.mark.parametrize('eligible,wanted', [(332, 100), (57, 57)])
def test_report_checks_actual_limit_and_describes_shortfall(eligible, wanted):
    stats = dict(report_date='2026-10-26', target_date='2026-10-24', tasks=21,
                 new=eligible, video_success=eligible, eligible=eligible, excluded=0,
                 missing=0, selected=wanted, ranking=TOP100)
    text = build_card(stats, 'https://example.com')['elements'][0]['text']['content']
    assert '前100条' in text and '不足时取全部通过项' in text and '10%' not in text
    stats['selected'] = wanted - 1
    with pytest.raises(ValueError):
        build_card(stats, 'https://example.com')


def test_all_ranking_and_report_include_more_than_100_qualified_items():
    cards=[dict(ad_id=str(i),app_id='test',app_name='Test',platform='Android',
                raw_text='2026-10-24~2026-10-24',impressions_estimate=str(i)) for i in range(145)]
    reviews={('Android','test',str(i)):dict(decision='keep',checks={'real_person_talking_head':False},
                reason='Isolated fixture',method='sampled_frames') for i in range(145)}
    ranked=daily.rank(cards,reviews,'2026-10-24',ALL)
    assert ranked['counts']['selected']==145 and len(ranked['items'])==145
    assert ranked['items'][0]['ad_id']=='144'
    stats=dict(report_date='2026-10-26',target_date='2026-10-24',tasks=21,new=145,video_success=145,
               eligible=145,excluded=0,missing=0,selected=145,ranking=ALL)
    text=build_card(stats,'https://example.com')['elements'][0]['text']['content']
    assert '全部审核合格项' in text and '145' in text and '前100条' not in text
