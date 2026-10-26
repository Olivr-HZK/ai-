import copy

import pytest

from ua_workflows import social_chat_browser_daily as daily
from ua_workflows import social_chat_mixed_delivery as delivery
from ua_workflows.social_chat_style_tags import (
    LEGACY_STYLE_VERSION, STYLE_VERSION, style_policy, validate_style,
)


def reviewed(**changes):
    return {'decision': 'keep', 'style_tagging_version': STYLE_VERSION,
            'style_tags': ['欧美写实'], 'style_reason': '实际画面为写实角色与电影感光影', **changes}


def test_multiple_observed_styles_and_explicit_uncertainty():
    scope = {'style_tagging': style_policy()}
    assert validate_style(scope, reviewed(style_tags=['日式漫画', '韩式漫画'])) == ['日式漫画', '韩式漫画']
    assert validate_style(scope, reviewed(style_tags=['待确认'], style_reason='画面画法不足以稳定细分')) == ['待确认']


@pytest.mark.parametrize('change', [
    {'style_tags': None}, {'style_tags': []}, {'style_tags': '日式漫画'},
    {'style_tags': ['真人实拍风']}, {'style_tags': ['日韩风']},
    {'style_tags': ['日式漫画', '日式漫画']}, {'style_tags': ['待确认', '日式漫画']},
    {'style_reason': ''}, {'style_reason': '  '}, {'style_tagging_version': 'old'},
])
def test_missing_or_invented_style_cannot_reach_ranking(change):
    with pytest.raises(ValueError):
        validate_style({'style_tagging': style_policy()}, reviewed(**change))


def test_excluded_pending_and_historical_scopes():
    for decision in ('exclude', 'review'):
        assert validate_style({'style_tagging': style_policy()}, {'decision': decision}) == []
        with pytest.raises(ValueError, match='formal'):
            validate_style({'style_tagging': style_policy()}, reviewed(decision=decision))
    assert validate_style({}, {'decision': 'keep'}) is None
    assert ('风格标签', 4) not in delivery.extra_fields({})
    assert ('风格标签', 4) in delivery.extra_fields({'style_tagging': style_policy()})


def test_prepare_pins_style_requirement_without_changing_old_scope(tmp_path):
    cfg = daily.load(daily.CONFIG)
    cfg['collection_mode'] = 'direct-browser-v1'
    old = copy.deepcopy(cfg)
    old.pop('style_tagging')
    historical = tmp_path / 'old'
    daily.prepare('2026-10-21', historical, old)
    before = (historical / 'scope.json').read_bytes()
    assert daily.prepare('2026-10-21', historical, cfg)['status'] == 'resume_existing'
    assert (historical / 'scope.json').read_bytes() == before
    new = tmp_path / 'new'
    daily.prepare('2026-10-23', new, cfg)
    assert daily.load(new / 'scope.json')['style_tagging'] == style_policy()
    assert 'style_tags' in (new / 'browser-task.txt').read_text()


def test_multiselect_values_are_read_back_not_just_written():
    desired = {'唯一键': 'fixture', '风格标签': ['日式漫画', '韩式漫画']}
    delivery.verify([{'fields': {'唯一键': 'fixture', '风格标签': ['韩式漫画', '日式漫画']}}], [desired])
    for wrong in (None, [], ['日韩风'], ['日式漫画']):
        with pytest.raises(ValueError, match='风格标签'):
            delivery.verify([{'fields': {'唯一键': 'fixture', '风格标签': wrong}}], [desired])


def test_legacy_batch_keeps_original_labels_and_rejects_silent_migration():
    legacy = {'style_tagging': style_policy(LEGACY_STYLE_VERSION)}
    old_review = reviewed(style_tagging_version=LEGACY_STYLE_VERSION,
                          style_tags=['厚涂／CG风'], style_reason='原批次实际画面审核依据')
    assert validate_style(legacy, old_review) == ['厚涂／CG风']
    with pytest.raises(ValueError, match='version'):
        validate_style({'style_tagging': style_policy()}, old_review)
    with pytest.raises(ValueError, match='version'):
        validate_style(legacy, reviewed(style_tags=['3D角色']))
    for tag in ('厚涂／CG风', '古装', '韩式', '漫画'):
        with pytest.raises(ValueError, match='style tags'):
            validate_style({'style_tagging': style_policy()}, reviewed(style_tags=[tag]))
    for tag in ('日式漫画', '韩式漫画', '欧美写实', '中式漫画', '数字插画', '3D角色'):
        assert validate_style({'style_tagging': style_policy()}, reviewed(style_tags=[tag])) == [tag]


def test_prepare_preserves_pinned_v1_scope_when_default_is_v2(tmp_path):
    cfg = daily.load(daily.CONFIG)
    cfg['collection_mode'] = 'direct-browser-v1'
    legacy_cfg = copy.deepcopy(cfg)
    legacy_cfg['style_tagging'] = style_policy(LEGACY_STYLE_VERSION)
    daily.prepare('2026-10-25', tmp_path, legacy_cfg)
    before = (tmp_path / 'scope.json').read_bytes()
    prompt_before = (tmp_path / 'browser-task.txt').read_bytes()
    assert daily.prepare('2026-10-25', tmp_path, cfg)['status'] == 'resume_existing'
    assert (tmp_path / 'scope.json').read_bytes() == before
    assert (tmp_path / 'browser-task.txt').read_bytes() == prompt_before
    assert b'character-style-v1' in prompt_before


@pytest.mark.parametrize('bad_policy', [
    {'version': 'unknown', 'tags': [], 'required_for_keep': True},
    {'version': STYLE_VERSION, 'tags': ['古装'], 'required_for_keep': True},
])
def test_unknown_or_rewritten_taxonomy_rejected_before_scope_creation(tmp_path, bad_policy):
    cfg = daily.load(daily.CONFIG)
    cfg['collection_mode'] = 'direct-browser-v1'
    cfg['style_tagging'] = bad_policy
    with pytest.raises(ValueError, match='style tagging policy'):
        daily.prepare('2026-10-26', tmp_path, cfg)
    assert not (tmp_path / 'scope.json').exists()
