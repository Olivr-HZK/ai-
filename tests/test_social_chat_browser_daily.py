import copy
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from ua_workflows import social_chat_browser_daily as daily


TARGET = '2026-10-15'


def config():
    cfg = daily.load(daily.CONFIG)
    cfg['collection_mode'] = 'direct-browser-v1'
    return cfg


def card(n, value=None, first=TARGET, app='Tipsy Chat: Live Your Story', platform='Android'):
    return {'ad_id': str(n), 'app_id': 'app-' + platform, 'app_name': app, 'platform': platform,
            'impressions_estimate': str(n) if value is None else value,
            'raw_text': '投放天数\n' + first + '~' + first, 'channel': 'TikTok', 'regions': ['美国'],
            'video_url': 'https://example.com/video.mp4', 'detail_url': 'https://example.com/ad/' + str(n)}


def reviews(cards):
    return {(c['platform'], c['app_id'], c['ad_id']): {
        'decision': 'keep', 'checks': {'real_person_talking_head': False},
        'reason': '六帧为角色动画', 'method': 'sampled_frames'} for c in cards}


def selection(cards):
    result = daily.rank(cards, reviews(cards), TARGET)
    return {**result, 'scope_sha256': 'test-scope', 'generated_at': '2026-10-16T09:00:00+08:00'}


def test_yesterday_uses_shanghai_boundary():
    assert daily.yesterday(datetime(2026, 10, 15, 17, tzinfo=timezone.utc)) == TARGET
    assert daily.yesterday(datetime(2026, 10, 15, 15, tzinfo=timezone.utc)) == '2026-10-14'


def test_prepare_plans_both_media_types_for_each_app(tmp_path):
    cfg = config()
    cfg['media_types'] = ['视频', '图片']
    daily.prepare(TARGET, tmp_path, cfg)
    scope = daily.load(tmp_path / 'scope.json')
    assert len(scope['media_collection_checks']) == 21
    assert scope['media_filter_mode'] == 'default_images_and_videos'
    for task in scope['tasks']:
        checks = [x for x in scope['media_collection_checks'] if x['task_id'] == task['id']]
        assert len(checks) == 1
        assert checks[0]['media_types'] == ['视频', '图片']
        assert checks[0]['selected_types'] == []


def test_prepare_full_platform_list_resumable_and_pending(tmp_path):
    cfg = config()
    result = daily.prepare(TARGET, tmp_path, cfg)
    assert result['tasks'] == 21
    scope = daily.load(tmp_path / 'scope.json')
    assert [t['platform'] for t in scope['tasks']].count('iOS') == 10
    assert [t['platform'] for t in scope['tasks']].count('Android') == 11
    assert len([t for t in scope['tasks'] if t['app_name'] == 'Tipsy Chat: Live Your Story']) == 2
    assert scope['filters']['regions'] == scope['filters']['channels'] == scope['filters']['languages'] == []
    assert scope['content_checks'] == {'real_person_talking_head': False}
    assert scope['filters']['sort'] == '最新创意'
    assert scope['tasks'][0]['app_id'] == '6744018732'
    assert daily.prepare(TARGET, tmp_path, cfg)['status'] == 'resume_existing'
    with pytest.raises(ValueError, match='different scope'):
        daily.prepare('2026-10-14', tmp_path, cfg)
    with pytest.raises(ValueError, match='not ready'):
        daily.select(tmp_path, cfg)


@pytest.mark.parametrize('n,want', [(0, 0), (1, 1), (9, 1), (10, 1), (11, 2), (21, 3)])
def test_global_top_decile_rounding(n, want):
    cards = [card(i, app='A' if i % 2 else 'B', platform='iOS' if i % 2 else 'Android') for i in range(n)]
    result = selection(cards)
    assert result['counts']['selected'] == want
    assert result['counts']['eligible'] == n
    assert [c['ad_id'] for c in result['items']] == [str(i) for i in reversed(range(n))][:want]


def test_estimate_units_and_tie_order():
    assert daily.estimate('约 25万') == Decimal(250000)
    assert daily.estimate('988K') == Decimal(988000)
    assert daily.estimate('1,234.5M') == Decimal('1234500000')
    cards = [card(1, '100K', app='Z'), card(2, '0.1M', app='A')]
    assert selection(cards)['items'][0]['app_name'] == 'A'


@pytest.mark.parametrize('value', [None, True, '--', 'NaN', '-1', '10-20', '9' * 400])
def test_unknown_or_unrepresentable_estimate_rejected(value):
    with pytest.raises(ValueError):
        daily.estimate(value)


def test_discovery_date_and_content_filter_before_denominator():
    cards = [card(1, '999M', first='2026-10-14'), card(2, '998M'), card(3, '1K')]
    checks = reviews(cards)
    checks[('Android', 'app-Android', '2')].update(decision='exclude', checks={'real_person_talking_head': True})
    result = daily.rank(cards, checks, TARGET)
    assert result['counts'] == {'raw': 3, 'other_date': 1, 'talking_head': 1, 'relaunch': 0, 'eligible': 1, 'selected': 1}
    assert result['items'][0]['ad_id'] == '3'
    c = card(1)
    c.update(raw_text='投放起始 ' + TARGET, first_seen=TARGET)
    with pytest.raises(ValueError, match='Missing visible detail date range'):
        daily.discovered(c)
    c = card(2)
    c['first_discovered_date'] = '2026-10-14'
    with pytest.raises(ValueError, match='contradicts'):
        daily.discovered(c)


def test_unresolved_content_duplicate_and_missing_estimate_block_selection():
    c = card(1)
    checks = reviews([c])
    checks[('Android', 'app-Android', '1')]['decision'] = 'review'
    with pytest.raises(ValueError, match='Unresolved'):
        daily.rank([c], checks, TARGET)
    with pytest.raises(ValueError, match='Duplicate'):
        daily.rank([c, c], reviews([c]), TARGET)
    c['impressions_estimate'] = '--'
    with pytest.raises(ValueError, match='Unusable'):
        daily.rank([c], reviews([c]), TARGET)


def test_scope_or_incomplete_app_state_blocks(tmp_path):
    cfg = config()
    daily.prepare(TARGET, tmp_path, cfg)
    scope = daily.load(tmp_path / 'scope.json')
    scope['tasks'].pop()
    daily.save(tmp_path / 'scope.json', scope)
    with pytest.raises(ValueError, match='complete confirmed'):
        daily.select(tmp_path, cfg)
    scope['tasks'] = daily.tasks(cfg)
    scope['filters']['regions'] = ['日本']
    daily.save(tmp_path / 'scope.json', scope)
    with pytest.raises(ValueError, match='filters/content/date'):
        daily.select(tmp_path, cfg)


class FakeFeishu:
    def __init__(self):
        self.fields = []
        self.rows = []
        self.calls = []
        self.drop_writes = False

    def listing(self, route):
        return copy.deepcopy(self.fields if route.endswith('/fields') else self.rows)

    def call(self, method, route, **kwargs):
        self.calls.append((method, route, kwargs))
        body = kwargs['json']
        if route.endswith('/fields'):
            self.fields.append(body)
        elif not self.drop_writes:
            for item in body['records']:
                if route.endswith('/batch_create'):
                    # Live API rejects UUID v5 tokens with code 1254037.
                    token = kwargs['params']['client_token']
                    assert uuid.UUID(token).version == 4
                    self.rows.append({'record_id': 'rec' + str(len(self.rows)), 'fields': copy.deepcopy(item['fields'])})
                else:
                    next(r for r in self.rows if r['record_id'] == item['record_id'])['fields'].update(copy.deepcopy(item['fields']))
        return {}


def test_provision_idempotent_and_type_mismatch_blocked():
    client = FakeFeishu()
    assert daily.provision(config(), client)['fields'] == 23
    assert len(client.calls) == 23
    daily.provision(config(), client)
    assert len(client.calls) == 23
    client.fields[0]['type'] = 2
    with pytest.raises(ValueError, match='Unexpected remote field type'):
        daily.provision(config(), client)


def test_verify_numeric_string_readback_preserves_value_checks():
    chosen = selection([card(12)])
    fields = daily.record_fields(chosen, chosen['items'][0])
    actual = copy.deepcopy(fields)
    for name, kind in daily.FIELDS:
        if kind == 2:
            actual[name] = str(actual[name])
    daily.verify_rows([{'fields': actual}], [fields])
    for bad in ('NaN', 'not a number', '999999', True):
        actual['展示估值'] = bad
        with pytest.raises(ValueError, match='展示估值'):
            daily.verify_rows([{'fields': actual}], [fields])


def test_table_schema_uses_appid_and_select_fields():
    c = card(12)
    c['regions'] = ['美国', '日本']
    chosen = selection([c])
    fields = daily.record_fields(chosen, chosen['items'][0])
    assert fields['appid'] == c['app_id']
    assert fields['广告主'] == c['app_name']
    assert fields['来源'] == '每日最新' and fields['平台'] == '广大大'
    assert dict(daily.FIELDS)['来源'] == 3
    assert fields['投放地区'] == ['美国', '日本']
    assert fields['投放渠道'] == ['TikTok']
    assert all(name not in fields for name in ('素材名称', '全局排名', '有效候选总数', '入选数量'))
    kinds = dict(daily.FIELDS)
    assert kinds['广告主'] == kinds['平台'] == 3
    assert kinds['投放地区'] == kinds['投放渠道'] == kinds['文案语言'] == 4
    actual = copy.deepcopy(fields)
    actual['投放地区'].reverse()
    daily.verify_rows([{'fields': actual}], [fields])
    actual['投放地区'].pop()
    with pytest.raises(ValueError, match='投放地区'):
        daily.verify_rows([{'fields': actual}], [fields])


def test_sync_upsert_readback_and_retire_only_same_day():
    client = FakeFeishu()
    old = selection([card(1)])
    assert daily.sync(old, config(), client)['created'] == 1
    assert daily.sync(old, config(), client)['created'] == 0
    assert len(client.rows) == 1
    assert client.rows[0]['fields']['视频链接']['link'].endswith('.mp4')
    # Preserve an unrelated day's active record.
    client.rows.append({'record_id': 'other', 'fields': {'唯一键': 'other-day', '目标日期': '2026-10-14', '当前入选': True}})
    new = selection([card(2)])
    result = daily.sync(new, config(), client)
    assert result['retired'] == 1
    assert client.rows[0]['fields']['当前入选'] is False
    assert client.rows[1]['fields']['当前入选'] is True
    assert result['verified_active'] == 1


def test_sync_failed_write_does_not_retire_previous_selection():
    client = FakeFeishu()
    daily.sync(selection([card(1)]), config(), client)
    client.drop_writes = True
    with pytest.raises(ValueError, match='readback mismatch'):
        daily.sync(selection([card(2)]), config(), client)
    assert client.rows[0]['fields']['当前入选'] is True


def test_readback_checks_field_values_not_just_unique_key():
    fields = daily.record_fields(selection([card(1)]), selection([card(1)])['items'][0])
    broken = {**fields, '展示估值': 99}
    with pytest.raises(ValueError, match='展示估值'):
        daily.verify_rows([{'fields': broken}], [fields])
    rich = {**fields, '广告主': [{'type': 'text', 'text': fields['广告主']}]}
    daily.verify_rows([{'fields': rich}], [fields])


def test_discovery_uses_range_start_even_when_explicit_label_is_older():
    for separator in ('~', '～', '至'):
        c = card(1)
        c.update(raw_text='投放天数\n2天\n' + TARGET + separator + '2026-10-19')
        assert daily.discovered(c) == TARGET
    c.update(raw_text='首次发现\n(UTC+8) 2026-09-01\n投放天数\n' + TARGET + '~2026-10-19')
    assert daily.discovered(c) == TARGET
    c.update(raw_text='2026-10-19~2026-10-18')
    with pytest.raises(ValueError, match='Invalid visible detail date range'):
        daily.discovered(c)


def test_relaunch_badge_not_title_substring():
    assert daily.is_relaunch({'raw_text':'创意\n重投\n标题'})
    assert not daily.is_relaunch({'raw_text':'这是重投说明文案'})
    with pytest.raises(ValueError, match='Missing visible detail date range'):
        daily.discovered({'raw_text':'首次发现\n(UTC+8) 2026-09-01'})
