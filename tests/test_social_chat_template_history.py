import copy
import hashlib
import io
import json
from pathlib import Path
import pytest
from PIL import Image
from ua_workflows import social_chat_template_fallback as fallback
from ua_workflows.social_chat_template_history import confirmed_templates, history_state, identity, policy_for, representatives, load_confirmed_history, VERSION, LEGACY_VERSION
from ua_workflows import social_chat_browser_daily as daily


def test_verified_empty_history_becomes_new_baseline_but_errors_do_not():
    assert history_state([],source_state='verified')['status']=='new_baseline'
    assert history_state([],source_state='verified_no_prior_batch')['status']=='new_baseline'
    assert history_state([],source_state='missing_file')['status']=='pending'
    assert history_state([],source_state='verified',identity_verified=False)['status']=='pending'
    assert history_state([],source_state='verified',unreviewed_candidates=True)['status']=='pending'


def test_confirmed_template_does_not_require_previous_dedupe_or_ingestion_success():
    rows=[{'id':'prior','decision':'keep','visually_inspected':True,
           'template_review':{'relation':'distinct'},'previous_day_template_review':{'relation':'pending'}}]
    assert confirmed_templates(rows)==rows
    assert history_state(confirmed_templates(rows),source_state='verified')['status']=='compare_previous'
    assert not confirmed_templates([{'visually_inspected':True,'template_review':{'relation':'uncertain'}}])


def test_product_template_representatives_are_stable_and_keep_oses_separate():
    rows=[{'id':'a','ad_id':'2','product':'Tizz','os':'Android','template_key':'jealous','estimate_number':4400},
          {'id':'b','ad_id':'1','product':'Tizz','os':'Android','template_key':'jealous','estimate_number':4400},
          {'id':'c','ad_id':'3','product':'Tizz','os':'iOS','template_key':'jealous','estimate_number':4300}]
    kept,removed=representatives(rows)
    assert {r['id'] for r in kept}=={'b','c'} and removed=={'a':'b'}


def test_alias_identity_is_explicit_and_conflicting_ids_are_blocked():
    mapping={'products':[{'name':'Tizz','apps':{'Android':{'names':['TizzChat','Tizz','pkg.tizz'],'app_ids':['pkg.tizz']}}}]}
    assert identity('TizzChat','Android',mapping)==identity('pkg.tizz','Android',mapping)==('Tizz','Android')
    assert identity('TizzChat','iOS',mapping)==('TizzChat','iOS')
    with pytest.raises(ValueError):identity('TizzChat','Android',mapping,'different.package')


def test_new_policy_and_identity_map_do_not_migrate_old_scopes(tmp_path):
    config=daily.load(daily.CONFIG);old=copy.deepcopy(config)
    old['plugin_handoff']['previous_day_template_dedupe']=policy_for(LEGACY_VERSION)
    d=tmp_path/'old';daily.prepare('2026-11-03',d,old);before=(d/'scope.json').read_bytes()
    daily.prepare('2026-11-03',d,config);assert (d/'scope.json').read_bytes()==before
    n=tmp_path/'new';daily.prepare('2026-11-04',n,config);scope=daily.load(n/'scope.json')
    assert scope['previous_day_template_dedupe']['version']==VERSION
    assert scope['template_identity_mapping']['products'][0]['name']=='Tizz'
    assert scope['template_history_fallback']==fallback.FEISHU_FALLBACK
    assert 'social_chat_template_fallback' in (n/'handoff-task.md').read_text()


def test_revised_history_is_preferred_and_bad_hash_is_not_zero_history(tmp_path):
    original={'items':[]};(tmp_path/'review_observations.json').write_text(json.dumps(original))
    payload={'original_review_sha256':hashlib.sha256((tmp_path/'review_observations.json').read_bytes()).hexdigest(),
             'records':[{'id':'confirmed','visually_inspected':True,'template_review':{'relation':'distinct'},'previous_day_template_review':{'relation':'pending'}}]}
    revision=tmp_path/'revised.json';revision.write_text(json.dumps(payload))
    pointer={'path':'revised.json','sha256':hashlib.sha256(revision.read_bytes()).hexdigest()}
    (tmp_path/'template-history-latest.json').write_text(json.dumps(pointer))
    assert load_confirmed_history(tmp_path)[0]['id']=='confirmed'
    pointer['sha256']='bad';(tmp_path/'template-history-latest.json').write_text(json.dumps(pointer))
    with pytest.raises(ValueError,match='hash mismatch'):load_confirmed_history(tmp_path)


def png(color):
    stream = io.BytesIO()
    Image.new('RGB', (16, 24), color).save(stream, format='PNG')
    return stream.getvalue()


def feishu_row(ad_id, *, os_name='iOS', day='2026-11-08', **changes):
    fields = {'目标日期': day, '首次发现日期': day, '广告主': 'TizzChat: Craft Your World',
              '平台': '广大大', '来源': '每日最新', '广告ID': ad_id, '素材类型': '图片',
              '唯一键': f'广大大独立|{day}|{"f" * 20}|{os_name}|{ad_id}', '当前入选': True}
    fields.update(changes)
    return {'record_id': 'rec_' + ad_id, 'fields': fields}


class ReadOnlyTable:
    field = {'field_id': 'fldTest', 'field_name': '原素材现用列', 'type': 17}

    def __init__(self, rows, images):
        self.rows = rows
        self.images = images
        self.downloads = []
        self.session = self

    def listing(self, route):
        assert route.endswith('/fields')
        return [self.field, *({'field_name': name} for name in fallback.FIELDS)]

    def call(self, method, route, **options):
        assert method == 'POST' and route.endswith('/records/search')
        assert options['json']['filter']['conditions'][0]['value'] == ['2026-11-08']
        assert self.field['field_name'] in options['json']['field_names']
        if 'page_token' not in options['params'] and len(self.rows) > 1:
            return {'items': self.rows[:1], 'has_more': True, 'page_token': 'next'}
        return {'items': self.rows[1:] if 'page_token' in options['params'] else self.rows}

    def get(self, url, *, params, stream, timeout, allow_redirects):
        token = url.split('/')[-2]
        assert url.startswith('https://open.feishu.cn/open-apis/drive/v1/medias/')
        assert stream and not allow_redirects
        scope = json.loads(params['extra'])['bitablePerm']['attachments'][self.field['field_id']]
        assert list(scope.values()) == [[token]]
        self.downloads.append(token)
        body = self.images[token]

        class Response:
            status_code = 200

            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

            def iter_content(self, chunk_size):
                yield body

        return Response()

    def attach(self, row, tokens):
        row['fields'][self.field['field_name']] = [
            {'file_token': token, 'name': token + '.png', 'size': len(self.images[token])}
            for token in tokens]


@pytest.fixture
def fallback_run(tmp_path):
    config = daily.load(daily.CONFIG)
    config['feishu']['attachment_field_id'] = ReadOnlyTable.field['field_id']
    daily.prepare('2026-11-09', tmp_path, config)
    return tmp_path


def test_feishu_only_candidates_use_exact_date_product_and_os_and_stay_partial(fallback_run):
    good = feishu_row('good')
    rows = [good, feishu_row('other_os', os_name='Android'), feishu_row('older', day='2026-11-07'),
            feishu_row('unbound', 唯一键='plugin-reviewed|2026-11-08|batch|unbound'),
            feishu_row('manual', 来源='手动添加'), feishu_row('other_app', 广告主='Other App'),
            feishu_row('ambiguous'), feishu_row('ambiguous')]
    table = ReadOnlyTable(rows, {'good_image': png('blue')})
    table.attach(good, ['good_image'])
    path, result = fallback.prepare(fallback_run, 'Tizz: Craft Your World', 'iOS', client=table)
    assert table.downloads == ['good_image']
    assert daily.load(path) == result
    assert result['coverage'] == 'partial' and result['mode'] == 'compare_only'
    assert result['history_state']['no_match_status'] == 'pending'
    assert [row['ad_id'] for row in result['references']] == ['good']
    assert result['issues'][0]['ad_id'] == 'ambiguous'
    source = result['references'][0]['sources'][0]
    assert hashlib.sha256(Path(source['path']).read_bytes()).hexdigest() == source['sha256']
    assert fallback.history_state([], source_state='feishu_partial')['status'] == 'pending'


@pytest.mark.parametrize('hash_mismatch', [False, True])
def test_restore_requires_all_original_image_hashes_before_publishing(fallback_run, hash_mismatch):
    row = feishu_row('original', 当前入选=False)
    table = ReadOnlyTable([row], {'one': png('blue'), 'two': png('red')})
    table.attach(row, ['one', 'two'])
    expected = [hashlib.sha256(table.images[k]).hexdigest() for k in ('two', 'one')]
    if hash_mismatch:
        expected[0] = '0' * 64
    receipt, result = fallback.prepare(fallback_run, 'Tizz: Craft Your World', 'iOS',
                                       ad_id='original', expected=expected, client=table)
    assert result['mode'] == 'restore_original'
    if hash_mismatch:
        assert result['references'] == [] and result['history_state']['status'] == 'pending'
        assert not (receipt.parent / row['record_id']).exists()
    else:
        assert {s['sha256'] for s in result['references'][0]['sources']} == set(expected)
