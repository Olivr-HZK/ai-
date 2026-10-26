import copy
import hashlib
import json
import zipfile

import pytest

from ua_workflows.social_chat_selected_package import package_selection


def fixture(tmp_path):
    def media(name, data):
        (tmp_path / name).write_bytes(data)
        return {'path': name, 'sha256': hashlib.sha256(data).hexdigest()}
    video = media('male.webm', b'original-video-bytes')
    images = [media('image1.png', b'first-original-image'), media('image2.jpg', b'second-original-image')]
    common = {'source_collection': str(tmp_path), 'app_name': 'App: Male/Story', 'app_id': '',
              'platform': 'Android', 'decision': 'keep', 'review': {'decision': 'keep',
              'reason': '实际画面审核通过', 'style_tags': ['韩式漫画'], 'style_reason': '实际细腻漫画线条'}}
    return {'target_date': '2026-10-25', 'counts': {'selected': 2}, 'items': [
        {**common, 'global_rank': 1, 'ad_id': 'video-ad', 'media_type': '视频',
         'impressions_estimate': '8.5K', 'estimate_number': 8500,
         'local_source': video['path'], 'source_sha256': video['sha256']},
        {**common, 'global_rank': 2, 'ad_id': 'image-ad', 'media_type': '图片',
         'impressions_estimate': '1K', 'estimate_number': 1000, 'image_files': images},
    ]}


def test_mixed_package_contains_all_originals_and_is_idempotent(tmp_path):
    selected = fixture(tmp_path)
    receipt = package_selection(tmp_path, selected)
    assert receipt['selected'] == 2 and receipt['video_items'] == receipt['image_items'] == 1
    assert receipt['media_files'] == 3 and receipt['crc_and_media_sha256_verified']
    assert receipt['path'].endswith('Afizzy_竞品精选_2026-10-25_2条.zip')
    with zipfile.ZipFile(receipt['path']) as z:
        assert len(z.namelist()) == 6
        manifest = json.loads(z.read('校验清单.json'))
        for row in manifest['items']:
            assert row['app_id'] == ''
            for f in row['files']:
                assert hashlib.sha256(z.read(f['path'])).hexdigest() == f['sha256']
        assert manifest['items'][0]['files'][0]['path'].endswith('.webm')
        assert len(manifest['items'][1]['files']) == 2
        assert b'_source' not in z.read('校验清单.json')
    assert package_selection(tmp_path, selected)['sha256'] == receipt['sha256']
    changed = copy.deepcopy(selected)
    changed['items'][0]['review'] = {**changed['items'][0]['review'], 'reason': '经授权更新的审核说明'}
    newer = package_selection(tmp_path, changed)
    assert newer['path'].endswith('_v2.zip')
    assert hashlib.sha256(open(receipt['path'], 'rb').read()).hexdigest() == receipt['sha256']


@pytest.mark.parametrize('problem', ['excluded', 'tampered', 'rank', 'count', 'order', 'duplicate', 'escape'])
def test_unverified_or_inconsistent_selection_does_not_create_zip(tmp_path, problem):
    selected = fixture(tmp_path)
    first, second = selected['items']
    if problem == 'excluded':
        first['review'] = {**first['review'], 'decision': 'review'}
    elif problem == 'tampered':
        (tmp_path / 'male.webm').write_bytes(b'changed')
    elif problem == 'rank':
        first['global_rank'] = 9
    elif problem == 'count':
        selected['counts']['selected'] = 100
    elif problem == 'order':
        first['estimate_number'], first['impressions_estimate'] = 100, '100'
    elif problem == 'duplicate':
        selected['items'][1] = {**first, 'global_rank': 2}
    elif problem == 'escape':
        first['local_source'] = '../escape.mp4'
    with pytest.raises(ValueError):
        package_selection(tmp_path, selected)
    assert not list(tmp_path.rglob('*.zip'))


def test_zero_selection_does_not_claim_a_top100_package(tmp_path):
    assert package_selection(tmp_path, {'target_date': '2026-10-25', 'items': []}) == {'status': 'empty', 'selected': 0}
    assert not list(tmp_path.rglob('*.zip'))
