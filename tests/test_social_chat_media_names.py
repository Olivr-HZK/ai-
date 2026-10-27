from pathlib import Path

import pytest

from ua_workflows import social_chat_mixed_delivery as delivery
from ua_workflows.social_chat_media_names import media_filename


def test_upload_and_package_name_uses_target_date_rank_and_observed_advertiser():
    assert media_filename('2026-10-26', 1, 'Tipsy Chat', '.MP4') == '2026-10-26-001-Tipsy Chat.mp4'
    assert media_filename('2026-10-26', 100, 'Sea Soul: AI Chat', '.webm') == '2026-10-26-100-Sea Soul_ AI Chat.webm'
    assert media_filename('2026-10-26', 2, 'zeta', '.jpg', 1) == '2026-10-26-002-zeta-01.jpg'
    long_name=media_filename('2026-10-26', 1, '中文广告主'*100, '.png')
    assert len(long_name.encode()) < 255
    assert '/' not in media_filename('2026-10-26', 1, '../广告主/名称', '.png')


@pytest.mark.parametrize('sequence,advertiser,suffix,part', [
    (0, 'App', '.mp4', None), (True, 'App', '.mp4', None), ('1', 'App', '.mp4', None),
    (1, '', '.mp4', None), (1, 'App', '../mp4', None), (1, 'App', '.mp4', 0),
])
def test_bad_names_rejected(sequence, advertiser, suffix, part):
    with pytest.raises(ValueError):media_filename('2026-10-26', sequence, advertiser, suffix, part)


def test_named_upload_does_not_reuse_wrong_date_rank_or_legacy_token(tmp_path, monkeypatch):
    path=tmp_path/'source.webm';path.write_bytes(b'original unchanged media')
    sha=delivery.window.sha(path);cfg={'feishu':{'app_token':'isolated'}};calls=[]
    cache={'isolated|'+sha:{'file_token':'legacy-name','sha256':sha,'size':path.stat().st_size}}
    def fake_upload(client,config,path,name):calls.append(name);return 'token-'+str(len(calls))
    monkeypatch.setattr(delivery,'upload',fake_upload)
    def named(day,rank=1):return delivery.upload_selected(None,cfg,path,target_date=day,sequence=rank,
        advertiser='Tipsy Chat',source_sha256=sha,cache=cache)
    one=named('2026-10-26');assert one['name']=='2026-10-26-001-Tipsy Chat.webm'
    assert one['file_token']!='legacy-name'
    assert named('2026-10-26')==one and len(calls)==1
    assert named('2026-10-27')['file_token']!=one['file_token']
    assert named('2026-10-26',2)['file_token']!=one['file_token']
    assert len(calls)==3 and path.read_bytes()==b'original unchanged media'
    assert cache['isolated|'+sha]['file_token']=='legacy-name'


def test_attachment_name_and_exact_size_are_verified_from_remote_readback():
    expected={'key':[{'file_token':'token','name':'2026-10-26-001-Tipsy.mp4','size':12}]}
    def rows(name,size):return [{'fields':{'唯一键':'key',delivery.DEFAULT_ATTACHMENT_FIELD:[{'file_token':'token','name':name,'size':size}]}}]
    delivery.verify_attachment_names(rows('2026-10-26-001-Tipsy.mp4',12),expected)
    for name,size in [('ad-id.mp4',12),(None,12),('2026-10-26-001-Tipsy.mp4',11)]:
        with pytest.raises(ValueError,match='name/size mismatch'):
            delivery.verify_attachment_names(rows(name,size),expected)
