import hashlib
import json
from pathlib import Path
import zipfile

import pytest

from ua_workflows.social_chat_clip_vectors import package, read_bundle
from ua_workflows.social_chat_plugin_review_plan import prepare_queue


def fixture(tmp_path):
    cache=tmp_path/'embeddings';cache.mkdir()
    provenance={'model':'sentence-transformers/clip-ViT-B-32','revision':'isolated-fixture','files_sha256':{'weights':'fixture'}}
    (tmp_path/'model-provenance.json').write_text(json.dumps(provenance))
    fingerprint=hashlib.sha256(json.dumps(provenance,sort_keys=True).encode()).hexdigest()
    image=hashlib.sha256(b'image-fixture').hexdigest()
    (cache/(image+'.json')).write_text(json.dumps({'model_fingerprint':fingerprint,'image_sha256':image,'vector':[1.0]+[0.0]*511}))
    rows=[{'id':'history','day':'2026-10-24','app':'Product','os':'Android','app_id':'app.id','ad_id':'ad-history',
           'kind':'图片','value':123,'proxy':{'path':'/deliberately/missing/image.png','sha256':image},
           'sources':[{'path':'/not/packed/media.png','sha256':image}]}]
    index=tmp_path/'index.json';index.write_text(json.dumps(rows))
    return cache,index,rows,image,fingerprint


def test_portable_pack_roundtrip_has_no_source_paths_and_is_idempotent(tmp_path):
    cache,index,rows,image,fingerprint=fixture(tmp_path)
    first=package(index,cache,'2026-10-25',tmp_path/'out')
    second=package(index,cache,'2026-10-25',tmp_path/'out')
    assert first['path']==second['path'] and first['sha256']==second['sha256']
    bundle=read_bundle(first['path'],fingerprint)
    assert bundle['vectors']['history']==[1.0]+[0.0]*511
    assert bundle['records'][0]['proxy']=={'sha256':image}
    assert 'sources' not in bundle['records'][0]
    with zipfile.ZipFile(first['path']) as archive:
        assert archive.getinfo('vectors.f32').file_size==512*4
        assert b'/deliberately' not in archive.read('records.json')


def test_history_pack_dedupes_without_historical_image_or_media(tmp_path):
    cache,index,rows,image,fingerprint=fixture(tmp_path)
    receipt=package(index,cache,'2026-10-25',tmp_path/'out')
    # The actual current image is available; no historical paths exist.
    current_image=tmp_path/'current.png';current_image.write_bytes(b'image-fixture')
    current={**rows[0],'id':'today','day':'2026-10-25',
             'proxy':{'path':str(current_image),'sha256':image},
             'sources':[{'path':str(current_image),'sha256':image}]}
    index.write_text(json.dumps([current]))
    output=tmp_path/'plan.json'
    result=prepare_queue('2026-10-25',index,cache,output,history_vector_pack=receipt['path'])
    assert result['counts']=={'crossday_removed':1} and not result['review_certified']
    assert json.loads(output.read_text())['review_queue']==[]
    assert result['history_vector_pack']['images_required'] is False


def test_changed_payload_or_model_is_rejected(tmp_path):
    cache,index,*_=fixture(tmp_path);receipt=package(index,cache,'2026-10-25',tmp_path/'out')
    with pytest.raises(ValueError,match='fingerprints differ'):read_bundle(receipt['path'],'another-model')
    broken=tmp_path/'broken.zip'
    with zipfile.ZipFile(receipt['path']) as src, zipfile.ZipFile(broken,'w') as dst:
        for name in src.namelist():
            content=src.read(name)
            if name=='vectors.f32':content=b'X'+content[1:]
            dst.writestr(name,content)
    with pytest.raises(ValueError,match='checksum mismatch'):read_bundle(broken)
