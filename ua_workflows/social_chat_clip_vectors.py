"""Portable history-only CLIP vectors. No images, media, credentials or DB export."""
import argparse
from datetime import date
import hashlib
import json
import math
from pathlib import Path
import struct
import tempfile
import zipfile

FORMAT = 'social-chat-clip-vectors/v1'
DIMENSIONS = 512
FILES = ('records.json', 'image-sha256.json', 'vectors.f32', 'model.json')


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2).encode()


def digest(content):
    return hashlib.sha256(content).hexdigest()


def read_bundle(path, expected_model_fingerprint=None):
    with zipfile.ZipFile(path) as archive:
        if sorted(archive.namelist()) != sorted((*FILES, 'manifest.json', 'README.md')):
            raise ValueError('Unexpected/duplicate vector bundle members')
        manifest = json.loads(archive.read('manifest.json'))
        if manifest.get('format') != FORMAT or manifest.get('dimensions') != DIMENSIONS:
            raise ValueError('Unsupported vector bundle format/dimensions')
        contents = {}
        for name in FILES:
            contents[name] = archive.read(name)
            meta = manifest['files'][name]
            if len(contents[name]) != meta['bytes'] or digest(contents[name]) != meta['sha256']:
                raise ValueError('Vector bundle checksum mismatch: ' + name)
        if archive.testzip() is not None:
            raise ValueError('Vector bundle CRC mismatch')
    model = json.loads(contents['model.json'])
    fingerprint = model['model_fingerprint']
    if digest(json.dumps(model['provenance'], sort_keys=True).encode()) != fingerprint:
        raise ValueError('Model provenance fingerprint mismatch')
    if expected_model_fingerprint and fingerprint != expected_model_fingerprint:
        raise ValueError('History and current CLIP model fingerprints differ')
    images = json.loads(contents['image-sha256.json'])
    records = json.loads(contents['records.json'])
    if len(images) != len(set(images)) or len(images) != manifest['vectors'] or len(records) != manifest['records']:
        raise ValueError('Vector bundle counts/identities mismatch')
    if len({r['id'] for r in records}) != len(records):
        raise ValueError('Duplicate vector record identities')
    data = contents['vectors.f32']
    if len(data) != len(images)*DIMENSIONS*4:
        raise ValueError('Float32 vector payload size mismatch')
    by_image = {}
    for n, image_sha in enumerate(images):
        if len(image_sha) != 64 or any(c not in '0123456789abcdef' for c in image_sha):
            raise ValueError('Invalid image SHA256 identity')
        vector = list(struct.unpack_from('<512f', data, n*DIMENSIONS*4))
        if not all(math.isfinite(v) for v in vector) or not any(vector):
            raise ValueError('Non-finite/zero history vector')
        by_image[image_sha] = vector
    vectors = {}
    for row in records:
        date.fromisoformat(row['day'])
        vectors[row['id']] = by_image[row['proxy']['sha256']]
    return {'manifest': manifest, 'model': model, 'records': records, 'vectors': vectors}


def package(index, cache, target, output_dir):
    index, cache, output_dir = Path(index), Path(cache), Path(output_dir)
    raw = json.loads(index.read_text())
    rows = raw if isinstance(raw, list) else raw['records']
    target_date = date.fromisoformat(target)
    rows = sorted((r for r in rows if 0 <= (target_date-date.fromisoformat(r['day'])).days <= 7), key=lambda r:r['id'])
    if not rows or len({r['id'] for r in rows}) != len(rows):
        raise ValueError('Empty or duplicate normalized input records')
    provenance = json.loads((cache.parent/'model-provenance.json').read_text())
    fingerprint = digest(json.dumps(provenance, sort_keys=True).encode())
    if provenance.get('model') != 'sentence-transformers/clip-ViT-B-32':
        raise ValueError('Unverified CLIP model name')
    images = sorted({r['proxy']['sha256'] for r in rows})
    payload = bytearray()
    for image_sha in images:
        blob = json.loads((cache/(image_sha+'.json')).read_text())
        v = blob['vector']
        if blob.get('image_sha256') != image_sha or blob.get('model_fingerprint') != fingerprint:
            raise ValueError('Image/model-bound cache mismatch')
        if len(v) != DIMENSIONS or not all(isinstance(x,(int,float)) and math.isfinite(x) for x in v) or not any(v):
            raise ValueError('Invalid CLIP vector')
        payload.extend(struct.pack('<512f', *v))
    fields = ('id','day','app','os','app_id','ad_id','review_id','kind','estimate','value','relaunch')
    records = []
    for r in rows:
        record = {k:r[k] for k in fields if k in r}
        record['proxy'] = {'sha256':r['proxy']['sha256']}
        record['source_sha256s'] = [s['sha256'] for s in r.get('sources',[])]
        records.append(record)
    contents = {'records.json':encoded(records), 'image-sha256.json':encoded(images),
                'vectors.f32':bytes(payload), 'model.json':encoded({'model_fingerprint':fingerprint,'provenance':provenance})}
    dates = sorted({r['day'] for r in rows})
    manifest = {'format':FORMAT,'dimensions':DIMENSIONS,'dtype':'float32-little-endian',
                'records':len(records),'vectors':len(images),'target_date':target,'present_dates':dates,
                'source':'fresh_local_CLIP_cache','legacy_ve_arrow2_vectors_merged':False,
                'contains_images_or_videos':False,'paths_in_records':'none',
                'files':{name:{'sha256':digest(value),'bytes':len(value)} for name,value in contents.items()}}
    manifest_bytes = encoded(manifest)
    output_dir.mkdir(parents=True,exist_ok=True)
    stem = f'Afizzy_CLIP历史向量_{dates[0]}_{dates[-1]}_{len(records)}条'
    destination = output_dir/(stem+'.zip');version=1
    while destination.exists():
        if read_bundle(destination)['manifest'] == manifest:
            break
        version += 1;destination=output_dir/(stem+f'_v{version}.zip')
    if not destination.exists():
        with tempfile.NamedTemporaryFile(dir=output_dir,suffix='.partial',delete=False) as temp:
            temporary=Path(temp.name)
        try:
            with zipfile.ZipFile(temporary,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=6) as archive:
                for name, value in contents.items():archive.writestr(name,value)
                archive.writestr('manifest.json',manifest_bytes)
                archive.writestr('README.md',f'''# CLIP历史向量包

{len(records)}条素材索引，{len(images)}份512维CLIP向量。实际日期：{', '.join(dates)}。
本包不含媒体/代理图/凭据；没有合并旧VE或Arrow2库，也不包含文本向量。
历史去重只需本包；新素材仍须用同模型生成向量并保留原媒体和联合审核证据。
vectors.f32按image-sha256.json顺序存储little-endian float32，每条512维。
records.json用proxy.sha256关联向量，包含App/系统/日期/广告ID/估值及源哈希，无机器绝对路径。
未见重投标签不代表非重投。日期以记录为准，缺日不得当成零。
检查：python -m ua_workflows.social_chat_clip_vectors inspect /本包路径.zip
pre-review可通过--history-vector-pack读取本包；模型指纹不匹配时拒绝混用。
''')
            read_bundle(temporary)
            temporary.replace(destination)
        finally:temporary.unlink(missing_ok=True)
    receipt={'status':'packaged','path':str(destination.resolve()),'bytes':destination.stat().st_size,
             'sha256':digest(destination.read_bytes()),'records':len(records),'vectors':len(images),
             'present_dates':dates,'model_fingerprint':fingerprint}
    (output_dir/'package-receipt.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2))
    return receipt


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='command',required=True)
    pack=sub.add_parser('pack')
    for name in ('index','cache','date','output-dir'):pack.add_argument('--'+name,required=True)
    inspect=sub.add_parser('inspect');inspect.add_argument('path')
    args=parser.parse_args()
    if args.command=='pack':result=package(args.index,args.cache,args.date,args.output_dir)
    else:result=read_bundle(args.path)['manifest']
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
