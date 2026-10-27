"""Package an already-reviewed selection; never crawl, rank, or change review decisions."""
from __future__ import annotations

import argparse
import csv
from datetime import date
import hashlib
import io
import json
from pathlib import Path
import tempfile
import zipfile
from ua_workflows.social_chat_media_names import NAMING_VERSION, media_filename, media_stem


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def local_file(root, relative):
    root = Path(root).resolve()
    relative = Path(relative)
    path = (root / relative).resolve()
    if relative.is_absolute() or not path.is_relative_to(root) or not path.is_file():
        raise ValueError('Missing or out-of-root selected media')
    return path


def normalize(base, entry, rank, target_date):
    review = entry.get('review', {})
    if review.get('decision') != 'keep' or entry.get('decision', 'keep') != 'keep':
        raise ValueError('Only reviewed keep items may be packaged')
    if entry.get('rank', entry.get('global_rank', rank)) != rank:
        raise ValueError('Selection rank/order mismatch')
    if 'rows' in entry:  # Existing plugin-reviewed selection.
        row = entry['rows'][0]
        app, os_name = row['appOwnership']['name'], row['appOwnership']['platform']
        ad, app_id = row['adId'], row.get('app_id', '')
        estimate, numeric = row['displayEstimate']['raw'], row['displayEstimate']['value']
        kind = entry['kind']
        if row['relaunch']['value'] is True:
            raise ValueError('Relaunch item cannot enter a selected package')
        if kind != '视频':
            raise ValueError('Unsupported plugin media layout; normalize all original images first')
        sources = [(local_file(base, entry['path']), entry['sha256'])]
    else:  # Validated mixed-window daily selection.
        app, os_name, ad, app_id = entry['app_name'], entry['platform'], entry['ad_id'], entry['app_id']
        estimate = entry['impressions_estimate']
        numeric = entry['estimate_number']
        kind, root = entry['media_type'], Path(entry['source_collection'])
        if kind == '图片':
            sources = [(local_file(root, i['path']), i['sha256']) for i in entry['image_files']]
        elif kind == '视频':
            sources = [(local_file(root, entry['local_source']), entry['source_sha256'])]
        else:
            raise ValueError('Only video/image selections are supported')
    if not sources or not ad:
        raise ValueError('Selected item lacks media or ad identity')
    from ua_workflows.social_chat_browser_daily import estimate as parse_estimate
    if parse_estimate(numeric) != parse_estimate(estimate):
        raise ValueError('Selection estimate mismatch')
    prefix = media_stem(target_date, rank, app)
    files = []
    for n, (path, sha) in enumerate(sources, 1):
        if digest(path) != sha:
            raise ValueError('Selected media SHA256 mismatch')
        ext = path.suffix.lower()
        filename = media_filename(target_date, rank, app, ext, n if len(sources) > 1 else None)
        name = f'视频/{filename}' if kind == '视频' else (
            f'图片/{prefix}/{filename}' if len(sources) > 1 else f'图片/{filename}')
        files.append({'path': name, 'sha256': sha, 'size': path.stat().st_size, '_source': path})
    return {'rank': rank, 'app_name': app, 'os': os_name, 'app_id': app_id, 'ad_id': ad,
            'media_type': kind, 'estimate': estimate, 'estimate_number': float(numeric),
            'style_version': review.get('style_tagging_version', ''),
            'style_tags': review.get('style_tags', []), 'style_reason': review.get('style_reason', ''),
            'review_policy': review.get('review_policy', review.get('version', 'legacy')),
            'review_reason': review['reason'], 'files': files}


def package_selection(base, selection, output_dir=None):
    base = Path(base).resolve()
    target = date.fromisoformat(selection['target_date']).isoformat()
    items = [normalize(base, e, n, target) for n, e in enumerate(selection['items'], 1)]
    if not items:
        return {'status': 'empty', 'selected': 0}
    expected = selection.get('selected_count', selection.get('counts', {}).get('selected', len(items)))
    if expected != len(items):
        raise ValueError('Selection count mismatch')
    if [i['estimate_number'] for i in items] != sorted((i['estimate_number'] for i in items), reverse=True):
        raise ValueError('Selection is not in descending estimate order')
    keys = [(i['os'], i['app_id'], i['ad_id'], i['media_type']) for i in items]
    if len(keys) != len(set(keys)):
        raise ValueError('Duplicate selected identity')
    public = [{**i, 'files': [{k: v for k, v in f.items() if k != '_source'} for f in i['files']]} for i in items]
    manifest = {'version': 'selected-media-package/v1', 'naming_version': NAMING_VERSION, 'target_date': target,
                'selected': len(items), 'video_items': sum(i['media_type'] == '视频' for i in items),
                'image_items': sum(i['media_type'] == '图片' for i in items), 'items': public}
    encoded = json.dumps(manifest, ensure_ascii=False, indent=2).encode()
    fingerprint = hashlib.sha256(encoded).hexdigest()
    output = Path(output_dir) if output_dir else base / 'delivery'
    output.mkdir(parents=True, exist_ok=True)
    stem = f'Afizzy_竞品精选_{target}_{len(items)}条'
    destination = output / f'{stem}.zip'
    version = 1
    while destination.exists():
        with zipfile.ZipFile(destination) as existing:
            same = existing.read('校验清单.json') == encoded
        if same:
            break
        version += 1
        destination = output / f'{stem}_v{version}.zip'
    if not destination.exists():
        csv_out = io.StringIO(newline='')
        writer = csv.writer(csv_out)
        writer.writerow(['排名', 'App名称', '系统', 'App ID', '广告ID', '素材类型', '展示估值原文', '展示估值换算', '风格标签', '审核说明', '风格依据', '包内文件'])
        def cell(value):
            text = str(value)
            return "'" + text if text.lstrip().startswith(('=', '+', '-', '@')) else text
        for i in items:
            writer.writerow([cell(v) for v in [i['rank'], i['app_name'], i['os'], i['app_id'], i['ad_id'],
                i['media_type'], i['estimate'], i['estimate_number'], '、'.join(i['style_tags']),
                i['review_reason'], i['style_reason'], '；'.join(f['path'] for f in i['files'])]])
        note = (f'# Afizzy竞品精选 · {target}\n\n共{len(items)}条已审核入选素材：'
                f'视频{manifest["video_items"]}条、图片{manifest["image_items"]}条。按展示估值降序编号；多图素材归入同排名文件夹。\n\n'
                '仅包含本批筛选结果，不代表网站全量。保留原始媒体字节、原审核及风格版本，不重新排名或补数。'
                '视频六帧画面初筛不等于完整审听；缺App ID保持空白。清单含快照估值、风格标签及审核说明，校验清单含逐文件SHA256。\n')
        with tempfile.NamedTemporaryFile(dir=output, suffix='.partial', delete=False) as tmp:
            temporary = Path(tmp.name)
        try:
            with zipfile.ZipFile(temporary, 'w', compression=zipfile.ZIP_STORED, allowZip64=True) as archive:
                for i in items:
                    for f in i['files']:
                        archive.write(f['_source'], f['path'])
                archive.writestr('素材清单.csv', csv_out.getvalue().encode('utf-8-sig'))
                archive.writestr('打包说明.md', note)
                archive.writestr('校验清单.json', encoded)
            verify_archive(temporary, manifest)
            temporary.replace(destination)
        finally:
            temporary.unlink(missing_ok=True)
    verify_archive(destination, manifest)
    receipt = {'status': 'packaged', 'target_date': target, 'selected': len(items),
               'video_items': manifest['video_items'], 'image_items': manifest['image_items'],
               'media_files': sum(len(i['files']) for i in items), 'path': str(destination.resolve()),
               'size': destination.stat().st_size, 'sha256': digest(destination),
               'manifest_sha256': fingerprint, 'crc_and_media_sha256_verified': True}
    from ua_workflows.social_chat_browser_daily import save
    save(output / 'package-receipt.json', receipt)
    return receipt


def verify_archive(path, manifest):
    with zipfile.ZipFile(path) as archive:
        names = [f['path'] for i in manifest['items'] for f in i['files']]
        if sorted(archive.namelist()) != sorted(names + ['素材清单.csv', '打包说明.md', '校验清单.json']):
            raise ValueError('Unexpected ZIP entries')
        if json.loads(archive.read('校验清单.json')) != manifest or archive.testzip() is not None:
            raise ValueError('ZIP manifest/CRC verification failed')
        for i in manifest['items']:
            for f in i['files']:
                with archive.open(f['path']) as stream:
                    sha = hashlib.file_digest(stream, 'sha256').hexdigest()
                if sha != f['sha256'] or archive.getinfo(f['path']).file_size != f['size']:
                    raise ValueError('ZIP media bytes mismatch')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir', required=True)
    parser.add_argument('--output-dir')
    args = parser.parse_args()
    base = Path(args.run_dir)
    selection = json.loads((base / 'selection.json').read_text())
    print(json.dumps(package_selection(base, selection, args.output_dir), ensure_ascii=False))


if __name__ == '__main__':
    main()
