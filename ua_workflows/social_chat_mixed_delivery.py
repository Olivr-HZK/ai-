"""Validated mixed-media delivery and authorized Feishu synchronization."""
from __future__ import annotations
from ua_workflows.collection_runtime import collection_phase
import argparse
import hashlib
import html
import json
import math
import mimetypes
import os
import re
import uuid
from pathlib import Path
from ua_workflows import social_chat_browser_daily as daily
from ua_workflows import social_chat_mixed_window as window

EXTRA_FIELDS = [('素材类型', 3), ('图片链接', 15), ('图片附件', 17), ('视频附件', 17), ('风格标签', 4)]


def extra_fields(selection):
    return [(n, k) for n, k in EXTRA_FIELDS if n != '风格标签' or selection.get('style_tagging') is not None]

def options(value):
    values = value if isinstance(value, list) else [value]
    return list(dict.fromkeys(p.strip() for v in values for p in re.split(r'[/、,，;；\n]+', str(v or '')) if p.strip()))


def is_daily_row(row, target=None):
    """Only this automatic source may be updated or retired by daily runs."""
    f = row['fields']
    return (daily.text_field(f.get('平台')) == '广大大'
            and daily.text_field(f.get('来源')) == '每日最新'
            and (target is None or daily.text_field(f.get('目标日期')) == target))


def guard_manual_collisions(existing, desired):
    for fields in desired:
        row = existing.get(fields['唯一键'])
        window.require(row is None or is_daily_row(row), 'Automatic key collides with protected manual/external record')


def fields(selection, card):
    image = card['media_type'] == '图片'
    # Reuse common business fields, then replace the media-specific wording/link.
    common = daily.record_fields(selection, {**card, 'video_url': card.get('video_url') or card.get('external_url') or card['detail_url']})
    common['素材类型'] = card['media_type']
    common['投放地区'] = options(card.get('regions', []))
    common['投放渠道'] = options(card.get('channel', ''))
    common['文案语言'] = options(card.get('language', ''))
    if selection.get('style_tagging') is not None:
        from ua_workflows.social_chat_style_tags import validate_style
        common['风格标签'] = validate_style(selection, card.get('review', {}))
        common['审核说明'] += '；风格依据：' + card['review']['style_reason']
    if image:
        common['视频链接'] = None
        common['图片链接'] = {'text': '查看原图', 'link': card['image_files'][0]['url']}
        common['非真人口播判断'] = '不适用（静态图片）'
        common['审核方式'] = 'original_image；全部原图已查看'
        common['视频附件'] = []
    else:
        common['图片链接'] = None
        common['图片附件'] = []
        # YouTube-resolved media keeps the observed original external URL.
        url = card.get('video_url') or card.get('external_url')
        if not url:
            externals = card.get('links') or card.get('observed_external_links') or []
            url = next((x.get('href') if isinstance(x, dict) else x for x in externals if 'youtu' in str(x)), None)
        common['视频链接'] = {'text': '查看视频来源', 'link': url} if url else None
    return common

def verify(rows, desired):
    by_key = {}
    kinds = dict(daily.FIELDS + EXTRA_FIELDS)
    for row in rows:
        key = daily.text_field(row['fields'].get('唯一键'))
        if key:
            window.require(key not in by_key, 'Duplicate remote unique key')
            by_key[key] = row['fields']
    for expected in desired:
        actual = by_key.get(expected['唯一键'], {})
        for name, value in expected.items():
            got = actual.get(name)
            kind = kinds[name]
            if kind in (1, 3):
                same = daily.text_field(got) == value
            elif kind == 4:
                same = sorted(got or []) == sorted(value)
            elif kind == 15:
                same = not got if value is None else isinstance(got, dict) and got.get('link') == value['link']
            elif kind == 17:
                same = {x['file_token'] for x in (got or [])} == {x['file_token'] for x in value}
                same = same and all(x.get('size', 0) > 0 for x in (got or []))
            elif kind == 2:
                try:
                    same = math.isfinite(float(got)) and math.isclose(float(got), value, rel_tol=1e-12)
                except (TypeError, ValueError):
                    same = False
            else:
                same = got is value
            window.require(same, f'Remote mismatch: {name} / {expected["唯一键"]}')


def upload(client, config, path, name):
    with path.open('rb') as f:
        response = client.session.post('https://open.feishu.cn/open-apis/drive/v1/medias/upload_all',
            data={'file_name': name, 'parent_type': 'bitable_file', 'parent_node': config['feishu']['app_token'], 'size': str(path.stat().st_size)},
            files={'file': (name, f, mimetypes.guess_type(name)[0] or 'application/octet-stream')}, timeout=180)
    body = response.json()
    window.require(response.ok and body.get('code') == 0 and body.get('data', {}).get('file_token'), 'Attachment upload failed: ' + str(body.get('code')))
    return body['data']['file_token']


def upload_selected(client, config, path, *, target_date, sequence, advertiser, source_sha256, cache, part=None):
    """Reusable by plugin handoff and mixed daily delivery; cache includes the filename."""
    from ua_workflows.social_chat_media_names import media_filename
    path = Path(path)
    window.require(window.sha(path) == source_sha256, 'Media changed before upload')
    name = media_filename(target_date, sequence, advertiser, path.suffix, part)
    key = config['feishu']['app_token'] + '|' + source_sha256 + '|' + name
    if key not in cache:
        token = upload(client, config, path, name)
        cache[key] = {'file_token': token, 'sha256': source_sha256, 'size': path.stat().st_size, 'name': name}
    saved = cache[key]
    window.require(saved.get('name') == name and saved.get('sha256') == source_sha256
                   and saved.get('size') == path.stat().st_size and saved.get('file_token'), 'Named attachment cache mismatch')
    return {'file_token': saved['file_token'], 'name': name, 'size': saved['size']}


def verify_attachment_names(rows, expected):
    """expected maps record unique key to its selected named attachment receipts."""
    by_key = {daily.text_field(row['fields'].get('唯一键')): row['fields'] for row in rows}
    for key, desired in expected.items():
        actual = by_key.get(key, {})
        found = {item['file_token']: item for field in ('视频附件', '图片附件') for item in (actual.get(field) or [])}
        for item in desired:
            got = found.get(item['file_token'], {})
            window.require(got.get('name') == item['name'] and got.get('size') == item['size'],
                           'Remote attachment name/size mismatch: ' + key)


def delivery(base, result, candidates):
    out = base / 'delivery'; out.mkdir(exist_ok=True)
    daily.save(out / 'all_candidates.json', candidates)
    daily.save(out / 'selection.json', result)
    cards = []
    for c in result['items']:
        root = Path(c['source_collection']); ad = c['ad_id']; media_dir = out / 'media'; media_dir.mkdir(exist_ok=True)
        paths = [(root / i['path'], i['format'].lower()) for i in c['image_files']] if c['media_type'] == '图片' else [(root / c['local_source'], 'mp4')]
        links = []
        for n, (source, ext) in enumerate(paths, 1):
            target = media_dir / f'{c["task_id"]}-{ad}-{n}.{ext}'
            if not target.exists(): os.link(source, target)
            window.require(window.sha(source) == window.sha(target), 'Delivery media hash mismatch')
            links.append('media/' + target.name)
        if c['media_type'] == '图片':
            media = ''.join(f'<img loading="lazy" src="{html.escape(p)}">' for p in links)
        else:
            frame = daily.load(root / f'evidence/frames/{ad}/manifest.json')['frames'][0]
            source_poster = root / frame['path']
            poster = media_dir / f'{c["task_id"]}-{ad}-poster.png'
            if not poster.exists(): os.link(source_poster, poster)
            window.require(window.sha(poster) == frame['sha256'], 'Delivery poster changed')
            media = f'<video controls preload="none" poster="media/{poster.name}" src="{html.escape(links[0])}"></video>'
        style_text = ''
        if result.get('style_tagging') is not None:
            from ua_workflows.social_chat_style_tags import validate_style
            tags = validate_style(result, c.get('review', {}))
            style_text = '<p>风格：' + html.escape('、'.join(tags)) + '</p>'
        cards.append(f'<article>{media}<h3>{html.escape(c["app_name"])}</h3><p>{c["platform"]} · {c["media_type"]} · 展示估值 {html.escape(str(c["impressions_estimate"]))}</p>{style_text}<p>{html.escape(c["app_id"])}</p><a href="{html.escape(c["detail_url"])}" target="_blank">广大大详情</a></article>')
    counts = result['counts']
    from ua_workflows.social_chat_ranking import label as ranking_label
    ranking_text = ranking_label(result.get('ranking'))
    page = '<!doctype html><html lang="zh"><meta charset="utf-8"><title>竞品素材精选</title><style>body{background:#f4f5f7;color:#20252d;font:16px system-ui;margin:32px}main{display:grid;grid-template-columns:repeat(auto-fill,minmax(270px,1fr));gap:20px}article{background:white;border-radius:14px;padding:16px;overflow:hidden}img,video{width:100%;height:350px;object-fit:contain;background:#111;border-radius:8px}p{overflow-wrap:anywhere}</style>'
    page += f'<h1>{result["target_date"]} 竞品素材 · 展示估值{ranking_text}</h1><p>21项全部完成；视频 {counts.get("视频_new", 0)} 条，图片 {counts.get("图片_new", 0)} 条；可用 {counts["eligible"]} 条，入选 {counts["selected"]} 条。重投排除 {counts.get("relaunch", 0)} 条，真人口播排除 {counts.get("exclude", 0)} 条，无法获取 {counts.get("unavailable", 0)} 条；另{counts.get("out_of_scope_playable", 0)}条试玩不在本次范围。视频为六帧画面初筛，未完整审听。</p><main>' + ''.join(cards) + '</main></html>'
    (out / 'index.html').write_text(page)
    from ua_workflows.social_chat_selected_package import package_selection
    package = package_selection(base, result, out)
    if package['status'] == 'packaged':
        from urllib.parse import quote
        link = '<p><a download href="' + quote(Path(package['path']).name) + '">下载本日入选素材ZIP（' + str(package['selected']) + '条）</a></p>'
        (out / 'index.html').write_text(page.replace('<main>', link + '<main>', 1))
    return out


def sync(run_dir, config, client=None, apply=False):
    base = Path(run_dir).resolve()
    result = window.select(base, config)  # Always revalidate actual evidence.
    candidates = daily.load(base / 'all_candidates.json')
    out = delivery(base, result, candidates)
    desired = [fields(result, c) for c in result['items']]
    daily.save(base / 'feishu-pending.json', {'target_date': result['target_date'], 'records': desired})
    if not apply:
        return {'status': 'prepared', 'selected': len(desired), 'delivery': str(out)}
    client = client or daily.Feishu()
    daily.provision(config, client)
    route = daily.table_route(config)
    actual_fields = {x['field_name']: x for x in client.listing(route + '/fields')}
    required_extra = extra_fields(result)
    for name, kind in required_extra:
        if name not in actual_fields:
            client.call('POST', route + '/fields', json={'field_name': name, 'type': kind})
        else:
            window.require(actual_fields[name]['type'] == kind, 'Unexpected field type: ' + name)
    confirmed = {x['field_name']: x['type'] for x in client.listing(route + '/fields')}
    window.require(all(confirmed.get(n) == k for n, k in required_extra), 'Mixed field readback mismatch')
    all_rows = client.listing(route + '/records')
    existing = {}
    for row in all_rows:
        key = daily.text_field(row['fields'].get('唯一键'))
        if key:
            window.require(key not in existing, 'Duplicate remote unique key')
            existing[key] = row
    guard_manual_collisions(existing, desired)
    receipt_path = base / 'attachment-uploads.json'
    cache = daily.load(receipt_path) if receipt_path.exists() else {}
    named_attachments = {}
    for sequence, (card, f) in enumerate(zip(result['items'], desired), 1):
        root = Path(card['source_collection'])
        image = card['media_type'] == '图片'
        sources = [(root / i['path'], i['sha256'], i['format'].lower()) for i in card['image_files']] if image else [(root / card['local_source'], card['source_sha256'], 'mp4')]
        tokens = []
        for n, (path, digest, ext) in enumerate(sources, 1):
            named = upload_selected(client, config, path, target_date=result['target_date'], sequence=sequence,
                advertiser=card['app_name'], source_sha256=digest, cache=cache, part=n if len(sources) > 1 else None)
            daily.save(receipt_path, cache)
            named_attachments.setdefault(f['唯一键'], []).append(named)
            tokens.append({'file_token': named['file_token']})
        f['图片附件' if image else '视频附件'] = tokens
    daily.save(base / 'feishu-payload.json', {'records': desired})
    creates = [{'fields': f} for f in desired if f['唯一键'] not in existing]
    updates = [{'record_id': existing[f['唯一键']]['record_id'], 'fields': f} for f in desired if f['唯一键'] in existing]
    for start in range(0, len(creates), 100):
        chunk = creates[start:start + 100]; digest = hashlib.sha256(json.dumps(chunk, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        client.call('POST', route + '/records/batch_create', params={'client_token': str(uuid.UUID(hex=digest[:32], version=4))}, json={'records': chunk})
    for start in range(0, len(updates), 100):
        client.call('POST', route + '/records/batch_update', json={'records': updates[start:start + 100]})
    verified = client.listing(route + '/records'); verify(verified, desired)
    verify_attachment_names(verified, named_attachments)
    keys = {f['唯一键'] for f in desired}
    retire = [{'record_id': r['record_id'], 'fields': {'当前入选': False}} for r in verified if is_daily_row(r, result['target_date']) and r['fields'].get('当前入选') is True and daily.text_field(r['fields'].get('唯一键')) not in keys]
    for start in range(0, len(retire), 100):
        client.call('POST', route + '/records/batch_update', json={'records': retire[start:start + 100]})
    final = client.listing(route + '/records'); verify(final, desired)
    verify_attachment_names(final, named_attachments)
    daily.save(base / 'attachment-name-readback.json', {'verified': True, 'records': named_attachments})
    active = [r for r in final if is_daily_row(r, result['target_date']) and r['fields'].get('当前入选') is True]
    window.require(len(active) == len(keys) and {daily.text_field(r['fields'].get('唯一键')) for r in active} == keys, 'Remote active set mismatch')
    report = {'status': 'synced', 'created': len(creates), 'updated': len(updates), 'retired': len(retire), 'verified_active': len(active), 'attachment_names_verified': True, 'url': daily.table_url(config)}
    daily.save(base / 'feishu-sync.json', report)
    from collections import Counter
    daily.save(base / 'summary.json', {'producer': result['producer'], 'target_date': result['target_date'], 'feishu_written': True, 'ranking_computed': True, 'ranking': result.get('ranking'), 'combined_platform_tasks': result['task_count'], 'combined_target_videos': len(candidates), 'combined_counts': dict(Counter(c['decision'] for c in candidates)), 'out_of_scope_playable': result['counts'].get('out_of_scope_playable', 0), 'counts': result['counts'], 'feishu': report})
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__);p.add_argument('--run-dir', required=True);p.add_argument('--apply', action='store_true');a=p.parse_args()
    with collection_phase(a.run_dir, 'social_chat_mixed_delivery'):
        import fcntl
        with (Path(a.run_dir) / '.mixed-sync.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            from ua_workflows.social_chat_daily_state import tracked
            with tracked(a.run_dir, 'SYNC_PENDING' if a.apply else 'DELIVERY_PENDING'):
                print(json.dumps(sync(a.run_dir, daily.load(daily.CONFIG), apply=a.apply), ensure_ascii=False))

if __name__ == '__main__': main()
