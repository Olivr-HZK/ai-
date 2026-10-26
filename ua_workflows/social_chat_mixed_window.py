"""Evidence validation for browser-collected target-day mixed media.

Does not drive Guangdada, query its APIs, or mark the video-only FSM passed.
"""
from __future__ import annotations

from ua_workflows.collection_runtime import collection_phase
import argparse
import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from ua_workflows import social_chat_browser_daily as daily


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def file(root, relative):
    path = (root / relative).resolve()
    require(path.is_relative_to(root.resolve()) and path.is_file() and path.stat().st_size, 'Missing/unsafe evidence: ' + str(relative))
    return path


def hashed(root, entry):
    path = file(root, entry['path'])
    require(sha(path) == entry['sha256'], 'Evidence hash mismatch: ' + str(path))
    return path


def click(root, path, fingerprint, key=None, expected=None):
    event = daily.load(file(root, path))
    require(event['producer'] == 'gdd-checked-click/v1' and event['status'] == 'passed', 'Click not passed: ' + path)
    require(event['scope_sha256'] == fingerprint and event['task_id'] == root.name, 'Click scope/task mismatch')
    if key is not None:
        require(event['key'] == key, 'Unexpected click key')
    if expected is not None:
        require(event['after'] == expected, 'Actual click readback differs')
    for evidence in event['images'].values():
        hashed(root, evidence)
    return event


def page_number(event):
    value = event['after']
    value = value.get('page') if isinstance(value, dict) else value
    return value if type(value) is int else None


def collection(run_dir, config, task_id=None):
    base = Path(run_dir).resolve()
    scope = daily.load(base / 'scope.json')
    target = scope['target_date']
    fingerprint = hashlib.sha256(json.dumps(scope, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    require(daily.load(base / 'run.json')['scope_sha256'] == fingerprint, 'Scope changed')
    expected_tasks = daily.tasks(config)
    require([(t['app_name'], t['platform']) for t in scope['tasks']] == [(t['app_name'], t['platform']) for t in expected_tasks], 'Incomplete competitor/platform scope')
    if scope.get('collection_mode')=='direct-browser-v1':
        require([(t['id'],t.get('app_id'),t.get('category',scope['filters']['category'])) for t in scope['tasks']]==[(t['id'],t.get('app_id'),t.get('category',config['filters']['category'])) for t in expected_tasks], 'Direct registered identity/category scope mismatch')
    require(scope['filters'] == {**config['filters'], 'date': {'mode': 'custom', 'start': target, 'end': target}}, 'Unexpected filter scope')
    require(scope['content_checks'] == config['content_checks'] and scope['date_semantics'] == config['date_semantics'], 'Unexpected date/content semantics')
    require(scope['media_types'] == ['视频', '图片'] and scope['media_filter_mode'] == 'default_images_and_videos', 'Not one mixed list per App')
    checks = scope['media_collection_checks']
    require(len(checks) == len(scope['tasks']) and {c['task_id'] for c in checks} == {t['id'] for t in scope['tasks']}, 'Missing media checks')
    require(all(c['selected_types'] == [] and c['media_types'] == ['视频', '图片'] for c in checks), 'Media type must remain empty/default')
    if task_id is not None:
        require(task_id in {t['id'] for t in scope['tasks']}, 'Unknown task')
    if (base/'historical-direct-recovery.json').exists():
        from ua_workflows.social_chat_direct_recovery import collection as recovered
        cards,seals=recovered(base,scope,task_id)
        return base,scope,fingerprint,cards,seals
    all_cards, checklist = [], []
    for task in scope['tasks']:
        if task_id is not None and task['id'] != task_id:
            continue
        root = base / 'tasks' / task['id']
        if scope.get('collection_mode')=='direct-browser-v1':
            from ua_workflows.social_chat_direct import validate_task
            cards,seal=validate_task(base,scope,task)
            all_cards.extend(cards);checklist.append(seal)
            continue
        actual = daily.load(file(root, 'filters-readonly.json'))
        require(actual['scope_sha256'] == fingerprint, 'Filter fingerprint mismatch')
        f = actual['actual']
        require(f['category'] == task.get('category', scope['filters']['category']) and f['dedupe'] == '素材' and f['media'] == '图片&视频', 'Base filter mismatch')
        require(f['date'] == {'start': target, 'end': target} and f['query'] == task['app_id'] and f['sort'] is True, 'Date/App/sort mismatch')
        require(all(f[k] == '' for k in ['languages', 'regions', 'size']) and all(c['id'] == 'filter_search_type' for c in f['checked']), 'Unexpected restriction')
        good = []
        for path in sorted((root / 'evidence/clicks').glob('*.json')):
            e = daily.load(path)
            if e.get('status') == 'passed':
                good.append((path, e))
        query = [click(root, str(p.relative_to(root)), fingerprint, 'app.query', task['app_id']) for p, e in good if e['key'] == 'app.query' and e['after'] == task['app_id']]
        sorts = [click(root, str(p.relative_to(root)), fingerprint, 'filter.sort', '最新创意') for p, e in good if e['key'] == 'filter.sort']
        require(query and sorts and any(s['performed_at'] >= query[-1]['performed_at'] for s in sorts), 'Missing App search followed by fresh latest sort')
        if (base/'daily-machine/state.json').exists():
            selected=daily.load(file(root,'app-selection.json'))
            model=daily.fsm();model.identity(selected,task)
            candidate=model.app_click_check(root,selected,fingerprint,task['id'])
            require(any(s['performed_at'] >= candidate['performed_at'] for s in sorts), 'Sort must follow advertiser candidate click')
        seal = daily.load(file(root, 'collection_complete.json'))
        require(all(seal[k] == task[k] for k in ['id', 'app_name', 'platform', 'app_id']), 'Seal identity mismatch')
        require(seal['target_date'] == target and seal['media_mode'] == '图片&视频', 'Seal scope mismatch')
        text = file(root, 'evidence/terminal.txt').read_text()
        require(sha(file(root, seal.get('screenshot_path', 'evidence/terminal.jpg'))) == seal['screenshot_sha256'], 'Terminal screenshot changed')
        cards = [daily.load(p) for p in sorted((root / 'cards').glob('*.json'))]
        require(len(cards) == seal['checked_cards'], 'Checked-card count mismatch')
        ids, positions = set(), []
        for c in cards:
            require(c['scope_sha256'] == fingerprint and c.get('detail_loaded') is True, 'Card scope/loading mismatch')
            require(all(c[k] == task[k] for k in ['app_name', 'app_id', 'platform']), 'Card identity mismatch')
            require(c['ad_id'] not in ids, 'Duplicate card within App/platform')
            ids.add(c['ad_id'])
            require(parse_qs(urlparse(c['detail_url']).query).get('id') == [c['ad_id']], 'Detail URL/ad mismatch')
            click(root, c['ui_check'], fingerprint, 'card.identity', {'app_id': c['app_id'], 'platform': c['platform']})
            c['first_discovered_date'] = daily.discovered(c)
            c['first_discovered_source'] = 'detail_date_range_start'
            require(c['media_type'] in ['视频', '图片', '试玩'], 'Unknown media type')
            if c['media_type'] == '试玩':
                proof = daily.load(file(root, c['media_type_recheck']['evidence']))
                require('试玩广告' in c['raw_text'] and proof['ad_id'] == c['ad_id'] and not proof['actual']['videos'] and not proof['actual']['images'] and any(i['src'].endswith('.html') for i in proof['actual']['iframes']), 'Unproven playable classification')
            positions.append((c['page'], c['position']))
            c.update(task_id=task['id'], source_collection=str(root))
        require(positions == sorted(set(positions)), 'Non-contiguous/duplicate card order')
        pages = sorted({p for p, _ in positions})
        require(pages == list(range(1, len(pages) + 1)), 'Missing result page')
        for page in sorted({p for p, _ in positions}):
            pos = [n for p, n in positions if p == page]
            require(pos == list(range(1, len(pos) + 1)), 'Missing card position')
            if page < max(pages):
                require(len(pos) == 60, 'Incomplete page before next page')
            if page > 1:
                require(any(e['key'] in ['page.open', 'page.resume'] and page_number(e) == page for _, e in good), 'Missing page navigation readback')
                for p, e in good:
                    if e['key'] in ['page.open', 'page.resume'] and page_number(e) == page:
                        click(root, str(p.relative_to(root)), fingerprint)
        dates = [c['first_discovered_date'] for c in cards]
        require(sum(d == target for d in dates) == seal['target_date_items'], 'Target-day count differs')
        if seal['terminal_reason'] == 'explicit_no_results':
            require(not cards and '暂无数据' in text, 'Unproven zero result')
        elif seal['terminal_reason'] == 'first_discovered_before_target':
            require(cards and dates[-1] < target and all(d >= target for d in dates[:-1]), 'Invalid first-date cutoff')
            require(cards[-1]['ad_id'] == seal['cutoff_card'] and dates[-1] == seal['cutoff_date'], 'Cutoff evidence mismatch')
        else:
            raise ValueError('Unsupported terminal reason; add actual evidence validator: ' + seal['terminal_reason'])
        all_cards.extend(c for c in cards if c['first_discovered_date'] == target)
        checklist.append(seal)
    return base, scope, fingerprint, all_cards, checklist


def image_evidence(root, card):
    from PIL import Image
    manifest = daily.load(file(root, f'evidence/images/{card["ad_id"]}/manifest.json'))
    require(manifest['ad_id'] == card['ad_id'] and manifest['scope_sha256'] == card['scope_sha256'], 'Image manifest identity mismatch')
    expected_urls = {i['src'] for i in card['observed_images'] if 'ant-image-img' in i.get('cls', '') and 'zingfront.com/sp_opera/' in i['src']}
    require(expected_urls and {i['url'] for i in manifest['images']} == expected_urls, 'Original image missing/substituted')
    for e in manifest['images']:
        path = hashed(root, e)
        with Image.open(path) as im:
            im.load()
            require(im.size == (e['width'], e['height']) and im.format == e['format'] and e['decode_verified'] is True, 'Original image decode/size mismatch')
    return manifest['images']


def video_evidence(root, card):
    from PIL import Image
    manifest = daily.load(file(root, f'evidence/frames/{card["ad_id"]}/manifest.json'))
    require(manifest['ad_id'] == card['ad_id'] and manifest['scope_sha256'] == card['scope_sha256'], 'Frame manifest identity mismatch')
    source = manifest['source']
    hashed(root, source)
    require(source['decode_verified'] is True and min(source['width'], source['height'], source['duration_seconds']) > 0, 'Video decode not verified')
    fractions = [.03, .15, .3, .5, .7, .9]
    require(manifest['sampling']['fractions'] == fractions and [f['fraction'] for f in manifest['frames']] == fractions, 'Six sampling points missing')
    for e in manifest['frames']:
        path = hashed(root, e)
        with Image.open(path) as im:
            im.load()
            require(im.size == (e['width'], e['height']), 'Frame dimensions mismatch')
    return manifest


def select(run_dir, config, persist=True):
    base, scope, fingerprint, cards, checklist = collection(run_dir, config)
    observations = daily.load(base / 'review_observations.json')
    by_key = {}
    for r in observations:
        key = (r['task'], r['ad_id'])
        require(key not in by_key, 'Duplicate review observation')
        by_key[key] = r
    waiver_path = base / 'missing_media.json'
    waivers = {(x['task_id'], x['ad_id']): x for x in daily.load(waiver_path)} if waiver_path.exists() else {}
    candidates, all_items, out_of_scope, counts = [], [], [], Counter()
    for c in cards:
        daily.estimate(c['impressions_estimate'])  # A missing estimate cannot become a media waiver.
        if c['media_type'] == '试玩':
            out_of_scope.append(c)
            counts['out_of_scope_playable'] += 1
            continue
        root = Path(c['source_collection'])
        key = (c['task_id'], c['ad_id'])
        counts[c['media_type'] + '_new'] += 1
        if daily.is_relaunch(c):
            c.update(decision='relaunch', review_reason='可见重投标签：全量采集后排除', exclusion_reason='relaunch')
            all_items.append(c)
            counts['relaunch'] += 1
            continue
        if key in waivers:
            w = waivers[key]
            require(w.get('reason') and w.get('attempts') and key not in by_key, 'Invalid missing-media waiver')
            evidence_dir = 'images' if c['media_type'] == '图片' else 'frames'
            require(not (root / f'evidence/{evidence_dir}/{c["ad_id"]}/manifest.json').exists(), 'Waiver cannot hide available media')
            require(w.get('failure', {}).get('status') == 'failed' and w['failure']['ad_id'] == c['ad_id'], 'Waiver lacks failed attempt')
            for attempt in w['attempts']:
                file(base, attempt)
            c.update(decision='unavailable', review_reason=w['reason'], review={'decision': 'unavailable', 'method': 'media_unavailable'})
            counts['unavailable'] += 1
            all_items.append(c)
            continue
        r = by_key.get(key)
        require(r and r['reason'] and r['decision'] in ['keep', 'exclude'], 'Unreviewed target-day media: ' + str(key))
        require(sha(file(base, r['viewed_board'])) == r['viewed_board_sha256'], 'Viewed review board changed')
        evidence_dir = 'images' if c['media_type'] == '图片' else 'frames'
        require(sha(file(root, f'evidence/{evidence_dir}/{c["ad_id"]}/manifest.json')) == r['media_manifest_sha256'], 'Reviewed media manifest changed')
        from ua_workflows.social_chat_content_policy import validate_review
        no_real_person = validate_review(scope, r)
        from ua_workflows.social_chat_style_tags import validate_style
        tags = validate_style(scope, r)
        if c['media_type'] == '图片':
            originals = image_evidence(root, c)
            require(r['method'] == 'original_image' and r.get('images_inspected') is True and (no_real_person or r['decision'] == 'keep'), 'Static image must use original-image review')
            require(sorted(r['image_sha256s']) == sorted(i['sha256'] for i in originals), 'Not all original images viewed')
            c['image_files'] = originals
        else:
            manifest = video_evidence(root, c)
            require(r['method'] == 'sampled_frames' and r.get('frames_inspected') is True, 'Video frames not viewed')
            if not no_real_person:
                require(r['checks']['real_person_talking_head'] is (r['decision'] == 'exclude'), 'Unsupported content exclusion')
            c.update(source_sha256=manifest['source']['sha256'], local_source=manifest['source']['path'])
        counts[c['media_type'] + '_success'] += 1
        c.update(review=r, decision=r['decision'], review_reason=r['reason'], review_method=r['method'], estimate_number=float(daily.estimate(c['impressions_estimate'])))
        if tags is not None:
            c.update(style_tags=tags, style_reason=r.get('style_reason', ''), style_tagging_version=r.get('style_tagging_version'))
        all_items.append(c)
        counts[r['decision']] += 1
        if r['decision'] == 'keep':
            candidates.append(c)
    candidates.sort(key=lambda c: (-daily.estimate(c['impressions_estimate']), c['app_name'], c['platform'], c['app_id'], c['ad_id']))
    from ua_workflows.social_chat_ranking import count as selection_count, rule as ranking_rule
    ranking = ranking_rule(scope.get('ranking'))
    selected = [{**c, 'global_rank': n} for n, c in enumerate(candidates[:selection_count(len(candidates), ranking)], 1)]
    result = {'producer': 'social-chat-mixed-window/v1', 'status': 'ready', 'scope_sha256': fingerprint, 'target_date': scope['target_date'], 'generated_at': datetime.now(timezone.utc).isoformat(), 'run_dir': str(base), 'task_count': len(checklist), 'ranking': ranking, 'counts': {**counts, 'raw': len(cards), 'eligible': len(candidates), 'selected': len(selected)}, 'items': selected}
    if scope.get('style_tagging') is not None:
        result['style_tagging'] = scope['style_tagging']
    if not persist:
        return result
    daily.save(base / 'selection.json', result)
    daily.save(base / 'all_candidates.json', all_items)
    daily.save(base / 'out_of_scope_media.json', out_of_scope)
    daily.save(base / 'app_checklist.json', checklist)
    daily.save(base / 'validation.json', {'producer': result['producer'], 'scope_sha256': fingerprint, 'collection_evidence_verified': True, 'frame_and_delivery_hashes_verified': True, 'pending_media_count': counts['unavailable'], 'media_waiver_applied': bool(waivers), 'checked_at': result['generated_at']})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['collection', 'select'])
    parser.add_argument('--run-dir', required=True)
    args = parser.parse_args()
    with collection_phase(args.run_dir, 'social_chat_mixed_window'):
        config = daily.load(daily.CONFIG)
        if args.command == 'collection':
            _, _, fingerprint, cards, checklist = collection(args.run_dir, config)
            print(json.dumps({'scope_sha256': fingerprint, 'tasks': len(checklist), 'target_items': len(cards), 'types': dict(Counter(c['media_type'] for c in cards))}, ensure_ascii=False))
        else:
            from ua_workflows.social_chat_daily_state import tracked
            with tracked(args.run_dir, 'RANK_PENDING'):
                result = select(args.run_dir, config)
            print(json.dumps({k: v for k, v in result.items() if k != 'items'}, ensure_ascii=False))


if __name__ == '__main__':
    main()
