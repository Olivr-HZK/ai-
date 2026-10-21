"""Collect every accessible page for configured XHS keywords, preserving raw responses."""
import argparse
import csv
from datetime import datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import sys
import time
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TZ = ZoneInfo('Asia/Shanghai')
from ua_workflows.collection_runtime import collection_phase, post_json, record_failure


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2))
    temp.replace(path)


def next_cursor(payload):
    """Prefer documented top-level cursor; tolerate documented row-level cursors."""
    if 'next_page_token' in payload:
        token = payload['next_page_token']
    else:
        tokens = {row.get('next_page_token') for row in payload['results'] if row.get('next_page_token')}
        if len(tokens) > 1:
            raise ValueError('Conflicting row pagination cursors')
        token = next(iter(tokens), None)
    if token is not None and not isinstance(token, str):
        raise ValueError('Invalid pagination cursor type')
    return token or None


def main():
    import requests
    from ua_workflows.shared.config import project_env_values
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir')
    args = parser.parse_args()
    config = json.loads((ROOT / 'config/social_chat_xhs_daily.json').read_text())
    key = project_env_values().get('EVERYINFRA_API_KEY') or os.environ.get('EVERYINFRA_API_KEY')
    if not key:
        print('未配置 EVERYINFRA_API_KEY，未发送请求。', file=sys.stderr)
        return 2
    endpoint = config['api']['endpoint']
    if endpoint != 'https://api.everyinfra.com/api/v1/social':
        raise ValueError('Unexpected API destination')
    now = datetime.now(TZ)
    out = Path(args.run_dir).resolve() if args.run_dir else ROOT / 'data/xhs_keyword_runs' / now.strftime('%Y%m%dT%H%M%S_%f')
    if (out / 'run.json').exists():
        raise ValueError('Use a new output directory; existing evidence is never overwritten')
    save(out / 'run.json', {'started_at': now.isoformat(), 'keywords': config['keywords'],
                           'date_range': 'last_24_hours', 'sort': 'newest', 'content_type': 'all',
                           'scope': 'API-accessible search pages; rolling 24h at each request',
                           'downstream': 'search_only'})
    session = requests.Session()
    session.headers.update({'Authorization': 'Bearer ' + key})
    merged, results, billed = {}, [], Decimal('0')
    request_count = 0
    with collection_phase(out, 'xhs-search'):
        for index, keyword in enumerate(config['keywords'], 1):
            folder = out / f'keywords/{index:02}'
            task = {'keyword': keyword, 'status': 'collecting', 'pages': 0, 'raw_rows': 0,
                    'unique_notes': 0, 'video_notes': 0, 'image_notes': 0}
            seen_tokens, seen_pages, seen_ids = set(), set(), set()
            token = None
            page = 1
            try:
                while True:
                    body = {'platform': 'xiaohongshu', 'action': 'search',
                            'params': {'keyword': keyword, 'content_type': 'all',
                                       'date_range': 'last_24_hours', 'sort': 'newest'}}
                    if token:
                        body['params']['page_token'] = token
                    stem = folder / f'pages/{page:04}'
                    save(stem.with_suffix('.request.json'), {'requested_at': datetime.now(TZ).isoformat(), 'body': body})
                    # Retry only explicit, non-charged throttling; never blindly replay a timeout.
                    for attempt in range(3):
                        request_count += 1
                        response, payload = post_json(endpoint, key=key, body=body, session=session,
                                                      diagnostic_path=stem.with_suffix(f'.attempt-{attempt+1}.http.json'))
                        save(stem.with_suffix(f'.attempt-{attempt+1}.response.json'), payload)
                        charged = (payload.get('billing') or {}).get('charged_usd')
                        if charged is not None:
                            billed += Decimal(str(charged))
                        if response.status_code != 429 or (payload.get('billing') or {}).get('charged') is not False or attempt == 2:
                            break
                        delay = float(response.headers.get('Retry-After', '10'))
                        time.sleep(min(max(delay, 1), 60))
                    if not response.ok or not isinstance(payload.get('results'), list):
                        raise ValueError('API error HTTP ' + str(response.status_code) + '; response preserved; no automatic replay')
                    save(stem.with_suffix('.response.json'), payload)
                    rows = payload['results']
                    ids = [r.get('id') for r in rows]
                    if any(not isinstance(i, str) or not i for i in ids):
                        raise ValueError('Search returned a missing note ID')
                    fingerprint = tuple(sorted(ids))
                    if rows and fingerprint in seen_pages:
                        raise ValueError('Repeated page contents; stopped to avoid endless billed requests')
                    seen_pages.add(fingerprint)
                    upcoming = next_cursor(payload)
                    if upcoming and (upcoming == token or upcoming in seen_tokens):
                        raise ValueError('Repeated pagination cursor')
                    for row in rows:
                        note_id = row['id']
                        seen_ids.add(note_id)
                        if note_id not in merged:
                            merged[note_id] = {**row, 'matched_keywords': [], 'observations': []}
                        item = merged[note_id]
                        if keyword not in item['matched_keywords']:
                            item['matched_keywords'].append(keyword)
                        item['observations'].append({'keyword': keyword, 'page': page,
                                                     'response_file': str(stem.with_suffix('.response.json').relative_to(out))})
                    task.update(pages=page, raw_rows=task['raw_rows'] + len(rows), unique_notes=len(seen_ids))
                    save(folder / 'checkpoint.json', {**task, 'next_page_token': upcoming, 'updated_at': datetime.now(TZ).isoformat()})
                    print(json.dumps({'keyword': keyword, 'page': page, 'rows': len(rows),
                                      'unique': len(seen_ids), 'has_next': bool(upcoming)}, ensure_ascii=False), flush=True)
                    if not upcoming:
                        task.update(status='complete', terminal_reason='no_next_page_token')
                        break
                    if not rows:
                        raise ValueError('Empty page with a next cursor: incomplete rather than treating as final')
                    seen_tokens.add(upcoming)
                    token, page = upcoming, page + 1
            except Exception as exc:
                # Store the error category, not request headers/credential-bearing exception strings.
                task.update(status='incomplete', error_type=type(exc).__name__,
                            diagnostic=record_failure(out, 'xhs-search', exc))
                if isinstance(exc, ValueError):
                    task['reason'] = str(exc)
                else:
                    task['reason'] = 'Transport/response error; billing may be uncertain; inspect preserved evidence before retrying'
            task['video_notes'] = sum(merged[n].get('is_video') is True for n in seen_ids)
            task['image_notes'] = sum(merged[n].get('is_video') is False for n in seen_ids)
            save(folder / 'complete.json', task)
            results.append(task)
            save(out / 'progress.json', results)
            save(out / 'notes.json', list(merged.values()))
    summary = {'status': 'complete' if all(t['status'] == 'complete' for t in results) else 'incomplete',
               'started_at': now.isoformat(), 'finished_at': datetime.now(TZ).isoformat(),
               'date_range': 'last_24_hours', 'sort': 'newest', 'keywords': results,
               'keyword_count': len(results), 'completed_keywords': sum(t['status'] == 'complete' for t in results),
               'pages': sum(t['pages'] for t in results), 'request_count': request_count,
               'raw_rows': sum(t['raw_rows'] for t in results), 'unique_notes': len(merged),
               'video_notes': sum(r.get('is_video') is True for r in merged.values()),
               'image_notes': sum(r.get('is_video') is False for r in merged.values()),
               'reported_charged_usd': str(billed), 'media_downloaded': False, 'feishu_written': False}
    save(out / 'summary.json', summary)
    columns = ['笔记ID', '命中关键词', '标题', '正文摘要', '发布时间', '类型', '点赞', '收藏', '评论', '分享', '链接']
    with (out / '全部笔记互动数据.csv').open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f); writer.writerow(columns)
        for r in merged.values():
            writer.writerow([r['id'], '/'.join(r['matched_keywords']), r.get('title'), r.get('text'),
                             r.get('posted_at'), '视频' if r.get('is_video') is True else '图片' if r.get('is_video') is False else '未知',
                             r.get('like_count'), r.get('collect_count'), r.get('comment_count'), r.get('share_count'), r.get('url')])
    print(json.dumps({'directory': str(out), **summary}, ensure_ascii=False), flush=True)
    return 0 if summary['status'] == 'complete' else 1


if __name__ == '__main__':
    raise SystemExit(main())
