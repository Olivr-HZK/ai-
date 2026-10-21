"""One paid EveryInfra Douyin search request; never paginates or retries."""
import argparse
import csv
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
TZ = ZoneInfo('Asia/Shanghai')


def main():
    import requests
    from ua_workflows.shared.config import project_env_values
    from ua_workflows.collection_runtime import post_json
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--keyword', default='乙女向')
    parser.add_argument('--limit', type=int, default=20, choices=range(1, 51))
    args = parser.parse_args()
    key = project_env_values().get('EVERYINFRA_API_KEY') or os.environ.get('EVERYINFRA_API_KEY')
    if not key:
        print('未配置 EVERYINFRA_API_KEY；未发送请求。', file=sys.stderr)
        return 2
    now = datetime.now(TZ)
    out = ROOT / 'data/douyin_keyword_tests' / now.strftime('%Y%m%dT%H%M%S_%f')
    out.mkdir(parents=True)
    def save(name, value):
        (out / name).write_text(json.dumps(value, ensure_ascii=False, indent=2))
    body = {'platform': 'douyin', 'action': 'search', 'params': {
        'keyword': args.keyword, 'publish_time': 'one_day', 'sort': 'newest', 'limit': args.limit}}
    save('request.json', {'started_at': now.isoformat(), 'body': body})
    try:
        response, payload = post_json('https://api.everyinfra.com/api/v1/social',
                                      key=key, body=body, diagnostic_path=out/'http-diagnostic.json')
    except requests.exceptions.JSONDecodeError:
        save('summary.json', {'status': 'non_json_response', 'diagnostic': 'http-diagnostic.json'})
        print('返回非JSON，未重试。结果目录：' + str(out))
        print(json.dumps({'keyword': args.keyword, 'directory': str(out), 'status': 'non_json_response'}, ensure_ascii=False))
        return 1
    except requests.RequestException as exc:
        save('summary.json', {'status': 'transport_error_billing_unknown', 'error_type': type(exc).__name__, 'diagnostic': 'http-diagnostic.json'})
        print('未获取确定响应；未自动重试。结果目录：' + str(out))
        print(json.dumps({'keyword': args.keyword, 'directory': str(out), 'status': 'transport_error_billing_unknown', 'error_type': type(exc).__name__}, ensure_ascii=False))
        return 1
    save('response.json', payload)
    rows = payload.get('results') if isinstance(payload, dict) else None
    if not response.ok or not isinstance(rows, list):
        save('summary.json', {'status': 'api_error', 'http_status': response.status_code})
        print('API未成功，HTTP ' + str(response.status_code) + '；未重试。结果目录：' + str(out))
        return 1
    date_issues = []
    for row in rows:
        try:
            published = datetime.fromisoformat(row['posted_at'].replace('Z', '+00:00'))
            if not now-timedelta(hours=24) <= published <= datetime.now(TZ):
                date_issues.append({'id': row.get('id'), 'posted_at': row.get('posted_at')})
        except (KeyError, TypeError, ValueError, AttributeError):
            date_issues.append({'id': row.get('id'), 'reason': 'unparsed_posted_at'})
    ids = [row.get('id') for row in rows]
    identity_valid = all(isinstance(i, str) and i for i in ids) and len(ids) == len(set(ids))
    summary = {'status': 'search_verified' if identity_valid and not date_issues else 'response_needs_review',
               'keyword': args.keyword, 'publish_time': 'one_day', 'sort': 'newest', 'requested_limit': args.limit,
               'returned_notes': len(rows), 'video_notes': sum(r.get('media_type') == 'video' for r in rows),
               'video_urls': sum(bool(r.get('video_url')) for r in rows),
               'image_notes': sum(r.get('media_type') in ('image', 'images') for r in rows),
               'date_issues': date_issues, 'identity_valid': identity_valid,
               'charged_usd': (payload.get('billing') or {}).get('charged_usd'),
               'request_count': 1, 'media_downloaded': False, 'feishu_written': False}
    save('summary.json', summary)
    with (out / '互动数据.csv').open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['作品ID', '标题', '文案', '发布时间', '类型', '点赞', '收藏', '评论', '分享', '作品链接'])
        for row in rows:
            writer.writerow([row.get(k) for k in ['id', 'title', 'text', 'posted_at', 'media_type', 'like_count', 'collect_count', 'comment_count', 'share_count', 'url']])
    print(json.dumps({'directory': str(out), **summary}, ensure_ascii=False))
    return 0 if summary['status'] == 'search_verified' else 1


if __name__ == '__main__':
    raise SystemExit(main())
