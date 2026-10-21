"""One-page EveryInfra XHS search probe; no pagination, retries or downstream writes."""
import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    from ua_workflows.shared.config import project_env_values
    from ua_workflows.collection_runtime import post_json
    import requests

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--keyword', default='乙女向')
    parser.add_argument('--date-range', choices=['any_time', 'last_24_hours', 'last_7_days', 'last_30_days', 'last_6_months', 'last_1_year'], default='any_time')
    args = parser.parse_args()
    key = project_env_values().get('EVERYINFRA_API_KEY') or os.environ.get('EVERYINFRA_API_KEY')
    if not key:
        print('未配置 EVERYINFRA_API_KEY；未发送请求、未产生调用费用。', file=sys.stderr)
        return 2
    config = json.loads((ROOT / 'config/social_chat_xhs_daily.json').read_text())
    endpoint = config['api']['endpoint']
    if endpoint != 'https://api.everyinfra.com/api/v1/social':
        raise ValueError('Unexpected API destination')
    body = {'platform': 'xiaohongshu', 'action': 'search',
            'params': {'keyword': args.keyword, 'content_type': 'all',
                       'date_range': args.date_range, 'sort': 'newest'}}
    now = datetime.now(ZoneInfo('Asia/Shanghai'))
    out = ROOT / 'data' / 'xhs_keyword_tests' / now.strftime('%Y%m%dT%H%M%S_%f')
    out.mkdir(parents=True)
    def save(name, data):
        (out / name).write_text(json.dumps(data, ensure_ascii=False, indent=2))
    save('request.json', {'endpoint': endpoint, 'body': body, 'started_at': now.isoformat()})
    # A timeout/unknown billing outcome must not automatically trigger another paid request.
    try:
        response, payload = post_json(endpoint, key=key, body=body, diagnostic_path=out/'http-diagnostic.json')
    except requests.exceptions.JSONDecodeError:
        save('summary.json', {'status': 'non_json_response', 'diagnostic': 'http-diagnostic.json', 'automatic_retry': False})
        return 1
    except requests.RequestException as exc:
        save('summary.json', {'status': 'transport_error_billing_unknown',
                              'error_type': type(exc).__name__, 'automatic_retry': False})
        print('请求未取得确定响应；未自动重试。结果目录：' + str(out))
        return 1
    save('response.json', payload)
    rows = payload.get('results') if isinstance(payload, dict) else None
    if not response.ok or not isinstance(rows, list):
        save('summary.json', {'status': 'api_error', 'http_status': response.status_code,
                              'request_id': payload.get('id') if isinstance(payload, dict) else None,
                              'automatic_retry': False})
        print('API 测试未成功；HTTP ' + str(response.status_code) + '，结果目录：' + str(out))
        return 1
    ids = [r.get('id') for r in rows]
    checks = {'all_have_note_id': all(isinstance(i, str) and i for i in ids),
              'unique_note_ids': len(ids) == len(set(ids)),
              'all_have_note_url': all(isinstance(r.get('url'), str) and r['url'].startswith('https://www.xiaohongshu.com/') for r in rows)}
    summary = {'status': 'search_verified' if all(checks.values()) else 'response_needs_review',
               'keyword': args.keyword, 'date_range': args.date_range, 'sort': 'newest',
               'page_count': 1, 'returned_notes': len(rows), 'checks': checks,
               'has_next_page': bool(payload.get('next_page_token') or any(r.get('next_page_token') for r in rows)),
               'video_notes': sum(r.get('is_video') is True for r in rows),
               'video_urls': sum(bool(r.get('video_url')) for r in rows),
               'image_urls': sum(bool(r.get('image_url') or r.get('images') or r.get('image_urls')) for r in rows),
               'billing': payload.get('billing'), 'request_id': payload.get('id'),
               'media_downloaded': False, 'feishu_written': False}
    save('summary.json', summary)
    # Print only the probe outcome, without signed media links or account balance.
    print(json.dumps({k: summary[k] for k in ['status', 'keyword', 'returned_notes', 'video_notes', 'video_urls', 'image_urls', 'has_next_page']}, ensure_ascii=False))
    print('结果目录：' + str(out))
    return 0 if all(checks.values()) else 1


if __name__ == '__main__':
    raise SystemExit(main())
