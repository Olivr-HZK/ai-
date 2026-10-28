"""One explicitly authorized retry of an uncertain page, retaining original evidence."""
import argparse
import fcntl
import hashlib
import json
from datetime import datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from ua_workflows.collection_runtime import write
from ua_workflows.social_search import collect_xhs, resolve_page_job, TZ
from ua_workflows.shared.config import project_env_values


def authorize_retry(base, keyword):
    base = Path(base).resolve()
    config = json.loads((ROOT/'config/social_chat_xhs_daily.json').read_text())
    index = config['keywords'].index(keyword) + 1
    folder = base/f'keywords/{index:02}'
    task = json.loads((folder/'complete.json').read_text())
    if task['status'] == 'complete':
        raise ValueError('Keyword already complete; no request sent')
    checkpoint = json.loads((folder/'checkpoint.json').read_text())
    number = checkpoint['pages'] + 1
    stem = folder/f'pages/{number:04}'
    canonical = stem.with_suffix('.job')
    request_path = stem.with_suffix('.request.json')
    if stem.with_suffix('.response.json').exists():
        raise ValueError('Successful page must not be retried')
    receipt = canonical/'submit-response.json'
    if receipt.exists() and json.loads(receipt.read_text()).get('job_id'):
        raise ValueError('Saved job ID must resume GET only; no additional submission')
    if not (canonical/'job-intent.json').exists():
        raise ValueError('Original persistent submission intent is required')
    body = {'platform':'xiaohongshu', 'action':'search', 'mode':'async',
            'params':{'keyword':keyword, 'content_type':'all', 'date_range':'last_24_hours', 'sort':'newest'}}
    if checkpoint['next_page_token']:
        body['params']['page_token'] = checkpoint['next_page_token']
    old = json.loads(request_path.read_text())
    if old['body']['params'] != body['params']:
        raise ValueError('Original page cursor/scope mismatch')
    job = canonical.with_suffix('.authorized-retry.job')
    if (job/'retry-authorization.json').exists():
        resolve_page_job(old, canonical)
        return config
    job.mkdir(parents=True, exist_ok=True)
    at = datetime.now(TZ).isoformat()
    write(job/'retry-authorization.json', {
        'keyword':keyword, 'page':number, 'authorized_at':at,
        'authorization':'User explicitly authorized one additional attempt; original fee remains unknown',
        'max_additional_attempts':1, 'original_job_dir':str(canonical.resolve()),
        'original_intent_sha256':hashlib.sha256((canonical/'job-intent.json').read_bytes()).hexdigest(),
        'body':body})
    write(job/'original-page-request.json', old)
    write(request_path, {**old, 'body':body, 'transport':'persistent_async_v1',
                         'async_job_dir':str(job.resolve()), 'retry_authorized_at':at})
    return config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir', required=True, type=Path)
    parser.add_argument('--keyword', required=True)
    args = parser.parse_args()
    with (ROOT/'data/xhs_daily/.run.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        config = authorize_retry(args.run_dir, args.keyword)
        result = collect_xhs(args.run_dir, config, key=project_env_values().get('EVERYINFRA_API_KEY'))
        result['prior_timeout_billing_unknown'] = True
        write(args.run_dir/'summary.json', result)
        print(json.dumps(result, ensure_ascii=False))
        if result['status'] != 'complete':
            raise SystemExit(1)


if __name__ == '__main__':
    main()
