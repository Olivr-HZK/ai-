"""Durable, one-submission asynchronous search jobs. Resume only by free GET."""
import fcntl
import hashlib
import json
from pathlib import Path
import re
import time
from uuid import uuid4
from urllib.parse import urlsplit

import requests

from ua_workflows.collection_runtime import exception_info, post_json, stamp, write


class JobPending(RuntimeError):
    pass


class JobBlocked(RuntimeError):
    pass


def load(path):
    return json.loads(Path(path).read_text())


def fingerprint(body):
    return hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def net_charge(folder):
    submit = load(Path(folder)/'submit-response.json') if (Path(folder)/'submit-response.json').exists() else {}
    result = load(Path(folder)/'result.json') if (Path(folder)/'result.json').exists() else {}
    state = load(Path(folder)/'job-state.json') if (Path(folder)/'job-state.json').exists() else {}
    if result.get('refund_state') == 'refunded' or state.get('refund_state')=='refunded': return 0.0
    return (submit.get('billing') or {}).get('charged_usd')


def execute(body, folder, *, key, endpoint='https://api.everyinfra.com/api/v1/social',
            session=None, poll_seconds=5, poll_budget=300):
    """No automatic POST retry, even for a refunded failure. Pending/error states survive."""
    folder = Path(folder); folder.mkdir(parents=True, exist_ok=True)
    if endpoint!='https://api.everyinfra.com/api/v1/social' and urlsplit(endpoint).hostname not in ('127.0.0.1','::1','localhost'):
        raise ValueError('Unexpected API destination')
    actual = {**body, 'mode':'async'}
    if body.get('action') != 'search' or body.get('platform') not in ('douyin','xiaohongshu'):
        raise ValueError('Async search scope mismatch')
    digest = fingerprint({'endpoint':endpoint,'body':actual})
    with (folder/'job.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        intent = folder/'job-intent.json'; submitted = folder/'submit-response.json'
        if intent.exists() and load(intent)['fingerprint'] != digest:
            raise ValueError('Stored async request scope changed')
        if (folder/'result.json').exists():
            result=load(folder/'result.json'); receipt=load(submitted)
            if result.get('status')!='succeeded' or result.get('job_id')!=receipt.get('job_id'):
                raise JobBlocked('Cached async job identity mismatch')
            return result
        if not submitted.exists():
            if intent.exists():
                raise JobBlocked('Submission outcome unknown; never resubmit automatically')
            if not isinstance(key,str) or not key:
                raise ValueError('Missing EveryInfra credential; no request sent')
            # Preflight is deliberately before the durable sending intent: blocked power uses no POST.
            from ua_workflows.collection_runtime import paid_request_power
            with paid_request_power(folder/'power.json'):
                write(intent, {'fingerprint':digest,'body':actual,'requested_at':stamp(),'status':'sending'})
                try:
                    response, receipt = post_json(endpoint,key=key,body=actual,
                                                 diagnostic_path=folder/'submit-http.json',session=session,
                                                 timeout=(15,30))
                    write(submitted, receipt)
                except BaseException as exc:
                    write(folder/'submission-error.json',exception_info(exc,[key]))
                    raise
        receipt = load(submitted); job_id = receipt.get('job_id')
        if not isinstance(job_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,160}',job_id):
            raise JobBlocked('No confirmed job ID; inspect receipt, never resubmit automatically')
        if receipt.get('status') == 'submit_unknown':
            write(folder/'job-state.json',{'job_id':job_id,'status':'submit_unknown','next_action':receipt.get('next_action')})
        if not isinstance(key,str) or not key:
            raise ValueError('Missing EveryInfra credential; saved job is retained')
        deadline=time.monotonic()+poll_budget; wall_deadline=time.time()+poll_budget
        get = session.get if session is not None else requests.get
        while True:
            pollfile=folder/('poll-'+uuid4().hex+'.json'); started=time.monotonic()
            try:
                response=get(endpoint.rsplit('/social',1)[0]+'/jobs/'+job_id,
                             headers={'Authorization':'Bearer '+key},timeout=(10,30))
                result=response.json()
                write(pollfile,{'at':stamp(),'http_status':response.status_code,'elapsed_seconds':time.monotonic()-started,'body':result})
                if not response.ok:
                    raise JobPending('Job query failed; saved job ID, only GET may resume')
                if result.get('job_id') != job_id:
                    raise JobBlocked('Async query returned another job ID')
                status=result.get('status')
                write(folder/'job-state.json',{'job_id':job_id,'status':status,'updated_at':stamp(),
                                             'billing_state':result.get('billing_state'),'refund_state':result.get('refund_state'),
                                             'last_poll_file':pollfile.name})
                if status=='succeeded':
                    if result.get('platform')!=body['platform'] or result.get('action')!='search' or not isinstance(result.get('results'),list):
                        raise JobBlocked('Async result scope/type mismatch')
                    write(folder/'result.json',result)
                    return result
                if status=='failed':
                    raise JobBlocked('Async job failed; inspect saved error/refund, no POST replay')
                if status=='submit_unknown':
                    raise JobPending('Supplier submission unknown; keep job ID, no POST replay')
                if status not in ('submitting','running','finalizing'):
                    raise JobBlocked('Unknown async status; no POST replay')
            except (requests.RequestException,ValueError) as exc:
                write(folder/'poll-error.json',{'at':stamp(),'exception':exception_info(exc,[key])})
                raise JobPending('Query interrupted; resume saved job with GET only') from exc
            if time.monotonic()>=deadline or time.time()>=wall_deadline:
                raise JobPending('Job still pending; resume saved job with GET only')
            time.sleep(min(poll_seconds,max(0,deadline-time.monotonic())))
