"""Daily search orchestration with page/keyword checkpoints and durable async jobs."""
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import json
import fcntl
import csv
from functools import wraps

from ua_workflows.collection_runtime import collection_phase, record_failure, write
from ua_workflows.everyinfra_jobs import execute, load, net_charge, JobBlocked, JobPending

TZ=ZoneInfo('Asia/Shanghai')


def batch_lock(function):
    @wraps(function)
    def run(base,*args,**kwargs):
        base=Path(base);base.mkdir(parents=True,exist_ok=True)
        with (base/'.search.lock').open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            return function(base,*args,**kwargs)
    return run


def cursor(payload):
    if 'next_page_token' in payload: token=payload['next_page_token']
    else:
        tokens={n.get('next_page_token') for n in payload['results'] if n.get('next_page_token')}
        if len(tokens)>1: raise ValueError('Conflicting pagination cursors')
        token=next(iter(tokens),None)
    if token is not None and not isinstance(token,str): raise ValueError('Invalid pagination cursor')
    return token or None


def page(body, request_file, response_file, job, *, key, endpoint, poll_budget):
    request_file, response_file, job=map(Path,(request_file,response_file,job))
    if request_file.exists():
        request=load(request_file)
        # Preserve existing raw scopes and their original request time.
        if request['body'].get('params')!=body['params']:
            raise ValueError('Stored page request/cursor mismatch')
        if response_file.exists(): return load(response_file),request
        if request.get('transport')!='persistent_async_v1':
            raise JobBlocked('Legacy request outcome uncertain; explicit retry authorization required')
    else:
        request={'body':{**body,'mode':'async'},'requested_at':datetime.now(TZ).isoformat(),
                 'started_at':datetime.now(TZ).isoformat(),'transport':'persistent_async_v1'}
        write(request_file,request)
    payload=execute(body,job,key=key,endpoint=endpoint,poll_budget=poll_budget)
    intent=load(job/'job-intent.json') if (job/'job-intent.json').exists() else None
    if intent:
        # The rolling window starts at the actual POST, including a deferred power-blocked run.
        request['requested_at']=request['started_at']=intent['requested_at']
    if job!=request_file.parent:
        # Keep the actual job directory explicit for the evidence validator.
        request['async_job_dir']=str(job.resolve())
    write(request_file,request)
    write(response_file,payload)
    return payload,request


def failed(base,stage,exc):
    from ua_workflows.collection_runtime import PowerPreflightError
    status='pending' if isinstance(exc,JobPending) else 'power_blocked' if isinstance(exc,PowerPreflightError) else 'incomplete'
    return {'status':status,'error_type':type(exc).__name__,'diagnostic':record_failure(base,stage,exc)}


@batch_lock
def collect_xhs(base,config,*,key,poll_budget=300):
    base=Path(base);runfile=base/'run.json'
    if runfile.exists():
        run=load(runfile)
        if run['keywords']!=config['keywords'] or run.get('date_range')!='last_24_hours' or run.get('sort')!='newest':
            raise ValueError('Stored XHS run scope changed')
    else:
        run={'started_at':datetime.now(TZ).isoformat(),'keywords':config['keywords'],
             'date_range':'last_24_hours','sort':'newest','content_type':'all','transport':'persistent_async_v1'}
        write(runfile,run)
    if (base/'summary.json').exists():
        previous=load(base/'summary.json')
        if previous.get('status')=='complete':return previous
    merged={};tasks=[];fees=0.0;calls=0
    with collection_phase(base,'xhs-search'):
        for index,keyword in enumerate(config['keywords'],1):
            folder=base/f'keywords/{index:02}';task={'keyword':keyword,'status':'collecting','pages':0,'raw_rows':0}
            token=None;seen_tokens=set();seen_pages=set();seen_ids=set()
            try:
                while True:
                    number=task['pages']+1;stem=folder/f'pages/{number:04}'
                    body={'platform':'xiaohongshu','action':'search','params':{'keyword':keyword,'content_type':'all','date_range':'last_24_hours','sort':'newest'}}
                    if token:body['params']['page_token']=token
                    job=stem.with_suffix('.job')
                    payload,request=page(body,stem.with_suffix('.request.json'),stem.with_suffix('.response.json'),job,
                                         key=key,endpoint=config['api']['endpoint'],poll_budget=poll_budget)
                    rows=payload.get('results')
                    if not isinstance(rows,list):raise ValueError('Invalid search results')
                    ids=[n.get('id') for n in rows]
                    if any(not isinstance(i,str) or not i for i in ids) or len(ids)!=len(set(ids)):raise ValueError('Missing/duplicate note ID in a page')
                    fingerprint=tuple(sorted(ids));upcoming=cursor(payload)
                    if rows and fingerprint in seen_pages:raise ValueError('Repeated page; stop before another paid request')
                    if upcoming and (upcoming==token or upcoming in seen_tokens):raise ValueError('Repeated pagination cursor')
                    if not rows and upcoming:raise ValueError('Empty page with a cursor')
                    seen_pages.add(fingerprint);seen_ids.update(ids)
                    for n in rows:
                        merged.setdefault(n['id'],{**n,'matched_keywords':[],'observations':[]})
                        row=merged[n['id']]
                        if keyword not in row['matched_keywords']:row['matched_keywords'].append(keyword)
                        row['observations'].append({'keyword':keyword,'page':number,'response_file':str(stem.with_suffix('.response.json').relative_to(base))})
                    fees+=0 if (job/'job-intent.json').exists() else (payload.get('billing') or {}).get('charged_usd',0) or 0
                    calls+=not (job/'job-intent.json').exists();task.update(pages=number,raw_rows=task['raw_rows']+len(rows))
                    write(folder/'checkpoint.json',{**task,'next_page_token':upcoming,'updated_at':datetime.now(TZ).isoformat()})
                    if not upcoming:task.update(status='complete',terminal_reason='no_next_page_token');break
                    seen_tokens.add(upcoming);token=upcoming
            except Exception as exc:task.update(failed(base,'xhs-search',exc))
            task.update(unique_notes=len(seen_ids),video_notes=sum(merged[i].get('is_video') is True for i in seen_ids),image_notes=sum(merged[i].get('is_video') is False for i in seen_ids))
            tasks.append(task);write(folder/'complete.json',task);write(base/'progress.json',tasks);write(base/'notes.json',list(merged.values()))
        receipts=list((base/'keywords').rglob('submit-response.json'))
        fees+=sum(net_charge(p.parent) or 0 for p in receipts)
        calls+=sum(1 for _ in (base/'keywords').rglob('job-intent.json'))
        summary={'status':'complete' if all(t['status']=='complete' for t in tasks) else 'incomplete','started_at':run['started_at'],'finished_at':datetime.now(TZ).isoformat(),
                 'date_range':'last_24_hours','sort':'newest','keywords':tasks,'keyword_count':len(tasks),'completed_keywords':sum(t['status']=='complete' for t in tasks),
                 'pages':sum(t['pages'] for t in tasks),'request_count':calls,'raw_rows':sum(t['raw_rows'] for t in tasks),'unique_notes':len(merged),
                 'video_notes':sum(n.get('is_video') is True for n in merged.values()),'image_notes':sum(n.get('is_video') is False for n in merged.values()),
                 'reported_charged_usd':str(round(fees,6)),'media_downloaded':False,'feishu_written':False}
        write(base/'summary.json',summary)
        with (base/'全部笔记互动数据.csv').open('w',encoding='utf-8-sig',newline='') as stream:
            writer=csv.writer(stream)
            writer.writerow(['笔记ID','命中关键词','标题','正文摘要','发布时间','类型','点赞','收藏','评论','分享','链接'])
            for n in merged.values():
                writer.writerow([n['id'],'/'.join(n['matched_keywords']),n.get('title'),n.get('text'),n.get('posted_at'),
                                 '视频' if n.get('is_video') is True else '图片' if n.get('is_video') is False else '未知',
                                 n.get('like_count'),n.get('collect_count'),n.get('comment_count'),n.get('share_count'),n.get('url')])
    return summary


@batch_lock
def collect_douyin(base,config,*,key,poll_budget=300):
    base=Path(base);old=load(base/'summary.json') if (base/'summary.json').exists() else {}
    if old.get('keywords'):
        if [t['keyword'] for t in old['keywords']]!=config['keywords']:raise ValueError('Stored Douyin run scope changed')
        if all(t.get('status') in ('search_verified','response_needs_review') for t in old['keywords']):return old
    oldtasks={t['keyword']:t for t in old.get('keywords',[])}
    tasks=[];start=old.get('started_at',datetime.now(TZ).isoformat())
    with collection_phase(base,'douyin-search'):
        for index,keyword in enumerate(config['keywords'],1):
            prior=oldtasks.get(keyword);folder=Path(prior['directory']) if prior and prior.get('directory') else base/f'search/{index:02}'
            task={'keyword':keyword,'directory':str(folder.resolve())}
            try:
                body={'platform':'douyin','action':'search','params':{'keyword':keyword,'publish_time':'one_day','sort':'newest','limit':50}}
                payload,request=page(body,folder/'request.json',folder/'response.json',folder,key=key,endpoint=config['api']['endpoint'],poll_budget=poll_budget)
                rows=payload.get('results')
                if not isinstance(rows,list):raise ValueError('Invalid search results')
                ids=[n.get('id') for n in rows]
                if any(not isinstance(i,str) or not i for i in ids) or len(ids)!=len(set(ids)):raise ValueError('Missing/duplicate post IDs')
                task.update(status='response_needs_review',returned_notes=len(rows),charged_usd=net_charge(folder) if (folder/'job-intent.json').exists() else (payload.get('billing') or {}).get('charged_usd'),
                            has_next_page_token=bool(payload.get('next_page_token')),has_more=payload.get('has_more'))
            except Exception as exc:
                task.update(failed(base,'douyin-search',exc))
                if (folder/'submit-response.json').exists():task['charged_usd']=net_charge(folder)
            tasks.append(task);write(base/'progress.json',tasks)
        summary={'keywords':tasks,'total_returned':sum(t.get('returned_notes',0) for t in tasks),'cost_usd':round(sum(t.get('charged_usd') or 0 for t in tasks),6),
                 'started_at':start,'finished_at':datetime.now(TZ).isoformat(),'pagination_supported_by_catalog':False,'full_search_exhaustion_proven':False}
        write(base/'summary.json',summary)
    return summary
