"""Unified daily mixed-media state gates and per-attempt timing; never drives UI."""
from contextlib import contextmanager
from datetime import datetime, timezone
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import time
import uuid

from ua_workflows.collection_runtime import configured_secrets, redact

# Scope chooses one direct App gate or the seven legacy App gates, in task order.
APP_STATES = ('CATEGORY_PENDING', 'FILTERS_PENDING', 'APP_PENDING', 'SORT_PENDING',
              'PROBE_PENDING', 'COLLECTING', 'COLLECTION_SUMMARY_PENDING', 'APP_COLLECTION_PENDING')
GLOBAL_BEFORE = ('START_NOTICE_PENDING', 'BROWSER_PENDING')
GLOBAL_AFTER = ('RELAUNCH_PENDING', 'MEDIA_PENDING', 'REVIEW_PENDING', 'RANK_PENDING',
                'DELIVERY_PENDING', 'SYNC_PENDING', 'REPORT_PENDING')
CONTROL_STATES = ('WAITING_HUMAN', 'PAUSED', 'RETRY_REQUIRED')
# Legacy graph has 17 executable state names; direct mode replaces its seven App names with one.


def read(p):
    return json.loads(Path(p).read_text())


def write(p, value):
    p = Path(p); p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + '.tmp')
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as f:
        json.dump(value, f, ensure_ascii=False, indent=2)
    tmp.replace(p)


def now():
    return datetime.now(timezone.utc).isoformat()


def fingerprint(scope):
    return hashlib.sha256(json.dumps(scope, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def plan(scope):
    steps = [{'state':'PREPARE','task':None}] + [{'state': s, 'task': None} for s in GLOBAL_BEFORE]
    app_states = ('APP_COLLECTION_PENDING',) if scope.get('collection_mode')=='direct-browser-v1' else tuple(s for s in APP_STATES if s!='APP_COLLECTION_PENDING')
    steps += [{'state': s, 'task': t['id']} for t in scope['tasks'] for s in app_states]
    return steps + [{'state': s, 'task': None} for s in GLOBAL_AFTER]


@contextmanager
def locked(base):
    folder = Path(base) / 'daily-machine'; folder.mkdir(parents=True, exist_ok=True)
    with (folder / 'lock').open('a') as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield folder


def init(base, prepared_started_at=None, prepared_monotonic=None):
    base = Path(base); scope = read(base/'scope.json')
    if read(base/'run.json')['scope_sha256'] != fingerprint(scope):
        raise ValueError('Scope fingerprint mismatch')
    with locked(base) as folder:
        p = folder/'state.json'
        if p.exists():
            return status(base)
        doc = {'schema': 'social-chat-daily-state/v1', 'scope_sha256': fingerprint(scope),
               'target_date': scope['target_date'], 'cursor': 0, 'control': None,
               'active_attempt': None, 'attempts': [], 'completed': [], 'max_attempts': 3,
               'prepared_at': now()}
        write(p, doc)
    a=begin(base)
    if prepared_started_at is not None:
        with locked(base) as folder:
            d=load(base);d['active_attempt'].update(started_at=prepared_started_at,monotonic_start=prepared_monotonic);write(folder/'state.json',d)
    return finish(base)


def load(base):
    doc = read(Path(base)/'daily-machine/state.json')
    if doc['scope_sha256'] != fingerprint(read(Path(base)/'scope.json')):
        raise ValueError('Scope changed; initialize a new run')
    return doc


def current(base, doc):
    steps = plan(read(Path(base)/'scope.json'))
    return steps[doc['cursor']] if doc['cursor'] < len(steps) else {'state':'COMPLETED', 'task':None}


def operation_summary(base):
    p=Path(base)/'daily-machine/operations.json';rows=read(p) if p.exists() else []
    out={}
    for r in rows:
        v=out.setdefault(r['action'],{'count':0,'seconds':0,'retries':0,'errors':0})
        v['count']+=1;v['seconds']+=r['elapsed_seconds'];v['retries']+=r['retry'];v['errors']+=bool(r.get('error'))
    return out


def status(base):
    doc = load(base); step = current(base, doc)
    summary = {}
    for a in doc['attempts']:
        key = a['state'] + '/' + (a['task'] or 'run')
        row = summary.setdefault(key, {'attempts':0,'retries':0,'active_seconds':0,'failed':0})
        row['attempts'] += 1; row['retries'] = row['attempts']-1
        row['active_seconds'] += a.get('elapsed_seconds', 0)
        row['failed'] += a['outcome'] in ('failed','interrupted')
    active = doc['active_attempt']
    return {'state': doc['control'] or step['state'], 'resume_state':step['state'],
            'task':step['task'], 'target_date':doc['target_date'], 'cursor':doc['cursor'],
            'total_steps':len(plan(read(Path(base)/'scope.json'))), 'active_attempt':active,
            'active_elapsed_seconds':max(0,time.monotonic()-active['monotonic_start']) if active else 0,
            'completed_steps':len(doc['completed']), 'timing':summary,
            'control_reason':doc.get('control_reason'),
            'current_wait_seconds':max(0,(datetime.now(timezone.utc)-datetime.fromisoformat(doc['held_at'])).total_seconds()) if doc['control'] and doc.get('held_at') else 0,
            'waiting_seconds':sum(r.get('waiting_seconds',0) for r in doc.get('resumes',[])),
            'operation_summary':operation_summary(base)}


def begin(base):
    with locked(base) as folder:
        doc=load(base); step=current(base,doc)
        if doc['control'] or step['state']=='COMPLETED' or doc['active_attempt']:
            raise ValueError('State not ready for a new attempt')
        count=sum(a['state']==step['state'] and a['task']==step['task'] for a in doc['attempts'])
        budget=doc.get('budgets',{}).get(step['state']+'/'+str(step['task']),doc['max_attempts'])
        if count >= budget:
            raise ValueError('Retry budget exhausted; resolve cause and explicitly grant further attempts')
        a={**step,'id':uuid.uuid4().hex,'number':count+1,'started_at':now(),
           'monotonic_start':time.monotonic(),'pid':os.getpid()}
        doc['active_attempt']=a; write(folder/'state.json',doc)
    return a


def evidence(base, path):
    base=Path(base).resolve(); p=(base/path).resolve()
    if not p.is_relative_to(base) or not p.is_file() or not p.stat().st_size:
        raise ValueError('Missing or unsafe evidence: '+str(path))
    return p


def ui_check(base, task, state, proof, scope):
    """Browser tool must supply actual observations, not success booleans."""
    p=evidence(base,proof); e=read(p); root=Path(base)/'tasks'/task['id']
    if e.get('scope_sha256')!=fingerprint(scope) or e.get('task_id')!=task['id']:
        raise ValueError('UI evidence scope/task mismatch')
    # Every stage gets a fresh browser observation made after begin.
    category = task.get('category', scope['filters']['category'])
    expected = {'category':category} if state=='CATEGORY_PENDING' else {
        'category':category,'dedupe':scope['filters']['dedupe'],
        'date':{'start':scope['target_date'],'end':scope['target_date']},
        'media':'图片&视频','selected_types':[], 'channels':[], 'regions':[], 'languages':[], 'size':''}
    if e.get('expected')!=expected or e.get('after')!=expected:
        raise ValueError('UI filter readback differs from scope')
    unchanged=e.get('before')==expected
    if (unchanged and e.get('action')!='readonly') or (not unchanged and e.get('action')!='repair'):
        raise ValueError('Correct filters must use readonly; only differences permit repair')
    for im in e.get('images',{}).values():
        q=evidence(base,im['path'])
        if hashlib.sha256(q.read_bytes()).hexdigest()!=im['sha256']:
            raise ValueError('UI screenshot hash mismatch')
    if set(e.get('images',{}))!={'before','after'}:
        raise ValueError('Missing actual before/after screenshots')
    return p


def validate(base, step, proof=None):
    from ua_workflows import social_chat_browser_daily as daily
    from ua_workflows import social_chat_mixed_window as window
    base=Path(base); scope=read(base/'scope.json'); config=daily.load(daily.CONFIG)
    started=load(base)['active_attempt']['started_at']
    def fresh(doc, key='performed_at'):
        value=doc.get(key)
        if not value:raise ValueError('Missing observation time')
        stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
        if stamp.tzinfo is None or not datetime.fromisoformat(started)<=stamp<=datetime.now(timezone.utc):raise ValueError('No fresh stage evidence after attempt start')
    s=step['state']; task=next((t for t in scope['tasks'] if t['id']==step['task']),None)
    root=base/'tasks'/task['id'] if task else base
    if s=='PREPARE':
        if read(base/'run.json')['scope_sha256']!=fingerprint(scope):raise ValueError('Scope mismatch')
        return [base/'scope.json',base/'run.json']
    if s=='APP_COLLECTION_PENDING':
        from ua_workflows.social_chat_direct import validate_task
        cards,seal=validate_task(base,scope,task)
        first_attempt=min([started]+[a['started_at'] for a in load(base)['attempts'] if a['state']==s and a['task']==task['id']])
        observed=read(root/'direct/prepare.json')['observed_at']
        if datetime.fromisoformat(observed.replace('Z','+00:00'))<datetime.fromisoformat(first_attempt):raise ValueError('Stale direct preparation')
        p=base/'daily-machine/app-seals'/f'{task["id"]}.json'
        write(p,{'task':task,'scope_sha256':fingerprint(scope),'verified_at':now(),'seal':seal,'target_items':len(cards),'input_sha256':hashlib.sha256((root/'direct/complete.json').read_bytes()).hexdigest()})
        return [p,root/'direct/complete.json']
    if s in ('CATEGORY_PENDING','FILTERS_PENDING'):
        p=ui_check(base,task,s,proof,scope);fresh(read(p),'observed_at');return [p]
    if s=='BROWSER_PENDING':
        p=evidence(base,proof or 'browser.json');d=read(p)
        if d.get('authenticated') is not True or not d.get('browser_session') or d.get('scope_sha256')!=fingerprint(scope):
            raise ValueError('Browser login/session not verified')
        fresh(d,'observed_at')
        if not d.get('images'): raise ValueError('Browser screenshot required')
        for im in d['images'].values():
            q=evidence(base,im['path'])
            if hashlib.sha256(q.read_bytes()).hexdigest()!=im['sha256']:raise ValueError('Browser screenshot hash mismatch')
        return [p]
    if s in ('APP_PENDING','SORT_PENDING'):
        key='app.query' if s=='APP_PENDING' else 'filter.sort'
        expected=task['app_id'] if s=='APP_PENDING' else '最新创意'
        events=[]
        for p in (root/'evidence/clicks').glob('*.json'):
            d=read(p)
            if d.get('key')==key and d.get('after')==expected:
                if d.get('performed_at','')<started:continue
                window.click(root,str(p.relative_to(root)),fingerprint(scope),key,expected);fresh(d);events.append(p)
        if not events:raise ValueError('Missing actual App/search or sort click')
        if s=='SORT_PENDING':
            queries=[read(p) for p in (root/'evidence/clicks').glob('*.json') if read(p).get('key')=='app.query' and read(p).get('status')=='passed']
            chosen=read(root/'app-selection.json');selected=read(root/chosen['ui_check'])
            if not queries or max(read(p)['performed_at'] for p in events)<max(max(q['performed_at'] for q in queries),selected['performed_at']):raise ValueError('Sort must follow latest App query and candidate click')
        if s=='APP_PENDING':
            p=evidence(base,proof);d=read(p)
            selected_ids=[] if d.get('search_mode')=='综合' else [task['app_id']]
            if not (d.get('app_id')==task['app_id'] and d.get('platform')==task['platform'] and d.get('selected_app_ids')==selected_ids and d.get('residual_keywords')==[]):
                raise ValueError('App identity/selected filter mismatch')
            model=daily.fsm();model.identity(d,task)
            candidate=model.app_click_check(root,d,fingerprint(scope),task['id'])
            fresh(candidate)
            if datetime.fromisoformat(d['observed_at'].replace('Z','+00:00')) < datetime.fromisoformat(candidate['performed_at'].replace('Z','+00:00')):
                raise ValueError('Selected identity must be observed after candidate click')
            if d.get('query_type')!='app_id':raise ValueError('Daily App query must use verified App ID')
            fresh(d,'observed_at');events.extend([root/d['ui_check'],p])
            write(root/'app-selection.json',d)
        return events
    if s=='PROBE_PENDING':
        if proof and Path(proof).name=='collection_complete.json':
            window.collection(base,config,task_id=task['id'])
            p=evidence(base,proof)
            if read(p)['terminal_reason']!='explicit_no_results':raise ValueError('Probe can only skip proven zero results')
            return [p]
        p=evidence(base,proof);c=read(p)
        if not(c.get('detail_loaded') is True and all(c.get(k)==task[k] for k in ('app_id','app_name','platform'))):raise ValueError('Probe identity mismatch')
        window.click(root,c['ui_check'],fingerprint(scope),'card.identity',{'app_id':task['app_id'],'platform':task['platform']})
        fresh(read(root/c['ui_check']));daily.discovered(c); return [root/c['ui_check'],p]
    if s in ('COLLECTING','COLLECTION_SUMMARY_PENDING'):
        _,_,_,cards,seals=window.collection(base,config,task_id=task['id'])
        if s=='COLLECTION_SUMMARY_PENDING':
            p=base/'daily-machine/app-seals'/f'{task["id"]}.json'
            write(p,{'task':task,'scope_sha256':fingerprint(scope),'verified_at':now(),'seal':seals[0],'target_items':len(cards),'input_sha256':hashlib.sha256((root/'collection_complete.json').read_bytes()).hexdigest()})
            return [p]
        return [root/'collection_complete.json']
    if s=='START_NOTICE_PENDING':
        p=evidence(base,'crawl-notice-start.json');d=read(p)
        if config['crawl_start_notice']['enabled'] and not(d.get('message_id') and d.get('chat_id')==config['crawl_start_notice']['chat_id'] and d.get('mention_all') is True and d.get('status') in ('verified','verified_by_send_response','verified_in_feishu_ui')):raise ValueError('Start notice receipt not verified')
        return [p]
    if s in ('RELAUNCH_PENDING','MEDIA_PENDING'):
        _,_,_,cards,_=window.collection(base,config)
        eligible=[c for c in cards if c['media_type']!='试玩' and not daily.is_relaunch(c)]
        if s=='MEDIA_PENDING':
            waivers={(x['task_id'],x['ad_id']):x for x in read(base/'missing_media.json')} if (base/'missing_media.json').exists() else {}
            for c in eligible:
                daily.estimate(c['impressions_estimate']);key=(c['task_id'],c['ad_id']);rt=Path(c['source_collection'])
                if key in waivers:
                    w=waivers[key];directory='images' if c['media_type']=='图片' else 'frames'
                    if (rt/f'evidence/{directory}/{c["ad_id"]}/manifest.json').exists():raise ValueError('Waiver hides available media')
                    if not(w.get('reason') and w.get('attempts') and w.get('failure',{}).get('status')=='failed' and w['failure'].get('ad_id')==c['ad_id']):raise ValueError('Unproven media failure')
                    for a in w['attempts']:evidence(base,a)
                elif c['media_type']=='图片':window.image_evidence(rt,c)
                else:window.video_evidence(rt,c)
        p=base/'daily-machine'/('relaunch-check.json' if s=='RELAUNCH_PENDING' else 'media-check.json')
        write(p,{'scope_sha256':fingerprint(scope),'checked_at':now(),'eligible':len(eligible),'relaunch':sum(daily.is_relaunch(c) for c in cards)})
        return [p]
    if s in ('REVIEW_PENDING','RANK_PENDING'):
        window.select(base,config,persist=s=='RANK_PENDING')
        return [evidence(base,'review_observations.json' if s=='REVIEW_PENDING' else 'selection.json')]
    if s=='DELIVERY_PENDING':
        if not evidence(base,'delivery/index.html') or not evidence(base,'feishu-pending.json'):raise ValueError('Delivery missing')
        return [base/'delivery/index.html',base/'feishu-pending.json']
    if s=='SYNC_PENDING':
        p=evidence(base,'feishu-sync.json');d=read(p);sel=read(base/'selection.json')
        if d.get('status')!='synced' or d.get('verified_active')!=len(sel['items']):raise ValueError('Remote readback not verified')
        return [p]
    if s=='REPORT_PENDING':
        p=evidence(base,'crawl-report-complete.json');d=read(p)
        if not(d.get('message_id') and d.get('status')=='sent_with_api_receipt' and d.get('chat_id')==config['crawl_completion_report']['chat_id']):raise ValueError('Daily report receipt missing')
        return [p]
    raise ValueError('Unknown state '+s)


def finish(base, outcome='passed', proof=None, error=None):
    base=Path(base).resolve()
    if outcome not in ('passed','failed','interrupted'):raise ValueError('Invalid attempt outcome')
    with locked(base) as folder:
        doc=load(base); a=doc['active_attempt']
        if not a:raise ValueError('No active attempt')
        step=current(base,doc)
        try:
            paths=validate(base,step,proof) if outcome=='passed' else []
        except Exception as exc:
            outcome='failed';error=str(exc);paths=[]
        a.update(outcome=outcome,ended_at=now(),elapsed_seconds=max(0,time.monotonic()-a['monotonic_start']),
                 evidence=[{'path':str(p.relative_to(Path(base))),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in paths])
        if error:a['error']=redact(str(error),configured_secrets())
        doc['attempts'].append(a);doc['active_attempt']=None
        if outcome=='passed':doc['completed'].append({**step,'attempt_id':a['id']});doc['cursor']+=1
        else:doc.update(control='RETRY_REQUIRED',control_reason=a.get('error','Interrupted'),held_at=now())
        write(folder/'state.json',doc)
    if outcome!='passed':raise ValueError(a.get('error','Attempt failed'))
    return status(base)


def hold(base, kind, reason):
    if kind not in CONTROL_STATES:raise ValueError('Invalid control state')
    if load(base)['active_attempt']:
        try:finish(base,'interrupted',error=reason)
        except ValueError:pass
    with locked(base) as folder:
        d=load(base);d.update(control=kind,control_reason=redact(reason,configured_secrets()),held_at=now());write(folder/'state.json',d)
    return status(base)


def resume(base, reason, extra_attempts=0, proof=None):
    if not reason.strip():raise ValueError('Resume needs actual resolution/confirmation')
    with locked(base) as folder:
        d=load(base)
        if not d['control']:raise ValueError('Not suspended')
        if extra_attempts<0 or extra_attempts>3:raise ValueError('Extra attempts must be 0..3')
        step=current(base,d)
        if (d['control'] in ('WAITING_HUMAN','PAUSED') and step['state'] in APP_STATES) or step['state']=='APP_COLLECTION_PENDING':
            scope=read(Path(base)/'scope.json');task=next(t for t in scope['tasks'] if t['id']==step['task'])
            if step['state']=='APP_COLLECTION_PENDING':
                from ua_workflows.social_chat_direct import validate_preparation
                p=evidence(base,proof)
                v=validate_preparation(Path(base)/'tasks'/task['id'],scope,task,str(p.relative_to(Path(base)/'tasks'/task['id'])))
                actual=v.get('actual',{})
                if not(v.get('scope_sha256')==fingerprint(scope) and v.get('task_id')==task['id'] and v.get('authenticated') is True and v.get('settled') is True and actual.get('query_terms')==[task['app_id']] and actual.get('selected_app_ids')==[] and actual.get('date')=={'start':scope['target_date'],'end':scope['target_date']} and actual.get('category')==task.get('category',scope['filters']['category'])):raise ValueError('Direct resume browser/App mismatch')
                for im in v.get('images',{}).values():
                    from ua_workflows.social_chat_mixed_window import hashed
                    hashed(Path(base)/'tasks'/task['id'],im)
                b=v.get('boundary')
                if any((Path(base)/'tasks'/task['id']/'cards').glob('*.json')) and not b:raise ValueError('Direct resume boundary proof required')
                if b:
                    from ua_workflows.social_chat_mixed_window import hashed
                    card=read(hashed(Path(base)/'tasks'/task['id'],{'path':b['card_path'],'sha256':b['card_sha256']}))
                    if b['expected']!=b['observed'] or b['expected']!=card['ad_id']:raise ValueError('Direct resume boundary mismatch')
            else:
                p=ui_check(base,task,'FILTERS_PENDING',proof,scope);v=read(p)
                if not(v.get('authenticated') is True and v.get('app_id')==task['app_id'] and v.get('platform')==task['platform'] and v.get('selected_app_ids')==([] if v.get('search_mode')=='综合' else [task['app_id']]) and v.get('residual_keywords')==[]):raise ValueError('Resume browser/App identity not verified')
            observed=datetime.fromisoformat(v['observed_at'].replace('Z','+00:00'))
            if observed.tzinfo is None or not datetime.fromisoformat(d['held_at'])<=observed<=datetime.now(timezone.utc):raise ValueError('Stale resume observation')
        step=current(base,d);key=step['state']+'/'+str(step['task'])
        budgets=d.setdefault('budgets',{});budgets[key]=budgets.get(key,d['max_attempts'])+extra_attempts
        d.setdefault('resumes',[]).append({'at':now(),'from':d['control'],'reason':redact(reason,configured_secrets()),'waiting_seconds':max(0,(datetime.now(timezone.utc)-datetime.fromisoformat(d.get('held_at',now()))).total_seconds())})
        d.update(control=None,control_reason=None);write(folder/'state.json',d)
    return status(base)



def operations(base, entries):
    """Batch once per page; observation times come from the actual browser script."""
    if not isinstance(entries,list):raise ValueError('Expected operation list')
    with locked(base) as folder:
        doc=load(base); a=doc['active_attempt']
        if not a:raise ValueError('No active attempt for browser operations')
        path=folder/'operations.json';rows=read(path) if path.exists() else []
        seen={r['id'] for r in rows}
        for e in entries:
            if not e.get('id') or e['id'] in seen:raise ValueError('Missing/duplicate operation id')
            if e.get('action') not in ('readonly','repair','search','sort','card','page','download','review'):raise ValueError('Unknown operation')
            if e['action'] in ('readonly','repair'):
                if 'expected' not in e or 'before' not in e or 'after' not in e:raise ValueError('Filter observation missing')
                if e['before']==e['expected'] and e['action']!='readonly':raise ValueError('Redundant filter mutation refused')
                if e['before']!=e['expected'] and e['action']=='readonly':raise ValueError('Incorrect filter cannot pass readonly')
                if not e.get('error') and e['after']!=e['expected']:raise ValueError('Filter not corrected')
            start=datetime.fromisoformat(e['started_at'].replace('Z','+00:00'));end=datetime.fromisoformat(e['ended_at'].replace('Z','+00:00'))
            if start.tzinfo is None or end.tzinfo is None or end<start or start<datetime.fromisoformat(a['started_at']) or end>datetime.now(timezone.utc):raise ValueError('Invalid operation time')
            paths=e.get('evidence_paths',[])
            if not paths:raise ValueError('Operation evidence required')
            for p in paths:evidence(base,p)
            key=(a['state'],a['task'],e.get('name'),e.get('page'),e.get('card'))
            count=sum((r['state'],r['task'],r.get('name'),r.get('page'),r.get('card'))==key for r in rows)
            item={**e,'state':a['state'],'task':a['task'],'attempt_id':a['id'],'operation_attempt':count+1,'retry':count>0,'elapsed_seconds':(end-start).total_seconds()}
            item=json.loads(redact(json.dumps(item,ensure_ascii=False),configured_secrets()))
            rows.append(item);seen.add(e['id'])
        write(path,rows)
    return {'recorded':len(entries),'total':len(rows)}

@contextmanager
def tracked(base, state):
    """New runs enforce order; historical runs remain unchanged/read-only compatible."""
    p=Path(base)/'daily-machine/state.json'
    if not p.exists():yield;return
    d=load(base);step=current(base,d)
    if step['state']!=state or d['control']:raise ValueError('Expected '+state+'; current '+str(status(base)['state']))
    begin(base)
    try:
        yield
        finish(base)
    except Exception as exc:
        if load(base)['active_attempt']:
            try:finish(base,'failed',error=str(exc))
            except ValueError:pass
        raise


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=['init','status','begin','complete','fail','hold','resume','operations'])
    p.add_argument('--run-dir',required=True,type=Path);p.add_argument('--proof');p.add_argument('--reason',default='')
    p.add_argument('--kind',choices=CONTROL_STATES,default='WAITING_HUMAN');p.add_argument('--extra-attempts',type=int,default=0)
    a=p.parse_args()
    if a.action=='init':out=init(a.run_dir)
    elif a.action=='status':out=status(a.run_dir)
    elif a.action=='operations':
        import sys
        out=operations(a.run_dir,json.load(sys.stdin))
    elif a.action=='begin':out=begin(a.run_dir)
    elif a.action=='complete':out=finish(a.run_dir,proof=a.proof)
    elif a.action=='fail':
        try:finish(a.run_dir,'failed',error=a.reason)
        except ValueError:pass
        out=status(a.run_dir)
    elif a.action=='hold':out=hold(a.run_dir,a.kind,a.reason)
    else:out=resume(a.run_dir,a.reason,a.extra_attempts,a.proof)
    print(json.dumps(out,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
