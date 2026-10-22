"""Decide UI login recovery by actual run origin; does not sign in or read secrets."""
import argparse
import json
from pathlib import Path
from ua_workflows.social_chat_daily_state import locked, read, write, now, fingerprint


def decision(base, captcha=False):
    base=Path(base);scope=read(base/'scope.json')
    if read(base/'run.json')['scope_sha256']!=fingerprint(scope):raise ValueError('Scope changed')
    p=base/'daily-machine/login-recovery.json';attempts=read(p) if p.exists() else []
    clear=read(base/'daily-machine/captcha-clear.json') if (base/'daily-machine/captcha-clear.json').exists() else {}
    if captcha or (attempts and attempts[-1]['outcome']=='captcha' and clear.get('after_attempt')!=len(attempts)):
        action='stop_waiting_human'
    elif scope.get('execution_origin','manual')!='scheduled':action='ask_user'
    elif len(attempts)>=2:action='stop_retry_exhausted'
    else:action='auto_relogin'
    return {'action':action,'execution_origin':scope.get('execution_origin','manual'),
            'attempts':len(attempts),'max_attempts':2,'target_date':scope['target_date']}


def record(base, outcome, proof):
    if outcome not in ('success','failed','captcha'):raise ValueError('Unknown login outcome')
    base=Path(base).resolve();p=(base/proof).resolve()
    if not p.is_relative_to(base) or not p.is_file():raise ValueError('Login attempt evidence required')
    e=read(p)
    if not e.get('started_at') or not e.get('ended_at') or not e.get('evidence_paths'):raise ValueError('Actual attempt times and evidence required')
    from datetime import datetime,timezone
    a=datetime.fromisoformat(e['started_at'].replace('Z','+00:00'));b=datetime.fromisoformat(e['ended_at'].replace('Z','+00:00'))
    if a.tzinfo is None or b.tzinfo is None or not a<=b<=datetime.now(timezone.utc):raise ValueError('Invalid login attempt time')
    for path in e['evidence_paths']:
        f=(base/path).resolve()
        if not f.is_relative_to(base) or not f.is_file():raise ValueError('Missing login evidence')
    if outcome=='success' and e.get('authenticated') is not True:raise ValueError('Login not actually verified')
    with locked(base) as folder:
        rule=decision(base)
        if rule['action']!='auto_relogin':raise ValueError('Automatic login not allowed: '+rule['action'])
        path=folder/'login-recovery.json';items=read(path) if path.exists() else []
        if any(x['proof']==str(p.relative_to(base)) for x in items):raise ValueError('Attempt already recorded')
        # Never copy credential input, raw HTML or error strings into the journal.
        items.append({'attempt':len(items)+1,'outcome':outcome,'recorded_at':now(),
                      'started_at':e['started_at'],'ended_at':e['ended_at'],
                      'elapsed_seconds':(b-a).total_seconds(),'proof':str(p.relative_to(base))})
        write(path,items)
    return decision(base)



def clear_captcha(base, proof, confirmation):
    if not confirmation or not confirmation.strip():raise ValueError('Actual user confirmation required')
    base=Path(base).resolve();p=(base/proof).resolve()
    if not p.is_relative_to(base) or not p.is_file():raise ValueError('Actual post-captcha UI proof required')
    e=read(p);scope=read(base/'scope.json');attempts=read(base/'daily-machine/login-recovery.json')
    if not attempts or attempts[-1]['outcome']!='captcha':raise ValueError('No captcha hold')
    from datetime import datetime,timezone
    at=datetime.fromisoformat(e['observed_at'].replace('Z','+00:00'))
    if e.get('scope_sha256')!=fingerprint(scope) or e.get('captcha_visible') is not False or at.tzinfo is None or not datetime.fromisoformat(attempts[-1]['recorded_at'])<=at<=datetime.now(timezone.utc):raise ValueError('Unverified captcha clearance')
    if not e.get('evidence_paths'):raise ValueError('Missing captcha-clear evidence')
    for value in e['evidence_paths']:
        f=(base/value).resolve()
        if not f.is_relative_to(base) or not f.is_file():raise ValueError('Missing captcha-clear screenshot')
    with locked(base) as folder:
        write(folder/'captcha-clear.json',{'after_attempt':len(attempts),'observed_at':e['observed_at'],'proof':str(p.relative_to(base)),'user_confirmed':True})
    return decision(base)

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['decision','record','clear-captcha'])
    p.add_argument('--run-dir',required=True,type=Path);p.add_argument('--captcha',action='store_true')
    p.add_argument('--confirmation');p.add_argument('--outcome',choices=['success','failed','captcha']);p.add_argument('--proof')
    a=p.parse_args()
    if a.action=='decision':out=decision(a.run_dir,a.captcha)
    elif a.action=='record':out=record(a.run_dir,a.outcome,a.proof)
    else:out=clear_captcha(a.run_dir,a.proof,a.confirmation)
    print(json.dumps(out,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
