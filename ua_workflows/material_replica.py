"""Explicitly triggered material replication: images via Video Lab, videos via Replica."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
import time

from ua_workflows.replica_cli import Checkpoint, operation_lock, upload_many, logo_many, download_many, error_result
from ua_workflows.replica_studio_api import ReplicaAPI, ReplicaError, digest, save_private, step_state
from ua_workflows.replica_images import IMAGE_SUFFIXES, REFERENCE, PROMPT, MODEL, VideoLab, image_identity, image_params, advance_image
from ua_workflows.shared.config import PROJECT_ROOT

VIDEOS = {'.mp4','.mov','.webm','.mkv'}
TERMINAL = {'downloaded','failed','needs_review'}


def plan(paths, *, reference=REFERENCE, prompt=PROMPT):
    if not isinstance(prompt,str) or not prompt.strip() or len(prompt)>60000:
        raise ReplicaError('invalid_input','Image editing prompt must contain 1–60000 characters')
    items = {}
    brand = None
    for value in paths:
        path = Path(value).expanduser().resolve()
        key = hashlib.sha256(str(path).encode()).hexdigest()[:24]
        if path.suffix.lower() in IMAGE_SUFFIXES:
            source = image_identity(path)
            brand = brand or image_identity(reference)
            item = {'key':key,'engine':'videolab','model':MODEL,'source':source,'reference':brand,
                    'params':image_params(source,brand,prompt),'stage':'pending'}
        elif path.suffix.lower() in VIDEOS and path.is_file() and 0<path.stat().st_size<=100*1024*1024:
            source = {'path':str(path),'name':path.name,'size':path.stat().st_size,'sha256':digest(path)}
            item = {'key':key,'engine':'replica','source':source,'stage':'pending'}
        else:
            raise ReplicaError('invalid_source','Select local PNG/JPEG/WebP images or MP4/MOV/WebM/MKV videos')
        items[key] = item
    if not items:
        raise ReplicaError('invalid_source','No input files selected')
    return items


class State:
    def __init__(self,root):
        self.root=Path(root).expanduser().resolve();self.path=self.root/'state.json'
        self.data=json.loads(self.path.read_text()) if self.path.exists() else {
            'version':1,'identities':{},'items':{},'created_at':time.time()}
        if self.data.get('version')!=1:raise ReplicaError('checkpoint_conflict','Unsupported material checkpoint version')

    def save(self):save_private(self.path,self.data)

    def add(self,items,output_dir,profile):
        settings={'output_dir':str(Path(output_dir).expanduser().resolve()),'videolab_profile':profile}
        if self.data.get('settings',settings)!=settings:
            raise ReplicaError('checkpoint_conflict','Run output directory or Video Lab profile changed')
        for key,item in items.items():
            old=self.data['items'].get(key)
            if old and any(old.get(k)!=item.get(k) for k in ('engine','source','reference','params')):
                raise ReplicaError('checkpoint_conflict','Source, reference or generation parameters changed; no automatic new task')
        self.data['settings']=settings
        for key,item in items.items():self.data['items'].setdefault(key,item)
        self.save()

    def bind(self,engine,identity):
        old=self.data['identities'].get(engine)
        if old is not None and old!=identity:
            raise ReplicaError('account_changed','Backend account/project differs from the existing run')
        self.data['identities'][engine]=identity;self.save()


def advance_video(item,api,checkpoint,root,output_dir,persist):
    if item['stage'] in TERMINAL:return
    if item['stage'] in {'pending','upload_unknown'}:
        if not Path(item['source']['path']).is_file() or digest(item['source']['path'])!=item['source']['sha256']:
            raise ReplicaError('checkpoint_conflict','Video source changed; no new upload')
        result=upload_many(lambda:api,[item['source']['path']],checkpoint)['results'][0]
        if not result.get('ok'):
            item.update(stage='upload_unknown' if result.get('code')=='result_unknown' else 'failed',
                        error=result.get('message') or result.get('code'));persist();return
        if len(result['ids'])!=1:raise ReplicaError('result_unknown','Video upload returned multiple tasks')
        item.update(stage='uploaded',job_id=result['ids'][0],request_id=result['request_id']);persist();return
    job=api.job(item['job_id'])
    item['last_status']=job.get('status')
    if job.get('status') in {'error','interrupted'}:
        item.update(stage='failed',error=job.get('error') or job.get('message'));persist();return
    intent=checkpoint.get('logos',item['job_id'])
    if intent or item['stage']=='logo_accepted' and step_state(job,'logo')=='ready':
        if step_state(job,'logo')=='ready':
            if (job.get('last_action')!='logo_replace' or intent and job.get('revision',0)<=intent.get('revision',-1)):
                item['stage']='logo_unknown';persist();return
            result=download_many(api,[item['job_id']],checkpoint,Path(output_dir)/item['key'])
            good=[x for x in result['results'] if x.get('ok')]
            if len(good)==1 and good[0].get('full_decode_passed'):
                item.update(stage='downloaded',output=good[0],visual_review_required=bool((job.get('logo_replace_view') or {}).get('flagged')))
                item.pop('error',None);persist();return
            failed=next((x for x in result['results'] if not x.get('ok')), {})
            raise ReplicaError(failed.get('code') or 'invalid_media','Video result could not be downloaded and verified',failed.get('http_status'))
        if step_state(job,'logo')=='review':
            item.update(stage='needs_review',error='Logo processing requires review');persist();return
        item['stage']='logo_unknown' if intent and intent['stage']!='logo_accepted' else 'logo_accepted'
        persist();return
    if step_state(job,'analysis')!='ready':
        item['stage']='analyzing';persist();return
    result=logo_many(api,[item['job_id']],checkpoint)['results'][0]
    item.update(stage='logo_accepted' if result.get('ok') else 'logo_unknown' if result.get('code')=='result_unknown' else 'failed')
    if not result.get('ok'):item['error']=result.get('message') or result.get('code')
    persist()


def snapshot(state):
    fields=('key','engine','model','stage','task_id','job_id','request_id','submission_request_id',
            'last_status','error','output','visual_review_required')
    return {'run_dir':str(state.root),'items':[
        {'source':v['source']['path'],**{k:v[k] for k in fields if k in v}}
        for v in state.data['items'].values()], 'backend_errors':state.data.get('backend_errors',{}),
        'feishu_written':False,'all_outputs_downloaded':bool(state.data['items']) and
        all(v['stage']=='downloaded' for v in state.data['items'].values())}


def process(state, *, wait=False, timeout=7200, interval=15, image_client=None, video_factory=None,
            clock=time.time, sleep=time.sleep):
    clients={};state.data['backend_errors']={}
    for item in state.data['items'].values():
        if item['stage']!='downloaded':continue
        output=item.get('output') or {};path=Path(output.get('path',''))
        if (not path.is_file() or path.stat().st_size!=output.get('size') or digest(path)!=output.get('sha256')):
            item.update(stage='needs_review',error='Verified local output is missing or changed; no automatic regeneration')
    engines={x['engine'] for x in state.data['items'].values() if x['stage'] not in TERMINAL}
    for engine in engines:
        try:
            if engine=='videolab':
                client=image_client or VideoLab(state.data['settings']['videolab_profile'])
                state.bind(engine,client.identity());clients[engine]=client
            else:
                old=state.data['identities'].get('replica') or {}
                client=(video_factory or (lambda:ReplicaAPI.configured(expected_account=old.get('account_id'),confirm_paid=True)))()
                auth=client.authenticate();state.bind(engine,{'account_id':auth['account_id']})
                clients[engine]=(client,Checkpoint(state.root/'replica-state.json',auth['account_id']))
        except ReplicaError as error:
            state.data['backend_errors'][engine]=error_result(error)
    state.save();deadline=clock()+timeout
    while True:
        for item in state.data['items'].values():
            if item['stage'] in TERMINAL or item['engine'] not in clients or clock()<item.get('check_after',0):continue
            try:
                if item['engine']=='videolab':
                    source=Path(item['source']['path'])
                    if source.exists() and digest(source)!=item['source']['sha256']:
                        raise ReplicaError('checkpoint_conflict','Source image changed; result remains bound to the original bytes')
                    advance_image(item,clients['videolab'],state.root,state.data['settings']['output_dir'],state.save)
                else:
                    api,checkpoint=clients['replica']
                    advance_video(item,api,checkpoint,state.root,state.data['settings']['output_dir'],state.save)
            except ReplicaError as error:
                item['error']=str(error)
                if error.code in {'invalid_media','invalid_source','checkpoint_conflict','model_unavailable'}:
                    item['stage']='needs_review'
                item['check_after']=clock()+(60 if (error.status or 0)>=500 else 30)
                state.save()
        active=[v for v in state.data['items'].values() if v['stage'] not in TERMINAL and v['engine'] in clients]
        if not wait or not active or clock()>=deadline:break
        sleep(min(interval,max(0,deadline-clock())))
    return snapshot(state)


def parser():
    p=argparse.ArgumentParser(description=__doc__)
    sub=p.add_subparsers(dest='command',required=True)
    for command in ('plan','run'):
        q=sub.add_parser(command,help='只读本地计划' if command=='plan' else '按素材类型提交复刻任务；会产生生成费用')
        q.add_argument('files',nargs='+',type=Path)
        q.add_argument('--brand-reference',type=Path,default=REFERENCE)
        q.add_argument('--prompt-file',type=Path,help='可选UTF-8图片编辑提示词；视频仍仅智能换Logo')
        if command=='run':
            q.add_argument('--run-dir',type=Path,required=True)
            q.add_argument('--output-dir',type=Path)
            q.add_argument('--videolab-profile',default='default')
            _wait_flags(q)
    for command in ('resume','status'):
        q=sub.add_parser(command);q.add_argument('--run-dir',required=True,type=Path)
        if command=='resume':_wait_flags(q)
    return p


def _wait_flags(p):
    p.add_argument('--wait',action=argparse.BooleanOptionalAction,default=True,
                   help='默认轮询到下载完成、明确失败或等待超时；--no-wait只推进一轮')
    p.add_argument('--timeout',type=float,default=7200)
    p.add_argument('--interval',type=float,default=15)


def execute(args,**clients):
    if args.command=='plan':
        prompt=args.prompt_file.read_text() if args.prompt_file else PROMPT
        return {'operation':'plan','items':list(plan(args.files,reference=args.brand_reference,prompt=prompt).values()),
                'business_submitted':False,'external_writes':0}
    root=args.run_dir.expanduser().resolve()
    if args.command=='status':
        if not (root/'state.json').is_file():raise ReplicaError('invalid_input','Run directory has no checkpoint')
        return snapshot(State(root))
    if args.timeout<=0 or args.interval<=0:raise ReplicaError('invalid_input','Timeout and interval must be positive')
    with operation_lock(root/'state.json'):
        state=State(root)
        if args.command=='run':
            prompt=args.prompt_file.read_text() if args.prompt_file else PROMPT
            state.add(plan(args.files,reference=args.brand_reference,prompt=prompt),args.output_dir or root/'results',args.videolab_profile)
        elif not state.path.exists():raise ReplicaError('invalid_input','No existing run to resume')
        return process(state,wait=args.wait,timeout=args.timeout,interval=args.interval,**clients)


def main(argv=None):
    try:
        args=parser().parse_args(argv)
        result=execute(args);print(json.dumps(result,ensure_ascii=False))
        if any(x['stage'] in {'failed','needs_review'} for x in result.get('items',[])):return 2
        pending_wait=getattr(args,'wait',False) and not result.get('all_outputs_downloaded')
        return 3 if pending_wait or result.get('backend_errors') or any(x['stage'] in {'unknown','upload_unknown','logo_unknown'}
                                                                       for x in result.get('items',[])) else 0
    except (ReplicaError,OSError,ValueError) as error:
        print(json.dumps(error_result(error),ensure_ascii=False),file=sys.stderr);return 1


if __name__=='__main__':raise SystemExit(main())
