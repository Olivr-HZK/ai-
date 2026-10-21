"""Explicitly authorized retry of one incomplete keyword; completed pages are never re-requested."""
import argparse
import csv
from datetime import datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from scripts.run_xhs_keyword_search import next_cursor,save,TZ
from ua_workflows.collection_runtime import post_json, collection_phase


def main():
 import requests
 from ua_workflows.shared.config import project_env_values
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-dir',type=Path,required=True);p.add_argument('--keyword',required=True);a=p.parse_args()
 base=a.run_dir.resolve();config=json.loads((ROOT/'config/social_chat_xhs_daily.json').read_text());summary=json.loads((base/'summary.json').read_text());index=config['keywords'].index(a.keyword)+1;folder=base/f'keywords/{index:02}';task=json.loads((folder/'complete.json').read_text())
 if task['status']=='complete':raise ValueError('Keyword already complete; no request sent')
 checkpoint=json.loads((folder/'checkpoint.json').read_text());page=checkpoint['pages']+1;token=checkpoint['next_page_token'];seen=set();fingerprints=set()
 for old in sorted((folder/'pages').glob('*.response.json')):
  if '.attempt-' in old.name or '.retry-' in old.name:continue
  response=json.loads(old.read_text());fingerprints.add(tuple(sorted(n['id'] for n in response['results'])));seen.update(n['id'] for n in response['results'])
 key=project_env_values().get('EVERYINFRA_API_KEY') or os.environ.get('EVERYINFRA_API_KEY');assert key,'Missing configured credential';session=requests.Session();session.headers['Authorization']='Bearer '+key
 stamp=datetime.now(TZ).strftime('%Y%m%dT%H%M%S_%f');save(base/f'resume-authorization-{stamp}.json',{'keyword':a.keyword,'from_page':page,'authorization':'User explicitly requested one retry of 星野 page 5 and continuation','time':datetime.now(TZ).isoformat(),'prior_fee_outcome':'unknown'})
 for name in ['summary.json','notes.json','progress.json']:
  path=base/name;save(base/f'before-resume-{stamp}'/name,json.loads(path.read_text()))
 newcost=Decimal('0');calls=0
 with collection_phase(base, 'xhs-resume'):
  while True:
   body={'platform':'xiaohongshu','action':'search','params':{'keyword':a.keyword,'content_type':'all','date_range':'last_24_hours','sort':'newest','page_token':token}}
   stem=folder/f'pages/{page:04}';request={'requested_at':datetime.now(TZ).isoformat(),'body':body};retry=stem.with_suffix(f'.retry-{stamp}.request.json');save(retry,request)
   r,response=post_json(config['api']['endpoint'],key=key,body=body,session=session,diagnostic_path=stem.with_suffix(f'.retry-{stamp}.http.json'));calls+=1;save(stem.with_suffix(f'.retry-{stamp}.response.json'),response)
   if not r.ok or not isinstance(response.get('results'),list):raise ValueError('Retry failed HTTP '+str(r.status_code)+'; no automatic replay')
   charged=(response.get('billing') or {}).get('charged_usd');newcost+=Decimal(str(charged)) if charged is not None else Decimal(0)
   ids=[n.get('id') for n in response['results']];assert all(isinstance(i,str) and i for i in ids),'Missing note ID';fp=tuple(sorted(ids));assert not ids or fp not in fingerprints,'Repeated page';fingerprints.add(fp);seen.update(ids)
   upcoming=next_cursor(response);assert upcoming!=token,'Repeated cursor'
   if stem.with_suffix('.request.json').exists():
    old=stem.with_suffix('.request.json');backup=stem.with_suffix(f'.before-{stamp}.request.json');save(backup,json.loads(old.read_text()))
   assert not stem.with_suffix('.response.json').exists(),'Refusing to overwrite successful page'
   save(stem.with_suffix('.request.json'),request);save(stem.with_suffix('.response.json'),response)
   task.update(pages=page,raw_rows=task['raw_rows']+len(ids),unique_notes=len(seen),status='collecting');save(folder/'checkpoint.json',{**task,'next_page_token':upcoming,'updated_at':datetime.now(TZ).isoformat()})
   print(json.dumps({'keyword':a.keyword,'page':page,'rows':len(ids),'unique':len(seen),'has_next':bool(upcoming)},ensure_ascii=False),flush=True)
   if not upcoming:
    task.update(status='complete',terminal_reason='no_next_page_token');break
   assert ids,'Empty page with next cursor';token=upcoming;page+=1
 task.pop('reason',None);task.pop('error_type',None);save(folder/'complete.json',task)
 # Rebuild merged evidence strictly in original keyword/page order.
 merged={};tasks=[]
 for i,keyword in enumerate(config['keywords'],1):
  root=base/f'keywords/{i:02}';t=json.loads((root/'complete.json').read_text());taskids=set();count=0
  for npage in range(1,t['pages']+1):
   path=root/f'pages/{npage:04}.response.json';payload=json.loads(path.read_text())
   for row in payload['results']:
    nid=row['id'];count+=1;taskids.add(nid)
    if nid not in merged:merged[nid]={**row,'matched_keywords':[],'observations':[]}
    if keyword not in merged[nid]['matched_keywords']:merged[nid]['matched_keywords'].append(keyword)
    merged[nid]['observations'].append({'keyword':keyword,'page':npage,'response_file':str(path.relative_to(base))})
  t.update(raw_rows=count,unique_notes=len(taskids),video_notes=sum(merged[n].get('is_video') is True for n in taskids),image_notes=sum(merged[n].get('is_video') is False for n in taskids));save(root/'complete.json',t);tasks.append(t)
 summary.update(status='complete' if all(t['status']=='complete' for t in tasks) else 'incomplete',keywords=tasks,completed_keywords=sum(t['status']=='complete' for t in tasks),pages=sum(t['pages'] for t in tasks),raw_rows=sum(t['raw_rows'] for t in tasks),unique_notes=len(merged),video_notes=sum(n['is_video'] is True for n in merged.values()),image_notes=sum(n['is_video'] is False for n in merged.values()),request_count=summary['request_count']+calls,reported_charged_usd=str(Decimal(summary['reported_charged_usd'])+newcost),finished_at=datetime.now(TZ).isoformat(),prior_timeout_billing_unknown=True)
 save(base/'summary.json',summary);save(base/'notes.json',list(merged.values()));save(base/'progress.json',tasks)
 with (base/'全部笔记互动数据.csv').open('w',encoding='utf-8-sig',newline='') as f:
  w=csv.writer(f);w.writerow(['笔记ID','命中关键词','标题','正文摘要','发布时间','类型','点赞','收藏','评论','分享','链接'])
  for row in merged.values():w.writerow([row['id'],'/'.join(row['matched_keywords']),row.get('title'),row.get('text'),row.get('posted_at'),'视频' if row['is_video'] else '图片',row.get('like_count'),row.get('collect_count'),row.get('comment_count'),row.get('share_count'),row.get('url')])
 print(json.dumps({k:summary[k] for k in ['status','completed_keywords','pages','unique_notes','video_notes','image_notes','reported_charged_usd']},ensure_ascii=False))


if __name__=='__main__':
 import fcntl
 lock_path=ROOT/'data/xhs_daily/.run.lock';lock_path.parent.mkdir(parents=True,exist_ok=True)
 with lock_path.open('a') as lock:
  fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
  main()
