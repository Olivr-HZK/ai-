#!/usr/bin/env python3
"""Operator bridge: claims tasks and writes a browser-agent handoff. Does not crawl."""
import argparse,json,urllib.request
from pathlib import Path
HERE=Path(__file__).resolve().parent;PROJECT=HERE.parents[1]
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['claim','heartbeat','progress','finish']);p.add_argument('--url',default='http://127.0.0.1:8877');p.add_argument('--job-id');p.add_argument('--stage');p.add_argument('--message',default='');p.add_argument('--counts-json',default='{}');p.add_argument('--data',default=str(PROJECT/'data/creative_platform'));a=p.parse_args();root=Path(a.data);key=json.loads((root/'credentials.json').read_text())['worker_token']
 body={'job_id':a.job_id,'name':'本机浏览器 Agent（交接模式）','status':a.stage,'message':a.message,'counts':json.loads(a.counts_json)}
 def request(path,payload):
  req=urllib.request.Request(a.url+path,data=json.dumps(payload).encode(),headers={'Content-Type':'application/json','Authorization':'Bearer '+key},method='POST')
  with urllib.request.urlopen(req,timeout=120) as r:return json.load(r)
 result=request('/api/worker/'+a.action,body)
 if a.action=='claim' and result.get('job'):
  j=result['job'];folder=root/'runs'/j['id'];folder.mkdir(parents=True,exist_ok=True);(folder/'request-scope.json').write_text(json.dumps(j['scope'],ensure_ascii=False,indent=2));print(json.dumps({'job_id':j['id'],'scope_file':str(folder/'request-scope.json'),'status':'preparing','instruction':'使用 guangdada-browser-crawl skill 通过已有有头浏览器执行。此桥接不自动驱动浏览器；执行完成后提交证据，不能伪造审核或进度。'},ensure_ascii=False))
 else:print(json.dumps(result,ensure_ascii=False))
if __name__=='__main__':main()
