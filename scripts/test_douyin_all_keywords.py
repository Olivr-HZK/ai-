"""Probe all configured keywords once, using Douyin's documented maximum limit."""
import json
import argparse
from pathlib import Path
import subprocess
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from ua_workflows.collection_runtime import keep_awake

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--run-dir',type=Path);args=parser.parse_args()
    now=datetime.now(ZoneInfo('Asia/Shanghai'))
    out=args.run_dir.resolve() if args.run_dir else ROOT/'data/douyin_keyword_runs'/now.strftime('%Y%m%dT%H%M%S_%f')
    out.mkdir(parents=True,exist_ok=True)
    if (out/'progress.json').exists():raise ValueError('Existing search evidence must not be overwritten')
    cfg=ROOT/'config/social_chat_douyin_daily.json'
    keywords=json.loads(cfg.read_text())['keywords']
    results=[]
    with keep_awake(out/'search-awake.json'):
        for keyword in keywords:
            proc=subprocess.run([sys.executable,str(ROOT/'scripts/test_douyin_keyword_search.py'),'--keyword',keyword,'--limit','50'],cwd=ROOT,capture_output=True,text=True)
            records=[]
            for line in proc.stdout.splitlines():
                try:records.append(json.loads(line))
                except ValueError:pass
            if records and 'directory' in records[-1] and (Path(records[-1]['directory'])/'response.json').exists():
                row=records[-1];p=json.loads((Path(row['directory'])/'response.json').read_text())
                row.update(has_next_page_token=bool(p.get('next_page_token')),pagination=p.get('pagination'),has_more=p.get('has_more'),response_keys=list(p),returned_rows_at_limit=row['returned_notes']==50)
            elif records and 'directory' in records[-1]:row={**records[-1], 'exit_code':proc.returncode}
            else:row={'keyword':keyword,'status':'failed','exit_code':proc.returncode}
            results.append(row)
            (out/'progress.json').write_text(json.dumps(results,ensure_ascii=False,indent=2))
            print(json.dumps(row,ensure_ascii=False),flush=True)
    summary={'keywords':results,'total_returned':sum(r.get('returned_notes',0) for r in results),'cost_usd':round(sum(r.get('charged_usd') or 0 for r in results),6),'pagination_supported_by_catalog':False,'full_search_exhaustion_proven':False,'started_at':now.isoformat(),'finished_at':datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()}
    (out/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2))
    print(json.dumps({'directory':str(out),'total_returned':summary['total_returned'],'cost_usd':summary['cost_usd']},ensure_ascii=False))

if __name__=='__main__':main()
