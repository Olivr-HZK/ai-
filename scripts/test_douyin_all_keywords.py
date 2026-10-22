"""Run/resume daily Douyin keyword sampling through durable asynchronous jobs."""
import argparse
import json
from pathlib import Path
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from ua_workflows.social_search import collect_douyin


def main():
    from ua_workflows.shared.config import project_env_values
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-dir',type=Path);a=p.parse_args()
    cfg=json.loads((ROOT/'config/social_chat_douyin_daily.json').read_text());key=project_env_values().get('EVERYINFRA_API_KEY')
    if not key:raise ValueError('Missing EveryInfra credential')
    out=a.run_dir.resolve() if a.run_dir else ROOT/'data/douyin_keyword_runs'/datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%dT%H%M%S_%f')
    result=collect_douyin(out,cfg,key=key)
    print(json.dumps({'directory':str(out),**result},ensure_ascii=False))
    return 0 if all(t['status'] in ('search_verified','response_needs_review') for t in result['keywords']) else 1


if __name__=='__main__':raise SystemExit(main())
