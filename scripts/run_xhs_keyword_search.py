"""Run/resume XHS keyword pages through durable async jobs; never replay unknown submissions."""
import argparse
import json
from pathlib import Path
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
TZ=ZoneInfo('Asia/Shanghai')
from ua_workflows.collection_runtime import write as save
from ua_workflows.social_search import collect_xhs, cursor as next_cursor


def main():
    from ua_workflows.shared.config import project_env_values
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-dir');a=p.parse_args()
    cfg=json.loads((ROOT/'config/social_chat_xhs_daily.json').read_text())
    key=project_env_values().get('EVERYINFRA_API_KEY')
    if not key:raise ValueError('Missing EveryInfra credential')
    out=Path(a.run_dir).resolve() if a.run_dir else ROOT/'data/xhs_keyword_runs'/datetime.now(TZ).strftime('%Y%m%dT%H%M%S_%f')
    result=collect_xhs(out,cfg,key=key)
    print(json.dumps({'directory':str(out),**result},ensure_ascii=False))
    return 0 if result['status']=='complete' else 1


if __name__=='__main__':raise SystemExit(main())
