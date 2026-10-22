"""Validate browser direct-session evidence; no browser or site API access."""
import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from ua_workflows import social_chat_browser_daily as daily

MODE = 'direct-browser-v1'


def category(scope, task):
    value = task.get('category', scope['filters']['category'])
    if value not in ('游戏', '工具'):
        raise ValueError('Invalid task category')
    return value


def validate_preparation(root, scope, task, relative):
    from ua_workflows.social_chat_mixed_window import file, hashed, require
    p=daily.load(file(Path(root),relative))
    fp=hashlib.sha256(json.dumps(scope,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
    expected=dict(category=category(scope,task),dedupe='素材',date={'start':scope['target_date'],'end':scope['target_date']},media='图片&视频',selected_types=[],channels=[],regions=[],languages=[],size='',search_mode='综合',query_terms=[task['app_id']],selected_app_ids=[],sort='最新创意')
    require(p.get('scope_sha256')==fp and p.get('task_id')==task['id'] and p.get('settled') is True and p.get('actual')==expected, 'Direct prepared filters/query mismatch')
    require(set(p.get('images',{}))=={'before','after'}, 'Missing direct preparation screenshots')
    for e in p['images'].values():hashed(Path(root),e)
    queries=[a for a in p['actions'] if a['name']=='app.query' and a.get('interaction')=={'action':'press','target':'Enter'}]
    sorts=[a for a in p['actions'] if a['name']=='sort' and a.get('after')=='最新创意']
    require(len(queries)==len(sorts)==1 and sorts[0]['started_at']>=queries[0]['ended_at'], 'Missing real query followed by latest sort')
    base_expected={k:v for k,v in expected.items() if k not in ('search_mode','query_terms','selected_app_ids','sort')}
    changes=[a for a in p['actions'] if a['name']=='filters']
    require(len(changes)==1 and changes[0]['after']==base_expected and changes[0]['action']==('readonly' if changes[0]['before']==base_expected else 'repair'), 'Invalid direct filter action')
    return p


def validate_task(base, scope, task):
    from ua_workflows.social_chat_mixed_window import require, file, hashed, sha
    base = Path(base).resolve(); root = base/'tasks'/task['id']
    fp = hashlib.sha256(json.dumps(scope, sort_keys=True, ensure_ascii=False).encode()).hexdigest()

    def document(path):
        value = daily.load(file(root, path))
        require(value.get('scope_sha256') == fp and value.get('task_id') == task['id'], 'Direct evidence scope/task mismatch')
        return value

    def images(value):
        require(set(value) == {'before', 'after'}, 'Direct before/after screenshots missing')
        for entry in value.values():
            p = hashed(root, entry)
            require(p.read_bytes().startswith((b'\x89PNG\r\n\x1a\n', b'\xff\xd8\xff')), 'Invalid screenshot format')

    prep = validate_preparation(root,scope,task,'direct/prepare.json'); images(prep['images'])
    preparations={'direct/prepare.json':prep}
    expected = dict(category=category(scope,task), dedupe='素材', date={'start':scope['target_date'],'end':scope['target_date']},
                    media='图片&视频', selected_types=[], channels=[], regions=[], languages=[], size='',
                    search_mode='综合', query_terms=[task['app_id']], selected_app_ids=[], sort='最新创意')
    require(prep['actual'] == expected and prep.get('settled') is True, 'Direct prepared filters/query mismatch')
    actions = prep['actions']
    queries = [a for a in actions if a['name']=='app.query' and a.get('interaction')=={'action':'press','target':'Enter'}]
    sorts = [a for a in actions if a['name']=='sort' and a.get('after')=='最新创意']
    require(len(queries)==1 and len(sorts)==1 and sorts[0]['started_at']>=queries[0]['ended_at'], 'Missing real query followed by latest sort')
    for action in actions:
        require(datetime.fromisoformat(action['ended_at'].replace('Z','+00:00')) >= datetime.fromisoformat(action['started_at'].replace('Z','+00:00')), 'Invalid direct operation timing')
        if action['name']=='filters':
            require(action['action']==('readonly' if action['before']==action['after'] else 'repair'), 'Redundant direct filter mutation')
    require(any(a['name']=='filters' for a in actions), 'Missing actual filter observation')
    seal = document('direct/complete.json'); images(seal['images'])
    cards = []
    paths = sorted((root/'cards').glob('*.json'))
    require(len(paths)==seal['checked_cards']==len(seal['cards']), 'Direct seal/card count mismatch')
    manifest = {e['path']:e for e in seal['cards']}
    ids = set(); page_rows = {}
    for path in paths:
        rel = str(path.relative_to(root)); require(rel in manifest and sha(path)==manifest[rel]['sha256'], 'Direct card seal hash mismatch')
        c = document(rel); images(c['evidence'])
        import re
        require(bool(re.fullmatch('[a-f0-9]{32}', c['ad_id'])), 'Invalid direct ad ID')
        require(c.get('detail_loaded') is True and c.get('observed_platform')==task['platform'] and all(c.get(k)==task[k] for k in ('app_id','platform','app_name')), 'Direct card identity mismatch')
        require(c['ad_id'] not in ids, 'Duplicate direct ad ID'); ids.add(c['ad_id'])
        url = urlparse(c['detail_url'])
        require(url.hostname=='guangdada.net' and parse_qs(url.query).get('id')==[c['ad_id']], 'Direct detail URL/ad mismatch')
        stores=[]
        for link in c['links']:
            u=urlparse(link['href'])
            if u.hostname in ('apps.apple.com','itunes.apple.com'):
                import re
                m=re.search(r'/id(\d+)',u.path)
                if m:stores.append((m[1],'iOS'))
            elif u.hostname=='play.google.com': stores.append((parse_qs(u.query).get('id',[None])[0],'Android'))
        if not stores:
            identity=document(c['identity_evidence']);images(identity['images'])
            u=urlparse(identity['url']); q=parse_qs(u.query)
            require(u.hostname=='guangdada.net' and q.get('store')==[task['app_id']] and q.get('os')==['1' if task['platform']=='iOS' else '2'], 'Unproven direct advertiser identity')
            require(c['observed_app_name']==identity['observed_app_name'] and c['observed_platform']==task['platform'], 'Advertiser identity mapping mismatch')
        else:require((task['app_id'],task['platform']) in stores, 'Observed store ID/platform mismatch')
        ref=c['prepare_evidence']
        if ref not in preparations:preparations[ref]=validate_preparation(root,scope,task,ref)
        require(c['observed_at']>=c['clicked_at']>=preparations[ref]['observed_at'], 'Direct card time/order mismatch')
        date=daily.discovered(c);require(c['first_discovered_date']==date and c['first_discovered_source']=='detail_date_range_start', 'Direct date source mismatch')
        kind=c['media_type'];require(kind in ('视频','图片','试玩'), 'Unknown direct media')
        if kind=='试玩':require('试玩广告' in c['raw_text'] and c['observed_iframes'] and not c['observed_videos'] and not c['observed_images'], 'Unproven direct playable')
        if date==scope['target_date']:daily.estimate(c['impressions_estimate'])
        page_rows.setdefault(c['page'],[]).append(c['position'])
        c.update(task_id=task['id'],source_collection=str(root));cards.append(c)
    pages=sorted(page_rows)
    require(pages==list(range(1,len(pages)+1)), 'Missing direct page')
    previous_signature=None
    for n in pages or [1]:
        p=document(f'direct/pages/{n:04d}.json');images(p['images'])
        require(p['page']==n and p['query_terms']==[task['app_id']] and p['settled'] is True, 'Direct page context mismatch')
        require(p['signature'] and p['signature']!=previous_signature, 'Direct page did not change');previous_signature=p['signature']
        positions=page_rows.get(n,[])
        require(positions==list(range(1,len(positions)+1)) and len(positions)<=p['visible_count'], 'Missing direct card position')
        if n<max(pages or [1]):require(len(positions)==p['visible_count'] and p['has_next'] is True, 'Incomplete direct page before next')
    dates=[c['first_discovered_date'] for c in cards]
    require(sum(d==scope['target_date'] for d in dates)==seal['target_date_items'], 'Direct target count mismatch')
    reason=seal['terminal_reason']
    if reason=='first_discovered_before_target':
        require(cards and dates[-1]<scope['target_date'] and all(d>=scope['target_date'] for d in dates[:-1]) and seal['cutoff_card']==cards[-1]['ad_id'], 'Invalid direct first-date cutoff')
    elif reason=='explicit_no_results':
        p=document('direct/pages/0001.json');require(not cards and p['explicit_no_results'] is True and p['visible_count']==0 and '暂无数据' in p['raw_text'], 'Unproven direct zero')
    elif reason=='next_disabled':
        p=document(f'direct/pages/{pages[-1]:04d}.json')
        require(cards and p['has_next'] is False and len(page_rows[pages[-1]])==p['visible_count'] and all(d>=scope['target_date'] for d in dates), 'Unproven direct end page')
    else:raise ValueError('Unsupported direct terminal reason')
    return [c for c in cards if c['first_discovered_date']==scope['target_date']], seal


def accept(base, task_id):
    """One state validation per App, rather than seven browser-stage CLI loops."""
    from ua_workflows import social_chat_daily_state as state
    current=state.status(base)
    if current['state']!='APP_COLLECTION_PENDING' or current['task']!=task_id:
        raise ValueError('Direct acceptance is not the current App step')
    import time
    start=time.perf_counter()
    try:
        result=state.finish(base)
    except Exception:
        daily.save(Path(base)/'tasks'/task_id/'direct/validation.json',{'status':'failed','validation_seconds':time.perf_counter()-start})
        raise
    timing={'status':'passed','validation_seconds':time.perf_counter()-start}
    daily.save(Path(base)/'tasks'/task_id/'direct/validation.json',timing)
    return {**result,**timing}


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['accept','check']);p.add_argument('--run-dir',required=True,type=Path);p.add_argument('--task',required=True);a=p.parse_args()
    if a.action=='accept': result=accept(a.run_dir,a.task)
    else:
        scope=daily.load(a.run_dir/'scope.json');task=next(t for t in scope['tasks'] if t['id']==a.task);cards,seal=validate_task(a.run_dir,scope,task);result={'task':a.task,'target_items':len(cards),'terminal_reason':seal['terminal_reason']}
    print(json.dumps(result,ensure_ascii=False))

if __name__=='__main__':main()
