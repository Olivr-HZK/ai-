"""Explicit revalidation of pre-v1 visible-browser evidence, without rewriting it."""
import argparse,hashlib,json,re,shutil
from pathlib import Path
from urllib.parse import urlparse,parse_qs
from PIL import Image
from ua_workflows import social_chat_browser_daily as daily
from ua_workflows import social_chat_daily_state as state


def check(ok,msg):
    if not ok:raise ValueError(msg)


def collection(base,scope,task_id=None):
    base=Path(base).resolve();fp=state.fingerprint(scope);request=daily.load(base/'historical-direct-recovery.json')
    check(request['scope_sha256']==fp and request['schema']=='visible-direct-recovery/v1' and request.get('authorization'),'Recovery request scope/authorization missing')
    for rel,digest in request['source_hashes'].items():
        check(hashlib.sha256((base/rel).read_bytes()).hexdigest()==digest,'Historical evidence changed: '+rel)
    cards=[];seals=[];evidence=[]
    def image(entry):
        if isinstance(entry,str):entry={'path':entry}
        check(entry and entry.get('path'),'Missing actual screenshot')
        p=(base/entry['path']).resolve();check(p.is_relative_to(base) and p.is_file(),'Screenshot missing/unsafe')
        if entry.get('sha256'):check(hashlib.sha256(p.read_bytes()).hexdigest()==entry['sha256'],'Screenshot hash changed')
        with Image.open(p) as im:im.verify()
        evidence.append(str(p.relative_to(base)))
    for task in scope['tasks']:
        if task_id and task['id']!=task_id:continue
        root=base/'tasks'/task['id'];seal=daily.load(root/'direct-complete.json')
        check(all(seal[k]==task[k] for k in ['id','app_id','app_name','platform']),'Historical seal identity mismatch')
        check(seal['target_date']==scope['target_date'],'Historical seal date mismatch')
        found=[];seen=set()
        for f in sorted((root/'cards').glob('*.json')):
            c=daily.load(f);check(c['scope_sha256']==fp and c['detail_loaded'] is True,'Historical card scope/loading mismatch')
            check(all(c[k]==task[k] for k in ['app_id','platform','app_name']),'Historical card identity mismatch')
            check(re.fullmatch('[a-f0-9]{32}',c['ad_id']) and c['ad_id'] not in seen,'Invalid/duplicate ad ID');seen.add(c['ad_id'])
            check(parse_qs(urlparse(c['detail_url']).query).get('id')==[c['ad_id']],'Detail URL mismatch')
            stores=[]
            for link in c['links']:
                u=urlparse(link['href']);q=parse_qs(u.query)
                if u.hostname in ['apps.apple.com','itunes.apple.com']:
                    m=re.search(r'/id(\d+)',u.path)
                    if m:stores.append((m[1],'iOS'))
                if u.hostname=='play.google.com' and q.get('id'):stores.append((q['id'][0],'Android'))
            if not stores:
                ident=c.get('observed_advertiser_identity')
                if not ident and (root/'evidence/advertiser-identity.json').exists():ident=daily.load(root/'evidence/advertiser-identity.json')
                check(ident,'No observed store/advertiser identity')
                q=parse_qs(urlparse(ident.get('actual_url') or ident.get('url','')).query)
                check(q.get('store')==[task['app_id']] and q.get('os')==['1' if task['platform']=='iOS' else '2'],'Unproven advertiser store/os')
                check(task['app_name'] in c['raw_text'],'Advertiser mapping not reflected in visible detail')
                stores=[(q['store'][0],task['platform'])]
            check((task['app_id'],task['platform']) in stores,'Actual store identity differs')
            before=c['evidence'].get('before')
            if not before:
                options=[root/f'evidence/direct/{c["page"]}-{c["position"]}-before.png',root/f'evidence/direct/{f.stem}-before.png',root/f'evidence/direct/{c["page"]}-{c["position"]}-preview-before.png']
                found_before=next((p for p in options if p.exists()),None);check(found_before,'Missing historical before image');before=str(found_before.relative_to(base))
            image(before);image(c['evidence']['after'])
            c['first_discovered_date']=daily.discovered(c);check(c['first_discovered_date']<=scope['target_date'],'Unexpected future card')
            if c['first_discovered_date']==scope['target_date']:daily.estimate(c['impressions_estimate'])
            if c['media_type']=='试玩':check('试玩广告' in c['raw_text'] and c['observed_iframes'] and not c['observed_videos'] and not c['observed_images'],'Unproven playable')
            elif c['media_type']=='图片':check(c['observed_images'] and all(i.get('is_original') for i in c['observed_images']),'Original image not observed')
            else:check(c['media_type']=='视频' and (c['observed_videos'] or any(urlparse(l['href']).hostname in ['youtube.com','www.youtube.com','youtu.be'] for l in c['links'])),'Unknown media type')
            tail=c['raw_text'].split('当页查看创意')[-1]
            for key,start,end in [('channels','投放渠道','文案语言'),('languages','文案语言','地区'),('regions','地区','素材尺寸')]:
                m=re.search(re.escape(start)+r'\s*\n(.*?)(?=\n(?:投放来源|文案语言|地区|素材尺寸|人气值|总人气值|投放天数|投放账号|展开更多信息)|$)',tail,re.S)
                c[key]=[s.strip() for s in m[1].splitlines() if s.strip() and not re.fullmatch(r'\(\d+\)',s.strip())] if m else []
                if not m:c.setdefault('missing_fields',[]).append(key)
            c['channel']='\n'.join(c['channels']);c['language']='\n'.join(c['languages'])
            c['video_url']=next((v.get('src') for v in c['observed_videos'] if (v.get('src') or '').startswith(('http://','https://'))),None)
            c['external_url']=next((l['href'] for l in c['links'] if urlparse(l['href']).hostname in ['youtube.com','www.youtube.com','youtu.be']),None)
            dim=re.search(r'(\d+)\s*[x×]\s*(\d+)',tail);c['dimensions']=dim.group(0) if dim else ''
            c.update(task_id=task['id'],source_collection=str(root));found.append(c)
        check(len(found)==seal['checked_cards'],'Historical checked count mismatch')
        positions=[(c['page'],c['position']) for c in found];check(positions==sorted(set(positions)),'Repeated positions')
        pages=sorted({a for a,b in positions});check(not pages or pages==list(range(1,max(pages)+1)),'Missing pages')
        for page in pages:
            pos=[b for a,b in positions if a==page];check(pos==list(range(1,len(pos)+1)),'Missing positions')
            if page<max(pages):check(len(pos)==60,'Incomplete prior page')
        if seal['terminal_reason']=='explicit_no_results':
            check(not found,'Zero result with cards');txt=(root/'evidence/direct-terminal.txt').read_text();check('暂无数据' in txt and task['app_id'] in txt,'Zero lacks query/no-data evidence');image(seal['image'])
        else:
            check(seal['terminal_reason']=='first_discovered_before_target' and found and found[-1]['first_discovered_date']<scope['target_date'] and all(c['first_discovered_date']==scope['target_date'] for c in found[:-1]),'Invalid historical cutoff')
            check(found[-1]['ad_id']==seal['cutoff_card'] and found[-1]['first_discovered_date']==seal['cutoff_date'],'Cutoff mismatch')
            if seal.get('image'):image(seal['image'])
        check(seal.get('target_date_items',sum(c['first_discovered_date']==scope['target_date'] for c in found))==sum(c['first_discovered_date']==scope['target_date'] for c in found),'Target-day count mismatch')
        cards.extend(c for c in found if c['first_discovered_date']==scope['target_date']);seals.append(seal)
    return cards,seals


def recover(base,authorization):
    base=Path(base).resolve();scope=daily.load(base/'scope.json');p=base/'historical-direct-recovery.json'
    if not p.exists():
        inputs=[base/'scope.json',base/'run.json',base/'crawl-notice-start.json']
        inputs+=list((base/'tasks').glob('*/cards/*.json'))+list((base/'tasks').glob('*/direct-complete.json'))
        inputs+=[f for f in (base/'tasks').glob('*/evidence/**/*') if f.is_file() and not any(s in f.parts for s in ['frames','images'])]
        daily.save(p,dict(schema='visible-direct-recovery/v1',scope_sha256=state.fingerprint(scope),authorization=authorization,requested_at=state.now(),source_hashes={str(f.relative_to(base)):hashlib.sha256(f.read_bytes()).hexdigest() for f in inputs}))
    from ua_workflows.social_chat_mixed_window import collection as mixed_collection
    _,_,_,cards,seals=mixed_collection(base,daily.load(daily.CONFIG))
    if daily.load(base/'daily-machine/state.json').get('execution_plan'):return state.status(base)
    folder=base/'daily-machine';old=folder/'state.json';archive=folder/'pre-direct-recovery-state.json';check(not archive.exists(),'Recovery archive already exists');shutil.copy2(old,archive)
    doc=dict(schema='social-chat-daily-state/v1',scope_sha256=state.fingerprint(scope),target_date=scope['target_date'],cursor=0,control=None,active_attempt=None,attempts=[],completed=[],max_attempts=3,prepared_at=state.now(),execution_plan=[{'state':'COLLECTION_RECOVERY_PENDING','task':None}]+[{'state':s,'task':None} for s in state.GLOBAL_AFTER],recovery_archive=str(archive.relative_to(base)))
    state.write(old,doc);state.begin(base);state.finish(base)
    return state.status(base)


def main():
    p=argparse.ArgumentParser();p.add_argument('--run-dir',type=Path,required=True);p.add_argument('--authorization',required=True);a=p.parse_args();print(json.dumps(recover(a.run_dir,a.authorization),ensure_ascii=False))
if __name__=='__main__':main()
