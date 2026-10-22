"""Bounded downloads of browser-observed media and existing review evidence bridge."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import fcntl
import hashlib
import importlib.util
import json
import re
from pathlib import Path
import subprocess
import sys
import time
from urllib.parse import urlparse

import requests
from PIL import Image
from ua_workflows import social_chat_browser_daily as daily


def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def inside(base, rel):
    p=(Path(base)/rel).resolve()
    if not p.is_relative_to(Path(base).resolve()):raise ValueError('Unsafe media path')
    return p


@contextmanager
def queue_lock(base):
    p=Path(base)/'runtime/direct-media.lock';p.parent.mkdir(parents=True,exist_ok=True)
    with p.open('a') as stream:
        try:fcntl.flock(stream,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise ValueError('A direct media queue is already active')
        yield


def source(card):
    if card['media_type']=='图片':
        return [('http',i['src']) for i in card['observed_images'] if i.get('is_original') and urlparse(i['src']).scheme in ('https','http')]
    urls=[]
    for video in card['observed_videos']:
        urls.extend([video.get('src')]+video.get('sources',[]))
    urls=[u for u in urls if u and urlparse(u).scheme in ('https','http')]
    if urls:return [('http',urls[0])]
    links=[l['href'] for l in card['links'] if urlparse(l['href']).hostname in ('youtube.com','www.youtube.com','m.youtube.com','youtu.be')]
    if not links:return []
    u=urlparse(links[0])
    from urllib.parse import parse_qs
    video_id=parse_qs(u.query).get('v',[''])[0] if u.path=='/watch' else u.path.strip('/').split('/')[-1] if u.hostname=='youtu.be' or u.path.startswith(('/shorts/','/embed/')) else ''
    return [('youtube' if re.fullmatch(r'[A-Za-z0-9_-]{11}',video_id) else 'invalid_youtube',links[0])]


def verify_media(p, kind):
    if not p.is_file() or not p.stat().st_size:raise ValueError('Empty media')
    if kind=='图片':
        with Image.open(p) as im:
            im.load();return dict(width=im.width,height=im.height,format=im.format,decode_verified=True)
    info=json.loads(subprocess.check_output(['ffprobe','-v','error','-protocol_whitelist','file,pipe','-show_streams','-show_format','-of','json',str(p)],timeout=30))
    v=next(s for s in info['streams'] if s['codec_type']=='video')
    duration=float(v.get('duration') or info['format']['duration'])
    if duration<=0:raise ValueError('Invalid video duration')
    subprocess.run(['ffmpeg','-v','error','-nostdin','-threads','1','-protocol_whitelist','file,pipe','-i',str(p),'-f','null','-'],capture_output=True,check=True,timeout=120)
    return dict(width=v['width'],height=v['height'],duration_seconds=duration,format=info['format']['format_name'],decode_verified=True)


def transfer(url, output):
    started=time.monotonic()
    with requests.get(url,stream=True,timeout=(15,30)) as response:
        if response.status_code!=200:raise ValueError('HTTP '+str(response.status_code))
        if any(t in response.headers.get('Content-Type','').lower() for t in ('html','json')):raise ValueError('Non-media HTTP response')
        size=0
        with output.open('wb') as stream:
            for chunk in response.iter_content(262144):
                size+=len(chunk)
                if size>512*1024*1024 or time.monotonic()-started>120:raise ValueError('Media size/time limit')
                stream.write(chunk)
        length=response.headers.get('Content-Length')
        if not size or (length and int(length)!=size):raise ValueError('Incomplete media body')


def download(base, card, index, kind, url):
    if not re.fullmatch(r'[a-f0-9]{32}',card['ad_id']):raise ValueError('Invalid media ad ID')
    base=Path(base).resolve();root=base/'tasks'/card['task_id'];stem=card['ad_id']+f'-{index}'
    proof=root/f'sources/{stem}.download.json';proof.parent.mkdir(parents=True,exist_ok=True)
    old=daily.load(proof) if proof.exists() else None
    if old:
        if old['source_url']!=url:raise ValueError('Observed resource changed; preserve prior receipt and resolve explicitly')
        if old['status']=='failed':return old
        p=inside(base,old['path'])
        if p.is_file() and sha(p)==old['sha256']:return old
        raise ValueError('Cached verified media changed')
    result=dict(task_id=card['task_id'],ad_id=card['ad_id'],media_type=card['media_type'],source_kind=kind,source_url=url,status='failed',attempts=[])
    if kind=='invalid_youtube':
        result['reason']='Observed YouTube URL is not a complete single-video link'
        result['attempts']=[{'kind':'source_observation','reason':result['reason']}]
        daily.save(proof,result);return result
    temp=root/f'sources/{stem}.part';log=root/f'sources/{stem}.youtube.log'
    for attempt in range(1,2 if kind=='youtube' else 3):
        started=time.monotonic()
        try:
            if kind=='youtube':
                with log.open('w') as stream:
                    subprocess.run([sys.executable,'-m','yt_dlp','--no-playlist','--no-progress','--socket-timeout','20','--retries','1','-f','best[ext=mp4]/best','-o',str(temp),url],stdout=stream,stderr=subprocess.STDOUT,timeout=180,check=True)
            else:transfer(url,temp)
            info=verify_media(temp,card['media_type']);ext='.mp4' if card['media_type']=='视频' else '.'+{'JPEG':'jpg'}.get(info['format'],info['format'].lower())
            dest=root/f'sources/{stem}{ext}';temp.replace(dest)
            result.update(status='verified',path=str(dest.relative_to(base)),sha256=sha(dest),bytes=dest.stat().st_size,media_info=info)
            result['attempts'].append(dict(number=attempt,status='verified',elapsed_seconds=time.monotonic()-started));break
        except Exception as exc:
            reason=str(exc)
            if kind=='youtube' and log.exists():
                lines=[line for line in log.read_text(errors='replace').splitlines() if 'ERROR:' in line]
                if lines:reason=lines[-1]
            reason=re.sub(r'https?://\S+','[source URL]',reason)[:500]
            result['attempts'].append(dict(number=attempt,status='failed',error_type=type(exc).__name__,reason=reason,log=str(log.relative_to(base)) if kind=='youtube' else None,elapsed_seconds=time.monotonic()-started))
    daily.save(proof,result)
    return result


def prefetch(base):
    base=Path(base).resolve();scope=daily.load(base/'scope.json');items=[];missing=[]
    tasks={t['id']:t for t in scope['tasks']}
    for f in sorted((base/'tasks').glob('*/cards/*.json')):
        c=daily.load(f)
        if c['first_discovered_date']!=scope['target_date'] or c['media_type'] not in ('视频','图片'):continue
        task=tasks[f.parent.parent.name]
        if any(c[k]!=task[k] for k in ('app_id','platform')) or c['scope_sha256']!=daily.load(base/'run.json')['scope_sha256']:raise ValueError('Media card scope/identity mismatch')
        c['task_id']=task['id'];urls=list(dict.fromkeys(source(c)))
        if not urls:
            receipt=f.parent.parent/f'sources/{c["ad_id"]}-missing.download.json'
            result=dict(task_id=task['id'],ad_id=c['ad_id'],status='failed',reason='No browser-observed original HTTP or YouTube URL',attempts=[{'kind':'source_observation','card_path':str(f.relative_to(base)),'card_sha256':sha(f)}])
            if not receipt.exists():daily.save(receipt,result)
            missing.append(result);continue
        items.extend((c,n+1,kind,url) for n,(kind,url) in enumerate(urls))
    with ThreadPoolExecutor(max_workers=3) as pool:
        results=list(pool.map(lambda i:download(base,*i),items))
    summary=dict(verified=sum(d['status']=='verified' for d in results),failed=sum(d['status']=='failed' for d in results),missing_original=len(missing),media_files=len(results))
    daily.save(base/'direct-download-result.json',summary);return summary


def review_media(base):
    """Create existing image/six-frame schemas, never a semantic review decision."""
    from ua_workflows.social_chat_mixed_window import collection
    base=Path(base).resolve();_,scope,fp,cards,_=collection(base,daily.load(daily.CONFIG))
    spec=importlib.util.spec_from_file_location('direct_frames',daily.ROOT/'skills/guangdada-browser-crawl/scripts/frames.py');frames=importlib.util.module_from_spec(spec);spec.loader.exec_module(frames)
    waivers=[];prepared=0
    for c in cards:
        if c['media_type']=='试玩' or daily.is_relaunch(c):continue
        root=Path(c['source_collection']);receipts=sorted((root/'sources').glob(c['ad_id']+'-*.download.json'));records=[daily.load(p) for p in receipts]
        urls=list(dict.fromkeys(source(c)))
        if not records or any(r['status']!='verified' for r in records):
            if not records:raise ValueError('Download not attempted: '+c['ad_id'])
            bad=next(r for r in records if r['status']!='verified')
            waivers.append(dict(task_id=c['task_id'],ad_id=c['ad_id'],reason=bad.get('reason') or bad['attempts'][-1]['reason'],failure={'status':'failed','ad_id':c['ad_id']},attempts=[str(p.relative_to(base)) for p in receipts]));continue
        if len(records)!=len(urls) or {(r['source_kind'],r['source_url']) for r in records}!=set(urls):raise ValueError('Incomplete/substituted media receipt set')
        for r in records:
            if sha(inside(base,r['path']))!=r['sha256']:raise ValueError('Downloaded media changed')
        if c['media_type']=='图片':
            out=root/f'evidence/images/{c["ad_id"]}/manifest.json'
            if not out.exists():
                images=[]
                for r in records:
                    p=inside(base,r['path']);info=verify_media(p,'图片');images.append(dict(url=r['source_url'],path=str(p.relative_to(root)),sha256=r['sha256'],**info))
                daily.save(out,dict(ad_id=c['ad_id'],scope_sha256=fp,images=images))
        else:
            out=root/f'evidence/frames/{c["ad_id"]}'
            if not (out/'manifest.json').exists():frames.extract(inside(base,records[0]['path']),root,out,fp,c['ad_id'])
        prepared+=1
    daily.save(base/'missing_media.json',waivers)
    return dict(prepared=prepared,unavailable=len(waivers),review_decisions_created=0)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['prefetch','review-media']);p.add_argument('--run-dir',type=Path,required=True);p.add_argument('--watch',action='store_true');a=p.parse_args()
    from ua_workflows.collection_runtime import collection_phase
    with queue_lock(a.run_dir), collection_phase(a.run_dir, 'social_chat_direct_media'):
        deadline=time.monotonic()+900
        while True:
            result=prefetch(a.run_dir) if a.action=='prefetch' else review_media(a.run_dir)
            if not a.watch or a.action!='prefetch':break
            scope=daily.load(a.run_dir/'scope.json')
            if all((a.run_dir/'tasks'/t['id']/'direct/complete.json').exists() for t in scope['tasks']):break
            if any(daily.load(p).get('control') for p in (a.run_dir/'tasks').glob('*/direct/checkpoint.json')):break
            if time.monotonic()>=deadline:break
            time.sleep(2)
    print(json.dumps(result,ensure_ascii=False))

if __name__=='__main__':main()
