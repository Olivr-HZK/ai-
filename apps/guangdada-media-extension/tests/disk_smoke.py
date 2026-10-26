"""Real disposable Chrome/OPFS/downloads test; all media comes from loopback.

The large MP4 has a valid trailing `free` box, so it is a real >512MiB
transfer without hours of video or a giant server-side memory allocation.
Only the disposable extension copy has allocation guards/fault injection.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import tempfile
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from zipfile import ZipFile
from playwright.sync_api import sync_playwright
import browser_smoke as media

SOURCE=Path(__file__).resolve().parents[1]
EVIDENCE=SOURCE.parents[1]/'data/guangdada-metadata-acceptance'
LARGE_MIB=8 if os.environ.get("DISK_QUICK_TEST") else 560
MIB=1024*1024
ZERO=bytes(65536)
VIDEO=b''
REQUESTS=Counter()
ACTIVE=0
PEAK=0
LOCK=threading.Lock()

def payload(path):
    if path=='/large.mp4':
        size=LARGE_MIB*MIB
        return VIDEO+struct.pack('>I4s',size-len(VIDEO),b'free'),size,'video/mp4'
    return media.PNG,4*MIB if path.startswith('/image-') else 16*MIB,'image/png'

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        global ACTIVE,PEAK
        REQUESTS[self.path]+=1
        with LOCK:ACTIVE+=1;PEAK=max(PEAK,ACTIVE)
        try:
            prefix,size,kind=payload(self.path)
            self.send_response(200);self.send_header('Content-Type',kind);self.send_header('Content-Length',str(size));self.end_headers()
            self.wfile.write(prefix);left=size-len(prefix)
            while left:
                chunk=ZERO[:min(left,len(ZERO))];self.wfile.write(chunk);left-=len(chunk)
                if self.path=='/partial.png' and left<size-MIB:
                    self.close_connection=True;return
                if self.path.split('?')[0]=='/slow.png':self.wfile.flush();time.sleep(.03)
        except (BrokenPipeError,ConnectionResetError):pass
        finally:
            with LOCK:ACTIVE-=1
    def log_message(self,*args):pass

def fixture(origin,scenario):
    paths=['/large.mp4']+[f'/image-{i}.png' for i in range(16)] if scenario=='large' else ['/partial.png','/image-ok.png'] if scenario=='partial' else ['/slow.png']*3 if scenario=='stop' else [f'/quota-{i}.png' for i in range(12)]
    # Unique URLs/ad IDs make the three slow requests independently selectable.
    if scenario=='stop':paths=['/slow.png?'+str(i) for i in range(3)]
    return '''<!doctype html><meta charset="utf-8"><body><script>
const paths=PATHS,origin=ORIGIN;
for(const [i,path] of paths.entries()){
 const value={ad_key:'disk-'+i,ads_type:path.endsWith('.mp4')?2:1};
 value[value.ads_type===2?'video_url':'preview_img_url']=origin+path;
 const card=document.createElement('div');card.className='shadow-common-light bg-white';card.style.cssText='height:60px;position:relative';card.textContent='素材'+i;card.__reactFiber$t={memoizedProps:{value}};document.body.append(card);
}
</script>'''.replace('PATHS',json.dumps(paths)).replace('ORIGIN',json.dumps(origin))

def expected_digest(path):
    prefix,size,_=payload(path);h=hashlib.sha256(prefix);left=size-len(prefix)
    while left:n=min(left,len(ZERO));h.update(ZERO[:n]);left-=n
    return h.hexdigest()

def main():
    global VIDEO
    EVIDENCE.mkdir(exist_ok=True)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);threading.Thread(target=server.serve_forever,daemon=True).start();origin=f'http://127.0.0.1:{server.server_port}'
    try:
        with tempfile.TemporaryDirectory() as temp,sync_playwright() as p:
            root=Path(temp);sample=root/'sample.mp4'
            subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','color=c=blue:s=64x64:d=0.3','-c:v','libx264','-pix_fmt','yuv420p','-movflags','+faststart',str(sample)],check=True)
            VIDEO=sample.read_bytes();extension=root/'extension';shutil.copytree(SOURCE,extension)
            manifest=json.loads((extension/'manifest.json').read_text());manifest['host_permissions']=['https://www.guangdada.net/*',origin+'/*'];(extension/'manifest.json').write_text(json.dumps(manifest))
            # Forbid whole-response/whole-blob buffers and explicit allocations
            # above 16MiB. These guards do not implement any download/ZIP logic.
            (extension/'test-probe.js').write_text('''
globalThis.__diskConfig={};
globalThis.__diskStats={maxExplicitUint8Array:0,forbiddenWholeBuffers:0,peakObservedJSHeap:0};
const Native=globalThis.Uint8Array;
globalThis.Uint8Array=new Proxy(Native,{construct(target,args){const n=typeof args[0]==='number'?args[0]:args[0]?.byteLength || args[0]?.length || 0;__diskStats.maxExplicitUint8Array=Math.max(n,__diskStats.maxExplicitUint8Array);if(n>16*1024*1024)throw Error('TEST: media-sized allocation');return Reflect.construct(target,args);}});
for(const proto of [Response.prototype,Blob.prototype])proto.arrayBuffer=function(){__diskStats.forbiddenWholeBuffers++;throw Error('TEST: whole media buffer');};
setInterval(()=>{__diskStats.peakObservedJSHeap=Math.max(__diskStats.peakObservedJSHeap,performance.memory?.usedJSHeapSize || 0);},100);
''')
            off=extension/'offscreen.js';text=off.read_text();text="import './test-probe.js';\n"+text
            text=text.replace("await send({type:'zip-ready'","await chrome.runtime.sendMessage({target:'test-probe',stats:globalThis.__diskStats});await send({type:'zip-ready'")
            off.write_text(text)
            store=extension/'disk-store.js';text=store.read_text().replace("const writers=new Set(),operations=new Set();","globalThis.__diskConfig=await chrome.runtime.sendMessage({target:'test-probe',config:true});let testWritten=0;const writers=new Set(),operations=new Set();")
            text=text.replace('return await writer[method](value);',"if(method==='write'){testWritten+=value?.byteLength || 0;if(globalThis.__diskConfig?.quota && testWritten>1048576)throw new DOMException('Test simulated disk full','QuotaExceededError');}return await writer[method](value);")
            text=text.replace("throw diskError(error);}})();return cleaning;", "throw Object.assign(diskError(error),{message:'Cleanup '+error.name+': '+error.message});}})();return cleaning;")
            store.write_text(text)
            bg=extension/'background.js';text=bg.read_text().replace("if(message.target==='offscreen'", "if(message.target==='test-probe')return;if(message.target==='offscreen'")
            text=text.replace("await chrome.runtime.sendMessage({target:'offscreen',type:'cleanup'}).catch(()=>{});", "await chrome.runtime.sendMessage({target:'offscreen',type:'cleanup'}).then(result=>chrome.storage.local.set({diskTestCleanup:result})).catch(error=>chrome.storage.local.set({diskTestCleanup:{error:error.message}}));")
            text+="\nchrome.runtime.onMessage.addListener((m,s,r)=>{if(m.target!=='test-probe')return;(m.config?chrome.storage.local.get('diskTestConfig').then(v=>v.diskTestConfig || {}):chrome.storage.local.set({diskTestStats:m.stats}).then(()=>({ok:true}))).then(r);return true;});\n";bg.write_text(text)
            def launch():
                ctx=p.chromium.launch_persistent_context(str(root/'profile'),channel='chromium',headless=True,accept_downloads=True,args=[f'--disable-extensions-except={extension}',f'--load-extension={extension}'])
                ctx.on('console',lambda msg:print('BROWSER',msg.text,flush=True) if msg.type=='error' else None)
                worker=ctx.service_workers[0] if ctx.service_workers else ctx.wait_for_event('serviceworker');extid=worker.url.split('/')[2]
                ctx.route('https://www.guangdada.net/**',lambda route:route.fulfill(content_type='text/html',body=fixture(origin,'large')))
                page=ctx.new_page();page.goto('https://www.guangdada.net/modules/creative/display-ads')
                control=ctx.new_page();control.goto(f'chrome-extension://{extid}/report.html');tab=worker.evaluate("async()=>(await chrome.tabs.query({url:'https://www.guangdada.net/*'}))[0].id")
                return ctx,worker,page,control,tab
            ctx,worker,page,control,tab=launch()
            def state():return worker.evaluate('async()=>(await chrome.storage.local.get("job")).job')
            def wait(predicate,timeout=180):
                end=time.monotonic()+timeout;value=None
                while time.monotonic()<end:
                    value=state()
                    if value and predicate(value):return value
                    page.wait_for_timeout(100)
                raise AssertionError(value)
            def temp_dirs():return control.evaluate("async()=>{const root=await navigator.storage.getDirectory();try{const parent=await root.getDirectoryHandle('gdd-media-temporary');return Array.fromAsync(parent.keys());}catch(e){if(e.name==='NotFoundError')return [];throw e;}}")
            def clean():
                end=time.monotonic()+15
                while time.monotonic()<end:
                    if not temp_dirs():return
                    control.wait_for_timeout(100)
                raise AssertionError({'dirs':temp_dirs(),'cleanup':worker.evaluate('async()=>chrome.storage.local.get("diskTestCleanup")'),'contexts':worker.evaluate('async()=>chrome.runtime.getContexts({contextTypes:["OFFSCREEN_DOCUMENT"]})')})
            def start(scenario,config=None):
                worker.evaluate('async config=>chrome.storage.local.set({diskTestConfig:config})',config or {})
                page.route('https://www.guangdada.net/**',lambda route:route.fulfill(content_type='text/html',body=fixture(origin,scenario)));page.reload()
                value=control.evaluate("async id=>chrome.runtime.sendMessage({type:'start',tabId:id,kinds:['image','video','playable'],scroll:false,autoResolveExternal:true})",tab);assert value['ok'],value
            def download(value):return worker.evaluate('async id=>(await chrome.downloads.search({id}))[0]',value['downloadId'])
            # A prior crashed run must not consume space indefinitely.
            control.evaluate("async()=>{const root=await navigator.storage.getDirectory();const parent=await root.getDirectoryHandle('gdd-media-temporary',{create:true});const old=await parent.getDirectoryHandle('abandoned',{create:true});const w=await (await old.getFileHandle('old.bin',{create:true})).createWritable();await w.write('stale');await w.close();}")
            started=time.monotonic();start('large');large=wait(lambda j:j['phase'] in ('complete','failed'))
            assert large['phase']=='complete' and large['storageMode']=='disk',large
            assert len(large['items'])==17 and all(i['state']=='complete' for i in large['items']),large
            assert sum(i['bytes'] for i in large['items'])==(LARGE_MIB+64)*MIB
            dl=download(large);assert dl['state']=='complete'
            hashes={};video_path=root/'large.mp4'
            with ZipFile(dl['filename']) as z:
                assert len(z.namelist())==18 and z.testzip() is None
                md=z.read('下载报告.md').decode();assert '512' in md and '成功获取 17 个文件；请求失败 0 个文件' in md and 'BATCH_LIMIT' not in md
                (EVIDENCE/'large-download-report.md').write_text(md)
                for item in large['items']:
                    path='/'+item['url'].rsplit('/',1)[-1];h=hashlib.sha256()
                    with z.open(item['filename']) as src:
                        out=video_path.open('wb') if item['kind']=='video' else None
                        while data:=src.read(65536):
                            h.update(data)
                            if out:out.write(data)
                        if out:out.close()
                    assert h.hexdigest()==expected_digest(path);hashes[path]=h.hexdigest()
            subprocess.run(['ffmpeg','-v','error','-i',str(video_path),'-f','null','-'],check=True)
            stats=worker.evaluate('async()=>(await chrome.storage.local.get("diskTestStats")).diskTestStats');assert stats['forbiddenWholeBuffers']==0 and stats['maxExplicitUint8Array']<16*MIB,stats
            clean();large_seconds=time.monotonic()-started;print('PASS actual disk transfer:',sum(i['bytes'] for i in large['items']),stats,flush=True)
            # A broken body must never enter the ZIP, while the good item does.
            start('partial');partial=wait(lambda j:j['id']!=large['id'] and j['phase'] in ('complete','failed'));assert partial['phase']=='complete';assert sorted(i['state'] for i in partial['items'])==['complete','failed']
            with ZipFile(download(partial)['filename']) as z:assert z.testzip() is None and len(z.namelist())==2;assert '传输' in z.read('下载报告.md').decode()
            clean()
            before_recovery=REQUESTS['/image-ok.png'];response=control.evaluate("async()=>chrome.runtime.sendMessage({type:'retry-failed'})");assert response['ok'],response
            recovered=wait(lambda j:j['id']!=partial['id'] and j['phase']=='failed');assert len(recovered['items'])==1 and recovered['items'][0]['url'].endswith('/partial.png');assert REQUESTS['/image-ok.png']==before_recovery;assert recovered['recovery']['previouslySaved']==1;clean()
            # Simulate quota exhaustion at the disk adapter only. No empty or
            # corrupt archive is offered; pending addresses stay in the ledger.
            before=len(worker.evaluate('async()=>chrome.downloads.search({})'));start('quota',{'quota':True});quota=wait(lambda j:j['id']!=partial['id'] and j['phase']=='failed')
            assert quota['errorCode']=='DISK_QUOTA' and '磁盘' in quota['error'] and not quota.get('downloadId'),quota
            assert len(quota['items'])==12 and sum(REQUESTS[f'/quota-{i}.png'] for i in range(12))<=3
            assert len(worker.evaluate('async()=>chrome.downloads.search({})'))==before;clean()
            control.goto('chrome-extension://'+worker.url.split('/')[2]+'/report.html?task='+quota['id']);control.wait_for_function("()=>document.body.textContent.includes('磁盘空间不足')")
            # Explicit stop while media is being written: no extra ZIP, and the
            # same private directory is fully removed.
            start('stop');active=wait(lambda j:j['id']!=quota['id'] and j['phase']=='fetching' and any(i['state']=='active' for i in j['items']));page.wait_for_timeout(300)
            assert control.evaluate("async()=>chrome.runtime.sendMessage({type:'stop'})")['ok'];wait(lambda j:j['id']==active['id'] and j['phase']=='stopped');clean();assert len(worker.evaluate('async()=>chrome.downloads.search({})'))==before
            # Real process/profile restart leaves a crash spool; the next job
            # removes it before it writes new media.
            control.evaluate("async()=>{const root=await navigator.storage.getDirectory();const parent=await root.getDirectoryHandle('gdd-media-temporary',{create:true});const old=await parent.getDirectoryHandle('restart-leftover',{create:true});const w=await (await old.getFileHandle('old.bin',{create:true})).createWritable();await w.write('old');await w.close();}")
            ctx.close();ctx,worker,page,control,tab=launch();assert 'restart-leftover' in temp_dirs()
            start('partial');restart=wait(lambda j:j['id']!=active['id'] and j['phase']=='complete');clean()
            result={'version':manifest['version'],'verification':'Actual isolated Chromium MV3/offscreen/OPFS/single Chrome download; loopback original media. Quota error is injected only at the disk write adapter, not a physical full disk.','total_media_bytes':(LARGE_MIB+64)*MIB,'single_video_bytes':LARGE_MIB*MIB,'quick_run':bool(os.environ.get('DISK_QUICK_TEST')),'success_files':17,'zip_bytes':dl['fileSize'],'single_zip_with_one_md':True,'zip_crc_sha256_original_bytes':hashes,'large_video_full_decode':'passed','large_test_seconds':round(large_seconds,2),'allocation_guard':stats,'peak_parallel_requests':PEAK,'stale_cleanup_on_start':True,'complete_stop_failure_cleanup':True,'interrupted_body_excluded':True,'retry_failed_excludes_saved_success':True,'quota_fatal_no_invalid_zip':True,'quota_media_requests':sum(REQUESTS[f'/quota-{i}.png'] for i in range(12)),'actual_profile_restart_cleanup':True,'restart_files':sum(i['state']=='complete' for i in restart['items'])}
            (EVIDENCE/'disk-acceptance.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n');print('PASS disk cleanup / transfer interruption / simulated quota / explicit stop / real profile restart',flush=True);ctx.close()
    finally:server.shutdown()

if __name__=='__main__':main()
