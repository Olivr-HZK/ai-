"""Reproduce 1.14.0 early stops, then verify delayed close/read recovery."""
import json
import shutil
import subprocess
import tempfile
import threading
import time
from collections import Counter
from http.server import ThreadingHTTPServer
from pathlib import Path
from zipfile import ZipFile
from playwright.sync_api import sync_playwright
import browser_smoke as media

SOURCE=Path(__file__).resolve().parents[1]
EVIDENCE=SOURCE.parents[1]/'data/guangdada-detail-close-acceptance'

def fixture(origin):
    return '''<!doctype html><html><meta charset="utf-8"><body><span class="text-blue-600" id="newest">最新创意</span><div id="cards"></div><ul class="ant-pagination"><li class="ant-pagination-item-active" title="1">1</li><li class="ant-pagination-next"><button>下一页</button></li></ul><script>
const origin=ORIGIN,query=new URL(location).searchParams;window.currentPage=Number(query.get('start')||1);window.scenario=query.get('scenario');window.nextClicks=0;window.opens=0;window.closes=0;
function show(value){window.opens++;const modal=document.createElement('div');modal.className='ant-modal-content';modal.role='dialog';modal.style.cssText='position:fixed;inset:100px;background:white';modal.innerHTML='<button aria-label="Close">X</button><a href="/modules/creative/display-ads/detail?id='+value.ad_key+'">单页打开</a><span>2026-10-24 ~ 2026-10-26</span><a href="'+origin+'/p'+currentPage+'.mp4">官方资源限制，请点击原始链接查看具体内容/下载素材</a>';document.body.append(modal);
 modal.querySelector('button').onclick=()=>{window.closes++;if(scenario==='permanent')return;if(scenario==='foreign'){modal.querySelector('a').href='/modules/creative/display-ads/detail?id=foreign-ad';return;}if(scenario==='hidden'){modal.style.visibility='hidden';return;}setTimeout(()=>modal.remove(),scenario==='both'||scenario==='slow'?3500:0);};
}
function render(){const date=currentPage>20?'2026-10-23':'2026-10-24';const missing=currentPage===4||['permanent','hidden','foreign','dates'].includes(scenario);const values=[{ad_key:'video-'+currentPage,ads_type:2,...(missing?{}:{video_url:origin+'/p'+currentPage+'.mp4'})},{ad_key:'image-'+currentPage,ads_type:1,preview_img_url:origin+'/p'+currentPage+'.png'}];cards.replaceChildren();for(const [i,value]of values.entries()){const card=document.createElement('div');card.className='shadow-common-light bg-white';card.style.cssText='height:100px;position:relative';card.textContent=(scenario==='dates'&&i===0?'日期尚未显示':date+' ~ 2026-10-26');card.__reactFiber$t={memoizedProps:{value}};card.onclick=()=>show(value);cards.append(card);}const active=document.querySelector('.ant-pagination-item-active');active.title=currentPage;active.textContent=currentPage;const next=document.querySelector('.ant-pagination-next');next.classList.toggle('ant-pagination-disabled',currentPage===21);next.querySelector('button').disabled=currentPage===21;}
document.querySelector('.ant-pagination-next button').onclick=()=>{nextClicks++;currentPage++;render();};render();
</script></body></html>'''.replace('ORIGIN',json.dumps(origin))

def main():
    EVIDENCE.mkdir(exist_ok=True);server=ThreadingHTTPServer(('127.0.0.1',0),media.Handler);threading.Thread(target=server.serve_forever,daemon=True).start();origin=f'http://127.0.0.1:{server.server_port}'
    try:
        with tempfile.TemporaryDirectory() as temp,sync_playwright() as p:
            root=Path(temp);sample=root/'sample.mp4';subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','color=c=blue:s=64x64:d=0.3','-c:v','libx264','-pix_fmt','yuv420p',str(sample)],check=True);media.VIDEO=sample.read_bytes()
            def session(name,scenario,start=1,legacy=False,inject=False):
                ext=root/name
                if legacy:
                    ext.mkdir()
                    with ZipFile(SOURCE.parents[1]/'data/guangdada-media-extension-1.14.0.zip') as z:z.extractall(ext)
                else:shutil.copytree(SOURCE,ext)
                manifest=json.loads((ext/'manifest.json').read_text());manifest['host_permissions']=['https://www.guangdada.net/*',origin+'/*'];(ext/'manifest.json').write_text(json.dumps(manifest))
                ctx=p.chromium.launch_persistent_context(str(root/(name+'-profile')),channel='chromium',headless=True,accept_downloads=True,args=[f'--disable-extensions-except={ext}',f'--load-extension={ext}'])
                worker=ctx.service_workers[0] if ctx.service_workers else ctx.wait_for_event('serviceworker');extid=worker.url.split('/')[2];ctx.route('https://www.guangdada.net/**',lambda route:route.fulfill(content_type='text/html',body=fixture(origin)))
                page=ctx.new_page();page.goto(f'https://www.guangdada.net/modules/creative/display-ads?scenario={scenario}&start={start}');control=ctx.new_page();control.goto(f'chrome-extension://{extid}/report.html');tab=worker.evaluate("async()=>(await chrome.tabs.query({url:'https://www.guangdada.net/*'}))[0].id")
                if inject:
                    worker.evaluate('''()=>{const native=chrome.scripting.executeScript.bind(chrome.scripting);let count=0;globalThis.readDrops=0;chrome.scripting.executeScript=async options=>{if(options.func?.name==='collectMedia'){const [meta]=await native({target:options.target,world:'MAIN',func:()=>window.currentPage});if(meta.result===5&&++count===2){readDrops++;return [{result:undefined}];}}return native(options);};}''')
                if scenario=='dates':
                    worker.evaluate('''()=>{const native=chrome.scripting.executeScript.bind(chrome.scripting);let jumped=false;globalThis.realNow=Date.now;globalThis.dateOffset=0;Date.now=()=>realNow()+dateOffset;chrome.scripting.executeScript=async options=>{const result=await native(options);if(!jumped&&options.func?.name==='detailControl'&&options.args?.[0]==='close'){jumped=true;dateOffset=65000;}return result;};}''')
                response=control.evaluate('async m=>chrome.runtime.sendMessage(m)',{'type':'start','tabId':tab,'kinds':['video','image'],'mode':'pages','untilEnd':True,'firstDiscoveredSince':'2026-10-24','autoResolveExternal':scenario!='dates'})
                assert response['ok'],response
                return ctx,worker,page,control
            def wait(worker,predicate,timeout=140):
                end=time.monotonic()+timeout;value=None
                while time.monotonic()<end:
                    value=worker.evaluate('async()=>(await chrome.storage.local.get("job")).job')
                    if value and predicate(value):return value
                    time.sleep(.12)
                raise AssertionError({'phase':value and value.get('phase'),'error':value and value.get('collectionError'),'pages':value and len(value.get('pages',[]))})
            def report(worker,job,count):
                dl=worker.evaluate('async id=>(await chrome.downloads.search({id}))[0]',job['downloadId'])
                with ZipFile(dl['filename']) as z:
                    assert z.testzip() is None and len(z.namelist())==count+1
                    for n in z.namelist():
                        if n.endswith('.mp4'):
                            assert z.read(n)==media.VIDEO;decoded=root/'decode.mp4';decoded.write_bytes(z.read(n));subprocess.run(['ffmpeg','-v','error','-i',str(decoded),'-f','null','-'],check=True)
                        elif n.endswith('.png'):assert z.read(n)==media.PNG
                    return z.read('下载报告.md').decode()
            # Same normal click, 3.5-second close; original version aborts.
            ctx,w,page,control=session('legacy-close','slow',4,legacy=True);old=wait(w,lambda j:j['phase']=='complete');assert old['collectionIncomplete'] and '详情未能确认关闭' in old['collectionError'];assert page.evaluate('nextClicks')==0;assert len(old['selections'])==2;report(w,old,2);ctx.close();print('PASS reproduced 1.14.0: slow detail close stops page 4 with saved addresses',flush=True)
            ctx,w,page,control=session('legacy-read','read',5,legacy=True,inject=True);old=wait(w,lambda j:j['phase']=='complete');assert old['collectionIncomplete'] and '无法读取页面' in old['collectionError'];assert page.evaluate('nextClicks')==0;report(w,old,2);ctx.close();print('PASS reproduced 1.14.0: one missing observation stops page 5',flush=True)
            # Both failures in one until-end/date run; retain all 20 target
            # pages and stop only at page 21's actual older boundary.
            ctx,w,page,control=session('fixed','both',inject=True);fixed=wait(w,lambda j:j['phase']=='complete');assert not fixed['collectionIncomplete'] and fixed['stopReason']=='date-cutoff';assert fixed['dateCutoff']['pageNumber']==21;assert [x['pageNumber'] for x in fixed['pages']]==list(range(1,22));assert len(fixed['items'])==40 and w.evaluate('readDrops')==1 and fixed['detailCloseRecoveries']>=1;assert page.evaluate('closes')==1 and page.evaluate('nextClicks')==20;md=report(w,fixed,40);(EVIDENCE/'twenty-page-report.md').write_text(md);ctx.close();print('PASS fixed: 20 target pages, 40 original media, date boundary 21; one close click/read retry',flush=True)
            # A retained but CSS-hidden dialog must not block collection.
            ctx,w,page,control=session('hidden','hidden',20);hidden=wait(w,lambda j:j['phase']=='complete');assert not hidden['collectionIncomplete'] and hidden['dateCutoff']['pageNumber']==21;assert page.locator('[role=dialog]').count()==1 and not page.locator('[role=dialog]').is_visible();report(w,hidden,2);ctx.close();print('PASS hidden retained dialog does not block close confirmation',flush=True)
            for scenario,code in [('foreign','DETAIL_ID_MISMATCH'),('permanent','DETAIL_CLOSE_TIMEOUT')]:
                ctx,w,page,control=session(scenario,scenario,4);paused=wait(w,lambda j:j['phase']=='collection-paused');assert not paused.get('downloadId');assert control.evaluate('async id=>chrome.runtime.sendMessage({type:"download-collected",id})',paused['id'])['ok'];bad=wait(w,lambda j:j['phase']=='complete');assert bad['collectionIncomplete'] and bad['collectionDiagnostic']['code']==code;assert page.evaluate('nextClicks')==0 and page.evaluate('closes')==1;assert page.locator('[role=dialog]').is_visible();md=report(w,bad,2);assert code in md;(EVIDENCE/(scenario+'-report.md')).write_text(md);ctx.close();print('PASS protected pause then explicit prefix download: '+code,flush=True)
            # Controlled worker clock during a genuine date-detail lookup;
            # useful lookup time must not consume the 60s media warm-up budget.
            ctx,w,page,control=session('date-budget','dates',20);budget=wait(w,lambda j:j['phase']=='complete');assert not budget['collectionIncomplete'] and budget['dateCutoff']['pageNumber']==21;assert budget['collection']['lookupElapsedMs']>=65000;report(w,budget,1);ctx.close();print('PASS date lookup excluded from page warm-up budget (isolated clock advance)',flush=True)
            result={'version':json.loads((SOURCE/'manifest.json').read_text())['version'],'verification':'Retained 1.14.0 ZIP vs new source in isolated real Chromium; slow close and injection-return loss are fixtures, not claimed real-site timing diagnosis.','legacy_close_failure_reproduced':True,'legacy_missing_read_reproduced':True,'twenty_target_pages_and_actual_date_boundary_21':True,'single_close_click':True,'bounded_read_only_retry':True,'hidden_dialog_ignored':True,'foreign_identity_preserved':True,'permanent_close_precise_diagnostic':True,'lookup_time_excluded_from_warm_budget':True,'zip_crc_original_bytes_full_video_decode':'passed'};(EVIDENCE/'readiness-acceptance.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    finally:server.shutdown()

if __name__=='__main__':main()
