"""Reproduce the six-page interruption and reach a real fixture boundary at 19."""
import json
from pathlib import Path
import shutil
import tempfile
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from zipfile import ZipFile
from playwright.sync_api import sync_playwright
import browser_smoke as media

SOURCE=Path(__file__).resolve().parents[1]
EVIDENCE=SOURCE.parents[1]/'data/guangdada-metadata-acceptance'
REQUESTS=Counter()
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        REQUESTS[self.path]+=1;self.send_response(200);self.send_header('Content-Type','image/png');self.send_header('Content-Length',str(len(media.PNG)));self.end_headers();self.wfile.write(media.PNG)
    def log_message(self,*args):pass

def fixture(origin):
    return r'''<!doctype html><meta charset="utf-8"><body><button class="text-blue-600"><span>最新创意</span></button><div id="cards"></div><ul class="ant-pagination"><li class="ant-pagination-item-active" title="1">1</li><li class="ant-pagination-next"><button>下一页</button></li></ul><script>
const origin=ORIGIN;window.pageNumber=1;window.nextClicks=0;window.raceReads=0;window.armNextRace=false;
window.repaint=duration=>{const card=document.querySelector('#cards').lastElementChild;card.remove();setTimeout(()=>document.querySelector('#cards').append(card),duration);};
function render(){document.querySelector('#cards').replaceChildren();for(let i=1;i<=2;i++){const value={ad_key:'ad-'+pageNumber+'-'+i,ads_type:1,preview_img_url:origin+'/page-'+pageNumber+'-'+i+'.png'};const card=document.createElement('div');card.className='shadow-common-light bg-white';card.style.cssText='height:100px;position:relative';card.textContent=(pageNumber>=19?'2026-10-21':'2026-10-22')+'~2026-10-23';card.__reactFiber$t={memoizedProps:{value}};document.querySelector('#cards').append(card);}const current=document.querySelector('.ant-pagination-item-active');current.title=String(pageNumber);current.textContent=String(pageNumber);const next=document.querySelector('.ant-pagination-next');next.classList.toggle('ant-pagination-disabled',pageNumber===20);next.querySelector('button').disabled=pageNumber===20;}
document.querySelector('.ant-pagination-next button').onclick=()=>{nextClicks++;pageNumber++;const pager=document.querySelector('.ant-pagination-item-active');pager.title=String(pageNumber);pager.textContent=String(pageNumber);document.querySelector('#cards').setAttribute('aria-busy','true');setTimeout(()=>{render();document.querySelector('#cards').removeAttribute('aria-busy');},100);};
const nativeQuery=document.querySelectorAll.bind(document);document.querySelectorAll=function(selector){if(selector==='.ant-pagination' && armNextRace && ++raceReads===2){armNextRace=false;repaint(1000);}return nativeQuery(selector);};render();
</script>'''.replace('ORIGIN',json.dumps(origin))

def main():
    EVIDENCE.mkdir(exist_ok=True)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);threading.Thread(target=server.serve_forever,daemon=True).start();origin=f'http://127.0.0.1:{server.server_port}'
    try:
        with tempfile.TemporaryDirectory() as temp,sync_playwright() as p:
            root=Path(temp)
            def session(name,legacy=False):
                extension=root/name
                if legacy:
                    with ZipFile(SOURCE.parents[1]/'data/guangdada-media-extension-1.11.0.zip') as z:
                        assert all('/' not in n and '..' not in n for n in z.namelist());z.extractall(extension)
                else:shutil.copytree(SOURCE,extension)
                m=json.loads((extension/'manifest.json').read_text());m['host_permissions']=['https://www.guangdada.net/*',origin+'/*'];(extension/'manifest.json').write_text(json.dumps(m))
                context=p.chromium.launch_persistent_context(str(root/(name+'-profile')),channel='chromium',headless=True,accept_downloads=True,args=[f'--disable-extensions-except={extension}',f'--load-extension={extension}'])
                worker=context.service_workers[0] if context.service_workers else context.wait_for_event('serviceworker');extid=worker.url.split('/')[2]
                context.route('https://www.guangdada.net/**',lambda r:r.fulfill(content_type='text/html',body=fixture(origin)));page=context.new_page();page.goto('https://www.guangdada.net/modules/creative/display-ads');tab_id=worker.evaluate("async()=>(await chrome.tabs.query({url:'https://www.guangdada.net/*'}))[0].id");control=context.new_page();control.goto(f'chrome-extension://{extid}/report.html')
                return context,worker,page,control,tab_id
            def state(worker):return worker.evaluate('async()=>(await chrome.storage.local.get("job")).job')
            def wait(worker,fn,timeout=150):
                end=time.monotonic()+timeout
                while time.monotonic()<end:
                    j=state(worker)
                    if j and fn(j):return j
                    time.sleep(.07)
                raise AssertionError(j)
            def start(control,tab,count=20,until=True):
                return control.evaluate('''async p=>chrome.runtime.sendMessage({type:'start',tabId:p.tab,mode:'pages',pageCount:p.count,untilEnd:p.until,firstDiscoveredSince:'2026-10-22',kinds:['image'],autoResolveExternal:false})''',{'tab':tab,'count':count,'until':until})
            def report(worker,job,count):
                dl=worker.evaluate('async id=>(await chrome.downloads.search({id}))[0]',job['downloadId'])
                with ZipFile(dl['filename']) as z:
                    assert z.testzip() is None and len(z.namelist())==count+1
                    for n in z.namelist():
                        if n.endswith('.png'):assert z.read(n)==media.PNG
                    return z.read('下载报告.md').decode()
            # The shipped old ZIP produces the exact generic error after six
            # saved pages when the site momentarily detaches one card.
            context,worker,page,control,tab=session('legacy',True);assert start(control,tab)['ok'];wait(worker,lambda j:len(j['pages'])==6 and j['phase']=='collecting');page.evaluate('repaint(1100)');legacy=wait(worker,lambda j:j['phase']=='complete');assert legacy['collectionIncomplete'] and len(legacy['pages'])==6 and '页面素材或页码已变化' in legacy['collectionError'];assert not legacy.get('dateCutoff');assert len(legacy['items'])==12;report(worker,legacy,12);context.close();print('legacy reproduced: six pages, generic identity interruption, no date cutoff',flush=True)
            # Same repaint now waits and restores, then the date boundary is
            # reached only at page 19. No page-19 media request is made.
            context,worker,page,control,tab=session('fixed');assert start(control,tab)['ok'];wait(worker,lambda j:len(j['pages'])==6 and j['phase']=='collecting');page.evaluate('repaint(1100)');recovering=wait(worker,lambda j:j.get('collection',{}).get('identityWait'));assert recovering['phase']=='collecting';assert not recovering.get('downloadId');fixed=wait(worker,lambda j:j['phase']=='complete');assert not fixed['collectionIncomplete'] and fixed['stopReason']=='date-cutoff';assert fixed['dateCutoff']['pageNumber']==19 and fixed['dateCutoff']['firstDiscovered']=='2026-10-21';assert len(fixed['items'])==36 and page.evaluate('nextClicks')==18;assert fixed['pageRecoveryCount']>=1 and fixed['pageDiagnostic']['recovered'];md=report(worker,fixed,36);assert '采集结束原因：达到首次发现日期截止' in md and '第 19 页 · 第 1 个' in md and '翻页稳定性检查' in md;assert not REQUESTS['/page-19-1.png'] and not REQUESTS['/page-19-2.png'];(EVIDENCE/'nineteen-page-report.md').write_text(md);print('fixed verified: six-page repaint recovered, only page 19 stops, 36 files one ZIP',flush=True)
            # Permanent mismatch never gets re-labelled as date/EOF. Accelerate
            # only the test-owned wait clock; production has the full 15s bound.
            old=fixed['id'];page.reload();assert start(control,tab)['ok'];wait(worker,lambda j:j['id']!=old and len(j['pages'])==1 and j['phase']=='collecting');page.evaluate("document.querySelector('#cards').lastElementChild.__reactFiber$t.memoizedProps.value.ad_key='changed-ad'");wait(worker,lambda j:j.get('collection',{}).get('identityWait'));worker.evaluate('''()=>{globalThis.testRealNow=Date.now;Date.now=()=>testRealNow()+16000;}''');paused=wait(worker,lambda j:j['id']!=old and j['phase']=='collection-paused');worker.evaluate('Date.now=globalThis.testRealNow');assert not paused.get('downloadId');assert control.evaluate('async id=>chrome.runtime.sendMessage({type:"download-collected",id})',paused['id'])['ok'];permanent=wait(worker,lambda j:j['id']!=old and j['phase']=='complete');assert permanent['collectionIncomplete'] and not permanent.get('dateCutoff') and page.evaluate('nextClicks')==0;assert '15秒' in permanent['collectionError'];md=report(worker,permanent,2);assert 'actual' not in md and 'changed-ad' in md and '采集结束原因：提前中断' in md;(EVIDENCE/'permanent-mismatch-report.md').write_text(md);print('permanent mismatch: exact diagnostic, no click, saved URLs retained',flush=True)
            # Repaint between preflight and persisted click intent must put the
            # state back at next, then issue exactly one real click after repair.
            old=permanent['id'];page.reload();assert start(control,tab,count=2,until=False)['ok'];wait(worker,lambda j:j['id']!=old and j['collection']['stage']=='next');page.evaluate('armNextRace=true;raceReads=0');raced=wait(worker,lambda j:j['id']!=old and j['phase']=='complete');assert raced['stopReason']=='page-limit' and not raced['collectionIncomplete'];assert page.evaluate('nextClicks')==1 and len(raced['items'])==4 and raced['pageRecoveryCount']>=1;report(worker,raced,4);context.close()
            result={'version':json.loads((SOURCE/'manifest.json').read_text())['version'],'verification':'Isolated Chromium with actual shipped 1.11.0 ZIP and fixed extension, loopback media and visible normal DOM repaint fixtures. Not a real user 19-page collection.','legacy_exact_six_page_error_reproduced':True,'same_six_page_repaint_recovered':True,'date_cutoff_page':19,'downloaded_files':36,'no_media_requests_at_date_boundary':True,'permanent_change_stops_with_exact_diagnostic':True,'preflight_click_race_recovers_with_single_click':True,'zip_crc_original_bytes':'passed'};(EVIDENCE/'stability-acceptance.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n');print(json.dumps(result,ensure_ascii=False),flush=True)
    finally:server.shutdown()
if __name__=='__main__':main()
