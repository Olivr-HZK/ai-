"""Date cutoff through actual MV3 popup, worker, normal detail UI and ZIP."""
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from zipfile import ZipFile
from playwright.sync_api import sync_playwright, expect
import browser_smoke as media

SOURCE=Path(__file__).resolve().parents[1]
REQUESTS=Counter()
VIDEO=b''

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        REQUESTS[self.path]+=1
        body=VIDEO if self.path.endswith('.mp4') else media.PNG
        self.send_response(200);self.send_header('Content-Type','video/mp4' if self.path.endswith('.mp4') else 'image/png');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    def log_message(self,*args):pass

def fixture(origin):
    return r'''<!doctype html><html><meta charset="utf-8"><body><button id="newest" class="text-blue-600"><span>最新创意</span></button><div id="cards"></div><ul class="ant-pagination"><li class="ant-pagination-item-active" title="1">1</li><li class="ant-pagination-next"><button>下一页</button></li></ul><script>
const origin=ORIGIN;window.currentPage=1;window.nextClicks=0;window.opens=[];window.scenario='normal';
function records(n){return n===1?[{id:'p1a',date:'2026-10-23',kind:2},{id:'p1b',date:'2026-10-22',kind:1}]:n===2?[{id:'p2a',date:'2026-10-22',kind:1},{id:'p2old',date:'2026-10-21',kind:1},{id:'p2later',date:'2026-10-23',kind:1}]:[{id:'p3a',date:'2026-10-20',kind:1}];}
function showDetail(v){opens.push(v.ad_key);const modal=document.createElement('div');modal.role='dialog';modal.className='ant-modal-content';modal.style.cssText='position:fixed;inset:100px;background:white;z-index:99';modal.innerHTML='<button aria-label="Close">X</button><a href="/modules/creative/display-ads/detail?id='+((scenario==='wrong')?'another-ad':v.ad_key)+'">单页打开</a><p>首次发现 (UTC+8) 2020-01-01</p>';
 modal.querySelector('button').onclick=()=>{modal.remove();if(scenario==='logout')document.body.insertAdjacentHTML('beforeend','<div role="dialog">其他设备登录，强制退出，请重新登录</div>');};document.body.append(modal);
 if(scenario==='hold')return;
 setTimeout(()=>modal.insertAdjacentHTML('beforeend','<p>'+(scenario==='unknown'?'2026-02-30':v.actualDate)+'~2026-10-23</p>'),180);
}
function render(){document.querySelector('#cards').replaceChildren();for(const r of records(currentPage)){
 const v={ad_key:r.id,ads_type:r.kind,actualDate:r.date,...(r.kind===2?{video_url:origin+'/'+r.id+'.mp4'}:{preview_img_url:origin+'/'+r.id+'.png'})};
 const card=document.createElement('div');card.className='shadow-common-light bg-white';card.style.cssText='height:120px;position:relative';card.textContent=r.id+'\n首次发现 (UTC+8) 2020-01-01\n';
 if(!(r.id==='p1b' && ['detail','unknown','wrong','hold','logout'].includes(scenario)))card.textContent+=r.date+'~2026-10-23';
 card.__reactFiber$t={memoizedProps:{value:v}};card.onclick=()=>showDetail(v);document.querySelector('#cards').append(card);
 }const current=document.querySelector('.ant-pagination-item-active');current.title=String(currentPage);current.textContent=String(currentPage);const next=document.querySelector('.ant-pagination-next');next.classList.toggle('ant-pagination-disabled',currentPage===3);next.querySelector('button').disabled=currentPage===3;}
document.querySelector('.ant-pagination-next button').onclick=()=>{nextClicks++;currentPage++;if(scenario==='sort-change')document.querySelector('#newest').classList.remove('text-blue-600');document.querySelector('#cards').setAttribute('aria-busy','true');setTimeout(()=>{render();document.querySelector('#cards').removeAttribute('aria-busy');},200);};render();
</script></body></html>'''.replace('ORIGIN',json.dumps(origin))

def main():
    global VIDEO
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);threading.Thread(target=server.serve_forever,daemon=True).start();origin=f'http://127.0.0.1:{server.server_port}'
    try:
        with tempfile.TemporaryDirectory() as temp,sync_playwright() as p:
            root=Path(temp);sample=root/'sample.mp4';subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','color=c=blue:s=64x64:d=0.3','-c:v','libx264','-pix_fmt','yuv420p',str(sample)],check=True);VIDEO=sample.read_bytes()
            extension=root/'extension';shutil.copytree(SOURCE,extension);manifest=json.loads((extension/'manifest.json').read_text());manifest['host_permissions']=['https://www.guangdada.net/*',origin+'/*'];(extension/'manifest.json').write_text(json.dumps(manifest))
            context=p.chromium.launch_persistent_context(str(root/'profile'),channel='chromium',headless=True,accept_downloads=True,args=[f'--disable-extensions-except={extension}',f'--load-extension={extension}'])
            worker=context.service_workers[0] if context.service_workers else context.wait_for_event('serviceworker');extid=worker.url.split('/')[2]
            context.route('https://www.guangdada.net/**',lambda r:r.fulfill(content_type='text/html',body=fixture(origin)))
            page=context.new_page();page.goto('https://www.guangdada.net/modules/creative/display-ads');tab_id=worker.evaluate("async()=>(await chrome.tabs.query({url:'https://www.guangdada.net/*'}))[0].id")
            control=context.new_page();control.goto(f'chrome-extension://{extid}/report.html')
            evidence=SOURCE.parents[1]/'data/guangdada-metadata-acceptance';evidence.mkdir(exist_ok=True)
            def state():return worker.evaluate('async()=>(await chrome.storage.local.get("job")).job')
            def wait_job(old=None,phase='complete'):
                end=time.monotonic()+35
                while time.monotonic()<end:
                    value=state()
                    if value and value['id']!=old and value['phase']==phase:return value
                    page.wait_for_timeout(100)
                raise AssertionError(value)
            def finish_partial(old):
                paused=wait_job(old,'collection-paused');assert not paused.get('downloadId')
                answer=control.evaluate('async id=>chrome.runtime.sendMessage({type:"download-collected",id})',paused['id']);assert answer['ok'],answer
                return wait_job(old)
            def popup_page():
                popup=context.new_page();popup.set_viewport_size({'width':380,'height':1050});page.bring_to_front();popup.goto(f'chrome-extension://{extid}/popup.html');expect(popup.locator('#start')).to_be_enabled();return popup
            def source(scenario='normal'):
                page.reload();page.evaluate('(s)=>{scenario=s;render();}',scenario)
            def start(since='2026-10-22',count=1000,until=True):
                return control.evaluate('''async p=>chrome.runtime.sendMessage({type:'start',tabId:p.id,mode:'pages',pageCount:p.count,untilEnd:p.until,firstDiscoveredSince:p.since,kinds:['image','video'],autoResolveExternal:false})''',{'id':tab_id,'count':count,'until':until,'since':since})
            def zip_report(job,count):
                dl=worker.evaluate('async id=>(await chrome.downloads.search({id}))[0]',job['downloadId'])
                with ZipFile(dl['filename']) as z:
                    assert z.testzip() is None and len(z.namelist())==count+1
                    for name in z.namelist():
                        if name.endswith('.mp4'):
                            assert z.read(name)==VIDEO;target=root/'decoded.mp4';target.write_bytes(z.read(name));subprocess.run(['ffmpeg','-v','error','-i',str(target),'-f','null','-'],check=True)
                        elif name.endswith('.png'):assert z.read(name)==media.PNG
                    return z.read('下载报告.md').decode()
            # Actual popup validation/settings, close popup while collecting.
            popup=popup_page();popup.locator('#mode').select_option('pages');popup.locator('#end').select_option('last');popup.locator('#date-enabled').check();popup.locator('#start').click();expect(popup.locator('#message')).to_contain_text('请选择最早首次发现日期');assert state() is None
            popup.locator('#date-since').fill('2026-10-22');popup.locator('#date-since').blur();popup.close();popup=popup_page();popup.locator('#mode').select_option('pages');popup.locator('#end').select_option('last');expect(popup.locator('#date-enabled')).to_be_checked();expect(popup.locator('#date-since')).to_have_value('2026-10-22');popup.screenshot(path=str(evidence/'date-options.png'),full_page=True);popup.locator('#start').click();popup.close()
            normal=wait_job();assert normal['stopReason']=='date-cutoff' and not normal['collectionIncomplete'];assert len(normal['items'])==3 and normal['selectedCardCount']==5;assert normal['dateCutoff']['pageNumber']==2 and normal['dateCutoff']['cardIndex']==2;assert page.evaluate('nextClicks')==1;assert not page.evaluate('opens');md=zip_report(normal,3);assert '日期未纳入 2 项' in md and '首次发现日期截止' in md and '2026-10-22（含当天）' in md;assert not REQUESTS['/p2old.png'] and not REQUESTS['/p2later.png'] and not REQUESTS['/p3a.png']
            (evidence/'date-cutoff-sample-report.md').write_text(md)
            # Retry only approved saved addresses; no further site reads/clicks.
            old=normal['id'];control.evaluate("async()=>chrome.runtime.sendMessage({type:'retry'})");retry=wait_job(old);zip_report(retry,3);assert page.evaluate('nextClicks')==1
            # Page limit can win; newest fields from an independent old label never stop it.
            source();assert start(count=1,until=False)['ok'];limited=wait_job(retry['id']);assert limited['stopReason']=='page-limit' and len(limited['items'])==2 and not limited.get('dateCutoff');zip_report(limited,2)
            source();assert start('2026-10-19')['ok'];eof=wait_job(limited['id']);assert eof['stopReason']=='last-page' and len(eof['items'])==6;zip_report(eof,6)
            # First card already too old: report only, no media request or empty ZIP.
            source();before=sum(REQUESTS.values());assert start('2026-10-24')['ok'];empty=wait_job(eof['id'],'failed');assert empty['stopReason']=='date-cutoff' and not empty['items'] and not empty.get('downloadId');assert sum(REQUESTS.values())==before
            # Missing list date: own detail visible range, not old independent first-discovery label.
            source('detail');assert start(count=1,until=False)['ok'];detail=wait_job(empty['id']);assert len(detail['items'])==2 and page.evaluate('opens')==['p1b'];assert detail['selections'][1]['dateSource']=='visible_detail_date_range';zip_report(detail,2)
            # Unreadable and wrong-identity dates stop collection, preserve approved prefix only.
            for scenario in ['unknown','wrong']:
                source(scenario);before=REQUESTS['/p1b.png'];assert start()['ok'];partial=finish_partial(detail['id']);assert partial['collectionIncomplete'] and len(partial['items'])==1 and not partial.get('dateCutoff');assert page.evaluate('nextClicks')==0;md=zip_report(partial,1);assert '日期未核验 1 项' in md and '未确认达到日期截止' in md;assert REQUESTS['/p1b.png']==before
                if scenario=='wrong':assert page.locator('[role=dialog]').count()==1
                detail=partial
            # A forced logout during close cannot erase the date approved just before it.
            source('logout');assert start()['ok'];logout=finish_partial(detail['id']);assert logout['collectionIncomplete'] and len(logout['items'])==2 and '登录失效' in logout['collectionError'];zip_report(logout,2)
            # Wrong sort is rejected before replacing the current saved job.
            source();page.locator('#newest').evaluate('(e)=>e.classList.remove("text-blue-600")');answer=start();assert not answer['ok'] and '最新创意' in answer['error'];assert state()['id']==logout['id'];answer=start('2026-02-30');assert not answer['ok'] and state()['id']==logout['id']
            # Sort changes on a later page: stop with known approved first-page URLs.
            source('sort-change');assert start()['ok'];changed=finish_partial(logout['id']);assert changed['collectionIncomplete'] and len(changed['items'])==2 and len(changed['pages'])==1;assert '排序' in changed['collectionError'];zip_report(changed,2)
            # Explicit stop while checking a date doesn't launch the saved prefix.
            source('hold');before=sum(REQUESTS.values());assert start()['ok'];page.wait_for_function('opens.length===1');control.evaluate("async()=>chrome.runtime.sendMessage({type:'stop'})");stopped=wait_job(changed['id'],'stopped');assert not stopped.get('downloadId') and sum(REQUESTS.values())==before
            result={'version':manifest['version'],'verification':'Isolated real Chromium popup/worker/detail UI with loopback original media; no real user account logout or website batch download.','inclusive_same_day':True,'cross_page_cutoff':{'page':2,'card':2,'downloaded_files':3,'no_requests_for_boundary_or_following':True},'saved_address_retry':True,'page_limit_and_eof':True,'zero_eligible_report_only':True,'missing_card_date_detail_fallback':True,'independent_old_first_discovery_label_ignored':True,'invalid_missing_or_wrong_identity_date_stops':True,'logout_after_date_checkpoint_retains_approved_urls':True,'sort_validation_and_change_stop':True,'explicit_stop_no_download':True,'settings_persisted':True,'zip_crc_original_bytes_full_video_decode':'passed'}
            (evidence/'dates-acceptance.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n');print(json.dumps(result,ensure_ascii=False),flush=True);context.close()
    finally:server.shutdown()

if __name__=='__main__':main()
