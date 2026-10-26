"""Real MV3/offscreen flow in a disposable profile and loopback fixtures only."""
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
HEADERS=[]
VIDEO=b''

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        REQUESTS[self.path]+=1
        HEADERS.append({k:self.headers.get(k) for k in ['Cookie','Authorization']})
        data=media.PNG if self.path.endswith('.png') else VIDEO
        self.send_response(200);self.send_header('Content-Type','image/png' if self.path.endswith('.png') else 'video/mp4');self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
    def log_message(self,*args):pass

def fixture(origin):
    return '''<!doctype html><html><head><meta charset="utf-8"></head><body><div id="cards"></div><ul class="ant-pagination"><li class="ant-pagination-item-active" title="1">1</li><li class="ant-pagination-next"><button>下一页</button></li></ul><script>
const origin=ORIGIN;window.scenario=new URL(location).searchParams.get('scenario')||'hold';window.pageNumber=1;window.opens=0;
window.logout=()=>{document.body.innerHTML='<div role="dialog">其他设备登录，强制退出，请重新登录</div>';};
function detail(value){window.opens++;const modal=document.createElement('div');modal.role='dialog';modal.className='ant-modal-content';modal.style.cssText='position:fixed;inset:100px;background:white;z-index:999';modal.innerHTML='<button aria-label="Close">X</button><span>创意详情</span><a href="/modules/creative/display-ads/detail?id='+value.ad_key+'">单页打开</a>';
 modal.querySelector('button').onclick=()=>{modal.remove();if(window.scenario==='logout-first-close')window.logout();};document.body.append(modal);
 const add=()=>{const link=document.createElement('a');link.textContent='官方资源限制，请点击原始链接查看具体内容/下载素材';link.href=origin+'/'+value.ad_key+'.mp4';modal.append(link);};
 if(value.ad_key==='late' || window.scenario==='only-missing')window.releaseDetail=add;else setTimeout(add,200);
}
const records=window.scenario==='only-missing'?[{ad_key:'late',ads_type:2}]:window.scenario==='logout-next'?[{ad_key:'direct',ads_type:2,video_url:origin+'/direct.mp4'},{ad_key:'image',ads_type:1,preview_img_url:origin+'/image.png'}]:[{ad_key:'direct',ads_type:2,video_url:origin+'/direct.mp4'},{ad_key:'detail',ads_type:2},{ad_key:'late',ads_type:2},{ad_key:'image',ads_type:1,preview_img_url:origin+'/image.png'}];
for(const value of records){const card=document.createElement('div');card.className='shadow-common-light bg-white';card.style.cssText='height:100px;position:relative';card.textContent=value.ad_key;card.__reactFiber$t={memoizedProps:{value}};card.onclick=()=>detail(value);document.querySelector('#cards').append(card);}
document.querySelector('.ant-pagination-next button').onclick=()=>window.logout();
</script></body></html>'''.replace('ORIGIN',json.dumps(origin))

def main():
    global VIDEO
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);threading.Thread(target=server.serve_forever,daemon=True).start();origin=f'http://127.0.0.1:{server.server_port}'
    try:
        with tempfile.TemporaryDirectory() as temp,sync_playwright() as p:
            root=Path(temp);sample=root/'sample.mp4'
            subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','color=c=blue:s=64x64:d=0.3','-f','lavfi','-i','sine=frequency=440:duration=0.3','-c:v','libx264','-pix_fmt','yuv420p','-c:a','aac','-shortest',str(sample)],check=True)
            VIDEO=sample.read_bytes();extension=root/'extension';shutil.copytree(SOURCE,extension)
            manifest=json.loads((extension/'manifest.json').read_text());manifest['host_permissions']=['https://www.guangdada.net/*',origin+'/*'];(extension/'manifest.json').write_text(json.dumps(manifest))
            context=p.chromium.launch_persistent_context(str(root/'profile'),channel='chromium',headless=True,accept_downloads=True,args=[f'--disable-extensions-except={extension}',f'--load-extension={extension}'])
            worker=context.service_workers[0] if context.service_workers else context.wait_for_event('serviceworker');extid=worker.url.split('/')[2]
            context.route('https://www.guangdada.net/**',lambda route:route.fulfill(content_type='text/html',body=fixture(origin)))
            context.add_cookies([{'name':'GDD_LOGIN_FIXTURE','value':'MUST_NOT_REACH_CDN','domain':'www.guangdada.net','path':'/','secure':True}])
            page=context.new_page()
            def source(scenario):
                page.goto('https://www.guangdada.net/modules/creative/display-ads?scenario='+scenario)
            def popup_page():
                popup=context.new_page();page.bring_to_front();popup.goto(f'chrome-extension://{extid}/popup.html');popup.wait_for_function("()=>!document.querySelector('#start').disabled || !document.querySelector('#progress').hidden");return popup
            def job():return worker.evaluate('async()=>(await chrome.storage.local.get("job")).job')
            def wait_for(predicate):
                end=time.monotonic()+30;value=None
                while time.monotonic()<end:
                    value=job()
                    if value and predicate(value):return value
                    page.wait_for_timeout(100)
                raise AssertionError(value)
            def complete(old=None):return wait_for(lambda j:j['phase']=='complete' and j['id']!=old)
            def report(value,count):
                download=worker.evaluate('async id=>(await chrome.downloads.search({id}))[0]',value['downloadId'])
                with ZipFile(download['filename']) as z:
                    assert z.testzip() is None and len(z.namelist())==count+1,z.namelist()
                    for name in z.namelist():
                        if name.endswith('.mp4'):
                            assert z.read(name)==VIDEO
                            decoded=root/'decoded.mp4';decoded.write_bytes(z.read(name));subprocess.run(['ffmpeg','-v','error','-i',str(decoded),'-f','null','-'],check=True)
                        if name.endswith('.png'):assert z.read(name)==media.PNG
                    text=z.read('下载报告.md').decode();assert '地址采集与保存' in text and '未取得地址的已选素材' in text;return text
            def start_current():
                popup=popup_page();popup.locator('#mode').select_option('current');popup.locator('#start').click();popup.close()
            # All direct URLs are already saved while a later detail is pending.
            # The first resolved detail is saved before advancing to the next.
            source('hold');start_current();page.wait_for_function('window.opens===2')
            collecting=wait_for(lambda j:j['phase']=='collecting' and sum(bool(i.get('url')) for i in j['selections'])==3)
            assert collecting['selectedCardCount']==4 and len(collecting['items'])==3 and collecting['addressesSavedAt']
            assert sum(REQUESTS.values())==0,'Media requested before the complete address pass'
            page.evaluate('window.releaseDetail()');full=complete();assert not full['collectionIncomplete'];assert sum(bool(i.get('url')) for i in full['selections'])==4;report(full,4)
            # Logout during closing the first detail: its URL must already be
            # durable, and the rest of the original selection must still exist.
            source('logout-first-close');start_current();partial=complete(full['id'])
            assert partial['collectionIncomplete'] and partial['addressStage']=='interrupted';assert partial['selectedCardCount']==4 and len(partial['items'])==3
            assert partial['missing'][0]['adId']=='late' and partial['missing'][0]['reasonCode']=='ADDRESS_LOGIN_LOST'
            text=report(partial,3);assert '未取得地址的已选素材（1 项）' in text and '登录' in text and '未生成媒体 ZIP' not in text
            # Closing the original site and retrying uses only the saved ledger.
            urls=[i['url'] for i in partial['items']];page.close();page=context.new_page();page.goto('about:blank')
            popup=popup_page();expect(popup.locator('#retry')).to_be_enabled();popup.locator('#retry').click();popup.close();retried=complete(partial['id']);assert [i['url'] for i in retried['items']]==urls;report(retried,3)
            # Point-select all: choices and direct URLs survive a logout during
            # the very first detail lookup, before a download job even exists.
            source('logout-first-close');tab_id=worker.evaluate("async()=>(await chrome.tabs.query({url:'https://www.guangdada.net/*'})).find(t=>t.url.includes('logout-first-close')).id")
            before=sum(REQUESTS.values());popup=popup_page();popup.locator('#select').click();popup.close();page.get_by_role('button',name='全选本页',exact=True).click()
            end=time.monotonic()+15
            while time.monotonic()<end:
                cart=worker.evaluate('async id=>(await chrome.storage.local.get("selectionCarts")).selectionCarts?.[id]',tab_id)
                if cart and len(cart['entries'])==4 and sum(bool(i.get('url')) for i in cart['entries'])==3 and cart.get('addressWarning'):break
                page.wait_for_timeout(100)
            else:raise AssertionError(cart)
            assert sum(REQUESTS.values())==before
            popup=popup_page();expect(popup.locator('#mode')).to_have_value('basket');popup.locator('#start').click();popup.close();basket=complete(retried['id']);assert basket['selectedCardCount']==4;report(basket,3)
            # New pagination tasks pause first. An explicit finish downloads
            # the saved prefix without claiming unseen pages or EOF.
            source('logout-next');popup=popup_page();popup.locator('#mode').select_option('pages');popup.locator('#end').select_option('count');popup.locator('#pages').fill('3');popup.locator('#start').click();popup.close()
            paused=wait_for(lambda j:j['phase']=='collection-paused' and j['id']!=basket['id']);assert not paused.get('downloadId')
            assert worker.evaluate('async id=>chrome.runtime.sendMessage({type:"download-collected",id})',paused['id'])['ok'];pages=complete(basket['id'])
            assert pages['collectionIncomplete'] and pages['stopReason']=='collection-interrupted' and len(pages['pages'])==1 and pages['selectedCardCount']==2
            assert pages['collection']['targetPages']==3;assert '未访问页面没有卡片名单' in report(pages,2)
            # Explicit Stop has priority over the automatic partial download.
            source('hold');before=sum(REQUESTS.values());start_current();page.wait_for_function('window.opens===2');wait_for(lambda j:j['phase']=='collecting' and len(j['items'])==3)
            popup=popup_page();popup.locator('#stop').click();popup.close();stopped=wait_for(lambda j:j['phase']=='stopped' and j['id']!=pages['id']);assert stopped['selectedCardCount']==4 and len(stopped['items'])==3;assert not stopped.get('downloadId');assert sum(REQUESTS.values())==before
            # Close and reopen a real isolated browser profile during address
            # collection; the next worker reads the original Chrome storage.
            source('hold');start_current();page.wait_for_function('window.opens===2');saved=wait_for(lambda j:j['phase']=='collecting' and len(j['items'])==3)
            context.close()
            context=p.chromium.launch_persistent_context(str(root/'profile'),channel='chromium',headless=True,accept_downloads=True,args=[f'--disable-extensions-except={extension}',f'--load-extension={extension}'])
            worker=context.service_workers[0] if context.service_workers else context.wait_for_event('serviceworker');assert worker.url.split('/')[2]==extid
            context.route('https://www.guangdada.net/**',lambda route:route.fulfill(content_type='text/html',body=fixture(origin)))
            page=context.new_page();source('only-missing');page.evaluate('window.logout()')
            recovered=complete(stopped['id']);assert recovered['id']==saved['id'] and recovered['collectionIncomplete'];assert len(recovered['items'])==3;report(recovered,3)
            # No addresses + logout still leaves a complete selection report,
            # with no empty ZIP and no media requests.
            source('only-missing');before=sum(REQUESTS.values());start_current();page.wait_for_function('window.opens===1');page.evaluate('window.logout()')
            empty=wait_for(lambda j:j['id']!=recovered['id'] and j['phase']=='failed');assert empty['selectedCardCount']==1 and not empty['items'] and not empty.get('downloadId');assert empty['missing'][0]['reasonCode']=='ADDRESS_LOGIN_LOST';assert sum(REQUESTS.values())==before
            popup=popup_page();popup.locator('#report').click();viewer=context.pages[-1];viewer.wait_for_function("()=>document.body.textContent.includes('未取得地址的已选素材（1 项）')");assert '登录' in viewer.locator('body').inner_text();viewer.close();popup.close()
            assert all(not h['Cookie'] and not h['Authorization'] for h in HEADERS)
            evidence=SOURCE.parents[1]/'data/guangdada-metadata-acceptance';evidence.mkdir(exist_ok=True)
            result={'version':manifest['version'],'verification':'Isolated real Chromium extension/offscreen with local media; login loss is a visible page fixture, not the real account.','saved_before_media_requests':True,'detail_saved_before_close':True,'full_files':4,'logout_files':3,'offline_retry_files':3,'all_selected_choices_survive_logout':4,'cart_after_logout_files':3,'pagination_interrupted_files':2,'explicit_stop_no_download':True,'actual_browser_restart_recovered_files':3,'zero_address_report_without_zip':True,'cdn_requests_without_site_cookie':True,'zip_crc_original_bytes_full_decode':'passed'}
            (evidence/'addresses-acceptance.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n');context.close()
            print('PASS: durable address-first ledger; no media until address pass ends; logout/closed source tab/offline retry; all selected choices preserved; partial pagination ZIP; explicit stop; actual persistent-profile browser restart recovery; zero-address MD; CRC/media bytes/full decode.')
    finally:server.shutdown()

if __name__=='__main__':main()
