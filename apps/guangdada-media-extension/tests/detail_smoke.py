"""MV3 regression: a YouTube link exists only after opening its own detail."""
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import time
from http.server import ThreadingHTTPServer
from zipfile import ZipFile
from playwright.sync_api import sync_playwright, expect
import external_smoke as external
import browser_smoke as media

SOURCE=Path(__file__).resolve().parents[1]

def fixture(origin):
    return '''<!doctype html><html><head><meta charset="utf-8"></head><body><div id="cards"></div><ul class="ant-pagination"><li class="ant-pagination-item-active" title="1">1</li><li class="ant-pagination-next"><button>下一页</button></li></ul><script>
const origin=ORIGIN;window.pageNumber=1;window.opens=0;window.closes=0;window.scenario='normal';
function showDetail(value){window.opens++;const modal=document.createElement('div');modal.role='dialog';modal.className='ant-modal-content';modal.style.cssText='position:fixed;inset:100px;background:white;z-index:999';modal.innerHTML='<button aria-label="Close">X</button><span>创意详情</span>';
 modal.querySelector('button').onclick=()=>{window.closes++;modal.remove();};document.body.append(modal);
 setTimeout(()=>{const id=window.scenario==='wrong'?'another-ad':value.ad_key;const a=document.createElement('a');a.href='/modules/creative/display-ads/detail?id='+id;a.textContent='单页打开';modal.append(a);
 if(window.scenario==='slow')return;const link=document.createElement('a');link.textContent='官方资源限制，请点击原始链接查看具体内容/下载素材';link.href=window.pageNumber===1?'https://www.youtube.com/watch?v=aBcD1234_-Z':origin+'/watch';modal.append(link);
 const install=document.createElement('a');install.href='https://play.google.com/store/apps/details?id=NOT_THE_MEDIA';install.textContent='Install';modal.append(install);},250);
}
function render(){const values=['wrong','slow'].includes(window.scenario)?[{ad_key:'expected-ad',ads_type:2}]:window.pageNumber===1?[{ad_key:'detail-only',ads_type:2},{ad_key:'image',ads_type:1,preview_img_url:origin+'/photo.png'},{ad_key:'direct',ads_type:2,video_url:origin+'/direct.mp4'}]:[{ad_key:'detail-page2',ads_type:2}];const root=document.querySelector('#cards');root.replaceChildren();for(const value of values){const card=document.createElement('div');card.className='shadow-common-light bg-white';card.style.cssText='height:100px;position:relative';card.textContent=value.ad_key;card.__reactFiber$t={memoizedProps:{value}};card.onclick=()=>showDetail(value);root.append(card);}document.querySelector('.ant-pagination-item-active').title=String(window.pageNumber);document.querySelector('.ant-pagination-item-active').textContent=String(window.pageNumber);const next=document.querySelector('.ant-pagination-next');next.classList.toggle('ant-pagination-disabled',window.pageNumber===2);next.querySelector('button').disabled=window.pageNumber===2;}
window.go=n=>{window.pageNumber=n;render();};document.querySelector('.ant-pagination-next button').onclick=()=>go(2);render();
</script></body></html>'''.replace('ORIGIN',json.dumps(origin))

def main():
    server=ThreadingHTTPServer(('127.0.0.1',0),external.Handler);threading.Thread(target=server.serve_forever,daemon=True).start();origin=f'http://127.0.0.1:{server.server_port}'
    try:
        with tempfile.TemporaryDirectory() as temp,sync_playwright() as p:
            root=Path(temp);sample=root/'sample.mp4'
            subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','color=c=blue:s=64x64:d=0.3','-f','lavfi','-i','sine=frequency=440:duration=0.3','-c:v','libx264','-pix_fmt','yuv420p','-c:a','aac','-shortest',str(sample)],check=True)
            external.VIDEO=sample.read_bytes();extension=root/'extension';shutil.copytree(SOURCE,extension)
            offscreen=extension/'offscreen.js';adapter='''const nativeFetch=fetch;globalThis.fetch=async(input,options)=>{const url=new URL(input);const target=url.origin===ORIGIN?url.href:url.hostname==='www.youtube.com'?ORIGIN+'/fixture-youtube?v='+url.searchParams.get('v'):url.hostname==='r1.googlevideo.com'?ORIGIN+'/fixture-cdn':null;if(!target)throw new Error('Unexpected remote request blocked');const response=await nativeFetch(target,options);return new Proxy(response,{get(obj,key){if(key==='url')return url.href;const v=Reflect.get(obj,key,obj);return typeof v==='function'?v.bind(obj):v;}});};\n'''.replace('ORIGIN',json.dumps(origin));offscreen.write_text(adapter+offscreen.read_text())
            manifest=json.loads((extension/'manifest.json').read_text());manifest['host_permissions']=['https://*/*','http://*/*'];(extension/'manifest.json').write_text(json.dumps(manifest))
            context=p.chromium.launch_persistent_context(str(root/'profile'),channel='chromium',headless=True,accept_downloads=True,args=[f'--disable-extensions-except={extension}',f'--load-extension={extension}'])
            worker=context.service_workers[0] if context.service_workers else context.wait_for_event('serviceworker');extid=worker.url.split('/')[2]
            context.route('https://www.guangdada.net/**',lambda route:route.fulfill(content_type='text/html',body=fixture(origin)))
            page=context.new_page();page.goto('https://www.guangdada.net/modules/creative/display-ads')
            tab_id=worker.evaluate("async()=>(await chrome.tabs.query({url:'https://www.guangdada.net/*'}))[0].id")
            def popup_page():
                popup=context.new_page();page.bring_to_front();popup.goto(f'chrome-extension://{extid}/popup.html');expect(popup.locator('#select')).to_be_enabled();return popup
            def job():return worker.evaluate('async()=>(await chrome.storage.local.get("job")).job')
            def wait_job(old=None,phase='complete'):
                end=time.monotonic()+30
                while time.monotonic()<end:
                    value=job()
                    if value and value['id']!=old and value['phase']==phase:return value
                    page.wait_for_timeout(100)
                raise AssertionError(value)
            def report(value,count):
                download=worker.evaluate('async id=>(await chrome.downloads.search({id}))[0]',value['downloadId'])
                with ZipFile(download['filename']) as z:
                    assert z.testzip() is None and len(z.namelist())==count+1
                    for name in z.namelist():
                        if name.endswith('.mp4'):assert z.read(name)==external.VIDEO
                    return z.read('下载报告.md').decode()
            # Existing (pre-fix) cart entries with no URL are repaired at start.
            worker.evaluate('''async id=>chrome.storage.local.set({selectionCarts:{[id]:{active:true,revision:1,entries:[{adId:'detail-only',cardIndex:1,pageNumber:1,sourceCardCount:3,kind:'video',reasonCode:'NO_MEDIA_URL'}]}}})''',tab_id)
            popup=popup_page();popup.locator('#start').click();popup.close();old_cart=wait_job();assert old_cart['items'][0]['detailSource'];assert '详情读取原始外链 1 项' in report(old_cart,1)
            assert page.evaluate('window.opens===1 && window.closes===1')
            # Whole current page captures the missing link, leaves direct media alone.
            popup=popup_page();popup.locator('#mode').select_option('current');popup.locator('#start').click();popup.close();current=wait_job(old_cart['id']);assert len(current['items'])==3 and not current['missing'];report(current,3)
            # Point selection captures before leaving a page; the second page
            # has no first-page DOM or data from which its link could be recovered.
            popup=popup_page();popup.locator('#clear-cart').click();popup.locator('#select').click();popup.close()
            expect(page.get_by_role('button',name='选择素材 1',exact=True)).to_be_visible()
            page.get_by_role('button',name='选择素材 1',exact=True).click()
            def wait_cart(count):
                end=time.monotonic()+15
                while time.monotonic()<end:
                    cart=worker.evaluate('async id=>(await chrome.storage.local.get("selectionCarts")).selectionCarts[id]',tab_id)
                    if len(cart['entries'])==count and all(i.get('detailSource') for i in cart['entries']):return cart
                    page.wait_for_timeout(100)
                raise AssertionError(cart)
            wait_cart(1);page.evaluate('go(2)');expect(page.get_by_role('button',name='选择素材 1',exact=True)).to_have_attribute('data-ad-id','detail-page2')
            page.get_by_role('button',name='选择素材 1',exact=True).click();wait_cart(2)
            popup=popup_page();popup.locator('#start').click();popup.close();basket=wait_job(current['id']);assert basket['selectedCardCount']==2 and len(basket['items'])==2;assert '详情读取原始外链 2 项' in report(basket,2)
            # Automatic pagination captures each page's detail before clicking next.
            page.evaluate('go(1)');popup=popup_page();popup.locator('#mode').select_option('pages');popup.locator('#end').select_option('last');popup.locator('#start').click();popup.close();multi=wait_job(basket['id']);assert len(multi['pages'])==2 and not multi['missing'];report(multi,4)
            # Opt out: no detail opens during the download, no external parsing.
            page.evaluate('go(1)');before=page.evaluate('window.opens');popup=popup_page();popup.locator('#mode').select_option('current');popup.locator('#resolve-external').uncheck();popup.locator('#start').click();popup.close();disabled=wait_job(multi['id']);assert page.evaluate('window.opens')==before;assert len(disabled['missing'])==1;report(disabled,2)
            # Stop while a detail is loading: close only our matching dialog,
            # retain the originally selected slot and do not generate a ZIP.
            page.evaluate("window.scenario='slow';render()");popup=popup_page();popup.locator('#mode').select_option('current');popup.locator('#start').click();popup.close()
            expect(page.locator('[role=dialog]')).to_be_visible();page.wait_for_timeout(400)
            popup=context.new_page();page.bring_to_front();popup.goto(f'chrome-extension://{extid}/popup.html');expect(popup.locator('#stop')).to_be_enabled();popup.locator('#stop').click();popup.close();stopped=wait_job(disabled['id'],'stopped');assert stopped['selectedCardCount']==1 and stopped['selections'][0]['adId']=='expected-ad';assert not stopped.get('downloadId');expect(page.locator('[role=dialog]')).to_have_count(0)
            # Wrong creative ID: refuse its original link and preserve accounting.
            page.evaluate("window.scenario='wrong';render()");popup=popup_page();popup.locator('#mode').select_option('current');popup.locator('#start').click();popup.close();wrong=wait_job(stopped['id'],'failed');assert wrong['missing'][0]['reasonCode']=='DETAIL_ID_MISMATCH';assert not wrong.get('downloadId');assert not page.locator('[role=dialog]').count() or page.locator('[role=dialog]').is_visible()
            # A foreign identity must remain open for the user, never closed by us.
            expect(page.locator('[role=dialog]')).to_be_visible();page.get_by_role('button',name='Close',exact=True).click()
            evidence=SOURCE.parents[1]/'data/guangdada-external-1.9.1-acceptance';evidence.mkdir(exist_ok=True)
            (evidence/'detail-acceptance.json').write_text(json.dumps({'version':'1.9.1','verification':'isolated Chromium/loopback fixtures, not real YouTube download','current_files':3,'old_cart_repaired':True,'cross_page_detail_files':2,'pagination_files':4,'disabled_no_detail_clicks':True,'stop_during_detail_retains_selection':True,'wrong_ad_id_rejected':True,'zip_crc_and_original_bytes':'passed'},indent=2))
            context.close();print('PASS: detail-only original link; old-cart repair; current/cross-page/pagination; opt-out; wrong-ID rejection; preserve foreign dialog; MD provenance; one ZIP and original video bytes.')
    finally:server.shutdown()

if __name__=='__main__':main()
