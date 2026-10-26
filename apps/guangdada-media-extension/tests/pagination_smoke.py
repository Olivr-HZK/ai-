"""Exercise real extension pagination in disposable Chromium, never user Chrome."""
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
import browser_smoke as media

SOURCE = Path(__file__).resolve().parents[1]


def fixture(origin):
    return """<!doctype html><html><head><meta charset="utf-8"></head><body><div id="cards"></div>
<ul class="ant-pagination"><li class="ant-pagination-item-active" title="4">4</li><li class="ant-pagination-next" title="下一页"><button aria-label="Next Page">下一页</button></li></ul>
<script>
const origin=ORIGIN;
window.currentPage=4;window.nextClicks=0;window.freeze=false;window.delay=600;
function records(page) {
 if(page===4)return [
  {ad_key:'shared',ads_type:1,preview_img_url:origin+'/shared.png'},
  {ad_key:'v4',ads_type:2,resource_urls:[{video_url:origin+'/v4.mp4'}]},
  {ad_key:'missing',ads_type:2,preview_img_url:origin+'/cover.png'}];
 if(page===5)return [
  {ad_key:'shared',ads_type:1,preview_img_url:origin+'/shared-new.png'},
  {ad_key:'v5',ads_type:2,resource_urls:[{video_url:origin+'/v5.mp4'}]},
  {ad_key:'same-url',ads_type:1,preview_img_url:origin+'/shared.png'}];
 return [
  {ad_key:'i6',ads_type:1,preview_img_url:origin+'/i6.png'},
  {ad_key:'v6',ads_type:2,resource_urls:[{video_url:origin+'/v6.mp4'}]},
  {ad_key:'v4-again',ads_type:2,resource_urls:[{video_url:origin+'/v4.mp4'}]}];
}
function renderCards(page) {
 document.querySelector('#cards').replaceChildren();
 for(const value of records(page)){
  const card=document.createElement('div');card.className='shadow-common-light bg-white';card.style.cssText='height:1000px;position:relative';
  card.innerHTML='<img src="'+origin+'/cover.png"><img src="'+origin+'/icon.png">';
  const bound=page===6 && value.ad_key==='v6'?{...value,resource_urls:[]}:value;
  card.__reactFiber$t={memoizedProps:{},return:{memoizedProps:{value:bound}}};document.querySelector('#cards').append(card);
  if(bound!==value){
   const observer=new IntersectionObserver(entries=>{if(entries.some(e=>e.isIntersecting)){setTimeout(()=>{bound.resource_urls=value.resource_urls;},300);observer.disconnect();}});
   observer.observe(card);
  }
 }
}
function renderPager(){
 const current=document.querySelector('.ant-pagination-item-active');current.title=String(window.currentPage);current.textContent=String(window.currentPage);
 const next=document.querySelector('.ant-pagination-next');next.classList.toggle('ant-pagination-disabled',window.currentPage===6);next.querySelector('button').disabled=window.currentPage===6;
}
document.querySelector('.ant-pagination-next button').onclick=()=>{
 window.nextClicks++;
 if(window.freeze || window.currentPage===6)return;
 window.currentPage++;renderPager();document.querySelector('#cards').setAttribute('aria-busy','true');
 setTimeout(()=>{renderCards(window.currentPage);document.querySelector('#cards').removeAttribute('aria-busy');},window.delay);
};
renderCards(4);renderPager();
</script></body></html>""".replace('ORIGIN', json.dumps(origin))


def main():
    server = ThreadingHTTPServer(('127.0.0.1', 0), media.Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    origin = f'http://127.0.0.1:{server.server_port}'
    try:
        with tempfile.TemporaryDirectory() as temp, sync_playwright() as p:
            root = Path(temp)
            sample = root/'sample.mp4'
            subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','color=c=blue:s=64x64:d=0.3','-c:v','libx264','-pix_fmt','yuv420p',str(sample)],check=True)
            media.VIDEO=sample.read_bytes()
            extension=root/'extension';shutil.copytree(SOURCE,extension)
            manifest=json.loads((extension/'manifest.json').read_text())
            manifest['host_permissions']=['https://www.guangdada.net/*',origin+'/*']
            (extension/'manifest.json').write_text(json.dumps(manifest))
            context=p.chromium.launch_persistent_context(str(root/'profile'),channel='chromium',headless=True,accept_downloads=True,
                args=[f'--disable-extensions-except={extension}',f'--load-extension={extension}'])
            worker=context.service_workers[0] if context.service_workers else context.wait_for_event('serviceworker')
            extension_id=worker.url.split('/')[2]
            context.route('https://www.guangdada.net/**',lambda route:route.fulfill(content_type='text/html',body=fixture(origin)))
            page=context.new_page();page.goto('https://www.guangdada.net/modules/creative/display-ads')
            tab_id=worker.evaluate("async()=>(await chrome.tabs.query({url:'https://www.guangdada.net/*'}))[0].id")

            def popup_page():
                popup=context.new_page();page.bring_to_front()
                popup.goto(f'chrome-extension://{extension_id}/popup.html')
                expect(popup.locator('#start')).to_be_enabled(timeout=10000)
                return popup

            def wait_phase(expected, timeout=25):
                deadline=time.monotonic()+timeout
                while time.monotonic()<deadline:
                    state=worker.evaluate("async()=>(await chrome.storage.local.get('job')).job")
                    if state and state['phase']==expected:
                        print(f"verified {expected}: {len(state.get('pages',[]))} pages",flush=True)
                        return state
                    page.wait_for_timeout(100)
                details=page.evaluate("""()=>({current:window.currentPage,clicks:window.nextClicks,pager:document.querySelector('.ant-pagination')?.outerHTML,cards:document.querySelector('#cards')?.outerHTML})""")
                raise AssertionError({'job':state,'page':details})

            def download_count():return len(worker.evaluate('async()=>chrome.downloads.search({})'))
            def finish_partial():
                paused=wait_phase('collection-paused');assert not paused.get('downloadId')
                answer=popup.evaluate('async id=>chrome.runtime.sendMessage({type:"download-collected",id})',paused['id']);assert answer['ok'],answer
                return wait_phase('complete')
            def start(popup,count=3):
                return popup.evaluate("""async p=>chrome.runtime.sendMessage({type:'start',tabId:p.tabId,mode:'pages',pageCount:p.count,kinds:['image','video']})""",{'tabId':tab_id,'count':count})

            # Use the actual popup form, start at page 4, over-request to prove EOF.
            popup=popup_page();popup.locator('#mode').select_option('pages');popup.locator('#pages').fill('0');popup.locator('#end').select_option('last')
            popup.locator('#start').click();popup.close()
            job=wait_phase('complete')
            assert [i['pageNumber'] for i in job['pages']]==[4,5,6],job
            assert job['cardCount']==9 and len(job['items'])==5 and job['duplicateCount']==3,job
            assert len(job['missing'])==1 and job['missing'][0]['pageNumber']==4,job
            assert job['stopReason']=='last-page' and page.evaluate('window.nextClicks')==2,job
            assert download_count()==1
            download=worker.evaluate('async id=>(await chrome.downloads.search({id}))[0]',job['downloadId'])
            with ZipFile(download['filename']) as archive:
                assert archive.testzip() is None
                assert len(archive.namelist())==6,archive.namelist()
                report=archive.read('下载报告.md').decode('utf-8')
                assert '成功获取 5 个文件；请求失败 0 个文件' in report and '全部选择（9 项）' in report,report
                assert '重复合并 3 项' in report and '缺少原地址 1 项' in report
                assert all(f'第 {n} 页' in report for n in [4,5,6])
                for name in archive.namelist():
                    if name.endswith('.mp4'):
                        assert archive.read(name)==media.VIDEO
                        path=root/Path(name).name;path.write_bytes(archive.read(name))
                        subprocess.run(['ffmpeg','-v','error','-i',str(path),'-f','null','-'],check=True)
                    elif name.endswith('.png'):assert archive.read(name)==media.PNG
                assert any('第0004页/video/0002_' in n for n in archive.namelist())
                assert not any('shared-new' in n for n in archive.namelist())

            # Repackage the exact collected batch without clicking more pages.
            popup=popup_page()
            assert popup.evaluate("async()=>chrome.runtime.sendMessage({type:'retry'})")['ok']
            wait_phase('complete');assert download_count()==2 and page.evaluate('window.nextClicks')==2
            page.reload()
            assert start(popup,2)['ok'];limited=wait_phase('complete')
            assert [i['pageNumber'] for i in limited['pages']]==[4,5] and limited['stopReason']=='page-limit',limited
            assert page.evaluate('window.nextClicks')==1 and download_count()==3

            # New origin on later pages pauses for a narrow permission gesture.
            page.reload()
            page.evaluate("""()=>{
              const original=records;records=page=>original(page).map(value=>page===5 && value.ad_key==='v5'?{...value,resource_urls:[{video_url:'http://localhost:1/new.mp4'}]}:value);
            }""")
            assert start(popup,2)['ok'];waiting=wait_phase('awaiting-permission')
            assert 'http://localhost:1/*' in waiting['mediaOrigins'] and download_count()==3,waiting
            denied=popup.evaluate("async()=>chrome.runtime.sendMessage({type:'resume'})")
            assert not denied['ok'] and '授权' in denied['error'],denied
            assert popup.evaluate("async()=>chrome.runtime.sendMessage({type:'stop'})")['ok']
            wait_phase('stopped')

            # Cancel during delayed card replacement: no later pages or ZIP.
            page.reload();page.evaluate('window.delay=4000')
            assert start(popup,3)['ok']
            page.wait_for_function('window.nextClicks===1')
            assert popup.evaluate("async()=>chrome.runtime.sendMessage({type:'stop'})")['ok']
            wait_phase('stopped');page.wait_for_timeout(1200)
            assert download_count()==3 and page.evaluate('window.nextClicks')==1
            assert (worker.evaluate('async()=>(await chrome.storage.local.get("job")).job'))['items']

            # A frozen button is a failure, never a fabricated last page.
            page.reload();page.evaluate('window.freeze=true')
            assert start(popup,3)['ok'];page.wait_for_function('window.nextClicks===1')
            # Advance only this isolated worker's clock; task state is now
            # owned by the durable per-task store, not the legacy job mirror.
            worker.evaluate("()=>{globalThis.testRealNow=Date.now;Date.now=()=>testRealNow()+31000;}")
            partial=finish_partial()
            worker.evaluate('Date.now=globalThis.testRealNow')
            assert '超时' in partial['collectionError'] and partial['collectionIncomplete'] and download_count()==4,partial
            assert partial['addressStage']=='interrupted' and len(partial['pages'])==1
            assert popup.evaluate("async()=>chrome.runtime.sendMessage({type:'retry'})")['ok']
            wait_phase('complete');assert download_count()==5

            # Reject jumps and unknown pagination instead of silently harvesting.
            page.reload();page.evaluate('window.freeze=true')
            assert start(popup,3)['ok'];page.wait_for_function('window.nextClicks===1')
            page.evaluate("document.querySelector('.ant-pagination-item-active').title='10'")
            jumped=finish_partial();assert '顺序' in jumped['collectionError'] and download_count()==6 and len(jumped['pages'])==1
            page.reload();page.locator('.ant-pagination').evaluate('(el)=>el.remove()')
            partial=start(popup,2);assert not partial['ok'] and '分页' in partial['error'],partial
            assert worker.evaluate('async()=>(await chrome.storage.local.get("job")).job.id')==jumped['id']
            assert download_count()==6
            for count in (0,1001,1.5):
                rejected=start(popup,count);assert not rejected['ok'] and '1-1000' in rejected['error'],rejected
            context.close()
            print('PASS: actual popup form; three async pages starting at 4 -> one ZIP, CRC/media decode; cross-page dedupe/missing report; EOF/page limit; repackage; new-origin permission pause; cancel; timeout/jump/unknown-pager partial downloads from saved addresses; invalid limits.')
    finally:
        server.shutdown()


if __name__=='__main__':main()
