"""Cross-page click selection through real extension UI in isolated Chromium."""
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import time
from http.server import ThreadingHTTPServer
from zipfile import ZipFile
from playwright.sync_api import sync_playwright,expect
import browser_smoke as media
SOURCE=Path(__file__).resolve().parents[1]


def fixture(origin):
    return """<!doctype html><html><head><meta charset="utf-8"></head><body><div id="cards"></div><ul class="ant-pagination">
<li class="ant-pagination-prev"><button>上一页</button></li><li class="ant-pagination-item-active" title="1">1</li><li class="ant-pagination-next"><button>下一页</button></li></ul><script>
const origin=ORIGIN;window.pageNumber=Number(new URL(location).searchParams.get('p') || 1);window.cardOpens=0;
function records(){return window.pageNumber===1?[
 {ad_key:'a',ads_type:1,preview_img_url:origin+'/a.png'},
 {ad_key:'b',ads_type:2,resource_urls:[{video_url:origin+'/b.mp4'}]},
 {ad_key:'missing',ads_type:2,preview_img_url:origin+'/cover.png'}
]:[
 {ad_key:'c',ads_type:2,resource_urls:[{video_url:origin+'/c.mp4'}]},
 {ad_key:'d',ads_type:1,preview_img_url:origin+'/d.png'},
 {ad_key:'a',ads_type:1,preview_img_url:origin+'/a.png'}
];}
function render(){
 document.querySelector('#cards').replaceChildren();
 for(const value of records()){
  const card=document.createElement('div');card.className='shadow-common-light bg-white';card.style.cssText='height:110px;position:relative';card.textContent=value.ad_key;
  card.__reactFiber$t={memoizedProps:{value}};card.onclick=()=>window.cardOpens++;document.querySelector('#cards').append(card);
 }
 document.querySelector('#cards').removeAttribute('aria-busy');
}
function pager(){
 const active=document.querySelector('.ant-pagination-item-active');active.title=String(window.pageNumber);active.textContent=String(window.pageNumber);
 for(const [className,disabled] of [['prev',window.pageNumber===1],['next',window.pageNumber===2]]){
  const li=document.querySelector('.ant-pagination-'+className);li.classList.toggle('ant-pagination-disabled',disabled);li.querySelector('button').disabled=disabled;
 }
}
window.go=n=>{window.pageNumber=n;pager();document.querySelector('#cards').setAttribute('aria-busy','true');history.replaceState(null,'','?p='+n);setTimeout(render,1200);};
document.querySelector('.ant-pagination-prev button').onclick=()=>window.go(1);
document.querySelector('.ant-pagination-next button').onclick=()=>window.go(2);
render();pager();
</script></body></html>""".replace('ORIGIN',json.dumps(origin))


def main():
    server=ThreadingHTTPServer(('127.0.0.1',0),media.Handler)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    origin=f'http://127.0.0.1:{server.server_port}'
    try:
        with tempfile.TemporaryDirectory() as temp,sync_playwright() as p:
            root=Path(temp);sample=root/'sample.mp4'
            subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','color=c=blue:s=64x64:d=0.3','-c:v','libx264','-pix_fmt','yuv420p',str(sample)],check=True)
            media.VIDEO=sample.read_bytes()
            extension=root/'extension';shutil.copytree(SOURCE,extension)
            manifest=json.loads((extension/'manifest.json').read_text());manifest['host_permissions']=['https://www.guangdada.net/*',origin+'/*'];(extension/'manifest.json').write_text(json.dumps(manifest))
            context=p.chromium.launch_persistent_context(str(root/'profile'),channel='chromium',headless=True,accept_downloads=True,args=[f'--disable-extensions-except={extension}',f'--load-extension={extension}'])
            worker=context.service_workers[0] if context.service_workers else context.wait_for_event('serviceworker');extension_id=worker.url.split('/')[2]
            context.route('https://www.guangdada.net/**',lambda route:route.fulfill(content_type='text/html',body=fixture(origin)))
            page=context.new_page();page.goto('https://www.guangdada.net/modules/creative/display-ads?p=1')
            tab_id=worker.evaluate("async()=>(await chrome.tabs.query({url:'https://www.guangdada.net/*'}))[0].id")
            def popup_page():
                popup=context.new_page();page.bring_to_front();popup.goto(f'chrome-extension://{extension_id}/popup.html')
                expect(popup.locator('#select')).to_be_enabled();return popup
            def cart():return worker.evaluate('async id=>(await chrome.storage.local.get("selectionCarts")).selectionCarts?.[id]',tab_id)
            def wait_count(count):
                end=time.monotonic()+10
                while time.monotonic()<end:
                    value=cart()
                    if value and len(value['entries'])==count:return value
                    page.wait_for_timeout(50)
                raise AssertionError(value)
            def badge(number):return page.get_by_role('button',name=f'选择素材 {number}',exact=True)
            def ready(ad_id):expect(page.locator('[data-gdd-download-number]').first).to_have_attribute('data-ad-id',ad_id,timeout=10000)

            popup=popup_page();popup.locator('#select').click();ready('a')
            badge(1).click();expect(badge(1)).to_have_attribute('aria-pressed','true')
            badge(2).click();wait_count(2)
            page.get_by_role('button',name='下一页',exact=True).click();ready('c')
            # Duplicate creative a is already checked on page 2.
            expect(badge(3)).to_have_attribute('aria-pressed','true')
            badge(1).click();selected=wait_count(3)
            expect(page.locator('[data-gdd-selection-count]')).to_contain_text('合计 3 张')
            assert [(i['adId'],i['pageNumber'],i['cardIndex']) for i in selected['entries']]==[('a',1,1),('b',1,2),('c',2,1)],selected
            assert page.evaluate('window.cardOpens')==0
            popup=popup_page();expect(popup.locator('#mode')).to_have_value('basket')
            expect(popup.locator('#cart-count')).to_have_text('跨页已选 3 张')
            expect(popup.locator('#cart-pages')).to_contain_text('第 1 页：1、2')
            expect(popup.locator('#cart-pages')).to_contain_text('第 2 页：1');popup.close()

            page.get_by_role('button',name='上一页',exact=True).click();ready('a')
            expect(badge(1)).to_have_attribute('aria-pressed','true');expect(badge(2)).to_have_attribute('aria-pressed','true')
            page.reload();ready('a');expect(badge(1)).to_have_attribute('aria-pressed','true')
            # The page cannot spoof cart data through public window messages.
            page.evaluate("window.postMessage({type:'selection-update',action:'all',url:'https://evil.test/a.mp4'},'*')")
            assert len(cart()['entries'])==3
            # A second tab gets a separate cart.
            other=context.new_page();other.goto(page.url)
            other_id=worker.evaluate("async()=>(await chrome.tabs.query({url:'https://www.guangdada.net/*'})).sort((a,b)=>b.id-a.id)[0].id")
            assert other_id!=tab_id
            assert not worker.evaluate('async id=>(await chrome.storage.local.get("selectionCarts")).selectionCarts?.[id]',other_id)
            other.close();page.bring_to_front()

            # Block a same-tick next-page click while a choice is still being saved.
            page.evaluate("document.querySelector('[data-gdd-download-number]').click();document.querySelector('.ant-pagination-next button').click()")
            wait_count(2);assert page.evaluate('window.pageNumber')==1
            badge(1).click();wait_count(3)
            page.get_by_role('button',name='清空本页',exact=True).click();left=wait_count(1)
            assert left['entries'][0]['adId']=='c'
            page.get_by_role('button',name='全选本页',exact=True).click();wait_count(4)
            badge(3).click();wait_count(3)  # Exclude the missing video explicitly.
            page.get_by_role('button',name='收起点选',exact=True).click()
            expect(page.locator('[data-gdd-download-number]')).to_have_count(0)
            assert len(cart()['entries'])==3 and not cart()['active']

            popup=popup_page()
            stale=popup.evaluate("async id=>chrome.runtime.sendMessage({type:'start-basket',tabId:id,kinds:['image','video'],expectedRevision:-1})",tab_id)
            assert not stale['ok'] and '清单发生变化' in stale['error'],stale
            popup.locator('#start').click();popup.close()
            end=time.monotonic()+20
            while time.monotonic()<end:
                job=worker.evaluate('async()=>(await chrome.storage.local.get("job")).job')
                if job and job['phase'] in ('complete','failed'):break
                page.wait_for_timeout(100)
            assert job['phase']=='complete' and job['mode']=='selection' and len(job['items'])==3,job
            downloads=worker.evaluate('async()=>chrome.downloads.search({})');assert len(downloads)==1,downloads
            with ZipFile(downloads[0]['filename']) as archive:
                assert archive.testzip() is None and len(archive.namelist())==4
                report=archive.read('下载报告.md').decode('utf-8')
                assert '选择 3 项；成功获取 3 个文件；请求失败 0 个文件' in report,report
                assert all(position in report for position in ['第 1 页 · 第 1 个','第 1 页 · 第 2 个','第 2 页 · 第 1 个'])
                for name in archive.namelist():
                    if name.endswith('.mp4'):
                        assert archive.read(name)==media.VIDEO
                        path=root/Path(name).name;path.write_bytes(archive.read(name))
                        subprocess.run(['ffmpeg','-v','error','-i',str(path),'-f','null','-'],check=True)
                    elif name.endswith('.png'):assert archive.read(name)==media.PNG
                assert any('第0002页/video/0001_' in name for name in archive.namelist())
            assert len(cart()['entries'])==3
            popup=popup_page();popup.locator('#clear-cart').click()
            expect(popup.locator('#cart-count')).to_have_text('跨页已选 0 张')
            expect(popup.locator('#start')).to_be_disabled()
            empty=popup.evaluate("async id=>chrome.runtime.sendMessage({type:'start-basket',tabId:id,kinds:['image','video']})",tab_id)
            assert not empty['ok'] and '清单为空' in empty['error'],empty
            assert len(worker.evaluate('async()=>chrome.downloads.search({})'))==1
            context.close()
            print('PASS: click p1 slots 1+2 and p2 slot1 -> one ZIP with 2 decoded videos+1 image; restore previous page/reload; duplicate badges; tab isolation; clear page keeps others; all/hide; pending navigation guard; stale/empty cart refusal; no cover or unselected media.')
    finally:server.shutdown()


if __name__=='__main__':main()
