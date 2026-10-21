"""Playable originals, failures, selection and pagination in isolated Chromium."""
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from zipfile import ZipFile
from playwright.sync_api import sync_playwright, expect
import browser_smoke as media

SOURCE=Path(__file__).resolve().parents[1]
GAME=b'<!doctype html><html><body><canvas></canvas><script>window.SHOULD_NEVER_RUN=true</script><script src="/should-not-run.js"></script></body></html>'
SAMPLE=SOURCE.parents[1]/'data/guangdada-playable-acceptance/sample.html'
REAL=SAMPLE.read_bytes() if SAMPLE.exists() else GAME
REQUESTS=Counter()

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        REQUESTS[self.path]+=1
        data=REAL if self.path=='/real.html' else GAME
        status=404 if self.path=='/missing.html' else 403 if self.path=='/forbidden.html' else 200
        kind='text/html'
        if self.path=='/login.html':data=b'<html><body><input type="password"><script></script></body></html>'
        if self.path=='/wrong':data=media.PNG;kind='image/png'
        self.send_response(status);self.send_header('Content-Type',kind);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
    def log_message(self,*args):pass


def fixture(origin):
    return '''<!doctype html><html><meta charset="utf-8"><body><div id="cards"></div><ul class="ant-pagination"><li class="ant-pagination-prev"><button>上一页</button></li><li class="ant-pagination-item-active" title="1">1</li><li class="ant-pagination-next"><button>下一页</button></li></ul><script>
const origin=ORIGIN;window.pageNumber=1;window.opens=0;
const p=(ad_key,url)=>({ad_key,ads_type:7,preview_img_url:origin+'/cover.png',resource_urls:url?[{type:7,html_url:origin+url}]:[]});
function records(){return window.pageNumber===1?[
 p('real','/real.html'),p('game','/game.html'),p('no-html'),
 {ad_key:'video',ads_type:2,video_url:origin+'/clip.mp4'},
 {ad_key:'image',ads_type:1,preview_img_url:origin+'/photo.png'},
 p('login','/login.html'),p('wrong','/wrong'),p('missing','/missing.html'),
 {ad_key:'unknown',ads_type:99},p('video-in-html-field','/clip.mp4'),
 {ad_key:'fallback',ads_type:7,cdn_url:[origin+'/cover.png',origin+'/fallback.htm']}
]:[p('real','/real.html'),p('forbidden','/forbidden.html'),{ad_key:'entry',ads_type:7,html_url:origin+'/entry'},{ad_key:'other-image',ads_type:1,preview_img_url:origin+'/other.png'}];}
function render(){const cards=document.querySelector('#cards');cards.replaceChildren();for(const value of records()){const card=document.createElement('div');card.className='shadow-common-light bg-white';card.style.cssText='height:80px;position:relative';card.textContent=value.ad_key;card.__reactFiber$t={memoizedProps:{value}};card.onclick=()=>window.opens++;cards.append(card);}cards.removeAttribute('aria-busy');}
function pager(){const current=document.querySelector('.ant-pagination-item-active');current.title=String(window.pageNumber);current.textContent=String(window.pageNumber);for(const [name,disabled] of [['prev',window.pageNumber===1],['next',window.pageNumber===2]]){const li=document.querySelector('.ant-pagination-'+name);li.classList.toggle('ant-pagination-disabled',disabled);li.querySelector('button').disabled=disabled;}}
window.go=n=>{window.pageNumber=n;pager();document.querySelector('#cards').setAttribute('aria-busy','true');setTimeout(render,250);};document.querySelector('.ant-pagination-next button').onclick=()=>go(2);document.querySelector('.ant-pagination-prev button').onclick=()=>go(1);render();pager();
</script></body></html>'''.replace('ORIGIN',json.dumps(origin))


def main():
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);threading.Thread(target=server.serve_forever,daemon=True).start();origin=f'http://127.0.0.1:{server.server_port}'
    try:
        with tempfile.TemporaryDirectory() as temp,sync_playwright() as p:
            root=Path(temp);extension=root/'extension';shutil.copytree(SOURCE,extension)
            manifest=json.loads((extension/'manifest.json').read_text());manifest['host_permissions']=['https://www.guangdada.net/*',origin+'/*'];(extension/'manifest.json').write_text(json.dumps(manifest))
            context=p.chromium.launch_persistent_context(str(root/'profile'),channel='chromium',headless=True,accept_downloads=True,args=[f'--disable-extensions-except={extension}',f'--load-extension={extension}'])
            worker=context.service_workers[0] if context.service_workers else context.wait_for_event('serviceworker');extension_id=worker.url.split('/')[2]
            context.route('https://www.guangdada.net/**',lambda route:route.fulfill(content_type='text/html',body=fixture(origin)))
            page=context.new_page();page.goto('https://www.guangdada.net/modules/creative/display-ads')
            tab_id=worker.evaluate("async()=>(await chrome.tabs.query({url:'https://www.guangdada.net/*'}))[0].id")
            def popup_page():
                popup=context.new_page();page.bring_to_front();popup.goto(f'chrome-extension://{extension_id}/popup.html');expect(popup.locator('#start')).to_be_enabled();return popup
            def only_playable(popup):
                popup.locator('#image').uncheck();popup.locator('#video').uncheck();expect(popup.locator('#playable')).to_be_checked()
            def job():return worker.evaluate('async()=>(await chrome.storage.local.get("job")).job')
            def wait_phase(phase,previous_id=None):
                end=time.monotonic()+30
                while time.monotonic()<end:
                    value=job()
                    if value and value['id']!=previous_id and value['phase']==phase:return value
                    page.wait_for_timeout(100)
                raise AssertionError(value)
            def cart():return worker.evaluate('async id=>(await chrome.storage.local.get("selectionCarts")).selectionCarts[id]',tab_id)
            def wait_cart(count):
                end=time.monotonic()+10
                while time.monotonic()<end:
                    value=cart()
                    if len(value['entries'])==count:return value
                    page.wait_for_timeout(50)
                raise AssertionError(value)
            def check_zip(value,success,failed):
                download=worker.evaluate('async id=>(await chrome.downloads.search({id}))[0]',value['downloadId'])
                with ZipFile(download['filename']) as archive:
                    assert archive.testzip() is None and len(archive.namelist())==success+1,archive.namelist()
                    report=archive.read('下载报告.md').decode('utf-8')
                    assert f'成功获取 {success} 个文件；请求失败 {failed} 个文件' in report,report
                    assert 'Playable 试玩' in report and '外部' in report and '未验证离线可玩性' in report
                    for name in archive.namelist():
                        if name.endswith(('.html','.htm')):
                            assert '/playable/' in name and archive.read(name)==(REAL if '_real.html' in name else GAME)
                    assert not any(name.endswith(('.png','.mp4','.js')) for name in archive.namelist())
                    return report
            popup=popup_page();only_playable(popup);popup.screenshot(path=str(root/'playable-popup.png'))
            popup.locator('#start').click();popup.close();current=wait_phase('complete')
            assert current['playableCards']==8 and len(current['selections'])==11,current
            report=check_zip(current,3,3)
            for text in ['HTTP 404','登录或授权页面','未提供可识别','未提供可下载的原 HTML','未用图片、视频或ZIP','类型未勾选']:
                assert text in report,(text,report)
            assert current['items'][0]['playableInfo']['externalReferences']>=1
            # Click across pages, include one unavailable choice, keep first-page provenance.
            popup=popup_page();popup.locator('#select').click()
            expect(page.locator('[data-gdd-download-number]').first).to_have_attribute('data-ad-id','real')
            for count,number in enumerate([1,2,3],1):page.get_by_role('button',name=f'选择素材 {number}',exact=True).click();wait_cart(count)
            page.get_by_role('button',name='下一页',exact=True).click()
            expect(page.locator('[data-gdd-download-number]').first).to_have_attribute('data-ad-id','real')
            expect(page.get_by_role('button',name='选择素材 2',exact=True)).to_have_attribute('data-ad-id','forbidden')
            for count,number in enumerate([2,3],4):page.get_by_role('button',name=f'选择素材 {number}',exact=True).click();wait_cart(count)
            popup=popup_page();only_playable(popup);expect(popup.locator('#mode')).to_have_value('basket');popup.locator('#start').click();popup.close()
            selected=wait_phase('complete',current['id']);report=check_zip(selected,3,1)
            assert selected['selectedCardCount']==5 and 'HTTP 403' in report and '第 2 页 · 第 3 个' in report
            assert page.evaluate('window.opens')==0
            # Automatic pagination includes Playable, deduplicates it and reports missing HTML.
            page.evaluate('go(1)');expect(page.get_by_role('button',name='选择素材 2',exact=True)).to_have_attribute('data-ad-id','game')
            popup=popup_page();only_playable(popup);popup.locator('#mode').select_option('pages');popup.locator('#end').select_option('last');popup.locator('#start').click();popup.close()
            multi=wait_phase('complete',selected['id']);report=check_zip(multi,4,4)
            assert multi['selectedCardCount']==15 and multi['duplicateCount']==1 and multi['stopReason']=='last-page',multi
            assert '第 2 页 · 第 3 个' in report and '重复合并 1 项' in report
            downloads=worker.evaluate('async()=>chrome.downloads.search({})');assert len(downloads)==3,downloads
            assert REQUESTS['/should-not-run.js']==0 and REQUESTS['/cover.png']==0 and REQUESTS['/clip.mp4']==0 and REQUESTS['/photo.png']==0,REQUESTS
            assert 'host_permissions' not in json.loads((SOURCE/'manifest.json').read_text())
            if SAMPLE.exists():
                evidence={'version':'1.8.0','original_sample_bytes':len(REAL),'original_sample_sha256':hashlib.sha256(REAL).hexdigest(),'verification':'Real CDN HTML sample served unchanged through local fixture; extension ZIP bytes verified. Not a live logged-in Guangdada page acceptance.','modes':{'current':{'selected':11,'success':3,'failed':3},'cross_page':{'selected':5,'success':3,'failed':1},'pagination':{'selected':15,'success':4,'failed':4,'duplicates':1}},'extra_dependency_requests':REQUESTS['/should-not-run.js']}
                (SAMPLE.parent/'acceptance.json').write_text(json.dumps(evidence,ensure_ascii=False,indent=2))
                shutil.copy2(root/'playable-popup.png',SAMPLE.parent/'playable-popup.png')
            context.close()
            print('PASS: actual Playable checkbox; original HTML bytes/ZIP CRC; no script execution or dependency fetch; current-page/cross-page/automatic-last-page; ads_type=7/html_url/cdn fallback; missing/login/non-HTML/403/404 report; filtered media and duplicate handling.')
    finally:server.shutdown()

if __name__=='__main__':main()
