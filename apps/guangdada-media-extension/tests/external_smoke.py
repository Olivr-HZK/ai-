"""Real MV3/offscreen flow on isolated fixtures; never uses the user's Chrome."""
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
from urllib.parse import urlparse,parse_qs
from playwright.sync_api import sync_playwright, expect
import browser_smoke as media

SOURCE=Path(__file__).resolve().parents[1]
REQUESTS=Counter()
VIDEO=b''
LARGE=b'<html><body><canvas></canvas><script>window.NEVER=true</script>'+b' '*(9*1024*1024)+b'</body></html>'
ID='aBcD1234_-Z'
CIPHER='cipher12345'
CDN='https://r1.googlevideo.com/videoplayback?fixture=secret'

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        REQUESTS[self.path]+=1
        kind='text/html';data=b'<video src="/resolved.mp4"></video><script src="/never.js"></script>'
        if self.path.endswith('.mp4'):kind='video/mp4';data=VIDEO
        if self.path.endswith('.png'):kind='image/png';data=media.PNG
        if self.path=='/poster-only':data=b'<meta property="og:image" content="/cover.png"><script>window.NEVER=true</script>'
        if self.path.startswith('/fixture-youtube?'):
            vid=parse_qs(urlparse(self.path).query).get('v',[ID])[0]
            fmt={'signatureCipher':'s=secret'} if vid==CIPHER else {'url':CDN,'mimeType':'video/mp4; codecs="avc1.42001E, mp4a.40.2"','height':360}
            body={'videoDetails':{'videoId':vid},'playabilityStatus':{'status':'OK'},'streamingData':{'formats':[fmt]}}
            data=('<script>var ytInitialPlayerResponse = '+json.dumps(body)+';throw new Error("MUST_NOT_RUN");</script>').encode()
        if self.path=='/fixture-cdn':kind='video/mp4';data=VIDEO
        if self.path=='/large.html':data=LARGE
        self.send_response(200);self.send_header('Content-Type',kind);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
    def log_message(self,*args):pass

def fixture(origin):
    return '''<!doctype html><html><body><div id="cards"></div><ul class="ant-pagination"><li class="ant-pagination-item-active" title="1">1</li><li class="ant-pagination-next"><button>下一页</button></li></ul><script>
const origin=ORIGIN;window.pageNumber=1;window.opens=0;
const records=()=>window.pageNumber===1?[
 {ad_key:'image',ads_type:1,preview_img_url:origin+'/photo.png'},
 {ad_key:'external',ads_type:2,video_url:origin+'/watch'},
 {ad_key:'youtube',ads_type:2,video_url:'https://youtu.be/aBcD1234_-Z'},
 {ad_key:'cipher',ads_type:2,video_url:'https://www.youtube.com/watch?v=cipher12345'},
 {ad_key:'incomplete',ads_type:2,video_url:'https://www.youtube.com/watch?v='},
 {ad_key:'fallback',ads_type:2,video_url:'https://www.youtube.com/watch?v=',cdn_url:[origin+'/backup.mp4',origin+'/cover.png']},
 {ad_key:'poster-only',ads_type:2,video_url:origin+'/poster-only'}
]:[{ad_key:'second-page',ads_type:2,video_url:origin+'/page2-watch'}];
function render(){const root=document.querySelector('#cards');root.replaceChildren();for(const value of records()){const card=document.createElement('div');card.className='shadow-common-light bg-white';card.style.cssText='height:80px;position:relative';card.textContent=value.ad_key;card.__reactFiber$t={memoizedProps:{value}};card.onclick=()=>window.opens++;root.append(card);}document.querySelector('.ant-pagination-item-active').title=String(window.pageNumber);document.querySelector('.ant-pagination-item-active').textContent=String(window.pageNumber);const next=document.querySelector('.ant-pagination-next');next.classList.toggle('ant-pagination-disabled',window.pageNumber===2);next.querySelector('button').disabled=window.pageNumber===2;}
window.go=n=>{window.pageNumber=n;render();};document.querySelector('button').onclick=()=>go(2);render();
</script></body></html>'''.replace('ORIGIN',json.dumps(origin))

def main():
    global VIDEO
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);threading.Thread(target=server.serve_forever,daemon=True).start();origin=f'http://127.0.0.1:{server.server_port}'
    try:
        with tempfile.TemporaryDirectory() as temp,sync_playwright() as p:
            root=Path(temp);sample=root/'sample.mp4'
            subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','color=c=blue:s=64x64:d=0.3','-f','lavfi','-i','sine=frequency=440:duration=0.3','-c:v','libx264','-pix_fmt','yuv420p','-c:a','aac','-shortest','-movflags','+faststart',str(sample)],check=True)
            VIDEO=sample.read_bytes();extension=root/'extension';shutil.copytree(SOURCE,extension)
            # Context.route does not intercept extension offscreen requests.
            # A transport adapter in this disposable copy serves YouTube/CDN
            # fixtures only from loopback while preserving response.url for the
            # unmodified parser. Other remote requests are explicitly blocked.
            offscreen=extension/'offscreen.js'
            adapter='''const fixtureFetch=globalThis.fetch.bind(globalThis);globalThis.fetch=async(input,options)=>{const url=new URL(input);let target;if(url.origin===ORIGIN)target=url.href;else if(url.hostname==='www.youtube.com')target=ORIGIN+'/fixture-youtube?v='+encodeURIComponent(url.searchParams.get('v')||'');else if(url.hostname==='r1.googlevideo.com')target=ORIGIN+'/fixture-cdn';else throw new Error('Fixture blocked unexpected remote fetch');const response=await fixtureFetch(target,options);return new Proxy(response,{get(obj,key){if(key==='url')return url.href;const v=Reflect.get(obj,key,obj);return typeof v==='function'?v.bind(obj):v;}});};\n'''.replace('ORIGIN',json.dumps(origin))
            offscreen.write_text(adapter+offscreen.read_text())
            manifest=json.loads((extension/'manifest.json').read_text());manifest['host_permissions']=['https://*/*','http://*/*'];(extension/'manifest.json').write_text(json.dumps(manifest))
            context=p.chromium.launch_persistent_context(str(root/'profile'),channel='chromium',headless=True,accept_downloads=True,args=[f'--disable-extensions-except={extension}',f'--load-extension={extension}'])
            worker=context.service_workers[0] if context.service_workers else context.wait_for_event('serviceworker');extid=worker.url.split('/')[2]
            context.route('https://www.guangdada.net/**',lambda route:route.fulfill(content_type='text/html',body=fixture(origin)))
            page=context.new_page();page.goto('https://www.guangdada.net/modules/creative/display-ads')
            tab_id=worker.evaluate("async()=>(await chrome.tabs.query({url:'https://www.guangdada.net/*'}))[0].id")
            def popup_page():
                popup=context.new_page();page.bring_to_front();popup.goto(f'chrome-extension://{extid}/popup.html');popup.wait_for_function("()=>!document.querySelector('#start').disabled || !document.querySelector('#progress').hidden");return popup
            def job():return worker.evaluate('async()=>(await chrome.storage.local.get("job")).job')
            def wait_phase(phase,old=None):
                end=time.monotonic()+35
                while time.monotonic()<end:
                    value=job()
                    if value and value['phase']==phase and value['id']!=old:return value
                    page.wait_for_timeout(100)
                raise AssertionError(value)
            def check_zip(value,success):
                download=worker.evaluate('async id=>(await chrome.downloads.search({id}))[0]',value['downloadId'])
                with ZipFile(download['filename']) as archive:
                    assert archive.testzip() is None and len(archive.namelist())==success+1,archive.namelist()
                    report=archive.read('下载报告.md').decode();assert '视频外链自动补全' in report
                    for name in archive.namelist():
                        if name.endswith('.mp4'):
                            assert archive.read(name)==VIDEO
                            target=root/'decode.mp4';target.write_bytes(archive.read(name));subprocess.run(['ffmpeg','-v','error','-i',str(target),'-f','null','-'],check=True)
                    return report
            # Simulate an ungranted discovered CDN in this disposable worker.
            # Network and fetch are real; the native grant prompt itself is not
            # emulated as successful real-user approval.
            worker.evaluate("""()=>{globalThis.allowFixtureCDN=false;const original=chrome.permissions.contains.bind(chrome.permissions);chrome.permissions.contains=async options=>options.origins.some(o=>o.startsWith('https://r1.googlevideo.com'))&&!globalThis.allowFixtureCDN?false:original(options);} """)
            popup=popup_page();expect(popup.locator('#resolve-external')).to_be_checked();popup.locator('#start').click();popup.close()
            waiting=wait_phase('awaiting-permission');assert waiting['waitingForExternalPermission'];assert 'https://r1.googlevideo.com/*' in waiting['mediaOrigins'];assert not REQUESTS['/fixture-cdn']
            assert worker.evaluate("async()=> (await chrome.runtime.getContexts({contextTypes:['OFFSCREEN_DOCUMENT']})).length")==1
            page.wait_for_timeout(300);assert job()['phase']=='awaiting-permission'
            image_requests=REQUESTS['/photo.png'];assert image_requests==1
            popup=popup_page();expect(popup.locator('#authorize')).to_be_visible()
            worker.evaluate('globalThis.allowFixtureCDN=true')
            # A fresh status read may resume an already granted site before
            # this test's click; both paths must retain the offscreen bytes.
            if popup.locator('#authorize').is_visible():popup.locator('#authorize').click()
            popup.close();current=wait_phase('complete')
            assert REQUESTS['/photo.png']==image_requests and REQUESTS['/fixture-cdn']==1
            assert len(current['selections'])==7 and len(current['items'])==7,current
            assert sum(i['state']=='complete' for i in current['items'])==4
            report=check_zip(current,4)
            for text in ['播放器签名','11位视频ID','自动补全成功','没有提供可直接下载','补全成功 3 项']:assert text in report,(text,report)
            assert 'fixture=secret' not in report and 'https://' not in report
            assert REQUESTS['/cover.png']==0 and REQUESTS['/never.js']==0
            assert sum(v for k,v in REQUESTS.items() if k.startswith('/fixture-youtube?'))==2,REQUESTS
            # Cross-page cart and pagination retain source choices and fallback addresses.
            popup=popup_page();popup.locator('#select').click();expect(page.locator('[data-gdd-download-number]').first).to_be_visible()
            page.get_by_role('button',name='选择素材 6',exact=True).click();page.wait_for_timeout(350)
            page.evaluate('go(2)');expect(page.get_by_role('button',name='选择素材 1',exact=True)).to_have_attribute('data-ad-id','second-page')
            page.get_by_role('button',name='选择素材 1',exact=True).click();page.wait_for_timeout(350)
            popup=popup_page();expect(popup.locator('#mode')).to_have_value('basket');popup.locator('#start').click();popup.close();selected=wait_phase('complete',current['id']);assert selected['selectedCardCount']==2;check_zip(selected,2)
            page.evaluate('go(1)');page.wait_for_timeout(350);popup=popup_page();popup.locator('#mode').select_option('pages');popup.locator('#end').select_option('last');popup.locator('#start').click();popup.close();multi=wait_phase('complete',selected['id']);assert len(multi['pages'])==2;check_zip(multi,5)
            # Disabling the option causes no external-page extraction or fallback.
            page.evaluate('go(1)');popup=popup_page();popup.locator('#mode').select_option('current');popup.locator('#resolve-external').uncheck();popup.locator('#start').click();popup.close();disabled=wait_phase('complete',multi['id']);assert not disabled['autoResolveExternal'];assert not any(i.get('externalInfo') for i in disabled['items']);check_zip(disabled,1)
            def set_only(value):
                page.evaluate("""value=>{const root=document.querySelector('#cards');root.replaceChildren();const card=document.createElement('div');card.className='shadow-common-light bg-white';card.style.height='80px';card.__reactFiber$t={memoizedProps:{value}};root.append(card);} """,value)
            # All-failed resolution remains reviewable without an empty ZIP.
            set_only({'ad_key':'only-incomplete','ads_type':2,'video_url':'https://www.youtube.com/watch?v='})
            popup=popup_page();popup.locator('#mode').select_option('current');popup.locator('#start').click();popup.close();failed=wait_phase('failed',disabled['id']);assert failed['items'][0]['errorCode']=='EXTERNAL_INCOMPLETE_ID';assert not failed.get('downloadId')
            popup=popup_page();popup.locator('#report').click();report_tab=context.pages[-1];report_tab.wait_for_function("()=>document.body.textContent.includes('11位视频ID')");report_tab.close();popup.close()
            # The external HTML parsing cap must not regress large Playable
            # originals, which retain the existing media/batch size boundary.
            set_only({'ad_key':'large-playable','ads_type':7,'html_url':origin+'/large.html'})
            popup=popup_page();popup.locator('#mode').select_option('current');popup.locator('#start').click();popup.close();large=wait_phase('complete',failed['id'])
            download=worker.evaluate('async id=>(await chrome.downloads.search({id}))[0]',large['downloadId'])
            with ZipFile(download['filename']) as archive:
                assert archive.testzip() is None;name=next(n for n in archive.namelist() if n.endswith('.html'));assert archive.read(name)==LARGE
            # A declined / pending discovered-origin request can be stopped.
            page.evaluate('go(1)');worker.evaluate('globalThis.allowFixtureCDN=false');popup=popup_page();popup.locator('#mode').select_option('current');popup.locator('#start').click();popup.close();wait_phase('awaiting-permission',large['id'])
            popup=popup_page();popup.locator('#stop').click();popup.close();stopped=wait_phase('stopped');assert not stopped.get('downloadId');assert worker.evaluate('async()=> (await chrome.downloads.search({})).length')==5
            assert page.evaluate('window.opens')==3  # Only the incomplete-ID card needs a normal detail click.
            assert 'host_permissions' not in json.loads((SOURCE/'manifest.json').read_text())
            evidence=SOURCE.parents[1]/'data/guangdada-metadata-acceptance';evidence.mkdir(exist_ok=True)
            (evidence/'acceptance.json').write_text(json.dumps({'version':manifest['version'],'verification':'Isolated Chromium using loopback-only transport adapter and static YouTube fixtures, real MP4 bytes/audio+video and FFmpeg decode. Discovered-origin gate simulated; no real user permission prompt or logged-in website acceptance.','current':{'selected':7,'success':4,'failed':3},'cross_page':{'selected':2,'success':2},'pagination':{'pages':2,'success':5},'disabled':{'success':1},'all_failed_report':True,'large_playable_bytes':len(LARGE),'downloads':5,'script_requests':REQUESTS['/never.js'],'cover_requests':REQUESTS['/cover.png']},ensure_ascii=False,indent=2))
            popup=popup_page();popup.screenshot(path=str(evidence/'popup.png'));context.close()
            print('PASS: static YouTube combined-stream resolution/cipher/incomplete ID; generic HTML/observed backups; no cover/script fetch; MD accounting and one ZIP; preserved buffers while new-origin permission pending/resumed; current/cart/pagination; opt-out; stop; CRC and full audio+video decode.')
    finally:server.shutdown()

if __name__=='__main__':main()
