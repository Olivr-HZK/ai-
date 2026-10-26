"""Real isolated MV3 queue, simultaneous collection, targeted controls and restart."""
import json
import shutil
import subprocess
import tempfile
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from zipfile import ZipFile
from playwright.sync_api import sync_playwright, expect
import browser_smoke as media

SOURCE=Path(__file__).resolve().parents[1]
REQUESTS=Counter();GATES={name:threading.Event() for name in ['a','e','g']};RECOVER=False

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        REQUESTS[self.path]+=1
        if self.path in ['/a.mp4','/e.mp4','/g.mp4']:GATES[self.path[1]].wait(25)
        body=media.VIDEO if self.path.endswith('.mp4') else media.PNG
        code=404 if self.path=='/failed.png' and not RECOVER else 200
        self.send_response(code);self.send_header('Content-Type','video/mp4' if self.path.endswith('.mp4') else 'image/png');self.send_header('Content-Length',str(len(body)));self.end_headers()
        try:self.wfile.write(body)
        except (BrokenPipeError,ConnectionResetError):pass
    def log_message(self,*args):pass

def fixture(origin):
    return '''<!doctype html><html><meta charset="utf-8"><body><div id="cards"></div><script>
const origin=ORIGIN,group=new URL(location).searchParams.get('group');window.opens=0;
const data={a:[['a','video','a.mp4'],['failed','image','failed.png']],b:[['b-video','video','b.mp4'],['b-image','image','b.png']],c:[['c','image','c.png']],d:[['detail','video',null]],e:[['e','video','e.mp4']],f:[['f','image','f.png']],g:[['g','video','g.mp4']]}[group];
for(const [id,kind,path] of data){const value={ad_key:id,ads_type:kind==='video'?2:1,...(path?(kind==='video'?{video_url:origin+'/'+path}:{preview_img_url:origin+'/'+path}):{})};const card=document.createElement('div');card.className='shadow-common-light bg-white';card.style.cssText='height:120px;position:relative';card.textContent=id;card.__reactFiber$t={memoizedProps:{value}};
card.onclick=()=>{window.opens++;const modal=document.createElement('div');modal.role='dialog';modal.className='ant-modal-content';modal.style.cssText='position:fixed;inset:100px;background:white';modal.innerHTML='<button aria-label="Close">X</button><a href="/modules/creative/display-ads/detail?id='+id+'">单页打开</a>';modal.querySelector('button').onclick=()=>modal.remove();document.body.append(modal);};document.querySelector('#cards').append(card);}
</script></body></html>'''.replace('ORIGIN',json.dumps(origin))

def main():
    global RECOVER
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);threading.Thread(target=server.serve_forever,daemon=True).start();origin=f'http://127.0.0.1:{server.server_port}'
    evidence=SOURCE.parents[1]/'data/guangdada-tasks-acceptance';evidence.mkdir(exist_ok=True)
    try:
        with tempfile.TemporaryDirectory() as temp,sync_playwright() as p:
            root=Path(temp);sample=root/'sample.mp4';subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','color=c=green:s=64x64:d=0.3','-c:v','libx264','-pix_fmt','yuv420p',str(sample)],check=True);media.VIDEO=sample.read_bytes()
            extension=root/'extension';shutil.copytree(SOURCE,extension);manifest=json.loads((extension/'manifest.json').read_text());manifest['host_permissions']=['https://www.guangdada.net/*',origin+'/*'];(extension/'manifest.json').write_text(json.dumps(manifest))
            def launch():
                ctx=p.chromium.launch_persistent_context(str(root/'profile'),channel='chromium',headless=True,accept_downloads=True,args=[f'--disable-extensions-except={extension}',f'--load-extension={extension}'])
                worker=ctx.service_workers[0] if ctx.service_workers else ctx.wait_for_event('serviceworker')
                ctx.route('https://www.guangdada.net/**',lambda route:route.fulfill(content_type='text/html',body=fixture(origin)))
                return ctx,worker
            context,worker=launch();extid=worker.url.split('/')[2];page=context.new_page();control=context.new_page();control.goto(f'chrome-extension://{extid}/report.html')
            def source(group):page.goto('https://www.guangdada.net/modules/creative/display-ads?group='+group)
            def read(id=None):return worker.evaluate('async id=>{const key=id?"mediaTask:"+id:"job";return (await chrome.storage.local.get(key))[key]}',id)
            def wait(predicate,id=None,timeout=35):
                end=time.monotonic()+timeout;last=None
                while time.monotonic()<end:
                    last=read(id)
                    if last and predicate(last):return last
                    page.wait_for_timeout(80)
                raise AssertionError({'id':id,'phase':last and last.get('phase'),'items':[(i.get('adId'),i.get('state')) for i in (last or {}).get('items',[])]})
            def send(type,**kwargs):return control.evaluate('async message=>chrome.runtime.sendMessage(message)',{'type':type,**kwargs})
            def popup():
                win=context.new_page();page.bring_to_front();win.goto(f'chrome-extension://{extid}/popup.html');win.wait_for_function('!document.querySelector("#task-list").disabled || !document.querySelector("#start").disabled');return win
            def start(name):
                win=popup();win.locator('#mode').select_option('current');win.locator('#task-name').fill(name);win.locator('#start').click();win.close();return wait(lambda j:j.get('taskName')==name and j['phase']!='collecting')
            def archive(job,expected):
                download=worker.evaluate('async id=>(await chrome.downloads.search({id}))[0]',job['downloadId'])
                with ZipFile(download['filename']) as z:
                    assert z.testzip() is None and len(z.namelist())==len(expected)+1
                    actual=[z.read(n) for n in z.namelist() if n!='下载报告.md'];assert sorted(actual)==sorted(expected)
                    report=z.read('下载报告.md').decode();assert job['id'] in report and job['taskName'] in report
                    for n in z.namelist():
                        if n.endswith('.mp4'):
                            decoded=root/'decode.mp4';decoded.write_bytes(z.read(n));subprocess.run(['ffmpeg','-v','error','-i',str(decoded),'-f','null','-'],check=True)
                    return report
            source('a');a=start('第一批 A');a=wait(lambda j:j['phase']=='fetching' and REQUESTS['/a.mp4'],a['id'])
            source('b');win=popup();expect(win.locator('#select')).to_be_enabled();win.locator('#new-selection').click();win.close();page.wait_for_selector('[data-gdd-download-number]');page.get_by_role('button',name='选择素材 1',exact=True).click();page.get_by_role('button',name='选择素材 2',exact=True).click()
            win=popup();expect(win.locator('#mode')).to_have_value('basket');win.locator('#task-name').fill('第二批 B');win.locator('#start').click();win.close();b=wait(lambda j:j.get('taskName')=='第二批 B' and j['phase']=='queued')
            assert read(a['id'])['phase']=='fetching';assert not REQUESTS['/b.mp4'] and not REQUESTS['/b.png'];assert len(read(a['id'])['selections'])==2 and len(b['selections'])==2
            source('c');c=start('排队 C');assert c['phase']=='queued';assert send('select-task',id=b['id'])['ok'];assert send('stop',id=c['id'])['ok'];assert read(c['id'])['phase']=='stopped';assert read(a['id'])['phase']=='fetching';assert not REQUESTS['/c.png']
            # Hold a second address detail open. A must still checkpoint media
            # completion, independently of the collector's long-running lock.
            source('d');tab=worker.evaluate("async()=>(await chrome.tabs.query({url:'https://www.guangdada.net/*'}))[0].id")
            control.evaluate('message=>{void chrome.runtime.sendMessage(message).then(r=>window.detailResult=r)}',{'type':'start','tabId':tab,'kinds':['video'],'taskName':'采址 D'})
            page.wait_for_function('window.opens===1');d=wait(lambda j:j.get('taskName')=='采址 D' and j['phase']=='collecting');GATES['a'].set();wait(lambda j:any(i['adId']=='a' and i.get('state')=='complete' for i in j['items']),a['id']);assert read(d['id'])['phase']=='collecting'
            assert send('stop',id=d['id'])['ok'];assert read(d['id'])['phase']=='stopped';a=wait(lambda j:j['phase']=='complete',a['id']);b=wait(lambda j:j['phase']=='complete',b['id']);assert REQUESTS['/a.mp4']==1
            assert 'HTTP 404' in archive(a,[media.VIDEO]);archive(b,[media.VIDEO,media.PNG]);assert not REQUESTS['/c.png']
            assert send('select-task',id=a['id'])['ok'];viewer=context.new_page();viewer.goto(f'chrome-extension://{extid}/report.html?task='+a['id']);viewer.wait_for_function('document.querySelector("#report").textContent.includes("第一批 A")');assert send('select-task',id=b['id'])['ok'];assert '第一批 A' in viewer.locator('#report').inner_text() and '第二批 B' not in viewer.locator('#report').inner_text();viewer.close()
            # Retrying A targets only A's failed source, without touching B.
            RECOVER=True;before=REQUESTS.copy();retry=send('retry-failed',id=a['id']);assert retry['ok'];recovered=wait(lambda j:j.get('parentTaskId')==a['id'] and j['phase']=='complete');assert REQUESTS['/a.mp4']==before['/a.mp4'];assert REQUESTS['/b.mp4']==before['/b.mp4'];archive(recovered,[media.PNG]);assert read(a['id'])['phase']=='complete' and read(b['id'])['phase']=='complete'
            source('g');g=start('单独停止 G');wait(lambda j:j['phase']=='fetching' and REQUESTS['/g.mp4'],g['id']);source('f');h=start('停止后接续 H');assert h['phase']=='queued'
            source('d');control.evaluate('message=>{void chrome.runtime.sendMessage(message)}',{'type':'start','tabId':tab,'kinds':['video'],'taskName':'采址 I'});page.wait_for_function('window.opens===1');i=wait(lambda j:j.get('taskName')=='采址 I' and j['phase']=='collecting')
            started=time.monotonic();assert send('stop',id=g['id'])['ok'];assert time.monotonic()-started<3;assert read(i['id'])['phase']=='collecting';assert read(g['id'])['phase']=='stopped' and not read(g['id']).get('downloadId');h=wait(lambda j:j['phase']=='complete',h['id']);archive(h,[media.PNG]);assert send('stop',id=i['id'])['ok'];assert read(i['id'])['phase']=='stopped'
            # Real browser restart: queued F survives; interrupted E keeps its
            # saved ledger but cannot claim its temporary media was delivered.
            source('e');e=start('重启中断 E');wait(lambda j:j['phase']=='fetching' and REQUESTS['/e.mp4'],e['id']);source('f');f=start('重启后排队 F');assert f['phase']=='queued'
            shot=popup();shot.screenshot(path=str(evidence/'tasks-popup.png'),full_page=True);shot.close();context.close()
            context,worker=launch();control=context.new_page();control.goto(f'chrome-extension://{extid}/report.html');page=context.new_page();page.goto('about:blank')
            f=wait(lambda j:j['phase']=='complete',f['id']);e=read(e['id']);assert e['phase']=='failed' and e['items'][0]['url']==origin+'/e.mp4' and not e.get('downloadId');archive(f,[media.PNG]);assert read(a['id'])['phase']=='complete' and read(b['id'])['phase']=='complete'
            result={'version':manifest['version'],'verification':'Isolated real Chromium MV3/OPFS/downloads with loopback fixtures only; no user account/profile modified.','select_and_queue_B_while_A_fetching':True,'stop_queued_C_does_not_cancel_A':True,'A_progress_saved_during_D_detail_collection':True,'stop_collector_D_preserves_A_and_B':True,'stop_download_G_while_I_collecting_starts_only_H':True,'one_zip_and_pinned_report_per_task':True,'failed_retry_A_does_not_repeat_success_or_B':True,'actual_profile_restart_preserves_queued_F_and_interrupted_E_ledger':True,'zip_crc_original_bytes_video_full_decode':'passed','zip_tasks':[a['id'],b['id'],recovered['id'],h['id'],f['id']]}
            (evidence/'tasks-acceptance.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n');context.close();print('PASS: A download + B point-selection/queue; targeted queued/collector stop; background checkpoints during detail collection; separate ZIP/MD; pinned reports; failed-only retry isolation; actual browser restart queue/ledger preservation; CRC/bytes/full decode.')
    finally:
        for gate in GATES.values():gate.set()
        server.shutdown()

if __name__=='__main__':main()
