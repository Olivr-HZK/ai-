"""Isolated Chromium integration test; never touches a user's Chrome profile."""
import base64
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from zipfile import ZipFile
from playwright.sync_api import sync_playwright

SOURCE = Path(__file__).resolve().parents[1]
PNG = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=")
VIDEO = b""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/slow.png":
            time.sleep(2)
        payload = VIDEO if self.path.endswith((".mp4", ".webp")) else PNG
        status = {"/missing.png":404,"/forbidden.png":403,"/rate.png":429}.get(self.path,200)
        if self.path == '/empty.png':payload=b''
        if self.path == '/not-media.png':payload=b'<html>not media</html>'
        self.send_response(status)
        self.send_header("Content-Type", "text/html" if self.path=='/not-media.png' else "video/mp4" if payload is VIDEO else "image/png")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass  # Cancellation deliberately closes the request.

    def log_message(self, *args):
        pass


def main():
    global VIDEO
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    origin = f"http://127.0.0.1:{server.server_port}"
    try:
        with tempfile.TemporaryDirectory() as temp, sync_playwright() as p:
            root = Path(temp)
            sample = root / "sample.mp4"
            subprocess.run(["ffmpeg", "-v", "error", "-f", "lavfi", "-i",
                            "color=c=blue:s=64x64:d=0.3", "-c:v", "libx264",
                            "-pix_fmt", "yuv420p", "-movflags", "+faststart", str(sample)], check=True)
            VIDEO = sample.read_bytes()
            extension = root / "extension"
            shutil.copytree(SOURCE, extension)
            manifest = json.loads((extension / "manifest.json").read_text())
            # Headless cannot physically click Chrome's toolbar to grant activeTab.
            # Grant the same origins requested by the production button in this
            # disposable test copy; no user's profile or permissions are changed.
            manifest["host_permissions"] = ["https://*/*", "http://*/*"]
            (extension / "manifest.json").write_text(json.dumps(manifest))
            context = p.chromium.launch_persistent_context(
                str(root / "profile"), channel="chromium", headless=True, accept_downloads=True,
                args=[f"--disable-extensions-except={extension}", f"--load-extension={extension}"],
            )
            worker = context.service_workers[0] if context.service_workers else context.wait_for_event("serviceworker")
            extension_id = worker.url.split("/")[2]
            page = context.new_page()
            context.route("https://www.guangdada.net/**", lambda route: route.fulfill(
                content_type="text/html",
                body=f"""<html><body><img src="{origin}/a.png"><img src="{origin}/a.png">
                <div style="background-image:url('{origin}/b.png');height:80px">Background</div>
                <video src="{origin}/clip.webp" poster="{origin}/a.png"></video>
                <img data-src="{origin}/lazy.png"><img src="{origin}/missing.png">
                <script>
                const records=[
                  ...['a','b','lazy','shadow','missing'].map((n,i)=>({{ad_key:'image'+i,ads_type:1,preview_img_url:'{origin}/'+n+'.png'}})),
                  ...['clip','card','vue3'].map((n,i)=>({{ad_key:'video'+i,ads_type:2,preview_img_url:'{origin}/cover.png',resource_urls:[{{video_url:'{origin}/'+n+(i===1?'.webp':'.mp4'),image_url:'{origin}/cover.png'}}]}})),
                  {{ad_key:'no-video',ads_type:2,preview_img_url:'{origin}/cover.png',resource_urls:[]}},
                  {{ad_key:'playable',ads_type:3,preview_img_url:'{origin}/cover.png'}}
                ];
                for(const value of records) {{
                  const card=document.createElement('div');card.className='shadow-common-light bg-white';card.style.height='60px';card.style.position='relative';
                  card.innerHTML='<img src="{origin}/icon.png"><img src="{origin}/cover.png"><img src="{origin}/cover.png">';
                  card.__reactFiber$test={{memoizedProps:{{}},return:{{memoizedProps:{{value,list:[...records,{{ad_key:'offpage',ads_type:1,preview_img_url:'{origin}/unrelated.png'}}]}}}}}};
                  document.body.append(card);
                }}
                </script>
                </body></html>"""
            ))
            page.goto("https://www.guangdada.net/test")
            tab_id = worker.evaluate("async () => (await chrome.tabs.query({url:'https://www.guangdada.net/*'}))[0].id")
            popup = context.new_page()
            popup.goto(f"chrome-extension://{extension_id}/popup.html")
            result = popup.evaluate("""async id => {
              const {collectMedia} = await import('./collector.js');
              return (await chrome.scripting.executeScript({target:{tabId:id},world:'MAIN',func:collectMedia,args:[false]}))[0].result;
            }""", tab_id)
            assert any(i["kind"] == "video" for i in result["items"]), result
            assert any("shadow.png" in i["url"] for i in result["items"]), result
            assert any("card.webp" in i["url"] and i["kind"] == "video" for i in result["items"]), result
            assert any("vue3.mp4" in i["url"] for i in result["items"]), result
            assert result['cardCount'] == 10 and len(result['missing']) == 2, result
            assert not any(any(x in i["url"] for x in ['cover','icon','unrelated']) for i in result['items']), result
            response = popup.evaluate("""async id => chrome.runtime.sendMessage({
              type:'start',tabId:id,kinds:['image','video'],scroll:false
            })""", tab_id)
            assert response["ok"], response
            # Packaging continues after the popup is closed.
            popup.close()
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                job = worker.evaluate("async () => (await chrome.storage.local.get('job')).job")
                if job["phase"] in ("complete", "failed"):
                    break
                page.wait_for_timeout(200)
            assert job["phase"] == "complete", job
            assert len(job["items"]) == 8, job
            assert sum(i["state"] == "complete" for i in job["items"]) == 7, job
            assert sum(i["state"] == "failed" for i in job["items"]) == 1, job
            # Validate actual bytes via Chrome's resulting local download paths.
            downloads = worker.evaluate("async () => chrome.downloads.search({})")
            assert len(downloads) == 1, downloads
            assert downloads[0]["state"] == "complete"
            with ZipFile(downloads[0]["filename"]) as archive:
                assert archive.testzip() is None
                assert len(archive.namelist()) == 8, archive.namelist()
                report = archive.read("下载报告.md").decode("utf-8")
                assert "成功获取 7 个文件；请求失败 1 个文件" in report and "全部选择（10 项）" in report, report
                assert "HTTP 404" in report and "未用封面代替" in report and "未识别的广告类型" in report
                assert "下载报告.json" not in archive.namelist()
                for name in archive.namelist():
                    if name.endswith(".mp4"):
                        assert archive.read(name) == VIDEO
                        path = root / Path(name).name
                        path.write_bytes(archive.read(name))
                        subprocess.run(["ffmpeg", "-v", "error", "-i", str(path), "-f", "null", "-"], check=True)
                    elif name.endswith(".png"):
                        assert archive.read(name) == PNG
            popup = context.new_page()
            popup.goto(f"chrome-extension://{extension_id}/popup.html")

            def wait_phase(expected):
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    state = worker.evaluate("async () => (await chrome.storage.local.get('job')).job")
                    if state["phase"] == expected:
                        return state
                    page.wait_for_timeout(100)
                raise AssertionError(state)

            assert popup.evaluate("async () => chrome.runtime.sendMessage({type:'retry'})")["ok"]
            wait_phase("complete")
            assert len(worker.evaluate("async () => chrome.downloads.search({})")) == 2
            # A real-sized page: 60 cards, 180 DOM images, only 60 creatives.
            page.evaluate("""origin => {
              document.body.innerHTML = '';
              for(let i=0;i<60;i++){
                const card=document.createElement('div');
                card.className='shadow-common-light bg-white';card.style.height='60px';card.style.position='relative';
                card.innerHTML=`<img src="${origin}/icon.png"><img src="${origin}/cover${i}.png"><img src="${origin}/cover${i}.png">`;
                const value={ad_key:'ad'+i,ads_type:i<30?2:1,preview_img_url:origin+'/image'+i+'.png',
                  resource_urls:i<30?[{video_url:origin+'/video'+i+'.mp4'}]:[]};
                card.__reactFiber$t={memoizedProps:{},return:{memoizedProps:{value,list:[{ad_key:'off-page',video_url:origin+'/extra.mp4'}]}}};
                document.body.append(card);
              }
            }""", origin)
            sixty = popup.evaluate("""async id => {
                const {collectMedia}=await import('./collector.js');
                return (await chrome.scripting.executeScript({target:{tabId:id},world:'MAIN',func:collectMedia}))[0].result;
            }""", tab_id)
            assert sixty["cardCount"] == 60 and len(sixty["items"]) == 60
            assert sum(i["kind"] == "video" for i in sixty["items"]) == 30
            assert not any("cover" in i["url"] or "icon" in i["url"] or "extra" in i["url"] for i in sixty["items"])
            await_selection = popup.evaluate("""async id => {
                const {collectMedia}=await import('./collector.js');
                return (await chrome.scripting.executeScript({target:{tabId:id},world:'MAIN',func:collectMedia,args:[true]}))[0].result;
            }""", tab_id)
            assert await_selection["selectionActive"] and not await_selection["selectedCardNumbers"]
            empty = popup.evaluate("""async id=>chrome.runtime.sendMessage({type:'start',tabId:id,kinds:['video'],usePageSelection:true})""", tab_id)
            assert not empty["ok"] and "尚未勾选" in empty["error"]
            page.evaluate("() => {window.cardOpens=0;for(const c of document.querySelectorAll('.shadow-common-light.bg-white')) c.addEventListener('click',()=>window.cardOpens++);}")
            page.get_by_role("button", name="选择素材 1", exact=True).click()
            page.get_by_role("button", name="选择素材 3", exact=True).click()
            assert page.evaluate("window.cardOpens") == 0
            picked = popup.evaluate("""async id=>{
                const {collectMedia}=await import('./collector.js');
                return (await chrome.scripting.executeScript({target:{tabId:id},world:'MAIN',func:collectMedia}))[0].result;
            }""", tab_id)
            assert picked["selectedCardNumbers"] == [1,3]
            changed = popup.evaluate("""async id=>chrome.runtime.sendMessage({type:'start',tabId:id,kinds:['video'],usePageSelection:true,expectedCardKeys:['wrong-page']})""", tab_id)
            assert not changed["ok"] and "排列已变化" in changed["error"]
            picked_run = popup.evaluate("""async params=>chrome.runtime.sendMessage({type:'start',tabId:params.id,kinds:['video'],usePageSelection:true,expectedCardKeys:params.keys})""",
                                       {"id":tab_id,"keys":picked["cardKeys"]})
            assert picked_run["ok"], picked_run
            selected_job = wait_phase("complete")
            assert selected_job["selectedCardNumbers"] == [1,3] and len(selected_job["items"]) == 2
            selected_download = worker.evaluate("async id=>(await chrome.downloads.search({id}))[0]", selected_job["downloadId"])
            with ZipFile(selected_download["filename"]) as archive:
                names = archive.namelist()
                assert len(names) == 3 and any("/0001_" in n for n in names) and any("/0003_" in n for n in names), names
            page.get_by_role("button", name="全选", exact=True).click()
            assert page.locator("[data-gdd-download-number][aria-pressed=true]").count() == 60
            page.get_by_role("button", name="清空", exact=True).click()
            assert page.locator("[data-gdd-download-number][aria-pressed=true]").count() == 0
            page.get_by_role("button", name="退出选择", exact=True).click()
            assert page.locator("[data-gdd-download-number]").count() == 0
            # Every asset failing must not produce a misleading empty ZIP.
            page.evaluate("(url) => { document.body.innerHTML = ''; const card = document.createElement('div'); card.className='shadow-common-light bg-white'; card.style.height='60px';card.style.position='relative'; card.__reactFiber$t={memoizedProps:{value:{ad_key:'missing',ads_type:1,preview_img_url:url}}}; document.body.append(card); }", origin + "/missing.png")
            assert popup.evaluate("""async id => chrome.runtime.sendMessage({
                type:'start',tabId:id,kinds:['image'],scroll:false
            })""", tab_id)["ok"]
            assert "所有媒体获取失败" in wait_phase("failed")["error"]
            assert len(worker.evaluate("async () => chrome.downloads.search({})")) == 3
            report_page=context.new_page();report_page.goto(f"chrome-extension://{extension_id}/report.html")
            assert 'HTTP 404' in report_page.locator('#report').inner_text()
            report_page.close()
            # Cancel while fetching: no extra ZIP and no stuck task.
            page.evaluate("(url) => { document.body.innerHTML = ''; const card = document.createElement('div'); card.className='shadow-common-light bg-white'; card.style.height='60px';card.style.position='relative'; card.__reactFiber$t={memoizedProps:{value:{ad_key:'slow',ads_type:1,preview_img_url:url}}}; document.body.append(card); }", origin + "/slow.png")
            assert popup.evaluate("""async id => chrome.runtime.sendMessage({
                type:'start',tabId:id,kinds:['image'],scroll:false
            })""", tab_id)["ok"]
            assert popup.evaluate("async () => chrome.runtime.sendMessage({type:'stop'})")["ok"]
            wait_phase("stopped")
            page.wait_for_timeout(300)
            assert len(worker.evaluate("async () => chrome.downloads.search({})")) == 3
            # Full selection ledger: actual HTTP/network/content errors are
            # distinct from unsupported and missing URLs, with one MD per ZIP.
            page.evaluate("""origin=>{
              document.body.innerHTML='';
              const records=[
                ...['ok.png?token=DO_NOT_EXPORT','ok.png?token=DO_NOT_EXPORT','missing.png','forbidden.png','rate.png','not-media.png','empty.png'].map((path,i)=>({ad_key:'image'+i,ads_type:1,preview_img_url:origin+'/'+path})),
                {ad_key:'no-video',ads_type:2}, {ad_key:'playable',ads_type:3},
                {ad_key:'stream',ads_type:2,video_url:origin+'/video.m3u8'},
                {ad_key:'blob',ads_type:2,video_url:'blob:https://www.guangdada.net/123'},
                {ad_key:'invalid',ads_type:1,preview_img_url:'invalid url'},
                {ad_key:'network',ads_type:2,video_url:'http://127.0.0.1:1/broken.mp4'}
              ];
              for(const value of records){const card=document.createElement('div');card.className='shadow-common-light bg-white';card.style.height='60px';card.__reactFiber$t={memoizedProps:{value}};document.body.append(card);}
            }""",origin)
            assert popup.evaluate("async id=>chrome.runtime.sendMessage({type:'start',tabId:id,kinds:['image','video']})",tab_id)['ok']
            mixed=wait_phase('complete')
            downloads=worker.evaluate('async()=>chrome.downloads.search({})')
            assert len(downloads)==4,downloads
            mixed_download=next(i for i in downloads if i['id']==mixed['downloadId'])
            with ZipFile(mixed_download['filename']) as archive:
                assert archive.testzip() is None and len(archive.namelist())==2
                report=archive.read('下载报告.md').decode('utf-8')
                assert '选择 13 项；成功获取 1 个文件；请求失败 6 个文件' in report,report
                assert '缺少原地址 2 项；不支持 3 项；重复合并 1 项' in report,report
                for reason in ['HTTP 404','HTTP 403','HTTP 429','网页或 JSON','空文件','HLS/DASH','非 HTTP','格式无效','网络请求失败','未识别的广告类型']:
                    assert reason in report,(reason,report)
                assert 'DO_NOT_EXPORT' not in report and 'http://' not in report
                assert '全部选择（13 项）' in report and '= 13 项' in report
            # Missing/unsupported-only batches still persist a report, allow
            # opening the real popup button and explicitly saving the same MD.
            page.evaluate("""()=>{document.body.innerHTML='';for(const value of [{ad_key:'playable',ads_type:3},{ad_key:'no-video',ads_type:2}]){const card=document.createElement('div');card.className='shadow-common-light bg-white';card.style.height='60px';card.__reactFiber$t={memoizedProps:{value}};document.body.append(card);}}""")
            assert popup.evaluate("async id=>chrome.runtime.sendMessage({type:'start',tabId:id,kinds:['image','video']})",tab_id)['ok']
            unsupported=wait_phase('failed');assert len(unsupported['items'])==0 and len(unsupported['selections'])==2
            popup.reload();popup.wait_for_selector('#report')
            with context.expect_page() as opened:popup.locator('#report').click()
            report_page=opened.value;report_page.wait_for_load_state()
            text=report_page.locator('#report').inner_text()
            assert '全部选择（2 项）' in text and '未识别的广告类型' in text and '未提供可下载视频地址' in text
            report_page.locator('#save').click()
            deadline=time.monotonic()+10
            while time.monotonic()<deadline:
                downloads=worker.evaluate('async()=>chrome.downloads.search({})')
                md_download=next((i for i in downloads if i['mime']=='text/markdown'),None)
                if md_download and md_download['state']=='complete':break
                report_page.wait_for_timeout(100)
            assert md_download and md_download['state']=='complete',downloads
            assert '全部选择（2 项）' in Path(md_download['filename']).read_text()
            assert len(downloads)==5 and len([i for i in downloads if i['mime']=='application/zip'])==4
            # Production manifest remains narrowly permissioned.
            assert "host_permissions" not in json.loads((SOURCE / "manifest.json").read_text())
            print("PASS: one ZIP + one Markdown report per batch; CRC/media decode; full 13-card accounting; actual HTTP 403/404/429, HTML/empty/network failures; missing/playable/HLS/blob/invalid URL reasons; all-failed and unsupported-only report view/save; repackage/cancel/60-card/selection regression.")
            context.close()
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
