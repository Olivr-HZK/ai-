"""Disposable Chromium acceptance: visible card metadata, no extra detail clicks.
No real account, browser profile or remote media is used.
"""
import json, shutil, subprocess, tempfile, threading, time
from pathlib import Path
from zipfile import ZipFile
from http.server import ThreadingHTTPServer
from playwright.sync_api import sync_playwright, expect
import browser_smoke as media
SOURCE=Path(__file__).resolve().parents[1]
EVIDENCE=SOURCE.parents[1]/'data/guangdada-metadata-acceptance'

def fixture(origin):
    return '''<!doctype html><html><head><meta charset="utf-8"></head><body><div id=cards></div><ul class=ant-pagination><li class=ant-pagination-item-active title=1>1</li><li class=ant-pagination-next><button>Next</button></li></ul><script>
const origin=ORIGIN;window.pageNumber=1;window.opens=0;window.hidden=false;
function render(){
 const values=window.pageNumber===1?[
  {ad_key:'image',ads_type:1,preview_img_url:origin+'/a.png',name:'List App | <A>',appid:'ai.test.a',platform:'Android',estimate:'1.2K',badge:'重投'},
  {ad_key:'video',ads_type:2,video_url:origin+'/v.mp4',name:'List B',appid:'123456',platform:'iOS',estimate:'0',badge:'非重投'},
  {ad_key:'no-link',ads_type:2,name:'No URL App',appid:'ai.test.missing',platform:'Android',estimate:'10',badge:'重投'},
  {ad_key:'same-file',ads_type:1,preview_img_url:origin+'/a.png',name:'Other App',appid:'ai.test.other',platform:'Android',estimate:'2M',badge:null}
 ]:[{ad_key:'page2',ads_type:1,preview_img_url:origin+'/p2.png',name:'Page Two',appid:'ai.page.two',platform:'Android',estimate:'4663',badge:'重投'}];
 const root=document.querySelector('#cards');root.replaceChildren();
 for(const v of values){const card=document.createElement('div');card.className='shadow-common-light bg-white';card.style.cssText='height:110px;position:relative';card.__reactFiber$t={memoizedProps:{value:v}};
 const a=document.createElement('a');a.href=v.platform==='iOS'?'https://apps.apple.com/us/app/test/id'+v.appid:'https://play.google.com/store/apps/details?id='+v.appid+'&token=DO_NOT_EXPORT';a.textContent=v.name;if(v.ad_key==='image'){const header=document.createElement('div');header.className='flex p-3 pb-0';const name=document.createElement('span');name.className='cursor-pointer';name.textContent=v.name;const icon=document.createElement('img');icon.alt='Google Play';icon.style.cssText='width:16px;height:16px';header.append(name,icon);card.append(header);}else card.append(a);
 const est=document.createElement('p');est.textContent='展示估值：'+v.estimate;card.append(est);
 if(v.badge){const b=document.createElement('span');b.className='ant-tag';b.textContent=v.badge;card.append(b);}
 const hidden=document.createElement('span');hidden.className='ant-tag';hidden.style.display='none';hidden.textContent='重投';card.append(hidden);
 card.onclick=()=>{window.opens++;throw new Error('Unexpected detail click for list metadata');};root.append(card);}
 document.querySelector('.ant-pagination-item-active').title=String(window.pageNumber);document.querySelector('.ant-pagination-item-active').textContent=String(window.pageNumber);
 const next=document.querySelector('.ant-pagination-next');next.classList.toggle('ant-pagination-disabled',window.pageNumber===2);next.querySelector('button').disabled=window.pageNumber===2;
}
window.go=n=>{window.pageNumber=n;render();};document.querySelector('.ant-pagination-next button').onclick=()=>go(2);render();
window.logout=()=>{document.querySelector('#cards').replaceChildren();const d=document.createElement('div');d.setAttribute('role','dialog');d.textContent='其他设备登录，强制退出';document.body.append(d);};
</script></body></html>'''.replace('ORIGIN',json.dumps(origin))

def main():
 server=ThreadingHTTPServer(('127.0.0.1',0),media.Handler);threading.Thread(target=server.serve_forever,daemon=True).start();origin=f'http://127.0.0.1:{server.server_port}'
 try:
  with tempfile.TemporaryDirectory() as temp,sync_playwright() as p:
   root=Path(temp);sample=root/'sample.mp4';subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','color=c=blue:s=64x64:d=0.2','-c:v','libx264','-pix_fmt','yuv420p',str(sample)],check=True);media.VIDEO=sample.read_bytes()
   ext=root/'extension';shutil.copytree(SOURCE,ext);manifest=json.loads((ext/'manifest.json').read_text());manifest['host_permissions']=['https://www.guangdada.net/*',origin+'/*'];(ext/'manifest.json').write_text(json.dumps(manifest))
   ctx=p.chromium.launch_persistent_context(str(root/'profile'),channel='chromium',headless=True,accept_downloads=True,args=[f'--disable-extensions-except={ext}',f'--load-extension={ext}'])
   worker=ctx.service_workers[0] if ctx.service_workers else ctx.wait_for_event('serviceworker');extid=worker.url.split('/')[2];ctx.route('https://www.guangdada.net/**',lambda r:r.fulfill(content_type='text/html',body=fixture(origin)))
   page=ctx.new_page();page.goto('https://www.guangdada.net/modules/creative/display-ads');tab_id=worker.evaluate("async()=>(await chrome.tabs.query({url:'https://www.guangdada.net/*'}))[0].id")
   def popup():
    control=ctx.new_page();page.bring_to_front();control.goto(f'chrome-extension://{extid}/popup.html');expect(control.locator('#start')).to_be_enabled();return control
   def job():return worker.evaluate('async()=>(await chrome.storage.local.get("job")).job')
   def wait(old=None):
    end=time.monotonic()+45
    while time.monotonic()<end:
     j=job()
     if j and j['id']!=old and j['phase'] in ['complete','failed']:assert j['phase']=='complete',j;return j
     page.wait_for_timeout(100)
    raise AssertionError(j)
   def report(j,count):
    d=worker.evaluate('async id=>(await chrome.downloads.search({id}))[0]',j['downloadId'])
    with ZipFile(d['filename']) as z:
     assert z.testzip() is None and len(z.namelist())==count+1,z.namelist()
     for n in z.namelist():
      if n.endswith('.mp4'):assert z.read(n)==media.VIDEO
      elif n.endswith('.png'):assert z.read(n)==media.PNG
     md=z.read('下载报告.md').decode();assert '素材信息' in md;assert 'DO_NOT_EXPORT' not in md;return md
   # Actual popup flow; explicitly disable only the unrelated video-link lookup.
   control=popup();control.locator('#resolve-external').uncheck();control.locator('#start').click();control.close();current=wait();assert page.evaluate('window.opens')==0
   md=report(current,2);rows=current['selections'];assert len(rows)==4
   assert rows[0]['metadata']['fields']['appName']['value']=='List App | <A>';assert rows[0]['metadata']['fields']['appId'] is None;assert rows[1]['metadata']['fields']['platform']['value']=='iOS';assert rows[1]['metadata']['fields']['displayEstimate']['value']==0;assert rows[1]['metadata']['fields']['relaunch']['value'] is False
   assert rows[2]['metadata']['fields']['appId']['value']=='ai.test.missing';assert rows[3]['metadata']['fields']['relaunch'] is None
   assert 'List App &#124; &lt;A&gt;' in md and '2M' in md and '重复合并' in md and '缺少原地址' in md
   # Cross-page point-selection is durable before a logout, and the saved queue
   # subsequently downloads without any list or metadata read.
   control=popup();control.locator('#select').click();control.close();page.get_by_role('button',name='选择素材 2',exact=True).click()
   def cart_ready(n):
    end=time.monotonic()+15
    while time.monotonic()<end:
     c=worker.evaluate('async id=>(await chrome.storage.local.get("selectionCarts")).selectionCarts[id]',tab_id)
     if len(c['entries'])==n and all(e.get('metadata') for e in c['entries']):return c
     page.wait_for_timeout(100)
    raise AssertionError(c)
   cart_ready(1);page.evaluate('go(2)');expect(page.get_by_role('button',name='选择素材 1',exact=True)).to_have_attribute('data-ad-id','page2');page.get_by_role('button',name='选择素材 1',exact=True).click();cart_ready(2)
   page.evaluate('logout()');control=popup();control.locator('#mode').select_option('basket');control.locator('#start').click();control.close();basket=wait(current['id']);report(basket,2);assert [e['metadata']['fields']['appId']['value'] for e in basket['selections']]==['123456','ai.page.two'];assert page.evaluate('window.opens')==0
   # Auto-pagination saves each page before leaving it; no metadata click.
   page.reload();control=popup();control.locator('#mode').select_option('pages');control.locator('#end').select_option('last');control.locator('#resolve-external').uncheck();control.locator('#start').click();control.close();multi=wait(basket['id']);assert len(multi['pages'])==2;report(multi,3);assert page.evaluate('window.opens')==0
   EVIDENCE.mkdir(exist_ok=True);(EVIDENCE/'metadata-acceptance.json').write_text(json.dumps({'version':manifest['version'],'verification':'isolated Chromium/loopback, not authenticated GDD','current_selected':4,'current_files':2,'cross_page_files':2,'pagination_pages':2,'pagination_files':3,'no_metadata_detail_clicks':True,'visible_zero_and_rounded_estimate':True,'hidden_badge_ignored':True,'unknown_not_false':True,'duplicate_and_missing_media_metadata':True,'saved_cart_download_after_logout':True,'zip_crc_original_media_bytes':True},ensure_ascii=False,indent=2));(EVIDENCE/'sample-download-report.md').write_text(md);ctx.close()
   print('PASS: card App/estimate/relaunch, no metadata detail clicks, cross-page persistence/logout, pagination, zero/rounded/hidden/unknown, duplicate/missing accounting, one ZIP+MD and exact bytes.')
 finally:server.shutdown()
if __name__=='__main__':main()
