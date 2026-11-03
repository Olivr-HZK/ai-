import base64,json,hashlib,tempfile,threading,unittest,zipfile,subprocess,time,requests,os
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from unittest.mock import patch
from ua_workflows.guangdada_direct import PageReader,Paused,collect,download,transfer,report,profile_lock,write,good_url,READERS,main
from ua_workflows.guangdada_headless_ui import PresetUI,PRESETS
PNG=base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=')
HTML='''<!doctype html><meta charset="utf-8"><body>
<button role="tab" aria-selected="true">游戏</button><button role="tab" aria-selected="false">工具</button>
<button id="preset">afizzy竞品游戏</button><span class="ant-tag" id="tag"></span>
<div class="ant-picker-range"><input id="from"><input id="to"></div><button id="sort">最新创意</button>
<div id="cards"></div><ul class="ant-pagination"><li class="ant-pagination-item-active" title="1">1</li><li class="ant-pagination-next"><button>下一页</button></li></ul>
<script>
let n=1;function render(){document.querySelector('#cards').replaceChildren();for(let i=0;i<2;i++){const v={ad_key:'p'+n+'-'+i,ads_type:1,preview_img_url:'ORIGIN/'+i+'.png?token=SECRET'};let card=document.createElement('div');card.className='shadow-common-light bg-white';card.style.height='100px';card.innerHTML='<div class="p-3 pb-0"><span class="cursor-pointer">Example App</span><img alt="Google Play"></div>'+(n===1?'2026-11-02':'2026-11-01')+' ~ 2026-11-02';card.__reactFiber$t={memoizedProps:{value:v}};document.querySelector('#cards').append(card);}let e=document.querySelector('.ant-pagination-item-active');e.title=String(n);e.textContent=String(n);}
document.querySelector('#preset').onclick=()=>{document.querySelector('#tag').textContent='Applied';document.querySelector('#from').value='2020-01-01';document.querySelector('#to').value='2020-01-01';};document.querySelector('#sort').onclick=e=>e.target.className='text-blue-600';document.querySelector('.ant-pagination-next button').onclick=()=>{n++;render();};render();
</script>'''
class DirectTests(unittest.TestCase):
 def test_no_extension_runtime_in_browser_reader(self):
  source=READERS.read_text()
  for token in ['chrome.runtime','chrome.permissions','chrome-extension://','offscreen','load-extension']:self.assertNotIn(token,source)
 def test_invalid_and_stream_urls_rejected(self):
  for s in ['file:///x','https://u:p@example.com/a.mp4','https://example.com/a.m3u8']:
   with self.assertRaises(ValueError):good_url(s)
 def test_profile_must_be_owned(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);(root/'Preferences').write_text('{}')
   with self.assertRaises(Paused):
    with profile_lock(root):pass
 def test_profile_mutual_exclusion(self):
  with tempfile.TemporaryDirectory() as d:
   with profile_lock(Path(d)):
    with self.assertRaises(Paused):
     with profile_lock(Path(d)):pass
 def test_report_no_urls_or_signatures(self):
  s={'date':'2026-11-02','preset':PRESETS[0],'phase':'paused','rows':[{'pageNumber':1,'cardIndex':1,'adId':'ad','url':'https://cdn.example/a?token=SECRET','kind':'video','state':'missing','reason':'未取得地址'}]}
  text=report(s);self.assertNotIn('SECRET',text);self.assertNotIn('https://',text);self.assertIn('缺少原地址',text)
 def test_detail_address_is_checkpointed_before_close_failure(self):
  reader=PageReader.__new__(PageReader);reader.safe=lambda:None
  class P:
   def wait_for_timeout(s,*args):pass
  reader.page=P();row={'adId':'one','cardIndex':1};saved=[]
  def call(method,arg=None,args=None):
   if args[0]=='open':return {'opened':True,'pending':True}
   if args[0]=='read':return {'adId':'one','url':'https://cdn.example/one.mp4'}
   if args[0]=='close':raise Paused('close failed')
  reader.call=call
  with self.assertRaises(Paused):reader.detail(row,checkpoint=lambda:saved.append(dict(row)))
  self.assertEqual(saved[0]['url'],'https://cdn.example/one.mp4')
 def test_unknown_page_click_intent_is_not_replayed(self):
  class R:
   def wait_page(s,**kw):self.assertIn('after',kw);raise Paused('restore next page')
  state={'context':{},'next_intent':{'pageNumber':1,'cardKeys':['old']},'rows':[]}
  with tempfile.TemporaryDirectory() as d:
   with self.assertRaises(Paused):collect(R(),state,Path(d)/'state.json','2026-11-02')
  self.assertEqual(state['phase'],'paused');self.assertIn('next_intent',state)

class PipelineTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name);self.cookies=[]
  cookies=self.cookies
  class Handler(BaseHTTPRequestHandler):
   def do_GET(s):
    cookies.append(s.headers.get('Cookie'))
    payload=b'<!doctype html><html><script src="https://dependency.example/x.js"></script></html>' if s.path.startswith('/playable') else PNG
    s.send_response(200);s.send_header('Content-Type','text/html' if s.path.startswith('/playable') else 'image/png');s.send_header('Content-Length',str(len(payload)));s.end_headers();s.wfile.write(payload)
   def log_message(s,*a):pass
  server=ThreadingHTTPServer(('127.0.0.1',0),Handler);threading.Thread(target=server.serve_forever,daemon=True).start();self.addCleanup(server.server_close);self.addCleanup(server.shutdown);self.origin=f'http://127.0.0.1:{server.server_port}'
 def test_detail_uses_actual_player_and_rejects_site_placeholder(self):
  from playwright.sync_api import sync_playwright
  with sync_playwright() as p:
   browser=p.chromium.launch(headless=True);page=browser.new_page();page.route('**/*',lambda route:route.abort())
   markup='<div class="ant-modal-content"><a href="https://www.guangdada.net/modules/creative/display-ads/detail?id=creative">单页打开</a><a href="https://guangdada.net/modules/creative/display-ads">官方资源限制 原始链接</a>PLAYER<button aria-label="Close">关闭</button></div>'
   page.set_content(markup.replace('PLAYER','<video src="https://cdn.example/original.mp4"></video>'))
   reader=PageReader(page);result=reader.call('detail',args=['read',{'adId':'creative'}]);self.assertEqual(result['url'],'https://cdn.example/original.mp4')
   page.set_content(markup.replace('PLAYER',''));self.assertNotIn('url',reader.call('detail',args=['read',{'adId':'creative'}]))
   browser.close()
 def test_broken_cover_never_clicks_app_icon_as_media(self):
  from playwright.sync_api import sync_playwright
  with sync_playwright() as p:
   browser=p.chromium.launch(headless=True);page=browser.new_page();page.set_content('<div class="shadow-common-light bg-white" style="width:300px;height:300px"><div class="p-3 pb-0"><img style="width:64px;height:64px"></div></div>')
   page.evaluate('''()=>{let c=document.querySelector('.shadow-common-light');c.__reactFiber$t={memoizedProps:{value:{ad_key:'creative'}}};window.appClicks=0;window.cardClicks=0;c.querySelector('img').onclick=()=>window.appClicks++;c.onclick=()=>window.cardClicks++;}''')
   PageReader(page).call('detail',args=['open',{'cardIndex':1,'adId':'creative'}]);self.assertEqual(page.evaluate('window.appClicks'),0);self.assertEqual(page.evaluate('window.cardClicks'),1);browser.close()
 def test_public_youtube_has_no_cookie_or_config_and_reuses_verified_file(self):
  real_run=subprocess.run;calls=[]
  def run(command,**kwargs):
   if 'yt_dlp' not in command:return real_run(command,**kwargs)
   calls.append(command);Path(command[command.index('--output')+1].replace('%(ext)s','mp4')).write_bytes(PNG)
   return subprocess.CompletedProcess(command,0,b'',b'')
  row={'adId':'youtube','kind':'video','url':'https://www.youtube.com/watch?v=test','pageNumber':1,'cardIndex':1}
  with patch('ua_workflows.guangdada_direct.subprocess.run',side_effect=run):
   result=download(row,self.root/'files');self.assertEqual(result['state'],'complete');self.assertEqual(result['bytes'],len(PNG))
   self.assertEqual(download(result,self.root/'files'),result)
  self.assertEqual(len(calls),1);self.assertIn('--ignore-config',calls[0]);self.assertIn('--no-playlist',calls[0]);self.assertFalse(any('cookie' in x for x in calls[0]));self.assertIn('[protocol=https]',calls[0][calls[0].index('--format')+1])
 def test_youtube_refusal_is_failure_and_temporary_files_are_removed(self):
  row={'adId':'youtube','kind':'video','url':'https://www.youtube.com/watch?v=test','pageNumber':1,'cardIndex':1}
  with patch('ua_workflows.guangdada_direct.subprocess.run',return_value=subprocess.CompletedProcess([],1,b'',b'ERROR: Sign in to confirm https://example.com/private?token=SECRET')):
   result=download(row,self.root/'files')
  self.assertEqual(result['state'],'failed');self.assertIn('Sign in',result['reason']);self.assertNotIn('SECRET',result['reason']);self.assertEqual(list((self.root/'files').iterdir()),[])
 def test_http_failure_keeps_status_code(self):
  response=requests.Response();response.status_code=404
  with patch('ua_workflows.guangdada_direct.requests.get',side_effect=requests.HTTPError(response=response)):
   result=download({'adId':'gone','kind':'image','url':self.origin+'/gone.png','pageNumber':1,'cardIndex':1},self.root)
  self.assertEqual(result['reason'],'HTTP 404')
 def test_retry_failed_keeps_old_archive_and_does_not_fetch_success_again(self):
  private=self.root/'private';work=private/'runs/2026-11-02_tool';output=self.root/'output/2026-11-02_tool/tool';output.mkdir(parents=True)
  files=work/'tool_files';files.mkdir(parents=True);name='第0001页/image/0001_cached.png';(files/name).parent.mkdir(parents=True);(files/name).write_bytes(PNG)
  old=output/'tool_2026-11-02.zip';old.write_bytes(b'original delivery')
  old_sha=hashlib.sha256(old.read_bytes()).hexdigest()
  rows=[{'adId':'cached','kind':'image','url':self.origin+'/cached.png','pageNumber':1,'cardIndex':1,'state':'complete','firstDiscovered':'2026-11-02','filename':name,'bytes':len(PNG),'sha256':hashlib.sha256(PNG).hexdigest()},{'adId':'retry','kind':'image','url':self.origin+'/retry.png','pageNumber':1,'cardIndex':2,'state':'failed','reason':'ConnectionError','firstDiscovered':'2026-11-02'}]
  write(work/'tool.json',{'date':'2026-11-02','preset':PRESETS[1],'phase':'complete','rows':rows,'sample_limit':None,'archive':{'path':str(old),'sha256':old_sha,'bytes':old.stat().st_size}})
  (private/'.gdd-direct-root').touch()
  self.assertEqual(main(['retry-failed','--date','2026-11-02','--only','tool','--state-dir',str(private),'--output',str(self.root/'output')]),0)
  result=json.loads((work/'tool.json').read_text());self.assertEqual(len(self.cookies),1);self.assertEqual(old.read_bytes(),b'original delivery');self.assertEqual(len(result['archive_history']),1);self.assertEqual(result['download_revision'],1);self.assertTrue(all(r['state']=='complete' for r in result['rows']));self.assertEqual(result['rows'][1]['download_attempts'][0]['reason'],'ConnectionError');self.assertTrue(Path(result['archive']['path']).name.endswith('_retry1.zip'))
 def test_partial_zip_is_not_full_day_success_and_returns_failure(self):
  private=self.root/'private';work=private/'runs/2026-11-02_tool';work.mkdir(parents=True);(private/'.gdd-direct-root').touch()
  rows=[{'adId':name,'kind':'image','url':self.origin+'/'+name+'.png','pageNumber':1,'cardIndex':index,'state':'pending','firstDiscovered':'2026-11-02'} for index,name in enumerate(['good','bad'],1)]
  write(work/'tool.json',{'date':'2026-11-02','preset':PRESETS[1],'phase':'addresses-saved','stop_reason':'date-cutoff','rows':rows,'sample_limit':None})
  actual=download
  def one_failure(row,directory):return {**row,'state':'failed','reason':'HTTP 404'} if row['adId']=='bad' else actual(row,directory)
  with patch('ua_workflows.guangdada_direct.download',side_effect=one_failure):code=main(['download-collected','--date','2026-11-02','--only','tool','--state-dir',str(private),'--output',str(self.root/'output')])
  summary=json.loads((self.root/'output/2026-11-02_tool/summary.json').read_text());self.assertEqual(code,2);self.assertTrue(summary['complete']);self.assertFalse(summary['all_media_downloaded']);self.assertFalse(summary['full_day_complete']);self.assertEqual(summary['media_failed'],1)
 def test_real_browser_without_extension_collects_then_downloads_after_close(self):
  from playwright.sync_api import sync_playwright
  state={'date':'2026-11-02','preset':PRESETS[0],'phase':'collecting','rows':[]};path=self.root/'state.json'
  with sync_playwright() as p:
   c=p.chromium.launch_persistent_context(str(self.root/'profile'),channel='chromium',headless=True,args=['--disable-extensions'])
   c.route('https://www.guangdada.net/**',lambda route:route.fulfill(content_type='text/html',body=HTML.replace('ORIGIN',self.origin)))
   page=c.new_page();page.goto('https://www.guangdada.net/modules/creative/display-ads');reader=PageReader(page)
   self.assertFalse(any(w.url.startswith('chrome-extension:') for w in c.service_workers))
   evidence=self.root/'evidence';evidence.mkdir();proof=PresetUI(page,reader,0,timeout=6).prepare(PRESETS[0],'2026-11-02',evidence)
   self.assertEqual(proof['date_start'],'2026-11-02');state['context']=reader.call('context');collect(reader,state,path,'2026-11-02');c.close()
  self.assertEqual(state['stop_reason'],'date-cutoff');self.assertEqual(len(state['completed_pages']),1)
  transfer(state,path,self.root/'media',self.root/'out');self.assertEqual(state['phase'],'complete');self.assertEqual(self.cookies,[None,None])
  with zipfile.ZipFile(state['archive']['path']) as z:
   self.assertIsNone(z.testzip());self.assertEqual(len(z.namelist()),3)
   for n in z.namelist():
    if not n.endswith('.md'):self.assertEqual(z.read(n),PNG)
   text=z.read('下载报告.md').decode();self.assertIn('Example App',text);self.assertNotIn('SECRET',text)
  self.assertEqual(state['rows'][2]['state'],'dateExcluded')
  before=len(self.cookies);transfer(state,path,self.root/'media',self.root/'out');self.assertEqual(len(self.cookies),before)
 def test_invalid_ad_id_not_used_as_path(self):
  r=download({'url':self.origin+'/x.png','kind':'image','adId':'../escape','pageNumber':1,'cardIndex':1},self.root)
  self.assertEqual(r['state'],'failed');self.assertFalse((self.root.parent/'escape.png').exists())
 def test_missing_urls_reported_without_empty_zip(self):
  state={'date':'2026-11-02','preset':PRESETS[0],'phase':'addresses-saved','rows':[{'adId':'missing','kind':'video','pageNumber':1,'cardIndex':1,'state':'missing','reason':'缺少原地址'}]}
  transfer(state,self.root/'state.json',self.root/'media',self.root/'out');self.assertEqual(state['phase'],'no-media');self.assertEqual(list((self.root/'out').glob('*.zip')),[])
 def test_playable_preserves_html_without_executing_or_fetching_dependencies(self):
  row={'adId':'play','kind':'playable','url':self.origin+'/playable.html','pageNumber':1,'cardIndex':1}
  with patch('ua_workflows.guangdada_direct.subprocess.run',side_effect=AssertionError('HTML must not execute')):result=download(row,self.root/'media')
  self.assertEqual(result['state'],'complete');self.assertIn(b'dependency.example',(self.root/'media'/result['filename']).read_bytes());self.assertEqual(len(self.cookies),1)
 def test_failed_media_does_not_create_successful_empty_zip(self):
  row={'adId':'bad','kind':'image','url':'file:///private','pageNumber':1,'cardIndex':1,'state':'pending'}
  state={'date':'2026-11-02','preset':PRESETS[0],'phase':'addresses-saved','rows':[row]}
  transfer(state,self.root/'state.json',self.root/'media',self.root/'out');self.assertEqual(state['phase'],'failed');self.assertFalse(state.get('archive'))
 def test_pack_error_retains_recoverable_packing_state(self):
  row={'adId':'image','kind':'image','url':self.origin+'/x.png','pageNumber':1,'cardIndex':1,'state':'pending'}
  state={'date':'2026-11-02','preset':PRESETS[0],'phase':'addresses-saved','rows':[row]}
  with patch('ua_workflows.guangdada_direct.zipfile.ZipFile',side_effect=OSError('disk write fail')):
   with self.assertRaises(OSError):transfer(state,self.root/'state.json',self.root/'media',self.root/'out')
  self.assertEqual(json.loads((self.root/'state.json').read_text())['phase'],'packing');self.assertFalse(state.get('archive'))
class InstallerTests(unittest.TestCase):
 def check_layout(self,bundled):
  source=Path(__file__).resolve().parents[1]/'apps/guangdada-direct/install.sh'
  with tempfile.TemporaryDirectory() as tmp:
   base=Path(tmp);root=base/'project with spaces';(root/'ua_workflows').mkdir(parents=True)
   directory=root if bundled else root/'apps/guangdada-direct';directory.mkdir(parents=True,exist_ok=True)
   installer=directory/'install.sh';installer.write_bytes(source.read_bytes());requirements=directory/'requirements-direct.txt';requirements.write_text('fixture-only\n')
   python=base/'fake-python';python.write_text('''#!/bin/sh
printf '%s|%s\\n' "$PWD" "$*" >> "$GDD_INSTALL_LOG"
if [ "$1" = "-m" ] && [ "$2" = "venv" ]; then
 mkdir -p .venv/bin
 cp "$0" .venv/bin/python
fi
exit 0
''');python.chmod(0o755);log=base/'calls.log'
   subprocess.run(['sh',str(installer)],cwd=base,env={**os.environ,'GDD_PYTHON':str(python),'GDD_INSTALL_LOG':str(log)},check=True,capture_output=True)
   calls=log.read_text();self.assertIn(str(root)+'|-m venv .venv',calls);self.assertIn('-m pip install -r '+str(requirements),calls);self.assertIn(str(root)+'|-m playwright install chromium',calls);self.assertFalse((base/'.venv').exists())
 def test_repository_layout_installs_at_project_root(self):self.check_layout(False)
 def test_bundle_layout_installs_at_bundle_root(self):self.check_layout(True)

if __name__=='__main__':unittest.main()
