"""Independent browser/CDN collector. Never loads or calls a Chrome extension."""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor,as_completed
import contextlib
import datetime as dt
import fcntl
import hashlib
import html
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlparse
import zipfile
import requests
from .guangdada_headless_ui import PRESETS,AVAILABLE_PRESETS,selected_presets,preset_scope_suffix,PresetUI,PreparationError,unique_visible
from .shared.config import DATA_DIR, STATE_ROOT, PROJECT_ROOT, project_env_path

ROOT=Path(__file__).resolve().parents[1]
SITE='https://www.guangdada.net/modules/creative/display-ads'
READERS=ROOT/'apps/guangdada-direct/browser-readers.js'

class Paused(RuntimeError):pass

def ffprobe_binary():
 found=shutil.which('ffprobe')
 if found:return found
 for value in ['/opt/homebrew/bin/ffprobe','/usr/local/bin/ffprobe']:
  if Path(value).is_file():return value
 raise Paused('需要先安装ffprobe/FFmpeg，未开始采集')

def write(path,data):
 path.parent.mkdir(parents=True,exist_ok=True,mode=0o700);tmp=path.with_name(path.name+'.tmp')
 with tmp.open('w') as f:
  os.chmod(tmp,0o600);json.dump(data,f,ensure_ascii=False,indent=2);f.flush();os.fsync(f.fileno())
 tmp.replace(path)

def sha(path):
 h=hashlib.sha256()
 with path.open('rb') as f:
  for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
 return h.hexdigest()

@contextlib.contextmanager
def profile_lock(root):
 if root.exists() and any(root.iterdir()) and not any((root/n).exists() for n in ['.gdd-direct-root','.gdd-headless-root']):raise Paused('拒绝使用非专用浏览器目录')
 root.mkdir(parents=True,exist_ok=True,mode=0o700)
 (root/'.gdd-direct-root').touch(mode=0o600,exist_ok=True)
 with (root/'direct.lock').open('a') as f,(root/'runner.lock').open('a') as legacy:
  try:
   fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB);fcntl.flock(legacy,fcntl.LOCK_EX|fcntl.LOCK_NB)
  except BlockingIOError:raise Paused('本专用配置已有独立采集进程')
  yield

class PageReader:
 def __init__(self,page):self.page=page;self.source=READERS.read_text()
 def call(self,method,arg=None,args=None):
  # No extension permissions, runtime messaging, background worker or offscreen.
  result=self.page.evaluate(self.source,{'method':method,'args':args or []})
  if result and result.get('error'):raise Paused(result['error'])
  return result
 def safe(self):
  a=self.call('access')
  if a.get('blocked'):raise Paused(a['reason'])
 def wait_page(self,expected=None,after=None,timeout=60):
  end=time.monotonic()+timeout;key=None;since=0
  while time.monotonic()<end:
   self.safe()
   try:
    meta=self.call('pagination');s=self.call('scan');ids=s['cardKeys'];finger=(s['pageNumber'],tuple(ids))
    ready=not meta['loading'] and ids and all(ids) and s['newestFirst']
    if expected:ready=ready and s['pageNumber']==expected['pageNumber'] and ids==expected['cardKeys']
    if after:ready=ready and s['pageNumber']==after['pageNumber']+1 and ids!=after['cardKeys']
    if not ready:key=None;since=0
    elif finger!=key:key=finger;since=time.monotonic()
    elif time.monotonic()-since>=2:return s
   except Paused:key=None;since=0
   except Exception:key=None;since=0
   self.page.wait_for_timeout(200)
  raise Paused('页面没有在预算内稳定；已保存地址保留，未跳页或重复点击')
 def detail(self,row,read_date=False,checkpoint=None):
  expected={'cardIndex':row['cardIndex'],'adId':row['adId'],'readDate':read_date};opened=False;result={}
  try:
   self.safe();result=self.call('detail',args=['open',expected]);opened=bool(result.get('opened'));end=time.monotonic()+10
   while not result.get('failure') and time.monotonic()<end:
    if (result.get('firstDiscovered') if read_date else result.get('url')):break
    self.safe();self.page.wait_for_timeout(200);result=self.call('detail',args=['read',expected])
   if result.get('adId') not in [None,row['adId']]:raise Paused('详情广告身份不符')
   if result.get('adId')==row['adId']:
    if read_date and result.get('firstDiscovered'):
     try:
      if dt.date.fromisoformat(result['firstDiscovered']).isoformat()==result['firstDiscovered']:row.update(firstDiscovered=result['firstDiscovered'],dateSource=result.get('dateSource'))
     except ValueError:pass
    elif result.get('url'):row.update(url=result['url'],detailSource=True)
    if checkpoint:checkpoint()
   return result
  finally:
   if opened and result.get('failure')!='DETAIL_ID_MISMATCH':
    self.call('detail',args=['close',expected]);end=time.monotonic()+15
    while time.monotonic()<end:
     self.safe();closed=self.call('detail',args=['idle',expected])
     if not closed.get('visible'):break
     if closed.get('adId') not in [None,row['adId']]:raise Paused('关闭期间详情身份变化')
     self.page.wait_for_timeout(200)
    else:raise Paused('详情关闭未确认，断点保留')

def canonical(v):return json.dumps(v,ensure_ascii=False,sort_keys=True)

def collect(reader,state,path,date,max_pages=None,sample_limit=None):
 rows=state.setdefault('rows',[]);completed=state.setdefault('completed_pages',[])
 try:
  if state.get('next_intent'):
   scan=reader.wait_page(after=state['next_intent']);state.pop('next_intent');write(path,state)
  else:scan=reader.wait_page(expected=state.get('pending_page'))
  while True:
   reader.safe()
   if canonical(reader.call('context'))!=canonical(state['context']):raise Paused('原筛选、日期或排序发生变化')
   anchor={'pageNumber':scan['pageNumber'],'cardKeys':scan['cardKeys']}
   if anchor in completed:raise Paused('整页身份已采集，未重复采集')
   state['pending_page']=anchor;write(path,state)
   by_index={r['cardIndex']:r for r in scan['items']+scan['missing']}
   for index,ad_id in enumerate(scan['cardKeys'],1):
    old=next((r for r in rows if r['adId']==ad_id and r['pageNumber']==scan['pageNumber'] and r['cardIndex']==index),None)
    row=old or {**by_index[index],'pageNumber':scan['pageNumber'],'state':'pending'}
    if old is None:rows.append(row)
    observed=scan.get('cardDates',[None]*len(scan['cardKeys']))[index-1]
    if not row.get('firstDiscovered'):
     if not observed:
      observed_detail=reader.detail(row,True,lambda:write(path,state))
      observed={'date':observed_detail.get('firstDiscovered'),'source':observed_detail.get('dateSource')}
     value=(observed or {}).get('date')
     try:valid=dt.date.fromisoformat(value).isoformat()==value
     except (ValueError,TypeError):valid=False
     if not valid:row.update(state='dateUnchecked',reason='未取得可核验首次发现日期');write(path,state);raise Paused('日期未核验')
     row.update(firstDiscovered=value,dateSource=observed.get('source'))
    if row['firstDiscovered']<date:
     row.update(state='dateExcluded',reason='早于日期下限，未下载')
     for later in range(index+1,len(scan['cardKeys'])+1):
      observed_later=(scan.get('cardDates') or [None]*len(scan['cardKeys']))[later-1]
      if not any(r['pageNumber']==scan['pageNumber'] and r['cardIndex']==later for r in rows):rows.append({**by_index[later],'pageNumber':scan['pageNumber'],'firstDiscovered':(observed_later or {}).get('date'),'state':'dateExcluded','reason':'前项已触发日期截止，未继续核验或下载'})
     state.update(phase='addresses-saved',stop_reason='date-cutoff');write(path,state);return
    # Missing fields never erase an already persisted original address.
    recoverable=row.get('reasonCode') not in ['UNSUPPORTED_TYPE','STREAM_MANIFEST','UNSUPPORTED_SCHEME','PLAYABLE_NOT_HTML']
    if not row.get('url') and row.get('kind') and recoverable:
     reader.call('pagination',args=['warm',{**anchor,'cardIndex':index}]);end=time.monotonic()+4
     while time.monotonic()<end:
      reader.safe();fresh=reader.call('scan')
      if fresh['cardKeys']!=anchor['cardKeys']:raise Paused('补读地址时素材身份变化')
      candidate=next((i for i in fresh['items'] if i['adId']==ad_id and i['cardIndex']==index),None)
      if candidate:row.update(candidate);write(path,state);break
      reader.page.wait_for_timeout(200)
    if not row.get('url') and row.get('kind')=='video' and recoverable:
     found=reader.detail(row,checkpoint=lambda:write(path,state))
     if found.get('url'):row.update(url=found['url'],detailSource=True)
     else:row.update(state='missing',reason='列表及对应详情未提供可确认原视频地址')
    if not row.get('url'):row.update(state='missing',reason=row.get('reason','未取得原地址'))
    write(path,state)  # every URL is durable before moving to the next card
    if sample_limit and sum(bool(r.get('url')) for r in rows)>=sample_limit:
     state.update(phase='addresses-saved',stop_reason='sample-limit');write(path,state);return
   completed.append(anchor);state.pop('pending_page',None);write(path,state)
   meta=reader.call('pagination',args=['read',anchor])
   if not meta['hasNext'] or max_pages and len(completed)>=max_pages:
    state.update(phase='addresses-saved',stop_reason='last-page' if not meta['hasNext'] else 'page-limit');write(path,state);return
   state['next_intent']=anchor;write(path,state)
   # Persist the uncertain action BEFORE clicking, and never replay it.
   reader.safe();result=reader.call('pagination',args=['next',anchor])
   if not result.get('clicked'):raise Paused('翻页未确认，未自动重复点击')
   scan=reader.wait_page(after=anchor);state.pop('next_intent');write(path,state)
 except BaseException as e:
  state['phase']='paused';state['pause_reason']=str(e).split('https://')[0][:300];write(path,state);raise


def good_url(url):
 u=urlparse(url)
 if u.scheme not in ['https','http'] or not u.hostname or u.username or u.password:raise ValueError('原地址格式或协议不支持')
 if re.search(r'\.(m3u8|mpd|ts)$',u.path,re.I):raise ValueError('HLS/DASH分片尚不支持合并')
 return u

def download_youtube(row,directory):
 # Public videos only. Never read browser cookies, login credentials or CLI
 # configuration; an authentication/verification refusal is a real failure.
 with tempfile.TemporaryDirectory(prefix='youtube-',dir=directory) as temporary:
  command=[sys.executable,'-m','yt_dlp','--ignore-config','--no-playlist','--no-cache-dir','--socket-timeout','20','--retries','2','--fragment-retries','2','--no-warnings','--no-progress','--quiet','--format','bestvideo[ext=mp4][vcodec^=avc1][protocol=https]+bestaudio[ext=m4a][protocol=https]/best[ext=mp4][protocol=https]/bestvideo[protocol=https]+bestaudio[protocol=https]/best[protocol=https]','--merge-output-format','mp4/mkv','--ffmpeg-location',str(Path(ffprobe_binary()).parent),'--output',str(Path(temporary)/'media.%(ext)s'),'--',row['url']]
  process=subprocess.run(command,capture_output=True,timeout=600)
  if process.returncode:
   message=process.stderr.decode('utf-8','replace')
   reason=next((line for line in reversed(message.splitlines()) if 'ERROR:' in line),'YouTube下载器不可用或下载失败')
   raise ValueError(re.sub(r'https?://\S+','[URL]',reason)[:250])
  candidates=[p for p in Path(temporary).iterdir() if p.is_file() and p.suffix.lower() in ['.mkv','.mp4','.webm','.mov'] and not re.search(r'\.f\d+\.',p.name)]
  if len(candidates)!=1:raise ValueError('YouTube下载未得到唯一完整媒体文件')
  source=candidates[0]
  probe=subprocess.run([ffprobe_binary(),'-v','error','-show_entries','stream=codec_type','-of','json',str(source)],capture_output=True,timeout=30)
  streams=json.loads(probe.stdout or '{}').get('streams',[])
  if probe.returncode or not source.stat().st_size or not any(s.get('codec_type')=='video' for s in streams):raise ValueError('YouTube原视频流校验失败')
  name=f"第{row['pageNumber']:04d}页/video/{row['cardIndex']:04d}_{row['adId']}{source.suffix.lower()}"
  dest=directory/name;dest.parent.mkdir(parents=True,exist_ok=True);source.replace(dest)
  return {**row,'state':'complete','filename':name,'bytes':dest.stat().st_size,'sha256':sha(dest),'reason':'','resolver':'yt-dlp-public-no-cookies'}

def download(row,directory,depth=0):
 try:
  url=row['url'];u=good_url(url);kind=row['kind']
  if not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',row['adId']):raise ValueError('广告ID不能用于安全文件命名')
  if depth>2:raise ValueError('外链静态解析超过预算')
  if kind not in ['image','video','playable']:raise ValueError('素材类型不支持')
  if row.get('state')=='complete' and row.get('filename'):
   cached=(directory/row['filename']).resolve()
   if cached.is_relative_to(directory.resolve()) and cached.is_file() and sha(cached)==row.get('sha256'):return row
  if kind=='video' and u.hostname.lower() in ['youtube.com','www.youtube.com','m.youtube.com','youtu.be','www.youtu.be','youtube-nocookie.com','www.youtube-nocookie.com']:
   directory.mkdir(parents=True,exist_ok=True)
   return download_youtube(row,directory)
  suffix=Path(u.path).suffix.lower();suffix=suffix if re.fullmatch(r'\.[a-zA-Z0-9]{1,5}',suffix or '') else {'image':'.img','video':'.mp4','playable':'.html'}[kind]
  if kind=='playable':suffix='.html'
  name=f"第{row['pageNumber']:04d}页/{kind}/{row['cardIndex']:04d}_{row['adId']}{suffix}"
  dest=directory/name;dest.parent.mkdir(parents=True,exist_ok=True)
  if row.get('state')=='complete' and dest.exists() and sha(dest)==row.get('sha256'):return {**row,'filename':name}
  part=dest.with_name(dest.name+'.part');started=time.monotonic()
  with requests.get(url,stream=True,timeout=(20,30),headers={'Accept-Encoding':'identity'}) as response:
   response.raise_for_status();ctype=response.headers.get('Content-Type','').lower()
   # External pages are parsed statically; never execute or fetch dependencies.
   if kind=='video' and 'text/html' in ctype:
    chunks=[];size=0
    for b in response.iter_content(65536):
     size+=len(b)
     if size>8*1024*1024:raise ValueError('外链静态页面超过8MiB')
     chunks.append(b)
    text=b''.join(chunks).decode('utf-8','replace')
    urls=re.findall(r'(?:contentUrl|og:video(?::url)?|src)["\s:=]+(https?[^"<>\s]+)',text,re.I)
    direct=next((html.unescape(x).replace('\\/','/') for x in urls if re.search(r'\.(mp4|webm|mov)(?:[?]|$)',x,re.I)),None)
    if not direct:raise ValueError('外链没有可静态确认的完整视频直链，分离流/验证不支持')
    return download({**row,'url':direct},directory,depth+1)
   with part.open('wb') as f:
    for b in response.iter_content(1024*1024):
     if time.monotonic()-started>120:raise ValueError('媒体传输超过120秒预算')
     f.write(b)
   expected=response.headers.get('Content-Length')
   if expected and not response.headers.get('Content-Encoding') and part.stat().st_size!=int(expected):raise ValueError('媒体长度校验不符')
  if not part.stat().st_size:raise ValueError('媒体为空')
  if kind=='playable':
   with part.open('rb') as f:prefix=f.read(65536).lower()
   if b'<html' not in prefix and b'<script' not in prefix and b'<!doctype' not in prefix:raise ValueError('Playable不是可确认的HTML')
  else:
   probe=subprocess.run([ffprobe_binary(),'-v','error','-show_entries','stream=codec_type','-of','json',str(part)],capture_output=True,timeout=30)
   streams=json.loads(probe.stdout or '{}').get('streams',[])
   if probe.returncode or not any(s.get('codec_type')=='video' for s in streams):raise ValueError('原媒体格式或流校验失败')
  part.replace(dest)
  return {**row,'state':'complete','filename':name,'bytes':dest.stat().st_size,'sha256':sha(dest),'reason':''}
 except Exception as e:
  if 'part' in locals():
   try:part.unlink(missing_ok=True)
   except OSError:pass
  reason=str(e) if isinstance(e,ValueError) else f'HTTP {e.response.status_code}' if isinstance(e,requests.HTTPError) and e.response is not None else type(e).__name__
  return {**row,'state':'failed','reason':re.sub(r'https?://\S+','[URL]',reason)[:250]}


def md(value):return str(value if value is not None else '未知').replace('|','&#124;').replace('\n',' ')
def report(state):
 rows=state.get('rows',[])
 out=['# 下载报告',f"- 采集方式：独立浏览器批量采集（不加载扩展）",f"- 目标日期：{state['date']}",f"- 预设：{state['preset']['category']} / {state['preset']['preset']}",f"- 状态：{state['phase']}",f"- 结束原因：{state.get('stop_reason',state.get('pause_reason','尚未结束'))}",'','| 选择位置 | 广告 ID | 类型 | 首次发现日期 | 结果 | 文件 | 原因 | 建议 |','|---|---|---|---|---|---|---|---|']
 names={'image':'图片','video':'视频','playable':'Playable 试玩'}
 results={'complete':'成功','failed':'请求失败','missing':'缺少原地址','dateExcluded':'日期未纳入','dateUnchecked':'日期未核验','pending':'未发起','duplicate':'重复合并'}
 for r in rows:out.append('| '+' | '.join(md(x) for x in [f"第 {r['pageNumber']} 页 · 第 {r['cardIndex']} 个",r['adId'],names.get(r.get('kind'),'未知'),r.get('firstDiscovered'),results.get(r.get('state'),'未发起'),r.get('filename','—'),r.get('reason',''),'Playable外部依赖未打包' if r.get('kind')=='playable' else ''])+' |')
 out+=['','## 素材信息','| 选择位置 | 广告 ID | App 名称 | App ID | 平台 | 展示估值 | 重投标签 |','|---|---|---|---|---|---|---|']
 for r in rows:
  fields=(r.get('metadata') or {}).get('fields',{})
  vals=[]
  for k in ['appName','appId','platform','displayEstimate','relaunch']:
   v=fields.get(k);vals.append(v.get('raw',v.get('value')) if v else '未知')
  out.append('| '+' | '.join(md(x) for x in [f"第 {r['pageNumber']} 页 · 第 {r['cardIndex']} 个",r['adId'],*vals])+' |')
 return '\n'.join(out)+'\n'

def transfer(state,path,directory,output):
 state['phase']='downloading';write(path,state);rows=state['rows'];seen={};targets=[]
 for i,r in enumerate(rows):
  if not r.get('url') or r.get('state') in ['dateExcluded','dateUnchecked']:continue
  if r['url'] in seen:r.update(state='duplicate',reason='与前项相同原地址合并');continue
  seen[r['url']]=i;targets.append((i,r))
 with ThreadPoolExecutor(max_workers=3) as pool:
  work={pool.submit(download,r,directory):i for i,r in targets}
  for future in as_completed(work):rows[work[future]]=future.result();write(path,state)
 for r in rows:
  if r.get('state')=='duplicate':
   original=rows[seen[r['url']]]
   r.update(duplicateOf=original['adId'],filename=original.get('filename',''),reason='与广告 '+original['adId']+' 相同原地址合并；原项'+('下载成功' if original.get('state')=='complete' else '尚未下载成功'))
 files=[r for r in rows if r.get('state')=='complete']
 state['phase']='packing' if files else 'failed' if targets else 'no-media';write(path,state)
 text=report({**state,'phase':'complete'} if files else state);output.mkdir(parents=True,exist_ok=True);(output/'下载报告.md').write_text(report(state))
 if not files:write(path,state);return
 revision=state.get('download_revision',0)
 archive=output/(state['preset']['key']+'_'+state['date']+(f'_retry{revision}' if revision else '')+'.zip');temporary=archive.with_name(archive.name+'.part')
 with zipfile.ZipFile(temporary,'w',compression=zipfile.ZIP_STORED,allowZip64=True) as z:
  for r in files:z.write(directory/r['filename'],r['filename'])
  z.writestr('下载报告.md',text)
 with zipfile.ZipFile(temporary) as z:
  if z.testzip() is not None:raise RuntimeError('ZIP CRC校验失败')
 if archive.exists() and sha(archive)!=sha(temporary):raise Paused('已有不同交付包，未覆盖')
 temporary.replace(archive);state['phase']='complete';state['archive']={'path':str(archive),'sha256':sha(archive),'bytes':archive.stat().st_size};write(path,state);(output/'下载报告.md').write_text(text)


def ensure_login(page,credentials):
 page.goto(SITE,wait_until='domcontentloaded',timeout=90000)
 end=time.monotonic()+15
 while time.monotonic()<end:
  password=unique_visible(page.locator('input[type=password]'),'登录密码框',True)
  if password:break
  if any(n.is_visible() for n in page.get_by_text('游戏',exact=True).all()):return
  page.wait_for_timeout(200)
 else:
  page.goto('https://www.guangdada.net/user/login',wait_until='domcontentloaded',timeout=90000)
  end=time.monotonic()+45;password=None
  while time.monotonic()<end:
   password=unique_visible(page.locator('input[type=password]'),'登录密码框',True)
   if password:break
   page.wait_for_timeout(200)
  if password is None:raise Paused('正常登录入口没有就绪，未重复提交凭证')
 if not credentials or not credentials.exists():raise Paused('需要配置现有项目凭证路径；未请求浏览器扩展授权')
 from dotenv import dotenv_values
 values=dotenv_values(credentials);email=values.get('GUANGDADA_EMAIL');secret=values.get('GUANGDADA_PASSWORD')
 if not email or not secret:raise Paused('已有配置缺少广大大账号字段')
 field=unique_visible(page.locator('input[type=email],input[placeholder*=邮箱]'),'登录邮箱')
 button=unique_visible(page.get_by_role('button',name=re.compile(r'^登\s*录$')),'登录按钮')
 if urlparse(page.url).hostname not in ['guangdada.net','www.guangdada.net']:raise Paused('登录目的站点不符')
 try:field.fill(email);password.fill(secret);button.click()
 except Exception:raise Paused('登录表单操作失败，未输出凭证') from None
 finally:del values,email,secret
 end=time.monotonic()+60
 while time.monotonic()<end:
  dialogs=page.locator('[role=dialog],.ant-modal-content')
  if any(n.is_visible() and re.search('验证码|安全验证',n.inner_text()) for n in dialogs.all()):raise Paused('需要人工验证，未自动处理')
  if not password.is_visible():
   page.goto(SITE,wait_until='domcontentloaded',timeout=90000);return
  page.wait_for_timeout(200)
 raise Paused('正常登录一次后仍未确认成功，未重复提交')


def date_arg(value):
 try:
  if dt.date.fromisoformat(value).isoformat()!=value:raise ValueError()
 except ValueError:raise argparse.ArgumentTypeError('需要有效YYYY-MM-DD')
 return value


def main(argv=None):
 p=argparse.ArgumentParser(description='独立广大大批量采集：无扩展、无扩展权限、先保存地址再3路CDN下载')
 p.add_argument('command',choices=['run','resume','download-collected','retry-failed'])
 p.add_argument('--date',required=True,type=date_arg)
 p.add_argument('--state-dir',type=Path,default=DATA_DIR/'afizzy_daily/collector-state' if STATE_ROOT!=PROJECT_ROOT else Path.home()/'.local/share/gdd-direct')
 p.add_argument('--output',type=Path,default=DATA_DIR/'afizzy_daily/raw' if STATE_ROOT!=PROJECT_ROOT else Path.home()/'Downloads/广大大独立采集')
 p.add_argument('--credentials-env',type=Path,default=project_env_path())
 range_args=p.add_mutually_exclusive_group()
 range_args.add_argument('--only',choices=[p['key'] for p in AVAILABLE_PRESETS])
 range_args.add_argument('--presets',nargs='+',choices=[p['key'] for p in AVAILABLE_PRESETS])
 p.add_argument('--sample-per-preset',type=int,help='显式小样测试数量，不冒充整日完成')
 p.add_argument('--max-pages',type=int)
 p.add_argument('--selectors',type=Path)
 p.add_argument('--headed',action='store_true')
 a=p.parse_args(argv)
 if a.sample_per_preset is not None and a.sample_per_preset<1:p.error('小样数量必须为正数')
 if a.max_pages is not None and a.max_pages<1:p.error('页数必须为正数')
 from playwright.sync_api import sync_playwright
 ffprobe_binary()
 scope=selected_presets([a.only] if a.only else a.presets)
 suffix=preset_scope_suffix(scope)+(f'_sample{a.sample_per_preset}' if a.sample_per_preset else '')
 private=a.state_dir.expanduser().resolve();work=private/'runs'/f'{a.date}_{suffix}';output=a.output.expanduser().resolve()/f'{a.date}_{suffix}'
 states=[];attention=[]
 with profile_lock(private):
  work.mkdir(parents=True,exist_ok=True,mode=0o700);output.mkdir(parents=True,exist_ok=True)
  for preset in scope:
   path=work/(preset['key']+'.json')
   if path.exists():
    state=json.loads(path.read_text())
    if state['date']!=a.date or state['preset']!=preset or state.get('sample_limit')!=a.sample_per_preset:raise Paused('断点日期/范围不同')
   else:
    state={'schema':1,'date':a.date,'preset':preset,'phase':'not-started','rows':[],'sample_limit':a.sample_per_preset};write(path,state)
   states.append((state,path))
  if a.command not in ['download-collected','retry-failed'] and any(s['phase'] not in ['addresses-saved','complete','downloading','packing','no-media'] for s,_ in states):
   with sync_playwright() as engine:
    browser=engine.chromium.launch_persistent_context(str(private/'profile'),channel='chromium',headless=not a.headed,args=['--disable-extensions'])
    try:
     if any(w.url.startswith('chrome-extension:') for w in browser.service_workers):raise Paused('发现扩展运行环境，未继续')
     page=browser.new_page();print(json.dumps({'stage':'checking-normal-login','extension_used':False}),flush=True)
     try:ensure_login(page,a.credentials_env)
     except BaseException:
      page.screenshot(path=str(output/'login-attention.png'),mask=[page.locator('input')],full_page=False)
      raise
     reader=PageReader(page)
     write(work/'browser-mode.json',{'extension_loaded':False,'extension_runtime_called':False,'headless':not a.headed,'browser_version':browser.browser.version})
     for state,path in states:
      if state['phase'] in ['addresses-saved','complete','downloading','packing','no-media']:continue
      if state['phase']=='paused' and a.command!='resume':attention.append(state['preset']['key']+'需要resume核对原断点');continue
      evidence=output/state['preset']['key'];evidence.mkdir(exist_ok=True)
      try:
       print(json.dumps({'stage':'prepare','preset':state['preset']['preset'],'date':a.date},ensure_ascii=False),flush=True)
       proof=PresetUI(page,reader,0,json.loads(a.selectors.read_text()) if a.selectors else None,timeout=60).prepare(state['preset'],a.date,evidence)
       context=reader.call('context')
       if state.get('context') and canonical(context)!=canonical(state['context']):raise Paused('恢复条件与原断点不符')
       if state.get('pending_page') or state.get('next_intent'):
        target=(state.get('next_intent') or state['pending_page'])['pageNumber']+(1 if state.get('next_intent') else 0)
        if target!=proof['pageNumber']:
         jumper=unique_visible(page.locator('.ant-pagination-options-quick-jumper input'),'恢复页码输入框')
         reader.safe();jumper.fill(str(target));jumper.press('Enter')
       state['context']=context;state['phase']='collecting';write(path,state)
       collect(reader,state,path,a.date,a.max_pages,a.sample_per_preset)
       print(json.dumps({'stage':'addresses-saved','preset':state['preset']['preset'],'addresses':sum(bool(r.get('url')) for r in state['rows']),'reason':state.get('stop_reason')},ensure_ascii=False),flush=True)
      except BaseException as error:
       state.update(phase='paused',pause_reason=type(error).__name__+': '+str(error).split('https://')[0][:180]);write(path,state)
       (evidence/'下载报告.md').write_text(report(state));attention.append(state['preset']['key']+': '+state['pause_reason'])
       try:page.screenshot(path=str(evidence/'paused.png'),full_page=False)
       except Exception:pass
       break
    finally:browser.close()
  # Downloading has no browser/page/login dependency, and can be repeated from
  # the durable URL ledger without re-reading Guangdada.
  for state,path in states:
   if a.command=='retry-failed':
    failures=[r for r in state['rows'] if r.get('state')=='failed']
    if not failures:continue
    if state.get('archive'):state.setdefault('archive_history',[]).append(state.pop('archive'))
    state['download_revision']=state.get('download_revision',0)+1
    for r in failures:r.setdefault('download_attempts',[]).append({'state':r['state'],'reason':r.get('reason'),'at':dt.datetime.now(dt.timezone.utc).isoformat()})
    state['phase']='addresses-saved';write(path,state)
   if state.get('archive'):
    if Path(state['archive']['path']).exists() and sha(Path(state['archive']['path']))==state['archive']['sha256']:continue
    raise Paused('原交付ZIP缺失或变化，未覆盖')
   if state['phase'] in ['addresses-saved','downloading','packing','no-media'] or a.command=='download-collected' and state['rows']:
    transfer(state,path,work/(state['preset']['key']+'_files'),output/state['preset']['key'])
    print(json.dumps({'stage':'download-finished','preset':state['preset']['preset'],'success':sum(r.get('state')=='complete' for r in state['rows']),'failed':sum(r.get('state')=='failed' for r in state['rows']),'archive':state.get('archive')},ensure_ascii=False),flush=True)
  complete=not attention and all(s['phase'] in ['complete','no-media'] for s,_ in states)
  all_media=all(r.get('state') in ['complete','dateExcluded'] or r.get('state')=='duplicate' and r.get('filename') for s,_ in states for r in s['rows'])
  collection_complete=complete and not a.sample_per_preset and not a.max_pages and all(s.get('stop_reason') in ['date-cutoff','last-page'] for s,_ in states)
  summary={'date':a.date,'scope':[s['preset']['key'] for s,_ in states],'sample_limit':a.sample_per_preset,'complete':complete,'collection_complete':collection_complete,'all_media_downloaded':all_media,'full_day_complete':collection_complete and all_media,'media_failed':sum(r.get('state')=='failed' for s,_ in states for r in s['rows']),'attention':attention,'output':str(output),'extension_used':False}
  write(output/'summary.json',summary);print(json.dumps(summary,ensure_ascii=False),flush=True)
  return 0 if summary['complete'] and all_media else 2

if __name__=='__main__':
 try:raise SystemExit(main())
 except (Paused,PreparationError) as error:print('采集已暂停：'+str(error));raise SystemExit(2)
