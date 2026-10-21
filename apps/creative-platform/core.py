"""Local team creative collection portal. No browser driver or GDD private API."""
from __future__ import annotations
import base64,contextlib,hashlib,hmac,json,mimetypes,os,re,secrets,shutil,sqlite3,time,uuid
from pathlib import Path

STAGES=['queued','preparing','waiting_login','waiting_captcha','configuring','collecting','downloading','reviewing','packaging','completed','failed','cancelled']
ACTIVE=set(STAGES)-{'queued','completed','failed','cancelled'}
TRANSITIONS={
 'queued':{'preparing','cancelled'},'preparing':{'waiting_login','configuring','failed','cancelled'},
 'waiting_login':{'configuring','failed','cancelled'},'waiting_captcha':{'configuring','collecting','failed','cancelled'},
 'configuring':{'collecting','waiting_login','waiting_captcha','failed','cancelled'},
 'collecting':{'downloading','waiting_login','waiting_captcha','failed','cancelled'},
 'downloading':{'reviewing','failed','cancelled'},'reviewing':{'packaging','failed','cancelled'},
 'packaging':{'failed','cancelled'},'failed':{'queued'},'cancelled':set(),'completed':set()}
class Problem(Exception):
 def __init__(self,message,status=400):self.message=message;self.status=status

def pw_hash(p,salt=None):
 salt=salt or secrets.token_hex(16)
 return salt+':'+hashlib.scrypt(p.encode(),salt=salt.encode(),n=16384,r=8,p=1).hex()
def pw_valid(p,stored):return hmac.compare_digest(pw_hash(p,stored.split(':')[0]),stored)
def digest(s):return hashlib.sha256(s.encode()).hexdigest()
def file_hash(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for chunk in iter(lambda:f.read(1048576),b''):h.update(chunk)
 return h.hexdigest()
def safe_file(root,value):
 p=(root/value).resolve()
 if not p.is_relative_to(root.resolve()) or not p.is_file():raise Problem('素材路径无效')
 return p

def normalize_scope(x,registry):
 if not isinstance(x,dict):raise Problem('请填写任务条件')
 names=x.get('apps',[])
 if not isinstance(names,list) or not 1<=len(names)<=30:raise Problem('请填写1～30个完整App名称')
 tasks=[]
 for row in names:
  name=str(row.get('app_name','')).strip();platform=row.get('platform')
  if not 2<=len(name)<=180 or platform not in ['iOS','Android']:raise Problem('App名称或平台无效')
  match=next((r for r in registry if r['app_name']==name and r['platform']==platform),None)
  app_id=str(row.get('app_id') or (match or {}).get('app_id','')).strip()
  if len(app_id)>200:raise Problem('App ID过长')
  task={'app_name':name,'platform':platform,'app_id':app_id or None,'identity_status':'registry_match_requires_browser_check' if match else 'requires_browser_check'}
  if any(t['app_name']==name and t['platform']==platform for t in tasks):raise Problem('同一App/平台重复')
  tasks.append(task)
 date=x.get('date',{'preset':'30天'})
 if date.get('preset')=='自定义':
  import datetime
  try:a=datetime.date.fromisoformat(date['start']);b=datetime.date.fromisoformat(date['end'])
  except (ValueError,KeyError,TypeError):raise Problem('请填写正确的自定义起止日期')
  if a>b or (b-a).days>366:raise Problem('日期顺序错误或范围超过366天')
 elif date.get('preset') not in ['7天','30天','90天','1年']:raise Problem('日期选项无效')
 def strings(k,limit=50):
  v=x.get(k,[])
  if not isinstance(v,list) or len(v)>limit or any(not isinstance(s,str) or not 0<len(s)<=80 for s in v):raise Problem(k+'参数无效')
  return list(dict.fromkeys(s.strip() for s in v if s.strip()))
 category=x.get('category','工具');mode=x.get('mode','素材');media=x.get('media','视频');ratio=x.get('ratio','9:16')
 if category not in ['工具','游戏'] or mode not in ['素材','广告'] or media!='视频' or ratio not in ['不限','9:16','16:9','1:1','4:5','3:4']:raise Problem('分类或比例无效；平台v1目前仅支持视频任务')
 content=x.get('content',{})
 if not isinstance(content,dict):raise Problem('内容条件无效')
 from datetime import datetime,timezone,timedelta
 return {'apps':tasks,'category':category,'mode':mode,'media':media,'ratio':ratio,'channels':strings('channels'),'languages':strings('languages'),'regions':strings('regions'),'date':date,'date_semantics':'active_in_window','date_anchor':datetime.now(timezone(timedelta(hours=8))).date().isoformat(),'content':{k:bool(content.get(k)) for k in ['male_character','female_audience','exclude_talking_head']},'selection':'all_matching','source':'广大大'}

class Portal:
 def __init__(self,root,project):
  self.root=Path(root).resolve();self.project=Path(project).resolve();self.root.mkdir(parents=True,exist_ok=True);os.chmod(self.root,0o700)
  self.db_path=self.root/'portal.sqlite3';self.registry=json.loads((self.project/'config/social_chat_browser_app_ids.json').read_text())['items']
  cred=self.root/'credentials.json'
  if not cred.exists():
   value={'admin_username':'admin','admin_password':secrets.token_urlsafe(22),'worker_token':secrets.token_urlsafe(40)}
   fd=os.open(cred,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
   with os.fdopen(fd,'w') as f:json.dump(value,f)
  self.credentials=json.loads(cred.read_text());self.worker_token=self.credentials['worker_token']
  with self.db() as db:
   db.executescript('''CREATE TABLE IF NOT EXISTS users(id TEXT PRIMARY KEY, username TEXT UNIQUE NOT NULL, password TEXT NOT NULL, role TEXT NOT NULL, created REAL NOT NULL);
CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY,user_id TEXT NOT NULL,expires REAL NOT NULL);
CREATE TABLE IF NOT EXISTS invites(token TEXT PRIMARY KEY,remaining INTEGER NOT NULL,expires REAL NOT NULL);
CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY,title TEXT NOT NULL,owner TEXT NOT NULL,scope TEXT NOT NULL,status TEXT NOT NULL,counts TEXT NOT NULL DEFAULT '{}',checkpoint TEXT NOT NULL DEFAULT '{}',error TEXT,lease_until REAL NOT NULL DEFAULT 0,created REAL NOT NULL,updated REAL NOT NULL,import_key TEXT UNIQUE);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT,job_id TEXT NOT NULL,stage TEXT NOT NULL,message TEXT NOT NULL,created REAL NOT NULL);
CREATE TABLE IF NOT EXISTS materials(id TEXT PRIMARY KEY,job_id TEXT NOT NULL,ad_id TEXT NOT NULL,platform TEXT NOT NULL,app_id TEXT NOT NULL,decision TEXT NOT NULL,metadata TEXT NOT NULL,created REAL NOT NULL,UNIQUE(job_id,platform,app_id,ad_id));
CREATE INDEX IF NOT EXISTS materials_job ON materials(job_id);
CREATE TABLE IF NOT EXISTS worker(id INTEGER PRIMARY KEY CHECK(id=1),heartbeat REAL NOT NULL,name TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS login_attempts(ip TEXT NOT NULL,at REAL NOT NULL);''')
   db.execute('INSERT OR IGNORE INTO users VALUES(?,?,?,?,?)',(str(uuid.uuid4()),self.credentials['admin_username'],pw_hash(self.credentials['admin_password']),'admin',time.time()))
 @contextlib.contextmanager
 def db(self):
  conn=sqlite3.connect(self.db_path,timeout=15);conn.row_factory=sqlite3.Row;conn.execute('PRAGMA journal_mode=WAL');conn.execute('PRAGMA foreign_keys=ON')
  try:yield conn;conn.commit()
  except:conn.rollback();raise
  finally:conn.close()
 def user(self,token):
  with self.db() as db:
   r=db.execute('SELECT users.id,username,role FROM sessions JOIN users ON users.id=sessions.user_id WHERE token=? AND expires>?',(digest(token),time.time())).fetchone()
   return dict(r) if r else None
 def login(self,username,password,ip):
  if not isinstance(username,str) or not isinstance(password,str) or len(password)>256:raise Problem('登录信息无效')
  with self.db() as db:
   db.execute('DELETE FROM login_attempts WHERE at<?',(time.time()-300,))
   if db.execute('SELECT COUNT(*) FROM login_attempts WHERE ip=?',(ip,)).fetchone()[0]>=10:raise Problem('登录尝试过多，请5分钟后再试',429)
   r=db.execute('SELECT * FROM users WHERE username=?',(username,)).fetchone()
   if not r or not pw_valid(password,r['password']):
    db.execute('INSERT INTO login_attempts VALUES(?,?)',(ip,time.time()));db.commit();raise Problem('用户名或密码错误',401)
   token=secrets.token_urlsafe(40);db.execute('INSERT INTO sessions VALUES(?,?,?)',(digest(token),r['id'],time.time()+86400*7));db.execute('DELETE FROM login_attempts WHERE ip=?',(ip,))
   return token
 def register(self,x):
  u=x.get('username','').strip();p=x.get('password','');invite=x.get('invite','')
  if not re.fullmatch(r'[A-Za-z0-9_.@-]{3,80}',u) or not 12<=len(p)<=128:raise Problem('用户名需3～80位；密码至少12位')
  with self.db() as db:
   db.execute('BEGIN IMMEDIATE');r=db.execute('SELECT * FROM invites WHERE token=? AND expires>? AND remaining>0',(digest(invite),time.time())).fetchone()
   if not r:raise Problem('邀请码无效或已到期',403)
   try:db.execute('INSERT INTO users VALUES(?,?,?,?,?)',(str(uuid.uuid4()),u,pw_hash(p),'member',time.time()))
   except sqlite3.IntegrityError:raise Problem('用户名已存在',409)
   db.execute('UPDATE invites SET remaining=remaining-1 WHERE token=?',(digest(invite),))
 def invite(self):
  token=secrets.token_urlsafe(22)
  with self.db() as db:db.execute('INSERT INTO invites VALUES(?,?,?)',(digest(token),20,time.time()+86400*7))
  return {'invite':token,'uses':20,'expires_days':7}
 def event(self,db,job,stage,message):db.execute('INSERT INTO events(job_id,stage,message,created) VALUES(?,?,?,?)',(job,stage,message,time.time()))
 def job_dict(self,r):
  d=dict(r)
  for k in ['scope','counts','checkpoint']:d[k]=json.loads(d[k])
  d.pop('lease_until',None);return d
 def create_job(self,x,user,provenance=None):
  scope=normalize_scope(x,self.registry);jid=str(uuid.uuid4());title=str(x.get('title') or '、'.join(a['app_name'] for a in scope['apps']))[:200];now=time.time()
  if provenance:scope['ai_intake']=provenance
  with self.db() as db:
   if db.execute("SELECT COUNT(*) FROM jobs WHERE owner=? AND status='queued'",(user['id'],)).fetchone()[0]>=20:raise Problem('待执行任务已达20个上限',429)
   db.execute('INSERT INTO jobs(id,title,owner,scope,status,created,updated) VALUES(?,?,?,?,?,?,?)',(jid,title,user['id'],json.dumps(scope,ensure_ascii=False),'queued',now,now));self.event(db,jid,'queued','任务已入队，等待浏览器采集端领取。')
  return jid
 def jobs(self):
  with self.db() as db:return [self.job_dict(r) for r in db.execute('SELECT jobs.*,users.username FROM jobs LEFT JOIN users ON owner=users.id ORDER BY created DESC LIMIT 100')]
 def job(self,jid):
  with self.db() as db:
   r=db.execute('SELECT * FROM jobs WHERE id=?',(jid,)).fetchone()
   if not r:raise Problem('任务不存在',404)
   d=self.job_dict(r);d['events']=[dict(e) for e in db.execute('SELECT stage,message,created FROM events WHERE job_id=? ORDER BY id DESC LIMIT 100',(jid,))];return d
 def claim(self,jid=None):
  now=time.time()
  with self.db() as db:
   db.execute('BEGIN IMMEDIATE')
   # An expired lease is not permission to run another account session. Explicit recovery is required.
   active=db.execute("SELECT id FROM jobs WHERE status NOT IN ('queued','completed','failed','cancelled') LIMIT 1").fetchone()
   if active:raise Problem('已有任务占用浏览器账号；请先恢复或结束该任务',409)
   r=db.execute("SELECT * FROM jobs WHERE status='queued' "+('AND id=? ' if jid else '')+'ORDER BY created LIMIT 1',([jid] if jid else [])).fetchone()
   if not r:return None
   db.execute("UPDATE jobs SET status='preparing',lease_until=?,updated=? WHERE id=?",(now+180,now,r['id']));self.event(db,r['id'],'preparing','已领取任务，等待执行浏览器采集。')
   return self.job_dict(db.execute('SELECT * FROM jobs WHERE id=?',(r['id'],)).fetchone())
 def advance(self,jid,status,counts=None,checkpoint=None,message=''):
  if status not in STAGES or status=='completed':raise Problem('完成状态只能由素材证据校验生成')
  with self.db() as db:
   db.execute('BEGIN IMMEDIATE');r=db.execute('SELECT * FROM jobs WHERE id=?',(jid,)).fetchone()
   if not r:raise Problem('任务不存在',404)
   if status!=r['status'] and status not in TRANSITIONS[r['status']]:raise Problem('不允许的状态切换',409)
   old=json.loads(r['counts']);new=counts or old
   if not isinstance(new,dict) or not isinstance(checkpoint or {},dict):raise Problem('进度格式无效')
   if any(not isinstance(v,int) or isinstance(v,bool) or v<0 for v in new.values()):raise Problem('数量必须是非负整数')
   if any(new.get(k,0)<v for k,v in old.items()):raise Problem('累计数量不能回退')
   db.execute('UPDATE jobs SET status=?,counts=?,checkpoint=?,updated=?,lease_until=? WHERE id=?',(status,json.dumps(new),json.dumps(checkpoint or json.loads(r['checkpoint'])),time.time(),time.time()+180,jid));self.event(db,jid,status,message[:1500] or '执行阶段已更新')
 def worker_heartbeat(self,name):
  with self.db() as db:db.execute('INSERT INTO worker VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET heartbeat=excluded.heartbeat,name=excluded.name',(time.time(),str(name)[:100]))
 def snapshot(self):
  with self.db() as db:
   worker=db.execute('SELECT * FROM worker').fetchone();totals=dict(db.execute('SELECT decision,COUNT(*) n FROM materials GROUP BY decision').fetchall());active=db.execute("SELECT COUNT(*) FROM jobs WHERE status NOT IN ('completed','failed','cancelled')").fetchone()[0]
  return {'materials':sum(totals.values()),'keep':totals.get('keep',0),'review':totals.get('review',0),'excluded':totals.get('exclude',0),'open_jobs':active,'worker_online':bool(worker and time.time()-worker['heartbeat']<90),'worker_name':worker['name'] if worker else None,'execution_mode':'browser_agent_handoff','auto_crawl_available':False}
 def material_list(self,decision='keep',q='',job=None):
  with self.db() as db:
   sql='SELECT * FROM materials WHERE decision=?';args=[decision]
   if job:sql+=' AND job_id=?';args.append(job)
   result=[]
   for r in db.execute(sql+' ORDER BY created,id',args):
    m=json.loads(r['metadata']);m.update(id=r['id'],job_id=r['job_id']);m.pop('source_url',None)
    if q and q.lower() not in json.dumps(m,ensure_ascii=False).lower():continue
    if m.get('video'):m['video']='/media/'+r['id']+'/video.mp4';m['poster']='/media/'+r['id']+'/poster.png'
    result.append(m)
   return result
 def review_material(self,mid,user,decision,note,confirmed):
  if decision not in ['keep','exclude'] or not isinstance(note,str) or not 2<=len(note.strip())<=1500 or confirmed is not True:raise Problem('请确认已查看素材，并填写复核说明')
  with self.db() as db:
   db.execute('BEGIN IMMEDIATE');r=db.execute('SELECT * FROM materials WHERE id=?',(mid,)).fetchone()
   if not r:raise Problem('素材不存在',404)
   if r['decision']!='review':raise Problem('此素材已被复核，请刷新页面',409)
   m=json.loads(r['metadata']);video=self.root/'materials'/mid/'video.mp4'
   if decision=='keep' and (not video.is_file() or file_hash(video)!=m['video_sha256']):raise Problem('视频文件校验失败，不能确认通过')
   m['previous_review_reason']=m['reason'];m['decision']=decision;m['reason']=note.strip();m['manual_review']={'user':user['username'],'at':time.time(),'method':'human_review_user_confirmed_viewed','audio_reviewed':False}
   db.execute('UPDATE materials SET decision=?,metadata=? WHERE id=?',(decision,json.dumps(m,ensure_ascii=False),mid))
   job=db.execute('SELECT counts FROM jobs WHERE id=?',(r['job_id'],)).fetchone();counts=json.loads(job[0])
   for key in ['keep','review','exclude']:counts['excluded' if key=='exclude' else key]=db.execute('SELECT COUNT(*) FROM materials WHERE job_id=? AND decision=?',(r['job_id'],key)).fetchone()[0]
   db.execute('UPDATE jobs SET counts=?,updated=? WHERE id=?',(json.dumps(counts),time.time(),r['job_id']));self.event(db,r['job_id'],'completed',f"{user['username']}复核素材{r['ad_id']}：{decision}；{note.strip()}")
 def import_delivery(self,run,jid=None):
  """Operator-only ingestion, requires real files and review evidence. Never runs a crawl."""
  run=Path(run).resolve();delivery=run/'delivery';payload=json.loads((delivery/'manifest.json').read_text());items=payload['items'];reviews={r['ad_id']:r for r in json.loads((run/'reviews.json').read_text())['items']};verification=json.loads((run/'verification.json').read_text())
  if not items or len(items)>10000 or len({(i['platform'],i['app_id'],i['ad_id']) for i in items})!=len(items):raise Problem('素材清单为空或有重复')
  if verification.get('raw_count')!=len(items):raise Problem('采集总数与封账结果不一致')
  all_cards=list((run/'tasks').glob('*/cards/*.json'))
  if len(all_cards)!=len(items):raise Problem('原始逐卡清单不完整')
  raw_ids=set()
  for raw_file in all_cards:
   card=json.loads(raw_file.read_text());raw_ids.add((card['platform'],card['app_id'],card['ad_id']))
   task_root=raw_file.parents[1];event=json.loads(safe_file(task_root,card['ui_check']).read_text())
   if event.get('status')!='passed' or event.get('after',{}).get('ad_id')!=card['ad_id']:raise Problem('逐卡点击证据不完整')
   for proof in event['images'].values():
    if file_hash(safe_file(task_root,proof['path']))!=proof['sha256']:raise Problem('逐卡截图哈希不符')
  if raw_ids!={(i['platform'],i['app_id'],i['ad_id']) for i in items}:raise Problem('原始卡片与交付清单不一致')
  for task_root in (run/'tasks').iterdir():
   if not (task_root/'cards').is_dir():continue
   pages=[json.loads(p.read_text()) for p in sorted((task_root/'pages').glob('*.json'))]
   if not pages or [p['page'] for p in pages]!=list(range(1,len(pages)+1)) or pages[-1]['has_next'] is not False or sum(p['visible_count'] for p in pages)!=len(list((task_root/'cards').glob('*.json'))):raise Problem('分页或末页证据不完整')
  checked=[]
  for item in items:
   decision=item['decision'];ad=item['ad_id'];review=reviews.get(ad)
   if decision not in ['keep','exclude','review'] or not review or review['decision']!=decision or review.get('frames_inspected') is not True:raise Problem('内容审核证据不完整')
   manifest_file=safe_file(run,review['manifest']);m=json.loads(manifest_file.read_text())
   # Source path resolves against the task containing evidence/frames/<ad>/manifest.json.
   task=manifest_file.parents[3]
   if file_hash(manifest_file)!=review['manifest_sha256'] or len(m['frames'])!=6:raise Problem('抽帧清单校验失败')
   source=safe_file(task,m['source']['path'])
   if not m['source'].get('decode_verified') or file_hash(source)!=m['source']['sha256']:raise Problem('原视频校验失败')
   for frame in m['frames']:
    if file_hash(safe_file(task,frame['path']))!=frame['sha256']:raise Problem('抽帧哈希不符')
   board=safe_file(run,review['board'])
   if file_hash(board)!=review['board_sha256']:raise Problem('审核图哈希不符')
   if decision in ['keep','review']:
    video=safe_file(delivery,item['video']);poster=safe_file(delivery,item['poster'])
    if file_hash(video)!=item['video_sha256'] or item['video_sha256']!=m['source']['sha256']:raise Problem('交付与审核源视频不一致')
    checked.append((item,video,poster))
   else:checked.append((item,None,None))
  key=digest(str(run)+':'+file_hash(delivery/'manifest.json'));now=time.time()
  with self.db() as db:
   existing=db.execute('SELECT id FROM jobs WHERE import_key=?',(key,)).fetchone()
   if existing:
    if jid and jid!=existing['id']:raise Problem('该批次已导入，不能用旧结果完成另一个新任务')
    return existing['id']
   if jid:
    job=db.execute('SELECT * FROM jobs WHERE id=?',(jid,)).fetchone()
    if not job or job['status']!='packaging':raise Problem('任务必须处于打包校验阶段')
    scope=json.loads(job['scope']);expected={(a['platform'],a['app_id']) for a in scope['apps']}
    if any((i['platform'],i['app_id']) not in expected for i,_,_ in checked):raise Problem('素材App与任务不一致')
   else:
    jid=str(uuid.uuid4());owner=db.execute("SELECT id FROM users WHERE role='admin' LIMIT 1").fetchone()[0]
    scope={'apps':[{'app_name':items[0]['app_name'],'app_id':items[0]['app_id'],'platform':items[0]['platform']}],'date':{'preset':'30天'},'category':'工具','mode':'素材','media':'视频','ratio':'9:16','channels':['Facebook','TikTok','AppLovin'],'languages':['英语','土耳其语','西班牙语','葡萄牙语','德语','韩语'],'regions':[],'date_anchor':'2026-10-20','source':'广大大','content':{'male_character':True,'female_audience':True,'exclude_talking_head':True}}
    db.execute('INSERT INTO jobs(id,title,owner,scope,status,created,updated,import_key) VALUES(?,?,?,?,?,?,?,?)',(jid,'Talkie Lab · 30天竖屏男性角色素材',owner,json.dumps(scope,ensure_ascii=False),'packaging',now,now,key))
   counts={'found':len(items),'detail_success':len(items),'downloaded':len(items),'reviewed':len(items),'keep':sum(i['decision']=='keep' for i in items),'review':sum(i['decision']=='review' for i in items),'excluded':sum(i['decision']=='exclude' for i in items)}
   for item,video,poster in checked:
    mid=str(uuid.uuid5(uuid.UUID(jid),item['platform']+':'+item['app_id']+':'+item['ad_id']));folder=self.root/'materials'/mid;folder.mkdir(parents=True,exist_ok=True)
    if video:
     for src,name in [(video,'video.mp4'),(poster,'poster.png')]:
      dest=folder/name
      if not dest.exists():
       try:os.link(src,dest)
       except OSError:shutil.copy2(src,dest)
    db.execute('INSERT OR REPLACE INTO materials VALUES(?,?,?,?,?,?,?,?)',(mid,jid,item['ad_id'],item['platform'],item['app_id'],item['decision'],json.dumps(item,ensure_ascii=False),now))
   db.execute("UPDATE jobs SET status='completed',counts=?,import_key=?,updated=? WHERE id=?",(json.dumps(counts),key,now,jid));self.event(db,jid,'completed','已校验逐条原视频、六帧、审核图、交付哈希后导入；画面初筛，未逐条审听音轨。')
  return jid
 def handoff(self,jid):
  j=self.job(jid);scope=json.dumps({k:v for k,v in j['scope'].items() if k!='ai_intake'},ensure_ascii=False,indent=2)
  return f'''请使用 guangdada-browser-crawl skill，在已有的有头浏览器中执行任务 {jid}。\n严格按以下scope采集，不使用广大大私有接口或旧爬虫CLI。未完成App身份/条件检查不得点卡；按每个App/平台分别执行，每次查询后重新点击最新创意。不限条件只读核对，有残留才清除。30天在投任务需要遍历全部页，不能套用单日首次发现截止规则。逐卡保存实际详情、真实点击日志、前后截图和SHA256，遇登录或验证码暂停人工。视频下载并抽六帧实际查看；不能自动填写语义审核成功。仅使用本批条件，不能继承日采集的前10%、地区或重投排除。完成后用worker提交证据，通过校验后才可标记完成。\n\n{scope}\n\n本机任务目录：{self.root/'runs'/jid}\n当前平台采集端为人工浏览器agent交接模式，不宣称后台自动抓取。'''
