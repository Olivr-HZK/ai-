// Uses only the supplied, authorized browser tab. No browser driver/site API.
import * as fs from 'node:fs/promises';
import path from 'node:path';
import {createHash, randomUUID} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
const exec = promisify(execFile), stamp=()=>new Date().toISOString();
const hash=x=>createHash('sha256').update(x).digest('hex');
const normalize=v=>Array.isArray(v)?v.map(normalize):v&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,normalize(v[k])])):v;
const equal=(a,b)=>JSON.stringify(normalize(a))===JSON.stringify(normalize(b));

export async function createDirectSession({tab,browser,runDir,taskId,projectRoot,python,ui={},local}){
 const base=path.resolve(runDir),scope=JSON.parse(await fs.readFile(path.join(base,'scope.json'),'utf8'));
 const fp=JSON.parse(await fs.readFile(path.join(base,'run.json'),'utf8')).scope_sha256;
 if(scope.collection_mode!=='direct-browser-v1')throw Error('Direct mode must be fixed in a new scope');
 const task=scope.tasks.find(t=>t.id===taskId);if(!task)throw Error('Unknown direct task');
 const root=path.join(base,'tasks',task.id),category=task.category||scope.filters.category;
 const save=async(rel,value)=>{const p=path.resolve(root,rel);if(!p.startsWith(root+path.sep))throw Error('Unsafe direct path');await fs.mkdir(path.dirname(p),{recursive:true});const tmp=p+'.'+randomUUID()+'.tmp';await fs.writeFile(tmp,typeof value==='object'&&!Buffer.isBuffer(value)&&!(value instanceof Uint8Array)?JSON.stringify(value,null,2):value,{mode:0o600});await fs.rename(tmp,p);};
 const load=async rel=>JSON.parse(await fs.readFile(path.join(root,rel),'utf8'));
 const shot=async rel=>{const b=await tab.screenshot({fullPage:false});await save(rel,b);return {path:rel,sha256:hash(b)};};
 const cli=local||(async(module,args=[])=>{if(!projectRoot)throw Error('projectRoot required for local state bridge');const r=await exec(python||path.join(projectRoot,'.venv/bin/python'),['-m',module,...args,'--run-dir',base],{cwd:projectRoot,maxBuffer:2e6});return JSON.parse(r.stdout);});
 let hadCheckpoint=false;
 const S={task,page:1,pos:1,detail_open:false,last_ad_id:null,prepare_path:'direct/prepare.json'};
 try{const c=await load('direct/checkpoint.json');if(c.scope_sha256!==fp||c.task_id!==task.id)throw Error('Checkpoint scope mismatch');Object.assign(S,c);hadCheckpoint=true;}catch(e){if(e.code!=='ENOENT')throw e;}
 const checkpoint=async()=>save('direct/checkpoint.json',{...S,task:undefined,scope_sha256:fp,task_id:task.id,updated_at:stamp()});
 const observe=ui.observe||(async()=>tab.playwright.evaluate(()=>{
  const text=document.body.innerText, inputs=[...document.querySelectorAll('input[type=radio]:checked')];
  const cats=[...document.querySelectorAll('div')].filter(e=>e.textContent==='游戏'||e.textContent==='工具');
  const category=cats.find(e=>e.classList.contains('border-primary'))?.textContent;
  const qs=[...text.matchAll(/综合\s*:\s*([^\n]+)/g)].map(x=>x[1].trim());
  const typ=document.querySelector('#filter_ads_type')?.closest('.ant-select');
  const media=typ?.innerText.trim();
  const list=[...document.querySelectorAll('.shadow-common-light.bg-white')].map(e=>e.innerText+'\n'+[...e.querySelectorAll('img[alt="Google Play"],img[alt="App Store"]')].map(i=>i.alt).join(','));
  const mode=document.querySelector('#filter_position')?.parentElement.getAttribute('title');
  const newest=[...document.querySelectorAll('span')].find(e=>e.textContent==='最新创意')?.parentElement;
  const next=document.querySelector('.ant-pagination-next');
  return {category,dedupe:inputs.map(e=>e.parentElement.parentElement.innerText).find(t=>['素材','广告','广告主'].includes(t)),date:Object.fromEntries([...document.querySelectorAll('.ant-picker-input input')].map(e=>[e.placeholder==='开始日期'?'start':'end',e.value])),
   media,selected_types:[...(typ?.querySelectorAll('.ant-select-selection-item')||[])].map(e=>e.textContent),
   channels:[...document.querySelectorAll('input[type=checkbox]:checked')].filter(e=>e.id!=='filter_search_type').map(e=>e.parentElement.parentElement.innerText),
   regions:document.querySelector('input[placeholder="国家/地区"]')?.closest('.ant-select')?.innerText.trim()||'',languages:document.querySelector('input[placeholder="文案语言"]')?.closest('.ant-select')?.innerText.trim()||'',size:document.querySelector('input[placeholder="素材规格"]')?.closest('.ant-select')?.innerText.trim()||'',
   search_mode:mode,query_terms:qs,selected_app_ids:text.includes('广告主平台:')?['unresolved-advertiser']:[],sort:newest?.classList.contains('text-blue-600')?'最新创意':null,
   page:Number(document.querySelector('.ant-pagination-item-active')?.innerText||1),rows:list,has_next:next?!(next.classList.contains('ant-pagination-disabled')||next.querySelector('button')?.disabled):null,
   loading:!!document.querySelector('.ant-skeleton,.ant-spin-spinning'),explicit_no_results:text.includes('暂无数据'),raw_text:text,captcha:/安全验证|滑块|完成验证/.test(text),login_required:location.pathname.includes('/auth/login'),detail_open:!!document.querySelector('[role=dialog]')};
 }));
 const filters=v=>({category:v.category,dedupe:v.dedupe,date:v.date,media:v.media,selected_types:v.selected_types,channels:v.channels,regions:v.regions?[v.regions]:[],languages:v.languages?[v.languages]:[],size:v.size});
 const expected={category,dedupe:'素材',date:{start:scope.target_date,end:scope.target_date},media:'图片&视频',selected_types:[],channels:[],regions:[],languages:[],size:''};
 const guard=v=>{if(v.captcha)throw Error('CAPTCHA_STOP');if(v.login_required)throw Error('LOGIN_REQUIRED');};
 const wait=async(fn,ready)=>{let v;const end=Date.now()+20000;for(let n=0;n<256&&Date.now()<end;n++){v=await fn();guard(v);if(ready(v))return v;await tab.playwright.domSnapshot();}throw Error('Visible UI did not become ready');};
 let staleSignature=null;
 const settled=async()=>{let emptySince=null;return wait(observe,v=>{
  if(v.loading||v.detail_open||!equal(v.query_terms,[task.app_id])||v.sort!=='最新创意'||!equal(filters(v),expected)){emptySince=null;return false;}
  if(v.rows.length){if(staleSignature&&hash(JSON.stringify(v.rows))===staleSignature)return false;staleSignature=null;return true;}
  if(!v.explicit_no_results){emptySince=null;return false;}
  emptySince??=Date.now();return Date.now()-emptySince>=500;
 });};
 const recordPage=async(before)=>{const v=await settled();if(v.page!==S.page)throw Error('Page readback mismatch');const image=await shot(`direct/screens/page-${S.page}-${randomUUID()}.png`);const p={scope_sha256:fp,task_id:task.id,page:v.page,query_terms:v.query_terms,visible_count:v.rows.length,signature:hash(JSON.stringify(v.rows)),has_next:v.has_next,settled:true,explicit_no_results:v.explicit_no_results,raw_text:v.raw_text,images:{before:before||image,after:image},observed_at:stamp()};await save(`direct/pages/${String(S.page).padStart(4,'0')}.json`,p);return p;};
 const prepare=async(resuming=false)=>{
  const before=await shot(`direct/screens/prepare-${randomUUID()}-before.png`),v=await observe();guard(v);staleSignature=v.rows.length&&!equal(v.query_terms,[task.app_id])?hash(JSON.stringify(v.rows)):null;const actions=[],started=stamp();
  if(!equal(filters(v),expected)){
   if(ui.repairFilters)await ui.repairFilters(filters(v),expected);
   else{
    if(v.category!==category)await tab.playwright.getByText(category,{exact:true}).click();
    if(!equal(v.date,expected.date)){await tab.playwright.getByRole('textbox',{name:'开始日期',exact:true}).fill(scope.target_date);await tab.playwright.getByRole('textbox',{name:'结束日期',exact:true}).fill(scope.target_date);await tab.playwright.getByRole('textbox',{name:'结束日期',exact:true}).press('Enter');}
    if(v.dedupe!=='素材')await tab.playwright.getByText('素材',{exact:true}).click();
    if(v.channels.length||v.regions||v.languages||v.size||v.selected_types.length)throw Error('Residual restrictions need visible UI repair adapter');
   }
  }
  const repaired=await observe();guard(repaired);if(!equal(filters(repaired),expected))throw Error('Base filters readback mismatch');
  actions.push({name:'filters',action:equal(filters(v),expected)?'readonly':'repair',before:filters(v),after:filters(repaired),started_at:started,ended_at:stamp()});
  if(repaired.query_terms.length||repaired.selected_app_ids.length)await tab.playwright.getByRole('button',{name:'嘹 清空',exact:true}).click();
  if(repaired.search_mode!=='综合'){if(!ui.selectComprehensive)throw Error('Select comprehensive using a visible UI adapter');await ui.selectComprehensive();}
  let at=stamp();const input=ui.queryInput||tab.playwright.locator('#display-search-input-container input.ant-select-input').nth(1);await input.fill(task.app_id);await input.press('Enter');actions.push({name:'app.query',action:'search',interaction:{action:'press',target:'Enter'},started_at:at,ended_at:stamp()});
  at=stamp();await tab.playwright.getByText('最新创意',{exact:true}).click();actions.push({name:'sort',action:'sort',after:'最新创意',started_at:at,ended_at:stamp()});
  const ready=await settled();if(ready.selected_app_ids.length)throw Error('Residual advertiser');
  const actual={...filters(ready),search_mode:ready.search_mode,query_terms:ready.query_terms,selected_app_ids:ready.selected_app_ids,sort:ready.sort};
  const p={producer:'gdd-direct-preparation/v1',scope_sha256:fp,task_id:task.id,actual,actions,settled:true,authenticated:true,observed_at:stamp(),images:{before,after:await shot(`direct/screens/prepare-${randomUUID()}-after.png`)}};
  const rel=resuming?`direct/resumes/${randomUUID()}.json`:'direct/prepare.json';await save(rel,p);S.prepare_path=rel;return p;
 };
 const readRaw=ui.readDetail||(async()=>tab.playwright.getByRole('dialog').evaluate(el=>({raw_text:el.innerText,links:[...el.querySelectorAll('a[href]')].map(a=>({text:a.innerText,href:a.href})),observed_videos:[...el.querySelectorAll('video')].map(v=>({src:v.currentSrc||v.src,poster:v.poster,sources:[...v.querySelectorAll('source')].map(s=>s.src)})),observed_images:[...el.querySelectorAll('img.ant-image-img')].map(i=>({src:i.src,cls:i.className,is_original:true})),observed_iframes:[...el.querySelectorAll('iframe')].map(i=>({src:i.src})),observed_platform:({'Google Play':'Android','App Store':'iOS'})[el.querySelector('img[alt="Google Play"],img[alt="App Store"]')?.alt]||null})));
 let identityPath=null;
 const identity=async d=>{
  if(d.observed_platform!==task.platform)throw Error('IDENTITY_MISMATCH');
  const stores=d.links.map(l=>{const u=new URL(l.href);if(['apps.apple.com','itunes.apple.com'].includes(u.hostname))return {app_id:u.pathname.match(/\/id(\d+)/)?.[1],platform:'iOS'};if(u.hostname==='play.google.com')return {app_id:u.searchParams.get('id'),platform:'Android'};return null;}).filter(Boolean);
  if(stores.length){if(!stores.some(s=>s.app_id===task.app_id&&s.platform===task.platform))throw Error('IDENTITY_MISMATCH');return;}
  if(!d.observed_app_name||d.observed_platform!==task.platform)throw Error('DETAIL_NOT_READY');
  if(!identityPath){
   if(!browser)throw Error('Observed advertiser identity requires current browser tabs adapter');
   const before=await shot(`direct/screens/identity-${randomUUID()}-before.png`),old=new Set((await browser.tabs.list()).map(t=>t.id));
   await tab.playwright.getByRole('dialog').getByText(d.observed_app_name,{exact:true}).click();let found;
   for(let n=0;n<32&&!found;n++){found=(await browser.tabs.list()).find(t=>!old.has(t.id));if(!found)await tab.playwright.domSnapshot();}
   if(!found)throw Error('Identity page not opened');const opened=await browser.tabs.get(found.id),url=await opened.url(),u=new URL(url);
   if(u.hostname!=='guangdada.net'||u.searchParams.get('store')!==task.app_id||u.searchParams.get('os')!==(task.platform==='iOS'?'1':'2'))throw Error('IDENTITY_MISMATCH');
   const bytes=await opened.screenshot({fullPage:false}),rel=`direct/screens/identity-${randomUUID()}-after.png`;await save(rel,bytes);identityPath=`direct/identity-${randomUUID()}.json`;
   await save(identityPath,{scope_sha256:fp,task_id:task.id,url,observed_app_name:d.observed_app_name,observed_at:stamp(),images:{before,after:{path:rel,sha256:hash(bytes)}}});await opened.close();
  }
  const p=await load(identityPath);if(p.observed_app_name!==d.observed_app_name)throw Error('IDENTITY_MISMATCH');d.identity_evidence=identityPath;
 };
 const detail=async()=>{
  const d=await readRaw();guard({...d,captcha:/安全验证|滑块|完成验证/.test(d.raw_text)});
  d.observed_app_name ||= d.raw_text.match(/受众画像\s*\n([^\n]+)\n当页查看创意/)?.[1];
  const range=d.raw_text.match(/(\d{4}-\d{2}-\d{2})\s*[~～至]\s*(\d{4}-\d{2}-\d{2})/),url=d.links.find(l=>l.text.trim()==='单页打开')?.href;
  if(!range||!url)throw Error('DETAIL_NOT_READY');if(!/^[a-f0-9]{32}$/.test(new URL(url).searchParams.get('id')||''))throw Error('Invalid observed ad ID');await identity(d);
  const kind=d.observed_iframes.length&&d.raw_text.includes('试玩广告')?'试玩':d.observed_videos.length?'视频':d.observed_images.length?'图片':d.links.some(l=>['www.youtube.com','youtube.com','youtu.be'].includes(new URL(l.href).hostname))?'视频':null;
  const youtube=d.links.some(l=>['www.youtube.com','youtube.com','youtu.be'].includes(new URL(l.href).hostname));
  if(range[1]===scope.target_date&&kind==='视频'&&!d.observed_videos.some(v=>/^https?:/.test(v.src||'')||v.sources?.some(u=>/^https?:/.test(u)))&&!youtube&&!/官方资源限制|原始链接|无可用素材/.test(d.raw_text))throw Error('DETAIL_NOT_READY');
  const estimate=d.raw_text.match(/展示估值\s*\n\s*([\d,.]+[KkMm万亿]?)/)?.[1];if(!kind||(range[1]===scope.target_date&&!estimate))throw Error('DETAIL_NOT_READY');
  const info=d.raw_text.split('\n当页查看创意')[1]||d.raw_text;
  const section=(start,end)=>info.split(start+'\n')[1]?.split('\n'+end)[0]?.trim();
  const channel=section('投放渠道','文案语言'),language=section('文案语言','地区'),region=section('地区','素材尺寸'),dimensions=info.split('素材尺寸')[1]?.match(/\d+\s*x\s*\d+/)?.[0];
  return {...d,channel:channel||'',language:language||'',regions:region?region.split('\n').filter(x=>!/^\(\d+\)$/.test(x)):[],dimensions:dimensions||'',missing_fields:[!channel&&'channel',!language&&'language',!region&&'regions',!dimensions&&'dimensions'].filter(Boolean),video_url:d.observed_videos.find(v=>/^https?:/.test(v.src||''))?.src,external_url:d.links.find(l=>['www.youtube.com','youtube.com','youtu.be'].includes(new URL(l.href).hostname))?.href,scope_sha256:fp,task_id:task.id,app_name:task.app_name,app_id:task.app_id,platform:task.platform,detail_url:url,ad_id:new URL(url).searchParams.get('id'),media_type:kind,impressions_estimate:estimate,first_discovered_date:range[1],first_discovered_source:'detail_date_range_start',detail_loaded:true,observed_at:stamp()};
 };
 const readReady=async()=>{const end=Date.now()+20000;let d;for(let n=0;n<64&&Date.now()<end;n++){const v=await observe();guard(v);try{d=await detail();if(d.ad_id===S.last_ad_id)throw Error('DETAIL_NOT_READY');return d;}catch(e){if(e.message!=='DETAIL_NOT_READY')throw e;}await tab.playwright.domSnapshot();}throw Error('Detail readiness timeout');};
 const open=async pos=>{if(ui.openCard)return ui.openCard(pos);return tab.playwright.locator('.shadow-common-light.bg-white').nth(pos-1).locator(':scope > div').nth(1).click();};
 const close=async()=>{await tab.playwright.getByRole('button',{name:'Close',exact:true}).click();await tab.playwright.getByRole('dialog').waitFor({state:'hidden'});S.detail_open=false;await checkpoint();};
 const cards=async()=>{let names=[];try{names=(await fs.readdir(path.join(root,'cards'))).filter(x=>x.endsWith('.json')).sort();}catch(e){if(e.code!=='ENOENT')throw e;}return Promise.all(names.map(async n=>{const rel='cards/'+n,b=await fs.readFile(path.join(root,rel));return {path:rel,sha256:hash(b),card:JSON.parse(b)};}));};
 const seal=async reason=>{const rows=await cards(),before=await shot(`direct/screens/terminal-${randomUUID()}-before.png`),after=await shot(`direct/screens/terminal-${randomUUID()}-after.png`);const last=rows.at(-1)?.card;const value={...task,scope_sha256:fp,task_id:task.id,target_date:scope.target_date,media_mode:'图片&视频',checked_cards:rows.length,target_date_items:rows.filter(x=>x.card.first_discovered_date===scope.target_date).length,terminal_reason:reason,cutoff_card:reason==='first_discovered_before_target'?last.ad_id:null,cards:rows.map(({path,sha256})=>({path,sha256})),images:{before,after},observed_at:stamp()};await save('direct/complete.json',value);await cli('ua_workflows.social_chat_direct',['accept','--task',task.id]);if(task.id===scope.tasks.at(-1).id)await cli('ua_workflows.browser_collection_runtime',['stop']);return {status:'app_accepted',task_id:task.id,target_items:value.target_date_items,reason};};
 const onError=async error=>{const message=String(error.message||error);S.control=message==='CAPTCHA_STOP'?'WAITING_HUMAN':'RETRY_REQUIRED';await checkpoint();const evidence=await shot(`direct/screens/error-${randomUUID()}.png`);await save(`direct/errors/${randomUUID()}.json`,{error:message,page:S.page,next_position:S.pos,detail_open:S.detail_open,evidence,observed_at:stamp()});await cli('ua_workflows.social_chat_daily_state',['hold','--kind',S.control,'--reason',message]);await cli('ua_workflows.browser_collection_runtime',['stop']);return {status:S.control,task_id:task.id,page:S.page,next_position:S.pos,error:message};};
 const api={
  async start(){if(hadCheckpoint)return {status:'existing_checkpoint',task_id:task.id,page:S.page,next_position:S.pos,control:S.control||null};try{await cli('ua_workflows.social_chat_daily_state',['begin']);await cli('ua_workflows.browser_collection_runtime',['start']);await prepare();S.page=1;S.pos=1;await checkpoint();const p=await recordPage();return p.explicit_no_results?seal('explicit_no_results'):{status:'ready',task_id:task.id};}catch(e){return onError(e);}},
  async batch(maxCards=10){try{
   if(S.control)throw Error('Resume required');if(!Number.isInteger(maxCards)||maxCards<1||maxCards>10)throw Error('Batch must be 1..10');
   const started=Date.now(),result=[];const page=await load(`direct/pages/${String(S.page).padStart(4,'0')}.json`);
   if(S.detail_open){const prior=await cards();if(prior.at(-1)?.card.ad_id===S.last_ad_id)await close();else throw Error('Unsaved open detail requires resume');}
   while(result.length<maxCards&&S.pos<=page.visible_count&&Date.now()-started<45000){
    const v=await settled();if(v.page!==S.page||hash(JSON.stringify(v.rows))!==page.signature)throw Error('LIST_CONTEXT_CHANGED');
   const before=await shot(`direct/screens/${S.page}-${S.pos}-${randomUUID()}-before.png`),clicked=stamp();S.detail_open=true;await checkpoint();await open(S.pos);await tab.playwright.getByRole('dialog').waitFor({state:'visible'});
    const c=await readReady();if((await cards()).some(x=>x.card.ad_id===c.ad_id))throw Error('DUPLICATE_AD_ID');
    c.clicked_at=clicked;c.page=S.page;c.position=S.pos;c.prepare_evidence=S.prepare_path;c.evidence={before,after:await shot(`direct/screens/${S.page}-${S.pos}-${randomUUID()}-after.png`)};
    await save(`cards/${String(S.page).padStart(4,'0')}-${String(S.pos).padStart(4,'0')}.json`,c);S.pos++;S.last_ad_id=c.ad_id;await checkpoint();await close();result.push(c.ad_id);
    if(c.first_discovered_date<scope.target_date)return await seal('first_discovered_before_target');
   }
   if(S.pos>page.visible_count&&page.has_next===false)return await seal('next_disabled');
   return {status:S.pos>page.visible_count?'page_end':'yielded',task_id:task.id,page:S.page,next_position:S.pos,saved:result.length,elapsed_ms:Date.now()-started};
  }catch(e){return onError(e);}},
  async nextPage(){try{await cli('ua_workflows.browser_collection_runtime',['renew']);const p=await load(`direct/pages/${String(S.page).padStart(4,'0')}.json`);if(S.pos!==p.visible_count+1||p.has_next!==true)throw Error('Current page incomplete/no next');const before=await shot(`direct/screens/page-next-${randomUUID()}.png`);await tab.playwright.getByRole('button',{name:'下一页',exact:true}).click();const v=await wait(observe,v=>!v.loading&&v.rows.length>0&&v.page===S.page+1&&hash(JSON.stringify(v.rows))!==p.signature);S.page=v.page;S.pos=1;await checkpoint();await recordPage(before);return {status:'ready',page:S.page};}catch(e){return onError(e);}},
  async resume(confirmation){try{
   if(!confirmation?.trim())throw Error('Actual human confirmation/resolution required');const v=await observe();guard(v);await cli('ua_workflows.browser_collection_runtime',['start']);if(v.detail_open)await close();
   const p=await prepare(true),rows=await cards(),last=rows.at(-1)?.card;
   if(last){await tab.playwright.getByRole('button',{name:'页 '+last.page,exact:true}).click();await wait(observe,v=>!v.loading&&v.rows.length>0&&v.page===last.page);await open(last.position);await tab.playwright.getByRole('dialog').waitFor({state:'visible'});
    const old=S.last_ad_id;S.last_ad_id=null;const actual=await readReady();S.last_ad_id=old;if(actual.ad_id!==last.ad_id)throw Error('RESUME_BOUNDARY_CHANGED');p.boundary={expected:last.ad_id,observed:actual.ad_id,card_path:rows.at(-1).path,card_sha256:rows.at(-1).sha256};await close();}
   if((await observe()).page!==S.page){await tab.playwright.getByRole('button',{name:'页 '+S.page,exact:true}).click();await wait(observe,v=>!v.loading&&v.rows.length>0&&v.page===S.page);}
   p.confirmation=confirmation;await save(S.prepare_path,p);await cli('ua_workflows.social_chat_daily_state',['resume','--reason',confirmation,'--proof','tasks/'+task.id+'/'+S.prepare_path]);await cli('ua_workflows.social_chat_daily_state',['begin']);S.control=null;S.detail_open=false;await checkpoint();await recordPage();return {status:'ready',page:S.page,next_position:S.pos};
  }catch(e){return onError(e);}},
  checkpoint:()=>({...S}),prepare,observe
 };const collect=api.batch;api.batch=async n=>{const start=stamp(),clock=Date.now(),r=await collect(n);await save('direct/batches/'+randomUUID()+'.json',{...r,started_at:start,ended_at:stamp(),elapsed_seconds:(Date.now()-clock)/1000});return r;};return api;
}
