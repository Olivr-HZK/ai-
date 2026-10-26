import {allowedPage,normalize,pageLimit} from './media.js';
import {collectMedia} from './collector.js';
import {cartPages} from './selection.js';
import {buildReport} from './report.js';
import {dateLimit,datePlan} from './date-filter.js';
import {recoveryItems} from './recovery.js';
import {taskActive} from './tasks.js';
import {canResumeCollection,supportsCollectionResume,resumePage} from './collection-resume.js';
const $=id=>document.getElementById(id);
let busy=false,validTab=false,scanReady=false,tabId,scan=null,latestJob=null,basket=null,collectorId=null,tasks=[];
const active=taskActive;
const phaseNames={collecting:'采集地址','collection-paused':'采集已暂停',queued:'排队等待下载','awaiting-permission':'等待授权',fetching:'下载中',packing:'汇总 ZIP',downloading:'保存 ZIP',complete:'已完成',failed:'失败',stopped:'已停止'};
function renderTasks(result){
  collectorId=result.collector || null;tasks=result.tasks || [];
  $('task-list').replaceChildren(...tasks.slice().reverse().map(task=>{const option=document.createElement('option');option.value=task.id;option.textContent=`${task.taskName} · ${phaseNames[task.phase] || task.phase} · ${task.complete}/${task.total}`;return option;}));
  $('task-list').disabled=busy || !tasks.length;
  if(result.job)$('task-list').value=result.job.id;
  $('task-hint').textContent=collectorId?'有任务正在采集地址，请等采集结束后再操作列表。':tasks.some(t=>['fetching','packing','downloading'].includes(t.phase) || t.waitingForExternalPermission)?'后台正在下载；可以继续选择下一批，每批独立出包。':'每批独立保存地址、ZIP 和报告；下载按队列执行。';
}
function updateScope(){
  const mode=$('mode').value,multi=mode==='pages',untilEnd=$('end').value==='last';
  $('page-options').hidden=!multi;$('count-row').hidden=!multi || untilEnd;
  $('pages').disabled=busy || !!collectorId || untilEnd;
  $('date-enabled').disabled=busy || !!collectorId;
  $('date-since').disabled=busy || !!collectorId;
  $('date-row').hidden=!$('date-enabled').checked;$('date-hint').hidden=!$('date-enabled').checked;
  if(mode==='basket'){
    $('scope').textContent=`已选清单共 ${basket?.entries.length || 0} 张，覆盖 ${cartPages(basket?.entries || []).length} 页。可以继续手动翻页勾选，最后一起打包。`;
    $('start').textContent='保存已选地址并下载 ZIP';
  }else if(multi){
    $('scope').textContent=untilEnd?'从当前页开始一直采集到列表末页，每页获取全部所选类型，最后合成一个 ZIP。':`从当前页开始，最多采集 ${$('pages').value || '?'} 页的地址（含当前页），遇末页提前停止。地址保存后统一下载。`;
    if($('date-enabled').checked)$('scope').textContent+=` 首次发现早于 ${$('date-since').value || '所选日期'} 时提前停止；当天保留。`;
    $('start').textContent='先采集地址，再统一下载 ZIP';
  }else{
    $('scope').textContent=`下载当前页全部 ${scan?.cardCount || 0} 张。需要挑选时，请点「跨页点选素材」。`;
    $('start').textContent='保存本页地址并下载 ZIP';
  }
  $('cart').hidden=!basket?.active && !basket?.entries.length;
  $('cart-count').textContent=`跨页已选 ${basket?.entries.length || 0} 张`;
  $('cart-pages').replaceChildren(...cartPages(basket?.entries || []).map(page=>{
    const li=document.createElement('li');li.textContent=`第 ${page.pageNumber} 页：${page.selectedCardNumbers.join('、')}（${page.selectedCardNumbers.length} 张）`;return li;
  }));
  $('select').textContent=basket?.active || basket?.entries.length?'继续跨页点选素材':'跨页点选素材';
  $('clear-cart').disabled=busy || !!collectorId || !basket?.entries.length;
  $('start').disabled=busy || !validTab || !!collectorId || (mode==='basket'?!basket?.entries.length:!scanReady);
}
function render(job){
  latestJob=job;const running=active(job);
  $('select').disabled=busy || !validTab || !!collectorId;
  $('new-selection').disabled=busy || !validTab || !!collectorId;
  $('task-name').disabled=busy || !!collectorId;
  $('resolve-external').disabled=busy || !!collectorId;
  if(job?.phase==='collecting')$('resolve-external').checked=job.autoResolveExternal!==false;
  if(job?.phase==='collecting' && job.mode==='pages'){$('mode').value='pages';$('date-enabled').checked=!!job.firstDiscoveredSince;if(job.firstDiscoveredSince)$('date-since').value=job.firstDiscoveredSince;}
  $('mode').disabled=busy || !!collectorId;$('end').disabled=busy || !!collectorId;updateScope();
  if(!job)return;
  const paused=canResumeCollection(job),resumable=supportsCollectionResume(job);
  $('progress').hidden=false;
  const n=state=>job.items.filter(i=>i.state===state).length;
  const collectionProgress=job.phase==='collecting' || paused;
  $('bar').max=Math.max(1,collectionProgress?(job.collection?.targetPages || job.pages.length+1):job.items.length);
  $('bar').value=collectionProgress?(resumable?job.collection.completedPages.length:job.pages.length):n('complete');
  const videos=job.items.filter(i=>i.kind==='video').length, images=job.items.filter(i=>i.kind==='image').length, playables=job.items.filter(i=>i.kind==='playable').length;
  const phases={collecting:'阶段 1：采集并保存地址','collection-paused':'采集已暂停，断点保留',queued:'地址已保存，排队等待下载','awaiting-permission':'素材已收集，等待媒体站点授权',fetching:'阶段 2：下载并写入本机临时磁盘',packing:'正在完成 ZIP 目录和报告',downloading:'正在保存 ZIP',complete:'ZIP 已保存',failed:'打包失败',stopped:'已停止'};
  const scope=job.mode==='pages'
    ? `${resumable?`已处理 ${job.collection.completedPages.length} 页 · 已观察 `:'已采集 '}${job.pages.length}${job.collection.untilEnd?' 页 / 直到末页':`/${job.collection.targetPages} 页`} · ${job.cardCount} 张卡片${job.pages.length?`（第 ${job.pages[0].pageNumber}–${job.pages.at(-1).pageNumber} 页）`:''}`
    : job.mode==='selection'?`跨页选择 ${job.selectedCardCount} 张 · ${job.pages.length} 页`:`本页 ${job.cardCount ?? '?'} 张卡片 · 本批选择 ${job.selectedCardCount ?? job.selectedCardNumbers?.length ?? job.cardCount ?? '?'} 张`;
  const inDate=i=>!job.firstDiscoveredSince || i.dateStatus==='included';
  const saved=(job.selections || []).filter(i=>inDate(i) && i.url && job.requestedKinds?.includes(i.kind)).length;
  const missing=(job.selections || []).filter(i=>inDate(i) && !i.url && (!i.kind || job.requestedKinds?.includes(i.kind))).length;
  $('counts').textContent=`${scope}${job.storageMode==='disk'?`\n已写入媒体 ${(job.items.reduce((sum,i)=>sum+(i.state==='complete'?(i.bytes || 0):0),0)/1024/1024).toFixed(1)} MB`:''}\n地址已保存 ${saved} 项 · 未取得地址 ${missing} 项\n视频 ${videos} · 图片 ${images} · Playable ${playables}\n已获取 ${n('complete')} · 获取中 ${n('active')} · 等待 ${n('pending')} · 失败 ${n('failed')}`;
  $('folder').textContent=`${job.taskName || '原任务'} · ${phases[job.phase] || '旧版任务，请重新打包'}：${job.zipName || ''}`;
  $('errors').replaceChildren(...buildReport(job).selections.filter(i=>['failed','missing','unsupported','cancelled','unattempted','dateUnchecked'].includes(i.state)).map(item=>{
    const li=document.createElement('li');li.textContent=`${item.pageNumber?`第${item.pageNumber}页 `:''}第${item.cardIndex}个 · ${item.result}：${item.reason}`;return li;
  }));
  $('collection-controls').hidden=!resumable || !['collecting','collection-paused'].includes(job.phase);
  $('pause-collection').hidden=job.phase!=='collecting';$('pause-collection').disabled=busy;
  $('resume-collection').hidden=!paused;$('resume-collection').disabled=busy || !validTab || !!collectorId;
  $('download-collected').hidden=!paused;$('download-collected').disabled=busy;
  $('collection-checkpoint').textContent=`已处理 ${job.collection?.completedPages?.length || 0} 页；${paused?'恢复':'当前'}位置：第 ${resumePage(job) || '?'} 页。\n${paused?'恢复原筛选及最新创意排序，等待该页加载并关闭详情后，点击「继续采集」。选择结束采集后将只下载已有地址，后续页不再继续。':'页面异常会暂停保留断点；暂停期间不会自动跳过卡住的页或下载本批媒体。'}`;
  $('retry').disabled=busy || running || paused || !job.zipName || (job.collectionIncomplete && !job.addressFirst) || !job.items.length;
  const recoverable=recoveryItems(job).length;
  $('retry-failed').disabled=busy || running || !recoverable;
  $('retry-failed').textContent=job.phase==='complete'?`补下载失败项（${recoverable} 项）`:'补下载未交付素材';
  $('stop').disabled=!running && !paused;
  $('authorize').hidden=job.phase!=='awaiting-permission';$('authorize').disabled=busy;
  $('message').textContent=[job.mode==='pages' && job.collectionIncomplete?'采集提前中断，尚未确认达到日期截止或列表末页。':'',job.error || job.detailProgress || phases[job.phase] || '',
    job.phase==='collecting'?'本任务先保存全部地址；其他任务的后台下载继续。采集结束前请勿手动翻页或更改筛选。':'',
    job.phase==='queued'?'此任务地址已独立保存；前一下载任务结束后自动开始。可继续选择下一批，也可只停止此排队任务。':'',
    job.phase==='awaiting-permission'?(job.waitingForExternalPermission?'外链解析发现新的素材站点，请点击「授权并继续打包」；已获取文件会保留。':'点击「授权并继续打包」，仅授权本批实际使用的媒体站点。'):'',
    job.collectionError?paused?`采集暂停：${job.collectionError}；断点和已存地址保留，等待继续。`:`地址采集中断：${job.collectionError}；已保存地址继续下载，未取得地址的选择写入报告。`:'',
    job.stopReason==='last-page'?'已到列表末页。':'',
    job.firstDiscoveredSince?`首次发现日期下限：${job.firstDiscoveredSince}（含当天）。`:'',
    job.stopReason==='date-cutoff'?`已按日期停止：第 ${job.dateCutoff.pageNumber} 页第 ${job.dateCutoff.cardIndex} 个首次发现于 ${job.dateCutoff.firstDiscovered}；该素材及后续位置未下载，截止详情写入报告。`:'',
    job.duplicateCount?`相同素材合并 ${job.duplicateCount} 项。`:'',
    job.missing?.length?`${job.missing.length} 张卡片缺少原素材或不支持，详见下载报告。`:'',
    job.phase==='complete'?(job.collectionIncomplete?'已保存已有地址下载成功的素材；采集范围未完成，详情见「下载报告.md」。':'ZIP 包含成功获取的媒体和「下载报告.md」，可逐项核对。'):''
  ].filter(Boolean).join('\n');
}
async function request(type,extra={}){
  const result=await chrome.runtime.sendMessage({type,...extra});
  if(!result?.ok)throw new Error(result?.error || '插件未响应');renderTasks(result);render(result.job);return result.job;
}
async function refreshBasket(choose=false){
  if(!validTab)return;
  const result=await chrome.runtime.sendMessage({type:'get-basket',tabId});
  if(!result?.ok)throw new Error(result?.error || '无法读取已选清单');
  basket=result.basket;
  if(choose && (basket?.active || basket?.entries.length))$('mode').value='basket';
  updateScope();
}
for(const id of ['mode','end'])$(id).onchange=updateScope;
$('pages').oninput=updateScope;
for(const id of ['date-enabled','date-since'])$(id).onchange=()=>{updateScope();chrome.storage.local.set({dateCutoffSettings:{enabled:$('date-enabled').checked,since:$('date-since').value}}).catch(()=>{});};
$('start').onclick=async()=>{
  const kinds=['image','video','playable'].filter(id=>$(id).checked),mode=$('mode').value,untilEnd=$('end').value==='last';
  if(!kinds.length){$('message').textContent='请至少选择图片、视频或 Playable。';return;}
  if(mode==='basket' && !basket?.entries.length){$('message').textContent='跨页清单为空，请先点选素材。';return;}
  if(mode!=='basket' && !scanReady){$('message').textContent='请等待列表加载后重新打开插件。';return;}
  let count=1,firstDiscoveredSince=null;
  try{
    if(mode==='pages' && !untilEnd)count=pageLimit($('pages').value);
    if(mode==='pages' && $('date-enabled').checked){firstDiscoveredSince=dateLimit($('date-since').value);if(!firstDiscoveredSince)throw new Error('请选择最早首次发现日期。');}
  }catch(e){$('message').textContent=e.message;return;}
  busy=true;render(latestJob);let errorText='';
  try{
    const indices=firstDiscoveredSince?new Set(datePlan(scan,firstDiscoveredSince,scan.pageNumber).included):null;
    const source=mode==='basket'?basket.entries:scan.items.filter(i=>!indices || indices.has(i.cardIndex));
    const items=normalize(source.filter(i=>i.url && kinds.includes(i.kind)));
    const origins=[...new Set(items.map(i=>`${new URL(i.url).origin}/*`))];
    const autoResolveExternal=$('resolve-external').checked;
    const taskName=$('task-name').value;
    const message=mode==='basket'?{type:'start-basket',tabId,kinds,autoResolveExternal,taskName,expectedRevision:basket.revision}:{type:'start',tabId,kinds,autoResolveExternal,taskName,mode,pageCount:count,untilEnd,firstDiscoveredSince,expectedCardKeys:scan.cardKeys};
    // Persist the job before a native permission prompt can destroy this popup.
    // The permission call still originates directly from the user's click.
    const started=chrome.runtime.sendMessage(message);
    const authorization=origins.length?chrome.permissions.request({origins}):Promise.resolve(true);
    const [result,granted]=await Promise.all([started,authorization]);
    if(!result?.ok)throw new Error(result?.error || '插件未响应');
    renderTasks(result);render(result.job);$('task-name').value='';
    if(!granted)throw new Error('未授权读取媒体站点，清单已保留，可稍后授权继续。');
  }catch(e){errorText=e.message;}
  finally{busy=false;await request('status').catch(()=>{});if(errorText)$('message').textContent=errorText;}
};
$('authorize').onclick=async()=>{
  busy=true;render(latestJob);let errorText='';
  try{
    if(!(await chrome.permissions.request({origins:latestJob.mediaOrigins})))throw new Error('未授权，素材清单已保留，可稍后再继续。');
    await request('status');
  }catch(e){errorText=e.message;}
  finally{busy=false;await request('status').catch(()=>{});if(errorText)$('message').textContent=errorText;}
};
for(const type of ['retry','retry-failed','stop'])$(type).onclick=()=>request(type,{id:latestJob?.id}).catch(e=>{$('message').textContent=e.message;});
for(const type of ['pause-collection','resume-collection','download-collected'])$(type).onclick=async()=>{
  busy=true;render(latestJob);let errorText='';
  try{await request(type,{id:latestJob?.id,tabId});}catch(error){errorText=error.message;}
  finally{busy=false;await request('status').catch(()=>{});if(errorText)$('message').textContent=errorText;}
};
$('task-list').onchange=()=>request('select-task',{id:$('task-list').value}).catch(e=>{$('message').textContent=e.message;});
$('downloads').onclick=()=>chrome.tabs.create({url:'chrome://downloads/'});
$('report').onclick=()=>chrome.tabs.create({url:chrome.runtime.getURL('report.html')+'?task='+encodeURIComponent(latestJob.id)});
$('select').onclick=async()=>{
  try{
    const result=await chrome.runtime.sendMessage({type:'selection-session',tabId});
    if(!result?.ok)throw new Error(result?.error || '无法进入点选模式');window.close();
  }catch(e){$('message').textContent=e.message;}
};
$('clear-cart').onclick=async()=>{
  try{
    const result=await chrome.runtime.sendMessage({type:'clear-basket',tabId});
    if(!result?.ok)throw new Error(result?.error || '无法清空');await refreshBasket();
  }catch(e){$('message').textContent=e.message;}
};
$('new-selection').onclick=async()=>{
  try{
    const cleared=await chrome.runtime.sendMessage({type:'clear-basket',tabId});if(!cleared?.ok)throw new Error(cleared?.error || '无法清空清单');
    const selected=await chrome.runtime.sendMessage({type:'selection-session',tabId});if(!selected?.ok)throw new Error(selected?.error || '无法选择下一批');
    window.close();
  }catch(e){$('message').textContent=e.message;}
};
const [tab]=await chrome.tabs.query({active:true,currentWindow:true});
tabId=tab?.id;validTab=allowedPage(tab?.url);
$('start').disabled=true;$('select').disabled=!validTab;
if(!validTab)$('message').textContent='请切换到已登录的广大大（guangdada.net）页面。';
if(validTab){
  try{
    const [r]=await chrome.scripting.executeScript({target:{tabId},world:'MAIN',func:collectMedia,args:[false]});
    scan=r.result;scanReady=scan.cardCount>0;
    $('message').textContent=`本页 ${scan.cardCount} 张卡片，可跨页点选或自动采集。`;
  }catch(e){$('message').textContent=e.message;}
  await refreshBasket(true).catch(()=>{});
}
const dateSettings=(await chrome.storage.local.get('dateCutoffSettings')).dateCutoffSettings;
if(dateSettings){$('date-enabled').checked=!!dateSettings.enabled;$('date-since').value=dateSettings.since || '';}
await request('status').catch(e=>{$('message').textContent=e.message;});
setInterval(()=>{request('status').catch(()=>{});if(!busy)refreshBasket().catch(()=>{});},1500);
