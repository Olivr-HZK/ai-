import {allowedPage,normalize,pageLimit} from './media.js';
import {collectMedia} from './collector.js';
import {cartPages} from './selection.js';
import {buildReport} from './report.js';
const $=id=>document.getElementById(id);
let busy=false,validTab=false,scanReady=false,tabId,scan=null,latestJob=null,basket=null;
const active=job=>['collecting','awaiting-permission','fetching','packing','downloading'].includes(job?.phase);
function updateScope(){
  const mode=$('mode').value,multi=mode==='pages',untilEnd=$('end').value==='last';
  $('page-options').hidden=!multi;$('count-row').hidden=!multi || untilEnd;
  $('pages').disabled=busy || active(latestJob) || untilEnd;
  if(mode==='basket'){
    $('scope').textContent=`已选清单共 ${basket?.entries.length || 0} 张，覆盖 ${cartPages(basket?.entries || []).length} 页。可以继续手动翻页勾选，最后一起打包。`;
    $('start').textContent='打包跨页已选素材 ZIP';
  }else if(multi){
    $('scope').textContent=untilEnd?'从当前页开始一直采集到列表末页，每页获取全部所选类型，最后合成一个 ZIP。':`从当前页开始，最多采集 ${$('pages').value || '?'} 页（含当前页），遇末页提前停止。每页获取全部所选类型。`;
    $('start').textContent='翻页采集并打包一个 ZIP';
  }else{
    $('scope').textContent=`下载当前页全部 ${scan?.cardCount || 0} 张。需要挑选时，请点「跨页点选素材」。`;
    $('start').textContent='打包本页全部 ZIP';
  }
  $('cart').hidden=!basket?.active && !basket?.entries.length;
  $('cart-count').textContent=`跨页已选 ${basket?.entries.length || 0} 张`;
  $('cart-pages').replaceChildren(...cartPages(basket?.entries || []).map(page=>{
    const li=document.createElement('li');li.textContent=`第 ${page.pageNumber} 页：${page.selectedCardNumbers.join('、')}（${page.selectedCardNumbers.length} 张）`;return li;
  }));
  $('select').textContent=basket?.active || basket?.entries.length?'继续跨页点选素材':'跨页点选素材';
  $('clear-cart').disabled=busy || !basket?.entries.length;
  $('start').disabled=busy || !validTab || active(latestJob) || (mode==='basket'?!basket?.entries.length:!scanReady);
}
function render(job){
  latestJob=job;const running=active(job);
  $('select').disabled=busy || !validTab || running;
  $('resolve-external').disabled=busy || running;
  if(running)$('resolve-external').checked=job.autoResolveExternal!==false;
  $('mode').disabled=busy || running;$('end').disabled=busy || running;updateScope();
  if(!job)return;
  $('progress').hidden=false;
  const n=state=>job.items.filter(i=>i.state===state).length;
  $('bar').max=Math.max(1,job.phase==='collecting'?(job.collection?.targetPages || job.pages.length+1):job.items.length);
  $('bar').value=job.phase==='collecting'?job.pages.length:n('complete');
  const videos=job.items.filter(i=>i.kind==='video').length, images=job.items.filter(i=>i.kind==='image').length, playables=job.items.filter(i=>i.kind==='playable').length;
  const phases={collecting:'正在翻页采集','awaiting-permission':'素材已收集，等待媒体站点授权',fetching:'正在获取媒体',packing:'正在生成 ZIP',downloading:'正在保存 ZIP',complete:'ZIP 已保存',failed:'打包失败',stopped:'已停止'};
  const scope=job.mode==='pages'
    ? `已采集 ${job.pages.length}${job.collection.untilEnd?' 页 / 直到末页':`/${job.collection.targetPages} 页`} · ${job.cardCount} 张卡片${job.pages.length?`（第 ${job.pages[0].pageNumber}–${job.pages.at(-1).pageNumber} 页）`:''}`
    : job.mode==='selection'?`跨页选择 ${job.selectedCardCount} 张 · ${job.pages.length} 页`:`本页 ${job.cardCount ?? '?'} 张卡片 · 本批选择 ${job.selectedCardCount ?? job.selectedCardNumbers?.length ?? job.cardCount ?? '?'} 张`;
  $('counts').textContent=`${scope}\n视频 ${videos} · 图片 ${images} · Playable ${playables}\n已获取 ${n('complete')} · 获取中 ${n('active')} · 等待 ${n('pending')} · 失败 ${n('failed')}`;
  $('folder').textContent=`${phases[job.phase] || '旧版任务，请重新打包'}：${job.zipName || ''}`;
  $('errors').replaceChildren(...buildReport(job).selections.filter(i=>['failed','missing','unsupported','cancelled','unattempted'].includes(i.state)).map(item=>{
    const li=document.createElement('li');li.textContent=`${item.pageNumber?`第${item.pageNumber}页 `:''}第${item.cardIndex}个 · ${item.result}：${item.reason}`;return li;
  }));
  $('retry').disabled=busy || running || !job.zipName || job.collectionIncomplete || !job.items.length;
  $('stop').disabled=!running;
  $('authorize').hidden=job.phase!=='awaiting-permission';$('authorize').disabled=busy;
  $('message').textContent=[job.error || job.detailProgress || phases[job.phase] || '',
    job.phase==='collecting'?'请保持列表标签页打开，自动采集期间不要手动翻页或更改筛选。关闭插件弹窗仍会继续。':'',
    job.phase==='awaiting-permission'?(job.waitingForExternalPermission?'外链解析发现新的素材站点，请点击「授权并继续打包」；已获取文件会保留。':'点击「授权并继续打包」，仅授权本批实际使用的媒体站点。'):'',
    job.stopReason==='last-page'?'已到列表末页。':'',
    job.duplicateCount?`相同素材合并 ${job.duplicateCount} 项。`:'',
    job.missing?.length?`${job.missing.length} 张卡片缺少原素材或不支持，详见下载报告。`:'',
    job.phase==='complete'?'ZIP 包含成功获取的媒体和「下载报告.md」，可逐项核对。':''
  ].filter(Boolean).join('\n');
}
async function request(type,extra={}){
  const result=await chrome.runtime.sendMessage({type,...extra});
  if(!result?.ok)throw new Error(result?.error || '插件未响应');render(result.job);return result.job;
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
$('start').onclick=async()=>{
  const kinds=['image','video','playable'].filter(id=>$(id).checked),mode=$('mode').value,untilEnd=$('end').value==='last';
  if(!kinds.length){$('message').textContent='请至少选择图片、视频或 Playable。';return;}
  if(mode==='basket' && !basket?.entries.length){$('message').textContent='跨页清单为空，请先点选素材。';return;}
  if(mode!=='basket' && !scanReady){$('message').textContent='请等待列表加载后重新打开插件。';return;}
  let count=1;
  try{if(mode==='pages' && !untilEnd)count=pageLimit($('pages').value);}catch(e){$('message').textContent=e.message;return;}
  busy=true;render(latestJob);let errorText='';
  try{
    const source=mode==='basket'?basket.entries:scan.items;
    const items=normalize(source.filter(i=>i.url && kinds.includes(i.kind)));
    const origins=[...new Set(items.map(i=>`${new URL(i.url).origin}/*`))];
    const autoResolveExternal=$('resolve-external').checked;
    const message=mode==='basket'?{type:'start-basket',tabId,kinds,autoResolveExternal,expectedRevision:basket.revision}:{type:'start',tabId,kinds,autoResolveExternal,mode,pageCount:count,untilEnd,expectedCardKeys:scan.cardKeys};
    // Persist the job before a native permission prompt can destroy this popup.
    // The permission call still originates directly from the user's click.
    const started=chrome.runtime.sendMessage(message);
    const authorization=origins.length?chrome.permissions.request({origins}):Promise.resolve(true);
    const [result,granted]=await Promise.all([started,authorization]);
    if(!result?.ok)throw new Error(result?.error || '插件未响应');
    render(result.job);
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
for(const type of ['retry','stop'])$(type).onclick=()=>request(type).catch(e=>{$('message').textContent=e.message;});
$('downloads').onclick=()=>chrome.tabs.create({url:'chrome://downloads/'});
$('report').onclick=()=>chrome.tabs.create({url:chrome.runtime.getURL('report.html')});
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
await request('status').catch(e=>{$('message').textContent=e.message;});
setInterval(()=>{request('status').catch(()=>{});if(!busy)refreshBasket().catch(()=>{});},1500);
