import {needsDetail} from './detail.js';
import {mergeMetadata} from './metadata.js';
import {calendarDate} from './date-filter.js';

// Serialized into MAIN world. Capture visible filter/query controls, excluding
// media cards, transient dialogs and page navigation. Never read credentials.
export function collectionContext(){
  const visible=e=>!!e?.getClientRects().length && getComputedStyle(e).visibility!=='hidden';
  const excluded=e=>!!e.closest('.shadow-common-light.bg-white,.ant-pagination,[role="dialog"],.ant-modal-content,.ant-drawer-content,#gdd-download-selection-toolbar');
  const text=e=>String(e.innerText || e.textContent || '').replace(/\s+/g,' ').trim();
  const label=e=>e.getAttribute('aria-label') || e.getAttribute('placeholder') || e.getAttribute('name') || e.getAttribute('id') || '';
  const inputs=[...document.querySelectorAll('input,select,textarea')].filter(e=>visible(e) && !excluded(e) && !/password|hidden|file/i.test(e.type || '') && !/password|passwd|token|session|auth/i.test(`${e.name || ''} ${e.autocomplete || ''}`)).map(e=>({label:label(e),type:e.type || e.tagName,value:String(e.value || ''),checked:['radio','checkbox'].includes(e.type)?!!e.checked:null}));
  const selected=[...document.querySelectorAll('.ant-select-selection-item,.ant-tag,.ant-checkbox-wrapper-checked,.ant-radio-wrapper-checked,.ant-radio-button-wrapper-checked,[aria-checked="true"],[aria-selected="true"],[aria-pressed="true"],.text-blue-600')].filter(e=>visible(e) && !excluded(e) && text(e).length<=300).map(e=>({text:text(e),label:label(e),value:e.getAttribute('data-value') || e.getAttribute('value') || ''}));
  const chips=[];
  for(const e of document.querySelectorAll('span,div,label')){
    if(!visible(e) || excluded(e) || !/^已选\s*[:：]?$/.test(text(e)))continue;
    let root=e.parentElement;
    for(let depth=0;root && depth<5;depth++,root=root.parentElement){
      if(root.querySelector('.shadow-common-light.bg-white'))break;
      if(!/清空/.test(text(root)))continue;
      chips.push({text:text(root),images:[...root.querySelectorAll('img')].filter(visible).map(img=>{let path='';try{path=new URL(img.currentSrc || img.src,location.href).pathname;}catch{}return {alt:img.alt || '',title:img.title || '',path};})});break;
    }
  }
  const url=new URL(location.href);
  const query=[...url.searchParams].filter(([k])=>! /^(page|pageIndex|pageNum|page_number|current|offset)$/i.test(k) && !/token|password|session|authorization/i.test(k)).sort((a,b)=>JSON.stringify(a).localeCompare(JSON.stringify(b)));
  const sort=rows=>rows.sort((a,b)=>JSON.stringify(a).localeCompare(JSON.stringify(b)));
  return {version:1,location:url.origin+url.pathname,query,hash:url.hash,inputs:sort(inputs),selected:sort(selected),chips:sort(chips)};
}

const same=(a,b)=>JSON.stringify(a)===JSON.stringify(b);
const canonical=value=>Array.isArray(value)?value.map(canonical):value && typeof value==='object'?Object.fromEntries(Object.keys(value).sort().map(key=>[key,canonical(value[key])])):value;
export const sameCollectionContext=(a,b)=>a?.version===1 && b?.version===1 && JSON.stringify(canonical(a))===JSON.stringify(canonical(b));
const require=(ok,message)=>{if(!ok)throw Object.assign(new Error(message),{code:'COLLECTION_RESUME_MISMATCH'});};
const validKeys=keys=>Array.isArray(keys) && keys.length>0 && keys.every(k=>typeof k==='string' && k.length>0);
export const supportsCollectionResume=job=>job?.mode==='pages' && job.collection?.resumeVersion===1 && job.collection.context?.version===1;
export const canResumeCollection=job=>job?.phase==='collection-paused' && supportsCollectionResume(job);
export function resumePage(job){
  const c=job?.collection;
  if(!c)return null;
  if(['next','waiting'].includes(c.stage))return c.lastPage?.pageNumber+1 || null;
  return c.pendingPage?.pageNumber || c.readyPage || c.initialPage || null;
}
export function initializeCollection(job,context,scan){
  require(context?.version===1 && validKeys(scan.cardKeys) && Number.isInteger(scan.pageNumber),'页面或筛选信息尚未就绪，未建立可恢复断点。');
  Object.assign(job.collection,{resumeVersion:1,context:structuredClone(context),initialPage:scan.pageNumber,completedPages:[],pendingPage:{pageNumber:scan.pageNumber,cardKeys:[...scan.cardKeys]},readyPage:scan.pageNumber,readyKeys:[...scan.cardKeys],snapshot:structuredClone(scan)});
}
export function markPageComplete(job,scan){
  const c=job.collection;if(!supportsCollectionResume(job))return;
  c.completedPages ||= [];
  const previous=c.completedPages.find(p=>p.pageNumber===scan.pageNumber);
  require(!previous || same(previous.cardKeys,scan.cardKeys),'已完成页面的素材身份变化，断点未覆盖。');
  if(!previous)c.completedPages.push({pageNumber:scan.pageNumber,cardKeys:[...scan.cardKeys]});
  c.pendingPage=null;
}
export function pauseCollectionState(job,error,now=new Date().toISOString()){
  require(supportsCollectionResume(job),'旧任务未保存筛选断点，不能直接续采。');
  job.phase='collection-paused';job.addressStage='paused';job.collectionIncomplete=true;job.stopReason='collection-paused';
  job.collectionError=String(error.message || error);delete job.detailProgress;
  job.collection.pause={at:now,reason:job.collectionError,code:error.code || 'COLLECTION_INTERRUPTED',stage:job.collection.stage,pageNumber:resumePage(job)};
  job.collection.events ||= [];job.collection.events.push({type:'paused',...job.collection.pause});
  return job;
}
export function planCollectionResume(job,scan,context){
  require(canResumeCollection(job),'当前任务没有可继续的采集断点。');
  const c=job.collection;
  require(sameCollectionContext(c.context,context),'网页筛选、搜索或排序与原任务不同，请恢复原条件后继续。');
  require(!scan.loading && validKeys(scan.cardKeys) && Number.isInteger(scan.pageNumber),'该页尚未加载完整，请恢复页面后再继续。');
  if(job.firstDiscoveredSince)require(scan.newestFirst===true,'请恢复原任务的「最新创意」排序后继续。');
  if(['next','waiting'].includes(c.stage)){
    const from=c.lastPage;require(from && validKeys(from.cardKeys),'缺少上一页身份断点，不能继续翻页。');
    if(scan.pageNumber===from.pageNumber){
      require(same(scan.cardKeys,from.cardKeys),'上一页素材身份已变化，请恢复原列表。');
      require(c.stage==='next' || ['not-clicked','not-started'].includes(c.clickIntent?.outcome),`上次翻页结果尚未确认，请在网页手动打开第 ${from.pageNumber+1} 页，加载完成后再继续；插件不会重复点击下一页。`);
      return {stage:'next',pageNumber:from.pageNumber};
    }
    require(scan.pageNumber===from.pageNumber+1,`请回到第 ${from.pageNumber+1} 页继续，不能跳过未采集页面。`);
    require(!same(scan.cardKeys,from.cardKeys),'下一页仍显示上一页素材，请等页面更新完成。');
    const anchor=c.pendingPage;
    require(!anchor || anchor.pageNumber!==scan.pageNumber || same(anchor.cardKeys,scan.cardKeys),'此页已保存的素材身份发生变化，不能混入原任务。');
    require(!(c.seenKeys || []).includes(JSON.stringify(scan.cardKeys)),'此页整页素材已经采集，不能当成新的下一页。');
    return {stage:'warming',pageNumber:scan.pageNumber};
  }
  const anchor=c.pendingPage || {pageNumber:c.readyPage || c.initialPage,cardKeys:c.readyKeys};
  require(scan.pageNumber===anchor.pageNumber,`请恢复第 ${anchor.pageNumber} 页后继续采集，不能跳过卡住的页。`);
  require(same(scan.cardKeys,anchor.cardKeys),'卡住页的素材或顺序已变化，原选择保留，请恢复原列表或新建任务。');
  return {stage:'warming',pageNumber:scan.pageNumber};
}
export function restorePageScan(job,scan){
  const c=job.collection,previous=c.snapshot?.pageNumber===scan.pageNumber?c.snapshot:null;
  const saved=new Map((job.selections || []).filter(r=>r.pageNumber===scan.pageNumber).map(r=>[`${r.cardIndex}:${r.adId}`,r]));
  const items=[],missing=[];
  for(const fresh of [...scan.items,...scan.missing]){
    const old=saved.get(`${fresh.cardIndex}:${fresh.adId}`),metadata=mergeMetadata(old?.metadata,fresh.metadata);
    const row=old?.url && (!fresh.url || (old.detailSource && needsDetail(fresh)))?{...fresh,...old,metadata}:{...fresh,metadata};
    if(row.url){delete row.reason;delete row.reasonCode;items.push(row);}else missing.push(row);
  }
  const cardDates=scan.cardKeys.map((adId,i)=>{
    const old=saved.get(`${i+1}:${adId}`);
    return previous?.cardKeys?.[i]===adId && previous.cardDates?.[i] || (calendarDate(old?.firstDiscovered) && ['visible_card_date_range','visible_detail_date_range'].includes(old.dateSource)?{date:old.firstDiscovered,source:old.dateSource}:null) || scan.cardDates?.[i] || null;
  });
  return {...scan,items,missing,cardDates};
}
export function activateCollectionResume(job,plan,scan,tabId,now=Date.now()){
  const c=job.collection;
  if(plan.stage==='warming')Object.assign(c,{stage:'warming',readyPage:scan.pageNumber,readyKeys:[...scan.cardKeys],pendingPage:{pageNumber:scan.pageNumber,cardKeys:[...scan.cardKeys]},snapshot:restorePageScan(job,scan),warmed:[],warmSince:now,mediaSince:0,mediaKey:'',clickIntent:null});
  else{c.stage='next';c.clickIntent=null;}
  c.tabId=tabId;delete c.identityWait;delete c.pause;
  c.resumeCount=(c.resumeCount || 0)+1;c.events ||= [];c.events.push({type:'resumed',at:new Date(now).toISOString(),pageNumber:plan.pageNumber});
  job.phase='collecting';job.addressStage='collecting';job.collectionIncomplete=false;delete job.collectionError;delete job.error;delete job.detailProgress;delete job.stopReason;
  return job;
}
