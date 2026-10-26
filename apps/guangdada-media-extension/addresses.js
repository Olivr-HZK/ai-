import {mergeCreatives} from './media.js';
import {dateEligible} from './date-filter.js';
import {mergeMetadata} from './metadata.js';

// The saved selection ledger is the source of truth. Rebuild the download
// queue only from observed addresses; never substitute covers or guessed URLs.
const slot=row=>`${row.pageNumber ?? ''}:${row.cardIndex}:${row.adId || ''}`;
export function rememberAddresses(job, rows, page = null) {
  job.pages ||= [];
  const entries=new Map((job.selections || []).map(row=>[slot(row),row]));
  for(const row of rows){
    const old=entries.get(slot(row));
    // A login overlay or a later incomplete snapshot cannot erase an address
    // already captured for the same selected creative.
    const next=old?.url && !row.url?{...old,...Object.fromEntries(Object.entries(row).filter(([,v])=>v!==undefined)),url:old.url}:{...old,...row};
    next.metadata=mergeMetadata(old?.metadata,row.metadata);
    if(next.url){
      next.addressCapturedAt=old?.url===next.url?old.addressCapturedAt || new Date().toISOString():new Date().toISOString();
      delete next.reason;delete next.reasonCode;
    }
    entries.set(slot(row),next);
  }
  job.selections=[...entries.values()];
  if(page){
    const index=job.pages.findIndex(p=>p.pageNumber===page.pageNumber);
    if(index<0)job.pages.push({...page});else job.pages[index]={...job.pages[index],...page};
  }
  syncAddresses(job);
}
export function syncAddresses(job) {
  const rows=job.selections || [],raw=rows.filter(row=>dateEligible(job,row) && row.url && job.requestedKinds.includes(row.kind));
  job.items=mergeCreatives(raw);
  job.missing=rows.filter(row=>dateEligible(job,row) && !row.url);
  job.rawCount=raw.length;job.duplicateCount=raw.length-job.items.length;
  job.selectedCardCount=rows.length;
  job.cardCount=(job.pages || []).reduce((n,p)=>n+(p.cardCount || 0),0);
  job.matchedCards=rows.filter(row=>row.adId).length;
  for(const kind of ['video','image','playable'])job[`${kind}Cards`]=rows.filter(row=>row.kind===kind).length;
  job.unsupported=job.missing.filter(row=>!row.kind).length;
  for(const page of job.pages || [])page.missingCount=job.missing.filter(row=>row.pageNumber===page.pageNumber).length;
}
export function interruptAddresses(job, error) {
  job.collectionIncomplete=true;
  job.addressStage='interrupted';
  job.collectionError=String(error.message || error);
  const code=error.code || 'ADDRESS_COLLECTION_INTERRUPTED';
  for(const row of job.selections || []){
    if(row.url || (row.kind && !job.requestedKinds.includes(row.kind)) || ['UNSUPPORTED_TYPE','STREAM_MANIFEST','UNSUPPORTED_SCHEME','PLAYABLE_NOT_HTML'].includes(row.reasonCode))continue;
    row.reasonCode=code;
    row.reason=`未取得原地址：${job.collectionError}；此项未发起媒体请求。`;
  }
  syncAddresses(job);
}

// Read only visible login/verification UI; never access credentials or log in.
export function pageAccess() {
  const visible=e=>!!e.getClientRects().length && getComputedStyle(e).visibility!=='hidden';
  const dialogs=[...document.querySelectorAll('[role="dialog"],.ant-modal-content,.ant-message-notice,.ant-notification-notice')].filter(visible);
  const text=dialogs.map(e=>e.innerText).join('\n');
  if(/其他设备.*登录|强制退出|登录.*失效|登录.*过期|重新登录|请先登录/.test(text) || /\/(?:login|signin)(?:\/|$)/i.test(location.pathname))return {blocked:true,code:'ADDRESS_LOGIN_LOST',reason:'广大大提示登录失效或需要重新登录，地址采集已中断'};
  if(dialogs.some(e=>/验证码|安全验证/.test(e.innerText)))return {blocked:true,code:'ADDRESS_VERIFICATION_REQUIRED',reason:'页面出现验证码或安全验证，地址采集已停止等待人工处理'};
  return {blocked:false};
}
