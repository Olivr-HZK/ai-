export const DETAIL_CLOSE_WAIT_MS=15000;
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));

// Retry observations only. The caller must never wrap a click/open/close here:
// a missing injection result does not prove that an action was not performed.
export async function retryObservation(read,{attempts=5,delay=250,sleep=pause,onRetry=async()=>{}}={}){
  for(let attempt=1;;attempt++)try{return await read();}
  catch(error){
    if(error.code!=='PAGE_READ_UNAVAILABLE' || attempt>=attempts){error.readAttempts=attempt;throw error;}
    await onRetry(attempt,error);await sleep(delay);
  }
}

// One identity-checked close click, then bounded read-only observations. A
// replacement/foreign dialog is never clicked, removed or treated as closed.
export async function waitForDetailClose({close,observe,adId,stopping=()=>false,onWait=async()=>{},now=Date.now,sleep=pause,timeout=DETAIL_CLOSE_WAIT_MS}){
  const start=now();let last;
  try{last=await close();}
  catch(error){
    // The close may have happened even if Chrome lost its return value.
    // Observe its result, never click again on an uncertain outcome.
    if(error.code!=='PAGE_READ_UNAVAILABLE' || error.diagnostic?.stage!=='result')throw error;
    last={closeResultUnavailable:true};
  }
  const fail=(code,message)=>Object.assign(new Error(message),{code,diagnostic:{action:'detail-close',expectedAdId:adId,actualAdId:last?.adId || null,detailVisible:last?.visible ?? null,waitedMs:now()-start,code}});
  if(last?.failure)throw fail(last.failure,'对应素材详情未能安全关闭');
  for(;;){
    last=await observe();
    if(last?.failure)throw fail(last.failure,'详情身份或窗口状态变化，未关闭其他窗口');
    if(last?.visible===false)return {closed:true,waitedMs:now()-start};
    if(last?.adId && last.adId!==adId)throw fail('DETAIL_ID_MISMATCH','关闭等待期间出现其他素材详情，未继续点击');
    if(stopping())return {closed:false,cancelled:true,waitedMs:now()-start};
    if(now()-start>=timeout)throw fail('DETAIL_CLOSE_TIMEOUT','等待对应素材详情关闭超过15秒；已停止继续采集，保存的地址保留');
    await onWait(now()-start,last);await sleep(250);
  }
}

// Date/detail lookups are useful work, not a failure to load media on a page.
export async function outsideWarmBudget(collection,work,now=Date.now){
  const started=now();
  try{return await work();}
  finally{
    const elapsed=Math.max(0,now()-started);
    collection.warmSince+=elapsed;
    if(collection.mediaSince)collection.mediaSince+=elapsed;
    collection.lookupElapsedMs=(collection.lookupElapsedMs || 0)+elapsed;
  }
}
