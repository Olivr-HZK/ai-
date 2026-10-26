import {allowedPage, normalize, fileName, parseSelection, pageLimit, mergeCreatives} from './media.js';
import {collectMedia} from './collector.js';
import {pageControl} from './pagination.js';
import {selectionContext,selectionEntry,refreshEntries,cartPages} from './selection.js';
import {selectionRows} from './report.js';
import {detailControl,needsDetail} from './detail.js';
import {rememberAddresses,syncAddresses,interruptAddresses,pageAccess} from './addresses.js';
import {dateLimit,datePlan,requireNewest} from './date-filter.js';
import {PAGE_SETTLE_MS,PAGE_REPAINT_WAIT_MS,canWaitForPage,pageMismatch,pageWaitMessage} from './page-stability.js';
import {recoveryJob} from './recovery.js';
import {clearDiskSessions} from './disk-store.js';
import {readMetadataEvidence,parseMetadata,mergeMetadata} from './metadata.js';
import {createTaskStore,taskActive,ownsDownload,nextQueued,taskSummary,taskName} from './tasks.js';
import {retryObservation,waitForDetailClose,outsideWarmBudget} from './readiness.js';
import {collectionContext,sameCollectionContext,supportsCollectionResume,canResumeCollection,initializeCollection,markPageComplete,pauseCollectionState,planCollectionResume,activateCollectionResume} from './collection-resume.js';
let detailCancelled=false;
const pauseRequests=new Set();
function checkCollectionPause(job){if(job && pauseRequests.has(job.id))throw Object.assign(new Error('用户暂停了地址采集，断点和已存地址保留'),{code:'COLLECTION_PAUSE_REQUEST'});}
let detailJobs=0;
let chain = Promise.resolve();
const locked = fn => { const next = chain.then(fn); chain = next.catch(() => {}); return next; };
const taskStore=createTaskStore(chrome.storage.local);
const read=id=>taskStore.get(id);
const save=job=>taskStore.save(job);
const running=taskActive;
const collecting=async()=>(await taskStore.all()).find(j=>j.phase==='collecting');
async function state(){return {job:await read(),tasks:(await taskStore.all()).map(taskSummary),collector:(await collecting())?.id || null};}
async function refreshRuntime(){
  const jobs=await taskStore.all(),download=jobs.find(ownsDownload),collector=jobs.find(j=>j.phase==='collecting');
  await chrome.action.setBadgeText({text:collector?'采址':download?'ZIP':jobs.some(j=>j.phase==='queued')?'排队':jobs.some(j=>j.phase==='awaiting-permission')?'授权':jobs.some(j=>j.phase==='collection-paused')?'暂停':jobs.length?'✓':''});
  if(jobs.some(running))await chrome.alarms.create('zip-status',{periodInMinutes:0.5});
  else await chrome.alarms.clear('zip-status');
}
let queueChain=Promise.resolve();
function queueLocked(fn){const pending=queueChain.then(fn);queueChain=pending.catch(()=>{});return pending;}
const cleanupTask=id=>queueLocked(()=>closeOffscreen(id));
function pumpQueue(){
  return queueLocked(async()=>{
    if(!(await taskStore.all()).some(ownsDownload)){
      const next=nextQueued(await taskStore.all());
      if(next)await launch(next);
    }
    await refreshRuntime();
  });
}
async function enqueue(job){
  job.queuedAt ||= new Date().toISOString();job.phase='queued';
  await save(job);await pumpQueue();return job;
}
const originsFor = items => [...new Set(items.map(item => `${new URL(item.url).origin}/*`))];
const sameKeys = (a,b) => JSON.stringify(a) === JSON.stringify(b);
let collectionTimer = null;
function scheduleCollection(id, delay = 500) {
  if (collectionTimer?.id === id) return;
  if (collectionTimer) clearTimeout(collectionTimer.timer);
  collectionTimer = {id, timer:setTimeout(() => {
    collectionTimer = null;
    locked(() => collectStep(id)).catch(console.error);
  }, delay)};
}
async function closeOffscreen(id=null) {
  const active=(await taskStore.all()).find(ownsDownload);
  if(active && (!id || active.id!==id))return false;
  const contexts=await chrome.runtime.getContexts({contextTypes:['OFFSCREEN_DOCUMENT']});
  if(contexts.length){
    const owner=await chrome.runtime.sendMessage({target:'offscreen',type:'owner'}).catch(()=>null);
    if(owner?.id && ((id && owner.id!==id) || (!id && ownsDownload(await read(owner.id)))))return false;
    await chrome.runtime.sendMessage({target:'offscreen',type:'cleanup',id:owner?.id}).catch(()=>{});
  }
  await chrome.offscreen.closeDocument().catch(()=>{});
  // Closing the document releases any Chrome file locks still held after an
  // aborted stream. Sweep from the worker only after that context has closed.
  await clearDiskSessions();
  return true;
}
async function inspect(tabId, func, args = []) {
  const operation=typeof args[0]==='string'?args[0]:'read',readOnly=func===collectMedia || func===collectionContext || func===selectionContext || func===readMetadataEvidence || (func===pageControl && operation==='read') || (func===detailControl && ['read','idle'].includes(operation));
  const expectedAdId=func===detailControl?args[1]?.adId:func===readMetadataEvidence?args[0]?.adId:null;
  const unavailable=stage=>Object.assign(new Error(`暂时未读到页面返回结果（${func.name}/${operation}，${stage}）；已保存地址保留。`),{code:'PAGE_READ_UNAVAILABLE',diagnostic:{action:operation,script:func.name,stage,tabId,expectedAdId}});
  await retryObservation(async()=>{
    const [access]=await chrome.scripting.executeScript({target:{tabId},world:'MAIN',func:pageAccess});
    if(!access?.result)throw unavailable('access');
    if(access.result.blocked)throw Object.assign(new Error(access.result.reason),{code:access.result.code});
  });
  const invoke=async()=>{
    const [value]=await chrome.scripting.executeScript({target:{tabId},world:'MAIN',func,args});
    if(!value?.result)throw unavailable('result');
    if(value.result.error)throw Object.assign(new Error(value.result.error),{code:value.result.code,diagnostic:value.result.diagnostic,notClicked:value.result.notClicked});
    return value;
  };
  const result=readOnly?await retryObservation(invoke):await invoke();
  if(func===collectMedia){
    const [metadata]=await chrome.scripting.executeScript({target:{tabId},world:'MAIN',func:readMetadataEvidence});
    const rows=metadata?.result?.rows || [];
    // Two reads can straddle a repaint. Never attach another card's evidence.
    for(const item of [...result.result.items,...result.result.missing]){
      const row=rows.find(e=>e.cardIndex===item.cardIndex && e.adId===item.adId);
      if(row)item.metadata=parseMetadata(row);
    }
  }
  return result.result;
}
const detailReasons={DETAIL_NOT_OPENED:'未能打开对应素材详情，未取得详情原始链接',DETAIL_BUSY:'页面已有其他详情窗口，未读取其他素材的外链',DETAIL_ID_MISMATCH:'详情广告ID与所选卡片不一致，未读取外链',DETAIL_CARD_CHANGED:'卡片身份或排列变化，未读取外链',DETAIL_TIMEOUT:'已打开对应详情，但10秒内未读到明确的原始媒体链接',DETAIL_CLOSE_FAILED:'读取详情后无法确认关闭，请关闭详情再试',DETAIL_CLOSE_TIMEOUT:'对应详情超过15秒仍未关闭，保存的地址保留'};
async function checkpointJob(job) {
  job.addressesSavedAt=new Date().toISOString();
  await save(job);
}
function recordPageAddresses(job,scan,pageNumber,numbers=scan.cardKeys.map((_,i)=>i+1)) {
  const plan=job.firstDiscoveredSince?datePlan(scan,job.firstDiscoveredSince,pageNumber):null;
  const rows=selectionRows(scan,pageNumber,numbers).map(row=>({...row,...plan?.rows[row.cardIndex-1]}));
  if(plan?.cutoff)job.dateCutoff=plan.cutoff;
  rememberAddresses(job,rows,{pageNumber,cardCount:scan.cardCount,selectedCardNumbers:numbers});
  return plan;
}
function createJob(mode,kinds,autoResolveExternal,name) {
  const createdAt=new Date().toISOString(),stamp=createdAt.replace(/[:.]/g,'-');
  const label=taskName(name,createdAt),safe=label.replace(/[<>:"/\\|?*\x00-\x1f]/g,'_').slice(0,40);
  return {id:crypto.randomUUID(),taskName:label,createdAt,zipName:`广大大/${safe}_${stamp}.zip`,mode,autoResolveExternal:autoResolveExternal!==false,addressFirst:true,addressStage:'collecting',phase:'collecting',selections:[],items:[],pages:[],missing:[],cardCount:0,matchedCards:0,videoCards:0,imageCards:0,playableCards:0,unsupported:0,selectedCardCount:0,duplicateCount:0,rawCount:0,requestedKinds:kinds};
}
async function completeDetails(tabId,scan,numbers,job,onCheckpoint=async()=>{},readDate=false) {
  const resolveExternal=job?job.autoResolveExternal!==false:true;
  const selected=new Set(numbers),targets=[...scan.items,...scan.missing].filter(i=>selected.has(i.cardIndex) && (readDate || ((!job || job.requestedKinds.includes(i.kind)) && resolveExternal && needsDetail(i))));
  detailJobs++;
  try {
    for(const [index,item] of targets.entries()) {
      if(detailCancelled)throw Object.assign(new Error('用户停止了地址采集'),{code:'CANCELLED'});
      checkCollectionPause(job);
      const needUrl=!readDate && resolveExternal && needsDetail(item);
      if(job){job.detailProgress=`正在保存第 ${item.cardIndex} 个素材${readDate?'首次发现日期':'详情地址'}（${index+1}/${targets.length}）`;await checkpointJob(job);}
      const expected={cardIndex:item.cardIndex,adId:item.adId,...(readDate?{readDate:true}:{})};let result,opened=false,scroll,detailMetadata;
      try {
        result=await inspect(tabId,detailControl,['open',expected]);opened=!!result.opened;scroll=result.scroll;
        const started=Date.now(),deadline=started+10000;
        const ready=()=>readDate?result.firstDiscovered:result.url;
        while(!ready() && !result.failure && Date.now()<deadline) {
          if(detailCancelled)throw Object.assign(new Error('用户停止了地址采集'),{code:'CANCELLED'});
          checkCollectionPause(job);
          await new Promise(resolve=>setTimeout(resolve,200));
          result=await inspect(tabId,detailControl,['read',expected]);
          if(!result.url && !result.failure && !result.dialogVisible && !result.identityConfirmed && Date.now()-started>1200)result={failure:'DETAIL_NOT_OPENED'};
        }
        if(!ready() && !result.failure)result={...result,failure:'DETAIL_TIMEOUT'};
        if(result.adId===item.adId){
          const evidence=await inspect(tabId,readMetadataEvidence,[expected]);
          if(evidence.rows?.[0])detailMetadata=parseMetadata(evidence.rows[0]);
        }
        // Information failure is separate from media failure. Keep a usable URL.
        item.metadata=mergeMetadata(item.metadata,detailMetadata) || parseMetadata({adId:item.adId});
        item.metadata.detailChecked=true;
        if(result.failure)item.metadata.detailFailure=detailReasons[result.failure] || '详情素材信息未读取到';
        else if(item.metadata.missingFields.length)item.metadata.detailFailure='对应详情未显示完整字段';
        if(readDate && result.firstDiscovered){
          scan.cardDates ||= scan.cardKeys.map(()=>null);
          scan.cardDates[item.cardIndex-1]={date:result.firstDiscovered,source:result.dateSource};
        } else if(!readDate && result.url) {
          const fresh={...item,url:result.url,candidateUrls:result.candidateUrls,detailSource:true};delete fresh.reason;delete fresh.reasonCode;
          scan.items=scan.items.filter(i=>i.cardIndex!==item.cardIndex).concat(fresh);scan.missing=scan.missing.filter(i=>i.cardIndex!==item.cardIndex);
        } else if(!readDate && needUrl && !item.url) {
          item.reasonCode=result.failure;item.reason=`列表未提供可下载视频地址；${detailReasons[result.failure] || '详情未提供可确认的原始视频链接'}（未用封面代替）`;
        }
        // Persist BEFORE closing a detail or touching the next card. A forced
        // logout during that next action must not discard this original URL.
        await onCheckpoint(scan);
        if(readDate && !result.firstDiscovered)throw Object.assign(new Error(`第 ${scan.pageNumber} 页第 ${item.cardIndex} 个素材未能核验首次发现日期：${detailReasons[result.failure] || '详情未提供有效日期范围'}；已停止继续采集，未核验素材不下载。`),{code:'DATE_UNAVAILABLE'});
      } finally {
        if(opened && result?.failure!=='DETAIL_ID_MISMATCH') {
          const closed=await waitForDetailClose({adId:item.adId,close:()=>inspect(tabId,detailControl,['close',{...expected,scroll}]),observe:()=>inspect(tabId,detailControl,['idle',expected]),stopping:()=>detailCancelled || !!job && pauseRequests.has(job.id),
            onWait:async elapsed=>{if(job){job.detailProgress=`等待第 ${item.cardIndex} 个详情关闭（${Math.floor(elapsed/1000)}/15秒）；地址已保存，请勿手动翻页。`;await save(job);}}});
          if(job && closed.waitedMs>2000){job.detailCloseRecoveries=(job.detailCloseRecoveries || 0)+1;await save(job);}
          if(closed.cancelled){checkCollectionPause(job);throw Object.assign(new Error('用户停止了地址采集'),{code:'CANCELLED'});}
        }
      }
    }
    checkCollectionPause(job);
    if(targets.length){const after=await inspect(tabId,collectMedia);if(!sameKeys(after.cardKeys,scan.cardKeys))throw new Error('地址采集时列表素材发生变化，后续采集已停止');if(job?.firstDiscoveredSince)requireNewest(after);}
    return scan;
  } finally {if(job)delete job.detailProgress;detailJobs--;}
}
async function completePageDates(job,scan,pageNumber) {
  let plan=datePlan(scan,job.firstDiscoveredSince,pageNumber);
  while(plan.pending){
    const index=plan.pending.cardIndex;
    if(!plan.pending.adId)throw Object.assign(new Error(`第 ${pageNumber} 页第 ${index} 个卡片身份未就绪，无法核验首次发现日期；未核验素材不下载。`),{code:'DATE_UNAVAILABLE'});
    scan=await completeDetails(job.collection.tabId,scan,[index],job,async updated=>{recordPageAddresses(job,updated,pageNumber);await checkpointJob(job);},true);
    if(!scan.cardDates?.[index-1])throw Object.assign(new Error(`第 ${pageNumber} 页第 ${index} 个素材未取得首次发现日期，已停止继续采集。`),{code:'DATE_UNAVAILABLE'});
    plan=datePlan(scan,job.firstDiscoveredSince,pageNumber);
  }
  return scan;
}
async function addressCollectionFailed(job,error) {
  if(error.diagnostic)job.collectionDiagnostic={...error.diagnostic,code:error.code || 'COLLECTION_CHECK_FAILED',pageNumber:job.collection?.readyPage || job.collection?.lastPage?.pageNumber || null,readAttempts:error.readAttempts || null};
  if(detailCancelled || error.code==='CANCELLED') {
    pauseRequests.delete(job.id);
    job.phase='stopped';job.collectionIncomplete=true;job.addressStage='stopped';job.error='用户停止了地址采集，已保存的地址保留，未开始下载';
    await save(job);return reconcile(job);
  }
  if(supportsCollectionResume(job))return pauseCollection(job,error);
  if(job.addressFirst) {
    interruptAddresses(job,error);
    return finishCollection(job,'collection-interrupted');
  }
  job.phase='failed';job.collectionIncomplete=true;job.error=`翻页采集未完成：${error.message} 已采集 ${job.pages.length} 页，未生成 ZIP，请处理后重新开始。`;
  await save(job);return reconcile(job);
}
async function pauseCollection(job,error){
  if(collectionTimer?.id===job.id){clearTimeout(collectionTimer.timer);collectionTimer=null;}
  pauseCollectionState(job,error);pauseRequests.delete(job.id);
  await save(job);await pumpQueue();return job;
}
async function reconcile(job) {
  if (!job) return job;
  if(job.phase==='awaiting-permission' && (await chrome.permissions.contains({origins:job.mediaOrigins}))){
    const contexts=job.waitingForExternalPermission?await chrome.runtime.getContexts({contextTypes:['OFFSCREEN_DOCUMENT']}):[];
    if(contexts.length){
      job.phase='fetching';delete job.waitingForExternalPermission;await save(job);
      await chrome.runtime.sendMessage({target:'offscreen',type:'permission-updated',id:job.id});return job;
    }
    return enqueue(job);
  }
  if (job.phase === 'collecting') scheduleCollection(job.id);
  else if (job.phase === 'downloading' && job.downloadId !== undefined) {
    const [download] = await chrome.downloads.search({id:job.downloadId});
    if (!download || download.state === 'interrupted') {
      job.phase = 'failed'; job.error = download?.error || 'ZIP 下载记录已移除';
    } else if (download.state === 'complete') job.phase = 'complete';
  } else if (['fetching', 'packing'].includes(job.phase)) {
    const contexts = await chrome.runtime.getContexts({contextTypes:['OFFSCREEN_DOCUMENT']});
    if (!contexts.length) { job.phase = 'failed'; job.error = '打包环境已关闭，请重新打包'; }
  }
  await save(job);
  if (!running(job))await cleanupTask(job.id);
  await pumpQueue();
  return job;
}
async function launch(job) {
  if((await taskStore.all()).some(j=>j.id!==job.id && ownsDownload(j)))throw new Error('另一任务正在下载，本任务已保留在队列。');
  if(!(await closeOffscreen()))throw new Error('另一任务仍占用下载环境，本任务已保留在队列。');
  job.storageMode='disk';delete job.archiveBytes;delete job.zip64;
  job.phase = 'fetching'; job.error = ''; delete job.downloadId;
  delete job.waitingForExternalPermission;
  for (const [index,item] of job.items.entries()) { item.filename ||= fileName(item,index,'媒体');item.state = 'pending'; for(const key of ['error','errorCode','httpStatus','suggestion','bytes','playableInfo','externalInfo','resolvedUrl'])delete item[key]; }
  await save(job);
  try {
    await chrome.offscreen.createDocument({url:'offscreen.html', reasons:['BLOBS'], justification:'将媒体流式写入本机临时磁盘，汇总为一个 ZIP 文件'});
    const result = await chrome.runtime.sendMessage({target:'offscreen', type:'run', job});
    if (!result?.ok) throw new Error(result?.error || '打包环境启动失败');
    await chrome.alarms.create('zip-status', {periodInMinutes:0.5});
  } catch (error) { job.phase = 'failed'; job.error = error.message; await save(job); }
  return job;
}
async function finishCollection(job, reason) {
  job.stopReason = reason;
  if(reason!=='collection-interrupted')job.collectionIncomplete=false;
  if(job.addressFirst){
    syncAddresses(job);job.addressStage=job.collectionIncomplete?'interrupted':'complete';job.addressCollectionFinishedAt=new Date().toISOString();
    await checkpointJob(job);
  }
  job.items = job.items.map((item,index) => ({...item, filename:fileName(item,index,'媒体'), state:'pending'}));
  if (!job.items.length) {
    job.phase='failed';job.error='采集范围内没有所选类型的可下载原素材地址，未生成 ZIP；请查看或保存下载报告。';
    await save(job);return reconcile(job);
  }
  job.mediaOrigins = originsFor(job.items);
  if (!(await chrome.permissions.contains({origins:job.mediaOrigins}))) {
    job.phase = 'awaiting-permission';
    await save(job); await reconcile(job); return job;
  }
  return enqueue(job);
}
function appendPage(job, scan, pageNumber) {
  markPageComplete(job,scan);
  recordPageAddresses(job,scan,pageNumber);
  job.collection.lastPage={pageNumber,cardKeys:scan.cardKeys};
  const fingerprint=JSON.stringify(scan.cardKeys);
  if(!job.collection.seenKeys.includes(fingerprint))job.collection.seenKeys.push(fingerprint);
}
function beginWarming(c, meta) {
  c.stage='warming';c.readyPage=meta.pageNumber;c.readyKeys=meta.cardKeys;
  c.warmSince=Date.now();c.warmed=[];c.snapshot=null;c.mediaKey='';c.mediaSince=0;
  if(c.resumeVersion===1){c.pendingPage={pageNumber:meta.pageNumber,cardKeys:[...meta.cardKeys]};c.clickIntent=null;}
}
function mergeSnapshot(previous, scan) {
  const items=new Map((previous?.items || []).map(item=>[item.cardIndex,item]));
  const oldRows=[...(previous?.items || []),...(previous?.missing || [])];
  for(const item of [...scan.items,...scan.missing])item.metadata=mergeMetadata(oldRows.find(e=>e.adId===item.adId)?.metadata,item.metadata);
  for(const item of scan.items){
    const previous=items.get(item.cardIndex);
    // Resuming can see the same incomplete list URL again. Keep the address
    // already verified in this creative's detail instead of opening it twice.
    items.set(item.cardIndex,previous?.adId===item.adId && previous.detailSource && needsDetail(item)?{...item,...previous,metadata:mergeMetadata(previous.metadata,item.metadata)}:item);
  }
  return {...scan,cardDates:scan.cardKeys.map((_,i)=>previous?.cardDates?.[i] || scan.cardDates?.[i] || null),items:[...items.values()],missing:scan.missing.filter(item=>!items.has(item.cardIndex))};
}
async function collectStep(id) {
  const job = await read(id);
  if (job?.id !== id || job.phase !== 'collecting') return;
  let identityConfirmed=false;
  try {
    const c=job.collection;
    checkCollectionPause(job);
    if(!c || ['initializing','details','dates'].includes(c.stage))throw Object.assign(new Error('地址采集过程已中断或重启，继续处理上次已保存的地址'),{code:'ADDRESS_COLLECTION_RECOVERED'});
    const tab=await chrome.tabs.get(c.tabId);
    if (!allowedPage(tab.url) || new URL(tab.url).origin + new URL(tab.url).pathname !== c.location) {
      throw new Error('广大大标签页已离开原列表，请回到列表后重新开始。');
    }
    if(supportsCollectionResume(job) && !sameCollectionContext(await inspect(c.tabId,collectionContext),c.context))throw Object.assign(new Error('网页筛选、搜索或排序变化，采集已暂停，请恢复原条件后继续。'),{code:'COLLECTION_CONTEXT_CHANGED'});
    if (c.stage === 'warming') {
      if (Date.now()-c.warmSince>60000) throw new Error('等待本页原媒体地址加载超时（60秒）。');
      const meta=await inspect(c.tabId,pageControl,['read',{pageNumber:c.readyPage,cardKeys:c.readyKeys}]);
      if (!meta.loading) {
        const scan=await inspect(c.tabId,collectMedia);
        if(!sameKeys(scan.cardKeys,c.readyKeys))throw pageMismatch({pageNumber:c.readyPage,cardKeys:c.readyKeys},scan);
        identityConfirmed=true;
        if(job.firstDiscoveredSince)requireNewest(scan);
        c.snapshot=mergeSnapshot(c.snapshot,scan);
        recordPageAddresses(job,c.snapshot,c.readyPage);
        await checkpointJob(job);
        if(job.firstDiscoveredSince && datePlan(c.snapshot,job.firstDiscoveredSince,c.readyPage).pending){
          c.stage='dates';await checkpointJob(job);
          c.snapshot=await outsideWarmBudget(c,()=>completePageDates(job,c.snapshot,c.readyPage));
          c.stage='warming';recordPageAddresses(job,c.snapshot,c.readyPage);await checkpointJob(job);
        }
        const numbers=job.firstDiscoveredSince?datePlan(c.snapshot,job.firstDiscoveredSince,c.readyPage).included:c.snapshot.cardKeys.map((_,i)=>i+1);
        if(job.dateCutoff && !numbers.length){appendPage(job,c.snapshot,c.readyPage);delete c.snapshot;return await finishCollection(job,'date-cutoff');}
        const mediaKey=JSON.stringify(c.snapshot.items.map(i=>[i.cardIndex,i.url]));
        if(mediaKey!==c.mediaKey){c.mediaKey=mediaKey;c.mediaSince=Date.now();}
        // Some cards expose their original URL only after entering the viewport.
        // Visit each unresolved media card once; keep URLs read on earlier views.
        const unresolved=c.snapshot.missing.filter(i=>numbers.includes(i.cardIndex) && i.kind && !c.warmed.includes(i.cardIndex));
        if(unresolved.length){
          const cardIndex=unresolved[0].cardIndex;
          await inspect(c.tabId,pageControl,['warm',{pageNumber:c.readyPage,cardKeys:c.readyKeys,cardIndex}]);
          c.warmed.push(cardIndex);c.mediaSince=Date.now();
        } else if(Date.now()-c.mediaSince >= (c.snapshot.missing.some(i=>numbers.includes(i.cardIndex) && i.kind)?4000:1200)) {
          if(job.autoResolveExternal && job.requestedKinds.includes('video')){
            c.stage='details';await checkpointJob(job);
            c.snapshot=await outsideWarmBudget(c,()=>completeDetails(c.tabId,c.snapshot,numbers,job,async updated=>{recordPageAddresses(job,updated,c.readyPage);await checkpointJob(job);}));
            c.stage='warming';
          }
          appendPage(job,c.snapshot,c.readyPage);
          delete c.snapshot;
          if(job.dateCutoff)return await finishCollection(job,'date-cutoff');
          if(!meta.hasNext)return await finishCollection(job,'last-page');
          if(c.targetPages && job.pages.length>=c.targetPages)return await finishCollection(job,'page-limit');
          c.stage='next';
        }
      }
    } else if (c.stage === 'next') {
      // Do not persist click intent until the previous page has passed a fresh
      // identity check. A brief DOM repaint must not strand us in waiting.
      const before=await inspect(c.tabId,pageControl,['read',c.lastPage]);
      if(before.loading)throw Object.assign(new Error('页面仍在加载，未点击下一页。'),{code:'PAGE_LOADING',notClicked:true,diagnostic:{action:'next',expectedPage:c.lastPage.pageNumber,actualPage:before.pageNumber,expectedCards:c.lastPage.cardKeys.length,actualCards:before.cardKeys.length,loading:true}});
      identityConfirmed=true;
      // Persist intent before the click: a worker restart must never click twice.
      c.stage = 'waiting'; c.waitSince = Date.now(); c.stableKey = ''; c.stableSince = 0;
      if(c.resumeVersion===1)c.clickIntent={fromPage:c.lastPage.pageNumber,expectedPage:c.lastPage.pageNumber+1,outcome:'uncertain'};
      await save(job);
      if(pauseRequests.has(job.id)){if(c.clickIntent)c.clickIntent.outcome='not-clicked';c.stage='next';await save(job);checkCollectionPause(job);}
      let result;
      try {result=await inspect(c.tabId, pageControl, ['next',c.lastPage]);}
      catch(error){
        // Only the page script's explicit pre-click rejection is retryable.
        // A transport error can have an unknown click outcome; never click twice.
        if(error.notClicked){if(c.clickIntent)c.clickIntent.outcome='not-clicked';c.stage='next';}
        throw error;
      }
      if(c.clickIntent)c.clickIntent.outcome=result.clicked?'clicked':'not-clicked';
      if (!result.clicked) return await finishCollection(job,'last-page');
    } else {
      if (Date.now() - c.waitSince > 30000) throw new Error('等待下一页超时（30秒），请检查登录、安全验证或页面加载情况。');
      let meta;
      try { meta = await inspect(c.tabId,pageControl); }
      catch(error) {
        if(error.code?.startsWith('ADDRESS_'))throw error;
        scheduleCollection(id);return; // The list may be temporarily unmounted.
      }
      if (meta.pageNumber !== c.lastPage.pageNumber && meta.pageNumber !== c.lastPage.pageNumber + 1) {
        throw new Error('页码未按顺序前进，请勿在采集期间手动翻页或更改筛选。');
      }
      const ready = !meta.loading && meta.pageNumber === c.lastPage.pageNumber + 1 && meta.cardKeys.length && meta.cardKeys.every(Boolean) && !sameKeys(meta.cardKeys,c.lastPage.cardKeys);
      if (ready) {
        if(c.pendingPage?.pageNumber===meta.pageNumber && !sameKeys(c.pendingPage.cardKeys,meta.cardKeys))throw pageMismatch(c.pendingPage,meta);
        const fingerprint = JSON.stringify(meta.cardKeys);
        if (c.seenKeys.includes(fingerprint)) throw new Error('翻页后出现已采集过的整页素材，已停止以免重复。');
        if (fingerprint !== c.stableKey) { c.stableKey = fingerprint; c.stableSince = Date.now(); }
        else if (Date.now() - c.stableSince >= PAGE_SETTLE_MS) {
          const scan = await inspect(c.tabId,collectMedia);
          if (!sameKeys(scan.cardKeys,meta.cardKeys) || scan.pageNumber!==meta.pageNumber) throw pageMismatch({pageNumber:meta.pageNumber,cardKeys:meta.cardKeys},scan);
          identityConfirmed=true;
          if(job.firstDiscoveredSince)requireNewest(scan);
          if(c.resumeVersion===1)c.pendingPage={pageNumber:meta.pageNumber,cardKeys:[...scan.cardKeys]};
          recordPageAddresses(job,scan,meta.pageNumber);await checkpointJob(job);
          await inspect(c.tabId,pageControl,['read',{pageNumber:meta.pageNumber,cardKeys:scan.cardKeys}]);
          beginWarming(c,meta);c.snapshot=scan;
        }
      } else { c.stableKey = ''; c.stableSince = 0; }
    }
    if(identityConfirmed && c.identityWait){
      job.pageRecoveryCount=(job.pageRecoveryCount || 0)+1;
      delete c.identityWait;
      if(job.detailProgress?.startsWith('等待列表稳定：'))delete job.detailProgress;
      if(job.pageDiagnostic)job.pageDiagnostic.recovered=true;
    }
    checkCollectionPause(job);
    await save(job); scheduleCollection(id,c.stage==='warming'?200:500);
  } catch (error) {
    if(error.diagnostic){
      job.pageDiagnostic={...error.diagnostic,code:error.code || 'PAGE_CHECK_FAILED',stage:job.collection?.stage,observedAt:new Date().toISOString(),recovered:false};
    }
    if(canWaitForPage(error)){
      const c=job.collection,now=Date.now();
      c.identityWait ||= {since:now};
      job.pageDiagnostic.waitedMs=now-c.identityWait.since;
      if(now-c.identityWait.since<PAGE_REPAINT_WAIT_MS){
        job.detailProgress=pageWaitMessage(error.diagnostic);
        await checkpointJob(job);scheduleCollection(id,300);return job;
      }
      delete job.detailProgress;
      error.message=`等待列表稳定超过15秒，采集提前中断：期望第 ${error.diagnostic.expectedPage} 页，实际第 ${error.diagnostic.actualPage} 页；卡片 ${error.diagnostic.actualCards}/${error.diagnostic.expectedCards ?? '?'}${error.diagnostic.firstDifference?`，第 ${error.diagnostic.firstDifference} 个素材身份持续不一致`:''}。未将此次中断当作日期截止或列表末页。`;
    }
    return addressCollectionFailed(job,error);
  }
}
const carts = async () => (await chrome.storage.local.get('selectionCarts')).selectionCarts || {};
async function getCart(tabId){return (await carts())[tabId] || null;}
async function saveCart(tabId,cart){
  cart.entries=cart.entries.map(row=>row.url && !row.addressCapturedAt?{...row,addressCapturedAt:new Date().toISOString()}:row);
  const all=await carts();all[tabId]=cart;await chrome.storage.local.set({selectionCarts:all});
}
async function selectionSnapshot(tabId){
  const before=await inspect(tabId,selectionContext);
  if(before.loading)throw new Error('页面正在加载，已选清单保留，请加载完成后继续。');
  const scan=await inspect(tabId,collectMedia),after=await inspect(tabId,selectionContext);
  if(after.loading || before.pageNumber!==after.pageNumber)throw new Error('正在翻页，请稍后点选。');
  return {scan,pageNumber:after.pageNumber};
}
async function selectionMessage(message,sender,internal){
  const tabId=internal?message.tabId:sender.tab.id;
  if(!Number.isInteger(tabId))throw new Error('无法确定素材标签页。');
  let cart=await getCart(tabId);
  if(message.type==='get-basket')return {ok:true,basket:cart};
  if(message.type==='clear-basket'){
    if(await collecting())throw new Error('地址采集期间不能清空点选清单');
    if(cart){cart.entries=[];cart.revision++;await saveCart(tabId,cart);}
    return {ok:true,basket:cart};
  }
  const tab=await chrome.tabs.get(tabId);
  if(!allowedPage(tab.url))throw new Error('请回到广大大展示广告列表。');
  const collector=await collecting();
  if(message.type==='selection-session'){
    if(collector)throw new Error('仍有任务正在采集地址，请等采集结束后再选择下一批。');
    cart ||= {entries:[],revision:0,active:false};
    cart.active=true;await saveCart(tabId,cart);
    await chrome.scripting.executeScript({target:{tabId},files:['selection-ui.js']});
    return {ok:true,basket:cart};
  }
  if(!cart?.active)return {ok:true,active:false};
  if(collector)return {ok:true,active:true,paused:true};
  if(message.type==='selection-update' && message.action==='hide'){
    cart.active=false;await saveCart(tabId,cart);return {ok:true,active:false};
  }
  let {scan,pageNumber}=await selectionSnapshot(tabId);
  if(message.type==='selection-update' && (message.expectedPage!==pageNumber || !sameKeys(message.expectedCardKeys,scan.cardKeys)))throw new Error('页面素材已变化，此次点击未保存，请重新点选。');
  const refreshed=refreshEntries(cart.entries,scan);
  let changed=JSON.stringify(refreshed)!==JSON.stringify(cart.entries);cart.entries=refreshed;
  let numbers=[];
  if(message.type==='selection-update') {
    const ids=new Set(scan.cardKeys.filter(Boolean));
    if(message.action==='clear')cart.entries=[];
    else if(message.action==='none')cart.entries=cart.entries.filter(i=>!ids.has(i.adId));
    else if(message.action==='all') {
      const selected=new Set(cart.entries.map(i=>i.adId));
      scan.cardKeys.forEach((id,index)=>{if(id && !selected.has(id)){cart.entries.push(selectionEntry(scan,pageNumber,index+1));selected.add(id);}});
      numbers=scan.cardKeys.flatMap((id,i)=>cart.entries.some(e=>e.adId===id && (!e.url || needsDetail(e)))?[i+1]:[]);
    } else if(message.action==='toggle') {
      const entry=selectionEntry(scan,pageNumber,message.cardIndex);
      if(cart.entries.some(i=>i.adId===entry.adId))cart.entries=cart.entries.filter(i=>i.adId!==entry.adId);
      else {cart.entries.push(entry);numbers=[message.cardIndex];}
    } else throw new Error('未知的点选操作。');
    cart.revision++;changed=true;
  }
  // Commit every choice and every immediately available CDN URL first. A
  // logout during later detail lookups cannot lose the rest of this selection.
  if(changed)await saveCart(tabId,cart);
  if(numbers.length) {
    detailCancelled=false;
    try {
      scan=await completeDetails(tabId,scan,numbers,null,async updated=>{
        cart.entries=refreshEntries(cart.entries,updated);cart.revision++;await saveCart(tabId,cart);
      });
    } catch(error) {
      const selectedIds=new Set(numbers.map(n=>scan.cardKeys[n-1]));
      for(const row of cart.entries)if(!row.url && selectedIds.has(row.adId)){
        row.reasonCode=error.code || 'ADDRESS_COLLECTION_INTERRUPTED';row.reason=`未取得原地址：${error.message}；已保存的其他地址保留。`;
      }
      cart.addressWarning=error.message;cart.revision++;await saveCart(tabId,cart);
    }
  }
  const selectedAdIds=cart.entries.map(i=>i.adId),picked=new Set(selectedAdIds);
  return {ok:true,active:true,revision:cart.revision,total:cart.entries.length,pages:cartPages(cart.entries),pageNumber,cardKeys:scan.cardKeys,selectedAdIds,selectedOnPage:scan.cardKeys.filter(id=>picked.has(id)).length};
}
let transferChain=Promise.resolve();
function transferLocked(fn){const next=transferChain.then(fn);transferChain=next.catch(()=>{});return next;}
async function stopTask(job){
  if(!job || (!running(job) && job.phase!=='collection-paused'))throw new Error('此任务当前未在运行、暂停或排队');
  pauseRequests.delete(job.id);
  if(collectionTimer?.id===job.id){clearTimeout(collectionTimer.timer);collectionTimer=null;}
  if(job.downloadId!==undefined && job.phase==='downloading')await chrome.downloads.cancel(job.downloadId).catch(()=>{});
  if(['collecting','collection-paused'].includes(job.phase)){job.collectionIncomplete=true;job.addressStage='stopped';}
  job.phase='stopped';
  for(const item of job.items)if(['pending','active',undefined].includes(item.state)){item.state='failed';item.error='用户停止了本批任务';item.errorCode='CANCELLED';}
  await save(job);await cleanupTask(job.id);await pumpQueue();return job;
}
async function transferMessage(message){
  const job=await read(message.id);
  if(!job || !ownsDownload(job))throw new Error('下载任务已停止或不是当前下载任务');
  if(message.type==='need-origin'){
    if(!job.items[message.index] || job.items[message.index].kind!=='video' || job.autoResolveExternal===false)throw new Error('无效外链权限请求');
    const u=new URL(message.url);if(!['http:','https:'].includes(u.protocol) || u.username || u.password)throw new Error('无效媒体站点');
    const origin=`${u.origin}/*`,granted=await chrome.permissions.contains({origins:[origin]});
    if(!granted){job.mediaOrigins=[...new Set([...(job.mediaOrigins || []),origin])];job.waitingForExternalPermission=true;job.phase='awaiting-permission';await save(job);}
    return {granted,job};
  }
  if(message.type==='item' && job.items[message.index]){
    Object.assign(job.items[message.index],{state:message.state,error:message.error || '',errorCode:message.errorCode || '',httpStatus:message.httpStatus || null,suggestion:message.suggestion || '',bytes:message.bytes || 0,playableInfo:message.playableInfo || null,externalInfo:message.externalInfo || null,resolvedUrl:message.resolvedUrl || null});
    if(message.filename)job.items[message.index].filename=message.filename;
  }else if(message.type==='disk-ready')job.storageMode='disk';
  else if(message.type==='packing')job.phase='packing';
  else if(message.type==='pack-error'){job.phase='failed';job.error=message.error;job.errorCode=message.errorCode || '';}
  else if(message.type==='zip-ready'){
    if(!message.url.startsWith(`blob:${chrome.runtime.getURL('')}`))throw new Error('无效 ZIP 地址');
    job.storageMode=message.storageMode || job.storageMode;job.archiveBytes=message.archiveBytes;job.zip64=!!message.zip64;
    try{job.downloadId=await chrome.downloads.download({url:message.url,filename:job.zipName,conflictAction:'uniquify',saveAs:false});job.phase='downloading';}
    catch(error){job.phase='failed';job.error=error.message;}
  }
  await save(job);
  if(job.phase==='failed')void locked(()=>reconcile(job)).catch(console.error);
  return {job};
}
chrome.runtime.onMessage.addListener((message, sender, respond) => {
  if(message.target==='offscreen' || sender.id!==chrome.runtime.id)return;
  const internal=!!sender.url?.startsWith(chrome.runtime.getURL(''));
  if(sender.url===chrome.runtime.getURL('offscreen.html')){
    transferLocked(()=>transferMessage(message)).then(result=>respond({ok:true,...result}),error=>respond({ok:false,error:error.message}));return true;
  }
  if(internal && message.type==='select-task'){
    read(message.id).then(async job=>{if(!job || !message.id)throw new Error('找不到此任务');await taskStore.focus(job);return state();}).then(value=>respond({ok:true,...value}),error=>respond({ok:false,error:error.message}));return true;
  }
  if(internal && message.type==='stop'){
    read(message.id).then(job=>{
      if(job?.phase==='collecting'){
        pauseRequests.delete(job.id);
        detailCancelled=true;
        return locked(async()=>job.phase==='stopped'?job:stopTask(job));
      }
      return transferLocked(()=>stopTask(job));
    }).then(async job=>respond({ok:true,...await state(),job}),error=>respond({ok:false,error:error.message}));return true;
  }
  if(internal && message.type==='pause-collection'){
    read(message.id).then(job=>{
      if(job?.phase!=='collecting' || !supportsCollectionResume(job))throw new Error('当前没有支持暂停的翻页采集任务。');
      pauseRequests.add(job.id);
      return locked(async()=>{
        if(job.phase==='collection-paused')return job;
        if(job.phase!=='collecting'){pauseRequests.delete(job.id);throw new Error('地址采集已结束，未再暂停。');}
        return pauseCollection(job,Object.assign(new Error('用户暂停了地址采集，断点和已存地址保留'),{code:'COLLECTION_PAUSE_REQUEST'}));
      });
    }).then(async job=>respond({ok:true,...await state(),job}),error=>respond({ok:false,error:error.message}));return true;
  }
  // Read-only popup state must remain available while the serialized collector
  // is waiting on a detail; otherwise reopening the popup cannot show Stop.
  if(internal && detailJobs && ['status','get-basket'].includes(message.type)){
    (message.type==='status'?state().then(value=>({ok:true,...value})):getCart(message.tabId).then(basket=>({ok:true,basket}))).then(respond,error=>respond({ok:false,error:error.message}));return true;
  }
  const pageMessage=['selection-read','selection-update'].includes(message.type) && sender.frameId===0 && allowedPage(sender.tab?.url);
  if(!internal && !pageMessage)return;
  if(['selection-session','get-basket','clear-basket','selection-read','selection-update'].includes(message.type)){
    locked(()=>selectionMessage(message,sender,internal)).then(respond,error=>respond({ok:false,error:error.message}));return true;
  }
  locked(async () => {
    let job = await read(message.id);
    if(message.type==='start-basket') {
      if(await collecting())throw new Error('已有任务正在采集地址，请等地址采集结束后新建下一批。');
      const cart=await getCart(message.tabId);
      if(!cart?.entries.length)throw new Error('跨页清单为空，请先点选素材。');
      if(message.expectedRevision!==undefined && message.expectedRevision!==cart.revision)throw new Error('已选清单发生变化，请核对后再次打包。');
      const kinds=['image','video','playable'].filter(k=>message.kinds?.includes(k));if(!kinds.length)throw new Error('请至少选择图片、视频或 Playable。');
      detailCancelled=false;job=createJob('selection',kinds,message.autoResolveExternal,message.taskName);job.pages=cartPages(cart.entries);await taskStore.focus(job);
      rememberAddresses(job,cart.entries);await checkpointJob(job);
      try {
        // Fully captured choices can download without reading the site again,
        // including when the account has already been forced out.
        if(cart.entries.some(i=>kinds.includes(i.kind) && (!i.url || needsDetail(i)))) {
          const tab=await chrome.tabs.get(message.tabId);
          if(!allowedPage(tab.url))throw new Error('原列表标签页已离开广大大，未能继续获取缺失地址');
          const {scan}=await selectionSnapshot(tab.id);
          const update=async updated=>{
            cart.entries=refreshEntries(cart.entries,updated);cart.revision++;
            rememberAddresses(job,cart.entries);await checkpointJob(job);await saveCart(tab.id,cart);
          };
          await update(scan);
          if(job.autoResolveExternal && kinds.includes('video'))await completeDetails(tab.id,scan,scan.cardKeys.flatMap((id,i)=>cart.entries.some(e=>e.adId===id)?[i+1]:[]),job,update);
          for(const row of job.selections)if(!row.url && kinds.includes(row.kind) && !scan.cardKeys.includes(row.adId)){
            row.reasonCode='ADDRESS_NOT_ON_PAGE';row.reason='该选择未保存原地址，且当前页未显示该卡片；请返回原页补选。';
          }
        }
        return finishCollection(job,'selected-pages');
      } catch(error) {return addressCollectionFailed(job,error);}
    }
    if(message.type==='start') {
      if(await collecting())throw new Error('已有任务正在采集地址，请等地址采集结束后新建下一批。');
      const tab=await chrome.tabs.get(message.tabId);
      if(!allowedPage(tab.url))throw new Error('请在已登录的 guangdada.net 页面使用。');
      const multi=message.mode==='pages',untilEnd=multi && message.untilEnd===true;
      const firstDiscoveredSince=multi?dateLimit(message.firstDiscoveredSince):null;
      const targetPages=untilEnd?null:pageLimit(multi?message.pageCount:1);
      let scan=await inspect(tab.id,collectMedia);
      if(firstDiscoveredSince)requireNewest(scan);
      if(message.expectedCardKeys && !sameKeys(message.expectedCardKeys,scan.cardKeys))throw new Error('页面素材或排列已变化，请核对编号后再下载。');
      const kinds=['image','video','playable'].filter(k=>message.kinds?.includes(k));if(!kinds.length)throw new Error('请至少选择图片、视频或 Playable。');
      if(message.usePageSelection && (!scan.selectionActive || !scan.selectedCardNumbers.length))throw new Error('尚未勾选素材，请先点击卡片左上角的编号。');
      detailCancelled=false;job=createJob(multi?'pages':'current',kinds,message.autoResolveExternal,message.taskName);
      if(firstDiscoveredSince)job.firstDiscoveredSince=firstDiscoveredSince;
      const numbers=parseSelection(multi?'':message.usePageSelection?scan.selectedCardNumbers.join(','):message.selection,scan.cardCount);
      job.selectedCardNumbers=numbers;
      if(multi){
        const context=await inspect(tab.id,collectionContext);
        const verified=await inspect(tab.id,pageControl,['read',{pageNumber:scan.pageNumber,cardKeys:scan.cardKeys}]);
        if(verified.loading)throw new Error('页面正在加载，尚未创建采集任务。');
        job.collection={tabId:tab.id,location:new URL(tab.url).origin+new URL(tab.url).pathname,targetPages,untilEnd,stage:'initializing',seenKeys:[]};
        initializeCollection(job,context,scan);
      }
      recordPageAddresses(job,scan,scan.pageNumber,numbers);await taskStore.focus(job);await checkpointJob(job);
      try {
        const {pageNumber}=await inspect(tab.id,selectionContext);
        if(pageNumber!==scan.pageNumber)throw new Error('地址采集时页面页码发生变化');
        if(multi) {
          const meta=await inspect(tab.id,pageControl,['read',{cardKeys:scan.cardKeys,pageNumber}]);
          if(meta.loading || scan.cardKeys.some(k=>!k))throw new Error('页面尚未加载完成，未能继续地址采集');
          beginWarming(job.collection,meta);job.collection.snapshot=scan;
          await checkpointJob(job);await chrome.alarms.create('zip-status',{periodInMinutes:0.5});scheduleCollection(job.id);
          return job;
        }
        if(job.autoResolveExternal && kinds.includes('video'))scan=await completeDetails(tab.id,scan,numbers,job,async updated=>{recordPageAddresses(job,updated,pageNumber,numbers);await checkpointJob(job);});
        recordPageAddresses(job,scan,pageNumber,numbers);
        return finishCollection(job,'current-page');
      } catch(error) {return addressCollectionFailed(job,error);}
    }
    if (!job) return null;
    if(message.type==='resume-collection'){
      if(!canResumeCollection(job))throw new Error('当前任务没有可继续的采集断点；旧任务缺少筛选记录时请新建任务。');
      if(await collecting())throw new Error('另一任务正在采集地址，请先暂停它再恢复此任务。');
      const tab=await chrome.tabs.get(message.tabId);
      if(!allowedPage(tab.url))throw new Error('请在恢复原筛选的广大大列表页继续采集。');
      const context=await inspect(tab.id,collectionContext);
      const idle=await inspect(tab.id,detailControl,['idle',{}]);
      if(idle.visible || idle.failure)throw new Error('请先关闭素材详情或其他详情窗口，再继续采集。');
      const meta=await inspect(tab.id,pageControl);
      if(meta.loading)throw new Error('该页仍在加载，断点保留，请页面恢复后再继续。');
      const scan=await inspect(tab.id,collectMedia);
      await inspect(tab.id,pageControl,['read',{pageNumber:scan.pageNumber,cardKeys:scan.cardKeys}]);
      const after=await inspect(tab.id,collectionContext);
      if(!sameCollectionContext(context,after))throw new Error('核验期间筛选发生变化，断点保留，请稳定后再继续。');
      const plan=planCollectionResume(job,{...scan,loading:meta.loading},after);
      return transferLocked(async()=>{
        const latest=await read(job.id);
        if(latest!==job || !canResumeCollection(latest))throw new Error('任务状态已变化，未恢复采集，请重新核对。');
        const continued=structuredClone(job);activateCollectionResume(continued,plan,scan,tab.id);
        if(plan.stage==='warming')recordPageAddresses(continued,continued.collection.snapshot,scan.pageNumber);
        try{await save(continued);}catch(error){await save(job).catch(()=>{});throw new Error('恢复状态未能保存，任务保持暂停，请重试。');}
        detailCancelled=false;pauseRequests.delete(job.id);await refreshRuntime();scheduleCollection(continued.id,0);return continued;
      });
    }
    if(message.type==='download-collected'){
      return transferLocked(async()=>{
        const latest=await read(job.id);
        if(latest!==job || !canResumeCollection(latest))throw new Error('请先暂停可恢复的采集任务。');
        job.collection.finalizedAt=new Date().toISOString();
        return finishCollection(job,'collection-interrupted');
      });
    }
    if(message.type==='retry-failed'){
      if(running(job))throw new Error('请先等待当前任务完成或停止。');
      const next=recoveryJob(job,crypto.randomUUID());
      if(!(await chrome.permissions.contains({origins:next.mediaOrigins})))throw new Error('请先授权本批素材所需的媒体站点。');
      // Keep the original ledger/report available internally; never erase its
      // successful media or request new website pages during recovery.
      next.taskName=(job.taskName || '原任务')+' · 补下载';next.parentTaskId=job.id;next.phase='queued';
      await chrome.storage.local.set({recoverySource:job});await taskStore.focus(next);return enqueue(next);
    }
    if (message.type === 'resume' || message.type === 'retry') {
      if(job.phase==='collection-paused')throw new Error('采集已暂停，请选择继续采集，或结束采集并下载已有地址。');
      if(job.collectionIncomplete && !job.addressFirst)throw new Error('旧任务翻页未完成，请新建地址采集任务。');
      if (message.type === 'resume' && job.phase !== 'awaiting-permission') throw new Error('当前没有等待授权的任务。');
      if (message.type === 'retry' && running(job)) throw new Error('请先停止当前任务');
      if (!job.zipName) throw new Error('请创建新版打包任务');
      if (!job.items.length) throw new Error('没有可重新获取的原素材地址，请查看报告并返回原页重新选择。');
      if (!(await chrome.permissions.contains({origins:job.mediaOrigins || originsFor(job.items)}))) throw new Error('请授权本批素材所需的媒体站点。');
      if(message.type==='resume' && job.waitingForExternalPermission)return reconcile(job);
      if(message.type==='resume')return enqueue(job);
      const template=createJob(job.mode,job.requestedKinds,job.autoResolveExternal,(job.taskName || '原任务')+' · 重下');
      const next={...structuredClone(job),id:template.id,taskName:template.taskName,createdAt:template.createdAt,zipName:template.zipName,parentTaskId:job.id};
      next.phase='queued';delete next.downloadId;delete next.queuedAt;
      for(const item of next.items){item.state='pending';for(const key of ['error','errorCode','httpStatus','suggestion','bytes','playableInfo','externalInfo','resolvedUrl'])delete item[key];}
      await taskStore.focus(next);return enqueue(next);
    }
    if(message.type==='status'){
      for(const item of await taskStore.all())if(running(item))await reconcile(item);
    }else await reconcile(job);
    return job;
  }).then(async job=>respond({ok:true,...await state(),job:job || await read()}),error=>respond({ok:false,error:error.message}));
  return true;
});
chrome.downloads.onChanged.addListener(delta => {
  transferLocked(async()=>{const job=(await taskStore.all()).find(j=>j.downloadId===delta.id);if(job && ownsDownload(job))await reconcile(job);}).catch(console.error);
});
chrome.alarms.onAlarm.addListener(alarm => {
  if (alarm.name==='zip-status') locked(async()=>{for(const job of await taskStore.all())if(running(job))await reconcile(job);}).catch(console.error);
});
// A permission prompt can close Chrome's extension popup. The saved job must
// continue without relying on code in that destroyed popup context.
chrome.permissions.onAdded.addListener(()=>{
  locked(async()=>{for(const job of await taskStore.all())if(job.phase==='awaiting-permission')await reconcile(job);}).catch(console.error);
});
// Resume a persisted pagination wait after a service-worker restart.
locked(async()=>{
  for(const job of await taskStore.all())if(running(job))await reconcile(job);
  await pumpQueue();
}).catch(console.error);
// A same-origin document reload loses injected UI but keeps the saved cart.
chrome.tabs.onUpdated.addListener((tabId,change,tab)=>{
  if(change.status!=='complete' || !allowedPage(tab.url))return;
  void getCart(tabId).then(cart=>{
    if(cart?.active)return chrome.scripting.executeScript({target:{tabId},files:['selection-ui.js']});
  }).catch(()=>{});
});
chrome.tabs.onRemoved.addListener(tabId=>{
  locked(async()=>{const all=await carts();if(all[tabId]){delete all[tabId];await chrome.storage.local.set({selectionCarts:all});}}).catch(console.error);
});
