import {allowedPage, normalize, fileName, parseSelection, pageLimit, mergeCreatives} from './media.js';
import {collectMedia} from './collector.js';
import {pageControl} from './pagination.js';
import {selectionContext,selectionEntry,refreshEntries,cartPages} from './selection.js';
import {selectionRows} from './report.js';
import {detailControl,needsDetail} from './detail.js';
let detailCancelled=false;
let detailJobs=0;
let chain = Promise.resolve();
const locked = fn => { const next = chain.then(fn); chain = next.catch(() => {}); return next; };
const read = async () => (await chrome.storage.local.get('job')).job;
const save = job => chrome.storage.local.set({job});
const running = job => ['collecting', 'awaiting-permission', 'fetching', 'packing', 'downloading'].includes(job?.phase);
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
async function closeOffscreen() { await chrome.offscreen.closeDocument().catch(() => {}); }
async function inspect(tabId, func, args = []) {
  const [result] = await chrome.scripting.executeScript({target:{tabId}, world:'MAIN', func, args});
  if (!result?.result) throw new Error('无法读取页面，请保持广大大列表标签页打开。');
  if (result.result.error) throw new Error(result.result.error);
  return result.result;
}
const detailReasons={DETAIL_NOT_OPENED:'未能打开对应素材详情，未取得详情原始链接',DETAIL_BUSY:'页面已有其他详情窗口，未读取其他素材的外链',DETAIL_ID_MISMATCH:'详情广告ID与所选卡片不一致，未读取外链',DETAIL_CARD_CHANGED:'卡片身份或排列变化，未读取外链',DETAIL_TIMEOUT:'已打开对应详情，但10秒内未读到明确的原始媒体链接',DETAIL_CLOSE_FAILED:'读取详情后无法确认关闭，请关闭详情再试'};
async function completeDetails(tabId,scan,numbers,job){
  const selected=new Set(numbers),targets=[...scan.items,...scan.missing].filter(i=>selected.has(i.cardIndex) && needsDetail(i));
  if(job)detailJobs++;
  try{
  for(const [index,item] of targets.entries()){
    if(detailCancelled)throw new Error('已停止详情外链补全。');
    if(job){job.detailProgress=`正在读取第 ${item.cardIndex} 个素材详情外链（${index+1}/${targets.length}）`;await save(job);}
    const expected={cardIndex:item.cardIndex,adId:item.adId};let result,opened=false,scroll;
    try{
      result=await inspect(tabId,detailControl,['open',expected]);opened=!!result.opened;scroll=result.scroll;
      const started=Date.now(),deadline=started+10000;
      while(!result.url && !result.failure && Date.now()<deadline){
        if(detailCancelled)throw new Error('已停止详情外链补全。');
        await new Promise(resolve=>setTimeout(resolve,200));
        result=await inspect(tabId,detailControl,['read',expected]);
        if(!result.url && !result.failure && !result.dialogVisible && !result.identityConfirmed && Date.now()-started>1200)result={failure:'DETAIL_NOT_OPENED'};
      }
      if(!result.url && !result.failure)result={failure:'DETAIL_TIMEOUT'};
    }finally{
      if(opened && result?.failure!=='DETAIL_ID_MISMATCH'){const closed=await inspect(tabId,detailControl,['close',{...expected,scroll}]);if(closed.failure)throw new Error(detailReasons[closed.failure] || '详情未能安全关闭。');
        for(let i=0;i<10;i++){if(!(await inspect(tabId,detailControl,['idle',expected])).visible)break;await new Promise(resolve=>setTimeout(resolve,200));if(i===9)throw new Error('详情未能确认关闭，请关闭详情后重新开始。');}}
    }
    if(result.url){
      const fresh={...item,url:result.url,candidateUrls:result.candidateUrls,detailSource:true};delete fresh.reason;delete fresh.reasonCode;
      scan.items=scan.items.filter(i=>i.cardIndex!==item.cardIndex).concat(fresh);scan.missing=scan.missing.filter(i=>i.cardIndex!==item.cardIndex);
    }else if(!item.url){item.reasonCode=result.failure;item.reason=`列表未提供可下载视频地址；${detailReasons[result.failure] || '详情未提供可确认的原始视频链接'}（未用封面代替）`;}
  }
  if(targets.length){const after=await inspect(tabId,collectMedia);if(!sameKeys(after.cardKeys,scan.cardKeys))throw new Error('读取详情时列表素材已变化，请回到原页重新打包。');}
  if(job)delete job.detailProgress;
  return scan;
  }catch(error){if(job){job.phase='failed';job.collectionIncomplete=true;job.error=error.message;delete job.detailProgress;await save(job);}throw error;}
  finally{if(job)detailJobs--;}
}
async function reconcile(job) {
  if (!job) return job;
  if(job.phase==='awaiting-permission' && (await chrome.permissions.contains({origins:job.mediaOrigins}))){
    const contexts=job.waitingForExternalPermission?await chrome.runtime.getContexts({contextTypes:['OFFSCREEN_DOCUMENT']}):[];
    if(contexts.length){
      job.phase='fetching';delete job.waitingForExternalPermission;await save(job);
      await chrome.runtime.sendMessage({target:'offscreen',type:'permission-updated'});return job;
    }
    return launch(job);
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
  await chrome.action.setBadgeText({text:running(job) ? (job.phase === 'collecting' ? '翻页' : 'ZIP') : job.phase === 'complete' ? '✓' : '!'});
  if (!running(job) || (job.phase === 'awaiting-permission' && !job.waitingForExternalPermission)) {
    await chrome.alarms.clear('zip-status');
    await closeOffscreen();
  }
  return job;
}
async function launch(job) {
  await closeOffscreen();
  job.phase = 'fetching'; job.error = ''; delete job.downloadId;
  delete job.waitingForExternalPermission;
  for (const item of job.items) { item.state = 'pending'; for(const key of ['error','errorCode','httpStatus','suggestion','bytes','playableInfo','externalInfo','resolvedUrl'])delete item[key]; }
  await save(job);
  try {
    await chrome.offscreen.createDocument({url:'offscreen.html', reasons:['BLOBS'], justification:'将采集的图片视频汇总为一个 ZIP 文件'});
    const result = await chrome.runtime.sendMessage({target:'offscreen', type:'run', job});
    if (!result?.ok) throw new Error(result?.error || '打包环境启动失败');
    await chrome.alarms.create('zip-status', {periodInMinutes:0.5});
  } catch (error) { job.phase = 'failed'; job.error = error.message; await save(job); }
  return job;
}
async function finishCollection(job, reason) {
  job.stopReason = reason;
  job.collectionIncomplete = false;
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
  return launch(job);
}
function appendPage(job, scan, pageNumber) {
  job.selections.push(...selectionRows(scan,pageNumber));
  const raw = scan.items.filter(i => job.requestedKinds.includes(i.kind)).map(i=>({...i,pageNumber}));
  job.rawCount += raw.length;
  job.items = mergeCreatives([...job.items, ...raw]);
  job.duplicateCount = job.rawCount - job.items.length;
  job.missing.push(...scan.missing.map(i=>({...i,pageNumber})));
  for (const name of ['cardCount','matchedCards','videoCards','imageCards','playableCards','unsupported']) job[name] += scan[name] || 0;
  job.selectedCardCount += scan.cardCount;
  job.pages.push({pageNumber, cardCount:scan.cardCount, selectedCardNumbers:parseSelection('',scan.cardCount), missingCount:scan.missing.length});
  job.collection.lastPage = {pageNumber,cardKeys:scan.cardKeys};
  job.collection.seenKeys.push(JSON.stringify(scan.cardKeys));
}
function beginWarming(c, meta) {
  c.stage='warming';c.readyPage=meta.pageNumber;c.readyKeys=meta.cardKeys;
  c.warmSince=Date.now();c.warmed=[];c.snapshot=null;c.mediaKey='';c.mediaSince=0;
}
function mergeSnapshot(previous, scan) {
  const items=new Map((previous?.items || []).map(item=>[item.cardIndex,item]));
  for(const item of scan.items)items.set(item.cardIndex,item);
  return {...scan,items:[...items.values()],missing:scan.missing.filter(item=>!items.has(item.cardIndex))};
}
async function collectStep(id) {
  const job = await read();
  if (job?.id !== id || job.phase !== 'collecting') return;
  try {
    const c = job.collection, tab = await chrome.tabs.get(c.tabId);
    if (!allowedPage(tab.url) || new URL(tab.url).origin + new URL(tab.url).pathname !== c.location) {
      throw new Error('广大大标签页已离开原列表，请回到列表后重新开始。');
    }
    if (c.stage === 'warming') {
      if (Date.now()-c.warmSince>60000) throw new Error('等待本页原媒体地址加载超时（60秒）。');
      const meta=await inspect(c.tabId,pageControl,['read',{pageNumber:c.readyPage,cardKeys:c.readyKeys}]);
      if (!meta.loading) {
        const scan=await inspect(c.tabId,collectMedia);
        if(!sameKeys(scan.cardKeys,c.readyKeys))throw new Error('补齐媒体地址时卡片身份已变化。');
        c.snapshot=mergeSnapshot(c.snapshot,scan);
        const mediaKey=JSON.stringify(c.snapshot.items.map(i=>[i.cardIndex,i.url]));
        if(mediaKey!==c.mediaKey){c.mediaKey=mediaKey;c.mediaSince=Date.now();}
        // Some cards expose their original URL only after entering the viewport.
        // Visit each unresolved media card once; keep URLs read on earlier views.
        const unresolved=c.snapshot.missing.filter(i=>i.kind && !c.warmed.includes(i.cardIndex));
        if(unresolved.length){
          const cardIndex=unresolved[0].cardIndex;
          await inspect(c.tabId,pageControl,['warm',{pageNumber:c.readyPage,cardKeys:c.readyKeys,cardIndex}]);
          c.warmed.push(cardIndex);c.mediaSince=Date.now();
        } else if(Date.now()-c.mediaSince >= (c.snapshot.missing.some(i=>i.kind)?4000:1200)) {
          if(job.autoResolveExternal && job.requestedKinds.includes('video'))c.snapshot=await completeDetails(c.tabId,c.snapshot,c.snapshot.cardKeys.map((_,i)=>i+1),job);
          appendPage(job,c.snapshot,c.readyPage);
          delete c.snapshot;
          if(!meta.hasNext)return await finishCollection(job,'last-page');
          if(c.targetPages && job.pages.length>=c.targetPages)return await finishCollection(job,'page-limit');
          c.stage='next';
        }
      }
    } else if (c.stage === 'next') {
      // Persist intent before the click: a worker restart must never click twice.
      c.stage = 'waiting'; c.waitSince = Date.now(); c.stableKey = ''; c.stableSince = 0;
      await save(job);
      const result = await inspect(c.tabId, pageControl, ['next',c.lastPage]);
      if (!result.clicked) return await finishCollection(job,'last-page');
    } else {
      if (Date.now() - c.waitSince > 30000) throw new Error('等待下一页超时（30秒），请检查登录、安全验证或页面加载情况。');
      let meta;
      try { meta = await inspect(c.tabId,pageControl); }
      catch { scheduleCollection(id); return; } // The list may be temporarily unmounted.
      if (meta.pageNumber !== c.lastPage.pageNumber && meta.pageNumber !== c.lastPage.pageNumber + 1) {
        throw new Error('页码未按顺序前进，请勿在采集期间手动翻页或更改筛选。');
      }
      const ready = !meta.loading && meta.pageNumber === c.lastPage.pageNumber + 1 && meta.cardKeys.length && meta.cardKeys.every(Boolean) && !sameKeys(meta.cardKeys,c.lastPage.cardKeys);
      if (ready) {
        const fingerprint = JSON.stringify(meta.cardKeys);
        if (c.seenKeys.includes(fingerprint)) throw new Error('翻页后出现已采集过的整页素材，已停止以免重复。');
        if (fingerprint !== c.stableKey) { c.stableKey = fingerprint; c.stableSince = Date.now(); }
        else if (Date.now() - c.stableSince >= 1000) {
          const scan = await inspect(c.tabId,collectMedia);
          await inspect(c.tabId,pageControl,['read',{pageNumber:meta.pageNumber,cardKeys:scan.cardKeys}]);
          if (!sameKeys(scan.cardKeys,meta.cardKeys)) throw new Error('页面素材仍在变化，请等待加载完成后重试。');
          beginWarming(c,meta);
        }
      } else { c.stableKey = ''; c.stableSince = 0; }
    }
    await save(job); scheduleCollection(id,c.stage==='warming'?200:500);
  } catch (error) {
    job.phase = 'failed'; job.collectionIncomplete = true;
    job.error = `翻页采集未完成：${error.message} 已采集 ${job.pages.length} 页，未生成 ZIP，请处理后重新开始。`;
    await save(job); await reconcile(job);
  }
}
const carts = async () => (await chrome.storage.local.get('selectionCarts')).selectionCarts || {};
async function getCart(tabId){return (await carts())[tabId] || null;}
async function saveCart(tabId,cart){const all=await carts();all[tabId]=cart;await chrome.storage.local.set({selectionCarts:all});}
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
    if(cart){cart.entries=[];cart.revision++;await saveCart(tabId,cart);}
    return {ok:true,basket:cart};
  }
  const tab=await chrome.tabs.get(tabId);
  if(!allowedPage(tab.url))throw new Error('请回到广大大展示广告列表。');
  const job=await read();
  if(message.type==='selection-session'){
    if(running(job))throw new Error('请先等待打包任务完成或停止。');
    cart ||= {entries:[],revision:0,active:false};
    cart.active=true;await saveCart(tabId,cart);
    await chrome.scripting.executeScript({target:{tabId},files:['selection-ui.js']});
    return {ok:true,basket:cart};
  }
  if(!cart?.active)return {ok:true,active:false};
  if(job?.phase==='collecting')return {ok:true,active:true,paused:true};
  if(message.type==='selection-update' && message.action==='hide'){
    cart.active=false;await saveCart(tabId,cart);return {ok:true,active:false};
  }
  let {scan,pageNumber}=await selectionSnapshot(tabId);
  if(message.type==='selection-update' && ['toggle','all'].includes(message.action)){
    const numbers=message.action==='all'?scan.cardKeys.map((_,i)=>i+1):cart.entries.some(i=>i.adId===scan.cardKeys[message.cardIndex-1])?[]:[message.cardIndex];
    detailCancelled=false;scan=await completeDetails(tabId,scan,numbers);
  }
  const refreshed=refreshEntries(cart.entries,scan);
  let changed=JSON.stringify(refreshed)!==JSON.stringify(cart.entries);cart.entries=refreshed;
  if(message.type==='selection-update'){
    if(message.expectedPage!==pageNumber || !sameKeys(message.expectedCardKeys,scan.cardKeys))throw new Error('页面素材已变化，此次点击未保存，请重新点选。');
    const ids=new Set(scan.cardKeys.filter(Boolean));
    if(message.action==='clear')cart.entries=[];
    else if(message.action==='none')cart.entries=cart.entries.filter(i=>!ids.has(i.adId));
    else if(message.action==='all'){
      const selected=new Set(cart.entries.map(i=>i.adId));
      scan.cardKeys.forEach((id,index)=>{if(id && !selected.has(id)){cart.entries.push(selectionEntry(scan,pageNumber,index+1));selected.add(id);}});
    }else if(message.action==='toggle'){
      const entry=selectionEntry(scan,pageNumber,message.cardIndex);
      if(cart.entries.some(i=>i.adId===entry.adId))cart.entries=cart.entries.filter(i=>i.adId!==entry.adId);
      else cart.entries.push(entry);
    }else throw new Error('未知的点选操作。');
    cart.revision++;changed=true;
  }
  if(changed)await saveCart(tabId,cart);
  const selectedAdIds=cart.entries.map(i=>i.adId),picked=new Set(selectedAdIds);
  return {ok:true,active:true,revision:cart.revision,total:cart.entries.length,pages:cartPages(cart.entries),pageNumber,cardKeys:scan.cardKeys,selectedAdIds,selectedOnPage:scan.cardKeys.filter(id=>picked.has(id)).length};
}
chrome.runtime.onMessage.addListener((message, sender, respond) => {
  if(message.target==='offscreen' || sender.id!==chrome.runtime.id)return;
  const internal=!!sender.url?.startsWith(chrome.runtime.getURL(''));
  if(internal && message.type==='stop')detailCancelled=true;
  // Read-only popup state must remain available while the serialized collector
  // is waiting on a detail; otherwise reopening the popup cannot show Stop.
  if(internal && detailJobs && ['status','get-basket'].includes(message.type)){
    (message.type==='status'?read().then(job=>({ok:true,job})):getCart(message.tabId).then(basket=>({ok:true,basket}))).then(respond,error=>respond({ok:false,error:error.message}));return true;
  }
  const pageMessage=['selection-read','selection-update'].includes(message.type) && sender.frameId===0 && allowedPage(sender.tab?.url);
  if(!internal && !pageMessage)return;
  if(['selection-session','get-basket','clear-basket','selection-read','selection-update'].includes(message.type)){
    locked(()=>selectionMessage(message,sender,internal)).then(respond,error=>respond({ok:false,error:error.message}));return true;
  }
  locked(async () => {
    let job = await read();
    if (sender.url === chrome.runtime.getURL('offscreen.html')) {
      if (!job || message.id !== job.id || !running(job)) return job;
      if(message.type==='need-origin'){
        if(!job.items[message.index] || job.items[message.index].kind!=='video' || job.autoResolveExternal===false)throw new Error('无效外链权限请求');
        const u=new URL(message.url);if(!['http:','https:'].includes(u.protocol) || u.username || u.password)throw new Error('无效媒体站点');
        const origin=`${u.origin}/*`,granted=await chrome.permissions.contains({origins:[origin]});
        if(!granted){job.mediaOrigins=[...new Set([...(job.mediaOrigins || []),origin])];job.waitingForExternalPermission=true;job.phase='awaiting-permission';await save(job);}
        return {granted};
      }
      if (message.type === 'item' && job.items[message.index]) {
        Object.assign(job.items[message.index], {state:message.state,error:message.error || '',errorCode:message.errorCode || '',httpStatus:message.httpStatus || null,suggestion:message.suggestion || '',bytes:message.bytes || 0,playableInfo:message.playableInfo || null,externalInfo:message.externalInfo || null,resolvedUrl:message.resolvedUrl || null});
        if(message.filename)job.items[message.index].filename=message.filename;
      }
      else if (message.type === 'packing') job.phase = 'packing';
      else if (message.type === 'pack-error') { job.phase = 'failed'; job.error = message.error; }
      else if (message.type === 'zip-ready') {
        if (!message.url.startsWith(`blob:${chrome.runtime.getURL('')}`)) throw new Error('无效 ZIP 地址');
        try {
          job.downloadId = await chrome.downloads.download({url:message.url,filename:job.zipName,conflictAction:'uniquify',saveAs:false});
          job.phase = 'downloading';
        } catch (error) { job.phase = 'failed'; job.error = error.message; }
      }
      await save(job); return job;
    }
    if(message.type==='start-basket'){
      job=await reconcile(job);if(running(job))throw new Error('已有任务，请先等待完成或停止。');
      const tab=await chrome.tabs.get(message.tabId);
      if(!allowedPage(tab.url))throw new Error('请在广大大标签页打包已选清单。');
      let cart=await getCart(tab.id);
      if(!cart?.entries.length)throw new Error('跨页清单为空，请先点选素材。');
      if(message.expectedRevision!==undefined && message.expectedRevision!==cart.revision)throw new Error('已选清单发生变化，请核对后再次打包。');
      detailCancelled=false;
      const kinds=['image','video','playable'].filter(k=>message.kinds?.includes(k));if(!kinds.length)throw new Error('请至少选择图片、视频或 Playable。');
      // Refresh the current page, including old choices made before detail
      // capture existed. Previously captured pages keep their observed links.
      const {scan:currentScan}=await selectionSnapshot(tab.id);
      if(message.autoResolveExternal!==false && kinds.includes('video'))await completeDetails(tab.id,currentScan,currentScan.cardKeys.flatMap((id,i)=>cart.entries.some(e=>e.adId===id)?[i+1]:[]));
      cart.entries=refreshEntries(cart.entries,currentScan);await saveCart(tab.id,cart);
      const raw=cart.entries.filter(i=>i.url && kinds.includes(i.kind));
      const items=mergeCreatives(raw),pages=cartPages(cart.entries),missing=cart.entries.filter(i=>!i.url);
      const stamp=new Date().toISOString().replace(/[:.]/g,'-');
      job={id:crypto.randomUUID(),createdAt:new Date().toISOString(),zipName:`广大大/广大大媒体_${stamp}.zip`,mode:'selection',autoResolveExternal:message.autoResolveExternal!==false,phase:'collecting',selections:cart.entries.map(i=>({...i})),items,pages,missing,cardCount:pages.reduce((n,p)=>n+p.cardCount,0),selectedCardCount:cart.entries.length,matchedCards:cart.entries.length,videoCards:cart.entries.filter(i=>i.kind==='video').length,imageCards:cart.entries.filter(i=>i.kind==='image').length,playableCards:cart.entries.filter(i=>i.kind==='playable').length,unsupported:missing.filter(i=>!i.kind).length,duplicateCount:raw.length-items.length,requestedKinds:kinds};
      return finishCollection(job,'selected-pages');
    }
    if (message.type === 'start') {
      job = await reconcile(job);
      if (running(job)) throw new Error('已有任务，请先等待完成或停止。');
      const tab = await chrome.tabs.get(message.tabId);
      if (!allowedPage(tab.url)) throw new Error('请在已登录的 guangdada.net 页面使用。');
      const multi = message.mode === 'pages', untilEnd=multi && message.untilEnd===true;
      const targetPages=untilEnd?null:pageLimit(multi?message.pageCount:1);
      let scan = await inspect(tab.id,collectMedia);
      detailCancelled=false;
      if (message.expectedCardKeys && !sameKeys(message.expectedCardKeys,scan.cardKeys)) throw new Error('页面素材或排列已变化，请关闭并重新打开插件，核对编号后再下载。');
      const kinds = ['image','video','playable'].filter(k=>message.kinds?.includes(k));
      if (!kinds.length) throw new Error('请至少选择图片、视频或 Playable。');
      const stamp = new Date().toISOString().replace(/[:.]/g,'-');
      job = {id:crypto.randomUUID(),createdAt:new Date().toISOString(),zipName:`广大大/广大大媒体_${stamp}.zip`,mode:multi?'pages':'current',autoResolveExternal:message.autoResolveExternal!==false,phase:'collecting',selections:[],items:[],pages:[],missing:[],cardCount:0,matchedCards:0,videoCards:0,imageCards:0,playableCards:0,unsupported:0,selectedCardCount:0,duplicateCount:0,rawCount:0,requestedKinds:kinds};
      if (multi) {
        const meta = await inspect(tab.id,pageControl,['read',{cardKeys:scan.cardKeys,pageNumber:(await inspect(tab.id,pageControl)).pageNumber}]);
        if (meta.loading || scan.cardKeys.some(k=>!k)) throw new Error('页面尚未加载完成，请稍后再开始翻页。');
        job.collection = {tabId:tab.id,location:new URL(tab.url).origin+new URL(tab.url).pathname,targetPages,untilEnd,stage:'next',seenKeys:[]};
        beginWarming(job.collection,meta);
        await save(job); await chrome.alarms.create('zip-status',{periodInMinutes:0.5}); scheduleCollection(job.id);
        return job;
      }
      if (message.usePageSelection && (!scan.selectionActive || !scan.selectedCardNumbers.length)) throw new Error('尚未勾选素材，请先点击卡片左上角的编号。');
      job.selectedCardNumbers = parseSelection(message.usePageSelection?scan.selectedCardNumbers.join(','):message.selection,scan.cardCount);
      const selected = new Set(job.selectedCardNumbers);
      const {pageNumber}=await inspect(tab.id,selectionContext);
      // Persist the original choices before opening details so cancellation or
      // an interrupted read still has an honest per-selection report.
      job.selections=selectionRows(scan,pageNumber,job.selectedCardNumbers);
      job.selectedCardCount=selected.size;job.cardCount=scan.cardCount;
      job.missing=scan.missing.filter(i=>selected.has(i.cardIndex)).map(i=>({...i,pageNumber}));
      if(job.autoResolveExternal && kinds.includes('video'))scan=await completeDetails(tab.id,scan,job.selectedCardNumbers,job);
      job.selections=selectionRows(scan,pageNumber,job.selectedCardNumbers);
      const raw = scan.items.filter(i=>selected.has(i.cardIndex) && kinds.includes(i.kind)).map(i=>({...i,pageNumber}));
      job.items = normalize(raw);
      for (const name of ['cardCount','matchedCards','videoCards','imageCards','playableCards']) job[name] = scan[name] || 0;
      job.missing = scan.missing.filter(i=>selected.has(i.cardIndex)).map(i=>({...i,pageNumber}));
      job.pages=[{pageNumber,cardCount:scan.cardCount,selectedCardNumbers:job.selectedCardNumbers,missingCount:job.missing.length}];
      job.unsupported = job.missing.filter(i=>!i.kind).length;
      job.selectedCardCount = selected.size; job.duplicateCount = raw.length-job.items.length;
      return finishCollection(job,'current-page');
    }
    if (!job) return null;
    if (message.type === 'stop') {
      if (collectionTimer) { clearTimeout(collectionTimer.timer); collectionTimer = null; }
      if (job.downloadId !== undefined && job.phase === 'downloading') await chrome.downloads.cancel(job.downloadId).catch(()=>{});
      if (job.phase === 'collecting') job.collectionIncomplete = true;
      job.phase = 'stopped'; await save(job); await closeOffscreen();
      for (const item of job.items) if (['pending','active',undefined].includes(item.state)) { item.state='failed'; item.error='用户停止了本批任务'; item.errorCode='CANCELLED'; }
    }
    if (message.type === 'resume' || message.type === 'retry') {
      if (job.collectionIncomplete) throw new Error('翻页未完成，请回到列表重新开始采集。');
      if (message.type === 'resume' && job.phase !== 'awaiting-permission') throw new Error('当前没有等待授权的任务。');
      if (message.type === 'retry' && running(job)) throw new Error('请先停止当前任务');
      if (!job.zipName) throw new Error('请创建新版打包任务');
      if (!job.items.length) throw new Error('没有可重新获取的原素材地址，请查看报告并返回原页重新选择。');
      if (!(await chrome.permissions.contains({origins:job.mediaOrigins || originsFor(job.items)}))) throw new Error('请授权本批素材所需的媒体站点。');
      if(message.type==='resume' && job.waitingForExternalPermission)return reconcile(job);
      job.id = crypto.randomUUID(); return launch(job);
    }
    return reconcile(job);
  }).then(job=>respond({ok:true,job}),error=>respond({ok:false,error:error.message}));
  return true;
});
chrome.downloads.onChanged.addListener(delta => {
  locked(async()=>{const job=await read();if(job?.downloadId===delta.id)await reconcile(job);}).catch(console.error);
});
chrome.alarms.onAlarm.addListener(alarm => {
  if (alarm.name==='zip-status') locked(async()=>reconcile(await read())).catch(console.error);
});
// A permission prompt can close Chrome's extension popup. The saved job must
// continue without relying on code in that destroyed popup context.
chrome.permissions.onAdded.addListener(()=>{
  locked(async()=>{const job=await read();if(job?.phase==='awaiting-permission')await reconcile(job);}).catch(console.error);
});
// Resume a persisted pagination wait after a service-worker restart.
locked(async()=>{const job=await read();if(job?.phase==='collecting')scheduleCollection(job.id);}).catch(console.error);
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
