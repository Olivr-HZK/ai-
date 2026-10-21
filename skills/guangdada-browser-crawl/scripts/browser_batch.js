/* Paste in the authorized browser-tool REPL. No page.evaluate, API, sleep, or driver.
 * Adapters read visible UI using the tool's documented API; locator factories
 * resolve fresh visible card/close controls. persist must atomically save locally.
 */
async function gddCollectVisibleBatch({tab, scopeSha256, task, targetDate, page,
  startPosition=1, maxCards=10, batchBudgetMs=45000, detailTimeoutMs=20000,
  maxObservations=64, batchId, observeList, readDetail, cardLocator, closeLocator,
  persist, sha256}) {
  const required=[observeList,readDetail,cardLocator,closeLocator,persist,sha256];
  if(required.some(fn=>typeof fn!=='function'))throw Error('Missing browser batch adapter');
  if(!/^[a-z0-9-]+$/.test(task?.id || '') || !task.app_name || !task.app_id || !['iOS','Android'].includes(task.platform) || !scopeSha256 ||
     !/^[a-z0-9-]+$/.test(batchId || '') || !Number.isInteger(page) || page<1 ||
     !Number.isInteger(startPosition) || startPosition<1 || !Number.isInteger(maxCards) || maxCards<1 || maxCards>60 ||
     !Number.isFinite(batchBudgetMs) || batchBudgetMs<1 || batchBudgetMs>55000 ||
     !Number.isFinite(detailTimeoutMs) || detailTimeoutMs<1 || detailTimeoutMs>30000 ||
     !Number.isInteger(maxObservations) || maxObservations<1 || maxObservations>128)throw Error('Invalid bounded batch scope');
  const dateOK=s=>typeof s==='string' && /^\d{4}-\d{2}-\d{2}$/.test(s) && !isNaN(new Date(s+'T00:00:00Z')) && new Date(s+'T00:00:00Z').toISOString().slice(0,10)===s;
  if(!dateOK(targetDate))throw Error('Invalid fixed target date');
  const start=Date.now(), startedAt=new Date(start).toISOString(), completed=[],operations=[];
  let position=startPosition,phase='list',active=null,baseList=null,lastAdId=null,terminal=null;
  const prefix='evidence/batches/'+batchId;
  const clean=e=>String(e?.message || e).replace(/https?:\/\/[^\s<>"']+/g,'[URL omitted]');
  const snapshot=async(name)=>{
    const bytes=await tab.getScreenshot({emit:false});
    const png=[137,80,78,71,13,10,26,10].every((v,i)=>bytes[i]===v),jpg=bytes[0]===255&&bytes[1]===216&&bytes[2]===255;
    if(!png&&!jpg)throw Error('Invalid native screenshot');
    const path=prefix+'-'+name+(png?'.png':'.jpg');await persist(path,bytes);
    return {path,sha256:await sha256(bytes),format:png?'png':'jpg'};
  };
  const save=async(path,object)=>persist(path,JSON.stringify(object,null,2));
  const checkpoint=async(detailOpen)=>save(prefix+'-checkpoint.json',{
    producer:'gdd-browser-batch-checkpoint/v1',scope_sha256:scopeSha256,task_id:task.id,
    page,next_position:position,detail_open:detailOpen,completed,terminal,updated_at:new Date().toISOString()});
  const listCheck=l=>{
    if(l?.captcha || l?.login_required)throw Error('HUMAN_REQUIRED');
    if(l?.detail_open!==false || l?.page!==page || l?.app_id!==task.app_id || l?.platform!==task.platform ||
       !Number.isInteger(l?.card_count) || l.card_count<0 || l.card_count>60 || !l?.signature)throw Error('LIST_CONTEXT_CHANGED');
    if(baseList && (l.signature!==baseList.signature || l.card_count!==baseList.card_count))throw Error('LIST_CONTEXT_CHANGED');
  };
  const readiness=d=>{
    if(d?.captcha || d?.login_required)throw Error('HUMAN_REQUIRED');
    if(d?.loading!==false)return {ready:false,missing:['detail_loading']};
    if(d.app_id && d.app_id!==task.app_id || d.platform && d.platform!==task.platform)throw Error('DETAIL_IDENTITY_MISMATCH');
    const missing=[];
    if(d.app_id!==task.app_id || d.platform!==task.platform || !d.app_name)missing.push('identity');
    if(!/^[a-f0-9]{32}$/.test(d.ad_id || ''))missing.push('ad_id');
    if(lastAdId && d.ad_id===lastAdId)missing.push('stale_previous_detail');
    let u;try{u=new URL(d.detail_url);}catch{}
    if(!u || u.protocol!=='https:' || !(u.hostname==='guangdada.net'||u.hostname.endsWith('.guangdada.net')) || u.searchParams.get('id')!==d.ad_id)missing.push('detail_url');
    const raw=d.raw_text || '',range=raw.match(/(?<![\d-])(\d{4}-\d{2}-\d{2})\s*[~～至]\s*(\d{4}-\d{2}-\d{2})(?!\d)/);
    if(!range || !dateOK(range[1]) || !dateOK(range[2]) || range[1]>range[2])missing.push('visible_date_range');
    const estimate=String(d.impressions_estimate ?? '').replace(/[,，\s]/g,'');
    const inTarget=range && dateOK(range[1]) && range[1]===targetDate;
    if(inTarget && !/^(?:约|≈|~)?\d+(?:\.\d+)?[KkMmBb万亿]?$/.test(estimate))missing.push('estimate');
    if(!['视频','图片','试玩'].includes(d.media_type))missing.push('media_type');
    // Details need an observed source, not a playing/fully buffered video or recommendations.
    const http=s=>{try{return ['http:','https:'].includes(new URL(s).protocol);}catch{return false;}};
    const youtube=(d.links||[]).some(l=>{try{const h=new URL(l.href).hostname;return h==='youtu.be'||h==='youtube.com'||h.endsWith('.youtube.com');}catch{return false;}});
    const media=d.media_type==='视频' ? (d.observed_videos||[]).some(v=>http(v.src))||youtube :
      d.media_type==='图片' ? (d.observed_images||[]).some(i=>i.is_original===true && http(i.src)) :
      (d.observed_iframes||[]).some(i=>http(i.src));
    if(inTarget && !media && !(d.media_source_unavailable===true && /官方资源限制|原始链接|无可用素材/.test(raw)))missing.push('media_source');
    return {ready:!missing.length,missing,range:range?{start:range[1],end:range[2]}:null};
  };
  const summary=async(status,reason,error=null)=>{
    const result={producer:'gdd-browser-visible-batch/v1',scope_sha256:scopeSha256,task_id:task.id,
      page,status,reason,error,started_at:startedAt,ended_at:new Date().toISOString(),elapsed_ms:Date.now()-start,
      completed,next_position:position,terminal,operations:operations.map(o=>({...o,evidence_paths:o.evidence_paths.map(p=>'tasks/'+task.id+'/'+p)}))};
    await save(prefix+'-result.json',result);return result;
  };
  try {
    baseList=await observeList();listCheck(baseList);
    if(!baseList.card_count)return await summary('needs_attention','ZERO_REQUIRES_EXPLICIT_EVIDENCE');
    if(startPosition>baseList.card_count)throw Error('INVALID_RESUME_POSITION');
    while(position<=baseList.card_count && completed.length<maxCards){
      if(Date.now()-start>=batchBudgetMs)return await summary('yielded','BATCH_TIME_BUDGET');
      phase='list';const list=await observeList();listCheck(list);
      const slot=String(page).padStart(4,'0')+'-'+String(position).padStart(4,'0');
      const eventId=batchId+'-'+slot;
      active={id:eventId,name:'card.open',action:'card',page,card:position,started_at:new Date().toISOString(),evidence_paths:[]};
      const before=await snapshot(slot+'-before');active.evidence_paths.push(before.path);
      phase='open';const locator=await cardLocator({page,position,list});
      if(typeof locator?.click!=='function')throw Error('Missing fresh card locator');
      const clickedAt=new Date().toISOString();await locator.click();
      phase='readiness';const waitStart=Date.now();let detail,ready,observations=0;
      do {
        detail=await readDetail();observations++;ready=readiness(detail);
        if(ready.ready)break;
      }while(observations<maxObservations && Date.now()-waitStart<detailTimeoutMs && Date.now()-start<batchBudgetMs);
      const readyElapsedMs=Date.now()-waitStart;
      if(!ready.ready)throw Error('DETAIL_NOT_READY:'+ready.missing.join(','));
      if(completed.some(c=>c.ad_id===detail.ad_id))throw Error('DUPLICATE_AD_ID');
      phase='save';const after=await snapshot(slot+'-after');active.evidence_paths.push(after.path);
      const identity={app_id:detail.app_id,platform:detail.platform};
      const eventPath='evidence/clicks/'+eventId+'.json';
      const event={producer:'gdd-checked-click/v1',scope_sha256:scopeSha256,task_id:task.id,event_id:eventId,
        key:'card.identity',kind:'click',performed_at:clickedAt,status:'passed',error:null,
        expected:identity,after:identity,before:{page,position},images:{before,after},
        readiness:{observations,elapsed_ms:readyElapsedMs,required_fields:['identity','ad_id','detail_url','visible_date_range','media_type',...(ready.range.start===targetDate?['estimate','media_source']:[])]}};
      await save(eventPath,event);active.evidence_paths.push(eventPath);
      const cardPath='cards/'+slot+'.json';
      // Preserve the actually observed name; existing identity mapping rules apply on final validation.
      const card={...detail,scope_sha256:scopeSha256,page,position,detail_loaded:true,
        first_discovered_date:ready.range.start,first_discovered_source:'detail_date_range_start',ui_check:eventPath};
      await save(cardPath,card);active.evidence_paths.push(cardPath);
      completed.push({position,ad_id:detail.ad_id,card_file:cardPath,sha256:await sha256(JSON.stringify(card,null,2))});
      lastAdId=detail.ad_id;position++;
      if(ready.range.start<targetDate)terminal={reason:'first_discovered_before_target',ad_id:detail.ad_id,date:ready.range.start,position:position-1,screenshot_path:after.path,screenshot_sha256:after.sha256,raw_text:detail.raw_text};
      await checkpoint(true); // A close failure must not cause this saved card to reopen.
      phase='close';const close=await closeLocator();if(typeof close?.click!=='function')throw Error('Missing close locator');await close.click();
      phase='list_return';let returned,reads=0;const returnStart=Date.now();
      do {returned=await observeList();reads++;if(returned?.captcha||returned?.login_required)throw Error('HUMAN_REQUIRED');if(returned?.detail_open===false)break;}
      while(reads<maxObservations && Date.now()-returnStart<detailTimeoutMs && Date.now()-start<batchBudgetMs);
      listCheck(returned);await checkpoint(false);
      active.ended_at=new Date().toISOString();active.readiness_observations=observations;active.readiness_ms=event.readiness.elapsed_ms;
      operations.push(active);active=null;
      if(terminal)return await summary('cutoff','TARGET_DATE_CUTOFF');
    }
    return await summary('yielded',position>baseList.card_count?'PAGE_END_VERIFY_NEXT':'BATCH_CARD_LIMIT');
  }catch(exc){
    const message=clean(exc);
    if(active){active.ended_at=new Date().toISOString();active.error=message;operations.push(active);}
    // No retry/click/navigation on error. Return control with the last durable checkpoint.
    return await summary('needs_attention',phase,message);
  }
}
if(typeof module!=='undefined')module.exports={gddCollectVisibleBatch};
