import {normalize} from './media.js';
import {dateEligible} from './date-filter.js';
import {exportMetadata,metadataNames} from './metadata.js';
import {supportsCollectionResume,resumePage} from './collection-resume.js';

// Keep diagnostics independent of browser globals so both the ZIP and viewer
// use exactly the same accounting. Never copy signed media URLs into reports.
export function selectionRows(scan, pageNumber, numbers = scan.cardKeys.map((_,i)=>i+1)) {
  const records = new Map([...scan.missing, ...scan.items].map(item=>[item.cardIndex,item]));
  return numbers.map(cardIndex=>({...records.get(cardIndex), cardIndex, pageNumber,
    adId:scan.cardKeys[cardIndex-1] || null}));
}
export function failureDetail(code, status) {
  const http = Number(status);
  if (http) {
    const reasons = {
      401:'服务器要求身份验证；当前请求未被接受。',
      403:'服务器拒绝访问；可能涉及签名过期、防盗链或访问权限，无法仅凭状态码确定。',
      404:'服务器未找到该媒体，原链接可能已失效或文件已移除。',
      410:'服务器明确表示该媒体已不可用。',
      408:'服务器等待请求超时。',
      429:'服务器限制请求频率。'
    };
    return {code:`HTTP_${http}`, reason:`HTTP ${http}：${reasons[http] || (http>=500?'媒体服务器发生错误。':'媒体服务器返回失败状态。')}`,
      suggestion:http===429?'稍后再试，缩小本批选择范围。':http>=500?'稍后重试。':'回到对应页重新加载原素材、更新链接后再试；仍失败请检查网站是否可正常播放或下载。'};
  }
  const details = {
    TIMEOUT:['媒体下载超时（120秒）。','检查网络，稍后重新打包。'],
    BATCH_LIMIT:['本批媒体超过 512MB，此文件未加入 ZIP。','缩小页数或选择范围后分批打包。'],
    DISK_QUOTA:['本机临时磁盘空间不足，已停止继续下载，未生成不完整ZIP。','释放本机磁盘空间后，按已保存地址重新下载。'],
    DISK_IO:['本机临时文件读写或ZIP校验失败，已停止继续下载。','检查本机磁盘可用空间和Chrome状态，按已保存地址重试。'],
    DISK_UNAVAILABLE:['当前浏览器无法使用本机临时磁盘，未开始媒体下载。','更新Chrome并重新加载插件后重试。'],
    TASK_STATE:['任务进度无法保存，已停止生成ZIP。','检查Chrome状态和本机空间后重试；未保存的媒体不会冒充已交付。'],
    NOT_MEDIA:['服务器返回网页或 JSON，未当作媒体保存。','回到原页确认登录及原素材可用，重新加载后再试。'],
    PLAYABLE_NOT_HTML:['服务器或卡片未提供可识别的 Playable HTML 原文件；未用图片、视频、JSON或错误网页代替。','回到试玩卡片重新加载，确认原试玩可打开后重新提交。'],
    PLAYABLE_LOGIN_PAGE:['获取 Playable 时返回了登录或授权页面，未当作试玩保存。','确认广大大登录和试玩访问权限，回到对应页重新加载后提交。'],
    EMPTY_RESPONSE:['服务器未返回媒体内容或返回空文件。','重新加载原素材后再试。'],
    NETWORK:['网络请求失败；浏览器未提供 HTTP 状态，无法确定具体原因。','检查网络、CDN授权及网站原素材是否可用；可能涉及连接、跨域或重定向限制。'],
    TRANSFER_INTERRUPTED:['服务器响应后媒体传输中断；未保存不完整文件，浏览器未提供具体原因。','检查网络，重新加载原素材后再试。'],
    CANCELLED:['用户停止了本批任务。','重新提交或重新打包。'],
    EXTERNAL_NO_MEDIA:['已尝试解析外链，页面没有提供可直接下载的原视频地址。','可在原平台确认视频是否可播放；当前不运行外链脚本或调用平台私有接口。'],
    EXTERNAL_INCOMPLETE_ID:['YouTube 外链缺少完整的11位视频ID，无法确定素材；未猜测补齐ID。','回到对应卡片加载完整外链，或更新选择清单后重试。'],
    EXTERNAL_ID_MISMATCH:['YouTube 页面的视频ID与所选外链不一致，未保存可能错误的素材。','确认所选素材的外链是否正确后重新提交。'],
    EXTERNAL_ACCESS_REQUIRED:['外链页面要求登录、授权或视频当前不可播放，未继续获取。','在原平台确认访问权限及可播放状态；插件不会代填登录或处理验证。'],
    EXTERNAL_SIGNATURE_REQUIRED:['外链视频需要播放器签名处理，未取得可直接下载的合流原址。','当前纯插件不处理播放器签名；此类外链本次无法自动补全。'],
    EXTERNAL_SEPARATE_STREAMS:['外链只提供分离音视频流或HLS/DASH，当前插件不能合并为完整视频。','本次不把无声音轨的视频当成完整外链下载成功。'],
    EXTERNAL_INVALID_URL:['外链解析得到无效、非HTTP或非公开站点地址，未继续请求。','请检查卡片的完整原素材外链。'],
    EXTERNAL_DRM:['外链页面标明视频使用DRM授权，未尝试下载。','当前插件不获取或处理DRM授权。'],
    EXTERNAL_PAGE_LIMIT:['外链网页超过8MB解析上限，已停止读取。','该外链未能自动解析，可缩小范围后在原平台检查。'],
    EXTERNAL_DEPTH_LIMIT:['外链网页嵌套超过两层，已停止继续解析。','在原平台检查是否存在可直接获取的原视频。'],
    EXTERNAL_ATTEMPT_LIMIT:['此素材已达到8次地址尝试上限，未再重试。','在下载报告中核对各次尝试结果，更新原地址后再提交。']
  };
  const [reason,suggestion] = details[code] || details.NETWORK;
  return {code:details[code]?code:'NETWORK',reason,suggestion};
}
function clean(value) {
  // Browser/system error strings may contain a signed URL. Do not persist it
  // in exported diagnostics, even when an unexpected exception supplies one.
  return String(value || '').replace(/(?:https?|blob):[^\s<>"']+/gi,'[链接已省略]');
}
const labels={complete:'成功',failed:'下载失败',unsupported:'不支持',missing:'缺少原地址',duplicate:'重复合并',filtered:'类型未勾选',dateFiltered:'日期未纳入',dateUnchecked:'日期未核验',cancelled:'已停止',unattempted:'未执行',active:'下载中',pending:'等待下载'};
const phases={collecting:'正在采集并保存地址','collection-paused':'采集已暂停，断点保留',queued:'地址已保存，排队等待下载', 'awaiting-permission':'等待媒体站点授权',fetching:'正在按已保存地址下载媒体',packing:'媒体获取结束，正在生成 ZIP',packaged:'媒体获取结束（本报告随 ZIP 交付）',downloading:'正在保存 ZIP',complete:'ZIP 已保存',failed:'本批未完成',stopped:'本批已停止'};
function externalInfo(value){
  if(!value?.attempted)return null;
  return {attempted:true,resolved:!!value.resolved,attempts:(value.attempts || []).slice(0,8).map(a=>({method:clean(a.method),host:clean(a.host).replace(/[^a-z0-9.\[\]:-]/gi,''),code:clean(a.code),httpStatus:a.httpStatus || null}))};
}
export function buildReport(job) {
  const terminal=['failed','stopped'].includes(job.phase);
  const files=(job.items || []).map(item=>({filename:item.filename || '',pageNumber:item.pageNumber ?? null,cardIndex:item.cardIndex,adId:clean(item.adId),kind:item.kind,state:item.state || 'pending',bytes:item.bytes || 0,error:clean(item.error),errorCode:item.errorCode || '',httpStatus:item.httpStatus || null,suggestion:clean(item.suggestion),playableNote:clean(item.playableInfo?.note),externalInfo:externalInfo(item.externalInfo)}));
  // Older persisted jobs cannot reconstruct deduplicated selections reliably.
  const source=job.selections || [...(job.items || []),...(job.missing || [])];
  const bySlot=new Map(),byUrl=new Map(),byId=new Map();
  const slot=entry=>`${entry.pageNumber ?? ''}:${entry.cardIndex}:${entry.adId || ''}`;
  (job.items || []).forEach((entry,index)=>{
    bySlot.set(slot(entry),index);
    if(!byUrl.has(entry.url))byUrl.set(entry.url,index);
    if(entry.adId && !byId.has(entry.adId))byId.set(entry.adId,index);
  });
  const rows=source.map(entry=>{
    const row={pageNumber:entry.pageNumber ?? null,cardIndex:entry.cardIndex,adId:clean(entry.adId),kind:entry.kind || 'other',metadata:exportMetadata(entry.metadata,entry.adId),firstDiscovered:entry.firstDiscovered || null,detailSource:!!entry.detailSource,filename:'',bytes:0,reason:'',reasonCode:'',suggestion:''};
    const set=(state,reason,code,suggestion='')=>Object.assign(row,{state,result:labels[state],reason:clean(reason),reasonCode:code,suggestion:clean(suggestion)});
    if(entry.kind && job.requestedKinds && !job.requestedKinds.includes(entry.kind))return set('filtered','本批没有勾选此媒体类型。','TYPE_FILTERED');
    if(!dateEligible(job,entry))return set(entry.dateStatus==='excluded'?'dateFiltered':'dateUnchecked',entry.dateReason || '首次发现日期尚未核验，未纳入本批下载。',entry.dateCode || 'DATE_NOT_CHECKED',entry.dateStatus==='excluded'?'按设定日期截止，未发起媒体请求。':'本批只下载已核验日期且在范围内的地址；如需此素材，请恢复原列表和登录后重新采集。');
    if(!entry.url){
      const code=entry.reasonCode || (entry.kind?'NO_MEDIA_URL':'UNSUPPORTED_TYPE');
      const unsupported=['UNSUPPORTED_TYPE','STREAM_MANIFEST','UNSUPPORTED_SCHEME','PLAYABLE_NOT_HTML'].includes(code);
      return set(unsupported?'unsupported':'missing',entry.reason || '卡片未提供原素材地址。',code,
        unsupported?'当前插件下载图片、直接视频及 Playable 原HTML；此项未发起请求。':code.startsWith('ADDRESS_')?'恢复登录或原列表后，返回对应页重新选择以补齐地址；其他已保存地址继续下载。':code.startsWith('DETAIL_')?'保持原列表和登录状态，关闭其他详情后返回对应页重新点选；此项未发起媒体请求。':'返回对应页加载或播放原素材后再试；此项未发起请求。');
    }
    const url=normalize([entry])[0]?.url;
    const index=bySlot.get(slot(entry)) ?? byUrl.get(url) ?? byId.get(entry.adId);
    if(index===undefined)return set('unsupported','没有可用的 HTTP 图片、视频或 Playable 原HTML地址。','UNSUPPORTED_URL');
    const file=files[index],canonical=job.items[index];
    row.externalInfo=file.externalInfo;
    const fileState=file.state==='complete'?'complete':file.state==='failed'?(file.errorCode==='CANCELLED'?'cancelled':'failed'):terminal?(job.phase==='stopped'?'cancelled':'unattempted'):file.state==='active'?'active':'pending';
    row.filename=file.filename;row.bytes=file.bytes;
    if((entry.pageNumber ?? null)!==(canonical.pageNumber ?? null) || entry.cardIndex!==canonical.cardIndex){
      row.mergedInto={pageNumber:canonical.pageNumber ?? null,cardIndex:canonical.cardIndex,state:fileState,result:labels[fileState]};
      return set('duplicate',`与${canonical.pageNumber?`第 ${canonical.pageNumber} 页 `:''}第 ${canonical.cardIndex} 个素材相同，合并为一份；对应文件${labels[fileState]}。`,'DUPLICATE',['failed','cancelled','unattempted'].includes(fileState)?file.error || job.error:'');
    }
    if(file.state==='complete')return set('complete',entry.kind==='playable'?(file.playableNote || 'Playable 原HTML获取成功；外部依赖未打包，离线可玩性未验证。'):file.externalInfo?.resolved?'视频外链自动补全成功，已获取可下载媒体。':'原媒体获取成功。','OK');
    if(file.state==='failed')return set(file.errorCode==='CANCELLED'?'cancelled':'failed',file.error || '媒体获取失败。',file.errorCode || 'FETCH_FAILED',file.suggestion);
    if(terminal)return set(job.phase==='stopped'?'cancelled':'unattempted',job.phase==='stopped'?'任务已停止，未完成此项下载。':`本批异常，未完成此项下载：${clean(job.error)}`,'NOT_COMPLETED');
    return set(file.state==='active'?'active':'pending',job.phase==='awaiting-permission'?'等待媒体站点授权，尚未发起请求。':'尚未完成媒体获取。','PENDING');
  });
  const counts=Object.fromEntries(Object.keys(labels).map(state=>[state,rows.filter(row=>row.state===state).length]));
  return {schemaVersion:3,createdAt:new Date().toISOString(),startedAt:job.createdAt || null,taskId:job.id,taskName:clean(job.taskName),
    phase:job.phase,phaseLabel:phases[job.phase] || job.phase,batchError:clean(job.error),zipName:job.zipName,
    total:files.length,succeeded:files.filter(i=>i.state==='complete').length,failed:files.filter(i=>i.state==='failed' && i.errorCode!=='CANCELLED').length,autoResolveExternal:job.autoResolveExternal!==false,
    mode:job.mode,pages:job.pages || [],requestedPages:job.collection?.targetPages,collectUntilEnd:job.collection?.untilEnd,stopReason:job.stopReason,collectionIncomplete:!!job.collectionIncomplete,
    collectionResume:supportsCollectionResume(job)?{paused:job.phase==='collection-paused',pageNumber:resumePage(job),completedPages:(job.collection.completedPages || []).map(p=>p.pageNumber),resumeCount:job.collection.resumeCount || 0,events:(job.collection.events || []).map(e=>({type:clean(e.type),at:clean(e.at),code:clean(e.code),reason:clean(e.reason),stage:clean(e.stage),pageNumber:e.pageNumber}))}:null,
    firstDiscoveredSince:job.firstDiscoveredSince || null,dateCutoff:job.dateCutoff?{pageNumber:job.dateCutoff.pageNumber,cardIndex:job.dateCutoff.cardIndex,adId:clean(job.dateCutoff.adId),firstDiscovered:job.dateCutoff.firstDiscovered}:null,
    pageRecoveryCount:job.pageRecoveryCount || 0,pageDiagnostic:job.pageDiagnostic?Object.fromEntries(['code','stage','action','expectedPage','actualPage','expectedCards','actualCards','firstDifference','expectedAdId','actualAdId','loading','observedAt','waitedMs','recovered'].map(key=>[key,typeof job.pageDiagnostic[key]==='string'?clean(job.pageDiagnostic[key]):job.pageDiagnostic[key]])):null,
    detailCloseRecoveries:job.detailCloseRecoveries || 0,collectionDiagnostic:job.collectionDiagnostic?Object.fromEntries(['code','stage','action','script','pageNumber','expectedAdId','actualAdId','detailVisible','waitedMs','readAttempts'].map(key=>[key,typeof job.collectionDiagnostic[key]==='string'?clean(job.collectionDiagnostic[key]):job.collectionDiagnostic[key]])):null,
    storageMode:job.storageMode || 'memory',archiveBytes:job.archiveBytes || null,zip64:!!job.zip64,
    recovery:job.recovery?{sourceZipName:clean(job.recovery.sourceZipName),sourceSelected:job.recovery.sourceSelected,previouslySaved:job.recovery.previouslySaved}:null,
    selectedCardNumbers:job.selectedCardNumbers,selectedCardCount:job.selectedCardCount ?? rows.length,reportedSelections:rows.length,
    accountingComplete:!!job.selections && rows.length===job.selectedCardCount,selectionCounts:counts,duplicateCount:job.duplicateCount || 0,cardCount:job.cardCount,matchedCards:job.matchedCards,videoCards:job.videoCards,imageCards:job.imageCards,
    addressFirst:!!job.addressFirst,addressStage:job.addressStage || '',addressesSavedAt:job.addressesSavedAt || null,collectionError:clean(job.collectionError),savedAddressSelections:source.filter(row=>dateEligible(job,row) && row.url && (!job.requestedKinds || job.requestedKinds.includes(row.kind))).length,
    missingCards:(job.missing || []).map(({pageNumber,cardIndex,adId,kind,reason,reasonCode})=>({pageNumber,cardIndex,adId:clean(adId),kind,reason:clean(reason),reasonCode})),unsupported:counts.unsupported,files,selections:rows};
}
// Escape Markdown table cells and embedded HTML from page-provided text.
const md=value=>String(value ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\|/g,'&#124;').replace(/`/g,'&#96;').replace(/[\r\n]+/g,' ');
export function reportMarkdown(report) {
  const c=report.selectionCounts;
  const location=row=>`${row.pageNumber?`第 ${row.pageNumber} 页 · `:''}第 ${row.cardIndex} 个`;
  const table=rows=>[
    `| 选择位置 | 广告 ID | 类型 | ${report.firstDiscoveredSince?'首次发现日期 | ':''}结果 | 文件 | 原因 | 建议 |`,
    `| --- | --- | --- | ${report.firstDiscoveredSince?'--- | ':''}--- | --- | --- | --- |`,
    ...rows.map(row=>`| ${[location(row),row.adId || '未读取到 ID',({video:'视频',image:'图片',playable:'Playable 试玩',other:'其他'})[row.kind] || '其他',...(report.firstDiscoveredSince?[row.firstDiscovered || '未核验']:[]),row.result,row.filename || '—',row.reason,row.suggestion || '—'].map(md).join(' | ')} |`)
  ].join('\n');
  const problems=report.selections.filter(row=>['failed','missing','unsupported','cancelled','unattempted','dateUnchecked'].includes(row.state) || ['failed','cancelled','unattempted'].includes(row.mergedInto?.state));
  const dateExcluded=report.selections.filter(row=>['dateFiltered','dateUnchecked'].includes(row.state));
  const missing=report.selections.filter(row=>row.state==='missing');
  const external=report.files.filter(f=>f.externalInfo);
  const externalRows=external.flatMap(f=>f.externalInfo.attempts.map((a,i)=>`| ${[location(f),i+1,a.method,a.host || '未取得有效站点',a.code==='OK'?'取得视频':a.code==='PARSED'?'已解析，继续尝试媒体地址':failureDetail(a.code,a.httpStatus).reason].map(md).join(' | ')} |`));
  const metadataRows=report.selections.map(row=>{
    const m=row.metadata,f=m?.fields || {},est=f.displayEstimate;
    const sources=[...new Set(Object.values(f).filter(Boolean).map(v=>v.source==='visible_detail'?'对应详情':'当前卡片'))];
    const missing=m?.missingFields?.map(k=>metadataNames[k]).join('、');
    const note=m?[sources.length?`来源：${sources.join('、')}`:'',missing?`缺失：${missing}`:'字段齐全',...(m.warnings || []),m.detailFailure || ''].filter(Boolean).join('；'):'旧任务或卡片未读取素材信息，全部未知';
    return `| ${[location(row),row.adId || '未读取到 ID',f.appName?.value || '未知',f.appId?.value || '未知',f.platform?.value || '未知',est?.raw ?? '未知',est?`${est.value}${est.precision==='rounded'?'（页面缩写换算，近似）':''}`:'未知',f.relaunch?(f.relaunch.value?'重投':'非重投（明确标注）'):'未知',row.result,row.filename || '—',note].map(md).join(' | ')} |`;
  });
  return [
    '# 广大大素材下载报告', '',
    report.taskId?`- 任务：${md(report.taskName || '原任务')}（${md(report.taskId)}）`:'',
    `- 生成时间：${md(new Date(report.createdAt).toLocaleString('zh-CN'))}`,
    `- 批次状态：${md(report.phaseLabel)}`,
    `- ZIP：${md(report.zipName)}`,
    `- 选择 ${report.selectedCardCount} 项；成功获取 ${report.succeeded} 个文件；请求失败 ${report.failed} 个文件。`,
    `- 缺少原地址 ${c.missing} 项；不支持 ${c.unsupported} 项；重复合并 ${c.duplicate} 项；类型未勾选 ${c.filtered} 项。`,
    report.recovery?`- 补下载来源：${md(report.recovery.sourceZipName)}；原批选择 ${report.recovery.sourceSelected ?? '未知'} 项，原 ZIP 已保存 ${report.recovery.previouslySaved} 个文件。本批仅重试未交付的已存地址，保留原页码与编号；不重新翻页、也不包含原包成功文件。`:'',
    report.mode==='pages'?`- 采集结束原因：${report.collectionIncomplete?'提前中断，未确认日期截止或列表末页':({'date-cutoff':'达到首次发现日期截止','last-page':'网站下一页明确禁用，已到末页','page-limit':'达到设定页数上限'})[report.stopReason] || (report.phase==='collecting'?'仍在采集':report.phase==='stopped'?'用户主动停止':'见任务状态')}。`:'',
    report.batchError?`- 本批异常：${md(report.batchError)}`:'',
    report.collectionIncomplete?(report.collectionResume?.paused?'**采集已暂停：断点、已选素材和地址保留；本批尚未启动媒体下载。恢复原筛选和对应页面后可继续采集；未访问页面不代表已采到末页。**':report.addressFirst?'**地址采集未完成：已保存地址继续下载，ZIP 仅包含实际下载成功的文件；未访问页面没有卡片名单，未计入已选素材，不代表已采到末页。**':'**翻页采集未完成：仅列出已确认采集的页面，后续页未采集，未生成媒体 ZIP。**'):'',
    !report.accountingComplete?'**旧版任务无法完整还原合并前的选择；新建下载任务可逐项对账。**':'', '',
    '成功表示原媒体获取成功；ZIP 是否保存以 Chrome 下载结果为准。编号按各页从左到右、从上到下排列。', '',
    ...(report.collectionResume?[
      '## 采集断点与恢复','',
      `当前${report.collectionResume.paused?'已暂停':'任务状态见上文'}；已处理页码：${report.collectionResume.completedPages.join('、') || '无'}；断点位置：第 ${md(report.collectionResume.pageNumber || '未知')} 页；已继续采集 ${report.collectionResume.resumeCount} 次。`,
      ...report.collectionResume.events.map(e=>`- ${md(e.at)} ${e.type==='paused'?'暂停':'继续'}：第 ${md(e.pageNumber || '未知')} 页 ${md(e.code)} ${md(e.reason)}`),
      '继续采集前核对原搜索、筛选、排序、页码和卡片身份。结果不确定的翻页不会重复点击；不会跳过未采集页面。选择结束采集并下载已有地址后，本任务不再续采。',''
    ]:[]),
    report.storageMode==='disk'?'媒体边下载边写入本机临时磁盘，再流式写入同一个ZIP；不再受旧版整批512MB内存阈值限制。临时空间不足或读写失败会明确停止，成功获取不代表ZIP已经保存。完成或停止后清理临时文件；异常退出的残留在下次任务开始时清理。':'',
    ...(report.firstDiscoveredSince?[
      '## 首次发现日期截止','',
      `日期下限：${md(report.firstDiscoveredSince)}（含当天）。按「最新创意」从新到旧，使用卡片或对应详情可见日期范围的起始日期；不以独立首次发现旧字段或网站筛选控件代替。`,
      report.dateCutoff?`触发截止：${location(report.dateCutoff)}，广告 ID ${md(report.dateCutoff.adId)}，首次发现 ${md(report.dateCutoff.firstDiscovered)}。该素材及其后的位置不下载，未继续翻页。`:`${report.collectionIncomplete?'采集中断，未确认达到日期截止。':'在本批页数或末页范围内未遇到早于下限的素材。'}`,
      `日期未纳入 ${c.dateFiltered} 项；日期未核验 ${c.dateUnchecked} 项。日期未纳入不计为媒体下载失败。`,'',
      dateExcluded.length?table(dateExcluded):'没有因日期未纳入或未核验而跳过的素材。',''
    ]:[]),
    ...(report.pageDiagnostic?[
      '## 翻页稳定性检查','',
      `页面短暂重绘后恢复 ${report.pageRecoveryCount} 次。最近检查${report.pageDiagnostic.recovered?'已恢复':'未恢复'}；阶段 ${md(report.pageDiagnostic.stage)}，动作 ${md(report.pageDiagnostic.action)}，代码 ${md(report.pageDiagnostic.code)}。`,
      `期望第 ${md(report.pageDiagnostic.expectedPage)} 页 / ${md(report.pageDiagnostic.expectedCards)} 张卡片；实际第 ${md(report.pageDiagnostic.actualPage)} 页 / ${md(report.pageDiagnostic.actualCards)} 张卡片；加载标记 ${report.pageDiagnostic.loading?'存在':'未见'}；累计等待 ${Math.round((report.pageDiagnostic.waitedMs || 0)/100)/10} 秒。`,
      report.pageDiagnostic.firstDifference?`首个身份不一致位置：第 ${md(report.pageDiagnostic.firstDifference)} 个；期望广告 ID ${md(report.pageDiagnostic.expectedAdId || '未读到')}，实际 ${md(report.pageDiagnostic.actualAdId || '未读到')}。`:'',
      `检查未通过期间不点击下一页、不采集不匹配素材。短暂不一致最多等待15秒；持续不一致${report.collectionResume?'暂停并保留采集断点，等待继续或明确结束后下载已有地址':'停止继续采集，并按已保存且日期符合的地址下载'}。`,''
    ]:[]),
    report.selections.some(row=>row.kind==='playable')?'Playable 保存原始HTML，不执行或改写脚本；外部脚本、图片、声音和动态依赖未打包，打开时可能需要联网，不保证离线可玩。':'',
    ...(report.addressFirst?[
      '## 地址采集与保存','',
      report.detailCloseRecoveries?`详情关闭超过原2秒等待后恢复 ${report.detailCloseRecoveries} 次；已确认关闭后才继续采集。`:'',
      ...(report.collectionDiagnostic?[
        `最近采集检查：代码 ${md(report.collectionDiagnostic.code)}；第 ${md(report.collectionDiagnostic.pageNumber || '未知')} 页；动作 ${md(report.collectionDiagnostic.action)}；脚本 ${md(report.collectionDiagnostic.script || 'detailControl')}；阶段 ${md(report.collectionDiagnostic.stage || 'close-confirmation')}。`,
        `期望广告 ID ${md(report.collectionDiagnostic.expectedAdId || '未涉及')}；实际 ${md(report.collectionDiagnostic.actualAdId || '未读取')}；详情仍可见 ${report.collectionDiagnostic.detailVisible===true?'是':report.collectionDiagnostic.detailVisible===false?'否':'未确定'}；等待 ${Math.round((report.collectionDiagnostic.waitedMs || 0)/100)/10} 秒；只读尝试 ${md(report.collectionDiagnostic.readAttempts || '未涉及')} 次。`
      ]:[]),
      `先保存已选素材的原媒体或外链地址，再统一下载。已保存地址 ${report.savedAddressSelections} 项（合并前），未取得地址 ${missing.length} 项。`,
      report.addressesSavedAt?`最近保存时间：${md(new Date(report.addressesSavedAt).toLocaleString('zh-CN'))}。`:'',
      report.collectionError?`采集中断原因：${md(report.collectionError)}。${report.collectionResume?.paused?'断点和已经保存的地址保留，等待继续采集，本批未启动媒体下载。':'已经保存的地址保留，并独立尝试下载；CDN失效、HTTP拒绝或外链解析失败仍按实际结果记录。'}`:'下载阶段不再读取广大大列表或检查账号登录；仍可能需要实际媒体站点授权。','',
      `### 未取得地址的已选素材（${missing.length} 项）`,'',missing.length?table(missing):'所有本批所选类型的素材均已取得地址，或属于已单列的不支持类型。',''
    ]:[]),
    '## 视频外链自动补全', '',
    report.selections.some(row=>row.detailSource)?`已从所选素材的详情读取原始外链 ${report.selections.filter(row=>row.detailSource).length} 项（核对广告ID后读取）。`:'',
    report.autoResolveExternal?`已启用；实际尝试 ${external.length} 项，补全成功 ${external.filter(f=>f.externalInfo.resolved && f.state==='complete').length} 项。`:'本批已关闭自动补全；按原素材地址直接获取。',
    externalRows.length?['| 选择位置 | 尝试 | 来源 | 站点 | 结果 / 原因 |','| --- | --- | --- | --- | --- |',...externalRows].join('\n'):'没有需要补全的外链尝试。', '',
    `## 失败及未完成内容（${problems.length} 项）`, '',
    problems.length?table(problems):'没有失败或未完成项。', '',
    `## 全部选择（${report.selections.length} 项）`, '', table(report.selections), '',
    `## 素材信息（${report.selections.length} 项）`,'',
    'App 归属仅按当前卡片或广告 ID 匹配的详情中可见的商店／广告主链接和明确名称记录，不使用搜索条件推断。展示估值保留原文；K、M、万、亿换算值为页面缩写的近似数。重投仅按明确标签记录；未见标签为未知，不等于非重投。字段缺失不阻止已保存媒体地址下载。',
    '这三个字段随列表采集，不为补齐素材信息单独点击详情；读取外链／日期时如已打开对应详情，也保留其中明确显示的信息。未显示的字段单独标缺失。', '',
    '| 选择位置 | 广告 ID | App 名称 | App ID | 平台 | 展示估值原文 | 展示估值换算 | 重投标签 | 结果 | 文件 | 来源与缺失说明 |',
    '| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |',...metadataRows,'',
    `对账：成功 ${c.complete} + 请求失败 ${c.failed} + 缺少原地址 ${c.missing} + 不支持 ${c.unsupported} + 重复 ${c.duplicate} + 类型未选 ${c.filtered}${report.firstDiscoveredSince?` + 日期未纳入 ${c.dateFiltered} + 日期未核验 ${c.dateUnchecked}`:''} + 已停止 ${c.cancelled} + 未执行 ${c.unattempted} + 下载中 ${c.active} + 等待 ${c.pending} = ${report.reportedSelections} 项。`, '',
    '重复项只保留一个文件；缺少原地址、不支持或类型未勾选的项没有发起下载请求。',
    '报告不包含原媒体签名链接或账号凭据。HTTP 状态是服务器返回的证据；可能原因和建议不代表已确定故障根因。', ''
  ].filter(line=>line!==undefined).join('\n');
}
