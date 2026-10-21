import {normalize} from './media.js';

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
    NOT_MEDIA:['服务器返回网页或 JSON，未当作媒体保存。','回到原页确认登录及原素材可用，重新加载后再试。'],
    PLAYABLE_NOT_HTML:['服务器或卡片未提供可识别的 Playable HTML 原文件；未用图片、视频、JSON或错误网页代替。','回到试玩卡片重新加载，确认原试玩可打开后重新提交。'],
    PLAYABLE_LOGIN_PAGE:['获取 Playable 时返回了登录或授权页面，未当作试玩保存。','确认广大大登录和试玩访问权限，回到对应页重新加载后提交。'],
    EMPTY_RESPONSE:['服务器未返回媒体内容或返回空文件。','重新加载原素材后再试。'],
    NETWORK:['网络请求失败；浏览器未提供 HTTP 状态，无法确定具体原因。','检查网络、CDN授权及网站原素材是否可用；可能涉及连接、跨域或重定向限制。'],
    TRANSFER_INTERRUPTED:['服务器响应后媒体传输中断；未保存不完整文件，浏览器未提供具体原因。','检查网络，重新加载原素材后再试。'],
    CANCELLED:['用户停止了本批任务。','重新提交或重新打包。']
  };
  const [reason,suggestion] = details[code] || details.NETWORK;
  return {code:details[code]?code:'NETWORK',reason,suggestion};
}
function clean(value) {
  // Browser/system error strings may contain a signed URL. Do not persist it
  // in exported diagnostics, even when an unexpected exception supplies one.
  return String(value || '').replace(/(?:https?|blob):[^\s<>"']+/gi,'[链接已省略]');
}
const labels={complete:'成功',failed:'下载失败',unsupported:'不支持',missing:'缺少原地址',duplicate:'重复合并',filtered:'类型未勾选',cancelled:'已停止',unattempted:'未执行',active:'下载中',pending:'等待下载'};
const phases={collecting:'正在翻页采集', 'awaiting-permission':'等待媒体站点授权',fetching:'正在获取媒体',packing:'媒体获取结束，正在生成 ZIP',packaged:'媒体获取结束（本报告随 ZIP 交付）',downloading:'正在保存 ZIP',complete:'ZIP 已保存',failed:'本批未完成',stopped:'本批已停止'};
export function buildReport(job) {
  const terminal=['failed','stopped'].includes(job.phase);
  const files=(job.items || []).map(item=>({filename:item.filename || '',pageNumber:item.pageNumber ?? null,cardIndex:item.cardIndex,adId:clean(item.adId),kind:item.kind,state:item.state || 'pending',bytes:item.bytes || 0,error:clean(item.error),errorCode:item.errorCode || '',httpStatus:item.httpStatus || null,suggestion:clean(item.suggestion),playableNote:clean(item.playableInfo?.note)}));
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
    const row={pageNumber:entry.pageNumber ?? null,cardIndex:entry.cardIndex,adId:clean(entry.adId),kind:entry.kind || 'other',filename:'',bytes:0,reason:'',reasonCode:'',suggestion:''};
    const set=(state,reason,code,suggestion='')=>Object.assign(row,{state,result:labels[state],reason:clean(reason),reasonCode:code,suggestion:clean(suggestion)});
    if(entry.kind && job.requestedKinds && !job.requestedKinds.includes(entry.kind))return set('filtered','本批没有勾选此媒体类型。','TYPE_FILTERED');
    if(!entry.url){
      const code=entry.reasonCode || (entry.kind?'NO_MEDIA_URL':'UNSUPPORTED_TYPE');
      const unsupported=['UNSUPPORTED_TYPE','STREAM_MANIFEST','UNSUPPORTED_SCHEME','PLAYABLE_NOT_HTML'].includes(code);
      return set(unsupported?'unsupported':'missing',entry.reason || '卡片未提供原素材地址。',code,
        unsupported?'当前插件下载图片、直接视频及 Playable 原HTML；此项未发起请求。':'返回对应页加载或播放原素材后再试；此项未发起请求。');
    }
    const url=normalize([entry])[0]?.url;
    const index=bySlot.get(slot(entry)) ?? byUrl.get(url) ?? byId.get(entry.adId);
    if(index===undefined)return set('unsupported','没有可用的 HTTP 图片、视频或 Playable 原HTML地址。','UNSUPPORTED_URL');
    const file=files[index],canonical=job.items[index];
    const fileState=file.state==='complete'?'complete':file.state==='failed'?(file.errorCode==='CANCELLED'?'cancelled':'failed'):terminal?(job.phase==='stopped'?'cancelled':'unattempted'):file.state==='active'?'active':'pending';
    row.filename=file.filename;row.bytes=file.bytes;
    if((entry.pageNumber ?? null)!==(canonical.pageNumber ?? null) || entry.cardIndex!==canonical.cardIndex){
      row.mergedInto={pageNumber:canonical.pageNumber ?? null,cardIndex:canonical.cardIndex,state:fileState,result:labels[fileState]};
      return set('duplicate',`与${canonical.pageNumber?`第 ${canonical.pageNumber} 页 `:''}第 ${canonical.cardIndex} 个素材相同，合并为一份；对应文件${labels[fileState]}。`,'DUPLICATE',['failed','cancelled','unattempted'].includes(fileState)?file.error || job.error:'');
    }
    if(file.state==='complete')return set('complete',entry.kind==='playable'?(file.playableNote || 'Playable 原HTML获取成功；外部依赖未打包，离线可玩性未验证。'):'原媒体获取成功。','OK');
    if(file.state==='failed')return set(file.errorCode==='CANCELLED'?'cancelled':'failed',file.error || '媒体获取失败。',file.errorCode || 'FETCH_FAILED',file.suggestion);
    if(terminal)return set(job.phase==='stopped'?'cancelled':'unattempted',job.phase==='stopped'?'任务已停止，未完成此项下载。':`本批异常，未完成此项下载：${clean(job.error)}`,'NOT_COMPLETED');
    return set(file.state==='active'?'active':'pending',job.phase==='awaiting-permission'?'等待媒体站点授权，尚未发起请求。':'尚未完成媒体获取。','PENDING');
  });
  const counts=Object.fromEntries(Object.keys(labels).map(state=>[state,rows.filter(row=>row.state===state).length]));
  return {schemaVersion:2,createdAt:new Date().toISOString(),startedAt:job.createdAt || null,
    phase:job.phase,phaseLabel:phases[job.phase] || job.phase,batchError:clean(job.error),zipName:job.zipName,
    total:files.length,succeeded:files.filter(i=>i.state==='complete').length,failed:files.filter(i=>i.state==='failed' && i.errorCode!=='CANCELLED').length,
    mode:job.mode,pages:job.pages || [],requestedPages:job.collection?.targetPages,collectUntilEnd:job.collection?.untilEnd,stopReason:job.stopReason,collectionIncomplete:!!job.collectionIncomplete,
    selectedCardNumbers:job.selectedCardNumbers,selectedCardCount:job.selectedCardCount ?? rows.length,reportedSelections:rows.length,
    accountingComplete:!!job.selections && rows.length===job.selectedCardCount,selectionCounts:counts,duplicateCount:job.duplicateCount || 0,cardCount:job.cardCount,matchedCards:job.matchedCards,videoCards:job.videoCards,imageCards:job.imageCards,
    missingCards:(job.missing || []).map(({pageNumber,cardIndex,adId,kind,reason,reasonCode})=>({pageNumber,cardIndex,adId:clean(adId),kind,reason:clean(reason),reasonCode})),unsupported:counts.unsupported,files,selections:rows};
}
// Escape Markdown table cells and embedded HTML from page-provided text.
const md=value=>String(value ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\|/g,'&#124;').replace(/`/g,'&#96;').replace(/[\r\n]+/g,' ');
export function reportMarkdown(report) {
  const c=report.selectionCounts;
  const location=row=>`${row.pageNumber?`第 ${row.pageNumber} 页 · `:''}第 ${row.cardIndex} 个`;
  const table=rows=>[
    '| 选择位置 | 广告 ID | 类型 | 结果 | 文件 | 原因 | 建议 |',
    '| --- | --- | --- | --- | --- | --- | --- |',
    ...rows.map(row=>`| ${[location(row),row.adId || '未读取到 ID',({video:'视频',image:'图片',playable:'Playable 试玩',other:'其他'})[row.kind] || '其他',row.result,row.filename || '—',row.reason,row.suggestion || '—'].map(md).join(' | ')} |`)
  ].join('\n');
  const problems=report.selections.filter(row=>['failed','missing','unsupported','cancelled','unattempted'].includes(row.state) || ['failed','cancelled','unattempted'].includes(row.mergedInto?.state));
  return [
    '# 广大大素材下载报告', '',
    `- 生成时间：${md(new Date(report.createdAt).toLocaleString('zh-CN'))}`,
    `- 批次状态：${md(report.phaseLabel)}`,
    `- ZIP：${md(report.zipName)}`,
    `- 选择 ${report.selectedCardCount} 项；成功获取 ${report.succeeded} 个文件；请求失败 ${report.failed} 个文件。`,
    `- 缺少原地址 ${c.missing} 项；不支持 ${c.unsupported} 项；重复合并 ${c.duplicate} 项；类型未勾选 ${c.filtered} 项。`,
    report.batchError?`- 本批异常：${md(report.batchError)}`:'',
    report.collectionIncomplete?'**翻页采集未完成：仅列出已确认采集的页面，后续页未采集，未生成媒体 ZIP。**':'',
    !report.accountingComplete?'**旧版任务无法完整还原合并前的选择；新建下载任务可逐项对账。**':'', '',
    '成功表示原媒体获取成功；ZIP 是否保存以 Chrome 下载结果为准。编号按各页从左到右、从上到下排列。', '',
    report.selections.some(row=>row.kind==='playable')?'Playable 保存原始HTML，不执行或改写脚本；外部脚本、图片、声音和动态依赖未打包，打开时可能需要联网，不保证离线可玩。':'',
    `## 失败及未完成内容（${problems.length} 项）`, '',
    problems.length?table(problems):'没有失败或未完成项。', '',
    `## 全部选择（${report.selections.length} 项）`, '', table(report.selections), '',
    `对账：成功 ${c.complete} + 请求失败 ${c.failed} + 缺少原地址 ${c.missing} + 不支持 ${c.unsupported} + 重复 ${c.duplicate} + 类型未选 ${c.filtered} + 已停止 ${c.cancelled} + 未执行 ${c.unattempted} + 下载中 ${c.active} + 等待 ${c.pending} = ${report.reportedSelections} 项。`, '',
    '重复项只保留一个文件；缺少原地址、不支持或类型未勾选的项没有发起下载请求。',
    '报告不包含原媒体签名链接或账号凭据。HTTP 状态是服务器返回的证据；可能原因和建议不代表已确定故障根因。', ''
  ].filter(line=>line!==undefined).join('\n');
}
