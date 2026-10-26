import {needsDetail} from './detail.js';
import {mergeMetadata} from './metadata.js';
// Cart data is written only by the extension worker after reading real cards.
export function selectionContext() {
  const active=[...document.querySelectorAll('.ant-pagination-item-active')].filter(el=>el.getClientRects().length);
  if(active.length>1)return {error:'分页控件不唯一，请关闭详情后继续点选。'};
  const pageNumber=active.length?Number(active[0].getAttribute('title') || active[0].textContent.trim()):1;
  if(!Number.isInteger(pageNumber) || pageNumber<1)return {error:'无法确认当前页码，请稍后继续。'};
  return {pageNumber,loading:[...document.querySelectorAll('.ant-spin-spinning, [aria-busy="true"]')].some(el=>el.getClientRects().length)};
}
export function cartPages(entries) {
  const pages = new Map();
  for (const item of entries) {
    if (!pages.has(item.pageNumber)) pages.set(item.pageNumber,{pageNumber:item.pageNumber,cardCount:item.sourceCardCount,selectedCardNumbers:[],missingCount:0});
    const page=pages.get(item.pageNumber);
    page.selectedCardNumbers.push(item.cardIndex);
    if (!item.url) page.missingCount++;
  }
  return [...pages.values()].sort((a,b)=>a.pageNumber-b.pageNumber).map(page=>({...page,selectedCardNumbers:[...new Set(page.selectedCardNumbers)].sort((a,b)=>a-b)}));
}
export function selectionEntry(scan, pageNumber, cardIndex) {
  const adId=scan.cardKeys[cardIndex-1];
  if (!adId) throw new Error('此卡片尚未加载完成，请稍后再选。');
  const item=scan.items.find(i=>i.cardIndex===cardIndex);
  const missing=scan.missing.find(i=>i.cardIndex===cardIndex);
  return {...(item || missing),adId,cardIndex,pageNumber,sourceCardCount:scan.cardCount};
}
export function refreshEntries(entries, scan) {
  return entries.map(entry=>{
    const index=scan.cardKeys.indexOf(entry.adId);
    if(index<0)return entry;
    const fresh=selectionEntry(scan,entry.pageNumber,index+1);
    const metadata=mergeMetadata(entry.metadata,fresh.metadata);
    if(entry.detailSource && needsDetail(fresh))return metadata?{...entry,metadata}:entry;
    // Keep the original page/slot even when a duplicate appears elsewhere.
    if(!fresh.url)return entry.url?{...entry,metadata}:{...entry,metadata,kind:fresh.kind,reason:fresh.reason,reasonCode:fresh.reasonCode};
    const next={...entry,metadata,url:fresh.url,kind:fresh.kind};
    if(entry.url!==fresh.url)next.addressCapturedAt=new Date().toISOString();
    if(fresh.detailSource)next.detailSource=true;
    if(fresh.candidateUrls)next.candidateUrls=[...fresh.candidateUrls];else delete next.candidateUrls;
    delete next.reason;delete next.reasonCode;return next;
  });
}
