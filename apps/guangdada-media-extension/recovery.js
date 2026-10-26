// A completed ZIP already contains successful files. A failed/stopped archive
// has delivered none, so retry all saved addresses in that case.
export function recoveryItems(job){
  if(!job || !['complete','failed','stopped'].includes(job.phase))return [];
  return (job.items || []).filter(i=>i.url && (job.phase!=='complete' || i.state!=='complete'));
}
export function recoveryJob(source,id,now=new Date().toISOString()){
  const items=structuredClone(recoveryItems(source));if(!items.length)throw new Error('没有可补下载的已存地址。');
  const pages=[...new Set(items.map(i=>i.pageNumber))].map(pageNumber=>({pageNumber,cardCount:items.filter(i=>i.pageNumber===pageNumber).length,selectedCardNumbers:items.filter(i=>i.pageNumber===pageNumber).map(i=>i.cardIndex),missingCount:0}));
  const recovery={sourceId:source.id,sourceZipName:source.zipName,sourceSelected:source.selectedCardCount,previouslySaved:source.phase==='complete'?(source.items || []).filter(i=>i.state==='complete').length:0};
  return {id,createdAt:now,zipName:`广大大/广大大补下载_${now.replace(/[:.]/g,'-')}.zip`,phase:'fetching',mode:'recovery',items,selections:structuredClone(items),missing:[],pages,
    selectedCardCount:items.length,cardCount:items.length,matchedCards:items.length,rawCount:items.length,duplicateCount:0,unsupported:0,
    imageCards:items.filter(i=>i.kind==='image').length,videoCards:items.filter(i=>i.kind==='video').length,playableCards:items.filter(i=>i.kind==='playable').length,
    requestedKinds:[...new Set(items.map(i=>i.kind))],mediaOrigins:[...new Set(items.map(i=>`${new URL(i.url).origin}/*`))],autoResolveExternal:source.autoResolveExternal!==false,
    addressFirst:true,addressStage:'complete',addressesSavedAt:source.addressesSavedAt || source.createdAt,addressCollectionFinishedAt:now,collectionIncomplete:false,stopReason:'saved-failed-addresses',recovery};
}
