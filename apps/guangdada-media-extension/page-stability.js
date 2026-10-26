export const PAGE_SETTLE_MS=2000;
export const PAGE_REPAINT_WAIT_MS=15000;
export function canWaitForPage(error) {
  if(!['PAGE_CARDS_CHANGED','PAGE_NUMBER_CHANGED','PAGE_LOADING'].includes(error.code))return false;
  const d=error.diagnostic;
  // A distant page jump is not a transient neighbour/old pagination label.
  return !!d && Number.isInteger(d.expectedPage) && Number.isInteger(d.actualPage) && Math.abs(d.expectedPage-d.actualPage)<=1;
}
export function pageMismatch(expected,actual,action='read') {
  const difference=expected.cardKeys.findIndex((id,i)=>id!==actual.cardKeys[i]);
  return Object.assign(new Error('页面卡片暂未与已保存的素材身份一致。'),{code:'PAGE_CARDS_CHANGED',notClicked:true,diagnostic:{action,expectedPage:expected.pageNumber,actualPage:actual.pageNumber,expectedCards:expected.cardKeys.length,actualCards:actual.cardKeys.length,firstDifference:difference>=0?difference+1:null,expectedAdId:expected.cardKeys[difference] || null,actualAdId:actual.cardKeys[difference] || null,loading:!!actual.loading}});
}
export function pageWaitMessage(diagnostic) {
  const d=diagnostic;
  return `等待列表稳定：期望第 ${d.expectedPage} 页，实际第 ${d.actualPage} 页；卡片 ${d.actualCards}/${d.expectedCards ?? '?'}${d.firstDifference?`，第 ${d.firstDifference} 个身份暂不一致`:''}。本阶段未继续点击或翻页。`;
}
