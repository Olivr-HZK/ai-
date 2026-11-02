// A calendar-date lower bound, inclusive. Never infer dates from URL fields,
// timestamps, the site's search controls, or an independent relaunch label.
export function calendarDate(value) {
  if(typeof value!=='string' || !/^\d{4}-\d{2}-\d{2}$/.test(value))return null;
  const date=new Date(`${value}T00:00:00Z`);
  return !isNaN(date) && date.toISOString().slice(0,10)===value?value:null;
}
export function dateLimit(value) {
  if(value===undefined || value===null || value==='')return null;
  const date=calendarDate(value);
  if(!date)throw new Error('请选择有效的最早首次发现日期（YYYY-MM-DD）。');
  return date;
}
export function sortStatusText(scan) {
  if(scan?.newestFirst===true)return '已确认「最新创意」排序。';
  const state=scan?.sortState;
  return state?.status==='not-found'?'未识别到列表的「最新创意」排序控件。':state?.status==='ambiguous'?'排序控件的选中状态存在冲突，尚未确认「最新创意」。':state?.status==='other-sort'?`当前识别到「${state.selectedLabel}」排序；日期截止需要「最新创意」。`:state?.status==='unconfirmed'?'已找到「最新创意」，但尚未确认它处于选中状态。':'';
}
export function requireNewest(scan) {
  if(scan.newestFirst===true)return;
  const state=scan.sortState,reason=sortStatusText(scan);
  throw Object.assign(new Error(reason?`${reason} 请确认网页已选中「最新创意」，等待页面加载后刷新插件状态，再开始。`:'日期截止需要按从新到旧采集；请先在广大大列表选择「最新创意」，再开始。'),{code:'DATE_SORT_REQUIRED',...(state?{diagnostic:{action:'sort-read',script:'collectMedia',sortStatus:state.status,selectedSort:state.selectedLabel,candidateCount:state.candidateCount}}:{})});
}
export function datePlan(scan, since, pageNumber) {
  if(!calendarDate(since))throw new Error('无效日期截止值');
  let cutoff=null,pending=null;
  const rows=scan.cardKeys.map((adId,index)=>{
    const cardIndex=index+1,observed=scan.cardDates?.[index];
    const firstDiscovered=calendarDate(observed?.date);
    const row={pageNumber,cardIndex,adId,firstDiscovered,dateSource:firstDiscovered?observed.source:null};
    if(cutoff)return {...row,dateStatus:'excluded',dateCode:'DATE_AFTER_CUTOFF',dateReason:'已在前一张素材触发日期截止，此位置未继续核验或采集。'};
    if(pending)return {...row,dateStatus:'pending',dateCode:'DATE_NOT_CHECKED',dateReason:'前面的素材日期尚未核验，此位置未继续核验或采集。'};
    if(!firstDiscovered){pending=row;return {...row,dateStatus:'pending',dateCode:'DATE_UNAVAILABLE',dateReason:'未能读取有效的首次发现日期，未纳入下载队列。'};}
    if(firstDiscovered<since){cutoff={...row,since};return {...row,dateStatus:'excluded',dateCode:'DATE_BEFORE_LIMIT',dateReason:`首次发现日期 ${firstDiscovered} 早于下限 ${since}，触发自动停止；此素材不下载。`};}
    return {...row,dateStatus:'included'};
  });
  return {rows,cutoff,pending,included:rows.filter(row=>row.dateStatus==='included').map(row=>row.cardIndex)};
}
export const dateEligible=(job,row)=>!job.firstDiscoveredSince || row.dateStatus==='included';
