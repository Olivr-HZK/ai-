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
export function requireNewest(scan) {
  if(scan.newestFirst!==true)throw Object.assign(new Error('日期截止需要按从新到旧采集；请先在广大大列表选择「最新创意」，再开始。'),{code:'DATE_SORT_REQUIRED'});
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
