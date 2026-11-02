import {allowedPage} from './media.js';
import {collectMedia} from './collector.js';
import {pageAccess} from './addresses.js';
import {selectionContext} from './selection.js';
import {retryObservation} from './readiness.js';

// Popup refresh is an observation, never a page navigation, detail click or
// extension restart. Always discard the previous scan if this read fails.
export function within(promise,message,ms=10000){
  let timer;
  return Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error(message)),ms);})]).finally(()=>clearTimeout(timer));
}
export async function readPopupPage(api,tab){
  const result={tabId:tab?.id,validTab:allowedPage(tab?.url),scan:null,scanReady:false,pageError:''};
  if(!result.validTab)return {...result,pageError:'请切换到已登录的广大大展示广告列表，再点击「刷新插件状态」。'};
  const observe=(func,args=[])=>retryObservation(async()=>{
    const [value]=await api.scripting.executeScript({target:{tabId:tab.id},world:'MAIN',func,args});
    if(!value?.result)throw Object.assign(new Error(`暂时未读到${func===collectMedia?'素材卡片':func===selectionContext?'当前页码':'登录状态'}的网页返回结果，已有限重试；请等页面加载后刷新插件状态。`),{code:'PAGE_READ_UNAVAILABLE'});
    if(value.result.error)throw new Error(value.result.error);
    return value.result;
  });
  try{
    const scan=await within((async()=>{
      const access=await observe(pageAccess);
      if(access.blocked)throw new Error(access.reason);
      const before=await observe(selectionContext);
      if(before.loading)throw new Error('网页仍在加载卡片，请等加载完成后刷新插件状态。');
      const current=await observe(collectMedia,[false]);
      const after=await observe(selectionContext);
      if(after.loading || before.pageNumber!==after.pageNumber || current.pageNumber!==after.pageNumber)throw new Error('网页正在翻页，请等页面稳定后刷新插件状态。');
      if(!Number.isInteger(current.cardCount) || current.cardCount<1 || !Array.isArray(current.cardKeys) || current.cardKeys.length!==current.cardCount || !Array.isArray(current.items) || !Array.isArray(current.missing))throw new Error('当前页卡片信息尚未加载完整，请稍后刷新插件状态。');
      return current;
    })(),'读取当前页超时，请检查网页后刷新插件状态。');
    return {...result,scan,scanReady:true};
  }catch(error){return {...result,pageError:String(error.message || error)};}
}
export function startBlockReason({busy,refreshing,statusReady,statusError,collectorId,tasks,validTab,mode,basket,scanReady,scan,pageError}){
  if(refreshing)return '正在刷新插件状态…';
  if(busy)return '当前操作仍在处理，请等它返回；刷新不会取消该操作。';
  if(!statusReady)return statusError || '尚未取得任务状态，请点击「刷新插件状态」。';
  if(collectorId){
    const name=tasks.find(t=>t.id===collectorId)?.taskName || collectorId;
    return `任务「${name}」仍标记为采集地址。请在「下载任务」中选中它核对进度；支持续采的任务可暂停保留断点。刷新不会强行结束任务。`;
  }
  if(!validTab)return pageError || '请切换到已登录的广大大列表后刷新插件状态。';
  if(mode==='basket')return basket?.entries.length?'':'跨页清单为空，请先点选素材，或改为「仅当前页全部」。';
  if(!scanReady)return pageError || '尚未读取到当前页卡片，请等页面加载完成后点击「刷新插件状态」。';
  if(mode==='pages' && scan?.cardKeys.some(k=>typeof k!=='string' || !k))return '自动翻页需要完整卡片身份；请等卡片信息加载完成后刷新插件状态。';
  return '';
}
