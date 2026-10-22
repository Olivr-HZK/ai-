import {makeZip} from './zip.js';
import {buildReport,reportMarkdown,failureDetail} from './report.js';
import {inspectPlayable} from './playable.js';
import {fetchExternalVideo,isHtml,EXTERNAL_PAGE_BYTES} from './external.js';
let current = null;
const LIMIT = 512 * 1024 * 1024;
const failure=(code,status)=>Object.assign(new Error(failureDetail(code,status).reason),{detail:failureDetail(code,status),httpStatus:status || null});
const send = async data => {
  const result = await chrome.runtime.sendMessage({target: 'background', ...data});
  if (!result?.ok) throw new Error(result?.error || '后台未响应');
  return result.job;
};
async function run(job) {
  const state = {id: job.id, controllers: new Set(), waiters:new Set(), bytes: 0, stopped: false, url: null};
  current = state;
  const entries = [];
  let cursor = 0;
  async function consume() {
    while (!state.stopped) {
      const index = cursor++;
      if (index >= job.items.length) return;
      const item = job.items[index];
      const controller = new AbortController(); state.controllers.add(controller);
      let timer,networkSince=0,remaining=120000;
      const pauseTimer=()=>{clearTimeout(timer);if(networkSince)remaining-=Date.now()-networkSince;networkSince=0;};
      const armTimer=()=>{pauseTimer();networkSince=Date.now();timer=setTimeout(()=>controller.abort(failure('TIMEOUT')),Math.max(0,remaining));};
      let reserved = 0, responseStatus = null;
      const discard=n=>{reserved-=n;state.bytes-=n;};
      const permission=async url=>{
        pauseTimer();
        while(!controller.signal.aborted){
          let wake,rejectWait;const waiting=new Promise((resolve,reject)=>{wake=resolve;rejectWait=reject;});
          waiting.catch(()=>{});
          // Register before querying: a grant arriving during the query cannot
          // be lost. The offscreen document retains the files already fetched.
          state.waiters.add(wake);
          const abort=()=>rejectWait(controller.signal.reason || failure('CANCELLED'));
          controller.signal.addEventListener('abort',abort,{once:true});
          try{const result=await send({type:'need-origin',id:job.id,index,url});if(result.granted){armTimer();return;}await waiting;}
          finally{state.waiters.delete(wake);controller.signal.removeEventListener('abort',abort);}
        }
        throw controller.signal.reason;
      };
      const request=async(url,external)=>{
        await permission(url);responseStatus=null;
        const response=await fetch(url,{credentials:external?'omit':'include',referrerPolicy:'no-referrer',signal:controller.signal});
        responseStatus=response.status;return response;
      };
      const read=async response=>{
        if(!response.body)throw failure('EMPTY_RESPONSE');
        const reader=response.body.getReader(),chunks=[];let size=0,html=false;
        try{
          while(true){
            const {value,done}=await reader.read();if(done)break;
            if(!size)html=isHtml(value,response.headers.get('content-type') || '');
            if(html && item.kind==='video' && job.autoResolveExternal!==false && size+value.length>EXTERNAL_PAGE_BYTES){await reader.cancel();throw failure('EXTERNAL_PAGE_LIMIT');}
            if(state.bytes+value.length>LIMIT){controller.abort(failure('BATCH_LIMIT'));throw controller.signal.reason;}
            size+=value.length;reserved+=value.length;state.bytes+=value.length;chunks.push(value);
          }
          const bytes=new Uint8Array(size);let at=0;for(const chunk of chunks){bytes.set(chunk,at);at+=chunk.length;}return bytes;
        }catch(error){discard(size);await reader.cancel().catch(()=>{});throw error;}
      };
      try {
        await send({type: 'item', id: job.id, index, state: 'active'});
        item.state='active';
        let bytes,contentUrl=item.url;
        if(item.kind==='video' && job.autoResolveExternal!==false){
          const resolved=await fetchExternalVideo(item,{request,read,discard,signal:controller.signal});
          bytes=resolved.bytes;item.externalInfo=resolved.externalInfo;item.resolvedUrl=resolved.resolvedUrl;
          item.filename=item.filename.replace(/\.[^.]+$/,`.${resolved.extension}`);
        }else{
          armTimer();const response=await fetch(item.url,{credentials:'include',signal:controller.signal});responseStatus=response.status;
          contentUrl=response.url || item.url;
          if(!response.ok)throw failure('HTTP',response.status);
          const contentType=response.headers.get('content-type') || '';
          if(/application\/json/i.test(contentType) || (item.kind!=='playable' && /text\/html/i.test(contentType))){await response.body?.cancel();throw failure(item.kind==='playable'?'PLAYABLE_NOT_HTML':'NOT_MEDIA');}
          bytes=await read(response);
        }
        if(!reserved)throw failure('EMPTY_RESPONSE');
        if(item.kind==='playable'){
          const info=inspectPlayable(bytes,contentUrl);
          if(info.errorCode)throw failure(info.errorCode);
          item.playableInfo=info;
        }
        if (state.stopped) return;
        entries.push({name: item.filename, bytes});
        Object.assign(item,{state:'complete',bytes:reserved});
        await send({type: 'item', id: job.id, index, state: 'complete',bytes:reserved,filename:item.filename,externalInfo:item.externalInfo || null,resolvedUrl:item.resolvedUrl || null,playableInfo:item.playableInfo || null});
      } catch (error) {
        state.bytes -= reserved;
        const detail=error.detail || controller.signal.reason?.detail || (error.code?failureDetail(error.code,error.httpStatus):failureDetail(responseStatus?'TRANSFER_INTERRUPTED':'NETWORK'));
        const result={state:'failed',error:detail.reason,errorCode:detail.code,httpStatus:error.httpStatus || responseStatus,suggestion:detail.suggestion,bytes:0,externalInfo:error.externalInfo || null};
        Object.assign(item,result);
        if (!state.stopped) await send({type: 'item', id: job.id, index,...result});
      } finally { pauseTimer(); state.controllers.delete(controller); }
    }
  }
  try {
    await Promise.all([consume(), consume(), consume()]);
    if (state.stopped) return;
    if (!entries.length) throw new Error('所有媒体获取失败，未生成 ZIP。请点击「查看下载报告.md」查看或保存报告。');
    await send({type: 'packing', id: job.id});
    const report = buildReport({...job,phase:'packaged'});
    entries.sort((a, b) => a.name.localeCompare(b.name));
    entries.push({name: '下载报告.md', bytes: new TextEncoder().encode(reportMarkdown(report))});
    const zip = makeZip(entries);
    entries.length = 0;
    state.url = URL.createObjectURL(zip);
    await send({type: 'zip-ready', id: job.id, url: state.url});
  } catch (error) {
    if (!state.stopped) await send({type: 'pack-error', id: job.id, error: error.message}).catch(() => {});
  }
}
chrome.runtime.onMessage.addListener((message, sender, respond) => {
  if (sender.id !== chrome.runtime.id || message.target !== 'offscreen') return;
  if (message.type === 'run') {
    if (current) { respond({ok: false, error: '已有打包任务'}); return; }
    respond({ok: true}); void run(message.job); return;
  }
  if (message.type === 'stop' && current) {
    current.stopped = true;
    for (const c of current.controllers) c.abort();
  }
  if(message.type==='permission-updated' && current)for(const wake of current.waiters)wake();
  if (message.type === 'release' && current?.url) URL.revokeObjectURL(current.url);
  respond({ok: true});
});
