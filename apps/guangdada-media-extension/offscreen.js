import {StreamZip} from './stream-zip.js';
import {crc32,crc32Accumulator} from './zip.js';
import {openDiskSession,diskError} from './disk-store.js';
import {buildReport,reportMarkdown,failureDetail} from './report.js';
import {inspectPlayableFile} from './playable.js';
import {fetchExternalVideo,isHtml,EXTERNAL_PAGE_BYTES} from './external.js';
let current=null;
const failure=(code,status)=>Object.assign(new Error(failureDetail(code,status).reason),{detail:failureDetail(code,status),code,httpStatus:status || null});
const send=async data=>{try{const result=await chrome.runtime.sendMessage({target:'background',...data});if(!result?.ok)throw new Error(result?.error || '后台未响应');return data.type==='need-origin'?{granted:result.granted ?? result.job?.granted}:result.job;}catch(error){throw Object.assign(new Error('任务进度无法保存，已停止生成ZIP。'),{code:'TASK_STATE',cause:error});}};
const stopped=()=>Object.assign(new Error('任务已停止'),{code:'CANCELLED'});
async function cleanup(state){
  if(!state)return;
  state.stopped=true;
  for(const c of state.controllers)c.abort(stopped());
  for(const wake of state.waiters)wake();
  if(state.url){URL.revokeObjectURL(state.url);state.url=null;}
  if(state.disk)await state.disk.cleanup();
}
async function run(job,state){
  let cursor=0,zip;
  const check=()=>{if(state.stopped)throw stopped();if(state.fatal)throw state.fatal;};
  const fatal=error=>{if(!state.fatal){state.fatal=error;for(const c of state.controllers)c.abort(error);}};
  async function consume(){
    while(!state.stopped && !state.fatal){
      const index=cursor++;if(index>=job.items.length)return;
      const item=job.items[index],controller=new AbortController();state.controllers.add(controller);
      let timer,networkSince=0,remaining=120000,responseStatus=null,payload=null,attempt=0;
      const pauseTimer=()=>{clearTimeout(timer);if(networkSince)remaining-=Date.now()-networkSince;networkSince=0;};
      const armTimer=()=>{pauseTimer();networkSince=Date.now();timer=setTimeout(()=>controller.abort(failure('TIMEOUT')),Math.max(0,remaining));};
      const permission=async url=>{
        pauseTimer();
        while(!controller.signal.aborted){
          check();let wake,rejectWait;const waiting=new Promise((resolve,reject)=>{wake=resolve;rejectWait=reject;});waiting.catch(()=>{});state.waiters.add(wake);
          const abort=()=>rejectWait(controller.signal.reason || failure('CANCELLED'));controller.signal.addEventListener('abort',abort,{once:true});
          try{const result=await send({type:'need-origin',id:job.id,index,url});if(result.granted)return;await waiting;}
          finally{state.waiters.delete(wake);controller.signal.removeEventListener('abort',abort);}
        }
        throw controller.signal.reason;
      };
      const request=async(url,external)=>{await permission(url);responseStatus=null;armTimer();const response=await fetch(url,{credentials:external?'omit':'include',referrerPolicy:'no-referrer',signal:controller.signal});responseStatus=response.status;return response;};
      const discard=async()=>{if(payload){const old=payload;payload=null;await state.disk.remove(old.name);}};
      const read=async response=>{
        if(!response.body)throw failure('EMPTY_RESPONSE');
        pauseTimer();let spool;
        try{spool=await state.disk.create(`item-${index}-${attempt++}.bin`);}catch(error){throw diskError(error);}
        const reader=response.body.getReader(),crc=crc32Accumulator();let size=0,prefix=new Uint8Array(),html=false;
        try{
          while(true){
            check();armTimer();let part;
            try{part=await reader.read();}finally{pauseTimer();}
            if(part.done)break;
            const value=part.value;
            if(prefix.length<1024){const head=new Uint8Array(Math.min(1024,prefix.length+value.length));head.set(prefix);head.set(value.subarray(0,head.length-prefix.length),prefix.length);prefix=head;}
            html ||= isHtml(prefix,response.headers.get('content-type') || '');
            if(html && item.kind==='video' && job.autoResolveExternal!==false && size+value.length>EXTERNAL_PAGE_BYTES)throw failure('EXTERNAL_PAGE_LIMIT');
            crc.update(value);size+=value.length;
            // The network chunk is released after this awaited disk write.
            await spool.write(value);
          }
          if(!size)throw failure('EMPTY_RESPONSE');
          await spool.close();const file=await spool.file();
          payload={name:spool.name,length:size,prefix,crc:crc.digest(),file,stream:()=>file.stream(),text:()=>file.text()};return payload;
        }catch(error){await spool.abort();await state.disk.remove(spool.name).catch(()=>{});throw error;}
        finally{await reader.cancel().catch(()=>{});reader.releaseLock();}
      };
      try{
        await send({type:'item',id:job.id,index,state:'active'});item.state='active';
        let data,contentUrl=item.url;
        if(item.kind==='video' && job.autoResolveExternal!==false){
          const resolved=await fetchExternalVideo(item,{request,read,discard,signal:controller.signal});data=resolved.bytes;item.externalInfo=resolved.externalInfo;item.resolvedUrl=resolved.resolvedUrl;item.filename=item.filename.replace(/\.[^.]+$/,`.${resolved.extension}`);
        }else{
          armTimer();const response=await fetch(item.url,{credentials:'include',signal:controller.signal});responseStatus=response.status;contentUrl=response.url || item.url;
          if(!response.ok){await response.body?.cancel();throw failure('HTTP',response.status);}
          const type=response.headers.get('content-type') || '';
          if(/application\/json/i.test(type) || (item.kind!=='playable' && /text\/html/i.test(type))){await response.body?.cancel();throw failure(item.kind==='playable'?'PLAYABLE_NOT_HTML':'NOT_MEDIA');}
          data=await read(response);
          if(item.kind!=='playable' && isHtml(data.prefix,type))throw failure('NOT_MEDIA');
        }
        pauseTimer();check();
        if(item.kind==='playable'){const info=await inspectPlayableFile(data.file,contentUrl,controller.signal);if(info.errorCode)throw failure(info.errorCode);item.playableInfo=info;}
        // Three bounded spools can download concurrently; one serialized ZIP
        // writer appends completed files and then frees their temporary spools.
        try{await zip.add(item.filename,data);}catch(error){throw diskError(error);}
        check();state.mediaBytes+=data.length;await discard();
        Object.assign(item,{state:'complete',bytes:data.length});
        await send({type:'item',id:job.id,index,state:'complete',bytes:data.length,filename:item.filename,externalInfo:item.externalInfo || null,resolvedUrl:item.resolvedUrl || null,playableInfo:item.playableInfo || null});
      }catch(error){
        pauseTimer();await discard().catch(()=>{});
        const actual=controller.signal.aborted?controller.signal.reason || error:error;
        if(String(actual.code || '').startsWith('DISK_') || actual.code==='TASK_STATE')fatal(actual);
        const detail=actual.detail || error.detail || (actual.code?failureDetail(actual.code,actual.httpStatus):failureDetail(responseStatus?'TRANSFER_INTERRUPTED':'NETWORK'));
        const result={state:'failed',error:detail.reason,errorCode:detail.code,httpStatus:error.httpStatus || responseStatus,suggestion:detail.suggestion,bytes:0,externalInfo:error.externalInfo || null};Object.assign(item,result);
        if(!state.stopped)await send({type:'item',id:job.id,index,...result});
      }finally{pauseTimer();state.controllers.delete(controller);}
    }
  }
  try{
    state.disk=await openDiskSession(job.id);check();
    const output=await state.disk.create('archive.zip');zip=new StreamZip(output);
    await send({type:'disk-ready',id:job.id});
    await Promise.all([consume(),consume(),consume()]);check();
    if(!zip.entries.length)throw new Error('所有媒体获取失败，未生成 ZIP。请点击「查看下载报告.md」查看或保存报告。');
    await send({type:'packing',id:job.id});
    const text=new TextEncoder().encode(reportMarkdown(buildReport({...job,storageMode:'disk',phase:'packaged'})));
    await zip.add('下载报告.md',{length:text.length,crc:crc32(text),stream:()=>new Blob([text]).stream()});
    let info;try{info=await zip.close();}catch(error){throw diskError(error);}check();
    const file=await output.file();if(file.size!==info.bytes)throw diskError(new Error('ZIP文件长度不符'));
    state.url=URL.createObjectURL(file);
    await send({type:'zip-ready',id:job.id,url:state.url,storageMode:'disk',archiveBytes:info.bytes,zip64:info.zip64});
  }catch(error){
    if(!state.stopped){const reason=String(error.code || '').startsWith('DISK_')?failureDetail(error.code).reason:error.message;await send({type:'pack-error',id:job.id,error:reason,errorCode:error.code || ''}).catch(()=>{});}
    await cleanup(state).catch(()=>{});
  }
}
chrome.runtime.onMessage.addListener((message,sender,respond)=>{
  if(sender.id!==chrome.runtime.id || message.target!=='offscreen')return;
  if(message.type==='owner'){respond({ok:true,id:current?.id || null});return;}
  if(message.type==='run'){
    if(current){respond({ok:false,error:'已有打包任务'});return;}
    current={id:message.job.id,controllers:new Set(),waiters:new Set(),mediaBytes:0,stopped:false,url:null,disk:null,fatal:null};respond({ok:true});void run(message.job,current);return;
  }
  if(message.type==='cleanup' || message.type==='stop' || message.type==='release'){
    if(message.id && current?.id!==message.id){respond({ok:false,error:'任务身份不匹配'});return;}
    cleanup(current).then(()=>respond({ok:true}),error=>respond({ok:false,error:error.message}));return true;
  }
  if(message.type==='permission-updated' && current && (!message.id || message.id===current.id))for(const wake of current.waiters)wake();
  respond({ok:true});
});
