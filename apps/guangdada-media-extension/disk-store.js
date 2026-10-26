export function diskError(error){
  if(String(error?.code || '').startsWith('DISK_'))return error;
  return Object.assign(new Error(error?.name==='QuotaExceededError'?'本机临时磁盘空间不足，无法继续写入 ZIP。':'本机临时文件写入或读取失败，无法继续生成可靠的 ZIP。'),{code:error?.name==='QuotaExceededError'?'DISK_QUOTA':'DISK_IO'});
}
export async function clearDiskSessions(storage=globalThis.navigator?.storage){
  if(!storage?.getDirectory)return;
  let parent;try{parent=await (await storage.getDirectory()).getDirectoryHandle('gdd-media-temporary');}catch(error){if(error.name==='NotFoundError')return;throw diskError(error);}
  for await(const name of parent.keys()){
    for(let attempt=0;;attempt++){
      try{await parent.removeEntry(name,{recursive:true});break;}
      catch(error){if(error.name==='NotFoundError')break;if(error.name!=='NoModificationAllowedError' || attempt>=4)throw diskError(error);await new Promise(resolve=>setTimeout(resolve,100));}
    }
  }
}
export async function openDiskSession(id,storage=globalThis.navigator?.storage){
  if(!storage?.getDirectory)throw Object.assign(new Error('当前浏览器无法使用本机临时磁盘，请更新Chrome后重试。'),{code:'DISK_UNAVAILABLE'});
  if(!/^[a-zA-Z0-9_-]{1,100}$/.test(id))throw new Error('无效临时任务标识');
  let parent,dir;
  try{
    parent=await (await storage.getDirectory()).getDirectoryHandle('gdd-media-temporary',{create:true});
    // The task queue permits only one offscreen ZIP writer. Other address
    // collectors/queued ledgers have no temporary media session to delete.
    for await(const name of parent.keys())await parent.removeEntry(name,{recursive:true});
    dir=await parent.getDirectoryHandle(id,{create:true});
  }catch(error){throw diskError(error);}
  const writers=new Set(),operations=new Set();let disposed=false,cleaning;
  const check=()=>{if(disposed)throw Object.assign(new Error('任务已停止'),{code:'CANCELLED'});};
  const track=fn=>{const pending=fn();operations.add(pending);pending.then(()=>operations.delete(pending),()=>operations.delete(pending));return pending;};
  const session={
    create(name){return track(async()=>{
      check();let handle,writer;
      try{handle=await dir.getFileHandle(name,{create:true});writer=(await handle.createWritable()).getWriter();writers.add(writer);check();}
      catch(error){if(writer){await writer.abort().catch(()=>{});writer.releaseLock();writers.delete(writer);}throw error.code==='CANCELLED'?error:diskError(error);}
      const call=(method,value)=>track(async()=>{check();try{return await writer[method](value);}catch(error){throw diskError(error);}});
      return {name,write:value=>call('write',value),async close(){try{await call('close');}finally{writer.releaseLock();writers.delete(writer);}},async abort(){try{await writer.abort();}catch{/* May already be closed or aborted. */}finally{writer.releaseLock();writers.delete(writer);}},async file(){check();try{return await handle.getFile();}catch(error){throw diskError(error);}}};
    });},
    async remove(name){try{await dir.removeEntry(name);}catch(error){if(error.name!=='NotFoundError' && !disposed)throw diskError(error);}},
    cleanup(){
      if(cleaning)return cleaning;disposed=true;
      // FileSystemWritableFileStream.write holds its stream lock until the
      // pending write settles. Aborting it while locked can reject without
      // closing its temporary file, so drain in-flight operations first.
      cleaning=(async()=>{await Promise.allSettled([...operations]);await Promise.allSettled([...writers].map(async w=>{try{await w.abort();}finally{w.releaseLock();}}));writers.clear();try{await parent.removeEntry(id,{recursive:true});}catch(error){if(error.name!=='NotFoundError')throw diskError(error);}})();return cleaning;
    }
  };return session;
}
