// Media bytes belong to one offscreen writer. Address ledgers and selections
// are independent durable tasks, including the legacy single-job ledger.
import {canResumeCollection,resumePage} from './collection-resume.js';
export const taskActive=job=>['collecting','queued','awaiting-permission','fetching','packing','downloading'].includes(job?.phase);
export const ownsDownload=job=>['fetching','packing','downloading'].includes(job?.phase) || (job?.phase==='awaiting-permission' && job.waitingForExternalPermission===true);
export const nextQueued=jobs=>jobs.filter(j=>j.phase==='queued').sort((a,b)=>(a.queuedAt || a.createdAt).localeCompare(b.queuedAt || b.createdAt) || a.id.localeCompare(b.id))[0];
export const taskSummary=job=>({id:job.id,taskName:job.taskName || `任务 ${job.createdAt}`,createdAt:job.createdAt,phase:job.phase,addressStage:job.addressStage,selectedCardCount:job.selectedCardCount || 0,complete:job.items?.filter(i=>i.state==='complete').length || 0,total:job.items?.length || 0,waitingForExternalPermission:!!job.waitingForExternalPermission,canResumeCollection:canResumeCollection(job),resumePage:resumePage(job),completedPages:job.collection?.completedPages?.length || 0});
export function taskName(value,createdAt){return String(value || '').trim().slice(0,60) || `任务 ${createdAt.slice(5,19).replace('T',' ')}`;}
export function createTaskStore(storage){
  const tasks=new Map();let selectedId,ready,chain=Promise.resolve();
  const locked=fn=>{const p=chain.then(fn);chain=p.catch(()=>{});return p;};
  const initialize=()=>ready ||= (async()=>{
    const saved=await storage.get(null);
    for(const [key,value] of Object.entries(saved))if(key.startsWith('mediaTask:') && value?.id===key.slice(10))tasks.set(value.id,value);
    if(saved.job?.id && !tasks.has(saved.job.id)){
      tasks.set(saved.job.id,saved.job);
      await storage.set({['mediaTask:'+saved.job.id]:saved.job});
    }
    selectedId=tasks.has(saved.selectedTaskId)?saved.selectedTaskId:saved.job?.id || tasks.keys().next().value;
    if(selectedId)await storage.set({selectedTaskId:selectedId,job:tasks.get(selectedId)});
  })();
  return {
    async get(id){await initialize();return tasks.get(id || selectedId) || null;},
    async all(){await initialize();return [...tasks.values()];},
    async save(job){await initialize();tasks.set(job.id,job);return locked(async()=>{await storage.set({['mediaTask:'+job.id]:job,...(selectedId===job.id?{job}:{})});});},
    async focus(job){await initialize();return locked(async()=>{const current=tasks.get(job.id) || job;tasks.set(current.id,current);selectedId=current.id;await storage.set({selectedTaskId:selectedId,['mediaTask:'+current.id]:current,job:current});});},
  };
}
