// Node-only execution of the actual service worker with a deterministic Chrome
// adapter. No browser process, DOM automation, permissions prompt or network.
import {parentPort,workerData} from 'node:worker_threads';
import {setImmediate as realImmediate} from 'node:timers';
let time=1000000,timerId=0,storage=structuredClone(workerData?.storage || {});
let releaseContext;
const timers=new Map(),listeners=[],events=()=>({addListener(fn){this.listeners.push(fn);},listeners:[]});
const clone=x=>structuredClone(x);
const defaultContext={version:1,location:'https://www.guangdada.net/modules/creative/display-ads',query:[],hash:'',inputs:[{label:'App',value:'zeta'}],selected:[{text:'最新创意'}],chips:[]};
const sim={page:1,lastPage:2,loading:false,empty:false,scope:defaultContext,permissions:false,nextMode:'advance',owner:null,dialog:null,detailsReady:true,reads:[],nextClicks:0,closeClicks:0,runs:[],cancels:[],writes:[],...clone(workerData?.sim || {})};
function scan(){
  if(sim.scans?.[sim.page])return clone(sim.scans[sim.page]);
  const keys=sim.empty?[]:[`p${sim.page}-a`,`p${sim.page}-b`];
  return {pageNumber:sim.page,cardKeys:keys,cardDates:keys.map(()=>({date:sim.oldFrom && sim.page>=sim.oldFrom?'2026-10-23':'2026-10-24',source:'visible_card_date_range'})),newestFirst:sim.newestFirst!==false,cardCount:keys.length,items:keys.map((adId,i)=>({adId,kind:'image',cardIndex:i+1,url:`https://cdn.example/${adId}.png`})),missing:[],selectedCardNumbers:[]};
}
globalThis.fetch=()=>{throw new Error('Offline test blocked network');};
globalThis.setTimeout=(fn,delay=0)=>{const id=++timerId;timers.set(id,{fn,at:time+Math.max(0,delay)});return id;};
globalThis.clearTimeout=id=>timers.delete(id);
Date.now=()=>time;
async function settle(){for(let i=0;i<40;i++)await Promise.resolve();await new Promise(realImmediate);for(let i=0;i<40;i++)await Promise.resolve();}
async function advance(ms){
  const target=time+ms;let count=0;
  for(;;){
    await settle();const next=[...timers].filter(([,t])=>t.at<=target).sort((a,b)=>a[1].at-b[1].at)[0];if(!next)break;
    if(++count>5000)throw new Error('Unbounded timer loop');time=Math.max(time,next[1].at);timers.delete(next[0]);next[1].fn();
  }
  time=target;await settle();
}
async function dispatch(message,sender={id:'offline-extension',url:'chrome-extension://offline-extension/popup.html'}){
  if(message.target==='offscreen'){
    if(message.type==='owner')return {ok:true,id:sim.owner};
    if(message.type==='run'){sim.owner=message.job.id;sim.runs.push(clone(message.job));return {ok:true};}
    return {ok:true};
  }
  return new Promise((resolve,reject)=>{
    let handled=false;
    for(const listener of listeners){try{if(listener(message,sender,resolve)===true)handled=true;}catch(error){reject(error);}}
    if(!handled)resolve(undefined);
  });
}
globalThis.chrome={
  runtime:{id:'offline-extension',getURL:p=>'chrome-extension://offline-extension/'+p,onMessage:{addListener:fn=>listeners.push(fn)},sendMessage:dispatch,async getContexts(){return sim.owner?[{contextType:'OFFSCREEN_DOCUMENT'}]:[];}},
  storage:{local:{async get(key){if(key===null)return clone(storage);const keys=Array.isArray(key)?key:[key];return Object.fromEntries(keys.map(k=>[k,clone(storage[k])]));},async set(values){
    const task=Object.entries(values).find(([key])=>key.startsWith('mediaTask:'))?.[1];
    if(sim.failResumeSave && task?.phase==='collecting' && task.collection?.resumeCount){sim.failResumeSave=false;throw new Error('Injected state write failure');}
    Object.assign(storage,clone(values));if(task)sim.writes.push({id:task.id,phase:task.phase,stage:task.collection?.stage,resumeCount:task.collection?.resumeCount || 0});
  }}},
  tabs:{async get(id){if(sim.closedTabs?.includes(id))throw new Error('Tab closed');return {id,url:sim.url || 'https://www.guangdada.net/modules/creative/display-ads'};},onUpdated:events(),onRemoved:events()},
  scripting:{async executeScript(options){
    const name=options.func?.name,action=typeof options.args?.[0]==='string'?options.args[0]:'read',expected=options.args?.[1];
    sim.reads.push({name,action,page:sim.page,time,tabId:options.target?.tabId});
    if(name==='pageAccess')return [{result:sim.blocked?{blocked:true,code:sim.blocked,reason:'Fixture login/captcha blocked'}:{blocked:false}}];
    if(name==='collectionContext'){
      if(sim.holdContext){sim.contextHeld=true;await new Promise(resolve=>{releaseContext=resolve;});sim.contextHeld=false;}
      return [{result:clone(sim.scope)}];
    }
    if(name==='readMetadataEvidence')return [{result:{rows:[]}}];
    if(name==='selectionContext')return [{result:{pageNumber:sim.page,loading:sim.loading}}];
    if(name==='collectMedia'){
      if(sim.readFailures>0){sim.readFailures--;return [{result:undefined}];}
      if(sim.empty)return [{result:undefined}];
      return [{result:scan()}];
    }
    if(name==='detailControl'){
      if(action==='idle')return [{result:{visible:!!sim.dialog,adId:sim.dialog?.adId || null}}];
      if(action==='close'){if(sim.dialog && sim.dialog.adId!==expected.adId)return [{result:{failure:'DETAIL_ID_MISMATCH'}}];sim.closeClicks++;if(!sim.holdClose)sim.dialog=null;return [{result:{closed:true}}];}
      if(action==='open'){sim.dialog={adId:expected.adId};return [{result:{opened:true,pending:true,dialogVisible:true,identityConfirmed:true,adId:expected.adId}}];}
      return [{result:sim.detailsReady?{adId:expected.adId,identityConfirmed:true,dialogVisible:true,...(expected.readDate?{firstDiscovered:'2026-10-24',dateSource:'visible_detail_date_range'}:{url:`https://cdn.example/${expected.adId}.mp4`})}:{pending:true,dialogVisible:true,adId:expected.adId}}];
    }
    if(name==='pageControl'){
      const keys=scan().cardKeys,diagnostic={action,expectedPage:expected?.pageNumber,actualPage:sim.page,expectedCards:expected?.cardKeys?.length,actualCards:keys.length,loading:sim.loading};
      if(expected && (expected.pageNumber!==sim.page || JSON.stringify(expected.cardKeys)!==JSON.stringify(keys)))return [{result:{error:'Fixture card identity mismatch',code:expected.pageNumber!==sim.page?'PAGE_NUMBER_CHANGED':'PAGE_CARDS_CHANGED',notClicked:true,diagnostic}}];
      if(action==='next'){
        if(sim.loading || sim.nextMode==='reject')return [{result:{error:'Fixture still loading',code:'PAGE_LOADING',notClicked:true,diagnostic}}];
        if(sim.page>=sim.lastPage)return [{result:{clicked:false,pageNumber:sim.page,cardKeys:keys,hasNext:false,loading:false}}];
        sim.nextClicks++;
        const previous=sim.page;if(!['stall','uncertain-stall'].includes(sim.nextMode))sim.page++;
        if(sim.loadNext){sim.loading=true;sim.empty=true;}
        if(sim.nextMode.startsWith('uncertain'))return [{result:undefined}];
        return [{result:{clicked:true,pageNumber:previous,cardKeys:keys,hasNext:true,loading:false}}];
      }
      return [{result:{pageNumber:sim.page,cardKeys:keys,hasNext:sim.page<sim.lastPage,loading:sim.loading}}];
    }
    throw new Error('Unmodeled script '+name);
  }},
  permissions:{async contains(){return sim.permissions;},onAdded:events()},
  downloads:{async search({id}){return [{id,state:'in_progress'}];},async cancel(id){sim.cancels.push(id);},async download(){return 1;},onChanged:events()},
  offscreen:{async createDocument(){},async closeDocument(){sim.owner=null;}},
  alarms:{async create(){},async clear(){},onAlarm:events()},action:{async setBadgeText(){}}
};
await import('../../background.js');await settle();
parentPort.on('message',async({id,command,...data})=>{
  try{
    let result;
    if(command==='send')result=await dispatch(data.message);
    else if(command==='fire'){void dispatch(data.message).then(r=>{sim.fireResult=r;});result={sent:true};}
    else if(command==='advance'){await advance(data.ms);result={time};}
    else if(command==='configure'){Object.assign(sim,clone(data.patch));if(!sim.holdContext && releaseContext){releaseContext();releaseContext=null;}result={ok:true};}
    else if(command==='state'){await settle();result={storage:clone(storage),sim:clone(sim),time};}
    else throw new Error('Unknown test command');
    parentPort.postMessage({id,result});
  }catch(error){parentPort.postMessage({id,error:error.stack || error.message});}
});
parentPort.postMessage({ready:true});
