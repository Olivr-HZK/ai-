// Actual popup module, deterministic DOM/Chrome adapter, no browser/network.
import {parentPort,workerData} from 'node:worker_threads';
import {readFile} from 'node:fs/promises';
import {taskSummary} from '../../tasks.js';
const clone=value=>structuredClone(value),elements=new Map();let interval,releaseStatus;
const sim={pageReady:true,page:1,url:'https://www.guangdada.net/modules/creative/display-ads',tabId:1,jobs:[],basket:null,calls:[],reads:[],...clone(workerData?.sim || {})};
const element=tag=>({tagName:tag.toUpperCase(),value:'',checked:false,disabled:false,hidden:false,textContent:'',children:[],replaceChildren(...children){this.children=children;}});
const html=await readFile(new URL('../../popup.html',import.meta.url),'utf8');
for(const match of html.matchAll(/<([a-z]+)[^>]*\bid="([^"]+)"[^>]*>/g)){
  const e=element(match[1]);e.checked=/\bchecked\b/.test(match[0]);e.hidden=/\bhidden\b/.test(match[0]);e.value=/\bvalue="([^"]*)"/.exec(match[0])?.[1] || '';elements.set(match[2],e);
}
elements.get('mode').value='current';elements.get('end').value='count';
globalThis.document={getElementById:id=>{if(!elements.has(id))throw new Error('Missing popup control: '+id);return elements.get(id);},createElement:element};
globalThis.window={close(){sim.closed=true;}};
globalThis.setInterval=fn=>{interval=fn;return 1;};globalThis.fetch=()=>{throw new Error('Offline popup test blocked network');};
const state=()=>({ok:true,job:sim.jobs.find(j=>j.id===sim.selected) || sim.jobs.at(-1) || null,tasks:sim.jobs.map(taskSummary),collector:sim.jobs.find(j=>j.phase==='collecting')?.id || null,basket:sim.basket});
globalThis.chrome={
  tabs:{async query(){return [{id:sim.tabId,url:sim.url}];},async create(){throw new Error('Unexpected tab creation');}},
  scripting:{async executeScript({func,args}){
    sim.reads.push({name:func.name,args});
    if(func.name==='pageAccess')return [{result:sim.blocked?{blocked:true,reason:'请重新登录广大大'}:{blocked:false}}];
    if(func.name==='selectionContext')return [{result:{pageNumber:sim.page,loading:!!sim.loading}}];
    if(func.name==='collectMedia'){
      if(sim.missingResults>0){sim.missingResults--;return [{result:undefined}];}
      if(!sim.pageReady)throw new Error('未识别到展示广告素材卡片');
      if(sim.scan)return [{result:clone(sim.scan)}];
      if(sim.partial)return [{result:{pageNumber:sim.page,cardCount:2,cardKeys:['ad-1',null],items:[{adId:'ad-1',kind:'image',cardIndex:1,url:'https://cdn.example/a.png'}],missing:[{cardIndex:2,reasonCode:'CARD_DATA_MISSING'}],cardDates:[],selectedCardNumbers:[]}}];
      return [{result:{pageNumber:sim.page,cardCount:1,cardKeys:['ad-1'],items:[{adId:'ad-1',kind:'image',cardIndex:1,url:'https://cdn.example/a.png'}],missing:[],cardDates:[],selectedCardNumbers:[]}}];
    }
    throw new Error('Unexpected page operation: '+func.name);
  }},
  storage:{local:{async get(){return {dateCutoffSettings:{enabled:false,since:''}};},async set(){}}},
  runtime:{async sendMessage(message){
    sim.calls.push(clone(message));
    if(['status','refresh-state'].includes(message.type)){
      if(sim.statusError)throw new Error('Injected worker response failure');
      const snapshot=clone(state());
      if(message.type==='status' && sim.holdStatus){sim.statusHeld=true;await new Promise(resolve=>{releaseStatus=resolve;});sim.statusHeld=false;}
      return snapshot;
    }
    if(message.type==='get-basket')return {ok:true,basket:clone(sim.basket)};
    throw new Error('Unexpected task mutation: '+message.type);
  }},
};
await import('../../popup.js');
parentPort.on('message',async({id,command,...data})=>{
  try{
    let result;
    if(command==='refresh'){await elements.get('refresh-state').onclick();result={ok:true};}
    else if(command==='configure'){Object.assign(sim,clone(data.patch));if(!sim.holdStatus && releaseStatus){releaseStatus();releaseStatus=null;}result={ok:true};}
    else if(command==='fields'){for(const [id,patch]of Object.entries(data.values))Object.assign(elements.get(id),patch);result={ok:true};}
    else if(command==='poll'){interval();result={ok:true};}
    else if(command==='state'){await new Promise(setImmediate);result={sim:clone(sim),elements:JSON.parse(JSON.stringify(Object.fromEntries(elements),(_,v)=>typeof v==='function'?undefined:v))};}
    else throw new Error('Unknown popup test command');
    parentPort.postMessage({id,result});
  }catch(error){parentPort.postMessage({id,error:error.stack || error.message});}
});
parentPort.postMessage({ready:true});
