import test from 'node:test';
import assert from 'node:assert/strict';
import {Worker} from 'node:worker_threads';

async function harness(t,data={}){
  const worker=new Worker(new URL('./helpers/background-harness.js',import.meta.url),{workerData:data});t.after(()=>worker.terminate());
  let seq=0;const pending=new Map();let ready;
  const boot=new Promise((resolve,reject)=>{ready=resolve;worker.on('error',reject);});
  worker.on('message',m=>{if(m.ready){ready();return;}const waiter=pending.get(m.id);if(!waiter)return;pending.delete(m.id);clearTimeout(waiter.timer);m.error?waiter.reject(new Error(m.error)):waiter.resolve(m.result);});
  worker.on('error',error=>{for(const p of pending.values()){clearTimeout(p.timer);p.reject(error);}pending.clear();});await boot;
  const rpc=(command,args={})=>new Promise((resolve,reject)=>{const id=++seq,timer=setTimeout(()=>{pending.delete(id);reject(new Error('Offline harness timed out: '+command));},10000);pending.set(id,{resolve,reject,timer});worker.postMessage({id,command,...args});});
  return {rpc,send:message=>rpc('send',{message}),state:()=>rpc('state'),advance:ms=>rpc('advance',{ms}),configure:patch=>rpc('configure',{patch})};
}
const start=async h=>{const r=await h.send({type:'start',tabId:1,mode:'pages',untilEnd:true,kinds:['image','video'],firstDiscoveredSince:'2026-10-24',autoResolveExternal:true});assert.equal(r.ok,true,r.error);return r.job.id;};
const read=(state,id)=>state.storage['mediaTask:'+id];
async function advanceUntil(h,predicate,max=80){for(let i=0;i<max;i++){const s=await h.state();if(predicate(s))return s;await h.advance(250);}throw new Error('Offline state did not reach expectation');}

test('actual worker manually pauses without media, persists across restart and resumes the same task',async t=>{
  const h=await harness(t),id=await start(h);assert.equal((await h.send({type:'pause-collection',id})).ok,true);let s=await h.state();assert.equal(read(s,id).phase,'collection-paused');assert.equal(s.sim.runs.length,0);assert.equal(read(s,id).collection.completedPages.length,0);
  const restart=await harness(t,{storage:s.storage});await restart.advance(90000);s=await restart.state();assert.equal(read(s,id).phase,'collection-paused');assert.equal(s.sim.reads.length,0);
  const r=await restart.send({type:'resume-collection',id,tabId:1});assert.equal(r.ok,true,r.error);assert.equal(r.job.id,id);await restart.advance(12000);s=await restart.state();assert.equal(read(s,id).phase,'awaiting-permission');assert.equal(read(s,id).stopReason,'last-page');assert.equal(read(s,id).selectedCardCount,4);assert.equal(new Set(read(s,id).selections.map(i=>i.adId)).size,4);assert.equal(read(s,id).collection.resumeCount,1);
});

test('page-load timeout pauses and resumes the arrived next page without reclicking or rereading completed pages',async t=>{
  const h=await harness(t,{sim:{loadNext:true}}),id=await start(h);await h.advance(40000);let s=await h.state();assert.equal(read(s,id).phase,'collection-paused');assert.equal(read(s,id).collection.stage,'waiting');assert.equal(s.sim.nextClicks,1);assert.deepEqual(read(s,id).collection.completedPages.map(p=>p.pageNumber),[1]);assert.equal(s.sim.runs.length,0);
  const mark=s.sim.reads.length;await h.configure({loading:false,empty:false,loadNext:false});const r=await h.send({type:'resume-collection',id,tabId:1});assert.equal(r.ok,true,r.error);await h.advance(7000);s=await h.state();assert.equal(read(s,id).stopReason,'last-page');assert.equal(s.sim.nextClicks,1);assert.equal(s.sim.reads.slice(mark).some(r=>r.name==='collectMedia' && r.page===1),false);assert.equal(read(s,id).selectedCardCount,4);
});

test('resume rejects filter changes, foreign detail, loading state and identity replacement while retaining checkpoint',async t=>{
  const h=await harness(t),id=await start(h);await h.send({type:'pause-collection',id});const original=(await h.state()).sim.scope;
  for(const patch of [{scope:{...original,inputs:[{label:'App',value:'another'}]}},{scope:original,dialog:{adId:'foreign'}},{dialog:null,loading:true},{loading:false,scans:{1:{pageNumber:1,cardKeys:['changed'],items:[{adId:'changed',cardIndex:1,kind:'image',url:'https://cdn.example/changed.png'}],missing:[],cardDates:[{date:'2026-10-24',source:'visible_card_date_range'}],newestFirst:true,cardCount:1}}}]){
    await h.configure(patch);const r=await h.send({type:'resume-collection',id,tabId:1});assert.equal(r.ok,false);const s=await h.state();assert.equal(read(s,id).phase,'collection-paused');assert.equal(s.sim.nextClicks,0);assert.equal(s.sim.runs.length,0);
  }
});

test('unknown next-click outcome at old page requires manual restoration, never a repeated click',async t=>{
  const h=await harness(t,{sim:{nextMode:'uncertain-stall'}}),id=await start(h);await h.advance(5000);let s=await h.state();assert.equal(read(s,id).phase,'collection-paused');assert.equal(read(s,id).collection.clickIntent.outcome,'uncertain');assert.equal(s.sim.nextClicks,1);
  const denied=await h.send({type:'resume-collection',id,tabId:1});assert.equal(denied.ok,false);assert.match(denied.error,/不会重复点击/);assert.equal((await h.state()).sim.nextClicks,1);
  await h.configure({page:2,nextMode:'advance'});assert.equal((await h.send({type:'resume-collection',id,tabId:1})).ok,true);await h.advance(5000);s=await h.state();assert.equal(read(s,id).stopReason,'last-page');assert.equal(s.sim.nextClicks,1);assert.equal(read(s,id).selectedCardCount,4);
});

test('lost click return with actual next page reached continues without duplicate click',async t=>{
  const h=await harness(t,{sim:{nextMode:'uncertain-advance'}}),id=await start(h);await h.advance(5000);assert.equal(read(await h.state(),id).phase,'collection-paused');assert.equal((await h.send({type:'resume-collection',id,tabId:1})).ok,true);await h.advance(5000);const s=await h.state();assert.equal(s.sim.nextClicks,1);assert.equal(read(s,id).selectedCardCount,4);assert.equal(read(s,id).collectionIncomplete,false);
});

test('only explicit finish of a paused collection queues its saved addresses and disables continuation',async t=>{
  const h=await harness(t,{sim:{permissions:true}}),id=await start(h);await h.send({type:'pause-collection',id});assert.equal((await h.state()).sim.runs.length,0);
  const r=await h.send({type:'download-collected',id});assert.equal(r.ok,true,r.error);const s=await h.state();assert.equal(s.sim.runs.length,1);assert.equal(s.sim.runs[0].id,id);assert.equal(s.sim.runs[0].items.length,2);assert.ok(s.sim.runs[0].items.every(i=>i.filename));assert.equal(read(s,id).collectionIncomplete,true);assert.equal((await h.send({type:'resume-collection',id,tabId:1})).ok,false);
});

test('captcha and login blocks pause, cannot be bypassed by resume, and resume uses a verified replacement tab',async t=>{
  const h=await harness(t),id=await start(h);await h.configure({blocked:'ADDRESS_VERIFICATION_REQUIRED'});await h.advance(1000);let s=await h.state();assert.equal(read(s,id).phase,'collection-paused');assert.equal(s.sim.runs.length,0);assert.equal((await h.send({type:'resume-collection',id,tabId:1})).ok,false);
  await h.configure({blocked:null,closedTabs:[1]});assert.equal((await h.send({type:'resume-collection',id,tabId:2})).ok,true);await h.advance(10000);s=await h.state();assert.equal(read(s,id).collection.tabId,2);assert.equal(read(s,id).stopReason,'last-page');
});

test('worker restart in a detail/date step pauses instead of guessing another detail click',async t=>{
  const h=await harness(t),id=await start(h);await h.send({type:'pause-collection',id});const state=await h.state();const j=read(state,id);j.phase='collecting';j.collection.stage='details';state.storage.job=j;
  const restarted=await harness(t,{storage:state.storage});await restarted.advance(1000);const s=await restarted.state();assert.equal(read(s,id).phase,'collection-paused');assert.equal(read(s,id).collection.pause.code,'ADDRESS_COLLECTION_RECOVERED');assert.equal(s.sim.reads.some(x=>x.name==='detailControl' && x.action==='open'),false);assert.equal(s.sim.runs.length,0);
});

test('pausing one collector does not close another task download and rejects another active collector on resume',async t=>{
  const a={id:'download-A',createdAt:'2026-10-26T00:00:00Z',phase:'fetching',items:[],selections:[],requestedKinds:['image'],pages:[]};const h=await harness(t,{storage:{job:a},sim:{owner:a.id}}),b=await start(h);await h.send({type:'pause-collection',id:b});let s=await h.state();assert.equal(read(s,a.id).phase,'fetching');assert.equal(s.sim.owner,a.id);assert.equal(s.sim.cancels.length,0);
  const c=await start(h);const r=await h.send({type:'resume-collection',id:b,tabId:1});assert.equal(r.ok,false);assert.match(r.error,/另一任务/);s=await h.state();assert.equal(read(s,b).phase,'collection-paused');assert.equal(read(s,c).phase,'collecting');
});

test('failed resume state write leaves source task paused and performs no navigation',async t=>{
  const h=await harness(t),id=await start(h);await h.send({type:'pause-collection',id});await h.configure({failResumeSave:true});const r=await h.send({type:'resume-collection',id,tabId:1});assert.equal(r.ok,false);assert.match(r.error,/状态未能保存/);await h.advance(50000);const s=await h.state();assert.equal(read(s,id).phase,'collection-paused');assert.equal(s.sim.nextClicks,0);assert.equal(s.sim.runs.length,0);
});

test('date cutoff remains inclusive after a pause and never requests beyond the boundary',async t=>{
  const h=await harness(t,{sim:{lastPage:5,oldFrom:3}}),id=await start(h);await h.send({type:'pause-collection',id});await h.send({type:'resume-collection',id,tabId:1});await h.advance(20000);const s=await h.state(),j=read(s,id);assert.equal(j.stopReason,'date-cutoff');assert.equal(j.dateCutoff.pageNumber,3);assert.equal(s.sim.nextClicks,2);assert.equal(j.items.length,4);assert.equal(j.selections.filter(r=>r.dateStatus==='included').length,4);
});

test('manual pause interrupts a held detail and resumes without losing its selected card',async t=>{
  const first={pageNumber:1,cardKeys:['detail','image'],cardDates:[0,1].map(()=>({date:'2026-10-24',source:'visible_card_date_range'})),newestFirst:true,cardCount:2,items:[{adId:'image',cardIndex:2,kind:'image',url:'https://cdn.example/image.png'}],missing:[{adId:'detail',cardIndex:1,kind:'video'}]};
  const h=await harness(t,{sim:{detailsReady:false,scans:{1:first}}}),id=await start(h);await h.advance(6000);let s=await h.state();assert.equal(read(s,id).collection.stage,'details');assert.ok(s.sim.dialog);
  const pending=h.send({type:'pause-collection',id});await h.advance(300);assert.equal((await pending).ok,true);s=await h.state();assert.equal(read(s,id).phase,'collection-paused');assert.equal(read(s,id).selectedCardCount,2);assert.equal(s.sim.closeClicks,1);assert.equal(s.sim.dialog,null);assert.equal(s.sim.runs.length,0);
  await h.configure({detailsReady:true});assert.equal((await h.send({type:'resume-collection',id,tabId:1})).ok,true);await h.advance(16000);s=await h.state();assert.equal(read(s,id).stopReason,'last-page');assert.equal(read(s,id).items.length,4);assert.equal(read(s,id).selections.find(i=>i.adId==='detail').url,'https://cdn.example/detail.mp4');
});

test('an address already saved before a held close is not replaced by an incomplete list URL or reopened',async t=>{
  const first={pageNumber:1,cardKeys:['detail'],cardDates:[{date:'2026-10-24',source:'visible_card_date_range'}],newestFirst:true,cardCount:1,items:[{adId:'detail',cardIndex:1,kind:'video',url:'https://www.youtube.com/watch?v='}],missing:[]};
  const h=await harness(t,{sim:{holdClose:true,lastPage:1,scans:{1:first}}}),id=await start(h);await h.advance(6000);let s=await h.state();assert.equal(read(s,id).selections[0].url,'https://cdn.example/detail.mp4');
  const pending=h.send({type:'pause-collection',id});await h.advance(300);assert.equal((await pending).ok,true);assert.equal((await h.send({type:'resume-collection',id,tabId:1})).ok,false);
  await h.configure({dialog:null,holdClose:false});assert.equal((await h.send({type:'resume-collection',id,tabId:1})).ok,true);await h.advance(5000);s=await h.state();assert.equal(read(s,id).items.length,1);assert.equal(read(s,id).items[0].url,'https://cdn.example/detail.mp4');assert.equal(s.sim.reads.filter(r=>r.name==='detailControl' && r.action==='open').length,1);assert.equal(s.sim.closeClicks,1);
});

test('repeated missing read results pause without dropping saved addresses, then allow same-page retry',async t=>{
  const h=await harness(t),id=await start(h);await h.configure({readFailures:5});await h.advance(3000);let s=await h.state();assert.equal(read(s,id).phase,'collection-paused');assert.equal(read(s,id).collectionDiagnostic.readAttempts,5);assert.equal(read(s,id).items.length,2);assert.equal(s.sim.nextClicks,0);
  assert.equal((await h.send({type:'resume-collection',id,tabId:1})).ok,true);await h.advance(12000);s=await h.state();assert.equal(read(s,id).stopReason,'last-page');assert.equal(read(s,id).items.length,4);
});

test('stop remains final while pause preserves a resumable checkpoint',async t=>{
  const h=await harness(t),id=await start(h);await h.send({type:'pause-collection',id});assert.equal((await h.send({type:'stop',id})).ok,true);const s=await h.state();assert.equal(read(s,id).phase,'stopped');assert.equal((await h.send({type:'resume-collection',id,tabId:1})).ok,false);assert.equal(s.sim.runs.length,0);
});

test('stop during resume validation wins without resurrecting the stopped task',async t=>{
  const h=await harness(t),id=await start(h);await h.send({type:'pause-collection',id});await h.configure({holdContext:true});await h.rpc('fire',{message:{type:'resume-collection',id,tabId:1}});await advanceUntil(h,s=>s.sim.contextHeld);
  assert.equal((await h.send({type:'stop',id})).ok,true);await h.configure({holdContext:false});await advanceUntil(h,s=>!!s.sim.fireResult);const s=await h.state();assert.equal(read(s,id).phase,'stopped');assert.equal(s.sim.fireResult.ok,false);assert.equal(s.sim.nextClicks,0);assert.equal(s.sim.runs.length,0);
});

test('filter changes during live resumed collection pause before another page is clicked',async t=>{
  const h=await harness(t),id=await start(h);await h.send({type:'pause-collection',id});await h.send({type:'resume-collection',id,tabId:1});const original=(await h.state()).sim.scope;await h.configure({scope:{...original,inputs:[{label:'App',value:'other'}]}});await h.advance(1000);const s=await h.state();assert.equal(read(s,id).phase,'collection-paused');assert.equal(read(s,id).collection.pause.code,'COLLECTION_CONTEXT_CHANGED');assert.equal(s.sim.nextClicks,0);
});

test('explicit stop followed by address-only retry assigns valid filenames without changing original checkpoint',async t=>{
  const h=await harness(t,{sim:{permissions:true}}),id=await start(h);await h.send({type:'stop',id});assert.equal((await h.send({type:'retry',id})).ok,true);const s=await h.state();assert.equal(read(s,id).phase,'stopped');assert.equal(s.sim.runs.length,1);assert.notEqual(s.sim.runs[0].id,id);assert.ok(s.sim.runs[0].items.every(i=>typeof i.filename==='string' && i.filename.endsWith('.png')));
});

test('resume retains the original total page limit rather than starting a new page budget',async t=>{
  const h=await harness(t,{sim:{lastPage:5}});
  const r=await h.send({type:'start',tabId:1,mode:'pages',untilEnd:false,pageCount:2,kinds:['image'],firstDiscoveredSince:'2026-10-24'});assert.equal(r.ok,true,r.error);const id=r.job.id;
  await advanceUntil(h,s=>read(s,id).collection.stage==='waiting' && s.sim.page===2);await h.send({type:'pause-collection',id});
  assert.equal((await h.send({type:'resume-collection',id,tabId:1})).ok,true);await h.advance(12000);const s=await h.state();assert.equal(read(s,id).stopReason,'page-limit');assert.equal(read(s,id).collection.targetPages,2);assert.deepEqual(read(s,id).collection.completedPages.map(p=>p.pageNumber),[1,2]);assert.equal(s.sim.nextClicks,1);
});

test('ending a paused task without eligible addresses keeps a report and does not create an empty ZIP',async t=>{
  const first={pageNumber:1,cardKeys:['video'],cardDates:[{date:'2026-10-24',source:'visible_card_date_range'}],newestFirst:true,cardCount:1,items:[],missing:[{adId:'video',cardIndex:1,kind:'video'}]};
  const h=await harness(t,{sim:{permissions:true,scans:{1:first}}});const r=await h.send({type:'start',tabId:1,mode:'pages',untilEnd:true,kinds:['image']});assert.equal(r.ok,true,r.error);const id=r.job.id;
  await h.send({type:'pause-collection',id});const ended=await h.send({type:'download-collected',id});assert.equal(ended.ok,true,ended.error);const s=await h.state();assert.equal(read(s,id).phase,'failed');assert.equal(read(s,id).selections.length,1);assert.equal(read(s,id).collectionIncomplete,true);assert.equal(s.sim.runs.length,0);assert.equal(read(s,id).downloadId,undefined);assert.equal((await h.send({type:'resume-collection',id,tabId:1})).ok,false);
});
