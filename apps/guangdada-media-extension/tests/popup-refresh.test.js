import test from 'node:test';
import assert from 'node:assert/strict';
import {Worker} from 'node:worker_threads';
import {within} from '../popup-state.js';
import {sortFixtureScan} from './helpers/sort-fixture.js';

const job=(id,phase)=>({id,taskName:id,createdAt:'2026-10-29T01:00:00Z',zipName:id+'.zip',phase,mode:'current',items:[],selections:[],missing:[],pages:[],requestedKinds:['image'],selectedCardCount:0,cardCount:0});
async function harness(t,sim={}){
  const worker=new Worker(new URL('./helpers/popup-harness.js',import.meta.url),{workerData:{sim}});t.after(()=>worker.terminate());let id=0,ready;
  const pending=new Map(),boot=new Promise((resolve,reject)=>{ready=resolve;worker.on('error',reject);});
  worker.on('message',m=>{if(m.ready){ready();return;}const wait=pending.get(m.id);if(!wait)return;pending.delete(m.id);clearTimeout(wait.timer);m.error?wait.reject(new Error(m.error)):wait.resolve(m.result);});
  worker.on('error',error=>{for(const p of pending.values()){clearTimeout(p.timer);p.reject(error);}pending.clear();});await boot;
  const rpc=(command,data={})=>new Promise((resolve,reject)=>{const key=++id,timer=setTimeout(()=>reject(new Error('Popup harness timeout: '+command)),10000);pending.set(key,{resolve,reject,timer});worker.postMessage({id:key,command,...data});});
  return {rpc,state:()=>rpc('state'),refresh:()=>rpc('refresh'),configure:patch=>rpc('configure',{patch})};
}

test('popup refresh displays the actual collector sort verdict and updates it after a sort change',async t=>{
  const selected=await sortFixtureScan('<label role="radio" aria-checked="true">最新创意</label>');
  const h=await harness(t,{scan:selected});let s=await h.state();assert.match(s.elements['refresh-message'].textContent,/已确认「最新创意」排序/);
  const other=await sortFixtureScan('<select><option selected>展示估值</option><option>最新创意</option></select>');
  await h.configure({scan:other});await h.refresh();s=await h.state();assert.match(s.elements['refresh-message'].textContent,/当前识别到「展示估值」/);assert.doesNotMatch(s.elements['refresh-message'].textContent,/已确认/);
});

test('actual popup recovers a disabled start after late cards load, preserving form values and tasks',async t=>{
  const original=job('原任务','complete'),h=await harness(t,{pageReady:false,jobs:[original]});let s=await h.state();assert.equal(s.elements.start.disabled,true);assert.equal(s.elements['refresh-state'].disabled,false);
  await h.rpc('fields',{values:{'task-name':{value:'下一批'},mode:{value:'pages'},pages:{value:'8'},'date-enabled':{checked:true},'date-since':{value:'2026-10-24'},image:{checked:false}}});
  await h.configure({pageReady:true});await h.refresh();s=await h.state();assert.equal(s.elements.start.disabled,false);assert.equal(s.elements['task-name'].value,'下一批');assert.equal(s.elements.pages.value,'8');assert.equal(s.elements['date-since'].value,'2026-10-24');assert.equal(s.elements['date-enabled'].checked,true);assert.equal(s.elements.image.checked,false);assert.deepEqual(s.sim.jobs,[original]);assert.equal(s.elements['ready-hint'].textContent,'');assert.match(s.elements['refresh-message'].textContent,/1 张卡片/);assert.ok(s.sim.calls.every(c=>['refresh-state','status','get-basket'].includes(c.type)));assert.ok(s.sim.reads.filter(r=>r.name==='collectMedia').every(r=>r.args[0]===false));
});

test('refresh names the actual collector, leaves it running and remains available while collection controls are disabled',async t=>{
  const original=job('仍在采集的任务','collecting'),h=await harness(t,{jobs:[original,job('已完成的任务','complete')]});await h.refresh();const s=await h.state();assert.equal(s.elements.start.disabled,true);assert.equal(s.elements['mode'].disabled,true);assert.equal(s.elements['refresh-state'].disabled,false);assert.match(s.elements['ready-hint'].textContent,/仍在采集的任务/);assert.equal(s.sim.jobs[0].phase,'collecting');
});

test('refresh discards a stale scan after switching to another site, then recovers after returning to the list',async t=>{
  const h=await harness(t);assert.equal((await h.state()).elements.start.disabled,false);await h.configure({url:'https://example.org/'});await h.refresh();let s=await h.state();assert.equal(s.elements.start.disabled,true);assert.match(s.elements['ready-hint'].textContent,/广大大/);
  await h.configure({url:'https://www.guangdada.net/modules/creative/display-ads',blocked:true});await h.refresh();s=await h.state();assert.equal(s.elements.start.disabled,true);assert.match(s.elements['ready-hint'].textContent,/登录/);
  await h.configure({blocked:false});await h.refresh();assert.equal((await h.state()).elements.start.disabled,false);
});

test('worker failures do not fake an unlocked task state and the refresh button permits retry',async t=>{
  const h=await harness(t,{statusError:true});let s=await h.state();assert.equal(s.elements.start.disabled,true);assert.equal(s.elements['refresh-state'].disabled,false);assert.match(s.elements['refresh-message'].textContent,/任务状态读取失败/);
  await h.configure({statusError:false});await h.refresh();s=await h.state();assert.equal(s.elements.start.disabled,false);assert.equal(s.elements['refresh-state'].disabled,false);
});

test('completed download observations refresh without resetting the selected task or page readiness',async t=>{
  const h=await harness(t,{jobs:[job('已保存地址的任务','downloading')]});assert.equal((await h.state()).elements.start.disabled,false);
  await h.configure({jobs:[job('已保存地址的任务','complete')]});await h.refresh();const s=await h.state();assert.match(s.elements['task-list'].children[0].textContent,/已完成/);assert.equal(s.elements['task-list'].value,'已保存地址的任务');assert.equal(s.elements.start.disabled,false);
});

test('a delayed status response from before refresh cannot overwrite the refreshed collector state',async t=>{
  const h=await harness(t,{jobs:[job('旧视图','collecting')]});await h.configure({holdStatus:true});await h.rpc('poll');assert.equal((await h.state()).sim.statusHeld,true);
  await h.configure({jobs:[job('旧视图','complete')]});await h.refresh();assert.equal((await h.state()).elements.start.disabled,false);
  await h.configure({holdStatus:false});const s=await h.state();assert.equal(s.elements.start.disabled,false);assert.match(s.elements['task-list'].children[0].textContent,/已完成/);
});

test('refresh retains a saved cross-page cart even when the current page is temporarily empty',async t=>{
  const cart={active:true,entries:[{adId:'chosen',pageNumber:2,cardIndex:3,kind:'image',url:'https://cdn.example/chosen.png'}]},h=await harness(t,{basket:cart});let s=await h.state();assert.equal(s.elements.mode.value,'basket');
  await h.configure({pageReady:false});await h.refresh();s=await h.state();assert.equal(s.elements.mode.value,'basket');assert.equal(s.elements.start.disabled,false);assert.deepEqual(s.sim.basket,cart);assert.equal(s.sim.calls.some(c=>/clear|stop|start|resume/.test(c.type)),false);
});

test('a partially identified page retains current-page download behavior but blocks unsafe automatic pagination',async t=>{
  const h=await harness(t,{partial:true});assert.equal((await h.state()).elements.start.disabled,false);
  await h.rpc('fields',{values:{mode:{value:'pages'}}});await h.refresh();let s=await h.state();assert.equal(s.elements.start.disabled,true);assert.match(s.elements['ready-hint'].textContent,/完整卡片身份/);
  await h.configure({partial:false});await h.refresh();s=await h.state();assert.equal(s.elements.start.disabled,false);
});

test('an unresponsive observation has a bounded wait and can be retried',async()=>{
  await assert.rejects(within(new Promise(()=>{}),'读取超时',5),/读取超时/);assert.equal(await within(Promise.resolve('ready'),'timeout',5),'ready');
});

test('a filtered list remains selected while loading and missing observations recover through the refresh button',async t=>{
  const filtered='https://www.guangdada.net/modules/creative/display-ads?keyword=zeta&platform=ios&country=US',h=await harness(t,{jobs:[job('上批','complete')]});
  assert.equal((await h.state()).elements.start.disabled,false);await h.configure({url:filtered,loading:true});await h.refresh();let s=await h.state();assert.equal(s.elements.start.disabled,true);assert.match(s.elements['ready-hint'].textContent,/加载/);
  await h.rpc('poll');s=await h.state();assert.match(s.elements['ready-hint'].textContent,/加载/);assert.equal(s.elements['refresh-state'].disabled,false);
  await h.configure({loading:false,missingResults:2});const mark=(await h.state()).sim.reads.length;await h.refresh();s=await h.state();assert.equal(s.elements.start.disabled,false);assert.equal(s.sim.url,filtered);assert.equal(s.sim.reads.slice(mark).filter(r=>r.name==='collectMedia').length,3);assert.equal(s.sim.jobs[0].phase,'complete');
});
