import test from 'node:test';
import assert from 'node:assert/strict';
import {createTaskStore,nextQueued,ownsDownload,taskActive,taskName} from '../tasks.js';
const storage=initial=>({data:structuredClone(initial),async get(key){return structuredClone(key===null?this.data:{[key]:this.data[key]});},async set(value){Object.assign(this.data,structuredClone(value));}});
test('legacy ledger migrates without dropping selections, addresses, metadata or failures',async()=>{
  const original={id:'old',createdAt:'2026-10-23',phase:'complete',items:[{url:'https://cdn.test/v.mp4',state:'failed'}],selections:[{adId:'a',metadata:{app:'zeta'}}]};
  const disk=storage({job:original,selectionCarts:{9:{entries:[{adId:'draft'}]}}}),store=createTaskStore(disk);
  assert.deepEqual(await store.get(),original);assert.deepEqual(disk.data['mediaTask:old'],original);assert.equal(disk.data.selectionCarts[9].entries[0].adId,'draft');
});
test('progress for A cannot replace selected B or modify its address ledger',async()=>{
  const disk=storage({}),store=createTaskStore(disk),a={id:'A',phase:'fetching',items:[{state:'pending'}]},b={id:'B',phase:'queued',items:[{url:'https://b.test/b.png',state:'pending'}]};
  await store.focus(a);await store.focus(b);a.items[0].state='complete';await Promise.all([store.save(a),store.save(b)]);
  assert.equal(disk.data.job.id,'B');assert.equal(disk.data['mediaTask:A'].items[0].state,'complete');assert.equal(disk.data['mediaTask:B'].items[0].state,'pending');
  const restarted=createTaskStore(disk);assert.equal((await restarted.get()).id,'B');assert.equal((await restarted.get('A')).items[0].state,'complete');assert.equal((await restarted.all()).length,2);
});
test('queue ignores stopped, collecting and permission-waiting ledgers; external wait owns writer',()=>{
  const jobs=[{id:'C',phase:'stopped',createdAt:'1'},{id:'D',phase:'collecting',createdAt:'1'},{id:'P',phase:'awaiting-permission',createdAt:'1'},{id:'B',phase:'queued',createdAt:'2'},{id:'A',phase:'queued',createdAt:'1'}];
  assert.equal(nextQueued(jobs).id,'A');assert.equal(ownsDownload(jobs[2]),false);assert.equal(ownsDownload({...jobs[2],waitingForExternalPermission:true}),true);assert.equal(taskActive(jobs[3]),true);assert.equal(taskActive(jobs[0]),false);
});
test('task names preserve user labels and are bounded with automatic fallback',()=>{
  assert.equal(taskName('  第一批  ','2026-10-26T01:00:00Z'),'第一批');assert.equal(taskName('x'.repeat(90),'now').length,60);assert.match(taskName('','2026-10-26T01:00:00Z'),/10-26 01:00:00/);
});
test('focusing a stale task reference cannot overwrite a resumed replacement',async()=>{
  const disk=storage({}),store=createTaskStore(disk),old={id:'A',phase:'collection-paused'},resumed={id:'A',phase:'collecting',collection:{resumeCount:1}};
  await store.focus(old);await store.save(resumed);await store.focus(old);assert.equal((await store.get('A')).phase,'collecting');assert.equal(disk.data.job.collection.resumeCount,1);
});
