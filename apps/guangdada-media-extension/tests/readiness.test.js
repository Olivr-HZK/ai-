import test from 'node:test';
import assert from 'node:assert/strict';
import {retryObservation,waitForDetailClose,outsideWarmBudget} from '../readiness.js';
import {buildReport,reportMarkdown} from '../report.js';

test('a slow matching close is observed past two seconds without a second click',async()=>{
  let time=0,clicks=0;const r=await waitForDetailClose({adId:'a',close:async()=>{clicks++;return {closed:true};},observe:async()=>({visible:time<3500,adId:time<3500?'a':null}),now:()=>time,sleep:async n=>{time+=n;}});
  assert.equal(clicks,1);assert.equal(r.closed,true);assert.equal(r.waitedMs,3500);
});
test('unknown close result is checked by observations without re-clicking',async()=>{
  let clicks=0;const r=await waitForDetailClose({adId:'a',close:async()=>{clicks++;throw Object.assign(new Error('no result'),{code:'PAGE_READ_UNAVAILABLE',diagnostic:{stage:'result'}});},observe:async()=>({visible:false})});
  assert.equal(clicks,1);assert.equal(r.closed,true);
});
test('replacement dialog and permanent close failures stop with identity evidence',async()=>{
  let time=0,clicks=0;
  await assert.rejects(waitForDetailClose({adId:'a',close:async()=>{clicks++;return {};},observe:async()=>({visible:true,adId:'foreign'}),now:()=>time}),e=>e.code==='DETAIL_ID_MISMATCH' && e.diagnostic.actualAdId==='foreign');
  await assert.rejects(waitForDetailClose({adId:'a',close:async()=>({}),observe:async()=>({visible:true,adId:'a'}),now:()=>time,sleep:async n=>{time+=n;}}),e=>e.code==='DETAIL_CLOSE_TIMEOUT' && e.diagnostic.waitedMs===15000);
  assert.equal(clicks,1);
});
test('explicit stop does not wait fifteen seconds for a slow close',async()=>{
  const r=await waitForDetailClose({adId:'a',close:async()=>({}),observe:async()=>({visible:true,adId:'a'}),stopping:()=>true});assert.equal(r.cancelled,true);
});
test('only missing read results are retried with a bounded attempt count',async()=>{
  let count=0;assert.equal(await retryObservation(async()=>{if(++count<3)throw Object.assign(new Error('not ready'),{code:'PAGE_READ_UNAVAILABLE'});return 'observed';},{sleep:async()=>{}}),'observed');assert.equal(count,3);
  count=0;await assert.rejects(retryObservation(async()=>{count++;throw Object.assign(new Error('no result'),{code:'PAGE_READ_UNAVAILABLE'});},{sleep:async()=>{}}),e=>e.readAttempts===5);assert.equal(count,5);
  count=0;await assert.rejects(retryObservation(async()=>{count++;throw Object.assign(new Error('login lost'),{code:'ADDRESS_LOGIN_LOST'});}));assert.equal(count,1);
});
test('detail/date work is excluded from the warm-up budget, including failed lookups',async()=>{
  const c={warmSince:1000,mediaSince:2000};let now=3000;
  await outsideWarmBudget(c,async()=>{now+=65000;},()=>now);assert.equal(c.warmSince,66000);assert.equal(c.mediaSince,67000);assert.equal(c.lookupElapsedMs,65000);
  await assert.rejects(outsideWarmBudget(c,async()=>{now+=5000;throw new Error('failure');},()=>now));assert.equal(c.lookupElapsedMs,70000);
});
test('collection diagnostics have action/identity evidence without signed URLs',()=>{
  const text=reportMarkdown(buildReport({phase:'failed',addressFirst:true,requestedKinds:[],items:[],selections:[],collectionDiagnostic:{code:'DETAIL_CLOSE_TIMEOUT',action:'detail-close',pageNumber:5,expectedAdId:'a',actualAdId:'https://cdn.test/?token=SECRET',detailVisible:true,waitedMs:15000}}));
  assert.match(text,/DETAIL_CLOSE_TIMEOUT/);assert.match(text,/第 5 页/);assert.match(text,/15 秒/);assert.doesNotMatch(text,/SECRET|https:/);
});
