// Mock browser-tool contract; this never opens or drives a real browser.
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const {gddCheckedClick} = require('../skills/guangdada-browser-crawl/scripts/browser_check.js');
(async () => {
  const files = new Map();
  let value = false, clicks = 0;
  const args = {tab:{getScreenshot:async()=>Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wl6hWQAAAAASUVORK5CYII=','base64'),playwright:{domSnapshot:async()=>String(value)}},scopeSha256:'hash',taskId:'test-ios',eventId:'first',key:'filter.category',expected:true,click:async()=>{clicks++;value=true},read:async()=>value,persist:async(p,b)=>files.set(p,b),sha256:async b=>crypto.createHash('sha256').update(b).digest('hex')};
  let result=await gddCheckedClick(args);
  assert.equal(clicks,1);assert.equal(result.event.status,'passed');
  assert.equal(result.event.before,false);assert.equal(result.event.after,true);
  assert.ok(files.has('evidence/clicks/first.before.png'));assert.ok(files.has('evidence/clicks/first.after.png'));
  value=false;
  await assert.rejects(gddCheckedClick({...args,eventId:'mismatch',click:async()=>{clicks++}}),/Click check failed/);
  assert.equal(JSON.parse(files.get('evidence/clicks/mismatch.json')).status,'mismatch');
  await assert.rejects(gddCheckedClick({...args,eventId:'failed',click:async()=>{throw new Error('click blocked')}}),/Click check failed/);
  assert.equal(JSON.parse(files.get('evidence/clicks/failed.json')).status,'failed');
  await assert.rejects(gddCheckedClick({...args,eventId:'no-save',persist:async()=>{throw new Error('disk full')}}),/disk full/);
  value=['韩国','日本'];
  result=await gddCheckedClick({...args,eventId:'set-order',key:'filter.regions',expected:['日本','韩国'],click:async()=>{clicks++}});
  assert.equal(result.event.status,'passed');assert.deepEqual(result.event.after,['韩国','日本']);
  const jpeg=Buffer.from([255,216,255,224,0,2,255,217]);
  result=await gddCheckedClick({...args,eventId:'jpeg',expected:value,click:async()=>{clicks++},tab:{...args.tab,getScreenshot:async()=>jpeg}});
  assert.equal(result.event.images.after.format,'jpg');assert.ok(files.has('evidence/clicks/jpeg.after.jpg'));
  const {gddReadOnlyCheck}=require('../skills/guangdada-browser-crawl/scripts/browser_check.js');
  const observation={...args,click:undefined};const clicksBefore=clicks;
  for (const [key,wanted] of [['filter.category','工具'],['filter.date',{mode:'preset',preset:'30天'}],['filter.channels',['Facebook']]]) {
    value=wanted;result=await gddReadOnlyCheck({...observation,key,expected:wanted,eventId:'read-'+key.split('.')[1]});
    assert.equal(result.event.kind,'read');assert.equal(result.event.status,'passed');
  }
  assert.equal(clicks,clicksBefore);
  value='教育';await assert.rejects(gddReadOnlyCheck({...observation,key:'filter.category',expected:'工具',eventId:'wrong-category'}),/Evidence check failed/);
  await assert.rejects(gddReadOnlyCheck({...observation,key:'filter.sort',expected:value,eventId:'readonly-sort'}),/require clicks/);
  await assert.rejects(gddReadOnlyCheck({...observation,key:'app',expected:value,eventId:'readonly-app'}),/require clicks/);
  console.log('Checked-click and read-only base-filter cases passed');
})().catch(e=>{console.error(e);process.exitCode=1});
