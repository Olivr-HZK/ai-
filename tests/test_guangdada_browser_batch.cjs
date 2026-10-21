// Deterministic mock browser-tool adapters; no real website or driver.
const test=require('node:test');const assert=require('node:assert/strict');const crypto=require('node:crypto');
const {gddCollectVisibleBatch:batch}=require('../skills/guangdada-browser-crawl/scripts/browser_batch.js');
const png=Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wl6hWQAAAAASUVORK5CYII=','base64');
const hash=x=>crypto.createHash('sha256').update(x).digest('hex');
function fixture(n=3){
 const f={files:new Map(),opened:[],closed:0,position:0,detail:false,reads:0};
 f.detailFor=p=>({loading:false,app_id:'app.test',app_name:'Example',platform:'Android',ad_id:p.toString(16).padStart(32,'0'),detail_url:'https://guangdada.net/modules/creative/display-ads/detail?id='+p.toString(16).padStart(32,'0'),raw_text:'2026-10-20 ~ 2026-10-21 展示估值 4663',impressions_estimate:'4663',media_type:p%2?'视频':'图片',observed_videos:[{src:'https://cdn.test/'+p+'.mp4'}],observed_images:[{src:'https://cdn.test/'+p+'.png',is_original:true}],regions:[],language:'英语'});
 f.args={tab:{getScreenshot:async()=>png},scopeSha256:'scope',task:{id:'android-01',app_id:'app.test',app_name:'Example',platform:'Android'},targetDate:'2026-10-20',page:1,batchId:'batch-one',maxCards:10,
 observeList:async()=>({page:1,app_id:'app.test',platform:'Android',card_count:n,signature:'same-actual-list',detail_open:f.detail}),
 cardLocator:async({position})=>({click:async()=>{f.position=position;f.detail=true;f.opened.push(position);f.reads=0;}}),
 closeLocator:async()=>({click:async()=>{f.detail=false;f.closed++;}}),
 readDetail:async()=>{f.reads++;return f.reads===1?{loading:true}:f.detailFor(f.position);},
 persist:async(p,b)=>f.files.set(p,b),sha256:async x=>hash(x)};return f;
}
test('one call collects ordered mixed cards, waits only for required fields, preserves snapshots and identity',async()=>{
 const f=fixture();const r=await batch(f.args);assert.equal(r.status,'yielded');assert.equal(r.reason,'PAGE_END_VERIFY_NEXT');assert.deepEqual(f.opened,[1,2,3]);assert.equal(f.closed,3);assert.equal(r.next_position,4);assert.equal(r.completed.length,3);
 for(const c of r.completed){const card=JSON.parse(f.files.get(c.card_file));assert.equal(hash(f.files.get(c.card_file)),c.sha256);const e=JSON.parse(f.files.get(card.ui_check));assert.equal(e.readiness.observations,2);assert.equal(e.key,'card.identity');assert.equal(card.first_discovered_date,'2026-10-20');assert.equal(hash(f.files.get(e.images.after.path)),e.images.after.sha256);}
 assert.ok(r.operations.every(o=>o.evidence_paths.every(p=>p.startsWith('tasks/android-01/'))));
});
test('first older date stops whole batch; old independent first-seen and relaunch do not stop it',async()=>{
 const f=fixture(5),original=f.detailFor;f.detailFor=p=>({...original(p),first_seen:'2020-01-01',relaunch:true,raw_text:p===3?'重投 2026-10-19~2026-10-20':'重投 2026-10-20~2026-10-21'});
 const r=await batch(f.args);assert.equal(r.status,'cutoff');assert.deepEqual(f.opened,[1,2,3]);assert.equal(r.terminal.date,'2026-10-19');assert.equal(r.completed.length,3);
});
test('captcha and wrong identity stop immediately, no blind retries or subsequent cards',async()=>{
 for(const bad of [{captcha:true},{loading:false,app_id:'other',platform:'Android'}]){
  const f=fixture();f.args.readDetail=async()=>bad;const r=await batch(f.args);assert.equal(r.status,'needs_attention');assert.deepEqual(f.opened,[1]);assert.equal(r.completed.length,0);assert.ok(r.operations[0].error);
 }
});
test('missing fields bounded by observation limit; optional recommendations need not be ready',async()=>{
 const f=fixture();f.args.maxObservations=3;f.args.readDetail=async()=>({...f.detailFor(1),raw_text:'not ready'});let r=await batch(f.args);assert.match(r.error,/visible_date_range/);assert.equal(r.completed.length,0);
 const g=fixture(1);g.args.readDetail=async()=>({...g.detailFor(1),recommendations_loading:true,video_buffered:false});r=await batch(g.args);assert.equal(r.completed.length,1);assert.equal(r.status,'yielded');
});
test('close failure keeps saved card checkpoint and does not reopen or collect next',async()=>{
 const f=fixture();f.args.closeLocator=async()=>({click:async()=>{throw Error('close blocked');}});const r=await batch(f.args);assert.equal(r.reason,'close');assert.equal(r.next_position,2);assert.equal(r.completed.length,1);const cp=JSON.parse(f.files.get('evidence/batches/batch-one-checkpoint.json'));assert.equal(cp.detail_open,true);assert.equal(cp.next_position,2);assert.deepEqual(f.opened,[1]);
});
test('list drift and changed App stop before another click',async()=>{
 const f=fixture();const original=f.args.observeList;f.args.observeList=async()=>({...await original(),signature:f.closed?'different':'same-actual-list'});const r=await batch(f.args);assert.equal(r.status,'needs_attention');assert.deepEqual(f.opened,[1]);assert.equal(r.completed.length,1);
});
test('zero results and partial page do not fabricate app completion or next-page action',async()=>{
 const f=fixture(0);let r=await batch(f.args);assert.equal(r.reason,'ZERO_REQUIRES_EXPLICIT_EVIDENCE');assert.deepEqual(f.opened,[]);
 const g=fixture(3);g.args.maxCards=1;r=await batch(g.args);assert.equal(r.reason,'BATCH_CARD_LIMIT');assert.equal(r.next_position,2);assert.deepEqual(g.opened,[1]);
});
test('reject invalid scope; screenshot/persistence failure cannot become success',async()=>{
 const f=fixture();await assert.rejects(batch({...f.args,maxCards:0}),/Invalid/);await assert.rejects(batch({...f.args,task:{...f.args.task,id:'../bad'}}),/Invalid/);
 const g=fixture();g.args.tab.getScreenshot=async()=>Buffer.from('not image');const r=await batch(g.args);assert.equal(r.completed.length,0);assert.equal(r.status,'needs_attention');assert.deepEqual(g.opened,[]);
 const h=fixture();h.args.persist=async()=>{throw Error('disk unavailable')};await assert.rejects(batch(h.args),/disk unavailable/);
});

test('stale previous-card detail is observed again rather than saved under next position',async()=>{
 const f=fixture(2);f.args.readDetail=async()=>{f.reads++;return f.detailFor(f.position===2 && f.reads===1?1:f.position);};
 const r=await batch(f.args);assert.equal(r.completed.length,2);assert.notEqual(r.completed[0].ad_id,r.completed[1].ad_id);
 const card=JSON.parse(f.files.get('cards/0001-0002.json'));assert.equal(JSON.parse(f.files.get(card.ui_check)).readiness.observations,2);
});
test('batch budget yields before opening another card without artificial sleeps',async()=>{
 const f=fixture(3),realNow=Date.now;let clock=realNow();Date.now=()=>clock;
 try{const original=f.args.closeLocator;f.args.closeLocator=async()=>{const locator=await original();return {click:async()=>{await locator.click();clock+=46000;}};};
 const r=await batch(f.args);assert.equal(r.reason,'BATCH_TIME_BUDGET');assert.deepEqual(f.opened,[1]);assert.equal(r.next_position,2);
 }finally{Date.now=realNow;}
});
test('old-date boundary does not wait for unused estimate or video loading',async()=>{
 const f=fixture();f.args.readDetail=async()=>({...f.detailFor(1),raw_text:'2026-10-19~2026-10-20',impressions_estimate:undefined,observed_videos:[]});
 const r=await batch(f.args);assert.equal(r.status,'cutoff');assert.equal(r.completed.length,1);assert.deepEqual(f.opened,[1]);
});
