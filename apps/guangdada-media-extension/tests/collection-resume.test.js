import test from 'node:test';
import assert from 'node:assert/strict';
import {initializeCollection,markPageComplete,pauseCollectionState,planCollectionResume,activateCollectionResume,canResumeCollection,resumePage,sameCollectionContext,restorePageScan,collectionContext} from '../collection-resume.js';
import {buildReport,reportMarkdown} from '../report.js';
const context={version:1,location:'https://www.guangdada.net/list',inputs:[{label:'日期',value:'2026-10-24'}],selected:[{text:'最新创意'}]};
const scan=(page=1,keys=['a','b'])=>({pageNumber:page,cardKeys:keys,cardDates:keys.map(()=>({date:'2026-10-24',source:'visible_card_date_range'})),newestFirst:true,items:keys.map((adId,i)=>({adId,cardIndex:i+1,kind:'image',url:`https://cdn.test/${adId}.png`})),missing:[],cardCount:keys.length});
function job(){const j={id:'one',mode:'pages',phase:'collecting',addressFirst:true,requestedKinds:['image','video'],firstDiscoveredSince:'2026-10-24',collection:{stage:'warming',seenKeys:[]},selections:[],items:[],pages:[]};initializeCollection(j,context,scan());return j;}
const pause=j=>pauseCollectionState(j,{message:'页面暂未加载',code:'PAGE_LOADING'},'2026-10-26T01:00:00Z');

test('pause retains the ledger, partial page, query and date boundary without starting media',()=>{
  const j=job();j.selections=[{adId:'a',url:'https://cdn.test/a.png'}];const original=structuredClone(j);pause(j);
  assert.equal(j.phase,'collection-paused');assert.equal(canResumeCollection(j),true);assert.equal(resumePage(j),1);assert.deepEqual(j.selections,original.selections);assert.deepEqual(j.collection.context,context);assert.equal(j.addressStage,'paused');assert.equal(j.collectionIncomplete,true);
});
test('same-page continuation restores known detail URLs, dates and metadata and uses a fresh wait budget',()=>{
  const j=job();j.selections=[{adId:'a',pageNumber:1,cardIndex:1,kind:'video',url:'https://youtu.be/9VpekDCF71c',detailSource:true,firstDiscovered:'2026-10-24',dateSource:'visible_detail_date_range',metadata:{adId:'a',fields:{appName:{value:'Zeta',source:'visible_card'}},missingFields:[]}}];pause(j);
  const fresh=scan();fresh.items=fresh.items.filter(x=>x.adId!=='a');fresh.missing=[{adId:'a',cardIndex:1,kind:'video'}];fresh.cardDates[0]=null;
  const plan=planCollectionResume(j,fresh,context);activateCollectionResume(j,plan,fresh,2,9000);
  assert.equal(j.phase,'collecting');assert.equal(j.collection.tabId,2);assert.equal(j.collection.warmSince,9000);assert.equal(j.collection.resumeCount,1);assert.equal(j.collection.snapshot.items.find(i=>i.adId==='a').url,'https://youtu.be/9VpekDCF71c');assert.equal(j.collection.snapshot.cardDates[0].date,'2026-10-24');assert.equal(j.collection.snapshot.items.find(i=>i.adId==='a').metadata.fields.appName.value,'Zeta');assert.equal(j.collection.events.length,2);
});
test('query/filter/sort changes, loading pages and changed card identities block resume',()=>{
  const j=pause(job());
  for(const [s,c] of [[scan(),{...context,inputs:[{value:'another app'}]}],[{...scan(),loading:true},context],[scan(1,['changed','b']),context],[scan(2,['c','d']),context],[{...scan(),newestFirst:false},context]])assert.throws(()=>planCollectionResume(j,s,c),e=>e.code==='COLLECTION_RESUME_MISMATCH');
  assert.equal(j.phase,'collection-paused');
});
test('a clicked/uncertain next-page intent never authorizes a repeated click from the old page',()=>{
  const j=job();markPageComplete(j,scan());j.collection.lastPage={pageNumber:1,cardKeys:['a','b']};j.collection.stage='waiting';j.collection.clickIntent={outcome:'uncertain'};pause(j);
  assert.equal(resumePage(j),2);assert.throws(()=>planCollectionResume(j,scan(),context),/不会重复点击/);
  assert.deepEqual(planCollectionResume(j,scan(2,['c','d']),context),{stage:'warming',pageNumber:2});assert.throws(()=>planCollectionResume(j,scan(3,['e','f']),context),/不能跳过/);
});
test('an explicit pre-click rejection can resume from the checked old page',()=>{
  const j=job();j.collection.lastPage={pageNumber:1,cardKeys:['a','b']};j.collection.stage='next';j.collection.clickIntent={outcome:'not-clicked'};pause(j);assert.deepEqual(planCollectionResume(j,scan(),context),{stage:'next',pageNumber:1});
});
test('an already observed new page must keep its saved identities; repeated complete pages are rejected',()=>{
  const j=job();j.collection.lastPage={pageNumber:1,cardKeys:['a','b']};j.collection.stage='waiting';j.collection.pendingPage={pageNumber:2,cardKeys:['c','d']};pause(j);
  assert.throws(()=>planCollectionResume(j,scan(2,['c','other']),context),/素材身份发生变化/);j.collection.seenKeys=[JSON.stringify(['c','d'])];assert.throws(()=>planCollectionResume(j,scan(2,['c','d']),context),/整页素材已经采集/);
});
test('a serialized restart preserves pause state and ignores object key ordering, not query values',()=>{
  const j=JSON.parse(JSON.stringify(pause(job()))),reordered={selected:context.selected,inputs:context.inputs,location:context.location,version:1};assert.equal(sameCollectionContext(context,reordered),true);assert.equal(canResumeCollection(j),true);assert.doesNotThrow(()=>planCollectionResume(j,scan(),reordered));assert.equal(sameCollectionContext(context,{...reordered,inputs:[]}),false);
});
test('legacy or already downloading/finished tasks cannot pretend to have a resumable checkpoint',()=>{
  assert.equal(canResumeCollection({phase:'collection-paused',mode:'pages',collection:{stage:'waiting'}}),false);
  const j=job();for(const phase of ['complete','fetching','queued','stopped']){j.phase=phase;assert.equal(canResumeCollection(j),false);assert.throws(()=>planCollectionResume(j,scan(),context));}
});
test('restoration cannot borrow an address or date from another ad at the same card index',()=>{
  const j=job();j.selections=[{adId:'other',pageNumber:1,cardIndex:1,url:'https://cdn.test/other.mp4',firstDiscovered:'2026-10-01',dateSource:'visible_detail_date_range'}];const fresh=scan();fresh.items=[];fresh.missing=[{adId:'a',cardIndex:1,kind:'video'}];fresh.cardDates=[null,null];delete j.collection.snapshot;const restored=restorePageScan(j,fresh);assert.equal(restored.items.length,0);assert.equal(restored.cardDates[0],null);
});
test('paused report describes a checkpoint, not an automatically started partial download, and redacts secrets',()=>{
  const j=pause(job());j.collection.pause.reason='https://cdn.test/file?token=SECRET';j.collection.events[0].reason=j.collection.pause.reason;const text=reportMarkdown(buildReport(j));assert.match(text,/采集已暂停/);assert.match(text,/本批尚未启动媒体下载/);assert.match(text,/采集断点与恢复/);assert.doesNotMatch(text,/已保存地址继续下载|SECRET|https:/);
});
test('visible scope reader excludes credentials, cards and pagination inputs',()=>{
  const previous={document:globalThis.document,location:globalThis.location,getComputedStyle:globalThis.getComputedStyle};
  const el=(name,value,type='text',excluded=false)=>({tagName:'INPUT',type,name:'',autocomplete:'',value,checked:false,getClientRects:()=>[{}],closest:()=>excluded?{}:null,getAttribute:k=>k==='placeholder'?name:null});
  globalThis.document={querySelectorAll:s=>s==='input,select,textarea'?[el('关键词','zeta'),el('密码','secret','password'),el('页码','5','text',true),el('卡片文案','unrelated','text',true)]:[]};globalThis.location={href:'https://www.guangdada.net/list?page=5&keyword=zeta&token=SECRET'};globalThis.getComputedStyle=()=>({visibility:'visible'});
  try{const c=collectionContext();assert.equal(c.inputs.length,1);assert.equal(c.inputs[0].value,'zeta');assert.deepEqual(c.query,[['keyword','zeta']]);assert.doesNotMatch(JSON.stringify(c),/SECRET|secret|unrelated/);}finally{Object.assign(globalThis,previous);}
});
