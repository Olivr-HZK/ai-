import test from 'node:test';
import assert from 'node:assert/strict';
import {collectMedia as legacyCollector} from './fixtures/collector-1.15.1.js';
import {sortFixtureScan} from './helpers/sort-fixture.js';
import {requireNewest,datePlan} from '../date-filter.js';

const selected=[
  ['original blue span','<span class="text-blue-600">最新创意</span>'],
  ['original blue button','<button class="text-blue-600"><span>最新创意</span></button>'],
  ['deeper active wrapper','<div class="text-blue-600"><div><span><span>最新创意</span></span></div></div>'],
  ['direct div and new blue utility','<div class="text-blue-500">最新创意</div>'],
  ['ARIA tab','<div role="tab" aria-selected="true"><span>最新创意</span></div>'],
  ['ARIA pressed','<button aria-pressed="true"><span>最新创意</span></button>'],
  ['ARIA radio','<label role="radio" aria-checked="true">最新创意</label>'],
  ['Ant radio','<label class="ant-radio-button-wrapper ant-radio-button-wrapper-checked"><span><input type="radio" checked></span><span>最新创意</span></label>'],
  ['linked native radio','<input id="sort-newest" type="radio" checked><label for="sort-newest"><span>最新创意</span></label>'],
  ['Ant tabs','<div class="ant-tabs-tab ant-tabs-tab-active"><div><span>最新创意</span></div></div>'],
  ['native select','<select><option>展示估值</option><option selected>最新创意</option></select>'],
  ['Ant select current value','<div class="ant-select"><div class="ant-select-selector"><span class="ant-select-selection-item">最新创意</span></div></div>'],
  ['state attribute','<button data-state="active">最新创意</button>'],
  ['generic selected control','<div class="is-selected"><span>最新创意</span></div>'],
  ['normalized spacing and arrow','<button aria-selected="true"> 最 新 创 意 <span>↓</span></button>'],
  ['distinct custom CSS accent','<div><div style="color:rgb(24, 110, 250)"><span>最新创意</span></div><div style="color:rgb(23, 36, 58)">展示估值</div></div>'],
  ['selected filled background','<div><button style="color:rgb(255, 255, 255);background-color:rgb(24, 110, 250)">最新创意</button><button>热门创意</button></div>'],
  ['duplicate text in one control','<button aria-selected="true"><span>最新创意</span><span>最新创意</span></button>'],
  ['unselected duplicate elsewhere','<div><button aria-selected="true">最新创意</button></div><div><span>最新创意</span></div>'],
];
for(const [name,html] of selected)test('serialized collector confirms newest sort: '+name,async()=>{
  const scan=await sortFixtureScan(html);
  assert.equal(scan.newestFirst,true,JSON.stringify(scan.sortState));assert.equal(scan.sortState.status,'confirmed');assert.equal(scan.sortState.selectedLabel,'最新创意');
  assert.equal(scan.items[0].adId,'sort-fixture-ad');assert.equal(scan.items[0].url,'https://cdn.example/sort-fixture-ad.png');assert.equal(scan.cardDates[0].date,'2026-11-02');
  assert.doesNotThrow(()=>requireNewest(scan));assert.equal(datePlan(scan,'2026-11-02',1).included.length,1);
});

const rejected=[
  ['plain label','<span>最新创意</span>'],
  ['other selected sort','<div><button aria-selected="false">最新创意</button><button aria-selected="true">展示估值</button></div>'],
  ['native select different value','<select><option selected>展示估值</option><option>最新创意</option></select>'],
  ['unselected Ant radio','<label class="ant-radio-button-wrapper"><input type="radio"><span>最新创意</span></label>'],
  ['explicit false overrides blue','<button class="text-blue-600" aria-selected="false">最新创意</button>'],
  ['hover class is not selected','<button class="hover:text-blue-600 focus:text-blue-600">最新创意</button>'],
  ['hidden stale control','<div style="display:none"><button class="text-blue-600">最新创意</button></div>'],
  ['hidden ARIA ancestor','<div aria-hidden="true"><span aria-selected="true">最新创意</span></div>'],
  ['hidden/inert ancestor','<div inert><button aria-selected="true">最新创意</button></div>'],
  ['hidden visibility ancestor','<div style="visibility:hidden"><button aria-selected="true">最新创意</button></div>'],
  ['detail text','<div role="dialog"><button class="text-blue-600">最新创意</button></div>'],
  ['card text','<div class="shadow-common-light bg-white"><span class="text-blue-600">最新创意</span></div>'],
  ['multiple active controls','<div><button aria-selected="true">最新创意</button></div><div><button aria-selected="true">最新创意</button></div>'],
  ['contradictory explicit state','<button aria-selected="false"><span aria-pressed="true">最新创意</span></button>'],
  ['two chosen sorts in same group','<div><button aria-selected="true">最新创意</button><button aria-selected="true">展示估值</button></div>'],
  ['blue inherited by every choice','<div style="color:rgb(24, 110, 250)"><button>最新创意</button><button>展示估值</button></div>'],
  ['transparent blue is not selected','<div><button style="background-color:rgba(24, 110, 250, 0)">最新创意</button><button>展示估值</button></div>'],
  ['multiple native selected values','<select multiple><option selected>展示估值</option><option selected>最新创意</option></select>'],
  ['missing control','<p>排序加载中</p>'],
];
for(const [name,html] of rejected)test('serialized collector rejects unconfirmed sort: '+name,async()=>{
  const scan=await sortFixtureScan(html);assert.equal(scan.newestFirst,false,JSON.stringify(scan.sortState));
  assert.throws(()=>requireNewest(scan),error=>error.code==='DATE_SORT_REQUIRED' && error.message.includes('最新创意') && !!error.diagnostic.sortStatus);
});

test('old package reproduces missed selected layouts; production fix does not bypass unselected controls',async()=>{
  const old=legacyCollector.toString();
  for(const name of ['deeper active wrapper','direct div and new blue utility','ARIA radio','Ant radio','native select','Ant select current value','distinct custom CSS accent']){
    const html=selected.find(r=>r[0]===name)[1];assert.equal((await sortFixtureScan(html,{collector:old})).newestFirst,false,name);assert.equal((await sortFixtureScan(html)).newestFirst,true,name);
  }
});

test('sort error distinguishes absent, different, unknown and conflicting states without card data',async()=>{
  for(const [html,status,text] of [['<p>排序</p>','not-found','未识别'],['<span>最新创意</span>','unconfirmed','未确认'],['<select><option selected>展示估值</option><option>最新创意</option></select>','other-sort','展示估值'],['<button aria-selected="true" aria-pressed="false">最新创意</button>','ambiguous','冲突']]){
    const scan=await sortFixtureScan(html);assert.equal(scan.sortState.status,status);assert.throws(()=>requireNewest(scan),e=>e.message.includes(text) && !JSON.stringify(e.diagnostic).includes('cdn.example'));
  }
});
