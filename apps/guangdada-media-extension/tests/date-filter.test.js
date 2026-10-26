import test from 'node:test';
import assert from 'node:assert/strict';
import {calendarDate,dateLimit,datePlan,requireNewest} from '../date-filter.js';
import {rememberAddresses,interruptAddresses} from '../addresses.js';
import {buildReport,reportMarkdown} from '../report.js';
const scan=dates=>({cardKeys:dates.map((_,i)=>`ad${i+1}`),cardDates:dates.map(date=>date?{date,source:'visible_card_date_range'}:null)});
test('date cutoff validates actual calendar dates without timezone conversion',()=>{
  assert.equal(calendarDate('2024-02-29'),'2024-02-29');
  for(const value of ['2026-02-29','2026-04-31','2026-2-01','2026-01-01T00:00:00Z','bad']){assert.equal(calendarDate(value),null);assert.throws(()=>dateLimit(value));}
  assert.equal(dateLimit(''),null);
});
test('cutoff includes the whole lower-bound day and stops at the first older card in original order',()=>{
  const p=datePlan(scan(['2026-10-23','2026-10-22','2026-10-22','2026-10-21','2026-10-23']), '2026-10-22',3);
  assert.deepEqual(p.included,[1,2,3]);assert.equal(p.cutoff.cardIndex,4);assert.equal(p.cutoff.pageNumber,3);
  assert.deepEqual(p.rows.map(r=>r.dateStatus),['included','included','included','excluded','excluded']);
  assert.equal(p.rows[4].dateCode,'DATE_AFTER_CUTOFF');assert.equal(p.pending,null);
});
test('an unknown or invalid date blocks that position and following positions, never invents a cutoff',()=>{
  for(const date of [null,'2026-02-30']){
    const p=datePlan(scan(['2026-10-23',date,'2026-10-21']),'2026-10-22',1);
    assert.deepEqual(p.included,[1]);assert.equal(p.pending.cardIndex,2);assert.equal(p.cutoff,null);assert.equal(p.rows[2].dateStatus,'pending');
  }
});
test('date cutoff requires a confirmed newest-first sort, not a missing or different ordering',()=>{
  assert.doesNotThrow(()=>requireNewest({newestFirst:true}));
  for(const value of [undefined,false,'true'])assert.throws(()=>requireNewest({newestFirst:value}),/最新创意/);
});
test('persistent ledger downloads only date-approved URLs, preserving original page slots and raw evidence',()=>{
  const j={firstDiscoveredSince:'2026-10-22',requestedKinds:['image'],pages:[]};
  const p=datePlan(scan(['2026-10-22',null,'2026-10-21']),'2026-10-22',2);
  const rows=p.rows.map(row=>({...row,kind:'image',url:`https://cdn.test/${row.cardIndex}.png?token=secret`}));
  rememberAddresses(j,rows,{pageNumber:2,cardCount:3});assert.equal(j.items.length,1);assert.equal(j.selections.length,3);
  // A later snapshot can approve a date while retaining the old captured URL.
  rememberAddresses(j,[{...rows[1],url:undefined,dateStatus:'included',firstDiscovered:'2026-10-22'}]);assert.equal(j.items.length,2);assert.ok(j.selections[1].url);
  interruptAddresses(j,new Error('登录失效'));assert.equal(j.items.length,2);
});
test('report explains cutoff and skipped dates separately from media failures with complete accounting',()=>{
  const p=datePlan(scan(['2026-10-22','2026-10-21','2026-10-20']),'2026-10-22',1);
  const j={addressFirst:true,phase:'complete',firstDiscoveredSince:'2026-10-22',dateCutoff:p.cutoff,stopReason:'date-cutoff',requestedKinds:['image'],pages:[]};
  rememberAddresses(j,p.rows.map(row=>({...row,kind:'image',url:`https://cdn.test/${row.cardIndex}.png?private=SECRET`})),{pageNumber:1,cardCount:3});j.items[0].state='complete';
  const r=buildReport(j),md=reportMarkdown(r);
  assert.equal(r.succeeded,1);assert.equal(r.failed,0);assert.equal(r.savedAddressSelections,1);assert.equal(r.selectionCounts.dateFiltered,2);
  assert.equal(Object.values(r.selectionCounts).reduce((a,b)=>a+b,0),3);assert.equal(r.accountingComplete,true);
  assert.match(md,/含当天/);assert.match(md,/触发截止：第 1 页 · 第 2 个/);assert.match(md,/日期未纳入 2 项/);assert.doesNotMatch(md,/https:|private|SECRET/);
});
test('interrupted date verification keeps unknown dates out of missing-media accounting and retry queue',()=>{
  const p=datePlan(scan(['2026-10-23',null]),'2026-10-22',1);
  const j={addressFirst:true,phase:'complete',firstDiscoveredSince:'2026-10-22',requestedKinds:['video'],pages:[]};
  rememberAddresses(j,p.rows.map(row=>({...row,kind:'video',url:`https://cdn.test/${row.cardIndex}.mp4`})),{pageNumber:1,cardCount:2});
  interruptAddresses(j,new Error('登录失效'));j.items[0].state='complete';
  const r=buildReport(j);assert.equal(r.selectionCounts.dateUnchecked,1);assert.equal(r.selectionCounts.missing,0);assert.equal(r.items,undefined);assert.equal(j.items.length,1);
  assert.match(reportMarkdown(r),/日期未核验 1 项/);assert.match(reportMarkdown(r),/未确认达到日期截止/);
});
