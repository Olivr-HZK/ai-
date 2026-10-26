import test from 'node:test';
import assert from 'node:assert/strict';
import {canWaitForPage,pageMismatch,pageWaitMessage} from '../page-stability.js';
import {buildReport,reportMarkdown} from '../report.js';
test('retry only explicit page-identity/loading checks, never unknown click outcomes or distant page jumps',()=>{
  for(const code of ['PAGE_CARDS_CHANGED','PAGE_NUMBER_CHANGED','PAGE_LOADING'])assert.ok(canWaitForPage({code,diagnostic:{expectedPage:6,actualPage:6}}));
  assert.ok(canWaitForPage({code:'PAGE_NUMBER_CHANGED',diagnostic:{expectedPage:6,actualPage:7}}));
  for(const error of [{code:'NETWORK'},{code:'ADDRESS_LOGIN_LOST'},{code:'PAGE_CARDS_CHANGED'},{code:'PAGE_NUMBER_CHANGED',diagnostic:{expectedPage:6,actualPage:19}}])assert.equal(canWaitForPage(error),false);
});
test('a temporary missing or changed card records its exact old/new identity without guessing a new page',()=>{
  const error=pageMismatch({pageNumber:6,cardKeys:['a','b']},{pageNumber:6,cardKeys:['a']});
  assert.equal(error.notClicked,true);assert.equal(error.diagnostic.firstDifference,2);assert.equal(error.diagnostic.expectedAdId,'b');assert.equal(error.diagnostic.actualAdId,null);
  assert.match(pageWaitMessage(error.diagnostic),/期望第 6 页，实际第 6 页/);assert.match(pageWaitMessage(error.diagnostic),/本阶段未继续点击或翻页/);
});
test('report makes interrupted collection distinct from a date boundary and preserves safe mismatch diagnostics',()=>{
  const j={mode:'pages',phase:'failed',addressFirst:true,collectionIncomplete:true,firstDiscoveredSince:'2026-10-22',selections:[],selectedCardCount:0,items:[],pageDiagnostic:{code:'PAGE_CARDS_CHANGED',stage:'warming',action:'read',expectedPage:6,actualPage:6,expectedCards:60,actualCards:59,firstDifference:12,expectedAdId:'a',actualAdId:'https://cdn.test/token?secret=SECRET',waitedMs:15300,recovered:false}};
  const text=reportMarkdown(buildReport(j));assert.match(text,/采集结束原因：提前中断/);assert.match(text,/未确认达到日期截止/);assert.match(text,/期望第 6 页 \/ 60 张/);assert.match(text,/第 12 个/);assert.match(text,/15.3 秒/);assert.doesNotMatch(text,/secret|SECRET|https:/);
  j.collectionIncomplete=false;j.phase='complete';j.stopReason='date-cutoff';j.dateCutoff={pageNumber:19,cardIndex:1,adId:'old',firstDiscovered:'2026-10-21'};assert.match(reportMarkdown(buildReport(j)),/采集结束原因：达到首次发现日期截止/);
});
