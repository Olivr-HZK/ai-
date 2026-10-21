import test from 'node:test';
import assert from 'node:assert/strict';
import {buildReport,reportMarkdown,failureDetail,selectionRows} from '../report.js';

const item=(cardIndex,extra={})=>({pageNumber:2,cardIndex,adId:`ad-${cardIndex}`,url:`https://cdn.test/${cardIndex}.mp4?token=secret`,kind:'video',...extra});
test('report accounts for every selection including duplicate failures and unselected types',()=>{
  const first=item(1),failed=item(3),duplicate=item(2,{url:first.url}),missing=item(4,{url:undefined,reasonCode:'NO_MEDIA_URL',reason:'视频卡片未提供地址'}),unsupported=item(5,{url:undefined,kind:undefined,reasonCode:'UNSUPPORTED_TYPE',reason:'试玩广告'}),filtered=item(6,{kind:'image'});
  const job={phase:'packing',requestedKinds:['video'],selectedCardCount:6,selections:[first,duplicate,failed,missing,unsupported,filtered],items:[{...first,filename:'1.mp4',state:'complete',bytes:42},{...failed,filename:'3.mp4',state:'failed',error:'HTTP 403',errorCode:'HTTP_403'}],missing:[missing,unsupported]};
  const report=buildReport(job);
  assert.equal(report.accountingComplete,true);
  assert.equal(report.succeeded,1);assert.equal(report.failed,1);
  assert.deepEqual(report.selections.map(i=>i.state),['complete','duplicate','failed','missing','unsupported','filtered']);
  assert.equal(Object.values(report.selectionCounts).reduce((a,b)=>a+b),6);
  const markdown=reportMarkdown(report);
  assert.match(markdown,/全部选择（6 项）/);assert.match(markdown,/HTTP 403/);assert.match(markdown,/第 2 页 · 第 2 个/);
  assert.doesNotMatch(JSON.stringify(report),/https:|token=|secret/);
  assert.doesNotMatch(markdown,/https:|token=|secret/);
  job.items[0].state='failed';job.items[0].error='HTTP 404';
  assert.match(buildReport(job).selections[1].reason,/对应文件下载失败/);
});
test('HTTP and local failures explain known evidence without asserting speculative causes',()=>{
  for(const status of [401,403,404,408,410,429,500,502]){
    const detail=failureDetail('HTTP',status);assert.equal(detail.code,`HTTP_${status}`);assert.match(detail.reason,new RegExp(`HTTP ${status}`));assert.ok(detail.suggestion);
  }
  assert.match(failureDetail('HTTP',403).reason,/无法仅凭状态码确定/);
  for(const code of ['TIMEOUT','BATCH_LIMIT','EMPTY_RESPONSE','NOT_MEDIA','NETWORK','CANCELLED'])assert.equal(failureDetail(code).code,code);
  assert.match(failureDetail('NETWORK').reason,/无法确定具体原因/);
});
test('stopped and incomplete batches preserve selections without claiming downloads failed on the server',()=>{
  const pending=item(1),active=item(2);
  const job={phase:'stopped',collectionIncomplete:true,selectedCardCount:2,selections:[pending,active],items:[{...pending,state:'pending'},{...active,state:'failed',errorCode:'CANCELLED',error:'用户停止'}]};
  let report=buildReport(job);assert.deepEqual(report.selections.map(i=>i.state),['cancelled','cancelled']);assert.equal(report.failed,0);
  assert.match(reportMarkdown(report),/翻页采集未完成/);
  job.phase='failed';report=buildReport(job);assert.equal(report.selections[0].state,'unattempted');
});
test('selection ledger retains unavailable cards and Markdown cannot inject HTML or table columns',()=>{
  const rows=selectionRows({cardKeys:['a','b','c'],items:[{cardIndex:1,kind:'video',url:'https://cdn.test/a.mp4'}],missing:[{cardIndex:2,kind:'video',reason:'missing'},{cardIndex:3,reason:'<script>bad</script> | `x`\nmore'}]},7,[1,3]);
  assert.deepEqual(rows.map(i=>[i.pageNumber,i.cardIndex,i.adId]),[[7,1,'a'],[7,3,'c']]);
  const report=buildReport({phase:'failed',selections:rows,selectedCardCount:2,items:[],missing:[],error:'oops https://cdn.test/a?token=secret'});
  const markdown=reportMarkdown(report);assert.doesNotMatch(markdown,/<script>|token=|secret/);assert.match(markdown,/&#124;/);assert.match(markdown,/&lt;script&gt;/);
});
