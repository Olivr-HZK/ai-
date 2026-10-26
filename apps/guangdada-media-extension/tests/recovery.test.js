import test from 'node:test';
import assert from 'node:assert/strict';
import {recoveryItems,recoveryJob} from '../recovery.js';
import {buildReport,reportMarkdown} from '../report.js';
const source={id:'old',phase:'complete',zipName:'广大大/old.zip',selectedCardCount:60,items:[{adId:'ok',kind:'image',pageNumber:1,cardIndex:1,url:'https://cdn.test/ok.png',state:'complete'},{adId:'fail',kind:'video',pageNumber:3,cardIndex:7,url:'https://cdn.test/video.mp4?secret=1',state:'failed',errorCode:'BATCH_LIMIT'}]};
test('failed-only recovery excludes saved originals, preserves source identities and does not mutate the original task',()=>{
  const next=recoveryJob(source,'new','2026-10-23T00:00:00.000Z');assert.equal(next.items.length,1);assert.equal(next.items[0].adId,'fail');assert.equal(next.selections[0].pageNumber,3);assert.equal(next.selections[0].cardIndex,7);assert.equal(next.selectedCardCount,1);assert.equal(next.recovery.previouslySaved,1);assert.equal(source.items.length,2);next.items[0].state='complete';assert.equal(source.items[1].state,'failed');
  const md=reportMarkdown(buildReport(next));assert.match(md,/原批选择 60 项/);assert.match(md,/原 ZIP 已保存 1 个文件/);assert.doesNotMatch(md,/secret=1|https:/);
});
test('a failed ZIP has delivered no files, so recovery includes its previously fetched files too; running/empty jobs cannot recover',()=>{
  assert.equal(recoveryItems({...source,phase:'failed'}).length,2);assert.equal(recoveryItems({...source,phase:'fetching'}).length,0);assert.throws(()=>recoveryJob({...source,items:[source.items[0]]},'id'),/没有/);
});
