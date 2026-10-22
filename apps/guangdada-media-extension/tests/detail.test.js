import test from 'node:test';
import assert from 'node:assert/strict';
import {needsDetail} from '../detail.js';
import {refreshEntries} from '../selection.js';
import {buildReport,reportMarkdown} from '../report.js';

test('only missing videos or incomplete YouTube without a usable backup need detail capture',()=>{
  assert.equal(needsDetail({kind:'video',reasonCode:'NO_MEDIA_URL'}),true);
  assert.equal(needsDetail({kind:'image',reasonCode:'NO_MEDIA_URL'}),false);
  assert.equal(needsDetail({kind:'video',reasonCode:'STREAM_MANIFEST'}),false);
  assert.equal(needsDetail({kind:'video',url:'https://www.youtube.com/watch?v='}),true);
  assert.equal(needsDetail({kind:'video',url:'https://youtu.be/9VpekDCF71c'}),false);
  assert.equal(needsDetail({kind:'video',url:'https://www.youtube.com/watch?v=',candidateUrls:['https://cdn.test/file.mp4']}),false);
});
test('cart refresh preserves a verified detail link when the list still lacks a full URL',()=>{
  const entry={adId:'a',cardIndex:7,pageNumber:1,kind:'video',url:'https://youtu.be/9VpekDCF71c',detailSource:true};
  const scan={cardKeys:['a'],cardCount:1,items:[{adId:'a',cardIndex:1,kind:'video',url:'https://www.youtube.com/watch?v='}],missing:[]};
  assert.deepEqual(refreshEntries([entry],scan),[entry]);
});
test('report distinguishes detail link capture from external media download success',()=>{
  const item={adId:'a',cardIndex:7,pageNumber:1,kind:'video',url:'https://youtu.be/9VpekDCF71c',detailSource:true};
  const md=reportMarkdown(buildReport({phase:'failed',selectedCardCount:1,requestedKinds:['video'],selections:[item],items:[{...item,state:'failed',error:'需要签名',errorCode:'EXTERNAL_SIGNATURE_REQUIRED'}],missing:[]}));
  assert.match(md,/详情读取原始外链 1 项/);assert.match(md,/请求失败 1/);assert.doesNotMatch(md,/9VpekDCF71c|https:/);
});
