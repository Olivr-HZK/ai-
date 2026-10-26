import test from 'node:test';
import assert from 'node:assert/strict';
import {rememberAddresses,interruptAddresses} from '../addresses.js';
import {buildReport,reportMarkdown} from '../report.js';
const job=()=>({addressFirst:true,addressStage:'collecting',phase:'collecting',selections:[],pages:[],requestedKinds:['video','image']});
const video=(n,extra={})=>({adId:`ad${n}`,cardIndex:n,pageNumber:1,kind:'video',url:`https://cdn.test/${n}.mp4?secret=PRIVATE`,...extra});
test('save all known selected addresses before detail work; repeated snapshots do not erase or duplicate them',()=>{
  const j=job();rememberAddresses(j,[video(1),video(2,{url:undefined,reasonCode:'NO_MEDIA_URL'})],{pageNumber:1,cardCount:60,selectedCardNumbers:[1,2]});
  assert.equal(j.selectedCardCount,2);assert.equal(j.items.length,1);assert.equal(j.cardCount,60);assert.ok(j.selections[0].addressCapturedAt);
  rememberAddresses(j,[video(1,{url:undefined,reasonCode:'ADDRESS_LOGIN_LOST'}),video(2,{detailSource:true})]);
  assert.equal(j.selectedCardCount,2);assert.equal(j.items.length,2);assert.equal(j.missing.length,0);assert.equal(j.selections[1].detailSource,true);
});
test('interruption retains original selection identities, explains missing addresses, and still queues all saved URLs',()=>{
  const j=job();rememberAddresses(j,[video(1),video(2,{url:undefined,reasonCode:'NO_MEDIA_URL'}),video(3,{kind:'playable',url:undefined,reasonCode:'NO_MEDIA_URL'})],{pageNumber:1,cardCount:3});
  interruptAddresses(j,Object.assign(new Error('其他设备登录，强制退出'),{code:'ADDRESS_LOGIN_LOST'}));
  assert.equal(j.items.length,1);assert.equal(j.selectedCardCount,3);assert.equal(j.selections[1].reasonCode,'ADDRESS_LOGIN_LOST');assert.equal(j.selections[2].reasonCode,'NO_MEDIA_URL');
  assert.equal(j.addressStage,'interrupted');assert.equal(j.collectionIncomplete,true);
});
test('address queue preserves cross-page duplicate accounting and type filters',()=>{
  const j=job();rememberAddresses(j,[video(1),video(1,{pageNumber:2}),video(3,{kind:'playable'})],{pageNumber:1,cardCount:3});
  assert.equal(j.selectedCardCount,3);assert.equal(j.items.length,1);assert.equal(j.duplicateCount,1);
});
test('partial ZIP report separately lists missing addresses, preserves the warning, and exports no signed links',()=>{
  const j=job();rememberAddresses(j,[video(1),video(2,{url:undefined,reasonCode:'NO_MEDIA_URL'})],{pageNumber:1,cardCount:2});
  interruptAddresses(j,Object.assign(new Error('登录失效'),{code:'ADDRESS_LOGIN_LOST'}));j.phase='complete';j.items[0].state='complete';j.items[0].filename='媒体/1.mp4';
  const report=buildReport(j),md=reportMarkdown(report);
  assert.equal(report.succeeded,1);assert.equal(report.selectionCounts.missing,1);assert.equal(report.accountingComplete,true);
  assert.match(md,/未取得地址的已选素材（1 项）/);assert.match(md,/登录失效/);assert.match(md,/已保存地址继续下载/);assert.doesNotMatch(md,/未生成媒体 ZIP|PRIVATE|secret=|https:/);
});
