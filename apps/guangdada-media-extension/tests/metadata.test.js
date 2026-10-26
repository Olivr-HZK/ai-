import test from 'node:test';
import assert from 'node:assert/strict';
import {parseMetadata,mergeMetadata,exportMetadata} from '../metadata.js';
import {rememberAddresses,interruptAddresses} from '../addresses.js';
import {refreshEntries} from '../selection.js';
import {recoveryJob} from '../recovery.js';
import {buildReport,reportMarkdown} from '../report.js';
const evidence=(extra={})=>({adId:'ad1',source:'visible_card',text:'Ad copy\n展示估值 4.7K',labels:['重投'],links:[{href:'https://play.google.com/store/apps/details?id=ai.test.app&token=SECRET',text:'Example | <App>'}],...extra});
const metadata=(extra={})=>parseMetadata(evidence(extra));
const row=(extra={})=>({adId:'ad1',cardIndex:1,pageNumber:1,kind:'video',url:'https://cdn.test/a.mp4?token=PRIVATE',metadata:metadata(),...extra});
test('explicit store identity and badge preserve values, source and rounded estimate precision',()=>{
 const m=metadata();assert.equal(m.fields.appName.value,'Example | <App>');assert.equal(m.fields.appId.value,'ai.test.app');assert.equal(m.fields.platform.value,'Android');assert.equal(m.fields.relaunch.value,true);assert.equal(m.fields.displayEstimate.value,4700);assert.equal(m.fields.displayEstimate.raw,'4.7K');assert.equal(m.fields.displayEstimate.precision,'rounded');
 assert.doesNotMatch(JSON.stringify(m),/SECRET|https:/);
});
test('absence is unknown; ad copy, search text and misleading links cannot establish identity or relaunch',()=>{
 const m=metadata({text:'Fake App first line\n不含重投\n重投次数 0\n展示估值 --',labels:['重投次数 0'],links:[{href:'https://play.google.com.evil.test/store/apps/details?id=x',text:'Fake App'},{href:'https://www.guangdada.net/search?store=fake&os=2',text:'Fake App'}]});
 for(const key of ['appName','appId','platform','relaunch','displayEstimate'])assert.equal(m.fields[key],null,key);
 const zero=metadata({text:'展示估值 0\n重投标签：否',labels:[]});assert.equal(zero.fields.displayEstimate.value,0);assert.equal(zero.fields.relaunch.value,false);
});
test('iOS and advertiser store identity, actual detail name, unit conversions and malformed estimates',()=>{
 const m=metadata({source:'visible_detail',text:'受众画像\nActual Name\n当页查看创意\n展示估值\n1.2万',links:[{href:'https://apps.apple.com/us/app/name/id123456',text:'App Store'}]});assert.equal(m.fields.appName.value,'Actual Name');assert.equal(m.fields.appId.value,'123456');assert.equal(m.fields.platform.value,'iOS');assert.equal(m.fields.displayEstimate.value,12000);
 for(const [raw,value] of [['1,234',1234],['2M',2000000],['0.5亿',50000000]])assert.equal(metadata({text:'展示估值 '+raw}).fields.displayEstimate.value,value);
 for(const raw of ['<1K','1K~2K','12,34','-1','NaN','2.7B'])assert.equal(metadata({text:'展示估值 '+raw}).fields.displayEstimate,null,raw);
 const adv=metadata({links:[{href:'https://www.guangdada.net/modules/advertiser/detail?store=ai.test.app&os=2',text:'Actual App'}]});assert.equal(adv.fields.appId.value,'ai.test.app');
});
test('multiple identities, conflicting badges and estimates are explicitly unknown',()=>{
 const m=metadata({text:'展示估值 10\n展示估值 20',labels:['重投','非重投'],links:[...evidence().links,{href:'https://play.google.com/store/apps/details?id=other.app',text:'Other'}]});assert.equal(m.fields.appId,null);assert.equal(m.fields.relaunch,null);assert.equal(m.fields.displayEstimate,null);assert.equal(m.warnings.length,3);
});
test('partial refresh cannot erase detail values; exact detail estimate wins over rounded list',()=>{
 const exact=metadata({source:'visible_detail',text:'展示估值 4663'});exact.detailChecked=true;
 let m=mergeMetadata(metadata(),exact);assert.equal(m.fields.displayEstimate.value,4663);
 m=mergeMetadata(m,metadata({text:'展示估值 4.7K',links:[],labels:[]}));assert.equal(m.fields.displayEstimate.value,4663);assert.equal(m.fields.appId.value,'ai.test.app');assert.equal(m.detailChecked,true);
 const wrong=metadata({adId:'different',links:[]});assert.equal(mergeMetadata(m,wrong),m);
 const conflict=mergeMetadata(m,metadata({links:[{href:'https://play.google.com/store/apps/details?id=different.app',text:'Other'}]}));assert.equal(conflict.fields.appId,null);assert.equal(conflict.fields.appName,null);assert.ok(conflict.conflicts.includes('appId'));assert.equal(mergeMetadata(conflict,metadata()).fields.appId,null);
});
test('cross-page refresh, durable ledger, interruption and failed-only retry preserve metadata',()=>{
 const entry=row(),scan={cardKeys:['ad1'],cardCount:1,items:[row({metadata:metadata({text:'',links:[],labels:[]})})],missing:[]};
 const updated=refreshEntries([entry],scan)[0];assert.equal(updated.metadata.fields.appId.value,'ai.test.app');
 const job={phase:'collecting',readMetadataDetails:true,selections:[],pages:[],requestedKinds:['video']};rememberAddresses(job,[updated]);rememberAddresses(job,[{...updated,metadata:metadata({text:'',links:[],labels:[]}),url:undefined}]);
 interruptAddresses(job,Object.assign(new Error('登录失效'),{code:'ADDRESS_LOGIN_LOST'}));assert.equal(job.items[0].metadata.fields.appId.value,'ai.test.app');
 job.phase='complete';job.items[0].state='failed';const recovered=recoveryJob(job,'new');assert.deepEqual(recovered.items[0].metadata,job.items[0].metadata);
});
test('MD metadata accounts for successes, missing, duplicates, filtered and date skipped, without signatures',()=>{
 const entries=[row(),row({cardIndex:2,metadata:metadata({text:'展示估值 0',labels:[]}),url:undefined}),row({cardIndex:3}),row({cardIndex:4,kind:'playable'}),row({cardIndex:5,dateStatus:'excluded'})];
 const job={phase:'complete',readMetadataDetails:true,firstDiscoveredSince:'2026-10-22',requestedKinds:['video'],selectedCardCount:5,selections:entries.map(e=>({...e,dateStatus:e.dateStatus || 'included'})),items:[{...entries[0],state:'complete',filename:'媒体/第0001页/video/0001_a.mp4'}]};
 const report=buildReport(job),md=reportMarkdown(report);assert.equal(report.selections.length,5);assert.match(md,/素材信息（5 项）/);assert.match(md,/Example &#124; &lt;App&gt;/);assert.match(md,/4.7K/);assert.match(md,/4700（页面缩写换算，近似）/);assert.match(md,/重投标签/);assert.match(md,/未知/);assert.match(md,/0001_a.mp4/);assert.doesNotMatch(md,/PRIVATE|SECRET|token=|https:/);
 assert.equal(report.selections[1].metadata.fields.displayEstimate.value,0);
 assert.equal(exportMetadata({...metadata(),extra:'SECRET'},'ad1').extra,undefined);assert.equal(exportMetadata(metadata(),'wrong'),null);
 const legacy=reportMarkdown(buildReport({phase:'failed',selections:[row({metadata:undefined})],items:[],requestedKinds:['video']}));assert.match(legacy,/旧任务或卡片未读取素材信息/);
});
