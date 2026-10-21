import test from 'node:test';
import assert from 'node:assert/strict';
import {normalize,fileName} from '../media.js';
import {inspectPlayable} from '../playable.js';
import {buildReport,reportMarkdown} from '../report.js';
const bytes=text=>new TextEncoder().encode(text);
test('Playable retains its own kind, original URL and HTML extension instead of becoming an image or video',()=>{
  for(const url of ['https://cdn.test/game.html?signature=secret','https://cdn.test/game.htm','https://cdn.test/entry']){
    const [item]=normalize([{url,kind:'playable',pageNumber:4,cardIndex:2}]);
    assert.equal(item.kind,'playable');assert.equal(item.url,url);
    assert.match(fileName(item,0,'媒体'),/^媒体\/第0004页\/playable\/0002_.*\.html?$/);
    assert.doesNotMatch(fileName(item,0,'媒体'),/signature|secret|jpg|mp4/);
  }
});
test('Playable inspection never runs script, records external dependencies without exporting URLs, rejects error/login content',()=>{
  const html='<html><body><canvas></canvas><script>throw new Error("MUST_NOT_EXECUTE")</script><script src="/game.js?secret=1"></script><img src="data:image/png;base64,AA"><img src="//other.test/a.png"></body></html>';
  const result=inspectPlayable(bytes(html),'https://cdn.test/game.html');
  assert.equal(result.externalReferences,2);assert.match(result.note,/未验证离线可玩性/);
  assert.doesNotMatch(JSON.stringify(result),/https:|secret|other.test/);
  for(const text of ['{"error":"login"}','<html><body>404 not found</body></html>','PNG bytes'])assert.equal(inspectPlayable(bytes(text),'https://cdn.test/game.html').errorCode,'PLAYABLE_NOT_HTML');
  const login='<html><body><input type="password"><script></script></body></html>';
  assert.equal(inspectPlayable(bytes(login),'https://cdn.test/game.html').errorCode,'PLAYABLE_LOGIN_PAGE');
  assert.equal(inspectPlayable(bytes('<html><script></script></html>'),'https://cdn.test/login').errorCode,'PLAYABLE_LOGIN_PAGE');
});
test('Playable reporting includes original location, missing HTML, filtered selection and online dependency limits',()=>{
  const item={kind:'playable',adId:'p',pageNumber:3,cardIndex:1,url:'https://cdn.test/a.html?secret=1',state:'complete',filename:'媒体/第0003页/playable/0001_a.html',playableInfo:inspectPlayable(bytes('<html><script src="a.js"></script></html>'),'https://cdn.test/a.html')};
  const missing={kind:'playable',adId:'m',pageNumber:3,cardIndex:2,reason:'Playable 卡片未提供原 HTML 地址',reasonCode:'NO_MEDIA_URL'};
  const report=buildReport({phase:'complete',requestedKinds:['playable'],selectedCardCount:2,selections:[item,missing],items:[item],missing:[missing]});
  const md=reportMarkdown(report);assert.match(md,/Playable 试玩/);assert.match(md,/第 3 页 · 第 1 个/);assert.match(md,/1 处外部资源引用/);assert.match(md,/未提供原 HTML/);assert.doesNotMatch(md,/secret=1|https:/);
  assert.equal(buildReport({phase:'complete',requestedKinds:['image'],selections:[item],items:[]}).selections[0].state,'filtered');
});
