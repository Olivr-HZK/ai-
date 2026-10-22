import test from 'node:test';
import assert from 'node:assert/strict';
import {youtubeUrl,discoveredUrl,extractVideoLinks,fetchExternalVideo,videoExtension,EXTERNAL_PAGE_BYTES} from '../external.js';
import {normalize} from '../media.js';
import {buildReport,reportMarkdown,failureDetail} from '../report.js';
const id='aBcD1234_-Z',base=`https://www.youtube.com/watch?v=${id}`;
const bytes=s=>new TextEncoder().encode(s),video=Uint8Array.from([0,0,0,20,102,116,121,112,105,115,111,109,0,0,0,0]);
const player=p=>`<script>var ytInitialPlayerResponse = ${JSON.stringify({videoDetails:{videoId:id},playabilityStatus:{status:'OK'},...p})};window.MUST_NOT_RUN=true;</script>`;
const format={url:'https://r1.googlevideo.com/videoplayback?token=secret',mimeType:'video/mp4; codecs="avc1.42001E, mp4a.40.2"',height:360};
test('YouTube URL variants preserve the observed full ID, dedupe, reject incomplete IDs and lookalikes',()=>{
  const variants=[base,`https://youtu.be/${id}?t=1`,`https://www.youtube-nocookie.com/embed/${id}`,`https://m.youtube.com/shorts/${id}`];
  for(const url of variants)assert.deepEqual(youtubeUrl(url),{id,url:base});
  assert.equal(normalize(variants.map(url=>({url,kind:'video'}))).length,1);
  for(const url of ['https://www.youtube.com/watch?v=','https://youtu.be/short','https://www.youtube.com/watch?v=abcDEF123456789'])assert.throws(()=>youtubeUrl(url),{code:'EXTERNAL_INCOMPLETE_ID'});
  assert.equal(youtubeUrl('https://youtube.com.evil.test/watch?v='+id),null);
  assert.equal(normalize(['a','b'].map(adId=>({adId,kind:'video',url:'https://www.youtube.com/watch?v='}))).length,2);
  for(const raw of ['javascript:alert(1)','https://user:password@cdn.test/a.mp4','http://127.0.0.1/a.mp4','http://2130706433/a.mp4'])assert.throws(()=>discoveredUrl(raw,'https://video.test/watch'));
});
test('static video tags, relative sources, JSON-LD and video metadata never collect thumbnails or execute scripts',()=>{
  const html=`<video poster="cover.jpg"><source src="/real.mp4?x=1&amp;y=2" type="video/mp4"></video><meta property="og:image" content="cover2.jpg"><meta property="og:video:url" content="//cdn.test/real.webm"><script>const fake='<video src="wrong.mp4"></video>';globalThis.MUST_NOT_RUN=true;</script><script type="application/ld+json">{"@graph":[{"@type":"VideoObject","contentUrl":"/json.mp4"}]}</script><iframe src="https://irrelevant.test/widget"></iframe>`;
  const parsed=extractVideoLinks(html,'https://video.test/watch');
  assert.deepEqual(parsed.candidates.map(c=>c.url),['https://video.test/real.mp4?x=1&y=2','https://cdn.test/real.webm','https://video.test/json.mp4']);
  assert.equal(globalThis.MUST_NOT_RUN,undefined);
  assert.equal(extractVideoLinks('<video src="http://localhost/secret"></video>','https://video.test/watch').errorCode,'EXTERNAL_INVALID_URL');
  assert.equal(extractVideoLinks('<input type="password"><video src="/a.mp4"></video>','https://video.test/login').errorCode,'EXTERNAL_ACCESS_REQUIRED');
});
test('YouTube accepts only matching public combined audio/video formats and diagnoses unsupported responses',()=>{
  assert.equal(extractVideoLinks(player({streamingData:{formats:[format]}}),base).candidates[0].url,format.url);
  for(const [p,code] of [
    [{videoDetails:{videoId:'other123456'}},'EXTERNAL_ID_MISMATCH'],
    [{playabilityStatus:{status:'LOGIN_REQUIRED'}},'EXTERNAL_ACCESS_REQUIRED'],
    [{streamingData:{formats:[{...format,url:null,signatureCipher:'s=secret'}]}},'EXTERNAL_SIGNATURE_REQUIRED'],
    [{streamingData:{adaptiveFormats:[format]}},'EXTERNAL_SEPARATE_STREAMS'],
    [{streamingData:{licenseInfos:[{}],formats:[format]}},'EXTERNAL_DRM'],
    [{streamingData:{formats:[{...format,mimeType:'video/mp4; codecs="avc1.42001E"'}]}},'EXTERNAL_NO_MEDIA'],
    [{streamingData:{formats:[{...format,url:'https://googlevideo.com.evil.test/a'}]}},'EXTERNAL_NO_MEDIA']
  ])assert.equal(extractVideoLinks(player(p),base).errorCode,code);
  assert.throws(()=>extractVideoLinks('x'.repeat(EXTERNAL_PAGE_BYTES+1),base),{code:'EXTERNAL_PAGE_LIMIT'});
});
function environment(responses){
  const requests=[];let reserved=0;
  return {requests,get reserved(){return reserved;},options:{signal:new AbortController().signal,
    request:async url=>{requests.push(url);const r=responses[url];if(r instanceof Error)throw r;if(!r)throw new Error('unexpected URL');return {ok:!(r.status>=400),status:r.status || 200,url,headers:new Headers({'content-type':r.type || 'text/html'}),payload:r.payload};},
    read:async r=>{reserved+=r.payload.length;return r.payload;},discard:n=>{reserved-=n;}}};
}
test('failed original -> static external page -> real bytes; rejected responses release byte reservations',async()=>{
  const env=environment({'https://video.test/old.mp4':{status:403},'https://video.test/watch':{payload:bytes('<video src="/real.webm"></video>')},'https://video.test/real.webm':{payload:video,type:'video/mp4'}});
  const result=await fetchExternalVideo({url:'https://video.test/old.mp4',candidateUrls:['https://video.test/watch']},env.options);
  assert.equal(result.externalInfo.resolved,true);assert.equal(result.externalInfo.attempts[0].code,'HTTP_403');
  assert.equal(result.bytes,video);assert.equal(result.extension,'mp4');assert.equal(env.reserved,video.length);assert.equal(env.requests.length,3);
  assert.equal(videoExtension(bytes('<html>not a video</html>')),null);
});
test('YouTube direct public format can resolve, but cipher/ID errors do not fetch unrelated media',async()=>{
  const env=environment({[base]:{payload:bytes(player({streamingData:{formats:[format]}}))},[format.url]:{payload:video,type:'video/mp4'}});
  const result=await fetchExternalVideo({url:`https://youtu.be/${id}`},env.options);assert.equal(result.externalInfo.resolved,true);assert.equal(env.requests.length,2);
  const invalid=environment({});await assert.rejects(fetchExternalVideo({url:'https://www.youtube.com/watch?v='},invalid.options),e=>e.code==='EXTERNAL_INCOMPLETE_ID' && e.externalInfo.attempts.length===1);assert.equal(invalid.requests.length,0);
  const cipher=environment({[base]:{payload:bytes(player({streamingData:{formats:[{signatureCipher:'s=secret'}]}}))}});
  await assert.rejects(fetchExternalVideo({url:base},cipher.options),e=>e.code==='EXTERNAL_SIGNATURE_REQUIRED' && !e.externalInfo.resolved);assert.equal(cipher.reserved,0);assert.equal(cipher.requests.length,1);
});
test('nested external pages have bounded attempts and cancellation cannot turn into a success',async()=>{
  const env=environment(Object.fromEntries([0,1,2].map(i=>[`https://video.test/p${i}`,{payload:bytes(`<video src="/p${i+1}"></video>`)}])));
  await assert.rejects(fetchExternalVideo({url:'https://video.test/p0'},env.options),{code:'EXTERNAL_DEPTH_LIMIT'});assert.equal(env.requests.length,3);assert.equal(env.reserved,0);
  const controller=new AbortController();controller.abort(new Error('stopped'));await assert.rejects(fetchExternalVideo({url:base},{...env.options,signal:controller.signal}),/stopped/);
});
test('MD report records successful and failed completion attempts without signatures or resolved URLs',()=>{
  const info={attempted:true,resolved:false,attempts:[{method:'YouTube外链',host:'www.youtube.com',code:'EXTERNAL_SIGNATURE_REQUIRED'}]};
  const item={url:base,adId:'ad',kind:'video',cardIndex:1,pageNumber:2,state:'failed',filename:'1.mp4',resolvedUrl:format.url,error:failureDetail('EXTERNAL_SIGNATURE_REQUIRED').reason,errorCode:'EXTERNAL_SIGNATURE_REQUIRED',externalInfo:info};
  const report=buildReport({phase:'complete',autoResolveExternal:true,selectedCardCount:1,selections:[item],items:[item]});
  const md=reportMarkdown(report);assert.match(md,/实际尝试 1 项，补全成功 0 项/);assert.match(md,/播放器签名/);assert.match(md,/www.youtube.com/);assert.doesNotMatch(JSON.stringify(report),/https:|secret|token=/);
  item.state='complete';info.resolved=true;assert.match(reportMarkdown(buildReport({phase:'complete',selections:[item],items:[item]})),/外链自动补全成功/);
});
