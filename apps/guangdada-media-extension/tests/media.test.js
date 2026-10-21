import test from "node:test";
import assert from "node:assert/strict";
import {allowedPage, normalize, fileName, parseSelection, pageLimit, mergeCreatives} from "../media.js";
import {cartPages,selectionEntry,refreshEntries} from '../selection.js';

test("only allow the actual site, not lookalike hosts", () => {
  assert.ok(allowedPage("https://www.guangdada.net/modules/creative"));
  for (const url of ["https://guangdada.net.evil.test", "http://guangdada.net", "file:///a", undefined]) assert.equal(allowedPage(url), false);
});
test("selection ranges, dedupe and invalid indices", () => {
  assert.deepEqual(parseSelection("1,3-5，4", 60), [1,3,4,5]);
  assert.deepEqual(parseSelection("", 3), [1,2,3]);
  for (const text of ["0", "61", "4-2", "a", ","]) assert.throws(()=>parseSelection(text, 60));
  assert.ok(fileName({url:"https://cdn.test/a.mp4",kind:"video",cardIndex:7},0,"媒体").includes("/0007_"));
});
test("deduplicate, preserve signed query, and never turn ordinary webp covers into videos", () => {
  const items = normalize([
    {url:"https://cdn.test/cover.webp",kind:"image"},
    {url:"https://cdn.test/movie.webp?sign=a%2Fb",kind:"video"},
    {url:"https://cdn.test/movie.mp4?sign=a%2Fb#one",kind:"video"},
    {url:"javascript:alert(1)",kind:"image"},
    {url:"https://cdn.test/stream.m3u8",kind:"video"},
    {url:"https://user:password@cdn.test/a.jpg",kind:"image"}
  ]);
  assert.deepEqual(items, [
    {url:"https://cdn.test/cover.webp",kind:"image"},
    {url:"https://cdn.test/movie.mp4?sign=a%2Fb",kind:"video"}
  ]);
});
test("safe names never include query credentials or path traversal", () => {
  const name = fileName({url:"https://cdn.test/a%2F..%2Ftest.png?secret=token",kind:"image"}, 0, "广大大/batch");
  assert.equal(name, "广大大/batch/image/0001_a_2F___2Ftest.png");
  assert.ok(!name.includes("token"));
});

test('pagination limits and cross-page names preserve original page and slot', () => {
  assert.equal(pageLimit('3'),3);
  assert.equal(pageLimit('53'),53);
  for(const value of [0,1001,1.5,'abc','']) assert.throws(()=>pageLimit(value));
  assert.equal(fileName({url:'https://cdn.test/a.mp4',kind:'video',cardIndex:3,pageNumber:5},0,'媒体'),'媒体/第0005页/video/0003_a.mp4');
});

test('cart groups original page slots and keeps unavailable selected cards',()=>{
  const scan={cardKeys:['a','b'],cardCount:60,items:[{adId:'a',cardIndex:1,kind:'image',url:'https://cdn.test/a.png'}],missing:[{adId:'b',cardIndex:2,kind:'video',reason:'no URL'}]};
  const a=selectionEntry(scan,2,1),b=selectionEntry(scan,1,2);
  assert.equal(a.adId,'a');assert.equal(b.reason,'no URL');
  assert.deepEqual(cartPages([a,b]).map(p=>[p.pageNumber,p.selectedCardNumbers,p.missingCount]),[[1,[2],1],[2,[1],0]]);
  assert.throws(()=>selectionEntry(scan,1,99));
});

test('refreshing a duplicate on another page updates URL without moving provenance',()=>{
  const original={adId:'a',cardIndex:3,pageNumber:1,sourceCardCount:60,kind:'video',reason:'not ready'};
  const scan={cardKeys:['a'],cardCount:20,items:[{adId:'a',cardIndex:1,kind:'video',url:'https://cdn.test/a.mp4?new'}],missing:[]};
  const [entry]=refreshEntries([original],scan);
  assert.equal(entry.pageNumber,1);assert.equal(entry.cardIndex,3);assert.equal(entry.sourceCardCount,60);
  assert.equal(entry.url,scan.items[0].url);assert.equal(entry.reason,undefined);
});

test('cross-page duplicates retain the first page even if signatures change', () => {
  const items=mergeCreatives([
    {url:'https://cdn.test/a.mp4?sig=first',kind:'video',adId:'a',pageNumber:4},
    {url:'https://cdn.test/a.mp4?sig=second',kind:'video',adId:'a',pageNumber:5},
    {url:'https://cdn.test/a.mp4?sig=first',kind:'video',adId:'other',pageNumber:6},
    {url:'https://cdn.test/b.png',kind:'image',adId:'b',pageNumber:6}
  ]);
  assert.equal(items.length,2);
  assert.equal(items[0].pageNumber,4);
  assert.equal(items[1].pageNumber,6);
});
