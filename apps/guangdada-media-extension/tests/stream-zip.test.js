import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,open,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {execFileSync} from 'node:child_process';
import {StreamZip,localHeader,centralHeader,endRecords} from '../stream-zip.js';
import {crc32,crc32Accumulator} from '../zip.js';
import {diskError} from '../disk-store.js';
const payload=bytes=>({length:bytes.length,crc:crc32(bytes),stream:()=>new Blob([bytes]).stream()});
const str=text=>new TextEncoder().encode(text);
test('incremental CRC matches original bytes across arbitrary chunk boundaries',()=>{
 const bytes=str('图片/视频🙂\0'.repeat(200)),c=crc32Accumulator();for(let i=0;i<bytes.length;i+=7)c.update(bytes.subarray(i,i+7));assert.equal(c.digest(),crc32(bytes));
});
test('streamed UTF-8 ZIP preserves exact original bytes and is readable by an independent ZIP reader',async()=>{
 const root=await mkdtemp(join(tmpdir(),'gdd-stream-zip-'));
 try{
  const path=join(root,'media.zip'),file=await open(path,'w');let position=0;
  const zip=new StreamZip({async write(bytes){await file.write(bytes,0,bytes.length,position);position+=bytes.length;},close:()=>file.close()});
  await Promise.all([zip.add('媒体/第0001页/video/0001_视频.mp4',payload(str('original bytes'))),zip.add('下载报告.md',payload(str('# 下载报告\n成功')))]);
  const info=await zip.close();assert.equal(info.bytes,position);assert.equal(info.zip64,false);
  execFileSync('python3',['-c','import zipfile,sys; z=zipfile.ZipFile(sys.argv[1]); assert z.testzip() is None; assert z.read("媒体/第0001页/video/0001_视频.mp4")==b"original bytes"; assert len(z.namelist())==2',path]);
 }finally{await rm(root,{recursive:true,force:true});}
});
test('ZIP64 large offsets round-trip through an actual sparse file above 4GB without allocating media-sized memory',async()=>{
 const root=await mkdtemp(join(tmpdir(),'gdd-zip64-'));
 try{
  const path=join(root,'large-offset.zip'),file=await open(path,'w'),initialOffset=0x100000000+17;let position=initialOffset;
  const zip=new StreamZip({async write(bytes){await file.write(bytes,0,bytes.length,position);position+=bytes.length;},close:()=>file.close()},{initialOffset});
  await zip.add('下载报告.md',payload(str('ZIP64 offset evidence')));const info=await zip.close();assert.equal(info.zip64,true);assert.ok(info.bytes>0x100000000);
  execFileSync('python3',['-c','import zipfile,sys; z=zipfile.ZipFile(sys.argv[1]); assert z.testzip() is None; assert z.read("下载报告.md")==b"ZIP64 offset evidence"',path]);
 }finally{await rm(root,{recursive:true,force:true});}
});
test('ZIP64 stores exact file sizes and entry counts, including their sentinel boundaries',()=>{
 const path=str('a'),entry={path,size:0x100000005,offset:0x100000009,crc:42};
 const l=localHeader(entry),lv=new DataView(l.buffer);assert.equal(lv.getUint32(18,true),0xffffffff);assert.equal(lv.getBigUint64(35,true),BigInt(entry.size));
 const c=centralHeader(entry),cv=new DataView(c.buffer);assert.equal(cv.getUint32(42,true),0xffffffff);assert.equal(cv.getBigUint64(67,true),BigInt(entry.offset));
 const ends=endRecords(65536,42,0x100000009,0x100000033);assert.equal(ends.length,3);assert.equal(new DataView(ends[0].buffer).getBigUint64(24,true),65536n);assert.equal(new DataView(ends[2].buffer).getUint16(8,true),65535);
});
test('changed or truncated temporary media cannot become a purported complete ZIP',async()=>{
 const writes=[],zip=new StreamZip({write:async b=>writes.push(b.length),close:async()=>assert.fail('invalid archive must not close')});
 await assert.rejects(zip.add('video.mp4',{length:100,crc:42,stream:()=>new Blob([str('short')]).stream()}),/大小|CRC/);await assert.rejects(zip.close());
 for(const name of ['../bad','/bad','x/../bad','x\\..\\bad']){const z=new StreamZip({write:async()=>{},close:async()=>{}});await assert.rejects(z.add(name,payload(str('x'))),/文件名/);}
});
test('disk failures keep quota and unavailable errors separate from HTTP/network failures',()=>{
 assert.equal(diskError({name:'QuotaExceededError',code:22}).code,'DISK_QUOTA');assert.equal(diskError({name:'UnknownError',code:0}).code,'DISK_IO');
 const error=Object.assign(new Error('unavailable'),{code:'DISK_UNAVAILABLE'});assert.equal(diskError(error),error);
});
