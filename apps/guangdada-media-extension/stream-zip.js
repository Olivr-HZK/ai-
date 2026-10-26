import {crc32Accumulator} from './zip.js';
const U32=0xffffffff,U16=0xffff;
const safe=n=>Number.isSafeInteger(n) && n>=0;
const bytesOf=(n)=>({bytes:new Uint8Array(n),view:null});
function block(n){const b=bytesOf(n);b.view=new DataView(b.bytes.buffer);return b;}
function u64(view,at,value){view.setBigUint64(at,BigInt(value),true);}
function nameBytes(name){
  if(typeof name!=='string' || !name || /^[\\/]/.test(name) || name.split(/[\\/]/).includes('..') || name.includes('\0'))throw new Error('无效的 ZIP 文件名');
  const bytes=new TextEncoder().encode(name);if(bytes.length>U16)throw new Error('ZIP 文件名过长');return bytes;
}
export function localHeader(entry){
  const large=entry.size>=U32,extra=large?20:0,b=block(30+entry.path.length+extra),v=b.view;
  v.setUint32(0,0x04034b50,true);v.setUint16(4,large?45:20,true);v.setUint16(6,0x800,true);v.setUint16(12,33,true);v.setUint32(14,entry.crc,true);
  v.setUint32(18,large?U32:entry.size,true);v.setUint32(22,large?U32:entry.size,true);v.setUint16(26,entry.path.length,true);v.setUint16(28,extra,true);b.bytes.set(entry.path,30);
  if(large){const p=30+entry.path.length;v.setUint16(p,1,true);v.setUint16(p+2,16,true);u64(v,p+4,entry.size);u64(v,p+12,entry.size);}
  return b.bytes;
}
export function centralHeader(entry){
  const size64=entry.size>=U32,offset64=entry.offset>=U32,data=(size64?16:0)+(offset64?8:0),extra=data?data+4:0,b=block(46+entry.path.length+extra),v=b.view;
  v.setUint32(0,0x02014b50,true);v.setUint16(4,extra?45:20,true);v.setUint16(6,extra?45:20,true);v.setUint16(8,0x800,true);v.setUint16(14,33,true);v.setUint32(16,entry.crc,true);
  v.setUint32(20,size64?U32:entry.size,true);v.setUint32(24,size64?U32:entry.size,true);v.setUint16(28,entry.path.length,true);v.setUint16(30,extra,true);v.setUint32(42,offset64?U32:entry.offset,true);b.bytes.set(entry.path,46);
  if(data){let p=46+entry.path.length;v.setUint16(p,1,true);v.setUint16(p+2,data,true);p+=4;if(size64){u64(v,p,entry.size);u64(v,p+8,entry.size);p+=16;}if(offset64)u64(v,p,entry.offset);}
  return b.bytes;
}
export function endRecords(count,size,offset,zip64Offset){
  const large=count>=U16 || size>=U32 || offset>=U32,out=[];
  if(large){
    const b=block(56),v=b.view;v.setUint32(0,0x06064b50,true);u64(v,4,44);v.setUint16(12,45,true);v.setUint16(14,45,true);u64(v,24,count);u64(v,32,count);u64(v,40,size);u64(v,48,offset);out.push(b.bytes);
    const l=block(20);l.view.setUint32(0,0x07064b50,true);u64(l.view,8,zip64Offset);l.view.setUint32(16,1,true);out.push(l.bytes);
  }
  const e=block(22),v=e.view;v.setUint32(0,0x06054b50,true);v.setUint16(8,Math.min(count,U16),true);v.setUint16(10,Math.min(count,U16),true);v.setUint32(12,Math.min(size,U32),true);v.setUint32(16,Math.min(offset,U32),true);out.push(e.bytes);return out;
}
// Only directory metadata is retained. Every payload is read from a disk-backed
// file and copied with backpressure; no archive-sized Blob/Uint8Array is built.
export class StreamZip {
  constructor(sink,{initialOffset=0}={}){if(!safe(initialOffset))throw new Error('无效ZIP偏移');this.sink=sink;this.position=initialOffset;this.entries=[];this.chain=Promise.resolve();this.closed=false;}
  async write(bytes){if(!safe(this.position+bytes.length))throw new Error('ZIP 超过可精确处理的文件大小');await this.sink.write(bytes);this.position+=bytes.length;}
  add(name,payload){
    if(this.closed)throw new Error('ZIP 已结束');
    const next=this.chain.then(()=>this.append(name,payload));this.chain=next;return next;
  }
  async append(name,payload){
    if(!safe(payload.length) || !Number.isInteger(payload.crc))throw new Error('无效ZIP文件大小或CRC');
    const entry={path:nameBytes(name),size:payload.length,crc:payload.crc>>>0,offset:this.position};
    await this.write(localHeader(entry));const crc=crc32Accumulator();let size=0;
    const reader=payload.stream().getReader();
    try{while(true){const {value,done}=await reader.read();if(done)break;size+=value.length;if(size>entry.size)throw new Error('临时媒体大小已变化');crc.update(value);await this.write(value);}}
    finally{await reader.cancel().catch(()=>{});reader.releaseLock();}
    if(size!==entry.size || crc.digest()!==entry.crc)throw new Error('临时媒体字节或CRC校验失败');
    this.entries.push(entry);
  }
  async close(){
    if(this.closed)throw new Error('ZIP 已结束');this.closed=true;await this.chain;
    const offset=this.position;for(const entry of this.entries)await this.write(centralHeader(entry));const size=this.position-offset,zip64Offset=this.position;
    for(const bytes of endRecords(this.entries.length,size,offset,zip64Offset))await this.write(bytes);
    await this.sink.close();return {bytes:this.position,files:this.entries.length,zip64:this.entries.length>=U16 || size>=U32 || offset>=U32};
  }
}
