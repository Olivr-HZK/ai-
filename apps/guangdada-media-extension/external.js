// Static external-video resolution. Never evaluate page scripts, submit forms,
// call player/private APIs, or use thumbnails as the requested video.
export const EXTERNAL_PAGE_BYTES=8*1024*1024;
const YT_HOSTS=new Set(['youtube.com','www.youtube.com','m.youtube.com','music.youtube.com','youtube-nocookie.com','www.youtube-nocookie.com','youtu.be','www.youtu.be']);
export const externalError=(code,status)=>Object.assign(new Error(code),{code,httpStatus:status || null});
function unescapeUrl(value) {
  return String(value || '').trim().replace(/\\u0026/gi,'&').replace(/\\\//g,'/').replace(/&(?:amp|#0*38|#x0*26);/gi,'&');
}
export function youtubeUrl(value) {
  let u;try{u=new URL(unescapeUrl(value));}catch{return null;}
  if(!YT_HOSTS.has(u.hostname))return null;
  const id=u.hostname.endsWith('youtu.be')?u.pathname.slice(1).split('/')[0]:u.pathname==='/watch'?u.searchParams.get('v'):u.pathname.match(/^\/(?:embed|shorts|live|v)\/([^/]+)/)?.[1];
  if(!/^[A-Za-z0-9_-]{11}$/.test(id || ''))throw externalError('EXTERNAL_INCOMPLETE_ID');
  return {id,url:`https://www.youtube.com/watch?v=${id}`};
}
export function videoUrl(value,base) {
  const u=new URL(unescapeUrl(value),base);
  if(!['http:','https:'].includes(u.protocol) || u.username || u.password)throw externalError('EXTERNAL_INVALID_URL');
  const yt=youtubeUrl(u.href);if(yt)return yt.url;
  u.hash='';return u.href;
}
function privateHost(host) {
  return host==='localhost' || host.endsWith('.localhost') || host.endsWith('.local') || host==='[::1]' || host.includes(':') || /^(?:0|10|127|169\.254|192\.168)\./.test(host) || /^172\.(?:1[6-9]|2\d|3[01])\./.test(host);
}
export function discoveredUrl(value,base) {
  const url=videoUrl(value,base),u=new URL(url),b=new URL(base);
  if(privateHost(u.hostname) && u.origin!==b.origin)throw externalError('EXTERNAL_INVALID_URL');
  return url;
}
export function isHtml(bytes,type='') {
  return /(?:text\/html|application\/xhtml)/i.test(type) || /^\s*(?:\uFEFF)?\s*(?:<!doctype\s+html|<html\b|<head\b|<body\b|<script\b|<meta\b|<video\b)/i.test(new TextDecoder().decode(bytes.subarray(0,1024)));
}
export function videoExtension(bytes) {
  const ascii=(start,end)=>String.fromCharCode(...bytes.subarray(start,end));
  if(bytes.length>=12 && ascii(4,8)==='ftyp')return 'mp4';
  if(bytes.length>=4 && bytes[0]===0x1a && bytes[1]===0x45 && bytes[2]===0xdf && bytes[3]===0xa3)return 'webm';
  if(bytes.length>=12 && /^(?:moov|mdat|wide)$/.test(ascii(4,8)))return 'mov';
  return null;
}
const decode=text=>String(text || '').replace(/&(#x[0-9a-f]+|#\d+|amp|quot|apos|lt|gt);/gi,(_,c)=>{
  if(c[0]==='#'){const n=c[1].toLowerCase()==='x'?parseInt(c.slice(2),16):parseInt(c.slice(1),10);return n>0 && n<=0x10ffff?String.fromCodePoint(n):'';}
  return ({amp:'&',quot:'"',apos:"'",lt:'<',gt:'>'})[c.toLowerCase()];
});
function attributes(tag) {
  const out={};for(const m of tag.matchAll(/([\w:-]+)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/g))out[m[1].toLowerCase()]=decode(m[2] ?? m[3] ?? m[4]);return out;
}
function objectAfter(text,marker) {
  const match=marker.exec(text);if(!match)return null;
  const start=text.indexOf('{',match.index+match[0].length);if(start<0)return null;
  let depth=0,quote=false,escape=false;
  for(let i=start;i<text.length;i++){
    const c=text[i];if(quote){if(escape)escape=false;else if(c==='\\')escape=true;else if(c==='"')quote=false;}
    else if(c==='"')quote=true;else if(c==='{')depth++;else if(c==='}' && --depth===0){try{return JSON.parse(text.slice(start,i+1));}catch{return null;}}
  }return null;
}
export function extractVideoLinks(html,base) {
  if(html.length>EXTERNAL_PAGE_BYTES)throw externalError('EXTERNAL_PAGE_LIMIT');
  const candidates=[],seen=new Set();let invalid=false;
  const add=(raw,method)=>{try{const url=discoveredUrl(raw,base);if(!seen.has(url)){seen.add(url);candidates.push({url,method});}}catch{invalid=true;}};
  const yt=youtubeUrl(base);
  if(yt){
    const player=objectAfter(html,/\bytInitialPlayerResponse\s*=/) || objectAfter(html,/["']ytInitialPlayerResponse["']\s*:/);
    if(!player)return {candidates,errorCode:'EXTERNAL_NO_MEDIA'};
    if(player.playabilityStatus?.status && player.playabilityStatus.status!=='OK')return {candidates,errorCode:'EXTERNAL_ACCESS_REQUIRED'};
    if(player.videoDetails?.videoId!==yt.id)return {candidates,errorCode:'EXTERNAL_ID_MISMATCH'};
    const data=player.streamingData || {},formats=Array.isArray(data.formats)?data.formats:[];
    if(data.licenseInfos?.length)return {candidates,errorCode:'EXTERNAL_DRM'};
    for(const f of [...formats].sort((a,b)=>(b.height || 0)-(a.height || 0))){
      const mime=String(f.mimeType || ''),codecs=mime.split('codecs=')[1] || '';
      // formats must positively contain video AND audio, never adaptive video-only.
      if(f.url && !f.signatureCipher && !f.cipher && /video\/(mp4|webm)/i.test(mime) && /(?:mp4a|opus|vorbis)/i.test(codecs) && /(?:avc|av01|vp0?9|vp8|hev|hvc)/i.test(codecs) && !f.drmFamilies?.length && !f.drmTrackType){
        try{const u=new URL(f.url);if(u.protocol==='https:' && (u.hostname==='googlevideo.com' || u.hostname.endsWith('.googlevideo.com')))add(f.url,'YouTube公开合流地址');}catch{/* Invalid public format. */}
      }
    }
    const cipher=[...formats,...(Array.isArray(data.adaptiveFormats)?data.adaptiveFormats:[])].some(f=>f.signatureCipher || f.cipher);
    return {candidates,errorCode:candidates.length?null:cipher?'EXTERNAL_SIGNATURE_REQUIRED':data.adaptiveFormats?.length || data.hlsManifestUrl || data.dashManifestUrl?'EXTERNAL_SEPARATE_STREAMS':'EXTERNAL_NO_MEDIA'};
  }
  if(/<input\b[^>]*\btype\s*=\s*["']?password\b/i.test(html))return {candidates,errorCode:'EXTERNAL_ACCESS_REQUIRED'};
  // Remove comments and script bodies before reading tags; markup quoted inside
  // JavaScript must not masquerade as a visible video element.
  const markup=html.replace(/<!--[\s\S]*?-->/g,'').replace(/<script\b[\s\S]*?<\/script\s*>/gi,'');
  for(const video of markup.matchAll(/<video\b([^>]*)>([\s\S]*?)(?:<\/video\s*>|$)/gi)){
    const a=attributes(video[1]);if(a.src)add(a.src,'video元素');
    for(const source of video[2].matchAll(/<source\b([^>]*)>/gi)){const a=attributes(source[1]);if(a.src && (!a.type || /^video\//i.test(a.type)))add(a.src,'video source元素');}
  }
  for(const tag of markup.matchAll(/<meta\b([^>]*)>/gi)){
    const a=attributes(tag[1]),key=(a.property || a.name || a.itemprop || '').toLowerCase();
    if(['og:video','og:video:url','og:video:secure_url','contenturl'].includes(key) && a.content)add(a.content,'视频元数据');
  }
  for(const script of html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script\s*>/gi)){
    if(attributes(script[1]).type!=='application/ld+json')continue;
    try{
      const queue=[JSON.parse(script[2])];let visits=0;
      while(queue.length && visits++<100){const obj=queue.shift();if(!obj || typeof obj!=='object')continue;if(Array.isArray(obj)){queue.push(...obj);continue;}
        if([obj['@type']].flat().some(t=>t==='VideoObject' || String(t).endsWith('/VideoObject')) && typeof obj.contentUrl==='string')add(obj.contentUrl,'VideoObject原视频地址');
        if(obj['@graph'])queue.push(obj['@graph']);if(obj.video)queue.push(obj.video);
      }
    }catch{/* Malformed JSON-LD is never executed. */}
  }
  for(const tag of markup.matchAll(/<iframe\b([^>]*)>/gi)){
    const src=attributes(tag[1]).src;try{if(src && youtubeUrl(new URL(decode(src),base).href))add(src,'YouTube嵌入外链');}catch{/* Incomplete ID: no guessing. */}
  }
  return {candidates:candidates.slice(0,4),errorCode:candidates.length?null:invalid?'EXTERNAL_INVALID_URL':'EXTERNAL_NO_MEDIA'};
}

export async function fetchExternalVideo(item,{request,read,discard,signal}) {
  const queue=[...new Set([item.url,...(item.candidateUrls || [])])].slice(0,4).map(url=>({url,method:'卡片原地址',depth:0}));
  const seen=new Set(),attempts=[];let pages=0,attempted=false,last=externalError('EXTERNAL_NO_MEDIA');
  while(queue.length && attempts.length<8){
    if(signal.aborted)throw signal.reason;
    const next=queue.shift();let url=next.url,bytes;
    const trace={method:next.method,code:'NETWORK'};
    try{
      if(youtubeUrl(url))attempted=true;
      url=videoUrl(url);if(seen.has(url))continue;seen.add(url);
      trace.host=new URL(url).hostname;attempts.push(trace);
      if(/\.(m3u8|mpd|ts)$/i.test(new URL(url).pathname))throw externalError('EXTERNAL_SEPARATE_STREAMS');
      const response=await request(url,next.method!=='卡片原地址' || !!youtubeUrl(url));
      if(!response.ok)throw externalError(`HTTP_${response.status}`,response.status);
      if(/application\/json/i.test(response.headers.get('content-type') || '')){await response.body?.cancel();throw externalError('NOT_MEDIA');}
      bytes=await read(response);
      if(!bytes.length)throw externalError('EMPTY_RESPONSE');
      if(isHtml(bytes,response.headers.get('content-type') || '')){
        attempted=true;discard(bytes.length);const text=new TextDecoder().decode(bytes);bytes=null;
        if(++pages>3 || next.depth>=2)throw externalError('EXTERNAL_DEPTH_LIMIT');
        const parsed=extractVideoLinks(text,response.url || url);trace.code=parsed.errorCode || 'PARSED';
        queue.unshift(...parsed.candidates.map(c=>({...c,depth:next.depth+1})));
        if(parsed.errorCode)last=externalError(parsed.errorCode);
        continue;
      }
      const extension=videoExtension(bytes);
      if(!extension)throw externalError('NOT_MEDIA');
      trace.code='OK';if(attempts.length>1)attempted=true;
      return {bytes,extension,resolvedUrl:response.url || url,externalInfo:attempted?{attempted:true,resolved:true,attempts}:null};
    }catch(error){
      if(bytes)discard(bytes.length);
      if(signal.aborted)throw signal.reason;
      if(!attempts.includes(trace))attempts.push(trace);
      trace.code=error.code || error.detail?.code || 'NETWORK';trace.httpStatus=error.httpStatus || null;
      last=error;if(attempts.length>1 || next.method!=='卡片原地址' || trace.code.startsWith('EXTERNAL_'))attempted=true;
    }
  }
  if(queue.length && attempts.length>=8)last=externalError('EXTERNAL_ATTEMPT_LIMIT');
  last.externalInfo=attempted?{attempted:true,resolved:false,attempts}:null;throw last;
}
