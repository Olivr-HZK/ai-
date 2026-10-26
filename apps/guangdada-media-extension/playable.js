// Inspect bytes as text only. Never render or execute the downloaded HTML in
// an extension document; preserve the original bytes in the ZIP.
export function inspectPlayable(bytes, responseUrl) {
  const html=new TextDecoder().decode(bytes);
  if(!/<(?:!doctype\s+html|html)(?:\s|>)/i.test(html) || !/<(?:script|canvas|iframe)(?:\s|>)/i.test(html))return {errorCode:'PLAYABLE_NOT_HTML'};
  const loginPath=/(?:^|\/)(?:login|signin|sign-in|authorize)(?:\/|$)/i.test(new URL(responseUrl).pathname);
  const passwordInput=/<input\b[^>]*\btype\s*=\s*["']?password(?:["'\s>])/i.test(html);
  if(loginPath || passwordInput)return {errorCode:'PLAYABLE_LOGIN_PAGE'};
  const dependencies=new Set();
  for(const tag of html.matchAll(/<(?:script|img|link|iframe|audio|video|source)\b[^>]*>/gi)){
    const attr=/\b(?:src|href)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/i.exec(tag[0]);
    const raw=attr?.[1] ?? attr?.[2] ?? attr?.[3];
    if(!raw || /^(?:data|blob|javascript):/i.test(raw))continue;
    try{const url=new URL(raw,responseUrl);if(['http:','https:'].includes(url.protocol))dependencies.add(url.href);}catch{/* Invalid references do not trigger a request. */}
  }
  return {externalReferences:dependencies.size,note:`Playable 原 HTML 获取成功${dependencies.size?`，静态检查发现 ${dependencies.size} 处外部资源引用`:''}。外部脚本、图片、声音及动态依赖未打包，可能需要联网；未执行试玩脚本，未验证离线可玩性。`};
}

// Large Playable HTML stays on disk too. Keep a bounded tag overlap so ordinary
// tags split across stream chunks are recognized, without retaining script bodies.
export async function inspectPlayableFile(file,responseUrl,signal) {
  const dependencies=new Set(),decoder=new TextDecoder();let carry='',html=false,renderable=false,password=false,capped=false;
  const scan=text=>{
    const source=carry+text;
    html ||= /<(?:!doctype\s+html|html)(?:\s|>)/i.test(source);
    renderable ||= /<(?:script|canvas|iframe)(?:\s|>)/i.test(source);
    password ||= /<input\b[^>]*\btype\s*=\s*["']?password(?:["'\s>])/i.test(source);
    for(const tag of source.matchAll(/<(?:script|img|link|iframe|audio|video|source)\b[^>]*>/gi)){
      const attr=/\b(?:src|href)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/i.exec(tag[0]),raw=attr?.[1] ?? attr?.[2] ?? attr?.[3];
      if(!raw || /^(?:data|blob|javascript):/i.test(raw))continue;
      if(raw.length>8192){capped=true;continue;}
      try{const url=new URL(raw,responseUrl);if(['http:','https:'].includes(url.protocol)){if(dependencies.size<4096)dependencies.add(url.href);else if(!dependencies.has(url.href))capped=true;}}catch{/* No dependency requests. */}
    }
    carry=source.slice(-65536);
  };
  const reader=file.stream().getReader();
  try{while(true){if(signal?.aborted)throw signal.reason;const {value,done}=await reader.read();if(done)break;scan(decoder.decode(value,{stream:true}));}scan(decoder.decode());}
  finally{await reader.cancel().catch(()=>{});reader.releaseLock();}
  if(!html || !renderable)return {errorCode:'PLAYABLE_NOT_HTML'};
  if(password || /(?:^|\/)(?:login|signin|sign-in|authorize)(?:\/|$)/i.test(new URL(responseUrl).pathname))return {errorCode:'PLAYABLE_LOGIN_PAGE'};
  return {externalReferences:dependencies.size,externalReferencesCapped:capped,note:`Playable 原 HTML 获取成功${dependencies.size?`，静态检查发现${capped?'至少':''} ${dependencies.size} 处外部资源引用`:''}。${capped?'引用数量受静态统计上限影响。':''}外部脚本、图片、声音及动态依赖未打包，可能需要联网；未执行试玩脚本，未验证离线可玩性。`};
}
