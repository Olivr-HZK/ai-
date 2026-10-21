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
