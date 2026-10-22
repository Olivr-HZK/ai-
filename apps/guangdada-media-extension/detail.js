// Executed in the page: use the site's normal card click and visible detail
// links. Never query a private API or take a link from another creative.
export function detailControl(action, expected) {
  const visible=e=>!!e?.getClientRects().length;
  const links=[...document.querySelectorAll('a[href]')].filter(visible);
  const detailLinks=links.filter(a=>{try{return new URL(a.href).pathname==='/modules/creative/display-ads/detail' && a.textContent.includes('单页打开');}catch{return false;}});
  if(detailLinks.length>1)return {failure:'DETAIL_BUSY'};
  const identity=detailLinks[0],id=identity && new URL(identity.href).searchParams.get('id');
  let root=identity?.closest('[role="dialog"],.ant-modal-content,.ant-drawer-content');
  if(identity && !root){
    for(let el=identity.parentElement,depth=0;el && el!==document.body && depth<9;el=el.parentElement,depth++){
      if([...el.querySelectorAll('button')].some(b=>visible(b) && /^(Close|关闭)$/i.test(b.getAttribute('aria-label') || b.textContent.trim()))){root=el;break;}
    }
  }
  const dialogVisible=!!identity || [...document.querySelectorAll('[role="dialog"],.ant-modal-content,.ant-drawer-content')].some(visible);
  if(action==='idle')return {visible:dialogVisible};
  if(action==='close'){
    if(root && id===expected.adId){
      const close=[...root.querySelectorAll('button')].find(b=>visible(b) && /^(Close|关闭)$/i.test(b.getAttribute('aria-label') || b.textContent.trim()));
      if(close)close.click();else return {failure:'DETAIL_CLOSE_FAILED'};
      if(expected.scroll)window.scrollTo(expected.scroll.x,expected.scroll.y);
      return {closed:true};
    }
    return identity?{failure:'DETAIL_ID_MISMATCH'}:{closed:true};
  }
  if(action==='read' || identity){
    if(!identity)return {pending:true,dialogVisible};
    if(id!==expected.adId)return {failure:'DETAIL_ID_MISMATCH'};
    if(!root)return {pending:true};
    const candidates=[...root.querySelectorAll('a[href]')].filter(visible).filter(a=>{
      const text=a.textContent.trim();let u;try{u=new URL(a.href);}catch{return false;}
      if(!['http:','https:'].includes(u.protocol) || u.username || u.password)return false;
      // The restriction banner is the site's explicit original-media link.
      // YouTube original links are also rendered in the overview section.
      return /官方资源限制.*原始链接/.test(text) || ['youtube.com','www.youtube.com','m.youtube.com','music.youtube.com','youtu.be','www.youtu.be','youtube-nocookie.com','www.youtube-nocookie.com'].includes(u.hostname);
    }).map(a=>a.href);
    if(!candidates.length)return {pending:true,identityConfirmed:true,dialogVisible:true};
    return {url:candidates[0],candidateUrls:[...new Set(candidates)].slice(0,4),adId:id};
  }
  if([...document.querySelectorAll('[role="dialog"],.ant-modal-content,.ant-drawer-content')].some(visible))return {failure:'DETAIL_BUSY'};
  const cards=[...document.querySelectorAll('.shadow-common-light.bg-white')].filter(visible).sort((a,b)=>{
    const ar=a.getBoundingClientRect(),br=b.getBoundingClientRect();return Math.abs(ar.top-br.top)<4?ar.left-br.left:ar.top-br.top;
  });
  const card=cards[expected.cardIndex-1];let value;
  const key=card && Object.keys(card).find(k=>k.startsWith('__reactFiber$'));
  for(let fiber=key && card[key],depth=0;fiber && depth<8;fiber=fiber.return,depth++){
    if(fiber.memoizedProps?.value?.ad_key){value=fiber.memoizedProps.value;break;}
  }
  if(!card || value?.ad_key!==expected.adId)return {failure:'DETAIL_CARD_CHANGED'};
  const scroll={x:window.scrollX,y:window.scrollY};card.scrollIntoView({block:'center'});
  const images=[...card.querySelectorAll('img')].filter(visible).sort((a,b)=>b.getBoundingClientRect().width*b.getBoundingClientRect().height-a.getBoundingClientRect().width*a.getBoundingClientRect().height);
  (images[0] || card).click();
  return {pending:true,opened:true,scroll};
}

export function needsDetail(item) {
  if(item.kind!=='video')return false;
  if(!item.url)return item.reasonCode==='NO_MEDIA_URL';
  const incomplete=url=>{try{const u=new URL(url);return /(^|\.)youtube(?:-nocookie)?\.com$|(^|\.)youtu\.be$/.test(u.hostname) && !(u.hostname.endsWith('youtu.be')?u.pathname.slice(1).split('/')[0]:u.pathname==='/watch'?u.searchParams.get('v'):u.pathname.split('/')[2])?.match(/^[\w-]{11}$/);}catch{return true;}};
  return incomplete(item.url) && !(item.candidateUrls || []).some(url=>!incomplete(url));
}
