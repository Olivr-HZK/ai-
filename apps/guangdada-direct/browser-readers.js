// Standalone page readers. Development-time rule reuse; no extension APIs/runtime.
async ({method,args=[]}) => {
async function collectMedia(showNumbers = false) {
  const cards = [...document.querySelectorAll('.shadow-common-light.bg-white')]
    .filter(card => card.getClientRects().length > 0)
    .sort((a,b) => {
      const ar=a.getBoundingClientRect(), br=b.getBoundingClientRect();
      return Math.abs(ar.top-br.top)<4 ? ar.left-br.left : ar.top-br.top;
    });
  if (!cards.length) throw new Error('未识别到展示广告素材卡片，请在广大大展示广告列表页使用。');
  const items = [], missing = [], cardKeys = [], cardDates = [];
  function visibleDate(card) {
    const ranges=[...String(card.innerText || '').matchAll(/(?<![\d-])(\d{4}-\d{2}-\d{2})\s*[~～至]\s*(\d{4}-\d{2}-\d{2})(?!\d)/g)];
    const valid=value=>{const d=new Date(value+'T00:00:00Z');return !isNaN(d) && d.toISOString().slice(0,10)===value;};
    if(ranges.length!==1 || !valid(ranges[0][1]) || !valid(ranges[0][2]) || ranges[0][1]>ranges[0][2])return null;
    return {date:ranges[0][1],source:'visible_card_date_range'};
  }
  const toolbarId = 'gdd-download-selection-toolbar';
  function updateSelection() {
    const label = document.querySelector(`#${toolbarId} [data-gdd-selection-count]`);
    const badges = cards.map(c=>c.querySelector('[data-gdd-download-number]')).filter(Boolean);
    for (const b of badges) {
      const checked = b.getAttribute('aria-pressed') === 'true';
      b.textContent = `${checked ? '☑' : '☐'} ${b.dataset.number}`;
      b.style.background = checked ? '#2563eb' : '#ffffff';
      b.style.color = checked ? '#ffffff' : '#17243a';
    }
    if (label) label.textContent = `已选 ${badges.filter(b=>b.getAttribute('aria-pressed')==='true').length}/${cards.length} · 选好后打开插件打包`;
  }
  let matchedCards = 0, videoCards = 0, imageCards = 0, playableCards = 0, unsupported = 0;
  function boundValue(card) {
    const key = Object.keys(card).find(k => k.startsWith('__reactFiber$'));
    let fiber = key && card[key];
    for (let depth = 0; fiber && depth < 8; depth++, fiber = fiber.return) {
      const value = fiber.memoizedProps?.value;
      if (value && typeof value === 'object' && typeof value.ad_key === 'string') return value;
    }
    return null;
  }
  for (const [index, card] of cards.entries()) {
    const value = boundValue(card);
    const cardIndex = index + 1;
    cardKeys.push(value?.ad_key || null);
    cardDates.push(visibleDate(card));
    if (showNumbers) {
      let badge = card.querySelector('[data-gdd-download-number]');
      if (!badge) {
        badge = document.createElement('button');
        badge.type = 'button';
        badge.setAttribute('data-gdd-download-number', '');
        badge.style.cssText = 'position:absolute;top:4px;left:4px;z-index:30;border:2px solid #2563eb;border-radius:6px;padding:4px 9px;font:bold 16px/22px system-ui;cursor:pointer;box-shadow:0 1px 4px #0005';
        badge.setAttribute('aria-pressed', 'false');
        for (const event of ['pointerdown','mousedown','dblclick']) badge.addEventListener(event,e=>e.stopPropagation());
        badge.addEventListener('click', e=>{
          e.preventDefault(); e.stopPropagation();
          badge.setAttribute('aria-pressed', String(badge.getAttribute('aria-pressed')!=='true'));
          updateSelection();
        });
        card.append(badge);
      }
      if (badge.dataset.adId !== (value?.ad_key || '')) badge.setAttribute('aria-pressed','false');
      badge.dataset.adId = value?.ad_key || '';
      badge.dataset.number = String(cardIndex);
      badge.setAttribute('aria-label', `选择素材 ${cardIndex}`);
    }
    if (!value) { missing.push({cardIndex, reasonCode:'CARD_DATA_MISSING', reason: '无法读取此卡片的素材数据，尚未取得原素材地址'}); continue; }
    matchedCards++;
    const resources = Array.isArray(value.resource_urls) ? value.resource_urls : [];
    const video = Number(value.ads_type) === 2;
    const image = Number(value.ads_type) === 1;
    const playable = Number(value.ads_type) === 7;
    const kind = playable?'playable':video?'video':'image';
    if (video) videoCards++;
    else if (image) imageCards++;
    else if (playable) playableCards++;
    else { unsupported++; missing.push({cardIndex, adId:value.ad_key, reasonCode:'UNSUPPORTED_TYPE', reason:`未识别的广告类型（${value.ads_type ?? '未知'}），当前支持图片、视频和 Playable 试玩`}); continue; }
    // One primary creative per card. Never download video covers, app logos,
    // blurred duplicate backgrounds, or recommendations from parent props.
    const cdn = Array.isArray(value.cdn_url)?value.cdn_url:[value.cdn_url];
    const playableUrls = [...resources.map(r=>r?.html_url),value.html_url,...cdn.filter(u=>typeof u==='string' && /\.html?(?:[?#]|$)/i.test(u))].filter(u=>typeof u==='string' && u.trim());
    const youtubeLink=u=>{try{return ['youtube.com','www.youtube.com','m.youtube.com','youtube-nocookie.com','www.youtube-nocookie.com','youtu.be','www.youtu.be'].includes(new URL(u).hostname);}catch{return false;}};
    const videos=video?[...resources.map(r=>r?.video_url),value.video_url,
      ...[...cdn,...resources.flatMap(r=>Array.isArray(r?.cdn_url)?r.cdn_url:[r?.cdn_url]),...resources.map(r=>r?.download_url),value.download_url].filter(u=>typeof u==='string' && (/\.(mp4|webm|mov|m4v)(?:[?#]|$)/i.test(u) || youtubeLink(u))),
      ...[value.source_url,value.page_url,value.landing_url,value.external_url,...resources.map(r=>r?.source_url)].filter(youtubeLink),
      ...[...card.querySelectorAll('a[href]')].filter(a=>a.getClientRects().length && youtubeLink(a.href)).map(a=>a.href)
    ].filter(u=>typeof u==='string' && u.trim()):[];
    const raw = playable ? playableUrls.find(u=>! /\.(mp4|webm|mov|m4v|png|jpg|jpeg|webp|gif)(?:[?#]|$)/i.test(u)) || playableUrls[0] : video
      ? videos.find(u=>{try{const v=new URL(u);return ['http:','https:'].includes(v.protocol) && !v.username && !v.password && !/\.(m3u8|mpd|ts)$/i.test(v.pathname);}catch{return false;}}) || videos[0]
      : resources.find(r => r && typeof r.image_url === 'string' && r.image_url)?.image_url || value.preview_img_url;
    let url, reasonCode='NO_MEDIA_URL', reason=playable?'Playable 卡片未提供可下载的原 HTML 地址（未用封面或视频代替）':video?'视频卡片未提供可下载视频地址（未用封面代替）':'图片卡片未提供可下载地址';
    try {
      if (!raw) throw new Error();
      reasonCode='INVALID_MEDIA_URL';reason='卡片提供的原素材地址格式无效，未发起下载';
      url = new URL(raw);
      if (!['https:', 'http:'].includes(url.protocol)) {reasonCode='UNSUPPORTED_SCHEME';reason='原素材使用非 HTTP 地址（如 blob），当前插件不支持直接下载';throw new Error();}
      if (url.username || url.password) {reasonCode='INVALID_MEDIA_URL';reason='原素材地址含嵌入式凭据，未发起下载';throw new Error();}
      if (/\.(m3u8|mpd|ts)$/i.test(url.pathname)) {reasonCode='STREAM_MANIFEST';reason='原素材为 HLS/DASH 清单或流片段，当前插件不支持合并下载';throw new Error();}
      if (playable && /\.(mp4|webm|mov|m4v|png|jpg|jpeg|webp|gif|zip)$/i.test(url.pathname)) {reasonCode='PLAYABLE_NOT_HTML';reason='试玩卡片没有提供原 HTML，当前地址为其他格式；未用图片、视频或ZIP冒充Playable HTML';throw new Error();}
    } catch {
      missing.push({cardIndex, adId:value.ad_key, kind, reasonCode, reason});
      continue;
    }
    const candidates=video?[...new Set(videos.filter(u=>{try{const v=new URL(u);return ['http:','https:'].includes(v.protocol) && !v.username && !v.password && !/\.(m3u8|mpd|ts)$/i.test(v.pathname);}catch{return false;}}))].slice(0,4):[];
    items.push({url:url.href, kind, cardIndex, adId:value.ad_key,...(candidates.length>1?{candidateUrls:candidates}:{})});
  }
  if (showNumbers) {
    let toolbar = document.getElementById(toolbarId);
    if (!toolbar) { toolbar=document.createElement('div');toolbar.id=toolbarId;document.body.append(toolbar); }
    toolbar.style.cssText='position:fixed;bottom:24px;left:50%;transform:translateX(-50%);z-index:2147483647;display:flex;align-items:center;gap:12px;padding:12px 18px;background:#142b4d;color:white;border-radius:12px;box-shadow:0 4px 24px #0004;font:14px/22px system-ui';
    toolbar.replaceChildren();
    const label=document.createElement('span');label.setAttribute('data-gdd-selection-count','');toolbar.append(label);
    for (const [text, action] of [['全选','all'],['清空','none'],['退出选择','close']]) {
      const button=document.createElement('button');button.type='button';button.textContent=text;
      button.style.cssText='padding:5px 10px;border:0;border-radius:6px;background:white;color:#142b4d;font:14px/22px system-ui;cursor:pointer';
      button.addEventListener('click',e=>{
        e.preventDefault();e.stopPropagation();
        for (const c of cards) {
          const b=c.querySelector('[data-gdd-download-number]');
          if (action==='close') b?.remove();
          else b?.setAttribute('aria-pressed',String(action==='all'));
        }
        if (action==='close') toolbar.remove(); else updateSelection();
      });
      toolbar.append(button);
    }
    updateSelection();
  }
  const selectionActive = !!document.getElementById(toolbarId);
  const selectedCardNumbers = cards.flatMap((card,i)=>{
    const b=card.querySelector('[data-gdd-download-number]');
    return b?.getAttribute('aria-pressed')==='true' && b.dataset.adId === cardKeys[i] ? [i+1] : [];
  });
  const activePages=[...document.querySelectorAll('.ant-pagination-item-active')].filter(e=>e.getClientRects().length);
  const number=activePages.length===1?Number(activePages[0].getAttribute('title') || activePages[0].textContent.trim()):activePages.length?null:1;
  const pageNumber=Number.isInteger(number) && number>0?number:null;
  // This function is serialized into the page: keep the sort reader local.
  // Read the rendered control's selection, never infer order from card dates,
  // a label alone, an internal sort code, or a hover/focus utility class.
  function readSort() {
    const normalize=e=>String(e?.textContent || '').replace(/[\s\u200b-\u200d\ufeff]/g,'').replace(/[↓↑▼▲▾▴⌄⌃]/g,'');
    const labels=new Set(['最新创意','热门创意','最热创意','热门素材','展示估值','最多展示','最长投放','投放天数','最新发现','最近发现','最后发现','相关性']);
    const excluded=e=>!!e.closest('.shadow-common-light.bg-white,.ant-pagination,[role="dialog"],.ant-modal-content,.ant-drawer-content,[role="tooltip"],#gdd-download-selection-toolbar');
    const visible=e=>{
      if(!e?.getClientRects().length || excluded(e))return false;
      for(let p=e;p;p=p.parentElement){
        const s=getComputedStyle(p);
        if(p.hidden || p.hasAttribute('inert') || p.getAttribute('aria-hidden')==='true' || s.display==='none' || ['hidden','collapse'].includes(s.visibility))return false;
      }
      return true;
    };
    const blue=value=>{
      const m=String(value).match(/^rgba?\(\s*([\d.]+)[,\s]+([\d.]+)[,\s]+([\d.]+)(?:\s*[,/]\s*([\d.]+))?\s*\)/i);
      return !!m && (m[4]===undefined || +m[4]>=0.8) && +m[3]>100 && +m[3]>+m[1]*1.2 && +m[3]>+m[2]*1.05;
    };
    function observe(leaf) {
      const label=normalize(leaf),nodes=[];
      for(let p=leaf;p && p!==document.body && nodes.length<5;p=p.parentElement){
        const same=normalize(p)===label,repeated=p.matches('button,label,[role="tab"],[role="radio"],[role="option"]') && normalize(p).split(label).join('')==='';
        if(!same && !repeated)break;
        nodes.push(p);
      }
      const positive=[],negative=[],inputs=new Set();let classSelected=false,accent=false;
      for(const p of nodes){
        for(const attr of ['aria-selected','aria-pressed','aria-checked','data-selected','data-active']){
          const value=p.getAttribute(attr);
          if(value==='true')positive.push(attr);
          if(value==='false')negative.push(attr);
        }
        const state=p.getAttribute('data-state');
        if(['active','selected','checked','on'].includes(state))positive.push('data-state');
        if(['inactive','unselected','unchecked','off'].includes(state))negative.push('data-state');
        for(const token of p.classList){
          if(/^(?:(?:is-)?(?:active|selected|checked)|ant-radio(?:-button)?-wrapper-checked|ant-tabs-tab-active|ant-select-item-option-selected)$/.test(token))classSelected=true;
          if(/^text-(?:blue|sky)-(?:400|500|600|700)$/.test(token))classSelected=true;
        }
        if(p.matches('.ant-select-selection-item,.ant-select-selection-selected-value') && p.closest('.ant-select'))positive.push('select-value');
        for(const input of p.querySelectorAll('input[type="radio"]'))inputs.add(input);
        if(p.tagName==='LABEL' && p.getAttribute('for')){
          const input=document.getElementById(p.getAttribute('for'));
          if(input?.matches('input[type="radio"]'))inputs.add(input);
        }
        const style=getComputedStyle(p);
        if(blue(style.color) || blue(style.backgroundColor))accent=true;
      }
      for(const input of inputs)(input.checked?positive:negative).push('radio');
      const state=positive.length && negative.length?'conflict':positive.length?'selected':negative.length?'unselected':classSelected?'selected':'unknown';
      return {label,nodes,anchor:nodes.at(-1),state,accent,evidence:positive[0] || (negative.length?'explicit-unselected':classSelected?'selected-class':null)};
    }
    const records=[],anchors=new Set();
    for(const leaf of document.querySelectorAll('span,button,div,label,a,[role="tab"],[role="radio"],[role="option"]')){
      const label=normalize(leaf);
      if(!labels.has(label) || !visible(leaf) || [...leaf.children].some(c=>normalize(c)===label))continue;
      const row=observe(leaf);
      if(!anchors.has(row.anchor)){anchors.add(row.anchor);records.push(row);}
    }
    const peers=row=>{
      let parent=row.anchor?.parentElement;
      for(let depth=0;parent && parent!==document.body && depth<3;depth++,parent=parent.parentElement){
        if(String(parent.textContent || '').length>300)break;
        const group=records.filter(r=>parent.contains(r.anchor));
        if(group.some(r=>r.label!==row.label))return group;
      }
      return [row];
    };
    // A distinctive rendered accent is useful when a site's CSS token changes.
    // It needs a nearby alternative sort control with a neutral appearance.
    for(const row of records)if(row.state==='unknown' && row.accent && peers(row).some(p=>p.label!==row.label && !p.accent)){
      row.state='selected';row.evidence='distinct-control-color';
    }
    // Native and Ant selects expose their current value rather than an active
    // tab. Closed native options themselves have no rendered client rects.
    const native=[];
    for(const select of document.querySelectorAll('select')){
      if(!visible(select))continue;
      const options=[...select.options];
      if(!options.some(o=>normalize(o)==='最新创意'))continue;
      const chosen=options.filter(o=>o.selected);
      native.push({label:chosen.length===1?normalize(chosen[0]):null,state:chosen.length===1?'selected':'conflict',evidence:'native-select'});
    }
    const latest=records.filter(r=>r.label==='最新创意'),chosen=latest.filter(r=>r.state==='selected');
    const selectedNative=native.filter(r=>r.state==='selected' && r.label==='最新创意');
    const alternative=latest.flatMap(peers).find(r=>r.label!=='最新创意' && r.state==='selected') || native.find(r=>r.label && r.label!=='最新创意');
    const count=latest.length+native.length;
    const conflict=latest.some(r=>r.state==='conflict') || native.some(r=>r.state==='conflict') || chosen.length+selectedNative.length>1 || !!alternative && chosen.length+selectedNative.length>0;
    const confirmed=!conflict && chosen.length+selectedNative.length===1;
    return {status:conflict?'ambiguous':confirmed?'confirmed':alternative?'other-sort':count?'unconfirmed':'not-found',selectedLabel:confirmed?'最新创意':alternative?.label || null,candidateCount:count,evidence:confirmed?(chosen[0] || selectedNative[0]).evidence:null};
  }
  const sortState=readSort(),newestFirst=sortState.status==='confirmed';
  return {items, missing, cardKeys, cardDates, newestFirst, sortState, pageNumber, selectionActive, selectedCardNumbers, cardCount:cards.length, matchedCards, videoCards, imageCards, playableCards, unsupported, limitReached:false};
}
function pageControl(action = 'read', expected = null) {
  let diagnostic;
  try {
  const visible = el => !!el && el.getClientRects().length > 0 && getComputedStyle(el).visibility !== 'hidden';
  const pagers = [...document.querySelectorAll('.ant-pagination')].filter(visible);
  if (pagers.length !== 1) throw new Error('未能唯一识别列表分页控件，请关闭详情弹窗并打开展示广告列表。');
  const pager = pagers[0];
  const current = pager.querySelector('.ant-pagination-item-active, [aria-current="page"]');
  const pageNumber = Number(current?.getAttribute('title') || current?.textContent.trim());
  if (!Number.isInteger(pageNumber) || pageNumber < 1) throw new Error('无法读取当前页码，已停止翻页。');
  const next = pager.querySelector('.ant-pagination-next, [title="下一页"], [aria-label="Next Page"], [aria-label="next page"]');
  if (!next) throw new Error('未识别到下一页控件，无法判断是否末页。');
  const disabled = el => !!el && (el.disabled || el.getAttribute('aria-disabled') === 'true' || el.classList.contains('ant-pagination-disabled'));
  const hasNext = !disabled(next) && !disabled(next.querySelector('button, a'));
  const cards = [...document.querySelectorAll('.shadow-common-light.bg-white')].filter(visible).sort((a,b) => {
    const ar=a.getBoundingClientRect(), br=b.getBoundingClientRect();
    return Math.abs(ar.top-br.top)<4 ? ar.left-br.left : ar.top-br.top;
  });
  const cardKeys = cards.map(card => {
    const key=Object.keys(card).find(k=>k.startsWith('__reactFiber$'));
    let fiber=key && card[key];
    for(let depth=0;fiber && depth<8;depth++,fiber=fiber.return) {
      if(typeof fiber.memoizedProps?.value?.ad_key === 'string') return fiber.memoizedProps.value.ad_key;
    }
    return null;
  });
  const loading = [...document.querySelectorAll('.ant-spin-spinning, .ant-skeleton, [aria-busy="true"]')].some(visible);
  const firstDifference=expected?.cardKeys?.findIndex((key,i)=>key!==cardKeys[i]);
  diagnostic={action,expectedPage:expected?.pageNumber ?? null,actualPage:pageNumber,expectedCards:expected?.cardKeys?.length ?? null,actualCards:cardKeys.length,loading,
    firstDifference:firstDifference>=0?firstDifference+1:null,expectedAdId:firstDifference>=0?expected.cardKeys[firstDifference]:null,actualAdId:firstDifference>=0?cardKeys[firstDifference] ?? null:null};
  if (expected && (expected.pageNumber !== pageNumber || JSON.stringify(expected.cardKeys) !== JSON.stringify(cardKeys))) {
    return {error:expected.pageNumber!==pageNumber?'页面页码暂未与采集位置一致。':'页面卡片暂未与已保存的素材身份一致。',code:expected.pageNumber!==pageNumber?'PAGE_NUMBER_CHANGED':'PAGE_CARDS_CHANGED',diagnostic,notClicked:true};
  }
  if (action === 'warm') {
    if (loading) return {error:'页面仍在加载，未滚动素材卡片。',code:'PAGE_LOADING',diagnostic,notClicked:true};
    const card = cards[expected?.cardIndex - 1];
    if (!card) throw new Error('待加载的素材卡片已变化。');
    card.scrollIntoView({block:'center',behavior:'instant'});
  }
  if (action === 'next') {
    if (loading) return {error:'页面仍在加载，未点击下一页。',code:'PAGE_LOADING',diagnostic,notClicked:true};
    if (!hasNext) return {pageNumber, cardKeys, hasNext, loading, clicked:false};
    // Selection badges belong to the previous page; never carry them forward.
    document.querySelectorAll('[data-gdd-download-number], #gdd-download-selection-toolbar').forEach(el=>el.remove());
    (next.querySelector('button, a') || next).click();
  }
  return {pageNumber, cardKeys, hasNext, loading, clicked:action === 'next' && hasNext};
  } catch (error) { return {error:error.message,diagnostic}; }
}
function detailControl(action, expected) {
  const visible=e=>!!e?.getClientRects().length && getComputedStyle(e).visibility!=='hidden' && getComputedStyle(e).display!=='none';
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
  if(action==='idle')return {visible:dialogVisible,adId:id || null};
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
    const metadataReady=/展示估值\s*[:：]?\s*[\d,]/.test(root.innerText || '');
    const info={adId:id,identityConfirmed:true,dialogVisible:true,metadataReady};
    if(expected.readDate){
      const ranges=[...String(root.innerText || '').matchAll(/(?<![\d-])(\d{4}-\d{2}-\d{2})\s*[~～至]\s*(\d{4}-\d{2}-\d{2})(?!\d)/g)];
      const valid=value=>{const d=new Date(value+'T00:00:00Z');return !isNaN(d) && d.toISOString().slice(0,10)===value;};
      if(ranges.length===1 && valid(ranges[0][1]) && valid(ranges[0][2]) && ranges[0][1]<=ranges[0][2])return {...info,firstDiscovered:ranges[0][1],dateSource:'visible_detail_date_range'};
      return {...info,pending:true};
    }
    const youtubeHost=host=>['youtube.com','www.youtube.com','m.youtube.com','music.youtube.com','youtu.be','www.youtu.be','youtube-nocookie.com','www.youtube-nocookie.com'].includes(host);
    const media=[...root.querySelectorAll('video,video source')].map(v=>v.currentSrc || v.src).filter(raw=>{try{const u=new URL(raw);return ['http:','https:'].includes(u.protocol) && !u.username && !u.password && !/\.(m3u8|mpd|ts)$/i.test(u.pathname);}catch{return false;}});
    const frames=[...root.querySelectorAll('iframe[src]')].map(v=>v.src).filter(raw=>{try{return youtubeHost(new URL(raw).hostname);}catch{return false;}});
    const links=[...root.querySelectorAll('a[href]')].filter(visible).filter(a=>{
      const text=a.textContent.trim();let u;try{u=new URL(a.href);}catch{return false;}
      if(!['http:','https:'].includes(u.protocol) || u.username || u.password)return false;
      // The restriction banner is the site's explicit original-media link.
      // YouTube original links are also rendered in the overview section.
      // Some restriction banners have a placeholder href resolving back to
      // the list page. That is not an original media link.
      const placeholder=/(^|\.)guangdada\.net$/.test(u.hostname) && !/\.(mp4|webm|mov|m4v)(?:$)/i.test(u.pathname);
      return !placeholder && (/官方资源限制.*原始链接/.test(text) || youtubeHost(u.hostname));
    }).map(a=>a.href);
    const candidates=[...new Set([...media,...frames,...links])];
    if(!candidates.length)return {...info,pending:true};
    return {...info,url:candidates[0],candidateUrls:[...new Set(candidates)].slice(0,4)};
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
  const images=[...card.querySelectorAll('img')].filter(e=>visible(e) && !e.closest('.p-3.pb-0') && e.getBoundingClientRect().width>=80 && e.getBoundingClientRect().height>=80).sort((a,b)=>b.getBoundingClientRect().width*b.getBoundingClientRect().height-a.getBoundingClientRect().width*a.getBoundingClientRect().height);
  (images[0] || card).click();
  return {pending:true,opened:true,scroll};
}
function pageAccess() {
  const visible=e=>!!e.getClientRects().length && getComputedStyle(e).visibility!=='hidden';
  const dialogs=[...document.querySelectorAll('[role="dialog"],.ant-modal-content,.ant-message-notice,.ant-notification-notice')].filter(visible);
  const text=dialogs.map(e=>e.innerText).join('\n');
  if(/其他设备.*登录|强制退出|登录.*失效|登录.*过期|重新登录|请先登录/.test(text) || /\/(?:login|signin)(?:\/|$)/i.test(location.pathname))return {blocked:true,code:'ADDRESS_LOGIN_LOST',reason:'广大大提示登录失效或需要重新登录，地址采集已中断'};
  if(dialogs.some(e=>/验证码|安全验证/.test(e.innerText)))return {blocked:true,code:'ADDRESS_VERIFICATION_REQUIRED',reason:'页面出现验证码或安全验证，地址采集已停止等待人工处理'};
  return {blocked:false};
}
function collectionContext(){
  const visible=e=>!!e?.getClientRects().length && getComputedStyle(e).visibility!=='hidden';
  const excluded=e=>!!e.closest('.shadow-common-light.bg-white,.ant-pagination,[role="dialog"],.ant-modal-content,.ant-drawer-content,#gdd-download-selection-toolbar');
  const text=e=>String(e.innerText || e.textContent || '').replace(/\s+/g,' ').trim();
  const label=e=>e.getAttribute('aria-label') || e.getAttribute('placeholder') || e.getAttribute('name') || e.getAttribute('id') || '';
  const inputs=[...document.querySelectorAll('input,select,textarea')].filter(e=>visible(e) && !excluded(e) && !/password|hidden|file/i.test(e.type || '') && !/password|passwd|token|session|auth/i.test(`${e.name || ''} ${e.autocomplete || ''}`)).map(e=>({label:label(e),type:e.type || e.tagName,value:String(e.value || ''),checked:['radio','checkbox'].includes(e.type)?!!e.checked:null}));
  const selected=[...document.querySelectorAll('.ant-select-selection-item,.ant-tag,.ant-checkbox-wrapper-checked,.ant-radio-wrapper-checked,.ant-radio-button-wrapper-checked,[aria-checked="true"],[aria-selected="true"],[aria-pressed="true"],.text-blue-600')].filter(e=>visible(e) && !excluded(e) && text(e).length<=300).map(e=>({text:text(e),label:label(e),value:e.getAttribute('data-value') || e.getAttribute('value') || ''}));
  const chips=[];
  for(const e of document.querySelectorAll('span,div,label')){
    if(!visible(e) || excluded(e) || !/^已选\s*[:：]?$/.test(text(e)))continue;
    let root=e.parentElement;
    for(let depth=0;root && depth<5;depth++,root=root.parentElement){
      if(root.querySelector('.shadow-common-light.bg-white'))break;
      if(!/清空/.test(text(root)))continue;
      chips.push({text:text(root),images:[...root.querySelectorAll('img')].filter(visible).map(img=>{let path='';try{path=new URL(img.currentSrc || img.src,location.href).pathname;}catch{}return {alt:img.alt || '',title:img.title || '',path};})});break;
    }
  }
  const url=new URL(location.href);
  const query=[...url.searchParams].filter(([k])=>! /^(page|pageIndex|pageNum|page_number|current|offset)$/i.test(k) && !/token|password|session|authorization/i.test(k)).sort((a,b)=>JSON.stringify(a).localeCompare(JSON.stringify(b)));
  const sort=rows=>rows.sort((a,b)=>JSON.stringify(a).localeCompare(JSON.stringify(b)));
  return {version:1,location:url.origin+url.pathname,query,hash:url.hash,inputs:sort(inputs),selected:sort(selected),chips:sort(chips)};
}
// This function is serialized into the page. Read visible evidence only, scoped
// to each real card or to the identity-checked detail. Never borrow search filters.
function readMetadataEvidence(expected = null) {
  const visible = e => !!e?.getClientRects().length && getComputedStyle(e).visibility !== 'hidden';
  const evidence = (root, adId, source) => {
    // Observed list header: App's clickable name precedes the store icon and
    // developer. Scope to that header, never to the ad copy or first text line.
    const headers=[...root.children].filter(e=>e.classList.contains('p-3') && e.classList.contains('pb-0') && visible(e));
    const header=source==='visible_card' && headers.length===1?headers[0]:null;
    const appNames=header?[...header.querySelectorAll('span.cursor-pointer')].filter(visible).map(e=>e.innerText.trim()).filter(Boolean):[];
    const platforms=header?[...header.querySelectorAll('img[alt="Google Play"],img[alt="App Store"]')].filter(visible).map(e=>({'Google Play':'Android','App Store':'iOS'})[e.alt]):[];
    return {
    adId, source, text: String(root.innerText || '').slice(0, 16000),
    appName:appNames.length===1?appNames[0]:null,platform:[...new Set(platforms)].length===1?platforms[0]:null,
    links: [...root.querySelectorAll('a[href]')].filter(visible).map(a => ({href:a.href,text:a.innerText || a.getAttribute('title') || ''})),
    labels: [...root.querySelectorAll('.ant-tag')].filter(visible).map(e=>e.innerText.trim()).filter(t=>t && t.length<80)
  };};
  if (expected) {
    const ids = [...document.querySelectorAll('a[href]')].filter(visible).filter(a=>{
      try{return new URL(a.href).pathname==='/modules/creative/display-ads/detail' && a.textContent.includes('单页打开');}catch{return false;}
    });
    if(ids.length!==1 || new URL(ids[0].href).searchParams.get('id')!==expected.adId)return {failure:'DETAIL_ID_MISMATCH'};
    let root=ids[0].closest('[role="dialog"],.ant-modal-content,.ant-drawer-content');
    if(!root)for(let el=ids[0].parentElement,depth=0;el && el!==document.body && depth<9;el=el.parentElement,depth++){
      if([...el.querySelectorAll('button')].some(b=>visible(b) && /^(Close|关闭)$/i.test(b.getAttribute('aria-label') || b.textContent.trim()))){root=el;break;}
    }
    return root?{rows:[evidence(root,expected.adId,'visible_detail')]}:{failure:'DETAIL_NOT_OPENED'};
  }
  const cards=[...document.querySelectorAll('.shadow-common-light.bg-white')].filter(visible).sort((a,b)=>{
    const ar=a.getBoundingClientRect(),br=b.getBoundingClientRect();return Math.abs(ar.top-br.top)<4?ar.left-br.left:ar.top-br.top;
  });
  return {rows:cards.map((card,index)=>{
    const key=Object.keys(card).find(k=>k.startsWith('__reactFiber$'));let adId=null;
    for(let f=key && card[key],depth=0;f && depth<8;f=f.return,depth++)if(typeof f.memoizedProps?.value?.ad_key==='string'){adId=f.memoizedProps.value.ad_key;break;}
    return {...evidence(card,adId,'visible_card'),cardIndex:index+1};
  })};
}

const keys=['appName','appId','platform','displayEstimate','relaunch'];
const clean=v=>String(v ?? '').replace(/(?:https?|blob):[^\s<>"']+/gi,'[链接已省略]').replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/g,'').trim().slice(0,300);
const names={appName:'App 名称',appId:'App ID',platform:'平台',displayEstimate:'展示估值',relaunch:'重投标签'};
const metadataNames=names;
function parseMetadata(e) {
  const source=e.source==='visible_detail'?'visible_detail':'visible_card';
  const fields=Object.fromEntries(keys.map(k=>[k,null])),warnings=[];
  const field=value=>value===null?null:{value:clean(value),source};
  const text=String(e.text || ''),stores=[];
  for(const link of e.links || [])try{
    const u=new URL(link.href);if(!['https:','http:'].includes(u.protocol) || u.username || u.password)continue;
    let id,platform;
    if(u.hostname==='play.google.com' && u.pathname==='/store/apps/details'){id=u.searchParams.get('id');platform='Android';}
    else if(['apps.apple.com','itunes.apple.com'].includes(u.hostname)){id=u.pathname.match(/\/id(\d+)(?:\/|$)/)?.[1];platform='iOS';}
    else if((u.hostname==='guangdada.net' || u.hostname.endsWith('.guangdada.net')) && /\/advertiser(?:\/|$)/.test(u.pathname)){
      id=u.searchParams.get('store');platform=({'1':'iOS','2':'Android'})[u.searchParams.get('os')];
    }
    if(!id || !platform || !/^[\w.-]{1,200}$/.test(id))continue;
    const label=clean(link.text);
    stores.push({id,platform,name:label && !/^(?:Install|安装|下载|Google Play|App Store|查看|打开|商店|去商店|View in.*|Download.*)$/i.test(label) && !label.includes('[链接已省略]')?label:null});
  }catch{/* Invalid visible links do not establish App identity. */}
  const identities=[...new Set(stores.map(s=>`${s.platform}:${s.id}`))];
  if(identities.length===1){
    fields.appId=field(stores[0].id);fields.platform=field(stores[0].platform);
    const appNames=[...new Set(stores.map(s=>s.name).filter(Boolean))];
    if(appNames.length===1)fields.appName=field(appNames[0]);
  }else if(identities.length>1)warnings.push('App 归属有多个商店身份，未猜测');
  if(identities.length<=1){
    if(e.appName && !fields.appName)fields.appName=field(e.appName);
    if(['Android','iOS'].includes(e.platform) && !fields.platform)fields.platform=field(e.platform);
    else if(e.platform && fields.platform && e.platform!==fields.platform.value){fields.platform=null;fields.appId=null;warnings.push('商店图标与链接平台不一致，未猜测');}
  }
  // These are explicit site labels. An arbitrary first line may be ad copy.
  const named=text.match(/(?:^|\n)\s*(?:App\s*名称|应用名称)\s*[:：]?\s*\n?([^\n]+)/i)?.[1] || (source==='visible_detail'?text.match(/受众画像\s*\n([^\n]+)\n当页查看创意/)?.[1]:null);
  if(named && !fields.appName && identities.length<=1)fields.appName=field(named);
  const estimates=[...text.matchAll(/展示估值\s*[:：]?\s*((?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?\s*[KkMm万亿]?)(?![\d.,A-Za-z万亿]|\s*[-~～至])/g)].map(m=>m[1].trim());
  if(new Set(estimates).size===1){
    const raw=estimates[0],m=raw.match(/^([\d,.]+)\s*([KkMm万亿]?)$/),unit=m[2],value=Number(m[1].replaceAll(',',''))*({'k':1e3,'m':1e6,'万':1e4,'亿':1e8}[unit.toLowerCase()] || 1);
    if(Number.isFinite(value) && value>=0)fields.displayEstimate={raw:clean(raw),value,precision:unit?'rounded':'visible_number',source};
  }else if(estimates.length)warnings.push('展示估值出现多个不同值，未猜测');
  const labels=new Set((e.labels || []).map(t=>String(t).trim()));
  const positive=labels.has('重投') || /(?:^|\n)\s*重投\s+(?=\d{4}-\d{2}-\d{2})/.test(text) || /(?:^|\n)\s*重投标签\s*[:：]?\s*(?:是|重投)(?:\n|$)/.test(text);
  const negative=labels.has('非重投') || labels.has('未重投') || /(?:^|\n)\s*重投标签\s*[:：]?\s*(?:否|非重投|未重投)(?:\n|$)/.test(text);
  if(positive!==negative)fields.relaunch={value:positive,raw:positive?'重投':'非重投（页面明确标注）',source};
  if(positive && negative)warnings.push('重投标签互相冲突，未猜测');
  return {schemaVersion:1,adId:clean(e.adId),fields,warnings,missingFields:keys.filter(k=>!fields[k])};
}

// Merge only the same creative. Missing later snapshots cannot erase captured
// values; a genuine contradiction stays explicit instead of silently winning.
function mergeMetadata(old, fresh) {
  if(!old)return fresh;
  if(!fresh || old.adId!==fresh.adId)return old;
  const fields={...old.fields},warnings=[...new Set([...(old.warnings || []),...(fresh.warnings || [])])];
  const conflicts=new Set([...(old.conflicts || []),...(fresh.conflicts || [])]);
  for(const key of keys){
    const a=fields[key],b=fresh.fields[key];if(!b || conflicts.has(key))continue;
    // Rounded and unrounded estimates may legitimately differ on two views.
    if(a && a.value!==b.value && !(key==='displayEstimate' && (a.precision==='rounded' || b.precision==='rounded'))){
      fields[key]=null;conflicts.add(key);warnings.push(`${names[key]}前后不一致，未猜测`);
    }else if(!a || b.source==='visible_detail' || (key==='displayEstimate' && a.precision==='rounded' && b.precision==='visible_number'))fields[key]=b;
  }
  if(conflicts.has('appId') || conflicts.has('platform'))for(const k of ['appName','appId','platform']){fields[k]=null;conflicts.add(k);}
  return {...old,fields,warnings:[...new Set(warnings)],conflicts:[...conflicts],missingFields:keys.filter(k=>!fields[k]),detailChecked:old.detailChecked || fresh.detailChecked,detailFailure:fresh.detailFailure || old.detailFailure};
}
function exportMetadata(metadata, adId) {
  if(!metadata || metadata.adId!==adId)return null;
  const fields={};
  for(const key of keys){
    const f=metadata.fields?.[key];fields[key]=null;if(!f)continue;
    const source=['visible_card','visible_detail'].includes(f.source)?f.source:null;if(!source)continue;
    if(key==='relaunch'){if(typeof f.value==='boolean')fields[key]={value:f.value,raw:clean(f.raw),source};}
    else if(key==='displayEstimate'){if(Number.isFinite(f.value) && f.value>=0)fields[key]={value:f.value,raw:clean(f.raw),precision:f.precision==='rounded'?'rounded':'visible_number',source};}
    else fields[key]={value:clean(f.value),source};
  }
  return {schemaVersion:1,adId:clean(adId),fields,missingFields:keys.filter(k=>!fields[k]),warnings:(metadata.warnings || []).map(clean),detailFailure:clean(metadata.detailFailure)};
}

if(method==='scan'){ const s=await collectMedia(false);const e=readMetadataEvidence();for(const item of [...s.items,...s.missing]){const row=e.rows?.find(r=>r.adId===item.adId&&r.cardIndex===item.cardIndex);if(row)item.metadata=parseMetadata(row);}return s; }
const functions={access:pageAccess,context:collectionContext,pagination:pageControl,detail:detailControl};
if(!functions[method])throw new Error('Unknown observation');return functions[method](...args);
}
