// The current display-ads page renders React cards. Read only each card's
// bound value; never the parent list/store or whole-document resource history.
export async function collectMedia(showNumbers = false) {
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
