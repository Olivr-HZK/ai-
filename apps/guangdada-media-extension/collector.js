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
  const items = [], missing = [], cardKeys = [];
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
  return {items, missing, cardKeys, selectionActive, selectedCardNumbers, cardCount:cards.length, matchedCards, videoCards, imageCards, playableCards, unsupported, limitReached:false};
}
