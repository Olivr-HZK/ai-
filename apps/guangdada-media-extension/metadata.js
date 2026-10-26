// This function is serialized into the page. Read visible evidence only, scoped
// to each real card or to the identity-checked detail. Never borrow search filters.
export function readMetadataEvidence(expected = null) {
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
export const metadataNames=names;
export function parseMetadata(e) {
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
export function mergeMetadata(old, fresh) {
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
export function exportMetadata(metadata, adId) {
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
