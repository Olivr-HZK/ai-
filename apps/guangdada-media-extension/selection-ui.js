// ISOLATED world: page JavaScript cannot call the privileged cart handlers.
(() => {
  if(globalThis.__gddSelectionUI){globalThis.__gddSelectionUI.refresh();return;}
  const toolbarId='gdd-download-selection-toolbar';
  let disposed=false,pending=0,polling=false,timer=null,observer,latest=null,note='',noteUntil=0;
  const $=selector=>document.querySelector(selector);
  const cards=()=>[...document.querySelectorAll('.shadow-common-light.bg-white')].filter(c=>c.getClientRects().length).sort((a,b)=>{
    const ar=a.getBoundingClientRect(),br=b.getBoundingClientRect();return Math.abs(ar.top-br.top)<4?ar.left-br.left:ar.top-br.top;
  });
  function clean(){document.querySelectorAll('[data-gdd-download-number], #'+toolbarId).forEach(el=>el.remove());}
  function dispose(){disposed=true;clearTimeout(timer);clearInterval(interval);observer?.disconnect();document.removeEventListener('click',blockNavigation,true);clean();delete globalThis.__gddSelectionUI;}
  async function send(type,extra={}){
    const response=await chrome.runtime.sendMessage({type,...extra});
    if(!response?.ok)throw new Error(response?.error || '插件未响应');return response;
  }
  function label(){
    const el=$('#'+toolbarId+' [data-gdd-selection-count]');
    if(el && latest)el.textContent=note || `第${latest.pageNumber}页选中 ${latest.selectedOnPage} 张 · 合计 ${latest.total} 张 / ${latest.pages.length} 页 · 可直接翻页继续选`;
  }
  async function update(action,cardIndex){
    if(pending || !latest)return;
    pending++;note='正在保存选择…';noteUntil=0;label();
    document.querySelectorAll('[data-gdd-download-number], #'+toolbarId+' button').forEach(b=>b.disabled=true);
    try{
      await send('selection-update',{action,cardIndex,expectedCardKeys:latest.cardKeys,expectedPage:latest.pageNumber});
      note='';
    }catch(error){note=error.message;noteUntil=Date.now()+2500;}
    finally{pending--;await refresh();}
  }
  function blockNavigation(e){
    if(pending && e.target.closest?.('.ant-pagination')){e.preventDefault();e.stopImmediatePropagation();note='正在保存勾选，请稍后翻页。';label();}
  }
  function render(data){
    latest=data;
    const list=cards();
    if(list.length!==data.cardKeys.length)return;
    const picked=new Set(data.selectedAdIds);
    for(const [index,card] of list.entries()){
      let badge=card.querySelector('[data-gdd-download-number]');
      if(!badge){
        badge=document.createElement('button');badge.type='button';badge.setAttribute('data-gdd-download-number','');
        badge.style.cssText='position:absolute;top:4px;left:4px;z-index:30;border:2px solid #2563eb;border-radius:6px;padding:4px 9px;font:bold 16px/22px system-ui;cursor:pointer;box-shadow:0 1px 4px #0005';
        for(const type of ['pointerdown','mousedown','dblclick'])badge.addEventListener(type,e=>e.stopPropagation());
        badge.addEventListener('click',e=>{e.preventDefault();e.stopPropagation();void update('toggle',Number(badge.dataset.number));});
        card.append(badge);
      }
      const adId=data.cardKeys[index],checked=picked.has(adId);
      badge.dataset.adId=adId || '';badge.dataset.number=String(index+1);
      badge.setAttribute('aria-label',`选择素材 ${index+1}`);badge.setAttribute('aria-pressed',String(checked));
      const text=`${checked?'☑':'☐'} ${index+1}`;if(badge.textContent!==text)badge.textContent=text;
      badge.style.background=checked?'#2563eb':'#ffffff';badge.style.color=checked?'#ffffff':'#17243a';badge.disabled=!!pending || !adId;
    }
    let toolbar=document.getElementById(toolbarId);
    if(!toolbar){
      toolbar=document.createElement('div');toolbar.id=toolbarId;
      toolbar.style.cssText='position:fixed;bottom:24px;left:50%;transform:translateX(-50%);z-index:2147483647;display:flex;flex-wrap:wrap;align-items:center;gap:10px;padding:12px 18px;max-width:95vw;background:#142b4d;color:white;border-radius:12px;box-shadow:0 4px 24px #0004;font:14px/22px system-ui';
      const count=document.createElement('span');count.setAttribute('data-gdd-selection-count','');toolbar.append(count);
      for(const [text,action] of [['全选本页','all'],['清空本页','none'],['清空全部','clear'],['收起点选','hide']]){
        const button=document.createElement('button');button.type='button';button.textContent=text;
        button.style.cssText='padding:5px 10px;border:0;border-radius:6px;background:white;color:#142b4d;font:14px/22px system-ui;cursor:pointer';
        button.addEventListener('click',e=>{e.preventDefault();e.stopPropagation();void update(action);});toolbar.append(button);
      }
      document.body.append(toolbar);
    }
    toolbar.querySelectorAll('button').forEach(b=>b.disabled=!!pending);label();
  }
  async function refresh(){
    if(disposed || polling)return;
    polling=true;
    try{
      const result=await send('selection-read');
      if(!result.active){dispose();return;}
      if(result.paused){clean();return;}
      if(!pending){if(Date.now()>=noteUntil)note='';render(result);}
    }catch(error){
      if(/context invalidated/i.test(error.message)){dispose();return;}
      note=error.message;label();
    }
    finally{polling=false;}
  }
  // Old MAIN-world badges have old event handlers. Replace them once on entry.
  clean();
  observer=new MutationObserver(records=>{
    if(records.every(r=>r.target.closest?.('#'+toolbarId+', [data-gdd-download-number]') || (r.addedNodes.length && [...r.addedNodes].every(n=>n.nodeType===1 && (n.id===toolbarId || n.hasAttribute('data-gdd-download-number'))))))return;
    clearTimeout(timer);timer=setTimeout(refresh,180);
  });
  observer.observe(document.body,{subtree:true,childList:true});
  document.addEventListener('click',blockNavigation,true);
  const interval=setInterval(refresh,1000);
  globalThis.__gddSelectionUI={refresh,dispose};void refresh();
})();
