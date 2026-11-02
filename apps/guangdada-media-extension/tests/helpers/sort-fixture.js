import {parseHTML} from 'linkedom';
import vm from 'node:vm';
import {collectMedia} from '../../collector.js';

// Real HTML parsing, with deterministic layout/style observations. No browser,
// site, network, clicks or media downloads; execute the serialized production
// collector, so accidental references to imported helpers fail here too.
export async function sortFixtureScan(html,{collector=collectMedia.toString(),date='2026-11-02',adId='sort-fixture-ad'}={}){
  const {document,HTMLElement}=parseHTML(`<!doctype html><html><body>${html}<div class="shadow-common-light bg-white" id="fixture-card">${date} ~ ${date}</div><ul class="ant-pagination"><li class="ant-pagination-item-active" title="1">1</li></ul></body></html>`);
  HTMLElement.prototype.getClientRects=function(){return this.style.display==='none'?[]:[{x:0,y:0,width:100,height:24}];};
  HTMLElement.prototype.getBoundingClientRect=function(){return {x:0,y:0,left:0,top:0,width:100,height:24};};
  HTMLElement.prototype.click=()=>{throw new Error('Sort observation must not click');};
  HTMLElement.prototype.scrollIntoView=()=>{throw new Error('Sort observation must not scroll');};
  // linkedom does not implement form selection state or computed rendering.
  for(const input of document.querySelectorAll('input'))input.checked=input.hasAttribute('checked');
  for(const select of document.querySelectorAll('select')){
    const options=[...select.querySelectorAll('option')];
    const selected=options.map(o=>o.hasAttribute('selected'));
    for(const [i,option] of options.entries())Object.defineProperty(option,'selected',{value:selected[i],writable:true});
    if(!options.some(o=>o.selected) && !select.hasAttribute('multiple') && options[0])options[0].selected=true;
  }
  const card=document.getElementById('fixture-card');
  card.__reactFiber$fixture={memoizedProps:{value:{ad_key:adId,ads_type:1,resource_urls:[{image_url:`https://cdn.example/${adId}.png`}]}}};
  const getComputedStyle=el=>{
    let color='rgb(23, 36, 58)',visibility='visible';
    for(let p=el;p;p=p.parentElement){
      if(p.style.color){color=p.style.color;break;}
    }
    for(let p=el;p;p=p.parentElement){if(p.style.visibility){visibility=p.style.visibility;break;}}
    return {display:el.style.display || 'block',visibility,color,backgroundColor:el.style.backgroundColor || 'rgba(0, 0, 0, 0)'};
  };
  const context={document,getComputedStyle,URL,console,fetch:()=>{throw new Error('Offline sort fixture must not fetch');}};
  return vm.runInNewContext(`(${collector})(false)`,context);
}
