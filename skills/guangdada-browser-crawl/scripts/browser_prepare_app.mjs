// Run in the authorized browser REPL. UI and state CLI bridges are supplied by
// the caller; this module never opens a browser or reads site internals.
import {gddCheckedClick} from './browser_check.mjs';
export async function gddPrepareApp(o){
 const {tab,task,scopeSha256,persist,sha256,runStage}=o;
 const required=['persist','sha256','runStage','readCategory','repairCategory','readFilters','repairFilters','selectSearchMode','readQuery','readSubmitted','clickSort','readSort'];
 for(const k of required)if(typeof o[k]!=='function')throw Error('Missing preparation adapter: '+k);
 const prefix='prepare-'+Date.now(),outputs={};
 const common={tab,scopeSha256,taskId:task.id,persist,sha256};
 const screenshot=async(name)=>{const b=await tab.getScreenshot({emit:false});
  const ext=b[0]===137?'png':b[0]===255?'jpg':null;if(!ext)throw Error('Invalid screenshot');
  const path='evidence/'+prefix+'-'+name+'.'+ext;await persist(path,b);return {path:'tasks/'+task.id+'/'+path,sha256:await sha256(b)};};
 const normalize=v=>Array.isArray(v)?v.map(normalize):v&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,normalize(v[k])])):v;
 const equal=(a,b)=>JSON.stringify(normalize(a))===JSON.stringify(normalize(b));
 const filters=async(stage,read,repair,expected)=>runStage(stage,async()=>{
  const before=await read(),a=await screenshot(stage+'-before');
  if(!equal(before,expected))await repair(before,expected);
  const after=await read(),b=await screenshot(stage+'-after');
  const path='evidence/'+prefix+'-'+stage+'.json';
  await persist(path,JSON.stringify({scope_sha256:scopeSha256,task_id:task.id,observed_at:new Date().toISOString(),before,after,expected,action:equal(before,expected)?'readonly':'repair',images:{before:a,after:b}},null,2));
  if(!equal(after,expected))throw Error('Preparation filter readback mismatch: '+stage);
  return path;
 });
 const from=o.from||'CATEGORY_PENDING';const order=['CATEGORY_PENDING','FILTERS_PENDING','APP_PENDING','SORT_PENDING'];
 if(!order.includes(from))throw Error('Unsupported preparation resume state');
 const wants=s=>order.indexOf(s)>=order.indexOf(from);
 try{
  if(wants('CATEGORY_PENDING'))await filters('CATEGORY_PENDING',o.readCategory,o.repairCategory,{category:'工具'});
  if(wants('FILTERS_PENDING'))await filters('FILTERS_PENDING',o.readFilters,o.repairFilters,o.expectedFilters);
  if(wants('APP_PENDING'))await runStage('APP_PENDING',async()=>{
   if(o.clearApp)await o.clearApp();
   if(!o.queryInput||typeof o.queryInput.fill!=='function'||typeof o.queryInput.press!=='function')throw Error('Missing actual query input locator');
   await o.selectSearchMode('综合');
   const submissionExpected={search_mode:'综合',query_terms:[task.app_id],selected_app_ids:[],residual_keywords:[]};
   const q=await gddCheckedClick({...common,eventId:prefix+'-query',key:'app.query',expected:task.app_id,
    click:async()=>{await o.queryInput.fill(task.app_id);await o.queryInput.press('Enter');},read:o.readQuery,
    persist:async(path,bytes)=>{if(path.endsWith('.json')){const e=JSON.parse(bytes);e.interaction={action:'press',target:'Enter'};e.submission=await o.readSubmitted();bytes=JSON.stringify(e,null,2);}await persist(path,bytes);}});
   const submitted=await o.readSubmitted();
   if(!equal(submitted,submissionExpected))throw Error('Submitted comprehensive query mismatch');
   const actual={app_name:task.app_name,platform:task.platform,app_id:task.app_id,...submitted};
   const path='evidence/'+prefix+'-app.json';
   await persist(path,JSON.stringify({...actual,scope_sha256:scopeSha256,observed_at:new Date().toISOString(),query_type:'app_id',query_app_id:task.app_id,query_ui_check:q.event_file,ui_check:q.event_file},null,2));return path;
  });
  if(wants('SORT_PENDING'))await runStage('SORT_PENDING',async()=>{
   const e=await gddCheckedClick({...common,eventId:prefix+'-sort',key:'filter.sort',expected:'最新创意',click:o.clickSort,read:o.readSort});return e.event_file;
  });
  return {status:'ready_for_probe',task_id:task.id,outputs};
 }catch(e){return {status:'needs_attention',task_id:task.id,error:String(e.message||e)};}
}
