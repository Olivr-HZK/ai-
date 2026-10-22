// Isolated UI fixture, never attaches to a browser or network.
import fs from 'node:fs/promises';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {createDirectSession} from '../skills/guangdada-browser-crawl/scripts/direct_session.mjs';
const runDir=process.argv[2],projectRoot=process.argv[3],exec=promisify(execFile);
const scope=JSON.parse(await fs.readFile(path.join(runDir,'scope.json'),'utf8'));
const png=await fs.readFile(path.join(runDir,'fixture.png'));
let open=false,currentTask=null,displayedTask=null,pos=1,page=1,query=[],sort=null,category='工具',searches=0,repairs=0,captcha=false,captchaDone=false;
const filters={dedupe:'素材',date:{end:scope.target_date,start:scope.target_date},media:'图片&视频',selected_types:[],channels:[],regions:'',languages:'',size:'',search_mode:'综合',selected_app_ids:[]};
const zero=t=>t.id==='android-10';
const raw=(t,n)=>{
 const type=t.id==='ios-01'&&n===1?'试玩':scope.tasks.indexOf(t)%3===0?'视频':'图片';
 const ad=createHash('md5').update(t.id+'/'+n).digest('hex');
 const url=t.platform==='iOS'?'https://apps.apple.com/us/app/id'+t.app_id:'https://play.google.com/store/apps/details?id='+t.app_id;
 return {raw_text:'创意\n受众画像\n'+t.app_name+'\n当页查看创意\n'+(type==='试玩'?'试玩广告\n':'')+(t.id==='ios-02'&&n===1?'重投\n':'')+(n===1?scope.target_date:'2020-01-01')+'~'+scope.target_date+'\n投放渠道\nFacebook\n文案语言\n英语\n地区\n日本\n韩国\n(2)\n素材尺寸\n竖版\n32 x 48\n展示估值\n'+(1000+scope.tasks.indexOf(t)),observed_app_name:t.app_name,observed_platform:t.platform,
 links:[{text:'单页打开',href:'https://guangdada.net/modules/creative/display-ads/detail?id='+ad},{text:'Download',href:url}],
 observed_videos:type==='视频'?[{src:'https://zingfront.com/sp_opera/'+ad+'.mp4',poster:''}]:[],
 observed_images:type==='图片'?[{src:'https://zingfront.com/sp_opera/'+ad+'.png',cls:'ant-image-img',is_original:true}]:[],
 observed_iframes:type==='试玩'?[{src:'https://example.org/fixture.html'}]:[]};
};
const tab={screenshot:async()=>png,playwright:{domSnapshot:async()=>{await new Promise(r=>setTimeout(r,5));return 'isolated asynchronous fixture';},getByText:(text)=>({click:async()=>{if(text==='最新创意')sort=text;else category=text;}}),getByRole:(role,o)=>({
 click:async()=>{if(o.name==='Close')open=false;else if(o.name==='嘹 清空')query=[];else if(o.name==='下一页')page++;else if(o.name?.startsWith('页 '))page=Number(o.name.slice(2));},waitFor:async()=>{if(role==='dialog'&&((o?.state==='hidden'&&open)||(o?.state==='visible'&&!open)))throw Error('Fixture dialog state');}
 })}};
const cli=async(module,args)=>{
 if(module==='ua_workflows.browser_collection_runtime')return {active:true};
 const r=await exec(path.join(projectRoot,'.venv/bin/python'),['-m',module,...args,'--run-dir',runDir],{cwd:projectRoot,maxBuffer:2e6});return JSON.parse(r.stdout);
};
for(const task of scope.tasks){
 currentTask=task;sort=null;open=false;page=1;
 const session=await createDirectSession({tab,runDir,taskId:task.id,projectRoot,local:cli,ui:{
  observe:async()=>({...filters,category,query_terms:query,sort,page,rows:!displayedTask||zero(displayedTask)?[]:displayedTask.id==='ios-04'?[displayedTask.id+'-'+page]:[displayedTask.id+'-1',displayedTask.id+'-2'],has_next:displayedTask?.id==='ios-04'&&page===1,loading:false,explicit_no_results:displayedTask?zero(displayedTask):false,raw_text:displayedTask&&zero(displayedTask)?'暂无数据':'fixture page',captcha,login_required:false,detail_open:open}),
  repairFilters:async(before,expected)=>{repairs++;category=expected.category;},
  queryInput:{fill:async v=>{pos=v;},press:async key=>{if(key!=='Enter')throw Error('Expected Enter');query=[pos];displayedTask=currentTask;searches++;}},
  readDetail:async()=>raw(currentTask,page===2?2:pos),openCard:async n=>{pos=n;open=true;if(task.id==='ios-03'&&n===2&&!captchaDone){captcha=true;captchaDone=true;}}
 }});
 const first=await session.start();if(!['ready','app_accepted'].includes(first.status))throw Error(JSON.stringify(first));
 if(first.status==='ready'){let r=await session.batch();if(r.status==='WAITING_HUMAN'){captcha=false;const resumed=await session.resume('Isolated fixture human clearance');if(resumed.status!=='ready')throw Error(JSON.stringify(resumed));r=await session.batch();}if(r.status==='page_end'){const next=await session.nextPage();if(next.status!=='ready')throw Error(JSON.stringify(next));r=await session.batch();}if(r.status!=='app_accepted')throw Error(JSON.stringify(r));}
}
console.log(JSON.stringify({searches,repairs,tasks:scope.tasks.length}));
