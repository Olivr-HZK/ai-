import {buildReport,reportMarkdown} from './report.js';
let markdown='',stamp='';
let taskId=new URL(location.href).searchParams.get('task');
const refresh=async()=>{
  const saved=await chrome.storage.local.get(taskId?'mediaTask:'+taskId:'job');
  const job=taskId?saved['mediaTask:'+taskId]:saved.job;
  if(job && !taskId){taskId=job.id;history.replaceState(null,'','?task='+encodeURIComponent(taskId));}
  markdown=job?reportMarkdown(buildReport(job)):'';stamp=job?.createdAt || '最近一批';
  document.getElementById('status').textContent=job?(job.taskName || '原任务')+' · 此任务报告随进度更新':'暂无任务';
  document.getElementById('report').textContent=markdown || '暂无下载任务。提交下载后，这里会逐项记录所选内容和结果。';
  document.getElementById('save').disabled=!job;
};
document.getElementById('save').onclick=async()=>{
  const url=URL.createObjectURL(new Blob([markdown],{type:'text/markdown;charset=utf-8'}));
  try{
    await chrome.downloads.download({url,filename:`广大大/下载报告_${stamp.replace(/[:.]/g,'-')}.md`,conflictAction:'uniquify',saveAs:false});
    document.getElementById('status').textContent='已提交保存报告，请在 Chrome 下载中确认完成。';
    setTimeout(()=>URL.revokeObjectURL(url),60000);
  }catch(error){URL.revokeObjectURL(url);document.getElementById('status').textContent=error.message;}
};
chrome.storage.onChanged.addListener((changes,area)=>{if(area==='local' && (taskId?changes['mediaTask:'+taskId]:changes.job))void refresh();});
await refresh();
