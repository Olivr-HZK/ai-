import {makeZip} from './zip.js';
import {buildReport,reportMarkdown,failureDetail} from './report.js';
import {inspectPlayable} from './playable.js';
let current = null;
const LIMIT = 512 * 1024 * 1024;
const failure=(code,status)=>Object.assign(new Error(failureDetail(code,status).reason),{detail:failureDetail(code,status),httpStatus:status || null});
const send = async data => {
  const result = await chrome.runtime.sendMessage({target: 'background', ...data});
  if (!result?.ok) throw new Error(result?.error || '后台未响应');
};
async function run(job) {
  const state = {id: job.id, controllers: new Set(), bytes: 0, stopped: false, url: null};
  current = state;
  const entries = [];
  let cursor = 0;
  async function consume() {
    while (!state.stopped) {
      const index = cursor++;
      if (index >= job.items.length) return;
      const item = job.items[index];
      const controller = new AbortController(); state.controllers.add(controller);
      const timer = setTimeout(() => controller.abort(failure('TIMEOUT')), 120000);
      let reserved = 0, responseStatus = null;
      try {
        await send({type: 'item', id: job.id, index, state: 'active'});
        item.state='active';
        const response = await fetch(item.url, {credentials: 'include', signal: controller.signal});
        responseStatus=response.status;
        if (!response.ok) throw failure('HTTP',response.status);
        const contentType = response.headers.get('content-type') || '';
        if (/application\/json/i.test(contentType) || (item.kind!=='playable' && /text\/html/i.test(contentType))) throw failure(item.kind==='playable'?'PLAYABLE_NOT_HTML':'NOT_MEDIA');
        if (!response.body) throw failure('EMPTY_RESPONSE');
        const reader = response.body.getReader(), chunks = [];
        while (true) {
          const {value, done} = await reader.read();
          if (done) break;
          if (state.bytes + value.length > LIMIT) { controller.abort(failure('BATCH_LIMIT')); throw controller.signal.reason; }
          state.bytes += value.length; reserved += value.length; chunks.push(value);
        }
        if (!reserved) throw failure('EMPTY_RESPONSE');
        const bytes = new Uint8Array(reserved); let at = 0;
        for (const chunk of chunks) { bytes.set(chunk, at); at += chunk.length; }
        if(item.kind==='playable'){
          const info=inspectPlayable(bytes,response.url);
          if(info.errorCode)throw failure(info.errorCode);
          item.playableInfo=info;
        }
        if (state.stopped) return;
        entries.push({name: item.filename, bytes});
        Object.assign(item,{state:'complete',bytes:reserved});
        await send({type: 'item', id: job.id, index, state: 'complete',bytes:reserved,playableInfo:item.playableInfo || null});
      } catch (error) {
        state.bytes -= reserved;
        const detail=error.detail || controller.signal.reason?.detail || failureDetail(responseStatus?'TRANSFER_INTERRUPTED':'NETWORK');
        const result={state:'failed',error:detail.reason,errorCode:detail.code,httpStatus:error.httpStatus || responseStatus,suggestion:detail.suggestion,bytes:0};
        Object.assign(item,result);
        if (!state.stopped) await send({type: 'item', id: job.id, index,...result});
      } finally { clearTimeout(timer); state.controllers.delete(controller); }
    }
  }
  try {
    await Promise.all([consume(), consume(), consume()]);
    if (state.stopped) return;
    if (!entries.length) throw new Error('所有媒体获取失败，未生成 ZIP。请点击「查看下载报告.md」查看或保存报告。');
    await send({type: 'packing', id: job.id});
    const report = buildReport({...job,phase:'packaged'});
    entries.sort((a, b) => a.name.localeCompare(b.name));
    entries.push({name: '下载报告.md', bytes: new TextEncoder().encode(reportMarkdown(report))});
    const zip = makeZip(entries);
    entries.length = 0;
    state.url = URL.createObjectURL(zip);
    await send({type: 'zip-ready', id: job.id, url: state.url});
  } catch (error) {
    if (!state.stopped) await send({type: 'pack-error', id: job.id, error: error.message}).catch(() => {});
  }
}
chrome.runtime.onMessage.addListener((message, sender, respond) => {
  if (sender.id !== chrome.runtime.id || message.target !== 'offscreen') return;
  if (message.type === 'run') {
    if (current) { respond({ok: false, error: '已有打包任务'}); return; }
    respond({ok: true}); void run(message.job); return;
  }
  if (message.type === 'stop' && current) {
    current.stopped = true;
    for (const c of current.controllers) c.abort();
  }
  if (message.type === 'release' && current?.url) URL.revokeObjectURL(current.url);
  respond({ok: true});
});
