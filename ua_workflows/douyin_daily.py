"""Daily Douyin keyword sampling, likes top decile, verified media, Feishu and report."""
import argparse
from datetime import datetime, timedelta
import json
import math
from pathlib import Path
import shutil
import subprocess
import sys
from urllib.parse import urlsplit

from ua_workflows import xhs_daily as common
from ua_workflows.social_chat_browser_daily import text_field
from ua_workflows.collection_runtime import keep_awake, post_json

ROOT=Path(__file__).resolve().parents[1]
CONFIG=ROOT/'config/social_chat_douyin_daily.json'
TZ=common.TZ
load,save,sha=common.load,common.save,common.sha


def select(base,config):
    base=Path(base);summary=load(base/'summary.json')
    if [t['keyword'] for t in summary['keywords']]!=config['keywords']:raise ValueError('Missing keyword search')
    notes={};raw=0;excluded=[]
    for task in summary['keywords']:
        if task.get('status') not in ('search_verified','response_needs_review'):raise ValueError('A keyword search is incomplete')
        folder=Path(task['directory']);request=load(folder/'request.json');response=load(folder/'response.json')
        expected={'platform':'douyin','action':'search','params':{'keyword':task['keyword'],'publish_time':'one_day','sort':'newest','limit':50}}
        if request['body']!=expected or response.get('platform')!='douyin' or response.get('action')!='search':raise ValueError('Search scope mismatch')
        rows=response.get('results')
        if not isinstance(rows,list) or len(rows)!=task['returned_notes']:raise ValueError('Raw response count mismatch')
        if response.get('next_page_token') or response.get('has_more'):raise ValueError('New pagination capability detected; review before ranking partial results')
        at=datetime.fromisoformat(request['started_at']);seen=set()
        for n in rows:
            ident=n.get('id');likes=n.get('like_count');url=n.get('url') or ''
            if not ident or ident in seen:raise ValueError('Missing/duplicate post ID in keyword response')
            seen.add(ident);raw+=1
            when=datetime.fromisoformat(n['posted_at'].replace('Z','+00:00'))
            if when.tzinfo is None or at.tzinfo is None:raise ValueError('Publication/request time must have timezone')
            if when<at-timedelta(hours=24):
                excluded.append({'id':ident,'keyword':task['keyword'],'posted_at':n['posted_at'],'requested_at':request['started_at'],'reason':'older_than_24h'})
                continue
            if when>at:raise ValueError('Publication time is later than request')
            if type(likes) is not int or likes<0:raise ValueError('Missing likes must not be treated as zero')
            if urlsplit(url).hostname not in ('www.douyin.com','douyin.com'):raise ValueError('Missing/unexpected post URL')
            # Use the first observed interaction snapshot consistently across keyword hits.
            if ident not in notes:notes[ident]={**n,'matched_keywords':[]}
            if task['keyword'] not in notes[ident]['matched_keywords']:notes[ident]['matched_keywords'].append(task['keyword'])
    if raw!=summary['total_returned']:raise ValueError('Search total mismatch')
    items=sorted(notes.values(),key=lambda n:(-n['like_count'],-datetime.fromisoformat(n['posted_at'].replace('Z','+00:00')).timestamp(),n['id']))
    count=(len(items)+9)//10
    result={'fraction':'0.10','rounding':'ceil','denominator':len(items),'selected':count,'raw_rows':raw,'excluded_old_hits':len(excluded),'excluded_old_unique':len({n['id'] for n in excluded}),
            'scope':'每关键词最近24小时，newest，单次最多50条；供应商未支持分页，不代表全站全量',
            'items':items[:count]}
    save(base/'excluded-old-posts.json',excluded)
    save(base/'notes.json',items);save(base/'selection.json',result);return result


def download(url,path):
    import requests
    host=urlsplit(url).hostname or ''
    if urlsplit(url).scheme not in ('http','https') or not any(host==s or host.endswith('.'+s) for s in ['douyinstatic.com','douyinvod.com','douyin.com','byteimg.com','ibytedtos.com','bytecdn.cn','pstatp.com','douyinpic.com','douyinmusicpromotion.com','amemv.com']):
        raise ValueError('Unexpected Douyin media host')
    if not path.exists():
        try:
            r=requests.get(url,timeout=(10,30));r.raise_for_status()
        except (requests.Timeout, requests.ConnectionError):
            # Official media CDN can stall through the inherited environment proxy.
            with requests.Session() as direct:
                direct.trust_env=False
                r=direct.get(url,timeout=(10,30));r.raise_for_status()
        if not r.content:raise ValueError('Empty media')
        path.write_bytes(r.content)


def video_file(url,path,base):
    download(url,path)
    try:
        meta=json.loads(subprocess.run(['ffprobe','-v','error','-show_entries','format=duration:stream=codec_type,width,height','-of','json',str(path)],capture_output=True,text=True,check=True).stdout)
    except (subprocess.CalledProcessError,ValueError):return None
    stream=next((s for s in meta.get('streams',[]) if s.get('codec_type')=='video' and s.get('width',0)>0 and s.get('height',0)>0),None)
    if not stream:return None
    subprocess.run(['ffmpeg','-v','error','-i',str(path),'-map','0:v:0','-f','null','-'],capture_output=True,check=True,timeout=180)
    dest=path.with_suffix('.mp4');path.rename(dest)
    return {'path':str(dest.relative_to(base)),'sha256':sha(dest),'size':dest.stat().st_size,'url':url,'width':stream['width'],'height':stream['height'],'duration_seconds':float(meta['format']['duration']),'decode_verified':True}


def media(note,base,config):
    import requests,os
    from ua_workflows.shared.config import project_env_values
    folder=base/'media'/note['id'];folder.mkdir(parents=True,exist_ok=True);manifest=folder/'manifest.json'
    if manifest.exists():
        result=load(manifest)
        if not result['files'] or not all((base/f['path']).is_file() and sha(base/f['path'])==f['sha256'] for f in result['files']):raise ValueError('Media evidence changed')
        return result
    files=[];rejected=[];details={}
    if note.get('video_url'):
        try:
            checked=video_file(note['video_url'],folder/'search-media.bin',base)
            if checked:files=[checked]
            else:rejected.append({'field':'search.video_url','reason':'No decodable video stream; may be audio, never uploaded as video'})
        except (requests.RequestException,ValueError,subprocess.CalledProcessError,subprocess.TimeoutExpired) as exc:
            rejected.append({'field':'search.video_url','reason':'Search source download/decode failed; try the authorized detail source','error_type':type(exc).__name__})
    if not files:
        api_response=folder/'detail-response.json'
        if api_response.exists():payload=load(api_response)
        else:
            if (folder/'detail-intent.json').exists():raise ValueError('Prior detail request outcome uncertain; do not replay automatically')
            key=project_env_values().get('EVERYINFRA_API_KEY') or os.environ.get('EVERYINFRA_API_KEY')
            if not key:raise ValueError('Missing EveryInfra key')
            body={'platform':'douyin','action':'video','params':{'url':note['url']}}
            save(folder/'detail-intent.json',{'body':body,'at':datetime.now(TZ).isoformat()})
            response,payload=post_json(config['api']['endpoint'],key=key,body=body,diagnostic_path=folder/'detail-http-diagnostic.json');save(api_response,payload)
            if not response.ok:raise ValueError('Detail API failed; response preserved')
        details=payload.get('results') or {}
        if details and details.get('id')!=note['id']:raise ValueError('Detail returned another post')
        url=details.get('video_url')
        if url and url!=note.get('video_url'):
            try:
                checked=video_file(url,folder/'detail-media.bin',base)
                if checked:files=[checked]
                else:rejected.append({'field':'detail.video_url','reason':'No video stream'})
            except (requests.RequestException,ValueError,subprocess.CalledProcessError,subprocess.TimeoutExpired) as exc:
                rejected.append({'field':'detail.video_url','reason':'Detail media download/decode failed; original image list may still be available','error_type':type(exc).__name__})
    is_video=bool(files)
    if not files:
        # A cover is not the original image set. Never substitute cover_image_url.
        urls=details.get('image_urls') or note.get('image_urls') or []
        if not urls or not all(isinstance(u,str) for u in urls):
            save(folder/'rejected-media.json',rejected)
            raise ValueError('No actual video stream or original image list; audio/cover cannot substitute')
        from PIL import Image
        for i,url in enumerate(dict.fromkeys(urls),1):
            original=folder/f'original-{i:02}.image';download(url,original);dest=folder/f'image-{i:02}.png'
            with Image.open(original) as im:im.load();im.convert('RGB').save(dest);w,h=im.size
            files.append({'path':str(dest.relative_to(base)),'sha256':sha(dest),'size':dest.stat().st_size,'url':url,'width':w,'height':h,'original_sha256':sha(original),'decode_verified':True})
    result={'note_id':note['id'],'reported_media_type':note.get('media_type'),'is_video':is_video,'actual_media_type':'video' if is_video else 'image','rejected':rejected,'files':files}
    save(manifest,result);return result


def fields(note,manifest,day,base):
    row=common.row_fields({**note,'is_video':manifest['is_video']},manifest,day,base)
    row.update({'来源':'每日最新','平台':'抖音','原文链接':{'text':'查看抖音作品','link':note['url']},'唯一键':'|'.join(['抖音',day,note['id']]),
                '采集窗口':'最近24小时；每关键词单次最多50条，无分页能力'})
    # Clear opposite media fields when updating an earlier failed/misclassified row.
    row['图片附件' if manifest['is_video'] else '视频附件']=[]
    row['图片链接' if manifest['is_video'] else '视频链接']=None
    return row


def verify(actual,expected):
    common.verify_row({k:v for k,v in actual.items() if k!='平台'},{k:v for k,v in expected.items() if k!='平台'})
    if text_field(actual.get('平台'))!='抖音':raise ValueError('Platform readback mismatch')


def provision(client,config):
    route=common.provision(client,config)
    field=next((f for f in client.listing(route+'/fields') if f['field_name']=='平台'),None)
    if not field:client.call('POST',route+'/fields',json={'field_name':'平台','type':3,'property':{'options':[{'name':'抖音'}]}})
    elif field['type']!=3:raise ValueError('Platform must be single select')
    elif '抖音' not in [o['name'] for o in (field.get('property') or {}).get('options',[])]:
        options=(field.get('property') or {}).get('options',[])
        client.call('PUT',route+'/fields/'+field['field_id'],json={'field_name':'平台','type':3,'property':{'options':options+[{'name':'抖音'}]}})
    check=next(f for f in client.listing(route+'/fields') if f['field_name']=='平台')
    if check['type']!=3 or '抖音' not in [o['name'] for o in (check.get('property') or {}).get('options',[])]:raise ValueError('Platform schema not verified')
    return route


def sync(base,selection,config,day):
    upload=common.upload_attachment
    client=common.authorized_client(config);route=provision(client,config)
    previous=client.listing(route+'/records');lookup={text_field(r['fields'].get('唯一键')):r for r in previous if r['fields'].get('唯一键')}
    if len(lookup)!=sum(bool(r['fields'].get('唯一键')) for r in previous):raise ValueError('Duplicate remote identity')
    uploads=load(base/'uploads.json') if (base/'uploads.json').exists() else {};expected=[];failures=[]
    for note in selection['items']:
        try:
            manifest=media(note,base,config);row=fields(note,manifest,day,base);tokens=[]
            for file in manifest['files']:
                digest=file['sha256']
                if digest not in uploads:
                    uploads[digest]=upload(client,config,base/file['path'],Path(file['path']).name);save(base/'uploads.json',uploads)
                tokens.append({'file_token':uploads[digest]})
            row['视频附件' if manifest['is_video'] else '图片附件']=tokens
            existing=lookup.get(row['唯一键'])
            row['首次发现日期']=text_field(existing['fields'].get('首次发现日期')) if existing else datetime.fromisoformat(load(base/'summary.json')['started_at']).astimezone(TZ).date().isoformat()
            if existing:client.call('PUT',route+'/records/'+existing['record_id'],json={'fields':row})
            else:
                result=client.call('POST',route+'/records/batch_create',params={'client_token':common.stable_uuid(row['唯一键'])},json={'records':[{'fields':row}]})
                lookup[row['唯一键']]=result['records'][0]
            expected.append(row);save(base/'expected-rows.json',expected)
        except Exception as exc:
            failures.append({'note_id':note['id'],'error_type':type(exc).__name__,'reason':str(exc) if isinstance(exc,ValueError) else 'Media/download/API/upload error; see evidence'})
        save(base/'media-failures.json',failures)
    remote=client.listing(route+'/records');bykey={text_field(r['fields'].get('唯一键')):r['fields'] for r in remote}
    for row in expected:verify(bykey[row['唯一键']],row)
    desired={r['唯一键'] for r in expected}
    if not failures:
        retire=[{'record_id':r['record_id'],'fields':{'当前入选':False}} for r in remote if text_field(r['fields'].get('平台'))=='抖音' and text_field(r['fields'].get('来源'))=='每日最新' and text_field(r['fields'].get('唯一键')).startswith('抖音|'+day+'|') and text_field(r['fields'].get('目标日期'))==day and r['fields'].get('当前入选') is True and text_field(r['fields'].get('唯一键')) not in desired]
        if retire:client.call('POST',route+'/records/batch_update',json={'records':retire})
    result={'status':'partial' if failures else 'complete','selected':selection['selected'],'verified_rows':len(expected),'failures':failures,'video_success':sum(r['素材类型']=='视频' for r in expected),'image_success':sum(r['素材类型']=='图片' for r in expected)}
    save(base/'sync.json',result);return client,result


def report(base,selection,config,client,synced,day):
    receipt=base/'report-receipt.json'
    if receipt.exists():return load(receipt)
    setting=config['crawl_completion_report'];chat=client.call('GET','im/v1/chats/'+setting['chat_id'])
    if chat.get('name')!=setting['chat_name'] or chat.get('chat_mode')!='group':raise ValueError('Unexpected report group')
    summary=load(base/'summary.json');url=config['feishu']['base_url']+'?table='+config['feishu']['table_id']
    snapshots=[load(Path(t['directory'])/'request.json')['started_at'] for t in summary['keywords'] if t.get('directory')]
    snapshot_note=f"本批搜索快照时间：{min(snapshots)} 至 {max(snapshots)}，各词以各自请求前24小时为准。" if snapshots else ''
    title='抖音素材日报'+('｜部分完成' if synced['status']=='partial' else '')+'｜'+day
    text=(f"**最近24小时 · {len(config['keywords'])} 个关键词 · 最新排序**\n\n📊 搜索命中：**{selection['raw_rows']} 条**\n"
          f"剔除超24h旧帖：**{selection.get('excluded_old_hits',0)} 条命中**\n24h内去重作品：**{selection['denominator']} 篇**\n按点赞量合并取前 **10%**，向上取整：**{selection['selected']} 篇**\n\n"
          f"🎬 视频下载并入表：**{synced['video_success']} 篇**\n🖼️ 图片下载并入表：**{synced['image_success']} 篇**\n"
          f"⚠️ 媒体/同步失败：**{len(synced['failures'])} 篇**，不补位、不改变分母\n"
          f"✅ 实际写表并读回验证：**{synced['verified_rows']} 篇**\n"
          f"已知搜索费用：${summary['cost_usd']}（不含视频详情及此前超时请求可能的费用）\n\n"
          f"{snapshot_note}\n"
          "平台标注抖音，来源标注每日最新。同赞按发布时间倒序；缺失互动保留为空，未套用真人口播筛选。\n"
          "范围限制：每关键词单次最多50条，当前接口没有翻页能力，不代表全站全部24小时作品。"
          "媒体类型以下载解码为准，音频和封面不冒充视频或原图。点击下方链接查看。")
    card={'config':{'wide_screen_mode':True},'header':{'template':'orange' if synced['status']=='partial' else 'blue','title':{'tag':'plain_text','content':title}},'elements':[{'tag':'div','text':{'tag':'lark_md','content':text}},{'tag':'action','actions':[{'tag':'button','type':'primary','text':{'tag':'plain_text','content':'查看多维表格'},'url':url}]}]}
    save(base/'report-payload.json',card)
    sent=client.call('POST','im/v1/messages',params={'receive_id_type':'chat_id'},json={'receive_id':setting['chat_id'],'msg_type':'interactive','content':json.dumps(card,ensure_ascii=False),'uuid':common.stable_uuid('douyin|'+day+'|'+setting['chat_id'])})
    if sent.get('chat_id')!=setting['chat_id'] or not sent.get('message_id'):raise ValueError('Report send receipt mismatch')
    result={'message_id':sent['message_id'],'chat_id':sent['chat_id'],'status':'sent_with_api_receipt'};save(receipt,result);return result


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',choices=['plan','run','provision']);parser.add_argument('--run-dir',type=Path);args=parser.parse_args();config=load(CONFIG)
    if args.action=='plan':
        if not args.run_dir:raise ValueError('--run-dir required')
        s=select(args.run_dir,config);print(json.dumps({k:v for k,v in s.items() if k!='items'},ensure_ascii=False));return
    if args.action=='provision':
        print(json.dumps({'status':'schema_verified','route':provision(common.authorized_client(config),config)},ensure_ascii=False));return
    import fcntl
    root=ROOT/'data/douyin_daily';root.mkdir(parents=True,exist_ok=True)
    with (root/'.run.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        now=datetime.now(TZ);base=args.run_dir or root/now.strftime('%Y%m%d');base.mkdir(parents=True,exist_ok=True)
        context=base/'daily-context.json'
        if not context.exists():save(context,{'target_date':(now-timedelta(days=1)).date().isoformat(),'execution_started_at':now.isoformat()})
        day=load(context)['target_date']
        if (base/'daily-result.json').exists() and (base/'report-receipt.json').exists() and load(base/'daily-result.json')['sync']['status']=='complete':
            print(json.dumps({'status':'already_complete','directory':str(base)},ensure_ascii=False));return
        with keep_awake(base/'awake.json'):
            if not (base/'search-intent.json').exists():
                save(base/'search-intent.json',{'at':now.isoformat()})
                subprocess.run([sys.executable,str(ROOT/'scripts/test_douyin_all_keywords.py'),'--run-dir',str(base)],cwd=ROOT,check=True)
            if not (base/'summary.json').exists():raise ValueError('Search incomplete; do not replay paid requests automatically')
            selection=select(base,config);client,synced=sync(base,selection,config,day);sent=report(base,selection,config,client,synced,day)
            save(base/'daily-result.json',{'target_date':day,'sync':synced,'report':sent});print(json.dumps({'directory':str(base),'sync':synced,'report':sent},ensure_ascii=False))
            if synced['status']!='complete':raise SystemExit(1)


if __name__=='__main__':main()
