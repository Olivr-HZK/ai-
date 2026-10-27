"""Allowlisted group @bot -> XHS note detail/media -> idempotent Bitable import."""
import argparse
import fcntl
import html
import json
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

import requests
from ua_workflows import xhs_daily as x
from ua_workflows import douyin_daily as d
from ua_workflows.shared.config import project_env_values

CONFIG=x.ROOT/'config/feishu_xhs_link_bot.json'
HOSTS={'www.xiaohongshu.com','xiaohongshu.com','xhslink.com','www.xhslink.com','xhslink.cn','www.xhslink.cn'}
DOUYIN_HOSTS={'www.douyin.com','douyin.com','v.douyin.com','www.iesdouyin.com','iesdouyin.com'}


def valid_url(url):
    try:
        p=urlsplit(url)
        return (p.scheme=='https' and p.hostname in HOSTS | DOUYIN_HOSTS
                and p.port is None and not p.username and not p.password
                and not any(ord(ch)<33 for ch in url))
    except ValueError:
        return False


def extract_links(strings):
    # ASCII URL characters avoid swallowing adjacent Chinese sharing copy.
    # Bare approved domains are accepted; arbitrary domains are never inferred.
    hosts='|'.join(re.escape(h) for h in sorted(HOSTS | DOUYIN_HOSTS,key=len,reverse=True))
    pattern=re.compile(r"(?<![A-Za-z0-9_.@/-])https?://[A-Za-z0-9._~:/?#@!$&*+,;=%+\-]+|(?<![A-Za-z0-9_.@/:%?=&-])(?:"+hosts+r")/[A-Za-z0-9._~:/?#@!$&*+,;=%+\-]*",re.I)
    urls=[]
    for text in strings:
        text=html.unescape(text)
        text=re.sub(r'[,;](?=https?://)', ' ', text, flags=re.I)
        for match in pattern.finditer(text):
            url=re.split(r'@@?_user_\d+',match.group(),maxsplit=1)[0].rstrip('.,;!')
            if url.lower().startswith('http://'):
                url='https://'+url[7:]
            elif not url.lower().startswith('https://'):url='https://'+url
            if valid_url(url) and url not in urls:urls.append(url)
    return urls


def links_from_message(event,bot_id,setting):
    msg=event.get('message',{})
    if (msg.get('chat_id')!=setting['allowed_chat_id'] or msg.get('chat_type')!='group'
        or event.get('sender',{}).get('sender_type')!='user'
        or not any(m.get('id',{}).get('open_id')==bot_id for m in msg.get('mentions',[]))):return []
    if msg.get('message_type') not in ('text','post'):return []
    content=json.loads(msg['content'])
    # Traverse structured rich-text links and ordinary text; do not execute commands.
    strings=[]
    def visit(v):
        if isinstance(v,str):strings.append(v)
        elif isinstance(v,dict):
            for z in v.values():visit(z)
        elif isinstance(v,list):
            for z in v:visit(z)
    visit(content)
    urls=extract_links(strings)
    if len(urls)>setting['max_links_per_message']:raise ValueError('一次最多提交5个素材链接')
    return urls


def resolve_url(url):
    if urlsplit(url).hostname in DOUYIN_HOSTS:
        return resolve_douyin_url(url)
    # Explicit redirects only, validate every destination before any outbound request.
    for _ in range(6):
        if not valid_url(url):raise ValueError('仅支持小红书原文或xhslink分享链接')
        if urlsplit(url).hostname.endswith('xiaohongshu.com'):
            match=re.search(r'/(?:explore|discovery/item)/([a-fA-F0-9]{24})(?:[/?#]|$)',url)
            if not match:raise ValueError('不是有效的小红书笔记链接')
            return url,match[1].lower()
        from urllib.parse import urljoin
        with requests.get(url,allow_redirects=False,timeout=(10,20),stream=True) as r:
            if r.status_code not in (301,302,303,307,308):raise ValueError('分享链接未返回笔记跳转')
            url=urljoin(url,r.headers.get('Location',''))
    raise ValueError('分享链接跳转过多')


def resolve_douyin_url(url):
    from urllib.parse import urljoin
    for _ in range(6):
        if not valid_url(url) or urlsplit(url).hostname not in DOUYIN_HOSTS:
            raise ValueError('不安全的抖音分享跳转')
        match=re.fullmatch(r'/(?:share/)?(?:video|note)/(\d+)(?:/)?',urlsplit(url).path)
        if match:return 'https://www.douyin.com/video/'+match[1],match[1]
        # Desktop search/profile pages identify the open work in modal_id.
        from urllib.parse import parse_qs
        modal=parse_qs(urlsplit(url).query).get('modal_id',[])
        if urlsplit(url).hostname in {'www.douyin.com','douyin.com'} and len(modal)==1 and re.fullmatch(r'\d+',modal[0]):
            return 'https://www.douyin.com/video/'+modal[0],modal[0]
        if urlsplit(url).hostname!='v.douyin.com':raise ValueError('不是抖音作品链接')
        with requests.get(url,allow_redirects=False,timeout=(10,20),stream=True) as r:
            if r.status_code not in (301,302,303,307,308):raise ValueError('抖音分享未返回作品跳转')
            url=urljoin(url,r.headers.get('Location',''))
    raise ValueError('分享链接跳转过多')


def fetch_douyin_note(url,note_id,root,config):
    folder=root/'media'/note_id;folder.mkdir(parents=True,exist_ok=True)
    response=folder/'detail-response.json';intent=folder/'detail-intent.json'
    if not response.exists():
        if intent.exists():raise ValueError('上次详情请求结果不确定，禁止重复付费')
        key=project_env_values().get('EVERYINFRA_API_KEY')
        if not key:raise ValueError('缺少EveryInfra密钥')
        body={'platform':'douyin','action':'video','params':{'url':url}}
        x.save(intent,{'body':body,'at':datetime.now(x.TZ).isoformat()})
        r=requests.post(config['api']['endpoint'],headers={'Authorization':'Bearer '+key},json=body,timeout=(15,120))
        payload=r.json();x.save(response,payload)
        x.save(folder/'detail-http.json',{'status':r.status_code})
        if not r.ok:raise ValueError('抖音详情接口失败')
    if (folder/'detail-http.json').exists() and x.load(folder/'detail-http.json')['status']!=200:
        raise ValueError('抖音详情接口失败，保留断点')
    payload=x.load(response)
    if payload.get('platform')!='douyin' or payload.get('action')!='video':raise ValueError('详情接口身份不匹配')
    note=payload.get('results')
    if not isinstance(note,dict) or str(note.get('id'))!=note_id or not note.get('posted_at'):
        raise ValueError('作品ID或发布时间校验失败')
    return {**note,'id':note_id,'url':url,'matched_keywords':[]}


def fetch_note(url,note_id,folder,config):
    response=folder/'note-response.json'; intent=folder/'note-intent.json'
    if not response.exists():
        key=project_env_values().get('EVERYINFRA_API_KEY')
        if not key:raise ValueError('缺少EveryInfra密钥')
        if intent.exists():raise ValueError('上次详情请求结果不确定，暂停重复付费请求，请检查断点')
        x.save(intent,{'url':url,'note_id':note_id,'at':datetime.now(x.TZ).isoformat()})
        r=requests.post(config['api']['endpoint'],headers={'Authorization':'Bearer '+key},json={'platform':'xiaohongshu','action':'note','params':{'url':url}},timeout=(15,120))
        body=r.json();x.save(response,{'http_status':r.status_code,'body':body})
    saved=x.load(response)
    if saved['http_status']!=200:raise ValueError('笔记详情接口失败，请检查保存的响应')
    note=saved['body'].get('results')
    if not isinstance(note,dict) or note.get('id')!=note_id or type(note.get('is_video')) is not bool:raise ValueError('笔记详情身份或素材类型校验失败')
    if not note.get('posted_at'):raise ValueError('接口未返回发布时间')
    return {**note,'url':url,'matched_keywords':[]}


def import_link(url,root,client,config):
    url,note_id=resolve_url(url)
    is_douyin=urlsplit(url).hostname in DOUYIN_HOSTS
    platform='抖音' if is_douyin else '小红书'
    if is_douyin:root=root/'douyin'
    folder=root/'notes'/note_id;folder.mkdir(parents=True,exist_ok=True)
    # One manual row per note, separate key namespace from daily ranking.
    route=f"bitable/v1/apps/{config['feishu']['app_token']}/tables/{config['feishu']['table_id']}"
    key=platform+'|手动|'+note_id
    remote=client.listing(route+'/records');existing=next((r for r in remote if x.text_field(r['fields'].get('唯一键'))==key),None)
    receipt=folder/'import-receipt.json'
    if receipt.exists() and existing:
        expected=x.load(folder/'expected-row.json');x.verify_row(existing['fields'],expected,config)
        return {'note_id':note_id,'status':'already_imported','record_id':existing['record_id']}
    note=fetch_douyin_note(url,note_id,root,config) if is_douyin else fetch_note(url,note_id,folder,config)
    manifest=d.media(note,root,config) if is_douyin else x.media(note,root,config)
    if is_douyin:note['is_video']=manifest['is_video']
    day=datetime.now(x.TZ).date().isoformat()
    fields=d.fields(note,manifest,day,root) if is_douyin else x.row_fields(note,manifest,day,root)
    fields.update({'来源':'手动增加','唯一键':key,'采集窗口':'群内手动链接收录；不参加每日排名','采集批次':'manual-link','首次发现日期':day})
    tokens=[]
    uploads=x.load(root/'uploads.json') if (root/'uploads.json').exists() else {}
    for m in manifest['files']:
        digest=m['sha256'];path=root/m['path']
        if digest not in uploads:
            uploads[digest]=x.upload_attachment(client,config,path,path.name);x.save(root/'uploads.json',uploads)
        tokens.append({'file_token':uploads[digest]})
    fields[x.attachment_field(config,note['is_video'])]=tokens
    x.save(folder/'expected-row.json',fields)
    if existing:
        record_id=existing['record_id'];client.call('PUT',route+'/records/'+record_id,json={'fields':fields})
    else:
        result=client.call('POST',route+'/records/batch_create',params={'client_token':x.stable_uuid(key)},json={'records':[{'fields':fields}]});record_id=result['records'][0]['record_id']
    actual=client.call('GET',route+'/records/'+record_id)['record'];x.verify_row(actual['fields'],fields,config)
    result={'status':'verified','note_id':note_id,'record_id':record_id,'attachments':len(tokens)};x.save(receipt,result);return result


def enqueue(event,bot_id,setting,root):
    msg=event.get('message',{});msg_id=msg.get('message_id','')
    if not re.fullmatch(r'om_[A-Za-z0-9]+',msg_id):raise ValueError('Invalid message ID')
    # Metadata only: no message body, link token, sender name or credentials.
    allowed=msg.get('chat_id')==setting['allowed_chat_id']
    mentioned=any(m.get('id',{}).get('open_id')==bot_id for m in msg.get('mentions',[]))
    audit={'message_id':msg_id,'received_at':datetime.now(x.TZ).isoformat(),
           'allowed_chat':allowed,'bot_mentioned':mentioned,'message_type':msg.get('message_type')}
    try:
        links=links_from_message(event,bot_id,setting)
        audit['supported_link_count']=len(links)
        audit['decision']='queued' if links else 'ignored'
        audit['reason']=('accepted' if links else 'other_chat' if not allowed else 'no_matching_bot_mention' if not mentioned else 'unsupported_message_type' if msg.get('message_type') not in ('text','post') else 'no_supported_link')
    except Exception as exc:
        audit.update(decision='rejected',reason=type(exc).__name__)
        x.save(root/'received'/(msg_id+'.json'),audit)
        return
    x.save(root/'received'/(msg_id+'.json'),audit)
    if not links:return
    path=root/'jobs'/(msg_id+'.json')
    if not path.exists():x.save(path,{'message_id':msg_id,'chat_id':msg['chat_id'],'links':links,'status':'queued'})


def work(root,setting):
    while True:
        for path in sorted((root/'jobs').glob('*.json')):
            job=x.load(path)
            if job['status'] not in ('queued','processing'):continue
            try:
                config=x.load(x.CONFIG);client=x.authorized_client(config);job['status']='processing';x.save(path,job)
                results=[];targets=[]
                for url in job['links']:
                    config=x.load(d.CONFIG if urlsplit(url).hostname in DOUYIN_HOSTS else x.CONFIG)
                    results.append(import_link(url,root,client,config))
                    target=x.bitable_url(config)
                    if target not in targets:targets.append(target)
                job.update(status='complete',results=results)
                text=f"素材链接手动收录完成：{len(results)}篇，信息和媒体附件已核验。\n"+'\n'.join(targets)
            except Exception as exc:
                job.update(status='blocked',error_type=type(exc).__name__)
                import traceback
                x.save(root/'errors'/(job['message_id']+'.json'),{'error_type':type(exc).__name__,'frames':[{'file':Path(t.filename).name,'line':t.lineno,'function':t.name} for t in traceback.extract_tb(exc.__traceback__)]})
                # Never expose URLs with tokens, credentials, supplier responses or raw exceptions.
                text='素材链接手动收录未完成，已保存断点；请管理员检查本地任务记录。不会自动重复付费请求。'
            x.save(path,job)
            try:
                receipt=client.call('POST','im/v1/messages/'+job['message_id']+'/reply',json={'msg_type':'text','content':json.dumps({'text':text},ensure_ascii=False),'uuid':x.stable_uuid(job['message_id']+'|reply|'+job['status'])})
                job['reply_message_id']=receipt['message_id'];x.save(path,job)
            except Exception:pass
        time.sleep(2)


def main():
    p=argparse.ArgumentParser();p.add_argument('--check',action='store_true');args=p.parse_args()
    setting=x.load(CONFIG);config=x.load(x.CONFIG);env=project_env_values()
    if not setting['enabled'] or env.get('FEISHU_APP_ID')!=setting['expected_app_id']:raise ValueError('Bot disabled or app identity mismatch')
    root=x.ROOT/setting['state_dir'];(root/'jobs').mkdir(parents=True,exist_ok=True)
    with (root/'.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        client=x.authorized_client(config);chat=client.call('GET','im/v1/chats/'+setting['allowed_chat_id'])
        if chat.get('name')!=setting['allowed_chat_name'] or chat.get('chat_mode')!='group':raise ValueError('Group identity mismatch')
        # Bot-info uses a top-level bot envelope, unlike most v1 data responses.
        r=client.session.get('https://open.feishu.cn/open-apis/bot/v3/info',timeout=30);body=r.json()
        if body.get('code')!=0 or body.get('bot',{}).get('app_name')!='广告之神':raise ValueError('Bot identity mismatch')
        bot_id=body['bot']['open_id'];x.provision(client,config)
        if args.check:print(json.dumps({'app':'广告之神','chat':chat['name'],'status':'ready'},ensure_ascii=False));return
        import lark_oapi as lark
        handler=lark.EventDispatcherHandler.builder('','').register_p2_im_message_receive_v1(lambda data:enqueue(json.loads(lark.JSON.marshal(data))['event'],bot_id,setting,root)).build()
        threading.Thread(target=work,args=(root,setting),daemon=True).start()
        lark.ws.Client(env['FEISHU_APP_ID'],env['FEISHU_APP_SECRET'],event_handler=handler,log_level=lark.LogLevel.ERROR).start()

if __name__=='__main__':main()
