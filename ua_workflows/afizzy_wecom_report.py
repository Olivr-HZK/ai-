"""One evidence-bound Afizzy daily summary to the authorized WeCom webhook."""
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit,parse_qs
import fcntl
import hashlib
import requests

from ua_workflows.afizzy_daily_report import load,save,sha,digest,require,product_sections,safe_name,TZ,table_url
from ua_workflows.shared.config import project_env_values


def build_payload(stats,url):
    t=stats['total'];require(t['selected']==stats['verified_records'],'WeCom import count is not verified')
    day=datetime.fromisoformat(stats['target_date'])
    lines=[f'**竞品素材日报 · {day.month}月{day.day}日**','',
           f'竞品上新：**{t["new"]}条**',f'去重后入表：**{t["selected"]}条**','', '**竞品明细**']
    for title, products in product_sections(stats['products']):
        if title:
            lines.extend(['', f'**{title}**'])
        for p in products:
            lines.append(f'{safe_name(p["product"])}：上新 **{p["new"]}** → 入表 **{p["selected"]}**')
    lines+=['',f'[查看精选素材]({url})']
    content='\n'.join(lines)
    require(len(content.encode('utf-8'))<=4096,'WeCom summary exceeds one message; do not split or truncate silently')
    require('<@' not in content and '@all' not in content,'WeCom mentions are prohibited')
    return {'msgtype':'markdown','markdown':{'content':content}}


def verified_payload(base):
    payload=load(base/'report/payload.json');stats=payload['stats']
    require(base.name==stats['target_date'],'Use the original target-date directory')
    pointer=base/'sync-latest.json'
    if pointer.exists():
        p=load(pointer);path=(base/p['path']).resolve()
        require(path.is_relative_to(base) and sha(path)==p['sha256'],'Latest ingestion receipt changed')
        sync=load(path);require(sync['status']=='complete' and sync['verified_total']==stats['verified_records'],'Latest table count differs from report')
        validation=(path.parent/sync['validation']).resolve()
        require(validation.is_relative_to(base) and sha(validation)==sync['validation_sha256'],'Latest independent validation changed')
        require(load(validation).get('status')=='passed','Latest independent validation is not passed')
    else:
        sync=load(base/'sync-receipt.json');require(sync['status']=='complete' and sync['verified']==stats['verified_records'],'Table ingestion not verified')
        require(sha(base/sync['validation_path'])==sync['validation_sha256'],'Independent validation changed')
        require(load(base/sync['validation_path']).get('status')=='passed','Independent validation is not passed')
    return payload


def send(base,config,*,post=None,credentials=None):
    base=Path(base).resolve();parent=config['daily_workflow']['completion_broadcast'];setting=parent.get('wecom',{})
    require(parent.get('enabled') is True,'Completion broadcast is disabled')
    require(setting.get('enabled') is True and setting.get('mention_all') is False,'WeCom broadcast is not authorized')
    values=project_env_values() if credentials is None else credentials
    endpoint=values.get(setting['webhook_env'],'') or ''
    parsed=urlsplit(endpoint);key=parse_qs(parsed.query).get('key',[''])[0]
    require(parsed.scheme=='https' and parsed.hostname=='qyapi.weixin.qq.com' and parsed.path=='/cgi-bin/webhook/send' and key,'Invalid configured WeCom endpoint')
    destination=hashlib.sha256(endpoint.encode()).hexdigest();folder=base/'wecom-report';folder.mkdir(exist_ok=True)
    receipt_path=base/'wecom-report-receipt.json'
    with (base/'.afizzy-wecom-report.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        payload=verified_payload(base);target=payload['stats']['target_date']
        require(target>=setting['first_target_date'],'No historical WeCom backfill is authorized')
        body=build_payload(payload['stats'],table_url(config));body_hash=digest(body)
        if receipt_path.exists():
            receipt=load(receipt_path)
            require(receipt['destination_sha256']==destination,'Existing WeCom destination changed')
            require(receipt['status']=='accepted_by_webhook','Existing WeCom intent is rejected/unknown; do not replay')
            require(receipt['payload_sha256']==body_hash,'WeCom already sent another version; no automatic resend')
            return receipt
        require(not (folder/'send-intent.json').exists(),'Unknown WeCom submission cannot be replayed')
        save(folder/'payload.json',body)
        intent={'status':'send_intent','channel':'wecom','target_date':target,'destination_sha256':destination,
                'payload_sha256':body_hash,'created_at':datetime.now(TZ).isoformat(),
                'reported_new':payload['stats']['total']['new'],'reported_ingested':payload['stats']['verified_records']}
        save(folder/'send-intent.json',intent)
        try:
            response=(post or requests.post)(endpoint,json=body,timeout=30,allow_redirects=False)
            data=response.json()
            if not isinstance(data,dict):raise ValueError('Unexpected acknowledgement structure')
        except Exception as error:
            save(receipt_path,{**intent,'status':'submission_unknown','error_type':type(error).__name__})
            raise ValueError('WeCom submission result unknown; do not resend') from None
        # Persist only the sanitized acknowledgement, never URL/request/headers.
        ack={'http_status':response.status_code,'errcode':data.get('errcode'),
             'errmsg':str(data.get('errmsg','')).replace(endpoint,'[redacted]').replace(key,'[redacted]')}
        save(folder/'response.json',ack)
        if response.status_code!=200 or data.get('errcode')!=0:
            save(receipt_path,{**intent,'status':'rejected_or_unconfirmed','http_status':response.status_code,'errcode':data.get('errcode')})
            raise ValueError('WeCom did not acknowledge delivery; inspect the sanitized receipt') from None
        receipt={**intent,'status':'accepted_by_webhook','http_status':200,'errcode':0,
                 'accepted_at':datetime.now(TZ).isoformat(),'response_sha256':sha(folder/'response.json'),
                 'message_id_available':False,'mention_all':False}
        save(receipt_path,receipt)
        return receipt
