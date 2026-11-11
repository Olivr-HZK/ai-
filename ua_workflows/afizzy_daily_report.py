"""Evidence-bound Afizzy completion broadcast; display groups merge both OSes."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime
import fcntl
import hashlib
import html
import json
from pathlib import Path
import re
import uuid

from ua_workflows.social_chat_browser_daily import CONFIG, ROOT, TZ, Feishu, load, save, table_url

BUCKETS = (
    'download_failed', 'original_url_missing', 'playable_archived',
    'landscape_excluded', 'relaunch_excluded', 'clip_intraday_removed',
    'clip_crossday_removed', 'content_excluded', 'content_held',
    'previous_template_removed', 'previous_template_pending',
    'template_variant_removed', 'selected',
)
DEDUPE = ('clip_intraday_removed', 'clip_crossday_removed',
          'previous_template_removed', 'template_variant_removed')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def product_aliases(groups):
    result = {}
    for product in groups['products']:
        for alias in [product['name'], *product['aliases']]:
            require(alias not in result or result[alias] == product['name'], 'Ambiguous report alias')
            result[alias] = product['name']
    return result


def aggregate(rows, groups):
    aliases = product_aliases(groups)
    regions = {product['name']: product.get('region') for product in groups['products']}
    products = defaultdict(list)
    identities = set()
    for row in rows:
        require(row['status'] in BUCKETS, 'Unknown or unfinished funnel status')
        identity = row['os'], row['ad_id']
        require(identity not in identities, 'Duplicate target-day creative identity')
        require(row['os'] in ('iOS', 'Android') and row['ad_id'] and row['app'], 'Missing creative identity')
        identities.add(identity)
        products[aliases.get(row['app'], row['app'])].append(row)

    def counts(items):
        result = {name: 0 for name in BUCKETS}
        result.update(Counter(item['status'] for item in items))
        result['new'] = len(items)
        result['unavailable'] = result['download_failed'] + result['original_url_missing']
        result['downloaded'] = len(items) - result['unavailable']
        result['dedupe_removed'] = sum(result[k] for k in DEDUPE)
        result['held'] = result['content_held'] + result['previous_template_pending']
        require(sum(result[k] for k in BUCKETS) == result['new'], 'Funnel does not reconcile')
        return result

    details = [{'product': name, 'region': regions.get(name) or '地区待确认',
                'observed_names': sorted({r['app'] for r in items}),
                'oses': sorted({r['os'] for r in items}), **counts(items)}
               for name, items in products.items()]
    details.sort(key=lambda row: (-row['new'], row['product']))
    total = counts(rows)
    require(all(sum(p[key] for p in details) == total[key] for key in total), 'Product totals differ')
    return {'total': total, 'products': details}


def evidence_rows(base):
    """Derive mutually exclusive outcomes from the actual saved phase evidence."""
    source = load(base/'source-ledger.json')
    media = load(base/'all-downloaded-media-index.json')
    byref = {(i['collection_ref']['preset'], i['collection_ref']['page'],
              i['collection_ref']['card'], i['ad_id']): i for i in media}
    require(len(byref) == len(media), 'Duplicate original-media index reference')
    land = {i['id'] for i in load(base/'excluded-landscape.json')['items']}
    relaunch = {i['id'] for i in load(base/'relaunch-exclusions.json')}
    plan = load(base/'pre-review/plan.json')
    clip = {key.split(':')[1]: value for key, value in plan['decisions'].items()}
    reviews = {r['id']: r for r in load(base/'review_observations.json')['items']}
    selection = load(base/'selection.json')
    selected = {i['id']: i for i in selection['items']}
    require(len(selected) == len(selection['items']), 'Duplicate selection ID')
    previous = load(base/'previous-day-template-review.json')
    variants = {id for group in load(base/'template-groups.json')
                for id in group['eligible_members'] if id != group['representative']}
    target = selection['target_date']
    rows, source_keys = [], set()
    for row in source:
        ref = row['preset'], row['pageNumber'], row['cardIndex'], row['adId']
        require(ref not in source_keys, 'Duplicate source-ledger reference')
        source_keys.add(ref)
        if row['state'] == 'dateExcluded':
            require(row['firstDiscovered'] != target, 'Target-day creative incorrectly date-excluded')
            continue
        require(row['firstDiscovered'] == target, 'Unknown/other-day discovery date')
        id = None
        if row['state'] == 'failed':
            state = 'download_failed'
        elif row['state'] == 'missing':
            state = 'original_url_missing'
        else:
            require(row['state'] == 'complete', 'Unfinished source collection')
            if row['kind'] == 'playable':
                state = 'playable_archived'
            else:
                item = byref[ref]; id = item['id']
                require(item['sha256'] == row['sha256'] and item['size'] == row['bytes'], 'Source binding mismatch')
                if id in land:
                    state = 'landscape_excluded'
                elif id in relaunch:
                    state = 'relaunch_excluded'
                elif clip[id]['status'] == 'intraday_removed':
                    state = 'clip_intraday_removed'
                elif clip[id]['status'] == 'crossday_removed':
                    state = 'clip_crossday_removed'
                else:
                    require(clip[id]['status'] == 'review_pending', 'Unknown CLIP result')
                    review = reviews[id]
                    require(review['visually_inspected'] is True and review['source_sha256'] == item['sha256'], 'Actual visual review missing')
                    if review['decision'] == 'exclude':
                        state = 'content_excluded'
                    elif review['decision'] == 'review':
                        state = 'content_held'
                    else:
                        require(review['decision'] == 'keep', 'Unknown content decision')
                        if id in previous['matches']:
                            state = 'previous_template_removed'
                        elif id in previous['pending']:
                            state = 'previous_template_pending'
                        elif id in variants:
                            state = 'template_variant_removed'
                        else:
                            require(id in selected, 'Qualified item has no final outcome')
                            state = 'selected'
        fields = row['metadata']['fields']
        rows.append({'id': id, 'ad_id': row['adId'], 'app': fields['appName']['value'],
                     'os': fields['platform']['value'], 'kind': row.get('kind', 'unknown'), 'status': state})
    result = Counter(r['status'] for r in rows)
    require(result['landscape_excluded'] == len(land) and result['relaunch_excluded'] == len(relaunch), 'First gates mismatch')
    require(result['clip_intraday_removed'] == plan['counts'].get('intraday_removed', 0) and
            result['clip_crossday_removed'] == plan['counts'].get('crossday_removed', 0), 'CLIP evidence mismatch')
    require(result['selected'] == len(selected) == selection['selected_count'], 'Selection evidence mismatch')
    require(result['previous_template_removed'] == len(previous['matches']) and
            result['previous_template_pending'] == len(previous['pending']) and
            result['template_variant_removed'] == len(variants), 'Template evidence mismatch')
    require(sum(result[k] for k in ('content_excluded', 'content_held', 'selected',
            'previous_template_removed', 'previous_template_pending', 'template_variant_removed')) == len(reviews), 'Review coverage mismatch')
    return rows


def prepare(base, config):
    base = Path(base).resolve()
    require(not (base/'report-receipt.json').exists() and not (base/'report/send-intent.json').exists(), 'Preserve a sent/uncertain report payload')
    result, sync = load(base/'daily-result.json'), load(base/'sync-receipt.json')
    target = result['target_date']; date.fromisoformat(target)
    require(load(base/'scope.json')['target_date'] == load(base/'selection.json')['target_date'] == sync['target_date'] == target, 'Batch dates disagree')
    require(result['workflow_finished'] is True and result['sync']['status'] == sync['status'] == 'complete', 'Original ingestion is not verified')
    require(sha(base/'scope.json') == result['scope_sha256'] and sha(base/'selection.json') == result['selection_sha256'], 'Pinned scope/selection hash changed')
    validation_path = base/sync['validation_path']; validation = load(validation_path)
    require(sha(validation_path) == sync['validation_sha256'] == result['source_attachments_sha256'], 'Independent readback hash mismatch')
    require(validation['status'] == 'passed' and validation['expected_fields_matched'] is True and
            validation['exact_token_name_size_and_local_sha256_verified'] is True, 'Independent readback not passed')
    rows = evidence_rows(base)
    setting = config['daily_workflow']['completion_broadcast']
    require(setting['enabled'] is True and setting['mention_all'] is False, 'Daily broadcast not enabled without mentions')
    require(base.name == target and target >= setting['first_target_date'], 'Use the original authorized target-date directory; no historical backfill')
    groups = load(ROOT/setting['product_groups'])
    stats = aggregate(rows, groups)
    selected_ids = {r['id'] for r in rows if r['status'] == 'selected'}
    files = validation['files']
    require({r['id'] for r in files} == selected_ids and len(files) == len(selected_ids), 'Verified attachment set differs from selection')
    require(len(files) == result['sync']['verified'] == sync['verified'] and
            {r['record_id'] for r in files} == set(sync['record_ids']), 'Remote record set mismatch')
    stats.update(target_date=target, report_date=datetime.now(TZ).date().isoformat(),
                 status=result['status'], scope_sha256=result['scope_sha256'],
                 products_config_sha256=sha(ROOT/setting['product_groups']),
                 selected_media=dict(Counter(r['kind'] for r in rows if r['status']=='selected')),
                 scope_complete=result['collection']['scope_complete'],
                 verified_records=len(files), missing_history_dates=load(base/'pre-review/plan.json')['missing_history_dates'],
                 marker='AFIZZY-DAY-'+target+'-'+result['scope_sha256'][:12])
    names = ['source-ledger.json', 'all-downloaded-media-index.json', 'excluded-landscape.json',
             'relaunch-exclusions.json', 'pre-review/plan.json', 'review_observations.json',
             'previous-day-template-review.json', 'template-groups.json', 'selection.json',
             'sync-receipt.json', 'daily-result.json']
    stats['evidence_sha256'] = {name:sha(base/name) for name in names}
    card = build_card(stats, table_url(config))
    payload = {'version': setting['version'], 'stats': stats, 'card': card}
    save(base/'report/payload.json', payload)
    save(base/'report/creative-outcomes.json', rows)
    return payload


def safe_name(name):
    return html.escape(re.sub(r'[\r\n]+', ' ', name)).replace('*', '\\*').replace('`', '\\`')


def product_sections(products):
    if not any('region' in product for product in products):
        return [(None, products)]
    sections = []
    for region, title in [('日韩', '日韩竞品'), ('欧美及巴西', '欧美及巴西竞品')]:
        members = [product for product in products
                   if region in (product.get('region') or '').split('／')]
        if members:
            sections.append((title, members))
    unclassified = [product for product in products
                    if not set((product.get('region') or '').split('／')) & {'日韩', '欧美及巴西'}]
    if unclassified:
        sections.append(('地区待确认', unclassified))
    return sections


def build_card(stats, url):
    t = stats['total']
    require(sum(t[k] for k in BUCKETS) == t['new'], 'Funnel does not reconcile')
    require(stats['verified_records'] == t['selected'], 'Selected/verified count mismatch')
    def md(text):
        return {'tag':'div','text':{'tag':'lark_md','content':text}}
    metrics = [('竞品上新',t['new']),('去重后入表',t['selected'])]
    elements = [{'tag':'column_set','flex_mode':'none','background_style':'grey','columns':[
        {'tag':'column','width':'weighted','weight':1,'elements':[md(f"**{value} 条**\n{label}")]}
        for label,value in metrics]}]
    elements += [{'tag':'hr'},md('**竞品明细**')]
    details = []
    for title, products in product_sections(stats['products']):
        if title:
            details.append(f'**{title}**')
        details.extend(f"**{safe_name(p['product'])}**　上新 **{p['new']}** → 入表 **{p['selected']}**" for p in products)
    elements.append(md('\n'.join(details)))
    elements += [{'tag':'hr'},{'tag':'action','actions':[{'tag':'button','type':'primary','text':{'tag':'plain_text','content':'查看精选素材'},'url':url}]}]
    day = date.fromisoformat(stats['target_date'])
    card = {'config':{'wide_screen_mode':True,'update_multi':True},
            'header':{'template':'blue','title':{'tag':'plain_text','content':f"竞品素材日报 · {day.month}月{day.day}日"}},'elements':elements}
    require('<at' not in json.dumps(card,ensure_ascii=False).lower(), 'Mentions are prohibited')
    return card


def normalized(text):
    return re.sub(r'[\s*`\\]+', '', html.unescape(text))


def verify_message(message, receipt, card):
    require(message.get('message_id') == receipt['message_id'] and message.get('chat_id') == receipt['chat_id'], 'Message identity mismatch')
    require(not message.get('deleted') and not message.get('mentions'), 'Message deleted or has mentions')
    content = message.get('body',{}).get('content', '')
    parsed = json.loads(content) if isinstance(content,str) else content
    def strings(value):
        if isinstance(value,str):
            return [value]
        if isinstance(value,list):
            return [s for v in value for s in strings(v)]
        if isinstance(value,dict):
            return [s for key,v in value.items() if key != 'tag' for s in strings(v)]
        return []
    body = normalized(' '.join(strings(parsed)))
    def expected_blocks(value):
        if isinstance(value,list):
            for item in value:
                yield from expected_blocks(item)
        elif isinstance(value,dict):
            if value.get('tag') in ('action','button'):
                return
            if value.get('tag') in ('lark_md','plain_text') and isinstance(value.get('content'),str):
                yield value['content']
            else:
                for item in value.values():
                    yield from expected_blocks(item)
    for block in expected_blocks(card['elements']):
        for line in block.splitlines():
            require(normalized(line) in body, 'Report readback content mismatch')


def confirm_send_response(base, receipt, card):
    """Validate the saved server response, without claiming an independent GET."""
    base = Path(base)
    response_path = base/'report/send-response.json'
    verify_message(load(response_path), receipt, card)
    receipt.update(status='verified_by_send_response',server_response_verified=True,
                   readback_status='permission_denied',readback_error_code=99991672,
                   verified_at=datetime.now(TZ).isoformat(),send_response_sha256=sha(response_path))
    save(base/'report-receipt.json',receipt)
    return receipt


def send(base, config, client):
    base = Path(base).resolve(); setting = config['daily_workflow']['completion_broadcast']
    require(setting.get('enabled') is True and setting.get('mention_all') is False, 'Report is not authorized')
    destination = config[setting['destination']]
    require(destination.get('enabled') is True and destination.get('mention_all') is False, 'Destination not authorized')
    with (base/'.afizzy-report.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        receipt_path = base/'report-receipt.json'
        payload_path = base/'report/payload.json'
        if receipt_path.exists():
            receipt = load(receipt_path)
            require(receipt['chat_id'] == destination['chat_id'], 'Existing report destination changed')
            require(receipt.get('message_id'), 'Unknown send intent cannot be replayed')
            payload = load(payload_path)
            require(digest(payload['card']) == receipt['card_sha256'], 'Sent report payload changed')
            if receipt.get('status') == 'updated_by_api_ack':
                ack = load(base/receipt['update_ack_path'])
                require(ack['message_id'] == receipt['message_id'] and ack['card_sha256'] == receipt['card_sha256'] and ack['status'] == 'accepted', 'Updated card acknowledgement mismatch')
                return receipt
            if receipt.get('status') == 'verified_by_send_response':
                verify_message(load(base/'report/send-response.json'),receipt,payload['card'])
                return receipt
        else:
            for name in ('crawl-report-complete.json',):
                if (base/name).exists():
                    raise ValueError('Existing legacy report must be reconciled; refusing another daily send')
            require(not (base/'report/send-intent.json').exists(), 'Unknown send intent cannot be replayed')
            payload = prepare(base, config)
            chat = client.call('GET','im/v1/chats/'+destination['chat_id'])
            require(chat.get('name') == destination['chat_name'] and chat.get('chat_mode') == 'group', 'Configured group identity changed')
            seed = 'afizzy-daily|'+payload['stats']['target_date']+'|'+destination['chat_id']
            intent = {'status':'send_intent','chat_id':destination['chat_id'],'chat_name':destination['chat_name'],
                'target_date':payload['stats']['target_date'],'uuid':str(uuid.uuid5(uuid.NAMESPACE_URL,seed)),
                'card_sha256':digest(payload['card']),'created_at':datetime.now(TZ).isoformat()}
            save(base/'report/send-intent.json',intent)
            try:
                message = client.call('POST','im/v1/messages',params={'receive_id_type':'chat_id'},json={
                    'receive_id':destination['chat_id'],'msg_type':'interactive',
                    'content':json.dumps(payload['card'],ensure_ascii=False),'uuid':intent['uuid']})
            except Exception as error:
                save(receipt_path,{**intent,'status':'send_unknown','error_type':type(error).__name__})
                raise
            save(base/'report/send-response.json',message)
            require(message.get('message_id') and message.get('chat_id') == destination['chat_id'], 'Send response identity mismatch; do not replay')
            receipt = {**intent,'message_id':message['message_id'],'status':'sent_pending_readback','mention_all':False}
            save(receipt_path,receipt)
        try:
            result = client.call('GET','im/v1/messages/'+receipt['message_id'])
        except ValueError as error:
            if 'code=99991672' not in str(error) or 'im:message' not in str(error):
                raise
            save(base/'report/readback-permission-error.json',{
                'code':99991672,'required_scopes':['im:message:readonly','im:message'],
                'action':'no_scope_expansion_no_replay_use_saved_send_response'})
            return confirm_send_response(base,receipt,payload['card'])
        save(base/'report/readback.json',result)
        require(len(result.get('items',[])) == 1, 'Expected exactly one message in readback')
        verify_message(result['items'][0],receipt,payload['card'])
        receipt.update(status='verified',verified_at=datetime.now(TZ).isoformat(),readback_sha256=sha(base/'report/readback.json'))
        save(receipt_path,receipt)
        return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=['prepare','send','send-wecom'])
    parser.add_argument('--run-dir',type=Path,required=True)
    args = parser.parse_args(); base = args.run_dir.resolve()
    require(base.parent == (ROOT/'data/afizzy_daily').resolve(), 'Use the fixed daily run directory')
    config = load(CONFIG)
    if args.command == 'prepare':
        require(not (base/'report-receipt.json').exists() and not (base/'report/send-intent.json').exists(), 'Preserve a sent/uncertain report payload')
        payload = prepare(base,config)
        print(json.dumps({'status':'prepared','target_date':payload['stats']['target_date'],'total':payload['stats']['total'],'products':payload['stats']['products']},ensure_ascii=False))
    elif args.command == 'send-wecom':
        from ua_workflows.afizzy_wecom_report import send as send_wecom
        print(json.dumps(send_wecom(base,config),ensure_ascii=False))
    else:
        result={}
        errors=[]
        try:result['feishu']=send(base,config,Feishu())
        except Exception as error:errors.append({'channel':'feishu','type':type(error).__name__})
        if config['daily_workflow']['completion_broadcast'].get('wecom',{}).get('enabled'):
            from ua_workflows.afizzy_wecom_report import send as send_wecom
            try:result['wecom']=send_wecom(base,config)
            except Exception as error:errors.append({'channel':'wecom','type':type(error).__name__})
        result['errors']=errors
        save(base/'report/channels-result.json',result)
        print(json.dumps(result,ensure_ascii=False))
        if errors:raise SystemExit(1)


if __name__ == '__main__':
    main()
