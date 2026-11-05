"""Create draft demands from an explicitly checked source row. No Replica calls."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import time
import uuid

from ua_workflows.replica_studio_api import ReplicaError, save_private

TYPE = '复刻需求类型'
LANG = '复刻目标语言'
PORTRAIT = '复刻人物参考图'
TRIGGER = '创建复刻需求'
CLAIM = '复刻需求创建标识'
LINKS = '已建复刻需求'
NOTE = '需求创建说明'
MARKER = '入口请求标识'


def linked_ids(value, table_id):
    if not isinstance(value, list):
        return []
    result = []
    for item in value:
        if (isinstance(item, dict) and 'record_ids' not in item and 'record_id' not in item
                and item.get('table_id') == table_id and item.get('text_arr') == []):
            continue  # Feishu omits record_ids entirely for an empty relation cell.
        if isinstance(item, dict) and 'record_ids' in item:
            if item.get('table_id', table_id) != table_id:
                raise ValueError('Entry relationship targets a different table')
            result.extend(linked_ids(item['record_ids'], table_id))
        else:
            ident = item.get('record_id') if isinstance(item, dict) else item
            if not isinstance(ident, str) or not re.fullmatch(r'rec[A-Za-z0-9]+', ident):
                raise ValueError('Entry relationship contains an invalid record ID')
            result.append(ident)
    return result


class SourceEntry:
    def __init__(self, client, settings, root):
        from ua_workflows.replica_request_pool import APP, TABLE
        self.client, self.settings = client, settings
        self.root = Path(root)
        self.source = f'bitable/v1/apps/{APP}/tables/{TABLE}'
        self.target = f'bitable/v1/apps/{APP}/tables/{settings["request_table_id"]}'
        self.validated = False

    def search(self, route, name, value):
        from ua_workflows.replica_request_pool import normalize_request_row
        rows, cursor = [], None
        while True:
            params = {'page_size': 100}
            if cursor:
                params['page_token'] = cursor
            try:
                data = self.client.call('POST', route + '/records/search', params=params,
                    json={'filter': {'conjunction': 'and', 'conditions': [
                        {'field_name': name, 'operator': 'is', 'value': [value]}]}})
            except ValueError as error:
                if 'code=1254607' in str(error):
                    raise ReplicaError('read_failed', 'Feishu source-entry query temporarily not ready') from None
                raise
            rows.extend(normalize_request_row(r) for r in data.get('items', []))
            if not data.get('has_more'):
                return rows
            next_cursor = data.get('page_token')
            if not next_cursor or next_cursor == cursor:
                raise ReplicaError('read_failed', 'Source-entry query did not advance')
            cursor = next_cursor

    def row(self, route, rid):
        from ua_workflows.replica_request_pool import record
        return record(self.client, route, rid)

    def validate(self):
        entry = self.settings['source_entry']
        for route, expected in [(self.source, entry['fields']), (self.target, {MARKER: entry['marker_field_id']})]:
            schema = {v['field_name']: v for v in self.client.listing(route + '/fields')}
            for name, field_id in expected.items():
                if schema.get(name, {}).get('field_id') != field_id:
                    raise ValueError('Source-entry field identity changed: ' + name)
            if route == self.target and schema[MARKER]['type'] != 1:
                raise ValueError('Source-entry marker field type changed')
            if route == self.source:
                types = {TYPE: 3, LANG: 4, PORTRAIT: 17, TRIGGER: 7, CLAIM: 1, LINKS: 18, NOTE: 1}
                if any(schema[name]['type'] != kind for name, kind in types.items()):
                    raise ValueError('Source-entry field type changed')
                prop = schema[LINKS].get('property') or {}
                if prop.get('table_id') != self.settings['request_table_id'] or prop.get('multiple') is not True:
                    raise ValueError('Source-entry demand relationship changed')
        self.validated = True

    @staticmethod
    def signature(fields):
        from ua_workflows.social_chat_replica import source_identity
        data = {TYPE: fields.get(TYPE), LANG: fields.get(LANG) or [],
                PORTRAIT: [source_identity(v) for v in fields.get(PORTRAIT) or []]}
        return hashlib.sha256(json.dumps(data, ensure_ascii=False, sort_keys=True).encode()).hexdigest()

    def payload(self, row):
        from ua_workflows.replica_request_pool import LANGUAGES
        f, rid = row['fields'], row['record_id']
        action, languages = f.get(TYPE), f.get(LANG) or []
        if action not in {'替换logo', '替换语言', '替换人物'}:
            raise ValueError('请先选择复刻需求类型，再勾选创建')
        if action == '替换语言' and (len(languages) != 1 or languages[0] not in LANGUAGES):
            raise ValueError('翻译需求请选择一个目标语言，再勾选创建')
        if action == '替换人物' and not f.get(PORTRAIT):
            raise ValueError('人物需求请先添加参考图；当前视频换人功能仍待开放')
        name = (f.get('广告主') or f.get('标题') or rid) + '｜' + action
        if action == '替换语言':
            name += '｜' + languages[0]
        result = {'需求名称': name[:250], '关联原素材': [rid], '需求类型': action,
                  '需求状态': '新建', '处理来源': '原素材'}
        if action == '替换logo':
            result['目标品牌'] = 'Afizzy'
        if action == '替换语言':
            result['目标语言'] = languages
        if action == '替换人物':
            result['人物参考图'] = [{'file_token': v['file_token']} for v in f[PORTRAIT]]
        return result

    def save(self, journal):
        save_private(self.root / (journal['token'] + '.json'), journal)

    def note(self, rid, message):
        row = self.row(self.source, rid)
        if row['fields'].get(NOTE) != message:
            self.client.call('PUT', self.source + '/records/' + rid, json={'fields': {NOTE: message}})

    def advance(self, j):
        from ua_workflows.replica_request_pool import relation_ids
        rid, token = j['source_record_id'], j['token']
        row = self.row(self.source, rid)
        f = row['fields']
        if j['stage'] in {'claim_intent', 'claim_unknown'}:
            if f.get(CLAIM) != token:
                return  # Only read back an uncertain claim; never repeat its PUT.
            j['stage'] = 'claimed'; self.save(j)
        if f.get(CLAIM) != token:
            j.update(stage='blocked', error='需求创建标识已变化，保留原记录核对'); self.save(j); return
        if j['stage'] == 'claimed':
            if not f.get(TRIGGER) or self.signature(f) != j['signature']:
                j.update(stage='blocked', error='领取后参数发生变化，未新增需求'); self.save(j); return
            j['stage'] = 'draft_intent'; self.save(j)
            try:
                result = self.client.call('POST', self.target + '/records/batch_create',
                    params={'client_token': token}, json={'records': [{'fields': j['fields']}]})
                records = result.get('records') or []
                if len(records) != 1 or not records[0].get('record_id'):
                    raise ValueError('Draft receipt is incomplete')
                j.update(stage='created', demand_record_id=records[0]['record_id']); self.save(j)
            except Exception as error:
                j.update(stage='draft_unknown', error=type(error).__name__); self.save(j)
                return
        if j['stage'] in {'draft_intent', 'draft_unknown'}:
            found = self.search(self.target, MARKER, token)
            if len(found) != 1 or relation_ids(found[0]['fields'].get('关联原素材')) != [rid]:
                return  # No proof or ambiguous proof is never permission to POST again.
            j.update(stage='created', demand_record_id=found[0]['record_id'], recovered_readonly=True); self.save(j)
        if j['stage'] == 'created':
            demand = self.row(self.target, j['demand_record_id'])
            if (demand['fields'].get(MARKER) != token
                    or relation_ids(demand['fields'].get('关联原素材')) != [rid]):
                raise ValueError('Created demand identity does not match its source')
            row = self.row(self.source, rid); f = row['fields']
            if f.get(CLAIM) != token:
                j.update(stage='blocked', error='回填前创建标识已变化'); self.save(j); return
            links = list(dict.fromkeys(linked_ids(f.get(LINKS) or [], self.settings['request_table_id']) + [j['demand_record_id']]))
            j.update(stage='ack_intent', links=links); self.save(j)
            try:
                self.client.call('PUT', self.source + '/records/' + rid, json={'fields': {
                    LINKS: links, TRIGGER: False, NOTE: '已创建新建草稿；请在需求池补充并改为已验收后启动'}})
            except Exception as error:
                j.update(stage='ack_unknown', error=type(error).__name__); self.save(j); return
        if j['stage'] in {'ack_intent', 'ack_unknown'}:
            f = self.row(self.source, rid)['fields']
            if (f.get(CLAIM) == token and not f.get(TRIGGER)
                    and j['demand_record_id'] in linked_ids(f.get(LINKS) or [], self.settings['request_table_id'])):
                j.update(stage='complete', finished_at=time.time()); self.save(j)

    def tick(self):
        if not self.validated:
            self.validate()
        self.root.mkdir(parents=True, exist_ok=True)
        journals = [json.loads(p.read_text()) for p in self.root.glob('*.json')]
        active = {j['source_record_id']: j for j in journals if j['stage'] not in {'complete', 'blocked'}}
        for j in active.values():
            self.advance(j)
        for row in self.search(self.source, TRIGGER, 'true'):
            row = self.row(self.source, row['record_id'])
            rid, f = row['record_id'], row['fields']
            if not f.get(TRIGGER):
                continue  # A stale search page is not a new manual click.
            if rid in active:
                continue
            try:
                fields = self.payload(row)
            except ValueError as error:
                self.note(rid, str(error)); continue
            previous = next((j for j in journals if j['token'] == f.get(CLAIM)), None)
            if f.get(CLAIM) and (previous is None or previous['stage'] != 'complete'):
                self.note(rid, '已有未确认创建意图，请保留标识核对，不重复创建'); continue
            token = str(uuid.uuid4())
            fields[MARKER] = token
            j = {'token': token, 'source_record_id': rid, 'stage': 'claim_intent',
                 'signature': self.signature(f), 'fields': fields, 'created_at': time.time()}
            self.save(j)
            try:
                self.client.call('PUT', self.source + '/records/' + rid, json={'fields': {
                    CLAIM: token, NOTE: '正在创建草稿需求'}})
            except Exception as error:
                j.update(stage='claim_unknown', error=type(error).__name__); self.save(j); continue
            self.advance(j)
