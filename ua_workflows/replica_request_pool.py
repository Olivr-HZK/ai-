"""Manually approved Feishu demands, with results returned to the demand row.

Only demand status 已验收 authorizes admission. Source-table operations are
read-only; existing CLI checkpoints/journals protect business writes.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

from ua_workflows import social_chat_browser_daily as daily
from ua_workflows.social_chat_replica import (
    APP, TABLE, PipelineRunner, ReplicaFeishu, State, TERMINAL, source_identity, reconcile_logos,
)
from ua_workflows.replica_cli import error_result
from ua_workflows.replica_recovery import UNKNOWN, retry_delay
from ua_workflows.replica_studio_api import ReplicaAPI, ReplicaError, save_private
from ua_workflows.replica_translation import translate_many, language_state, download_translations
from ua_workflows.social_chat_media_orientation import POLICY as ORIENTATION_POLICY

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / 'config/replica_request_pool.json'
LANGUAGES = dict(zip(['英语', '中文', '日语', '韩语', '德语', '法语', '西班牙语', '葡萄牙语',
                      '意大利语', '俄语', '印尼语', '阿拉伯语'],
                     ['en', 'zh', 'ja', 'ko', 'de', 'fr', 'es', 'pt', 'it', 'ru', 'id', 'ar']))
INPUT_FIELDS = ('关联原素材', '需求类型', '需求说明', '处理来源', '目标品牌', '目标语言', '人物参考图')


class DemandBlocked(ValueError):
    pass


def config(path=CONFIG):
    value = daily.load(path)
    if (value.get('app_token') != APP or value.get('source_table_id') != TABLE
            or value.get('trigger_status') != '已验收'
            or not re.fullmatch(r'tbl[A-Za-z0-9]+', value.get('request_table_id', ''))
            or value['request_table_id'] == TABLE):
        raise ValueError('Request-pool destination or manual approval policy changed')
    return value


def relation_ids(value):
    if isinstance(value, dict):
        value = value.get('link_record_ids', value.get('record_ids', []))
    if not isinstance(value, list):
        return []
    result = []
    for item in value:
        if isinstance(item, dict) and 'record_ids' in item:
            if item.get('table_id', TABLE) != TABLE or not isinstance(item['record_ids'], list):
                return []
            nested = relation_ids(item['record_ids'])
            if len(nested) != len(item['record_ids']):
                return []
            result.extend(nested)
            continue
        ident = item.get('record_id') if isinstance(item, dict) else item
        if not isinstance(ident, str) or not re.fullmatch(r'rec[A-Za-z0-9]+', ident):
            return []
        result.append(ident)
    return result


def normalize_request_row(row):
    """Feishu encodes link IDs inside text-shaped cells; preserve that payload."""
    value = daily.normalize_record_texts(row)
    if '关联原素材' in row['fields']:
        value['fields']['关联原素材'] = row['fields']['关联原素材']
    return value


def record(client, route, record_id):
    result = client.call('GET', route + '/records/' + record_id, params={'text_field_as_array': 'true'})
    row = result.get('record')
    if not row or row.get('record_id') != record_id:
        raise ValueError('Feishu record identity mismatch')
    return normalize_request_row(row)


def fingerprint(fields):
    values = {name: fields.get(name) for name in INPUT_FIELDS}
    values['关联原素材'] = relation_ids(values['关联原素材'])
    # Attachment URLs are temporary; token, filename and exact size define input.
    values['人物参考图'] = [source_identity(x) for x in values.get('人物参考图') or []]
    return hashlib.sha256(json.dumps(values, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def plan_request(row, source):
    f, sf = row['fields'], source['fields']
    links = relation_ids(f.get('关联原素材'))
    if len(links) != 1 or links[0] != source['record_id']:
        raise DemandBlocked('请关联一条明确的原素材记录')
    action = f.get('需求类型')
    if action == '替换人物':
        raise DemandBlocked('人物替换尚未开放：参考图设备令牌接口403，视频人物替换未完成验收；未提交生成')
    if action not in {'替换logo', '替换语言'}:
        raise DemandBlocked('请选择替换logo、替换语言或替换人物')
    if sf.get('素材类型') != '视频':
        raise DemandBlocked('本执行器只处理视频；图片品牌替换请在聊天中手动启动内置生成工具')
    origin = f.get('处理来源') or '原素材'
    if origin not in {'原素材', '已有复刻结果'}:
        raise DemandBlocked('处理来源不明确')
    field = daily.load(daily.CONFIG)['feishu']['attachment_field_name'] if origin == '原素材' else '复刻附件'
    files = sf.get(field) or []
    if len(files) != 1:
        raise DemandBlocked('所选处理来源必须有且只有一个明确附件，请先核对原素材')
    identity = source_identity(files[0])
    name, size = identity['name'], identity['size']
    if (not isinstance(name, str) or Path(name).name != name or '\\' in name
            or Path(name).suffix.lower() not in {'.mp4', '.mov', '.webm', '.mkv'}
            or type(size) is not int or not 0 < size <= 100 * 1024 * 1024):
        raise DemandBlocked('原附件不是可上传的100MiB以内视频，未提交')
    languages = f.get('目标语言') or []
    languages = [languages] if isinstance(languages, str) else languages
    if action == '替换语言' and (len(languages) != 1 or languages[0] not in LANGUAGES):
        raise DemandBlocked('每条语言需求请选择一个目标语言；多语言请分别建立需求')
    brand = (f.get('目标品牌') or 'Afizzy').strip()
    if action == '替换logo' and brand.casefold() != 'afizzy':
        raise DemandBlocked('当前Logo流程使用Afizzy默认品牌；其他品牌需要先配置对应Logo素材')
    target = sf.get('目标日期')
    if not isinstance(target, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', target):
        raise DemandBlocked('原素材目标日期缺失或不明确')
    return {'record_id': row['record_id'], 'source_record_id': source['record_id'], 'source_field_name': field,
            'target_date': target, 'action': action, 'source': identity, 'input_fingerprint': fingerprint(f),
            'languages': [LANGUAGES[x] for x in languages] if action == '替换语言' else [],
            'source_name': name, 'brand': brand,
            'projection': {'原素材附件': [{'file_token': identity['file_token']}], '原广告主': sf.get('广告主') or '',
                           '原素材类型': sf['素材类型'], '原文案语言': '、'.join(sf.get('文案语言') or []),
                           '原素材日期': target}}


class PoolState(State):
    def __init__(self, root, settings):
        root = Path(root).resolve()
        if not (root / 'scope.json').exists():
            save_private(root / 'scope.json', {'version': 1, 'workflow': 'manual_request_pool', 'app_token': APP,
                         'table_id': settings['request_table_id'], 'target_date': 'manual-request-pool', 'items': [],
                         'source_field': '原素材附件', 'target_field': '复刻结果',
                         'date_field': '原素材日期', 'media_type_field': '原素材类型',
                         'media_orientation_policy': ORIENTATION_POLICY})
        super().__init__(root)
        if self.scope.get('workflow') != 'manual_request_pool' or self.scope.get('table_id') != settings['request_table_id']:
            raise ValueError('Pool runtime belongs to a different workflow or table')

    def add(self, item):
        with self._mutex:
            if item['record_id'] in self.items:
                return
            candidate = {**self.scope, 'items': [*self.scope['items'], item]}
            save_private(self.root / 'scope.json', candidate)
            self.scope, self.items = candidate, {v['record_id']: v for v in candidate['items']}

    def all(self):
        with self._mutex:
            return [self.get(rid) for rid in list(self.items)]


class RequestRunner(PipelineRunner):
    def __init__(self, state, settings, *args, **kwargs):
        self.settings = settings
        if kwargs.get('api_factory') is None:
            kwargs['api_factory'] = lambda: ReplicaAPI.configured(auth_mode=settings.get('auth_mode'),
                expected_account=settings['expected_account'], confirm_paid=True)
        super().__init__(state, *args, **kwargs)

    def destination_route(self):
        return 'bitable/v1/apps/' + APP + '/tables/' + self.settings['request_table_id']

    def validate_destination_fields(self, schema):
        fields = {v['field_name']: v for v in schema}
        for name in ['原素材附件', '复刻结果']:
            if fields.get(name, {}).get('type') != 17 or fields[name]['field_id'] != self.settings['fields'][name]:
                raise ValueError('Request-pool attachment schema changed')
        link = fields.get('关联原素材', {})
        if (link.get('field_id') != self.settings['fields']['关联原素材'] or link.get('type') != 18
                or (link.get('property') or {}).get('table_id') != TABLE
                or (link.get('property') or {}).get('multiple') is not False):
            raise ValueError('Request-pool source relationship changed')

    def _finished(self):
        return False  # Wait for the next manual approval, without a calendar job.

    def observe(self, phase, **extra):
        values = self.state.all()
        if not any(v['stage'] not in TERMINAL or v['stage'] in UNKNOWN for v in values):
            return self.state.summary(scheduling='request_pool', batch_phase='waiting_manual_trigger', **extra)
        return super().observe(phase, **extra)

    def _write_fields(self, rid, fields):
        current = record(self.feishu, self.route, rid)
        changed = {k: v for k, v in fields.items() if current['fields'].get(k) != v}
        if changed:
            self.feishu.call('PUT', self.route + '/records/' + rid, json={'fields': changed})
        return record(self.feishu, self.route, rid)

    def _admit(self):
        self.refresh_feishu()
        if (self.settings.get('source_entry') or {}).get('enabled'):
            from ua_workflows.replica_request_entry import SourceEntry
            if not hasattr(self, '_source_entry'):
                self._source_entry = SourceEntry(self.feishu, self.settings, self.state.root / 'source-entry')
            self._source_entry.client = self.feishu
            self._source_entry.tick()
        rows = [normalize_request_row(r) for r in self.feishu.listing(self.route + '/records')]
        source_route = 'bitable/v1/apps/' + APP + '/tables/' + TABLE
        for row in rows:
            rid, fields = row['record_id'], row['fields']
            if rid in self.state.items or fields.get('需求状态') != '已验收':
                continue
            try:
                links = relation_ids(fields.get('关联原素材'))
                if len(links) != 1:
                    raise DemandBlocked('请先关联一条原素材')
                source = record(self.feishu, source_route, links[0])
                item = plan_request(row, source)
                path = self.state.root / 'admissions' / (rid + '.json')
                if path.exists():
                    saved = daily.load(path)
                    if saved['input_fingerprint'] != item['input_fingerprint']:
                        raise DemandBlocked('此前提交意图的参数已变化，请新建需求并保留原断点')
                    item = saved
                elif fields.get('执行标识') or fields.get('复刻结果'):
                    raise DemandBlocked('已有执行标识或结果，但本机没有对应断点；先核对，不重复提交')
                else:
                    item.update(execution_id=str(uuid.uuid4()), approved_at=self.now())
                    save_private(path, item)  # Before remote claim or any paid call.
                current = self._write_fields(rid, {**item['projection'], '执行标识': item['execution_id'],
                                            '提交时间': int(item['approved_at'] * 1000),
                                            '执行进度': '上传中', '执行说明': '已验收，等待上传及自动分析'})
                if (current['fields'].get('需求状态') != '已验收'
                        or current['fields'].get('执行标识') != item['execution_id']
                        or fingerprint(current['fields']) != item['input_fingerprint']):
                    raise DemandBlocked('领取时需求发生变化，未提交')
                with self._guard:
                    self.state.add(item)
            except DemandBlocked as error:
                self._write_fields(rid, {'执行进度': '需人工处理', '执行说明': str(error)})

    def _eligible(self, rid):
        self.refresh_feishu()
        item = self.state.items[rid]
        row = record(self.feishu, self.route, rid)
        fields = row['fields']
        source = record(self.feishu, 'bitable/v1/apps/' + APP + '/tables/' + TABLE, item['source_record_id'])
        files = source['fields'].get(item['source_field_name']) or []
        valid = (fields.get('需求状态') == '已验收' and fields.get('执行标识') == item['execution_id']
                 and fingerprint(fields) == item['input_fingerprint'] and len(files) == 1
                 and source_identity(files[0]) == item['source'])
        if not valid:
            value = self.state.get(rid)
            value.update(stage='needs_review', error='需求状态、参数、关联素材或原附件已变化；停止新增动作，请新建需求')
            self.state.put(rid, value)
        return valid

    def _upload_once(self):
        self.refresh_feishu()
        for value in self.state.all():
            if self.state.items[value['record_id']].get('engine') == 'videolab':
                continue
            if value['stage'] in {'queued', 'source_ready'} and self.now() >= value.get('upload_check_after', 0):
                rid = value['record_id']
                with self._claim(rid, {'queued', 'source_ready'}) as claimed:
                    if claimed is None or not self._eligible(rid):
                        continue
                    self.submit_paused = False
                    result = self.upload_all([rid])
                    self._check_submission(result)
                    self.observe('upload')
                    return True
        return False

    def _control_once(self):
        self._admit()
        return self._advance_requests()

    def _advance_requests(self):
        self.api.authenticate()
        self.reconcile_uploads()
        self.reconcile_unknown_logos()
        self.reconcile_writes()
        for value in self.state.all():
            rid = value['record_id']
            if value['stage'] in {'uploaded', 'logo_accepted', 'logo_submitting'}:
                with self._claim(rid, {'uploaded', 'logo_accepted', 'logo_submitting'}) as claimed:
                    if claimed is not None:
                        self.poll([rid])
            with self._claim(rid, {'analysis_ready'}) as claimed:
                if claimed is None or not self._eligible(rid):
                    continue
                self.submit_paused = False
                item = self.state.items[rid]
                if self.logo_required(rid):
                    try:
                        options = self.api.smart_options()
                    except ReplicaError as error:
                        if error.code != 'unavailable':
                            raise
                        self.state.put(rid, {**claimed, 'stage': 'needs_review',
                                            'error_code': error.code,
                                            'error': '平台换Logo服务不可用，未提交生成：' + str(error)})
                        continue
                    result = self.submit_all_logos([rid], options=options)
                else:
                    if not self.prepare_translation(rid, item):
                        continue
                    self.state.put(rid, {**self.state.get(rid), 'stage': 'logo_submitting'})
                    result = self.submit_translation(claimed['job_id'], item)
                    self._translation_receipt(rid, result)
                self._check_request_submission(rid, result)
        self._sync_progress()
        self.observe('request_pool')
        return True

    def submit_translation(self, job_id, item):
        return translate_many(self.api, [job_id], self.checkpoint, item['languages'], source_video=True)

    def uses_translation(self, value):
        return self.state.items[value['record_id']]['action'] == '替换语言'

    def logo_required(self, rid):
        return self.state.items[rid]['action'] == '替换logo'

    def prepare_translation(self, rid, item):
        return True

    def _check_request_submission(self, rid, result):
        denied = next((v for v in result.get('results', [])
                       if v.get('code') == 'permission_denied' and v.get('http_status') == 403
                       and '命令行令牌不能使用这个接口' in v.get('message', '')), None)
        if denied is not None:
            # This explicit endpoint policy rejection is scoped to this demand.
            # Revocation, missing scopes and all ambiguous responses still stop
            # the pipeline through the ordinary submission checks below.
            value = self.state.get(rid)
            if value['stage'] not in UNKNOWN:
                value['stage'] = 'needs_review'
            value.update(error=denied['message'], error_code='permission_denied', http_status=403)
            self.state.put(rid, value)
            return
        try:
            self._check_submission(result)
        except ReplicaError:
            self._sync_progress()  # Do not leave a rejected request showing 分析中.
            raise

    def _translation_receipt(self, rid, result):
        item = result['results'][0]
        value = self.state.get(rid)
        if item.get('ok'):
            value['stage'] = 'logo_ready' if item['code'] == 'already_done' else 'logo_accepted'
        else:
            prior = self.checkpoint.get('translations', value['job_id']) or {}
            uncertain = item.get('code') == 'result_unknown' or any(
                p['stage'] in {'translation_intent', 'translation_unknown'} for p in prior.get('attempts', []))
            value.update(stage='logo_unknown' if uncertain else 'failed', error=item.get('message') or item.get('code'))
        self.state.put(rid, value)

    def poll(self, record_ids=None):
        for rid in record_ids or list(self.state.items):
            value, item = self.state.get(rid), self.state.items[rid]
            if not self.uses_translation(value) or value['stage'] == 'uploaded':
                super().poll([rid])
                continue
            job = self.api.job(value['job_id'])
            state = language_state(job, item['languages'][0], ['voice'])
            value.update(last_status=job.get('status'), last_message=job.get('message'))
            if state == 'ready':
                value['stage'] = 'logo_ready'
            elif state in {'error', 'review'}:
                value.update(stage='failed', error=job.get('error') or 'Translation returned no usable result')
            elif value['stage'] == 'logo_submitting':
                value['stage'] = 'logo_unknown'
            self.state.put(rid, value)

    def reconcile_unknown_logos(self):
        # The Logo reconciler only has evidence for Logo attempts. Translation
        # intents use their own checkpoint's read-only recovery path.
        for value in self.state.all():
            rid = value['record_id']
            if self.uses_translation(value) and value['stage'] == 'logo_unknown' and self.can_reconcile(value):
                if self.now() >= value.get('translation_check_after', 0):
                    result = self.submit_translation(value['job_id'], self.state.items[rid])
                    self._translation_receipt(rid, result)
                    current = self.state.get(rid)
                    current['translation_check_after'] = self.now() + self.health.policy['reconcile_interval']
                    self.state.put(rid, current)
                    self._check_submission(result)
        if any(v['stage'] == 'logo_unknown' and not self.uses_translation(v)
               and self.can_reconcile(v) for v in self.state.all()):
            reconcile_logos(self.state, self.api, checkpoint=self.checkpoint.data, clock=self.now,
                            respect_cooldown=True, interval=self.health.policy['reconcile_interval'],
                            should_check=lambda v: not self.uses_translation(v)
                            and self.can_reconcile(v))

    def download_all(self, record_ids=None):
        for rid in record_ids or list(self.state.items):
            item, value = self.state.items[rid], self.state.get(rid)
            if item['action'] != '替换语言':
                super().download_all([rid])
                continue
            if value['stage'] != 'logo_ready' or self.now() < value.get('download_check_after', 0):
                continue
            result = download_translations(self.api, [value['job_id']], self.checkpoint,
                                           self.state.root / 'results', item['languages'])
            videos = [v for v in result['results'] if v.get('kind') == 'video']
            if len(videos) == 1 and videos[0].get('ok'):
                value.update(stage='downloaded', output={k: videos[0][k] for k in
                             ('path', 'name', 'size', 'sha256', 'full_decode_passed')})
            else:
                failure = videos[0] if len(videos) == 1 else {'code': 'no_downloadable_result'}
                count = value.get('download_attempts', 0) + 1
                value.update(download_attempts=count, error=failure.get('code'))
                if failure.get('code') == 'read_failed' or failure.get('retryable') or (failure.get('http_status') or 0) >= 500:
                    value['download_check_after'] = self.now() + retry_delay(count, failure.get('retry_after'))
                elif failure.get('code') in {'authentication_required', 'permission_denied', 'rate_limited'}:
                    raise ReplicaError(failure['code'], 'Translation download waiting', failure.get('http_status'),
                                       retry_after=failure.get('retry_after'))
                else:
                    value['stage'] = 'needs_review'
            self.state.put(rid, value)

    def _deliver_once(self):
        self.refresh_feishu()
        for value in self.state.all():
            if value['stage'] in {'logo_ready', 'downloaded', 'feishu_uploaded'}:
                if self.now() < value.get('download_check_after', 0):
                    continue
                rid = value['record_id']
                with self._claim(rid, {'logo_ready', 'downloaded', 'feishu_uploaded'}) as claimed:
                    if claimed is None or not self._eligible(rid):
                        continue
                    if claimed['stage'] == 'logo_ready':
                        self.download_all([rid])
                    current = self.state.get(rid)
                    if current['stage'] in {'downloaded', 'feishu_uploaded'} and not self.stop_requested():
                        self.deliver_records([current])
                    self.observe('deliver')
                    return True
        return False

    def _sync_progress(self):
        names = {'queued': '上传中', 'source_ready': '上传中', 'uploaded': '分析中',
                 'analysis_ready': '复刻中', 'logo_submitting': '复刻中', 'logo_accepted': '复刻中',
                 'logo_ready': '下载回填中', 'downloaded': '下载回填中', 'feishu_uploaded': '下载回填中',
                 'write_intent': '下载回填中', 'verified': '已完成', 'failed': '失败',
                 'source_failed': '失败', 'needs_review': '需人工处理'}
        for value in self.state.all():
            rid = value['record_id']
            row = record(self.feishu, self.route, rid)
            if fingerprint(row['fields']) != self.state.items[rid]['input_fingerprint']:
                continue
            fields = {'执行进度': names.get(value['stage'], '需人工处理'),
                      '执行说明': value.get('error') or value.get('visual_review_message') or value.get('last_message') or ''}
            if value.get('job_id'):
                fields['Replica任务ID'] = value['job_id']
            if value['stage'] == 'verified' and row['fields'].get('需求状态') in {'已验收', '结果返回'}:
                fields['需求状态'] = '结果返回'
                fields['完成时间'] = row['fields'].get('完成时间') or int(self.now() * 1000)
                fields['执行说明'] = value.get('visual_review_message') or '成品已下载、校验并回填；原素材保持'
            self._write_fields(rid, fields)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['status', 'plan', 'watch', 'pause'])
    parser.add_argument('--config', type=Path, default=CONFIG)
    parser.add_argument('--run-dir', type=Path, default=ROOT / 'data/replica-request-pool-worker')
    parser.add_argument('--background', action='store_true')
    parser.add_argument('--poll-interval', type=float, default=15)
    args = parser.parse_args(argv)
    settings = config(args.config)
    if args.action == 'pause':
        save_private(args.run_dir / 'pause-requested', {'requested_at': time.time()})
        print(json.dumps({'pause_requested': True})); return
    if args.action == 'status':
        path = args.run_dir / 'progress.json'
        print(json.dumps({'table_url': settings['table_url'], 'trigger_status': settings['trigger_status'],
                          'progress': daily.load(path) if path.exists() else None}, ensure_ascii=False)); return
    if args.action == 'plan':
        client = ReplicaFeishu()
        route = 'bitable/v1/apps/' + APP + '/tables/' + settings['request_table_id']
        rows = client.listing(route + '/records')
        print(json.dumps({'table_url': settings['table_url'], 'approved_record_ids': [
            v['record_id'] for v in rows if v['fields'].get('需求状态') == '已验收'],
            'business_submitted': False}, ensure_ascii=False)); return
    if not settings.get('watcher_enabled'):
        raise ValueError('Request-pool watcher is not enabled in the reviewed configuration')
    if not 1 <= args.poll_interval <= 300:
        raise ValueError('Polling interval must be between 1 and 300 seconds')
    if args.background:
        args.run_dir.mkdir(parents=True, exist_ok=True)
        with (args.run_dir / 'worker.log').open('a') as log:
            child = subprocess.Popen([sys.executable, '-u', '-m', 'ua_workflows.replica_request_pool', 'watch',
                                      '--config', str(args.config.resolve()), '--run-dir', str(args.run_dir.resolve()),
                                      '--poll-interval', str(args.poll_interval)], cwd=ROOT, stdin=subprocess.DEVNULL,
                                     stdout=log, stderr=log, start_new_session=True,
                                     env={**os.environ, 'PATH': '/opt/homebrew/bin:/usr/local/bin:' + os.environ.get('PATH', '')})
        print(json.dumps({'pid': child.pid, 'run_dir': str(args.run_dir)})); return
    args.run_dir.mkdir(parents=True, exist_ok=True)
    lock = os.open(args.run_dir / 'watcher.lock', os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(lock)
        print(json.dumps({'already_running': True, 'run_dir': str(args.run_dir)})); return
    try:
        state = PoolState(args.run_dir, settings)
        (state.root / 'pause-requested').unlink(missing_ok=True)
        worker = {'pid': os.getpid(), 'started_at': time.time(), 'phase': 'initializing',
                  'trigger_status': '已验收', 'table_id': settings['request_table_id']}
        save_private(state.root / 'worker.json', worker)
    except BaseException:
        os.close(lock)
        raise
    try:
        runner = RequestRunner(state, settings, poll_interval=args.poll_interval)
        runner._sync_progress()  # Publish durable failures from an interrupted watcher.
        for code in ('authentication_required', 'account_changed', 'permission_denied'):
            runner.health.clear(code)
        worker['phase'] = 'watching_manual_approvals'; save_private(state.root / 'worker.json', worker)
        runner.run()
        worker['phase'] = 'paused'
    except BaseException as error:
        worker.update(phase='needs_attention', error=getattr(error, 'code', type(error).__name__))
        raise
    finally:
        worker['finished_at'] = time.time(); save_private(state.root / 'worker.json', worker)
        os.close(lock)


if __name__ == '__main__':
    main()
