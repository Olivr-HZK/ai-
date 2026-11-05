"""Main-table Logo requests and multilingual translation after mandatory Logo."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from ua_workflows import social_chat_browser_daily as daily
from ua_workflows.replica_request_pool import RequestRunner, PoolState, DemandBlocked, plan_request, fingerprint, record, LANGUAGES
from ua_workflows.replica_main_translation import MainTranslationMixin
from ua_workflows.replica_studio_api import ReplicaError, save_private
from ua_workflows.social_chat_media_orientation import POLICY
from ua_workflows.social_chat_replica import APP, TABLE, State, ReplicaFeishu, source_identity, TERMINAL
from ua_workflows.replica_recovery import UNKNOWN
from ua_workflows.shared.config import DATA_DIR

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / 'config/replica_main_table.json'
TYPE, LANGUAGE, PORTRAIT = '复刻需求类型', '复刻目标语言', '复刻人物参考图'
STATUS, PROGRESS, MESSAGE = '复刻需求状态', '复刻执行进度', '复刻执行说明'
EXECUTION, JOB = '复刻执行标识', '复刻任务ID'
SOURCE, RESULT = '竞品素材（图片/视频/Playable）去除男频', '复刻附件'
ACTIONS = ['替换logo', '替换语言']


def config(path=CONFIG):
    v = daily.load(path)
    if (v.get('app_token') != APP or v.get('table_id') != TABLE
            or v.get('trigger_mode') != 'request_type' or v.get('trigger_field') != TYPE
            or v.get('trigger_values') != ACTIONS
            or v.get('source_field') != SOURCE or v.get('target_field') != RESULT
            or v.get('allowed_actions') != ACTIONS or v.get('translation_requires_logo') is not True):
        raise ValueError('Main-table request destination or type trigger changed')
    return v


def request_fields(f, rid):
    return {'关联原素材': [rid], '需求类型': f.get(TYPE), '目标语言': f.get(LANGUAGE) or [],
            '人物参考图': f.get(PORTRAIT) or [], '处理来源': '原素材', '目标品牌': 'Afizzy'}


def input_fingerprint(row):
    return fingerprint(request_fields(row['fields'], row['record_id']))


def plan(row):
    f, rid = row['fields'], row['record_id']
    if f.get(TYPE) not in ACTIONS:
        raise DemandBlocked('当前仅开放替换logo和替换语言，未提交其他动作')
    if f.get(RESULT):
        raise DemandBlocked('本行已有复刻附件，保留现有结果，不自动覆盖或重复提交')
    fields = request_fields(f, rid)
    languages = fields['目标语言']
    if f.get(TYPE) == '替换语言':
        if (not isinstance(languages, list) or not languages
                or any(lang not in LANGUAGES for lang in languages)):
            raise DemandBlocked('请先在复刻目标语言选择至少一个支持的语言，补齐后会自动继续')
        fields = {**fields, '目标语言': languages[:1]}
    value = plan_request({'record_id': rid, 'fields': fields}, row)
    if f.get(TYPE) == '替换语言':
        value['languages'] = list(dict.fromkeys(LANGUAGES[x] for x in languages))
        value['pipeline'] = ['logo', 'translation']
    value['input_fingerprint'] = input_fingerprint(row)
    value.pop('projection', None)
    return value


class MainState(PoolState):
    def __init__(self, root, settings):
        root = Path(root).resolve()
        if not (root / 'scope.json').exists():
            save_private(root / 'scope.json', {'version': 1, 'workflow': 'main_table_replica',
                'app_token': APP, 'table_id': TABLE, 'target_date': 'manual-main-table', 'items': [],
                'source_field': SOURCE, 'target_field': RESULT, 'date_field': '目标日期',
                'media_type_field': '素材类型', 'media_orientation_policy': POLICY})
        State.__init__(self, root)
        if (self.scope.get('workflow') != 'main_table_replica' or self.scope.get('table_id') != TABLE
                or self.scope.get('app_token') != APP or self.scope.get('source_field') != SOURCE
                or self.scope.get('target_field') != RESULT):
            raise ValueError('Main-table runtime belongs to another workflow')


class MainRunner(MainTranslationMixin, RequestRunner):
    def observe(self, phase, **extra):
        if not any(v['stage'] not in TERMINAL or v['stage'] in UNKNOWN for v in self.state.all()):
            return self.state.summary(scheduling='main_table', batch_phase='waiting_request', **extra)
        # Skip RequestRunner's historical pool label; use the shared pipeline watchdog.
        return super(RequestRunner, self).observe(phase, **extra)

    def destination_route(self):
        return f'bitable/v1/apps/{APP}/tables/{TABLE}'

    def validate_destination_fields(self, schema):
        fields = {f['field_name']: f for f in schema}
        types = {TYPE: 3, LANGUAGE: 4, PORTRAIT: 17, STATUS: 3, PROGRESS: 3, MESSAGE: 1,
                 EXECUTION: 1, JOB: 1, SOURCE: 17, RESULT: 17}
        for name, kind in types.items():
            if (fields.get(name, {}).get('field_id') != self.settings['fields'].get(name)
                    or fields[name]['type'] != kind):
                raise ValueError('Main-table request field identity/type changed: ' + name)

    def requested_rows(self):
        rows, cursor = [], None
        while True:
            params = {'page_size': 100}
            if cursor:
                params['page_token'] = cursor
            try:
                value = self.feishu.call('POST', self.route + '/records/search', params=params, json={
                    'filter': {'conjunction': 'and', 'conditions': [
                        {'field_name': TYPE, 'operator': 'isNotEmpty', 'value': []}]}})
            except ValueError as error:
                if 'code=1254607' in str(error):
                    raise ReplicaError('read_failed', 'Main-table request query temporarily not ready') from None
                raise
            # Options can be removed from the live select field. Referring to
            # a removed option makes Feishu reject the entire filter. Query
            # nonempty requests, then retain the exact supported actions here.
            for item in value.get('items') or []:
                row = daily.normalize_record_texts(item)
                if row['fields'].get(TYPE) in ACTIONS:
                    rows.append(row)
            if not value.get('has_more'):
                return rows
            next_cursor = value.get('page_token')
            if not next_cursor or next_cursor == cursor:
                raise ReplicaError('read_failed', 'Main-table query did not advance')
            cursor = next_cursor

    def _admit(self):
        self.refresh_feishu()
        for candidate in self.requested_rows():
            rid = candidate['record_id']
            if rid in self.state.items:
                continue
            row = record(self.feishu, self.route, rid)
            f = row['fields']
            if f.get(TYPE) not in ACTIONS or f.get(RESULT):
                continue
            try:
                item = plan(row)
                if item['action'] == '替换语言':
                    settings = self.api.translation_settings()
                    if not settings.get('available'):
                        raise DemandBlocked('平台翻译服务暂不可用，未上传视频或提交翻译')
                    if any(lang not in settings.get('enabled', item['languages']) for lang in item['languages']):
                        raise DemandBlocked('所选目标语言尚未在平台启用，未上传；请更换语言或联系平台维护')
                path = self.state.root / 'admissions' / (rid + '.json')
                if path.exists():
                    saved = daily.load(path)
                    if (saved['input_fingerprint'] != input_fingerprint(row) or saved['source'] != item['source']
                            or f.get(EXECUTION) not in (None, '', saved['execution_id'])):
                        raise DemandBlocked('原需求参数已变化，保留原执行记录，需先核对')
                    item = saved
                elif f.get(EXECUTION):
                    raise DemandBlocked('本行已有执行标识，但本机缺少原断点，先核对，不重复提交')
                else:
                    item.update(execution_id=str(uuid.uuid4()), requested_at=self.now(), trigger_mode='request_type')
                    save_private(path, item)
                current = self._write_fields(rid, {EXECUTION: item['execution_id'],
                    PROGRESS: '上传中', MESSAGE: '已检测到' + item['action'] + '需求，等待上传及自动分析'})
                cf = current['fields']
                if (cf.get(TYPE) != item['action'] or cf.get(EXECUTION) != item['execution_id']
                        or input_fingerprint(current) != item['input_fingerprint'] or cf.get(RESULT)):
                    raise DemandBlocked('领取期间本行需求、参数或结果已变化，未提交')
                with self._guard:
                    self.state.add(item)
            except DemandBlocked as error:
                self._write_fields(rid, {PROGRESS: '需人工处理', MESSAGE: str(error)})

    def _eligible(self, rid):
        self.refresh_feishu()
        item = self.state.items[rid]
        row = record(self.feishu, self.route, rid)
        f = row['fields']; files = f.get(SOURCE) or []
        valid = (item['action'] in ACTIONS and f.get(TYPE) == item['action']
                 and f.get(EXECUTION) == item['execution_id']
                 and input_fingerprint(row) == item['input_fingerprint'] and len(files) == 1
                 and source_identity(files[0]) == item['source'])
        value = self.state.get(rid)
        if f.get(RESULT):
            if value.get('delivery_kind') == 'translation_multi':
                receipts = value.get('attachment_receipts') or []
                valid = valid and len(f[RESULT]) == len(receipts) and bool(receipts) and {
                    (x.get('file_token'), x.get('name'), x.get('size')) for x in f[RESULT]} == {
                    (x['token'], x['name'], x['size']) for x in receipts}
            else:
                valid = valid and len(f[RESULT]) == 1 and f[RESULT][0].get('file_token') == value.get('feishu_token')
        if not valid:
            value.update(stage='needs_review', error='本行需求、参数或原附件已变化，停止新增操作')
            self.state.put(rid, value)
        return valid

    def _sync_progress(self):
        names = {'queued': '上传中', 'source_ready': '上传中', 'uploaded': '分析中',
                 'analysis_ready': '复刻中', 'logo_submitting': '复刻中', 'logo_accepted': '复刻中',
                 'logo_ready': '下载回填中', 'downloaded': '下载回填中', 'feishu_uploaded': '下载回填中',
                 'write_intent': '下载回填中', 'verified': '已完成', 'failed': '失败',
                 'source_failed': '失败', 'needs_review': '需人工处理'}
        self.refresh_feishu()
        rows = {r['record_id']: r for r in self.requested_rows()}
        for v in self.state.all():
            rid = v['record_id']; row = rows.get(rid)
            if row is None:
                continue
            if (input_fingerprint(row) != self.state.items[rid]['input_fingerprint']
                    or row['fields'].get(EXECUTION) != self.state.items[rid]['execution_id']):
                continue
            fields = {PROGRESS: names.get(v['stage'], '需人工处理'),
                      MESSAGE: v.get('error') or v.get('last_message') or v.get('visual_review_message') or ''}
            if v.get('job_id'):
                fields[JOB] = v['job_id']
            if v['stage'] == 'verified':
                fields[STATUS] = '结果返回'
                fields[MESSAGE] = v.get('visual_review_message') or '成品已下载核验并写回本行复刻附件'
            if any(row['fields'].get(k) != value for k, value in fields.items()):
                self._write_fields(rid, fields)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=['watch', 'pause', 'status', 'plan'])
    p.add_argument('--config', type=Path, default=CONFIG)
    p.add_argument('--run-dir', type=Path, default=DATA_DIR / 'replica-main-table-worker')
    p.add_argument('--poll-interval', type=float, default=15)
    p.add_argument('--background', action='store_true')
    args = p.parse_args(argv); settings = config(args.config)
    if args.action == 'pause':
        save_private(args.run_dir / 'pause-requested', {'requested_at': time.time()}); return
    if args.action == 'status':
        w = args.run_dir / 'worker.json'; progress = args.run_dir / 'progress.json'
        print(json.dumps({'worker': daily.load(w) if w.exists() else None,
                          'progress': daily.load(progress) if progress.exists() else None}, ensure_ascii=False)); return
    if args.action == 'plan':
        # Read-only, without initializing a Replica account or creating runtime scope.
        obj = MainRunner.__new__(MainRunner); obj.feishu = ReplicaFeishu(); obj.route = obj.destination_route()
        print(json.dumps({'requested_record_ids': [r['record_id'] for r in obj.requested_rows()],
                          'business_submitted': False, 'table_id': TABLE}, ensure_ascii=False)); return
    if not settings.get('watcher_enabled') or not 1 <= args.poll_interval <= 300:
        raise ValueError('Main-table watcher is disabled or polling interval invalid')
    args.run_dir.mkdir(parents=True, exist_ok=True)
    if args.background:
        with (args.run_dir / 'worker.log').open('a') as log:
            child = subprocess.Popen([sys.executable, '-u', '-m', 'ua_workflows.replica_main_table', 'watch',
                '--config', str(args.config.resolve()), '--run-dir', str(args.run_dir.resolve()),
                '--poll-interval', str(args.poll_interval)], cwd=ROOT, stdin=subprocess.DEVNULL,
                stdout=log, stderr=log, start_new_session=True,
                env={**os.environ, 'PATH': '/opt/homebrew/bin:/usr/local/bin:' + os.environ.get('PATH', '')})
        print(json.dumps({'pid': child.pid, 'run_dir': str(args.run_dir)})); return
    fd = os.open(args.run_dir / 'watcher.lock', os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd); print(json.dumps({'already_running': True})); return
    worker = {'pid': os.getpid(), 'started_at': time.time(), 'phase': 'initializing',
              'table_id': TABLE, 'trigger_mode': 'request_type', 'trigger_field': TYPE,
              'trigger_values': ACTIONS, 'demand_pool_bound': False,
              'auth_mode': settings.get('auth_mode')}
    save_private(args.run_dir / 'worker.json', worker)
    try:
        (args.run_dir / 'pause-requested').unlink(missing_ok=True)
        runner = MainRunner(MainState(args.run_dir, settings), settings, poll_interval=args.poll_interval)
        runner._sync_progress()
        worker['phase'] = 'watching_main_table'; save_private(args.run_dir / 'worker.json', worker)
        runner.run(); worker['phase'] = 'paused'
    except BaseException as error:
        worker.update(phase='needs_attention', error=getattr(error, 'code', type(error).__name__)); raise
    finally:
        worker['finished_at'] = time.time(); save_private(args.run_dir / 'worker.json', worker); os.close(fd)


if __name__ == '__main__': main()
