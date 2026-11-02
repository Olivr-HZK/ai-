"""CLI for the documented Replica analysis/Logo and optional translation interfaces."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import threading
import time
from urllib.parse import urljoin, urlsplit

from ua_workflows.replica_studio_api import (
    BASE_URL, ReplicaAPI, ReplicaError, digest, identifier, save_private, step_state,
)
from ua_workflows.shared.config import PROJECT_ROOT

DEFAULT_STATE = PROJECT_ROOT / 'data/replica-cli/state.json'


class Checkpoint:
    def __init__(self, path, account):
        self.path, self.account = Path(path), account
        self.mutex = threading.RLock()
        self.data = json.loads(self.path.read_text()) if self.path.exists() else {
            'version': 1, 'account_id': account, 'uploads': {}, 'logos': {}, 'downloads': {}}
        if self.data.get('version') != 1 or self.data.get('account_id') != account:
            raise ReplicaError('checkpoint_conflict', 'Checkpoint belongs to a different account or version')

    def get(self, section, key):
        with self.mutex:
            value = self.data.get(section, {}).get(key)
            return json.loads(json.dumps(value)) if value is not None else None

    def put(self, section, key, value):
        with self.mutex:
            candidate = json.loads(json.dumps(self.data))
            candidate.setdefault(section, {})[key] = value
            save_private(self.path, candidate)
            self.data = candidate


@contextmanager
def operation_lock(path):
    lock = Path(path).with_name(Path(path).name + '.lock')
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open('a') as stream:
        os.chmod(lock, 0o600)
        fcntl.flock(stream, fcntl.LOCK_EX)
        yield


def error_result(error):
    if isinstance(error, ReplicaError):
        return {'ok': False, 'code': error.code, 'message': str(error), 'http_status': error.status}
    if isinstance(error, OSError):
        return {'ok': False, 'code': 'local_io_failed', 'message': 'Local file operation failed'}
    return {'ok': False, 'code': 'invalid_input', 'message': 'Input or response could not be validated'}


def selected_ids(values):
    return list(dict.fromkeys(identifier(value) for value in values))


def upload_many(factory, paths, checkpoint, *, on_result=None):
    """Submit the whole selection serially; analysis concurrency belongs to Replica.

    Consume paths lazily so the record workflow can fetch a missing source just
    before its upload, rather than downloading the entire selection first.
    """
    results, seen, stopped, api = [], set(), False, None

    def upload_one(value):
        nonlocal stopped, api
        if stopped:
            return {'ok': False, 'path': value, 'code': 'not_submitted'}
        path = Path(value)
        if not path.is_file() or path.suffix.lower() not in {'.mp4', '.mov', '.webm', '.mkv'} or not 0 < path.stat().st_size <= 100 * 1024 * 1024:
            raise ReplicaError('invalid_source', 'Source must be an existing MP4/MOV/WebM/MKV video of at most 100 MiB')
        identity = {'path': value, 'name': path.name, 'bytes': path.stat().st_size, 'sha256': digest(path),
                    'variants': 1, 'character': '', 'auto_analyze': True}
        key = hashlib.sha256(value.encode()).hexdigest()
        old = checkpoint.get('uploads', key)
        if old and old.get('identity') != identity:
            raise ReplicaError('checkpoint_conflict', 'Source changed since its original upload intent')
        if old and old.get('stage') == 'uploaded':
            return {'ok': True, 'path': value, 'ids': old['ids'], 'request_id': old['request_id'], 'reused': True}
        if api is None:
            try:
                api = factory()
                auth = api.authenticate()
            except ReplicaError as error:
                stopped = True
                if error.code in {'read_failed', 'authentication_required', 'account_changed'}:
                    return {'ok': False, 'path': value, 'code': 'not_submitted', 'reason': error.code}
                raise
            if auth['account_id'] != checkpoint.account:
                stopped = True
                raise ReplicaError('account_changed', 'Upload account changed')
        if old and old.get('stage') != 'upload_not_submitted':
            # A previous intent always uses a read-only reconciliation; no second POST is sent.
            result = api.upload_status(old['request_id'])
            record = old
        else:
            record = old or {'identity': identity, 'request_id': secrets.token_hex(16)}
            record.update(stage='upload_intent', dispatch_started_at=time.time())
            try:
                checkpoint.put('uploads', key, record)
            except OSError:
                stopped = True
                raise
            try:
                result = api.upload(path, record['request_id'])
            except ReplicaError as error:
                record.update(stage='upload_unknown' if error.code == 'result_unknown' else 'upload_rejected', error=error.code,
                              dispatch_finished_at=time.time())
                try:
                    checkpoint.put('uploads', key, record)
                except OSError:
                    stopped = True
                    raise
                if error.code == 'result_unknown':
                    stopped = True
                    result = api.upload_status(record['request_id'])
                else:
                    if error.code in {'authentication_required', 'account_changed'}:
                        stopped = True
                    raise
        record.update(stage='uploaded', ids=result['ids'], dispatch_finished_at=time.time())
        try:
            checkpoint.put('uploads', key, record)
        except OSError:
            stopped = True
            raise
        return {'ok': True, 'path': value, 'ids': result['ids'], 'request_id': record['request_id'], 'recovered': bool(old)}

    for source in paths:
        value = str(Path(source).expanduser().resolve())
        if value in seen:
            continue
        seen.add(value)
        try:
            item = upload_one(value)
        except Exception as error:
            if isinstance(error, ReplicaError) and error.code in {'result_unknown', 'authentication_required', 'account_changed'}:
                stopped = True
            item = {'path': value, **error_result(error)}
        results.append(item)
        if on_result is not None:
            on_result(item)
    return {'operation': 'upload', 'results': results, 'selected': len(results),
            'accepted': sum(item['ok'] for item in results), 'stopped': stopped}


def logo_many(api, ids, checkpoint, *, on_result=None, options=None):
    ids, results, stopped = selected_ids(ids), [], False
    options = api.smart_options() if options is None else options
    for job_id in ids:
        try:
            if stopped:
                results.append({'id': job_id, 'ok': False, 'code': 'not_submitted'})
                continue
            record = checkpoint.get('logos', job_id)
            if record:
                results.append({'id': job_id, 'ok': record['stage'] == 'logo_accepted', 'code': record['stage'], 'reused': True})
                continue
            dispatched = False
            try:
                job = api.job(job_id)
                if job.get('source_kind') == 'playable':
                    results.append({'id': job_id, 'ok': False, 'code': 'playable_not_supported'})
                    continue
                if (job.get('logo_replace_view') or {}).get('state') in {'replaced', 'no_logo'}:
                    results.append({'id': job_id, 'ok': True, 'code': 'already_done'})
                    continue
                if step_state(job, 'analysis') != 'ready':
                    results.append({'id': job_id, 'ok': False, 'code': 'analysis_not_ready'})
                    continue
                record = {'stage': 'logo_intent', 'options': options, 'revision': job['revision']}
                try:
                    checkpoint.put('logos', job_id, record)
                except OSError:
                    stopped = True
                    raise
                dispatched = True
                result = api.smart_logo(job, options)
                record.update(stage='logo_accepted', receipt=result)
                checkpoint.put('logos', job_id, record)
                results.append({'id': job_id, 'ok': True, 'code': 'accepted', 'receipt': result})
            except Exception as error:
                if record:
                    uncertain = getattr(error, 'code', '') == 'result_unknown' or (dispatched and isinstance(error, OSError))
                    record.update(stage='logo_unknown' if uncertain else 'logo_needs_review',
                                  error=getattr(error, 'code', 'local_io_failed'))
                    try:
                        checkpoint.put('logos', job_id, record)
                    except OSError:
                        stopped = True
                    if uncertain:
                        stopped = True
                if getattr(error, 'code', '') in {'result_unknown', 'authentication_required', 'account_changed'}:
                    stopped = True
                results.append({'id': job_id, **error_result(error)})
        finally:
            if on_result is not None and results and results[-1]['id'] == job_id:
                on_result(results[-1])
    return {'operation': 'smart_logo', 'results': results, 'selected': len(ids), 'stopped': stopped}


PUBLIC_JOB_FIELDS = ('id', 'project_id', 'status', 'revision', 'message', 'error', 'last_action',
                     'dashboard', 'step_states', 'logo_replace_view', 'translation', 'source_story', 'video_route')


def status_many(api, ids, *, stage=None, timeout=3600, interval=3.5, include_analysis=False, wait=time.sleep):
    ids, started, pending, results = selected_ids(ids), time.monotonic(), set(ids), {}
    while pending:
        for job_id in list(pending):
            try:
                job = api.job(job_id)
                fields = PUBLIC_JOB_FIELDS + (('analysis',) if include_analysis else ())
                item = {'id': job_id, 'ok': True, 'job': {k: job[k] for k in fields if k in job}}
                state = step_state(job, stage) if stage else None
                if stage:
                    item['step_state'] = state
                    if state in {'error', 'review'}:
                        item.update(ok=False, code='step_failed' if state == 'error' else 'needs_review')
                results[job_id] = item
                if not stage or state in {'ready', 'error', 'review'}:
                    pending.remove(job_id)
            except ReplicaError as error:
                results[job_id] = {'id': job_id, **error_result(error)}
                if error.code in {'authentication_required', 'account_changed'}:
                    return {'operation': 'status', 'results': list(results.values()), 'pending': sorted(pending), 'state': 'authentication_required'}
                if not stage:
                    pending.remove(job_id)
        if not pending or not stage:
            break
        if time.monotonic() - started >= timeout:
            break
        wait(interval)
    return {'operation': 'status', 'results': [results[i] for i in ids if i in results],
            'pending': [i for i in ids if i in pending], 'state': 'waiting' if pending else 'settled'}


def decode_video(path):
    if not shutil.which('ffmpeg'):
        raise ReplicaError('dependency_missing', 'ffmpeg is required to verify downloaded videos')
    result = subprocess.run(['ffmpeg', '-v', 'error', '-xerror', '-i', str(path), '-map', '0:v:0', '-f', 'null', '-'],
                            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=600)
    if result.returncode:
        raise ReplicaError('invalid_media', 'Downloaded video failed full decoding')


def download_many(api, ids, checkpoint, output, *, plan_only=False, on_result=None):
    ids = selected_ids(ids)
    plan = api.download_plan(ids)
    if plan_only:
        return {'operation': 'download_plan', 'plan': plan}
    output, results = Path(output).expanduser().resolve(), []
    for project in plan.get('projects', []):
        for file in project.get('files', []):
            previous_count = len(results)
            try:
                if file.get('text') is not None:
                    continue
                address = urlsplit(urljoin(BASE_URL + '/', file.get('url', '')))
                parts = address.path.split('/')
                job_id = parts[3] if len(parts) >= 6 and parts[1:3] == ['api', 'jobs'] and parts[4] == 'files' else None
                if job_id not in ids:
                    raise ReplicaError('invalid_media', 'Download plan contains an unselected task')
                name = file.get('name')
                if not isinstance(name, str) or Path(name).name != name or '\\' in name or Path(name).suffix.lower() not in {'.mp4', '.mov', '.webm', '.mkv'}:
                    raise ReplicaError('invalid_media', 'Download plan contains an unsupported filename')
                key = hashlib.sha256(json.dumps([job_id, file['url'], name, file.get('bytes')]).encode()).hexdigest()
                previous = checkpoint.get('downloads', key)
                if previous:
                    saved = Path(previous['path'])
                    if saved.is_file() and saved.stat().st_size == previous['size'] and digest(saved) == previous['sha256']:
                        results.append({'id': job_id, 'ok': True, **previous, 'reused': True})
                        continue
                destination = output / job_id / name
                index = 2
                while destination.exists():
                    destination = output / job_id / f'{Path(name).stem}_{index}{Path(name).suffix}'
                    index += 1
                try:
                    receipt = api.download(job_id, file, destination)
                    decode_video(destination)
                    receipt['full_decode_passed'] = True
                    checkpoint.put('downloads', key, receipt)
                    results.append({'id': job_id, 'ok': True, **receipt})
                except Exception as error:
                    results.append({'id': job_id, 'name': name, **error_result(error)})
            finally:
                if on_result is not None and len(results) > previous_count:
                    on_result(results[-1])
    return {'operation': 'download', 'results': results, 'saved': sum(x['ok'] for x in results),
            'skipped': plan.get('skipped', []), 'missing': plan.get('missing', 0)}


def parser():
    p = argparse.ArgumentParser(description='Replica Studio CLI：自动分析、智能换 Logo、可选翻译、状态与下载。')
    p.add_argument('--session-file', type=Path, help='已授权会话的私密 JSON 文件（0600）；也可用项目 .env 中 REPLICA_SESSION_FILE')
    p.add_argument('--checkpoint', type=Path, default=DEFAULT_STATE, help='上传／付费动作／下载断点')
    sub = p.add_subparsers(dest='command', required=True)
    login = sub.add_parser('login', help='打开CLI专用SSO窗口；登录后业务仅发送HTTP请求，不导出Cookie')
    login.add_argument('--timeout', type=float, default=600, help='等待用户完成SSO的秒数')
    login.add_argument('--probe', action='store_true', help='仅核对文档OIDC入口，不打开登录窗口')
    sub.add_parser('session', help='验证当前 HTTP 会话，返回认证状态，不输出 Cookie/CSRF')
    upload = sub.add_parser('upload', help='单线程连续提交全部文件，每文件1组、自动分析，由服务端排队处理')
    upload.add_argument('files', nargs='+', type=Path)
    uploaded = sub.add_parser('upload-status', help='只读核对原上传标识；404不代表未发生')
    uploaded.add_argument('request_id')
    status = sub.add_parser('status', help='读取多任务状态；可等待分析或智能换 Logo 完成')
    status.add_argument('ids', nargs='+')
    status.add_argument('--wait-for', choices=['analysis', 'logo', 'translation'])
    status.add_argument('--languages', nargs='+', help='等待翻译时指定语言，如 de ja')
    status.add_argument('--parts', nargs='+', choices=['text', 'voice', 'subtitles'], default=['voice'])
    status.add_argument('--source-language', help='原语言代码；原语言字幕查询时可指定')
    status.add_argument('--timeout', type=float, default=3600)
    status.add_argument('--interval', type=float, default=3.5)
    status.add_argument('--analysis', action='store_true', help='包含文档公开的分析视图')
    logo = sub.add_parser('logo', help='多选智能 AI 换 Logo；逐任务入队，不重复已提交付费动作')
    logo.add_argument('ids', nargs='+')
    translate = sub.add_parser('translate', help='多任务翻译文字／语音或仅字幕；默认只语音，不强制重做')
    translate.add_argument('ids', nargs='+')
    translate.add_argument('--languages', nargs='+', required=True)
    translate.add_argument('--parts', nargs='+', choices=['text', 'voice', 'subtitles'], default=['voice'])
    translate.add_argument('--source-language')
    translate.add_argument('--source-video', action='store_true', help='明确改用可用的原视频剧情部分，不生成人物图或换人')
    translate.add_argument('--check-only', action='store_true', help='只读预检，不切换路线或提交翻译')
    download = sub.add_parser('download', help='多选下载Logo或翻译视频／字幕，不另行拼接或生成')
    download.add_argument('ids', nargs='+')
    download.add_argument('--step', choices=['logo', 'translation'], default='logo')
    download.add_argument('--languages', nargs='+', help='翻译结果的语言；logo步骤仅source')
    download.add_argument('--output-dir', type=Path, default=PROJECT_ROOT / 'data/replica-cli/results')
    download.add_argument('--plan-only', action='store_true', help='只返回多ID保存计划')
    return p


def execute(args, factory=None):
    factory = factory or (lambda: ReplicaAPI.configured(session_file=args.session_file))
    if args.command == 'login':
        if args.probe:
            return ReplicaAPI().login_start()
        if args.timeout <= 0:
            raise ReplicaError('invalid_input', 'Login timeout must be positive')
        from ua_workflows.replica_auth_bridge import start_login
        status = start_login(timeout=args.timeout)
        return {'operation': 'login', 'cli_authenticated': bool(status.get('authenticated')), **status}
    api = factory()
    auth = api.authenticate()
    if args.command == 'session':
        return {'operation': 'session', 'authenticated': True, **auth}
    if args.command == 'upload-status':
        return {'operation': 'upload_status', **api.upload_status(args.request_id)}
    if args.command == 'status':
        if args.timeout <= 0 or args.interval <= 0:
            raise ReplicaError('invalid_input', 'Timeout and polling interval must be positive')
        if args.wait_for == 'translation':
            from ua_workflows.replica_translation import translation_status_many
            return translation_status_many(api, args.ids, args.languages, args.parts, args.source_language,
                                           timeout=args.timeout, interval=args.interval)
        return status_many(api, args.ids, stage=args.wait_for, timeout=args.timeout, interval=args.interval, include_analysis=args.analysis)
    if args.command == 'translate' and args.check_only:
        from ua_workflows.replica_translation import translate_many
        return translate_many(api, args.ids, None, args.languages, args.parts, args.source_language,
                              source_video=args.source_video, check_only=True)
    with operation_lock(args.checkpoint):
        checkpoint = Checkpoint(args.checkpoint, auth['account_id'])
        if args.command == 'upload':
            return upload_many(factory, args.files, checkpoint)
        if args.command == 'logo':
            return logo_many(api, args.ids, checkpoint)
        if args.command == 'translate':
            from ua_workflows.replica_translation import translate_many
            return translate_many(api, args.ids, checkpoint, args.languages, args.parts, args.source_language,
                                  source_video=args.source_video)
        if args.command == 'download':
            if args.step == 'translation':
                from ua_workflows.replica_translation import download_translations
                return download_translations(api, args.ids, checkpoint, args.output_dir, args.languages, plan_only=args.plan_only)
            if args.languages and args.languages != ['source']:
                raise ReplicaError('invalid_selection', 'Logo download only supports source language')
            return download_many(api, args.ids, checkpoint, args.output_dir, plan_only=args.plan_only)
    raise ReplicaError('invalid_input', 'Unknown command')


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        result = execute(args)
        print(json.dumps(result, ensure_ascii=False))
        if (result.get('status') == 'browser_login_required' or result.get('state') == 'authentication_required' or
                result.get('operation') == 'login' and not result.get('cli_authenticated')):
            return 3
        if result.get('pending'):
            return 4
        if any(not item.get('ok') for item in result.get('results', [])) or result.get('missing') or result.get('skipped'):
            return 2
        return 0
    except (ReplicaError, OSError, ValueError) as error:
        print(json.dumps(error_result(error), ensure_ascii=False))
        return 3 if getattr(error, 'code', '') in {'authentication_required', 'account_changed'} else 2


if __name__ == '__main__':
    raise SystemExit(main())
