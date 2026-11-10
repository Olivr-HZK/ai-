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
import sys
import threading
import time
from urllib.parse import urljoin, urlsplit

import requests

from ua_workflows.replica_studio_api import (
    BASE_URL, ReplicaAPI, ReplicaError, digest, identifier, logo_review_fields, save_private, step_state,
)
from ua_workflows.shared.config import PROJECT_ROOT, DATA_DIR

DEFAULT_STATE = DATA_DIR / 'replica-cli/state.json'


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
        value = {'ok': False, 'code': error.code, 'message': str(error), 'http_status': error.status}
        if error.retry_after is not None:
            value['retry_after'] = error.retry_after
        if error.request_not_applied:
            value['request_not_applied'] = True
        if error.retryable:
            value['retryable'] = True
        return value
    if isinstance(error, requests.RequestException):
        return {'ok': False, 'code': 'read_failed', 'message': 'Media transport interrupted'}
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
                if error.code in {'read_failed', 'authentication_required', 'account_changed', 'permission_denied',
                                  'rate_limited', 'temporarily_rejected'}:
                    return {**error_result(error), 'path': value, 'code': 'not_submitted', 'reason': error.code}
                raise
            if auth['account_id'] != checkpoint.account:
                stopped = True
                raise ReplicaError('account_changed', 'Upload account changed')
        retryable = old and old.get('stage') == 'upload_retry' and old.get('request_not_applied') is True
        if retryable and time.time() < old.get('retry_at', 0):
            return {'ok': False, 'path': value, 'code': 'retry_wait',
                    'retry_after': old['retry_at'] - time.time(), 'request_not_applied': True}
        if old and old.get('stage') != 'upload_not_submitted' and not retryable:
            # A previous intent always uses a read-only reconciliation; no second POST is sent.
            result = api.upload_status(old['request_id'])
            record = old
        else:
            record = old or {'identity': identity, 'request_id': secrets.token_hex(16)}
            if retryable:
                record.setdefault('rejected_attempts', []).append({k: record.get(k) for k in
                    ('dispatch_started_at', 'dispatch_finished_at', 'error', 'http_status', 'request_not_applied')})
            record.update(stage='upload_intent', dispatch_started_at=time.time(),
                          attempts=record.get('attempts', 0) + 1)
            # Old rejection proof cannot authorize replay of this new dispatch.
            record.pop('request_not_applied', None)
            record.pop('retry_at', None)
            try:
                checkpoint.put('uploads', key, record)
            except OSError:
                stopped = True
                raise
            try:
                result = api.upload(path, record['request_id'])
            except ReplicaError as error:
                safe_retry = error.request_not_applied and error.code in {'rate_limited', 'temporarily_rejected'}
                record.update(stage='upload_retry' if safe_retry else 'upload_unknown' if error.code == 'result_unknown' else 'upload_rejected', error=error.code,
                              http_status=error.status,
                              dispatch_finished_at=time.time())
                if safe_retry:
                    from ua_workflows.replica_recovery import retry_delay
                    record.update(request_not_applied=True, retry_at=time.time() + retry_delay(record['attempts'], error.retry_after))
                try:
                    checkpoint.put('uploads', key, record)
                except OSError:
                    stopped = True
                    raise
                if error.code == 'result_unknown':
                    stopped = True
                    result = api.upload_status(record['request_id'])
                else:
                    if error.code in {'authentication_required', 'account_changed', 'permission_denied', 'rate_limited', 'temporarily_rejected'}:
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
            if isinstance(error, ReplicaError) and error.code in {'result_unknown', 'authentication_required', 'account_changed', 'permission_denied', 'rate_limited', 'temporarily_rejected'}:
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
                    results.append({'id': job_id, 'ok': True, 'code': 'already_done', **logo_review_fields(job)})
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
                if getattr(error, 'code', '') in {'result_unknown', 'authentication_required', 'account_changed', 'permission_denied', 'rate_limited', 'temporarily_rejected'}:
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
                if error.code in {'authentication_required', 'account_changed', 'permission_denied'}:
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
    p = argparse.ArgumentParser(description='Replica Studio CLI：自动分析、人物参考图上传、智能换 Logo、可选翻译、状态与下载。')
    credentials = p.add_mutually_exclusive_group()
    credentials.add_argument('--token-file', type=Path, help='设备授权令牌文件（0600）；默认 ~/.config/replica-studio/cli.json')
    credentials.add_argument('--session-file', type=Path, help='兼容旧 Cookie 会话文件（0600）')
    p.add_argument('--auth-mode', choices=['device', 'browser'], help='覆盖项目REPLICA_AUTH_MODE；browser使用专用Chrome持久化会话')
    p.add_argument('--checkpoint', type=Path, default=DEFAULT_STATE, help='上传／付费动作／下载断点')
    sub = p.add_subparsers(dest='command', required=True)
    login = sub.add_parser('login', help='按项目鉴权配置登录：专用Chrome持久化会话或设备授权')
    login.add_argument('--timeout', type=float, default=600, help='等待完成登录或设备授权的秒数')
    login.add_argument('--probe', action='store_true', help='兼容诊断：仅核对旧OIDC入口，不启动设备授权')
    login.add_argument('--label', help='本机令牌的名称')
    login.add_argument('--no-browser', action='store_true', help='显示授权地址与短码，不自动打开浏览器')
    login.add_argument('--legacy-browser', action='store_true', help='显式使用旧CLI专用Chrome登录桥')
    sub.add_parser('session', help='验证当前认证，返回账号状态，不输出令牌/Cookie/CSRF')
    sub.add_parser('whoami', help='查询当前认证账号；不输出原始令牌或会话值')
    sub.add_parser('logout', help='仅删除本机设备令牌；服务端撤销请到工作台 /cli 页面')
    upload = sub.add_parser('upload', help='单线程连续提交全部文件，每文件1组、自动分析，由服务端排队处理')
    upload.add_argument('files', nargs='+', type=Path)
    uploaded = sub.add_parser('upload-status', help='只读核对原上传标识；404不代表未发生')
    uploaded.add_argument('request_id')
    status = sub.add_parser('status', help='读取多任务状态；可等待分析或智能换 Logo 完成')
    status.add_argument('ids', nargs='+')
    status.add_argument('--wait-for', choices=['analysis', 'logo', 'translation'])
    status.add_argument('--languages', nargs='+', help='等待翻译时指定语言，如 de ja')
    status.add_argument('--parts', nargs='+', choices=['text', 'voice', 'subtitles'], default=['voice'])
    status.add_argument('--input', dest='translation_input', choices=['original', 'swap'], default='original',
                        help='等待翻译时核对实际视频输入；默认完整原视频')
    status.add_argument('--source-language', help='原语言代码；原语言字幕查询时可指定')
    status.add_argument('--timeout', type=float, default=3600)
    status.add_argument('--interval', type=float, default=3.5)
    status.add_argument('--analysis', action='store_true', help='包含文档公开的分析视图')
    logo = sub.add_parser('logo', help='多选智能 AI 换 Logo；逐任务入队，不重复已提交付费动作')
    logo.add_argument('ids', nargs='+')
    logo.add_argument('--yes', action='store_true', help='设备授权模式下确认提交付费智能Logo动作')
    characters = sub.add_parser('characters', help='只读列出任务角色ID及当前人物参考图')
    characters.add_argument('id')
    portrait = sub.add_parser('portrait', help='上传你提供的人物图并替换指定角色参考图，不生成图片或视频')
    portrait.add_argument('id')
    portrait.add_argument('image', type=Path)
    portrait.add_argument('--source-id', help='多角色任务必须指定；单角色自动选择唯一角色')
    portrait.add_argument('--replace', action='store_true', help='允许更换此前已确认的参考图；未知上传仍只读核对')
    portrait.add_argument('--check-only', action='store_true', help='只检查图片与角色映射，不上传或写断点')
    translate = sub.add_parser('translate', help='仅翻译完整当前视频的口播，保留全片内容')
    translate.add_argument('ids', nargs='+')
    translate.add_argument('--languages', nargs='+', required=True)
    translate.add_argument('--parts', nargs='+', choices=['voice'], default=['voice'], help='兼容参数，固定为voice')
    translate.add_argument('--source-language')
    translate.add_argument('--input', dest='translation_input', choices=['original', 'swap'], default='original',
                           help='original整条当前原视频（含换Logo版本）；swap已生成的换人片段')
    translate.add_argument('--source-video', action='store_true', help='兼容别名：等同--input original，不再切换剧情路线')
    translate.add_argument('--check-only', action='store_true', help='只读预检，不切换路线或提交翻译')
    translate.add_argument('--yes', action='store_true', help='设备授权模式下确认提交付费翻译动作')
    download = sub.add_parser('download', help='多选下载Logo或翻译视频／字幕，不另行拼接或生成')
    download.add_argument('ids', nargs='+')
    download.add_argument('--step', choices=['logo', 'translation'], default='logo')
    download.add_argument('--languages', nargs='+', help='翻译结果的语言；logo步骤仅source')
    download.add_argument('--output-dir', type=Path, default=DATA_DIR / 'replica-cli/results')
    download.add_argument('--plan-only', action='store_true', help='只返回多ID保存计划')
    return p


def execute(args, factory=None):
    factory = factory or (lambda: ReplicaAPI.configured(token_file=args.token_file, session_file=args.session_file,
                                                       auth_mode=args.auth_mode, confirm_paid=getattr(args, 'yes', False)))
    if args.command == 'login':
        if args.probe:
            return ReplicaAPI().login_start()
        if args.timeout <= 0:
            raise ReplicaError('invalid_input', 'Login timeout must be positive')
        from ua_workflows.shared.config import project_env_values
        mode = args.auth_mode or ('device' if args.token_file else project_env_values().get('REPLICA_AUTH_MODE') or 'device')
        if args.legacy_browser or mode == 'browser':
            if args.token_file:
                raise ReplicaError('invalid_input', 'Legacy browser login does not write a CLI token file')
            from ua_workflows.replica_auth_bridge import start_login
            status = start_login(timeout=args.timeout)
            return {'operation': 'login', 'cli_authenticated': bool(status.get('authenticated')), **status}
        if args.session_file:
            raise ReplicaError('invalid_input', 'Device login saves a token file; omit --session-file')
        from ua_workflows.replica_device_auth import start_device_login
        def show_approval(value):
            print('请在浏览器核对授权码 ' + value['user_code'] + ' 后批准本机：\n' + value['verification_uri'],
                  file=sys.stderr, flush=True)
        return start_device_login(path=args.token_file, label=args.label, timeout=args.timeout,
                                  no_browser=args.no_browser, on_prompt=show_approval)
    if args.command == 'logout':
        if args.session_file:
            raise ReplicaError('invalid_input', 'logout only removes a device token, not a legacy session')
        from ua_workflows.replica_device_auth import logout
        return logout(args.token_file)
    api = factory()
    auth = api.authenticate()
    if args.command in {'session', 'whoami'}:
        return {'operation': 'session', 'authenticated': True, **auth}
    if (auth.get('auth_mode') == 'device' and args.command in {'logo', 'translate'}
            and not getattr(args, 'check_only', False) and not args.yes):
        raise ReplicaError('confirmation_required', 'Device-authorized paid actions require --yes')
    if args.command == 'upload-status':
        return {'operation': 'upload_status', **api.upload_status(args.request_id)}
    if args.command == 'characters':
        from ua_workflows.replica_portraits import character_list
        return character_list(api, args.id)
    if args.command == 'portrait' and args.check_only:
        from ua_workflows.replica_portraits import replace_portrait
        return replace_portrait(api, args.id, args.image, None, source_id=args.source_id, check_only=True)
    if args.command == 'status':
        if args.timeout <= 0 or args.interval <= 0:
            raise ReplicaError('invalid_input', 'Timeout and polling interval must be positive')
        if args.wait_for == 'translation':
            from ua_workflows.replica_translation import translation_status_many
            return translation_status_many(api, args.ids, args.languages, args.parts, args.source_language,
                                           timeout=args.timeout, interval=args.interval,input_mode=args.translation_input,
                                           trim={'remove_cta':False,'remove_gameplay':False} if args.translation_input=='original' else None)
        return status_many(api, args.ids, stage=args.wait_for, timeout=args.timeout, interval=args.interval, include_analysis=args.analysis)
    if args.command == 'translate' and args.check_only:
        from ua_workflows.replica_translation import translate_voice_many
        if args.source_video and args.translation_input != 'original':
            raise ReplicaError('invalid_translation', '--source-video cannot select swap input')
        return translate_voice_many(api, args.ids, None, args.languages, args.source_language,
                                    input_mode=args.translation_input, check_only=True)
    with operation_lock(args.checkpoint):
        checkpoint = Checkpoint(args.checkpoint, auth['account_id'])
        if args.command == 'upload':
            return upload_many(factory, args.files, checkpoint)
        if args.command == 'logo':
            return logo_many(api, args.ids, checkpoint)
        if args.command == 'portrait':
            from ua_workflows.replica_portraits import replace_portrait
            return replace_portrait(api, args.id, args.image, checkpoint, source_id=args.source_id, replace=args.replace)
        if args.command == 'translate':
            from ua_workflows.replica_translation import translate_voice_many
            if args.source_video and args.translation_input != 'original':
                raise ReplicaError('invalid_translation', '--source-video cannot select swap input')
            return translate_voice_many(api, args.ids, checkpoint, args.languages, args.source_language,
                                        input_mode=args.translation_input)
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
        if result.get('ok') is False or any(not item.get('ok') for item in result.get('results', [])) or result.get('missing') or result.get('skipped'):
            return 2
        return 0
    except (ReplicaError, OSError, ValueError) as error:
        print(json.dumps(error_result(error), ensure_ascii=False))
        return 3 if getattr(error, 'code', '') in {'authentication_required', 'account_changed', 'permission_denied', 'authorization_denied'} else 2


if __name__ == '__main__':
    raise SystemExit(main())
