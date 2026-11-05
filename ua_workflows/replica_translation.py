"""Opt-in translation helpers. No collection, Feishu writes or extra generation."""
from __future__ import annotations

import hashlib
import json
import os
import math
from pathlib import Path
import subprocess
import time
from urllib.parse import urljoin, urlsplit, unquote

from ua_workflows.replica_studio_api import (
    BASE_URL, BUSY, ReplicaError, TRANSLATION_LANGUAGES, digest, identifier, media_url, translation_options,
)
from ua_workflows.replica_cli import decode_video, error_result, selected_ids


def video_duration(path):
    try:
        result = subprocess.run(['ffprobe', '-v', 'error', '-select_streams', 'v:0',
            '-show_entries', 'stream=duration:format=duration', '-of', 'json', str(path)],
            capture_output=True, text=True, timeout=30, check=True)
        info = json.loads(result.stdout)
        streams = info.get('streams') or []
        raw = streams[0].get('duration') if streams else None
        duration = float(raw if raw not in (None, 'N/A') else info['format']['duration'])
        if not math.isfinite(duration) or duration <= 0:
            raise ValueError('Invalid duration')
        return duration
    except (OSError, ValueError, KeyError, IndexError, subprocess.SubprocessError) as error:
        raise ReplicaError('invalid_media', 'Video duration could not be verified') from error


def translation_input(job, *, source_video=False, input_mode=None):
    source = job.get('source_story') or {}
    segments = job.get('segments') or []
    original = source_video or source.get('active') or (not job.get('analysis') and source.get('available') and not segments)
    if job.get('source_kind') == 'playable':
        return {'ready': False, 'code': 'playable_not_supported'}
    if job.get('status') in BUSY or (job.get('dashboard') or {}).get('running'):
        return {'ready': False, 'code': 'job_busy'}
    if job.get('status') in {'error', 'interrupted'} or (job.get('dashboard') or {}).get('has_error'):
        return {'ready': False, 'code': 'job_needs_review'}
    if input_mode == 'original':
        ready = bool(job.get('source_file') or source.get('available'))
        return {'ready': ready, 'route': 'original', 'code': 'ready' if ready else 'source_unavailable',
                'would_switch_source': False, 'full_current_video': True}
    if original:
        return {'ready': bool(source.get('available')), 'route': 'source_video',
                'code': 'ready' if source.get('available') else 'source_story_unavailable',
                'would_switch_source': bool(source_video and not source.get('active'))}
    missing = []
    selections = (job.get('export_settings') or {}).get('segment_videos') or {}
    versions = job.get('video_versions')
    for segment in segments:
        selected = selections.get(str(segment['index']))
        candidates = [v.get('file') for v in versions or [] if v.get('segment_index') == segment['index'] or
                      v.get('segment_index') is None and v.get('file') == segment.get('video_file')]
        if versions is None and segment.get('video_file'):
            candidates.append(segment['video_file'])
        if not candidates or selected and selected not in candidates:
            missing.append(segment['index'])
    ready = bool(segments) and not missing
    return {'ready': ready, 'route': job.get('video_route') or 'model_swap',
            'code': 'ready' if ready else 'segments_not_ready', 'missing_segments': missing}


def language_state(job, language, parts, source_language=None, *, input_mode=None, trim=None):
    translation = job.get('translation') or {}
    source = source_language or translation.get('source_language')
    if source == language and parts != ['subtitles']:
        return 'error'
    row = translation.get('original') if parts == ['subtitles'] and language == source else (translation.get('languages') or {}).get(language)
    if not row:
        return 'waiting'
    if input_mode is not None and row.get('input') != input_mode:
        return 'review' if row.get('status') == 'completed' else 'waiting'
    if trim is not None and not any(trim.values()) and row.get('trim_removed'):
        return 'review'
    actual = row.get('parts') or (['subtitles'] if row.get('kind') == 'subtitles' or language == source else ['voice'])
    if set(actual) != set(parts):
        return 'review' if input_mode is not None and row.get('status') == 'completed' else 'waiting'
    if row.get('status') in {'failed', 'interrupted', 'missing', 'cancelled'}:
        return 'review' if row.get('pending') else 'error'
    if row.get('status') in {'running', 'queued'}:
        return 'running'
    if row.get('status') == 'completed':
        output = row.get('srt_file') if parts == ['subtitles'] else row.get('video_file')
        return 'ready' if output else 'review'
    return 'waiting'


def states_for(job, options):
    return {language: language_state(job, language, options['parts'], options.get('source_language'),
                                    input_mode=options.get('input'), trim=options.get('trim'))
            for language in options['languages']}


def translate_many(api, ids, checkpoint, languages, parts=('voice',), source_language=None, *, source_video=False, check_only=False, direct=False,
                   input_mode=None, trim=None):
    if direct and source_video:
        raise ReplicaError('invalid_translation', 'Direct translation cannot also request a route PUT')
    if source_video and input_mode is not None:
        raise ReplicaError('invalid_translation', 'Full original input does not use the legacy source-story route PUT')
    requested = translation_options(languages, parts, source_language, input_mode=input_mode, trim=trim)
    if input_mode is not None and hasattr(api, 'require_original_translation_transport'):
        api.require_original_translation_transport()
    settings = api.translation_settings()
    results, stopped = [], False
    for job_id in selected_ids(ids):
        if stopped:
            results.append({'id': job_id, 'ok': False, 'code': 'not_submitted'})
            continue
        attempt, saved = None, None
        try:
            job = api.job(job_id)
            options = dict(requested)
            source = source_language or (job.get('translation') or {}).get('source_language')
            if source:
                options = translation_options(options['languages'], options['parts'], source,
                                              input_mode=options.get('input'), trim=options.get('trim'))
            skipped = [x for x in options['languages'] if x == source and options['parts'] != ['subtitles']]
            options['languages'] = [x for x in options['languages'] if x not in skipped]
            if not options['languages']:
                raise ReplicaError('invalid_translation', 'Original language is only supported for subtitles')
            if not settings.get('available') or 'text' in options['parts'] and settings.get('text_available') is False:
                raise ReplicaError('translation_unavailable', 'Requested translation service is unavailable')
            enabled = settings.get('enabled')
            if enabled is not None and any(x not in enabled and not (x == source and options['parts'] == ['subtitles']) for x in options['languages']):
                raise ReplicaError('language_disabled', 'A target language is not enabled in the workspace')
            current = states_for(job, options)
            eligibility = translation_input(job, source_video=source_video, input_mode=input_mode)
            if direct:
                # Delegate input selection to the translation action itself.
                # Keep busy/error guards; no generated clips or route PUT are required locally.
                blocked = (job.get('source_kind') == 'playable' or job.get('status') in BUSY
                           or job.get('status') in {'error', 'interrupted'}
                           or (job.get('dashboard') or {}).get('running')
                           or (job.get('dashboard') or {}).get('has_error'))
                eligibility = {'ready': not blocked, 'route': 'server_selected',
                               'code': 'job_busy_or_needs_review' if blocked else 'ready'}
            if check_only:
                conflict = input_mode is not None and any(
                    state != 'ready' and ((job.get('translation') or {}).get('languages') or {}).get(lang, {}).get('status') == 'completed'
                    for lang, state in current.items())
                results.append({'id': job_id, 'ok': not conflict and (eligibility['ready'] or all(x == 'ready' for x in current.values())),
                                'code': 'existing_translation_mismatch' if conflict else eligibility['code'], 'input': eligibility, 'languages': current,
                                'skipped_original_languages': skipped})
                continue
            saved = checkpoint.get('translations', job_id) or {'attempts': []}
            for prior in saved['attempts']:
                states = states_for(job, prior['options'])
                if all(x == 'ready' for x in states.values()):
                    prior['stage'] = 'complete'
                elif prior['stage'] in {'translation_intent', 'translation_unknown'}:
                    if (job.get('last_action') == 'translate' and job.get('revision', 0) > prior['revision'] and
                            all(x in {'ready', 'running'} for x in states.values())):
                        prior.update(stage='accepted', recovered_readonly=True)
                    else:
                        raise ReplicaError('result_unknown', 'Previous translation is unconfirmed; only job status was read, no repeated POST')
                elif prior['stage'] in {'rejected', 'needs_review'} or any(x in {'error', 'review'} for x in states.values()):
                    raise ReplicaError('needs_review', 'Previous translation needs review; no automatic retry')
            if saved['attempts']:
                checkpoint.put('translations', job_id, saved)
            if any(set(p['options']['languages']) & set(options['languages'])
                   and any(p['options'].get(k) != options.get(k) for k in ('input', 'trim', 'parts', 'source_language'))
                   for p in saved['attempts']):
                raise ReplicaError('needs_review', 'An earlier translation used different input/content options; no automatic paid rerun')
            missing = [x for x, state in current.items() if state != 'ready']
            if not missing:
                results.append({'id': job_id, 'ok': True, 'code': 'already_done', 'languages': current})
                continue
            if input_mode is not None and any(
                    ((job.get('translation') or {}).get('languages') or {}).get(lang, {}).get('status') == 'completed'
                    for lang in missing):
                raise ReplicaError('needs_review', 'Existing completed translation has different input/content or removed sections; no automatic paid rerun')
            matching = [p for p in saved['attempts'] if p['options']['parts'] == options['parts'] and
                        p['options'].get('source_language') == options.get('source_language') and
                        set(p['options']['languages']) & set(missing)]
            if matching:
                covered = {language for p in matching if p['stage'] == 'accepted' for language in p['options']['languages']}
                if set(missing) <= covered:
                    results.append({'id': job_id, 'ok': True, 'code': 'accepted', 'reused': True, 'languages': current})
                    continue
                raise ReplicaError('needs_review', 'Requested languages overlap an earlier attempt; check its status before adding missing languages')
            if any(x in {'error', 'review', 'running'} for x in current.values()):
                raise ReplicaError('needs_review', 'A requested translation is already running or failed; no additional submission')
            if not eligibility['ready']:
                raise ReplicaError('translation_not_ready', 'Translation input is unavailable: ' + eligibility['code'])
            if source_video and not (job.get('source_story') or {}).get('active'):
                route = checkpoint.get('source_routes', job_id)
                if route and route['stage'] == 'intent':
                    raise ReplicaError('result_unknown', 'Previous route change is unconfirmed; no repeated PUT')
                checkpoint.put('source_routes', job_id, {'stage': 'intent', 'revision': job['revision']})
                job = api.use_source_video(job)
                checkpoint.put('source_routes', job_id, {'stage': 'applied', 'revision': job['revision']})
            options['languages'] = missing
            attempt = {'stage': 'translation_intent', 'revision': job['revision'], 'options': options}
            saved['attempts'].append(attempt)
            checkpoint.put('translations', job_id, saved)
            receipt = api.translate(job, options)
            attempt.update(stage='accepted', receipt=receipt)
            checkpoint.put('translations', job_id, saved)
            results.append({'id': job_id, 'ok': True, 'code': 'accepted', 'languages': missing, 'receipt': receipt,
                            'skipped_original_languages': skipped})
        except Exception as error:
            code = getattr(error, 'code', '')
            if attempt is not None:
                attempt.update(stage='translation_unknown' if code == 'result_unknown' or isinstance(error, OSError) else 'rejected',
                               error=code or 'local_io_failed')
                try:
                    checkpoint.put('translations', job_id, saved)
                except OSError:
                    stopped = True
            if code in {'result_unknown', 'authentication_required', 'account_changed', 'permission_denied', 'read_failed', 'bridge_upgrade_required'} or isinstance(error, OSError):
                stopped = True
            results.append({'id': job_id, **error_result(error)})
    return {'operation': 'translation_preflight' if check_only else 'translate', 'results': results, 'stopped': stopped}


def translation_status_many(api, ids, languages, parts=('voice',), source_language=None, *, timeout=3600, interval=3.5, wait=time.sleep,
                            input_mode=None, trim=None):
    options = translation_options(languages, parts, source_language, input_mode=input_mode, trim=trim)
    ids, pending, results = selected_ids(ids), set(selected_ids(ids)), {}
    started = time.monotonic()
    while pending:
        for job_id in ids:
            if job_id not in pending:
                continue
            try:
                job = api.job(job_id)
                states = states_for(job, options)
                state = 'ready' if all(x == 'ready' for x in states.values()) else 'error' if any(x in {'error', 'review'} for x in states.values()) else 'running'
                if state == 'running' and job.get('status') in {'error', 'interrupted'}:
                    state = 'error'
                results[job_id] = {'id': job_id, 'ok': state != 'error', 'step_state': state, 'languages': states,
                                   'translation': job.get('translation'), 'message': job.get('message')}
                if state != 'running':
                    pending.remove(job_id)
            except ReplicaError as error:
                results[job_id] = {'id': job_id, **error_result(error)}
                if error.code in {'authentication_required', 'account_changed', 'permission_denied'}:
                    return {'operation': 'status', 'results': list(results.values()), 'pending': sorted(pending), 'state': 'authentication_required'}
        if not pending or time.monotonic() - started >= timeout:
            break
        wait(min(interval, max(0, timeout - (time.monotonic() - started))))
    return {'operation': 'status', 'results': [results[i] for i in ids], 'pending': [i for i in ids if i in pending],
            'state': 'waiting' if pending else 'settled'}


def download_translations(api, ids, checkpoint, output, languages, *, plan_only=False):
    if not isinstance(languages, (list, tuple)) or not languages or any(x not in TRANSLATION_LANGUAGES | {'source'} for x in languages):
        raise ReplicaError('invalid_selection', 'Specify translation download languages')
    ids = selected_ids(ids)
    projects = {}
    revisions = {}
    language_files = {}
    for job_id in ids:
        job = api.job(job_id)
        project_id = identifier(job.get('project_id') or job_id)
        projects.setdefault(project_id, []).append(job_id)
        revisions[job_id] = job.get('revision')
        language_files[job_id] = {lang: ((job.get('translation') or {}).get('languages') or {}).get(lang, {}).get('video_file')
                                  for lang in languages}
    plan = api.download_plan(ids, step='translation', languages=languages)
    if plan_only:
        return {'operation': 'download_plan', 'plan': plan}
    output, results = Path(output).expanduser().resolve(), []
    for project in plan.get('projects', []):
        project_id = project.get('project_id')
        if project_id not in projects:
            raise ReplicaError('invalid_media', 'Download plan contains an unselected project')
        for file in project.get('files', []):
            name, subtitle = file.get('name'), 'text' in file
            suffixes = {'.srt', '.vtt'} if subtitle else {'.mp4', '.mov', '.webm', '.mkv'}
            if not isinstance(name, str) or Path(name).name != name or '\\' in name or Path(name).suffix.lower() not in suffixes:
                raise ReplicaError('invalid_media', 'Unsafe or unsupported translated filename')
            if subtitle:
                text = file['text']
                if not isinstance(text, str) or not text or len(text.encode('utf-8')) > 16 * 1024 * 1024:
                    raise ReplicaError('invalid_media', 'Invalid subtitle text')
                content = text.encode('utf-8')
                identity = [project_id, name, hashlib.sha256(content).hexdigest()]
                owner = {'project_id': project_id, 'ids': projects[project_id], 'kind': 'subtitle'}
            else:
                address = urlsplit(urljoin(BASE_URL + '/', file.get('url', '')))
                pieces = address.path.split('/')
                job_id = pieces[3] if len(pieces) >= 6 else None
                if job_id not in projects[project_id]:
                    raise ReplicaError('invalid_media', 'Download plan contains an unselected task')
                media_url(file.get('url', ''), job_id)
                identity = [job_id, revisions[job_id], name, file.get('url'), file.get('bytes')]
                owner = {'id': job_id, 'project_id': project_id, 'kind': 'video'}
                relative = unquote(address.path).split('/files/', 1)[1]
                matches = [lang for lang, remote in language_files[job_id].items() if remote == relative]
                if len(matches) == 1:
                    owner['language'] = matches[0]
            key = 'translation-' + hashlib.sha256(json.dumps(identity).encode()).hexdigest()
            old = checkpoint.get('downloads', key)
            if old and Path(old['path']).is_file() and Path(old['path']).stat().st_size == old['size'] and digest(old['path']) == old['sha256']:
                results.append({**owner, 'ok': True, **old, 'reused': True})
                continue
            destination = output / project_id / name
            index = 2
            while destination.exists():
                destination = output / project_id / f'{Path(name).stem}_{index}{Path(name).suffix}'
                index += 1
            try:
                if subtitle:
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    partial = destination.with_name(destination.name + '.part')
                    try:
                        with partial.open('wb') as stream:
                            os.chmod(partial, 0o600)
                            stream.write(content)
                            stream.flush()
                            os.fsync(stream.fileno())
                        partial.replace(destination)
                    finally:
                        partial.unlink(missing_ok=True)
                    receipt = {'path': str(destination), 'name': name, 'size': len(content), 'sha256': digest(destination)}
                else:
                    receipt = api.download(job_id, file, destination)
                    decode_video(destination)
                    receipt['full_decode_passed'] = True
                checkpoint.put('downloads', key, receipt)
                results.append({**owner, 'ok': True, **receipt})
            except Exception as error:
                results.append({**owner, 'name': name, **error_result(error)})
    return {'operation': 'download', 'step': 'translation', 'results': results, 'saved': sum(x['ok'] for x in results),
            'skipped': plan.get('skipped', []), 'missing': plan.get('missing', 0)}
