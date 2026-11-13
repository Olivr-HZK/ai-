"""Explicit per-job language assembly, with a durable non-replaying journal."""
from __future__ import annotations

import time
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit

from ua_workflows.replica_cli import decode_video, selected_ids
from ua_workflows.replica_studio_api import BASE_URL, BUSY, ReplicaError, TRANSLATION_LANGUAGES, digest, media_url
from ua_workflows.replica_translation import language_state

ASSEMBLY = {'source': 'original', 'other': 'none', 'cta': 'random',
            'logos': 'keep', 'subtitles': 'keep'}


def assemble_one(api, job_id, checkpoint, languages, *, check_only=False):
    job_id = selected_ids([job_id])[0]
    if (not isinstance(languages, (list, tuple)) or not languages
            or len(set(languages)) != len(languages)
            or any(lang not in TRANSLATION_LANGUAGES | {'source'} for lang in languages)):
        raise ReplicaError('invalid_selection', 'Select distinct supported assembly languages')
    job = api.job(job_id)
    if job.get('id') != job_id:
        raise ReplicaError('invalid_selection', 'Assembly task identity changed')
    if job.get('source_kind') == 'playable':
        raise ReplicaError('invalid_selection', 'This assembly workflow only supports video')
    logo = job.get('logo_replace_view') or {}
    if logo.get('state') not in {'replaced', 'no_logo'} or not logo.get('result_file'):
        raise ReplicaError('needs_review', 'A confirmed Logo result is required before assembly')
    prior = checkpoint.get('assemblies', job_id) if checkpoint else None
    options = {'languages': list(languages), 'use_defaults': True, 'assembly': dict(ASSEMBLY)}
    if prior:
        if prior['options'] != options:
            raise ReplicaError('needs_review', 'Earlier assembly used different options')
        if prior['stage'] in {'intent', 'unknown'}:
            raise ReplicaError('result_unknown', 'Earlier assembly has no confirmed receipt; do not resubmit')
        return {'id': job_id, 'ok': prior['stage'] == 'accepted', 'code': prior['stage'], 'reused': True}
    if job.get('status') in BUSY or (job.get('dashboard') or {}).get('running'):
        raise ReplicaError('job_busy', 'Wait for the existing task before assembly')
    for lang in languages:
        if lang != 'source' and language_state(job, lang, ['text', 'voice'], input_mode='original') != 'ready':
            raise ReplicaError('translation_not_ready', 'Text and voice translation is not complete for ' + lang)
    assets = api.request('GET', '/api/assembly-assets')
    cta_ids = {language: sorted(a['id'] for a in assets
                                if a.get('category') == 'cta' and a.get('available')
                                and a.get('scope') == 'default' and isinstance(a.get('id'), str)
                                and (a.get('language') or 'source') == language)
               for language in languages}
    missing = sorted(language for language, ids in cta_ids.items() if not ids)
    if missing:
        raise ReplicaError('missing_cta', 'No language-specific CTA for ' + ', '.join(missing))
    finish = (logo.get('report') or {}).get('finish') or {}
    if finish.get('cta_id'):
        raise ReplicaError('needs_review', 'Logo result already contains a CTA; verify the input before assembly')
    if check_only:
        return {'id': job_id, 'ok': True, 'code': 'ready', 'options': options}
    if checkpoint is None:
        raise ReplicaError('invalid_input', 'An assembly checkpoint is required')
    record = {'stage': 'intent', 'options': options, 'revision': job.get('revision'),
              'existing_exports': [v.get('file') for v in job.get('exports') or []],
              'cta_ids': cta_ids, 'at': time.time()}
    checkpoint.put('assemblies', job_id, record)
    try:
        receipt = api.request('POST', '/api/jobs/' + job_id + '/actions/export', body=options)
        if receipt.get('id') != job_id or not isinstance(receipt.get('status'), str):
            raise ReplicaError('result_unknown', 'Assembly receipt could not be confirmed')
        if receipt.get('generation_skipped'):
            raise ReplicaError('needs_review', 'Server reused a previous export; confirm its CTA and language')
        record.update(stage='accepted', receipt={k: receipt.get(k) for k in ('id', 'status', 'revision', 'last_action')})
        checkpoint.put('assemblies', job_id, record)
    except Exception as error:
        record.update(stage='unknown' if isinstance(error, OSError) or getattr(error, 'code', '') == 'result_unknown'
                      else 'rejected', error=getattr(error, 'code', 'local_io_failed'))
        checkpoint.put('assemblies', job_id, record)
        raise
    return {'id': job_id, 'ok': True, 'code': 'accepted', 'receipt': record['receipt']}


def assembly_status(api, job_id, checkpoint):
    job_id = selected_ids([job_id])[0]
    record = checkpoint.get('assemblies', job_id)
    if not record:
        raise ReplicaError('needs_review', 'No saved assembly request exists for this task')
    job = api.job(job_id)
    if job.get('id') != job_id:
        raise ReplicaError('invalid_selection', 'Assembly task identity changed')
    if record['stage'] != 'accepted':
        return {'id': job_id, 'state': 'unknown', 'message': 'Assembly receipt is not confirmed; no request was repeated'}
    if job.get('last_action') != 'export' or job.get('revision', 0) <= (record.get('revision') or 0):
        return {'id': job_id, 'state': 'waiting', 'message': 'Awaiting evidence from the original export request'}
    wanted = record['options']['languages']
    source = (job.get('translation') or {}).get('source_language')
    found = {}
    for export in job.get('exports') or []:
        filename = export.get('file')
        if not filename or filename in record['existing_exports']:
            continue
        language = export.get('language')
        if 'source' in wanted and language in (None, 'source', source):
            language = 'source'
        if language in wanted:
            if language in found:
                raise ReplicaError('needs_review', 'More than one new export matches ' + language)
            found[language] = export
    if set(found) == set(wanted):
        if not all(value.get('cta') in (record.get('cta_ids') or {}).get(language, [])
                   for language, value in found.items()):
            raise ReplicaError('needs_review', 'Assembly CTA identity does not match the selected language')
        return {'id': job_id, 'state': 'complete', 'exports': found}
    if job.get('status') in {'error', 'interrupted'}:
        return {'id': job_id, 'state': 'failed', 'message': job.get('error') or job.get('message')}
    return {'id': job_id, 'state': 'waiting', 'confirmed_languages': list(found)}


def download_assemblies(api, job_id, checkpoint, output):
    result = assembly_status(api, job_id, checkpoint)
    if result['state'] != 'complete':
        raise ReplicaError('needs_review', 'Assembly has not produced all requested language versions')
    exports = result['exports']
    plan = api.download_plan([job_id], step='assembly', languages=list(exports))
    files = [f for project in plan.get('projects', []) for f in project.get('files', []) if 'text' not in f]
    selected = {}
    for entry in files:
        address = urlsplit(urljoin(BASE_URL + '/', entry.get('url', '')))
        media_url(address.geturl(), job_id)
        relative = unquote(address.path).split('/files/', 1)[-1]
        matches = [lang for lang, export in exports.items() if export['file'] == relative]
        if len(matches) != 1 or matches[0] in selected:
            raise ReplicaError('invalid_media', 'Assembly download does not uniquely match the selected task and language')
        selected[matches[0]] = entry
    if set(selected) != set(exports):
        raise ReplicaError('invalid_media', 'Assembly download plan is missing requested languages')
    outputs = []
    for lang, entry in selected.items():
        key = 'assembly-' + job_id + '-' + lang
        prior = checkpoint.get('downloads', key)
        if prior:
            path = Path(prior['path'])
            if path.is_file() and path.stat().st_size == prior['size'] and digest(path) == prior['sha256']:
                outputs.append({'language': lang, **prior, 'reused': True})
                continue
            raise ReplicaError('invalid_media', 'Previously downloaded assembly changed')
        name = entry.get('name')
        if not isinstance(name, str) or Path(name).name != name or Path(name).suffix.lower() not in {'.mp4', '.mov', '.webm', '.mkv'}:
            raise ReplicaError('invalid_media', 'Unsafe assembly output name')
        destination = Path(output).expanduser().resolve() / job_id / (lang + '_' + name)
        if destination.exists():
            raise ReplicaError('invalid_media', 'Assembly destination already exists without a receipt')
        receipt = api.download(job_id, entry, destination)
        decode_video(destination)
        receipt.update(full_decode_passed=True, export_file=exports[lang]['file'])
        checkpoint.put('downloads', key, receipt)
        outputs.append({'language': lang, **receipt})
    return {'id': job_id, 'state': 'downloaded', 'outputs': outputs}
