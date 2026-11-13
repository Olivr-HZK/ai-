"""Advance one existing video task through Logo, translation and CTA assembly."""
from __future__ import annotations

from pathlib import Path

from ua_workflows.replica_assembly import assemble_one, assembly_status, download_assemblies
from ua_workflows.replica_cli import download_many, logo_many
from ua_workflows.replica_studio_api import BUSY, ReplicaError, TRANSLATION_LANGUAGES, logo_review_fields, step_state
from ua_workflows.replica_translation import language_state, translate_many

PARTS = ['text', 'voice']
TRIM = {'remove_cta': True, 'remove_gameplay': True}


def advance_one(api, job_id, checkpoint, languages, output):
    if (not languages or len(set(languages)) != len(languages)
            or any(lang not in TRANSLATION_LANGUAGES for lang in languages)):
        raise ReplicaError('invalid_selection', 'Select distinct supported translation languages')
    settings = api.translation_settings()
    if not settings.get('available') or settings.get('text_available') is False:
        raise ReplicaError('translation_unavailable', 'Text and voice translation must both be available')
    if any(lang not in settings.get('enabled', []) for lang in languages):
        raise ReplicaError('language_disabled', 'A target translation language is not enabled')
    assets = api.request('GET', '/api/assembly-assets')
    cta_languages = {a.get('language') or 'source' for a in assets
                     if a.get('category') == 'cta' and a.get('scope') == 'default' and a.get('available')}
    if not {'source', *languages} <= cta_languages:
        raise ReplicaError('missing_cta', 'A requested language has no matching default CTA')

    job = api.job(job_id)
    if job.get('id') != job_id:
        raise ReplicaError('invalid_selection', 'Task identity changed')
    source_language = (job.get('translation') or {}).get('source_language')
    if source_language in languages:
        raise ReplicaError('invalid_selection', 'A target language matches the original')
    if job.get('status') in {'error', 'interrupted'} or (job.get('dashboard') or {}).get('has_error'):
        raise ReplicaError('needs_review', job.get('error') or job.get('message') or 'Task failed')
    if job.get('status') in BUSY or (job.get('dashboard') or {}).get('running'):
        return {'id': job_id, 'state': 'waiting', 'message': job.get('message')}

    logo = job.get('logo_replace_view') or {}
    if logo.get('state') not in {'replaced', 'no_logo'}:
        if step_state(job, 'analysis') != 'ready':
            return {'id': job_id, 'state': 'waiting', 'message': 'Waiting for analysis'}
        options = {**api.smart_options(), 'logo_mode': 'auto', **TRIM}
        result = logo_many(api, [job_id], checkpoint, options=options, require_trim_match=True)['results'][0]
        if not result.get('ok'):
            raise ReplicaError(result.get('code', 'needs_review'), result.get('message') or 'Logo submission needs review')
        return {'id': job_id, 'state': 'waiting', 'message': 'Logo submitted'}
    if not logo.get('result_file') or any((logo.get('settings') or {}).get(k) is not True for k in TRIM):
        raise ReplicaError('needs_review', 'Logo result does not confirm both requested trim options')
    report = logo.get('report') or {}
    if (report.get('finish') or {}).get('cta_id'):
        raise ReplicaError('needs_review', 'Logo result already contains CTA; avoid appending another')

    proof = checkpoint.get('workflow_inputs', job_id)
    if not proof:
        files = download_many(api, [job_id], checkpoint, Path(output) / 'logo-input')['results']
        if len(files) != 1 or not files[0].get('ok'):
            raise ReplicaError('needs_review', 'Could not verify the Logo result')
        source_file = job.get('source_file')
        if (not isinstance(source_file, str) or Path(source_file).name != source_file
                or not api.source_matches(job_id, source_file, files[0]['size'], files[0]['sha256'])):
            raise ReplicaError('needs_review', 'Current translation input does not match the Logo result')
        proof = {'result_file': logo['result_file'], 'source_file': source_file,
                 'sha256': files[0]['sha256'], 'size': files[0]['size'],
                 'review': logo_review_fields(job)}
        checkpoint.put('workflow_inputs', job_id, proof)
    elif (proof.get('result_file') != logo['result_file'] or proof.get('source_file') != job.get('source_file')
          or not api.source_matches(job_id, proof['source_file'], proof['size'], proof['sha256'])):
        raise ReplicaError('needs_review', 'Logo or translation input changed since verification')

    states = {lang: language_state(job, lang, PARTS, input_mode='original', trim=TRIM) for lang in languages}
    if any(state in {'error', 'review'} for state in states.values()):
        raise ReplicaError('needs_review', 'Translation results do not match the requested options')
    if not all(state == 'ready' for state in states.values()):
        if any(state == 'running' for state in states.values()):
            return {'id': job_id, 'state': 'waiting', 'message': 'Translation in progress', 'languages': states}
        result = translate_many(api, [job_id], checkpoint, languages, PARTS, input_mode='original', trim=TRIM)['results'][0]
        if not result.get('ok'):
            raise ReplicaError(result.get('code', 'needs_review'), result.get('message') or 'Translation needs review')
        return {'id': job_id, 'state': 'waiting', 'message': 'Translation submitted'}

    if not checkpoint.get('assemblies', job_id):
        assemble_one(api, job_id, checkpoint, ['source', *languages])
        return {'id': job_id, 'state': 'waiting', 'message': 'CTA assembly submitted'}
    status = assembly_status(api, job_id, checkpoint)
    if status['state'] == 'failed':
        raise ReplicaError('needs_review', status.get('message') or 'CTA assembly failed')
    if status['state'] != 'complete':
        return status
    translations = (job.get('translation') or {}).get('languages') or {}
    translation_review = {
        language: {key: translations[language].get(key) for key in ('dubbed', 'no_speech', 'text_review')}
        for language in languages
    }
    return {**download_assemblies(api, job_id, checkpoint, output),
            'logo_review': logo_review_fields(job),
            'logo_count': report.get('logos'),
            'translation_review': translation_review}
