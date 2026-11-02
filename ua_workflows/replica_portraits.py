"""Upload a supplied character reference without generating an image or video."""
from __future__ import annotations

import time

from ua_workflows.replica_studio_api import BUSY, ReplicaError, identifier, prepare_portrait


def characters(api, job_id):
    job = api.job(identifier(job_id))
    cast = job.get('cast_members') or []
    if (not isinstance(cast, list) or any(not isinstance(row, dict) or not isinstance(row.get('source_id'), str) or not row['source_id'] for row in cast)
            or len({row['source_id'] for row in cast}) != len(cast)):
        raise ReplicaError('invalid_response', 'Task character mapping is not unique')
    return job, cast


def character_list(api, job_id):
    job, cast = characters(api, job_id)
    return {'operation': 'characters', 'id': job['id'], 'revision': job['revision'],
            'characters': [{key: row.get(key) for key in
                            ('source_id', 'source_name', 'character_name', 'portrait_ready', 'portrait_file')}
                           for row in cast]}


def replace_portrait(api, job_id, path, checkpoint, *, source_id=None, replace=False, check_only=False):
    picture = prepare_portrait(path)
    identity = picture['identity']
    job, cast = characters(api, job_id)
    if job.get('source_kind') == 'playable':
        raise ReplicaError('unsupported_action', 'This command only replaces a video task character reference')
    if source_id is None:
        if len(cast) != 1:
            raise ReplicaError('character_selection_required', 'Specify --source-id; available roles: ' +
                               ', '.join(str(row['source_id']) for row in cast))
        source_id = cast[0]['source_id']
    source_id = identifier(source_id)
    rows = [row for row in cast if row['source_id'] == source_id]
    if len(rows) != 1:
        raise ReplicaError('character_not_found', 'The selected source-id does not belong to this task')
    person = rows[0]
    result = {'operation': 'portrait', 'id': job['id'], 'source_id': source_id, 'image': identity,
              'video_generation_submitted': False}
    if check_only:
        return {**result, 'ok': True, 'check_only': True, 'submitted': False}
    if checkpoint.account != api.authenticate()['account_id']:
        raise ReplicaError('account_changed', 'Portrait checkpoint belongs to another account')
    key = job['id'] + ':' + source_id
    prior = checkpoint.get('portraits', key)
    if prior:
        # An uncertain write blocks changing the supplied image too. Only the
        # original bytes can establish the outcome of the original operation.
        uncertain = prior['stage'] in {'portrait_intent', 'portrait_unknown'}
        original = prior['identity']
        file = person.get('portrait_file')
        same = identity == original
        confirmed = (file and type(job.get('revision')) is int and job['revision'] > prior['revision']
                     and (same or uncertain) and api.portrait_matches(job['id'], file, original))
        if confirmed:
            prior.update(stage='portrait_uploaded', portrait_file=file, receipt_revision=job['revision'],
                         confirmed_at=time.time(), recovered_readonly=uncertain)
            checkpoint.put('portraits', key, prior)
            if same:
                return {**result, 'ok': True, 'submitted': False, 'reused': True,
                        'recovered_readonly': uncertain, 'portrait_file': file}
        elif uncertain:
            return {**result, 'ok': False, 'code': 'result_unknown', 'submitted': False,
                    'message': 'Original upload is unconfirmed; no POST was replayed'}
        if not replace:
            raise ReplicaError('checkpoint_conflict', 'Use --replace to change a previously confirmed reference; unknown writes cannot be replayed')
        if prior['stage'] != 'portrait_uploaded':
            raise ReplicaError('checkpoint_conflict', 'Previous operation is not a confirmed upload')
    if job.get('status') in BUSY or (job.get('dashboard') or {}).get('running'):
        raise ReplicaError('task_busy', 'Wait for the active task before replacing its reference')
    if (job.get('step_states') or {}).get('analysis') != 'done':
        raise ReplicaError('analysis_not_ready', 'Automatic analysis must identify the task characters first')
    api.require_portrait_transport()
    history = [] if not prior else [*prior.get('history', []), {k: v for k, v in prior.items() if k != 'history'}]
    record = {'stage': 'portrait_intent', 'identity': identity, 'revision': job['revision'],
              'previous_portrait_file': person.get('portrait_file'), 'dispatch_started_at': time.time(), 'history': history}
    checkpoint.put('portraits', key, record)  # Persist before any upload POST.
    try:
        receipt = api.upload_portrait(job['id'], source_id, picture)
        receipt_cast = receipt.get('cast_members')
        matched = ([row for row in receipt_cast if isinstance(row, dict) and row.get('source_id') == source_id]
                   if isinstance(receipt_cast, list) else [])
        if (receipt.get('id') != job['id'] or type(receipt.get('revision')) is not int
                or receipt['revision'] <= record['revision'] or len(matched) != 1 or not matched[0].get('portrait_file')):
            raise ReplicaError('result_unknown', 'Upload response does not confirm the selected character reference')
        record.update(stage='portrait_uploaded', portrait_file=matched[0]['portrait_file'],
                      receipt_revision=receipt['revision'], confirmed_at=time.time())
        checkpoint.put('portraits', key, record)
    except Exception as error:
        # A single POST maximum. Even a local receipt-write failure keeps the
        # original intent available for read-only byte reconciliation on restart.
        record.update(stage='portrait_unknown', error=getattr(error, 'code', type(error).__name__))
        checkpoint.put('portraits', key, record)
        raise
    return {**result, 'ok': True, 'submitted': True, 'portrait_file': record['portrait_file'],
            'revision': receipt['revision']}
