"""Submit the documented model-swap step for an existing video task."""
from ua_workflows.replica_portraits import characters
from ua_workflows.replica_studio_api import BUSY, SWAP_OPTIONS, ReplicaError, identifier


def swap_segments(job):
    if job.get('video_route', 'model_swap') != 'model_swap' or job.get('source_kind') == 'playable':
        raise ReplicaError('unsupported_action', 'Select a video task on the model_swap route')
    segments = job.get('segments', [])
    if (not isinstance(segments, list)
            or any(not isinstance(row, dict) or type(row.get('index')) is not int or row['index'] < 1
                   for row in segments)
            or len({row['index'] for row in segments}) != len(segments)):
        raise ReplicaError('invalid_response', 'Swap segments are not uniquely identified')
    return [{key: row.get(key) for key in ('index', 'start_s', 'end_s', 'video_file')} for row in segments]


def swap_status(job):
    segments = swap_segments(job)
    dashboard = job.get('dashboard') or {}
    if job.get('status') in {'error', 'interrupted'} or dashboard.get('has_error'):
        state = 'error'
    elif job.get('status') in BUSY or dashboard.get('running'):
        state = 'running'
    elif segments and all(isinstance(row['video_file'], str) and row['video_file'] for row in segments):
        state = 'ready'
    else:
        state = 'waiting'
    return state, segments


def replacements_inactive(api, job):
    mode, rules = job.get('text_replacements_mode'), job.get('text_replacements')
    if mode not in {'custom', 'defaults'} or not isinstance(rules, list):
        return False
    if mode == 'defaults' and api.settings().get('text_replacements') != rules:
        return False
    if not rules:
        return True
    detections = job.get('replacement_detections')
    if not isinstance(detections, list) or len(detections) != len(rules):
        return False
    if any(not isinstance(row, dict) or not isinstance(row.get('from'), str) or not row['from'].strip()
           for row in rules):
        return False
    if any(not isinstance(row, dict) or not isinstance(row.get('keyword'), str) or row.get('occurrences') != []
           for row in detections):
        return False
    keywords = {row['from'].strip().casefold() for row in rules}
    return len(keywords) == len(rules) and keywords == {row['keyword'].strip().casefold() for row in detections}


def swap_video(api, job_id, auth, checkpoint=None, *, check_only=False):
    job_id = identifier(job_id)
    result = {'operation': 'swap', 'id': job_id, 'submitted': False}
    prior = checkpoint.get('video_swaps', job_id) if checkpoint else None
    if prior and prior['stage'] != 'rejected':
        if prior['stage'] in {'intent', 'unknown'}:
            return {**result, 'ok': False, 'code': 'result_unknown',
                    'message': 'Original swap submission is unconfirmed. Read the task with status --wait-for swap.'}
        return {**result, 'ok': True, 'reused': True, 'receipt': prior['receipt']}

    job, cast = characters(api, job_id)
    state, segments = swap_status(job)
    if state in {'error', 'running'}:
        raise ReplicaError('swap_not_ready', 'Task is busy or needs review')
    if (job.get('step_states') or {}).get('analysis') != 'done' or not segments:
        raise ReplicaError('swap_not_ready', 'Finish analysis and prepare model-swap segments in Replica first')
    if not cast or any(person.get('portrait_ready') is not True or not person.get('portrait_file')
                       or person.get('portrait_current') is False for person in cast):
        raise ReplicaError('swap_not_ready', 'Select a current reference image for each task character first')
    if any(segment.get('video_prompt_custom') for segment in job['segments']):
        raise ReplicaError('swap_not_ready', 'Review and reset custom video prompts in Replica before submitting')
    if not replacements_inactive(api, job):
        raise ReplicaError('swap_not_ready', 'Clear text and speech rules, or verify that analysis found no matches')
    revision = job.get('revision')
    if type(revision) is not int or revision < 0:
        raise ReplicaError('invalid_response', 'Task revision is missing or invalid')
    options = {**SWAP_OPTIONS, 'revision': revision}
    result.update(options=options, segments=segments)
    if check_only:
        return {**result, 'ok': True, 'check_only': True, 'results_present': state == 'ready'}
    if auth.get('auth_mode') != 'device' or 'run' not in auth.get('scopes', []):
        raise ReplicaError('permission_denied', 'Video swap requires a device token with the run scope')

    intent = {'stage': 'intent', 'options': options,
              'portraits': {person['source_id']: person['portrait_file'] for person in cast}}
    checkpoint.put('video_swaps', job_id, intent)
    try:
        receipt = api.swap_video(job)
    except ReplicaError as error:
        stage = 'rejected' if error.request_not_applied or error.status in {401, 403} else 'unknown'
        checkpoint.put('video_swaps', job_id, {**intent, 'stage': stage, 'error': error.code})
        raise
    checkpoint.put('video_swaps', job_id, {**intent, 'stage': 'confirmed', 'receipt': receipt})
    return {**result, 'ok': True, 'submitted': True, 'receipt': receipt}
