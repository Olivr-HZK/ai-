"""Pinned XHS continuation rules and durable, bounded acquisition attempts."""
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path
import fcntl
import hashlib
import json
from zoneinfo import ZoneInfo

from ua_workflows.collection_runtime import record_failure, write
from ua_workflows.xhs_duration import DurationExcluded

RULE = 'majority_keywords_media_three_attempts_v1'
POLICY = {'rule': RULE, 'minimum_complete_keywords': 4, 'detail_attempts': 3, 'media_attempts': 3}


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load(path):
    return json.loads(Path(path).read_text())


def validate_policy(value):
    if value != POLICY:
        raise ValueError('Unsupported XHS continuation policy')
    return dict(value)


def policy_for(base):
    path = Path(base)/'run.json'
    value = load(path).get('continuation_policy') if path.exists() else None
    return validate_policy(value) if value is not None else None


def majority_incomplete_keywords(base, summary):
    policy = policy_for(base)
    if not policy:
        return []
    tasks = summary['keywords']; names = [t['keyword'] for t in tasks]
    if len(tasks) != 7 or len(set(names)) != 7 or names!=load(Path(base)/'run.json')['keywords']:
        raise ValueError('Majority continuation requires the pinned seven keywords')
    completed=sum(t.get('status') == 'complete' for t in tasks)
    if ((summary.get('status')=='complete') != (completed==7)
            or any(type(t.get('pages')) is not int or t['pages']<0 or
                   (t.get('status')=='complete' and t['pages']==0) for t in tasks)
            or summary.get('completed_keywords',completed)!=completed):
        raise ValueError('Search completeness summary does not match actual keyword pages')
    if completed < policy['minimum_complete_keywords']:
        return []
    return [t['keyword'] for t in tasks if t.get('status') != 'complete']


class MediaUnavailable(ValueError):
    """All authorized attempts consumed; retain unreviewed, omit from ranking."""


def scope_for(base, note, phase, policy):
    values = {'note_id': note['id'], 'url': note.get('url'), 'is_video': note['is_video']}
    if phase == 'media':
        values.update(video_url=note.get('video_url'), images=note.get('images'),
                      image_urls=note.get('image_urls'), image_url=note.get('image_url'))
    return {'rule': RULE, 'phase': phase, 'run_sha256': digest(Path(base)/'run.json'),
            'policy': policy, 'source_sha256': hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest(),
            'note_id': note['id']}


def read_state(folder, scope):
    path = folder/'state.json'
    if not path.exists():
        return {'scope': scope, 'attempts': []}
    state = load(path)
    attempts = state.get('attempts', [])
    if (state.get('scope') != scope or len(attempts) > 3
            or [a.get('number') for a in attempts] != list(range(1, len(attempts)+1))
            or any(a.get('outcome') not in ('unknown', 'failed', 'succeeded', 'excluded_duration') for a in attempts)):
        raise ValueError('Acquisition attempt scope or budget changed')
    for attempt in attempts:
        intent = folder/f"{attempt['number']:02}/intent.json"
        if not intent.is_file() or digest(intent) != attempt.get('intent_sha256'):
            raise ValueError('Acquisition attempt intent evidence changed')
        if attempt.get('outcome') in ('failed','excluded_duration'):
            evidence = folder/attempt['diagnostic']
            if not evidence.is_file() or digest(evidence) != attempt['diagnostic_sha256']:
                raise ValueError('Acquisition failure evidence changed')
    return state


def bounded_acquire(base, note, phase, operation, *, before_attempt=None, recover=None):
    """Count first attempts and interrupted/unknown outcomes across process restarts."""
    base = Path(base); policy = policy_for(base)
    if not policy:
        raise ValueError('Bounded acquisition requires a pinned continuation policy')
    folder = base/'media'/note['id']/('retry-'+phase); folder.mkdir(parents=True, exist_ok=True)
    scope = scope_for(base, note, phase, policy)
    with (folder/'.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
        state = read_state(folder, scope)
        for attempt in state['attempts']:
            if attempt['outcome']=='excluded_duration':
                raise DurationExcluded(note['id']+': retained video duration exclusion')
            if attempt['outcome'] == 'succeeded':
                result = folder/f"{attempt['number']:02}/result.json"
                if digest(result) != attempt['result_sha256']:
                    raise ValueError('Acquisition cached result evidence changed')
                return load(result)
            if attempt['outcome'] == 'unknown' and recover:
                value = recover(folder/f"{attempt['number']:02}")
                if value is not None:
                    result = folder/f"{attempt['number']:02}/result.json"; write(result, value)
                    attempt.update(outcome='succeeded', result_sha256=digest(result))
                    write(folder/'state.json', state); return value
        while len(state['attempts']) < policy[phase+'_attempts']:
            number = len(state['attempts'])+1; slot = folder/f'{number:02}'; slot.mkdir(exist_ok=True)
            # Power/credential preflight failures do not consume a paid submission intent.
            with before_attempt(slot) if before_attempt else nullcontext():
                intent = slot/'intent.json'; write(intent, {'scope': scope, 'number': number,
                                                         'at': datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()})
                attempt = {'number': number, 'outcome': 'unknown', 'intent_sha256': digest(intent)}
                state['attempts'].append(attempt); write(folder/'state.json', state)
                try:
                    value = operation(slot)
                except DurationExcluded:
                    evidence=base/'media'/note['id']/'video-duration-check.json'
                    attempt.update(outcome='excluded_duration',diagnostic='../video-duration-check.json',
                                   diagnostic_sha256=digest(evidence))
                    write(folder/'state.json',state)
                    raise
                except Exception as exc:
                    diagnostic = Path(record_failure(slot, 'xhs-'+phase, exc))
                    attempt.update(outcome='failed', error_type=type(exc).__name__,
                                   diagnostic=str(diagnostic.relative_to(folder)), diagnostic_sha256=digest(diagnostic))
                    write(folder/'state.json', state)
                else:
                    result = slot/'result.json'; write(result, value)
                    attempt.update(outcome='succeeded', result_sha256=digest(result))
                    write(folder/'state.json', state); return value
        state['status'] = 'exhausted'; write(folder/'state.json', state)
        raise MediaUnavailable(f"{note['id']}: {phase} acquisition exhausted after 3 attempts")


def exhausted_notes(base, notes):
    base = Path(base); policy = policy_for(base)
    if not policy:
        return []
    failures = []
    for note in notes:
        folder = base/'media'/note['id']
        if (folder/'manifest.json').exists():
            continue  # Available originals must pass the normal hash/review checks.
        for phase in ('detail', 'media'):
            if phase=='detail' and note.get('video_url'):
                continue
            path = folder/('retry-'+phase)/'state.json'
            if not path.exists():
                continue
            # Media scope uses the successfully cached original video URL.
            scoped_note = note
            if phase == 'media' and note['is_video']:
                detail = folder/'retry-detail/state.json'
                if detail.exists():
                    ds = read_state(detail.parent,scope_for(base,note,'detail',policy))
                    successes = [a for a in ds['attempts'] if a['outcome'] == 'succeeded']
                    if successes:
                        result = folder/f"retry-detail/{successes[-1]['number']:02}/result.json"
                        if digest(result) != successes[-1]['result_sha256']:
                            raise ValueError('Acquisition cached result evidence changed')
                        scoped_note = {**note, 'video_url': load(result)}
            state = read_state(path.parent, scope_for(base, scoped_note, phase, policy))
            attempts = state['attempts']
            if len(attempts) == 3 and all(a['outcome'] in ('failed', 'unknown') for a in attempts):
                if phase=='detail':
                    recoverable=False
                    for attempt in attempts:
                        slot=path.parent/f"{attempt['number']:02}"
                        response=slot/'video-api-response.json';meta=slot/'video-api-result-meta.json'
                        if attempt['outcome']=='unknown' and response.exists() and meta.exists() and load(meta).get('ok') is True:
                            value=load(response).get('results')
                            recoverable |= isinstance(value,dict) and value.get('id')==note['id'] and bool(value.get('video_url'))
                    if recoverable:continue
                failures.append({'note_id': note['id'], 'is_video': note['is_video'], 'phase': phase,
                                 'attempts': 3, 'unknown_attempts': sum(a['outcome'] == 'unknown' for a in attempts),
                                 'evidence': str(path.relative_to(base)), 'sha256': digest(path), 'reviewed': False})
                break
    return failures
