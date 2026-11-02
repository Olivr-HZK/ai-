"""Durable read-only reconciliation and timeout alerts for the serial workflow."""
from __future__ import annotations

import hashlib
from functools import wraps
import json
import math
import os
from pathlib import Path
import time
import threading

from ua_workflows.replica_studio_api import ReplicaError, save_private

DEFAULT_TIMEOUTS = {'stall_timeout': 900, 'stage_timeout': 7200, 'unknown_timeout': 900,
                    'reconcile_interval': 60}
UNKNOWN = {'upload_unknown', 'logo_unknown', 'write_unknown', 'attachment_unknown'}
WATCHED = UNKNOWN | {'uploaded', 'logo_accepted', 'logo_submitting', 'write_intent', 'feishu_upload_intent'}


def synchronized(method):
    @wraps(method)
    def locked(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)
    return locked


def timeout_policy(overrides=None):
    values = {**DEFAULT_TIMEOUTS, **(overrides or {})}
    if set(values) != set(DEFAULT_TIMEOUTS) or any(
            isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v <= 0
            for v in values.values()):
        raise ReplicaError('invalid_input', 'Timeouts and reconciliation interval must be positive finite seconds')
    return values


class RunHealth:
    """No timers or threads: observe between bounded HTTP calls and file operations."""

    def __init__(self, root, policy=None, clock=time.time):
        self.root, self.clock = Path(root), clock
        self._lock = threading.RLock()
        self.policy = timeout_policy(policy)
        path = self.root / 'health.json'
        self.data = json.loads(path.read_text()) if path.exists() else {'version': 1, 'records': {}, 'alerts': {}}

    @synchronized
    def persist(self):
        save_private(self.root / 'health.json', self.data)
        save_private(self.root / 'alerts.json', {
            'checked_at': self.clock(), 'active': list(self.data['alerts'].values()), 'policy': self.policy})

    @synchronized
    def alert(self, code, *, record_id=None, message='', now=None):
        now = self.clock() if now is None else now
        key = code + ':' + (record_id or 'batch')
        if key in self.data['alerts']:
            return
        if key not in self.data['alerts']:
            value = {'code': code, 'record_id': record_id, 'message': message, 'first_seen_at': now}
            self.data['alerts'][key] = value
            with (self.root / 'alerts.jsonl').open('a') as stream:
                os.chmod(stream.fileno(), 0o600)
                stream.write(json.dumps({'event': 'alert', **value}, ensure_ascii=False) + '\n')
            print(json.dumps({'phase': 'alert', **value}, ensure_ascii=False), flush=True)
        self.persist()

    @synchronized
    def clear(self, code, record_id=None):
        key = code + ':' + (record_id or 'batch')
        value = self.data['alerts'].pop(key, None)
        if value:
            with (self.root / 'alerts.jsonl').open('a') as stream:
                os.chmod(stream.fileno(), 0o600)
                stream.write(json.dumps({'event': 'resolved', 'resolved_at': self.clock(), **value}, ensure_ascii=False) + '\n')

    @synchronized
    def observe(self, phase, values, delivery_count=0):
        now = self.clock()
        # Poll timestamps/revisions and repeated errors are not progress evidence.
        signatures = {}
        for value in values:
            rid = value['record_id']
            signature = json.dumps([value.get(k) for k in
                                    ('stage', 'job_id', 'last_status', 'last_message', 'feishu_token')], ensure_ascii=False)
            signatures[rid] = signature
            old = self.data['records'].get(rid)
            changed = old is None or old['signature'] != signature
            entry = {'signature': signature, 'last_progress_at': now if changed else old['last_progress_at'],
                     'stage': value['stage'], 'stage_started_at': now if not old or old.get('stage') != value['stage'] else old['stage_started_at'],
                     'unknown_since': (old.get('unknown_since') if old and old.get('unknown_since') is not None else now)
                     if value['stage'] in UNKNOWN else None}
            self.data['records'][rid] = entry
            if phase == 'pipeline' and value['stage'] in WATCHED and now - entry['stage_started_at'] >= self.policy['stage_timeout']:
                self.alert('record_stage_timeout', record_id=rid, message='Task stage deadline exceeded; no generation replay', now=now)
            else:
                self.clear('record_stage_timeout', rid)
            if value['stage'] in WATCHED and now - entry['last_progress_at'] >= self.policy['stall_timeout']:
                self.alert('record_stalled', record_id=rid, message='No observable task progress; no business request will be replayed', now=now)
            else:
                self.clear('record_stalled', rid)
            if value['stage'] in UNKNOWN:
                self.alert('result_unknown', record_id=rid, message=value.get('error', 'Unconfirmed operation; read-only reconciliation only'), now=now)
                if now - entry['unknown_since'] >= self.policy['unknown_timeout']:
                    self.alert('unknown_timeout', record_id=rid, message='Read-only reconciliation deadline reached; operator attention required', now=now)
                else:
                    self.clear('unknown_timeout', rid)
            else:
                self.clear('result_unknown', rid)
                self.clear('unknown_timeout', rid)
        batch_signature = hashlib.sha256(json.dumps([phase, signatures, delivery_count], sort_keys=True).encode()).hexdigest()
        if self.data.get('phase') != phase:
            self.data.update(phase=phase, phase_started_at=now)
            self.clear('stage_timeout')
        if self.data.get('signature') != batch_signature:
            self.data.update(signature=batch_signature, last_progress_at=now)
            self.clear('batch_stalled')
        if phase != 'pipeline' and now - self.data['phase_started_at'] >= self.policy['stage_timeout']:
            self.alert('stage_timeout', message='Stage deadline exceeded; waiting and read-only checks continue', now=now)
        else:
            self.clear('stage_timeout')
        if now - self.data['last_progress_at'] >= self.policy['stall_timeout']:
            self.alert('batch_stalled', message='No observable batch progress', now=now)
        else:
            self.clear('batch_stalled')
        self.persist()

    @synchronized
    def unknown_pending(self, values):
        now = self.clock()
        for value in values:
            since = self.data['records'].get(value['record_id'], {}).get('unknown_since')
            since = now if since is None else since
            if value['stage'] in UNKNOWN - {'attachment_unknown'} and now - since < self.policy['unknown_timeout']:
                return True
        return False

    @synchronized
    def reconciliation_allowed(self, value):
        since = self.data['records'].get(value['record_id'], {}).get('unknown_since')
        return (value['stage'] not in UNKNOWN or since is None
                or self.clock() - since < self.policy['unknown_timeout'])


def find_uploaded_source(api, prior, claimed_ids, *, jobs=None):
    """A same-name task is a candidate, not proof. Verify time, bytes and SHA256."""
    identity = prior.get('identity') or {}
    started = prior.get('dispatch_started_at')
    jobs = api.list_jobs() if jobs is None else jobs
    candidates = [job for job in jobs if job.get('source_name') == identity.get('name')
                  and job.get('id') not in claimed_ids and job.get('variant') == 1
                  and job.get('source_kind', 'video') == 'video']
    proof = {'candidates': [job['id'] for job in candidates], 'verified': [], 'recovered_id': None}
    # Legacy intents without a submission window cannot safely claim a task by name.
    if not isinstance(started, (int, float)) or not identity.get('sha256') or not identity.get('bytes'):
        proof['reason'] = 'Missing original dispatch time or source fingerprint'
        return None, proof
    end = prior.get('dispatch_finished_at', started + 360)
    candidates = [job for job in candidates if isinstance(job.get('created_at'), (int, float))
                  and started - 30 <= job['created_at'] <= end + 30]
    if len(candidates) != 1:
        proof['reason'] = 'No unique time-window candidate; no task selected or source downloaded'
        return None, proof
    for candidate in candidates:
        job = api.job(candidate['id'])
        if (job.get('source_name') != identity['name'] or not isinstance(job.get('created_at'), (int, float))
                or not started - 30 <= job['created_at'] <= end + 30 or job.get('variant') != 1):
            continue
        if api.source_matches(job['id'], job.get('source_file'), identity['bytes'], identity['sha256']):
            proof['verified'].append(job['id'])
    if len(proof['verified']) == 1:
        proof['recovered_id'] = proof['verified'][0]
        return {'request_id': prior['request_id'], 'ids': proof['verified'], 'receipt_confirmed': False}, proof
    proof['reason'] = 'No uniquely verified source; original request remains unknown'
    return None, proof
