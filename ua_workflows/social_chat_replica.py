"""Record-bound Replica API processing and verified Feishu replica-attachment backfill."""
from __future__ import annotations

import argparse
from collections import Counter
import json
import hashlib
import os
from pathlib import Path
import re
import time
import threading

import requests
import subprocess
import sys

from ua_workflows import social_chat_browser_daily as daily
from ua_workflows.replica_studio_api import ReplicaAPI, ReplicaError, digest, save_private, step_state
from ua_workflows.replica_cli import Checkpoint, operation_lock, upload_many, logo_many, download_many
from ua_workflows.replica_recovery import RunHealth, UNKNOWN, find_uploaded_source, timeout_policy
from ua_workflows.replica_pipeline import AsyncPipeline
from ua_workflows.xhs_daily import upload_attachment

SOURCE_FIELD_ID = 'fldRHIjFcO'
TARGET_FIELD_ID = 'fld6ygGkqD'
APP = 'Adz4bj9ECaCCJrsgkj7coheYn3c'
TABLE = 'tblwhAqPfQoBXK3Q'
TERMINAL = {'verified', 'skipped_existing', 'failed', 'needs_review', 'source_failed'} | UNKNOWN


class ReplicaFeishu(daily.Feishu):
    """Retry only read-only Feishu requests; uncertain writes are never replayed."""

    def __init__(self):
        for attempt in range(3):
            try:
                super().__init__()
                return
            except (requests.Timeout, requests.ConnectionError):
                if attempt == 2:
                    raise ReplicaError('read_failed', 'Feishu authorization network temporarily unavailable') from None
                time.sleep(attempt + 1)

    def call(self, method, route, **kwargs):
        if method != 'GET':
            return super().call(method, route, **kwargs)
        attempt, refreshed = 0, False
        while True:
            try:
                return super().call(method, route, **kwargs)
            except ValueError as error:
                if 'code=99991663' not in str(error) or refreshed:
                    raise
                # A long upload batch can outlive the token. Only the original
                # read is retried after obtaining a fresh application token.
                self.__init__()
                refreshed = True
            except (requests.Timeout, requests.ConnectionError):
                attempt += 1
                if attempt >= 3:
                    raise ReplicaError('read_failed', 'Feishu read-only request temporarily unavailable; preserve original write intents') from None
                time.sleep(attempt)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def fields_by_id(schema):
    by_id = {field['field_id']: field for field in schema}
    for field_id in (SOURCE_FIELD_ID, TARGET_FIELD_ID):
        require(field_id in by_id and by_id[field_id]['type'] == 17, 'Attachment field identity/type mismatch')
    return by_id[SOURCE_FIELD_ID]['field_name'], by_id[TARGET_FIELD_ID]['field_name']


def source_identity(attachment):
    return {key: attachment[key] for key in ('file_token', 'name', 'size')}


def prepare_rows(rows, schema, target_date, *, record_ids=None):
    source_field, target_field = fields_by_id(schema)
    selected = set(record_ids) if record_ids is not None else None
    items = []
    for row in rows:
        fields = row['fields']
        if fields.get('目标日期') != target_date or fields.get('素材类型') != '视频':
            continue
        if selected is not None and row['record_id'] not in selected:
            continue
        if fields.get(target_field):
            continue
        originals = fields.get(source_field) or []
        require(len(originals) == 1, 'A video record must have exactly one original attachment')
        source = source_identity(originals[0])
        name = source['name']
        require(Path(name).name == name and '\\' not in name and Path(name).suffix.lower() in {'.mp4', '.mov', '.webm', '.mkv'}, 'Unsupported or unsafe source filename')
        require(0 < source['size'] <= 100 * 1024 * 1024, 'Source exceeds Replica upload contract')
        items.append({'record_id': row['record_id'], 'source': source,
                      'advertiser': fields.get('广告主'), 'ad_id': fields.get('广告ID')})
    require(len({item['record_id'] for item in items}) == len(items), 'Duplicate record IDs')
    return {'version': 1, 'target_date': target_date, 'app_token': APP, 'table_id': TABLE,
            'source_field_id': SOURCE_FIELD_ID, 'target_field_id': TARGET_FIELD_ID,
            'source_field': source_field, 'target_field': target_field,
            'workflow': ['upload_auto_analyze', 'smart_logo', 'download_logo', 'replica_attachment'], 'items': items}


class State:
    def __init__(self, root):
        self._mutex = threading.RLock()
        self.root = Path(root).resolve()
        self.scope = json.loads((self.root / 'scope.json').read_text())
        self.items = {item['record_id']: item for item in self.scope['items']}

    def path(self, record_id):
        require(record_id in self.items and re.fullmatch(r'[A-Za-z0-9_-]+', record_id), 'Unknown record identity')
        return self.root / 'items' / (record_id + '.json')

    def get(self, record_id):
        path = self.path(record_id)
        return json.loads(path.read_text()) if path.exists() else {'record_id': record_id, 'stage': 'queued'}

    def put(self, record_id, value):
        require(value['record_id'] == record_id, 'State identity mismatch')
        with self._mutex:
            save_private(self.path(record_id), value)

    def all(self):
        return [self.get(record_id) for record_id in self.items]

    def summary(self, **extra):
        with self._mutex, operation_lock(self.root / 'progress'):
            values = self.all()
            counts = dict(Counter(item['stage'] for item in values))
            summary = {'target_date': self.scope['target_date'], 'total': len(self.items), 'counts': counts,
                       'verified': counts.get('verified', 0), 'all_terminal': all(item['stage'] in TERMINAL for item in values), **extra}
            previous = self.root / 'progress.json'
            if previous.exists():
                old_summary = json.loads(previous.read_text())
                for key in ('scheduling', 'batch_phase', 'reconciliation_pending'):
                    if key in old_summary:
                        summary.setdefault(key, old_summary[key])
            alerts = self.root / 'alerts.json'
            if alerts.exists():
                summary['active_alerts'] = json.loads(alerts.read_text()).get('active', [])
                summary['needs_attention'] = bool(summary['active_alerts'])
            save_private(self.root / 'progress.json', summary)
            return summary


def prepare(root, target_date, client=None, *, expected_count=None, record_ids=None):
    root = Path(root).resolve()
    config = daily.load(daily.CONFIG)
    require(config['feishu']['app_token'] == APP and config['feishu']['table_id'] == TABLE, 'Unexpected destination table')
    client = client or daily.Feishu()
    route = daily.table_route(config)
    schema, rows = client.listing(route + '/fields'), client.listing(route + '/records')
    scope = prepare_rows(rows, schema, target_date, record_ids=record_ids)
    root.mkdir(parents=True, exist_ok=True)
    if (root / 'scope.json').exists():
        prior = json.loads((root / 'scope.json').read_text())
        # A resumed run keeps its initial scope even after its own attachments have been written.
        require(prior['target_date'] == target_date and prior['app_token'] == APP and prior['table_id'] == TABLE, 'Existing batch scope differs')
        if expected_count is not None:
            require(len(prior['items']) == expected_count, 'Existing batch count differs')
        return State(root)
    if expected_count is not None:
        require(len(scope['items']) == expected_count, 'Live pending record count changed; review scope before generating')
    save_private(root / 'fields-before.json', schema)
    save_private(root / 'records-before.json', rows)
    save_private(root / 'scope.json', scope)
    state = State(root)
    for item in scope['items']:
        state.put(item['record_id'], {'record_id': item['record_id'], 'stage': 'queued'})
    return state


def record(client, route, record_id):
    result = client.call('GET', route + '/records/' + record_id, params={'text_field_as_array': 'true'})
    value = result.get('record')
    require(value and value['record_id'] == record_id, 'Feishu record identity mismatch')
    return daily.normalize_record_texts(value)


def check_record(row, item, scope):
    fields = row['fields']
    require(row['record_id'] == item['record_id'] and fields.get('目标日期') == scope['target_date'] and fields.get('素材类型') == '视频', 'Source row/date/type changed')
    sources = fields.get(scope['source_field']) or []
    require(len(sources) == 1 and source_identity(sources[0]) == item['source'], 'Original attachment changed')


def reuse_source(state, item):
    """Reuse a record-bound source only when its saved hash and byte size match."""
    value = state.get(item['record_id'])
    if value['stage'] not in {'queued', 'source_failed', 'source_ready'}:
        return False
    destination = Path(value.get('source_path') or state.root / 'sources' / item['record_id'] / item['source']['name'])
    if (not value.get('source_sha256') or not destination.is_file()
            or destination.stat().st_size != item['source']['size']
            or digest(destination) != value['source_sha256']):
        return False
    value.update(stage='source_ready', source_path=str(destination.resolve()), source_bytes_verified=True)
    value.pop('error', None)
    state.put(item['record_id'], value)
    return True


def fetch_source(state, item, client, route):
    record_id = item['record_id']
    value = state.get(record_id)
    if value['stage'] not in {'queued', 'source_failed', 'source_ready'}:
        return value
    destination = state.root / 'sources' / record_id / item['source']['name']
    if reuse_source(state, item):
        return state.get(record_id)
    row = record(client, route, record_id)
    check_record(row, item, state.scope)
    if row['fields'].get(state.scope['target_field']):
        value.update(stage='skipped_existing')
        state.put(record_id, value)
        return value
    source = row['fields'][state.scope['source_field']][0]
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(destination.name + '.part')
    for attempt in range(3):
        try:
            size = 0
            with client.session.get(source['url'], stream=True, timeout=(20, 120)) as response:
                response.raise_for_status()
                with partial.open('wb') as stream:
                    os.chmod(partial, 0o600)
                    for block in response.iter_content(1024 * 1024):
                        size += len(block)
                        require(size <= item['source']['size'], 'Original exceeds recorded byte size')
                        stream.write(block)
            require(size == item['source']['size'], 'Original byte size mismatch')
            partial.replace(destination)
            value.update(stage='source_ready', source_path=str(destination), source_sha256=digest(destination), source_bytes_verified=True)
            # An old decode receipt does not describe newly fetched bytes.
            value.pop('source_decode_passed', None)
            value.pop('error', None)
            state.put(record_id, value)
            return value
        except Exception:
            partial.unlink(missing_ok=True)
            if attempt == 2:
                raise
            time.sleep(attempt + 1)
            row = record(client, route, record_id)
            check_record(row, item, state.scope)
            source = row['fields'][state.scope['source_field']][0]


def download_sources(state, client=None):
    """Optional serial prefetch. The normal run also fetches missing files lazily."""
    parent = client
    route = daily.table_route(daily.load(daily.CONFIG))
    candidates = [item for item in state.scope['items'] if state.get(item['record_id'])['stage'] in {'queued', 'source_failed', 'source_ready'}]
    for done, item in enumerate(candidates, 1):
        try:
            if not reuse_source(state, item):
                parent = parent or ReplicaFeishu()
                fetch_source(state, item, parent, route)
        except Exception as error:
            value = state.get(item['record_id'])
            value.update(stage='source_failed', error=getattr(error, 'code', type(error).__name__))
            state.put(item['record_id'], value)
        if done % 10 == 0 or done == len(candidates):
            print(json.dumps({'phase': 'original_download', 'processed': done, **state.summary()}, ensure_ascii=False), flush=True)
    return state.summary()


def backfill(state, values, client, route, *, on_progress=None):
    updates, expected, before = [], {}, {}

    def persist(value):
        state.put(value['record_id'], value)
        if on_progress is not None:
            on_progress(value)

    for value in values:
        # Only the reconciliation path may handle an uncertain prior operation.
        if value['stage'] in {'write_intent', 'write_unknown', 'feishu_upload_intent', 'attachment_unknown'}:
            continue
        record_id = value['record_id']
        item, receipt = state.items[record_id], value['output']
        path = Path(receipt['path'])
        require(receipt.get('full_decode_passed') and path.is_file() and path.stat().st_size == receipt['size'] and digest(path) == receipt['sha256'], 'Unverified replica cannot be written')
        row = record(client, route, record_id)
        check_record(row, item, state.scope)
        old = row['fields'].get(state.scope['target_field']) or []
        token = value.get('feishu_token')
        if old:
            found = next((attachment for attachment in old if token and attachment['file_token'] == token), None)
            if found:
                require(found['name'] == receipt['name'] and found['size'] == receipt['size'], 'Existing replica receipt mismatch')
                value.update(stage='verified')
            else:
                value.update(stage='skipped_existing')
            persist(value)
            continue
        if not token:
            value.update(stage='feishu_upload_intent')
            persist(value)
            try:
                token = upload_attachment(client, {'feishu': {'app_token': APP}}, path, receipt['name'])
            except (requests.RequestException, TimeoutError):
                value.update(stage='attachment_unknown', error='Attachment upload receipt unknown; verified local result retained; no automatic POST replay')
                persist(value)
                continue
            require(isinstance(token, str) and token, 'Attachment upload returned no valid token')
            value.update(stage='feishu_uploaded', feishu_token=token)
            persist(value)
        row = record(client, route, record_id)
        check_record(row, item, state.scope)
        if row['fields'].get(state.scope['target_field']):
            value.update(stage='skipped_existing')
            persist(value)
            continue
        before[record_id] = row
        expected[record_id] = value
        updates.append({'record_id': record_id, 'fields': {state.scope['target_field']: [{'file_token': token}]}})
    if not updates:
        return
    batch_id = str(time.time_ns())
    journal_path = state.root / 'write-audit' / (batch_id + '-dispatch.json')
    save_private(state.root / 'write-audit' / (batch_id + '-before.json'), list(before.values()))
    journal = {'version': 2, 'batch_id': batch_id, 'phase': 'prepared',
               'records': {rid: {'file_token': v['feishu_token'], 'name': v['output']['name'], 'size': v['output']['size']}
                           for rid, v in expected.items()}}
    save_private(journal_path, journal)
    for value in expected.values():
        value.update(stage='write_intent', write_batch_id=batch_id)
        persist(value)
    # Only a durable prepared journal proves no POST was dispatched. After this
    # boundary even a process death before the call is conservatively unknown.
    journal.update(phase='dispatching', dispatched_at=time.time())
    save_private(journal_path, journal)
    try:
        client.call('POST', route + '/records/batch_update', json={'records': updates})
        journal['phase'] = 'accepted'
    except Exception as error:
        journal.update(phase='unknown', error=type(error).__name__)
    save_private(journal_path, journal)
    after = []
    for record_id, value in expected.items():
        row = record(client, route, record_id)
        after.append(row)
        check_record(row, state.items[record_id], state.scope)
        attachment = next((a for a in row['fields'].get(state.scope['target_field'], []) if a['file_token'] == value['feishu_token']), None)
        if attachment and attachment['name'] == value['output']['name'] and attachment['size'] == value['output']['size']:
            value.update(stage='verified', remote_name=attachment['name'], remote_size=attachment['size'])
            value.pop('error', None)
        else:
            value.update(stage='write_unknown', error='Remote attachment write could not be confirmed')
        persist(value)
    save_private(state.root / 'write-audit' / (batch_id + '-after.json'), after)


def stable_fields(value):
    if isinstance(value, list):
        return [stable_fields(item) for item in value]
    if isinstance(value, dict):
        return {key: stable_fields(item) for key, item in value.items()
                if not ('file_token' in value and key in {'url', 'tmp_url', 'tmp_download_url'})}
    return value


def reconcile_logos(state, api=None, *, checkpoint=None, clock=time.time, respect_cooldown=False, interval=60,
                    should_check=None, on_progress=None):
    """Read back uncertain Logo requests; never submits or retries generation."""
    checkpoint = checkpoint or json.loads((state.root / 'cli-state.json').read_text())
    api = api or ReplicaAPI.configured(expected_account=checkpoint['account_id'])
    require(api.authenticate()['account_id'] == checkpoint['account_id'], 'Logo reconciliation account changed')
    results = []
    for value in state.all():
        if value['stage'] != 'logo_unknown':
            continue
        if should_check is not None and not should_check(value):
            continue
        if respect_cooldown and clock() < value.get('logo_check_after', 0):
            continue
        job_id = value['job_id']
        prior = checkpoint['logos'].get(job_id)
        require(prior is not None, 'Unknown Logo has no original intent to reconcile')
        try:
            job = api.job(job_id)
        except ReplicaError as error:
            if error.code in {'authentication_required', 'account_changed', 'permission_denied'}:
                raise
            value.update(logo_check_after=clock() + interval, reconcile_error=error.code)
            state.put(value['record_id'], value)
            if on_progress is not None:
                on_progress()
            results.append({'record_id': value['record_id'], 'job_id': job_id, 'stage': value['stage'], 'read_error': error.code})
            continue
        require(job.get('id') == job_id, 'Logo reconciliation task identity mismatch')
        view = job.get('logo_replace_view') or {}
        condition = step_state(job, 'logo')
        prior_revision = (prior.get('continuation') or {}).get('revision', prior.get('revision', 0))
        same_action = job.get('last_action') == 'logo_replace' and job.get('revision', 0) > prior_revision
        proof = {'checked_at': clock(), 'job': job, 'prior_stage': prior.get('stage'), 'prior_revision': prior_revision}
        save_private(state.root / 'logo-reconciliation' / (job_id + '.json'), proof)
        value.update(logo_check_after=clock() + interval, last_status=job.get('status'), last_message=job.get('message'))
        if same_action and condition == 'ready' and view.get('result_file'):
            value.update(stage='logo_ready', visual_review_flagged=bool(view.get('flagged')),
                         visual_review_message=view.get('warning') or job.get('message'), recovered_logo_readonly=True)
            value.pop('error', None)
        elif same_action and condition == 'running':
            value.update(stage='logo_accepted', recovered_logo_readonly=True)
            value.pop('error', None)
        elif same_action and condition == 'error':
            value.update(stage='failed', error=job.get('error') or job.get('message') or 'Confirmed server Logo failure')
        elif same_action and condition in {'review', 'ready'}:
            value.update(stage='needs_review', error=job.get('message') or 'No verified Logo result')
        # Unconfirmed state remains quarantined; the worker's checkpoint is never
        # rewritten here because its live process owns that file.
        state.put(value['record_id'], value)
        if on_progress is not None:
            on_progress()
        results.append({'record_id': value['record_id'], 'job_id': job_id, 'stage': value['stage'], 'server_status': job.get('status')})
    return {'operation': 'reconcile_logos', 'results': results}


def verify(state, client=None, *, remote_samples=3):
    values = state.all()
    client = client or ReplicaFeishu()
    route = daily.table_route(daily.load(daily.CONFIG))
    schema = client.listing(route + '/fields')
    require(fields_by_id(schema) == (state.scope['source_field'], state.scope['target_field']), 'Attachment schema changed')
    rows = client.listing(route + '/records')
    save_private(state.root / 'records-after.json', rows)
    before = {row['record_id']: row for row in json.loads((state.root / 'records-before.json').read_text())}
    after = {row['record_id']: row for row in rows}
    verified, mismatches = [], []
    for value in values:
        if value['stage'] != 'verified':
            continue
        rid = value['record_id']
        row = after[rid]
        check_record(row, state.items[rid], state.scope)
        attachments = row['fields'].get(state.scope['target_field']) or []
        expected = {'file_token': value['feishu_token'], 'name': value['output']['name'], 'size': value['output']['size']}
        if len(attachments) != 1 or source_identity(attachments[0]) != expected:
            mismatches.append(rid)
        else:
            verified.append(value)
    changed = []
    for rid in before.keys() & after.keys():
        left, right = stable_fields(before[rid]['fields']), stable_fields(after[rid]['fields'])
        if rid in state.items:
            left.pop(state.scope['target_field'], None)
            right.pop(state.scope['target_field'], None)
        if left != right:
            changed.append(rid)
    samples = []
    if verified and remote_samples:
        choices = [verified[0], verified[len(verified) // 2], max(verified, key=lambda v: v['output']['size'])]
        chosen = list({value['record_id']: value for value in choices}.values())[:remote_samples]
        for value in chosen:
            rid = value['record_id']
            attachment = after[rid]['fields'][state.scope['target_field']][0]
            output = state.root / 'remote-verification' / rid / attachment['name']
            output.parent.mkdir(parents=True, exist_ok=True)
            size = 0
            with client.session.get(attachment['url'], stream=True, timeout=(20, 120)) as response:
                response.raise_for_status()
                with output.open('wb') as stream:
                    os.chmod(output, 0o600)
                    for block in response.iter_content(1024 * 1024):
                        size += len(block)
                        require(size <= attachment['size'], 'Remote verification exceeds expected length')
                        stream.write(block)
            require(size == value['output']['size'] and digest(output) == value['output']['sha256'], 'Remote verification hash mismatch')
            samples.append({'record_id': rid, 'size': size, 'sha256': digest(output), 'matches_local': True})
    result = {**state.summary(), 'remote_attachment_verified': len(verified), 'remote_mismatches': mismatches,
              'before_rows': len(before), 'after_rows': len(after), 'other_fields_changed_record_ids': changed,
              'added_record_ids': sorted(after.keys() - before.keys()), 'missing_record_ids': sorted(before.keys() - after.keys()),
              'remote_hash_samples': samples, 'visual_review_flagged': sum(bool(v.get('visual_review_flagged')) for v in verified)}
    result['exceptions'] = [{key: value.get(key) for key in ('record_id', 'job_id', 'stage', 'error', 'last_message')}
                            for value in values if value['stage'] in TERMINAL and value['stage'] not in {'verified', 'skipped_existing'}]
    result['integrity_passed'] = not mismatches and not changed and before.keys() == after.keys() and len(verified) == sum(v['stage'] == 'verified' for v in values)
    result['all_requested_results_written'] = len(verified) == len(state.items)
    uncertain = any(value['stage'] in UNKNOWN for value in values)
    authorized_exceptions = (state.root / 'exception-policy.json').exists()
    result['verification_passed'] = result['integrity_passed'] and result['all_terminal'] and not uncertain and (result['all_requested_results_written'] or authorized_exceptions)
    save_private(state.root / 'verification.json', result)
    return result


class Runner:
    def __init__(self, state, api_factory=None, feishu=None, *, timeouts=None, clock=time.time, wait=time.sleep):
        self.state = state
        self.health = RunHealth(state.root, timeouts, clock)
        self.wait = wait
        # run/resume executes the user's explicitly authorized, fixed-scope
        # workflow. This confirmation does not open any additional actions.
        self.factory = api_factory or (lambda: ReplicaAPI.configured(confirm_paid=True))
        self.api = self.factory()
        auth = self.api.authenticate()
        self.checkpoint = Checkpoint(state.root / 'cli-state.json', auth['account_id'])
        self.feishu = feishu or ReplicaFeishu()
        self._owns_feishu = feishu is None
        self._feishu_auth_at = time.monotonic()
        self.route = daily.table_route(daily.load(daily.CONFIG))
        require(fields_by_id(self.feishu.listing(self.route + '/fields')) == (state.scope['source_field'], state.scope['target_field']), 'Attachment schema changed')
        self.submit_paused = False

    def now(self):
        return self.health.clock() if hasattr(self, 'health') else time.time()

    def can_reconcile(self, value):
        return self.record_available(value['record_id']) and (not hasattr(self, 'health') or self.health.reconciliation_allowed(value))

    def observe(self, phase, **extra):
        if not hasattr(self, 'health'):
            self.health = RunHealth(self.state.root)
        downloads = len(self.checkpoint.data.get('downloads', {})) if hasattr(self, 'checkpoint') else 0
        values = self.state.all()
        self.health.observe(phase, values, downloads)
        return self.state.summary(scheduling=getattr(self, 'scheduling_mode', 'batch'), batch_phase=phase, submit_paused=self.submit_paused,
                                  reconciliation_pending=self.health.unknown_pending(values), **extra)

    def record_available(self, record_id):
        return True

    def stop_requested(self):
        return (self.state.root / 'pause-requested').exists()

    def refresh_feishu(self):
        if self._owns_feishu and (self.feishu is None or time.monotonic() - self._feishu_auth_at >= 3600):
            self.feishu = ReplicaFeishu()
            self._feishu_auth_at = time.monotonic()

    def deliver_records(self, values):
        self.refresh_feishu()
        backfill(self.state, values, self.feishu, self.route,
                 on_progress=lambda current: self.observe('backfill_all'))

    def reconcile_uploads(self):
        jobs = None
        for value in self.state.all():
            if value['stage'] not in {'failed', 'upload_unknown'}:
                continue
            if not self.can_reconcile(value):
                continue
            if self.now() < value.get('upload_check_after', 0):
                continue
            key = hashlib.sha256(str(Path(value.get('source_path', '')).resolve()).encode()).hexdigest()
            prior = self.checkpoint.get('uploads', key)
            if not prior or prior['stage'] not in {'upload_unknown', 'upload_intent'}:
                continue
            result = None
            try:
                result = self.api.upload_status(prior['request_id'])
            except ReplicaError as error:
                if error.code in {'authentication_required', 'account_changed', 'permission_denied'}:
                    raise
                value.update(stage='upload_unknown', error=error.code, request_id=prior['request_id'])
                if error.code == 'result_unknown':
                    claimed = {v['job_id'] for v in self.state.all() if v.get('job_id')}
                    try:
                        if jobs is None:
                            jobs = self.api.list_jobs()
                        result, proof = find_uploaded_source(self.api, prior, claimed, jobs=jobs)
                        save_private(self.state.root / 'upload-reconciliation' / (value['record_id'] + '.json'),
                                     {'checked_at': self.now(), 'original_request_id': prior['request_id'], **proof})
                    except ReplicaError as lookup_error:
                        if lookup_error.code in {'authentication_required', 'account_changed', 'permission_denied'}:
                            raise
                        value['reconcile_error'] = lookup_error.code
                interval = self.health.policy['reconcile_interval'] if hasattr(self, 'health') else 60
                value['upload_check_after'] = self.now() + interval
            if result:
                claimed = {v['job_id'] for v in self.state.all() if v.get('job_id') and v['record_id'] != value['record_id']}
                require(len(result['ids']) == 1 and result['ids'][0] not in claimed, 'Recovered upload belongs to another record')
                prior.update(stage='uploaded', ids=result['ids'], recovered_readonly=True,
                             receipt_confirmed=result.get('receipt_confirmed', True))
                self.checkpoint.put('uploads', key, prior)
                value.update(stage='uploaded', job_id=result['ids'][0], request_id=prior['request_id'],
                             original_receipt_confirmed=result.get('receipt_confirmed', True))
                value.pop('error', None)
            self.state.put(value['record_id'], value)
            if hasattr(self, 'health'):
                self.observe(self.health.data.get('phase', 'reconcile_unknown'))

    def continue_interrupted_logos(self):
        # The site's Continue button uses this same action with pending.settings,
        # force_new=false. Only a confirmed server interruption is eligible;
        # an unknown POST is never replayed by this path.
        for value in self.state.all():
            if self.stop_requested():
                break
            if not self.record_available(value['record_id']):
                continue
            if value['stage'] != 'failed' or value.get('last_status') != 'interrupted' or not value.get('job_id'):
                continue
            job_id = value['job_id']
            prior = self.checkpoint.get('logos', job_id)
            if not prior or prior['stage'] != 'logo_accepted' or prior.get('continuation'):
                continue
            job = self.api.job(job_id)
            view = job.get('logo_replace_view') or {}
            pending = view.get('pending') or {}
            options = pending.get('settings') or {}
            if job.get('status') != 'interrupted' or job.get('last_action') != 'logo_replace' or pending.get('status') != 'interrupted' or options.get('logo_mode') != 'smart':
                continue
            require(all(prior['options'].get(key) == item for key, item in options.items()), 'Interrupted Logo settings differ from original intent')
            prior['continuation'] = {'stage': 'intent', 'revision': job['revision'], 'options': options}
            self.checkpoint.put('logos', job_id, prior)
            value['stage'] = 'logo_submitting'
            self.state.put(value['record_id'], value)
            try:
                receipt = self.api.smart_logo(job, {**options, 'use_defaults': True})
                prior['continuation'].update(stage='accepted', receipt=receipt)
                self.checkpoint.put('logos', job_id, prior)
                value.update(stage='logo_accepted')
                value.pop('error', None)
            except ReplicaError as error:
                prior['continuation'].update(stage='unknown', error=error.code)
                self.checkpoint.put('logos', job_id, prior)
                value.update(stage='logo_unknown', error=error.code)
                self.submit_paused = True
            self.state.put(value['record_id'], value)

    def reconcile_writes(self):
        for value in self.state.all():
            if not self.record_available(value['record_id']):
                continue
            if value['stage'] in {'write_intent', 'write_unknown'}:
                if not self.can_reconcile(value):
                    continue
                if self.now() < value.get('write_check_after', 0):
                    continue
                row = record(self.feishu, self.route, value['record_id'])
                check_record(row, self.state.items[value['record_id']], self.state.scope)
                found = next((a for a in row['fields'].get(self.state.scope['target_field'], [])
                              if a['file_token'] == value.get('feishu_token')), None)
                receipt = value['output']
                if found and found['name'] == receipt['name'] and found['size'] == receipt['size']:
                    value.update(stage='verified', remote_name=found['name'], remote_size=found['size'])
                    value.pop('error', None)
                else:
                    batch_id = value.get('write_batch_id', '')
                    require(not batch_id or re.fullmatch(r'[0-9]+', batch_id), 'Invalid write batch journal identity')
                    path = self.state.root / 'write-audit' / (batch_id + '-dispatch.json')
                    journal = json.loads(path.read_text()) if batch_id and path.exists() else {}
                    identity = {'file_token': value.get('feishu_token'), 'name': receipt['name'], 'size': receipt['size']}
                    unsent = (journal.get('version') == 2 and journal.get('batch_id') == batch_id
                              and journal.get('phase') == 'prepared' and journal.get('records', {}).get(value['record_id']) == identity)
                    if unsent and not row['fields'].get(self.state.scope['target_field']):
                        value.update(stage='feishu_uploaded')
                        value.pop('error', None)
                        value.pop('write_batch_id', None)
                    else:
                        value.update(stage='write_unknown', error='Interrupted write not confirmed; no automatic replay',
                                     write_check_after=self.now() + (self.health.policy['reconcile_interval'] if hasattr(self, 'health') else 60))
                self.state.put(value['record_id'], value)
            elif value['stage'] == 'feishu_upload_intent':
                value.update(stage='attachment_unknown', error='Interrupted attachment upload; verified local result retained, no token receipt and no POST replay')
                self.state.put(value['record_id'], value)

    def reconcile_unknown_logos(self):
        if any(value['stage'] == 'logo_unknown' and self.can_reconcile(value) and self.now() >= value.get('logo_check_after', 0) for value in self.state.all()):
            reconcile_logos(self.state, self.api, checkpoint=self.checkpoint.data, clock=self.now,
                            respect_cooldown=True, interval=self.health.policy['reconcile_interval'], should_check=self.can_reconcile,
                            on_progress=lambda: self.observe(self.health.data.get('phase', 'reconcile_unknown')))

    def poll(self, record_ids=None):
        selected = set(record_ids) if record_ids is not None else None
        values = [value for value in self.state.all() if value['stage'] in {'uploaded', 'logo_accepted', 'logo_submitting'}
                  and (selected is None or value['record_id'] in selected)]
        for value in values:
            try:
                job = self.api.job(value['job_id'])
                require(job.get('id') == value['job_id'], 'Replica polling task identity changed')
                value.pop('poll_error', None)
                stage = 'analysis' if value['stage'] == 'uploaded' else 'logo'
                condition = step_state(job, stage)
                if value['stage'] == 'logo_submitting':
                    intent = self.checkpoint.get('logos', value['job_id'])
                    if intent and intent.get('stage') != 'logo_accepted':
                        revision = (intent.get('continuation') or {}).get('revision', intent.get('revision', 0))
                        if job.get('last_action') != 'logo_replace' or job.get('revision', 0) <= revision:
                            condition = 'waiting'
                value.update(project_id=job.get('project_id'), last_status=job.get('status'), last_message=job.get('message'))
                if condition == 'ready':
                    value['stage'] = 'analysis_ready' if stage == 'analysis' else 'logo_ready'
                    if stage == 'logo':
                        view = job.get('logo_replace_view') or {}
                        value['visual_review_flagged'] = bool(view.get('flagged'))
                        value['visual_review_message'] = view.get('warning') or job.get('message')
                        if not view.get('result_file'):
                            value.update(stage='needs_review', error='Smart Logo returned no downloadable result; original will not be written as replica')
                elif condition in {'error', 'review'}:
                    value.update(stage='failed' if condition == 'error' else 'needs_review', error=job.get('error') or job.get('message'))
                elif stage == 'analysis' and job.get('status') == 'uploaded' and not job.get('last_action') and time.time() - job.get('created_at', time.time()) > 300:
                    upload = self.api.upload_status(value['request_id'])
                    if upload.get('analysis_status') in {'not_requested', 'failed', 'interrupted'} or (upload.get('analysis_status') == 'unknown' and time.time() - job['created_at'] > 600):
                        value.update(stage='needs_review', error='Source saved but auto-analysis not queued; no extra analysis request submitted', upload_analysis_status=upload['analysis_status'])
                elif value['stage'] == 'logo_submitting':
                    checkpoint = self.checkpoint.get('logos', value['job_id'])
                    if checkpoint and checkpoint.get('stage') == 'logo_accepted':
                        value['stage'] = 'logo_accepted'
                    elif checkpoint and job.get('last_action') == 'logo_replace' and condition == 'running' and job.get('revision', 0) > checkpoint.get('revision', 0):
                        checkpoint.update(stage='logo_accepted', recovered_readonly=True,
                                          receipt={key: job[key] for key in ('id', 'status', 'revision')})
                        self.checkpoint.put('logos', value['job_id'], checkpoint)
                        value['stage'] = 'logo_accepted'
                    elif checkpoint:
                        value['stage'] = 'logo_unknown'
                        self.submit_paused = True
                    else:
                        value['stage'] = 'analysis_ready'
                self.state.put(value['record_id'], value)
                if hasattr(self, 'health'):
                    self.observe(self.health.data.get('phase', 'wait_all_analyses'))
            except ReplicaError as error:
                if error.code in {'authentication_required', 'account_changed', 'permission_denied'}:
                    self.submit_paused = True
                value['poll_error'] = error.code
                self.state.put(value['record_id'], value)
                if hasattr(self, 'health'):
                    self.observe(self.health.data.get('phase', 'wait_all_analyses'))

    def upload_all(self, record_ids=None):
        """Prepare each missing source just in time, then submit without a job cap."""
        by_path = {}
        selected = set(record_ids) if record_ids is not None else None

        def paths():
            for value in self.state.all():
                if self.submit_paused or self.stop_requested():
                    break
                if value['stage'] not in {'queued', 'source_ready'}:
                    continue
                if selected is not None and value['record_id'] not in selected:
                    continue
                item = self.state.items[value['record_id']]
                try:
                    if not reuse_source(self.state, item):
                        self.refresh_feishu()
                        fetch_source(self.state, item, self.feishu, self.route)
                    value = self.state.get(item['record_id'])
                    if value['stage'] != 'source_ready':
                        continue
                except (ReplicaError, requests.RequestException):
                    # Reads can be retried on the next tick; no upload intent exists.
                    raise
                except (OSError, ValueError) as error:
                    value.update(stage='source_failed', error=getattr(error, 'code', type(error).__name__))
                    self.state.put(value['record_id'], value)
                    continue
                path = str(Path(value['source_path']).resolve())
                require(path not in by_path, 'One local source is bound to multiple records')
                by_path[path] = value
                yield path

        def uploaded(item):
            value = by_path[item['path']]
            if item.get('ok'):
                value.update(stage='uploaded', job_id=item['ids'][0], request_id=item['request_id'])
                value.pop('error', None)
            elif item.get('code') == 'not_submitted':
                value['stage'] = 'source_ready'
            else:
                key = hashlib.sha256(item['path'].encode()).hexdigest()
                prior = self.checkpoint.get('uploads', key) or {}
                uncertain = item.get('code') == 'result_unknown' or prior.get('stage') in {'upload_intent', 'upload_unknown'}
                value.update(stage='upload_unknown' if uncertain else 'failed',
                             error=item.get('code'))
            self.state.put(value['record_id'], value)
            if item.get('code') in {'not_submitted', 'result_unknown', 'authentication_required', 'account_changed', 'permission_denied', 'local_io_failed'}:
                self.submit_paused = True
            self.observe('upload_all')

        result = upload_many(lambda: self.api, paths(), self.checkpoint, on_result=uploaded)
        self.submit_paused = result['stopped'] or self.submit_paused
        return result

    def submit_all_logos(self, record_ids=None, *, options=None):
        values = self.state.all()
        selected = set(record_ids) if record_ids is not None else None
        eligible = [value for value in values if value['stage'] == 'analysis_ready' and (selected is None or value['record_id'] in selected)]
        if not eligible:
            return {'selected': 0, 'results': [], 'stopped': False}
        ids = [value['job_id'] for value in values if value.get('job_id')]
        require(len(ids) == len(set(ids)), 'One Replica job is mapped to more than one Feishu record')
        gate_path = self.state.root / 'analysis-gate.json' if selected is None else self.state.root / 'analysis-ready' / (eligible[0]['record_id'] + '.json')
        save_private(gate_path, {
            'confirmed_at': time.time(), 'total': len(values),
            'eligible_record_ids': [value['record_id'] for value in eligible],
            'excluded': [{key: value.get(key) for key in ('record_id', 'stage', 'error')}
                         for value in values if value['stage'] in TERMINAL and value['stage'] != 'verified'],
            'all_sources_accounted': selected is None,
            'all_nonexception_analyses_ready_or_already_processed': selected is None,
            'gate_scope': 'batch' if selected is None else 'selected_ready_records'})
        for value in eligible:
            value['stage'] = 'logo_submitting'
            self.state.put(value['record_id'], value)
        by_id = {value['job_id']: value for value in eligible}

        def submitted(item):
            value = by_id[item['id']]
            if item.get('ok'):
                value['stage'] = 'logo_ready' if item['code'] == 'already_done' else 'logo_accepted'
            elif item.get('code') == 'not_submitted':
                value['stage'] = 'analysis_ready'
            else:
                prior = self.checkpoint.get('logos', value['job_id']) or {}
                uncertain = item.get('code') in {'result_unknown', 'logo_unknown', 'logo_intent'} or prior.get('stage') in {'logo_intent', 'logo_unknown'}
                value.update(stage='logo_unknown' if uncertain else 'failed', error=item.get('code'))
            self.state.put(value['record_id'], value)
            self.observe('submit_all_logos')

        result = logo_many(self.api, list(by_id), self.checkpoint, on_result=submitted, options=options)
        self.submit_paused = result['stopped']
        return result

    def download_all(self, record_ids=None):
        selected = set(record_ids) if record_ids is not None else None
        values = [value for value in self.state.all() if value['stage'] == 'logo_ready' and (selected is None or value['record_id'] in selected)]
        by_id = {value['job_id']: value for value in values}

        def downloaded(item):
            if item.get('ok'):
                value = by_id[item['id']]
                value.update(stage='downloaded', output={k: item[k] for k in ('path', 'name', 'size', 'sha256', 'full_decode_passed')})
                self.state.put(value['record_id'], value)
            self.observe('download_all')

        try:
            result = download_many(self.api, [value['job_id'] for value in values], self.checkpoint, self.state.root / 'results', on_result=downloaded)
        except ReplicaError as error:
            if error.status != 400:
                raise
            result = {'results': []}
            for value in values:
                try:
                    single = download_many(self.api, [value['job_id']], self.checkpoint, self.state.root / 'results', on_result=downloaded)
                    result['results'].extend(single['results'])
                except ReplicaError as individual:
                    if individual.status != 400:
                        raise
                    result['results'].append({'id': value['job_id'], 'ok': False, 'code': 'no_downloadable_result'})
        for value in values:
            files = [item for item in result['results'] if item['id'] == value['job_id'] and item.get('ok')]
            if len(files) == 1:
                value.update(stage='downloaded', output={k: files[0][k] for k in ('path', 'name', 'size', 'sha256', 'full_decode_passed')})
            else:
                value['download_attempts'] = value.get('download_attempts', 0) + 1
                if value['download_attempts'] >= 3:
                    value.update(stage='failed', error='Replica download could not be verified')
            self.state.put(value['record_id'], value)

    def tick(self):
        if (self.state.root / 'pause-requested').exists():
            return self.state.summary(pause_requested=True)
        try:
            self.api.authenticate()
        except ReplicaError as error:
            self.submit_paused = True
            self.health.alert(error.code, message='Replica session unavailable; no new business action submitted')
            summary = self.observe(self.health.data.get('phase', 'initialization_wait'))
            return {**summary, 'health_error': error.code}
        self.submit_paused = False
        self.observe(self.health.data.get('phase', 'initialization'))
        for code in ('authentication_required', 'account_changed', 'permission_denied'):
            self.health.clear(code)
        self.reconcile_uploads()
        self.reconcile_unknown_logos()
        self.reconcile_writes()
        values = self.state.all()
        if any(value['stage'] in {'queued', 'source_ready'} for value in values):
            phase = 'upload_all'
            self.observe(phase)
            self.upload_all()
        elif any(value['stage'] == 'uploaded' for value in values):
            phase = 'wait_all_analyses'
            self.observe(phase)
            self.poll()
        elif any(value['stage'] == 'analysis_ready' for value in values):
            phase = 'submit_all_logos'
            self.observe(phase)
            self.submit_all_logos()
        else:
            self.continue_interrupted_logos()
            values = self.state.all()
            if any(value['stage'] in {'logo_accepted', 'logo_submitting'} for value in values):
                phase = 'wait_all_logos'
                self.observe(phase)
                self.poll()
            elif any(value['stage'] == 'logo_ready' for value in values):
                phase = 'download_all'
                self.observe(phase)
                save_private(self.state.root / 'logo-gate.json', {
                    'confirmed_at': time.time(), 'all_nonexception_logos_finished': True,
                    'ready_record_ids': [value['record_id'] for value in values if value['stage'] == 'logo_ready']})
                self.download_all()
            else:
                phase = 'reconcile_unknown' if any(value['stage'] in UNKNOWN for value in values) else 'backfill_all'
                self.observe(phase)
                ready = [value for value in values if value['stage'] in {'downloaded', 'feishu_uploaded'}]
                # Official Bitable batch_update accepts at most 500 records.
                # This is a write payload bound, not an in-flight processing cap.
                for start in range(0, len(ready), 500):
                    self.refresh_feishu()
                    backfill(self.state, ready[start:start + 500], self.feishu, self.route,
                             on_progress=lambda value: self.observe('backfill_all'))
        for code in ('read_failed', 'network_wait'):
            self.health.clear(code)
        summary = self.observe(phase)
        print(json.dumps({'phase': 'staged_processing', **summary}, ensure_ascii=False), flush=True)
        return summary

    def run(self):
        with operation_lock(self.state.root / 'batch-run'):
            while True:
                try:
                    summary = self.tick()
                except (requests.Timeout, requests.ConnectionError):
                    self.health.alert('network_wait', message='Transport interrupted; resume from durable receipts, never replay uncertain POSTs')
                    summary = {**self.observe(self.health.data.get('phase', 'service_wait')), 'health_error': 'read_failed'}
                except ReplicaError as error:
                    if error.code not in {'read_failed', 'authentication_required', 'account_changed', 'permission_denied'} and not (error.status and error.status >= 500):
                        raise
                    self.health.alert(error.code, message='HTTP workflow waiting; business write intents retained')
                    summary = {**self.observe(self.health.data.get('phase', 'service_wait')), 'health_error': error.code}
                if summary.get('pause_requested') or (summary['all_terminal'] and not self.health.unknown_pending(self.state.all())):
                    if summary['all_terminal'] and not any(v['stage'] in UNKNOWN for v in self.state.all()):
                        summary = self.observe('complete')
                    return summary
                if summary.get('health_error') in {'authentication_required', 'account_changed', 'permission_denied'}:
                    return summary
                delay = 30 if summary.get('health_error') else 8
                if summary['all_terminal'] and summary.get('reconciliation_pending'):
                    delay = min(30, self.health.policy['reconcile_interval'])
                self.wait(delay)


class StagedRunner(Runner):
    """Explicit whole-batch compatibility scheduler."""


class PipelineRunner(AsyncPipeline, Runner):
    _terminal_stages = TERMINAL


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'download-sources', 'run', 'status', 'verify', 'pause', 'resume', 'reconcile-logos'])
    parser.add_argument('--run-dir', required=True, type=Path)
    parser.add_argument('--date')
    parser.add_argument('--expected-count', type=int)
    parser.add_argument('--background', action='store_true', help='Run a detached worker with durable log and final verification')
    parser.add_argument('--scheduling', choices=['pipeline', 'batch'], help='新批默认pipeline：每条完成即推进；batch保留整批阶段等待')
    parser.add_argument('--stall-timeout', type=float, help='无可见进展告警秒数，默认900；不重发生成')
    parser.add_argument('--stage-timeout', type=float, help='阶段超时告警秒数，默认7200；pipeline按逐任务阶段计时，batch按整阶段计时')
    parser.add_argument('--unknown-timeout', type=float, help='未知回执只读核对窗口秒数，默认900；超时保留未知')
    parser.add_argument('--reconcile-interval', type=float, help='未知回执核对间隔秒数，默认60')
    args = parser.parse_args()
    overrides = {key: getattr(args, key) for key in ('stall_timeout', 'stage_timeout', 'unknown_timeout', 'reconcile_interval') if getattr(args, key) is not None}
    if overrides:
        require(args.action in {'run', 'resume'}, 'Timeout options only apply to run/resume')
        policy_path = Path(args.run_dir) / 'timeout-policy.json'
        prior = json.loads(policy_path.read_text()) if policy_path.exists() else {}
        policy = timeout_policy({**prior, **overrides})
        save_private(policy_path, policy)
    if args.scheduling:
        require(args.action in {'run', 'resume'}, 'Scheduling can only be changed for run/resume')
        state = State(args.run_dir)
        save_private(state.root / 'scheduling.json', {'mode': args.scheduling, 'updated_at': time.time()})
    if args.action == 'resume':
        state = State(args.run_dir)
        (state.root / 'pause-requested').unlink(missing_ok=True)
        args.action = 'run'
    if args.background:
        require(args.action == 'run', 'Background mode is only for run')
        state = State(args.run_dir)
        log_path = state.root / 'worker.log'
        with log_path.open('a') as log:
            os.chmod(log_path, 0o600)
            child = subprocess.Popen([sys.executable, '-u', '-m', 'ua_workflows.social_chat_replica', 'run',
                                      '--run-dir', str(state.root)], cwd=Path(__file__).resolve().parents[1],
                                     stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
        print(json.dumps({'worker_pid': child.pid, 'run_dir': str(state.root), 'log': str(log_path)}), flush=True)
        return
    if args.action == 'prepare':
        require(args.date, 'Target date is required')
        state = prepare(args.run_dir, args.date, expected_count=args.expected_count)
        result = state.summary()
    else:
        state = State(args.run_dir)
        if args.action == 'download-sources':
            result = download_sources(state)
        elif args.action == 'run':
            policy_path = state.root / 'timeout-policy.json'
            policy = timeout_policy(json.loads(policy_path.read_text()) if policy_path.exists() else None)
            health = RunHealth(state.root, policy)
            worker = {'pid': os.getpid(), 'started_at': time.time(), 'phase': 'running'}
            save_private(state.root / 'worker.json', worker)
            try:
                while True:
                    try:
                        scheduling_path = state.root / 'scheduling.json'
                        mode = json.loads(scheduling_path.read_text()).get('mode') if scheduling_path.exists() else 'pipeline'
                        require(mode in {'pipeline', 'batch'}, 'Invalid scheduling mode')
                        runner_type = PipelineRunner if mode == 'pipeline' else Runner
                        result = runner_type(state, timeouts=policy).run()
                        break
                    except ReplicaError as error:
                        if error.code != 'read_failed' and not (error.status and error.status >= 500):
                            raise
                        print(json.dumps({'phase': 'service_wait', 'health_error': error.code, **state.summary()}, ensure_ascii=False), flush=True)
                        health = RunHealth(state.root, policy)
                        health.alert(error.code, message='Initialization read failed; no business action replayed')
                        health.observe(health.data.get('phase', 'initialization_wait'), state.all())
                        time.sleep(30)
                if result['all_terminal']:
                    for attempt in range(3):
                        try:
                            result = verify(state)
                            break
                        except (requests.RequestException, ReplicaError):
                            if attempt == 2:
                                raise
                            time.sleep(10)
                worker.update(phase='complete' if result.get('verification_passed') else 'paused' if result.get('pause_requested') else 'needs_attention', finished_at=time.time(), summary=result)
            except BaseException as error:
                health = RunHealth(state.root, policy)
                health.alert(getattr(error, 'code', type(error).__name__), message='Worker needs attention; durable receipts and unknown intents retained')
                worker.update(phase='needs_attention', finished_at=time.time(), error=getattr(error, 'code', type(error).__name__))
                raise
            finally:
                save_private(state.root / 'worker.json', worker)
        elif args.action == 'pause':
            save_private(state.root / 'pause-requested', {'requested_at': time.time()})
            result = {'pause_requested': True, **state.summary()}
        elif args.action == 'verify':
            result = verify(state)
        elif args.action == 'reconcile-logos':
            result = reconcile_logos(state)
        else:
            result = state.summary()
    print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
