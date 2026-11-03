"""Per-record asynchronous progression; one upload lane and one delivery lane.

The event loop coordinates bounded blocking HTTP work. Video analysis and
Logo generation remain server jobs; there is no per-video local worker fanout.
"""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
import copy
import threading

import requests

from ua_workflows.replica_recovery import UNKNOWN
from ua_workflows.replica_studio_api import ReplicaError


class AsyncPipeline:
    scheduling_mode = 'pipeline'

    def __init__(self, *args, poll_interval=8, **kwargs):
        super().__init__(*args, **kwargs)
        self.poll_interval = poll_interval
        self._guard = threading.RLock()
        self._busy = {}
        self._running = set()
        self._operations = {}
        self._halt = threading.Event()
        self._outcome = {'paused': False, 'fatal': None}
        self._loop = None
        self._events = {}
        self._role = 'control'

    def record_available(self, record_id):
        with self._guard:
            return record_id not in self._busy

    def stop_requested(self):
        return self._halt.is_set() or super().stop_requested()

    def _notify(self):
        if self._loop is not None and not self._loop.is_closed():
            targets = ('control',) if self._role != 'control' else ('upload', 'delivery')
            for role in targets:
                self._loop.call_soon_threadsafe(self._events[role].set)

    def _request_stop(self):
        self._halt.set()
        if self._loop is not None and not self._loop.is_closed():
            for event in self._events.values():
                self._loop.call_soon_threadsafe(event.set)

    def observe(self, phase, **extra):
        with self._guard:
            self._operations[self._role] = phase
            operations = dict(self._operations)
            # Keep snapshots ordered across lanes so a late observation cannot
            # replace a newer watchdog stage with an older one.
            summary = super().observe('complete' if phase == 'complete' else 'pipeline',
                                      active_operations=operations, **extra)
        self._notify()
        return summary

    @contextmanager
    def _claim(self, record_id, stages):
        with self._guard:
            value = self.state.get(record_id)
            claimed = record_id not in self._busy and value['stage'] in stages and not self.stop_requested()
            if claimed:
                self._busy[record_id] = self._role
        try:
            yield value if claimed else None
        finally:
            if claimed:
                with self._guard:
                    self._busy.pop(record_id)
                self._notify()

    def _clone_lane(self, role):
        lane = copy.copy(self)
        lane._role = role
        # Requests sessions/CSRF state are not shared between simultaneous lanes.
        lane.api = self.factory()
        if lane.api.authenticate()['account_id'] != self.checkpoint.account:
            raise ReplicaError('account_changed', 'Pipeline lane account changed')
        if lane._owns_feishu:
            lane.feishu = None  # Acquire a separate client only when first needed.
        return lane

    @staticmethod
    def _check_submission(result):
        for item in result.get('results', []):
            code = item.get('reason') or item.get('code')
            if code in {'authentication_required', 'account_changed', 'permission_denied', 'read_failed'}:
                raise ReplicaError(code, 'Pipeline submission waiting; original identities retained')

    def _upload_once(self):
        for item in self.state.all():
            if item['stage'] not in {'queued', 'source_ready'}:
                continue
            with self._claim(item['record_id'], {'queued', 'source_ready'}) as value:
                if value is None:
                    continue
                self.submit_paused = False
                result = self.upload_all([value['record_id']])
                self._check_submission(result)
                self.observe('upload')
                return True
        return False

    def _control_once(self):
        self.api.authenticate()
        self.submit_paused = False
        self.reconcile_uploads()
        self.reconcile_unknown_logos()
        self.reconcile_writes()
        self.continue_interrupted_logos()
        options = None
        # Each completed record advances before the next record is polled.
        for item in self.state.all():
            if self.stop_requested():
                break
            rid = item['record_id']
            if item['stage'] in {'uploaded', 'logo_accepted', 'logo_submitting'}:
                with self._claim(rid, {'uploaded', 'logo_accepted', 'logo_submitting'}) as value:
                    if value is not None:
                        self.poll([rid])
            with self._claim(rid, {'analysis_ready'}) as value:
                if value is None:
                    continue
                if options is None:
                    options = self.api.smart_options()
                result = self.submit_all_logos([rid], options=options)
                self._check_submission(result)
                if result['stopped']:
                    break  # Reconcile an unknown POST before another submission round.
        self.observe('poll_and_submit')
        return True

    def _deliver_once(self):
        for item in self.state.all():
            if item['stage'] not in {'logo_ready', 'downloaded', 'feishu_uploaded'}:
                continue
            if self.now() < item.get('download_check_after', 0):
                continue
            rid = item['record_id']
            with self._claim(rid, {'logo_ready', 'downloaded', 'feishu_uploaded'}) as value:
                if value is None:
                    continue
                if value['stage'] == 'logo_ready':
                    self.download_all([rid])
                    value = self.state.get(rid)
                    if value['stage'] == 'logo_ready':
                        value['download_check_after'] = self.now() + self.health.policy['reconcile_interval']
                        self.state.put(rid, value)
                if value['stage'] in {'downloaded', 'feishu_uploaded'} and not self.stop_requested():
                    # One completed record can be written while other server jobs run.
                    self.deliver_records([value])
                self.observe('deliver')
                return True
        return False

    def _finished(self):
        with self._guard:
            if self._busy or self._running:
                return False
        values = self.state.all()
        return (all(value['stage'] in self._terminal_stages for value in values)
                and not self.health.unknown_pending(values))

    async def _worker(self, role):
        lane = self if role == 'control' else None
        while not self._halt.is_set():
            if (self.state.root / 'pause-requested').exists():
                self._outcome['paused'] = True
                self._request_stop()
                break
            event = self._events[role]
            event.clear()
            retry = False
            try:
                if lane is None:
                    lane = await asyncio.to_thread(self._clone_lane, role)
                operation = {'upload': lane._upload_once, 'control': lane._control_once,
                             'delivery': lane._deliver_once}[role]
                with self._guard:
                    self._running.add(role)
                try:
                    worked = await asyncio.to_thread(operation)
                finally:
                    with self._guard:
                        self._running.discard(role)
                self.health.clear(role + '_network_wait')
            except (requests.Timeout, requests.ConnectionError) as error:
                self.health.alert(role + '_network_wait', message='Network interrupted; resume only from durable receipts')
                worked, retry = False, True
            except ReplicaError as error:
                if error.code == 'read_failed' or (error.status and error.status >= 500):
                    self.health.alert(role + '_network_wait', message='Read unavailable; no business POST replay')
                    worked, retry = False, True
                else:
                    self.health.alert(error.code, message='Pipeline needs attention; in-flight requests will finish before exit')
                    self._outcome['fatal'] = error
                    self._request_stop()
                    break
            except Exception as error:
                self.health.alert(type(error).__name__, message='Pipeline stopped; durable identities retained')
                self._outcome['fatal'] = error
                self._request_stop()
                break
            if self._finished():
                self._request_stop()
                break
            if role != 'control' and worked:
                continue
            try:
                await asyncio.wait_for(event.wait(), timeout=30 if retry else self.poll_interval)
            except asyncio.TimeoutError:
                pass

    async def run_async(self):
        self._loop = asyncio.get_running_loop()
        self._events = {role: asyncio.Event() for role in ('upload', 'control', 'delivery')}
        self.observe('pipeline')
        # Errors request a stop; gather still waits for all in-flight HTTP work.
        # Cancelling a business POST and then resubmitting would be unsafe.
        try:
            await asyncio.gather(*(self._worker(role) for role in self._events))
        except BaseException:
            self._request_stop()
            raise
        if self._outcome['fatal'] is not None:
            raise self._outcome['fatal']
        if self._outcome['paused'] or (self.state.root / 'pause-requested').exists():
            return self.observe('pipeline', pause_requested=True)
        if any(value['stage'] in UNKNOWN for value in self.state.all()):
            return self.observe('pipeline', needs_attention=True)
        return self.observe('complete')

    def run(self):
        from ua_workflows.replica_cli import operation_lock
        with operation_lock(self.state.root / 'batch-run'):
            return asyncio.run(self.run_async())
