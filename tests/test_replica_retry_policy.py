"""Offline request-boundary and scheduler recovery tests for the agreed policy."""
import asyncio
from collections import Counter
from email.utils import formatdate
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import requests

import test_replica_device_workflow_contract as contract
import test_replica_pipeline as pipeline
from ua_workflows import social_chat_replica as flow
from ua_workflows.replica_studio_api import ReplicaError, retry_after_seconds, save_private


class RetryHTTPTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        contract.DeviceWorkflowContractTests.setUpClass.__func__(cls)

    setUp = contract.DeviceWorkflowContractTests.setUp
    tearDown = contract.DeviceWorkflowContractTests.tearDown
    command = contract.DeviceWorkflowContractTests.command
    calls = contract.DeviceWorkflowContractTests.calls

    def upload_record(self):
        data = json.loads(self.checkpoint.read_text())
        return next(iter(data['uploads'].values()))

    def test_explicit_unprocessed_503_retries_with_same_id_after_durable_wait(self):
        now = [1000]
        self.server.overrides['POST', '/api/jobs'] = (
            503, {'detail': '工作区正在准备，请稍后'}, {'Retry-After': '30'})
        with patch('ua_workflows.replica_cli.time.time', side_effect=lambda: now[0]):
            first = self.command('upload', str(self.source))
            self.assertEqual(first['accepted'], 0)
            self.assertEqual(first['results'][0]['code'], 'temporarily_rejected')
            record = self.upload_record()
            self.assertEqual(record['stage'], 'upload_retry')
            self.assertTrue(record['request_not_applied'])
            self.assertEqual(record['retry_at'], 1030)
            self.command('upload', str(self.source))
            self.assertEqual(len(self.calls('POST', '/api/jobs')), 1)
            self.server.overrides.pop(('POST', '/api/jobs'))
            now[0] = 1031
            self.assertEqual(self.command('upload', str(self.source))['accepted'], 1)
        requests_sent = self.calls('POST', '/api/jobs')
        self.assertEqual(len(requests_sent), 2)
        self.assertEqual({r.headers['X-Replica-Upload-Id'] for r in requests_sent}, {record['request_id']})
        self.assertEqual(self.upload_record()['attempts'], 2)
        self.assertNotIn('request_not_applied', self.upload_record())

    def test_generic_503_is_unknown_even_with_retry_after_and_is_never_replayed(self):
        self.server.overrides['POST', '/api/jobs'] = (503, {'detail': 'Upstream failed'}, {'Retry-After': '2'})
        upload_id = 'a' * 32
        self.server.overrides['GET', '/api/source-uploads/' + upload_id] = (404, {})
        with patch('ua_workflows.replica_cli.secrets.token_hex', return_value=upload_id):
            self.command('upload', str(self.source))
            self.command('upload', str(self.source))
        self.assertEqual(self.upload_record()['stage'], 'upload_unknown')
        self.assertEqual(len(self.calls('POST', '/api/jobs')), 1)
        self.assertFalse(self.upload_record().get('request_not_applied'))

    def test_rate_limited_upload_retries_original_id_after_restart(self):
        self.server.overrides['POST', '/api/jobs'] = (429, {}, {'Retry-After': '90'})
        first = self.command('upload', str(self.source))
        self.assertEqual(first['results'][0]['retry_after'], 90)
        original = self.upload_record()
        self.assertEqual(original['stage'], 'upload_retry')
        # Reloading CLI/checkpoint alone must not turn the rejection into a new ID.
        self.server.overrides.pop(('POST', '/api/jobs'))
        with patch('ua_workflows.replica_cli.time.time', return_value=original['retry_at'] + 1):
            self.assertEqual(self.command('upload', str(self.source))['accepted'], 1)
        self.assertEqual({r.headers['X-Replica-Upload-Id'] for r in self.calls('POST', '/api/jobs')},
                         {original['request_id']})

    def test_old_rejection_proof_cannot_replay_a_later_unknown_attempt(self):
        self.server.overrides['POST', '/api/jobs'] = (429, {}, {'Retry-After': '2'})
        self.command('upload', str(self.source))
        first = self.upload_record()
        self.server.overrides['GET', '/api/source-uploads/' + first['request_id']] = (404, {})
        self.server.overrides['POST', '/api/jobs'] = requests.Timeout('Later response lost')
        original_send = self.server.send
        def check_boundary(request, **kwargs):
            if request.method == 'POST' and request.url.endswith('/api/jobs'):
                record = self.upload_record()
                self.assertEqual(record['stage'], 'upload_intent')
                self.assertNotIn('request_not_applied', record)
            return original_send(request, **kwargs)
        self.server.send = check_boundary
        with patch('ua_workflows.replica_cli.time.time', return_value=first['retry_at'] + 1):
            self.command('upload', str(self.source))
            self.command('upload', str(self.source))
        self.assertEqual(self.upload_record()['stage'], 'upload_unknown')
        self.assertEqual(len(self.calls('POST', '/api/jobs')), 2)

    def test_permanent_400_upload_rejection_is_not_automatically_retried(self):
        self.server.overrides['POST', '/api/jobs'] = (400, {'detail': 'Invalid video'})
        self.command('upload', str(self.source))
        self.assertEqual(self.upload_record()['stage'], 'upload_rejected')
        upload_id = self.upload_record()['request_id']
        self.server.overrides['GET', '/api/source-uploads/' + upload_id] = (404, {})
        self.command('upload', str(self.source))
        self.assertEqual(len(self.calls('POST', '/api/jobs')), 1)

    def test_get_503_returns_server_wait_without_blocking_inside_http(self):
        path = '/api/jobs/' + contract.JOB
        self.server.overrides['GET', path] = (503, {}, {'Retry-After': '120'})
        self.api.wait = Mock(side_effect=AssertionError('Scheduler should persist this wait'))
        with self.assertRaises(ReplicaError) as caught:
            self.api.job(contract.JOB)
        self.assertEqual(caught.exception.retry_after, 120)
        self.assertEqual(caught.exception.code, 'read_failed')
        self.assertEqual(len(self.calls('GET', path)), 1)

    def test_nonbillable_download_plan_timeout_is_a_read_failure_not_unknown_generation(self):
        self.api.authenticate()
        self.server.overrides['POST', '/api/jobs/download/files'] = requests.Timeout()
        with self.assertRaises(ReplicaError) as caught:
            self.api.download_plan([contract.JOB])
        self.assertEqual(caught.exception.code, 'read_failed')
        self.assertEqual(len(self.calls('POST', '/api/jobs/download/files')), 3)

    def test_logo_429_is_recorded_once_and_not_resubmitted_after_rate_limit_clears(self):
        path = '/api/jobs/' + contract.JOB + '/actions/logo_replace'
        self.server.overrides['POST', path] = (429, {}, {'Retry-After': '60'})
        first = self.command('logo', contract.JOB, '--yes')
        self.assertEqual(first['results'][0]['code'], 'rate_limited')
        self.assertTrue(first['stopped'])
        self.server.overrides.pop(('POST', path))
        self.command('logo', contract.JOB, '--yes')
        self.assertEqual(len(self.calls('POST', path)), 1)

    def test_download_rate_limit_preserves_status_and_wait_hint(self):
        path = '/api/jobs/' + contract.JOB + '/files/result.mp4'
        self.server.overrides['GET', path] = (429, {}, {'Retry-After': '45'})
        with self.assertRaises(ReplicaError) as caught:
            self.api.download(contract.JOB, {'name': 'result.mp4', 'bytes': len(self.media), 'url': path},
                              self.root / 'result.mp4')
        self.assertEqual(caught.exception.code, 'rate_limited')
        self.assertEqual(caught.exception.retry_after, 45)
        self.assertFalse((self.root / 'result.mp4.part').exists())

    def test_truncated_download_is_retryable_but_not_delivered(self):
        path = '/api/jobs/' + contract.JOB + '/files/result.mp4'
        target = self.root / 'short.mp4'
        with self.assertRaises(ReplicaError) as caught:
            self.api.download(contract.JOB, {'name': 'result.mp4', 'bytes': len(self.media) + 1, 'url': path}, target)
        self.assertTrue(caught.exception.retryable)
        self.assertFalse(target.exists())
        self.assertFalse(target.with_name('short.mp4.part').exists())

    def test_retry_after_supports_http_dates_and_rejects_invalid_values(self):
        self.assertEqual(retry_after_seconds(formatdate(1120, usegmt=True), now=1000), 120)
        self.assertEqual(retry_after_seconds('0'), 0)
        self.assertEqual(retry_after_seconds('-2'), 0)
        for value in ['nan', 'inf', 'not-a-date', None]:
            self.assertIsNone(retry_after_seconds(value))


class RetryPipelineTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = pipeline.PipelineTests.asyncSetUp
    asyncTearDown = pipeline.PipelineTests.asyncTearDown
    start = pipeline.PipelineTests.start
    until = pipeline.PipelineTests.until

    async def test_more_than_three_download_interruptions_retry_while_other_result_is_written(self):
        now = [1000]
        self.runner.health.clock = lambda: now[0]
        for rid in ['fast', 'slow']:
            self.state.put(rid, {'record_id': rid, 'stage': 'logo_ready', 'job_id': 'job-' + rid})
        original = self.runner.api.download
        attempts = []
        def download(job, file, path):
            if job == 'job-fast':
                attempts.append(job)
                if len(attempts) <= 4:
                    raise ReplicaError('read_failed', 'Temporary media failure', 503, retry_after=10)
            return original(job, file, path)
        self.runner.api.download = download
        self.runner._deliver_once()
        self.assertEqual(self.state.get('fast')['stage'], 'logo_ready')
        self.runner._deliver_once()
        self.assertEqual(self.state.get('slow')['stage'], 'verified')
        for _ in range(3):
            value = self.state.get('fast')
            now[0] = value['download_check_after'] + 1
            self.runner._deliver_once()
            self.assertEqual(self.state.get('fast')['stage'], 'logo_ready')
        self.assertEqual(self.state.get('fast')['download_attempts'], 4)
        now[0] = self.state.get('fast')['download_check_after'] + 1
        self.runner._deliver_once()
        self.assertEqual(self.state.get('fast')['stage'], 'verified')
        self.assertEqual(len(attempts), 5)
        self.assertEqual(len(self.client.writes), 2)
        self.assertEqual(self.backend.logos, [])
        self.assertEqual(self.backend.uploads, [])

    async def test_download_permission_error_stops_without_losing_generated_result(self):
        self.state.put('fast', {'record_id': 'fast', 'stage': 'logo_ready', 'job_id': 'job-fast'})
        self.state.put('slow', {'record_id': 'slow', 'stage': 'skipped_existing'})
        with patch.object(pipeline.API, 'download', side_effect=ReplicaError('permission_denied', 'Token denied', 403)):
            with self.assertRaises(ReplicaError) as caught:
                await asyncio.wait_for(self.start(), 8)
        self.assertEqual(caught.exception.code, 'permission_denied')
        self.assertEqual(self.state.get('fast')['stage'], 'logo_ready')
        self.assertEqual(self.backend.logos, [])
        self.assertEqual(self.backend.uploads, [])

    async def test_upload_rate_limit_cooldown_survives_restart_and_does_not_block_control(self):
        now = [1000]
        attempts = []
        def factory():
            api = self.backend.factory()
            original = api.upload
            def upload(path, upload_id):
                attempts.append((path.name, upload_id))
                if len(attempts) == 1:
                    raise ReplicaError('rate_limited', 'Mock quota', 429, retry_after=120,
                                       request_not_applied=True)
                return original(path, upload_id)
            api.upload = upload
            return api
        with patch('ua_workflows.replica_cli.time.time', side_effect=lambda: now[0]):
            self.runner = flow.PipelineRunner(self.state, api_factory=factory, feishu=self.client,
                                              clock=lambda: now[0], poll_interval=.01)
            with self.assertRaises(ReplicaError) as caught:
                self.runner._upload_once()
            self.runner._defer_lane(caught.exception, role='upload')
            self.assertEqual(self.state.get('fast')['stage'], 'source_ready')
            self.runner = flow.PipelineRunner(self.state, api_factory=factory, feishu=self.client,
                                              clock=lambda: now[0], poll_interval=.01)
            self.assertEqual(self.runner._lane_retries['upload']['retry_at'], 1120)
            self.start()
            await asyncio.sleep(.04)
            # Repeated control events cannot bypass the uploader's persisted wait.
            self.assertEqual(len(attempts), 1)
            self.assertIn('control', self.runner._operations)
            now[0] = 1121
            await self.until(lambda: len(self.backend.uploads) == 2)
            self.backend.analysis.update({'job-fast', 'job-slow'})
            self.backend.completed.update(self.backend.analysis)
            self.runner._events['control'].set()
            result = await asyncio.wait_for(self.task, 8)
        self.assertEqual(result['verified'], 2)
        self.assertEqual(attempts[0][1], attempts[1][1])
        self.assertEqual(Counter(self.backend.logos), {'job-fast': 1, 'job-slow': 1})

    async def test_logo_interruption_is_skipped_without_auto_continuation(self):
        original = pipeline.API.job
        def job(api, job_id):
            value = original(api, job_id)
            if job_id == 'job-fast' and job_id in self.backend.logos:
                value.update(status='interrupted', error='Mock generation interruption')
                value['logo_replace_view'].update(pending={'status': 'interrupted',
                                                           'settings': {'logo_mode': 'smart'}})
            return value
        self.backend.analysis.update({'job-fast', 'job-slow'})
        self.backend.completed.update(self.backend.analysis)
        with patch.object(pipeline.API, 'job', job):
            result = await asyncio.wait_for(self.start(), 8)
        self.assertEqual(result['verified'], 1)
        self.assertEqual(self.state.get('fast')['stage'], 'failed')
        self.assertEqual(Counter(self.backend.logos), {'job-fast': 1, 'job-slow': 1})
        self.assertEqual(self.backend.downloads, ['job-slow'])

    async def test_poll_rate_limit_preserves_task_and_defers_control_only(self):
        now = [1000]
        self.runner.health.clock = lambda: now[0]
        self.state.put('fast', {'record_id': 'fast', 'stage': 'uploaded', 'job_id': 'job-fast'})
        self.runner.api.job = Mock(side_effect=ReplicaError('rate_limited', 'Read limit', 429, retry_after=45))
        with self.assertRaises(ReplicaError) as caught:
            self.runner.poll(['fast'])
        self.runner._defer_lane(caught.exception, role='control')
        self.assertEqual(self.state.get('fast')['stage'], 'uploaded')
        self.assertEqual(self.state.get('fast')['poll_check_after'], 1045)
        self.assertEqual(self.runner._lane_retries['control']['retry_at'], 1045)
        self.assertNotIn('upload', self.runner._lane_retries)
        self.runner._upload_once()
        self.assertEqual(self.backend.uploads, ['job-slow'])

    async def test_unknown_logo_service_outage_defers_control_without_post_replay(self):
        self.state.put('fast', {'record_id': 'fast', 'stage': 'logo_unknown', 'job_id': 'job-fast'})
        self.runner.checkpoint.put('logos', 'job-fast', {'stage': 'logo_unknown', 'revision': 10})
        self.runner.api.job = Mock(side_effect=ReplicaError('read_failed', 'Workspace unavailable', 503, retry_after=120))
        with self.assertRaises(ReplicaError) as caught:
            self.runner.reconcile_unknown_logos()
        self.assertEqual(caught.exception.retry_after, 120)
        self.assertEqual(self.state.get('fast')['stage'], 'logo_unknown')
        self.assertEqual(self.backend.logos, [])


class InitializationRetryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        rows = [pipeline.fixtures.row('rec1')]
        save_private(self.root / 'scope.json', flow.prepare_rows(rows, pipeline.fixtures.SCHEMA, '2026-10-29'))
        self.state = flow.State(self.root)
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'queued'})
        self.argv = ['social_chat_replica', 'run', '--run-dir', str(self.root)]

    def test_initialization_rate_limit_is_persisted_and_obeyed_after_restart(self):
        now = [1000]
        with patch('sys.argv', self.argv), patch('builtins.print'), \
                patch.object(flow, 'PipelineRunner', side_effect=ReplicaError('rate_limited', 'Init limit', 429, retry_after=40)), \
                patch.object(flow.time, 'time', side_effect=lambda: now[0]), \
                patch.object(flow.time, 'sleep', side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                flow.main()
        journal = json.loads((self.root / 'initialization-retry.json').read_text())
        self.assertEqual(journal['retry_at'], 1040)
        self.assertEqual(journal['code'], 'rate_limited')
        now[0] = 1005
        calls = []
        def constructor(*args, **kwargs):
            calls.append(now[0])
            instance = Mock()
            instance.run.return_value = self.state.summary(pause_requested=True)
            return instance
        with patch('sys.argv', self.argv), patch('builtins.print'), \
                patch.object(flow, 'PipelineRunner', side_effect=constructor), \
                patch.object(flow.time, 'time', side_effect=lambda: now[0]), \
                patch.object(flow.time, 'sleep', side_effect=lambda seconds: now.__setitem__(0, now[0] + seconds)):
            flow.main()
        self.assertEqual(calls, [1040])
        self.assertEqual(json.loads((self.root / 'worker.json').read_text())['phase'], 'paused')

    def test_repeated_retry_counters_do_not_hide_upload_or_download_stalls(self):
        from ua_workflows.replica_recovery import RunHealth
        now = [1000]
        health = RunHealth(self.root, {'stall_timeout': 10, 'stage_timeout': 20}, clock=lambda: now[0])
        values = [{'record_id': 'upload', 'stage': 'source_ready', 'upload_check_after': 1100},
                  {'record_id': 'download', 'stage': 'logo_ready', 'download_attempts': 1}]
        with patch('builtins.print'):
            health.observe('pipeline', values)
            now[0] = 1021
            values[1]['download_attempts'] = 5
            health.observe('pipeline', values)
        alerts = list(health.data['alerts'].values())
        for rid in ['upload', 'download']:
            self.assertTrue(any(a['record_id'] == rid and a['code'] == 'record_stalled' for a in alerts))
            self.assertTrue(any(a['record_id'] == rid and a['code'] == 'record_stage_timeout' for a in alerts))

    def test_pause_during_initialization_wait_starts_no_authenticated_worker(self):
        save_private(self.root / 'initialization-retry.json', {'retry_at': 2000, 'attempts': 1})
        save_private(self.root / 'pause-requested', {'requested': True})
        with patch('sys.argv', self.argv), patch('builtins.print'), \
                patch.object(flow.time, 'time', return_value=1000), \
                patch.object(flow, 'PipelineRunner') as runner:
            flow.main()
        runner.assert_not_called()
        self.assertEqual(json.loads((self.root / 'worker.json').read_text())['phase'], 'paused')


if __name__ == '__main__':
    unittest.main()
