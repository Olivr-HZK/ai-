"""Offline failure injection for serial delivery, reconciliation and watchdogs."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, Mock, patch

import requests
import test_social_chat_replica as fixtures
from ua_workflows import social_chat_replica as flow
from ua_workflows.replica_cli import Checkpoint, logo_many
from ua_workflows.replica_recovery import RunHealth, find_uploaded_source, timeout_policy
from ua_workflows.replica_studio_api import ReplicaAPI, ReplicaError, digest, save_private


class Clock:
    def __init__(self): self.value = 1000.0
    def __call__(self): return self.value
    def wait(self, seconds): self.value += seconds


class API:
    account = 'account-a'
    def __init__(self):
        self.jobs = {}
        self.candidates = []
        self.reads, self.source_reads = [], []
    def authenticate(self): return {'account_id': self.account}
    def upload_status(self, request_id):
        self.reads.append(request_id)
        raise ReplicaError('result_unknown', 'Original request unconfirmed', 404)
    def list_jobs(self): return copy.deepcopy(self.candidates)
    def job(self, job_id): return copy.deepcopy(self.jobs[job_id])
    def source_matches(self, job_id, filename, size, sha):
        self.source_reads.append(job_id)
        return self.jobs[job_id].get('source_matches', True)
    def upload(self, *args): raise AssertionError('Recovery must never upload a source')
    def smart_logo(self, *args): raise AssertionError('Recovery must never generate')


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.rows = [fixtures.row(), fixtures.row('rec2')]
        save_private(self.root / 'scope.json', flow.prepare_rows(self.rows, fixtures.SCHEMA, '2026-10-29'))
        self.state = flow.State(self.root)
        self.path = self.root / 'result.mp4'; self.path.write_bytes(b'result')
        self.receipt = {'path': str(self.path), 'name': 'result.mp4', 'size': 6,
                        'sha256': digest(self.path), 'full_decode_passed': True}
        self.values = [{'record_id': row['record_id'], 'stage': 'downloaded', 'output': self.receipt} for row in self.rows]
        for value in self.values: self.state.put(value['record_id'], value)
        self.api = API(); self.client = fixtures.Feishu(self.rows); self.clock = Clock()
        self.runner = flow.Runner(self.state, api_factory=lambda: self.api, feishu=self.client,
                                  clock=self.clock, wait=self.clock.wait,
                                  timeouts={'stall_timeout': 10, 'stage_timeout': 30, 'unknown_timeout': 20, 'reconcile_interval': 1})

    def tearDown(self): self.temp.cleanup()

    @patch('builtins.print')
    def test_second_attachment_proxy_error_does_not_strand_first_confirmed_token(self, output):
        with patch.object(flow, 'upload_attachment', side_effect=['token-first', requests.exceptions.ProxyError('offline')]) as upload:
            flow.backfill(self.state, self.values, self.client, 'table')
        self.assertEqual(self.state.get('rec1')['stage'], 'verified')
        self.assertEqual(self.state.get('rec2')['stage'], 'attachment_unknown')
        self.assertEqual(len(self.client.writes), 1)
        self.runner.reconcile_writes()
        with patch.object(flow, 'upload_attachment', side_effect=AssertionError('Do not replay attachment uploads')):
            self.runner.tick()
        self.assertEqual(len(self.client.writes), 1)
        self.assertEqual(upload.call_count, 2)
        self.assertEqual(self.state.get('rec2')['output'], self.receipt)

    @patch('builtins.print')
    def test_read_failure_after_token_reuses_token_for_first_table_write(self, output):
        original = self.client.call
        reads = 0
        def call(method, route, **kwargs):
            nonlocal reads
            if method == 'GET':
                reads += 1
                if reads == 2: raise ReplicaError('read_failed', 'transient read')
            return original(method, route, **kwargs)
        self.client.call = call
        with patch.object(flow, 'upload_attachment', return_value='retained-token') as upload:
            with self.assertRaises(ReplicaError): flow.backfill(self.state, self.values[:1], self.client, 'table')
            self.assertEqual(self.state.get('rec1')['stage'], 'feishu_uploaded')
            flow.backfill(self.state, [self.state.get('rec1')], self.client, 'table')
        self.assertEqual(upload.call_count, 1)
        self.assertEqual(len(self.client.writes), 1)
        self.assertEqual(self.state.get('rec1')['stage'], 'verified')

    def intent(self, phase=None):
        value = {**self.values[0], 'stage': 'write_intent', 'feishu_token': 'token-a'}
        if phase:
            value['write_batch_id'] = '123'
            save_private(self.root / 'write-audit' / '123-dispatch.json', {
                'version': 2, 'batch_id': '123', 'phase': phase,
                'records': {'rec1': {'file_token': 'token-a', 'name': 'result.mp4', 'size': 6}}})
        self.state.put('rec1', value)

    def test_prepared_journal_proves_unsent_and_restores_token_without_post(self):
        self.intent('prepared'); self.runner.reconcile_writes()
        self.assertEqual(self.state.get('rec1')['stage'], 'feishu_uploaded')
        self.assertEqual(self.client.writes, [])

    def test_dispatch_boundary_is_unknown_even_if_remote_row_empty(self):
        for phase in ('dispatching', 'accepted', 'unknown'):
            self.intent(phase); self.runner.reconcile_writes()
            self.assertEqual(self.state.get('rec1')['stage'], 'write_unknown')
        self.assertEqual(self.client.writes, [])

    def test_legacy_intent_without_new_journal_remains_unknown(self):
        self.intent(); self.runner.reconcile_writes()
        self.assertEqual(self.state.get('rec1')['stage'], 'write_unknown')
        self.assertEqual(self.client.writes, [])

    def test_journal_mismatch_does_not_restore_first_write(self):
        self.intent('prepared')
        path = self.root / 'write-audit' / '123-dispatch.json'; data = json.loads(path.read_text())
        data['records']['rec1']['file_token'] = 'another-token'; save_private(path, data)
        self.runner.reconcile_writes()
        self.assertEqual(self.state.get('rec1')['stage'], 'write_unknown')

    @patch('builtins.print')
    def test_lost_batch_response_is_read_back_never_replayed(self, output):
        self.client.response_lost = True
        with patch.object(flow, 'upload_attachment', return_value='token-a'):
            flow.backfill(self.state, self.values[:1], self.client, 'table')
        self.assertEqual(self.state.get('rec1')['stage'], 'verified')
        self.runner.reconcile_writes()
        self.assertEqual(len(self.client.writes), 1)

    def test_write_unknown_can_recover_late_remote_commit_by_get_only(self):
        self.intent('unknown'); self.runner.reconcile_writes()
        self.client.rows['rec1']['fields']['复刻附件'] = [{'file_token': 'token-a', 'name': 'result.mp4', 'size': 6}]
        self.clock.wait(2); self.runner.reconcile_writes()
        self.assertEqual(self.state.get('rec1')['stage'], 'verified')
        self.assertEqual(self.client.writes, [])

    def test_process_death_during_attachment_upload_never_replays_it(self):
        self.state.put('rec1', {**self.values[0], 'stage': 'feishu_upload_intent'})
        self.runner.reconcile_writes()
        self.assertEqual(self.state.get('rec1')['stage'], 'attachment_unknown')

    def test_crash_while_preparing_dispatch_restores_all_tokens_for_first_write(self):
        def crash(value):
            if value['stage'] == 'write_intent': raise RuntimeError('simulated process death')
        with patch.object(flow, 'upload_attachment', side_effect=['token-a', 'token-b']) as upload:
            with self.assertRaises(RuntimeError):
                flow.backfill(self.state, self.values, self.client, 'table', on_progress=crash)
            self.assertEqual(self.client.writes, [])
            self.runner.reconcile_writes()
            self.assertTrue(all(v['stage'] == 'feishu_uploaded' for v in self.state.all()))
            flow.backfill(self.state, self.state.all(), self.client, 'table')
        self.assertEqual(upload.call_count, 2)
        self.assertEqual(len(self.client.writes), 1)
        self.assertTrue(all(v['stage'] == 'verified' for v in self.state.all()))

    def test_crash_after_dispatch_boundary_cannot_be_treated_as_unsent(self):
        original = self.client.call
        def crash(method, route, **kwargs):
            if method == 'POST': raise SystemExit('death just before transport')
            return original(method, route, **kwargs)
        self.client.call = crash
        with patch.object(flow, 'upload_attachment', return_value='token-a'):
            with self.assertRaises(SystemExit):
                flow.backfill(self.state, self.values[:1], self.client, 'table')
        self.runner.reconcile_writes()
        self.assertEqual(self.state.get('rec1')['stage'], 'write_unknown')
        self.assertEqual(self.client.writes, [])

    @patch('builtins.print')
    def test_worker_resumes_readback_after_table_write_without_second_post(self, output):
        self.state.put('rec2', {'record_id': 'rec2', 'stage': 'skipped_existing'})
        original = self.client.call
        failed = False
        def call(method, route, **kwargs):
            nonlocal failed
            if method == 'GET' and self.client.writes and not failed:
                failed = True
                raise requests.ReadTimeout('readback interrupted')
            return original(method, route, **kwargs)
        self.client.call = call
        with patch.object(flow, 'upload_attachment', return_value='token-a') as upload:
            self.runner.run()
        self.assertEqual(upload.call_count, 1)
        self.assertEqual(len(self.client.writes), 1)
        self.assertEqual(self.state.get('rec1')['stage'], 'verified')

    def unknown_upload(self, *, legacy=False):
        path = self.root / 'rec1.mp4'; path.write_bytes(b'src')
        prior = {'stage': 'upload_unknown', 'request_id': 'a' * 32,
                 'identity': {'name': path.name, 'bytes': 3, 'sha256': digest(path)}}
        if not legacy: prior.update(dispatch_started_at=1000, dispatch_finished_at=1010)
        key = hashlib.sha256(str(path.resolve()).encode()).hexdigest()
        self.runner.checkpoint.put('uploads', key, prior)
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'upload_unknown', 'source_path': str(path)})
        self.state.put('rec2', {'record_id': 'rec2', 'stage': 'skipped_existing'})
        self.api.jobs['job-a'] = {'id': 'job-a', 'source_name': path.name, 'source_file': 'source.mp4', 'created_at': 1005, 'variant': 1}
        self.api.candidates = [self.api.jobs['job-a']]
        return prior

    def test_unknown_upload_task_list_fallback_requires_hash_time_and_name(self):
        self.unknown_upload(); self.runner.reconcile_uploads()
        self.assertEqual(self.state.get('rec1')['stage'], 'uploaded')
        self.assertEqual(self.state.get('rec1')['job_id'], 'job-a')
        self.assertEqual(self.api.source_reads, ['job-a'])
        self.assertEqual(self.api.reads, ['a' * 32])
        self.assertIs(self.state.get('rec1')['original_receipt_confirmed'], False)

    def test_same_name_wrong_source_hash_cannot_bind(self):
        self.unknown_upload(); self.api.jobs['job-a']['source_matches'] = False
        self.runner.reconcile_uploads()
        self.assertEqual(self.state.get('rec1')['stage'], 'upload_unknown')

    def test_wrong_variant_cannot_bind_even_when_source_matches(self):
        self.unknown_upload(); self.api.candidates[0]['variant'] = 2
        self.runner.reconcile_uploads()
        self.assertEqual(self.state.get('rec1')['stage'], 'upload_unknown')
        self.assertEqual(self.api.source_reads, [])

    def test_old_same_name_task_outside_window_is_not_read_or_bound(self):
        self.unknown_upload(); self.api.candidates[0]['created_at'] = 100
        self.runner.reconcile_uploads()
        self.assertEqual(self.state.get('rec1')['stage'], 'upload_unknown')
        self.assertEqual(self.api.source_reads, [])

    def test_two_matching_candidates_remain_ambiguous(self):
        self.unknown_upload(); self.api.jobs['job-b'] = {**self.api.jobs['job-a'], 'id': 'job-b'}
        self.api.candidates.append(self.api.jobs['job-b'])
        self.runner.reconcile_uploads()
        self.assertEqual(self.state.get('rec1')['stage'], 'upload_unknown')

    def test_legacy_unknown_without_dispatch_time_not_guessed_by_name(self):
        self.unknown_upload(legacy=True); self.runner.reconcile_uploads()
        self.assertEqual(self.state.get('rec1')['stage'], 'upload_unknown')
        self.assertEqual(self.api.source_reads, [])

    def test_already_claimed_job_cannot_be_bound_to_second_record(self):
        self.unknown_upload(); self.state.put('rec2', {'record_id': 'rec2', 'stage': 'verified', 'job_id': 'job-a'})
        self.runner.reconcile_uploads()
        self.assertEqual(self.state.get('rec1')['stage'], 'upload_unknown')

    def test_task_list_is_fetched_once_for_multiple_unknown_uploads(self):
        self.unknown_upload()
        path = self.root/'rec2.mp4'; path.write_bytes(b'src')
        key = hashlib.sha256(str(path.resolve()).encode()).hexdigest()
        self.runner.checkpoint.put('uploads', key, {'stage': 'upload_unknown', 'request_id': 'b'*32,
                'dispatch_started_at': 1000, 'dispatch_finished_at': 1010,
                'identity': {'name': path.name, 'bytes': 3, 'sha256': digest(path)}})
        self.state.put('rec2', {'record_id': 'rec2', 'stage': 'upload_unknown', 'source_path': str(path)})
        self.api.candidates = []
        self.api.list_jobs = Mock(return_value=[])
        self.runner.reconcile_uploads()
        self.api.list_jobs.assert_called_once()

    @patch('builtins.print')
    def test_expired_upload_window_stops_background_reads_without_changing_unknown(self, output):
        self.unknown_upload()
        self.runner.observe('reconcile_unknown'); self.clock.wait(21)
        self.runner.reconcile_uploads()
        self.assertEqual(self.api.reads, [])
        self.assertEqual(self.state.get('rec1')['stage'], 'upload_unknown')

    @patch('builtins.print')
    def test_expired_logo_window_stops_background_reads_without_generating(self, output):
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'logo_unknown', 'job_id': 'job-a'})
        self.runner.checkpoint.put('logos', 'job-a', {'stage': 'logo_unknown', 'revision': 1})
        self.runner.observe('reconcile_unknown'); self.clock.wait(21)
        self.api.job = Mock(side_effect=AssertionError('Expired window must stop polling'))
        self.runner.reconcile_unknown_logos()
        self.api.job.assert_not_called()
        self.assertEqual(self.state.get('rec1')['stage'], 'logo_unknown')

    @patch('builtins.print')
    def test_expired_write_window_stops_reads_and_never_replays_post(self, output):
        self.intent('unknown'); self.runner.reconcile_writes()
        self.runner.observe('reconcile_unknown'); self.clock.wait(21)
        self.client.call = Mock(side_effect=AssertionError('Expired write must remain untouched'))
        self.runner.reconcile_writes()
        self.client.call.assert_not_called()
        self.assertEqual(self.state.get('rec1')['stage'], 'write_unknown')

    def test_unknown_logo_after_receipt_save_failure_is_not_resubmitted(self):
        api = Mock()
        api.smart_options.return_value = {'logo_mode': 'smart'}
        api.job.return_value = {'id': 'job-a', 'revision': 2, 'status': 'analysis_review', 'step_states': {'analysis': 'done'}}
        api.smart_logo.return_value = {'id': 'job-a', 'status': 'queued'}
        original = self.runner.checkpoint.put
        failed = False
        def put(section, key, value):
            nonlocal failed
            if section == 'logos' and value['stage'] == 'logo_accepted' and not failed:
                failed = True
                raise OSError('receipt disk failure after POST')
            return original(section, key, value)
        with patch.object(self.runner.checkpoint, 'put', side_effect=put):
            result = logo_many(api, ['job-a', 'job-b'], self.runner.checkpoint)
        self.assertTrue(result['stopped'])
        self.assertEqual(self.runner.checkpoint.get('logos','job-a')['stage'], 'logo_unknown')
        logo_many(api, ['job-a'], self.runner.checkpoint)
        self.assertEqual(api.smart_logo.call_count, 1)

    @patch('builtins.print')
    def test_authentication_failure_alerts_and_stops_without_business_calls(self, output):
        self.api.authenticate = Mock(side_effect=ReplicaError('authentication_required', 'Login required'))
        summary = self.runner.run()
        self.assertEqual(summary['health_error'], 'authentication_required')
        self.assertTrue(any(a['code'] == 'authentication_required' for a in summary['active_alerts']))
        self.assertEqual(self.client.writes, [])

    @patch('builtins.print')
    def test_session_failure_and_recovery_do_not_reset_existing_stage_deadline(self, output):
        self.state.put('rec1',{'record_id':'rec1','stage':'logo_accepted','job_id':'job-a',
                               'last_status':'replacing_logo','last_message':'Cloud working'})
        self.state.put('rec2',{'record_id':'rec2','stage':'skipped_existing'})
        self.api.jobs['job-a']={'id':'job-a','revision':4,'status':'replacing_logo','message':'Cloud working'}
        self.runner.observe('wait_all_logos'); started=self.runner.health.data['phase_started_at']
        self.clock.wait(15)
        self.api.authenticate=Mock(side_effect=ReplicaError('authentication_required','Login required'))
        self.runner.tick()
        self.assertEqual(self.runner.health.data['phase_started_at'],started)
        self.api.authenticate=Mock(return_value={'account_id':self.api.account});self.clock.wait(16)
        summary=self.runner.tick()
        self.assertEqual(self.runner.health.data['phase_started_at'],started)
        self.assertTrue(any(a['code']=='stage_timeout' for a in summary['active_alerts']))

    @patch('builtins.print')
    def test_timeout_options_are_saved_before_background_worker_launch(self, output):
        args = ['replica','run','--run-dir',str(self.root),'--background','--stall-timeout','10',
                '--stage-timeout','90','--unknown-timeout','30','--reconcile-interval','2']
        with patch.object(flow.sys,'argv',args),patch.object(flow.subprocess,'Popen',return_value=Mock(pid=123)) as launch:
            flow.main()
        self.assertEqual(json.loads((self.root/'timeout-policy.json').read_text()),
                         {'stall_timeout':10,'stage_timeout':90,'unknown_timeout':30,'reconcile_interval':2})
        self.assertEqual(launch.call_args.args[0][-2:],['--run-dir',str(self.root.resolve())])

    def test_invalid_timeout_rejected_before_background_launch(self):
        args = ['replica','run','--run-dir',str(self.root),'--background','--stage-timeout','nan']
        with patch.object(flow.sys,'argv',args),patch.object(flow.subprocess,'Popen') as launch:
            with self.assertRaises(ReplicaError): flow.main()
        launch.assert_not_called()
        self.assertFalse((self.root/'timeout-policy.json').exists())

    @patch('builtins.print')
    def test_unknown_reconciliation_wait_is_bounded_and_never_submits(self, output):
        self.unknown_upload(); self.api.candidates = []
        summary = self.runner.run()
        self.assertEqual(self.state.get('rec1')['stage'], 'upload_unknown')
        self.assertGreaterEqual(self.clock(), 1020)
        self.assertLess(self.clock(), 1030)
        self.assertTrue(any(a['code'] == 'unknown_timeout' for a in summary['active_alerts']))
        self.assertEqual(set(self.api.reads), {'a' * 32})

    @patch('builtins.print')
    def test_unknown_logo_recovers_automatically_without_post_or_checkpoint_overwrite(self, output):
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'logo_unknown', 'job_id': 'job-a'})
        self.state.put('rec2', {'record_id': 'rec2', 'stage': 'skipped_existing'})
        self.runner.checkpoint.put('logos', 'job-a', {'stage': 'logo_unknown', 'revision': 3})
        self.api.jobs['job-a'] = {'id': 'job-a', 'last_action': 'logo_replace', 'revision': 4,
                                  'status': 'complete', 'logo_replace_view': {'state': 'replaced', 'result_file': 'logo/result.mp4'}}
        before = self.runner.checkpoint.path.read_bytes()
        # Inspect the automatic reconciliation directly to avoid entering delivery.
        self.runner.reconcile_unknown_logos()
        self.assertEqual(self.state.get('rec1')['stage'], 'logo_ready')
        self.assertEqual(self.runner.checkpoint.path.read_bytes(), before)

    @patch('builtins.print')
    def test_unknown_logo_transient_get_error_does_not_block_healthy_work(self, output):
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'logo_unknown', 'job_id': 'job-a'})
        self.runner.checkpoint.put('logos', 'job-a', {'stage': 'logo_unknown', 'revision': 3})
        self.api.job = Mock(side_effect=ReplicaError('read_failed', 'network'))
        self.runner.reconcile_unknown_logos()
        self.assertEqual(self.state.get('rec1')['stage'], 'logo_unknown')
        self.assertEqual(self.state.get('rec1')['reconcile_error'], 'read_failed')

    @patch('builtins.print')
    def test_regular_tick_automatically_finds_accepted_logo_without_new_post(self, output):
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'logo_unknown', 'job_id': 'job-a'})
        self.state.put('rec2', {'record_id': 'rec2', 'stage': 'skipped_existing'})
        self.runner.checkpoint.put('logos', 'job-a', {'stage': 'logo_intent', 'revision': 3})
        self.api.jobs['job-a'] = {'id': 'job-a', 'last_action': 'logo_replace', 'revision': 4,
                                  'status': 'replacing_logo', 'logo_replace_view': {'state': 'pending'}}
        before = self.runner.checkpoint.path.read_bytes()
        summary = self.runner.tick()
        self.assertEqual(self.state.get('rec1')['stage'], 'logo_accepted')
        self.assertEqual(summary['batch_phase'], 'wait_all_logos')
        self.assertEqual(self.runner.checkpoint.path.read_bytes(), before)

    @patch('builtins.print')
    def test_old_logo_result_without_new_revision_does_not_prove_unknown_action(self, output):
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'logo_unknown', 'job_id': 'job-a'})
        self.runner.checkpoint.put('logos', 'job-a', {'stage': 'logo_unknown', 'revision': 5})
        self.api.jobs['job-a'] = {'id': 'job-a', 'last_action': 'logo_replace', 'revision': 5,
                                  'status': 'complete', 'logo_replace_view': {'state': 'replaced', 'result_file': 'old.mp4'}}
        self.runner.reconcile_unknown_logos()
        self.assertEqual(self.state.get('rec1')['stage'], 'logo_unknown')

    @patch('builtins.print')
    def test_crashed_submitting_record_cannot_accept_stale_result_as_new_action(self, output):
        self.state.put('rec1', {'record_id':'rec1','stage':'logo_submitting','job_id':'job-a'})
        self.runner.checkpoint.put('logos','job-a',{'stage':'logo_intent','revision':5})
        self.api.jobs['job-a']={'id':'job-a','revision':5,'status':'complete','last_action':'logo_replace',
                               'logo_replace_view':{'state':'replaced','result_file':'old.mp4'}}
        self.runner.poll()
        self.assertEqual(self.state.get('rec1')['stage'],'logo_unknown')

    @patch('builtins.print')
    def test_repeated_transport_wait_does_not_reset_stage_or_progress_timers(self, output):
        def wait(seconds):
            self.clock.wait(seconds)
            if self.clock() >= 1060:
                save_private(self.root / 'pause-requested', {'requested': True})
        self.runner.wait = wait
        with patch.object(flow, 'backfill', side_effect=requests.ReadTimeout('offline')):
            result = self.runner.run()
        self.assertTrue(result['pause_requested'])
        codes = {a['code'] for a in result['active_alerts']}
        self.assertIn('batch_stalled', codes)
        self.assertIn('stage_timeout', codes)

    @patch('builtins.print')
    def test_status_summary_keeps_current_phase_and_reconciliation_flag(self, output):
        self.unknown_upload(); self.api.candidates = []
        self.runner.tick()
        summary = self.state.summary()
        self.assertEqual(summary['batch_phase'], 'reconcile_unknown')
        self.assertTrue(summary['reconciliation_pending'])

    @patch('builtins.print')
    def test_worker_recovers_transport_failure_without_replaying_paid_actions(self, output):
        original = self.runner.tick
        attempts = 0
        def tick():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise requests.exceptions.ProxyError('offline')
            for rid in self.state.items:
                self.state.put(rid, {'record_id': rid, 'stage': 'skipped_existing'})
            return original()
        self.runner.tick = Mock(side_effect=tick)
        result = self.runner.run()
        self.assertTrue(result['all_terminal'])
        self.assertEqual(self.runner.tick.call_count, 2)


class WatchdogTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.root = Path(self.temp.name); self.clock = Clock()
        self.policy = {'stall_timeout': 10, 'stage_timeout': 20, 'unknown_timeout': 30, 'reconcile_interval': 1}
        self.watch = RunHealth(self.root, self.policy, self.clock)
        self.values = [{'record_id': 'rec1', 'stage': 'logo_accepted', 'job_id': 'job-a', 'last_message': 'Cloud working'}]
    def tearDown(self): self.temp.cleanup()
    def codes(self): return {a['code'] for a in json.loads((self.root/'alerts.json').read_text())['active']}

    @patch('builtins.print')
    def test_stall_and_stage_alerts_persist_and_are_not_spammed(self, output):
        self.watch.observe('wait_all_logos', self.values); self.clock.wait(21)
        self.watch.observe('wait_all_logos', self.values)
        self.assertTrue({'batch_stalled', 'record_stalled', 'stage_timeout'} <= self.codes())
        count = len((self.root/'alerts.jsonl').read_text().splitlines())
        self.watch.observe('wait_all_logos', self.values)
        self.assertEqual(len((self.root/'alerts.jsonl').read_text().splitlines()), count)

    @patch('builtins.print')
    def test_progress_resolves_stall_but_does_not_reset_stage_deadline(self, output):
        self.watch.observe('wait_all_logos', self.values); self.clock.wait(21)
        self.watch.observe('wait_all_logos', self.values)
        self.values[0]['last_message'] = 'Cloud assembling'
        self.watch.observe('wait_all_logos', self.values)
        self.assertNotIn('batch_stalled', self.codes())
        self.assertNotIn('record_stalled', self.codes())
        self.assertIn('stage_timeout', self.codes())

    @patch('builtins.print')
    def test_restart_does_not_reset_unknown_deadline(self, output):
        self.values[0]['stage'] = 'upload_unknown'
        self.watch.observe('reconcile_unknown', self.values); self.clock.wait(31)
        restarted = RunHealth(self.root, self.policy, self.clock)
        restarted.observe('reconcile_unknown', self.values)
        self.assertFalse(restarted.unknown_pending(self.values))
        self.assertIn('unknown_timeout', self.codes())

    @patch('builtins.print')
    def test_repeated_poll_timestamps_are_not_progress(self, output):
        self.watch.observe('wait_all_logos', self.values); self.clock.wait(11)
        self.values[0].update(poll_error='read_failed', revision=99, checked_at=10000)
        self.watch.observe('wait_all_logos', self.values)
        self.assertIn('record_stalled', self.codes())

    @patch('builtins.print')
    def test_download_completion_resets_batch_progress(self, output):
        self.watch.observe('download_all', self.values); self.clock.wait(11)
        self.watch.observe('download_all', self.values, delivery_count=1)
        self.assertNotIn('batch_stalled', self.codes())

    @patch('builtins.print')
    def test_extending_timeout_policy_resolves_old_timeout_alerts(self, output):
        self.watch.observe('wait_all_logos', self.values); self.clock.wait(21)
        self.watch.observe('wait_all_logos', self.values)
        new = RunHealth(self.root, {**self.policy, 'stage_timeout': 100, 'stall_timeout': 100}, self.clock)
        new.observe('wait_all_logos', self.values)
        self.assertEqual(self.codes(), set())

    def test_invalid_timeout_values_are_rejected(self):
        for value in (0, -1, float('inf'), float('nan'), True):
            with self.assertRaises(ReplicaError): timeout_policy({'stall_timeout': value})


class SourceFingerprintTests(unittest.TestCase):
    def test_private_journal_syncs_file_and_directory_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'journal.json'
            import os
            fsync = os.fsync
            with patch('os.fsync', wraps=fsync) as sync:
                save_private(path, {'phase':'dispatching'})
            self.assertEqual(sync.call_count, 2)
            self.assertEqual(json.loads(path.read_text())['phase'], 'dispatching')

    def test_source_match_checks_exact_bytes_and_sha_by_streamed_get(self):
        transport = MagicMock(); response = transport.get.return_value.__enter__.return_value
        response.status_code = 200; response.iter_content.side_effect = lambda n: iter([b'a', b'bc'])
        api = ReplicaAPI(transport)
        sha = hashlib.sha256(b'abc').hexdigest()
        self.assertTrue(api.source_matches('job-a', 'source.mp4', 3, sha))
        self.assertFalse(api.source_matches('job-a', 'source.mp4', 3, '0'*64))
        self.assertFalse(api.source_matches('job-a', 'source.mp4', 2, sha))
        self.assertTrue(all(call.kwargs['allow_redirects'] is False for call in transport.get.call_args_list))

    def test_source_match_rejects_path_escape_before_network(self):
        transport = Mock(); api = ReplicaAPI(transport)
        for filename in ('../private', 'other/source.mp4', '%2e%2e', 'bad\\file', '.', ''):
            with self.assertRaises(ReplicaError): api.source_matches('job-a', filename, 3, '0'*64)
        transport.get.assert_not_called()

    def test_source_read_interruption_is_not_a_negative_identity_proof(self):
        transport = Mock(); transport.get.side_effect = requests.ReadTimeout('offline')
        with self.assertRaises(ReplicaError) as error:
            ReplicaAPI(transport).source_matches('job-a', 'source.mp4', 3, '0'*64)
        self.assertEqual(error.exception.code, 'read_failed')

    def test_slow_candidate_hash_has_a_total_budget_and_remains_unknown(self):
        transport = MagicMock(); response = transport.get.return_value.__enter__.return_value
        response.status_code = 200; response.iter_content.return_value = iter([b'abc'])
        with patch('ua_workflows.replica_studio_api.time.monotonic', side_effect=[0,121]):
            with self.assertRaises(ReplicaError) as error:
                ReplicaAPI(transport).source_matches('job-a','source.mp4',3,hashlib.sha256(b'abc').hexdigest())
        self.assertEqual(error.exception.code,'read_failed')


if __name__ == '__main__': unittest.main()
