import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ua_workflows import social_chat_replica as replica
from ua_workflows.replica_studio_api import digest, save_private
from ua_workflows.replica_cli import Checkpoint


SCHEMA = [{'field_id': replica.SOURCE_FIELD_ID, 'field_name': '原素材', 'type': 17},
          {'field_id': replica.TARGET_FIELD_ID, 'field_name': '复刻附件', 'type': 17}]


def row(rid='rec1', day='2026-10-29', kind='视频'):
    return {'record_id': rid, 'fields': {'目标日期': day, '素材类型': kind, '广告主': 'sample',
            '原素材': [{'file_token': 'original-' + rid, 'name': rid + '.mp4', 'size': 3}], '复刻附件': []}}


class Feishu:
    def __init__(self, rows, *, reject=False, response_lost=False):
        self.rows = {r['record_id']: copy.deepcopy(r) for r in rows}
        self.writes = []
        self.reject, self.response_lost = reject, response_lost

    def listing(self, route):
        return copy.deepcopy(SCHEMA if route.endswith('/fields') else list(self.rows.values()))

    def call(self, method, route, **kwargs):
        if method == 'GET':
            return {'record': copy.deepcopy(self.rows[route.split('/')[-1]])}
        self.writes.append(copy.deepcopy(kwargs['json']))
        if not self.reject:
            for update in kwargs['json']['records']:
                self.rows[update['record_id']]['fields'].update({
                    name: [{**a, 'name': 'result.mp4', 'size': 6} for a in attachments]
                    for name, attachments in update['fields'].items()})
        if self.reject or self.response_lost:
            raise TimeoutError('simulated response loss')
        return {}


class ReplicaBackfillTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.rows = [row(), row('rec2')]
        scope = replica.prepare_rows(self.rows, SCHEMA, '2026-10-29')
        save_private(self.root / 'scope.json', scope)
        self.state = replica.State(self.root)
        self.output = self.root / 'result.mp4'
        self.output.write_bytes(b'result')
        self.receipt = {'path': str(self.output), 'name': 'result.mp4', 'size': 6,
                        'sha256': digest(self.output), 'full_decode_passed': True}
        self.values = [{'record_id': r['record_id'], 'stage': 'downloaded', 'output': self.receipt}
                       for r in self.rows]
        for value in self.values:
            self.state.put(value['record_id'], value)

    def tearDown(self):
        self.temp.cleanup()

    def test_scope_selects_only_requested_date_videos_and_empty_targets(self):
        filled = row('filled'); filled['fields']['复刻附件'] = [{'file_token': 'existing'}]
        scope = replica.prepare_rows(self.rows + [row('yesterday', '2026-10-28'),
                                    row('image', kind='图片'), filled], SCHEMA, '2026-10-29')
        self.assertEqual([r['record_id'] for r in scope['items']], ['rec1', 'rec2'])
        self.assertEqual(scope['workflow'], ['upload_auto_analyze', 'smart_logo', 'download_logo', 'replica_attachment'])

    def test_schema_identity_cannot_silently_select_another_attachment_field(self):
        with self.assertRaises(ValueError):
            replica.prepare_rows(self.rows, [{'field_id': 'different', 'field_name': '复刻附件', 'type': 17}], '2026-10-29')

    def test_record_identity_date_and_original_must_still_match(self):
        for mutation in ('date', 'token', 'size', 'record'):
            changed = copy.deepcopy(self.rows[0])
            if mutation == 'date': changed['fields']['目标日期'] = '2026-10-30'
            if mutation == 'token': changed['fields']['原素材'][0]['file_token'] = 'different'
            if mutation == 'size': changed['fields']['原素材'][0]['size'] = 4
            if mutation == 'record': changed['record_id'] = 'rec2'
            with self.assertRaises(ValueError):
                replica.check_record(changed, self.state.items['rec1'], self.state.scope)

    @patch.object(replica, 'upload_attachment', side_effect=['new-rec1', 'new-rec2'])
    def test_batch_only_updates_replica_field_and_keeps_record_token_mapping(self, upload):
        client = Feishu(self.rows)
        replica.backfill(self.state, self.values, client, 'table')
        self.assertEqual(client.writes, [{'records': [
            {'record_id': 'rec1', 'fields': {'复刻附件': [{'file_token': 'new-rec1'}]}},
            {'record_id': 'rec2', 'fields': {'复刻附件': [{'file_token': 'new-rec2'}]}}]}])
        for original in self.rows:
            after = client.rows[original['record_id']]['fields']
            self.assertEqual({k: v for k, v in after.items() if k != '复刻附件'},
                             {k: v for k, v in original['fields'].items() if k != '复刻附件'})
            self.assertEqual(self.state.get(original['record_id'])['stage'], 'verified')
        replica.backfill(self.state, self.state.all(), client, 'table')
        self.assertEqual(upload.call_count, 2)
        self.assertEqual(len(client.writes), 1)

    @patch.object(replica, 'upload_attachment')
    def test_existing_attachment_is_preserved_without_upload_or_write(self, upload):
        client = Feishu(self.rows)
        client.rows['rec1']['fields']['复刻附件'] = [{'file_token': 'someone-else'}]
        replica.backfill(self.state, self.values[:1], client, 'table')
        self.assertEqual(client.writes, [])
        upload.assert_not_called()
        self.assertEqual(self.state.get('rec1')['stage'], 'skipped_existing')

    @patch.object(replica, 'upload_attachment', return_value='new-rec1')
    def test_lost_write_response_is_reconciled_without_repeating_post(self, upload):
        client = Feishu(self.rows, response_lost=True)
        replica.backfill(self.state, self.values[:1], client, 'table')
        self.assertEqual(self.state.get('rec1')['stage'], 'verified')
        self.assertEqual(len(client.writes), 1)

    @patch.object(replica, 'upload_attachment', return_value='new-rec1')
    def test_unconfirmed_write_stops_that_record(self, upload):
        client = Feishu(self.rows, reject=True)
        replica.backfill(self.state, self.values[:1], client, 'table')
        self.assertEqual(self.state.get('rec1')['stage'], 'write_unknown')
        self.assertEqual(len(client.writes), 1)

    @patch.object(replica, 'upload_attachment')
    def test_changed_result_bytes_are_rejected_before_upload(self, upload):
        self.output.write_bytes(b'change')
        with self.assertRaises(ValueError):
            replica.backfill(self.state, self.values[:1], Feishu(self.rows), 'table')
        upload.assert_not_called()

    def test_interrupted_write_recovery_only_reads(self):
        value = self.values[0]
        value.update(stage='write_intent', feishu_token='new-rec1')
        self.state.put('rec1', value)
        client = Feishu(self.rows)
        client.rows['rec1']['fields']['复刻附件'] = [{'file_token': 'new-rec1', 'name': 'result.mp4', 'size': 6}]
        runner = replica.Runner.__new__(replica.Runner)
        runner.state, runner.feishu, runner.route = self.state, client, 'table'
        runner.reconcile_writes()
        self.assertEqual(self.state.get('rec1')['stage'], 'verified')
        self.assertEqual(client.writes, [])

    def test_unknown_upload_recovery_uses_original_request_id_without_upload(self):
        import hashlib
        from unittest.mock import Mock
        runner = replica.Runner.__new__(replica.Runner)
        runner.state = self.state
        runner.checkpoint = Checkpoint(self.root / 'cli-state.json', 'account-a')
        runner.api = Mock()
        runner.api.upload_status.return_value = {'ids': ['confirmed-job'], 'request_id': 'a' * 32}
        source_path = str(self.root / 'source.mp4')
        key = hashlib.sha256(str(Path(source_path).resolve()).encode()).hexdigest()
        runner.checkpoint.put('uploads', key, {'stage': 'upload_unknown', 'request_id': 'a' * 32})
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'failed', 'source_path': source_path})
        runner.reconcile_uploads()
        runner.api.upload_status.assert_called_once_with('a' * 32)
        runner.api.upload.assert_not_called()
        self.assertEqual(self.state.get('rec1')['job_id'], 'confirmed-job')
        self.assertEqual(self.state.get('rec1')['stage'], 'uploaded')

    def interrupted_runner(self, checkpoint_stage='logo_accepted'):
        from unittest.mock import Mock
        runner = replica.Runner.__new__(replica.Runner)
        runner.state, runner.api, runner.submit_paused = self.state, Mock(), False
        runner.checkpoint = Checkpoint(self.root / 'cli-state.json', 'account-a')
        runner.checkpoint.put('logos', 'job-1', {'stage': checkpoint_stage, 'options': {'logo_mode': 'smart'}})
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'failed', 'last_status': 'interrupted', 'job_id': 'job-1'})
        runner.api.job.return_value = {'id': 'job-1', 'revision': 9, 'status': 'interrupted', 'last_action': 'logo_replace',
                                      'logo_replace_view': {'pending': {'status': 'interrupted', 'settings': {'logo_mode': 'smart'}}}}
        runner.api.smart_logo.return_value = {'id': 'job-1', 'status': 'queued'}
        return runner

    def test_confirmed_interrupted_logo_remains_failed_without_continuation(self):
        runner = self.interrupted_runner()
        runner.poll()
        runner.poll()
        runner.api.smart_logo.assert_not_called()
        self.assertEqual(self.state.get('rec1')['stage'], 'failed')

    def test_unknown_logo_post_is_not_replayed_as_continuation(self):
        runner = self.interrupted_runner('logo_unknown')
        runner.poll()
        runner.api.smart_logo.assert_not_called()

    def test_newly_interrupted_logo_is_skipped_even_with_pending_settings(self):
        runner = self.interrupted_runner()
        runner.api.job.return_value['logo_replace_view']['pending']['settings']['remove_watermarks'] = True
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'logo_accepted', 'job_id': 'job-1'})
        runner.poll()
        runner.poll()
        self.assertEqual(self.state.get('rec1')['stage'], 'failed')
        runner.api.smart_logo.assert_not_called()

    def test_completed_without_result_file_is_not_written_as_replica(self):
        runner = self.interrupted_runner()
        runner.api.account = 'account-a'
        runner.factory = lambda: runner.api
        runner.api.job.return_value = {'id': 'job-1', 'revision': 10, 'status': 'analysis_review',
                                      'logo_replace_view': {'state': 'no_logo', 'result_file': None}}
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'logo_accepted', 'job_id': 'job-1'})
        runner.poll()
        self.assertEqual(self.state.get('rec1')['stage'], 'needs_review')
        runner.api.smart_logo.assert_not_called()

    def test_interrupted_local_receipt_is_recovered_from_running_job_without_post(self):
        runner = self.interrupted_runner()
        runner.api.account = 'account-a'
        runner.factory = lambda: runner.api
        runner.checkpoint.put('logos', 'job-1', {'stage': 'logo_intent', 'revision': 10})
        runner.api.job.return_value = {'id': 'job-1', 'revision': 11, 'status': 'replacing_logo', 'last_action': 'logo_replace'}
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'logo_submitting', 'job_id': 'job-1'})
        runner.poll()
        self.assertEqual(self.state.get('rec1')['stage'], 'logo_accepted')
        runner.api.smart_logo.assert_not_called()

    def test_verifier_reports_approved_skips_without_claiming_all_results_written(self):
        save_private(self.root / 'records-before.json', self.rows)
        save_private(self.root / 'exception-policy.json', {'user_decision': 'Skip exceptional items and continue others'})
        self.state.put('rec1', {**self.values[0], 'stage': 'verified', 'feishu_token': 'result-token'})
        self.state.put('rec2', {'record_id': 'rec2', 'stage': 'needs_review', 'error': 'No downloadable result'})
        client = Feishu(self.rows)
        client.rows['rec1']['fields']['复刻附件'] = [{'file_token': 'result-token', 'name': 'result.mp4', 'size': 6}]
        result = replica.verify(self.state, client, remote_samples=0)
        self.assertTrue(result['verification_passed'])
        self.assertFalse(result['all_requested_results_written'])
        self.assertEqual(result['remote_attachment_verified'], 1)
        self.assertEqual(len(result['exceptions']), 1)
        self.assertEqual(result['other_fields_changed_record_ids'], [])

    def test_stalled_auto_analysis_is_checked_readonly_then_skipped(self):
        import time
        runner = self.interrupted_runner()
        runner.api.account = 'account-a'
        runner.factory = lambda: runner.api
        runner.api.job.return_value = {'id': 'job-1', 'revision': 10, 'status': 'uploaded', 'created_at': time.time() - 1000,
                                      'last_action': None, 'step_states': {'analysis': 'pending'}}
        runner.api.upload_status.return_value = {'analysis_status': 'unknown'}
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'uploaded', 'job_id': 'job-1', 'request_id': 'a' * 32})
        runner.poll()
        runner.api.upload_status.assert_called_once_with('a' * 32)
        runner.api.smart_logo.assert_not_called()
        self.assertEqual(self.state.get('rec1')['stage'], 'needs_review')

    def test_uncertain_logo_completed_result_is_recovered_without_post_or_checkpoint_change(self):
        runner = self.interrupted_runner('logo_unknown')
        runner.api.authenticate.return_value = {'account_id': 'account-a'}
        runner.api.job.return_value = {'id': 'job-1', 'revision': 20, 'status': 'uploaded', 'last_action': 'logo_replace',
                                      'logo_replace_view': {'state': 'replaced', 'result_file': 'logo/result.mp4'}}
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'logo_unknown', 'job_id': 'job-1', 'error': 'result_unknown'})
        before = runner.checkpoint.path.read_bytes()
        replica.reconcile_logos(self.state, runner.api)
        self.assertEqual(self.state.get('rec1')['stage'], 'logo_ready')
        runner.api.smart_logo.assert_not_called()
        self.assertEqual(runner.checkpoint.path.read_bytes(), before)

    def test_uncertain_logo_without_acceptance_proof_remains_unknown(self):
        runner = self.interrupted_runner('logo_unknown')
        runner.api.authenticate.return_value = {'account_id': 'account-a'}
        runner.api.job.return_value = {'id': 'job-1', 'revision': 7, 'status': 'analysis_review',
                                      'last_action': 'analyze', 'logo_replace_view': {'state': 'none'}}
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'logo_unknown', 'job_id': 'job-1'})
        replica.reconcile_logos(self.state, runner.api)
        self.assertEqual(self.state.get('rec1')['stage'], 'logo_unknown')
        runner.api.smart_logo.assert_not_called()

    def test_running_uncertain_logo_requires_newer_revision_to_confirm_acceptance(self):
        runner = self.interrupted_runner('logo_unknown')
        runner.api.authenticate.return_value = {'account_id': 'account-a'}
        runner.checkpoint.put('logos', 'job-1', {'stage': 'logo_unknown', 'revision': 7})
        for revision, expected in ((7, 'logo_unknown'), (8, 'logo_accepted')):
            self.state.put('rec1', {'record_id': 'rec1', 'stage': 'logo_unknown', 'job_id': 'job-1'})
            runner.api.job.return_value = {'id': 'job-1', 'revision': revision, 'status': 'replacing_logo', 'last_action': 'logo_replace'}
            replica.reconcile_logos(self.state, runner.api)
            self.assertEqual(self.state.get('rec1')['stage'], expected)
        runner.api.smart_logo.assert_not_called()

    @patch.object(replica, 'ReplicaFeishu')
    def test_long_running_worker_refreshes_its_own_feishu_token_before_backfill(self, fresh):
        import time
        runner = replica.Runner.__new__(replica.Runner)
        runner.feishu, runner._owns_feishu = object(), True
        runner._feishu_auth_at = time.monotonic() - 3601
        runner.refresh_feishu()
        fresh.assert_called_once_with()
        self.assertIs(runner.feishu, fresh.return_value)
        runner.refresh_feishu()
        self.assertEqual(fresh.call_count, 1)

    @patch.object(replica, 'ReplicaFeishu')
    def test_injected_feishu_client_is_preserved(self, fresh):
        runner = replica.Runner.__new__(replica.Runner)
        original = object()
        runner.feishu, runner._owns_feishu, runner._feishu_auth_at = original, False, 0
        runner.refresh_feishu()
        fresh.assert_not_called()
        self.assertIs(runner.feishu, original)

    @patch.object(replica.time, 'sleep')
    @patch.object(replica.daily.Feishu, 'call')
    def test_feishu_read_timeout_can_retry_without_writing(self, call, wait):
        call.side_effect = [replica.requests.ReadTimeout(), {'record': {'record_id': 'rec1'}}]
        client = replica.ReplicaFeishu.__new__(replica.ReplicaFeishu)
        result = client.call('GET', 'table/records/rec1')
        self.assertEqual(result['record']['record_id'], 'rec1')
        self.assertEqual(call.call_count, 2)
        self.assertTrue(all(c.args[0] == 'GET' for c in call.call_args_list))

    @patch.object(replica.daily.Feishu, 'call', side_effect=replica.requests.ReadTimeout())
    def test_feishu_write_timeout_is_never_replayed(self, call):
        client = replica.ReplicaFeishu.__new__(replica.ReplicaFeishu)
        with self.assertRaises(replica.requests.ReadTimeout):
            client.call('POST', 'table/records/batch_update', json={'records': []})
        self.assertEqual(call.call_count, 1)

    @patch.object(replica.time, 'sleep')
    @patch.object(replica.daily.Feishu, 'call', side_effect=replica.requests.ReadTimeout())
    def test_exhausted_feishu_reads_leave_worker_recoverable(self, call, wait):
        client = replica.ReplicaFeishu.__new__(replica.ReplicaFeishu)
        with self.assertRaises(replica.ReplicaError) as error:
            client.call('GET', 'table/records/rec1')
        self.assertEqual(error.exception.code, 'read_failed')
        self.assertEqual(call.call_count, 3)

    @patch.object(replica.daily.Feishu, '__init__', return_value=None)
    @patch.object(replica.daily.Feishu, 'call')
    def test_expired_token_during_readback_refreshes_then_only_repeats_get(self, call, refresh):
        call.side_effect = [ValueError('HTTP 400, code=99991663, Invalid access token'), {'record': {'record_id': 'rec1'}}]
        client = replica.ReplicaFeishu.__new__(replica.ReplicaFeishu)
        self.assertEqual(client.call('GET', 'table/records/rec1')['record']['record_id'], 'rec1')
        refresh.assert_called_once_with()
        self.assertEqual(call.call_count, 2)
        self.assertTrue(all(c.args[0] == 'GET' for c in call.call_args_list))

    @patch.object(replica.daily.Feishu, '__init__', return_value=None)
    @patch.object(replica.daily.Feishu, 'call', side_effect=ValueError('code=99991663'))
    def test_repeated_token_rejection_cannot_loop_forever(self, call, refresh):
        client = replica.ReplicaFeishu.__new__(replica.ReplicaFeishu)
        with self.assertRaises(ValueError): client.call('GET', 'table/records/rec1')
        refresh.assert_called_once_with()
        self.assertEqual(call.call_count, 2)

    @patch.object(replica.time, 'sleep')
    @patch.object(replica.daily.Feishu, '__init__', return_value=None)
    def test_token_acquisition_can_recover_from_proxy_failure(self, acquire, wait):
        acquire.side_effect = [replica.requests.exceptions.ProxyError('offline'), None]
        replica.ReplicaFeishu()
        self.assertEqual(acquire.call_count, 2)

    @patch.object(replica.time, 'sleep')
    @patch.object(replica.daily.Feishu, '__init__', side_effect=replica.requests.exceptions.ProxyError('offline'))
    def test_persistent_token_acquisition_network_failure_is_recoverable(self, acquire, wait):
        with self.assertRaises(replica.ReplicaError) as error: replica.ReplicaFeishu()
        self.assertEqual(error.exception.code, 'read_failed')
        self.assertEqual(acquire.call_count, 3)

    @patch.object(replica, 'upload_attachment', return_value='result-token')
    @patch('ua_workflows.replica_cli.decode_video')
    def test_runner_carries_one_record_through_actual_batch_helpers(self, decode, upload):
        class API:
            account = 'account-a'
            replaced = False
            uploads = 0
            logos = 0

            def authenticate(self): return {'account_id': self.account}
            def list_jobs(self): return []
            def upload(self, path, request_id):
                self.uploads += 1
                return {'ids': ['job-source'], 'request_id': request_id}
            def smart_options(self): return {'logo_mode': 'smart', 'use_defaults': True}
            def job(self, job_id):
                return {'id': job_id, 'revision': 8, 'status': 'uploaded', 'dashboard': {'running': False},
                        'step_states': {'analysis': 'pending' if self.replaced else 'done'},
                        'logo_replace_view': {'state': 'replaced' if self.replaced else 'pending',
                                              'result_file': 'logo/result.mp4' if self.replaced else None}}
            def smart_logo(self, job, options):
                self.logos += 1
                self.replaced = True
                return {'id': job['id'], 'status': 'queued'}
            def download_plan(self, ids):
                return {'projects': [{'files': [{'url': '/api/jobs/job-source/files/logo/result.mp4', 'name': 'result.mp4', 'bytes': 6}]}]}
            def download(self, job_id, file, path):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b'result')
                return {'path': str(path), 'name': file['name'], 'size': 6, 'sha256': digest(path)}

        source = self.root / 'source.mp4'; source.write_bytes(b'src')
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'source_ready', 'source_path': str(source),
                               'source_sha256': digest(source), 'source_decode_passed': True})
        self.state.put('rec2', {'record_id': 'rec2', 'stage': 'skipped_existing'})
        api = API()
        client = Feishu(self.rows)
        runner = replica.Runner(self.state, api_factory=lambda: api, feishu=client)
        for _ in range(6): runner.tick()
        self.assertEqual(self.state.get('rec1')['stage'], 'verified')
        self.assertEqual(self.state.get('rec1')['job_id'], 'job-source')
        self.assertEqual(api.uploads, 1)
        self.assertEqual(api.logos, 1)
        self.assertEqual(len(client.writes), 1)
        runner.tick()
        self.assertEqual(api.uploads, 1)
        self.assertEqual(api.logos, 1)
        self.assertEqual(len(client.writes), 1)


class StagedSchedulingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.rows = [row('rec' + str(index)) for index in range(18)]
        save_private(self.root / 'scope.json', replica.prepare_rows(self.rows, SCHEMA, '2026-10-29'))
        self.state = replica.State(self.root)
        for original in self.rows:
            rid = original['record_id']
            path = self.root / (rid + '.mp4'); path.write_bytes(b'src')
            self.state.put(rid, {'record_id': rid, 'stage': 'source_ready', 'source_path': str(path),
                                 'source_sha256': digest(path), 'source_decode_passed': True})

        class API:
            account = 'account-a'

            def __init__(self):
                self.uploads, self.logos, self.lookups = [], [], []
                self.analysis_done, self.errors = set(), set()

            def authenticate(self): return {'account_id': self.account}
            def list_jobs(self): return []
            def upload(self, path, request_id):
                job_id = 'job-' + path.stem
                self.uploads.append(job_id)
                return {'ids': [job_id], 'request_id': request_id}
            def smart_options(self): return {'logo_mode': 'smart', 'use_defaults': True}
            def job(self, job_id):
                self.lookups.append(job_id)
                running_logo = job_id in self.logos or job_id == 'job-old'
                return {'id': job_id, 'revision': 10,
                        'status': 'error' if job_id in self.errors else 'replacing_logo' if running_logo else 'analysis_review' if job_id in self.analysis_done else 'analyzing',
                        'step_states': {'analysis': 'done' if job_id in self.analysis_done else 'pending'},
                        'dashboard': {'running': running_logo}, 'logo_replace_view': {'state': 'none'}}
            def smart_logo(self, job, options):
                self.logos.append(job['id'])
                return {'id': job['id'], 'status': 'queued'}
            def download_plan(self, ids):
                raise AssertionError('No result is ready; delivery must not run during intake or analysis barriers')

        self.api = API()
        self.runner = replica.StagedRunner(self.state, api_factory=lambda: self.api, feishu=Feishu(self.rows))

    def tearDown(self): self.temp.cleanup()

    @patch('builtins.print')
    def test_uploads_all_sources_without_five_or_fifteen_record_cap(self, output):
        result = self.runner.tick()
        self.assertEqual(len(self.api.uploads), 18)
        self.assertEqual(self.api.lookups, [])
        self.assertEqual(self.api.logos, [])
        self.assertEqual(result['batch_phase'], 'upload_all')
        self.assertTrue(all(v['stage'] == 'uploaded' for v in self.state.all()))

    @patch('builtins.print')
    def test_analysis_barrier_then_submits_all_logos_without_ten_record_cap(self, output):
        self.runner.tick()
        self.api.analysis_done.update('job-rec' + str(index) for index in range(17))
        self.runner.tick()
        self.assertEqual(self.api.logos, [])
        self.assertEqual(self.state.get('rec17')['stage'], 'uploaded')
        self.api.analysis_done.add('job-rec17')
        self.runner.tick()
        self.assertEqual(self.api.logos, [])
        result = self.runner.tick()
        self.assertEqual(len(self.api.logos), 18)
        self.assertEqual(result['batch_phase'], 'submit_all_logos')
        gate = json.loads((self.root / 'analysis-gate.json').read_text())
        self.assertEqual(len(gate['eligible_record_ids']), 18)
        self.runner.tick()
        self.assertEqual(len(self.api.uploads), 18)
        self.assertEqual(len(self.api.logos), 18)

    @patch('builtins.print')
    def test_confirmed_analysis_error_is_excluded_from_bulk_logo(self, output):
        self.runner.tick()
        self.api.analysis_done.update(self.api.uploads)
        self.api.errors.add('job-rec17')
        self.runner.tick()
        self.runner.tick()
        self.assertEqual(len(self.api.logos), 17)
        self.assertNotIn('job-rec17', self.api.logos)
        self.assertEqual(self.state.get('rec17')['stage'], 'failed')
        self.assertEqual(json.loads((self.root / 'analysis-gate.json').read_text())['excluded'][0]['record_id'], 'rec17')

    @patch('builtins.print')
    def test_existing_logo_job_is_preserved_during_staged_handoff(self, output):
        value = self.state.get('rec0'); value.update(stage='logo_accepted', job_id='job-old')
        self.state.put('rec0', value)
        self.runner.tick()
        self.api.analysis_done.update(self.api.uploads)
        self.runner.tick()
        self.runner.tick()
        self.assertEqual(len(self.api.uploads), 17)
        self.assertEqual(len(self.api.logos), 17)
        self.assertNotIn('job-old', self.api.logos)
        self.assertEqual(self.state.get('rec0')['job_id'], 'job-old')

    def test_upload_callback_persists_first_receipt_before_next_submission(self):
        received = []
        original = self.api.upload

        def upload(path, request_id):
            if path.stem == 'rec1':
                self.assertEqual(len(received), 1)
                key = __import__('hashlib').sha256(str((self.root / 'rec0.mp4').resolve()).encode()).hexdigest()
                self.assertEqual(self.runner.checkpoint.get('uploads', key)['stage'], 'uploaded')
            return original(path, request_id)

        self.api.upload = upload
        result = replica.upload_many(lambda: self.api, [self.root / 'rec0.mp4', self.root / 'rec1.mp4'],
                                     self.runner.checkpoint, on_result=received.append)
        self.assertEqual(result['accepted'], 2)
        self.assertEqual(len(received), 2)

    @patch('builtins.print')
    @patch.object(replica, 'fetch_source', side_effect=AssertionError('Cached sources must not be fetched again'))
    def test_cached_sources_need_no_prefetch_command(self, fetch, output):
        self.runner.tick()
        self.assertEqual(len(self.api.uploads), 18)
        fetch.assert_not_called()

    @patch('builtins.print')
    def test_missing_sources_are_fetched_just_before_their_upload(self, output):
        for rid in self.state.items:
            self.state.put(rid, {'record_id': rid, 'stage': 'queued'})
        events = []
        original = self.api.upload

        def fetch(state, item, client, route):
            rid = item['record_id']
            events.append(('fetch', rid))
            path = self.root / (rid + '.mp4')
            value = {'record_id': rid, 'stage': 'source_ready', 'source_path': str(path),
                     'source_sha256': digest(path), 'source_bytes_verified': True}
            state.put(rid, value)
            return value

        def upload(path, request_id):
            events.append(('upload', path.stem))
            return original(path, request_id)

        self.api.upload = upload
        with patch.object(replica, 'fetch_source', side_effect=fetch):
            self.runner.tick()
        self.assertEqual(events, [event for rid in self.state.items for event in [('fetch', rid), ('upload', rid)]])

    @patch('builtins.print')
    def test_all_logo_results_must_be_finished_before_any_download_or_backfill(self, output):
        from unittest.mock import Mock
        for index, rid in enumerate(self.state.items):
            self.state.put(rid, {'record_id': rid, 'job_id': 'job-' + rid,
                                 'stage': 'logo_accepted' if index == 17 else 'logo_ready'})
        self.runner.download_all = Mock(side_effect=AssertionError('A Logo is still running'))
        with patch.object(replica, 'backfill', side_effect=AssertionError('A Logo is still running')):
            result = self.runner.tick()
        self.assertEqual(result['batch_phase'], 'wait_all_logos')
        self.runner.download_all.assert_not_called()

    @patch('builtins.print')
    @patch('ua_workflows.replica_cli.decode_video')
    def test_downloads_entire_selection_then_backfills_without_ten_record_cap(self, decode, output):
        selected = []
        for rid in self.state.items:
            self.state.put(rid, {'record_id': rid, 'job_id': 'job-' + rid, 'stage': 'logo_ready'})

        def plan(ids):
            selected.extend(ids)
            return {'projects': [{'files': [{'url': '/api/jobs/' + job + '/files/logo/result.mp4',
                                             'name': 'result.mp4', 'bytes': 6} for job in ids]}]}

        def download(job, file, path):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b'result')
            return {'path': str(path), 'name': file['name'], 'size': 6, 'sha256': digest(path)}

        self.api.download_plan, self.api.download = plan, download
        client = self.runner.feishu
        self.runner.tick()
        self.assertEqual(len(selected), 18)
        self.assertTrue(all(value['stage'] == 'downloaded' for value in self.state.all()))
        self.assertEqual(client.writes, [])
        self.assertTrue((self.root / 'logo-gate.json').exists())
        with patch.object(replica, 'upload_attachment', side_effect=['token-' + rid for rid in self.state.items]) as upload:
            self.runner.tick()
        self.assertEqual(upload.call_count, 18)
        self.assertEqual(len(client.writes), 1)
        self.assertEqual(len(client.writes[0]['records']), 18)
        self.assertTrue(all(value['stage'] == 'verified' for value in self.state.all()))
        # Resuming the same checkpoint must not regenerate or rewrite anything.
        resumed = replica.Runner(self.state, api_factory=lambda: self.api, feishu=client)
        resumed.tick()
        self.assertEqual(len(client.writes), 1)
        self.assertEqual(self.api.uploads, [])
        self.assertEqual(self.api.logos, [])

    @patch('builtins.print')
    def test_polling_uses_one_session_on_the_calling_thread(self, output):
        import threading
        from unittest.mock import Mock
        self.runner.tick()
        original = self.api.job
        caller = threading.get_ident()

        def job(job_id):
            self.assertEqual(threading.get_ident(), caller)
            return original(job_id)

        self.api.job = job
        self.runner.factory = Mock(side_effect=AssertionError('Polling must reuse the authenticated session'))
        self.runner.tick()
        self.assertEqual(len(self.api.lookups), 18)

    @patch('builtins.print')
    def test_pause_during_upload_stops_before_preparing_the_next_source(self, output):
        original = self.api.upload

        def upload(path, request_id):
            result = original(path, request_id)
            save_private(self.root / 'pause-requested', {'requested': True})
            return result

        self.api.upload = upload
        self.runner.tick()
        self.assertEqual(len(self.api.uploads), 1)
        self.assertEqual(self.state.get('rec0')['stage'], 'uploaded')
        self.assertEqual(self.state.get('rec1')['stage'], 'source_ready')
        self.assertTrue(self.runner.tick()['pause_requested'])

    @patch('builtins.print')
    def test_unconfirmed_upload_is_excluded_and_remaining_intake_continues_on_next_tick(self, output):
        from unittest.mock import Mock
        original = self.api.upload

        def upload(path, request_id):
            if path.stem == 'rec0':
                raise replica.ReplicaError('result_unknown', 'no receipt')
            return original(path, request_id)

        self.api.upload = upload
        self.api.upload_status = Mock(side_effect=replica.ReplicaError('result_unknown', '404', 404))
        self.runner.tick()
        self.assertEqual(self.state.get('rec0')['stage'], 'upload_unknown')
        before = json.loads(self.runner.checkpoint.path.read_text())['uploads']
        original_request = next(iter(before.values()))['request_id']
        self.runner.tick()
        self.assertEqual(len(self.api.uploads), 17)
        self.assertEqual(self.state.get('rec0')['stage'], 'upload_unknown')
        self.assertTrue(all(call.args == (original_request,) for call in self.api.upload_status.call_args_list))
        checks = self.api.upload_status.call_count
        self.api.analysis_done.update(self.api.uploads)
        self.runner.tick()
        self.runner.tick()
        self.assertEqual(len(self.api.logos), 17)
        self.assertEqual(self.api.upload_status.call_count, checks)


class SourceReuseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        save_private(self.root / 'scope.json', replica.prepare_rows([row()], SCHEMA, '2026-10-29'))
        self.state = replica.State(self.root)
        self.path = self.root / 'sources' / 'rec1' / 'rec1.mp4'
        self.path.parent.mkdir(parents=True)
        self.path.write_bytes(b'src')

    def tearDown(self):
        self.temp.cleanup()

    @patch('builtins.print')
    @patch.object(replica, 'ReplicaFeishu', side_effect=AssertionError('Local cache does not need remote authorization'))
    def test_verified_cache_reuses_legacy_hash_without_fetch_or_source_decode(self, client, output):
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'source_failed', 'error': 'old error',
                                'source_path': str(self.path), 'source_sha256': digest(self.path),
                                'source_decode_passed': True})
        replica.download_sources(self.state)
        value = self.state.get('rec1')
        self.assertEqual(value['stage'], 'source_ready')
        self.assertTrue(value['source_bytes_verified'])
        self.assertNotIn('error', value)
        self.assertEqual(self.path.read_bytes(), b'src')
        client.assert_not_called()

    def test_equal_size_corruption_is_not_reused(self):
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'source_ready',
                                'source_path': str(self.path), 'source_sha256': digest(self.path)})
        self.path.write_bytes(b'bad')
        self.assertFalse(replica.reuse_source(self.state, self.state.items['rec1']))

    def test_unbound_same_name_or_size_file_is_not_trusted(self):
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'queued'})
        self.assertFalse(replica.reuse_source(self.state, self.state.items['rec1']))

    def test_existing_bound_source_outside_default_cache_is_reused(self):
        other = self.root / 'already-downloaded.mp4'
        other.write_bytes(b'src')
        self.state.put('rec1', {'record_id': 'rec1', 'stage': 'queued',
                                'source_path': str(other), 'source_sha256': digest(other)})
        self.assertTrue(replica.reuse_source(self.state, self.state.items['rec1']))
        self.assertEqual(self.state.get('rec1')['source_path'], str(other.resolve()))

    @patch('ua_workflows.replica_cli.decode_video', side_effect=AssertionError('Do not fully decode source before intake'))
    def test_missing_source_is_fetched_with_exact_bytes_and_hash_without_full_decode(self, decode):
        from unittest.mock import MagicMock
        self.path.unlink()
        client = Feishu([row()])
        client.rows['rec1']['fields']['原素材'][0]['url'] = 'https://media.invalid/source'
        client.session = MagicMock()
        response = client.session.get.return_value.__enter__.return_value
        response.iter_content.return_value = [b's', b'rc']
        result = replica.fetch_source(self.state, self.state.items['rec1'], client, 'table')
        self.assertEqual(result['stage'], 'source_ready')
        self.assertEqual(result['source_sha256'], digest(self.path))
        self.assertTrue(result['source_bytes_verified'])
        self.assertNotIn('source_decode_passed', result)
        decode.assert_not_called()

    @patch('ua_workflows.replica_cli.decode_video')
    @patch.object(replica.time, 'sleep')
    def test_incomplete_source_is_not_marked_ready(self, wait, decode):
        from unittest.mock import MagicMock
        self.path.unlink()
        client = Feishu([row()])
        client.rows['rec1']['fields']['原素材'][0]['url'] = 'https://media.invalid/source'
        client.session = MagicMock()
        response = client.session.get.return_value.__enter__.return_value
        response.iter_content.side_effect = lambda size: iter([b's'])
        with self.assertRaisesRegex(ValueError, 'byte size mismatch'):
            replica.fetch_source(self.state, self.state.items['rec1'], client, 'table')
        self.assertFalse(self.path.exists())
        self.assertEqual(self.state.get('rec1')['stage'], 'queued')
        decode.assert_not_called()

    def test_main_uses_new_pipeline_runner_for_pipeline_policy(self):
        from unittest.mock import Mock
        save_private(self.root / 'scheduling.json', {'mode': 'pipeline'})
        fake = Mock()
        fake.run.return_value = {'all_terminal': False, 'pause_requested': True}
        with patch.object(replica, 'PipelineRunner', return_value=fake) as runner, patch('builtins.print'), patch.object(
                replica.sys, 'argv', ['replica', 'run', '--run-dir', str(self.root)]):
            replica.main()
        runner.assert_called_once()
        fake.run.assert_called_once()
        self.assertEqual(json.loads((self.root / 'scheduling.json').read_text())['mode'], 'pipeline')

    def test_unsupported_scheduling_option_is_rejected_before_opening_batch(self):
        with patch('sys.stderr'), patch.object(replica.sys, 'argv', ['replica', 'run', '--run-dir', str(self.root), '--scheduling', 'legacy']), self.assertRaises(SystemExit):
            replica.main()

    def test_main_defaults_to_pipeline_without_policy(self):
        from unittest.mock import Mock
        fake = Mock()
        fake.run.return_value = {'all_terminal': False, 'pause_requested': True}
        with patch.object(replica, 'PipelineRunner', return_value=fake) as runner, patch.object(replica, 'Runner') as batch, patch('builtins.print'), patch.object(
                replica.sys, 'argv', ['replica', 'run', '--run-dir', str(self.root)]):
            replica.main()
        runner.assert_called_once()
        batch.assert_not_called()

    def test_main_preserves_stored_batch_policy(self):
        from unittest.mock import Mock
        save_private(self.root / 'scheduling.json', {'mode': 'batch'})
        fake = Mock()
        fake.run.return_value = {'all_terminal': False, 'pause_requested': True}
        with patch.object(replica, 'Runner', return_value=fake) as runner, patch.object(replica, 'PipelineRunner') as pipeline, patch('builtins.print'), patch.object(
                replica.sys, 'argv', ['replica', 'run', '--run-dir', str(self.root)]):
            replica.main()
        runner.assert_called_once()
        pipeline.assert_not_called()
        self.assertEqual(json.loads((self.root / 'scheduling.json').read_text())['mode'], 'batch')


if __name__ == '__main__':
    unittest.main()
