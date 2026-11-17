import json
import os
from pathlib import Path
import tempfile
import hashlib
import shutil
import subprocess
import unittest
from unittest.mock import Mock, patch

import requests

from ua_workflows.replica_studio_api import BASE_URL, ReplicaAPI, ReplicaError, media_url, step_state
from ua_workflows.replica_cli import Checkpoint, logo_many, upload_many, download_many, status_many, parser, execute


def response(value, status=200, headers=None):
    result = Mock(status_code=status, headers=headers or {})
    result.json.return_value = value
    return result


def ready(job_id):
    return {'id': job_id, 'project_id': 'project-' + job_id, 'status': 'analysis_review',
            'revision': 8, 'step_states': {'analysis': 'done'}, 'dashboard': {'running': False}, 'source_kind': 'video'}


class SwapTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.state = Path(directory.name) / 'state.json'
        self.transport = Mock(replica_device_auth=True)
        self.job = {
            **ready('job-a'), 'video_route': 'model_swap',
            'cast_members': [{'source_id': 'C1', 'portrait_ready': True, 'portrait_file': 'portraits/C1.png'}],
            'segments': [{'index': 1, 'start_s': 0, 'end_s': 2, 'video_file': None}],
            'text_replacements_mode': 'custom', 'text_replacements': [],
        }
        self.receipt = {'id': 'job-a', 'status': 'queued', 'revision': 9, 'queue_ticket': 'ticket-1'}
        self.scopes = ['read', 'run']
        self.transport.request.side_effect = self.http

    def http(self, method, url, **kwargs):
        if (method, url) == ('GET', BASE_URL + '/api/cli/whoami'):
            return response({'user': {'id': 'account-a'}, 'token': {'scopes': self.scopes}})
        if (method, url) == ('GET', BASE_URL + '/api/jobs/job-a'):
            return response(self.job)
        if (method, url) == ('GET', BASE_URL + '/api/settings'):
            return response({'text_replacements': [{'from': 'SeaHot', 'to': 'Afizzy'}]})
        if (method, url) == ('POST', BASE_URL + '/api/jobs/job-a/actions/videos'):
            self.assertEqual(kwargs['json'], {
                'revision': 8, 'video_route': 'model_swap', 'force_new': False, 'subtitle_erase': False,
            })
            self.assertEqual(kwargs['headers']['X-Replica-Confirm'], 'paid')
            intent = json.loads(self.state.read_text())['video_swaps']['job-a']
            self.assertEqual(intent['stage'], 'intent')
            self.assertEqual(intent['portraits'], {'C1': 'portraits/C1.png'})
            if isinstance(self.receipt, Exception):
                raise self.receipt
            return response(self.receipt)
        self.fail(f'Unexpected request {method} {url}')

    def run_cli(self, *args):
        parsed = parser().parse_args(['--checkpoint', str(self.state), *args])
        return execute(parsed, factory=lambda: ReplicaAPI(self.transport, confirm_paid=getattr(parsed, 'yes', False)))

    def posts(self):
        return [call for call in self.transport.request.call_args_list if call.args[0] == 'POST']

    def test_check_only_and_submit_then_reuse_receipt(self):
        self.assertTrue(self.run_cli('swap', 'job-a', '--check-only')['check_only'])
        self.assertFalse(self.state.exists())
        self.assertTrue(self.run_cli('swap', 'job-a', '--yes')['submitted'])
        self.assertTrue(self.run_cli('swap', 'job-a', '--yes')['reused'])
        self.assertEqual(len(self.posts()), 1)

    def test_unknown_submission_is_never_replayed(self):
        for receipt in (requests.Timeout(), {**self.receipt, 'id': 'job-b'}):
            with self.subTest(receipt=receipt):
                self.state.unlink(missing_ok=True)
                self.transport.request.reset_mock()
                self.receipt = receipt
                with self.assertRaisesRegex(ReplicaError, 'could not be confirmed'):
                    self.run_cli('swap', 'job-a', '--yes')
                result = self.run_cli('swap', 'job-a', '--yes')
                self.assertEqual(result['code'], 'result_unknown')
                self.assertEqual(len(self.posts()), 1)

    def test_preflight_rejects_unready_cast_replacement_rules_and_missing_run_scope(self):
        for changes in ({'cast_members': []}, {'segments': []}, {'text_replacements': [{'from': 'old'}]}):
            with self.subTest(changes=changes), patch.dict(self.job, changes):
                with self.assertRaises(ReplicaError) as found:
                    self.run_cli('swap', 'job-a', '--yes')
                self.assertEqual(found.exception.code, 'swap_not_ready')
        self.scopes = ['read']
        with self.assertRaises(ReplicaError) as found:
            self.run_cli('swap', 'job-a', '--yes')
        self.assertEqual(found.exception.code, 'permission_denied')
        self.assertFalse(self.state.exists())
        self.assertFalse(self.posts())

    def test_read_only_status_requires_segments_and_idle_job(self):
        self.job['segments'][0]['video_file'] = 'videos/swap1.mp4'
        self.job['status'] = 'generating'
        running = status_many(ReplicaAPI(self.transport), ['job-a'], stage='swap', timeout=0)
        self.assertEqual(running['results'][0]['step_state'], 'running')
        self.job['status'] = 'analysis_review'
        ready = self.run_cli('status', 'job-a', '--wait-for', 'swap')
        self.assertEqual(ready['results'][0]['step_state'], 'ready')
        self.assertEqual(ready['results'][0]['job']['segments'][0]['video_file'], 'videos/swap1.mp4')
        self.assertFalse(self.posts())

    def test_rules_require_complete_zero_match_evidence(self):
        self.job.update(text_replacements_mode='defaults', text_replacements=[{'from': 'SeaHot', 'to': 'Afizzy'}])
        for detections in ([], [{'keyword': 'Other', 'occurrences': []}],
                           [{'keyword': 'SeaHot', 'occurrences': [{'start_s': 0}]}]):
            with self.subTest(detections=detections):
                self.job['replacement_detections'] = detections
                with self.assertRaisesRegex(ReplicaError, 'verify that analysis found no matches'):
                    self.run_cli('swap', 'job-a', '--check-only')
        self.job['replacement_detections'] = [{'keyword': 'SeaHot', 'occurrences': []}]
        self.assertTrue(self.run_cli('swap', 'job-a', '--check-only')['ok'])
        self.assertFalse(self.posts())

    def test_transport_rejects_unconfirmed_or_broader_paid_actions(self):
        api = ReplicaAPI(self.transport)
        api.account = 'account-a'
        with self.assertRaises(ReplicaError) as found:
            api.swap_video(self.job)
        self.assertEqual(found.exception.code, 'confirmation_required')
        for change in ({'video_route': 'story_rebuild'}, {'force_new': True}, {'subtitle_erase': True},
                       {'edit': {}}, {'segment_index': 1}):
            with self.subTest(change=change), self.assertRaises(ReplicaError):
                api.request('POST', '/api/jobs/job-a/actions/videos', body={
                    'video_route': 'model_swap', 'force_new': False, 'subtitle_erase': False, 'revision': 8,
                    **change,
                })
        self.transport.request.assert_not_called()

    def test_device_upload_sends_paid_confirmation_only_with_yes(self):
        path = self.state.with_name('clip.mp4')
        path.write_bytes(b'video fixture')
        with self.assertRaises(ReplicaError) as found:
            self.run_cli('upload', str(path))
        self.assertEqual(found.exception.code, 'confirmation_required')
        self.assertFalse(self.posts())
        self.assertFalse(self.state.exists())

        def upload_http(method, url, **kwargs):
            if (method, url) == ('POST', BASE_URL + '/api/jobs'):
                headers = kwargs['headers']
                self.assertEqual(headers['X-Replica-Confirm'], 'paid')
                return response({'ids': ['job-a'], 'request_id': headers['X-Replica-Upload-Id']})
            return self.http(method, url, **kwargs)

        self.transport.request.side_effect = upload_http
        self.assertEqual(self.run_cli('upload', str(path), '--yes')['accepted'], 1)
        self.assertEqual(len(self.posts()), 1)


class FakeAPI:
    def __init__(self):
        self.uploads = []
        self.lookups = []
        self.logos = []
        self.jobs = {}
        self.upload_error = None
        self.logo_error = None
        self.plan = {'projects': [], 'missing': 0, 'skipped': []}

    def authenticate(self):
        return {'account_id': 'account-a', 'auth_mode': 'oidc'}

    def upload(self, path, upload_id):
        self.uploads.append((str(path), upload_id))
        if self.upload_error:
            raise self.upload_error
        return {'request_id': upload_id, 'ids': ['job-' + Path(path).stem]}

    def upload_status(self, upload_id):
        self.lookups.append(upload_id)
        raise ReplicaError('result_unknown', 'not confirmed', 404)

    def smart_options(self):
        return {'use_defaults': True, 'logo_mode': 'smart', 'replace_name': 'Afizzy', 'remove_cta': True}

    def job(self, job_id):
        return self.jobs.get(job_id, ready(job_id))

    def smart_logo(self, job, options):
        self.logos.append((job['id'], options))
        if self.logo_error:
            raise self.logo_error
        return {'id': job['id'], 'status': 'queued', 'revision': 9}

    def download_plan(self, ids):
        self.plan_ids = ids
        return self.plan


class ReplicaHTTPTests(unittest.TestCase):
    def test_unavailable_logo_reports_server_reason_and_sends_no_action(self):
        transport = Mock()
        transport.request.return_value = response({'logo_replace': {
            'available': False, 'reason': 'Tencent MPS is not configured'}})
        with self.assertRaises(ReplicaError) as caught:
            ReplicaAPI(transport).smart_options()
        self.assertEqual(caught.exception.code, 'unavailable')
        self.assertIn('Tencent MPS is not configured', str(caught.exception))
        transport.request.assert_called_once()
        self.assertEqual(transport.request.call_args.args[:2], ('GET', BASE_URL + '/api/settings'))

    def test_task_list_is_read_only_and_accepts_documented_array_response(self):
        transport = Mock()
        transport.request.return_value = response([{'id': 'job-a', 'source_name': 'source.mp4'}])
        result = ReplicaAPI(transport).list_jobs()
        self.assertEqual(result[0]['source_name'], 'source.mp4')
        transport.request.assert_called_once()
        self.assertEqual(transport.request.call_args.args[:2], ('GET', BASE_URL + '/api/jobs'))

    def test_task_list_cannot_silently_accept_duplicate_job_ids(self):
        transport = Mock()
        transport.request.return_value = response([{'id': 'job-a'}, {'id': 'job-a'}])
        with self.assertRaises(ReplicaError): ReplicaAPI(transport).list_jobs()

    def test_other_endpoints_still_require_object_responses(self):
        transport = Mock()
        transport.request.return_value = response([])
        with self.assertRaises(ReplicaError): ReplicaAPI(transport).job('job-a')

    def test_history_and_recycle_inspection_only_send_get_requests(self):
        transport = Mock()
        transport.request.side_effect = [response({'filter': 'all', 'tasks': [], 'counts': {'total': 0}}), response([]), response([])]
        api = ReplicaAPI(transport)
        self.assertEqual(api.task_history()['tasks'], [])
        self.assertEqual(api.deleted_jobs(), [])
        self.assertEqual(api.deleted_projects(), [])
        self.assertTrue(all(c.args[0] == 'GET' for c in transport.request.call_args_list))

    def test_history_filter_uses_fixed_read_query_and_rejects_extra_parameters(self):
        transport = Mock()
        transport.request.return_value = response({'filter': 'failed', 'tasks': []})
        api = ReplicaAPI(transport)
        self.assertEqual(api.task_history('failed')['tasks'], [])
        self.assertEqual(transport.request.call_args.args[:2], ('GET', BASE_URL + '/api/tasks?filter=failed'))
        with self.assertRaises(ReplicaError): api.task_history('all&limit=99999')
        self.assertEqual(transport.request.call_count, 1)

    def test_upload_reconciliation_503_stays_unknown_without_write(self):
        transport = Mock()
        transport.request.return_value = response({}, 503)
        client = ReplicaAPI(transport, wait=lambda _: None)
        with self.assertRaises(ReplicaError) as found:
            client.upload_status('a' * 32)
        self.assertEqual(found.exception.code, 'result_unknown')
        self.assertEqual(transport.request.call_count, 3)
        self.assertTrue(all(call.args[0] == 'GET' for call in transport.request.call_args_list))

    def test_session_is_checked_without_outputting_csrf(self):
        transport = Mock()
        transport.request.return_value = response({'user': {'id': 'account-a', 'enabled': True}, 'csrf_token': 'secret-test', 'auth_mode': 'oidc'})
        client = ReplicaAPI(transport)
        self.assertEqual(client.authenticate(), {'account_id': 'account-a', 'auth_mode': 'oidc'})
        self.assertNotIn('secret-test', json.dumps(client.authenticate()))
        self.assertEqual(transport.request.call_args.kwargs['headers']['Origin'], BASE_URL)

    def test_missing_cookie_session_stops_before_writes(self):
        transport = Mock()
        transport.request.return_value = response({'user': None, 'auth_mode': 'oidc'})
        client = ReplicaAPI(transport)
        with self.assertRaises(ReplicaError) as found:
            client.authenticate()
        self.assertEqual(found.exception.code, 'authentication_required')
        self.assertEqual(transport.request.call_args.args[0], 'GET')
        with self.assertRaises(ReplicaError):
            client.request('POST', '/api/jobs', body={})
        self.assertEqual(transport.request.call_count, 1)

    def test_paid_5xx_is_not_retried(self):
        transport = Mock()
        transport.request.return_value = response({}, 503)
        client = ReplicaAPI(transport, wait=lambda _: None)
        client.account, client.csrf = 'account-a', 'test-csrf'
        with self.assertRaises(ReplicaError) as found:
            client.request('POST', '/api/jobs/job-a/actions/logo_replace', body={'logo_mode': 'smart'})
        self.assertEqual(found.exception.code, 'result_unknown')
        self.assertEqual(transport.request.call_count, 1)

    def test_reads_may_retry_but_not_submit_generation(self):
        transport = Mock()
        transport.request.side_effect = [requests.Timeout(), response({}, 503), response(ready('job-a'))]
        client = ReplicaAPI(transport, wait=lambda _: None)
        self.assertEqual(client.job('job-a')['id'], 'job-a')
        self.assertEqual(transport.request.call_count, 3)
        self.assertTrue(all(call.args[0] == 'GET' for call in transport.request.call_args_list))

    def test_non_workflow_endpoints_rejected(self):
        transport = Mock()
        client = ReplicaAPI(transport)
        for path in ['/api/jobs/job-a/actions/portraits', '/api/jobs/job-a/actions/videos',
                     '/api/batch/all', '/api/jobs/job-a/actions/export']:
            with self.assertRaises(ReplicaError) as found:
                client.request('POST', path, body={})
            self.assertEqual(found.exception.code, 'unsupported_endpoint')
        transport.request.assert_not_called()

    def test_reanalysis_is_allowed_without_force(self):
        from ua_workflows.replica_studio_api import validate_workflow_body
        validate_workflow_body('POST', '/api/jobs/job-a/actions/analyze', {'force_new': False})
        with self.assertRaises(ReplicaError) as found:
            validate_workflow_body('POST', '/api/jobs/job-a/actions/analyze', {'force_new': True})
        self.assertEqual(found.exception.code, 'unsupported_action')

    def test_oidc_command_does_not_export_redirect_transaction_or_claim_login(self):
        transport = Mock()
        transport.get.return_value = response({}, 303, {'Location': 'https://auth.upupup.today/login?state=private-state&nonce=private-nonce'})
        result = ReplicaAPI(transport).login_start()
        self.assertFalse(result['cli_authenticated'])
        self.assertEqual(result['login_url'], BASE_URL + '/api/auth/oidc/login')
        self.assertNotIn('private-', json.dumps(result))

    def test_credential_file_permission_and_no_secret_in_errors(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'session.json'
            path.write_text(json.dumps({'replica_session': 'secret-test-cookie'}))
            path.chmod(0o644)
            with self.assertRaises(ReplicaError) as found:
                ReplicaAPI.configured(session_file=path)
            self.assertNotIn('secret-test-cookie', str(found.exception))
            path.chmod(0o600)
            api = ReplicaAPI.configured(session_file=path)
            self.assertEqual(api.session.cookies.get('replica_session'), 'secret-test-cookie')

    def test_cookie_and_csrf_headers_stay_on_fixed_api_origin(self):
        transport = Mock()
        transport.request.return_value = response({'id': 'job-a', 'status': 'queued'})
        api = ReplicaAPI(transport)
        api.account, api.csrf = 'account-a', 'csrf-only-in-header'
        api.smart_logo(ready('job-a'), {'logo_mode': 'smart'})
        args = transport.request.call_args
        self.assertTrue(args.args[1].startswith(BASE_URL + '/api/'))
        self.assertFalse(args.kwargs['allow_redirects'])
        self.assertEqual(args.kwargs['headers']['X-Replica-CSRF'], 'csrf-only-in-header')
        self.assertEqual(args.kwargs['json']['revision'], 8)

    def test_media_url_rejects_other_hosts_and_traversal(self):
        self.assertEqual(media_url('/api/jobs/job-a/files/video.mp4', 'job-a'), BASE_URL + '/api/jobs/job-a/files/video.mp4')
        for value in ['https://evil.example/api/jobs/job-a/files/x.mp4', '/api/jobs/job-b/files/x.mp4', '/api/jobs/job-a/files/%2e%2e/private']:
            with self.assertRaises(ReplicaError):
                media_url(value, 'job-a')

    def test_http_200_unrelated_json_is_not_an_accepted_task(self):
        transport = Mock()
        transport.request.return_value = response({})
        api = ReplicaAPI(transport)
        api.account, api.csrf = 'account-a', 'csrf'
        with self.assertRaises(ReplicaError) as found:
            api.smart_logo(ready('job-a'), {'logo_mode': 'smart'})
        self.assertEqual(found.exception.code, 'result_unknown')

    def test_generic_completed_does_not_mean_logo_done(self):
        self.assertEqual(step_state({'status': 'completed'}, 'logo'), 'waiting')
        self.assertEqual(step_state({'status': 'completed', 'logo_replace_view': {'state': 'kept_original'}}, 'logo'), 'review')
        self.assertEqual(step_state({'status': 'completed', 'logo_replace_view': {'state': 'replaced'}}, 'logo'), 'ready')

    def test_download_stream_checks_all_bytes_and_hash(self):
        transport = Mock()
        stream = Mock(status_code=200)
        stream.iter_content.return_value = iter([b'abc', b'def'])
        transport.get.return_value.__enter__ = Mock(return_value=stream)
        transport.get.return_value.__exit__ = Mock(return_value=None)
        with tempfile.TemporaryDirectory() as temp:
            destination = Path(temp) / 'saved.mp4'
            result = ReplicaAPI(transport).download('job-a', {'name': 'video.mp4', 'bytes': 6, 'url': '/api/jobs/job-a/files/video.mp4'}, destination)
            self.assertEqual(destination.read_bytes(), b'abcdef')
            self.assertEqual(result['sha256'], hashlib.sha256(b'abcdef').hexdigest())
            self.assertFalse(transport.get.call_args.kwargs['allow_redirects'])

    def test_short_media_response_does_not_leave_a_delivered_file(self):
        transport = Mock()
        stream = Mock(status_code=200)
        stream.iter_content.return_value = iter([b'abc'])
        transport.get.return_value.__enter__ = Mock(return_value=stream)
        transport.get.return_value.__exit__ = Mock(return_value=None)
        with tempfile.TemporaryDirectory() as temp:
            destination = Path(temp) / 'saved.mp4'
            with self.assertRaises(ReplicaError):
                ReplicaAPI(transport).download('job-a', {'name': 'video.mp4', 'bytes': 9, 'url': '/api/jobs/job-a/files/video.mp4'}, destination)
            self.assertFalse(destination.exists())
            self.assertFalse(destination.with_name('saved.mp4.part').exists())


class ReplicaBatchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.checkpoint = Checkpoint(self.root / 'state.json', 'account-a')
        self.api = FakeAPI()

    def tearDown(self):
        self.temp.cleanup()

    def videos(self, count):
        paths = []
        for index in range(count):
            path = self.root / f'video-{index}.mp4'
            path.write_bytes(f'fake-test-video-{index}'.encode())
            paths.append(path)
        return paths

    def test_multi_upload_has_independent_ids_and_is_not_repeated(self):
        paths = self.videos(8)
        first = upload_many(lambda: self.api, paths, self.checkpoint)
        self.assertEqual(first['accepted'], 8)
        self.assertEqual(len({upload_id for _, upload_id in self.api.uploads}), 8)
        second = upload_many(lambda: self.api, paths, self.checkpoint)
        self.assertTrue(all(x['reused'] for x in second['results']))
        self.assertEqual(len(self.api.uploads), 8)

    def test_selection_is_submitted_on_calling_thread_and_receipt_saved_before_next_file(self):
        import threading
        paths = self.videos(19)
        calling_thread = threading.get_ident()
        received = []
        original = self.api.upload
        self.api.authenticate = Mock(return_value={'account_id': 'account-a'})

        def upload(path, request_id):
            self.assertEqual(threading.get_ident(), calling_thread)
            self.assertEqual(len(received), len(self.api.uploads))
            return original(path, request_id)

        self.api.upload = upload
        result = upload_many(lambda: self.api, paths, self.checkpoint, on_result=received.append)
        self.assertEqual(result['accepted'], 19)
        self.assertEqual([value['path'] for value in received], [str(path.resolve()) for path in paths])
        self.api.authenticate.assert_called_once()

    def test_upload_does_not_wait_for_background_analysis(self):
        self.api.job = Mock(side_effect=AssertionError('Upload intake must not poll or wait for a job'))
        result = upload_many(lambda: self.api, self.videos(19), self.checkpoint)
        self.assertEqual(result['accepted'], 19)
        self.api.job.assert_not_called()

    def test_removed_workers_option_is_rejected(self):
        with patch('sys.stderr'), self.assertRaises(SystemExit):
            parser().parse_args(['upload', 'source.mp4', '--workers', '5'])

    def test_save_intent_failure_prevents_post(self):
        self.checkpoint.put = Mock(side_effect=OSError('disk full'))
        result = upload_many(lambda: self.api, self.videos(2), self.checkpoint)
        self.assertEqual(self.api.uploads, [])
        self.assertEqual(result['results'][1]['code'], 'not_submitted')

    def test_auth_probe_read_failure_does_not_fail_source_or_create_upload_intent(self):
        paths = self.videos(2)
        self.api.authenticate = Mock(side_effect=ReplicaError('read_failed', 'temporary outage'))
        result = upload_many(lambda: self.api, paths, self.checkpoint)
        self.assertTrue(result['stopped'])
        self.assertTrue(all(item['code'] == 'not_submitted' for item in result['results']))
        self.assertEqual(self.api.uploads, [])
        self.assertEqual(self.checkpoint.data['uploads'], {})
        self.api.authenticate = Mock(return_value={'account_id': 'account-a'})
        again = upload_many(lambda: self.api, paths, self.checkpoint)
        self.assertEqual(again['accepted'], 2)
        self.assertEqual(self.api.lookups, [])

    def test_explicitly_unsent_intent_uses_same_id_for_first_actual_post(self):
        path = self.videos(1)[0].resolve()
        identity = {'path': str(path), 'name': path.name, 'bytes': path.stat().st_size,
                    'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                    'variants': 1, 'character': '', 'auto_analyze': True}
        key = hashlib.sha256(str(path).encode()).hexdigest()
        self.checkpoint.put('uploads', key, {'stage': 'upload_not_submitted', 'identity': identity, 'request_id': 'a' * 32})
        result = upload_many(lambda: self.api, [path], self.checkpoint)
        self.assertEqual(result['accepted'], 1)
        self.assertEqual(self.api.uploads, [(str(path), 'a' * 32)])
        self.assertEqual(self.api.lookups, [])
        upload_many(lambda: self.api, [path], self.checkpoint)
        self.assertEqual(len(self.api.uploads), 1)

    def test_unknown_upload_retains_key_and_only_reads_on_rerun(self):
        self.api.upload_error = ReplicaError('result_unknown', 'offline')
        paths = self.videos(2)
        result = upload_many(lambda: self.api, paths, self.checkpoint)
        self.assertEqual(result['results'][0]['code'], 'result_unknown')
        self.assertEqual(result['results'][1]['code'], 'not_submitted')
        original_id = self.api.uploads[0][1]
        upload_many(lambda: self.api, paths[:1], self.checkpoint)
        self.assertEqual(len(self.api.uploads), 1)
        self.assertTrue(all(value == original_id for value in self.api.lookups))

    def test_multi_logo_only_submits_smart_and_deduplicates(self):
        result = logo_many(self.api, ['job-a', 'job-b', 'job-a'], self.checkpoint)
        self.assertEqual(result['selected'], 2)
        self.assertEqual([value[0] for value in self.api.logos], ['job-a', 'job-b'])
        self.assertTrue(all(options['logo_mode'] == 'smart' for _, options in self.api.logos))
        logo_many(self.api, ['job-a', 'job-b'], self.checkpoint)
        self.assertEqual(len(self.api.logos), 2)

    def test_logo_unknown_never_replays_and_stops_next_task(self):
        self.api.logo_error = ReplicaError('result_unknown', 'offline')
        result = logo_many(self.api, ['job-a', 'job-b'], self.checkpoint)
        self.assertEqual([x.get('code') for x in result['results']], ['result_unknown', 'not_submitted'])
        again = logo_many(self.api, ['job-a'], self.checkpoint)
        self.assertFalse(again['results'][0]['ok'])
        self.assertEqual(len(self.api.logos), 1)

    def test_logo_not_ready_or_already_done_is_not_paid_again(self):
        self.api.jobs['busy'] = {**ready('busy'), 'status': 'queued'}
        self.api.jobs['done'] = {**ready('done'), 'logo_replace_view': {'state': 'replaced'}}
        result = logo_many(self.api, ['busy', 'done'], self.checkpoint)
        self.assertEqual(self.api.logos, [])
        self.assertEqual(result['results'][1]['code'], 'already_done')

    def test_checkpoint_account_binding(self):
        self.checkpoint.put('logos', 'job-a', {'stage': 'logo_accepted'})
        with self.assertRaises(ReplicaError):
            Checkpoint(self.checkpoint.path, 'other-account')

    def test_download_plan_uses_entire_selected_id_set(self):
        result = download_many(self.api, ['job-a', 'job-b', 'job-a'], self.checkpoint, self.root, plan_only=True)
        self.assertEqual(self.api.plan_ids, ['job-a', 'job-b'])
        self.assertEqual(result['operation'], 'download_plan')

    def test_cli_upload_keeps_default_auto_analysis_path(self):
        args = parser().parse_args(['--checkpoint', str(self.root / 'cli-state.json'), 'upload', str(self.videos(1)[0])])
        result = execute(args, factory=lambda: self.api)
        self.assertEqual(result['accepted'], 1)

    def test_download_cannot_write_an_unselected_task(self):
        self.api.plan = {'projects': [{'files': [{'name': 'other.mp4', 'bytes': 9, 'url': '/api/jobs/other/files/other.mp4'}]}]}
        with self.assertRaises(ReplicaError):
            download_many(self.api, ['job-a'], self.checkpoint, self.root)

    def test_waiting_for_a_failed_step_does_not_report_success(self):
        self.api.jobs['job-a'] = {**ready('job-a'), 'status': 'error', 'error': 'generation failed'}
        result = status_many(self.api, ['job-a'], stage='logo')
        self.assertEqual(result['state'], 'settled')
        self.assertFalse(result['results'][0]['ok'])
        self.assertEqual(result['results'][0]['code'], 'step_failed')

    def test_kept_original_is_review_not_success(self):
        self.api.jobs['job-a'] = {**ready('job-a'), 'logo_replace_view': {'state': 'kept_original'}}
        result = status_many(self.api, ['job-a'], stage='logo')
        self.assertFalse(result['results'][0]['ok'])
        self.assertEqual(result['results'][0]['code'], 'needs_review')

    @unittest.skipUnless(shutil.which('ffmpeg'), 'ffmpeg not installed')
    def test_real_local_video_is_decoded_and_reused_without_another_download(self):
        source = self.root / 'fixture.mp4'
        subprocess.run(['ffmpeg', '-v', 'error', '-f', 'lavfi', '-i', 'color=c=blue:s=32x32:d=0.2',
                        '-an', '-c:v', 'libx264', '-pix_fmt', 'yuv420p', str(source)], check=True)
        media = source.read_bytes()
        self.api.plan = {'projects': [{'files': [{'name': 'result.mp4', 'bytes': len(media), 'url': '/api/jobs/job-a/files/result.mp4'}]}]}
        def download(job_id, file, destination):
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(media)
            return {'path': str(destination), 'name': file['name'], 'size': len(media), 'sha256': hashlib.sha256(media).hexdigest()}
        self.api.download = Mock(side_effect=download)
        result = download_many(self.api, ['job-a'], self.checkpoint, self.root / 'output')
        self.assertTrue(result['results'][0]['full_decode_passed'])
        again = download_many(self.api, ['job-a'], self.checkpoint, self.root / 'output')
        self.assertTrue(again['results'][0]['reused'])
        self.assertEqual(self.api.download.call_count, 1)


if __name__ == '__main__':
    unittest.main()
