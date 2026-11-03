"""Exercise real request preparation with fake credentials and an in-memory server.

These tests establish the client's HTTP contract, not the production server's
token allowlist. No live token, source video, or network connection is used.
"""
import hashlib
import io
import json
from email import policy
from email.parser import BytesParser
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

import requests
from requests.adapters import BaseAdapter

from ua_workflows.replica_cli import execute, parser
from ua_workflows.replica_device_auth import save_token
from ua_workflows.replica_studio_api import BASE_URL, ReplicaAPI, ReplicaError


FAKE_TOKEN = 'mock-only-device-credential'
JOB = 'mock-job'


class MemoryServer(BaseAdapter):
    def __init__(self, media):
        self.media = media
        self.calls = []
        self.overrides = {}
        self.generated = False

    def send(self, request, **kwargs):
        path = urlsplit(request.url).path
        key = (request.method, path)
        self.calls.append(request)
        headers = {}
        if key in self.overrides:
            value = self.overrides[key]
            if isinstance(value, Exception):
                raise value
            status, data = value[:2]
            if len(value) == 3:
                headers = value[2]
        elif key == ('GET', '/api/cli/whoami'):
            status, data = 200, {'user': {'id': 'account-a', 'enabled': True},
                                 'token': {'scopes': ['read', 'upload', 'run', 'download']}}
        elif key == ('GET', '/api/settings'):
            status, data = 200, {'logo_replace': {'available': True, 'defaults': {
                'logo_id': 'mock-logo', 'replace_name': 'Afizzy', 'remove_cta': True,
                'remove_gameplay': False, 'remove_watermarks': False, 'rename_voice': True}}}
        elif key == ('POST', '/api/jobs'):
            status, data = 200, {'request_id': request.headers['X-Replica-Upload-Id'],
                                 'ids': [JOB], 'analysis_status': 'queued'}
        elif key == ('GET', '/api/jobs/' + JOB):
            status, data = 200, {'id': JOB, 'project_id': JOB, 'revision': 9 if self.generated else 8,
                                 'status': 'uploaded' if self.generated else 'analysis_review',
                                 'step_states': {'analysis': 'done'}, 'source_kind': 'video',
                                 'dashboard': {'running': False},
                                 'logo_replace_view': {'state': 'replaced' if self.generated else 'none'}}
        elif key == ('POST', '/api/jobs/' + JOB + '/actions/logo_replace'):
            self.generated = True
            status, data = 200, {'id': JOB, 'revision': 9, 'status': 'queued'}
        elif key == ('POST', '/api/jobs/download/files'):
            status, data = 200, {'projects': [{'project_id': JOB, 'files': [{
                'name': 'result.mp4', 'bytes': len(self.media),
                'url': '/api/jobs/' + JOB + '/files/result.mp4?download=1'}]}],
                                 'missing': 0, 'skipped': []}
        elif key == ('GET', '/api/jobs/' + JOB + '/files/result.mp4'):
            status, data = 200, self.media
        else:
            raise AssertionError('Unexpected mock request: ' + str(key))
        response = requests.Response()
        response.status_code, response.request, response.url = status, request, request.url
        response._content = data if isinstance(data, bytes) else json.dumps(data).encode()
        response._content_consumed = True
        response.raw = io.BytesIO(response._content)
        response.headers['Content-Type'] = 'video/mp4' if isinstance(data, bytes) else 'application/json'
        response.headers.update(headers)
        return response

    def close(self):
        pass


class DeviceWorkflowContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which('ffmpeg'):
            raise unittest.SkipTest('FFmpeg is needed for the real output decode check')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'fixture.mp4'
            subprocess.run(['ffmpeg', '-nostdin', '-v', 'error', '-f', 'lavfi', '-i',
                            'color=c=blue:s=32x32:r=10:d=0.3', '-an', '-c:v', 'libx264',
                            '-pix_fmt', 'yuv420p', str(path)], check=True, capture_output=True)
            cls.media = path.read_bytes()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.token = self.root / 'fake-cli.json'
        save_token(self.token, FAKE_TOKEN, 'account-a')
        self.server = MemoryServer(self.media)
        self.api = ReplicaAPI.configured(token_file=self.token, auth_mode='device',
                                         expected_account='account-a', confirm_paid=True)
        self.api.wait = lambda _: None
        self.api.session.mount('https://', self.server)
        self.addCleanup(self.api.session.close)
        self.source = self.root / 'source.mp4'
        self.source.write_bytes(self.media)
        self.checkpoint = self.root / 'state.json'
        self.network = patch('socket.socket.connect', side_effect=AssertionError('Network forbidden'))
        self.socket_connect = self.network.start()
        self.addCleanup(self.network.stop)
        self.network_ex = patch('socket.socket.connect_ex', side_effect=AssertionError('Network forbidden'))
        self.socket_connect_ex = self.network_ex.start()
        self.addCleanup(self.network_ex.stop)

    def tearDown(self):
        self.socket_connect.assert_not_called()
        self.socket_connect_ex.assert_not_called()

    def command(self, *args):
        return execute(parser().parse_args(['--checkpoint', str(self.checkpoint), *args]),
                       factory=lambda: self.api)

    def calls(self, method, path):
        return [r for r in self.server.calls if r.method == method and urlsplit(r.url).path == path]

    def assert_device_headers(self):
        for request in self.server.calls:
            self.assertEqual(request.headers['Authorization'], 'Bearer ' + FAKE_TOKEN)
            self.assertEqual(urlsplit(request.url).netloc, urlsplit(BASE_URL).netloc)
            for key in ['Cookie', 'Origin', 'X-Replica-Account', 'X-Replica-CSRF']:
                self.assertNotIn(key, request.headers)

    def test_full_cli_workflow_uses_device_auth_and_preserves_original_defaults(self):
        self.assertEqual(self.command('upload', str(self.source))['accepted'], 1)
        status = self.command('status', JOB, '--wait-for', 'analysis')
        self.assertTrue(status['results'][0]['ok'])
        self.assertTrue(self.command('logo', JOB, '--yes')['results'][0]['ok'])
        self.assertTrue(self.command('status', JOB, '--wait-for', 'logo')['results'][0]['ok'])
        result = self.command('download', JOB, '--output-dir', str(self.root / 'output'))
        saved = result['results'][0]
        self.assertTrue(saved['ok'])
        self.assertTrue(saved['full_decode_passed'])
        self.assertEqual(saved['sha256'], hashlib.sha256(self.media).hexdigest())
        self.assertEqual(Path(saved['path']).read_bytes(), self.media)
        upload = self.calls('POST', '/api/jobs')[0]
        multipart = BytesParser(policy=policy.default).parsebytes(
            ('Content-Type: ' + upload.headers['Content-Type'] + '\r\n\r\n').encode() + upload.body)
        parts = {p.get_param('name', header='content-disposition'): p for p in multipart.iter_parts()}
        self.assertEqual(set(parts), {'variants', 'character', 'auto_analyze', 'files'})
        self.assertEqual(parts['auto_analyze'].get_payload(decode=True), b'true')
        self.assertEqual(parts['variants'].get_payload(decode=True), b'1')
        self.assertEqual(parts['files'].get_payload(decode=True), self.media)
        self.assertRegex(upload.headers['X-Replica-Upload-Id'], r'^[0-9a-f]{32}$')
        logo = self.calls('POST', '/api/jobs/' + JOB + '/actions/logo_replace')[0]
        body = json.loads(logo.body)
        self.assertTrue(body['use_defaults'])
        self.assertEqual(body['logo_mode'], 'smart')
        self.assertEqual(body['revision'], 8)
        self.assertFalse(body['remove_watermarks'])  # Preserve the platform default.
        self.assertEqual(logo.headers['X-Replica-Confirm'], 'paid')
        self.assertNotIn('X-Replica-Confirm', upload.headers)
        plan = self.calls('POST', '/api/jobs/download/files')[0]
        self.assertEqual(json.loads(plan.body), {'ids': [JOB], 'languages': ['source'], 'step': 'logo'})
        self.assertNotIn('X-Replica-Confirm', plan.headers)
        self.assert_device_headers()

    def test_repeating_confirmed_cli_work_does_not_upload_or_generate_twice(self):
        self.command('upload', str(self.source))
        self.command('upload', str(self.source))
        self.command('logo', JOB, '--yes')
        self.command('logo', JOB, '--yes')
        self.assertEqual(len(self.calls('POST', '/api/jobs')), 1)
        self.assertEqual(len(self.calls('POST', '/api/jobs/' + JOB + '/actions/logo_replace')), 1)

    def test_paid_confirmation_is_enforced_before_cli_posts(self):
        with self.assertRaises(ReplicaError) as error:
            self.command('logo', JOB)
        self.assertEqual(error.exception.code, 'confirmation_required')
        self.assertFalse(any(r.method == 'POST' for r in self.server.calls))
        self.assertFalse(self.checkpoint.exists())

    def test_server_403_is_visible_and_does_not_trigger_cookie_fallback_or_retry(self):
        self.api.authenticate()
        path = '/api/jobs/' + JOB + '/actions/logo_replace'
        self.server.overrides['POST', path] = (403, {'detail': 'Mock run scope denied'})
        with patch('ua_workflows.replica_auth_bridge.bridge_status') as bridge:
            with self.assertRaises(ReplicaError) as error:
                self.api.smart_logo({'id': JOB, 'revision': 8}, {'logo_mode': 'smart'})
        self.assertEqual(error.exception.status, 403)
        self.assertEqual(error.exception.code, 'permission_denied')
        self.assertIn('Mock run scope denied', str(error.exception))
        self.assertEqual(len(self.calls('POST', path)), 1)
        bridge.assert_not_called()

    def test_unknown_logo_response_is_not_replayed(self):
        path = '/api/jobs/' + JOB + '/actions/logo_replace'
        self.server.overrides['POST', path] = requests.Timeout('mock timeout')
        first = self.command('logo', JOB, '--yes')
        second = self.command('logo', JOB, '--yes')
        self.assertFalse(first['results'][0]['ok'])
        self.assertEqual(second['results'][0]['code'], 'logo_unknown')
        self.assertEqual(len(self.calls('POST', path)), 1)

    def test_upload_denial_is_not_reported_as_uploaded(self):
        self.server.overrides['POST', '/api/jobs'] = (403, {'detail': 'Mock upload scope denied'})
        result = self.command('upload', str(self.source))
        self.assertEqual(result['accepted'], 0)
        self.assertTrue(result['stopped'])
        self.assertEqual(result['results'][0]['code'], 'permission_denied')
        self.assertEqual(len(self.calls('POST', '/api/jobs')), 1)

    def test_download_denial_leaves_no_completed_or_partial_file(self):
        path = '/api/jobs/' + JOB + '/files/result.mp4'
        self.server.overrides['GET', path] = (403, {'detail': 'Mock download scope denied'})
        target = self.root / 'denied.mp4'
        with self.assertRaises(ReplicaError) as error:
            self.api.download(JOB, {'name': 'result.mp4', 'bytes': len(self.media), 'url': path}, target)
        self.assertEqual(error.exception.status, 403)
        self.assertFalse(target.exists())
        self.assertFalse(target.with_name(target.name + '.part').exists())

    def test_download_exact_size_guard_rejects_short_stream(self):
        path = '/api/jobs/' + JOB + '/files/result.mp4'
        target = self.root / 'short.mp4'
        with self.assertRaises(ReplicaError) as error:
            self.api.download(JOB, {'name': 'result.mp4', 'bytes': len(self.media) + 1, 'url': path}, target)
        self.assertEqual(error.exception.code, 'invalid_media')
        self.assertFalse(target.exists())
        self.assertFalse(target.with_name(target.name + '.part').exists())


if __name__ == '__main__':
    unittest.main()
