import base64
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

import requests

from ua_workflows.replica_auth_bridge import Broker, BridgeSession, BridgeMediaResponse, BridgeResponse
from ua_workflows.replica_studio_api import ReplicaError


class BridgeTransportTests(unittest.TestCase):
    def test_transport_sends_json_without_a_browser_page_call(self):
        result = {'status': 200, 'headers': {}, 'body': base64.b64encode(b'{"id":"job-a"}').decode()}
        with patch('ua_workflows.replica_auth_bridge.rpc', return_value=result) as rpc:
            response = BridgeSession().request('POST', 'https://replica-studio.upupup.today/api/jobs/job-a/actions/logo_replace',
                                               json={'logo_mode': 'smart'}, headers={'X-Replica-CSRF': 'test-csrf'})
        self.assertEqual(response.json(), {'id': 'job-a'})
        self.assertEqual(rpc.call_args.args[0]['json'], {'logo_mode': 'smart'})
        self.assertNotIn('cookies', rpc.call_args.args[0])

    def test_upload_keeps_original_filename_and_form_fields(self):
        result = {'status': 200, 'body': base64.b64encode(b'{}').decode()}
        with patch('ua_workflows.replica_auth_bridge.rpc', return_value=result) as rpc:
            BridgeSession().request('POST', 'https://replica-studio.upupup.today/api/jobs',
                                    data={'auto_analyze': 'true'}, files={'files': ('video.mp4', io.BytesIO(b'video-bytes'), 'video/mp4')})
        sent = rpc.call_args.args[0]
        self.assertEqual(sent['form'], {'auto_analyze': 'true'})
        self.assertEqual(sent['files']['files']['name'], 'video.mp4')
        self.assertEqual(base64.b64decode(sent['files']['files']['base64']), b'video-bytes')

    def test_transport_rejects_external_origin(self):
        with patch('ua_workflows.replica_auth_bridge.rpc') as rpc:
            with self.assertRaises(requests.ConnectionError):
                BridgeSession().request('GET', 'https://example.com/api/auth/session')
            rpc.assert_not_called()

    def test_media_ranges_are_joined_without_changing_content(self):
        session = Mock()
        def part(body, range_value):
            return BridgeResponse({'status': 206, 'headers': {'content-range': range_value}, 'body': base64.b64encode(body).decode()})
        session.request.side_effect = [part(b'abc', 'bytes 0-2/6'), part(b'def', 'bytes 3-5/6')]
        response = BridgeMediaResponse(session, 'https://replica-studio.upupup.today/api/jobs/job-a/files/a.mp4', {})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(b''.join(response.iter_content(2)), b'abcdef')
        self.assertTrue(session.request.call_args.kwargs['headers']['Range'].startswith('bytes=3-'))

    def test_changed_range_total_is_rejected(self):
        session = Mock()
        session.request.side_effect = [BridgeResponse({'status': 206, 'headers': {'content-range': 'bytes 0-2/6'}, 'body': base64.b64encode(b'abc').decode()}),
                                       BridgeResponse({'status': 206, 'headers': {'content-range': 'bytes 3-5/9'}, 'body': base64.b64encode(b'def').decode()})]
        response = BridgeMediaResponse(session, 'https://replica-studio.upupup.today/api/jobs/job-a/files/a.mp4', {})
        with self.assertRaises(requests.ConnectionError):
            b''.join(response.iter_content(2))


class BrokerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.patch = patch('ua_workflows.replica_auth_bridge.RUNTIME', Path(self.temp.name))
        self.patch.start()
        self.response = Mock(status=200, headers={'content-type': 'application/json', 'set-cookie': 'private-cookie'})
        self.response.body = AsyncMock(return_value=b'{"id":"job-a"}')
        self.response.dispose = AsyncMock()
        self.context = Mock()
        self.context.request.fetch = AsyncMock(return_value=self.response)
        self.broker = Broker(self.context, None)

    async def asyncTearDown(self):
        self.patch.stop()
        self.temp.cleanup()

    async def test_business_calls_use_api_request_context_and_never_export_cookie(self):
        result = await self.broker.dispatch({'action': 'request', 'method': 'POST',
                                            'path': '/api/jobs/job-a/actions/logo_replace', 'headers': {'X-Replica-CSRF': 'test-csrf'},
                                            'json': {'logo_mode': 'smart'}})
        self.assertEqual(result['status'], 200)
        self.assertNotIn('set-cookie', result['headers'])
        self.context.request.fetch.assert_awaited_once()
        kwargs = self.context.request.fetch.call_args.kwargs
        self.assertEqual(kwargs['max_redirects'], 0)
        self.assertEqual(kwargs['max_retries'], 0)
        self.assertEqual(kwargs['headers']['Origin'], 'https://replica-studio.upupup.today')
        self.context.new_page.assert_not_called()
        audit = (Path(self.temp.name) / 'http-audit.jsonl').read_text()
        self.assertNotIn('csrf', audit)
        self.assertNotIn('private-cookie', audit)

    async def test_task_listing_is_an_allowed_read_only_request(self):
        self.response.body = AsyncMock(return_value=b'[{"id":"job-a"}]')
        result = await self.broker.dispatch({'action': 'request', 'method': 'GET', 'path': '/api/jobs', 'headers': {}})
        self.assertEqual(result['status'], 200)
        self.assertEqual(self.context.request.fetch.call_args.kwargs['method'], 'GET')
        self.context.new_page.assert_not_called()

    async def test_translation_and_narrow_source_route_use_http_only(self):
        for method, path, body in [('POST', '/api/jobs/job-a/actions/translate', {'languages': ['ja'], 'parts': ['text', 'voice'], 'remember_parts': False}),
                                   ('PUT', '/api/jobs/job-a', {'revision': 8, 'video_route': 'source_video'})]:
            await self.broker.dispatch({'action': 'request', 'method': method, 'path': path, 'json': body})
            self.assertEqual(json.loads(self.context.request.fetch.call_args.kwargs['data']), body)
            self.assertEqual(self.context.request.fetch.call_args.kwargs['max_retries'], 0)
        self.context.new_page.assert_not_called()

    async def test_translation_cannot_force_retry_or_modify_other_job_settings(self):
        for method, path, body in [('POST', '/api/jobs/job-a/actions/translate', {'languages': ['ja'], 'force_new': True}),
                                   ('PUT', '/api/jobs/job-a', {'revision': 8, 'video_route': 'model_swap'}),
                                   ('PUT', '/api/jobs/job-a', {'revision': 8, 'video_route': 'source_video', 'character': 'other'})]:
            with self.assertRaises(ReplicaError):
                await self.broker.dispatch({'action': 'request', 'method': method, 'path': path, 'json': body})
        self.context.request.fetch.assert_not_called()

    async def test_extra_generation_and_cookie_headers_are_rejected(self):
        for path in ['/api/jobs/job-a/actions/portraits', '/api/jobs/job-a/actions/videos', '/api/batch/all',
                     '/api/deleted-projects/project-a/restore', '/api/deleted-characters/job-a/restore']:
            with self.assertRaises(ValueError):
                await self.broker.dispatch({'action': 'request', 'method': 'POST', 'path': path, 'headers': {}})
        with self.assertRaises(ValueError):
            await self.broker.dispatch({'action': 'request', 'method': 'GET', 'path': '/api/auth/session', 'headers': {'Cookie': 'injected'}})
        self.context.request.fetch.assert_not_called()

    async def test_status_only_exposes_authenticated_account_not_csrf_or_cookie(self):
        self.response.json = AsyncMock(return_value={'user': {'id': 'account-a', 'enabled': True}, 'auth_mode': 'oidc', 'csrf_token': 'private-csrf'})
        self.context.request.get = AsyncMock(return_value=self.response)
        result = await self.broker.dispatch({'action': 'status'})
        self.assertTrue(result['authenticated'])
        self.assertNotIn('private-csrf', json.dumps(result))
        self.assertNotIn('private-cookie', json.dumps(result))


if __name__ == '__main__':
    unittest.main()
