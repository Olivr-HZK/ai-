"""Offline tests for supplied portrait uploads; no image or video generation."""
import base64
import copy
import hashlib
import json
from pathlib import Path
import struct
import subprocess
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, Mock, patch
import zlib

import requests

from ua_workflows.replica_auth_bridge import Broker
from ua_workflows.replica_cli import Checkpoint, execute, main, parser
from ua_workflows.replica_portraits import character_list, replace_portrait
from ua_workflows.replica_studio_api import BASE_URL, ReplicaAPI, ReplicaError, prepare_portrait


def png(width=320, height=400):
    def chunk(kind, data):
        return struct.pack('>I', len(data)) + kind + data + struct.pack('>I', zlib.crc32(kind + data))
    return (b'\x89PNG\r\n\x1a\n' + chunk(b'IHDR', struct.pack('>2I5B', width, height, 8, 2, 0, 0, 0)) +
            chunk(b'IDAT', zlib.compress((b'\0' + b'\x30\x80\xc0' * width) * height)) + chunk(b'IEND', b''))


def job():
    return {'id': 'job-a', 'revision': 8, 'status': 'analysis_review', 'source_kind': 'video',
            'step_states': {'analysis': 'done'}, 'cast_members': [
                {'source_id': 'person-1', 'character_name': 'Alice', 'portrait_file': 'portraits/old.png', 'portrait_ready': True}],
            'video_route': 'model_swap', 'segments': [{'index': 1, 'video_file': 'old.mp4'}],
            'text_replacements': [{'from': 'old', 'to': 'new'}]}


class FakeAPI:
    def __init__(self):
        self.value = job()
        self.posts = []
        self.media = {}
        self.accept = True
        self.error = None
        self.account = 'account-a'
        self.transport_error = None

    def authenticate(self): return {'account_id': self.account}
    def job(self, job_id):
        assert job_id == self.value['id']
        return copy.deepcopy(self.value)
    def require_portrait_transport(self):
        if self.transport_error: raise self.transport_error
    def upload_portrait(self, job_id, source_id, picture):
        self.posts.append((job_id, source_id, picture))
        if self.accept:
            self.value['revision'] += 1
            person = next(row for row in self.value['cast_members'] if row['source_id'] == source_id)
            filename = 'portraits/upload-' + str(self.value['revision']) + '.png'
            person.update(portrait_file=filename, portrait_ready=True)
            self.media[filename] = picture['data']
        if self.error: raise self.error
        return self.job(job_id)
    def portrait_matches(self, job_id, filename, identity):
        data = self.media.get(filename, b'')
        return len(data) == identity['bytes'] and hashlib.sha256(data).hexdigest() == identity['sha256']


class PortraitTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.path = self.root / 'my person.png'
        self.path.write_bytes(png())
        self.cp = Checkpoint(self.root / 'state.json', 'account-a')
        self.api = FakeAPI()

    def tearDown(self): self.temp.cleanup()

    def replace(self, **kwargs):
        return replace_portrait(self.api, 'job-a', self.path, self.cp, **kwargs)

    def test_real_png_jpeg_webp_metadata_and_original_bytes(self):
        for extension in ('png', 'jpg', 'webp'):
            path = self.root / ('portrait.' + extension)
            if extension == 'png': path.write_bytes(self.path.read_bytes())
            elif extension == 'webp':
                path.write_bytes(base64.b64decode('UklGRigAAABXRUJQVlA4TBsAAAAvP8FjAAdQwMIUuP8BAEX6/58i+p/63//+DxQA'))
            else:
                subprocess.run(['ffmpeg', '-v', 'error', '-y', '-i', str(self.path), '-frames:v', '1', str(path)],
                               check=True, capture_output=True)
            picture = prepare_portrait(path)
            self.assertEqual(picture['data'], path.read_bytes())
            self.assertEqual((picture['identity']['width'], picture['identity']['height']), (320, 400))
            self.assertEqual(picture['identity']['sha256'], hashlib.sha256(path.read_bytes()).hexdigest())

    def test_invalid_short_side_format_and_size_never_post(self):
        for data in (png(319, 400), b'not an image'):
            self.path.write_bytes(data)
            with self.assertRaises(ReplicaError): self.replace()
        self.path.write_bytes(png())
        wrong = self.root / 'picture.gif';wrong.write_bytes(png())
        with self.assertRaises(ReplicaError): prepare_portrait(wrong)
        with self.path.open('wb') as stream: stream.truncate(30 * 1024 * 1024 + 1)
        with self.assertRaises(ReplicaError): self.replace()
        self.assertEqual(self.api.posts, [])

    def test_single_character_upload_uses_exact_bytes_without_other_changes(self):
        before = copy.deepcopy(self.api.value)
        result = self.replace()
        self.assertTrue(result['ok'])
        self.assertFalse(result['video_generation_submitted'])
        self.assertEqual(result['source_id'], 'person-1')
        self.assertEqual(self.api.posts[0][2]['data'], self.path.read_bytes())
        self.assertEqual(self.api.value['segments'], before['segments'])
        self.assertEqual(self.api.value['text_replacements'], before['text_replacements'])
        self.assertEqual(self.cp.get('portraits', 'job-a:person-1')['stage'], 'portrait_uploaded')

    def test_multiple_characters_require_explicit_mapping_and_only_selected_changes(self):
        self.api.value['cast_members'].append({'source_id': 'person-2', 'character_name': 'Bob', 'portrait_file': 'bob.png'})
        original = copy.deepcopy(self.api.value['cast_members'][0])
        with self.assertRaises(ReplicaError) as error: self.replace()
        self.assertEqual(error.exception.code, 'character_selection_required')
        self.assertEqual(self.api.posts, [])
        result = self.replace(source_id='person-2')
        self.assertEqual(result['source_id'], 'person-2')
        self.assertEqual(self.api.value['cast_members'][0], original)

    def test_busy_unanalyzed_missing_or_duplicate_role_never_posts(self):
        for change in ({'status': 'generating'}, {'step_states': {'analysis': 'pending'}}, {'cast_members': []},
                       {'cast_members': [job()['cast_members'][0]] * 2}):
            self.api.value = {**job(), **change}
            with self.assertRaises(ReplicaError): self.replace()
        self.api.value = job()
        with self.assertRaises(ReplicaError): self.replace(source_id='other')
        self.assertEqual(self.api.posts, [])

    def test_repeated_same_image_reuses_readonly_proof_with_one_post(self):
        self.replace()
        result = self.replace()
        self.assertTrue(result['reused'])
        self.assertFalse(result['submitted'])
        self.assertEqual(len(self.api.posts), 1)

    def test_new_image_requires_replace_and_preserves_history(self):
        self.replace()
        self.path.write_bytes(png(321, 400))
        with self.assertRaises(ReplicaError): self.replace()
        self.replace(replace=True)
        record = self.cp.get('portraits', 'job-a:person-1')
        self.assertEqual(len(record['history']), 1)
        self.assertEqual(len(self.api.posts), 2)

    def test_lost_response_recovers_original_bytes_without_second_post(self):
        self.api.error = ReplicaError('result_unknown', 'Connection lost')
        with self.assertRaises(ReplicaError): self.replace()
        self.assertEqual(self.cp.get('portraits', 'job-a:person-1')['stage'], 'portrait_unknown')
        self.cp = Checkpoint(self.root / 'state.json', 'account-a')
        result = self.replace()
        self.assertTrue(result['recovered_readonly'])
        self.assertEqual(len(self.api.posts), 1)

    def test_unconfirmed_original_cannot_be_replayed_even_with_new_image_and_replace(self):
        self.api.accept = False;self.api.error = ReplicaError('result_unknown', 'Connection lost')
        with self.assertRaises(ReplicaError): self.replace()
        self.path.write_bytes(png(321, 400))
        result = self.replace(replace=True)
        self.assertFalse(result['ok'])
        self.assertEqual(result['code'], 'result_unknown')
        self.assertEqual(len(self.api.posts), 1)

    def test_intent_save_failure_stops_before_post(self):
        with patch.object(self.cp, 'put', side_effect=OSError('Disk full')):
            with self.assertRaises(OSError): self.replace()
        self.assertEqual(self.api.posts, [])

    def test_receipt_save_failure_recovers_without_post_replay(self):
        put = self.cp.put
        calls = 0
        def save(*args):
            nonlocal calls
            calls += 1
            if calls == 2: raise OSError('Disk full after upload')
            put(*args)
        with patch.object(self.cp, 'put', side_effect=save):
            with self.assertRaises(OSError): self.replace()
        self.assertTrue(self.replace()['recovered_readonly'])
        self.assertEqual(len(self.api.posts), 1)

    def test_account_and_old_bridge_fail_before_intent(self):
        self.api.account = 'account-b'
        with self.assertRaises(ReplicaError): self.replace()
        self.api.account = 'account-a';self.api.transport_error = ReplicaError('bridge_upgrade_required', 'Old bridge')
        with self.assertRaises(ReplicaError): self.replace()
        self.assertFalse(self.cp.path.exists())
        self.assertEqual(self.api.posts, [])

    def test_cli_character_listing_and_check_only_do_not_create_checkpoint(self):
        cp = self.root / 'unused.json'
        result = execute(parser().parse_args(['characters', 'job-a']), factory=lambda: self.api)
        self.assertEqual(result['characters'][0]['source_id'], 'person-1')
        result = execute(parser().parse_args(['--checkpoint', str(cp), 'portrait', 'job-a', str(self.path), '--check-only']), factory=lambda: self.api)
        self.assertFalse(result['submitted'])
        self.assertFalse(cp.exists())
        self.assertEqual(self.api.posts, [])

    def test_cli_command_dispatch_and_unknown_exit_code(self):
        args = parser().parse_args(['--checkpoint', str(self.cp.path), 'portrait', 'job-a', str(self.path)])
        self.assertTrue(execute(args, factory=lambda: self.api)['ok'])
        with patch('ua_workflows.replica_cli.execute', return_value={'ok': False, 'code': 'result_unknown'}), patch('builtins.print'):
            self.assertEqual(main(['portrait', 'job-a', str(self.path)]), 2)


class PortraitHTTPTests(unittest.TestCase):
    def client(self):
        transport = Mock()
        response = Mock(status_code=200)
        response.json.return_value = {**job(), 'revision': 9}
        transport.request.return_value = response
        api = ReplicaAPI(transport);api.account='account-a';api.csrf='private-csrf'
        return api, transport

    def test_http_multipart_field_and_auth_are_exact(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'person.png';path.write_bytes(png())
            api, transport = self.client()
            api.upload_portrait('job-a', 'person-1', prepare_portrait(path))
        call = transport.request.call_args
        self.assertEqual(call.args, ('POST', BASE_URL + '/api/jobs/job-a/portrait/person-1'))
        self.assertEqual(set(call.kwargs['files']), {'file'})
        self.assertEqual(call.kwargs['files']['file'], ('person.png', png(), 'image/png'))
        self.assertEqual(call.kwargs['headers']['X-Replica-CSRF'], 'private-csrf')
        self.assertFalse(call.kwargs['allow_redirects'])
        self.assertNotIn('json', call.kwargs)

    def test_portrait_json_extra_fields_and_retry_are_rejected(self):
        api, transport = self.client()
        for kwargs in ({'body': {}}, {'files': {'file': ('p.png', b'a', 'image/png')}, 'body': {'force_new': True}},
                       {'files': {'files': ('p.png', b'a', 'image/png')}},
                       {'files': {'file': ('p.png', b'a', 'image/png')}, 'safe_retry': True}):
            with self.assertRaises(ReplicaError): api.request('POST', '/api/jobs/job-a/portrait/person-1', **kwargs)
        transport.request.assert_not_called()

    def test_unknown_portrait_http_write_never_retries(self):
        api, transport = self.client();transport.request.side_effect = requests.Timeout()
        with self.assertRaises(ReplicaError) as error:
            api.request('POST', '/api/jobs/job-a/portrait/person-1', files={'file': ('p.png', b'a', 'image/png')})
        self.assertEqual(error.exception.code, 'result_unknown')
        self.assertEqual(transport.request.call_count, 1)

    def test_old_bridge_capability_is_checked_before_upload(self):
        api, transport = self.client();transport.replica_capabilities=['translation_v1']
        with self.assertRaises(ReplicaError) as error: api.require_portrait_transport()
        self.assertEqual(error.exception.code, 'bridge_upgrade_required')
        transport.request.assert_not_called()

    def test_portrait_readback_hash_is_bound_to_task_and_nested_media_path(self):
        api, transport = self.client()
        response = Mock(status_code=200)
        response.iter_content.return_value = [png()]
        transport.get.return_value = MagicMock()
        transport.get.return_value.__enter__.return_value = response
        identity = {'bytes': len(png()), 'sha256': hashlib.sha256(png()).hexdigest()}
        self.assertTrue(api.portrait_matches('job-a', 'portraits/selected.png', identity))
        self.assertEqual(transport.get.call_args.args[0], BASE_URL + '/api/jobs/job-a/files/portraits/selected.png')
        identity['sha256'] = '0' * 64
        self.assertFalse(api.portrait_matches('job-a', 'portraits/selected.png', identity))
        with self.assertRaises(ReplicaError): api.portrait_matches('job-a', '../other-job/selected.png', identity)
        transport.request.assert_not_called()

    def test_malformed_upload_response_is_unknown_not_success(self):
        api, transport = self.client()
        transport.request.return_value.json.return_value = {'id': 'other-job', 'revision': 9}
        picture = {'data': png(), 'mime': 'image/png', 'identity': {
            'name': 'person.png', 'bytes': len(png()), 'sha256': hashlib.sha256(png()).hexdigest()}}
        with self.assertRaises(ReplicaError) as error: api.upload_portrait('job-a', 'person-1', picture)
        self.assertEqual(error.exception.code, 'result_unknown')
        self.assertEqual(transport.request.call_count, 1)


class PortraitBrokerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.runtime=patch('ua_workflows.replica_auth_bridge.RUNTIME',Path(self.temp.name));self.runtime.start()
        self.response=Mock(status=200,headers={})
        self.response.body=AsyncMock(return_value=b'{}');self.response.dispose=AsyncMock()
        self.context=Mock();self.context.request.fetch=AsyncMock(return_value=self.response)
        self.broker=Broker(self.context,None)

    async def asyncTearDown(self):self.runtime.stop();self.temp.cleanup()

    def payload(self):
        return {'action':'request','method':'POST','path':'/api/jobs/job-a/portrait/person-1',
                'files':{'file':{'name':'person.png','mimeType':'image/png','base64':base64.b64encode(png()).decode()}}}

    async def test_bridge_accepts_only_portrait_multipart_and_no_browser_page_action(self):
        await self.broker.dispatch(self.payload())
        call=self.context.request.fetch.call_args
        self.assertEqual(call.kwargs['multipart']['file']['buffer'],png())
        self.assertEqual(call.kwargs['max_retries'],0)
        self.assertEqual(call.kwargs['max_redirects'],0)
        self.context.new_page.assert_not_called()

    async def test_bridge_rejects_json_extra_form_wrong_field_and_generation(self):
        for mutation in ({'files':None,'json':{}},{'form':{'force_new':True}},
                         {'files':{'files':self.payload()['files']['file']}},
                         {'path':'/api/jobs/job-a/actions/videos'}):
            with self.assertRaises(ValueError): await self.broker.dispatch({**self.payload(),**mutation})
        self.context.request.fetch.assert_not_called()


if __name__ == '__main__': unittest.main()
