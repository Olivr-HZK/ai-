import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ua_workflows.replica_assembly import assemble_one, assembly_status, download_assemblies
from ua_workflows.replica_cli import Checkpoint
from ua_workflows.replica_studio_api import ReplicaError, validate_workflow_body


class API:
    def __init__(self):
        self.posts = []
        self.fail = False
        self.job_value = {
            'id': 'job1', 'revision': 5, 'status': 'uploaded',
            'translation': {'source_language': 'de', 'languages': {
                lang: {'status': 'completed', 'input': 'original', 'parts': ['text', 'voice'],
                       'video_file': lang + '.mp4'} for lang in ('ja', 'ko')}},
            'logo_replace_view': {'state': 'replaced', 'result_file': 'logo/result.mp4', 'report': {}},
        }
        self.assets = [{'id': (lang or 'source') + '-cta', 'category': 'cta', 'available': True, 'scope': 'default', 'language': lang}
                       for lang in (None, 'ja', 'ko')]

    def job(self, job_id):
        return self.job_value

    def request(self, method, path, *, body=None):
        if method == 'GET':
            return self.assets
        validate_workflow_body(method, path, body)
        self.posts.append(body)
        if self.fail:
            raise ReplicaError('result_unknown', 'Response lost')
        return {'id': 'job1', 'status': 'queued', 'revision': 6}


class AssemblyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.checkpoint = Checkpoint(Path(self.tmp.name) / 'state.json', 'account')
        self.api = API()

    def test_preflight_and_submit_once(self):
        self.assertEqual(assemble_one(self.api, 'job1', None, ['source', 'ja', 'ko'], check_only=True)['code'], 'ready')
        result = assemble_one(self.api, 'job1', self.checkpoint, ['source', 'ja', 'ko'])
        self.assertEqual(result['code'], 'accepted')
        self.assertTrue(assemble_one(self.api, 'job1', self.checkpoint, ['source', 'ja', 'ko'])['reused'])
        self.assertEqual(len(self.api.posts), 1)

    def test_unknown_never_reposts(self):
        self.api.fail = True
        with self.assertRaises(ReplicaError):
            assemble_one(self.api, 'job1', self.checkpoint, ['ja'])
        with self.assertRaises(ReplicaError) as error:
            assemble_one(self.api, 'job1', self.checkpoint, ['ja'])
        self.assertEqual(error.exception.code, 'result_unknown')
        self.assertEqual(len(self.api.posts), 1)

    def test_missing_cta_or_existing_cta_stops_before_post(self):
        self.api.assets = []
        with self.assertRaises(ReplicaError):
            assemble_one(self.api, 'job1', self.checkpoint, ['ja'])
        self.api.assets = [{'id': 'ja-cta', 'category': 'cta', 'available': True, 'scope': 'default', 'language': 'ja'}]
        self.api.job_value['logo_replace_view']['report']['finish'] = {'cta_id': 'existing'}
        with self.assertRaises(ReplicaError):
            assemble_one(self.api, 'job1', self.checkpoint, ['ja'])
        self.assertEqual(self.api.posts, [])

    def test_status_requires_a_new_cta_export_for_every_language(self):
        assemble_one(self.api, 'job1', self.checkpoint, ['source', 'ja', 'ko'])
        self.api.job_value.update(last_action='export', revision=6)
        self.api.job_value['exports'] = [
            {'language': None, 'file': 'out/source.mp4', 'cta': 'source-cta'},
            {'language': 'ja', 'file': 'out/ja.mp4', 'cta': 'ja-cta'},
        ]
        self.assertEqual(assembly_status(self.api, 'job1', self.checkpoint)['state'], 'waiting')
        self.api.job_value['exports'].append({'language': 'ko', 'file': 'out/ko.mp4', 'cta': 'ko-cta'})
        self.assertEqual(assembly_status(self.api, 'job1', self.checkpoint)['state'], 'complete')
        self.api.job_value['exports'].append({'language': 'ko', 'file': 'out/other.mp4', 'cta': 'ko-cta'})
        with self.assertRaises(ReplicaError):
            assembly_status(self.api, 'job1', self.checkpoint)

    def test_download_matches_export_identity_and_refuses_changed_file(self):
        assemble_one(self.api, 'job1', self.checkpoint, ['ja'])
        self.api.job_value.update(last_action='export', revision=6, exports=[
            {'language': 'ja', 'file': 'out/ja.mp4', 'cta': 'ja-cta'},
        ])
        self.api.download_plan = lambda ids, **kwargs: {'projects': [{'files': [
            {'name': 'ja.mp4', 'bytes': 3, 'url': '/api/jobs/job1/files/out/ja.mp4'}
        ]}]}

        def download(job_id, file, destination):
            from ua_workflows.replica_studio_api import digest
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(b'abc')
            return {'path': str(destination), 'name': destination.name, 'size': 3, 'sha256': digest(destination)}

        self.api.download = download
        with patch('ua_workflows.replica_assembly.decode_video'):
            result = download_assemblies(self.api, 'job1', self.checkpoint, self.tmp.name)
        path = Path(result['outputs'][0]['path'])
        self.assertEqual(path.read_bytes(), b'abc')
        path.write_bytes(b'xyz')
        with self.assertRaises(ReplicaError):
            download_assemblies(self.api, 'job1', self.checkpoint, self.tmp.name)

    def test_waiting_workflow_does_not_exit_as_success(self):
        from ua_workflows.replica_cli import main
        with patch('ua_workflows.replica_cli.execute', return_value={'state': 'waiting'}), \
             patch('builtins.print'):
            self.assertEqual(main(['workflow', 'job1', '--languages', 'ja', '--output-dir', self.tmp.name]), 4)
