import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ua_workflows.replica_ad_workflow import advance_one
from ua_workflows.replica_cli import Checkpoint
from ua_workflows.replica_studio_api import ReplicaError


class API:
    def __init__(self):
        self.value = {
            'id': 'job1', 'status': 'uploaded', 'revision': 10,
            'source_file': 'source.mp4', 'step_states': {'analysis': 'done'},
            'logo_replace_view': {'state': 'replaced', 'result_file': 'logo/result.mp4',
                                  'settings': {'remove_cta': True, 'remove_gameplay': True}, 'report': {}},
            'translation': {'source_language': 'en', 'languages': {}},
        }
        self.posts = []
        self.matched = True

    def job(self, job_id):
        return copy.deepcopy(self.value)

    def translation_settings(self):
        return {'available': True, 'text_available': True, 'enabled': ['ja', 'ko']}

    def request(self, method, path, *, body=None):
        if method == 'GET':
            return [{'id': (lang or 'source') + '-cta', 'category': 'cta', 'scope': 'default', 'available': True, 'language': lang}
                    for lang in (None, 'ja', 'ko')]
        self.posts.append((method, path, body))
        return {'id': 'job1', 'status': 'queued', 'revision': 11}

    def source_matches(self, *args):
        return self.matched

    def translate(self, job, options):
        self.posts.append(('translate', options))
        return {'id': 'job1', 'status': 'queued', 'revision': 11}


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.checkpoint = Checkpoint(Path(self.temp.name) / 'state.json', 'account')
        self.api = API()
        self.download = {'results': [{'ok': True, 'size': 3, 'sha256': 'a' * 64}]}

    def run_step(self):
        with patch('ua_workflows.replica_ad_workflow.download_many', return_value=self.download):
            return advance_one(self.api, 'job1', self.checkpoint, ['ja', 'ko'], self.temp.name)

    def test_source_mismatch_stops_before_paid_submission(self):
        self.api.matched = False
        with self.assertRaises(ReplicaError):
            self.run_step()
        self.assertEqual(self.api.posts, [])

    def test_completed_translation_is_required_before_assembly(self):
        result = self.run_step()
        self.assertEqual(result['message'], 'Translation submitted')
        self.assertEqual(self.api.posts[0][1]['parts'], ['text', 'voice'])
        self.assertEqual(self.api.posts[0][1]['trim'], {'remove_cta': True, 'remove_gameplay': True})
        self.api.value['translation']['languages'] = {
            lang: {'status': 'completed', 'input': 'original', 'parts': ['text', 'voice'],
                   'video_file': lang + '.mp4'} for lang in ('ja', 'ko')}
        result = self.run_step()
        self.assertEqual(result['message'], 'CTA assembly submitted')
        self.assertEqual(self.api.posts[1][2]['languages'], ['source', 'ja', 'ko'])

    def test_existing_cta_stops_before_translation(self):
        self.api.value['logo_replace_view']['report']['finish'] = {'cta_id': 'old'}
        with self.assertRaises(ReplicaError):
            self.run_step()
        self.assertEqual(self.api.posts, [])
