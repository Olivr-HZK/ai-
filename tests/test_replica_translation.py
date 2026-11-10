import copy
import contextlib
import io
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import Mock

from ua_workflows.replica_cli import Checkpoint, execute, parser
from ua_workflows.replica_studio_api import BASE_URL, ReplicaAPI, ReplicaError, digest, translation_options
from ua_workflows.replica_translation import (
    download_translations, language_state, translate_many, translate_voice_many, translation_input, translation_status_many, video_duration,
)


def job(jid='job-a'):
    return {'id': jid, 'project_id': 'project-' + jid, 'revision': 8, 'status': 'uploaded',
            'last_action': 'logo_replace', 'source_kind': 'video', 'analysis': None, 'segments': [],
            'step_states': {'analysis': 'pending'}, 'dashboard': {'running': False},
            'source_story': {'available': True, 'active': False, 'story_end_s': 10},
            'translation': {'source_language': 'en', 'languages': {}},
            'logo_replace_view': {'state': 'replaced', 'active': 'run-1', 'result_file': 'logo/result.mp4'}}


def complete(parts=('voice',), *, subtitles=False):
    return {'status': 'completed', 'parts': list(parts), 'kind': 'subtitles' if subtitles else 'video',
            'video_file': None if subtitles else 'translations/ja/video.mp4', 'srt_file': 'translations/ja/subtitles.srt'}


class FakeAPI:
    account = 'account-a'

    def __init__(self):
        self.jobs = {jid: job(jid) for jid in ('job-a', 'job-b')}
        self.settings = {'available': True, 'text_available': True, 'enabled': ['ja', 'de']}
        self.posts, self.puts, self.reads = [], [], []
        self.error = None
        self.plan = {'projects': [], 'missing': 0, 'skipped': []}

    def authenticate(self): return {'account_id': self.account}
    def translation_settings(self): return self.settings
    def job(self, jid):
        self.reads.append(jid)
        return copy.deepcopy(self.jobs[jid])
    def use_source_video(self, value):
        self.puts.append(copy.deepcopy(value))
        self.jobs[value['id']]['source_story']['active'] = True
        self.jobs[value['id']]['revision'] += 1
        return self.job(value['id'])
    def translate(self, value, options):
        self.posts.append((value['id'], copy.deepcopy(options)))
        if self.error: raise self.error
        current = self.jobs[value['id']]
        current.update(status='translating', last_action='translate', revision=current['revision'] + 1)
        for language in options['languages']:
            row = {'status': 'queued', 'parts': options['parts']}
            if options['parts'] == ['subtitles'] and language == options.get('source_language'):
                current['translation']['original'] = row
            else:
                current['translation']['languages'][language] = row
        return {key: current[key] for key in ('id', 'status', 'revision', 'last_action')}
    def download_plan(self, ids, **kwargs):
        self.plan_selection = (ids, kwargs)
        return self.plan


class TranslationHTTPTests(unittest.TestCase):
    def client(self, value, status=200):
        response = Mock(status_code=status)
        response.json.return_value = value
        transport = Mock()
        transport.request.return_value = response
        api = ReplicaAPI(transport, wait=lambda _: None)
        api.account, api.csrf = 'account-a', 'test-csrf'
        return api, transport

    def test_translation_uses_documented_payload_and_existing_auth(self):
        api, transport = self.client({'id': 'job-a', 'status': 'queued', 'revision': 9})
        api.translate(job(), translation_options(['ja', 'de'], ['text', 'voice'], 'en'))
        call = transport.request.call_args
        self.assertEqual(call.args, ('POST', BASE_URL + '/api/jobs/job-a/actions/translate'))
        self.assertEqual(call.kwargs['json'], {'languages': ['ja', 'de'], 'parts': ['text', 'voice'],
                                             'source_language': 'en', 'remember_parts': False})
        self.assertEqual(call.kwargs['headers']['X-Replica-Account'], 'account-a')
        self.assertEqual(call.kwargs['headers']['X-Replica-CSRF'], 'test-csrf')

    def test_full_original_payload_explicitly_preserves_all_content(self):
        api, transport = self.client({'id':'job-a','status':'queued','revision':9})
        options=translation_options(['ko'],['text','voice'],'en',input_mode='original',
                                    trim={'remove_cta':False,'remove_gameplay':False})
        api.translate(job(),options)
        self.assertEqual(transport.request.call_args.kwargs['json'],{
            'languages':['ko'],'parts':['text','voice'],'source_language':'en','input':'original',
            'trim':{'remove_cta':False,'remove_gameplay':False},'remember_parts':False})
        self.assertEqual(transport.request.call_count,1)

    def test_invalid_input_and_trim_never_reach_transport(self):
        api, transport=self.client({})
        for extra in [{'input':'source_video'},{'input':'original','trim':{'remove_cta':False}},
                      {'input':'original','trim':{'remove_cta':0,'remove_gameplay':False}},
                      {'input':'swap','trim':{'remove_cta':False,'remove_gameplay':False}},
                      {'input':'original','trim':{'remove_cta':False,'remove_gameplay':False,'extra':True}}]:
            with self.assertRaises(ReplicaError):
                api.translate(job(),{'languages':['ja'],'parts':['text','voice'],**extra})
        transport.request.assert_not_called()

    def test_old_bridge_blocks_full_original_before_paid_intent(self):
        api, transport=self.client({});transport.replica_capabilities=['translation_v1']
        with self.assertRaises(ReplicaError) as found:
            translate_many(api,['job-a'],None,['ja'],input_mode='original',trim={'remove_cta':False,'remove_gameplay':False})
        self.assertEqual(found.exception.code,'bridge_upgrade_required')
        transport.request.assert_not_called()

    def test_bad_parts_language_force_and_preferences_never_reach_transport(self):
        api, transport = self.client({})
        bad = [{'languages': ['source']}, {'languages': ['hu']}, {'languages': []},
               {'languages': ['ja'], 'parts': ['voice', 'subtitles']},
               {'languages': ['ja'], 'force_new': True}, {'languages': ['ja'], 'remember_parts': True},
               {'languages': ['ja'], 'portraits': True}]
        for body in bad:
            with self.assertRaises(ReplicaError): api.request('POST', '/api/jobs/job-a/actions/translate', body={'remember_parts': False, **body})
        transport.request.assert_not_called()

    def test_minimal_sdk_request_does_not_inherit_server_preference_write_default(self):
        api, transport = self.client({'id': 'job-a', 'status': 'queued'})
        api.translate(job(), {'languages': ['ja']})
        self.assertIs(transport.request.call_args.kwargs['json']['remember_parts'], False)
        transport.reset_mock()
        with self.assertRaises(ReplicaError):
            api.request('POST', '/api/jobs/job-a/actions/translate', body={'languages': ['ja']})
        transport.request.assert_not_called()

    def test_translation_unknown_and_invalid_receipts_are_not_retried(self):
        for value, status in (({}, 503), ({'id': 'other', 'status': 'queued'}, 200)):
            api, transport = self.client(value, status)
            with self.assertRaises(ReplicaError) as found: api.translate(job(), translation_options(['ja']))
            self.assertEqual(found.exception.code, 'result_unknown')
            self.assertEqual(transport.request.call_count, 1)

    def test_source_switch_is_only_route_and_latest_revision(self):
        value = job(); value['source_story']['active'] = True
        api, transport = self.client(value)
        api.use_source_video(job())
        self.assertEqual(transport.request.call_args.kwargs['json'], {'revision': 8, 'video_route': 'source_video'})
        transport.reset_mock()
        for body in ({'revision': 8, 'video_route': 'model_swap'},
                     {'revision': 8, 'video_route': 'source_video', 'character': 'changed'}):
            with self.assertRaises(ReplicaError): api.request('PUT', '/api/jobs/job-a', body=body)
        transport.request.assert_not_called()

    def test_old_owned_bridge_stops_before_a_translation_request(self):
        api, transport = self.client({})
        transport.replica_capabilities = []
        with self.assertRaises(ReplicaError) as found: api.translation_settings()
        self.assertEqual(found.exception.code, 'bridge_upgrade_required')
        transport.request.assert_not_called()

    def test_download_plan_selects_translation_languages_without_export(self):
        api, transport = self.client({'projects': []})
        api.download_plan(['job-a', 'job-a'], step='translation', languages=['ja', 'de'])
        self.assertEqual(transport.request.call_args.kwargs['json'],
                         {'ids': ['job-a'], 'step': 'translation', 'languages': ['ja', 'de']})


class TranslationWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.api = FakeAPI()
        self.checkpoint = Checkpoint(self.root / 'state.json', self.api.account)

    def tearDown(self): self.temp.cleanup()

    def test_voice_interface_preserves_full_input_and_works_without_text_service(self):
        self.api.settings['text_available'] = False
        result = translate_voice_many(self.api, ['job-a'], self.checkpoint, ['ja', 'de'])
        self.assertTrue(result['results'][0]['ok'])
        self.assertEqual(self.api.posts[0][1], {
            'languages': ['ja', 'de'], 'parts': ['voice'], 'source_language': 'en',
            'input': 'original', 'trim': {'remove_cta': False, 'remove_gameplay': False},
            'remember_parts': False,
        })
        self.assertEqual(self.api.puts, [])
        self.assertEqual(self.checkpoint.get('translations', 'job-a')['attempts'][0]['stage'], 'accepted')

    def test_voice_interface_unknown_receipt_cannot_repeat_paid_post(self):
        self.api.error = ReplicaError('result_unknown', 'Lost response')
        first = translate_voice_many(self.api, ['job-a'], self.checkpoint, ['ja'])
        self.assertEqual(first['results'][0]['code'], 'result_unknown')
        self.api.error = None
        second = translate_voice_many(self.api, ['job-a'], self.checkpoint, ['ja'])
        self.assertEqual(second['results'][0]['code'], 'result_unknown')
        self.assertEqual(len(self.api.posts), 1)

    def test_voice_preflight_needs_no_checkpoint_and_writes_nothing(self):
        result = translate_voice_many(self.api, ['job-a'], None, ['ja'], check_only=True)
        self.assertTrue(result['results'][0]['ok'])
        self.assertEqual(self.api.posts, [])
        self.assertEqual(self.api.puts, [])
        self.assertFalse(self.checkpoint.path.exists())

    def test_cli_rejects_text_and_subtitle_generation(self):
        for parts in (['text', 'voice'], ['subtitles']):
            with self.subTest(parts=parts), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    parser().parse_args(['translate', 'job-a', '--languages', 'ja', '--parts', *parts])
        self.assertEqual(self.api.posts, [])

    def test_cli_status_defaults_to_voice_only(self):
        value = complete()
        value.update(input='original', trim_removed=[])
        self.api.jobs['job-a']['translation']['languages']['ja'] = value
        args = parser().parse_args(['status', 'job-a', '--wait-for', 'translation', '--languages', 'ja'])
        result = execute(args, factory=lambda: self.api)
        self.assertEqual(result['pending'], [])
        self.assertEqual(args.parts, ['voice'])
        self.assertEqual(self.api.posts, [])

    def test_direct_translation_no_generated_clips_or_route_put(self):
        self.api.jobs['job-a'].update(analysis={'done': True}, segments=[{'index':0}])
        result=self.submit(direct=True)
        self.assertEqual(result['results'][0]['code'],'accepted')
        self.assertEqual(self.api.puts,[])
        self.assertEqual(len(self.api.posts),1)
        self.assertFalse(self.api.posts[0][1]['remember_parts'])

    def test_direct_unknown_never_reposts_and_checks_same_job(self):
        self.api.error=ReplicaError('result_unknown','lost response')
        self.submit(direct=True);result=self.submit(direct=True)
        self.assertEqual(result['results'][0]['code'],'result_unknown')
        self.assertEqual(len(self.api.posts),1);self.assertEqual(self.api.puts,[])
        self.assertEqual(set(self.api.reads),{'job-a'})

    def test_direct_refuses_route_put_and_busy_jobs(self):
        with self.assertRaises(ReplicaError):self.submit(direct=True,source_video=True)
        self.api.jobs['job-a']['status']='analyzing'
        self.assertFalse(self.submit(direct=True)['results'][0]['ok'])
        self.assertEqual(self.api.posts,[])

    def submit(self, ids=('job-a',), languages=('ja',), parts=('voice',), **kwargs):
        return translate_many(self.api, ids, self.checkpoint, languages, parts, **kwargs)

    def test_logo_result_with_pending_analysis_uses_original_without_route_put(self):
        self.assertEqual(translation_input(job())['route'], 'source_video')
        result = self.submit()
        self.assertTrue(result['results'][0]['ok'])
        self.assertEqual(len(self.api.posts), 1)
        self.assertEqual(self.api.puts, [])

    def test_cli_preflight_is_readonly_even_when_source_switch_requested(self):
        value = self.api.jobs['job-a']
        value.update(analysis={'story': 'known'}, segments=[{'index': 1}])
        args = parser().parse_args(['--checkpoint', str(self.root / 'unused.json'), 'translate', 'job-a',
                                   '--languages', 'ja', '--parts', 'voice', '--source-video', '--check-only'])
        result = execute(args, factory=lambda: self.api)
        self.assertFalse(result['results'][0]['input']['would_switch_source'])
        self.assertEqual(result['results'][0]['input']['route'],'original')
        self.assertEqual(self.api.posts, [])
        self.assertEqual(self.api.puts, [])
        self.assertFalse((self.root / 'unused.json').exists())

    def test_cli_defaults_full_original_voice_only_no_trim_and_no_put(self):
        args=parser().parse_args(['--checkpoint',str(self.root/'state.json'),'translate','job-a','--languages','ko'])
        self.api.settings['enabled'].append('ko')
        result=execute(args,factory=lambda:self.api)
        self.assertTrue(result['results'][0]['ok'])
        self.assertEqual(self.api.puts,[])
        options=self.api.posts[0][1]
        self.assertEqual(options['input'],'original');self.assertEqual(options['parts'],['voice'])
        self.assertEqual(options['trim'],{'remove_cta':False,'remove_gameplay':False})

    def test_full_original_ignores_short_story_range_without_route_mutation(self):
        value=self.api.jobs['job-a'];value.update(source_file='source.mp4',analysis={'story':True},segments=[{'index':1,'start_s':0,'end_s':5}])
        value['source_story'].update(active=True,story_end_s=5,video_file='source_story/clip.mp4')
        result=self.submit(parts=['text','voice'],input_mode='original',trim={'remove_cta':False,'remove_gameplay':False})
        self.assertTrue(result['results'][0]['ok']);self.assertEqual(self.api.puts,[])
        self.assertEqual(self.api.posts[0][1]['input'],'original')

    def test_old_swap_result_is_not_full_original_and_not_automatically_regenerated(self):
        self.submit(parts=['text','voice'])
        self.api.jobs['job-a'].update(status='uploaded')
        self.api.jobs['job-a']['translation']['languages']['ja']={**complete(['text','voice']),'input':'swap'}
        result=self.submit(parts=['text','voice'],input_mode='original',trim={'remove_cta':False,'remove_gameplay':False})
        self.assertEqual(result['results'][0]['code'],'needs_review');self.assertEqual(len(self.api.posts),1)

    def test_full_original_unknown_only_reconciles_same_options_no_paid_replay(self):
        self.api.error=ReplicaError('result_unknown','Lost')
        kwargs={'parts':['text','voice'],'input_mode':'original','trim':{'remove_cta':False,'remove_gameplay':False}}
        self.submit(**kwargs);self.submit(**kwargs)
        self.assertEqual(len(self.api.posts),1);self.assertEqual(self.api.puts,[])

    def test_result_must_match_input_parts_and_no_removed_content(self):
        value=job();row={**complete(['text','voice']),'input':'swap','trim_removed':[]}
        value['translation']['languages']['ja']=row
        self.assertEqual(language_state(value,'ja',['text','voice'],input_mode='original'),'review')
        row['input']='original';row['trim_removed']=['cta']
        self.assertEqual(language_state(value,'ja',['text','voice'],input_mode='original',
                                       trim={'remove_cta':False,'remove_gameplay':False}),'review')

    def test_completed_server_result_without_checkpoint_does_not_authorize_a_paid_replacement(self):
        self.api.jobs['job-a']['translation']['languages']['ja']={**complete(['text','voice']),'input':'swap'}
        preflight=self.submit(parts=['text','voice'],input_mode='original',trim={'remove_cta':False,'remove_gameplay':False},check_only=True)
        self.assertEqual(preflight['results'][0]['code'],'existing_translation_mismatch')
        self.assertFalse(preflight['results'][0]['ok'])
        result=self.submit(parts=['text','voice'],input_mode='original',trim={'remove_cta':False,'remove_gameplay':False})
        self.assertEqual(result['results'][0]['code'],'needs_review');self.assertEqual(self.api.posts,[])

    def test_status_default_requires_original_text_and_voice_without_submitting(self):
        self.api.jobs['job-a']['translation']['languages']['ja']={**complete(['text','voice']),'input':'swap'}
        args=parser().parse_args(['status','job-a','--wait-for','translation','--languages','ja','--timeout','0.01'])
        result=execute(args,factory=lambda:self.api)
        self.assertFalse(result['results'][0]['ok']);self.assertEqual(self.api.posts,[])

    def test_source_route_is_changed_only_when_explicit_and_available(self):
        value = self.api.jobs['job-a']
        value.update(analysis={'story': 'known'}, segments=[{'index': 1}])
        blocked = self.submit()
        self.assertFalse(blocked['results'][0]['ok'])
        self.assertEqual(self.api.posts, [])
        result = self.submit(source_video=True)
        self.assertTrue(result['results'][0]['ok'])
        self.assertEqual(len(self.api.puts), 1)
        self.assertEqual(len(self.api.posts), 1)

    def test_stale_segment_selection_and_unavailable_original_are_blocked(self):
        value = job(); value.update(analysis={'story': 'known'}, segments=[{'index': 1, 'video_file': 'a.mp4'}],
                                  export_settings={'segment_videos': {'1': 'missing.mp4'}})
        self.assertFalse(translation_input(value)['ready'])
        value['source_story']['available'] = False
        self.assertFalse(translation_input(value, source_video=True)['ready'])

    def test_multi_job_languages_are_deduplicated_and_repeat_is_not_paid(self):
        first = self.submit(ids=['job-a', 'job-a', 'job-b'], languages=['en', 'ja', 'de', 'ja'], parts=['text', 'voice'])
        self.assertTrue(all(x['ok'] for x in first['results']))
        self.assertEqual([entry[0] for entry in self.api.posts], ['job-a', 'job-b'])
        self.assertEqual(self.api.posts[0][1]['languages'], ['ja', 'de'])
        again = self.submit(ids=['job-a', 'job-b'], languages=['ja', 'de'], parts=['voice', 'text'])
        self.assertTrue(all(x.get('reused') for x in again['results']))
        self.assertEqual(len(self.api.posts), 2)

    def test_unknown_paid_post_stops_following_jobs_and_never_replays(self):
        self.api.error = ReplicaError('result_unknown', 'simulated lost response')
        first = self.submit(ids=['job-a', 'job-b'])
        self.assertTrue(first['stopped'])
        self.assertEqual(first['results'][1]['code'], 'not_submitted')
        self.api.error = None
        again = self.submit()
        self.assertEqual(again['results'][0]['code'], 'result_unknown')
        self.assertEqual(len(self.api.posts), 1)

    def test_unknown_translation_is_recovered_only_from_matching_real_output(self):
        self.api.error = ReplicaError('result_unknown', 'lost response')
        self.submit()
        current = self.api.jobs['job-a']
        current.update(revision=9, last_action='translate')
        current['translation']['languages']['ja'] = complete()
        result = self.submit()
        self.assertEqual(result['results'][0]['code'], 'already_done')
        self.assertEqual(len(self.api.posts), 1)

    def test_one_accepted_language_does_not_claim_additional_language_submitted(self):
        self.submit()
        result = self.submit(languages=['ja', 'de'])
        self.assertFalse(result['results'][0]['ok'])
        self.assertEqual(len(self.api.posts), 1)

    def test_completed_language_is_reused_and_only_new_language_is_submitted(self):
        self.api.jobs['job-a']['translation']['languages']['ja'] = complete()
        self.submit(languages=['ja', 'de'])
        self.assertEqual(self.api.posts[0][1]['languages'], ['de'])

    def test_save_intent_failure_prevents_generation(self):
        self.checkpoint.put = Mock(side_effect=OSError('disk full'))
        result = self.submit()
        self.assertTrue(result['stopped'])
        self.assertEqual(self.api.posts, [])

    def test_disabled_services_and_languages_never_submit(self):
        for update, parts in (({'available': False}, ['voice']), ({'text_available': False}, ['text']),
                              ({'enabled': []}, ['voice'])):
            self.api.settings = {'available': True, 'enabled': ['ja'], **update}
            self.assertFalse(self.submit(parts=parts)['results'][0]['ok'])
        self.assertEqual(self.api.posts, [])

    def test_generic_completed_and_wrong_parts_are_not_translation_success(self):
        value = job(); value['status'] = 'completed'
        self.assertEqual(language_state(value, 'ja', ['voice']), 'waiting')
        value['translation']['languages']['ja'] = complete()
        self.assertEqual(language_state(value, 'ja', ['text', 'voice']), 'waiting')
        self.api.jobs['job-a'] = value
        result = translation_status_many(self.api, ['job-a'], ['ja'], ['text', 'voice'], timeout=0)
        self.assertEqual(result['pending'], ['job-a'])

    def test_subtitles_and_original_subtitles_need_files_but_no_video(self):
        value = job(); value['translation']['original'] = complete(['subtitles'], subtitles=True)
        value['translation']['languages']['ja'] = complete(['subtitles'], subtitles=True)
        self.assertEqual(language_state(value, 'en', ['subtitles']), 'ready')
        self.assertEqual(language_state(value, 'ja', ['subtitles']), 'ready')
        value['translation']['original']['srt_file'] = None
        self.assertEqual(language_state(value, 'en', ['subtitles']), 'review')

    def test_failed_provider_row_is_not_retried_by_status(self):
        self.api.jobs['job-a']['translation']['languages']['ja'] = {'status': 'failed', 'parts': ['voice'], 'pending': {'status': 'unknown'}}
        result = translation_status_many(self.api, ['job-a'], ['ja'], timeout=0)
        self.assertFalse(result['results'][0]['ok'])
        self.assertEqual(self.api.posts, [])

    def test_legacy_checkpoint_round_trip_keeps_all_existing_sections(self):
        self.checkpoint.put('logos', 'old-job', {'stage': 'logo_accepted'})
        self.submit()
        restored = Checkpoint(self.checkpoint.path, self.api.account)
        self.assertEqual(restored.get('logos', 'old-job')['stage'], 'logo_accepted')
        self.assertEqual(restored.get('translations', 'job-a')['attempts'][0]['stage'], 'accepted')

    @unittest.skipUnless(shutil.which('ffmpeg'), 'ffmpeg not installed')
    def test_video_and_utf8_subtitles_download_and_reuse(self):
        source = self.root / 'fixture.mp4'
        subprocess.run(['ffmpeg', '-v', 'error', '-f', 'lavfi', '-i', 'color=c=blue:s=32x32:d=0.2',
                        '-an', '-c:v', 'libx264', '-pix_fmt', 'yuv420p', str(source)], check=True)
        content = source.read_bytes(); subtitle = '1\r\n00:00:00,000 --> 00:00:00,200\r\nこんにちは\r\n'
        self.assertAlmostEqual(video_duration(source),0.2,places=2)
        self.api.plan['projects'] = [{'project_id': 'project-job-a', 'files': [
            {'name': 'ja.mp4', 'bytes': len(content), 'url': '/api/jobs/job-a/files/translations/ja/video.mp4'},
            {'name': 'ja.srt', 'text': subtitle}]}]
        def download(jid, file, destination):
            destination.parent.mkdir(parents=True, exist_ok=True); destination.write_bytes(content)
            return {'path': str(destination), 'name': file['name'], 'size': len(content), 'sha256': digest(destination)}
        self.api.download = Mock(side_effect=download)
        first = download_translations(self.api, ['job-a'], self.checkpoint, self.root / 'out', ['ja'])
        self.assertEqual(first['saved'], 2)
        self.assertEqual((self.root / 'out/project-job-a/ja.srt').read_bytes(), subtitle.encode('utf-8'))
        self.assertTrue(first['results'][0]['full_decode_passed'])
        again = download_translations(self.api, ['job-a'], self.checkpoint, self.root / 'out', ['ja'])
        self.assertTrue(all(x.get('reused') for x in again['results']))
        self.assertEqual(self.api.download.call_count, 1)

    def test_unselected_projects_tasks_and_unsafe_names_are_rejected(self):
        for project_id, file in [('other-project', {'name': 'ja.srt', 'text': 'text'}),
                                 ('project-job-a', {'name': '../ja.srt', 'text': 'text'}),
                                 ('project-job-a', {'name': 'ja.mp4', 'bytes': 3, 'url': '/api/jobs/job-b/files/a.mp4'}),
                                 ('project-job-a', {'name': 'ja.mp4', 'bytes': 3, 'url': 'https://evil.example/api/jobs/job-a/files/a.mp4'})]:
            self.api.plan['projects'] = [{'project_id': project_id, 'files': [file]}]
            with self.assertRaises(ReplicaError): download_translations(self.api, ['job-a'], self.checkpoint, self.root / 'out', ['ja'])
        self.assertFalse((self.root / 'out').exists())


if __name__ == '__main__':
    unittest.main()
