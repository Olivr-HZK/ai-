"""Logo prerequisite, exact input identity, multilingual write/recovery contracts."""
import asyncio
import copy
import json
from pathlib import Path
from unittest.mock import patch

from test_replica_main_table import MainFixture,row
from ua_workflows import replica_main_table as main
from ua_workflows.replica_studio_api import ReplicaError
from ua_workflows.replica_studio_api import save_private


class TranslationTests(MainFixture):
    def translation(self,languages=('日语','德语')):
        self.client.rows['recMain']=row(None,'替换语言')
        self.client.rows['recMain']['fields'][main.LANGUAGE]=list(languages)

    async def test_two_languages_logo_first_exact_input_then_one_atomic_attachment_write(self):
        self.translation();before=copy.deepcopy(self.client.rows['recMain']['fields'])
        self.start();await self.until(lambda:self.state.get('recMain')['stage']=='verified' if self.state.items else False)
        await self.until(lambda:self.client.rows['recMain']['fields'][main.STATUS]=='结果返回')
        self.assertEqual(self.backend.uploads,['job-source'])
        self.assertEqual(self.backend.logos,['job-source'])
        self.assertEqual(self.backend.translations,[('job-source',['ja','de'])])
        self.assertEqual(self.backend.routes,set())
        self.assertEqual(self.backend.translation_options['job-source']['input'],'original')
        self.assertEqual(self.backend.translation_options['job-source']['parts'],['text','voice'])
        self.assertEqual(self.backend.translation_options['job-source']['trim'],{'remove_cta':False,'remove_gameplay':False})
        events=[e[0] for e in self.backend.events]
        self.assertLess(events.index('logo'),events.index('verify-logo-input'))
        self.assertLess(events.index('verify-logo-input'),events.index('translate'))
        f=self.client.rows['recMain']['fields']
        self.assertEqual({x['name'] for x in f[main.RESULT]}, {'source_translation_ja.mp4','source_translation_de.mp4'})
        self.assertTrue(self.state.get('recMain')['translation_input_proof']['matches_logo'])
        self.assertEqual(self.state.items['recMain']['pipeline'],['logo','translation'])
        for k in [main.SOURCE,main.TYPE,main.LANGUAGE]:self.assertEqual(f[k],before[k])
        writes=[c for c in self.client.calls if c[1].endswith('/batch_update')]
        self.assertEqual(len(writes),1)
        self.assertEqual(len(writes[0][2]['json']['records'][0]['fields'][main.RESULT]),2)
        self.runner._control_once();self.runner._upload_once();self.runner._deliver_once()
        self.assertEqual(len(self.backend.uploads),1);self.assertEqual(len(self.backend.translations),1)

    async def test_missing_language_waits_without_upload_then_starts_after_language_added(self):
        self.translation(())
        self.runner._control_once();self.runner._upload_once()
        self.assertEqual(self.state.items,{})
        self.assertIn('至少一个',self.client.rows['recMain']['fields'][main.MESSAGE])
        self.assertEqual(self.backend.uploads,[])
        self.client.rows['recMain']['fields'][main.LANGUAGE]=['日语']
        self.start();await self.until(lambda:self.state.get('recMain')['stage']=='verified' if self.state.items else False)
        self.assertEqual(self.backend.logos,['job-source'])
        self.assertEqual(self.backend.translations,[('job-source',['ja'])])

    async def test_logo_must_finish_before_translation(self):
        self.translation();self.backend.logo_hold=True
        self.start();await self.until(lambda:bool(self.backend.logos))
        await asyncio.sleep(.03)
        self.assertEqual(self.backend.translations,[])
        self.backend.completed.add('job-source');self.runner._events['control'].set()
        await self.until(lambda:self.state.get('recMain')['stage']=='verified')
        self.assertEqual(len(self.backend.translations),1)

    async def test_logo_failure_never_translates_or_retries(self):
        self.translation();self.backend.logo_fail=True
        self.start();await self.until(lambda:self.state.get('recMain')['stage']=='failed' if self.state.items else False)
        self.runner._control_once();self.runner._upload_once()
        self.assertEqual(self.backend.translations,[]);self.assertEqual(len(self.backend.logos),1)

    async def test_original_video_different_from_logo_result_blocks_translation(self):
        self.translation();self.backend.wrong_source=True
        self.start();await self.until(lambda:self.state.get('recMain')['stage']=='needs_review' if self.state.items else False)
        self.assertIn('不是换Logo成品',self.state.get('recMain')['error'])
        self.assertEqual(self.backend.translations,[])

    async def test_short_translated_output_is_not_backfilled_or_regenerated(self):
        self.translation(('日语',))
        with patch('ua_workflows.replica_main_translation.video_duration',side_effect=lambda p:28.4 if 'translation-input-proof' in str(p) else 5.0):
            self.start();await self.until(lambda:self.state.get('recMain')['stage']=='needs_review' if self.state.items else False)
        value=self.state.get('recMain')
        self.assertIn('5.000秒',value['error']);self.assertIn('28.400秒',value['error'])
        self.assertNotIn(main.RESULT,self.client.rows['recMain']['fields'])
        self.runner._control_once();self.assertEqual(len(self.backend.translations),1)

    async def test_duration_cannot_be_verified_blocks_translation_before_paid_post(self):
        self.translation(('日语',))
        with patch('ua_workflows.replica_main_translation.video_duration',side_effect=ReplicaError('invalid_media','duration unavailable')):
            self.start();await self.until(lambda:self.state.get('recMain')['stage']=='needs_review' if self.state.items else False)
        self.assertEqual(self.backend.translations,[])

    async def test_language_mutation_after_claim_blocks_upload(self):
        self.translation();self.runner._admit()
        self.client.rows['recMain']['fields'][main.LANGUAGE]=['法语']
        self.runner._upload_once();self.assertEqual(self.backend.uploads,[])
        self.assertEqual(self.state.get('recMain')['stage'],'needs_review')

    async def test_disabled_language_not_uploaded(self):
        self.translation(('法语',));self.runner._control_once();self.runner._upload_once()
        self.assertEqual(self.state.items,{})
        self.assertIn('尚未在平台启用',self.client.rows['recMain']['fields'][main.MESSAGE])
        self.assertEqual(self.backend.uploads,[])

    async def test_lost_write_receipt_is_read_only_until_late_attachment_appears(self):
        self.translation();self.client.lose_write=True
        self.start();await self.until(lambda:self.state.get('recMain')['stage']=='write_unknown' if self.state.items else False)
        def count():return len([c for c in self.client.calls if c[1].endswith('/batch_update')])
        self.assertEqual(count(),1)
        value=self.state.get('recMain');value['write_check_after']=0;self.state.put('recMain',value)
        self.runner.reconcile_writes();self.assertEqual(count(),1)
        journal=json.loads(self.runner._delivery_journal('recMain').read_text())
        self.client.rows['recMain']['fields'][main.RESULT]=[
            {'file_token':v['token'],'name':v['name'],'size':v['size']} for v in journal['files']]
        value=self.state.get('recMain');value['write_check_after']=0;self.state.put('recMain',value)
        self.runner.reconcile_writes();self.assertEqual(self.state.get('recMain')['stage'],'verified')
        self.assertEqual(count(),1)

    async def test_unknown_second_attachment_never_reposts_first_or_second(self):
        self.translation();calls=[]
        def upload(client,cfg,path,name):
            calls.append(name)
            if len(calls)==2:raise TimeoutError('Media receipt lost')
            return self.upload_language(client,cfg,path,name)
        with patch('ua_workflows.replica_main_translation.upload_attachment',side_effect=upload):
            self.start();await self.until(lambda:self.state.get('recMain')['stage']=='attachment_unknown' if self.state.items else False)
            self.runner.reconcile_writes();self.runner._deliver_once();self.runner._control_once()
            self.assertEqual(len(calls),2)
        self.assertNotIn(main.RESULT,self.client.rows['recMain']['fields'])
        self.assertEqual(len(self.backend.translations),1)

    async def test_unknown_logo_is_never_mistaken_for_translation_or_reposted(self):
        self.translation();self.backend.unknown_logo.add('job-source')
        # Inspect the persisted unknown receipt before the next control tick can
        # reconcile it. Polling a transient stage races the background worker.
        self.runner._control_once()
        self.runner._upload_once()
        self.runner._control_once()
        self.assertEqual(self.state.get('recMain')['stage'],'logo_unknown')
        self.assertEqual(self.backend.logos,['job-source'])
        self.assertEqual(self.backend.translations,[])
        self.backend.completed.add('job-source')
        value=self.state.get('recMain');value['logo_check_after']=0;self.state.put('recMain',value)
        self.runner.reconcile_unknown_logos();self.start()
        await self.until(lambda:self.state.get('recMain')['stage']=='verified')
        self.assertEqual(len(self.backend.logos),1)
        self.assertEqual(len(self.backend.translations),1)

    async def test_translation_unknown_only_reads_same_job_without_new_paid_post(self):
        self.translation(('日语',));original=self.runner.api.__class__.translate
        def lost(api,job,options):
            original(api,job,options)
            raise ReplicaError('result_unknown','Lost translation receipt')
        with patch.object(self.runner.api.__class__,'translate',lost):
            self.start();await self.until(lambda:self.state.get('recMain')['stage']=='logo_unknown' if self.state.items else False)
        value=self.state.get('recMain');value['translation_check_after']=0;self.state.put('recMain',value)
        self.runner.reconcile_unknown_logos()
        await self.until(lambda:self.state.get('recMain')['stage']=='verified')
        self.assertEqual(len(self.backend.translations),1)

    async def stop_after_download(self):
        self.translation()
        with patch.object(self.runner.__class__,'_deliver_languages'):
            self.start();await self.until(lambda:self.state.get('recMain')['stage']=='downloaded' if self.state.items else False)
            self.runner._request_stop();await self.task;self.task=None

    async def test_crash_after_first_confirmed_attachment_only_uploads_remaining_prepared_file(self):
        await self.stop_after_download();journal_path=self.runner._delivery_journal('recMain')
        def crash(path,value):
            save_private(path,value)
            if Path(path)==journal_path and value['files'][0]['upload_stage']=='accepted' and value['files'][1]['upload_stage']=='prepared':
                raise SystemExit('Crash after confirmed first token')
        with patch('ua_workflows.replica_main_translation.save_private',side_effect=crash):
            with self.assertRaises(SystemExit):self.runner.deliver_records([self.state.get('recMain')])
        self.runner.reconcile_writes()
        with patch('ua_workflows.replica_main_translation.upload_attachment',side_effect=self.upload_language) as upload:
            self.runner.deliver_records([self.state.get('recMain')])
            self.assertEqual(upload.call_count,1)
            self.assertIn('_de.mp4',upload.call_args.args[3])
        self.assertEqual(self.state.get('recMain')['stage'],'verified')

    async def test_crash_at_write_dispatch_boundary_never_replays_even_before_send(self):
        await self.stop_after_download();journal_path=self.runner._delivery_journal('recMain')
        def crash(path,value):
            save_private(path,value)
            if Path(path)==journal_path and value['write_stage']=='dispatching':raise SystemExit('Crash at write boundary')
        with patch('ua_workflows.replica_main_translation.save_private',side_effect=crash):
            with self.assertRaises(SystemExit):self.runner.deliver_records([self.state.get('recMain')])
        self.runner.deliver_records([self.state.get('recMain')])
        self.assertEqual(self.state.get('recMain')['stage'],'write_unknown')
        self.assertEqual(len([c for c in self.client.calls if c[1].endswith('/batch_update')]),0)
