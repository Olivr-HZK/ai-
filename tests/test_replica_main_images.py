"""Offline main-table pictures: real decode, fake CLIs and Feishu writes only."""
import asyncio
import copy
import io
import json
from pathlib import Path
import uuid
from unittest.mock import patch

from PIL import Image

from test_replica_main_table import MainFixture, API, row
from ua_workflows import replica_main_table as main
from ua_workflows import material_replica as material
from ua_workflows.replica_images import ENDPOINT, WORKFLOW, MODEL, PROVIDER
from ua_workflows.replica_studio_api import ReplicaError, save_private


class Lab:
    def __init__(self, root):
        self.root = root
        self.calls, self.creates, self.tasks = [], [], {}
        self.lost = False
        self.recoverable = False
        self.status = 'completed'
        self.identity_error = None
        self.bad_dimensions = False
        self.output_sizes = {}
        self.account = {'email':'fixture@example.invalid', 'profile':'default',
                        'endpoint':ENDPOINT, 'project_id':'project-a'}

    def identity(self):
        if self.identity_error:
            raise self.identity_error
        return copy.deepcopy(self.account)

    def recover(self, request):
        self.calls.append(('recover',request))
        return self.tasks[request]['id'] if self.recoverable and request in self.tasks else None

    def call(self,*args,**kwargs):
        self.calls.append(args)
        if args[:2] == ('task','create'):
            request = args[args.index('--submission-request-id')+1]
            assert uuid.UUID(request)
            states = [json.loads(p.read_text()) for p in self.root.glob('image-runs/*/state.json')]
            assert any(v.get('submission_request_id') == request and v['stage']=='submitting'
                       for s in states for v in s['items'].values())
            params = json.loads(Path(args[args.index('--params-file')+1]).read_text())
            assert args[args.index('--workflow')+1] == WORKFLOW
            assert params['generation_config']['image'] == {
                'provider':PROVIDER, 'model':MODEL, 'size':'__original_image__'}
            assert params['task_config'] == {'execution_count':1}
            assert len(params['input_data']['images']) == 2
            task = {'id':str(uuid.uuid4()), 'workflow_id':WORKFLOW, 'status':'pending', 'params':params}
            self.tasks[request] = task
            self.creates.append(request)
            if self.lost:
                raise ReplicaError('result_unknown','Lost create receipt')
            return {'task':task}
        if args[:2] == ('task','get'):
            assert any(t['id']==args[2] for t in self.tasks.values())
            return {'task':{'id':args[2], 'workflow_id':WORKFLOW, 'status':self.status}}
        if args[:2] == ('task','outputs'):
            return {'task':{'id':args[2], 'workflow_id':WORKFLOW, 'status':'completed'},
                    'outputs':[{'path':'outputs/'+args[2]+'.png'}]}
        if args[:2] == ('media','download'):
            remote = args[args.index('--path')+1]
            task = next(t for t in self.tasks.values() if t['id']==Path(remote).stem)
            source = task['params']['input_data']['images'][0]['path']
            with Image.open(source) as image:
                size = (1,1) if self.bad_dimensions else self.output_sizes.get(image.size, image.size)
            output = Path(args[args.index('--output')+1])
            output.parent.mkdir(parents=True,exist_ok=True)
            Image.new('RGB',size,'red').save(output)
            return {'bytes':output.stat().st_size}
        raise AssertionError(args)


class ImageTests(MainFixture):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.reference = self.root/'brand.png'
        Image.new('RGB',(32,32),'white').save(self.reference)
        self.lab = Lab(self.root)
        self.runner.image_client_factory = lambda:self.lab
        self.image_patches = [patch('ua_workflows.replica_main_images.REFERENCE',self.reference),
                              patch.object(self.client.session,'get',side_effect=self.stream)]
        for p in self.image_patches:p.start()
        self.media = {}
        self.image_row()

    async def asyncTearDown(self):
        # Stop lanes before restoring streaming mocks.
        self.runner._request_stop()
        if self.task is not None:
            await asyncio.wait_for(self.task,8);self.task=None
        for p in reversed(self.image_patches):p.stop()
        await super().asyncTearDown()

    def image_row(self, count=1, sizes=None):
        r = row(None)
        r['fields']['素材类型'] = '图片'
        files = []
        for i in range(count):
            content=io.BytesIO()
            Image.new('RGB',(sizes or [(72,128)]*count)[i],('blue','green')[i%2]).save(content,format='PNG')
            token='image-'+str(i)
            self.media[token]=content.getvalue()
            files.append({'file_token':token,'name':'source.png','size':len(self.media[token]),
                          'url':'https://fixture.invalid/'+token})
        r['fields'][main.SOURCE]=files
        self.client.rows['recMain']=r
        return r

    def stream(self,url,**kwargs):
        from unittest.mock import Mock
        name=url.rsplit('/',1)[-1]
        data=b'src' if name=='source.mp4' else self.media[name]
        response=Mock()
        response.iter_content.side_effect=lambda size:iter([data])
        context=Mock()
        context.__enter__=Mock(return_value=response)
        context.__exit__=Mock(return_value=False)
        return context

    def tick(self):
        self.runner._control_once()
        checkpoint=self.root/'image-runs/recMain/state.json'
        if checkpoint.exists():
            state=material.State(checkpoint.parent)
            for item in state.data['items'].values():item['check_after']=0
            state.save()
        value=self.state.get('recMain') if self.state.items else {}
        if value.get('image_check_after'):
            value['image_check_after']=0;self.state.put('recMain',value)
        self.runner._upload_once()
        self.runner._deliver_once()
        self.runner._sync_progress()

    def test_plan_routes_images_with_frozen_reference_without_backend_calls(self):
        item=main.plan(self.client.rows['recMain'])
        self.assertEqual(item['engine'],'videolab')
        self.assertEqual(item['brand_reference']['path'],str(self.reference))
        self.assertEqual(len(item['sources']),1)
        self.assertEqual(self.lab.creates,[])
        self.assertEqual(self.backend.uploads,[])

    async def test_image_only_roundtrip_does_not_need_replica_session(self):
        with patch.object(API,'authenticate',side_effect=ReplicaError('authentication_required','Replica offline')):
            self.start()
            await self.until(lambda:self.state.items and self.state.get('recMain')['stage']=='verified')
        self.assertEqual(len(self.lab.creates),1)
        self.assertEqual(self.backend.uploads,[])
        self.assertEqual(self.backend.logos,[])
        self.assertEqual(self.backend.translations,[])
        value=self.state.get('recMain')
        self.assertTrue(value['visual_review_required'])
        self.assertEqual(self.client.rows['recMain']['fields'][main.RESULT][0]['size'],value['outputs'][0]['size'])

    def test_multi_image_same_names_one_row_multiple_results_and_no_regeneration(self):
        original=copy.deepcopy(self.image_row(2)['fields'])
        for _ in range(5):self.tick()
        value=self.state.get('recMain')
        self.assertEqual(value['stage'],'verified')
        self.assertEqual(len(self.lab.creates),2)
        self.assertEqual(len(value['outputs']),2)
        self.assertEqual(len({v['path'] for v in value['outputs']}),2)
        self.assertEqual(self.client.rows['recMain']['fields'][main.SOURCE],original[main.SOURCE])
        self.assertEqual(len(self.client.rows['recMain']['fields'][main.RESULT]),2)
        writes=[c for c in self.client.calls if c[1].endswith('/batch_update')]
        self.assertEqual(len(writes),1)
        self.assertEqual(len(self.client.rows),1)

    def test_model_size_rounding_delivers_original_dimensions_to_main_table(self):
        sizes = {(941, 1672): (944, 1664), (1080, 1080): (1072, 1072)}
        self.image_row(2, sizes=list(sizes))
        self.lab.output_sizes = sizes
        for _ in range(3):
            self.tick()
        value = self.state.get('recMain')
        self.assertEqual(value['stage'], 'verified')
        self.assertTrue(value['visual_review_required'])
        self.assertEqual(len(self.lab.creates), 2)
        self.assertEqual(len(value['outputs']), 2)
        attachments = self.client.rows['recMain']['fields'][main.RESULT]
        run = material.State(self.root/'image-runs/recMain')
        for source_size, output, attachment, item in zip(
                sizes, value['outputs'], attachments, run.data['items'].values(), strict=True):
            with self.subTest(source_size=source_size):
                with Image.open(output['path']) as image:
                    self.assertEqual(image.size, source_size)
                self.assertEqual(attachment['size'], output['size'])
                with Image.open(item['verified_download']['path']) as image:
                    self.assertEqual(image.size, sizes[source_size])

    def test_existing_result_or_unsupported_image_translation_never_creates(self):
        self.client.rows['recMain']['fields'][main.RESULT]=[{'file_token':'historical'}]
        self.tick();self.assertEqual(self.state.items,{})
        self.client.rows['recMain']['fields'].pop(main.RESULT)
        self.client.rows['recMain']['fields'].update({main.TYPE:'替换语言',main.LANGUAGE:['日语']})
        self.tick()
        self.assertEqual(self.state.items,{})
        self.assertEqual(self.lab.creates,[])
        self.assertIn('图片当前仅支持',self.client.rows['recMain']['fields'][main.MESSAGE])

    def test_landscape_in_multi_image_blocks_entire_row_before_generation(self):
        self.image_row(2,sizes=[(72,128),(128,72)])
        self.tick()
        self.assertEqual(self.state.get('recMain')['stage'],'needs_review')
        self.assertEqual(self.lab.creates,[])

    def test_changed_second_attachment_or_type_stops_before_submission(self):
        self.image_row(2);self.runner._admit()
        self.client.rows['recMain']['fields'][main.SOURCE][1]['file_token']='changed'
        self.runner._upload_once()
        self.assertEqual(self.state.get('recMain')['stage'],'needs_review')
        self.assertEqual(self.lab.creates,[])

    def test_failure_is_terminal_after_scan_or_restart(self):
        self.tick();self.lab.status='failed';self.tick();self.tick()
        self.assertEqual(self.state.get('recMain')['stage'],'failed')
        self.runner=main.MainRunner(main.MainState(self.root,self.settings),self.settings,
            api_factory=lambda:API(self.backend),feishu=self.client,image_client_factory=lambda:self.lab)
        self.tick()
        self.assertEqual(len(self.lab.creates),1)
        self.assertNotIn(main.RESULT,self.client.rows['recMain']['fields'])

    def test_unknown_create_is_only_reconciled_by_original_uuid_after_restart(self):
        self.lab.lost=True;self.tick()
        self.assertEqual(self.state.get('recMain')['stage'],'image_unknown')
        original=self.lab.creates[0]
        self.runner=main.MainRunner(main.MainState(self.root,self.settings),self.settings,
            api_factory=lambda:API(self.backend),feishu=self.client,image_client_factory=lambda:self.lab)
        self.tick();self.assertEqual(self.lab.creates,[original])
        self.assertEqual(self.state.get('recMain')['stage'],'image_unknown')
        self.lab.recoverable=True;self.tick()
        self.assertEqual(self.state.get('recMain')['stage'],'verified')
        self.assertEqual([c[1] for c in self.lab.calls if c[0]=='recover'],[original,original])

    def test_changed_brand_reference_or_account_cannot_create(self):
        self.runner._admit()
        Image.new('RGB',(32,32),'black').save(self.reference)
        self.runner._upload_once()
        self.assertEqual(self.state.get('recMain')['stage'],'needs_review')
        self.assertEqual(self.lab.creates,[])

    def test_changed_account_after_acceptance_is_review_not_new_task(self):
        self.tick();self.lab.account['project_id']='project-b';self.tick()
        self.assertEqual(self.state.get('recMain')['stage'],'needs_review')
        self.assertEqual(len(self.lab.creates),1)

    def test_bad_output_dimensions_never_written(self):
        self.tick();self.lab.bad_dimensions=True;self.tick()
        self.assertEqual(self.state.get('recMain')['stage'],'needs_review')
        self.assertNotIn(main.RESULT,self.client.rows['recMain']['fields'])

    def test_lost_attachment_receipt_never_reuploads_or_regenerates(self):
        self.tick()
        with patch('ua_workflows.replica_main_translation.upload_attachment',side_effect=TimeoutError('Lost')) as upload:
            self.tick();self.tick()
        self.assertEqual(upload.call_count,1)
        self.assertEqual(self.state.get('recMain')['stage'],'attachment_unknown')
        self.assertEqual(len(self.lab.creates),1)

    def test_unknown_write_only_reads_original_row_without_reposting(self):
        self.tick();self.client.lose_write=True;self.tick()
        value=self.state.get('recMain')
        self.assertEqual(value['stage'],'write_unknown')
        receipts=value['attachment_receipts']
        self.client.rows['recMain']['fields'][main.RESULT]=[
            {'file_token':f['token'],'name':f['name'],'size':f['size']} for f in receipts]
        value['write_check_after']=0;self.state.put('recMain',value)
        self.tick()
        self.assertEqual(self.state.get('recMain')['stage'],'verified')
        self.assertEqual(len([c for c in self.client.calls if c[1].endswith('/batch_update')]),1)

    def test_foreign_result_appearing_after_acceptance_is_preserved(self):
        self.tick()
        self.client.rows['recMain']['fields'][main.RESULT]=[{'file_token':'foreign'}]
        self.tick()
        self.assertEqual(self.state.get('recMain')['stage'],'needs_review')
        self.assertEqual(self.client.rows['recMain']['fields'][main.RESULT],[{'file_token':'foreign'}])
        self.assertEqual(len(self.lab.creates),1)

    def test_corrupted_cached_source_cannot_be_regenerated(self):
        self.tick();value=self.state.get('recMain')
        Path(value['image_sources'][0]['path']).write_bytes(b'corrupt')
        self.tick()
        self.assertEqual(self.state.get('recMain')['stage'],'needs_review')
        self.assertEqual(len(self.lab.creates),1)

    def test_crash_after_durable_create_intent_never_creates_on_restart(self):
        original=material.State.save
        def crash(state):
            original(state)
            if any(v['stage']=='submitting' for v in state.data['items'].values()):
                raise SystemExit('Crash before task create')
        with patch.object(material.State,'save',crash), self.assertRaises(SystemExit):
            self.tick()
        self.runner=main.MainRunner(main.MainState(self.root,self.settings),self.settings,
            api_factory=lambda:API(self.backend),feishu=self.client,image_client_factory=lambda:self.lab)
        self.tick();self.tick()
        self.assertEqual(self.lab.creates,[])
        self.assertEqual(self.state.get('recMain')['stage'],'image_unknown')

    def test_crash_after_confirmed_attachment_reuses_its_token(self):
        self.image_row(2);self.tick()
        journal=self.runner._delivery_journal('recMain')
        def crash(path,value):
            save_private(path,value)
            if Path(path)==journal and value['files'][0]['upload_stage']=='accepted' and value['files'][1]['upload_stage']=='prepared':
                raise SystemExit('Crash after first attachment receipt')
        with patch('ua_workflows.replica_main_translation.save_private',side_effect=crash), self.assertRaises(SystemExit):
            self.tick()
        first=json.loads(journal.read_text())['files'][0]['token']
        with patch('ua_workflows.replica_main_translation.upload_attachment',side_effect=self.upload_language) as upload:
            self.tick()
        self.assertEqual(upload.call_count,1)
        self.assertEqual(self.state.get('recMain')['stage'],'verified')
        self.assertEqual(self.state.get('recMain')['attachment_receipts'][0]['token'],first)
        self.assertEqual(len(self.lab.creates),2)

    def test_crash_at_write_boundary_reads_without_replaying_write(self):
        self.tick()
        original=self.state.put
        def crash(rid,value):
            original(rid,value)
            if value['stage']=='write_intent':raise SystemExit('Crash before HTTP write')
        with patch.object(self.state,'put',side_effect=crash), self.assertRaises(SystemExit):
            self.tick()
        self.tick()
        self.assertEqual(self.state.get('recMain')['stage'],'write_unknown')
        self.assertFalse(any(c[1].endswith('/batch_update') for c in self.client.calls))
        self.assertEqual(len(self.lab.creates),1)

    def test_corrupted_downloaded_output_stops_before_attachment_upload(self):
        self.tick()
        with patch.object(self.runner,'_deliver_once',return_value=False):self.tick()
        value=self.state.get('recMain')
        self.assertEqual(value['stage'],'downloaded')
        Path(value['outputs'][0]['path']).write_bytes(b'corrupt')
        self.runner._deliver_once()
        self.assertEqual(self.state.get('recMain')['stage'],'needs_review')
        self.assertNotIn(main.RESULT,self.client.rows['recMain']['fields'])

    async def test_replica_auth_failure_does_not_block_images_in_mixed_listener(self):
        video=row(None);video['record_id']='recVideo';self.client.rows['recVideo']=video
        with patch.object(API,'authenticate',side_effect=ReplicaError('authentication_required','Replica expired')):
            self.start()
            await self.until(lambda:self.state.items and self.state.get('recMain')['stage']=='verified')
        self.assertIn(self.state.get('recVideo')['stage'],{'queued','source_ready'})
        self.assertEqual(self.backend.uploads,[])
        self.assertEqual(len(self.lab.creates),1)

    async def test_waiting_translation_admission_does_not_block_new_images(self):
        translation = row(None, '替换语言')
        translation['record_id'] = 'recTranslation'
        self.client.rows = {'recTranslation': translation, **self.client.rows}
        with patch.object(API, 'authenticate', side_effect=ReplicaError('authentication_required', 'Replica expired')):
            self.start()
            await self.until(lambda: 'recMain' in self.state.items and self.state.get('recMain')['stage'] == 'verified')
        self.assertNotIn('recTranslation', self.state.items)
        self.assertNotIn(main.EXECUTION, translation['fields'])
        self.assertEqual(self.backend.uploads, [])

    def test_expired_replica_at_video_download_keeps_image_delivery_running(self):
        video = row(None)
        video['record_id'] = 'recVideo'
        self.client.rows = {'recVideo': video, **self.client.rows}
        self.runner._admit()
        self.state.put('recVideo', {**self.state.get('recVideo'), 'stage': 'logo_ready', 'job_id': 'original-job'})
        with patch.object(API, 'authenticate', side_effect=ReplicaError('authentication_required', 'Replica expired')):
            self.tick()
            self.tick()
        value = self.state.get('recVideo')
        self.assertEqual((value['stage'], value['job_id']), ('logo_ready', 'original-job'))
        self.assertGreater(value['download_check_after'], self.runner.now())
        self.assertEqual(self.state.get('recMain')['stage'], 'verified')

    async def test_videolab_login_missing_does_not_stop_video_workflow(self):
        video=row(None);video['record_id']='recVideo';self.client.rows['recVideo']=video
        self.lab.identity_error=ReplicaError('authentication_required','Video Lab login required')
        self.start()
        await self.until(lambda:'recVideo' in self.state.items and self.state.get('recVideo')['stage']=='verified')
        self.assertEqual(self.backend.uploads,['job-source'])
        self.assertEqual(self.backend.logos,['job-source'])
        self.assertEqual(self.lab.creates,[])
        self.assertEqual(self.state.get('recMain')['stage'],'queued')

    def test_backend_identity_changing_between_checks_cannot_submit_under_new_account(self):
        original=self.lab.identity
        def changed():
            result=original()
            self.lab.account['project_id']='project-b'
            return result
        with patch.object(self.lab,'identity',side_effect=changed):self.tick()
        self.assertEqual(self.lab.creates,[])

    def test_temporary_videolab_auth_failure_waits_without_paid_submission(self):
        self.lab.identity_error=ReplicaError('authentication_required','Login required')
        self.tick();self.assertEqual(self.lab.creates,[])
        self.lab.identity_error=None;self.tick();self.tick()
        self.assertEqual(self.state.get('recMain')['stage'],'verified')
        self.assertEqual(len(self.lab.creates),1)

    def test_mixed_video_and_image_use_their_own_backends(self):
        video=row(None);video['record_id']='recVideo';self.client.rows['recVideo']=video
        # Control/submit both and feed each backend its own source bytes.
        original_stream=self.stream
        def stream(url,**kwargs):
            if url.endswith('source.mp4'):
                from unittest.mock import Mock
                response=Mock();response.iter_content.side_effect=lambda size:iter([b'src'])
                response.__enter__=Mock(return_value=response);response.__exit__=Mock(return_value=False)
                return response
            return original_stream(url,**kwargs)
        with patch.object(self.client.session,'get',side_effect=stream):
            for _ in range(8):self.tick()
        self.assertEqual(self.state.get('recMain')['stage'],'verified')
        self.assertEqual(self.state.get('recVideo')['stage'],'verified')
        self.assertEqual(self.backend.uploads,['job-source'])
        self.assertEqual(self.backend.logos,['job-source'])
        self.assertEqual(len(self.lab.creates),1)

    def test_pending_server_image_does_not_block_later_image_submission(self):
        second=copy.deepcopy(self.client.rows['recMain']);second['record_id']='recImage2'
        self.client.rows['recImage2']=second
        self.lab.status='pending';self.runner._admit()
        self.runner._upload_once()
        first=self.state.get('recMain');first['image_check_after']=0;self.state.put('recMain',first)
        self.runner._upload_once()
        self.assertEqual(len(self.lab.creates),2)
        self.assertEqual(self.state.get('recImage2')['stage'],'image_processing')

    def test_image_polling_does_not_starve_video_uploads(self):
        video=row(None);video['record_id']='recVideo';self.client.rows['recVideo']=video
        self.lab.status='pending';self.runner._admit()
        self.runner._upload_once()
        first=self.state.get('recMain');first['image_check_after']=0;self.state.put('recMain',first)
        self.runner._upload_once()
        self.assertEqual(self.backend.uploads,['job-source'])
        self.assertEqual(len(self.lab.creates),1)
