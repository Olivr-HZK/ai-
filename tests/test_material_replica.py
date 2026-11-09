"""No-network image/video routing, paid boundaries, recovery and actual image integrity."""
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import uuid

from PIL import Image

import test_replica_pipeline as video_fixtures
from ua_workflows import material_replica as flow
from ua_workflows.replica_images import MODEL, PROVIDER, WORKFLOW, ENDPOINT, VideoLab, image_identity
from ua_workflows.replica_studio_api import ReplicaError


class Lab:
    def __init__(self,state):
        self.state=state;self.calls=[];self.creates=0;self.downloads=0
        self.lost=False;self.recovered=None;self.status='completed';self.task_id=str(uuid.uuid4())
        self.output_size=(720,1280);self.wrong_task=False;self.unsafe_path=False
        self.unavailable=False;self.contracts=0
        self.account={'email':'fixture@example.invalid','profile':'default','endpoint':ENDPOINT,'project_id':'project-a'}
    def identity(self):return copy.deepcopy(self.account)
    def contract(self):
        self.contracts+=1
        if self.unavailable:raise ReplicaError('model_unavailable','Requested model unavailable')
        return {}
    def recover(self,request):
        self.calls.append(('recover',request));return self.recovered
    def call(self,*args,**kwargs):
        self.calls.append(args)
        if args[:2]==('task','create'):
            if self.unavailable:raise ReplicaError('VALIDATION_ERROR','Backend validation rejected requested model',request_not_applied=True)
            self.creates+=1
            disk=json.loads(self.state.path.read_text())
            intent=next(v for v in disk['items'].values() if v.get('stage')=='submitting')
            assert intent['submission_request_id']==args[args.index('--submission-request-id')+1]
            assert uuid.UUID(intent['submission_request_id'])
            params=json.loads(Path(args[args.index('--params-file')+1]).read_text())
            assert params['generation_config']['image']['provider']==PROVIDER
            assert params['generation_config']['image']['model']==MODEL
            assert params['task_config']['execution_count']==1
            if self.lost:raise ReplicaError('result_unknown','Lost task receipt')
            return {'task':{'id':self.task_id,'workflow_id':WORKFLOW,'status':'pending'}}
        if args[:2]==('task','get'):
            return {'task':{'id':str(uuid.uuid4()) if self.wrong_task else self.task_id,
                            'workflow_id':WORKFLOW,'status':self.status}}
        if args[:2]==('task','outputs'):
            return {'task':{'id':self.task_id,'workflow_id':WORKFLOW,'status':'completed'},'subtasks':[],
                    'outputs':[{'field':'edited_image_path','path':'outputs/../wrong.png' if self.unsafe_path else 'outputs/image.png'}]}
        if args[:2]==('media','download'):
            self.downloads+=1
            path=Path(args[args.index('--output')+1]);path.parent.mkdir(parents=True,exist_ok=True)
            Image.new('RGB',self.output_size,'red').save(path)
            return {'bytes':path.stat().st_size,'output':str(path)}
        raise AssertionError(args)


class Video(video_fixtures.API):
    def upload(self,path,request_id):
        response=super().upload(path,request_id);self.backend.analysis.update(response['ids']);return response
    def smart_logo(self,job,options):
        response=super().smart_logo(job,options);self.backend.completed.add(job['id']);return response


class MaterialTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.source=self.root/'ad.png';Image.new('RGB',(720,1280),'blue').save(self.source)
        self.reference=self.root/'brand.png';Image.new('RGB',(300,300),'white').save(self.reference)
        self.video=self.root/'ad.mp4';self.video.write_bytes(b'local original video')
        self.state=flow.State(self.root/'run');self.lab=Lab(self.state)
        self.backend=video_fixtures.Backend();self.video_api=Video(self.backend)
        self.decoding=patch('ua_workflows.replica_cli.decode_video');self.decoding.start();self.addCleanup(self.decoding.stop)
        self.addCleanup(self.temp.cleanup)

    def add(self,paths=None):
        self.state.add(flow.plan(paths or [self.source],reference=self.reference),self.root/'out','default')

    def tick(self):
        return flow.process(self.state,image_client=self.lab,video_factory=lambda:self.video_api)

    def item(self):return next(iter(self.state.data['items'].values()))

    def test_plan_is_local_and_selects_exact_backends_and_reference(self):
        with patch.object(VideoLab,'call',side_effect=AssertionError('No remote calls in plan')):
            items=flow.plan([self.source,self.video],reference=self.reference)
        self.assertEqual({i['engine'] for i in items.values()},{'videolab','replica'})
        image=next(v for v in items.values() if v['engine']=='videolab')
        self.assertEqual(image['params']['generation_config']['image'],{
            'provider':'model-api-router','model':'gpt-image-2','size':'__original_image__'})
        self.assertEqual(image['params']['input_data']['images'],[{'path':str(self.source.resolve())},{'path':str(self.reference.resolve())}])

    def test_image_submit_once_then_download_decode_and_resume_no_paid_repeat(self):
        self.add();self.tick();self.assertEqual(self.item()['stage'],'accepted')
        frozen=self.item()['submitted_params']['input_data']['images'][0]['path']
        self.assertNotEqual(frozen,str(self.source.resolve()))
        self.assertEqual(Path(frozen).read_bytes(),self.source.read_bytes())
        self.tick();self.assertEqual(self.item()['stage'],'downloaded')
        self.assertEqual((self.item()['output']['width'],self.item()['output']['height']),(720,1280))
        self.assertTrue(self.item()['visual_review_required'])
        self.tick();self.assertEqual(self.lab.creates,1);self.assertEqual(self.lab.downloads,1)
        restored=flow.State(self.state.root);self.assertEqual(restored.data,self.state.data)

    def test_mixed_batch_preserves_video_chain_and_no_image_upload_to_replica(self):
        self.add([self.source,self.video])
        for _ in range(5):self.tick()
        self.assertTrue(all(v['stage']=='downloaded' for v in self.state.data['items'].values()))
        self.assertEqual(self.backend.uploads,['job-ad']);self.assertEqual(self.backend.logos,['job-ad'])
        self.assertEqual(self.lab.creates,1)

    def test_corrupted_cached_result_is_not_reused_or_regenerated(self):
        self.add();self.tick();self.tick()
        Path(self.item()['output']['path']).write_bytes(b'corrupted')
        self.tick();self.assertEqual(self.item()['stage'],'needs_review')
        self.assertEqual(self.lab.creates,1);self.assertEqual(self.lab.downloads,1)

    def test_lost_image_receipt_only_reconciles_original_uuid(self):
        self.add();self.lab.lost=True;self.tick();self.assertEqual(self.item()['stage'],'unknown')
        original=self.item()['submission_request_id'];self.item()['check_after']=0
        self.tick();self.assertEqual(self.lab.creates,1);self.assertEqual(self.item()['stage'],'unknown')
        self.lab.recovered=self.lab.task_id;self.item()['check_after']=0;self.tick()
        self.assertEqual(self.item()['stage'],'downloaded');self.assertEqual(self.lab.creates,1)
        self.assertTrue(all(c[1]==original for c in self.lab.calls if c[0]=='recover'))

    def test_crash_with_intent_but_no_receipt_never_recreates(self):
        self.add();self.item().update(stage='submitting',submission_request_id=str(uuid.uuid4()));self.state.save()
        restored=flow.State(self.state.root)
        flow.process(restored,image_client=self.lab)
        self.assertEqual(self.lab.creates,0);self.assertEqual(next(iter(restored.data['items'].values()))['stage'],'unknown')

    def test_generation_failure_is_terminal_and_not_automatically_retried(self):
        self.add();self.tick();self.lab.status='failed';self.tick();self.tick()
        self.assertEqual(self.item()['stage'],'failed');self.assertEqual(self.lab.creates,1);self.assertEqual(self.lab.downloads,0)

    def test_explicit_preflight_rejection_is_failed_without_repeated_creation(self):
        self.add();original=self.lab.call
        def reject(*args,**kwargs):
            if args[:2]==('task','create'):
                self.lab.creates+=1
                raise ReplicaError('VALIDATION_ERROR','Rejected before create',request_not_applied=True)
            return original(*args,**kwargs)
        self.lab.call=reject;self.tick();self.tick()
        self.assertEqual(self.item()['stage'],'failed');self.assertEqual(self.lab.creates,1)

    def test_changed_source_or_prompt_cannot_start_another_paid_task(self):
        self.add();self.tick();Image.new('RGB',(720,1280),'green').save(self.source)
        self.tick();self.assertEqual(self.item()['stage'],'needs_review');self.assertEqual(self.lab.creates,1)
        with self.assertRaises(ReplicaError):
            self.state.add(flow.plan([self.source],reference=self.reference,prompt='Changed prompt'),self.root/'out','default')

    def test_reference_change_is_rejected_before_submission(self):
        self.add();Image.new('RGB',(300,300),'black').save(self.reference);self.tick()
        self.assertEqual(self.lab.creates,0);self.assertEqual(self.item()['stage'],'needs_review')

    def test_persist_failure_before_create_prevents_paid_request(self):
        self.add()
        original=self.state.save
        def fail_at_intent():
            if self.item()['stage']=='submitting':raise OSError('disk full')
            original()
        with patch.object(self.state,'save',side_effect=fail_at_intent):
            with self.assertRaises(OSError):self.tick()
        self.assertEqual(self.lab.creates,0)

    def test_account_or_project_change_blocks_only_that_backend_before_paid_create(self):
        self.add([self.source,self.video]);self.state.bind('videolab',self.lab.identity())
        self.lab.account['project_id']='another-project'
        for _ in range(5):self.tick()
        image=next(v for v in self.state.data['items'].values() if v['engine']=='videolab')
        video=next(v for v in self.state.data['items'].values() if v['engine']=='replica')
        self.assertEqual(image['stage'],'pending');self.assertEqual(video['stage'],'downloaded')
        self.assertEqual(self.lab.creates,0)

    def test_unavailable_requested_model_has_no_fallback(self):
        self.add();self.lab.unavailable=True;self.tick()
        self.assertEqual(self.lab.creates,0);self.assertEqual(self.item()['stage'],'failed')

    def test_wrong_task_id_or_path_is_never_downloaded(self):
        for unsafe in (False,True):
            with self.subTest(unsafe=unsafe):
                self.state=flow.State(self.root/('unsafe' if unsafe else 'foreign'))
                self.lab=Lab(self.state);self.add();self.tick()
                self.lab.unsafe_path=unsafe;self.lab.wrong_task=not unsafe;self.tick()
                self.assertEqual(self.lab.downloads,0)

    def test_wrong_dimensions_are_not_reported_as_replication_success(self):
        self.add();self.tick();self.lab.output_size=(72,128);self.tick()
        self.assertEqual(self.item()['stage'],'needs_review');self.assertNotIn('output',self.item())

    def test_restart_after_restoring_dimensions_reuses_verified_download(self):
        self.add()
        self.tick()
        self.lab.output_size = (720, 1272)
        save = self.state.save

        def interrupted_save():
            if self.item()['stage'] == 'downloaded':
                raise OSError('interrupted before committing result')
            save()

        with patch.object(self.state, 'save', side_effect=interrupted_save):
            with self.assertRaises(OSError):
                self.tick()
        self.state = flow.State(self.state.root)
        self.assertEqual(self.item()['stage'], 'downloading')
        raw = self.item()['verified_download']
        self.assertEqual(image_identity(raw['path']), raw)
        calls = list(self.lab.calls)
        self.tick()
        self.assertEqual(self.item()['stage'], 'downloaded')
        self.assertEqual((self.item()['output']['width'], self.item()['output']['height']), (720, 1280))
        self.assertEqual(self.lab.calls, calls)

    def test_existing_unverified_destination_is_not_overwritten(self):
        self.add();self.tick()
        dest=self.root/'out'/self.item()['key']/'ad_afizzy_replica.png';dest.parent.mkdir(parents=True);dest.write_bytes(b'keep me')
        self.tick();self.assertEqual(dest.read_bytes(),b'keep me');self.assertEqual(self.lab.downloads,0)
        self.assertEqual(self.item()['stage'],'needs_review')

    def test_video_processing_failure_never_submits_extra_analysis_or_retries(self):
        self.add([self.video]);self.tick();self.backend.errors.add('job-ad')
        self.tick();self.tick();self.assertEqual(self.item()['stage'],'failed')
        self.assertEqual(self.backend.uploads,['job-ad']);self.assertEqual(self.backend.logos,[])

    def test_invalid_image_bytes_and_playable_are_rejected_locally(self):
        broken=self.root/'broken.png';broken.write_bytes(b'not image')
        for path in (broken,self.root/'ad.html'):
            with self.assertRaises(ReplicaError):flow.plan([path],reference=self.reference)


class CLIContractTests(unittest.TestCase):
    def test_readonly_timeout_retries_once_but_create_is_never_replayed(self):
        calls=[]
        def runner(argv,**kwargs):
            calls.append(argv)
            if len(calls)==1 or 'create' in argv:
                return subprocess.CompletedProcess(argv,7,'','{"code":"TIMEOUT_ERROR","retryable":true}')
            return subprocess.CompletedProcess(argv,0,'{"email":"fixture@example.invalid"}','')
        client=VideoLab(runner=runner)
        with patch('ua_workflows.replica_images.time.sleep'):
            self.assertEqual(client.call('whoami')['email'],'fixture@example.invalid')
        self.assertEqual(len(calls),2);calls.clear()
        with self.assertRaises(ReplicaError):client.call('task','create')
        self.assertEqual(len(calls),1)

    def test_project_selection_is_not_classified_as_a_readonly_retry(self):
        calls=[]
        def runner(argv,**kwargs):
            calls.append(argv);return subprocess.CompletedProcess(argv,7,'','{"code":"TIMEOUT_ERROR","retryable":true}')
        with self.assertRaises(ReplicaError):VideoLab(runner=runner).call('project','use','project-a')
        self.assertEqual(len(calls),1)

    def test_wrapper_uses_argv_json_without_shell_or_raw_credentials(self):
        calls=[]
        def runner(argv,**kwargs):
            calls.append((argv,kwargs));return subprocess.CompletedProcess(argv,0,'{"email":"test@example.invalid"}','')
        result=VideoLab(runner=runner).call('whoami')
        self.assertEqual(result['email'],'test@example.invalid')
        argv,kwargs=calls[0];self.assertEqual(argv[:5],['video-lab','--profile','default','--endpoint',ENDPOINT])
        self.assertNotIn('shell',kwargs);self.assertNotIn('--token',argv)

    def test_only_exact_original_uuid_can_be_recovered(self):
        request=str(uuid.uuid4());task=str(uuid.uuid4());client=VideoLab()
        with patch.object(client,'call',return_value={'tasks':[
            {'task_id':task,'workflow_id':WORKFLOW,'submission_request_id':request,'endpoint':ENDPOINT},
            {'task_id':str(uuid.uuid4()),'workflow_id':WORKFLOW,'submission_request_id':str(uuid.uuid4()),'endpoint':ENDPOINT}]}):
            self.assertEqual(client.recover(request),task)

    def test_contract_cannot_silently_replace_gpt_image2_with_another_model(self):
        client=VideoLab()
        with patch.object(client,'call',return_value={'workflow_id':WORKFLOW,'availability':{'workflow_available':True},
            'model_options_by_provider':{PROVIDER:[{'value':'different-model','available':True}]}}):
            with self.assertRaises(ReplicaError):client.contract()

    def test_missing_local_receipt_checks_remote_original_uuid_without_replaying_create(self):
        client=VideoLab();request=str(uuid.uuid4());task=str(uuid.uuid4())
        with patch.object(client,'call',side_effect=[{'tasks':[]},{'tasks':[
            {'id':task,'workflow_id':WORKFLOW,'submission_request_id':request}]}]) as call:
            self.assertEqual(client.recover(request),task)
            self.assertEqual(call.call_args_list[-1].args[:2],('task','list'))
            self.assertFalse(any(c.args[:2]==('task','create') for c in call.call_args_list))
