"""Offline asynchronous lane tests; no Replica/Feishu network requests."""
import asyncio
from collections import Counter
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

import test_social_chat_replica as fixtures
from ua_workflows import social_chat_replica as flow
from ua_workflows.replica_recovery import RunHealth
from ua_workflows.replica_studio_api import ReplicaError, digest, save_private


class Backend:
    def __init__(self):
        self.lock = threading.RLock()
        self.uploads, self.logos, self.downloads, self.events = [], [], [], []
        self.analysis, self.errors, self.completed = set(), set(), set()
        self.unknown_logo = set()
        self.unknown_upload = set()
        self.upload_hold = None
        self.download_hold = None
        self.upload_started = threading.Event(); self.upload_release = threading.Event()
        self.download_started = threading.Event(); self.download_release = threading.Event()
        self.clients = []
        self.active_uploads = 0; self.max_uploads = 0
        self.active_downloads = 0; self.max_downloads = 0

    def factory(self):
        client = API(self)
        self.clients.append(client)
        return client


class API:
    account = 'account-a'
    def __init__(self, backend): self.backend = backend
    def authenticate(self): return {'account_id': self.account}
    def list_jobs(self): return []
    def upload(self, path, request_id):
        b = self.backend; job = 'job-' + path.stem
        with b.lock:
            b.active_uploads += 1; b.max_uploads = max(b.max_uploads, b.active_uploads)
        try:
            if job == b.upload_hold:
                b.upload_started.set()
                if not b.upload_release.wait(8): raise AssertionError('Upload test was not released')
            with b.lock:
                b.uploads.append(job); b.events.append(('upload', job))
            if job in b.unknown_upload:
                raise ReplicaError('result_unknown','Upload receipt lost')
            return {'request_id': request_id, 'ids': [job]}
        finally:
            with b.lock: b.active_uploads -= 1
    def upload_status(self, request_id): raise ReplicaError('result_unknown','Not confirmed',404)
    def smart_options(self): return {'use_defaults': True, 'logo_mode': 'smart'}
    def job(self, job):
        b = self.backend
        with b.lock:
            logo = job in b.logos
            done = job in b.completed and logo
            error = job in b.errors
            analysis = job in b.analysis
        return {'id':job,'revision':11 if logo else 10,'project_id':job,
                'last_action':'logo_replace' if logo else 'analyze',
                'status':'error' if error else 'uploaded' if done else 'replacing_logo' if logo else 'analysis_review' if analysis else 'analyzing',
                'message':'Test failure' if error else 'Cloud ready' if done else 'Cloud working',
                'step_states':{'analysis':'done' if analysis else 'pending'},
                'logo_replace_view':{'state':'replaced' if done else 'none','result_file':'logo/result.mp4' if done else None,
                                     'flagged':done,'warning':'Sample review reminder' if done else None}}
    def smart_logo(self, job, options):
        b = self.backend
        with b.lock:
            b.logos.append(job['id']); b.events.append(('logo',job['id']))
        if job['id'] in b.unknown_logo:
            raise ReplicaError('result_unknown','Accepted but response lost')
        return {'id':job['id'],'status':'queued','revision':11}
    def download_plan(self, ids):
        return {'projects':[{'files':[{'name':'result.mp4','bytes':6,'url':'/api/jobs/'+job+'/files/logo/result.mp4'} for job in ids]}]}
    def download(self, job, file, path):
        b = self.backend
        with b.lock:
            b.active_downloads += 1; b.max_downloads = max(b.max_downloads,b.active_downloads)
        try:
            if job == b.download_hold:
                b.download_started.set()
                if not b.download_release.wait(8): raise AssertionError('Download test was not released')
            path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'result')
            with b.lock: b.downloads.append(job); b.events.append(('download',job))
            return {'path':str(path),'name':'result.mp4','size':6,'sha256':digest(path)}
        finally:
            with b.lock: b.active_downloads -= 1


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory();self.root=Path(self.temp.name).resolve()
        self.rows=[fixtures.row('fast'),fixtures.row('slow')]
        save_private(self.root/'scope.json',flow.prepare_rows(self.rows,fixtures.SCHEMA,'2026-10-29'))
        self.state=flow.State(self.root)
        for row in self.rows:
            rid=row['record_id'];path=self.root/(rid+'.mp4');path.write_bytes(b'src')
            self.state.put(rid,{'record_id':rid,'stage':'source_ready','source_path':str(path),'source_sha256':digest(path),'source_bytes_verified':True})
        self.backend=Backend();self.client=fixtures.Feishu(self.rows)
        self.runner=flow.PipelineRunner(self.state,api_factory=self.backend.factory,feishu=self.client,poll_interval=.01)
        self.task=None
        self.patches=[patch('builtins.print'),patch('ua_workflows.replica_cli.decode_video'),
                      patch.object(flow,'upload_attachment',side_effect=lambda client,config,path,name:'token-'+path.parent.name)]
        for p in self.patches:p.start()

    async def asyncTearDown(self):
        self.backend.upload_release.set();self.backend.download_release.set()
        self.runner._request_stop()
        if self.task is not None and not self.task.done():
            await asyncio.wait_for(self.task,10)
        for p in reversed(self.patches):p.stop()
        self.temp.cleanup()

    def start(self):
        self.task=asyncio.create_task(self.runner.run_async());return self.task

    async def until(self, predicate, *, timeout=8):
        async def wait():
            while not predicate():
                if self.task is not None and self.task.done():
                    await self.task
                    self.fail('Pipeline ended before expected state')
                await asyncio.sleep(.005)
        await asyncio.wait_for(wait(),timeout)

    async def finish_slow(self):
        self.backend.analysis.add('job-slow');self.backend.completed.add('job-slow')
        self.runner._events['control'].set()
        return await asyncio.wait_for(self.task,8)

    async def test_fast_video_reaches_backfill_while_another_analysis_is_pending(self):
        self.backend.analysis.add('job-fast');self.backend.completed.add('job-fast')
        self.start()
        await self.until(lambda:self.state.get('fast')['stage']=='verified')
        self.assertEqual(self.state.get('slow')['stage'],'uploaded')
        self.assertEqual(self.backend.logos,['job-fast'])
        self.assertEqual(len(self.client.writes),1)
        self.assertTrue(self.state.get('fast')['visual_review_flagged'])
        result=await self.finish_slow()
        self.assertTrue(result['all_terminal'])
        self.assertEqual(result['scheduling'],'pipeline')
        self.assertEqual(Counter(self.backend.logos),{'job-fast':1,'job-slow':1})
        gate=self.root/'analysis-ready'/'fast.json'
        self.assertEqual(__import__('json').loads(gate.read_text())['gate_scope'],'selected_ready_records')
        self.assertFalse((self.root/'analysis-gate.json').exists())

    async def test_slow_upload_does_not_block_fast_analysis_logo_and_delivery(self):
        self.backend.analysis.add('job-fast');self.backend.completed.add('job-fast')
        self.backend.upload_hold='job-slow'
        self.start();await self.until(self.backend.upload_started.is_set)
        await self.until(lambda:self.state.get('fast')['stage']=='verified')
        self.assertEqual(self.backend.uploads,['job-fast'])
        self.backend.upload_release.set()
        result=await self.finish_slow()
        self.assertEqual(result['verified'],2)
        self.assertEqual(self.backend.max_uploads,1)
        self.assertEqual(len(self.backend.clients),3)

    async def test_slow_download_does_not_block_other_logo_submission(self):
        self.backend.analysis.add('job-fast');self.backend.completed.add('job-fast')
        self.backend.download_hold='job-fast'
        self.start();await self.until(self.backend.download_started.is_set)
        self.backend.analysis.add('job-slow');self.backend.completed.add('job-slow')
        self.runner._events['control'].set()
        await self.until(lambda:'job-slow' in self.backend.logos)
        self.assertEqual(self.backend.downloads,[])
        self.backend.download_release.set()
        result=await asyncio.wait_for(self.task,8)
        self.assertEqual(result['verified'],2)
        self.assertEqual(self.backend.max_downloads,1)

    async def test_active_attachment_upload_is_not_misclassified_by_reconciliation(self):
        self.backend.analysis.update({'job-fast','job-slow'});self.backend.completed.update(self.backend.analysis)
        entered=threading.Event();release=threading.Event()
        def media(client,config,path,name):
            if path.parent.name=='job-fast':
                entered.set()
                if not release.wait(8):raise AssertionError('Media test was not released')
            return 'token-'+path.parent.name
        with patch.object(flow,'upload_attachment',side_effect=media):
            try:
                self.start();await self.until(entered.is_set)
                await asyncio.to_thread(self.runner.reconcile_writes)
                self.assertEqual(self.state.get('fast')['stage'],'feishu_upload_intent')
            finally:release.set()
            result=await asyncio.wait_for(self.task,8)
        self.assertEqual(result['verified'],2)
        self.assertTrue(all(v['stage']=='verified' for v in self.state.all()))

    async def test_unknown_logo_response_is_reconciled_without_replaying_generation(self):
        self.backend.analysis.update({'job-fast','job-slow'});self.backend.completed.update(self.backend.analysis)
        self.backend.unknown_logo.add('job-fast')
        self.runner.health.policy['reconcile_interval']=.01
        result=await asyncio.wait_for(self.start(),8)
        self.assertEqual(result['verified'],2)
        self.assertEqual(Counter(self.backend.logos),{'job-fast':1,'job-slow':1})
        self.assertTrue(self.state.get('fast').get('recovered_logo_readonly'))

    async def test_unknown_upload_does_not_block_other_delivery_or_get_replayed(self):
        self.backend.unknown_upload.add('job-fast')
        self.backend.analysis.add('job-slow');self.backend.completed.add('job-slow')
        self.runner.health.policy.update(unknown_timeout=.08,reconcile_interval=.01)
        result=await asyncio.wait_for(self.start(),8)
        self.assertEqual(result['verified'],1)
        self.assertEqual(self.state.get('fast')['stage'],'upload_unknown')
        self.assertTrue(result['needs_attention'])
        self.assertNotEqual(result['batch_phase'],'complete')
        self.assertEqual(Counter(self.backend.uploads),{'job-fast':1,'job-slow':1})
        self.assertEqual(self.backend.logos,['job-slow'])

    async def test_pause_waits_for_inflight_upload_receipt_and_starts_no_next_upload(self):
        self.backend.upload_hold='job-fast'
        self.start();await self.until(self.backend.upload_started.is_set)
        save_private(self.root/'pause-requested',{'requested':True})
        self.runner._events['control'].set()
        await asyncio.sleep(.03)
        self.assertFalse(self.task.done())
        self.backend.upload_release.set()
        result=await asyncio.wait_for(self.task,8)
        self.assertTrue(result['pause_requested'])
        self.assertEqual(self.backend.uploads,['job-fast'])
        self.assertEqual(self.state.get('fast')['stage'],'uploaded')
        self.assertEqual(self.backend.logos,[])

    async def test_analysis_failure_does_not_block_other_record(self):
        self.backend.analysis.update({'job-fast','job-slow'});self.backend.completed.update(self.backend.analysis)
        self.backend.errors.add('job-slow')
        result=await asyncio.wait_for(self.start(),8)
        self.assertEqual(result['verified'],1)
        self.assertEqual(self.state.get('slow')['stage'],'failed')
        self.assertEqual(self.backend.logos,['job-fast'])

    async def test_resume_keeps_upload_ids_and_advances_confirmed_tasks_once(self):
        self.backend.upload_hold='job-fast'
        self.start();await self.until(self.backend.upload_started.is_set)
        save_private(self.root/'pause-requested',{'requested':True})
        self.runner._events['control'].set()
        self.backend.upload_release.set()
        await asyncio.wait_for(self.task,8)
        request_id=self.state.get('fast')['request_id']
        (self.root/'pause-requested').unlink()
        self.backend.analysis.update({'job-fast','job-slow'})
        self.backend.completed.update(self.backend.analysis)
        self.runner=flow.PipelineRunner(self.state,api_factory=self.backend.factory,feishu=self.client,poll_interval=.01)
        result=await asyncio.wait_for(self.start(),8)
        self.assertEqual(result['verified'],2)
        self.assertEqual(self.state.get('fast')['request_id'],request_id)
        self.assertEqual(Counter(self.backend.uploads),{'job-fast':1,'job-slow':1})
        self.assertEqual(Counter(self.backend.logos),{'job-fast':1,'job-slow':1})
        self.assertEqual(len(self.client.writes),2)

    async def test_auth_failure_waits_for_inflight_upload_without_new_submissions(self):
        self.backend.upload_hold='job-fast'
        self.start();await self.until(self.backend.upload_started.is_set)
        self.runner.api.account='account-b'
        # Real SDK authentication checks its pinned account. Simulate the same
        # error on the control lane while a POST is still waiting for a receipt.
        self.runner.api.authenticate=lambda: (_ for _ in ()).throw(ReplicaError('authentication_required','Login expired'))
        self.runner._events['control'].set()
        await self.until(self.runner._halt.is_set)
        self.assertFalse(self.task.done())
        self.backend.upload_release.set()
        with self.assertRaises(ReplicaError) as error:
            await asyncio.wait_for(self.task,8)
        self.assertEqual(error.exception.code,'authentication_required')
        self.assertEqual(self.backend.uploads,['job-fast'])
        self.assertEqual(self.state.get('fast')['stage'],'uploaded')
        self.assertEqual(self.backend.logos,[])

    async def test_all_uploads_continue_without_inflight_limit(self):
        # Extend the offline scope beyond the historical 15-task cap.
        rows=[fixtures.row('rec'+str(n)) for n in range(100)]
        save_private(self.root/'scope.json',flow.prepare_rows(rows,fixtures.SCHEMA,'2026-10-29'))
        state=flow.State(self.root)
        for row in rows:
            rid=row['record_id'];path=self.root/(rid+'.mp4');path.write_bytes(b'src')
            state.put(rid,{'record_id':rid,'stage':'source_ready','source_path':str(path),'source_sha256':digest(path)})
        self.state=state;self.runner=flow.PipelineRunner(state,api_factory=self.backend.factory,feishu=fixtures.Feishu(rows),poll_interval=.01)
        # This asserts admission beyond the old cap, not an 8-second disk benchmark.
        # Hosted CI fsyncs 100 checkpoints on shared storage; keep a bounded wait.
        self.start();await self.until(lambda:len(self.backend.uploads)==100, timeout=45)
        save_private(self.root/'pause-requested',{'requested':True});self.runner._events['control'].set()
        await asyncio.wait_for(self.task,8)
        self.assertEqual(len(set(self.backend.uploads)),100)
        self.assertEqual(self.backend.max_uploads,1)

    async def test_changed_lane_account_stops_before_source_submission(self):
        count=0
        def factory():
            nonlocal count
            count+=1;client=API(self.backend)
            if count>1:client.account='account-b'
            return client
        self.runner=flow.PipelineRunner(self.state,api_factory=factory,feishu=self.client,poll_interval=.01)
        with self.assertRaises(ReplicaError) as error:
            await asyncio.wait_for(self.start(),8)
        self.assertEqual(error.exception.code,'account_changed')
        self.assertEqual(self.backend.uploads,[])


class PipelinePolicyTests(unittest.TestCase):
    def test_pipeline_timeout_applies_to_each_task_stage_not_whole_batch_age(self):
        with tempfile.TemporaryDirectory() as directory,patch('builtins.print'):
            now=[1000]
            health=RunHealth(directory,{'stage_timeout':20,'stall_timeout':100},lambda:now[0])
            values=[{'record_id':'old','stage':'uploaded'},{'record_id':'new','stage':'uploaded'}]
            health.observe('pipeline',values)
            now[0]=1015;values[1]['stage']='logo_accepted';health.observe('pipeline',values)
            now[0]=1025;health.observe('pipeline',values)
            alerts=list(health.data['alerts'].values())
            self.assertTrue(any(a['code']=='record_stage_timeout' and a['record_id']=='old' for a in alerts))
            self.assertFalse(any(a['code']=='record_stage_timeout' and a['record_id']=='new' for a in alerts))
            self.assertFalse(any(a['code']=='stage_timeout' for a in alerts))


if __name__=='__main__':unittest.main()
