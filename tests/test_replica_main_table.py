"""Offline main-table-only execution; reject every other table/API mutation."""
import asyncio
import copy
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

import test_replica_pipeline as pipeline
from test_replica_request_pool import API as PoolAPI
from ua_workflows import replica_main_table as main
from ua_workflows.replica_studio_api import ReplicaError

TYPES = {main.SOURCE:17, main.RESULT:17, main.TYPE:3, main.LANGUAGE:4, main.PORTRAIT:17,
         main.STATUS:3, main.PROGRESS:3, main.MESSAGE:1, main.EXECUTION:1, main.JOB:1}
SCHEMA = [{'field_name':n, 'field_id':'field-'+str(i), 'type':t} for i,(n,t) in enumerate(TYPES.items())]


def row(status='新建', action='替换logo'):
    return {'record_id':'recMain', 'fields':{main.STATUS:status, main.TYPE:action,
        main.LANGUAGE:['日语'] if action == '替换语言' else [], '目标日期':'2026-10-29',
        '素材类型':'视频', '广告主':'Fixture', '文案语言':['英语'],
        main.SOURCE:[{'file_token':'original-token', 'name':'source.mp4', 'size':3,
                      'url':'https://fixture.invalid/source.mp4'}]}}


class Feishu:
    def __init__(self):
        self.rows = {'recMain':row()}; self.calls=[]; self.lock=threading.RLock();self.tokens={};self.lose_write=False
        stream=Mock(); stream.iter_content.side_effect=lambda size:iter([b'src'])
        self.session=Mock()
        self.session.get.return_value.__enter__=Mock(return_value=stream)
        self.session.get.return_value.__exit__=Mock(return_value=False)

    def listing(self, route):
        assert route == f'bitable/v1/apps/{main.APP}/tables/{main.TABLE}/fields', route
        self.calls.append(('GET',route,{})); return copy.deepcopy(SCHEMA)

    def call(self, method, route, **kwargs):
        assert route.startswith(f'bitable/v1/apps/{main.APP}/tables/{main.TABLE}/'), route
        self.calls.append((method,route,copy.deepcopy(kwargs)))
        with self.lock:
            if route.endswith('/records/search') and method == 'POST':
                assert kwargs['json']['filter'] == {'conjunction':'or','conditions':[
                    {'field_name':main.TYPE,'operator':'is','value':[action]} for action in main.ACTIONS]}
                return {'items':copy.deepcopy([r for r in self.rows.values() if r['fields'].get(main.TYPE) in main.ACTIONS])}
            rid=route.rsplit('/',1)[-1]
            if method == 'GET': return {'record':copy.deepcopy(self.rows[rid])}
            if method == 'PUT':
                fields=kwargs['json']['fields']
                assert set(fields) <= {main.STATUS,main.PROGRESS,main.MESSAGE,main.EXECUTION,main.JOB}
                self.rows[rid]['fields'].update(copy.deepcopy(fields))
                return {'record':copy.deepcopy(self.rows[rid])}
            if method == 'POST' and route.endswith('/batch_update'):
                if self.lose_write:raise TimeoutError('Write receipt lost')
                for r in kwargs['json']['records']:
                    assert set(r['fields']) == {main.RESULT}
                    value=copy.deepcopy(r['fields'])
                    for a in value[main.RESULT]:a.update(self.tokens.get(a['file_token'],{'name':'result.mp4','size':6}))
                    self.rows[r['record_id']]['fields'].update(value)
                return {}
        raise AssertionError((method,route))


class API(PoolAPI):
    def job(self,jid):
        value=super().job(jid);value['source_file']='source.mp4'
        for lang,row in value['translation']['languages'].items():
            row['video_file']='translations/'+lang+'/video.mp4'
            options=getattr(self.backend,'translation_options',{}).get(jid,{})
            row.update(parts=options.get('parts',['voice']),input=options.get('input','swap'),trim_removed=[])
        return value

    def smart_logo(self,job,options):
        if getattr(self.backend,'logo_hold',False) or getattr(self.backend,'logo_fail',False):
            value=pipeline.API.smart_logo(self,job,options)
            if getattr(self.backend,'logo_fail',False):self.backend.errors.add(job['id'])
            return value
        return super().smart_logo(job,options)

    def translate(self,job,options):
        self.backend.events.append(('translate',job['id']))
        if not hasattr(self.backend,'translation_options'):self.backend.translation_options={}
        self.backend.translation_options[job['id']]=copy.deepcopy(options)
        return super().translate(job,options)

    def download_plan(self,ids,**kwargs):
        if kwargs.get('step')=='translation':
            return {'projects':[{'project_id':ids[0],'files':[
                {'name':'result.mp4','bytes':6,'url':'/api/jobs/'+ids[0]+'/files/translations/'+lang+'/video.mp4'}
                for lang in kwargs['languages']]}]}
        return super().download_plan(ids,**kwargs)

    def download(self,jid,file,path):
        value=super().download(jid,file,path)
        if file['url'].endswith('/files/source.mp4'):
            self.backend.events.append(('verify-logo-input',jid))
            if getattr(self.backend,'wrong_source',False):
                path.write_bytes(b'wrong!')
                from ua_workflows.replica_studio_api import digest
                value['sha256']=digest(path)
        return value


class MainFixture(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.root=Path(self.temp.name).resolve()
        self.settings={'expected_account':'account-a','fields':{f['field_name']:f['field_id'] for f in SCHEMA}}
        self.client=Feishu(); self.backend=pipeline.Backend()
        self.backend.routes=set(); self.backend.translations=[]
        self.state=main.MainState(self.root,self.settings)
        self.runner=main.MainRunner(self.state,self.settings,api_factory=lambda:API(self.backend),feishu=self.client,poll_interval=.01)
        self.task=None
        self.patches=[patch('builtins.print'),patch('ua_workflows.replica_cli.decode_video'),
            patch('ua_workflows.replica_translation.decode_video'),
            patch('ua_workflows.social_chat_media_orientation.require_non_landscape',return_value={'allowed':True}),
            patch('ua_workflows.social_chat_replica.upload_attachment',return_value='result-token')]
        self.patches.extend([
            patch('ua_workflows.replica_main_translation.video_duration',return_value=28.4),
            patch('ua_workflows.replica_main_translation.require_non_landscape',return_value={'allowed':True}),
            patch('ua_workflows.replica_main_translation.upload_attachment',side_effect=self.upload_language)])
        for p in self.patches:p.start()

    def upload_language(self,client,config,path,name):
        token='token-'+name;self.client.tokens[token]={'name':name,'size':path.stat().st_size};return token

    async def asyncTearDown(self):
        self.runner._request_stop()
        if self.task is not None: await asyncio.wait_for(self.task,8)
        for p in reversed(self.patches):p.stop()
        self.temp.cleanup()

    async def until(self,predicate):
        async def wait():
            while not predicate():
                if self.task.done():await self.task;self.fail('Worker stopped')
                await asyncio.sleep(.005)
        await asyncio.wait_for(wait(),8)

    def start(self):self.task=asyncio.create_task(self.runner.run_async())


class MainTests(MainFixture):
    async def test_no_request_never_starts_regardless_of_hidden_status(self):
        with patch('ua_workflows.replica_request_entry.SourceEntry') as creator:
            for status in [None,'新建','修改中','已完成','已验收','结果返回']:
                self.client.rows['recMain']['fields'][main.STATUS]=status
                self.client.rows['recMain']['fields'].pop(main.TYPE,None)
                self.runner._control_once();self.runner._upload_once()
            creator.assert_not_called()
        self.assertEqual(self.state.items,{})
        self.assertEqual(self.backend.uploads,[])

    async def test_logo_full_roundtrip_same_row_no_pool_no_duplicates(self):
        self.client.rows['recMain']=row(None); original=copy.deepcopy(self.client.rows['recMain']['fields'])
        self.start();await self.until(lambda:self.client.rows['recMain']['fields'][main.STATUS]=='结果返回')
        f=self.client.rows['recMain']['fields']
        self.assertEqual(f[main.RESULT][0]['file_token'],'result-token')
        for name in [main.SOURCE,main.TYPE,main.LANGUAGE,'目标日期','素材类型','广告主']:
            self.assertEqual(f[name],original[name])
        self.assertEqual(self.state.get('recMain')['stage'],'verified')
        self.assertEqual(list(self.client.rows),['recMain'])
        self.assertEqual(self.backend.logos,['job-source'])
        f[main.STATUS]='已验收';self.runner._events['control'].set()
        await self.until(lambda:f[main.STATUS]=='结果返回')
        self.assertEqual(len(self.backend.uploads),1);self.assertEqual(len(self.backend.logos),1)
        self.assertNotIn('request_table_id',self.state.scope)

    async def test_other_types_never_admitted_or_submitted(self):
        for action in ['替换人物']:
            self.client.rows['recMain']=row('已验收',action)
            # Server-side query excludes these; stale candidates cannot authorize admission.
            with patch.object(self.runner,'requested_rows',return_value=[self.client.rows['recMain']]):
                self.runner._control_once();self.runner._upload_once()
            self.assertNotIn(main.PROGRESS,self.client.rows['recMain']['fields'])
            with self.assertRaises(main.DemandBlocked):main.plan(self.client.rows['recMain'])
        self.assertEqual(self.state.items,{})
        self.assertEqual(self.backend.uploads,[]);self.assertEqual(self.backend.translations,[])
        self.assertEqual(self.backend.routes,set());self.assertEqual(self.backend.logos,[])

    async def test_existing_output_is_preserved_and_not_submitted(self):
        r=row('已验收');r['fields'][main.RESULT]=[{'file_token':'historic'}];self.client.rows['recMain']=r
        self.runner._control_once();self.runner._upload_once()
        self.assertEqual(self.backend.uploads,[]);self.assertEqual(r['fields'][main.RESULT],[{'file_token':'historic'}])
        self.assertNotIn(main.PROGRESS,r['fields'])

    async def test_mutated_source_or_input_or_new_result_blocks_before_upload(self):
        for mutation in [lambda f:f[main.SOURCE][0].update(file_token='changed'),
                         lambda f:f.pop(main.TYPE),
                         lambda f:f.update({main.RESULT:[{'file_token':'external'}]})]:
            with self.subTest(mutation=mutation):
                self.client.rows['recMain']=row('已验收');self.state.items.clear();self.state.scope['items']=[]
                path=self.root/'admissions/recMain.json';path.unlink(missing_ok=True)
                self.runner._admit();mutation(self.client.rows['recMain']['fields']);self.runner._upload_once()
                self.assertEqual(self.state.get('recMain')['stage'],'needs_review')
        self.assertEqual(self.backend.uploads,[])

    async def test_missing_checkpoint_never_reuses_foreign_execution_id(self):
        self.client.rows['recMain']=row('已验收');self.client.rows['recMain']['fields'][main.EXECUTION]='foreign'
        self.runner._admit();self.assertEqual(self.state.items,{})
        self.assertEqual(self.client.rows['recMain']['fields'][main.EXECUTION],'foreign')

    async def test_wrong_schema_and_temporary_query_failures(self):
        bad=copy.deepcopy(SCHEMA);bad[0]['field_id']='changed'
        with self.assertRaises(ValueError):self.runner.validate_destination_fields(bad)
        with patch.object(self.client,'call',side_effect=ValueError('code=1254607')):
            with self.assertRaises(ReplicaError) as found:self.runner.requested_rows()
            self.assertEqual(found.exception.code,'read_failed')

    async def test_query_pagination_is_read_only_and_main_table_only(self):
        with patch.object(self.client,'call',side_effect=[{'items':[row('已验收')],'has_more':True,'page_token':'next'},
                                                        {'items':[],'has_more':False}]) as call:
            self.assertEqual(len(self.runner.requested_rows()),1)
            self.assertEqual(call.call_args_list[1].kwargs['params']['page_token'],'next')

    async def test_hidden_status_does_not_gate_request_or_upload(self):
        for status in [None,'新建','修改中','已完成','已验收','结果返回']:
            with self.subTest(status=status):
                r=row(status);self.client.rows['recMain']=r
                self.assertEqual(main.plan(r)['action'],'替换logo')
        self.runner._admit()
        self.client.rows['recMain']['fields'][main.STATUS]='修改中'
        self.assertTrue(self.runner._eligible('recMain'))
        self.runner._upload_once()
        self.assertEqual(len(self.backend.uploads),1)


if __name__=='__main__':unittest.main()
