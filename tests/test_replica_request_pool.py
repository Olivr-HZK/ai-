"""Offline demand approval, routing and result isolation; no paid/live calls."""
import asyncio
import copy
import fcntl
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

import test_replica_pipeline as pipeline
from ua_workflows import replica_request_pool as pool


class Feishu:
    def __init__(self, rows):
        self.lock = threading.RLock()
        self.rows = {r['record_id']: copy.deepcopy(r) for r in rows}
        self.source = {'record_id': 'recSource', 'fields': {
            '目标日期': '2026-10-29', '素材类型': '视频', '广告主': 'Fixture', '文案语言': ['英语'],
            '竞品素材（图片/视频/Playable）去除男频': [{'file_token': 'original-token', 'name': 'source.mp4',
                                         'size': 3, 'url': 'https://fixture.invalid/source.mp4'}]}}
        self.writes = []
        stream = Mock()
        stream.iter_content.side_effect = lambda size: iter([b'src'])
        self.session = Mock()
        self.session.get.return_value.__enter__ = Mock(return_value=stream)
        self.session.get.return_value.__exit__ = Mock(return_value=False)

    def listing(self, route):
        if route.endswith('/fields'):
            return [{'field_name': '原素材附件', 'field_id': 'source-field', 'type': 17},
                    {'field_name': '复刻结果', 'field_id': 'result-field', 'type': 17},
                    {'field_name': '关联原素材', 'field_id': 'link-field', 'type': 18,
                     'property': {'table_id': pool.TABLE, 'multiple': False}}]
        if route.endswith('/records'):
            with self.lock:
                return [pool.daily.normalize_record_texts(r) for r in copy.deepcopy(list(self.rows.values()))]
        raise AssertionError(route)

    def call(self, method, route, **kwargs):
        source = '/tables/' + pool.TABLE + '/'
        if method != 'GET':
            if source in route:
                raise AssertionError('Source table must never be written')
            self.writes.append((method, route, copy.deepcopy(kwargs)))
        with self.lock:
            if method == 'GET':
                rid = route.rsplit('/', 1)[-1]
                return {'record': copy.deepcopy(self.source if rid == 'recSource' else self.rows[rid])}
            if method == 'PUT':
                rid = route.rsplit('/', 1)[-1]
                fields = copy.deepcopy(kwargs['json']['fields'])
                if '原素材附件' in fields:
                    fields['原素材附件'] = copy.deepcopy(self.source['fields']['竞品素材（图片/视频/Playable）去除男频'])
                self.rows[rid]['fields'].update(fields)
                return {'record': copy.deepcopy(self.rows[rid])}
            if method == 'POST' and route.endswith('/batch_update'):
                for item in kwargs['json']['records']:
                    fields = item['fields']
                    assert set(fields) == {'复刻结果'}
                    fields = copy.deepcopy(fields)
                    fields['复刻结果'][0].update(name='result.mp4', size=6)
                    self.rows[item['record_id']]['fields'].update(fields)
                return {}
        raise AssertionError((method, route))


class API(pipeline.API):
    def upload(self, path, request_id):
        value = super().upload(path, request_id)
        self.backend.analysis.update(value['ids'])
        return value

    def smart_logo(self, job, options):
        value = super().smart_logo(job, options)
        self.backend.completed.add(job['id'])
        return value

    def translation_settings(self):
        return {'available': True, 'enabled': ['en', 'ja', 'de']}

    def use_source_video(self, job):
        self.backend.routes.add(job['id'])
        return {**job, 'revision': 11, 'source_story': {'available': True, 'active': True}}

    def translate(self, job, options):
        self.backend.translations.append((job['id'], options['languages']))
        return {'id': job['id'], 'revision': 12, 'status': 'translating'}

    def job(self, job_id):
        value = super().job(job_id)
        value['source_story'] = {'available': True, 'active': job_id in self.backend.routes}
        value['translation'] = {'source_language': 'en', 'languages': {}}
        for job, languages in self.backend.translations:
            if job != job_id:
                continue
            value.update(last_action='translate', revision=12, status='uploaded')
            value['translation']['languages'].update({lang: {'status': 'completed', 'parts': ['voice'],
                                                             'video_file': 'result.mp4'} for lang in languages})
        return value

    def download_plan(self, ids, **kwargs):
        plan = super().download_plan(ids)
        if kwargs:
            for project in plan['projects']:
                project['project_id'] = ids[0]
        return plan


def demand(status='新建', action='替换logo'):
    return {'record_id': 'recDemand', 'fields': {'需求名称': 'Fixture', '关联原素材': ['recSource'],
                '需求状态': status, '需求类型': action, '目标语言': ['日语'] if action == '替换语言' else []}}


class PoolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.settings = {'request_table_id': 'tblPool', 'expected_account': 'account-a',
                         'fields': {'原素材附件': 'source-field', '复刻结果': 'result-field', '关联原素材': 'link-field'}}
        self.client = Feishu([demand()])
        self.backend = pipeline.Backend()
        self.backend.routes = set()
        self.backend.translations = []
        self.state = pool.PoolState(self.root, self.settings)
        self.runner = pool.RequestRunner(self.state, self.settings,
                         api_factory=lambda: API(self.backend), feishu=self.client, poll_interval=.01)
        self.task = None
        self.patches = [patch('builtins.print'), patch('ua_workflows.replica_cli.decode_video'),
                        patch('ua_workflows.replica_translation.decode_video'),
                        patch('ua_workflows.social_chat_media_orientation.require_non_landscape', return_value={'allowed': True}),
                        patch('ua_workflows.social_chat_replica.upload_attachment', return_value='result-token')]
        for p in self.patches:
            p.start()

    async def asyncTearDown(self):
        self.runner._request_stop()
        if self.task is not None and not self.task.done():
            await asyncio.wait_for(self.task, 8)
        for p in reversed(self.patches):
            p.stop()
        self.temp.cleanup()

    async def until(self, condition):
        async def wait():
            while not condition():
                if self.task is not None and self.task.done():
                    await self.task
                    self.fail('Worker stopped before expected result')
                await asyncio.sleep(.005)
        await asyncio.wait_for(wait(), 8)

    def start(self):
        self.task = asyncio.create_task(self.runner.run_async())

    async def test_all_nonapproved_statuses_create_no_upload_or_generation(self):
        for status in ['新建', '修改中', '已完成', '结果返回']:
            self.client.rows['recDemand']['fields']['需求状态'] = status
            self.runner._control_once()
            self.runner._upload_once()
        self.assertEqual(self.backend.uploads, [])
        self.assertEqual(self.backend.logos, [])
        self.assertEqual(self.state.items, {})

    async def test_enabled_source_entry_runs_before_manual_approval_admission(self):
        self.runner.settings['source_entry'] = {'enabled': True}
        with patch('ua_workflows.replica_request_entry.SourceEntry') as factory:
            self.runner._admit()
            self.runner._admit()
            factory.assert_called_once()
            self.assertEqual(factory.return_value.tick.call_count, 2)
        self.assertEqual(self.backend.uploads, [])
        self.assertEqual(self.backend.logos, [])
        self.assertEqual(self.state.items, {})

    async def test_manual_approval_drives_logo_and_returns_result_to_demand_only(self):
        original = copy.deepcopy(self.client.source)
        self.start()
        await asyncio.sleep(.03)
        self.assertEqual(self.backend.uploads, [])
        self.client.rows['recDemand']['fields']['需求状态'] = '已验收'
        self.runner._events['control'].set()
        await self.until(lambda: self.client.rows['recDemand']['fields'].get('需求状态') == '结果返回')
        self.assertEqual(self.state.get('recDemand')['stage'], 'verified')
        self.assertEqual(self.backend.logos, ['job-source'])
        self.assertEqual(self.client.source, original)
        self.assertEqual(self.client.rows['recDemand']['fields']['复刻结果'][0]['file_token'], 'result-token')
        self.assertTrue(all('/tables/tblPool/' in route for _, route, _ in self.client.writes))
        self.client.rows['recDemand']['fields']['需求状态'] = '已验收'
        self.runner._events['control'].set()
        await self.until(lambda: self.client.rows['recDemand']['fields'].get('需求状态') == '结果返回')
        self.assertEqual(len(self.backend.uploads), 1)
        self.assertEqual(len(self.backend.logos), 1)

    async def test_real_feishu_text_shaped_relation_preserves_ids_through_execution(self):
        row = demand('已验收')
        row['fields']['关联原素材'] = [{'type': 'text', 'text': 'Original material title',
                                       'record_ids': ['recSource'], 'table_id': pool.TABLE}]
        self.client.rows['recDemand'] = row
        self.start()
        await self.until(lambda: self.client.rows['recDemand']['fields'].get('需求状态') == '结果返回')
        self.assertEqual(self.backend.uploads, ['job-source'])
        self.assertEqual(self.state.items['recDemand']['source_record_id'], 'recSource')

    async def test_relation_display_text_never_replaces_missing_or_wrong_table_identity(self):
        self.assertEqual(pool.relation_ids([{'type': 'text', 'text': 'recSource'}]), [])
        self.assertEqual(pool.relation_ids([{'type': 'text', 'text': 'Title', 'record_ids': ['recSource'],
                                            'table_id': 'tblOther'}]), [])

    async def test_real_paginated_feishu_listing_retains_relation_ids_and_scalar_text(self):
        cell = [{'type': 'text', 'text': 'Display title', 'record_ids': ['recSource'], 'table_id': pool.TABLE}]
        client = pool.daily.Feishu.__new__(pool.daily.Feishu)
        client.call = Mock(side_effect=[
            {'items': [{'record_id': 'recOne', 'fields': {'关联原素材': cell,
                        '需求说明': [{'type': 'text', 'text': 'Plain text'}]}}], 'has_more': True, 'page_token': 'next'},
            {'items': [{'record_id': 'recTwo', 'fields': {'关联原素材': cell}}], 'has_more': False}])
        rows = client.listing('bitable/v1/apps/app/tables/tbl/records')
        self.assertEqual([pool.relation_ids(r['fields']['关联原素材']) for r in rows], [['recSource'], ['recSource']])
        self.assertEqual(rows[0]['fields']['需求说明'], 'Plain text')
        self.assertEqual(client.call.call_args_list[1].kwargs['params']['page_token'], 'next')

    async def test_translation_uses_supplied_language_without_logo_and_returns_to_pool(self):
        self.client.rows['recDemand'] = demand('已验收', '替换语言')
        self.start()
        await self.until(lambda: self.client.rows['recDemand']['fields'].get('需求状态') == '结果返回')
        self.assertEqual(self.backend.translations, [('job-source', ['ja'])])
        self.assertEqual(self.backend.logos, [])
        self.assertEqual(self.state.get('recDemand')['stage'], 'verified')

    async def test_translation_endpoint_denial_does_not_block_other_logo_demand(self):
        self.client.rows['recDemand'] = demand('已验收', '替换语言')
        other = demand('已验收')
        other['record_id'] = 'recLogo'
        self.client.rows['recLogo'] = other

        def upload(api, path, request_id):
            job = 'job-' + path.parent.name
            api.backend.uploads.append(job)
            api.backend.analysis.add(job)
            return {'request_id': request_id, 'ids': [job]}

        denied = pool.ReplicaError('permission_denied',
                    'Device-authorized request was denied: 命令行令牌不能使用这个接口', 403)
        with patch.object(API, 'upload', upload), patch.object(API, 'use_source_video', side_effect=denied) as route:
            self.start()
            await self.until(lambda: self.client.rows['recLogo']['fields'].get('需求状态') == '结果返回')
            self.assertFalse(self.task.done())
            self.assertEqual(self.state.get('recDemand')['stage'], 'needs_review')
            self.assertEqual(self.state.get('recDemand')['http_status'], 403)
            self.assertEqual(self.client.rows['recDemand']['fields']['执行进度'], '需人工处理')
            self.assertIn('命令行令牌不能使用这个接口', self.client.rows['recDemand']['fields']['执行说明'])
            self.assertEqual(self.backend.translations, [])
            self.assertEqual(self.backend.logos, ['job-recLogo'])
            await asyncio.sleep(.05)
            route.assert_called_once()
            self.assertEqual(len(self.backend.uploads), 2)

    async def test_generic_permission_or_auth_failure_stops_and_syncs_rejected_state(self):
        self.client.rows['recDemand'] = demand('已验收', '替换语言')
        self.runner._admit()
        for code, status, message in [('permission_denied', 403, 'Missing run scope'),
                                      ('authentication_required', 401, 'Token expired')]:
            self.state.put('recDemand', {'record_id': 'recDemand', 'stage': 'failed', 'error': message})
            with self.assertRaises(pool.ReplicaError) as caught:
                self.runner._check_request_submission('recDemand', {'results': [
                    {'code': code, 'http_status': status, 'message': message}]})
            self.assertEqual(caught.exception.code, code)
            self.assertEqual(self.client.rows['recDemand']['fields']['执行说明'], message)

    async def test_unavailable_logo_is_reported_without_post_or_watcher_exit(self):
        self.client.rows['recDemand'] = demand('已验收')
        with patch.object(API, 'smart_options', side_effect=pool.ReplicaError('unavailable', 'MPS is not configured')):
            self.start()
            await self.until(lambda: self.client.rows['recDemand']['fields'].get('执行进度') == '需人工处理')
            self.assertEqual(self.state.get('recDemand')['stage'], 'needs_review')
            self.assertIn('MPS is not configured', self.client.rows['recDemand']['fields']['执行说明'])
            self.assertEqual(self.backend.logos, [])
            self.assertEqual(self.runner.checkpoint.data.get('logos'), {})
            self.assertFalse(self.task.done())
            self.assertEqual(len(self.backend.uploads), 1)

    async def test_endpoint_denial_never_overwrites_unknown_business_intent(self):
        self.client.rows['recDemand'] = demand('已验收', '替换语言')
        self.runner._admit()
        self.state.put('recDemand', {'record_id': 'recDemand', 'stage': 'logo_unknown'})
        self.runner._check_request_submission('recDemand', {'results': [
            {'code': 'permission_denied', 'http_status': 403, 'message': '命令行令牌不能使用这个接口'}]})
        self.assertEqual(self.state.get('recDemand')['stage'], 'logo_unknown')

    async def test_person_replacement_is_honestly_blocked_without_paid_calls(self):
        self.client.rows['recDemand'] = demand('已验收', '替换人物')
        self.runner._control_once()
        self.assertEqual(self.client.rows['recDemand']['fields']['执行进度'], '需人工处理')
        self.assertEqual(self.client.rows['recDemand']['fields']['需求状态'], '已验收')
        self.assertIn('403', self.client.rows['recDemand']['fields']['执行说明'])
        self.assertEqual(self.backend.uploads, [])
        self.assertEqual(self.state.items, {})

    async def test_missing_language_does_not_upload(self):
        self.client.rows['recDemand'] = demand('已验收', '替换语言')
        self.client.rows['recDemand']['fields']['目标语言'] = []
        self.runner._control_once()
        self.assertEqual(self.state.items, {})
        self.assertEqual(self.backend.uploads, [])

    async def test_modified_approved_inputs_stop_before_upload(self):
        self.client.rows['recDemand'] = demand('已验收')
        self.runner._admit()
        self.client.rows['recDemand']['fields']['需求说明'] = 'Changed after approval'
        self.runner._upload_once()
        self.assertEqual(self.state.get('recDemand')['stage'], 'needs_review')
        self.assertEqual(self.backend.uploads, [])

    async def test_source_change_after_upload_blocks_next_paid_logo_action(self):
        self.client.rows['recDemand'] = demand('已验收')
        self.runner._admit()
        self.runner._upload_once()
        self.client.source['fields']['竞品素材（图片/视频/Playable）去除男频'][0]['file_token'] = 'changed'
        self.runner._control_once()
        self.assertEqual(self.state.get('recDemand')['stage'], 'needs_review')
        self.assertEqual(len(self.backend.uploads), 1)
        self.assertEqual(self.backend.logos, [])

    async def test_existing_remote_execution_id_without_local_proof_never_replays(self):
        self.client.rows['recDemand'] = demand('已验收')
        self.client.rows['recDemand']['fields']['执行标识'] = 'unknown-other-executor'
        self.runner._admit()
        self.assertEqual(self.state.items, {})
        self.assertEqual(self.backend.uploads, [])

    async def test_same_source_can_have_separate_demand_records(self):
        self.client.rows['recDemand'] = demand('已验收')
        second = demand('已验收', '替换语言')
        second['record_id'] = 'recOther'
        self.client.rows['recOther'] = second
        self.runner._admit()
        self.assertEqual(set(self.state.items), {'recDemand', 'recOther'})
        self.assertNotEqual(self.state.items['recDemand']['execution_id'], self.state.items['recOther']['execution_id'])

    async def test_second_watcher_stops_before_auth_or_worker_metadata(self):
        settings = {**self.settings, 'app_token': pool.APP, 'source_table_id': pool.TABLE,
                    'trigger_status': '已验收', 'watcher_enabled': True}
        path = self.root / 'config.json'
        path.write_text(json.dumps(settings))
        fd = os.open(self.root / 'watcher.lock', os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with patch.object(pool.ReplicaAPI, 'configured', side_effect=AssertionError('Must stop before auth')) as auth:
                pool.main(['watch', '--config', str(path), '--run-dir', str(self.root)])
            auth.assert_not_called()
            self.assertFalse((self.root / 'worker.json').exists())
        finally:
            os.close(fd)


if __name__ == '__main__':
    unittest.main()
