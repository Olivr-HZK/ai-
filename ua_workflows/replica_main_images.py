"""Main-table image requests reuse the material CLI and vector delivery journal."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import os
import re

import requests

from ua_workflows import material_replica as material
from ua_workflows.replica_images import IMAGE_SUFFIXES, REFERENCE, PROMPT, VideoLab, image_identity, image_params
from ua_workflows.replica_request_pool import DemandBlocked, record
from ua_workflows.replica_studio_api import ReplicaAPI, ReplicaError, save_private
from ua_workflows.social_chat_media_orientation import require_non_landscape, OrientationError
from ua_workflows.social_chat_replica import source_identity, TERMINAL
from ua_workflows.replica_recovery import UNKNOWN


def plan_images(row, source_field):
    fields = row['fields']
    if fields.get('复刻需求类型') != '替换logo':
        raise DemandBlocked('图片当前仅支持替换logo，不提交图片翻译或人物替换')
    files = fields.get(source_field) or []
    sources = []
    for attachment in files:
        try:
            identity = source_identity(attachment)
            name, size = identity['name'], identity['size']
            valid = (isinstance(name, str) and Path(name).name == name and '\\' not in name
                     and Path(name).suffix.lower() in IMAGE_SUFFIXES
                     and isinstance(identity['file_token'], str) and bool(identity['file_token'])
                     and type(size) is int and 0 < size <= 100 * 1024 * 1024)
        except (KeyError, TypeError):
            valid = False
        if not valid:
            raise DemandBlocked('图片原附件须为100MiB以内PNG/JPEG/WebP，未提交')
        sources.append(identity)
    if not sources or len({s['file_token'] for s in sources}) != len(sources):
        raise DemandBlocked('请提供明确且不重复的图片原附件')
    target = fields.get('目标日期')
    if not isinstance(target, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', target):
        raise DemandBlocked('原素材目标日期缺失或不明确')
    try:
        reference = image_identity(REFERENCE)
    except ReplicaError as error:
        raise DemandBlocked('图片品牌参考图不可用，未提交生成：'+str(error)) from error
    return {'record_id': row['record_id'], 'source_record_id': row['record_id'],
            'source_field_name': source_field, 'source': sources[0], 'sources': sources,
            'source_name': sources[0]['name'], 'target_date': target, 'media_type': '图片',
            'action': '替换logo', 'engine': 'videolab', 'languages': [], 'brand': 'Afizzy',
            'brand_reference': reference, 'image_prompt': PROMPT}


class DeferredReplica:
    """Pin the checkpoint locally; verify the real account before any Replica I/O.

    Image-only listening must not require a live Replica browser session.
    Each pipeline lane gets its own deferred transport, as before.
    """
    def __init__(self, factory, account):
        self.factory, self.account, self.client = factory, account, None

    def __getattr__(self, name):
        if self.client is None:
            client = self.factory()
            if client.authenticate()['account_id'] != self.account:
                raise ReplicaError('account_changed', 'Replica account differs from the main-table checkpoint')
            self.client = client
        return getattr(self.client, name)


class MainImagesMixin:
    def __init__(self, state, settings, *args, image_client_factory=None, **kwargs):
        factory = kwargs.pop('api_factory', None) or (lambda: ReplicaAPI.configured(
            auth_mode=settings.get('auth_mode'), expected_account=settings['expected_account'], confirm_paid=True))
        self.image_client_factory = image_client_factory or VideoLab
        kwargs['api_factory'] = lambda: DeferredReplica(factory, settings['expected_account'])
        self._video_check_after = 0
        self._video_upload_after = 0
        self._last_upload_kind = None
        super().__init__(state, settings, *args, **kwargs)

    def checkpoint_account(self):
        # This is an expected identity, not a claim that Replica authenticated.
        return self.settings['expected_account']

    def _clone_lane(self, role):
        lane = copy.copy(self)
        lane._role, lane.api = role, self.factory()
        if lane._owns_feishu:
            lane.feishu = None
        return lane

    def _is_image(self, rid):
        return self.state.items[rid].get('engine') == 'videolab'

    def _control_once(self):
        self._admit()
        self.reconcile_writes()
        videos = [v for v in self.state.all() if not self._is_image(v['record_id'])
                  and (v['stage'] not in TERMINAL or v['stage'] in UNKNOWN)]
        if videos and self.now() >= self._video_check_after:
            try:
                result = self._advance_requests()
                self.health.clear('replica_wait')
                return result
            except ReplicaError as error:
                if not self._replica_can_wait(error):
                    raise
                # Retain original video checkpoints while the image lane runs.
                self._video_check_after = self.now() + 60
                self.health.alert('replica_wait', message='Replica等待恢复；图片仍走Video Lab，不重放视频请求')
        self._sync_progress()
        self.observe('main_table')
        return True

    @staticmethod
    def _replica_can_wait(error):
        return error.code in {'authentication_required','permission_denied','read_failed',
                              'rate_limited','temporarily_rejected','unavailable'} or (error.status or 0) >= 500

    def _fetch_images(self, rid, value):
        item = self.state.items[rid]
        self.refresh_feishu()
        row = record(self.feishu, self.route, rid)
        self.check_delivery_record(row,item)
        if not self._eligible(rid):
            return False
        receipts = value.setdefault('image_sources', [])
        for index, (source, remote) in enumerate(zip(item['sources'], row['fields'][item['source_field_name']])):
            path = self.state.root / 'image-sources' / rid / str(index) / source['name']
            if index < len(receipts):
                saved = receipts[index]
                if image_identity(path) != saved:
                    raise ReplicaError('checkpoint_conflict', '已冻结图片原件改变，未重新生成')
            else:
                path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                partial = path.with_name(path.name + '.part')
                try:
                    size = 0
                    with self.feishu.session.get(remote['url'], stream=True, timeout=(20,120)) as response:
                        response.raise_for_status()
                        with partial.open('wb') as stream:
                            os.chmod(partial, 0o600)
                            for block in response.iter_content(1024*1024):
                                size += len(block)
                                if size > source['size']:
                                    raise ReplicaError('invalid_media', '图片下载超过原附件字节数')
                                stream.write(block)
                            stream.flush(); os.fsync(stream.fileno())
                    if size != source['size']:
                        raise ReplicaError('invalid_media', '图片下载字节数与原附件不一致')
                    partial.replace(path)
                finally:
                    partial.unlink(missing_ok=True)
                receipts.append(image_identity(path))
                self.state.put(rid, value)
            require_non_landscape(path, receipts[index]['sha256'])
        return self._eligible(rid)

    def _advance_images(self, rid, value):
        item = self.state.items[rid]
        if not self._fetch_images(rid, value):
            return
        root = self.state.root / 'image-runs' / rid
        with material.operation_lock(root/'state.json'):
            self._advance_image_run(rid, value, root)

    def _advance_image_run(self, rid, value, root):
        item = self.state.items[rid]
        image_state = material.State(root)
        if not image_state.path.exists():
            if image_identity(item['brand_reference']['path']) != item['brand_reference']:
                raise ReplicaError('checkpoint_conflict', '品牌参考图已改变，未提交图片生成')
            plans = material.plan([s['path'] for s in value['image_sources']],
                                  reference=item['brand_reference']['path'], prompt=item['image_prompt'])
            image_state.add(plans, root/'results', 'default')
        expected = {s['path']:s for s in value['image_sources']}
        values = list(image_state.data['items'].values())
        if (len(values) != len(expected) or {v['source']['path'] for v in values} != set(expected)
                or any(v['source'] != expected[v['source']['path']]
                       or v.get('reference') != item['brand_reference']
                       or v.get('params') != image_params(v['source'],item['brand_reference'],item['image_prompt'])
                       or v.get('engine') != 'videolab' for v in values)):
            raise ReplicaError('checkpoint_conflict','图片执行断点与原记录输入不匹配')
        image_state.add({}, root/'results', 'default')
        client = self.image_client_factory()
        identity = client.identity()
        with self._guard:
            pin = self.state.root/'image-account.json'
            if pin.exists() and json.loads(pin.read_text()) != identity:
                raise ReplicaError('account_changed', 'Video Lab账号或项目已变化，未提交新请求')
            save_private(pin, identity)
        image_state.bind('videolab', identity)
        material.process(image_state, image_client=client)
        values = list(image_state.data['items'].values())
        # The material CLI fsyncs every UUID before create and reconciles only it.
        value['image_tasks'] = [{k:v[k] for k in ('key','stage','task_id','submission_request_id','error') if k in v}
                                for v in values]
        ids = [v['task_id'] for v in values if v.get('task_id')]
        if ids:
            value['job_id'] = 'videolab:' + ','.join(ids)
        errors = [v.get('error','图片需复查') for v in values if v['stage'] in {'failed','needs_review'}]
        if errors:
            value.update(stage='failed' if any(v['stage']=='failed' for v in values) else 'needs_review',
                         error='；'.join(errors))
        elif all(v['stage'] == 'downloaded' for v in values):
            outputs = [{**v['output'], 'name':f"{index+1:03d}-"+v['output']['name'],
                        'full_decode_passed':v['image_decode_passed']} for index,v in enumerate(values)]
            value.update(stage='downloaded', outputs=outputs, delivery_kind='image_multi',
                         visual_review_required=True,
                         visual_review_message='图片技术核验通过，品牌替换效果仍需人工验收')
            value.pop('error', None)
        else:
            unknown = any(v['stage'] in {'submitting','unknown'} for v in values)
            value.update(stage='image_unknown' if unknown else 'image_processing',
                         image_check_after=self.now() + max(1,self.poll_interval),
                         last_message='Video Lab图片任务等待原请求结果' if unknown else 'Video Lab GPT Image 2处理中')
            if image_state.data.get('backend_errors'):
                value['error'] = 'Video Lab认证或查询暂不可用，保留原断点等待'
            elif unknown:
                value['error'] = '图片生成回执待核对，仅查询原UUID，不重复提交'
            else:
                value.pop('error', None)
        self.state.put(rid, value)

    def _upload_once(self):
        values = self.state.all()
        video_ready = any(not self._is_image(v['record_id']) and v['stage'] in {'queued','source_ready'}
                          and self.now() >= v.get('upload_check_after',0) for v in values)
        # Alternate new submissions across backends. Poll the least recently
        # checked image row, so slow queued tasks cannot starve later requests.
        if video_ready and self._last_upload_kind == 'image' and self._upload_video_once():
            return True
        images = sorted((v for v in values if self._is_image(v['record_id'])),
                        key=lambda v:(v['stage'] != 'queued',v.get('image_check_after',0)))
        for value in images:
            rid = value['record_id']
            if not self._is_image(rid) or self.now() < value.get('image_check_after',0):
                continue
            with self._claim(rid, {'queued','image_processing','image_unknown'}) as claimed:
                if claimed is None or not self._eligible(rid):
                    continue
                try:
                    self._advance_images(rid, claimed)
                except (ReplicaError, OrientationError, requests.RequestException, OSError) as error:
                    value = self.state.get(rid)
                    terminal = isinstance(error,OrientationError) or getattr(error,'code',None) in {
                        'invalid_media','invalid_source','checkpoint_conflict','account_changed'}
                    value.update(stage='needs_review' if terminal else value['stage'], error=str(error),
                                 image_check_after=self.now()+60)
                    self.state.put(rid,value)
                self.observe('images')
                self._last_upload_kind = 'image'
                return True
        return self._upload_video_once()

    def _upload_video_once(self):
        if self.now() < self._video_upload_after:
            return False
        try:
            worked = super()._upload_once()
            if worked:
                self._last_upload_kind = 'video'
            return worked
        except ReplicaError as error:
            if not self._replica_can_wait(error):
                raise
            self._video_upload_after = self.now()+60
            self.health.alert('replica_wait', message='Replica上传等待恢复；图片断点保持')
            return False

    def check_delivery_record(self,row,item):
        if item.get('engine') != 'videolab':
            return super().check_delivery_record(row,item)
        fields = row['fields']
        if (row['record_id'] != item['record_id'] or fields.get('素材类型') != '图片'
                or fields.get('目标日期') != item['target_date']
                or [source_identity(x) for x in fields.get(item['source_field_name'],[])] != item['sources']):
            raise ReplicaError('checkpoint_conflict','图片原记录、日期或附件已变化，未回填')

    def deliver_records(self, values):
        for value in values:
            if not self._is_image(value['record_id']):
                super().deliver_records([value]); continue
            for output in value['outputs']:
                try:
                    actual = image_identity(output['path'])
                    valid = all(actual[k] == output[k] for k in ('size','sha256','width','height'))
                except ReplicaError:
                    valid = False
                if not valid:
                    value.update(stage='needs_review',error='图片成品本地校验失败，未回填')
                    self.state.put(value['record_id'],value)
                    break
            else:
                self._deliver_languages(value)
