"""Image replication through the user-authorized Video Lab CLI, never raw tokens."""
from __future__ import annotations

import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import time
from urllib.parse import urlsplit
import uuid

from PIL import Image, ImageOps

from ua_workflows.replica_studio_api import ReplicaError, digest, save_private
from ua_workflows.shared.config import PROJECT_ROOT

ENDPOINT = 'https://video-lab.upupup.today'
WORKFLOW = 'preset_image_editing'
PROVIDER = 'model-api-router'
MODEL = 'gpt-image-2'
REFERENCE = PROJECT_ROOT / 'skills/social-chat-browser-daily/assets/afizzy-approved-brand.png'
IMAGE_SUFFIXES = {'.png', '.jpg', '.jpeg', '.webp'}
PROMPT = ('图片1是需要编辑的竞品原图，图片2仅作为已认可的Afizzy应用图标和小写afizzy品牌字标参考。'
          '将图片1中的竞品应用图标、Logo、品牌名称和品牌字标统一替换为参考图中的Afizzy图标及afizzy名称，'
          '保留原标识的位置、大小、透视和整体版式。品牌图标旁边的竞品文字也必须替换，不能只换图标而保留竞品名称。'
          '正文及CTA中如有竞品品牌提及，仅将品牌词改为afizzy，保留其余文字、原语言、内容和样式。'
          '原人物、脸、姿势、服装、画风、场景、背景、构图和全部非品牌内容保持不变，保留商店徽标、署名及免责声明。'
          '不移植参考图中的人物、背景、文案或其他元素。检查全图不得残留竞品品牌名称或图标。'
          '如果原图没有可识别的竞品品牌标识，保持原图不变，不另外增加品牌标识。'
          '保持完整原图的比例和尺寸，不裁切、不增加人物。')


def image_identity(path):
    path = Path(path).expanduser().resolve()
    if path.suffix.lower() not in IMAGE_SUFFIXES or not path.is_file() or not 0 < path.stat().st_size <= 100*1024*1024:
        raise ReplicaError('invalid_source', 'Image must be a local PNG/JPEG/WebP of at most 100 MiB')
    before = digest(path)
    try:
        with Image.open(path) as image:
            if getattr(image, 'n_frames', 1) != 1:
                raise ValueError('Animated images are not supported')
            image.load()
            width, height = ImageOps.exif_transpose(image).size
            kind = image.format
    except (OSError, ValueError) as error:
        raise ReplicaError('invalid_media', 'Image could not be fully decoded') from error
    if digest(path) != before:
        raise ReplicaError('invalid_media', 'Image changed during inspection')
    return {'path': str(path), 'name': path.name, 'size': path.stat().st_size,
            'sha256': before, 'width': width, 'height': height, 'format': kind}


def image_params(source, reference, prompt=PROMPT):
    return {'input_data': {'images': [{'path': source['path']}, {'path': reference['path']}],
                           'prompt': {'mode': 'direct', 'text': prompt}},
            'generation_config': {'image': {'provider': PROVIDER, 'model': MODEL,
                                            'size': '__original_image__'}},
            'task_config': {'execution_count': 1}}


class VideoLab:
    def __init__(self, profile='default', runner=subprocess.run):
        self.profile, self.runner = profile, runner
        self._contract = None

    def call(self, *args, timeout=120, _attempt=0):
        readonly=(args==('whoami',) or args[:2] in {
            ('workflow','list'),('workflow','show'),('workflow','contract'),('workflow','authoring-contract'),('workflow','form'),
            ('project','current'),('project','list'),('task','get'),('task','outputs'),('task','list'),('task','recent')})
        try:
            result = self.runner(['video-lab', '--profile', self.profile, '--endpoint', ENDPOINT,
                                  *map(str,args), '--json'], capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired as error:
            if readonly and _attempt==0:
                time.sleep(1)
                return self.call(*args,timeout=timeout,_attempt=1)
            raise ReplicaError('read_failed', 'Video Lab CLI timed out; no automatic submission replay') from error
        except OSError as error:
            raise ReplicaError('dependency_missing', 'Install Video Lab CLI and complete video-lab login') from error
        if result.returncode:
            try:
                details = json.loads(result.stderr.strip().splitlines()[-1])
            except (ValueError, IndexError):
                details = {}
            code = str(details.get('code') or 'cli_error')
            if readonly and _attempt==0 and (code=='TIMEOUT_ERROR' or details.get('retryable') is True):
                time.sleep(1)
                return self.call(*args,timeout=timeout,_attempt=1)
            context=details.get('details') or {}
            not_created=(context.get('path') in {'/api/v1/tasks/preflight','/api/v1/tasks/validate'}
                         and context.get('status') in {400,401,403,404,409,422})
            raise ReplicaError(code, 'Video Lab CLI returned '+code, retryable=bool(details.get('retryable')),
                               request_not_applied=not_created)
        try:
            return json.loads(result.stdout)
        except ValueError as error:
            raise ReplicaError('result_unknown', 'Video Lab CLI returned no verifiable JSON receipt') from error

    def identity(self):
        identity = self.call('whoami')
        projects = self.call('project', 'current')
        project = projects.get('cliCurrentProjectId') or projects.get('currentProjectId')
        if not identity.get('email') or not project:
            raise ReplicaError('authentication_required', 'Login to Video Lab and select a project')
        return {'email': identity['email'], 'profile': self.profile, 'endpoint': ENDPOINT, 'project_id': project}

    def contract(self):
        if self._contract is not None:
            return self._contract
        contract = self.call('workflow', 'authoring-contract', WORKFLOW)
        models = (contract.get('model_options_by_provider') or {}).get(PROVIDER) or []
        if (contract.get('workflow_id') != WORKFLOW or not (contract.get('availability') or {}).get('workflow_available')
                or not any(m.get('value') == MODEL and m.get('available') is True for m in models)):
            raise ReplicaError('model_unavailable', 'Video Lab does not advertise the requested gpt-image-2 model; no fallback')
        self._contract=contract
        return contract

    def recover(self, request_id):
        # Local accepted ledger is authoritative for the original UUID. A miss
        # remains unknown; task-create is never used as a status query.
        recent = self.call('task', 'recent', '--limit', '100')
        rows = recent if isinstance(recent,list) else recent.get('tasks', recent.get('entries', []))
        matches = {x.get('task_id') for x in rows if x.get('submission_request_id') == request_id
                   and x.get('workflow_id') == WORKFLOW and x.get('endpoint') == ENDPOINT}
        matches.discard(None)
        if len(matches) > 1:
            raise ReplicaError('result_unknown', 'Multiple tasks claim the original submission UUID')
        if matches:return next(iter(matches))
        remote = self.call('task','list','--limit','100')
        rows = remote if isinstance(remote,list) else remote.get('tasks',[])
        matches={x.get('id') or x.get('task_id') for x in rows if x.get('submission_request_id')==request_id
                 and x.get('workflow_id')==WORKFLOW}
        matches.discard(None)
        if len(matches)>1:raise ReplicaError('result_unknown','Multiple remote tasks claim the original submission UUID')
        # A page without the UUID is not proof that creation never happened.
        return next(iter(matches)) if matches else None


def _snapshot_images(item, root):
    params=json.loads(json.dumps(item['params']))
    for i,identity in enumerate((item['source'],item['reference'])):
        target=Path(root)/'inputs'/(identity['sha256']+Path(identity['path']).suffix.lower())
        target.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        if not target.exists():
            temporary=target.with_name(target.name+'.'+uuid.uuid4().hex+'.tmp')
            try:
                with Path(identity['path']).open('rb') as source, os.fdopen(os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'wb') as output:
                    shutil.copyfileobj(source,output);output.flush();os.fsync(output.fileno())
                if temporary.stat().st_size!=identity['size'] or digest(temporary)!=identity['sha256']:
                    raise ReplicaError('checkpoint_conflict','Image changed while freezing the submission input')
                os.replace(temporary,target)
            finally:temporary.unlink(missing_ok=True)
        if target.stat().st_size!=identity['size'] or digest(target)!=identity['sha256']:
            raise ReplicaError('checkpoint_conflict','Frozen submission image identity changed')
        params['input_data']['images'][i]['path']=str(target)
    return params


def _task(response, task_id=None):
    task = response.get('task') or response
    tid = task.get('id') or task.get('task_id')
    try:
        uuid.UUID(tid)
    except (ValueError, TypeError, AttributeError) as error:
        raise ReplicaError('result_unknown', 'Task receipt has no valid task ID') from error
    if task_id and tid != task_id or task.get('workflow_id') not in {None, WORKFLOW}:
        raise ReplicaError('result_unknown', 'Video Lab task identity differs from the original request')
    return task, tid


def _output_path(response, task_id):
    task, _ = _task(response, task_id)
    if task.get('status') != 'completed':
        raise ReplicaError('invalid_media', 'Task outputs are not complete')
    outputs = list(response.get('outputs') or [])
    for subtask in response.get('subtasks') or []:
        if subtask.get('status') != 'completed':
            raise ReplicaError('invalid_media', 'An image subtask has not completed')
        outputs.extend(subtask.get('outputs') or [])
    paths = set()
    for output in outputs:
        path = output.get('path')
        if not isinstance(path,str):
            continue
        normalized = path.lstrip('/')
        parsed = urlsplit(path)
        if (parsed.scheme or parsed.netloc or parsed.query or '\\' in path or '%' in path
                or any(part in {'.','..'} for part in normalized.split('/'))
                or not normalized.startswith(('outputs/', 'uploads/temp/'))):
            raise ReplicaError('invalid_media', 'Output is not a safe managed Video Lab artifact')
        if PurePosixPath(path).suffix.lower() in IMAGE_SUFFIXES:
            paths.add(path)
    if len(paths) != 1:
        raise ReplicaError('invalid_media', 'Expected one unambiguous edited image artifact')
    return paths.pop()


def advance_image(item, client, root, output_dir, persist):
    """Advance one item once. persist must fsync before any create boundary."""
    stage = item['stage']
    if stage in {'failed','needs_review','downloaded'}:
        return
    if stage == 'pending':
        if image_identity(item['source']['path']) != item['source'] or image_identity(item['reference']['path']) != item['reference']:
            raise ReplicaError('checkpoint_conflict', 'Original image or brand reference changed; no new generation')
        # The official CLI validates the pinned provider/model before POSTing
        # a task. Do not gate creation on the slower discovery endpoints.
        params_file = Path(root)/'params'/(item['key']+'.json')
        submitted_params=_snapshot_images(item,root)
        save_private(params_file, submitted_params)
        item.update(stage='submitting', submission_request_id=str(uuid.uuid4()))
        item['submitted_params']=submitted_params
        persist()
        try:
            response = client.call('task','create','--workflow',WORKFLOW,'--params-file',params_file,
                                   '--submission-request-id',item['submission_request_id'],'--summary',timeout=240)
            task, tid = _task(response)
            item.update(stage='accepted',task_id=tid,receipt=response)
            persist()
        except BaseException as error:
            item.update(stage='failed' if getattr(error,'request_not_applied',False) else 'unknown',
                        error=getattr(error,'code',type(error).__name__))
            persist()
            raise
        return
    if stage in {'submitting','unknown'}:
        recovered = client.recover(item['submission_request_id'])
        if not recovered:
            item['stage']='unknown';persist();return
        item.update(stage='accepted',task_id=recovered,recovered_from_original_uuid=True)
        persist()
    response = client.call('task','get',item['task_id'])
    task, tid = _task(response,item['task_id'])
    item['last_status']=task.get('status')
    if task.get('status') in {'failed','cancelled','canceled','interrupted'}:
        item.update(stage='failed',error='Video Lab task '+task['status']);persist();return
    if task.get('status') != 'completed':
        persist();return
    outputs = client.call('task','outputs',tid,'--summary')
    remote = _output_path(outputs,tid)
    suffix = PurePosixPath(remote).suffix.lower()
    destination = Path(output_dir)/item['key']/(Path(item['source']['name']).stem+'_afizzy_replica'+suffix)
    if destination.exists():
        raise ReplicaError('invalid_media','Unverified destination already exists; it will not be overwritten')
    item.update(stage='downloading',artifact=remote,download_destination=str(destination))
    persist()
    receipt = client.call('media','download','--type','images','--path',remote,'--output',destination,'--timeout','5m',timeout=330)
    actual = image_identity(destination)
    if receipt.get('bytes') != actual['size']:
        raise ReplicaError('invalid_media','Downloaded image size differs from the CLI receipt')
    if (actual['width'],actual['height']) != (item['source']['width'],item['source']['height']):
        raise ReplicaError('invalid_media','Generated image dimensions differ from the original; review required')
    item.update(stage='downloaded',output=actual,download_receipt=receipt,
                image_decode_passed=True,visual_review_required=True)
    item.pop('error',None);persist()
