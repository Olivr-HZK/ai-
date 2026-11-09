"""Main-table video translation and shared attachment-vector delivery journals."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import time

from ua_workflows.replica_cli import download_many
from ua_workflows.replica_recovery import retry_delay
from ua_workflows.replica_request_pool import record
from ua_workflows.replica_studio_api import ReplicaError, digest, save_private
from ua_workflows.replica_translation import translate_many, language_state, download_translations, video_duration
from ua_workflows.social_chat_media_orientation import require_non_landscape
from ua_workflows.social_chat_replica import APP, check_record, upload_attachment
from ua_workflows.replica_studio_api import step_state

TRANSLATION_PARTS = ['text', 'voice']
TRANSLATION_TRIM = {'remove_cta': False, 'remove_gameplay': False}


def _same_attachments(row, field, files):
    actual=row['fields'].get(field) or []
    expected={(x['token'],x['name'],x['size']) for x in files if x.get('token')}
    return (len(expected)==len(files)==len(actual) and
            {(x.get('file_token'),x.get('name'),x.get('size')) for x in actual}==expected)


class MainTranslationMixin:
    def uses_translation(self,value):
        return (self.state.items[value['record_id']]['action']=='替换语言'
                and value.get('logo_prerequisite_complete') is True)

    def logo_required(self,rid):
        return not self.uses_translation(self.state.get(rid))

    def _finish_logo_prerequisite(self,rid):
        value=self.state.get(rid)
        if self.state.items[rid]['action']!='替换语言' or value.get('logo_prerequisite_complete') or value['stage']!='logo_ready':return
        job=self.api.job(value['job_id']);view=job.get('logo_replace_view') or {}
        if job.get('last_action')!='logo_replace' or step_state(job,'logo')!='ready' or not view.get('result_file'):
            raise ReplicaError('read_failed','Logo成品完成证据发生变化，未提交翻译')
        value.update(stage='analysis_ready',logo_prerequisite_complete=True,
                     prerequisite_logo_result_file=view['result_file'],prerequisite_logo_revision=job['revision'],
                     visual_review_flagged=bool(view.get('flagged')),visual_review_message=view.get('warning') or job.get('message'),
                     last_message='Logo已完成，正在核验翻译输入')
        self.state.put(rid,value)

    def prepare_translation(self,rid,item):
        value=self.state.get(rid)
        if not value.get('logo_prerequisite_complete'):return False
        if self.now()<value.get('translation_input_check_after',0):return False
        try:
            job=self.api.job(value['job_id']);view=job.get('logo_replace_view') or {}
            if (job.get('last_action')!='logo_replace' or step_state(job,'logo')!='ready'
                    or view.get('result_file')!=value['prerequisite_logo_result_file']):
                raise ReplicaError('invalid_media','翻译前的Logo结果或路线已变化，未提交翻译')
            proof=value.get('translation_input_proof') or {}
            selected=job.get('source_file')
            if (proof.get('revision')==job['revision'] and proof.get('matches_logo')
                    and proof.get('input')=='original' and selected==proof.get('selected_video_file')
                    and proof.get('duration_seconds')):
                return self._eligible(rid)
            downloaded=download_many(self.api,[job['id']],self.checkpoint,self.state.root/'translation-inputs')
            files=[v for v in downloaded['results'] if v.get('ok')]
            if len(files)!=1:
                failure=next((v for v in downloaded['results'] if not v.get('ok')), {})
                raise ReplicaError(failure.get('code') or 'invalid_media','Logo中间成品尚未核验',failure.get('http_status'))
            logo=files[0]
            # The translation action's input=original selects the full current
            # source, including Logo replacement. source_video is a different
            # story-only route and must not be selected by this workflow.
            source=job.get('source_file')
            if not isinstance(source,str) or not source:raise ReplicaError('invalid_media','任务未声明原视频文件')
            receipt=self.api.download(job['id'],{'name':Path(source).name,'bytes':logo['size'],
                'url':'/api/jobs/'+job['id']+'/files/'+source},self.state.root/'translation-input-proof'/job['id']/'source.mp4')
            if receipt['sha256']!=logo['sha256']:
                raise ReplicaError('invalid_media','当前翻译源不是换Logo成品，未提交翻译')
            proof={'revision':job['revision'],'selected_video_file':source,'logo_result_file':view['result_file'],
                   'sha256':logo['sha256'],'size':logo['size'],'matches_logo':True,
                   'input':'original','trim':dict(TRANSLATION_TRIM),'duration_seconds':video_duration(receipt['path'])}
            value.update(translation_input_proof=proof,logo_intermediate=logo)
            value.pop('error',None);value.pop('translation_input_check_after',None);self.state.put(rid,value)
            return self._eligible(rid)
        except ReplicaError as error:
            temporary=error.code in {'read_failed','rate_limited','result_unknown'} or (error.status or 0)>=500
            value.update(error=str(error))
            if temporary:
                count=value.get('translation_input_attempts',0)+1
                value.update(translation_input_attempts=count,translation_input_check_after=self.now()+retry_delay(count,error.retry_after))
            else:value['stage']='needs_review'
            self.state.put(rid,value)
            return False

    def submit_translation(self, job_id, item):
        value=next(v for v in self.state.all() if v.get('job_id')==job_id)
        prior=self.checkpoint.get('translations',job_id) or {}
        if prior.get('attempts'):
            return translate_many(self.api,[job_id],self.checkpoint,item['languages'],parts=TRANSLATION_PARTS,
                                  input_mode='original',trim=TRANSLATION_TRIM)
        if not self.uses_translation(value) or not (value.get('translation_input_proof') or {}).get('matches_logo'):
            return {'results':[{'id':job_id,'ok':False,'code':'invalid_translation','message':'必须先核验换Logo成品，再提交翻译'}]}
        job=self.api.job(job_id)
        proof=value['translation_input_proof'];selected=job.get('source_file')
        if selected!=proof['selected_video_file'] or proof.get('input')!='original' or not proof.get('duration_seconds'):
            return {'results':[{'id':job_id,'ok':False,'code':'invalid_translation','message':'实际翻译输入与已核验Logo成品不匹配，未提交'}]}
        source=(job.get('translation') or {}).get('source_language')
        if source in item['languages']:
            return {'results':[{'id':job_id,'ok':False,'code':'invalid_translation',
                                'message':'目标语言包含原视频语言，请选择不同语言；未提交翻译'}]}
        return translate_many(self.api,[job_id],self.checkpoint,item['languages'],parts=TRANSLATION_PARTS,
                              input_mode='original',trim=TRANSLATION_TRIM)

    def poll(self, record_ids=None):
        for rid in record_ids or list(self.state.items):
            value=self.state.get(rid);item=self.state.items[rid]
            if not self.uses_translation(value) or value['stage']=='uploaded':
                super().poll([rid]);self._finish_logo_prerequisite(rid);continue
            if value['stage'] not in {'logo_accepted','logo_submitting'}:
                continue
            job=self.api.job(value['job_id'])
            states={lang:language_state(job,lang,TRANSLATION_PARTS,input_mode='original',trim=TRANSLATION_TRIM)
                    for lang in item['languages']}
            value.update(last_status=job.get('status'),last_message=job.get('message'),translation_states=states)
            if all(s=='ready' for s in states.values()):
                value['stage']='logo_ready'
            elif any(s in {'error','review'} for s in states.values()) or job.get('status') in {'error','interrupted'}:
                value.update(stage='failed',error=job.get('error') or job.get('message') or '翻译任务没有可用结果')
            elif value['stage']=='logo_submitting':
                value['stage']='logo_unknown'
            self.state.put(rid,value)

    def download_all(self, record_ids=None):
        for rid in record_ids or list(self.state.items):
            item=self.state.items[rid];value=self.state.get(rid)
            if item['action']!='替换语言':
                super().download_all([rid]);continue
            if not self.uses_translation(value) or value['stage']!='logo_ready' or self.now()<value.get('download_check_after',0):continue
            result=download_translations(self.api,[value['job_id']],self.checkpoint,
                                         self.state.root/'results',item['languages'])
            videos=[v for v in result['results'] if v.get('kind')=='video']
            ready=[v for v in videos if v.get('ok')]
            mapping={v.get('language'):v for v in ready}
            if (len(mapping)==len(ready)==len(item['languages']) and set(mapping)==set(item['languages'])
                    and all(v.get('full_decode_passed') for v in ready)):
                expected=(value.get('translation_input_proof') or {}).get('duration_seconds')
                durations={}
                try:
                    if not expected:
                        raise ReplicaError('invalid_media','缺少完整翻译输入时长证据，未回填')
                    for lang in item['languages']:
                        actual=video_duration(mapping[lang]['path']);durations[lang]=actual
                        if abs(actual-expected)>0.25:
                            raise ReplicaError('translation_duration_mismatch',
                                f'{lang}翻译成片{actual:.3f}秒与完整输入{expected:.3f}秒不符，未回填、不自动重做')
                except ReplicaError as error:
                    value.update(stage='needs_review',error=str(error),translation_durations=durations)
                    self.state.put(rid,value);continue
                outputs=[]
                for lang in item['languages']:
                    receipt={k:mapping[lang][k] for k in ['path','name','size','sha256','full_decode_passed']}
                    receipt.update(language=lang,duration_seconds=durations[lang],
                                   name=Path(item['source_name']).stem+'_translation_'+lang+Path(receipt['name']).suffix)
                    outputs.append(receipt)
                value.update(stage='downloaded',outputs=outputs,delivery_kind='translation_multi')
                value.pop('error',None);value.pop('download_check_after',None)
            else:
                failure=next((v for v in videos if not v.get('ok')),None)
                code=failure.get('code') if failure else 'translation_outputs_mismatch'
                if code in {'authentication_required', 'account_changed', 'permission_denied'}:
                    raise ReplicaError(code, 'Replica translation download requires the original account')
                count=value.get('download_attempts',0)+1
                temporary=bool(failure and (code in {'read_failed','rate_limited'} or failure.get('retryable')
                                           or (failure.get('http_status') or 0)>=500))
                value.update(download_attempts=count,error=code)
                if temporary:value['download_check_after']=self.now()+retry_delay(count,failure.get('retry_after'))
                else:value['stage']='needs_review'
            self.state.put(rid,value)

    def deliver_records(self, values):
        for value in values:
            if self.state.items[value['record_id']]['action']=='替换语言':self._deliver_languages(value)
            else:super().deliver_records([value])

    def _delivery_journal(self,rid):
        folder = 'image-delivery' if self.state.items[rid].get('engine') == 'videolab' else 'translation-delivery'
        return self.state.root/folder/(rid+'.json')

    def check_delivery_record(self,row,item):
        check_record(row,item,self.state.scope)

    def _read_language_write(self,value,journal):
        if not self._eligible(value['record_id']):return
        row=record(self.feishu,self.route,value['record_id'])
        self.check_delivery_record(row,self.state.items[value['record_id']])
        if _same_attachments(row,self.state.scope['target_field'],journal['files']):
            value.update(stage='verified',attachment_receipts=journal['files'])
            value.pop('error',None)
        elif row['fields'].get(self.state.scope['target_field']):
            value.update(stage='needs_review',error='本行已有其他附件，未覆盖')
        else:
            value.update(stage='write_unknown',error='成品附件写回回执待核对，不重复发送写表请求',
                         write_check_after=self.now()+self.health.policy['reconcile_interval'])
        self.state.put(value['record_id'],value)

    def _deliver_languages(self,value):
        rid=value['record_id'];self.refresh_feishu()
        outputs=value['outputs'];path=self._delivery_journal(rid)
        signature=hashlib.sha256(json.dumps(outputs,sort_keys=True).encode()).hexdigest()
        if path.exists():
            journal=json.loads(path.read_text())
            if journal['signature']!=signature or journal['job_id']!=value['job_id']:
                raise ReplicaError('checkpoint_conflict','多附件交付断点不匹配')
        else:
            journal={'version':1,'record_id':rid,'job_id':value['job_id'],'signature':signature,
                     'files':[{**v,'upload_stage':'prepared'} for v in outputs],'write_stage':'prepared'}
            save_private(path,journal)
        if journal['write_stage']!='prepared':
            self._read_language_write(value,journal);return
        if not self._eligible(rid):return
        for f in journal['files']:
            local=Path(f['path'])
            if not (f.get('full_decode_passed') and local.is_file() and local.stat().st_size==f['size'] and digest(local)==f['sha256']):
                raise ReplicaError('invalid_media','本地成品校验失败，未上传附件')
            require_non_landscape(local,f['sha256'])
            if f['upload_stage']=='accepted':continue
            if f['upload_stage']!='prepared':
                value.update(stage='attachment_unknown',error='成品附件上传回执未知，不重复上传')
                self.state.put(rid,value);return
            f['upload_stage']='dispatching';save_private(path,journal)
            value.update(stage='feishu_upload_intent',delivery_kind=value.get('delivery_kind','translation_multi'));self.state.put(rid,value)
            try:
                token=upload_attachment(self.feishu,{'feishu':{'app_token':APP}},local,f['name'])
                if not isinstance(token,str) or not token:raise ValueError('Empty attachment token')
            except Exception as error:
                f.update(upload_stage='unknown',upload_error=type(error).__name__);save_private(path,journal)
                value.update(stage='attachment_unknown',error='成品附件上传回执未知，保留已核验成品，不自动重传')
                self.state.put(rid,value);return
            f.update(upload_stage='accepted',token=token);save_private(path,journal)
        value.update(stage='feishu_uploaded',attachment_receipts=journal['files']);self.state.put(rid,value)
        if not self._eligible(rid):return
        row=record(self.feishu,self.route,rid)
        self.check_delivery_record(row,self.state.items[rid])
        if row['fields'].get(self.state.scope['target_field']):
            self._read_language_write(value,journal);return
        journal.update(write_stage='dispatching',dispatched_at=time.time());save_private(path,journal)
        value.update(stage='write_intent');self.state.put(rid,value)
        try:
            self.feishu.call('POST',self.route+'/records/batch_update',json={'records':[
                {'record_id':rid,'fields':{self.state.scope['target_field']:[{'file_token':f['token']} for f in journal['files']]}}]})
        except Exception as error:
            journal.update(write_stage='unknown',write_error=type(error).__name__)
        else:journal['write_stage']='accepted'
        save_private(path,journal);self._read_language_write(value,journal)

    def can_reconcile(self,value):
        if value.get('delivery_kind') in {'translation_multi','image_multi'} and value['stage'] in {'write_intent','write_unknown'}:
            return False  # The legacy writer expects one token; use the vector journal below.
        return super().can_reconcile(value)

    def reconcile_unknown_logos(self):
        super().reconcile_unknown_logos()
        for rid in list(self.state.items):self._finish_logo_prerequisite(rid)

    def reconcile_writes(self):
        for value in self.state.all():
            if value.get('delivery_kind') not in {'translation_multi','image_multi'} or not self.record_available(value['record_id']):continue
            path=self._delivery_journal(value['record_id'])
            if not path.exists():continue
            journal=json.loads(path.read_text())
            if value['stage'] in {'write_intent','write_unknown'} and self.now()>=value.get('write_check_after',0):
                self._read_language_write(value,journal)
            elif value['stage']=='feishu_upload_intent':
                if any(f['upload_stage'] in {'dispatching','unknown'} for f in journal['files']):
                    value.update(stage='attachment_unknown',error='附件派发中断，未重放上传')
                else:value['stage']='downloaded'  # Only accepted/prepared attachment intents can continue.
                self.state.put(value['record_id'],value)
        super().reconcile_writes()
