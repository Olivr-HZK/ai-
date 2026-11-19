"""需求池执行器（母版共享版）：一条需求一条流水线，只有 Logo 母版跨需求复用。

- 需求状态「已验收」才会执行；其余状态只读跳过。
- 母版：主表有 `复刻母版` 就直接用，没有就换一次并写回主表。
- 替换logo：母版就是成品。
- 替换语言：在母版任务上提交翻译（text+voice、input=original），不重新上传。
- 替换人物：用母版文件新建任务 → 分析 → 参考图 → 换人 → 接回母版 CTA。
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import subprocess
import sys
import time
import uuid
from urllib.parse import quote
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ua_workflows.social_chat_browser_daily import Feishu  # noqa: E402
from ua_workflows.social_chat_mixed_delivery import upload  # noqa: E402
from ua_workflows.replica_studio_api import ReplicaAPI, media_url  # noqa: E402
from ua_workflows.replica_cli import Checkpoint  # noqa: E402
from ua_workflows.replica_translation import translate_many  # noqa: E402

APP = 'Adz4bj9ECaCCJrsgkj7coheYn3c'
MAIN = 'tblwhAqPfQoBXK3Q'
POOL = 'tblKXqqPOYqk3XPd'
ROOT = Path('data/replica-pool-run')
# launchd 的 PATH 很窄，ffmpeg/ffprobe 通常在 Homebrew 目录
class PlatformRejected(RuntimeError):
    """平台明确拒绝/生成失败：直接判失败，不自动重试。"""


ENV = {**os.environ, 'PATH': '/opt/homebrew/bin:/usr/local/bin:' + os.environ.get('PATH', '/usr/bin:/bin:/usr/sbin:/sbin')}
PORTRAIT = Path('data/replica-cast-50-20261116/portraits/01.png')
LANG = {'日语': 'ja', '韩语': 'ko', '英语': 'en', '中文': 'zh', '德语': 'de', '法语': 'fr',
        '西班牙语': 'es', '葡萄牙语': 'pt', '意大利语': 'it', '俄语': 'ru', '印尼语': 'id', '阿拉伯语': 'ar'}


def text(value):
    if value is None:
        return ''
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        return '、'.join(x.get('text', '') if isinstance(x, dict) else str(x) for x in value).strip()
    if isinstance(value, dict):
        return (value.get('text') or value.get('name') or '').strip()
    return str(value).strip()


def link_ids(value):
    if isinstance(value, dict):
        return [x for x in (value.get('link_record_ids') or []) if isinstance(x, str)]
    if isinstance(value, list):
        ids = []
        for row in value:
            if isinstance(row, dict):
                ids.extend(x for x in (row.get('record_ids') or []) if isinstance(x, str))
                if isinstance(row.get('record_id'), str):
                    ids.append(row['record_id'])
        return ids
    return []

def fingerprint(fields):
    """同一素材/同一原视频 + 同类型 + 同语言 + 同角色 + 同一张参考图 = 完全相同的需求。"""
    links = link_ids(fields.get('关联原素材'))
    videos = fields.get('原视频') or []
    refs = fields.get('人物参考图') or []
    lang = text(fields.get('目标语言（单选）')) or text(fields.get('目标语言'))
    ref = tuple((a.get('name'), a.get('size')) for a in refs)
    source = links[0] if len(links) == 1 else (
        f"video:{videos[0].get('name')}:{videos[0].get('size')}" if len(videos) == 1 else '')
    return (
        source,
        text(fields.get('需求类型')),
        lang,
        text(fields.get('换人角色')),
        ref,
    )


class Runner:
    def __init__(self, run_dir: Path, feishu: Feishu, api: ReplicaAPI, auth, dry: bool, auth_mode=None):
        self.auth_mode = auth_mode
        self.dir = run_dir
        self.dir.mkdir(parents=True, exist_ok=True)
        self.feishu = feishu
        self.api = api
        self.auth = auth
        self.dry = dry
        self.ck = Checkpoint(self.dir / 'checkpoint.json', auth['account_id'])

    # ---------- 基础 ----------
    def cli(self, *args, timeout=1800, checkpoint=None):
        mode = ['--auth-mode', self.auth_mode] if self.auth_mode else []
        path = str(checkpoint or self.ck.path)
        cmd = ['./scripts/replica-studio', *mode, '--checkpoint', path, *args]
        readonly = args[0] in {'status', 'characters', 'download'}
        attempts = 3 if readonly else 1
        last = None
        for attempt in range(attempts):
            out = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=ENV)
            if out.returncode == 0:
                try:
                    body = json.loads(out.stdout.strip().splitlines()[-1])
                except Exception:
                    body = None
                if body is not None and not (body.get('ok') is False and body.get('code') in {'read_failed', 'result_unknown'}):
                    return body
                last = RuntimeError(f'CLI 失败 {args[:2]}: {str(body)[:300]}')
            else:
                last = RuntimeError(f'CLI 失败 {args[:2]}: {out.stdout[-300:]} {out.stderr[-300:]}')
            if attempt + 1 < attempts:
                time.sleep(15)  # 只读命令遇到读失败/网络抖动时重试
        raise last

    def record(self, table, rid):
        return self.feishu.call('GET', f'bitable/v1/apps/{APP}/tables/{table}/records/{rid}',
                                params={'user_id_type': 'open_id'})['record']['fields']

    def put(self, table, rid, fields):
        self.feishu.call('PUT', f'bitable/v1/apps/{APP}/tables/{table}/records/{rid}',
                         params={'user_id_type': 'open_id'}, json={'fields': fields})

    def download(self, att, dest: Path):
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists() and dest.stat().st_size == att.get('size'):
            return dest
        with self.feishu.session.get(att['url'], timeout=(15, 300), stream=True) as resp:
            resp.raise_for_status()
            with dest.open('wb') as fh:
                for chunk in resp.iter_content(1 << 20):
                    fh.write(chunk)
        if dest.stat().st_size != att['size']:
            raise RuntimeError('下载字节数不符')
        return dest

    def attach(self, path: Path, name=None):
        token = upload(self.feishu, {'feishu': {'app_token': APP}}, path, name or path.name)
        return [{'file_token': token}]

    # ---------- 母版 ----------
    def ensure_master(self, source_id):
        src = self.record(MAIN, source_id)
        atts = src.get('竞品素材（图片/视频/Playable）去除男频') or []
        if not atts:
            raise RuntimeError('素材没有原附件')
        source = atts[0]
        master_atts = src.get('复刻母版') or []
        job_id = text(src.get('母版任务ID'))
        story_end = src.get('剧情切点')
        if master_atts and text(src.get('母版状态')) == '已换好' and job_id:
            local = self.dir / 'masters' / (hashlib.sha256(source['file_token'].encode()).hexdigest()[:16] + '.mp4')
            path = self.download(master_atts[0], local)
            return {'path': path, 'job_id': job_id, 'story_end': story_end, 'reused': True}
        if self.dry:
            return {'path': None, 'job_id': None, 'story_end': None, 'reused': False}
        src_path = self.download(source, self.dir / 'sources' / source['name'])
        up = self.cli('upload', str(src_path), '--yes')
        job = up['results'][0]['ids'][0]
        self.cli('status', job, '--wait-for', 'analysis', '--timeout', '1200', '--interval', '6')
        self.cli('logo', job, '--yes')
        self.cli('status', job, '--wait-for', 'logo', '--timeout', '2400', '--interval', '12')
        got = self.cli('download', job, '--step', 'logo', '--output-dir', str(self.dir / 'masters'))
        master = Path(got['results'][0]['path'])
        view = self.api.job(job).get('logo_replace_view') or {}
        finish = ((view.get('report') or {}).get('finish') or {})
        self.put(MAIN, source_id, {'复刻母版': self.attach(master), '母版任务ID': job,
                                   '剧情切点': finish.get('story_s'), '母版状态': '已换好'})
        return {'path': master, 'job_id': job, 'story_end': finish.get('story_s'), 'reused': False}

    # ---------- 需求 ----------
    def ensure_job(self, master):
        """直接上传的视频还没有任务：上传并等分析，返回任务 ID。"""
        if master.get('job_id'):
            return master['job_id']
        up = self.cli('upload', str(master['path']), '--yes')
        job = up['results'][0]['ids'][0]
        self.cli('status', job, '--wait-for', 'analysis', '--timeout', '1500', '--interval', '6')
        master['job_id'] = job
        return job

    def portrait_ready(self, job_id, source_id, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            for person in (self.api.job(job_id).get('cast_members') or []):
                if person.get('source_id') == source_id and person.get('portrait_file'):
                    return True
            time.sleep(5)
        return False

    def translate(self, job_id, lang, mode='original'):
        trim = {'remove_cta': True, 'remove_gameplay': False} if mode == 'original' else None
        result = translate_many(self.api, [job_id], self.ck, [lang], parts=('text', 'voice'),
                                input_mode=mode, trim=trim)
        res = result['results'][0]
        if not res.get('ok'):
            raise RuntimeError('翻译未提交：' + str(res.get('code')))
        for _ in range(240):
            ko = ((self.api.job(job_id).get('translation') or {}).get('languages') or {}).get(lang) or {}
            if ko.get('status') == 'completed':
                break
            if ko.get('status') in {'failed', 'error'}:
                raise RuntimeError('翻译失败')
            time.sleep(20)
        else:
            raise RuntimeError('翻译超时')
        got = self.cli('download', job_id, '--step', 'translation', '--languages', lang,
                       '--output-dir', str(self.dir / 'translations'))
        return Path(got['results'][0]['path'])

    def job_state(self, key):
        path = self.dir / 'jobs' / f'{key}.json'
        return json.loads(path.read_text()) if path.exists() else {}

    def save_job(self, key, value):
        path = self.dir / 'jobs' / f'{key}.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, ensure_ascii=False, indent=1))

    @contextmanager
    def video_lock(self, key):
        path = self.dir / 'locks' / f'{key}.lock'
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('a') as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def swap(self, master, tag, portraits):
        """同一视频共用一个任务：第一次分析+换人，之后换参考图并 force_new 重新生成。"""
        key = hashlib.sha256(Path(master['path']).read_bytes()).hexdigest()[:16]
        with self.video_lock(key):
            cache = self.job_state(key)
            job, force = cache.get('job_id'), bool(cache.get('swaps'))
            if job:
                state = self.api.job(job)
                if state.get('status') in {'error', 'interrupted'}:
                    raise PlatformRejected(str(state.get('error') or state.get('message') or '任务失败')[:250])
            if not job:
                up = self.cli('upload', str(master['path']), '--yes', checkpoint=self.dir / 'ck' / f'{key}.json')
                job = up['results'][0]['ids'][0]
                self.cli('status', job, '--wait-for', 'analysis', '--timeout', '1500', '--interval', '6')
                cache = {'job_id': job, 'swaps': 0}
                self.save_job(key, cache)
            cast = self.cli('characters', job)['characters']
            if len(portraits) != len(cast):
                raise RuntimeError(f'视频识别出 {len(cast)} 个角色，需要 {len(cast)} 张参考图（当前 {len(portraits)} 张）')
            own = self.dir / 'ck' / f'{tag}.json'
            own.parent.mkdir(parents=True, exist_ok=True)
            for person, portrait in zip(cast, portraits):
                source_id = person['source_id']
                try:
                    self.cli('portrait', job, str(portrait), '--source-id', source_id, '--replace', checkpoint=own)
                except RuntimeError as error:
                    if 'result_unknown' not in str(error):
                        raise
                    if not self.portrait_ready(job, source_id):
                        self.cli('portrait', job, str(portrait), '--source-id', source_id, '--replace',
                                 checkpoint=own.with_suffix(f'.retry-{source_id}.json'))
                    if not self.portrait_ready(job, source_id):
                        raise RuntimeError(f'{source_id} 参考图上传未生效')
            try:
                self.cli('swap', job, '--yes', *(['--force-new'] if force else []), checkpoint=own)
                self.cli('status', job, '--wait-for', 'swap', '--timeout', '3600', '--interval', '15')
            except RuntimeError as error:
                state = self.api.job(job)
                if state.get('status') in {'error', 'interrupted'}:
                    raise PlatformRejected(str(state.get('error') or state.get('message') or error)[:250])
                raise
            state = self.api.job(job)
            seg = (state.get('segments') or [])[0]
            story_end = (state.get('source_story') or {}).get('story_end_s') or seg.get('end_s')
            src_lang = ((state.get('translation') or {}).get('source_language') or '')
            url = media_url(f'/api/jobs/{job}/files/{quote(seg["video_file"])}?download=1', job)
            out = self.dir / 'swaps' / f'{tag}-raw.mp4'
            out.parent.mkdir(parents=True, exist_ok=True)
            with self.api.session.get(url, stream=True, timeout=(15, 300), allow_redirects=False) as r:
                r.raise_for_status()
                out.write_bytes(r.content)
            cache['swaps'] = int(cache.get('swaps') or 0) + 1
            self.save_job(key, cache)
            return job, out, story_end, src_lang

    def assemble(self, segment: Path, second: Path, tag, second_start=None):
        import subprocess as sp
        final = self.dir / 'finals' / f'{tag}.mp4'
        final.parent.mkdir(parents=True, exist_ok=True)
        graph = ("[0:v]fps=30,scale=480:854,setsar=1,setpts=PTS-STARTPTS,format=yuv420p[v0];"
                 "[1:v]fps=30,scale=480:854,setsar=1,setpts=PTS-STARTPTS,format=yuv420p[v1];"
                 "[0:a]aresample=48000,asetpts=PTS-STARTPTS[a0];[1:a]aresample=48000,asetpts=PTS-STARTPTS[a1];"
                 "[v0][a0][v1][a1]concat=n=2:v=1:a=1[v][a]")
        tail = [] if second_start is None else ['-ss', str(second_start)]
        sp.run(['ffmpeg', '-hide_banner', '-loglevel', 'error', '-i', str(segment),
                *tail, '-i', str(second), '-filter_complex', graph,
                '-map', '[v]', '-map', '[a]', '-r', '30', '-fps_mode', 'cfr',
                '-c:v', 'libx264', '-crf', '19', '-preset', 'medium', '-pix_fmt', 'yuv420p',
                '-c:a', 'aac', '-b:a', '192k', '-movflags', '+faststart', str(final), '-y'], check=True)
        sp.run(['ffmpeg', '-v', 'error', '-i', str(final), '-f', 'null', '-'], check=True, env=ENV)
        return final


    def source_sig(self, source_id):
        """素材附件也要一致：素材换了原片就不算同一个需求。"""
        src = self.record(MAIN, source_id)
        atts = src.get('竞品素材（图片/视频/Playable）去除男频') or []
        return (atts[0].get('name'), atts[0].get('size')) if atts else ('', 0)

    def duplicate(self, fields):
        """找一条已完成、且参数完全相同的需求，返回 (record_id, 复刻结果)。"""
        key = fingerprint(fields)
        if not key[0]:
            return None
        mine = None if key[0].startswith('video:') else self.source_sig(key[0])
        rows = self.feishu.call('POST', f'bitable/v1/apps/{APP}/tables/{POOL}/records/search',
                                params={'page_size': 500, 'user_id_type': 'open_id'}, json={}).get('items', [])
        for row in rows:
            other = row['fields']
            if text(other.get('复刻状态')) != '已完成':
                continue
            if not (other.get('复刻结果') or []):
                continue
            if fingerprint(other) == key and (mine is None or self.source_sig(key[0]) == mine):
                return row['record_id'], other['复刻结果']
        return None


    # ---------- 主流程 ----------
    def run_row(self, row):
        rid, f = row['record_id'], row['fields']
        name = text(f.get('需求名称'))
        kind = text(f.get('需求类型'))
        raw = f.get('目标语言') or []
        if isinstance(raw, str):
            raw = [raw]
        langs = [LANG[x] for x in raw if x in LANG]
        if not langs:
            one = text(f.get('目标语言（单选）'))
            langs = [LANG[one]] if one in LANG else []
        lang = langs[0] if langs else ''
        role = text(f.get('换人角色')) or (text(f.get('人物参考图')[0].get('name')).rsplit('.', 1)[0]
                                            if f.get('人物参考图') else '')
        links = link_ids(f.get('关联原素材'))
        videos = f.get('原视频') or []
        refs = f.get('人物参考图') or []
        # 先把输入校验干净，任何付费动作之前就停
        problems = []
        if not links and not videos:
            problems.append('必须关联素材或上传原视频（二选一）')
        if videos and len(videos) != 1:
            problems.append(f'原视频只能上传 1 个（当前 {len(videos)} 个）')
        if links and len(links) != 1 and not videos:
            problems.append('关联素材必须恰好一条')
        if kind not in {'替换logo', '替换语言', '替换人物'}:
            problems.append('需求类型为空或不受支持')
        if kind == '替换语言' and not lang:
            problems.append('替换语言必须选一个目标语言')
        if kind == '替换人物' and len(langs) > 1 and not all(langs):
            problems.append('目标语言有无法识别的选项')
        if kind == '替换人物':
            # 换人角色只是标签：参考图才是真正的主体，没填就用品名兜底
            if not refs:
                problems.append('替换人物必须上传人物参考图（几张由视频里的角色数决定）')
        if problems:
            if not self.dry:
                self.put(POOL, rid, {'复刻状态': '待补充信息', '执行说明': '；'.join(problems)})
            print('  [blocked]', name, '|', '；'.join(problems))
            return
        same = self.duplicate(f)
        if same:
            other, result = same
            if not self.dry:
                self.put(POOL, rid, {'复刻结果': result, '复刻状态': '已完成',
                                     '执行说明': f'与 {other} 需求完全相同，直接复用那条的成品；如需重做请新建需求并说明'})
            print('  [duplicate]', name, '->', other)
            return
        direct = bool(videos)
        source_id = links[0] if links else ''
        plan = {'需求': name, '类型': kind, '语言': lang, '角色': role,
                '素材': ('原视频：' + videos[0]['name']) if direct else source_id,
                '参考图': refs[0]['name'] if refs else ''}
        if direct:
            if not self.dry:
                self.put(POOL, rid, {'复刻状态': '换人中'})
            source_video = (None if self.dry else
                            self.download(videos[0], self.dir / 'sources' / videos[0]['name']))
            master = {'path': source_video, 'job_id': None, 'story_end': None, 'reused': False}
            plan['母版'] = '不适用（直接上传视频）'
        else:
            if not self.dry:
                self.put(POOL, rid, {'复刻状态': '换Logo中'})
            master = self.ensure_master(source_id)
            plan['母版'] = '复用' if master['reused'] else '新建'
        if self.dry:
            print('  [plan]', json.dumps(plan, ensure_ascii=False))
            return
        self.put(POOL, rid, {'复刻状态': '翻译中' if kind == '替换语言' else ('换人中' if kind == '替换人物' else '已完成')})
        if direct:
            src_lang = ''
        else:
            src = self.record(MAIN, source_id)
            src_lang = LANG.get(text((src.get('文案语言') or [''])[0]) if isinstance(src.get('文案语言'), list) else text(src.get('文案语言')), '')
        tag = rid[-8:]
        if kind == '替换logo':
            final, note = master['path'], '母版即成品'
        elif kind == '替换语言':
            job = self.ensure_job(master)
            if not src_lang:
                src_lang = ((self.api.job(job).get('translation') or {}).get('source_language') or '')
            if lang == src_lang:
                final, note = master['path'], '目标语言=源语言，视频本身即成品'
            else:
                story = self.translate(job, lang)
                self.put(POOL, rid, {'复刻状态': '拼接中'})
                cta, cta_lang = self.cta_for(lang)
                final = self.assemble(story, cta, tag)
                where = '原视频' if direct else '母版'
                note = f'{where}任务 {job[:8]}… 翻译（text+voice，去 CTA）+ {cta_lang} CTA'
        elif kind == '替换人物':
            portraits = [self.download(att, self.dir / 'portraits' / att['name']) for att in refs]
            swap_job, seg, story_end, job_src = self.swap(master, tag, portraits)
            src_lang = src_lang or job_src
            self.put(POOL, rid, {'复刻状态': '拼接中'})
            targets = langs or [src_lang]
            finals, notes = [], []
            for code in dict.fromkeys(targets):
                if code == src_lang:
                    if direct:
                        out = self.assemble(seg, master['path'], f'{tag}-{code}', second_start=story_end)
                        notes.append(f'{code}（片段+原视频片尾）')
                    else:
                        cta, _ = self.cta_for(src_lang)
                        out = self.assemble(seg, cta, f'{tag}-{code}')
                        notes.append(f'{code}（源语言片段+CTA）')
                else:
                    story = self.translate(swap_job, code, mode='swap')
                    cta, cta_lang = self.cta_for(code)
                    out = self.assemble(story, cta, f'{tag}-{code}')
                    notes.append(f'{code}（翻译+{cta_lang} CTA）')
                finals.append((code, out))
            note = f'换人任务 {swap_job[:8]}…（{role}）；' + '；'.join(notes)
            self.put(POOL, rid, {'复刻结果': [a for code, out in finals for a in self.attach(out, f'{name}_{code}.mp4')],
                                 '复刻状态': '已完成', '执行说明': note})
            print('  done', name, '->', [str(o) for _, o in finals])
            return
        else:
            raise RuntimeError('不支持的需求类型')
        self.put(POOL, rid, {'复刻结果': self.attach(final, f'{name}.mp4'), '复刻状态': '已完成', '执行说明': note})
        print('  done', name, '->', final)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--plan', action='store_true')
    p.add_argument('--only', help='只跑某条需求 record_id')
    p.add_argument('--run-dir', default=str(ROOT))
    p.add_argument('--auth-mode', choices=['device', 'browser'], help='默认取项目 REPLICA_AUTH_MODE')
    p.add_argument('--types', default='替换人物', help='只处理这些需求类型，逗号分隔（默认 替换人物）')
    a = p.parse_args()
    feishu = Feishu()
    api = ReplicaAPI.configured(auth_mode=a.auth_mode, confirm_paid=True); auth = api.authenticate()
    runner = Runner(Path(a.run_dir), feishu, api, auth, dry=a.plan, auth_mode=a.auth_mode)
    rows = feishu.call('POST', f'bitable/v1/apps/{APP}/tables/{POOL}/records/search',
                       params={'page_size': 500, 'user_id_type': 'open_id'}, json={}).get('items', [])
    for row in rows:
        f = row['fields']
        if a.only and row['record_id'] != a.only:
            continue
        if text(f.get('需求类型')) not in {x.strip() for x in a.types.split(',') if x.strip()}:
            continue
        if text(f.get('需求状态')) != '已验收':
            continue
        if text(f.get('复刻状态')) in {'已完成', '待补充信息', '需人工处理', '失败'}:
            continue  # 已完成或需要人工处理的行不再自动跑
        print('领取', row['record_id'], text(f.get('需求名称')))
        try:
            runner.run_row(row)
        except PlatformRejected as e:
            print('  平台拒绝', e)
            if not a.plan:
                runner.put(POOL, row['record_id'], {'复刻状态': '失败', '执行说明': f'平台拒绝生成：{e}'[:900]})
        except Exception as e:
            print('  失败', e)
            if not a.plan:
                runner.put(POOL, row['record_id'], {'复刻状态': '需人工处理', '执行说明': str(e)[:900]})


if __name__ == '__main__':
    main()
