"""XHS keyword search -> global likes top decile -> media -> Bitable -> daily card."""
from ua_workflows.collection_runtime import collection_phase, post_json, record_failure
from ua_workflows.social_selection import selection_policy, selection_report
from ua_workflows.xhs_tolerance import MediaUnavailable, bounded_acquire, exhausted_notes, policy_for
from ua_workflows.xhs_duration import DurationExcluded, check_video, excluded_notes as duration_excluded_notes, policy_for as duration_policy_for
import argparse
from datetime import date, datetime, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import uuid
from zoneinfo import ZoneInfo

from ua_workflows.social_chat_browser_daily import Feishu, text_field

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / 'config/social_chat_xhs_daily.json'
TZ = ZoneInfo('Asia/Shanghai')
FIELDS = [('笔记ID', 1), ('标题', 1), ('正文摘要', 1), ('原文链接', 15), ('发布时间', 1),
          ('命中关键词', 4), ('作者昵称', 1), ('作者ID', 1), ('点赞数', 2), ('收藏数', 2),
          ('评论数', 2), ('分享数', 2), ('采集窗口', 1), ('平台', 3), ('来源', 3), ('素材类型', 3),
          ('视频附件', 17), ('图片附件', 17), ('视频链接', 15), ('图片链接', 15),
          ('目标日期', 1), ('首次发现日期', 1), ('当前入选', 7), ('素材尺寸', 1),
          ('采集批次', 1), ('唯一键', 1), ('同步时间', 1)]


def load(p):
    return json.loads(Path(p).read_text())


def save(p, value):
    p = Path(p); p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + '.tmp'); tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2)); tmp.replace(p)


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def stable_uuid(seed):
    return str(uuid.UUID(hex=hashlib.sha256(seed.encode()).hexdigest()[:32], version=4))


IMAGE_RULE = 'person_or_digital_human_v1'
CALENDAR_DAY_RULE = 'previous_calendar_day_asia_shanghai_v1'


def target_day_window(base):
    """Use the pinned Shanghai calendar day; legacy scopes keep their old rule."""
    path=Path(base)/'daily-context.json'
    context=load(path) if path.exists() else {}
    rule=context.get('date_filter_rule')
    if rule is None:return None
    if rule!=CALENDAR_DAY_RULE or context.get('timezone')!='Asia/Shanghai':
        raise ValueError('Unsupported pinned date filter')
    target=date.fromisoformat(context['target_date'])
    executed=datetime.fromisoformat(context['execution_started_at'])
    if executed.tzinfo is None or target!=executed.astimezone(TZ).date()-timedelta(days=1):
        raise ValueError('Target date must be the day before the pinned execution date')
    start=datetime.combine(target,datetime.min.time(),tzinfo=TZ)
    return start,start+timedelta(days=1)


def reviewed_images(note, base):
    """Fail closed: every original image needs a hash-bound visual decision."""
    if note['is_video']:
        return None
    folder = Path(base) / 'media' / note['id']
    if not (folder / 'manifest.json').exists() or not (folder / 'image-review.json').exists():
        raise ValueError('IMAGE_REVIEW_PENDING: ' + note['id'])
    manifest = load(folder / 'manifest.json')
    if manifest.get('note_id') != note['id'] or manifest.get('is_video') is not False:
        raise ValueError('Image manifest identity mismatch')
    review = load(folder / 'image-review.json')
    if review.get('rule') != IMAGE_RULE or review.get('note_id') != note['id']:
        raise ValueError('Image review identity/rule mismatch')
    entries = review.get('images', [])
    decisions = {r['path']: r for r in entries}
    files = manifest['files']
    urls = list(dict.fromkeys(note.get('images') or note.get('image_urls') or ([note['image_url']] if note.get('image_url') else [])))
    if not urls or [m['url'] for m in files] != urls or len(decisions) != len(entries) or set(decisions) != {m['path'] for m in files}:
        raise ValueError('Image review does not cover all original images')
    kept = []
    for m in files:
        r = decisions[m['path']]
        if r.get('sha256') != m['sha256'] or sha(Path(base) / m['path']) != m['sha256']:
            raise ValueError('Image review hash mismatch')
        if r.get('decision') not in ('keep', 'exclude') or not r.get('reason') or not r.get('visually_inspected') or not r.get('reviewer'):
            raise ValueError('IMAGE_REVIEW_PENDING: missing actual visual decision')
        if r['decision'] == 'keep':
            kept.append(m)
    return {**manifest, 'files': kept, 'image_review_rule': IMAGE_RULE}


AUDIENCE_RULE = 'male_subject_v1'


def audience_review_digest(base):
    paths = sorted((Path(base) / 'media').glob('*/audience-review.json'))
    return hashlib.sha256(''.join(str(p.relative_to(base))+sha(p) for p in paths).encode()).hexdigest()


def male_subject_media(note, base, manifest):
    """Content curation only: visual male presentation, never inferred identity."""
    path = Path(base) / 'media' / note['id'] / 'audience-review.json'
    if not path.exists():
        raise ValueError('AUDIENCE_REVIEW_PENDING: ' + note['id'])
    review = load(path)
    if review.get('rule') != AUDIENCE_RULE or review.get('note_id') != note['id']:
        raise ValueError('Audience review identity/rule mismatch')
    entries = review.get('files', [])
    mapping = {r['path']: r for r in entries}
    if len(mapping) != len(entries) or set(mapping) != {m['path'] for m in manifest['files']}:
        raise ValueError('AUDIENCE_REVIEW_PENDING: incomplete file coverage')
    kept = []
    for m in manifest['files']:
        r = mapping[m['path']]
        if r.get('sha256') != m['sha256'] or sha(Path(base) / m['path']) != m['sha256']:
            raise ValueError('Audience review hash mismatch')
        if r.get('presentation') not in ('male_subject', 'female_subject', 'mixed', 'no_person', 'uncertain'):
            raise ValueError('AUDIENCE_REVIEW_PENDING: missing visual presentation')
        if r.get('visually_inspected') is not True or not r.get('reason') or not r.get('reviewer') or not r.get('evidence'):
            raise ValueError('AUDIENCE_REVIEW_PENDING: missing actual visual evidence')
        # An actually inspected but ambiguous subject is held out conservatively.
        # Missing visual evidence still blocks the entire ranking above.
        if r['presentation'] == 'male_subject':
            kept.append(m)
    return {**manifest, 'files': kept, 'audience_review_rule': AUDIENCE_RULE}


def eligible_media(note, base, config):
    if note['is_video']:
        path = Path(base) / 'media' / note['id'] / 'manifest.json'
        if not path.exists():
            raise ValueError('AUDIENCE_REVIEW_PENDING: video needs actual frames ' + note['id'])
        manifest = load(path)
        if manifest.get('note_id') != note['id'] or manifest.get('is_video') is not True:
            raise ValueError('Video manifest identity mismatch')
    else:
        manifest = reviewed_images(note, base)
        if not manifest['files']:
            return manifest
    if config.get('audience_content_filter', {}).get('enabled'):
        return male_subject_media(note, base, manifest)
    return manifest


def prepare_image_review(base):
    """Download image candidates before ranking; never calls paid video APIs."""
    from PIL import Image, ImageDraw
    base = Path(base); config = load(CONFIG); pending = []; failures = []
    from concurrent.futures import ThreadPoolExecutor, as_completed
    candidates = reviewable_notes(base)[0]
    notes = [n for n in candidates if not n['is_video']]
    pool = ThreadPoolExecutor(max_workers=3)
    futures = {pool.submit(media, n, base, config): n for n in notes}
    for future in as_completed(futures):
        note = futures[future]
        try:
            manifest = future.result()
            try:
                reviewed_images(note, base)
            except ValueError:
                pending.extend({'note_id': note['id'], **m} for m in manifest['files'])
        except MediaUnavailable:
            continue
        except Exception as exc:
            failures.append({'note_id': note['id'], 'error_type': type(exc).__name__, 'diagnostic': record_failure(base, 'xhs-media', exc)})
        save(base / 'image-review-progress.json', {'pending_images': len(pending), 'failures': failures})
    pool.shutdown(wait=True)
    pending.sort(key=lambda m: (m['note_id'], m['path']))
    directory = base / 'image-review-sheets'; directory.mkdir(exist_ok=True)
    for start in range(0, len(pending), 24):
        batch = pending[start:start+24]; sheet = Image.new('RGB', (1600, 1560), 'white'); draw = ImageDraw.Draw(sheet)
        for index, item in enumerate(batch):
            im = Image.open(base / item['path']).convert('RGB'); im.thumbnail((390, 225))
            x, y = (index % 4)*400, (index // 4)*260
            sheet.paste(im, (x+(400-im.width)//2, y))
            draw.text((x+4,y+228), f"{start+index+1} {item['note_id']}", fill='black')
            draw.text((x+4,y+243), Path(item['path']).name, fill='black')
        sheet.save(directory / f'{start//24+1:03}.jpg')
    result = {'rule': IMAGE_RULE, 'status': 'IMAGE_REVIEW_PENDING' if pending or failures else 'complete', 'images': pending, 'failures': failures}
    if policy_for(base):
        stats=reviewable_notes(base)[1]
        result.update(media_unavailable_notes=stats['media_unavailable_notes'],media_unavailable_ids=stats['media_unavailable_ids'])
    save(base / 'image-review-index.json', result)
    return {k:v for k,v in result.items() if k!='images'}


def extract_audience_frames(source, folder, base):
    # Audio/container duration can outlast sparse video frames. Select actual decoded
    # video frame indices instead of seeking beyond the last real frame.
    import fcntl, shutil, tempfile
    source,folder,base=map(Path,(source,folder,base));folder.mkdir(exist_ok=True)
    fractions=(.02,.2,.4,.6,.8,.98);source_sha=sha(source);index_file=folder/'actual-frame-index.json'
    with (folder/'.extract.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if index_file.exists():
            cached=load(index_file);samples=cached.get('samples',[]);count=cached.get('frame_count',0)
            if (cached.get('source')!=str(source.relative_to(base)) or cached.get('source_sha256')!=source_sha
                    or type(count) is not int or count<=0 or len(samples)!=6):
                raise ValueError('Cached actual frame evidence changed')
            for i,(sample,fraction) in enumerate(zip(samples,fractions),1):
                frame=folder/f'{i:02}.jpg'
                if (sample.get('path')!=str(frame.relative_to(base)) or sample.get('frame_index')!=round((count-1)*fraction)
                        or not isinstance(sample.get('timestamp_seconds'),(int,float)) or not math.isfinite(sample['timestamp_seconds'])
                        or not frame.is_file() or sha(frame)!=sample.get('sha256')):
                    raise ValueError('Cached actual frame evidence changed')
            return samples
        probe=subprocess.run(['ffprobe','-v','error','-select_streams','v:0','-show_entries','frame=best_effort_timestamp_time','-of','json',str(source)],capture_output=True,text=True,check=True,timeout=180)
        frames=json.loads(probe.stdout)['frames']
        if not frames:raise ValueError('Missing actual video frames')
        indices=[round((len(frames)-1)*fraction) for fraction in fractions];unique=sorted(set(indices));samples=[]
        # One decode pass for all six indices, including sparse videos with repeated indices.
        with tempfile.TemporaryDirectory(prefix='.extract-',dir=folder) as temporary:
            expression='+'.join(f'eq(n\\,{i})' for i in unique)
            subprocess.run(['ffmpeg','-v','error','-y','-threads','1','-i',str(source),'-vf','select='+expression,
                            '-fps_mode','vfr','-frames:v',str(len(unique)),'-q:v','2','-threads','1',str(Path(temporary)/'%02d.jpg')],
                           capture_output=True,check=True,timeout=240)
            extracted={n:Path(temporary)/f'{i:02}.jpg' for i,n in enumerate(unique,1)}
            if any(not p.is_file() or not p.stat().st_size for p in extracted.values()) or sha(source)!=source_sha:
                raise ValueError('Missing actual video frame or changed source')
            for index,n in enumerate(indices,1):
                frame=folder/f'{index:02}.jpg';shutil.copyfile(extracted[n],frame)
                samples.append({'path':str(frame.relative_to(base)),'frame_index':n,
                                'timestamp_seconds':float(frames[n]['best_effort_timestamp_time']),'sha256':sha(frame)})
        save(index_file,{'source':str(source.relative_to(base)),'source_sha256':source_sha,'frame_count':len(frames),
                         'extraction_mode':'single_decode_actual_indices_v1','samples':samples})
        return samples


def prepare_audience_review(base):
    """Prepare real image/frame evidence for the gender-presentation curation gate."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from PIL import Image, ImageDraw
    base=Path(base);config=load(CONFIG);pending=[];failures=[]
    def prepare(note):
        manifest=media(note,base,config) if note['is_video'] else reviewed_images(note,base)
        if not manifest['files']:return []
        try:
            male_subject_media(note,base,manifest)
            return []
        except ValueError:
            pass
        evidence=[]
        if note['is_video']:
            source=base/manifest['files'][0]['path'];duration=manifest['files'][0]['duration_seconds']
            folder=source.parent/'audience-frames';folder.mkdir(exist_ok=True)
            for sample in extract_audience_frames(source,folder,base):
                evidence.append({'note_id':note['id'],'media_path':manifest['files'][0]['path'],'sha256':manifest['files'][0]['sha256'],'evidence_path':sample['path'],'timestamp_seconds':sample['timestamp_seconds'],'frame_sha256':sample['sha256']})
        else:
            evidence=[{'note_id':note['id'],'media_path':m['path'],'sha256':m['sha256'],'evidence_path':m['path']} for m in manifest['files']]
        return evidence
    with ThreadPoolExecutor(max_workers=3) as pool:
        candidates = reviewable_notes(base)[0]
        jobs={pool.submit(prepare,n):n for n in candidates}
        for future in as_completed(jobs):
            try:pending.extend(future.result())
            except (MediaUnavailable,DurationExcluded):continue
            except Exception as exc:failures.append({'note_id':jobs[future]['id'],'error_type':type(exc).__name__,'diagnostic':record_failure(base, 'xhs-audience-media', exc)})
    pending.sort(key=lambda m:(m['note_id'],m['media_path'],m.get('timestamp_seconds',0)))
    directory=base/'audience-review-sheets';directory.mkdir(exist_ok=True)
    for start in range(0,len(pending),24):
        sheet=Image.new('RGB',(1600,1560),'white');draw=ImageDraw.Draw(sheet)
        for index,m in enumerate(pending[start:start+24]):
            with Image.open(base/m['evidence_path']) as source:im=source.convert('RGB');im.thumbnail((390,225))
            x,y=index%4*400,index//4*260;sheet.paste(im,(x+(400-im.width)//2,y));draw.text((x+4,y+228),f"{start+index+1} {m['note_id']}",fill='black');draw.text((x+4,y+243),Path(m['media_path']).name+' '+str(round(m.get('timestamp_seconds',0),2)),fill='black')
        sheet.save(directory/f'{start//24+1:03}.jpg')
    result={'rule':AUDIENCE_RULE,'status':'AUDIENCE_REVIEW_PENDING' if pending or failures else 'complete','items':pending,'failures':failures}
    if policy_for(base):
        stats=reviewable_notes(base)[1]
        result.update(media_unavailable_notes=stats['media_unavailable_notes'],media_unavailable_ids=stats['media_unavailable_ids'])
        result.update(video_duration_excluded=stats.get('video_duration_excluded',0))
    save(base/'audience-review-index.json',result);return {k:v for k,v in result.items() if k!='items'}


def old_post_exclusion_authorized(base):
    """Only this explicitly authorized run may exclude old supplier search hits."""
    base = Path(base).resolve()
    policy = base/'date-policy.json'
    if not policy.exists():
        return False
    d = load(policy)
    authorization = (base/d.get('authorization_file', '')).resolve()
    if (d.get('version') != 'exclude_old_hits_v1'
            or d.get('run_sha256') != sha(base/'run.json')
            or d.get('target_date') != load(base/'daily-context.json')['target_date']
            or not authorization.is_relative_to(base) or not authorization.is_file()
            or d.get('authorization_sha256') != sha(authorization)):
        raise ValueError('Date policy authorization evidence mismatch')
    approved = load(authorization)
    if approved.get('exclude_old_hits') is not True or not approved.get('answer'):
        raise ValueError('Explicit old-post exclusion authorization required')
    return True


def partial_collection_keywords(base, summary):
    """Keep collection incomplete; a bound manual approval may deliver available results."""
    base = Path(base).resolve()
    if policy_for(base):
        from ua_workflows.xhs_tolerance import majority_incomplete_keywords
        return majority_incomplete_keywords(base, summary)
    path = base/'partial-collection-policy.json'
    if not path.exists(): return []
    policy = load(path); approval = load(base/'partial-delivery-authorization.json')
    proof = load(base/'repeated-page-validation.json')
    if (policy.get('version') != 'authorized_available_results_v1'
            or policy.get('target_date') != load(base/'daily-context.json')['target_date']
            or policy.get('run_sha256') != sha(base/'run.json')
            or policy.get('summary_sha256') != sha(base/'summary.json')
            or policy.get('authorization_sha256') != sha(base/'partial-delivery-authorization.json')
            or policy.get('repeat_proof_sha256') != sha(base/'repeated-page-validation.json')
            or approval.get('allow_partial_delivery') is not True or not approval.get('answer')):
        raise ValueError('Partial delivery authorization evidence mismatch')
    keyword = policy['incomplete_keyword']
    unfinished = [t for t in summary['keywords'] if t.get('status') != 'complete']
    if (len(unfinished) != 1 or unfinished[0]['keyword'] != keyword
            or unfinished[0]['pages'] != policy['accepted_pages']):
        raise ValueError('Partial collection scope changed')
    index = [t['keyword'] for t in summary['keywords']].index(keyword) + 1
    page = policy['accepted_pages'] + 1
    expected_path = f'keywords/{index:02}/pages/{page:04}.response.json'
    if proof.get('response_path') != expected_path:
        raise ValueError('Partial collection repeat page mismatch')
    repeated_path = base/expected_path; duplicate_path = (base/proof['duplicate_response_path']).resolve()
    if not duplicate_path.is_relative_to(repeated_path.parent):
        raise ValueError('Invalid duplicate evidence path')
    repeated, duplicate = load(repeated_path), load(duplicate_path)
    if (sha(repeated_path) != proof['response_sha256'] or sha(duplicate_path) != proof['duplicate_response_sha256']
            or not repeated.get('results') or not repeated.get('next_page_token')
            or sorted(n['id'] for n in repeated['results']) != sorted(n['id'] for n in duplicate['results'])):
        raise ValueError('Repeated page evidence changed')
    return [keyword]


def validated_notes(base):
    base = Path(base); summary = load(base / 'summary.json'); run = load(base / 'run.json'); notes = load(base / 'notes.json')
    config = load(CONFIG)
    partial = partial_collection_keywords(base, summary)
    if (summary['status'] != 'complete' and not partial) or [t['keyword'] for t in summary['keywords']] != config['keywords']:
        raise ValueError('All configured keyword searches must finish before ranking')
    if summary['date_range'] != 'last_24_hours' or run['sort'] != 'newest':
        raise ValueError('Unexpected search scope')
    # Verify each real cursor transition and raw response, rather than trusting summary.json.
    observed = {}; eligible_ids = set(); old_hits = []; snapshots = []
    window=target_day_window(base)
    exclude_old = old_post_exclusion_authorized(base) if window is None else False
    for index, task in enumerate(summary['keywords'], 1):
        previous = None
        for page in range(1, task['pages'] + 1):
            stem = base / f'keywords/{index:02}/pages/{page:04}'
            req, resp = load(stem.with_suffix('.request.json')), load(stem.with_suffix('.response.json'))
            if req.get('transport')=='persistent_async_v1':
                from ua_workflows.social_search import resolve_page_job
                job=resolve_page_job(req, stem.with_suffix('.job'))
                receipt=load(job/'submit-response.json');completed=load(job/'result.json')
                if completed!=resp or completed.get('status')!='succeeded' or completed.get('job_id')!=receipt.get('job_id'):
                    raise ValueError('Async page job evidence mismatch')
            params = req['body']['params']
            expected = {'keyword': task['keyword'], 'content_type': 'all', 'date_range': 'last_24_hours', 'sort': 'newest'}
            if previous: expected['page_token'] = previous
            if params != expected: raise ValueError('Search request/cursor mismatch')
            if 'next_page_token' in resp:
                previous = resp['next_page_token'] or None
            else:
                tokens = {r.get('next_page_token') for r in resp['results'] if r.get('next_page_token')}
                if len(tokens) > 1: raise ValueError('Conflicting cursor')
                previous = next(iter(tokens), None)
            if bool(previous) != (page < task['pages'] or task['keyword'] in partial): raise ValueError('Incomplete pagination')
            at = datetime.fromisoformat(req['requested_at']); snapshots.append(at)
            if at.tzinfo is None: raise ValueError('Request timestamp must have timezone')
            for n in resp['results']:
                when = datetime.fromisoformat(n['posted_at'].replace('Z', '+00:00'))
                if when.tzinfo is None:
                    raise ValueError('Publication timestamp must have timezone')
                if window is not None and n['id'] in observed:
                    first_when=datetime.fromisoformat(observed[n['id']]['posted_at'].replace('Z','+00:00'))
                    if first_when!=when:raise ValueError('Conflicting publication timestamps for the same note ID')
                if window is not None:
                    outside=not (window[0]<=when<window[1])
                    reason='outside_target_calendar_day'
                else:
                    outside=when<at-timedelta(hours=24)
                    reason='older_than_24h'
                if window is None and (when > at or (outside and not exclude_old)):
                    raise ValueError('Publication timestamp is missing or outside the requested rolling window')
                if outside:
                    old_hits.append({'id':n['id'], 'keyword':task['keyword'], 'page':page,
                                     'posted_at':n['posted_at'], 'requested_at':req['requested_at'],
                                     'response_file':str(stem.with_suffix('.response.json').relative_to(base)),
                                     'reason':reason})
                else:
                    eligible_ids.add(n['id'])
                if n['id'] not in observed: observed[n['id']] = {**n, 'matched_keywords': []}
                if task['keyword'] not in observed[n['id']]['matched_keywords']: observed[n['id']]['matched_keywords'].append(task['keyword'])
    if len(observed) != summary['unique_notes'] or len(notes) != len(observed): raise ValueError('Merged count mismatch')
    for n in notes:
        if n['id'] not in observed or any(n.get(k) != observed[n['id']].get(k) for k in observed[n['id']]):
            raise ValueError('Merged note does not match raw evidence')
        if window is None or n['id'] in eligible_ids:
            if type(n.get('like_count')) is not int or n['like_count'] < 0: raise ValueError('Missing/invalid likes cannot be treated as zero')
            if type(n.get('is_video')) is not bool: raise ValueError('Unknown media type')
    stats = {'raw_unique_notes':len(notes), 'date_excluded_hits':len(old_hits),
             'collection_status':'partial' if partial else 'complete', 'incomplete_keywords':partial,
             'date_excluded_unique':len(set(observed)-eligible_ids),
             'date_eligible_unique_notes':len(eligible_ids),
             'snapshot_start':min(snapshots).astimezone(TZ).isoformat() if snapshots else None,
             'snapshot_end':max(snapshots).astimezone(TZ).isoformat() if snapshots else None}
    if policy_for(base):
        stats.update(continuation_rule=policy_for(base)['rule'],keyword_count=7,
                     completed_keywords=sum(t.get('status')=='complete' for t in summary['keywords']))
    if window is not None:
        stats.update(date_filter_rule=CALENDAR_DAY_RULE,target_date=window[0].date().isoformat(),
                     timezone='Asia/Shanghai',target_day_start=window[0].isoformat(),target_day_end_exclusive=window[1].isoformat())
        save(base/'excluded-outside-target-day.json',old_hits)
        save(base/'date-validation.json',{'status':'passed_target_calendar_day_filter',**stats,
                                         'context_sha256':sha(base/'daily-context.json'),
                                         'provider_search_window':'last_24_hours',
                                         'coverage':'API acquired results filtered locally; not all posts on the target calendar day'})
    elif exclude_old:
        save(base/'excluded-old-posts.json',old_hits)
        save(base/'date-validation.json',{'status':'passed_with_authorized_old_post_exclusion',
                                         **stats, 'date_policy_sha256':sha(base/'date-policy.json')})
    return [n for n in notes if n['id'] in eligible_ids], stats


def reviewable_notes(base):
    base = Path(base).resolve(); notes, stats = validated_notes(base)
    if duration_policy_for(base):
        excluded=duration_excluded_notes(base,notes);ids={r['note_id'] for r in excluded}
        save(base/'excluded-over-duration.json',excluded)
        stats.update(video_duration_filter=duration_policy_for(base),video_duration_excluded=len(ids),video_duration_excluded_ids=sorted(ids))
        notes=[n for n in notes if n['id'] not in ids]
    if policy_for(base):
        failures=exhausted_notes(base,notes)
        save(base/'unavailable-media-after-three-attempts.json',failures)
        excluded={f['note_id'] for f in failures}
        stats.update(media_unavailable_notes=len(excluded),media_unavailable_ids=sorted(excluded),
                     media_review_status='partial' if excluded else 'complete')
        return [n for n in notes if n['id'] not in excluded],stats
    policy_path = base/'unavailable-media-policy.json'
    if not policy_path.exists(): return notes, stats
    policy = load(policy_path); authorization = load(base/'media-unavailable-authorization.json')
    if (policy.get('version') != 'authorized_unavailable_media_v1'
            or policy.get('run_sha256') != sha(base/'run.json')
            or policy.get('summary_sha256') != sha(base/'summary.json')
            or policy.get('authorization_sha256') != sha(base/'media-unavailable-authorization.json')
            or authorization.get('allow_unavailable_media') is not True or not authorization.get('answer')):
        raise ValueError('Unavailable media authorization evidence mismatch')
    mapping = {n['id']:n for n in notes}; excluded = set()
    for failure in policy.get('failures',[]):
        ident = failure['note_id']
        if ident in excluded or ident not in mapping or not mapping[ident]['is_video']:
            raise ValueError('Unavailable media identity mismatch')
        if (base/f'media/{ident}/manifest.json').exists():
            raise ValueError('Available media cannot use the unavailable exception')
        if not failure.get('evidence'): raise ValueError('Actual media failure evidence required')
        for item in failure['evidence']:
            path = (base/item['path']).resolve()
            if not path.is_relative_to(base) or not path.is_file() or sha(path)!=item['sha256']:
                raise ValueError('Unavailable media failure evidence changed')
        excluded.add(ident)
    if not excluded: raise ValueError('Empty unavailable media exception')
    stats.update(media_unavailable_notes=len(excluded),media_unavailable_ids=sorted(excluded),
                 media_review_status='partial',unavailable_media_policy_sha256=sha(policy_path))
    return [n for n in notes if n['id'] not in excluded],stats


def select(base):
    base = Path(base); config = load(CONFIG)
    notes, date_stats = reviewable_notes(base)
    raw_count = date_stats['raw_unique_notes']; date_count = len(notes)
    person_notes = [n for n in notes if n['is_video'] or reviewed_images(n, base)['files']]
    person_count = len(person_notes)
    if config.get('audience_content_filter', {}).get('enabled'):
        notes = [n for n in person_notes if eligible_media(n, base, config)['files']]
    else:
        notes = person_notes
    notes.sort(key=lambda n: (-n['like_count'], -datetime.fromisoformat(n['posted_at'].replace('Z', '+00:00')).timestamp(), n['id']))
    policy = selection_policy(len(notes), config.get('ranking', {}).get('rule_version', 'likes_top10_below100_else_decile_v1')); count = policy['selected']
    result = {**policy, **date_stats, 'audience_review_rule': AUDIENCE_RULE if config.get('audience_content_filter', {}).get('enabled') else None, 'audience_excluded': person_count-len(notes), 'audience_review_digest': audience_review_digest(base), 'image_review_rule': IMAGE_RULE, 'raw_unique_notes': raw_count, 'image_content_excluded': date_count-person_count, 'denominator': len(notes),
              'tie_policy': 'likes desc, posted_at desc, note_id asc', 'items': notes[:count]}
    save(base / 'selection.json', result)
    return result


def authorized_client(config):
    from ua_workflows.shared.config import project_env_values
    if project_env_values().get('FEISHU_APP_ID') != config['feishu']['expected_app_id']:
        raise ValueError('Feishu application identity does not match 广告之神')
    return Feishu()


def bitable_url(config):
    target = config['feishu']
    url = target['base_url'] + '?table=' + target['table_id']
    if target.get('view_id'):
        url += '&view=' + target['view_id']
    return url


def daily_destination(base, config, context):
    """Keep an unfinished batch on its original table after a destination change."""
    target = context.get('feishu_target')
    snapshot_path = Path(base)/'runtime/config-snapshot.json'
    if target is None and snapshot_path.exists():
        snapshot=load(snapshot_path)
        if (snapshot.get('target_date')!=context['target_date'] or not (Path(base)/'run.json').is_file()
                or snapshot.get('run_sha256')!=sha(Path(base)/'run.json')):
            raise ValueError('Historical destination snapshot does not match the batch')
        target=snapshot['config']['feishu']
    if target is None:
        # Earlier batches predate destination snapshots and used the former XHS table.
        target={**config['feishu'],'table_id':'tblwhAqPfQoBXK3Q'}
        target.pop('view_id',None)
    if (not target.get('table_id') or target.get('app_token')!=config['feishu']['app_token']
            or target.get('base_url')!=config['feishu']['base_url']):
        raise ValueError('Pinned XHS destination does not match the configured base')
    return {**config,'feishu':{**config['feishu'],**target,**({} if 'view_id' in target else {'view_id':None})}}


def provision(client, config):
    route = f"bitable/v1/apps/{config['feishu']['app_token']}/tables/{config['feishu']['table_id']}"
    existing = {f['field_name']: f for f in client.listing(route+'/fields')}
    for name, kind in FIELDS:
        if name in existing:
            if existing[name]['type'] != kind: raise ValueError('Field type mismatch: '+name)
        else:
            client.call('POST', route+'/fields', json={'field_name': name, 'type': kind})
    verified = {f['field_name']: f['type'] for f in client.listing(route+'/fields')}
    if any(verified.get(n) != k for n, k in FIELDS): raise ValueError('Schema readback failed')
    return route


def api_video(note, folder, config):
    base=Path(folder).parents[1]
    if policy_for(base):
        import fcntl
        from ua_workflows.collection_runtime import paid_request_power
        from ua_workflows.shared.config import project_env_values
        key=project_env_values().get('EVERYINFRA_API_KEY')
        def valid(response):
            result=response.get('results')
            if not isinstance(result,dict) or result.get('id')!=note['id'] or not result.get('video_url'):
                raise ValueError('Video URL unavailable or note ID mismatch')
            return result['video_url']
        def attempt(slot):
            save(slot/'video-api-intent.json',{'note_id':note['id'],'url':note['url'],'at':datetime.now(TZ).isoformat()})
            r,response=post_json(config['api']['endpoint'],key=key,
                body={'platform':'xiaohongshu','action':'video','params':{'url':note['url']}},
                diagnostic_path=slot/'video-api-http.json')
            save(slot/'video-api-result-meta.json',{'ok':r.ok,'status_code':r.status_code})
            save(slot/'video-api-response.json',response)
            if not r.ok:raise ValueError('Video API failed HTTP '+str(r.status_code))
            return valid(response)
        def recover(slot):
            path=slot/'video-api-response.json'
            meta=slot/'video-api-result-meta.json'
            if path.exists() and meta.exists() and load(meta).get('ok') is True:
                try:return valid(load(path))
                except ValueError:return None
        def preflight(slot):
            if not key:raise ValueError('Missing EVERYINFRA_API_KEY')
            return paid_request_power(slot/'video-api-power.json')
        runtime=base/'runtime';runtime.mkdir(exist_ok=True)
        with (runtime/'.detail-submission.lock').open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            return bounded_acquire(base,note,'detail',attempt,
                before_attempt=preflight,recover=recover)
    import requests
    from ua_workflows.shared.config import project_env_values
    p = folder/'video-api-response.json'
    if p.exists():
        response = load(p)
    else:
        if (folder/'video-api-intent.json').exists(): raise ValueError('Previous video API outcome uncertain; inspect before replay')
        key = project_env_values().get('EVERYINFRA_API_KEY')
        if not key: raise ValueError('Missing EVERYINFRA_API_KEY')
        from ua_workflows.collection_runtime import paid_request_power
        with paid_request_power(folder/'video-api-power.json'):
            save(folder/'video-api-intent.json', {'note_id': note['id'], 'url': note['url'], 'at': datetime.now(TZ).isoformat()})
            r, response = post_json(config['api']['endpoint'], key=key,
                                   body={'platform': 'xiaohongshu', 'action': 'video', 'params': {'url': note['url']}},
                                   diagnostic_path=folder/'video-api-http.json')
            save(p, response)
            if not r.ok: raise ValueError('Video API failed HTTP '+str(r.status_code))
    result = response.get('results')
    if not isinstance(result, dict) or result.get('id') != note['id'] or not result.get('video_url'):
        raise ValueError('Video URL unavailable or note ID mismatch')
    return result['video_url']


from ua_workflows.resumable_media import media_get


def download(url, path):
    from ua_workflows.resumable_media import download as transfer
    return transfer(url,path,allowed_hosts=('xhscdn.com','rednotecdn.com'),
                    referer='https://www.xiaohongshu.com/',get=media_get)


def media(note, base, config):
    base=Path(base)
    duration_check=base/'media'/note['id']/'video-duration-check.json'
    if note['is_video'] and duration_policy_for(base) and duration_check.exists():
        check_video(note,base,base/load(duration_check)['source'])
    if (base/'media'/note['id']/'manifest.json').exists() or not policy_for(base):
        result=_media_once(note,base,config)
        if note['is_video'] and duration_policy_for(base):check_video(note,base,base/result['files'][0]['path'])
        return result
    resolved=note
    if note['is_video'] and not note.get('video_url'):
        folder=base/'media'/note['id'];folder.mkdir(parents=True,exist_ok=True)
        resolved={**note,'video_url':api_video(note,folder,config)}
    return bounded_acquire(base,resolved,'media',lambda slot:_media_once(resolved,base,config))


def _media_once(note, base, config):
    folder=base/'media'/note['id'];folder.mkdir(parents=True,exist_ok=True);manifest=folder/'manifest.json'
    if manifest.exists():
        result=load(manifest)
        if all((base/m['path']).is_file() and sha(base/m['path'])==m['sha256'] for m in result['files']):return result
        raise ValueError('Existing media hash changed')
    files=[]
    if note['is_video']:
        url=note.get('video_url') or api_video(note,folder,config);path=folder/'video.mp4';download(url,path)
        p=subprocess.run(['ffprobe','-v','error','-show_entries','format=duration:stream=codec_type,width,height,duration,duration_ts,time_base','-of','json',str(path)],capture_output=True,text=True,check=True)
        probe=json.loads(p.stdout);stream=next(s for s in probe['streams'] if s.get('codec_type')=='video')
        video_duration=check_video(note,base,path,probe)
        subprocess.run(['ffmpeg','-v','error','-i',str(path),'-map','0:v:0','-f','null','-'],capture_output=True,check=True,timeout=180)
        files.append({'path':str(path.relative_to(base)),'sha256':sha(path),'size':path.stat().st_size,'width':stream['width'],'height':stream['height'],'duration_seconds':video_duration if video_duration is not None else float(probe['format']['duration']),'url':url,'decode_verified':True})
    else:
        urls=note.get('images') or note.get('image_urls') or ([note['image_url']] if note.get('image_url') else [])
        if not urls or not all(isinstance(u,str) for u in urls):raise ValueError('Missing/unsupported image list')
        from PIL import Image
        for index,url in enumerate(dict.fromkeys(urls),1):
            original=folder/f'original-{index:02}.image';download(url,original);path=folder/f'image-{index:02}.png'
            # Decode all supplied images; retain original bytes and source URLs alongside normalized attachments.
            try:
                with Image.open(original) as im:im.load();im.convert('RGB').save(path)
            except Exception:
                subprocess.run(['ffmpeg','-v','error','-y','-i',str(original),'-frames:v','1',str(path)],capture_output=True,check=True,timeout=60)
            with Image.open(path) as im:im.load();width,height=im.size
            files.append({'path':str(path.relative_to(base)),'sha256':sha(path),'size':path.stat().st_size,'width':width,'height':height,'url':url,'original_path':str(original.relative_to(base)),'original_sha256':sha(original),'decode_verified':True})
    result={'note_id':note['id'],'is_video':note['is_video'],'files':files};save(manifest,result);return result


def row_fields(note, manifest, day, base):
    values={'平台':'小红书','来源':'每日最新','素材类型':'视频' if note['is_video'] else '图片','笔记ID':note['id'],
            '标题':note.get('title') or '', '正文摘要':note.get('text') or '',
            '原文链接':{'text':'查看小红书笔记','link':note['url']},
            '发布时间':datetime.fromisoformat(note['posted_at'].replace('Z','+00:00')).astimezone(TZ).isoformat(),
            '命中关键词':note['matched_keywords'],'作者昵称':note.get('author_name') or '', '作者ID':note.get('author_id') or '',
            '目标日期':day,'当前入选':True,'采集批次':base.name,
            '唯一键':'|'.join(['小红书',day,note['id']]),'同步时间':datetime.now(TZ).isoformat(),
            '采集窗口':f'北京时间{day}自然日；搜索接口最近24小时' if target_day_window(base) else '最近24小时；各页以请求时刻为准',
            '素材尺寸':' / '.join(dict.fromkeys(f"{m['width']}×{m['height']}" for m in manifest['files']))}
    for src,dst in [('like_count','点赞数'),('collect_count','收藏数'),('comment_count','评论数'),('share_count','分享数')]:
        value=note.get(src)
        if value is not None and (type(value) is not int or value<0):raise ValueError('Invalid interaction count')
        values[dst]=value
    values['视频链接' if note['is_video'] else '图片链接']={'text':'媒体源链接','link':manifest['files'][0]['url']}
    return values


def verify_row(actual, expected):
    kinds=dict(FIELDS)
    for name,value in expected.items():
        got=actual.get(name);kind=kinds[name]
        if value is None:ok=got is None or got==''
        elif kind in (1,3):ok=text_field(got)==value
        elif kind==2:
            try:ok=type(got) in (str,int,float) and math.isfinite(float(got)) and float(got)==value
            except (ValueError,TypeError):ok=False
        elif kind==4:ok=sorted(got or [])==sorted(value)
        elif kind==15:ok=isinstance(got,dict) and got.get('link')==value['link']
        elif kind==17:ok={m['file_token'] for m in got or []}=={m['file_token'] for m in value} and all(m.get('size',0)>0 for m in got or [])
        else:ok=got is value
        if not ok:raise ValueError('Readback mismatch: '+name)


def upload_attachment(client,config,path,name):
    if path.stat().st_size<=20*1024*1024:
        from ua_workflows.social_chat_mixed_delivery import upload
        return upload(client,config,path,name)
    # Official Drive media prepare/part/finish API for files above the upload_all limit.
    import zlib
    checkpoint=path.with_suffix(path.suffix+'.feishu-upload.json');digest=sha(path)
    state=load(checkpoint) if checkpoint.exists() else None
    if state and (state['sha256']!=digest or state['app_token']!=config['feishu']['app_token']):raise ValueError('Upload checkpoint identity mismatch')
    if state and state.get('file_token'):return state['file_token']
    if not state:
        prepared=client.call('POST','drive/v1/medias/upload_prepare',json={'file_name':name,'parent_type':'bitable_file','parent_node':config['feishu']['app_token'],'size':path.stat().st_size})
        if not prepared.get('upload_id') or prepared.get('block_size',0)<=0 or prepared.get('block_num')!=math.ceil(path.stat().st_size/prepared['block_size']):raise ValueError('Unexpected upload block plan')
        state={**prepared,'sha256':digest,'app_token':config['feishu']['app_token'],'completed_parts':[]};save(checkpoint,state)
    with path.open('rb') as f:
        for seq in range(state['block_num']):
            if seq in state['completed_parts']:continue
            f.seek(seq*state['block_size']);data=f.read(state['block_size'])
            r=client.session.post('https://open.feishu.cn/open-apis/drive/v1/medias/upload_part',data={'upload_id':state['upload_id'],'seq':str(seq),'size':str(len(data)),'checksum':str(zlib.adler32(data)&0xffffffff)},files={'file':(name,data,'application/octet-stream')},timeout=120)
            body=r.json()
            if not r.ok or body.get('code')!=0:raise ValueError('Feishu part upload failed code='+str(body.get('code')))
            state['completed_parts'].append(seq);save(checkpoint,state)
    finished=client.call('POST','drive/v1/medias/upload_finish',json={'upload_id':state['upload_id'],'block_num':state['block_num']})
    if not finished.get('file_token'):raise ValueError('Missing upload finish token')
    state['file_token']=finished['file_token'];save(checkpoint,state);return finished['file_token']


def verify_delivery_rows(base, selection, config, remote, expected, uploads):
    """Require unique remote rows and exact attachment bytes before reporting."""
    notes={n['id']:n for n in selection['items']}; checked=[]
    for fields in expected:
        matches=[r for r in remote if text_field(r['fields'].get('唯一键'))==fields['唯一键']]
        if len(matches)!=1:raise ValueError('Delivery unique key readback mismatch')
        actual=matches[0]['fields'];verify_row(actual,fields)
        note=notes[fields['笔记ID']]
        manifest=eligible_media(note,base,config) if config.get('audience_content_filter',{}).get('enabled') else (media(note,base,config) if note['is_video'] else reviewed_images(note,base))
        attachments=actual.get('视频附件' if note['is_video'] else '图片附件') or []
        if len(attachments)!=len(manifest['files']):raise ValueError('Delivery attachment count mismatch')
        files=[]
        for item in manifest['files']:
            path=base/item['path'];token=uploads[item['sha256']]
            if path.stat().st_size!=item['size'] or sha(path)!=item['sha256']:
                raise ValueError('Delivery local media changed')
            found=[a for a in attachments if a.get('file_token')==token]
            if len(found)!=1 or found[0].get('size')!=item['size']:
                raise ValueError('Delivery exact attachment size mismatch')
            files.append({'path':item['path'],'sha256':item['sha256'],'size':item['size'],'file_token':token})
        checked.append({'unique_key':fields['唯一键'],'record_id':matches[0]['record_id'],'files':files})
    result={'status':'passed','verified_rows':len(checked),'verified_attachments':sum(len(r['files']) for r in checked),'unique_keys_fields_tokens_exact_sizes_and_local_hashes':True,'rows':checked}
    save(base/'delivery-validation.json',result)
    return result


def sync(base, selection, config, day):
    client=authorized_client(config);route=provision(client,config);uploads=load(base/'uploads.json') if (base/'uploads.json').exists() else {};expected=[];failures=[]
    rows=client.listing(route+'/records');by_key={}
    for r in rows:
        key=text_field(r['fields'].get('唯一键'))
        if key and key in by_key:raise ValueError('Duplicate remote unique key')
        if key:by_key[key]=r
    for note in selection['items']:
        try:
            manifest=eligible_media(note,base,config) if config.get('audience_content_filter',{}).get('enabled') else (media(note,base,config) if note['is_video'] else reviewed_images(note,base));fields=row_fields(note,manifest,day,base);tokens=[]
            for m in manifest['files']:
                path=base/m['path'];digest=m['sha256']
                if digest not in uploads:
                    token=upload_attachment(client,config,path,path.name);uploads[digest]=token;save(base/'uploads.json',uploads)
                tokens.append({'file_token':uploads[digest]})
            fields['视频附件' if note['is_video'] else '图片附件']=tokens
            existing=by_key.get(fields['唯一键'])
            if existing and (text_field(existing['fields'].get('来源'))!='每日最新' or text_field(existing['fields'].get('平台'))!='小红书' or text_field(existing['fields'].get('目标日期'))!=day):
                raise ValueError('Protected manual/platform/date record conflicts with daily unique key')
            # First discovery means first locally observed date, never publication date.
            if not existing or not existing['fields'].get('首次发现日期'):fields['首次发现日期']=datetime.fromisoformat(load(base/'run.json')['started_at']).astimezone(TZ).date().isoformat()
            else:fields['首次发现日期']=text_field(existing['fields']['首次发现日期'])
            if existing:
                if text_field(existing['fields'].get('采集批次'))==base.name:
                    fields['同步时间']=text_field(existing['fields'].get('同步时间'))
                try:
                    verify_row(existing['fields'],fields)
                except ValueError:
                    client.call('PUT',route+'/records/'+existing['record_id'],json={'fields':fields})
            else:
                created=client.call('POST',route+'/records/batch_create',params={'client_token':stable_uuid(fields['唯一键'])},json={'records':[{'fields':fields}]})
                record=created['records'][0];by_key[fields['唯一键']]=record
            expected.append(fields);save(base/'expected-rows.json',expected)
        except Exception as exc:
            failures.append({'note_id':note['id'],'error_type':type(exc).__name__,'reason':str(exc) if isinstance(exc,ValueError) else 'Media/API/upload failed; see local evidence','diagnostic':record_failure(base, 'xhs-sync', exc)})
        save(base/'media-failures.json',failures)
        print(json.dumps({'phase':'media_and_sync','note_id':note['id'],'verified_pending':len(expected),'failures':len(failures)},ensure_ascii=False),flush=True)
    remote=client.listing(route+'/records');lookup={text_field(r['fields'].get('唯一键')):r for r in remote}
    for e in expected:verify_row(lookup[e['唯一键']]['fields'],e)
    desired={e['唯一键'] for e in expected}
    # Retire only previous XHS selections for the same reporting day after successful new readback.
    if not failures:
        old=[{'record_id':r['record_id'],'fields':{'当前入选':False}} for r in remote if text_field(r['fields'].get('来源'))=='每日最新' and text_field(r['fields'].get('平台'))=='小红书' and text_field(r['fields'].get('唯一键')).startswith('小红书|'+day+'|') and text_field(r['fields'].get('目标日期'))==day and r['fields'].get('当前入选') is True and text_field(r['fields'].get('唯一键')) not in desired]
        if old:client.call('POST',route+'/records/batch_update',json={'records':old})
    final=client.listing(route+'/records')
    verify_delivery_rows(base,selection,config,final,expected,uploads)
    result={'status':'complete' if not failures else 'partial','selected':selection['selected'],'verified_rows':len(expected),'failures':failures,
            'video_success':sum(e['素材类型']=='视频' for e in expected),'image_success':sum(e['素材类型']=='图片' for e in expected)}
    save(base/'sync.json',result);return client,result


def report(base,selection,config,client,synced,day):
    receipt=base/'report-receipt.json'
    if receipt.exists():return load(receipt)
    setting=config['crawl_completion_report'];chat=client.call('GET','im/v1/chats/'+setting['chat_id'])
    if chat.get('name')!=setting['chat_name'] or chat.get('chat_mode')!='group':raise ValueError('Group identity mismatch')
    summary=load(base/'summary.json');start=datetime.fromisoformat(load(base/'run.json')['started_at']).astimezone(TZ).isoformat();url=bitable_url(config)
    calendar_day=selection.get('date_filter_rule')==CALENDAR_DAY_RULE
    scope_text=(f'北京时间{day}自然日，7个关键词，最新排序' if calendar_day else '最近24小时，7个关键词，最新排序')
    time_note=('本地只保留目标自然日00:00至次日00:00前发布的内容；搜索接口仍按实际请求前24小时获取，可能不能完整覆盖目标自然日。\n'
               if calendar_day else '窗口以每页实际请求时刻往前24小时为准；若有断点续采，不代表严格自然日。\n')
    date_note = (f"日期窗口剔除：**{selection.get('date_excluded_hits',0)} 条命中 / {selection.get('date_excluded_unique',0)} 篇**\n"
                 f"实际请求快照：{selection.get('snapshot_start')} 至 {selection.get('snapshot_end')}；搜索接口各页分别向前24小时。\n") if selection.get('snapshot_start') else ''
    fee_note = '原超时请求收费仍未知，另有用户授权单次恢复；未将未知费用记为0。\n' if summary.get('prior_timeout_billing_unknown') else ''
    partial_collection = selection.get('collection_status') == 'partial'
    partial_delivery=partial_collection or bool(selection.get('media_unavailable_notes')) or synced['status']!='complete'
    title=('小红书素材日报' if not partial_delivery else '小红书素材日报｜部分完成')+'｜'+day
    coverage_note = ('**采集不完整：'+ '、'.join(selection['incomplete_keywords']) +'未到末页；按授权以已获结果继续，排名仅基于已获数据。**\n') if partial_collection else ''
    if selection.get('completed_keywords') is not None:
        coverage_note+=f"实际完整采集关键词：**{selection['completed_keywords']}/{selection['keyword_count']}**。\n"
    media_label='原素材' if policy_for(base) else '原视频'
    unavailable_note = (f"{media_label}获取失败／未审核：**{selection['media_unavailable_notes']} 篇**，经明确授权单列，不计入可审核排名分母；排名仅基于已取得并审核的素材，未虚构内容判断。\n") if selection.get('media_unavailable_notes') else ''
    coverage = '，存在未完成关键词' if partial_collection else '，全部关键词已到接口末页'
    duration_note=(f"视频时长上限：**5分钟**（300秒含）；超限排除：**{selection.get('video_duration_excluded',0)} 篇**，不抽帧、不纳入排名。\n") if selection.get('video_duration_filter') else ''
    text=(f"**采集口径：{scope_text}**\n执行开始：{start}\n\n"
          f"{coverage_note}{unavailable_note}"
          f"📊 搜索命中：**{summary['raw_rows']} 条**，共 **{summary['pages']} 页**{coverage}\n"
          f"去重笔记：**{summary['unique_notes']} 篇**（视频 {summary['video_notes']}，图文 {summary['image_notes']}）\n\n"
          f"{date_note}{fee_note}{duration_note}"
          f"人物图片审核剔除：**{selection.get('image_content_excluded', 0)} 篇**；已审核合格候选：**{selection['denominator']} 篇**\n"
          f"男性主体审核剔除：**{selection.get('audience_excluded',0)} 篇**\n"
          f"{selection_report(selection)}\n"
          f"🎬 视频下载并入表：**{synced['video_success']} 篇**\n🖼️ 图文下载并入表：**{synced['image_success']} 篇**\n"
          f"⚠️ 入选项媒体/同步失败：**{len(synced['failures'])} 篇**，不补位、不改写分母\n"
          f"✅ 实际写表并读回验证：**{synced['verified_rows']} 篇**，附媒体附件\n\n"
          "同赞按发布时间从新到旧排序。缺失互动数保留为空；入表图片及视频均已按当前主体规则审核，图文仅上传审核合格图片。\n"
          f"{time_note}"
          "统计仅限API可返回结果，不代表全站全部笔记。点击下方链接查看。")
    card={'config':{'wide_screen_mode':True},'header':{'template':'orange' if partial_delivery else 'blue','title':{'tag':'plain_text','content':title}},'elements':[{'tag':'div','text':{'tag':'lark_md','content':text}},{'tag':'action','actions':[{'tag':'button','type':'primary','text':{'tag':'plain_text','content':'查看多维表格'},'url':url}]}]}
    save(base/'report-payload.json',card)
    sent=client.call('POST','im/v1/messages',params={'receive_id_type':'chat_id'},json={'receive_id':setting['chat_id'],'msg_type':'interactive','content':json.dumps(card,ensure_ascii=False),'uuid':stable_uuid('xhs|'+day+'|'+setting['chat_id'])})
    if sent.get('chat_id')!=setting['chat_id'] or not sent.get('message_id'):raise ValueError('Message receipt mismatch')
    result={'message_id':sent['message_id'],'chat_id':sent['chat_id'],'status':'sent_with_api_receipt'};save(receipt,result);return result


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['plan','run','provision','prepare-images','prepare-audience']);p.add_argument('--run-dir',type=Path);args=p.parse_args();config=load(CONFIG)
    if args.action=='provision':
        print(json.dumps({'status':'schema_verified','route':provision(authorized_client(config),config)},ensure_ascii=False));return
    if args.action=='prepare-audience':
        if not args.run_dir:raise ValueError('--run-dir required')
        with collection_phase(args.run_dir, 'xhs-prepare-audience'):
            print(json.dumps(prepare_audience_review(args.run_dir),ensure_ascii=False));return
    if args.action=='prepare-images':
        if not args.run_dir:raise ValueError('--run-dir required')
        with collection_phase(args.run_dir, 'xhs-prepare-images'):
            print(json.dumps(prepare_image_review(args.run_dir),ensure_ascii=False));return
    if args.action=='plan':
        if not args.run_dir:raise ValueError('--run-dir required for plan')
        selected=select(args.run_dir);print(json.dumps({k:v for k,v in selected.items() if k!='items'},ensure_ascii=False));return
    import fcntl
    folder=ROOT/'data/xhs_daily';folder.mkdir(parents=True,exist_ok=True)
    with (folder/'.run.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        now=datetime.now(TZ);base=args.run_dir or folder/now.strftime('%Y%m%d')
        context=base/'daily-context.json'
        if context.exists():
            value=load(context);day=value['target_date']
        else:
            day=(now-timedelta(days=1)).date().isoformat()
            value={'target_date':day,'execution_started_at':now.isoformat(),'timezone':'Asia/Shanghai',
                   'feishu_target':{k:config['feishu'][k] for k in ('app_token','table_id','base_url','view_id') if k in config['feishu']}}
            if config.get('date_semantics')==CALENDAR_DAY_RULE:value['date_filter_rule']=CALENDAR_DAY_RULE
            save(context,value)
        # Completed batches keep their original selection and receipt across rule changes.
        prior=base/'daily-result.json';receipt=base/'report-receipt.json'
        if prior.exists() and receipt.exists() and load(prior)['sync']['status']=='complete':
            previous=load(prior);sent=load(receipt)
            if (previous['target_date']!=day or previous['selection_sha256']!=sha(base/'selection.json')
                    or not sent.get('message_id') or previous['report']['message_id']!=sent['message_id']):
                raise ValueError('Completed delivery evidence changed; refusing recomputation')
            print(json.dumps({'status':'already_delivered','directory':str(base),'target_date':day,
                              'collection_status':previous.get('collection_status','complete')},ensure_ascii=False));return
        config=daily_destination(base,config,value)
        with collection_phase(base, 'xhs-daily'):
            if not (base/'summary.json').exists() or (load(base/'summary.json').get('status')!='complete' and not partial_collection_keywords(base, load(base/'summary.json'))):
                from ua_workflows.social_search import collect_xhs
                from ua_workflows.shared.config import project_env_values
                searched=collect_xhs(base,config,key=project_env_values().get('EVERYINFRA_API_KEY'))
                if searched['status']!='complete' and not partial_collection_keywords(base,searched):
                    raise ValueError('SEARCH_INCOMPLETE: insufficient completed keywords; resume saved jobs without paid replay')
            prior=base/'daily-result.json'
            review_status=prepare_image_review(base)
            if review_status['status']!='complete':
                save(base/'image-review-status.json',review_status)
                print(json.dumps({'directory':str(base),**review_status},ensure_ascii=False));return
            try:
                selection=select(base)
            except ValueError as exc:
                if 'AUDIENCE_REVIEW_PENDING' not in str(exc):raise
                pending={'status':'AUDIENCE_REVIEW_PENDING','rule':AUDIENCE_RULE,'detail':str(exc),'prior_selection_superseded':True}
                save(base/'audience-review-status.json',pending)
                print(json.dumps(pending,ensure_ascii=False));return
            if prior.exists() and (base/'report-receipt.json').exists() and load(prior)['sync']['status']=='complete' and load(prior).get('image_review_rule')==IMAGE_RULE and load(prior).get('selection_sha256')==sha(base/'selection.json'):
                print(json.dumps({'status':'already_complete','directory':str(base),'target_date':day},ensure_ascii=False))
                return
            client,synced=sync(base,selection,config,day)
            sent=report(base,selection,config,client,synced,day)
            save(base/'daily-result.json',{'target_date':day,'collection_status':selection.get('collection_status','complete'),
                  'media_review_status':selection.get('media_review_status','complete'),'media_unavailable_ids':selection.get('media_unavailable_ids',[]),
                  'video_duration_filter':selection.get('video_duration_filter'),'video_duration_excluded':selection.get('video_duration_excluded',0),
                  'delivery_status':'partial' if selection.get('media_unavailable_notes') or selection.get('collection_status')=='partial' or synced['status']!='complete' else 'complete',
                  'incomplete_keywords':selection.get('incomplete_keywords',[]),'image_review_rule':IMAGE_RULE,'selection_sha256':sha(base/'selection.json'),'sync':synced,'report':sent})
            print(json.dumps({'target_date':day,'directory':str(base),'sync':synced,'report':sent},ensure_ascii=False))
            if synced['status']!='complete':raise SystemExit(1)


if __name__=='__main__':main()
