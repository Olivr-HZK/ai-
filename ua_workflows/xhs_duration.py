"""Pinned maximum duration, measured from original video streams before decode/review."""
from pathlib import Path
import hashlib
import json
import math
import subprocess
from fractions import Fraction

from ua_workflows.collection_runtime import write

POLICY={'rule':'video_stream_duration_max300_v1','max_seconds':300}


def load(path):return json.loads(Path(path).read_text())


def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate_policy(value):
    if value!=POLICY:raise ValueError('Unsupported pinned video duration filter')
    return dict(value)


def policy_for(base):
    path=Path(base)/'run.json'
    value=load(path).get('video_duration_filter') if path.exists() else None
    return validate_policy(value) if value is not None else None


class DurationExcluded(ValueError):
    """An observed stream exceeds the authorized limit; no acquisition retry."""


def stream_seconds(probe):
    stream=next(s for s in probe['streams'] if s.get('codec_type')=='video')
    value=stream.get('duration')
    if value in (None,'N/A'):
        if stream.get('duration_ts') is None or not stream.get('time_base'):
            raise ValueError('Actual video stream duration unavailable')
        value=int(stream['duration_ts'])*Fraction(stream['time_base'])
    seconds=float(value)
    if not math.isfinite(seconds) or seconds<=0:raise ValueError('Invalid actual video stream duration')
    return seconds


def check_video(note,base,source,probe=None):
    base=Path(base).resolve();source=Path(source).resolve();policy=policy_for(base)
    if not policy:return None
    if not source.is_relative_to(base):raise ValueError('Video duration source outside batch')
    folder=base/'media'/note['id'];checkfile=folder/'video-duration-check.json';rawfile=folder/'video-duration-probe.json'
    digest=sha(source);run_sha=sha(base/'run.json')
    if checkfile.exists():
        check=load(checkfile)
        if (check.get('note_id')!=note['id'] or check.get('policy')!=policy or check.get('run_sha256')!=run_sha
                or check.get('source')!=str(source.relative_to(base)) or check.get('source_sha256')!=digest
                or check.get('source_size')!=source.stat().st_size or not rawfile.is_file()
                or check.get('probe_sha256')!=sha(rawfile)):
            raise ValueError('Video duration evidence changed')
        raw=load(rawfile)
        if raw.get('source_sha256')!=digest:raise ValueError('Video duration probe source changed')
        seconds=stream_seconds(raw['probe'])
        if seconds!=check.get('duration_seconds') or check.get('decision')!=('exclude' if seconds>300 else 'keep'):
            raise ValueError('Video duration decision does not match actual probe')
    else:
        if probe is None:
            result=subprocess.run(['ffprobe','-v','error','-show_entries','stream=codec_type,duration,duration_ts,time_base',
                                   '-of','json',str(source)],capture_output=True,text=True,check=True,timeout=30)
            probe=json.loads(result.stdout)
        seconds=stream_seconds(probe)
        if sha(source)!=digest:raise ValueError('Video duration source changed during probe')
        write(rawfile,{'source_sha256':digest,'probe':probe})
        check={'note_id':note['id'],'policy':policy,'run_sha256':run_sha,'source':str(source.relative_to(base)),
               'source_sha256':digest,'source_size':source.stat().st_size,'probe_sha256':sha(rawfile),
               'duration_seconds':seconds,'decision':'exclude' if seconds>300 else 'keep',
               'reason':'video_stream_duration_exceeds_300_seconds' if seconds>300 else 'within_video_duration_limit'}
        write(checkfile,check)
    if seconds>300:raise DurationExcluded(note['id']+': actual video stream exceeds 300 seconds')
    return seconds


def excluded_notes(base,notes):
    base=Path(base).resolve()
    if not policy_for(base):return []
    excluded=[]
    for note in notes:
        if not note['is_video']:continue
        folder=base/'media'/note['id'];manifest=folder/'manifest.json';check=folder/'video-duration-check.json'
        if manifest.exists():
            m=load(manifest)
            if m.get('note_id')!=note['id'] or m.get('is_video') is not True or len(m.get('files',[]))!=1:
                raise ValueError('Video duration manifest identity mismatch')
            item=m['files'][0];source=(base/item['path']).resolve()
            if not source.is_relative_to(base) or sha(source)!=item['sha256']:
                raise ValueError('Video duration original media hash changed')
        elif check.exists():source=(base/load(check)['source']).resolve()
        else:continue
        try:check_video(note,base,source)
        except DurationExcluded:
            evidence=load(check)
            excluded.append({'note_id':note['id'],'duration_seconds':evidence['duration_seconds'],'max_seconds':300,
                             'reason':evidence['reason'],'evidence':str(check.relative_to(base)),'sha256':sha(check),
                             'visually_reviewed':False})
    return excluded
