"""Bounded CDN transfers with verified ranges, entity-bound checkpoints and atomic delivery."""
from contextlib import contextmanager
import fcntl
import hashlib
import json
from pathlib import Path
import re
import time
from urllib.parse import urlsplit, urljoin
from uuid import uuid4

import requests

from ua_workflows.collection_runtime import exception_info, stamp, write

MAX_BYTES=512*1024*1024


class MediaIntegrityError(ValueError):pass


def digest(data):return hashlib.sha256(data).hexdigest()


def file_digest(path):
    value=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b''):value.update(chunk)
    return value.hexdigest()


def media_get(url, *, direct=False, **kwargs):
    if not direct:return requests.get(url,**kwargs)
    session=requests.Session();session.trust_env=False
    try:response=session.get(url,**kwargs)
    except BaseException:session.close();raise
    close=response.close
    def close_all():
        try:close()
        finally:session.close()
    response.close=close_all
    return response


def validate_url(url,hosts):
    parsed=urlsplit(url);host=parsed.hostname or ''
    if parsed.scheme not in ('http','https') or parsed.username or parsed.password or not any(host==h or host.endswith('.'+h) for h in hosts):
        raise ValueError('Unexpected media host')


@contextmanager
def opened(url,hosts,get,**kwargs):
    # Validate every redirect, including redirects reached through a signed CDN URL.
    for _ in range(5):
        validate_url(url,hosts)
        response=get(url,allow_redirects=False,**kwargs)
        if response.status_code in (301,302,303,307,308):
            try:url=urljoin(url,response.headers['Location'])
            finally:response.close()
            continue
        with response:yield response
        return
    raise MediaIntegrityError('Too many media redirects')


def read_range(response,length):
    data=bytearray()
    for chunk in response.iter_content(chunk_size=min(length,65536)):
        data.extend(chunk)
        if len(data)>length:raise MediaIntegrityError('Media range exceeded expected length')
    if len(data)!=length:raise requests.exceptions.ChunkedEncodingError('Incomplete media byte range')
    return bytes(data)


def download(url,path,*,allowed_hosts,referer,get=media_get,chunk_size=65536,min_chunk=8192):
    validate_url(url,allowed_hosts);path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    partial=path.with_suffix(path.suffix+'.part');checkpoint=path.with_suffix(path.suffix+'.transfer.json')
    log=path.with_suffix(path.suffix+'.transfer-log.json');source_hash=digest(url.encode())
    with path.with_suffix(path.suffix+'.download.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        state=json.loads(checkpoint.read_text()) if checkpoint.exists() else None
        if path.exists():
            if path.stat().st_size<=0:raise MediaIntegrityError('Empty cached media')
            if state:
                if state['source_hash']!=source_hash:raise MediaIntegrityError('Cached media source changed')
                if state.get('sha256') and file_digest(path)!=state['sha256']:raise MediaIntegrityError('Cached media hash changed')
                if state.get('total') and path.stat().st_size!=state['total']:raise MediaIntegrityError('Cached media size changed')
            return
        events=[];preferred=bool(state and state.get('direct'));headers={'Accept-Encoding':'identity','Referer':referer,'User-Agent':'Mozilla/5.0'}
        def event(**values):
            events.append({'at':stamp(),**values});write(log,events)
        def archive():
            for old in (partial,checkpoint):
                if old.exists():old.rename(old.with_name(old.name+'.saved-'+uuid4().hex))
        def full(response,direct):
            expected=response.headers.get('Content-Length')
            if expected is not None and (not expected.isdigit() or not 0<int(expected)<=MAX_BYTES):raise MediaIntegrityError('Invalid or oversized media length')
            fullpart=path.with_suffix(path.suffix+'.full.part');size=0;started=time.monotonic()
            with fullpart.open('wb') as stream:
                for data in response.iter_content(chunk_size=65536):
                    size+=len(data)
                    if size>MAX_BYTES or time.monotonic()-started>300:raise MediaIntegrityError('Media transfer size/time limit exceeded')
                    stream.write(data)
                stream.flush()
                import os;os.fsync(stream.fileno())
            if size==0 or (expected is not None and size!=int(expected)):raise requests.exceptions.ChunkedEncodingError('Incomplete media response')
            archive();write(checkpoint,{'source_hash':source_hash,'total':size,'sha256':file_digest(fullpart),'complete':True,'direct':direct})
            fullpart.replace(path)
        # Probe for real range support. A 200 response is handled as a complete fresh download.
        probe=None
        for direct in (preferred,not preferred):
            try:
                with opened(url,allowed_hosts,get,direct=direct,headers={**headers,'Range':'bytes=0-0'},stream=True,timeout=(5,8)) as response:
                    response.raise_for_status()
                    if response.headers.get('Content-Encoding') not in (None,'identity'):raise MediaIntegrityError('Encoded range response is not byte-addressable')
                    if response.status_code==200:
                        full(response,direct);event(stage='complete_full',direct=direct,bytes=path.stat().st_size);return
                    match=re.fullmatch(r'bytes 0-0/(\d+)',response.headers.get('Content-Range',''))
                    if response.status_code!=206 or not match:raise MediaIntegrityError('CDN returned an unexpected byte range')
                    total=int(match[1])
                    if not 0<total<=MAX_BYTES:raise MediaIntegrityError('Media exceeds size limit')
                    read_range(response,1)
                    probe={'total':total,'etag':response.headers.get('ETag'),'last_modified':response.headers.get('Last-Modified')};preferred=direct
                break
            except requests.RequestException as exc:
                event(stage='probe',direct=direct,exception=exception_info(exc))
                if direct==(not bool(state and state.get('direct'))):raise
        identity={'source_hash':source_hash,**probe}
        # Without a validator a cross-process resume cannot prove the entity stayed unchanged.
        resumable=bool(probe['etag'] and not probe['etag'].startswith('W/')) or bool(probe['last_modified'])
        if state and (any(state.get(k)!=v for k,v in identity.items()) or not resumable):
            archive();state=None
        if not state:
            if partial.exists():archive()
            state={**identity,'offset':0,'segments':[],'direct':preferred,'chunk_size':chunk_size}
            write(checkpoint,state)
        offset=state['offset'];segments=state['segments']
        if offset:
            if not partial.exists() or partial.stat().st_size<offset:raise MediaIntegrityError('Partial media is shorter than its checkpoint')
            position=0
            with partial.open('rb') as stream:
                for seg in segments:
                    if seg['start']!=position or digest(stream.read(seg['size']))!=seg['sha256']:raise MediaIntegrityError('Partial media checkpoint hash mismatch')
                    position+=seg['size']
            if position!=offset:raise MediaIntegrityError('Partial media offsets mismatch')
        if partial.exists():
            with partial.open('r+b') as stream:stream.truncate(offset)  # discard an uncommitted tail after a crash
        size=state.get('chunk_size',chunk_size)
        while offset<probe['total']:
            end=min(offset+size,probe['total'])-1;completed=False
            for direct in (preferred,not preferred):
                request_headers={**headers,'Range':f'bytes={offset}-{end}'}
                validator=probe['etag'] if probe['etag'] and not probe['etag'].startswith('W/') else probe['last_modified']
                if validator:request_headers['If-Range']=validator
                try:
                    with opened(url,allowed_hosts,get,direct=direct,headers=request_headers,stream=True,timeout=(5,12)) as response:
                        response.raise_for_status()
                        if response.status_code!=206 or response.headers.get('Content-Range')!=f"bytes {offset}-{end}/{probe['total']}":raise MediaIntegrityError('CDN returned an unexpected byte range')
                        if any(response.headers.get(header) not in (None,probe[field]) for header,field in [('ETag','etag'),('Last-Modified','last_modified')]):raise MediaIntegrityError('Remote media entity changed during download')
                        data=read_range(response,end-offset+1)
                    with partial.open('ab') as stream:
                        stream.write(data);stream.flush()
                        import os;os.fsync(stream.fileno())
                    segments.append({'start':offset,'size':len(data),'sha256':digest(data)});offset=end+1;preferred=direct
                    state.update(offset=offset,direct=preferred,chunk_size=size);write(checkpoint,state);completed=True
                    break
                except requests.RequestException as exc:event(stage='range',offset=offset,length=end-offset+1,direct=direct,exception=exception_info(exc))
            if not completed:
                if size<=min_chunk:raise requests.ReadTimeout('Media paths exhausted; checkpoint preserved')
                size=max(min_chunk,size//2);state['chunk_size']=size;write(checkpoint,state)
        if partial.stat().st_size!=probe['total']:raise MediaIntegrityError('Completed media size mismatch')
        state.update(sha256=file_digest(partial),complete=True);write(checkpoint,state)
        partial.replace(path);event(stage='complete_range',bytes=offset,direct=preferred,segments=len(segments))
