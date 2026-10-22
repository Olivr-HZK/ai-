from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import json
import re
import threading

import pytest
import requests

from ua_workflows import resumable_media as media


@pytest.fixture
def cdn():
    state={'data':bytes(range(256))*1024,'etag':'"v1"','fail':False,'wrong':False,'full':False,'calls':[], 'truncate':False}
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*a):pass
        def do_GET(self):
            if self.path.startswith('/redirect'):
                self.send_response(302);self.send_header('Location','http://untrusted.invalid/secret');self.end_headers();return
            start,end=map(int,re.fullmatch(r'bytes=(\d+)-(\d+)',self.headers['Range']).groups());end=min(end,len(state['data'])-1)
            state['calls'].append((start,end))
            if state['full']:
                self.send_response(200);self.send_header('Content-Length',str(len(state['data'])));self.end_headers()
                self.wfile.write(state['data'][:5] if state['truncate'] else state['data']);return
            self.send_response(206);headerstart=0 if state['wrong'] and start else start
            self.send_header('Content-Range',f'bytes {headerstart}-{end}/{len(state["data"])}')
            self.send_header('Content-Length',str(end-start+1))
            if state['etag']:self.send_header('ETag',state['etag'])
            self.end_headers()
            data=state['data'][start:end+1]
            self.wfile.write(data[:5] if state['fail'] and start>=65536 else data)
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    session=requests.Session();session.trust_env=False
    def get(url,direct=False,**kwargs):return session.get(url,**kwargs)
    try:yield state,f'http://127.0.0.1:{server.server_port}/media?token=private-signature',get
    finally:session.close();server.shutdown();server.server_close();thread.join()


def download(cdn,path):
    state,url,get=cdn
    return media.download(url,path,allowed_hosts=('127.0.0.1',),referer='https://example.com/',get=get)


def interrupted(cdn,path):
    cdn[0]['fail']=True
    with pytest.raises(requests.RequestException):download(cdn,path)
    assert not path.exists()
    saved=json.loads(path.with_suffix('.mp4.transfer.json').read_text())
    assert saved['offset']==65536 and len(saved['segments'])==1
    cdn[0]['fail']=False;cdn[0]['calls'].clear()


def test_real_http_resume_uses_only_verified_offset_and_clips_uncommitted_tail(cdn,tmp_path):
    path=tmp_path/'video.mp4';interrupted(cdn,path)
    partial=path.with_suffix('.mp4.part')
    with partial.open('ab') as f:f.write(b'uncommitted tail')
    download(cdn,path)
    assert path.read_bytes()==cdn[0]['data']
    assert cdn[0]['calls'][0]==(0,0) and cdn[0]['calls'][1][0]==65536
    assert not partial.exists()
    log=path.with_suffix('.mp4.transfer-log.json').read_text()
    assert 'private-signature' not in log
    before=list(cdn[0]['calls']);download(cdn,path);assert cdn[0]['calls']==before


def test_changed_entity_restarts_without_mixing_bytes(cdn,tmp_path):
    path=tmp_path/'video.mp4';interrupted(cdn,path)
    cdn[0].update(etag='"v2"',data=b'n'*len(cdn[0]['data']))
    download(cdn,path)
    assert path.read_bytes()==cdn[0]['data'] and cdn[0]['calls'][1][0]==0
    assert list(tmp_path.glob('*.saved-*'))


def test_changed_partial_hash_is_rejected(cdn,tmp_path):
    path=tmp_path/'video.mp4';interrupted(cdn,path)
    partial=path.with_suffix('.mp4.part');data=partial.read_bytes();partial.write_bytes(b'bad'+data[3:])
    with pytest.raises(media.MediaIntegrityError,match='hash'):download(cdn,path)
    assert not path.exists()


def test_wrong_range_cannot_be_promoted(cdn,tmp_path):
    cdn[0]['wrong']=True;path=tmp_path/'video.mp4'
    with pytest.raises(media.MediaIntegrityError,match='unexpected byte range'):download(cdn,path)
    assert not path.exists()


def test_range_ignored_full_body_is_atomic_and_size_checked(cdn,tmp_path):
    cdn[0]['full']=True;path=tmp_path/'image.png';download(cdn,path)
    assert path.read_bytes()==cdn[0]['data']
    path.unlink();cdn[0]['truncate']=True
    with pytest.raises(requests.RequestException):download(cdn,path)
    assert not path.exists()


def test_no_validator_does_not_trust_cross_process_partial(cdn,tmp_path):
    cdn[0]['etag']=None;path=tmp_path/'video.mp4';interrupted(cdn,path);download(cdn,path)
    assert cdn[0]['calls'][1][0]==0 and path.read_bytes()==cdn[0]['data']


def test_redirect_host_is_checked_before_request(cdn,tmp_path):
    state,url,get=cdn
    with pytest.raises(ValueError,match='host'):
        media.download(url.replace('/media','/redirect'),tmp_path/'video.mp4',allowed_hosts=('127.0.0.1',),referer='',get=get)
    assert state['calls']==[]


def test_downsize_stalled_ranges_and_pin_working_direct_route(cdn,tmp_path):
    state,url,get=cdn;calls=[]
    def flaky(url,direct=False,headers=None,**kw):
        start,end=map(int,headers['Range'].removeprefix('bytes=').split('-'));calls.append((start,end,direct))
        if not direct:raise requests.exceptions.ProxyError('proxy down')
        if end-start+1>8192:raise requests.ReadTimeout('large range stalls')
        return get(url,direct=direct,headers=headers,**kw)
    path=tmp_path/'video.mp4'
    media.download(url,path,allowed_hosts=('127.0.0.1',),referer='',get=flaky)
    assert path.read_bytes()==state['data']
    state2=json.loads(path.with_suffix('.mp4.transfer.json').read_text())
    assert state2['chunk_size']==8192 and state2['direct'] is True
