"""Run social daily regressions with remote DNS, network and local-proxy egress blocked."""
from pathlib import Path
import socket
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))


def main():
    import pytest
    bound=set()
    original_bind=socket.socket.bind
    original_connect=socket.socket.connect
    original_connect_ex=socket.socket.connect_ex
    original_lookup=socket.getaddrinfo
    loopback=('127.0.0.1','::1','localhost')
    def bind(self,address):
        result=original_bind(self,address)
        if isinstance(address,tuple):bound.add(self.getsockname()[1])
        return result
    def connect(method):
        def guarded(self,address):
            if isinstance(address,tuple) and (address[0] not in loopback or address[1] not in bound):
                raise AssertionError('Offline tests may only connect to their own local servers; proxies are blocked')
            return method(self,address)
        return guarded
    def lookup(host,*a,**kw):
        if host not in loopback and host not in (None,''):
            raise AssertionError('Remote DNS is disabled in offline tests')
        return original_lookup(host,*a,**kw)
    socket.socket.bind=bind
    socket.socket.connect=connect(original_connect)
    socket.socket.connect_ex=connect(original_connect_ex)
    socket.getaddrinfo=lookup
    files=['test_everyinfra_jobs','test_resumable_media','test_collection_power','test_collection_runtime',
           'test_daily_runtime_integration','test_douyin_daily','test_xhs_daily','test_xhs_link_bot',
           'test_social_share_formats','test_social_selection']
    try:
        for probe in (lambda:socket.getaddrinfo('api.everyinfra.com',443),
                      lambda:socket.socket().connect(('127.0.0.1',7897))):
            try:probe()
            except AssertionError:pass
            else:raise RuntimeError('Offline network guard self-check failed')
        return pytest.main(['-q',*[str(ROOT/'tests'/(f+'.py')) for f in files]])
    finally:
        socket.socket.bind=original_bind;socket.socket.connect=original_connect
        socket.socket.connect_ex=original_connect_ex;socket.getaddrinfo=original_lookup


if __name__=='__main__':raise SystemExit(main())
