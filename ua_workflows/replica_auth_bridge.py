"""CLI-owned login context and fixed-origin HTTP transport; never exports cookies.

The user performs the server-managed OIDC browser login. Business calls use the
context's APIRequestContext, not page buttons, DOM evaluation or page fetch.
"""
from __future__ import annotations

import asyncio
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import socketserver
import stat
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit

import requests

from ua_workflows.shared.config import PROJECT_ROOT
from ua_workflows.replica_studio_api import BASE_URL, allowed, media_url, validate_workflow_body, portrait_path

RUNTIME = PROJECT_ROOT / 'data/replica-cli'
CHUNK_SIZE = 8 * 1024 * 1024
SOCKET = Path('/tmp') / f'replica-cli-{os.getuid()}-{hashlib.sha256(str(PROJECT_ROOT).encode()).hexdigest()[:10]}.sock'
SAFE_HEADERS = {'accept', 'content-type', 'origin', 'range', 'x-replica-csrf', 'x-replica-account', 'x-replica-upload-id'}


def rpc(value, timeout=200):
    if not SOCKET.exists():
        raise requests.ConnectionError('Replica login bridge is not running')
    info = SOCKET.lstat()
    if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise requests.ConnectionError('Replica login bridge socket is not private')
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(timeout)
            connection.connect(str(SOCKET))
            stream = connection.makefile('rwb')
            stream.write(json.dumps(value).encode() + b'\n')
            stream.flush()
            line = stream.readline(180 * 1024 * 1024)
            result = json.loads(line)
    except (OSError, ValueError):
        raise requests.ConnectionError('Replica login bridge could not be reached') from None
    if result.get('error'):
        raise requests.ConnectionError('Replica authenticated transport failed')
    return result


def bridge_status():
    try:
        return rpc({'action': 'status'}, timeout=15)
    except requests.RequestException:
        return None


class BridgeResponse:
    def __init__(self, value):
        self.status_code = value['status']
        self.headers = value.get('headers', {})
        self.body = base64.b64decode(value.get('body', ''), validate=True)

    def json(self):
        return json.loads(self.body)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def iter_content(self, chunk_size):
        for start in range(0, len(self.body), chunk_size):
            yield self.body[start:start + chunk_size]


class BridgeMediaResponse:
    """Range reads bound APIResponse buffering; the caller still receives a stream."""
    def __init__(self, session, url, kwargs):
        self.session, self.url, self.kwargs = session, url, kwargs
        self.current = self._read(0)
        self.offset, self.total = 0, None
        if self.current.status_code == 206:
            match = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)', self.current.headers.get('content-range', ''))
            if not match or int(match[1]) != 0 or int(match[2]) + 1 != len(self.current.body):
                raise requests.ConnectionError('Invalid media range response')
            self.total = int(match[3])
            self.status_code = 200
        else:
            self.status_code = self.current.status_code

    def _read(self, offset):
        headers = {**self.kwargs.get('headers', {}), 'Range': f'bytes={offset}-{offset + CHUNK_SIZE - 1}'}
        return self.session.request('GET', self.url, headers=headers)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def iter_content(self, chunk_size):
        current = self.current
        while True:
            for chunk in current.iter_content(chunk_size):
                yield chunk
            self.offset += len(current.body)
            if self.total is None or self.offset == self.total:
                return
            if not current.body or self.offset > self.total:
                raise requests.ConnectionError('Incomplete media range response')
            current = self._read(self.offset)
            match = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)', current.headers.get('content-range', ''))
            if (current.status_code != 206 or not match or int(match[1]) != self.offset or
                    int(match[3]) != self.total or int(match[2]) - int(match[1]) + 1 != len(current.body)):
                raise requests.ConnectionError('Media range identity changed')


class BridgeSession:
    def request(self, method, url, **kwargs):
        target = urlsplit(url)
        if target.scheme != 'https' or target.netloc != urlsplit(BASE_URL).netloc:
            raise requests.ConnectionError('Unexpected transport origin')
        payload = {'action': 'request', 'method': method, 'path': target.path + ('?' + target.query if target.query else ''),
                   'headers': kwargs.get('headers', {})}
        if 'json' in kwargs:
            payload['json'] = kwargs['json']
        if 'files' in kwargs:
            payload['form'] = kwargs.get('data', {})
            payload['files'] = {}
            for field, (name, stream, mime) in kwargs['files'].items():
                data = stream.read(100 * 1024 * 1024 + 1) if hasattr(stream, 'read') else stream
                if len(data) > 100 * 1024 * 1024:
                    raise requests.ConnectionError('Upload exceeds documented size')
                payload['files'][field] = {'name': name, 'mimeType': mime, 'base64': base64.b64encode(data).decode()}
        return BridgeResponse(rpc(payload))

    def get(self, url, **kwargs):
        if kwargs.get('stream'):
            return BridgeMediaResponse(self, url, kwargs)
        return self.request('GET', url, **kwargs)


def start_login(timeout=600):
    RUNTIME.mkdir(parents=True, exist_ok=True)
    RUNTIME.chmod(0o700)
    status = bridge_status()
    if status and status.get('authenticated'):
        return status
    if status:
        rpc({'action': 'begin_login'}, timeout=60)
    else:
        log_path = RUNTIME / 'login-bridge.log'
        with log_path.open('a') as log:
            os.chmod(log_path, 0o600)
            keys = ('PATH', 'HOME', 'USER', 'LOGNAME', 'TMPDIR', 'LANG', 'LC_ALL', 'HTTP_PROXY', 'HTTPS_PROXY',
                    'ALL_PROXY', 'NO_PROXY', 'http_proxy', 'https_proxy', 'all_proxy', 'no_proxy')
            environment = {key: os.environ[key] for key in keys if key in os.environ}
            subprocess.Popen([sys.executable, '-m', 'ua_workflows.replica_auth_bridge'], cwd=PROJECT_ROOT,
                             env=environment, stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        status = bridge_status()
        if status and status.get('authenticated'):
            return status
        time.sleep(2)
    return {'authenticated': False, 'status': 'login_wait_timeout', 'login_url': BASE_URL + '/api/auth/oidc/login'}


class Broker:
    def __init__(self, context, loop):
        self.context, self.loop = context, loop

    async def dispatch(self, value):
        if value.get('action') == 'begin_login':
            page = self.context.pages[0] if self.context.pages else await self.context.new_page()
            await page.goto(BASE_URL + '/api/auth/oidc/login', wait_until='domcontentloaded', timeout=45000)
            return {'opened': True}
        if value.get('action') == 'status':
            response = await self.context.request.get(BASE_URL + '/api/auth/session', headers={'Origin': BASE_URL},
                                                      timeout=10000, max_redirects=0, max_retries=0)
            try:
                auth = await response.json()
                user = auth.get('user') or {}
                return {'authenticated': bool(user.get('id') and user.get('enabled') and auth.get('csrf_token')),
                        'account_id': user.get('id'), 'auth_mode': auth.get('auth_mode'), 'transport': 'cli_owned_http_context',
                        'capabilities': ['translation_v1', 'portrait_upload_v1']}
            finally:
                await response.dispose()
        if value.get('action') != 'request':
            raise ValueError('Unsupported local operation')
        method, path = value.get('method'), value.get('path', '')
        parsed = urlsplit(path)
        media = re.fullmatch(r'/api/jobs/([A-Za-z0-9_-]{1,64})/files/.+', parsed.path)
        if parsed.scheme or parsed.netloc or not (allowed(method, parsed.path) or (method == 'GET' and media)):
            raise ValueError('Unsupported API operation')
        if media:
            media_url(BASE_URL + path, media[1])
        validate_workflow_body(method, parsed.path, value.get('json'))
        portrait = bool(portrait_path(parsed.path))
        if portrait and (not value.get('files') or value.get('form') or 'json' in value):
            raise ValueError('Portrait replacement requires one multipart image without extra fields')
        headers = value.get('headers') or {}
        if any(key.lower() not in SAFE_HEADERS for key in headers):
            raise ValueError('Unsupported request header')
        headers['Origin'] = BASE_URL
        kwargs = {'method': method, 'headers': headers, 'timeout': 180000, 'max_redirects': 0,
                  'max_retries': 0, 'fail_on_status_code': False}
        if value.get('files'):
            field_name = 'file' if portrait else 'files'
            if method != 'POST' or not (portrait or parsed.path == '/api/jobs') or set(value['files']) != {field_name}:
                raise ValueError('Unsupported multipart operation')
            multipart = {**value.get('form', {})}
            for field, file in value['files'].items():
                data = base64.b64decode(file['base64'], validate=True)
                if len(data) > (30 if portrait else 100) * 1024 * 1024:
                    raise ValueError('Upload too large')
                if portrait and (not data or file['mimeType'] not in {'image/png', 'image/jpeg', 'image/webp'}):
                    raise ValueError('Invalid portrait upload')
                multipart[field] = {'name': file['name'], 'mimeType': file['mimeType'], 'buffer': data}
            kwargs['multipart'] = multipart
        elif 'json' in value:
            kwargs['data'] = json.dumps(value['json'])
            headers['Content-Type'] = 'application/json'
        response = await self.context.request.fetch(BASE_URL + path, **kwargs)
        try:
            body = await response.body()
            if media and len(body) > CHUNK_SIZE:
                raise ValueError('Media range limit exceeded')
            if not media and len(body) > 16 * 1024 * 1024:
                raise ValueError('JSON response limit exceeded')
            # Set-Cookie is handled inside the owned context, never exported to the CLI.
            result = {'status': response.status, 'headers': {key: val for key, val in response.headers.items()
                      if key.lower() in {'content-type', 'content-length', 'content-range', 'etag'}},
                      'body': base64.b64encode(body).decode()}
            with (RUNTIME / 'http-audit.jsonl').open('a') as audit:
                os.chmod(audit.name, 0o600)
                audit.write(json.dumps({'time': time.time(), 'method': method, 'path': parsed.path, 'status': response.status}) + '\n')
            return result
        finally:
            await response.dispose()


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        try:
            value = json.loads(self.rfile.readline(150 * 1024 * 1024))
            future = asyncio.run_coroutine_threadsafe(self.server.broker.dispatch(value), self.server.broker.loop)
            result = future.result(timeout=195)
        except Exception:
            result = {'error': 'bridge_request_failed'}
        self.wfile.write(json.dumps(result).encode() + b'\n')


class Server(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True


async def serve():
    from playwright.async_api import async_playwright
    RUNTIME.mkdir(parents=True, exist_ok=True)
    RUNTIME.chmod(0o700)
    lock = (RUNTIME / 'bridge.lock').open('a')
    os.chmod(lock.name, 0o600)
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if SOCKET.exists():
        info = SOCKET.lstat()
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid():
            raise RuntimeError('Unsafe bridge socket')
        SOCKET.unlink()
    async with async_playwright() as playwright:
        context = await playwright.chromium.launch_persistent_context(
            str(RUNTIME / 'browser-profile'), channel='chrome', headless=False,
            chromium_sandbox=True, ignore_https_errors=False, bypass_csp=False)
        broker = Broker(context, asyncio.get_running_loop())
        server = Server(str(SOCKET), Handler)
        SOCKET.chmod(0o600)
        server.broker = broker
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        closed = asyncio.Event()
        context.on('close', lambda _: closed.set())
        try:
            try:
                await broker.dispatch({'action': 'begin_login'})
            except Exception:
                pass  # User can still finish login in the already-opened window.
            await closed.wait()
        finally:
            server.shutdown()
            server.server_close()
            SOCKET.unlink(missing_ok=True)
            lock.close()


if __name__ == '__main__':
    asyncio.run(serve())
