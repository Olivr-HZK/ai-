"""Replica's device authorization contract, independent of a browser daemon.

Only approval URL/user code are public. Device codes and bearer credentials
stay in memory or an owner-only credential file, never business checkpoints.
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import platform
import secrets
import stat
import time
from urllib.parse import urlsplit
import webbrowser

import requests

from ua_workflows.replica_studio_api import BASE_URL, ReplicaError


def token_path():
    from ua_workflows.shared.config import project_env_values
    configured = project_env_values().get('REPLICA_CLI_TOKEN_FILE')
    if configured:
        return Path(configured).expanduser()
    base = Path(os.environ.get('XDG_CONFIG_HOME') or Path.home() / '.config')
    return base / 'replica-studio' / 'cli.json'


def valid_token(value):
    return isinstance(value, str) and 0 < len(value) <= 8192 and all(33 <= ord(c) <= 126 for c in value)


def load_token(path=None, *, required=False):
    path = Path(path).expanduser() if path is not None else token_path()
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
    except FileNotFoundError:
        if not required:
            return None
        raise ReplicaError('authentication_required', 'Run replica-studio login to authorize this computer') from None
    except OSError:
        raise ReplicaError('invalid_credentials_file', 'CLI token file is not a readable owner-only regular file') from None
    info = os.fstat(descriptor)
    if (not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.getuid()
            or info.st_size > 65536):
        os.close(descriptor)
        raise ReplicaError('invalid_credentials_file', 'CLI token file must be owned by this user with permissions 0600')
    with os.fdopen(descriptor, 'r', encoding='utf-8') as handle:
        try:
            value = json.load(handle)
        except (ValueError, UnicodeError):
            raise ReplicaError('invalid_credentials_file', 'CLI token file is not valid JSON') from None
    if (not isinstance(value, dict) or value.get('server') not in {BASE_URL, BASE_URL + '/'}
            or not valid_token(value.get('token'))
            or ('account_id' in value and (not isinstance(value['account_id'], str) or not value['account_id']))):
        raise ReplicaError('invalid_credentials_file', 'CLI token file does not match the fixed Replica server contract')
    return value


def save_token(path, token, account_id):
    if not valid_token(token) or not isinstance(account_id, str) or not account_id:
        raise ReplicaError('invalid_credentials_file', 'Refusing to save an unverified CLI credential')
    path = Path(path).expanduser()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.' + secrets.token_hex(8) + '.tmp')
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, 'w', encoding='utf-8') as handle:
            json.dump({'server': BASE_URL, 'token': token, 'account_id': account_id}, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


class DeviceTokenSession(requests.Session):
    replica_device_auth = True

    def __init__(self, token):
        super().__init__()
        if not valid_token(token):
            raise ReplicaError('invalid_credentials_file', 'Invalid CLI credential')
        self._bearer = token
        self.trust_env = False  # Do not load .netrc or environment proxies.
        self.replica_device_auth = True
        self.replica_capabilities = ['translation_v1', 'portrait_upload_v1', 'device_auth_v1']

    def request(self, method, url, **kwargs):
        parsed = urlsplit(url)
        if (parsed.scheme != 'https' or parsed.netloc != urlsplit(BASE_URL).netloc or
                parsed.username or parsed.password or parsed.fragment or not parsed.path.startswith('/api/')):
            raise ReplicaError('unsupported_endpoint', 'CLI credential is restricted to the fixed Replica API origin')
        headers = dict(kwargs.pop('headers', {}) or {})
        if (any(key.lower() in {'authorization', 'cookie', 'host'} for key in headers)
                or any(key.lower() in {'authorization', 'cookie', 'host'} for key in self.headers)
                or kwargs.get('cookies') or kwargs.get('auth') or self.auth is not None):
            raise ReplicaError('unsupported_action', 'Cannot inject credentials into the CLI token transport')
        headers['Authorization'] = 'Bearer ' + self._bearer
        kwargs.update(headers=headers, allow_redirects=False)
        self.cookies.clear()
        try:
            return super().request(method, url, **kwargs)
        finally:
            self.cookies.clear()


def public_identity(value):
    """Project whoami into explicit public fields; never echo raw token metadata."""
    user = value.get('user')
    if (not isinstance(user, dict) or not isinstance(user.get('id'), str) or not user['id']
            or user.get('enabled') is False):
        raise ReplicaError('authentication_required', 'CLI identity response does not identify an enabled account')
    result = {'account_id': user['id'], 'auth_mode': 'device'}
    metadata = value.get('token')
    scopes = metadata.get('scopes') if isinstance(metadata, dict) else None
    if isinstance(scopes, list) and all(isinstance(scope, str) and scope in {'read', 'upload', 'run', 'download', 'delete'} for scope in scopes):
        result['scopes'] = list(dict.fromkeys(scopes))
    return result


def _authorization_json(session, path, body, *, wait, clock, max_wait):
    deadline = clock() + max_wait
    while True:
        try:
            response = session.post(BASE_URL + path, json=body, timeout=(20, 30), allow_redirects=False)
        except requests.RequestException:
            raise ReplicaError('read_failed', 'Device authorization service could not be reached') from None
        try:
            try:
                value = response.json()
            except (ValueError, TypeError):
                raise ReplicaError('read_failed', 'Device authorization returned invalid JSON') from None
            detail = value.get('detail') if isinstance(value, dict) else None
            preparing = (response.status_code == 503 and 'Retry-After' in response.headers
                         and isinstance(detail, str) and '工作区正在准备' in detail)
            if preparing and clock() < deadline:
                # The published CLI contract explicitly says this request was
                # not processed. This loop is ONLY for device start/poll, never
                # business POSTs, connection failures or generic 503 responses.
                try:
                    delay = float(response.headers['Retry-After'])
                except (ValueError, TypeError):
                    delay = 2
                delay = delay if math.isfinite(delay) else 2
                response.close()
                wait(min(max(1, min(10, delay)), deadline - clock()))
                continue
            if response.status_code in {401, 403}:
                raise ReplicaError('authentication_required', 'Device authorization was not accepted', response.status_code)
            if not 200 <= response.status_code < 300:
                raise ReplicaError('read_failed', 'Device authorization returned an unexpected HTTP status', response.status_code)
            if not isinstance(value, dict):
                raise ReplicaError('read_failed', 'Device authorization returned an invalid response')
            return value
        finally:
            response.close()


def _positive(value):
    return type(value) in {int, float} and math.isfinite(value) and value > 0


def start_device_login(*, path=None, label=None, timeout=600, no_browser=False, on_prompt=None,
                       session=None, wait=time.sleep, clock=time.monotonic, opener=None):
    if not _positive(timeout):
        raise ReplicaError('invalid_input', 'Login timeout must be a positive finite number')
    label = label if label is not None else 'Replica CLI @ ' + platform.node()
    if not isinstance(label, str) or not label.strip() or len(label) > 200 or any(ord(c) < 32 for c in label):
        raise ReplicaError('invalid_input', 'Device label must be a short, nonempty name')
    session = session if session is not None else requests.Session()
    session.trust_env = False
    began = clock()
    started = _authorization_json(session, '/api/cli/login/start', {'label': label},
                                  wait=wait, clock=clock, max_wait=min(60, timeout))
    device_code, user_code, uri = (started.get(key) for key in ('device_code', 'user_code', 'verification_uri'))
    expires, interval = started.get('expires_in'), started.get('interval', 3)
    target = urlsplit(uri) if isinstance(uri, str) else None
    if (not valid_token(device_code) or not isinstance(user_code, str) or not 0 < len(user_code) <= 64
            or not all(c.isascii() and (c.isalnum() or c in '- ') for c in user_code)
            or not target or target.scheme != 'https' or target.netloc != urlsplit(BASE_URL).netloc
            or target.username or target.password or target.fragment
            or not (target.path == '/cli' or target.path.startswith('/cli/'))
            or device_code in uri or len(uri) > 4096 or not _positive(expires) or not _positive(interval)):
        raise ReplicaError('read_failed', 'Device authorization response does not match the published contract')
    deadline = min(began + timeout, clock() + expires)
    public = {'verification_uri': uri, 'user_code': user_code}
    if on_prompt is not None:
        on_prompt(public)
    if not no_browser:
        try:
            (opener or webbrowser.open)(uri)
        except Exception:
            pass  # The same public approval URL is already shown to the user.
    delay = min(30, max(1, interval))
    while clock() < deadline:
        wait(min(delay, deadline - clock()))
        if clock() >= deadline:
            break
        result = _authorization_json(session, '/api/cli/login/poll', {'device_code': device_code},
                                     wait=wait, clock=clock, max_wait=min(60, deadline - clock()))
        state = result.get('state')
        if state == 'approved':
            token = result.get('token')
            if not valid_token(token):
                raise ReplicaError('read_failed', 'Approval did not return a valid CLI credential')
            from ua_workflows.replica_studio_api import ReplicaAPI
            api = ReplicaAPI(DeviceTokenSession(token), wait=wait)
            identity_deadline = min(deadline, clock() + 60)
            while True:
                try:
                    identity = api.authenticate()
                    break
                except ReplicaError as error:
                    if error.status != 503 or clock() >= identity_deadline:
                        raise
                    wait(min(5, identity_deadline - clock()))  # GET identity is safe to re-read.
            destination = Path(path).expanduser() if path is not None else token_path()
            save_token(destination, token, identity['account_id'])
            return {'operation': 'login', 'cli_authenticated': True, 'authenticated': True,
                    **identity, 'token_file': str(destination)}
        if state in {'denied', 'expired'}:
            raise ReplicaError('authorization_denied' if state == 'denied' else 'authentication_required',
                               'Device authorization was denied' if state == 'denied' else 'Device authorization expired')
        if state not in {'pending', 'authorization_pending', 'slow_down'}:
            raise ReplicaError('read_failed', 'Device authorization returned an unrecognized state')
        if state == 'slow_down':
            delay = min(30, delay + 5)
    return {'operation': 'login', 'cli_authenticated': False, 'authenticated': False,
            'status': 'login_wait_timeout', **public}


def logout(path=None):
    destination = Path(path).expanduser() if path is not None else token_path()
    existed = load_token(destination) is not None
    if existed:
        destination.unlink()
    return {'operation': 'logout', 'local_token_removed': existed, 'server_token_revoked': False}
