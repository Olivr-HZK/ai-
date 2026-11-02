"""Direct HTTP client for Replica analysis, smart Logo and optional translation.

Authentication is the documented replica_session cookie, not an invented API key.
No business-page automation, external browser-profile access, cookie export or paid retries.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
import time
from pathlib import Path
from urllib.parse import quote, unquote, urljoin, urlsplit

import requests

BASE_URL = 'https://replica-studio.upupup.today'
BUSY = {'queued', 'analyzing', 'planning', 'making_images', 'making_portraits',
        'generating', 'exporting', 'translating', 'replacing_logo'}
TRANSLATION_LANGUAGES = frozenset('zh en ja de fr ko ru uk pt it es id nl tr fil ms el fi hr sk pl sv hi bg ro ar cs da ta hun vi th yue'.split())


def translation_options(languages, parts=('voice',), source_language=None):
    if (not isinstance(languages, (list, tuple)) or not languages or
            any(not isinstance(x, str) or x not in TRANSLATION_LANGUAGES for x in languages)):
        raise ReplicaError('invalid_translation', 'Specify supported target languages; source is not a translation language')
    if (not isinstance(parts, (list, tuple)) or not parts or
            any(not isinstance(x, str) or x not in {'text', 'voice', 'subtitles'} for x in parts) or
            'subtitles' in parts and set(parts) != {'subtitles'}):
        raise ReplicaError('invalid_translation', 'Subtitles must be selected alone; otherwise select text and/or voice')
    if source_language is not None and (not isinstance(source_language, str) or source_language not in TRANSLATION_LANGUAGES):
        raise ReplicaError('invalid_translation', 'Unsupported source language')
    result = {'languages': list(dict.fromkeys(languages)), 'parts': sorted(set(parts)), 'remember_parts': False}
    if source_language is not None:
        result['source_language'] = source_language
    return result


def validate_workflow_body(method, path, body):
    """Keep newly allowed writes narrow in both the SDK and owned HTTP bridge."""
    if method == 'PUT':
        if (not isinstance(body, dict) or set(body) != {'revision', 'video_route'} or
                body['video_route'] != 'source_video' or type(body['revision']) is not int or body['revision'] < 0):
            raise ReplicaError('unsupported_action', 'Only switching to source_video with the latest revision is allowed')
    if method == 'POST' and path.endswith('/actions/translate'):
        if (not isinstance(body, dict) or set(body) - {'languages', 'parts', 'source_language', 'force_new', 'remember_parts'} or
                body.get('force_new', False) is not False or body.get('remember_parts') is not False):
            raise ReplicaError('unsupported_action', 'Translation cannot force regeneration or change workspace preferences')
        translation_options(body.get('languages'), body.get('parts', ['voice']), body.get('source_language'))


class ReplicaError(RuntimeError):
    def __init__(self, code, message, status=None):
        super().__init__(message)
        self.code, self.status = code, status


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', value):
        raise ReplicaError('invalid_id', 'Invalid Replica task identifier')
    return value


def allowed(method, path):
    if method == 'GET':
        if path.startswith('/api/tasks?filter='):
            return path[len('/api/tasks?filter='):] in {'all', 'active', 'running', 'queued', 'review', 'completed', 'failed'}
        return path in {'/api/auth/session', '/api/settings', '/api/jobs', '/api/tasks',
                        '/api/deleted-projects', '/api/deleted-characters'} or bool(
            re.fullmatch(r'/api/jobs/[A-Za-z0-9_-]{1,64}', path) or
            re.fullmatch(r'/api/source-uploads/[a-f0-9]{32}', path))
    if method == 'PUT':
        return bool(re.fullmatch(r'/api/jobs/[A-Za-z0-9_-]{1,64}', path))
    return method == 'POST' and (path in {'/api/jobs', '/api/jobs/download/files'} or bool(
        re.fullmatch(r'/api/jobs/[A-Za-z0-9_-]{1,64}/actions/(logo_replace|translate)', path) or portrait_path(path)))


def portrait_path(path):
    return re.fullmatch(r'/api/jobs/[A-Za-z0-9_-]{1,64}/portrait/[A-Za-z0-9_-]{1,64}', path)


def prepare_portrait(path):
    """Read and inspect the exact upload bytes, without resizing or conversion."""
    path = Path(path).expanduser().resolve()
    formats = {'.png': ('png', 'image/png'), '.jpg': ('mjpeg', 'image/jpeg'),
               '.jpeg': ('mjpeg', 'image/jpeg'), '.webp': ('webp', 'image/webp')}
    if path.suffix.lower() not in formats or not path.is_file() or not 0 < path.stat().st_size <= 30 * 1024 * 1024:
        raise ReplicaError('invalid_portrait', 'Use a PNG/JPG/WebP image of at most 30 MiB')
    with path.open('rb') as stream:
        data = stream.read(30 * 1024 * 1024 + 1)
    if not 0 < len(data) <= 30 * 1024 * 1024:
        raise ReplicaError('invalid_portrait', 'Portrait exceeds 30 MiB')
    try:
        probe = subprocess.run(['ffprobe', '-v', 'error', '-select_streams', 'v:0',
                                '-show_entries', 'stream=codec_name,width,height', '-of', 'json', '-i', 'pipe:0'],
                               input=data, capture_output=True, timeout=20, check=True)
        stream = json.loads(probe.stdout)['streams'][0]
        width, height = stream['width'], stream['height']
        if (stream['codec_name'] != formats[path.suffix.lower()][0] or
                type(width) is not int or type(height) is not int or min(width, height) < 320):
            raise ValueError('Invalid image dimensions or encoding')
    except (OSError, ValueError, KeyError, IndexError, subprocess.SubprocessError):
        raise ReplicaError('invalid_portrait', 'Image must match its PNG/JPG/WebP extension and have a short side of at least 320 pixels; ffprobe is required') from None
    return {'data': data, 'mime': formats[path.suffix.lower()][1],
            'identity': {'path': str(path), 'name': path.name, 'bytes': len(data),
                         'sha256': hashlib.sha256(data).hexdigest(), 'width': width, 'height': height}}


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def save_private(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.tmp')
    with temporary.open('w', encoding='utf-8') as stream:
        os.chmod(temporary, 0o600)
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    # The dispatch journal must survive a crash after the rename and before a
    # business POST. Sync the directory entry as well as the temporary contents.
    directory = os.open(path.parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def media_url(value, job_id):
    url = urljoin(BASE_URL + '/', value)
    parsed = urlsplit(url)
    prefix = '/api/jobs/' + identifier(job_id) + '/files/'
    if (parsed.scheme != 'https' or parsed.netloc != urlsplit(BASE_URL).netloc or
            parsed.username or parsed.password or not parsed.path.startswith(prefix)):
        raise ReplicaError('invalid_media', 'Unexpected Replica media address')
    tail = unquote(parsed.path[len(prefix):])
    if not tail or any(part in {'.', '..', ''} for part in tail.replace('\\', '/').split('/')):
        raise ReplicaError('invalid_media', 'Unsafe Replica media path')
    return url


class ReplicaAPI:
    def __init__(self, session=None, *, expected_account=None, wait=time.sleep):
        self.session = session or requests.Session()
        self.expected_account = expected_account
        self.account = None
        self.csrf = None
        self.wait = wait

    @classmethod
    def configured(cls, *, session_file=None, expected_account=None):
        from ua_workflows.shared.config import project_env_values
        values = project_env_values()
        session_file = session_file or values.get('REPLICA_SESSION_FILE')
        if not session_file:
            from ua_workflows.shared.config import PROJECT_ROOT
            default = PROJECT_ROOT / 'data/replica-cli/session.json'
            session_file = default if default.is_file() else None
        if not session_file:
            from ua_workflows.replica_auth_bridge import bridge_status, BridgeSession, SOCKET
            status = bridge_status()
            if status and status.get('authenticated'):
                transport = BridgeSession()
                transport.replica_capabilities = status.get('capabilities', [])
                return cls(transport, expected_account=expected_account)
            if status is None and SOCKET.exists():
                raise ReplicaError('read_failed', 'Replica authenticated transport is temporarily unavailable')
            raise ReplicaError('authentication_required', 'Run replica-studio login or configure an authorized HTTP session')
        path = Path(session_file).expanduser()
        try:
            info = path.stat()
        except OSError:
            raise ReplicaError('authentication_required', 'Replica session file is not available') from None
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_size > 65536:
            raise ReplicaError('invalid_credentials_file', 'Replica credential file must be an owner-only regular file')
        try:
            credential = json.loads(path.read_text())
        except (OSError, ValueError):
            raise ReplicaError('invalid_credentials_file', 'Replica session file could not be read as JSON') from None
        if not isinstance(credential, dict):
            raise ReplicaError('invalid_credentials_file', 'Replica session file must contain an object')
        cookie = credential.get('replica_session')
        if not isinstance(cookie, str) or not cookie or any(c in cookie for c in '\r\n;'):
            raise ReplicaError('invalid_credentials_file', 'Replica credential file has no valid session credential')
        session = requests.Session()
        session.cookies.set('replica_session', cookie, domain='replica-studio.upupup.today', path='/', secure=True)
        return cls(session, expected_account=expected_account)

    def login_start(self):
        """Inspect the documented OIDC browser entry without exposing transaction values."""
        try:
            response = self.session.get(BASE_URL + '/api/auth/oidc/login', timeout=(20, 30), allow_redirects=False)
        except requests.RequestException:
            raise ReplicaError('read_failed', 'OIDC login entry could not be reached') from None
        target = urlsplit(response.headers.get('Location', ''))
        if response.status_code != 303 or target.scheme != 'https' or target.netloc != 'auth.upupup.today':
            raise ReplicaError('unsupported_login', 'OIDC entry did not return the documented identity-provider redirect')
        return {'status': 'browser_login_required', 'login_url': BASE_URL + '/api/auth/oidc/login',
                'redirect_origin': target.scheme + '://' + target.netloc, 'cli_authenticated': False}

    def request(self, method, path, *, body=None, files=None, headers=None, safe_retry=False):
        if not allowed(method, path):
            raise ReplicaError('unsupported_endpoint', 'Endpoint outside analysis / smart-Logo / translation workflow')
        validate_workflow_body(method, path, body)
        if method == 'POST' and portrait_path(path):
            if not files or set(files) != {'file'} or body:
                raise ReplicaError('unsupported_action', 'Portrait replacement only accepts one multipart file')
        if safe_retry and method != 'GET' and path != '/api/jobs/download/files':
            raise ReplicaError('unsupported_action', 'Generation and route changes must not be automatically retried')
        write = method != 'GET'
        request_headers = {'Accept': 'application/json', 'Origin': BASE_URL, **(headers or {})}
        if write:
            if not self.account or not self.csrf:
                raise ReplicaError('authentication_required', 'Authenticate the HTTP session before submitting work')
            request_headers.update({'X-Replica-CSRF': self.csrf, 'X-Replica-Account': self.account})
        retry = method == 'GET' or safe_retry
        for attempt in range(3 if retry else 1):
            try:
                kwargs = {'headers': request_headers, 'timeout': (20, 180), 'allow_redirects': False}
                if files is not None:
                    kwargs.update(data=body, files=files)
                elif body is not None:
                    kwargs['json'] = body
                response = self.session.request(method, BASE_URL + path, **kwargs)
            except requests.RequestException:
                if retry and attempt < 2:
                    self.wait(2 * (attempt + 1))
                    continue
                raise ReplicaError('result_unknown' if write else 'read_failed', 'Replica request could not be confirmed') from None
            if response.status_code in {401, 403}:
                self.csrf = None
                raise ReplicaError('authentication_required', 'Replica session is missing, expired or not authorized', response.status_code)
            if response.status_code >= 500 and retry and attempt < 2:
                self.wait(2 * (attempt + 1))
                continue
            if not 200 <= response.status_code < 300:
                raise ReplicaError('result_unknown' if write and response.status_code >= 500 else 'http_error',
                                   f'Replica API returned HTTP {response.status_code}', response.status_code)
            try:
                data = response.json()
            except (ValueError, TypeError):
                raise ReplicaError('result_unknown' if write else 'read_failed', 'Replica response is not valid JSON') from None
            if method == 'GET' and path in {'/api/jobs', '/api/deleted-projects', '/api/deleted-characters'}:
                if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
                    raise ReplicaError('read_failed', 'Replica task list is not a JSON object array')
                return data
            if not isinstance(data, dict):
                raise ReplicaError('result_unknown' if write else 'read_failed', 'Replica response is not a JSON object')
            return data

    def authenticate(self):
        result = self.request('GET', '/api/auth/session')
        user = result.get('user') or {}
        if (not isinstance(user, dict) or not isinstance(user.get('id'), str) or not user['id'] or
                user.get('enabled') is not True or not isinstance(result.get('csrf_token'), str) or not result['csrf_token']):
            raise ReplicaError('authentication_required', 'A valid Replica session and CSRF value are required')
        if (self.expected_account and user['id'] != self.expected_account) or (self.account and user['id'] != self.account):
            raise ReplicaError('account_changed', 'Replica account does not match the pinned batch account')
        self.account, self.csrf = user['id'], result['csrf_token']
        return {'account_id': self.account, 'auth_mode': result.get('auth_mode')}

    def settings(self):
        return self.request('GET', '/api/settings')

    def smart_options(self):
        logo = self.settings().get('logo_replace') or {}
        if not logo.get('available'):
            raise ReplicaError('unavailable', 'Smart Logo processing is unavailable for this account')
        defaults = logo.get('defaults') or {}
        return {'use_defaults': True, 'logo_mode': 'smart', 'brand': '', **{
            k: defaults[k] for k in ('logo_id', 'replace_name', 'remove_cta', 'remove_gameplay', 'remove_watermarks', 'rename_voice') if k in defaults}}

    def upload(self, path, upload_id):
        path = Path(path)
        if not re.fullmatch(r'[a-f0-9]{32}', upload_id):
            raise ReplicaError('invalid_upload_id', 'Invalid upload identity')
        if path.suffix.lower() not in {'.mp4', '.mov', '.webm', '.mkv'} or not 0 < path.stat().st_size <= 100 * 1024 * 1024:
            raise ReplicaError('invalid_source', 'Source video does not meet the upload contract')
        with path.open('rb') as stream:
            result = self.request('POST', '/api/jobs', body={'variants': '1', 'character': '', 'auto_analyze': 'true'},
                                  files={'files': (path.name, stream, 'application/octet-stream')},
                                  headers={'X-Replica-Upload-Id': upload_id})
        return self.validate_upload(result, upload_id)

    @staticmethod
    def validate_upload(result, upload_id):
        ids = result.get('ids')
        if result.get('request_id') != upload_id or not isinstance(ids, list) or len(ids) != 1:
            raise ReplicaError('result_unknown', 'Upload result needs reconciliation')
        identifier(ids[0])
        return result

    def upload_status(self, upload_id):
        if not re.fullmatch(r'[a-f0-9]{32}', upload_id):
            raise ReplicaError('invalid_upload_id', 'Invalid upload identity')
        try:
            result = self.request('GET', '/api/source-uploads/' + upload_id)
        except ReplicaError as error:
            if error.status == 404 or error.status == 503 or error.code == 'read_failed':
                raise ReplicaError('result_unknown', 'Original upload cannot yet be confirmed; no repeated upload', error.status) from None
            raise
        return self.validate_upload(result, upload_id)

    def job(self, job_id):
        result = self.request('GET', '/api/jobs/' + identifier(job_id))
        if result.get('id') != job_id:
            raise ReplicaError('read_failed', 'Replica task identity mismatch')
        return result

    def list_jobs(self):
        result = self.request('GET', '/api/jobs')
        ids = [identifier(item.get('id')) for item in result]
        if len(ids) != len(set(ids)):
            raise ReplicaError('read_failed', 'Replica task list contains duplicate identifiers')
        return result

    def task_history(self, filter='all'):
        if filter not in {'all', 'active', 'running', 'queued', 'review', 'completed', 'failed'}:
            raise ReplicaError('invalid_filter', 'Unsupported task-history filter')
        result = self.request('GET', '/api/tasks' + ('?filter=' + filter if filter != 'all' else ''))
        if result.get('filter') != filter or not isinstance(result.get('tasks'), list):
            raise ReplicaError('read_failed', 'Replica task history does not match the requested filter')
        return result

    def deleted_jobs(self):
        return self.request('GET', '/api/deleted-characters')

    def deleted_projects(self):
        return self.request('GET', '/api/deleted-projects')

    def smart_logo(self, job, options):
        if options.get('logo_mode') != 'smart' or options.get('force_new'):
            raise ReplicaError('unsupported_action', 'Only smart Logo processing without forced regeneration is allowed')
        result = self.request('POST', '/api/jobs/' + identifier(job['id']) + '/actions/logo_replace',
                              body={**options, 'revision': job['revision']})
        if result.get('id') != job['id'] or not isinstance(result.get('status'), str):
            raise ReplicaError('result_unknown', 'Smart Logo receipt could not be confirmed')
        return {k: result[k] for k in ('id', 'status', 'revision', 'last_action', 'generation_skipped') if k in result}

    def require_translation_transport(self):
        capabilities = vars(self.session).get('replica_capabilities')
        if capabilities is not None and 'translation_v1' not in capabilities:
            raise ReplicaError('bridge_upgrade_required', 'Close the old CLI login window, then run replica-studio login to load translation support')

    def require_portrait_transport(self):
        capabilities = vars(self.session).get('replica_capabilities')
        if capabilities is not None and 'portrait_upload_v1' not in capabilities:
            raise ReplicaError('bridge_upgrade_required', 'Close the old CLI login window, then run replica-studio login to load portrait upload support')

    def upload_portrait(self, job_id, source_id, picture):
        self.require_portrait_transport()
        job_id, source_id = identifier(job_id), identifier(source_id)
        data, identity = picture['data'], picture['identity']
        if (not isinstance(data, bytes) or not 0 < len(data) <= 30 * 1024 * 1024 or
                len(data) != identity['bytes'] or hashlib.sha256(data).hexdigest() != identity['sha256']):
            raise ReplicaError('invalid_portrait', 'Portrait bytes do not match the prepared image')
        result = self.request('POST', '/api/jobs/' + job_id + '/portrait/' + source_id,
                              files={'file': (identity['name'], data, picture['mime'])})
        if result.get('id') != job_id or type(result.get('revision')) is not int:
            raise ReplicaError('result_unknown', 'Portrait upload receipt could not be confirmed')
        return result

    def translation_settings(self):
        self.require_translation_transport()
        return self.settings().get('translation') or {}

    def use_source_video(self, job):
        self.require_translation_transport()
        if not (job.get('source_story') or {}).get('available'):
            raise ReplicaError('translation_not_ready', 'The source story is not available; no analysis or generation was started')
        result = self.request('PUT', '/api/jobs/' + identifier(job['id']),
                              body={'revision': job['revision'], 'video_route': 'source_video'})
        if result.get('id') != job['id'] or not (result.get('source_story') or {}).get('active'):
            raise ReplicaError('result_unknown', 'Source-video route change could not be confirmed')
        return result

    def translate(self, job, options):
        self.require_translation_transport()
        options = {'remember_parts': False, **options}
        validate_workflow_body('POST', '/api/jobs/' + identifier(job['id']) + '/actions/translate', options)
        result = self.request('POST', '/api/jobs/' + job['id'] + '/actions/translate', body=options)
        if result.get('id') != job['id'] or not isinstance(result.get('status'), str):
            raise ReplicaError('result_unknown', 'Translation receipt could not be confirmed')
        return {k: result[k] for k in ('id', 'status', 'revision', 'last_action', 'generation_skipped') if k in result}

    def download_plan(self, ids, *, step='logo', languages=None):
        ids = list(dict.fromkeys(identifier(i) for i in ids))
        if not ids:
            raise ReplicaError('invalid_selection', 'No completed task was selected')
        languages = ['source'] if languages is None else languages
        if (step not in {'logo', 'translation'} or not isinstance(languages, (list, tuple)) or not languages or
                any(x not in TRANSLATION_LANGUAGES | {'source'} for x in languages) or
                step == 'logo' and list(languages) != ['source']):
            raise ReplicaError('invalid_selection', 'Invalid download step or languages')
        return self.request('POST', '/api/jobs/download/files', body={'ids': ids, 'languages': list(dict.fromkeys(languages)), 'step': step}, safe_retry=True)

    def download(self, job_id, file, destination):
        name, expected = file.get('name'), file.get('bytes')
        if not isinstance(name, str) or Path(name).name != name or '\\' in name or not isinstance(expected, int) or expected <= 0:
            raise ReplicaError('invalid_media', 'Invalid download metadata')
        url = media_url(file['url'], job_id)
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial = destination.with_name(destination.name + '.part')
        sha, size = hashlib.sha256(), 0
        try:
            with self.session.get(url, stream=True, timeout=(20, 180), allow_redirects=False) as response:
                if response.status_code != 200:
                    raise ReplicaError('read_failed', f'Replica media returned HTTP {response.status_code}', response.status_code)
                with partial.open('wb') as stream:
                    os.chmod(partial, 0o600)
                    for block in response.iter_content(1024 * 1024):
                        size += len(block)
                        if size > expected:
                            raise ReplicaError('invalid_media', 'Media exceeds declared byte size')
                        sha.update(block)
                        stream.write(block)
            if size != expected:
                raise ReplicaError('invalid_media', 'Media byte size does not match the download plan')
            partial.replace(destination)
        except Exception:
            partial.unlink(missing_ok=True)
            raise
        return {'path': str(destination), 'name': name, 'size': size, 'sha256': sha.hexdigest()}

    def source_matches(self, job_id, filename, expected_bytes, expected_sha256):
        """Hash a candidate's documented original-media GET without storing a copy."""
        if (not isinstance(filename, str) or Path(filename).name != filename or '\\' in filename
                or filename in {'', '.', '..'} or '%' in filename or type(expected_bytes) is not int
                or not 0 < expected_bytes <= 100 * 1024 * 1024
                or not isinstance(expected_sha256, str) or not re.fullmatch(r'[a-f0-9]{64}', expected_sha256)):
            raise ReplicaError('invalid_media', 'Invalid original-media identity')
        address = media_url('/api/jobs/' + identifier(job_id) + '/files/' + quote(filename, safe=''), job_id)
        return self._media_matches(address, expected_bytes, expected_sha256)

    def portrait_matches(self, job_id, filename, identity):
        if (not isinstance(filename, str) or not filename or type(identity.get('bytes')) is not int
                or not 0 < identity['bytes'] <= 30 * 1024 * 1024
                or not re.fullmatch(r'[a-f0-9]{64}', identity.get('sha256', ''))):
            raise ReplicaError('invalid_media', 'Invalid portrait identity')
        address = media_url('/api/jobs/' + identifier(job_id) + '/files/' + quote(filename, safe='/'), job_id)
        return self._media_matches(address, identity['bytes'], identity['sha256'])

    def _media_matches(self, address, expected_bytes, expected_sha256):
        sha, size = hashlib.sha256(), 0
        deadline = time.monotonic() + 120
        try:
            with self.session.get(address, stream=True, timeout=(20, 180), allow_redirects=False) as response:
                if response.status_code != 200:
                    raise ReplicaError('read_failed', 'Candidate source could not be read', response.status_code)
                for block in response.iter_content(1024 * 1024):
                    if time.monotonic() > deadline:
                        raise ReplicaError('read_failed', 'Candidate source verification deadline exceeded; no task selected')
                    size += len(block)
                    if size > expected_bytes:
                        return False
                    sha.update(block)
        except requests.RequestException:
            raise ReplicaError('read_failed', 'Candidate source read interrupted; no task selected') from None
        return size == expected_bytes and sha.hexdigest() == expected_sha256


def step_state(job, stage):
    if job.get('status') in {'error', 'interrupted'} or (job.get('dashboard') or {}).get('has_error'):
        return 'error'
    if job.get('status') in BUSY or (job.get('dashboard') or {}).get('running'):
        return 'running'
    if stage == 'analysis':
        return 'ready' if (job.get('step_states') or {}).get('analysis') == 'done' else 'waiting'
    view = job.get('logo_replace_view') or {}
    if view.get('state') in {'replaced', 'no_logo'}:
        return 'ready'
    if view.get('state') == 'kept_original':
        return 'review'
    return 'waiting'
