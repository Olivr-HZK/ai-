"""Process-scoped macOS wake protection and redacted, single-attempt HTTP evidence."""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import traceback
from urllib.parse import urlsplit


def stamp():
    return datetime.now(timezone.utc).isoformat()


def write(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Exclusive permissions also apply when a diagnostic file already exists.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as stream:
        os.fchmod(stream.fileno(), 0o600)
        json.dump(data, stream, ensure_ascii=False, indent=2)


@contextmanager
def keep_awake(path=None):
    """Keep screen locking intact; refuse macOS work if assertion cannot be verified."""
    process = None
    state = {'started_at': stamp(), 'owner_pid': os.getpid(), 'platform': sys.platform}
    try:
        if sys.platform == 'darwin':
            process = subprocess.Popen(['/usr/bin/caffeinate', '-is', '-w', str(os.getpid())],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            state['caffeinate_pid'] = process.pid
            verified = False
            for _ in range(10):
                if process.poll() is not None:
                    break
                result = subprocess.run(['/usr/bin/pmset', '-g', 'assertions'],
                                        capture_output=True, text=True, timeout=3, check=True)
                verified = any(re.search(r'\bpid\s+' + str(process.pid) + r'\(', line)
                               and 'PreventUserIdleSystemSleep' in line
                               for line in result.stdout.splitlines())
                if verified:
                    break
                time.sleep(0.1)
            if not verified:
                raise RuntimeError('Sleep prevention assertion was not verified; collection stopped')
            state['status'] = 'active_verified'
            state['assertion_verified'] = True
        else:
            state['status'] = 'unsupported_platform'
        if path:
            write(path, state)
        yield state
    except BaseException as exc:
        state['exit_error_type'] = type(exc).__name__
        raise
    finally:
        if process is not None:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        state.update(finished_at=stamp(), status='released' if state.get('status') == 'active_verified' else state.get('status', 'failed'))
        if path:
            write(path, state)


def safe_url(value):
    """Only scheme, host and port: no userinfo, paths, queries or signed tokens."""
    try:
        p = urlsplit(value)
        host = p.hostname or ''
        if ':' in host:
            host = '[' + host + ']'
        return p.scheme + '://' + host + (':' + str(p.port) if p.port else '')
    except ValueError:
        return '[redacted-url]'


def redact(value, secrets=()):
    text = str(value)
    for secret in secrets:
        if secret:
            text = text.replace(secret, '[redacted]')
    text = re.sub(r'(?i)\b(?:https?|socks5h?)://[^\s\'\"<>]+', lambda m: safe_url(m.group()), text)
    text = re.sub(r'(?i)(Bearer\s+)[^\s\'\"<>]+', r'\1[redacted]', text)
    text = re.sub(r'(?i)((?:token|api[_-]?key|secret|password|authorization)\s*[=:]\s*)[^\s,;\'\"]+', r'\1[redacted]', text)
    # urllib3 can include a relative URL rather than a full URL in errors.
    return re.sub(r'(/[\w./%-]+)\?[^\s\'\"<>)]*', r'\1?[redacted]', text)


def exception_info(exc, secrets=()):
    return {'type': type(exc).__name__, 'message': redact(str(exc), secrets),
            # No locals or request/response bodies. Preserve chained stack frames and errors.
            'traceback': redact(''.join(traceback.format_exception(type(exc), exc, exc.__traceback__)), secrets)}


def post_json(url, *, key, body, diagnostic_path, timeout=(15, 120), session=None):
    """Exactly one POST, no retry or proxy failover. Caller persists raw response."""
    import requests
    proxies = requests.utils.get_environ_proxies(url)
    proxy_secrets = [key]
    for value in proxies.values():
        try:
            parsed = urlsplit(value)
            proxy_secrets.extend([parsed.username, parsed.password])
        except ValueError:
            pass
    wall, monotonic = time.time(), time.monotonic()
    evidence = {'started_at': stamp(), 'pid': os.getpid(), 'endpoint': safe_url(url),
                'method': 'POST', 'timeout_connect_seconds': timeout[0], 'timeout_read_seconds': timeout[1],
                'attempt': 1, 'automatic_retry': False,
                'proxy_candidates': {k: safe_url(v) for k, v in proxies.items() if k != 'no'},
                'proxy_note': 'requests environment/macOS proxy snapshot; not proof of the remote route',
                'stage': 'before_request', 'billing': 'not_sent'}
    write(diagnostic_path, evidence)
    try:
        with keep_awake(Path(diagnostic_path).with_suffix('.awake.json')):
            evidence.update(stage='request_wait', billing='unknown')
            write(diagnostic_path, evidence)
            send = session.post if session is not None else requests.post
            response = send(url, headers={'Authorization': 'Bearer ' + key}, json=body, timeout=timeout)
            evidence.update(stage='response_received', http_status=response.status_code,
                            request_ids={k: redact(v, proxy_secrets) for k, v in response.headers.items()
                                         if k.lower() in ('x-request-id', 'request-id', 'x-trace-id', 'traceparent', 'cf-ray')})
            # Do not label billing on HTTP status; only the supplier's payload/receipt can confirm it.
            evidence['stage'] = 'json_decode'
            payload = response.json()
            evidence['stage'] = 'complete'
            return response, payload
    except BaseException as exc:
        evidence['exception'] = exception_info(exc, proxy_secrets)
        raise
    finally:
        evidence.update(finished_at=stamp(), wall_elapsed_seconds=round(time.time()-wall, 6),
                        monotonic_elapsed_seconds=round(time.monotonic()-monotonic, 6))
        write(diagnostic_path, evidence)


def configured_secrets():
    """Read credentials only for redaction; never include configuration in logs."""
    from ua_workflows.shared.config import project_env_values
    values = dict(os.environ)
    values.update(project_env_values())
    return [v for k, v in values.items() if isinstance(v, str) and v
            and re.search(r'KEY|TOKEN|SECRET|PASSWORD|PASSWD', k, re.I)]


def record_failure(base, stage, exc):
    from uuid import uuid4
    path = Path(base)/'runtime'/('error-' + uuid4().hex + '.json')
    write(path, {'at': stamp(), 'stage': stage, 'pid': os.getpid(),
                 'exception': exception_info(exc, configured_secrets())})
    return str(path)


@contextmanager
def collection_phase(base, stage):
    """Protect a whole local phase and preserve failures, including non-HTTP errors."""
    from uuid import uuid4
    path = Path(base)/'runtime'/(stage + '-' + uuid4().hex + '.awake.json')
    try:
        with keep_awake(path):
            yield
    except BaseException as exc:
        if not isinstance(exc, SystemExit) or exc.code not in (None, 0):
            record_failure(base, stage, exc)
        raise
