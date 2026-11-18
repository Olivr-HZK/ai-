"""Stage markers, heartbeat and watchdog decisions for the daily pipeline.

Every stage writes its own marker so a run that stops between stages is visible
instead of looking like silence. Nothing here sends a business message unless an
explicit alert webhook is configured.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from uuid import NAMESPACE_URL, uuid5

TZ = timezone(timedelta(hours=8))
STAGES = ('crawl', 'prepare', 'embed', 'pre-review', 'joint-review', 'select', 'sync', 'report')
STATUSES = ('running', 'done', 'failed')
CONTROL = 'execution-control.json'
HEARTBEAT = 'heartbeat.json'


def now_iso(now=None):
    return (now or datetime.now(TZ)).astimezone(TZ).isoformat()


def load(path, default=None):
    path = Path(path)
    return json.loads(path.read_text()) if path.exists() else default


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + '\n')
    temporary.replace(path)


def mark(run_dir, stage, status, *, note=None, now=None, pid=None, target_date=None):
    if stage not in STAGES:
        raise ValueError('Unsupported stage: ' + str(stage))
    if status not in STATUSES:
        raise ValueError('Unsupported stage status: ' + str(status))
    path = Path(run_dir) / CONTROL
    control = load(path, {}) or {}
    stages = control.setdefault('stages', {})
    entry = stages.setdefault(stage, {})
    stamp = now_iso(now)
    entry['status'] = status
    entry['updated_at'] = stamp
    entry.setdefault('started_at', stamp)
    if status != 'running':
        entry['finished_at'] = stamp
    else:
        entry.pop('finished_at', None)
    entry['pid'] = os.getpid() if pid is None else pid
    if note is not None:
        entry['note'] = note
    control['updated_at'] = stamp
    control['target_date'] = control.get('target_date') or target_date or Path(run_dir).name
    save(path, control)
    return control


def state(run_dir):
    return load(Path(run_dir) / CONTROL, {}) or {}


def stale(run_dir, max_age_minutes, *, now=None):
    """Stages still marked running longer than the budget, plus outright failures."""
    current = (now or datetime.now(TZ)).astimezone(TZ)
    result = []
    for stage, entry in sorted((state(run_dir).get('stages') or {}).items()):
        if entry.get('status') == 'failed':
            result.append({'stage': stage, 'status': 'failed', 'since': entry.get('updated_at'),
                           'age_minutes': None, 'note': entry.get('note')})
            continue
        if entry.get('status') != 'running':
            continue
        try:
            since = datetime.fromisoformat(entry['updated_at'])
        except (KeyError, ValueError):
            result.append({'stage': stage, 'status': 'running', 'since': None,
                           'age_minutes': None, 'note': entry.get('note')})
            continue
        age = (current - since).total_seconds() / 60
        if age > max_age_minutes:
            result.append({'stage': stage, 'status': 'running', 'since': entry['updated_at'],
                           'age_minutes': round(age, 1), 'note': entry.get('note')})
    return result


def heartbeat(run_dir, *, status, stage=None, note=None, now=None, pid=None, target_date=None):
    if status not in ('started', 'done', 'failed'):
        raise ValueError('Unsupported heartbeat status: ' + str(status))
    path = Path(run_dir) / HEARTBEAT
    record = load(path, {}) or {}
    stamp = now_iso(now)
    record.update(status=status, updated_at=stamp, pid=os.getpid() if pid is None else pid)
    record.setdefault('started_at', stamp)
    record['target_date'] = record.get('target_date') or target_date or Path(run_dir).name
    if stage:
        record['stage'] = stage
    if note:
        record['note'] = note
    if status in ('done', 'failed'):
        record['finished_at'] = stamp
    save(path, record)
    return record


def read_heartbeat(run_dir):
    return load(Path(run_dir) / HEARTBEAT, None)


def finished_by_result(run_dir):
    """Old entries finish without a heartbeat; their daily result is the evidence."""
    result = load(Path(run_dir) / 'daily-result.json', None)
    if not isinstance(result, dict):
        return None
    if result.get('workflow_finished') is True:
        return result
    if str(result.get('status') or '').startswith(('complete', 'partial_completed')):
        return result
    return None


def check_daily(run_dir, *, max_age_minutes=120, now=None, target_date=None):
    """Decide whether today's run is on track, stalled, failed or never started."""
    current = (now or datetime.now(TZ)).astimezone(TZ)
    beat = read_heartbeat(run_dir)
    problems = stale(run_dir, max_age_minutes, now=current)
    if beat is None and (finished := finished_by_result(run_dir)):
        return {'status': 'done', 'source': 'daily-result.json',
                'target_date': finished.get('target_date') or target_date or Path(run_dir).name,
                'heartbeat': None, 'problems': problems,
                'daily_result_status': finished.get('status')}
    if beat is None:
        return {'status': 'missing', 'target_date': target_date or Path(run_dir).name,
                'stages': sorted((state(run_dir).get('stages') or {})), 'problems': problems}
    age = None
    try:
        age = round((current - datetime.fromisoformat(beat['updated_at'])).total_seconds() / 60, 1)
    except (KeyError, ValueError):
        pass
    if beat.get('status') == 'failed':
        decision = 'failed'
    elif beat.get('status') == 'done':
        decision = 'done'
    elif not problems and (age is None or age <= max_age_minutes):
        decision = 'running'
    else:
        decision = 'stalled'
    return {'status': decision, 'target_date': beat.get('target_date') or target_date or Path(run_dir).name,
            'heartbeat_age_minutes': age, 'heartbeat': beat, 'problems': problems}


def project_values():
    """Project .env values when available; alerts stay disabled without them."""
    try:
        from ua_workflows.shared.config import project_env_values
        return project_env_values() or {}
    except Exception:
        return {}


def alert_endpoint(values=None):
    values = values if values is not None else {}
    return (values.get('AFIZZY_ALERT_WECOM_WEBHOOK') or '').strip()


def notify_feishu(text, *, chat_id, chat_name=None, client=None):
    """Send one plain-text alert to the configured Feishu chat; no readback claim."""
    if not chat_id:
        return {'status': 'not_configured', 'sent': False}
    if client is None:
        from ua_workflows.social_chat_browser_daily import Feishu
        client = Feishu()
    message = client.call('POST', 'im/v1/messages', params={'receive_id_type': 'chat_id'},
                          json={'receive_id': chat_id, 'msg_type': 'text',
                                'content': json.dumps({'text': text[:4000]}, ensure_ascii=False),
                                'uuid': str(uuid5(NAMESPACE_URL, 'afizzy-alert|' + chat_id + '|'
                                                  + hashlib.sha256(text.encode()).hexdigest()))})
    if not isinstance(message, dict) or not message.get('message_id') or message.get('chat_id') != chat_id:
        raise ValueError('Feishu alert send response identity mismatch')
    result = {'status': 'accepted', 'sent': True, 'chat_id': chat_id,
              'message_id': message['message_id']}
    if chat_name:
        result['chat_name'] = chat_name
    return result


def notify(text, *, post=None, values=None, timeout=20, config=None, client=None):
    """Send a short alert: Feishu chat when configured, WeCom webhook otherwise."""
    values = dict(values or {})
    setting = (config or {}).get('alert_feishu') or {}
    chat_id = setting.get('chat_id') or values.get('AFIZZY_ALERT_FEISHU_CHAT_ID', '')
    chat_name = setting.get('chat_name') or values.get('AFIZZY_ALERT_FEISHU_CHAT_NAME', '')
    if chat_id and setting.get('enabled', True):
        return notify_feishu(text, chat_id=chat_id, chat_name=chat_name, client=client)
    endpoint = alert_endpoint(values)
    if not endpoint:
        return {'status': 'not_configured', 'sent': False}
    parsed = urlsplit(endpoint)
    key = parse_qs(parsed.query).get('key', [''])[0]
    if not (parsed.scheme == 'https' and parsed.hostname == 'qyapi.weixin.qq.com'
            and parsed.path == '/cgi-bin/webhook/send' and key):
        raise ValueError('Invalid configured alert webhook')
    body = {'msgtype': 'text', 'text': {'content': text[:2000]}}
    if post is None:
        import requests
        post = requests.post
    response = post(endpoint, json=body, timeout=timeout, allow_redirects=False)
    data = response.json()
    if not isinstance(data, dict) or data.get('errcode') != 0:
        raise ValueError('Alert webhook rejected the message')
    return {'status': 'accepted', 'sent': True,
            'destination_sha256': hashlib.sha256(endpoint.encode()).hexdigest()}


def render_alert(decision):
    status = decision.get('status')
    date = decision.get('target_date')
    if status == 'missing':
        return f'Afizzy {date} 任务没有启动痕迹（无心跳文件），请检查 05:00 采集是否执行。'
    if status == 'failed':
        beat = decision.get('heartbeat') or {}
        return f'Afizzy {date} 任务失败：{beat.get("stage") or "未知阶段"} {beat.get("note") or ""}'.strip()
    if status == 'stalled':
        beat = decision.get('heartbeat') or {}
        problems = '、'.join(f'{p["stage"]}={p["status"]}' for p in decision.get('problems') or [])
        return (f'Afizzy {date} 任务疑似停滞：心跳 {decision.get("heartbeat_age_minutes")} 分钟未更新，'
                f'当前阶段 {beat.get("stage") or "未知"} {problems}').strip()
    return f'Afizzy {date} 任务状态：{status}'
