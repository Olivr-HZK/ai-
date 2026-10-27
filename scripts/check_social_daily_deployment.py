"""Read-only deployment checks; never collect, write Base records, or send messages."""
from pathlib import Path
import argparse
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--live-feishu', action='store_true', help='Read real field schema and group identity')
    parser.add_argument('--profile', default='ai-social-adgod')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    from ua_workflows.shared.config import project_env_values
    values = project_env_values()
    results = []

    def check(name, passed, detail=None):
        results.append({'check': name, 'ok': bool(passed), 'detail': detail})

    for key in ('EVERYINFRA_API_KEY', 'FEISHU_APP_ID', 'FEISHU_APP_SECRET', 'GUANGDADA_PASSWORD'):
        check('credential:' + key, bool(values.get(key)))
    check('credential:guangdada-account', bool(values.get('GUANGDADA_EMAIL') or values.get('GUANGDADA_USERNAME')))
    check('feishu-identity', values.get('FEISHU_APP_ID') == 'cli_a906892c9c8c1cb3')
    for name in ('requests', 'PIL', 'yt_dlp', 'lark_oapi'):
        check('module:' + name, importlib.util.find_spec(name) is not None)
    for name in ('ffmpeg', 'ffprobe', 'node', 'lark-cli'):
        check('command:' + name, shutil.which(name) is not None)

    configs = [json.loads((ROOT / 'config' / name).read_text()) for name in (
        'social_chat_browser_daily.json', 'social_chat_xhs_daily.json', 'social_chat_douyin_daily.json')]
    codex_home = Path(os.environ.get('CODEX_HOME', str(Path.home() / '.codex')))
    automations = {}
    for path in (codex_home / 'automations').glob('*/automation.toml'):
        data = tomllib.loads(path.read_text())
        automations[data.get('id', path.parent.name)] = data
    ids = [config['schedule']['automation_id'] for config in configs]
    check('distinct-automation-ids', len(ids) == len(set(ids)))
    for config, aid in zip(configs, ids):
        data = automations.get(aid, {})
        schedule = config['schedule']
        hour, minute = map(int, schedule['time'].split(':'))
        rule = dict(piece.split('=', 1) for piece in data.get('rrule', '').removeprefix('RRULE:').split(';') if '=' in piece)
        expected_status = 'ACTIVE' if config.get('enabled', True) and config.get('execution') == 'scheduled' else 'PAUSED'
        ok = (data.get('status') == expected_status and data.get('kind') == 'heartbeat'
              and str(ROOT) in data.get('prompt', '') and bool(data.get('target_thread_id'))
              and schedule['timezone'] == 'Asia/Shanghai' and rule.get('FREQ') == 'DAILY'
              and rule.get('BYHOUR') == str(hour) and rule.get('BYMINUTE') == str(minute))
        check('scheduler:' + aid, ok, {'status': data.get('status'), 'expected_status': expected_status, 'time': schedule['time'], 'name': data.get('name')})

    if args.live_feishu:
        def cli(parts):
            response = subprocess.run(['lark-cli', '--profile', args.profile, *parts],
                                      cwd=ROOT, capture_output=True, text=True, timeout=45)
            try:
                data = json.loads(response.stdout)
            except ValueError:
                raise RuntimeError('CLI returned no JSON; inspect profile without printing credentials')
            if response.returncode or data.get('ok') is not True:
                raise RuntimeError('CLI read failed: ' + str((data.get('error') or {}).get('code', response.returncode)))
            return data['data']

        try:
            route = configs[0]['feishu']
            data = cli(['base', '+field-list', '--base-token', route['app_token'], '--table-id', route['table_id'], '--as', 'bot'])
            fields = {field['name']: field for field in data['fields']}
            from ua_workflows.social_chat_browser_daily import FIELDS as GDD
            from ua_workflows.xhs_daily import FIELDS as XHS

            def kind(field):
                if field['type'] == 'select':
                    return 4 if field.get('multiple') else 3
                if field['type'] == 'text' and field.get('style', {}).get('type') == 'url':
                    return 15
                return {'text': 1, 'number': 2, 'checkbox': 7, 'attachment': 17}.get(field['type'])

            mismatches = [name for name, expected in set(GDD + XHS)
                          if name not in fields or kind(fields[name]) != expected]
            check('feishu-schema', not mismatches, {'mismatches': sorted(mismatches)})
            options = {o['name'] for o in fields['平台'].get('options', [])}
            check('feishu-platform-options', {'广大大', '小红书', '抖音'} <= options)
            setting = configs[0]['crawl_completion_report']
            chat = cli(['im', 'chats', 'get', '--chat-id', setting['chat_id'], '--as', 'bot'])
            check('feishu-group', chat.get('name') == setting['chat_name'] and chat.get('chat_mode') == 'group')
            check('feishu-start-mention-permission', chat.get('at_all_permission') == 'all_members')
        except (KeyError, RuntimeError, subprocess.TimeoutExpired) as exc:
            check('feishu-live-read', False, str(exc))

    report = {'status': 'ready' if all(item['ok'] for item in results) else 'blocked',
              'project': str(ROOT), 'checks': results, 'live_feishu': args.live_feishu,
              'collection_started': False, 'messages_sent': False, 'records_written': False}
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report['status'] == 'ready' else 1


if __name__ == '__main__':
    raise SystemExit(main())
