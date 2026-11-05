"""Static repository contracts; does not import or execute business workflows."""
import ast
import hashlib
import json
from pathlib import Path
import tomllib


def constant(path, name):
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.Assign) and any(isinstance(n, ast.Name) and n.id == name for n in node.targets):
            return ast.literal_eval(node.value)
    raise ValueError(f'Missing constant {name}: {path}')


def check(root):
    root = Path(root)
    def require(ok, message):
        if not ok:
            raise ValueError(message)
    for name in ('social_chat_browser_daily.json', 'replica_main_table.json', 'replica_deploy.example.json'):
        path = root / 'config' / name
        try:
            json.loads(path.read_text())
        except ValueError as error:
            raise ValueError(f'Invalid configuration: {path}') from error
    project = tomllib.loads((root / 'pyproject.toml').read_text())
    for cli, module in (('material-replica', 'material_replica'), ('replica-studio', 'replica_cli')):
        require(project['project']['scripts'][cli] == f'ua_workflows.{module}:main', f'{cli} entrypoint drift')
        require(f'scripts/{module}.py' in (root / 'scripts' / cli).read_text(), f'{cli} wrapper drift')
        require(f'ua_workflows.{module}' in (root / 'scripts' / f'{module}.py').read_text(), f'{cli} shim drift')
    cfg = json.loads((root / 'config/social_chat_browser_daily.json').read_text())
    daily = cfg['daily_workflow']
    require(daily['presets'] == ['game', 'tool', 'tool2'], 'Daily preset scope drift')
    require(not daily['automatic_replication'] and not daily['video_replication']['enabled'], 'Automatic replication enabled')
    require(daily['replication_trigger'] == 'explicit_user_request_only', 'Manual replication gate drift')
    runbook = (root / daily['runbook']).read_text()
    require('--presets game tool tool2' in runbook and '05:00' in runbook, 'Runbook/config drift')
    skill = (root / 'skills/social-chat-browser-daily/SKILL.md').read_text()
    require('scripts/material-replica' in skill and 'Video Lab GPT Image 2' in skill, 'Skill/CLI drift')
    image = root / 'ua_workflows/replica_images.py'
    require(constant(image, 'MODEL') == 'gpt-image-2' and constant(image, 'WORKFLOW') == 'preset_image_editing', 'Image model drift')
    reference = cfg['plugin_handoff']['image_replication_after_ingestion']['brand_reference']
    require(hashlib.sha256((root / reference['path']).read_bytes()).hexdigest() == reference['sha256'], 'Brand reference drift')
    main = json.loads((root / 'config/replica_main_table.json').read_text())
    require(main['allowed_actions'] == constant(root / 'ua_workflows/replica_main_table.py', 'ACTIONS'), 'Main action allowlist drift')
    require(main['translation_requires_logo'] is True and main['auth_mode'] == 'browser', 'Main execution contract drift')
    print('Static config/skill/CLI contracts passed')
