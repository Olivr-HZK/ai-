"""Exercise drift failures and the offline guard, without running business commands."""
import shutil
import socket
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import ci_contracts

ROOT = Path(__file__).resolve().parents[1]


def test_repository_contracts():
    ci_contracts.check(ROOT)


def test_cli_drift_is_rejected():
    original = ci_contracts.constant
    with patch.object(ci_contracts, 'constant', side_effect=lambda path, name: 'wrong' if name == 'MODEL' else original(path, name)):
        with pytest.raises(ValueError, match='Image model drift'):
            ci_contracts.check(ROOT)


def test_ci_guard_blocks_business_cli_network_and_shell():
    # Never attempt a real CLI when collected by ordinary pytest.
    guard = sys.modules.get('ci_offline') or sys.modules.get('__main__')
    if not getattr(guard, 'GUARD_INSTALLED', False):
        pytest.skip('requires scripts/ci_offline.py guard')
    for argv in (['lark-cli', 'base'], ['videolab', 'task', 'create'], ['sh', '-c', 'true'],
                 ['git', 'fetch'], ['git', '-c', 'alias.bad=!curl example.invalid', 'bad']):
        with pytest.raises(RuntimeError, match='CI'):
            subprocess.run(argv, check=True)
    with pytest.raises(RuntimeError, match='CI'):
        socket.getaddrinfo('example.invalid', 443)
    if shutil.which('ffmpeg'):
        with pytest.raises(RuntimeError, match='CI'):
            subprocess.run(['ffmpeg', '-i', 'https://example.invalid/input', '/tmp/never-created.mp4'])
