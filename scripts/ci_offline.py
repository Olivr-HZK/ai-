"""Small, fixed offline regression gate. No production CLI entrypoints are run."""
from pathlib import Path
import os
import shutil
import sys
import subprocess
import re

GUARD_INSTALLED = False
ROOT = Path(__file__).resolve().parents[1]
TESTS = (
    'test_persistent_storage',
    'test_ci_contracts', 'test_replica_deploy', 'test_guangdada_preset_range',
    'test_material_replica', 'test_replica_cli', 'test_replica_pipeline',
    'test_replica_main_table', 'test_replica_main_translation', 'test_replica_main_images',
    'test_replica_retry_policy', 'test_replica_recovery',
    'test_social_chat_media_orientation', 'test_social_chat_template_history',
    'test_social_chat_feedback_policy', 'test_social_chat_mixed_delivery', 'test_social_chat_media_names',
    'test_social_chat_plugin_review_plan', 'test_social_chat_clip_vectors',
)


def install_guard():
    global GUARD_INSTALLED
    # Remove ambient credentials/proxies and hide ordinary CLI/user config.
    keep = {k: v for k, v in os.environ.items() if k in ('PATH', 'SYSTEMROOT', 'TMPDIR', 'CI')}
    os.environ.clear()
    os.environ.update(keep, PYTEST_DISABLE_PLUGIN_AUTOLOAD='1', PYTHONDONTWRITEBYTECODE='1',
                      PYTHON_DOTENV_DISABLED='1', REPLICA_AUTH_MODE='browser')
    import dotenv
    dotenv.load_dotenv = lambda *a, **k: False
    dotenv.dotenv_values = lambda *a, **k: {}
    allowed = {str(Path(shutil.which(n)).resolve()) for n in ('git', 'ffmpeg', 'ffprobe') if shutil.which(n)}

    popen = subprocess.Popen
    def local_popen(argv, *args, **kwargs):
        if isinstance(argv, (list, tuple)) and argv:
            name = Path(str(argv[0])).name
            if name in ('ffmpeg', 'ffprobe'):
                argv = [argv[0], '-protocol_whitelist', 'file,pipe', *argv[1:]]
            elif name == 'git':
                argv = [argv[0], '-c', 'core.hooksPath=/dev/null', '-c', 'commit.gpgsign=false', '-c', 'core.fsmonitor=false', *argv[1:]]
        return popen(argv, *args, **kwargs)
    subprocess.Popen = local_popen

    def audit(event, args):
        if event in ('socket.connect', 'socket.connect_ex', 'socket.getaddrinfo', 'socket.sendto'):
            raise RuntimeError('CI forbids network access')
        if event in ('os.system', 'os.exec', 'os.posix_spawn', 'pty.spawn'):
            raise RuntimeError('CI forbids shell/exec escape')
        if event == 'subprocess.Popen':
            executable, argv, cwd, env = args
            resolved = shutil.which(str(executable))
            if not resolved or str(Path(resolved).resolve()) not in allowed or isinstance(argv, str):
                raise RuntimeError('CI forbids external business CLIs and shells')
            # Media tools have network protocols too. Never allow URL inputs/outputs.
            if Path(resolved).name == 'git':
                tokens = list(argv[1:])
                while tokens and tokens[0] in ('-c', '-C'):
                    flag, value = tokens[:2]; tokens = tokens[2:]
                    if flag == '-c' and value not in ('core.hooksPath=/dev/null', 'commit.gpgsign=false', 'core.fsmonitor=false', 'protocol.allow=never'):
                        raise RuntimeError('CI forbids Git command/config escapes')
                if not tokens or tokens[0] not in ('init', 'add', 'commit', 'checkout', 'rev-parse', 'status', 'branch', 'update-ref', 'archive', 'config'):
                    raise RuntimeError('CI allows only local Git operations')
                if tokens[0] == 'config' and (len(tokens) != 3 or tokens[1] not in ('user.name', 'user.email')):
                    raise RuntimeError('CI allows only fixture Git identities')
            else:
                if list(argv).count('-protocol_whitelist') != 1 or list(argv)[1:3] != ['-protocol_whitelist', 'file,pipe']:
                    raise RuntimeError('CI requires local-only media protocols')
                if any(re.match(r'^[a-zA-Z][a-zA-Z0-9+.-]*:', str(a)) and str(a) not in ('v:0', 'a:0') and not str(a).startswith('pipe:') for a in argv):
                    raise RuntimeError('CI forbids remote media')
    sys.addaudithook(audit)
    GUARD_INSTALLED = True



def main():
    sys.path[:0] = [str(ROOT), str(ROOT / 'scripts'), str(ROOT / 'tests')]
    install_guard()
    from ci_contracts import check
    check(ROOT)
    import pytest
    return pytest.main(['-q', '-p', 'no:cacheprovider',
                        '--deselect=tests/test_social_chat_plugin_review_plan.py::test_actual_handoff_cli_pins_date_and_outputs_joint_queue',
                        '--deselect=tests/test_social_chat_plugin_review_plan.py::test_recent_history_component_behavior_matches_legacy_pure_cluster', *[str(ROOT / 'tests' / (n + '.py')) for n in TESTS]])


if __name__ == '__main__':
    raise SystemExit(main())
