"""Public mode selection only; no credentials, profile reads or network."""
import unittest
import tempfile
from pathlib import Path
from unittest.mock import Mock,patch
from ua_workflows.replica_cli import execute,parser
from ua_workflows.replica_studio_api import ReplicaAPI,ReplicaError


class BrowserSelectionTests(unittest.TestCase):
    def test_project_browser_login_starts_persistent_bridge_not_device_authorization(self):
        with patch('ua_workflows.shared.config.project_env_values',return_value={'REPLICA_AUTH_MODE':'browser'}), \
             patch('ua_workflows.replica_auth_bridge.start_login',return_value={'authenticated':True}) as bridge, \
             patch('ua_workflows.replica_device_auth.start_device_login') as device:
            self.assertTrue(execute(parser().parse_args(['login']))['cli_authenticated'])
            bridge.assert_called_once();device.assert_not_called()

    def test_explicit_device_mode_overrides_browser_project(self):
        with patch('ua_workflows.shared.config.project_env_values',return_value={'REPLICA_AUTH_MODE':'browser'}), \
             patch('ua_workflows.replica_device_auth.start_device_login',return_value={}) as device, \
             patch('ua_workflows.replica_auth_bridge.start_login') as bridge:
            execute(parser().parse_args(['--auth-mode','device','login','--no-browser']))
            device.assert_called_once();bridge.assert_not_called()

    def test_explicit_token_file_selects_device_without_using_browser_profile(self):
        with patch('ua_workflows.shared.config.project_env_values',return_value={'REPLICA_AUTH_MODE':'browser'}), \
             patch('ua_workflows.replica_device_auth.load_token',return_value={'token':'fake-only','account_id':'a'}), \
             patch('ua_workflows.replica_auth_bridge.bridge_status') as bridge:
            self.assertTrue(ReplicaAPI.configured(token_file='/fixture/fake-token').device_auth)
            bridge.assert_not_called()

    def test_browser_mode_never_falls_back_to_token(self):
        with patch('ua_workflows.shared.config.project_env_values',return_value={'REPLICA_AUTH_MODE':'browser'}), \
             patch('ua_workflows.replica_auth_bridge.bridge_status',return_value=None), \
             patch('ua_workflows.replica_auth_bridge.SOCKET') as socket, \
             patch('ua_workflows.replica_device_auth.load_token') as token:
            socket.exists.return_value=False
            with tempfile.TemporaryDirectory() as directory, \
                 patch('ua_workflows.shared.config.PROJECT_ROOT',Path(directory)):
                with self.assertRaises(ReplicaError):ReplicaAPI.configured(auth_mode='browser')
            token.assert_not_called()
