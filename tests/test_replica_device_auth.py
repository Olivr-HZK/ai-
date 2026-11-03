"""Device authorization and token transport tests with all HTTP mocked."""
import json
import io
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import Mock, patch

import requests

from ua_workflows.replica_cli import Checkpoint, execute, logo_many, parser, upload_many
from ua_workflows import replica_device_auth as device
from ua_workflows.replica_studio_api import BASE_URL, ReplicaAPI, ReplicaError


TOKEN = 'test-bearer-never-print'
DEVICE_CODE = 'test-device-secret-never-print'


def response(value, status=200, headers=None):
    result = Mock(status_code=status, headers=headers or {})
    result.json.return_value = value
    return result


def whoami(account='account-a'):
    return {'user': {'id': account, 'enabled': True, 'private': TOKEN},
            'token': {'scopes': ['read', 'upload', 'run', 'download'], 'secret': TOKEN}}


def started(**extra):
    return {'device_code': DEVICE_CODE, 'user_code': 'ABCD-1234',
            'verification_uri': BASE_URL + '/cli?code=ABCD-1234', 'expires_in': 600, 'interval': 3, **extra}


class Clock:
    def __init__(self): self.now = 1000; self.waits = []
    def __call__(self): return self.now
    def wait(self, seconds): self.waits.append(seconds); self.now += seconds


class DeviceStorageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.root = Path(self.temp.name)
        self.path = self.root / 'cli.json'
    def tearDown(self): self.temp.cleanup()

    def test_private_atomic_storage_load_and_compatible_account_pin(self):
        device.save_token(self.path, TOKEN, 'account-a')
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)
        self.assertEqual(device.load_token(self.path)['account_id'], 'account-a')
        self.assertEqual(list(self.root.glob('*.tmp')), [])
        api = ReplicaAPI.configured(token_file=self.path)
        self.assertTrue(api.device_auth)
        self.assertEqual(api.expected_account, 'account-a')
        with self.assertRaises(ReplicaError): ReplicaAPI.configured(token_file=self.path, expected_account='account-b')

    def test_public_cli_token_format_is_compatible_without_account_pin(self):
        self.path.write_text(json.dumps({'server': BASE_URL, 'token': TOKEN}));self.path.chmod(0o600)
        self.assertTrue(ReplicaAPI.configured(token_file=self.path).device_auth)

    def test_missing_explicit_token_never_falls_back_to_browser(self):
        with patch('ua_workflows.replica_auth_bridge.bridge_status') as browser:
            with self.assertRaises(ReplicaError): ReplicaAPI.configured(token_file=self.path)
        browser.assert_not_called()

    def test_missing_default_token_and_logout_never_fall_back_to_browser(self):
        with patch.object(device,'token_path',return_value=self.path), patch('ua_workflows.shared.config.project_env_values',return_value={}), patch('ua_workflows.replica_auth_bridge.bridge_status') as browser:
            with self.assertRaises(ReplicaError):ReplicaAPI.configured()
            device.save_token(self.path,TOKEN,'account-a');device.logout(self.path)
            with self.assertRaises(ReplicaError):ReplicaAPI.configured()
        browser.assert_not_called()

    def test_legacy_bridge_remains_available_only_when_explicitly_selected(self):
        with patch('ua_workflows.shared.config.project_env_values',return_value={}), patch('ua_workflows.replica_auth_bridge.bridge_status',return_value={'authenticated':True,'capabilities':['translation_v1']}) as bridge:
            api=ReplicaAPI.configured(auth_mode='browser')
        self.assertFalse(api.device_auth);bridge.assert_called_once()

    def test_default_token_precedes_old_bridge_without_reading_its_session(self):
        device.save_token(self.path, TOKEN, 'account-a')
        with patch.object(device, 'token_path', return_value=self.path), patch('ua_workflows.shared.config.project_env_values', return_value={}), patch('ua_workflows.replica_auth_bridge.bridge_status') as bridge:
            self.assertTrue(ReplicaAPI.configured().device_auth)
        bridge.assert_not_called()

    def test_permission_symlink_directory_size_and_owner_fail_without_secret_output(self):
        device.save_token(self.path, TOKEN, 'account-a');self.path.chmod(0o644)
        with self.assertRaises(ReplicaError) as error: device.load_token(self.path)
        self.assertNotIn(TOKEN, str(error.exception))
        self.path.chmod(0o600)
        link = self.root / 'link.json';link.symlink_to(self.path)
        with self.assertRaises(ReplicaError): device.load_token(link)
        with self.assertRaises(ReplicaError): device.load_token(self.root)
        with patch.object(device.os, 'getuid', return_value=os.getuid()+1):
            with self.assertRaises(ReplicaError): device.load_token(self.path)
        self.path.write_bytes(b' ' * 65537)
        with self.assertRaises(ReplicaError): device.load_token(self.path)

    def test_bad_server_token_and_json_are_rejected(self):
        for value in ({'server': 'https://evil.example', 'token': TOKEN}, {'server': BASE_URL, 'token': 'bad\nvalue'},
                      {'server': BASE_URL, 'token': TOKEN, 'account_id': []}, []):
            self.path.write_text(json.dumps(value));self.path.chmod(0o600)
            with self.assertRaises(ReplicaError): device.load_token(self.path)
        self.path.write_text('{malformed');
        with self.assertRaises(ReplicaError): device.load_token(self.path)

    def test_explicit_legacy_session_still_works(self):
        legacy=self.root/'session.json';legacy.write_text(json.dumps({'replica_session':'legacy-cookie'}));legacy.chmod(0o600)
        device.save_token(self.path,TOKEN,'account-a')
        with patch.object(device,'token_path',return_value=self.path):
            api=ReplicaAPI.configured(session_file=legacy)
        self.assertFalse(api.device_auth)
        self.assertEqual(api.session.cookies.get('replica_session'),'legacy-cookie')

    def test_configured_environment_path_is_shared_by_login_and_loading(self):
        with patch('ua_workflows.shared.config.project_env_values', return_value={'REPLICA_CLI_TOKEN_FILE': str(self.path)}):
            self.assertEqual(device.token_path(),self.path)

    def test_logout_only_removes_local_file(self):
        device.save_token(self.path,TOKEN,'account-a')
        result=device.logout(self.path)
        self.assertTrue(result['local_token_removed'])
        self.assertFalse(result['server_token_revoked'])
        self.assertFalse(self.path.exists())
        self.assertFalse(device.logout(self.path)['local_token_removed'])


class DeviceLoginTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.path=Path(self.temp.name)/'cli.json'
        self.session=Mock();self.clock=Clock();self.prompt=Mock();self.opener=Mock()
    def tearDown(self):self.temp.cleanup()
    def login(self, **kwargs):
        return device.start_device_login(path=self.path,session=self.session,clock=self.clock,wait=self.clock.wait,
                                         on_prompt=self.prompt,opener=self.opener,**kwargs)

    def test_pending_approved_validates_whoami_before_private_save_no_cookie_or_bridge(self):
        self.session.post.side_effect=[response(started()),response({'state':'pending'}),response({'state':'approved','token':TOKEN})]
        with patch.object(requests.Session,'request',return_value=response(whoami())) as http:
            result=self.login(label='Test computer')
        self.assertTrue(result['cli_authenticated']);self.assertEqual(result['auth_mode'],'device')
        self.assertEqual(device.load_token(self.path)['token'],TOKEN)
        self.assertNotIn(TOKEN,json.dumps(result));self.assertNotIn(DEVICE_CODE,json.dumps(result))
        self.assertNotIn(DEVICE_CODE,json.dumps(self.prompt.call_args.args[0]))
        self.opener.assert_called_once_with(started()['verification_uri'])
        self.assertEqual(self.session.post.call_args_list[0].args[0],BASE_URL+'/api/cli/login/start')
        self.assertEqual(self.session.post.call_args_list[1].kwargs['json'],{'device_code':DEVICE_CODE})
        self.assertTrue(all(c.kwargs['allow_redirects'] is False for c in self.session.post.call_args_list))
        self.assertEqual(http.call_args.args[:2],('GET',BASE_URL+'/api/cli/whoami'))
        self.assertFalse(self.session.trust_env)

    def test_approved_credential_survives_identity_failure_without_false_account_pin(self):
        device.save_token(self.path,'prior-token','account-prior')
        self.session.post.side_effect=[response(started()),response({'state':'approved','token':TOKEN})]
        with patch.object(requests.Session,'request',return_value=response({'user':None})):
            with self.assertRaises(ReplicaError): self.login()
        saved=device.load_token(self.path)
        self.assertEqual(saved['token'],TOKEN)
        self.assertNotIn('account_id',saved)
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode),0o600)
        with patch.object(requests.Session,'request',return_value=response(whoami())):
            result=ReplicaAPI.configured(token_file=self.path).authenticate()
        self.assertEqual(result['account_id'],'account-a')
        self.assertEqual(self.session.post.call_count,2)  # No second device approval.

    def test_denied_and_expired_are_not_saved_or_restarted(self):
        for state in ('denied','expired'):
            self.session.post.side_effect=[response(started()),response({'state':state})]
            with self.assertRaises(ReplicaError):self.login()
            self.assertFalse(self.path.exists())

    def test_timeout_is_bounded_and_no_browser_option_is_honored(self):
        self.session.post.side_effect=[response(started()),response({'state':'pending'})]
        result=self.login(timeout=5,no_browser=True)
        self.assertFalse(result['cli_authenticated']);self.assertEqual(sum(self.clock.waits),5)
        self.opener.assert_not_called();self.assertFalse(self.path.exists())
        self.assertNotIn(DEVICE_CODE,json.dumps(result))

    def test_slow_down_and_server_expiry_bound_polling(self):
        self.session.post.side_effect=[response(started(expires_in=12)),response({'state':'slow_down'}),response({'state':'pending'})]
        result=self.login(no_browser=True)
        self.assertFalse(result['authenticated']);self.assertEqual(self.clock.waits,[3,8,1])

    def test_untrusted_approval_origin_and_invalid_contract_never_open_browser(self):
        variants=[{'verification_uri':'https://evil.example/cli'}, {'verification_uri':BASE_URL+'/admin'},
                  {'verification_uri':BASE_URL+'/cli?private='+DEVICE_CODE}, {'expires_in':float('inf')},
                  {'interval':0}, {'device_code':None}, {'user_code':'bad\ncode'}]
        for change in variants:
            self.session.post.side_effect=None;self.session.post.return_value=response(started(**change))
            with self.assertRaises(ReplicaError):self.login()
        self.opener.assert_not_called();self.assertFalse(self.path.exists())

    def test_bootstrap_redirect_network_failure_and_unknown_state_never_save(self):
        self.session.post.return_value=response({},302)
        with self.assertRaises(ReplicaError):self.login()
        self.session.post.side_effect=requests.Timeout()
        with self.assertRaises(ReplicaError) as error:self.login()
        self.assertNotIn(DEVICE_CODE,str(error.exception))
        self.session.post.side_effect=[response(started()),response({'state':'surprise'})]
        with self.assertRaises(ReplicaError):self.login()
        self.assertFalse(self.path.exists())

    def test_workspace_preparation_waits_only_for_explicit_unprocessed_contract(self):
        self.session.post.side_effect=[response({'detail':'工作区正在准备，请稍后'},503,{'Retry-After':'2'}),response(started()),response({'state':'approved','token':TOKEN})]
        with patch.object(requests.Session,'request',return_value=response(whoami())):
            result=self.login(no_browser=True)
        self.assertTrue(result['authenticated']);self.assertEqual(self.clock.waits,[2,3])
        self.assertEqual(self.session.post.call_args_list[0].kwargs['json'],self.session.post.call_args_list[1].kwargs['json'])

    def test_generic_503_does_not_replay_and_preparation_deadline_is_bounded(self):
        self.session.post.return_value=response({'detail':'Service unavailable'},503,{'Retry-After':'2'})
        with self.assertRaises(ReplicaError):self.login()
        self.assertEqual(self.session.post.call_count,1)
        self.session.post.reset_mock();self.session.post.return_value=response({'detail':'工作区正在准备'},503,{'Retry-After':'nan'})
        with self.assertRaises(ReplicaError):self.login(timeout=5)
        self.assertEqual(sum(self.clock.waits),5);self.assertFalse(self.path.exists())

    def test_approved_token_waits_for_readonly_identity_without_reapproval(self):
        self.session.post.side_effect=[response(started()),response({'state':'approved','token':TOKEN})]
        with patch.object(requests.Session,'request',side_effect=[response({},503)]*3+[response(whoami())]) as http:
            result=self.login(no_browser=True)
        self.assertTrue(result['authenticated']);self.assertEqual(http.call_count,4)
        self.assertTrue(all(c.args[0]=='GET' for c in http.call_args_list))
        self.assertEqual(device.load_token(self.path)['token'],TOKEN)


class DeviceTransportTests(unittest.TestCase):
    def client(self,confirm_paid=False):
        return ReplicaAPI(device.DeviceTokenSession(TOKEN),confirm_paid=confirm_paid,wait=lambda _:None)

    def test_authentication_uses_bearer_whoami_and_only_public_projection(self):
        api=self.client()
        with patch.object(requests.Session,'request',return_value=response(whoami())) as http:
            result=api.authenticate()
        self.assertNotIn(TOKEN,json.dumps(result));self.assertEqual(result['account_id'],'account-a')
        self.assertEqual(http.call_args.kwargs['headers']['Authorization'],'Bearer '+TOKEN)
        self.assertNotIn('Origin',http.call_args.kwargs['headers'])
        self.assertNotIn('X-Replica-Account',http.call_args.kwargs['headers'])
        self.assertFalse(http.call_args.kwargs['allow_redirects'])
        self.assertFalse(api.session.trust_env);self.assertIsNone(api.csrf)

    def test_paid_action_requires_explicit_confirmation_and_then_sends_header_without_csrf(self):
        api=self.client();api.account='account-a'
        with patch.object(requests.Session,'request',return_value=response({'id':'job-a','status':'queued'})) as http:
            with self.assertRaises(ReplicaError) as error:api.smart_logo({'id':'job-a','revision':8},{'logo_mode':'smart'})
            self.assertEqual(error.exception.code,'confirmation_required');http.assert_not_called()
            api.confirm_paid=True
            api.smart_logo({'id':'job-a','revision':8},{'logo_mode':'smart'})
        headers=http.call_args.kwargs['headers']
        self.assertEqual(headers['X-Replica-Confirm'],'paid')
        self.assertNotIn('X-Replica-Account',headers)
        self.assertNotIn('Origin',headers)
        self.assertNotIn('X-Replica-CSRF',headers)

    def test_upload_reference_and_download_plan_do_not_send_paid_generation_confirmation(self):
        api=self.client();api.account='account-a'
        with patch.object(requests.Session,'request',return_value=response({'id':'job-a','revision':9})) as http:
            api.request('POST','/api/jobs/job-a/portrait/person-1',files={'file':('person.png',b'image','image/png')})
        self.assertNotIn('X-Replica-Confirm',http.call_args.kwargs['headers'])
        self.assertEqual(http.call_args.kwargs['headers']['Authorization'],'Bearer '+TOKEN)

    def test_api_and_media_are_fixed_origin_with_no_credential_injection_or_cookie_carryover(self):
        transport=device.DeviceTokenSession(TOKEN);transport.cookies.set('unwanted','value')
        with patch.object(requests.Session,'request',return_value=response({})) as http:
            transport.get(BASE_URL+'/api/jobs/job-a/files/portrait.png',allow_redirects=True)
            self.assertEqual(list(transport.cookies),[]);self.assertFalse(http.call_args.kwargs['allow_redirects'])
            for address in ('https://evil.example/api/jobs','http://replica-studio.upupup.today/api/jobs',BASE_URL+'/outside'):
                with self.assertRaises(ReplicaError):transport.get(address)
            for headers in ({'Cookie':'injected'},{'Authorization':'Bearer other'},{'Host':'evil.example'},
                            {'Origin':BASE_URL},{'X-Replica-Account':'other'},{'X-Replica-CSRF':'old'}):
                with self.assertRaises(ReplicaError):transport.get(BASE_URL+'/api/jobs',headers=headers)
        self.assertEqual(http.call_count,1)

    def test_revoked_token_has_no_browser_fallback_and_insufficient_scope_is_distinct(self):
        api=self.client()
        with patch.object(requests.Session,'request',return_value=response({},401)) as http, patch('ua_workflows.replica_auth_bridge.bridge_status') as bridge:
            with self.assertRaises(ReplicaError) as error:api.authenticate()
        self.assertEqual(error.exception.code,'authentication_required');bridge.assert_not_called();self.assertEqual(http.call_count,1)
        with patch.object(requests.Session,'request',return_value=response({},403)):
            with self.assertRaises(ReplicaError) as error:api.authenticate()
        self.assertEqual(error.exception.code,'permission_denied')

    def test_denial_detail_is_visible_without_echoing_bearer_credential(self):
        api=self.client()
        with patch.object(requests.Session,'request',return_value=response({'detail':'Origin rejected; Authorization: Bearer '+TOKEN},403)):
            with self.assertRaises(ReplicaError) as error:api.authenticate()
        self.assertIn('Origin rejected',str(error.exception))
        self.assertNotIn(TOKEN,str(error.exception))

    def test_saved_approval_with_wrong_account_cannot_open_an_existing_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'cli.json';device.save_token(path,TOKEN)
            cp=Path(directory)/'state.json';Checkpoint(cp,'account-a').put('uploads','existing',{'stage':'uploaded'})
            api=ReplicaAPI.configured(token_file=path)
            with patch.object(requests.Session,'request',return_value=response(whoami('account-b'))):
                auth=api.authenticate()
            with self.assertRaises(ReplicaError):Checkpoint(cp,auth['account_id'])

    def test_changed_token_account_cannot_use_existing_checkpoint(self):
        api=self.client();api.expected_account='account-a'
        with patch.object(requests.Session,'request',return_value=response(whoami('account-b'))):
            with self.assertRaises(ReplicaError) as error:api.authenticate()
        self.assertEqual(error.exception.code,'account_changed')

    def test_unknown_paid_write_is_not_retried_and_extra_actions_remain_forbidden(self):
        api=self.client(True);api.account='account-a'
        with patch.object(requests.Session,'request',side_effect=requests.Timeout()) as http:
            with self.assertRaises(ReplicaError) as error:api.smart_logo({'id':'job-a','revision':8},{'logo_mode':'smart'})
            self.assertEqual(error.exception.code,'result_unknown');self.assertEqual(http.call_count,1)
        for path in ('/api/jobs/job-a/actions/videos','/api/jobs/job-a/actions/portraits','/api/batch/all'):
            with self.assertRaises(ReplicaError):api.request('POST',path,body={})


class DeviceCommandTests(unittest.TestCase):
    def test_cli_displays_only_the_public_approval_link_and_short_code(self):
        def login(**kwargs):
            kwargs['on_prompt']({'user_code':'ABCD-1234','verification_uri':BASE_URL+'/cli?code=ABCD-1234'})
            return {'cli_authenticated':False}
        output=io.StringIO()
        with patch.object(device,'start_device_login',side_effect=login),patch('sys.stderr',output):
            execute(parser().parse_args(['login','--no-browser']))
        self.assertIn('ABCD-1234',output.getvalue())
        self.assertNotIn(DEVICE_CODE,output.getvalue());self.assertNotIn(TOKEN,output.getvalue())

    def test_missing_upload_scope_is_not_a_failed_video_and_stops_intake_before_intent(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'video.mp4';path.write_bytes(b'video')
            api=Mock();api.authenticate.side_effect=ReplicaError('permission_denied','Missing upload scope',403)
            checkpoint=Checkpoint(Path(directory)/'state.json','account-a')
            result=upload_many(lambda:api,[path],checkpoint)
            self.assertTrue(result['stopped'])
            self.assertEqual(result['results'][0]['code'],'not_submitted')
            self.assertEqual(result['results'][0]['reason'],'permission_denied')
            self.assertEqual(checkpoint.data['uploads'],{})
            api.upload.assert_not_called()

    def test_denied_paid_scope_stops_later_submissions(self):
        with tempfile.TemporaryDirectory() as directory:
            api=Mock();api.smart_options.return_value={'logo_mode':'smart'}
            api.job.return_value={'id':'job-a','revision':8,'step_states':{'analysis':'done'}}
            api.smart_logo.side_effect=ReplicaError('permission_denied','Missing run scope',403)
            result=logo_many(api,['job-a','job-b'],Checkpoint(Path(directory)/'state.json','account-a'))
            self.assertTrue(result['stopped']);self.assertEqual(api.smart_logo.call_count,1)
            self.assertEqual(result['results'][1]['code'],'not_submitted')

    def test_default_login_is_device_login_and_does_not_start_legacy_bridge(self):
        args=parser().parse_args(['login','--label','My computer','--no-browser'])
        with patch.object(device,'start_device_login',return_value={'cli_authenticated':False}) as login, patch('ua_workflows.replica_auth_bridge.start_login') as old:
            execute(args)
        self.assertTrue(login.call_args.kwargs['no_browser']);self.assertEqual(login.call_args.kwargs['label'],'My computer')
        old.assert_not_called()

    def test_legacy_browser_is_explicit(self):
        with patch('ua_workflows.replica_auth_bridge.start_login',return_value={'authenticated':True}) as old:
            self.assertTrue(execute(parser().parse_args(['login','--legacy-browser']))['cli_authenticated'])
        old.assert_called_once()

    def test_paid_confirmation_is_checked_before_any_checkpoint_is_written(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'state.json'
            api=Mock();api.authenticate.return_value={'account_id':'account-a','auth_mode':'device'}
            with self.assertRaises(ReplicaError) as error:
                execute(parser().parse_args(['--checkpoint',str(path),'logo','job-a']),factory=lambda:api)
            self.assertEqual(error.exception.code,'confirmation_required');self.assertFalse(path.exists())

    def test_paid_flag_is_forwarded_to_configured_sdk_and_identity_alias_is_safe(self):
        api=Mock();api.authenticate.return_value={'account_id':'account-a','auth_mode':'device'}
        with patch.object(ReplicaAPI,'configured',return_value=api) as configured:
            result=execute(parser().parse_args(['whoami']))
        self.assertEqual(result['auth_mode'],'device');self.assertFalse(configured.call_args.kwargs['confirm_paid'])
        with tempfile.TemporaryDirectory() as directory, patch.object(ReplicaAPI,'configured',return_value=api) as configured, patch('ua_workflows.replica_cli.logo_many',return_value={'results':[]}):
            execute(parser().parse_args(['--checkpoint',str(Path(directory)/'state.json'),'logo','job-a','--yes']))
        self.assertTrue(configured.call_args.kwargs['confirm_paid'])


if __name__=='__main__':unittest.main()
