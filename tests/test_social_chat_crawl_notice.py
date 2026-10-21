import json
import pytest
from ua_workflows.social_chat_crawl_notice import notify, verify_message


CONFIG = {'crawl_start_notice': {'enabled': True, 'mention_all': True,
          'chat_id': 'oc_test', 'chat_name': '霍振堃'}}


class Client:
    def __init__(self):
        self.sends = 0
        self.name = '霍振堃'

    def call(self, method, route, **kwargs):
        if route == 'im/v1/chats/oc_test':
            return {'name': self.name, 'chat_mode': 'group'}
        if method == 'POST':
            self.sends += 1
            assert kwargs['params']['receive_id_type'] == 'chat_id'
            assert kwargs['json']['receive_id'] == 'oc_test'
            text = json.loads(kwargs['json']['content'])['text']
            assert '<at user_id="all">' in text
            return {'message_id': 'om_test', 'chat_id': 'oc_test', 'body': {'content': json.dumps({'text': text})}}
        raise AssertionError('No message-history permission should be needed')


def test_start_once_per_run_and_uses_actual_target_date(tmp_path):
    (tmp_path / 'scope.json').write_text(json.dumps({'target_date': '2026-10-18'}))
    client = Client()
    assert notify(tmp_path, CONFIG, client)['status'] == 'verified_by_send_response'
    notify(tmp_path, CONFIG, client)
    assert client.sends == 1


def test_test_notice_does_not_require_or_claim_collection(tmp_path):
    client = Client()
    notify(tmp_path, CONFIG, client, test=True)
    assert client.sends == 1


def test_wrong_group_and_disabled_config_never_send(tmp_path):
    client = Client()
    client.name = 'other'
    with pytest.raises(ValueError, match='identity'):
        notify(tmp_path, CONFIG, client, test=True)
    with pytest.raises(ValueError, match='authorized'):
        notify(tmp_path, {}, client, test=True)
    assert client.sends == 0


def test_plain_at_everyone_text_is_not_a_real_mention():
    with pytest.raises(ValueError, match='@everyone'):
        verify_message({'chat_id': 'oc_test', 'body': {'content': json.dumps({'text': '@所有人 广大大抓取开始'})}},
                       'oc_test', '广大大抓取开始')
