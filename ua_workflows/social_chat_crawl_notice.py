"""Explicitly authorized, once-per-run Feishu crawl-start notice."""
from ua_workflows.collection_runtime import collection_phase
import argparse
import hashlib
import json
import uuid
from pathlib import Path

from ua_workflows.social_chat_browser_daily import CONFIG, Feishu, load, save


def verify_message(message, chat_id, marker):
    if message.get('chat_id') != chat_id:
        raise ValueError('Notice chat readback mismatch')
    text = json.loads(message['body']['content'])['text']
    if marker not in text:
        raise ValueError('Notice text readback mismatch')
    mentions = message.get('mentions') or []
    # Feishu normalizes the inline tag to @_all in returned text.
    if not ('@_all' in text or '<at user_id="all"' in text or
            any(m.get('id') == 'all' or m.get('key') == '@_all' for m in mentions)):
        raise ValueError('Notice @everyone readback missing')


def notify(run_dir, config, client, test=False):
    run_dir = Path(run_dir).resolve()
    setting = config.get('crawl_start_notice', {})
    if setting.get('enabled') is not True or setting.get('mention_all') is not True:
        raise ValueError('No authorized @everyone notification configured')
    chat_id, name = setting['chat_id'], setting['chat_name']
    chat = client.call('GET', 'im/v1/chats/' + chat_id)
    if chat.get('name') != name or chat.get('chat_mode') != 'group':
        raise ValueError('Configured test group identity changed')
    state_path = run_dir / ('crawl-notice-test.json' if test else 'crawl-notice-start.json')
    marker = '【测试】广大大抓取通知' if test else '广大大抓取开始'
    if state_path.exists():
        state = load(state_path)
        if state.get('chat_id') != chat_id:
            raise ValueError('Existing notification belongs to another group')
        if state.get('status') in ('verified', 'verified_by_send_response', 'verified_in_feishu_ui'):
            return state
        message_id = state['message_id']
    else:
        scope = load(run_dir / 'scope.json') if not test else {}
        if not test and not scope.get('target_date'):
            raise ValueError('Target date must be fixed before sending start notice')
        description = ('这是一条启动通知测试，本次不重新抓取。后续实际启动广大大抓取时，'
                       '会在本群通知。' if test else
                       '正在启动广大大素材抓取，目标首次发现日期：' + scope['target_date'] + '。')
        text = '<at user_id="all">所有人</at>\n' + marker + '\n' + description
        seed = str(run_dir) + '|' + chat_id + '|' + ('test' if test else 'start')
        request_id = str(uuid.UUID(hex=hashlib.sha256(seed.encode()).hexdigest()[:32], version=4))
        message = client.call('POST', 'im/v1/messages', params={'receive_id_type': 'chat_id'},
                              json={'receive_id': chat_id, 'msg_type': 'text',
                                    'content': json.dumps({'text': text}, ensure_ascii=False), 'uuid': request_id})
        message_id = message['message_id']
        # Persist the send receipt before readback, so a read-permission failure cannot resend.
        save(state_path, {'chat_id': chat_id, 'chat_name': name, 'message_id': message_id,
                          'status': 'sent_pending_readback', 'test': test})
        # Send API returns the server-normalized body and mentions too; validate
        # this receipt without requiring unrelated message-history permissions.
        verify_message(message, chat_id, marker)
        state = {'chat_id': chat_id, 'chat_name': name, 'message_id': message_id,
                 'status': 'verified_by_send_response', 'test': test, 'mention_all': True}
        save(state_path, state)
        return state
    result = client.call('GET', 'im/v1/messages/' + message_id)
    messages = result.get('items') or []
    if len(messages) != 1:
        raise ValueError('Notice readback did not return one message')
    verify_message(messages[0], chat_id, marker)
    state = {'chat_id': chat_id, 'chat_name': name, 'message_id': message_id,
             'status': 'verified', 'test': test, 'mention_all': True}
    save(state_path, state)
    return state


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--run-dir', required=True, type=Path)
    p.add_argument('--test', action='store_true')
    args = p.parse_args()
    from contextlib import nullcontext
    from ua_workflows.social_chat_daily_state import tracked
    with (nullcontext() if args.test else tracked(args.run_dir,'START_NOTICE_PENDING')), collection_phase(args.run_dir, 'social_chat_crawl_notice'):
        import fcntl
        args.run_dir.mkdir(parents=True, exist_ok=True)
        with (args.run_dir / '.crawl-notice.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            print(json.dumps(notify(args.run_dir, load(CONFIG), Feishu(), args.test), ensure_ascii=False))


if __name__ == '__main__':
    main()
