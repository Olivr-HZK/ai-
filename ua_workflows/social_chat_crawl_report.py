"""Authorized end-of-run report; counts come from evidence and verified table rows."""
from ua_workflows.collection_runtime import collection_phase
import argparse
import hashlib
import json
import uuid
from collections import Counter
from datetime import datetime
from pathlib import Path

from ua_workflows.social_chat_browser_daily import CONFIG, TZ, Feishu, load, save, table_route, table_url, text_field


def build_card(stats, url, test=False):
    if stats['eligible'] + stats['excluded'] + stats['missing'] + stats.get('relaunch', 0) != stats['new']:
        raise ValueError('Report counts do not reconcile')
    if stats['video_success'] + stats.get('image_success', 0) != stats['new'] - stats['missing'] - stats.get('relaunch', 0):
        raise ValueError('Media-success count mismatch')
    if stats.get('video_new', stats['new']) + stats.get('image_new', 0) != stats['new']:
        raise ValueError('Media-type count mismatch')
    if stats['selected'] != (stats['eligible'] + 9) // 10:
        raise ValueError('Top-decile count mismatch')
    title = ('【测试】' if test else '') + '广大大竞品素材日报｜' + stats['report_date']
    text = (f"**素材首次发现日期：{stats['target_date']}（北京时间）**\n"
            f"监测范围：{stats['tasks']} 项 App / 平台（iOS、Android 分开统计）\n\n"
            f"📊 竞品上新：**{stats['new']} 条**\n"
            f"（视频 {stats.get('video_new', stats['new'])} 条，图片 {stats.get('image_new', 0)} 条）\n"
            f"✅ 详情采集成功：**{stats['new']} 条**\n"
            f"🎬 视频获取成功：**{stats['video_success']} 条**\n"
            f"🖼️ 图片获取成功：**{stats.get('image_success', 0)} 条**\n"
            f"🔁 重投素材排除：**{stats.get('relaunch', 0)} 条**（采集后统一剔除）\n"
            f"🚫 真人口播排除：**{stats['excluded']} 条**\n"
            f"⚠️ 素材无法获取：**{stats['missing']} 条**（已记录原因后跳过）\n"
            f"📁 审核可用：**{stats['eligible']} 条**\n\n"
            f"按展示估值合并取前 **10%**，向上取整，**{stats['selected']} 条已导入多维表格并附素材附件**。\n"
            "点击下方多维表格链接查看。\n\n"
            "上新数指当前账号、监测名单和筛选条件下可见的目标日素材，不代表平台广告全集。")
    if stats.get('out_of_scope_playable'):
        text += f"\n另发现 {stats['out_of_scope_playable']} 条试玩广告，不在本次视频和图片统计范围内。"
    return {'config': {'wide_screen_mode': True},
            'header': {'template': 'blue', 'title': {'tag': 'plain_text', 'content': title}},
            'elements': [{'tag': 'div', 'text': {'tag': 'lark_md', 'content': text}},
                         {'tag': 'action', 'actions': [{'tag': 'button', 'type': 'primary',
                           'text': {'tag': 'plain_text', 'content': '查看多维表格'}, 'url': url}]}]}


def report(run_dir, config, client, test=False):
    run_dir = Path(run_dir).resolve()
    setting = config['crawl_completion_report']
    if setting.get('enabled') is not True:
        raise ValueError('Completion report is not authorized')
    chat_id = setting['chat_id']
    chat = client.call('GET', 'im/v1/chats/' + chat_id)
    if chat.get('name') != setting['chat_name'] or chat.get('chat_mode') != 'group':
        raise ValueError('Completion group identity changed')
    receipt = run_dir / ('crawl-report-test.json' if test else 'crawl-report-complete.json')
    if receipt.exists():
        previous = load(receipt)
        if previous.get('chat_id') != chat_id:
            raise ValueError('Existing report has a different destination')
        return previous
    summary = load(run_dir / 'summary.json')
    validation = load(run_dir / 'validation.json')
    if not (summary['feishu_written'] and summary['global_top10_computed'] and
            validation['collection_evidence_verified'] and validation['frame_and_delivery_hashes_verified']):
        raise ValueError('Collection/import evidence is not complete')
    candidates = load(run_dir / 'delivery/all_candidates.json')
    target = summary['target_date']
    if any(c['first_discovered_date'] != target for c in candidates):
        raise ValueError('Report contains another date')
    identities = {(c['platform'], c['app_id'], c['ad_id']) for c in candidates}
    if len(identities) != len(candidates):
        raise ValueError('Duplicate candidate identity')
    counts = Counter(c['decision'] for c in candidates)
    is_image = lambda c: c.get('media_type') in ('图片', 'image')
    if dict(counts) != summary['combined_counts'] or len(candidates) != summary['combined_target_videos']:
        raise ValueError('Candidate summary mismatch')
    missing_count = counts['review'] + counts['unavailable']
    if missing_count != validation['pending_media_count']:
        raise ValueError('Unresolved count mismatch')
    if counts['unavailable'] or counts['relaunch']:
        # Mixed runs must prove collection, actual reviews, and each failed-media
        # exception again, rather than trusting a hand-written summary.
        from ua_workflows.social_chat_mixed_window import select as mixed_select
        mixed_select(run_dir, config)
    # Recheck every downloaded candidate's six-frame source, including exclusions.
    for c in candidates:
        if c['decision'] in ('review', 'unavailable', 'relaunch'):
            continue
        root = Path(c['source_collection'])
        if is_image(c):
            review = c.get('review', {})
            if not review.get('images_inspected') or review.get('method') != 'original_image':
                raise ValueError('Original image review missing')
            images = c.get('image_files', [])
            if not images:
                raise ValueError('Original image evidence missing')
            for image in images:
                if image.get('decode_verified') is not True or min(image.get('width', 0), image.get('height', 0)) <= 0:
                    raise ValueError('Original image decode evidence invalid')
                if hashlib.sha256((root / image['path']).read_bytes()).hexdigest() != image['sha256']:
                    raise ValueError('Original image hash mismatch')
            continue
        if c['task_id'] == 'ios-03' and (root / 'tasks/heyyyy-ios').is_dir():
            root /= 'tasks/heyyyy-ios'
        frames = load(root / 'evidence/frames' / c['ad_id'] / 'manifest.json')
        if len(frames['frames']) != 6:
            raise ValueError('Missing six-frame evidence')
        if hashlib.sha256((root / frames['source']['path']).read_bytes()).hexdigest() != frames['source']['sha256']:
            raise ValueError('Downloaded video hash mismatch')
    from ua_workflows.social_chat_mixed_delivery import is_daily_row
    from ua_workflows.social_chat_browser_daily import unique_key
    rows = [r for r in client.listing(table_route(config) + '/records')
            if is_daily_row(r, target) and r['fields'].get('当前入选') is True]
    selection = load(run_dir / 'selection-with-media-waiver.json') if (run_dir / 'selection-with-media-waiver.json').exists() else load(run_dir / 'selection.json')
    expected = {unique_key(target, c) for c in selection['items']}
    actual = {text_field(r['fields'].get('唯一键')) for r in rows}
    image_ids = {c['ad_id'] for c in selection['items'] if is_image(c)}
    if expected != actual or len(rows) != len(expected) or any(
            not r['fields'].get('图片附件' if r['fields']['广告ID'] in image_ids else '视频附件') for r in rows):
        raise ValueError('Remote selected records/attachments mismatch')
    stats = {'report_date': datetime.now(TZ).date().isoformat(), 'target_date': target,
             'tasks': summary['combined_platform_tasks'], 'new': len(candidates),
             'video_new': sum(not is_image(c) for c in candidates),
             'image_new': sum(is_image(c) for c in candidates),
             'video_success': sum(not is_image(c) and c['decision'] not in ('review', 'unavailable', 'relaunch') for c in candidates),
             'image_success': sum(is_image(c) and c['decision'] not in ('review', 'unavailable', 'relaunch') for c in candidates), 'eligible': counts['keep'],
             'excluded': counts['exclude'], 'relaunch': counts['relaunch'], 'missing': missing_count, 'selected': len(rows),
             'out_of_scope_playable': summary.get('out_of_scope_playable', 0)}
    card = build_card(stats, table_url(config), test)
    save(run_dir / 'crawl-report-payload.json', {'stats': stats, 'card': card})
    seed = target + '|' + chat_id + '|complete|' + str(test)
    request_id = str(uuid.UUID(hex=hashlib.sha256(seed.encode()).hexdigest()[:32], version=4))
    message = client.call('POST', 'im/v1/messages', params={'receive_id_type': 'chat_id'},
                          json={'receive_id': chat_id, 'msg_type': 'interactive',
                                'content': json.dumps(card, ensure_ascii=False), 'uuid': request_id})
    if not message.get('message_id') or message.get('chat_id') != chat_id:
        raise ValueError('Completion send receipt mismatch')
    result = {'status': 'sent_with_api_receipt', 'chat_id': chat_id, 'chat_name': setting['chat_name'],
              'message_id': message['message_id'], 'test': test, 'stats': stats, 'url': table_url(config)}
    save(receipt, result)
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--run-dir', required=True, type=Path)
    p.add_argument('--test', action='store_true')
    args = p.parse_args()
    with collection_phase(args.run_dir, 'social_chat_crawl_report'):
        import fcntl
        with (args.run_dir / '.crawl-report.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            print(json.dumps(report(args.run_dir, load(CONFIG), Feishu(), args.test), ensure_ascii=False))


if __name__ == '__main__':
    main()
