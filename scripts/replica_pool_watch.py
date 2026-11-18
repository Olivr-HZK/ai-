"""只读监听：只看复刻需求池，判断每条需求要做什么，不提交、不写表。

用途：把「需求池模式」的判断逻辑跑起来，先看清每条需求会被怎么展开；
执行层（母版/翻译/换人）接上之前，这里不发任何付费请求，也不改飞书。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ua_workflows.social_chat_browser_daily import Feishu  # noqa: E402

APP = 'Adz4bj9ECaCCJrsgkj7coheYn3c'
MAIN = 'tblwhAqPfQoBXK3Q'
POOL = 'tblKXqqPOYqk3XPd'

ACCEPTED = '已验收'
TERMINAL = {'结果返回'}


def text(value):
    if value is None:
        return ''
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        return '、'.join(x.get('text', '') if isinstance(x, dict) else str(x) for x in value).strip()
    if isinstance(value, dict):
        return (value.get('text') or value.get('name') or '').strip()
    return str(value).strip()


def first(value):
    if isinstance(value, list):
        return text(value[0]) if value else ''
    return text(value)


def link_ids(value):
    """关联字段可能是 {"link_record_ids": [...]} 或 [{"record_ids": [...]}]。"""
    if isinstance(value, dict):
        return [x for x in (value.get('link_record_ids') or []) if isinstance(x, str)]
    if isinstance(value, list):
        ids = []
        for row in value:
            if not isinstance(row, dict):
                continue
            for key in ('record_ids', 'link_record_ids'):
                ids.extend(x for x in (row.get(key) or []) if isinstance(x, str))
            if isinstance(row.get('record_id'), str):
                ids.append(row['record_id'])
        return ids
    return []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-dir', default='data/replica-pool-watch')
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--interval', type=float, default=15.0)
    args = parser.parse_args()

    run_dir = Path(args.run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / 'watch.log'
    state_path = run_dir / 'state.json'

    lock = run_dir / 'watch.lock'
    if lock.exists():
        try:
            alive = int(lock.read_text().strip() or 0)
        except ValueError:
            alive = 0
        running = False
        if alive > 0:
            try:
                os.kill(alive, 0)
                running = True
            except OSError:
                running = False
        if running:
            print('另一个监听在跑：' + str(lock), file=sys.stderr)
            return 1
        lock.unlink(missing_ok=True)  # 上次进程已死，清掉陈旧锁
    lock.write_text(str(os.getpid()))

    feishu = Feishu()
    token_at = time.time()
    print('监听启动：只读 ' + POOL, flush=True)

    source_cache = {}
    try:
        while True:
            started = time.time()
            if time.time() - token_at > 5400:  # 飞书 tenant token 约 2 小时过期，提前换
                feishu, token_at = Feishu(), time.time()
            try:
                rows = feishu.call('POST', f'bitable/v1/apps/{APP}/tables/{POOL}/records/search',
                                   params={'page_size': 500, 'user_id_type': 'open_id'}, json={}).get('items', [])
            except Exception as error:  # 超时或 token 失效都不结束进程，下一轮再试
                with log_path.open('a') as log:
                    log.write(f'{time.strftime("%Y-%m-%dT%H:%M:%S")} 读取失败 {type(error).__name__}: {error}\n')
                print('读取失败，稍后重试：', error, flush=True)
                if '99991663' in str(error) or 'Invalid access token' in str(error):
                    feishu, token_at = Feishu(), time.time()
                if args.once:
                    return 1
                time.sleep(max(1.0, args.interval))
                continue
            items, counts = [], {}
            for row in rows:
                f = row['fields']
                status = text(f.get('需求状态'))
                kind = text(f.get('需求类型'))
                lang = text(f.get('目标语言（单选）')) or first(f.get('目标语言'))
                role = text(f.get('换人角色'))
                lip = bool(f.get('口型优先'))
                links = link_ids(f.get('关联原素材'))
                videos = f.get('原视频') or []
                source_id = links[0] if links else ''
                direct = bool(videos) and not links
                source, source_error = {}, ''
                if source_id:
                    cached = source_cache.get(source_id)
                    if cached and time.time() - cached[0] < 300:
                        source = cached[1]
                    else:
                        try:
                            source = feishu.call('GET', f'bitable/v1/apps/{APP}/tables/{MAIN}/records/{source_id}',
                                                 params={'user_id_type': 'open_id'})['record']['fields']
                            source_cache[source_id] = (time.time(), source)
                        except Exception as error:  # 单条读超时只影响这一行，不结束整轮
                            source_error = type(error).__name__
                            if cached:
                                source = cached[1]
                                source_error = ''
                master = bool(source.get('复刻母版'))
                master_state = text(source.get('母版状态'))
                src_lang = first(source.get('文案语言'))
                steps = []
                note = ''
                progress = text(f.get('复刻状态')) or text(f.get('执行进度'))
                if direct:
                    steps = ['原视频（直接上传）']
                    if kind == '替换人物':
                        steps.append('换人→' + role if role else '换人')
                    note = '可执行（不查母版）'
                elif source_error:
                    note = f'素材读取失败（{source_error}），本轮跳过'
                elif kind and kind != '替换人物':
                    note = f'主表负责（{kind}），换人表只接替换人物'
                elif status != ACCEPTED:
                    note = f'跳过：需求状态={status or "空"}'
                elif progress == '已完成':
                    note = '跳过：已完成'
                elif not source_id:
                    note = '停：没有关联素材'
                elif kind not in {'替换logo', '替换语言', '替换人物'}:
                    note = f'停：需求类型={kind or "空"}'
                else:
                    steps.append('母版（已有，复用）' if master else '母版（需换 Logo）')
                    if kind == '替换语言':
                        if not lang:
                            note = '停：替换语言没选语言'
                        elif lang != src_lang:
                            steps.append(f'翻译→{lang}')
                    elif kind == '替换人物':
                        if not role:
                            note = '停：替换人物没填角色'
                        if lang and src_lang and lang != src_lang:
                            steps.append(('翻译→' + lang + '（先）') if lip else ('翻译→' + lang + '（后）'))
                        steps.append('换人→' + role if role else '换人')
                        steps.append('口型优先：走 A' if lip else '默认路径：走 B')
                key = 'ok' if not note.startswith('停') else 'blocked'
                counts[key] = counts.get(key, 0) + 1
                items.append({'record_id': row['record_id'], '需求名称': text(f.get('需求名称')),
                              '需求状态': status, '需求类型': kind, '目标语言': lang, '换人角色': role,
                              '口型优先': lip, '素材': source_id, '母版': master, '母版状态': master_state,
                              '源语言': src_lang, '复刻状态': text(f.get('复刻状态')) or text(f.get('执行进度')), '判断': note or '可执行',
                              '顺序': ' → '.join(steps)})
            state = {'checked_at': time.strftime('%Y-%m-%dT%H:%M:%S'), 'total': len(items),
                     'counts': counts, 'items': items}
            state_path.write_text(json.dumps(state, ensure_ascii=False, indent=1))
            with log_path.open('a') as log:
                log.write(state['checked_at'] + ' ' + json.dumps(
                    {'total': len(items), 'counts': counts,
                     'rows': [{k: i[k] for k in ('需求名称', '需求状态', '判断', '顺序')} for i in items]},
                    ensure_ascii=False) + '\n')
            print(state['checked_at'], 'total', len(items), counts, flush=True)
            for i in items:
                print('  -', i['需求名称'], '|', i['需求状态'], '|', i['判断'], '|', i['顺序'], flush=True)
            if args.once:
                return 0
            time.sleep(max(1.0, args.interval - (time.time() - started)))
    finally:
        lock.unlink(missing_ok=True)


if __name__ == '__main__':
    raise SystemExit(main())
