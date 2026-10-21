#!/usr/bin/env python3
"""Local evidence/checkpoint validator. Never drives a browser or calls a network API."""
import argparse
import hashlib
import json
import re
import struct
import sys
from datetime import datetime, timezone
from pathlib import Path


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    tmp.replace(path)


def load(path):
    return json.loads(path.read_text())


class GateError(ValueError):
    pass


def require(test, message):
    if not test:
        raise GateError(message)


def local(root, relative):
    require(isinstance(relative, str) and relative, '必须提供相对文件路径')
    path = (root / relative).resolve()
    require(path.is_relative_to(root.resolve()), '证据和交付文件必须位于本任务目录内')
    require(path.is_file() and path.stat().st_size > 0, f'文件不存在或为空：{relative}')
    return path


def record(root, name, fingerprint, evidence=True):
    value = load(local(root, name))
    require(value.get('scope_sha256') == fingerprint, f'{name} 的条件指纹不匹配，不能复用旧任务')
    if evidence:
        require(bool(value.get('evidence')), f'{name} 缺少 UI/视觉证据路径')
        for file in value['evidence']:
            local(root, file)
    return value


def validate_scope(scope):
    if 'collection_limit_per_app' in scope:
        require(type(scope['collection_limit_per_app']) is int and scope['collection_limit_per_app'] > 0, '每 App 采集上限必须是正整数')
    require(isinstance(scope.get('filters'), dict) and scope['filters'], '缺少 filters')
    filters = scope['filters']
    require(filters.get('category') in ['游戏', '工具'], 'category 必须是 游戏 或 工具')
    require(filters.get('dedupe') in ['广告', '素材'], 'dedupe 必须是 广告 或 素材')
    for key in ['channels', 'regions']:
        require(isinstance(filters.get(key), list), f'{key} 必须是列表，空列表表示不限制')
    date = filters.get('date', {})
    require(isinstance(date, dict), 'date 必须是快捷或自定义日期对象')
    if date.get('mode') == 'preset':
        require(date.get('preset') in ['7天', '30天', '90天', '1年'], '日期快捷项仅支持 7天/30天/90天/1年')
        require(set(date) == {'mode', 'preset'}, '快捷日期不能混入自定义起止日期')
    elif date.get('mode') == 'custom':
        require(set(date) == {'mode', 'start', 'end'}, '自定义日期必须填写 start/end')
        dates = [datetime.strptime(date[k], '%Y-%m-%d').date() for k in ['start', 'end']]
        require(all(d.isoformat() == date[k] for d, k in zip(dates, ['start', 'end'])), '日期须为 YYYY-MM-DD')
        require(dates[0] <= dates[1], '自定义开始日期不能晚于结束日期')
    else:
        raise GateError('date.mode 必须为 preset 或 custom')
    require(filters.get('sort') in ['最新创意', '展示估值', '热度'], 'sort 必须选择 最新创意、展示估值 或 热度')
    require('time_window' not in filters, '请迁移为 date 对象，避免快捷/自定义日期混用')
    require(all(type(v) is bool for v in scope.get('content_checks', {}).values()), 'content_checks 必须使用布尔判据')
    tasks = scope.get('tasks', [])
    require(bool(tasks), '至少提供一个完整 App 名称 + 平台任务')
    ids = []
    for task in tasks:
        task_id = task.get('id', '')
        require(re.fullmatch(r'[a-z0-9][a-z0-9-]{0,63}', task_id), 'task id 只能用小写字母、数字、连字符')
        require(task.get('platform') in ['iOS', 'Android'], '平台必须是 iOS 或 Android')
        require(bool(task.get('app_name', '').strip()), '必须填写完整 App 名称')
        ids.append(task_id)
    require(len(set(ids)) == len(ids), 'task id 重复')


def file_hash(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest() if hasattr(hashlib, 'file_digest') else hashlib.sha256(stream.read()).hexdigest()


def image_evidence(root, item, allow_jpeg=False):
    path = local(root, item.get('path'))
    require(file_hash(path) == item.get('sha256'), '截图/抽帧图片哈希不符')
    data = path.read_bytes()
    width, height = 0, 0
    if len(data) >= 24 and data[:8] == b'\x89PNG\r\n\x1a\n' and data[12:16] == b'IHDR':
        width, height = struct.unpack('>II', data[16:24])
    elif allow_jpeg and data[:3] == b'\xff\xd8\xff':
        pos = 2
        while pos < len(data):
            require(data[pos] == 255, 'JPEG 标记损坏')
            while pos < len(data) and data[pos] == 255:
                pos += 1
            require(pos < len(data), 'JPEG 截断')
            marker = data[pos];pos += 1
            if marker in [1, 216, 217] or 208 <= marker <= 215:
                continue
            require(pos + 2 <= len(data), 'JPEG 段长度缺失')
            length = int.from_bytes(data[pos:pos+2], 'big')
            require(length >= 2 and pos + length <= len(data), 'JPEG 段截断')
            if marker in [192,193,194,195,197,198,199,201,202,203,205,206,207]:
                require(length >= 8, 'JPEG 尺寸段损坏')
                height = int.from_bytes(data[pos+3:pos+5], 'big')
                width = int.from_bytes(data[pos+5:pos+7], 'big')
                break
            require(marker != 218, 'JPEG 缺少尺寸段')
            pos += length
    else:
        raise GateError('截图须为实际 PNG/JPEG；视频抽帧须为 PNG')
    require(width > 0 and height > 0, '图片尺寸无效')
    return width, height


def png_image(root, item):
    return image_evidence(root, item, allow_jpeg=False)


def ui_equal(left, right, set_like=False):
    if set_like and isinstance(left, list) and isinstance(right, list):
        return sorted(json.dumps(v, sort_keys=True, ensure_ascii=False) for v in left) == sorted(json.dumps(v, sort_keys=True, ensure_ascii=False) for v in right)
    return json.dumps(left, sort_keys=True, ensure_ascii=False) == json.dumps(right, sort_keys=True, ensure_ascii=False)


def click_check(root, filename, fingerprint, task_id, key, expected):
    event = record(root, filename, fingerprint, evidence=False)
    readonly = event.get('producer') == 'gdd-readonly-check/v1'
    card_identity = event.get('producer') == 'gdd-checked-card/v1'
    if readonly:
        require(key.startswith('filter.') and key != 'filter.sort', '只读证据仅用于基础筛选；App和排序仍须真实点击')
        require(event.get('kind') == 'read' and ui_equal(event.get('before'), expected, key in ['filter.channels', 'filter.regions', 'filter.languages']), '只读检查前值必须已符合要求；有残留或变化须真实修改')
    elif card_identity:
        require(key == 'card.open' and event.get('kind') == 'click', '身份式点击证据仅用于真实点卡')
        require(isinstance(expected, dict) and re.fullmatch(r'[a-f0-9]{32}', expected.get('ad_id', '')), '点卡须读回真实32位广告 ID')
    else:
        require(event.get('producer') == 'gdd-checked-click/v1' and event.get('kind') == 'click', '缺少浏览器脚本点击记录')
    require(event.get('task_id') == task_id and event.get('key') == key, '点击记录任务或检查对象不匹配')
    require(event.get('status') == 'passed' and event.get('error') is None, '脚本点击未成功或读回状态不符')
    set_like = key in ['filter.channels', 'filter.regions', 'filter.languages']
    expected_before_click = {k: expected[k] for k in ['app_id', 'platform']} if card_identity else expected
    require(ui_equal(event.get('expected'), expected_before_click, set_like) and ui_equal(event.get('after'), expected, set_like), '点击后实际读回值与要求不符')
    require(event.get('event_id') and event.get('performed_at'), '点击记录缺少事件 ID 或执行时间')
    images = event.get('images', {})
    require(images.get('before') and images.get('after'), '点击必须保存前后截图')
    require(images['before']['path'] != images['after']['path'], '点击前后截图必须分别保存')
    for item in images.values():
        image_evidence(root, item, allow_jpeg=True)
    return event


def event_time(event):
    value = datetime.fromisoformat(event['performed_at'].replace('Z', '+00:00'))
    require(value.tzinfo is not None, '点击时间必须带时区')
    return value


def filter_checks(root, record_value, scope, task_id, fingerprint, include_sort=True):
    wanted = {k: v for k, v in scope['filters'].items() if include_sort or k != 'sort'}
    selected = record_value.get('selected', {})
    require(set(selected) == set(wanted) and all(ui_equal(selected[k], value, k in ['channels', 'regions', 'languages']) for k, value in wanted.items()), 'UI 筛选值与本阶段任务条件不完全一致')
    refs = record_value.get('ui_checks', {})
    require(set(refs) == set(wanted), '本阶段点击检查字段不完整或混入了其他阶段字段')
    for key, expected in wanted.items():
        require(key in refs, f'缺少 {key} 的脚本点击和截图检查')
        click_check(root, refs[key], fingerprint, task_id, 'filter.'+key, expected)


def app_click_check(root, value, fingerprint, task_id):
    expected = {k: value[k] for k in ['app_name', 'platform', 'app_id', 'selected_app_ids', 'residual_keywords']}
    click_check(root, value.get('ui_check'), fingerprint, task_id, 'app', expected)


def frame_check(root, ref, fingerprint, ad_id):
    manifest = record(root, ref, fingerprint, evidence=False)
    require(manifest.get('producer') == 'gdd-frames/v1' and manifest.get('ad_id') == ad_id, '缺少对应广告的视频抽帧脚本产物')
    source = manifest.get('source', {})
    require(file_hash(local(root, source.get('path'))) == source.get('sha256'), '抽帧源视频哈希不符')
    require(source.get('decode_verified') is True and source.get('duration_seconds', 0) > 0, '抽帧源视频未解码或时长无效')
    frames = manifest.get('frames', [])
    fractions = [0.03, 0.15, 0.30, 0.50, 0.70, 0.90]
    require(len(frames) == 6 and [f.get('fraction') for f in frames] == fractions, '必须有六处真实抽帧和采样时间点')
    require(len({f.get('path') for f in frames}) == 6, '抽帧路径重复')
    for frame in frames:
        dimensions = png_image(root, frame)
        require(dimensions == (frame.get('width'), frame.get('height')), '抽帧图片尺寸与清单不符')
        require(0 <= frame.get('requested_timestamp_seconds', -1) < source['duration_seconds'], '抽帧时间点不在视频内')
    return manifest


def identity(value, task):
    name_matches = value.get('app_name') == task['app_name']
    # Preserve the site's observed full name for renamed/localized products.
    # An alias only works for an explicitly scoped, exactly matching App ID;
    # the App click check still compares the actual observed name verbatim.
    verified_alias = (bool(task.get('app_id')) and
                      value.get('app_id') == task['app_id'] and
                      bool(str(value.get('app_name') or '').strip()) and
                      value.get('canonical_app_name') == task['app_name'])
    require(name_matches or verified_alias, 'App 全名不匹配')
    require(value.get('platform') == task['platform'], '平台不匹配')
    require(bool(value.get('app_id')), '缺少已选广告主 app_id')
    if task.get('app_id'):
        require(value['app_id'] == task['app_id'], 'App ID 不匹配')


def checkpoint(root, fingerprint):
    saved = []
    for path in (root / 'cards').glob('*.json'):
        try:
            card = load(path)
            if card.get('scope_sha256') == fingerprint and card.get('detail_loaded') is True and all(card.get(k) for k in ['ad_id', 'detail_url', 'app_id', 'platform', 'channel', 'regions']) and (card.get('video_url') or card.get('media_unavailable_reason')):
                click_check(root, card.get('ui_check'), fingerprint, root.name, 'card.open', {k: card[k] for k in ['ad_id', 'app_id', 'platform']})
                saved.append((int(card['page']), int(card['position'])))
        except (OSError, ValueError, KeyError, TypeError):
            continue
    page = 1
    while (root / 'pages' / f'{page:04d}.json').exists():
        try:
            sealed = load(root / 'pages' / f'{page:04d}.json')
            if sealed.get('scope_sha256') != fingerprint:
                break
            if sealed.get('has_next') is False:
                return {'saved_card_count': len(saved), 'next': None, 'note': '已记录末页；仍需通过数量和内容校验'}
            if sealed.get('has_next') is not True:
                break
            page += 1
        except (OSError, ValueError, TypeError):
            break
    position = 1
    while (page, position) in saved:
        position += 1
    return {'saved_card_count': len(saved), 'next': {'page': page, 'position': position}}


def check_task(run, scope, task, fingerprint, not_before=None):
    root = run / 'tasks' / task['id']
    passed = []
    state, action = 'BROWSER_PENDING', '打开可操作浏览器并确认已登录广大大'
    cards, reviews, keep = {}, [], []
    result = {'task_id': task['id'], 'app_name': task['app_name'], 'platform': task['platform'], 'checkpoint': checkpoint(root, fingerprint)}
    try:
        browser = record(run, 'browser.json', fingerprint)
        require(browser.get('authenticated') is True and browser.get('browser_session'), '浏览器未登录或无会话标识')
        passed.append('BROWSER_READY')
        state, action = 'CATEGORY_PENDING', '检查游戏/工具分类，已正确则只读验证，变化才点击修改'
        filters = record(root, 'filters.json', fingerprint)
        category_ref = filters.get('ui_checks', {}).get('category')
        category = click_check(root, category_ref, fingerprint, task['id'], 'filter.category', scope['filters']['category'])
        if not_before:
            require(event_time(category) >= not_before, '本 App 分类检查必须晚于上一个 App 封账，不能复用旧检查证据')
        passed.append('CATEGORY_VERIFIED')
        state, action = 'FILTERS_PENDING', '设置并检查除排序栏以外的全部筛选'
        filter_checks(root, filters, scope, task['id'], fingerprint, include_sort=False)
        base_times = [event_time(record(root, ref, fingerprint, evidence=False)) for ref in filters['ui_checks'].values()]
        require(all(t >= event_time(category) for t in base_times), '必须先核对游戏/工具，再核对其余筛选（可只读，变化才修改）')
        passed.append('BASE_FILTERS_VERIFIED')
        state, action = 'APP_PENDING', '输入完整 App 名，选择正确平台候选，清除旧广告主和残留关键词'
        app = record(root, 'app.json', fingerprint)
        identity(app, task)
        if app.get('query_type', 'full_name') == 'app_id':
            require(app.get('query_app_id') == app['app_id'], 'App ID 查询值与已选广告主不符')
        else:
            require(app.get('query_type', 'full_name') == 'full_name', '未知 App 查询方式')
            require(app.get('query_full_name') == task['app_name'], '未使用完整 App 名称查询')
        require(app.get('selected_app_ids') == [app['app_id']], '存在旧广告主或未真正选中候选')
        require(app.get('residual_keywords') == [], '存在额外关键词，可能制造假空结果')
        app_click_check(root, app, fingerprint, task['id'])
        expected = app.get('card_count')
        require(type(expected) is int and expected >= 0, '必须记录当前广告/素材列表的 UI 卡片数 card_count')
        require(app.get('count_basis') == scope['filters']['dedupe'], 'UI 卡片数口径与广告/素材模式不一致')
        site_expected = expected
        limit = scope.get('collection_limit_per_app')
        capped = limit is not None and site_expected > limit
        if limit is not None:
            expected = min(site_expected, limit)
        app_event = record(root, app['ui_check'], fingerprint, evidence=False)
        require(event_time(app_event) >= max(base_times), '必须先完成基础筛选，再输入并选中 App')
        passed.append('APP_VERIFIED')
        state, action = 'SORT_PENDING', 'App 确认后，再点击本次指定排序并检查'
        sort = record(root, 'sort.json', fingerprint)
        require(sort.get('selected') == scope['filters']['sort'], '排序值不符')
        sort_event = click_check(root, sort.get('ui_check'), fingerprint, task['id'], 'filter.sort', scope['filters']['sort'])
        require(event_time(sort_event) >= event_time(app_event), '排序必须在选中本 App 之后点击')
        passed.append('SORT_VERIFIED')
        state, action = 'PROBE_PENDING', '打开一张卡片验证详情身份与媒体可读取'
        if expected:
            probe = record(root, 'probe.json', fingerprint)
            require(probe.get('detail_loaded') is True, '最小点卡测试未成功')
            identity(probe, task)
            require(probe['app_id'] == app['app_id'], '测试卡片不是已选广告主')
            probe_card = record(root, probe.get('card_file'), fingerprint)
            identity(probe_card, task)
            require(probe_card.get('detail_loaded') is True and probe_card.get('app_id') == app['app_id'], '测试详情记录身份或加载状态不符')
            require(all(probe_card.get(k) for k in ['ad_id', 'detail_url', 'channel', 'regions']), '测试卡缺少关键详情字段')
            require(probe_card.get('video_url') or probe_card.get('media_unavailable_reason'), '测试卡未读取媒体或说明缺失原因')
            probe_event = click_check(root, probe_card.get('ui_check'), fingerprint, task['id'], 'card.open', {k: probe_card[k] for k in ['ad_id', 'app_id', 'platform']})
            require(event_time(probe_event) >= event_time(sort_event), '必须确认 App 后排序，再开始点卡')
            passed.append('PROBE_PASSED')
        else:
            require(app.get('empty_result_verified') is True, '零结果未在选择正确广告主后核实')
            passed.append('PROBE_NOT_APPLICABLE_ZERO_RESULTS')
        state, action = 'COLLECTING', '逐卡保存详情，逐页封账，直到明确末页'
        pages = [load(p) for p in sorted((root / 'pages').glob('*.json'))]
        require(bool(pages), '缺少逐页盘点文件')
        pages.sort(key=lambda p: p['page'])
        require([p['page'] for p in pages] == list(range(1, len(pages)+1)), '页码不连续')
        slots, observed = [], []
        for n, page in enumerate(pages):
            page = record(root, f"pages/{page['page']:04d}.json", fingerprint)
            if page['page'] > 1:
                page_event = click_check(root, page.get('ui_check'), fingerprint, task['id'], 'page.open', {'page': page['page']})
                require(event_time(page_event) >= event_time(sort_event), '翻页记录早于本 App 的排序确认')
            refs = page.get('cards', [])
            capped_last = capped and n == len(pages)-1
            require(type(page.get('visible_count')) is int and (page['visible_count'] >= len(refs) if capped_last else page['visible_count'] == len(refs)), '当前页卡片未全部获取')
            require(page['visible_count'] > 0 or expected == 0, '非零结果不能用空白加载页结束')
            if capped_last:
                require(type(page.get('has_next')) is bool, '达到上限时仍须记录实际下一页状态')
                require(page.get('terminal_reason') == 'limit_reached' and page.get('collection_limit') == limit, '达到用户上限须单独封账，不冒充末页')
                require(page['visible_count'] > len(refs) or page['has_next'], '达到上限的边界缺少未采卡片/下一页证据')
            else:
                require(page.get('has_next') is (n < len(pages)-1), '尚有下一页，或页面链不一致')
            if n == len(pages)-1 and not capped_last:
                require(page.get('terminal_reason') in ['next_disabled', 'explicit_no_results'], '末页缺少明确终止证据')
                require((page['terminal_reason'] == 'explicit_no_results') == (expected == 0), '空结果终止条件与 UI 数量矛盾')
            for position, ref in enumerate(refs, 1):
                card = record(root, ref, fingerprint)
                require(card.get('detail_loaded') is True, '卡片详情加载未完成')
                identity(card, task)
                require(card['app_id'] == app['app_id'], '卡片广告主与已选广告主不符')
                require(card.get('page') == page['page'] and card.get('position') == position, '卡片页码/位置不匹配')
                require(card.get('ad_id') and card.get('detail_url') and card.get('channel'), '卡片缺少关键详情字段')
                require(isinstance(card.get('regions'), list) and card['regions'], '卡片缺少投放地区')
                require(card.get('video_url') or card.get('media_unavailable_reason'), '视频地址缺失且没有记录原因')
                card_event = click_check(root, card.get('ui_check'), fingerprint, task['id'], 'card.open', {k: card[k] for k in ['ad_id', 'app_id', 'platform']})
                require(event_time(card_event) >= event_time(sort_event), '卡片点击早于本 App 的排序确认')
                slots.append((page['page'], position))
                observed.append(card['ad_id'])
                cards[card['ad_id']] = card
        require(len(slots) == expected, f'卡片覆盖数 {len(slots)} 与 UI 素材数 {expected} 不一致')
        require(len(set(observed)) == expected, '跨页出现重复广告 ID；结果可能变化，需核对分页而非直接完成')
        if expected:
            require(probe['card_file'] in [ref for page in pages for ref in page['cards']], '测试卡未纳入完整盘点')
        passed.append('CRAWL_COMPLETE')
        collection = {'app_name': task['app_name'], 'platform': task['platform'], 'app_id': app['app_id'], 'card_count': len(cards), 'page_count': len(pages), 'ad_ids': sorted(cards), 'count_basis': scope['filters']['dedupe'], 'sort': scope['filters']['sort']}
        if limit is not None:
            collection.update(site_card_count=site_expected, collection_limit=limit, termination_reason=pages[-1]['terminal_reason'])
        result['collection'] = collection
        state, action = 'COLLECTION_SUMMARY_PENDING', '写入本 App 采集完成清单，核对后才进入下一个 App'
        summary = record(root, 'collection_complete.json', fingerprint)
        require(summary.get('producer') == 'gdd-collection-seal/v1' and summary.get('status') == 'completed', '缺少脚本核对后的 App 完成清单')
        require(summary.get('collection') == collection and summary.get('completed_at'), 'App 完成清单与实际盘点不一致')
        completed_at = datetime.fromisoformat(summary['completed_at'])
        require(completed_at.tzinfo is not None, 'App 清单完成时间必须带时区')
        result['collection_completed_at'] = summary['completed_at']
        passed.append('APP_COLLECTION_COMPLETE')
        state, action = 'FRAMES_PENDING', '对全部视频运行抽帧脚本并校验源视频、六处截图和哈希'
        frame_items = record(root, 'frames.json', fingerprint, evidence=False).get('items', [])
        require(len(frame_items) == len(cards) and {i['ad_id'] for i in frame_items} == set(cards), '原始素材尚未全部完成脚本抽帧')
        frame_sources, frame_evidence = {}, {}
        for item in frame_items:
            manifest = frame_check(root, item['manifest'], fingerprint, item['ad_id'])
            frame_sources[item['ad_id']] = manifest['source']
            frame_evidence[item['ad_id']] = {f['path'] for f in manifest['frames']}
        passed.append('FRAMES_VERIFIED')
        state, action = 'REVIEW_PENDING', '对全部原始卡片记录保留/排除/待复核和理由'
        review_doc = record(root, 'reviews.json', fingerprint, evidence=False)
        reviews = review_doc.get('items', [])
        require(len(reviews) == len(cards) and {r['ad_id'] for r in reviews} == set(cards), '筛选结果未覆盖全部素材或存在重复项')
        for review in reviews:
            require(review.get('frames_inspected') is True, '尚未实际查看该视频的脚本抽帧截图')
            require(frame_evidence.get(review.get('ad_id'), set()).issubset(set(review.get('evidence', []))), '筛选证据必须引用该视频的全部六张脚本抽帧')
            require(review.get('decision') in ['keep', 'exclude', 'review'] and review.get('reason'), '缺少筛选决定或理由')
            require(review.get('method') in ['sampled_frames', 'full_video', 'metadata'], '必须标明实际审核方式')
            if review['method'] != 'metadata':
                require(review.get('evidence'), '内容筛选缺少采样图/视频证据')
                for file in review['evidence']:
                    local(root, file)
            if review['decision'] == 'keep':
                card = cards[review['ad_id']]
                allowed = scope['filters'].get('channels', [])
                allowed = ['FB News Feed' if c in ('Facebook', 'Facebook系') else c for c in allowed]
                require(not allowed or card['channel'] in allowed, '保留项包含 Facebook 系附带渠道或非目标渠道')
                wanted_regions = scope['filters'].get('regions', [])
                require(not wanted_regions or bool(set(wanted_regions) & set(card['regions'])), '保留项投放地区不符')
                wanted_languages = scope['filters'].get('languages', [])
                require(not wanted_languages or card.get('language') in wanted_languages, '保留项文案语言不符')
                for key, value in scope.get('content_checks', {}).items():
                    require(review.get('checks', {}).get(key) is value, f'保留项未满足内容条件：{key}')
                if scope.get('content_checks'):
                    require(review['method'] != 'metadata', '仅凭元数据不能通过视觉内容筛选')
                keep.append(review['ad_id'])
        passed.append('REVIEW_COMPLETE')
        state, action = 'MEDIA_PENDING', '下载所有保留项并校验文件、哈希、尺寸、时长及解码'
        downloads = record(root, 'downloads.json', fingerprint, evidence=False).get('items', [])
        require(len(downloads) == len(keep) and {d['ad_id'] for d in downloads} == set(keep), '保留项尚未全部下载')
        for item in downloads:
            video = local(root, item.get('path'))
            require(item.get('sha256') == file_hash(video), '视频哈希不符')
            require(item['sha256'] == frame_sources[item['ad_id']]['sha256'], '交付视频与抽帧审核的视频不是同一文件内容')
            require(item.get('decode_verified') is True and item.get('duration_seconds', 0) > 0, '视频未确认解码成功/时长无效')
            width, height = item.get('width', 0), item.get('height', 0)
            require(width > 0 and height > 0, '缺少真实视频尺寸')
            ratio = scope['filters'].get('aspect_ratio')
            if ratio:
                a, b = map(float, ratio.split(':'))
                require(abs(width/height-a/b) <= 0.01, '下载视频实际比例不符')
        passed.append('MEDIA_COMPLETE')
        state, action = 'DELIVERY_PENDING', '验证预览、相对路径及交付文件清单'
        delivery = record(root, 'delivery.json', fingerprint)
        require(delivery.get('preview_verified') is True and delivery.get('portable_links_verified') is True, '未验证预览/可移交链接')
        require(len(delivery.get('included_ad_ids', [])) == len(keep) and set(delivery.get('included_ad_ids', [])) == set(keep), '交付清单与保留项不一致')
        require(delivery.get('files'), '交付文件清单为空')
        for file in delivery['files']:
            local(root, file)
        for item in downloads:
            require(item['path'] in delivery['files'], '已下载视频未包含于交付文件清单')
        passed.append('DELIVERY_COMPLETE')
        state = 'COMPLETED_WITH_REVIEW' if any(r['decision'] == 'review' for r in reviews) else 'COMPLETED'
        action = '交付并披露待复核数量和审核方式' if state.endswith('WITH_REVIEW') else '交付结果'
        result['counts'] = {'collected': len(cards), 'keep': len(keep), 'review': sum(r['decision'] == 'review' for r in reviews), 'exclude': sum(r['decision'] == 'exclude' for r in reviews)}
    except (GateError, OSError, ValueError, KeyError, TypeError) as exc:
        result['gate_failure'] = str(exc)
    result.update(state=state, next_action=action, passed=passed)
    hold_path = root / 'hold.json'
    if hold_path.exists():
        hold = load(hold_path)
        result.update(suspended_state=state, state='WAITING_HUMAN', hold=hold, next_action='等待用户手动处理；恢复后重新核对登录态、筛选和广告主')
    return result


def inspect(run):
    scope = load(run/'scope.json'); validate_scope(scope)
    fingerprint = digest(scope)
    metadata = load(run/'run.json')
    require(metadata['scope_sha256'] == fingerprint, 'scope.json 已改变；请初始化新任务，旧证据不能沿用')
    tasks, previous, not_before = [], None, None
    for task in scope['tasks']:
        if previous:
            item = {'task_id':task['id'], 'app_name':task['app_name'], 'platform':task['platform'], 'state':'PREVIOUS_APP_PENDING', 'passed':[], 'blocked_by_task':previous, 'next_action':'先完成上一个 App 的采集并写入清单'}
        else:
            item = check_task(run, scope, task, fingerprint, not_before)
            if 'APP_COLLECTION_COMPLETE' in item['passed'] and item['state'] != 'WAITING_HUMAN':
                not_before = datetime.fromisoformat(item['collection_completed_at'])
            else:
                previous = task['id']
        tasks.append(item)
    complete = all(t['state'] in ['COMPLETED', 'COMPLETED_WITH_REVIEW'] for t in tasks)
    state = ('COMPLETED_WITH_REVIEW' if any(t['state'] == 'COMPLETED_WITH_REVIEW' for t in tasks) else 'COMPLETED') if complete else ('WAITING_HUMAN' if any(t['state'] == 'WAITING_HUMAN' for t in tasks) else 'IN_PROGRESS')
    next_collection = next((t['task_id'] for t in tasks if 'APP_COLLECTION_COMPLETE' not in t['passed'] or t['state'] == 'WAITING_HUMAN'), None)
    checklist = [{'task_id': t['task_id'], 'app_name':t['app_name'], 'platform':t['platform'], 'collection_status':'completed' if 'APP_COLLECTION_COMPLETE' in t['passed'] else 'pending', 'collection':t.get('collection'), 'collection_completed_at':t.get('collection_completed_at'), 'state':t['state']} for t in tasks]
    write(run/'app_checklist.json', {'scope_sha256':fingerprint, 'items':checklist, 'next_collection_task':next_collection})
    result = {'scope_sha256': fingerprint, 'state': state, 'checked_at': datetime.now(timezone.utc).isoformat(), 'tasks': tasks, 'next_collection_task':next_collection, 'collection_complete':next_collection is None}
    result['next_action'] = ('继续采集：'+next_collection) if next_collection else ('全部 App 已采集并登记，继续抽帧/审核/交付核验' if not complete else '交付结果')
    if state == 'WAITING_HUMAN':
        result['next_action'] = '等待用户处理验证；不得继续点卡'
    write(run/'state.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    init = sub.add_parser('init');init.add_argument('--config', required=True);init.add_argument('--run-dir', required=True)
    check = sub.add_parser('check');check.add_argument('--run-dir', required=True);check.add_argument('--require-complete', action='store_true')
    seal = sub.add_parser('seal');seal.add_argument('--run-dir', required=True);seal.add_argument('--task', required=True)
    hold = sub.add_parser('hold');hold.add_argument('--run-dir', required=True);hold.add_argument('--task', required=True);hold.add_argument('--reason', required=True);hold.add_argument('--cursor', required=True)
    resume = sub.add_parser('resume');resume.add_argument('--run-dir', required=True);resume.add_argument('--task', required=True);resume.add_argument('--confirmation', required=True);resume.add_argument('--verification', required=True)
    args = parser.parse_args();run = Path(args.run_dir).resolve()
    try:
        if args.command == 'init':
            require(not run.exists(), '任务目录已存在，拒绝覆盖')
            scope = load(Path(args.config));validate_scope(scope);run.mkdir(parents=True)
            write(run/'scope.json', scope);write(run/'run.json', {'scope_sha256': digest(scope)})
            for task in scope['tasks']:(run/'tasks'/task['id']).mkdir(parents=True)
        elif args.command == 'seal':
            current = inspect(run)
            item = next((t for t in current['tasks'] if t['task_id'] == args.task), None)
            require(item is not None and 'CRAWL_COMPLETE' in item['passed'] and item['state'] != 'WAITING_HUMAN', '尚未完成本 App 全部点卡与分页，不能填写完成清单')
            root = run/'tasks'/args.task
            scope = load(run/'scope.json')
            pages = sorted((root/'pages').glob('*.json'))
            last = load(pages[-1])
            write(root/'collection_complete.json', {'producer':'gdd-collection-seal/v1','scope_sha256':digest(scope),'status':'completed','completed_at':datetime.now(timezone.utc).isoformat(),'collection':item['collection'],'evidence':[str(pages[-1].relative_to(root))]+last['evidence']})
        elif args.command in ['hold', 'resume']:
            scope = load(run/'scope.json');task = next((t for t in scope['tasks'] if t['id'] == args.task), None)
            require(task is not None, '未知 task id')
            root = run/'tasks'/args.task
            if args.command == 'hold':
                write(root/'hold.json', {'reason': args.reason, 'cursor': args.cursor, 'created_at': datetime.now(timezone.utc).isoformat()})
            else:
                require(bool(args.confirmation.strip()), '必须记录真实用户确认语句')
                hold_doc = load(local(root, 'hold.json'))
                verification = record(root, args.verification, digest(scope))
                require(verification.get('authenticated') is True, '恢复后尚未登录')
                filter_checks(root, verification, scope, task['id'], digest(scope))
                app_click_check(root, verification, digest(scope), task['id'])
                # Recovery evidence must come from after this hold, never old click logs.
                refs = list(verification['ui_checks'].values()) + [verification['ui_check']]
                held_at = datetime.fromisoformat(hold_doc['created_at'])
                for ref in refs:
                    event = record(root, ref, digest(scope), evidence=False)
                    require(datetime.fromisoformat(event['performed_at'].replace('Z', '+00:00')) >= held_at, '恢复复用了暂停前的点击证据')
                identity(verification, task)
                require(verification.get('selected_app_ids') == [verification['app_id']] and verification.get('residual_keywords') == [], '恢复后广告主条件不符')
                require(verification['app_id'] == record(root, 'app.json', digest(scope))['app_id'], '恢复后广告主 ID 变化')
                history = root/'resumes.json';resumes = load(history) if history.exists() else []
                resumes.append({'hold': hold_doc, 'user_confirmation': args.confirmation, 'verification': args.verification, 'resumed_at': datetime.now(timezone.utc).isoformat()})
                write(history, resumes);(root/'hold.json').unlink()
        result = inspect(run);print(json.dumps(result, ensure_ascii=False, indent=2))
        return 2 if args.command == 'check' and args.require_complete and result['state'] not in ['COMPLETED', 'COMPLETED_WITH_REVIEW'] else 0
    except (GateError, OSError, ValueError, KeyError, TypeError) as exc:
        print(json.dumps({'error': str(exc)}, ensure_ascii=False));return 1


if __name__ == '__main__':
    sys.exit(main())
