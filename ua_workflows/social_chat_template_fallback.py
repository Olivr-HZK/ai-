"""Read previous-day original attachments for visual template review."""
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
import argparse
import hashlib
import json
import re
import shutil
import tempfile

from requests import RequestException

from ua_workflows import social_chat_browser_daily as daily
from ua_workflows.social_chat_attachments import resolve_field
from ua_workflows.social_chat_media_orientation import IMAGES, VIDEOS, inspect_media
from ua_workflows.social_chat_template_history import FEISHU_FALLBACK, history_state, identity

FIELDS = ('目标日期', '首次发现日期', '广告主', '平台', '来源', 'App ID', '广告ID', '唯一键', '素材类型', '当前入选')
MAX_BYTES = 512 * 1024 * 1024


def reference_day(scope):
    day = (date.fromisoformat(scope['target_date']) - timedelta(days=1)).isoformat()
    if (scope.get('template_history_fallback') != FEISHU_FALLBACK
            or scope['previous_day_template_dedupe']['reference_date'] != day):
        raise ValueError('本批未固定飞书模板备用路径或参考日不符')
    return day


def record_os(fields, day, ad_id):
    """Resolve the OS from the observed daily or legacy per-App writeback key."""
    key = daily.text_field(fields.get('唯一键')).split('|')
    if (len(key) == 5 and key[0] == '广大大独立' and key[1] == day and key[4] == ad_id
            and re.fullmatch(r'[0-9a-f]{20}', key[2]) and key[3] in ('iOS', 'Android')):
        return key[3]
    app_id = daily.text_field(fields.get('App ID'))
    if (len(key) == 4 and key[0] == day and key[3] == ad_id and app_id
            and key[2] == app_id and key[1] in ('iOS', 'Android')):
        return key[1]
    return None


def previous_rows(client, scope, day, ad_id):
    route = daily.table_route(scope)
    schema = client.listing(route + '/fields')
    attachment = resolve_field(scope, schema)
    names = {field['field_name'] for field in schema}
    required = set(FIELDS) - {'App ID'}
    if not required <= names:
        raise ValueError('模板备用来源缺少必要记录字段')
    filters = {'目标日期': day, '平台': '广大大', '来源': '每日最新'}
    if ad_id:
        filters['广告ID'] = ad_id
    body = {'field_names': [name for name in FIELDS if name in names] + [attachment['field_name']],
            'filter': {'conjunction': 'and', 'conditions': [
                {'field_name': name, 'operator': 'is', 'value': [value]}
                for name, value in filters.items()]}}
    rows, cursors, params = [], set(), {'page_size': 100}
    while True:
        page = client.call('POST', route + '/records/search', params=params, json=body)
        rows.extend(daily.normalize_record_texts(row) for row in page.get('items', []))
        if not page.get('has_more'):
            return attachment, rows
        cursor = page.get('page_token')
        if not cursor or cursor in cursors:
            raise ValueError('模板备用来源分页不完整')
        cursors.add(cursor)
        params = {**params, 'page_token': cursor}


def download(client, scope, field, record_id, attachment, path):
    token, size = attachment['file_token'], attachment['size']
    if not re.fullmatch(r'[A-Za-z0-9_-]+', token) or type(size) is not int or not 0 < size <= MAX_BYTES:
        raise ValueError('附件标识或字节数无效')
    # Construct the permission scope from the verified row rather than following a stored URL.
    extra = {'bitablePerm': {'tableId': scope['feishu']['table_id'],
                            'attachments': {field['field_id']: {record_id: [token]}}}}
    url = f'https://open.feishu.cn/open-apis/drive/v1/medias/{token}/download'
    with client.session.get(url, params={'extra': json.dumps(extra)}, stream=True,
                            timeout=(15, 60), allow_redirects=False) as response:
        if response.status_code != 200:
            raise ValueError(f'附件读取失败 HTTP {response.status_code}')
        written = 0
        with path.open('xb') as stream:
            for chunk in response.iter_content(chunk_size=256 * 1024):
                written += len(chunk)
                if written > size:
                    raise ValueError('附件超过表格记录的大小')
                stream.write(chunk)
    if written != size:
        raise ValueError('附件字节数与表格记录不符')


def fetch_record(client, scope, field, row, out, expected):
    fields, record_id = row['fields'], row['record_id']
    kind = daily.text_field(fields['素材类型'])
    attachments = fields.get(field['field_name']) or []
    if not attachments or (kind == '视频' and len(attachments) != 1):
        raise ValueError('原附件缺失或视频附件数量不明确')
    if len({item['file_token'] for item in attachments}) != len(attachments):
        raise ValueError('原附件有重复标识')
    sources = []
    with tempfile.TemporaryDirectory(prefix='.attachment-', dir=out) as staging:
        for index, attachment in enumerate(attachments, 1):
            suffix = Path(attachment['name']).suffix.lower()
            if suffix not in (IMAGES if kind == '图片' else VIDEOS):
                raise ValueError('原附件扩展名与素材类型不符')
            path = Path(staging) / f'{index:02d}{suffix}'
            download(client, scope, field, record_id, attachment, path)
            proof = inspect_media(path)
            if proof['status'] == 'unknown' or proof.get('kind') != kind:
                raise ValueError('附件无法确认为原图或视频流')
            sources.append({'path': str(out / record_id / path.name), 'sha256': proof['source_sha256'],
                            'bytes': path.stat().st_size})
        if expected and Counter(s['sha256'] for s in sources) != Counter(expected):
            raise ValueError('附件 SHA-256 与本地审核原件不一致')
        # Publish only the complete validated set, including every original in a multi-image row.
        Path(staging).rename(out / record_id)
    return sources


def prepare(run_dir, app, os_name, *, app_id='', ad_id='', expected=(), client=None):
    run_dir = Path(run_dir).resolve()
    scope_bytes = (run_dir / 'scope.json').read_bytes()
    scope = json.loads(scope_bytes)
    day = reference_day(scope)
    if expected and (not ad_id or any(not re.fullmatch(r'[0-9a-f]{64}', value) for value in expected)):
        raise ValueError('恢复原件须指定广告 ID 和全部原件 SHA-256')
    mapping = scope['template_identity_mapping']
    product = identity(app, os_name, mapping, app_id)
    root = run_dir / 'template-history-fallback'
    root.mkdir(exist_ok=True)
    out = Path(tempfile.mkdtemp(prefix='references-', dir=root))
    result = {'reference_date': day, 'scope_sha256': hashlib.sha256(scope_bytes).hexdigest(),
              'product': product[0], 'os': os_name, 'ad_id': ad_id,
              'mode': 'restore_original' if expected else 'compare_only', 'coverage': 'partial',
              'references': [], 'issues': []}
    try:
        client = client or daily.Feishu()
        field, rows = previous_rows(client, scope, day, ad_id)
        result['source'] = {'table_id': scope['feishu']['table_id'], 'attachment_field_id': field['field_id']}
        matches = []
        for row in rows:
            fields = row['fields']
            text = {name: daily.text_field(fields.get(name)) for name in FIELDS if name != '当前入选'}
            candidate = text['广告ID']
            if (text['目标日期'] != day or text['首次发现日期'] != day
                    or text['平台'] != '广大大' or text['来源'] != '每日最新'
                    or text['素材类型'] not in ('图片', '视频') or not candidate
                    or (ad_id and ad_id != candidate) or (not expected and fields.get('当前入选') is not True)
                    or record_os(fields, day, candidate) != os_name):
                continue
            try:
                candidate_product = identity(text['广告主'], os_name, mapping, text['App ID'])
            except ValueError:
                continue
            if candidate_product == product and not (app_id and text['App ID'] and text['App ID'] != app_id):
                matches.append((candidate, row))
        counts = Counter(candidate for candidate, _ in matches)
        for candidate, row in matches:
            record_id = row.get('record_id', '')
            if counts[candidate] != 1 or not re.fullmatch(r'[A-Za-z0-9_-]+', record_id):
                result['issues'].append({'ad_id': candidate, 'reason': '表格记录身份不唯一或无效'})
                continue
            try:
                sources = fetch_record(client, scope, field, row, out, expected)
            except (KeyError, TypeError, ValueError, OSError, RequestException) as error:
                result['issues'].append({'ad_id': candidate, 'reason': str(error) if type(error) is ValueError else type(error).__name__})
                continue
            result['references'].append({'ad_id': candidate, 'record_id': record_id,
                                         'kind': daily.text_field(row['fields']['素材类型']), 'sources': sources})
    except (KeyError, TypeError, ValueError, OSError, RequestException) as error:
        result['issues'].append({'reason': type(error).__name__})
    result['history_state'] = history_state(result['references'], source_state='feishu_partial')
    receipt = out / 'references.json'
    daily.save(receipt, result)
    return receipt, result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir', type=Path, required=True)
    parser.add_argument('--app', required=True)
    parser.add_argument('--os', choices=['Android', 'iOS'], required=True)
    parser.add_argument('--app-id', default='')
    parser.add_argument('--ad-id', default='')
    parser.add_argument('--expected-sha256', action='append', default=[])
    args = parser.parse_args()
    try:
        path, result = prepare(args.run_dir, args.app, args.os, app_id=args.app_id,
                               ad_id=args.ad_id, expected=args.expected_sha256)
    except (KeyError, ValueError, OSError) as error:
        parser.error(str(error))
    print(json.dumps({'receipt': str(path), 'references': len(result['references']),
                      'issues': len(result['issues']), **result['history_state']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
