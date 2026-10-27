"""Resolve the shared media attachment column and merge existing file tokens."""
DEFAULT_ATTACHMENT_FIELD = '竞品素材（图片/视频/Playable）去除男频'
LEGACY_ATTACHMENT_FIELDS = ('视频附件', '图片附件')


def configured_name(config):
    return config.get('feishu', {}).get('attachment_field_name', DEFAULT_ATTACHMENT_FIELD)


def resolve_field(config, fields):
    settings = config.get('feishu', {})
    field_id = settings.get('attachment_field_id')
    matches = [f for f in fields if f.get('field_id') == field_id] if field_id else [
        f for f in fields if f['field_name'] == configured_name(config)]
    if len(matches) != 1 or matches[0]['type'] != 17:
        raise ValueError('Unified media attachment field is missing, ambiguous, or not an attachment')
    return matches[0]


def merge_plan(rows, field):
    """Only add to the unified field; never clear source/replication/business fields."""
    target = field['field_name']
    updates, expected, kinds, missing = [], {}, {}, []
    for row in rows:
        fields = row['fields']
        kind = str(fields.get('素材类型', '')).strip().lower()
        if kind not in ('图片', 'image', 'playable', '试玩', '试玩广告'):
            continue
        source_names = ['图片附件'] if kind in ('图片', 'image') else [
            'Playable附件', 'playable附件', '试玩附件', 'HTML附件', '图片附件']
        sources = [a for name in source_names for a in (fields.get(name) or [])]
        if not sources:
            sources = fields.get('下载视频/图片') or []
        combined, tokens = [], set()
        for attachment in [*(fields.get(target) or []), *sources]:
            token = attachment.get('file_token')
            if not token:
                raise ValueError('Existing attachment has no file token')
            if token not in tokens:
                combined.append(attachment)
                tokens.add(token)
        if not combined:
            missing.append(row['record_id'])
            continue
        old_tokens = [a['file_token'] for a in (fields.get(target) or [])]
        if [a['file_token'] for a in combined] == old_tokens:
            continue
        updates.append({'record_id': row['record_id'], 'fields': {
            target: [{'file_token': a['file_token']} for a in combined]}})
        expected[row['record_id']] = combined
        kinds[row['record_id']] = kind
    return {'field_id': field['field_id'], 'field_name': target, 'updates': updates,
            'expected_attachments': expected, 'kinds': kinds, 'missing_record_ids': missing}


def verify_merge(rows, plan):
    by_id = {row['record_id']: row['fields'] for row in rows}
    for record_id, expected in plan['expected_attachments'].items():
        actual = by_id.get(record_id, {}).get(plan['field_name']) or []
        if [a['file_token'] for a in actual] != [a['file_token'] for a in expected]:
            raise ValueError('Unified attachment token readback mismatch: ' + record_id)
        for wanted, got in zip(expected, actual):
            if got.get('size') != wanted.get('size') or got.get('name') != wanted.get('name'):
                raise ValueError('Unified attachment name/size readback mismatch: ' + record_id)
    return {'verified_records': len(plan['expected_attachments']),
            'verified_files': sum(len(a) for a in plan['expected_attachments'].values())}
