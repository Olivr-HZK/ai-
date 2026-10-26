"""Evidence-backed style labels, separate from character/content eligibility."""

STYLE_VERSION = 'character-style-v2'
STYLE_FIELD = '风格标签'
LEGACY_STYLE_VERSION = 'character-style-v1'
LEGACY_STYLE_TAGS = ('欧美写实风', '东亚写实风', '日漫风', '韩漫风', '厚涂／CG风', '欧美卡通风', '混合风格', '待确认')
STYLE_TAGS = ('日式漫画', '韩式漫画', '欧美写实', '东亚写实', '中式漫画', '欧美卡通',
              '数字插画', '3D角色', '混合风格', '待确认')


def style_policy(version=STYLE_VERSION):
    if version == STYLE_VERSION:
        tags = STYLE_TAGS
    elif version == LEGACY_STYLE_VERSION:
        tags = LEGACY_STYLE_TAGS
    else:
        raise ValueError('Unsupported style tagging policy')
    return {'version': version, 'tags': list(tags), 'required_for_keep': True}


def validate_style_policy(policy):
    if not isinstance(policy, dict) or policy != style_policy(policy.get('version')):
        raise ValueError('Unsupported style tagging policy')
    return policy


def validate_style(scope, review):
    policy = scope.get('style_tagging')
    if policy is None:
        return None  # Historical scopes do not acquire a new review requirement.
    validate_style_policy(policy)
    tags = review.get('style_tags')
    if review.get('decision') != 'keep':
        if tags:
            raise ValueError('Excluded/pending content must not carry formal style tags')
        return []
    if review.get('style_tagging_version') != policy['version']:
        raise ValueError('Missing/pinned style tagging version required')
    if not isinstance(tags, list) or not tags or any(not isinstance(t, str) or t not in policy['tags'] for t in tags):
        raise ValueError('Missing/unsupported style tags')
    if len(set(tags)) != len(tags):
        raise ValueError('Duplicate style tags')
    if '待确认' in tags and len(tags) != 1:
        raise ValueError('Uncertain style cannot be mixed with asserted labels')
    if not isinstance(review.get('style_reason'), str) or not review['style_reason'].strip():
        raise ValueError('Style tags require an actual visual reason')
    return list(tags)
