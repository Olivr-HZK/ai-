"""Versioned visual review rules; historical scopes keep their original policy."""

NO_REAL_PERSON = 'no-real-person-v1'
MALE_PRIMARY_NO_REAL_PERSON = 'male-character-no-real-person-v1'


def policy_for(version):
    policy = {'version': version, 'real_person': 'exclude', 'uncertain': 'review'}
    if version == MALE_PRIMARY_NO_REAL_PERSON:
        policy['subject'] = 'male_primary'
    elif version != NO_REAL_PERSON:
        raise ValueError('Unsupported content review policy')
    return policy


def validate_review(scope, review):
    policy = scope.get('content_review_policy')
    if policy is None:
        return False
    version = policy.get('version') if isinstance(policy, dict) else None
    if policy != policy_for(version):
        raise ValueError('Unsupported content review policy')
    if review.get('review_policy') != version:
        raise ValueError('Review must explicitly use ' + version)
    presence = review.get('checks', {}).get('real_person_presence')
    if version == MALE_PRIMARY_NO_REAL_PERSON:
        if presence not in ('present', 'absent', 'uncertain'):
            raise ValueError('Real-person presence is missing; review required')
        if presence == 'present':
            expected = 'exclude'
        else:
            subject = review.get('checks', {}).get('primary_subject')
            if subject not in ('male', 'female', 'mixed', 'none', 'uncertain'):
                raise ValueError('Primary character subject is missing; review required')
            if subject == 'female':
                expected = 'exclude'
            elif presence == 'uncertain' or subject != 'male':
                raise ValueError('Real-person or male-primary subject is uncertain/mixed/missing; review required')
            else:
                expected = 'keep'
        if review.get('decision') != expected:
            raise ValueError('Male-primary/real-person evidence and review decision disagree')
        return True
    if presence not in ('present', 'absent'):
        raise ValueError('Real-person presence is missing or uncertain; review required')
    expected = 'exclude' if presence == 'present' else 'keep'
    if review.get('decision') != expected:
        raise ValueError('Real-person presence and review decision disagree')
    return True
