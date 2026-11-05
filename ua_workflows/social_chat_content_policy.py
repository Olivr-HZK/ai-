"""Versioned visual review rules; historical scopes keep their original policy."""

NO_REAL_PERSON = 'no-real-person-v1'
MALE_PRIMARY_NO_REAL_PERSON = 'male-character-no-real-person-v1'
PLUGIN_COMPANION = 'male-display-female-oriented-no-talking-head-v1'
PLUGIN_FEEDBACK = 'male-display-female-oriented-no-real-person-v2'


def policy_for(version):
    if version in (PLUGIN_COMPANION, PLUGIN_FEEDBACK):
        policy = {'version': version, 'male_display': 'required',
                  'female_oriented_appeal': 'required', 'uncertain': 'not_admitted'}
        if version == PLUGIN_COMPANION:
            policy['real_person_talking_head'] = 'exclude'
        else:
            policy.update(real_person='exclude', female_primary='exclude',
                          gender_evidence='context_not_costume', hands_only='retain_existing_scope')
        return policy
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
    if version in (PLUGIN_COMPANION, PLUGIN_FEEDBACK):
        checks = review.get('checks', {})
        if version == PLUGIN_FEEDBACK:
            presence = checks.get('real_person_presence')
            if presence not in ('present', 'absent', 'uncertain'):
                raise ValueError('Explicit real-person presence is required; absence of speech is insufficient')
            subject = checks.get('primary_subject')
            if presence == 'present':
                expected = 'exclude'
            else:
                if subject not in ('male', 'female', 'mixed', 'none', 'uncertain'):
                    raise ValueError('Explicit primary subject is required')
                if subject == 'female':
                    expected = 'exclude'
                elif presence == 'uncertain' or subject in ('mixed', 'uncertain'):
                    expected = 'review'
                elif subject == 'none':
                    expected = 'exclude'
                else:
                    expected = None
        else:
            speech = checks.get('real_person_talking_head')
            if speech not in ('present', 'absent', 'uncertain'):
                raise ValueError('Explicit talking-head evidence is required')
            expected = 'exclude' if speech == 'present' else 'review' if speech == 'uncertain' else None
        if expected is None:
            male, appeal = checks.get('male_character_display'), checks.get('female_oriented_appeal')
            if male not in ('yes', 'no', 'uncertain') or appeal not in ('yes', 'no', 'uncertain'):
                raise ValueError('Male display and female-oriented evidence are required')
            expected = 'exclude' if 'no' in (male, appeal) else 'review' if 'uncertain' in (male, appeal) else 'keep'
        if review.get('decision') != expected:
            raise ValueError('Plugin content evidence and decision disagree')
        return True
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
