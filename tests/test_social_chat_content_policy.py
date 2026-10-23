import copy

import pytest

from ua_workflows.social_chat_content_policy import NO_REAL_PERSON, MALE_PRIMARY_NO_REAL_PERSON, policy_for, validate_review
from ua_workflows import social_chat_browser_daily as daily


POLICY = {'version': NO_REAL_PERSON, 'real_person': 'exclude', 'uncertain': 'review'}
MALE_POLICY = policy_for(MALE_PRIMARY_NO_REAL_PERSON)


@pytest.mark.parametrize('presence,decision', [('present', 'exclude'), ('absent', 'keep')])
def test_new_policy_uses_presence_not_speech(presence, decision):
    assert validate_review({'content_review_policy': POLICY}, {
        'review_policy': NO_REAL_PERSON, 'decision': decision,
        'checks': {'real_person_presence': presence, 'real_person_talking_head': False},
    })


@pytest.mark.parametrize('presence,decision', [
    ('uncertain', 'keep'), (None, 'keep'), ('present', 'keep'), ('absent', 'exclude'),
])
def test_missing_uncertain_or_inconsistent_review_blocks_ranking(presence, decision):
    with pytest.raises(ValueError):
        validate_review({'content_review_policy': POLICY}, {
            'review_policy': NO_REAL_PERSON, 'decision': decision,
            'checks': {'real_person_presence': presence},
        })


def test_old_talking_head_record_cannot_pass_new_policy():
    with pytest.raises(ValueError, match='explicitly'):
        validate_review({'content_review_policy': POLICY}, {
            'decision': 'keep', 'checks': {'real_person_talking_head': False},
        })
    assert validate_review({}, {'checks': {'real_person_talking_head': False}}) is False


def test_prepare_pins_new_policy_without_rewriting_historical_scope(tmp_path):
    cfg = daily.load(daily.CONFIG)
    old = copy.deepcopy(cfg)
    old.pop('content_review_policy', None)
    old_dir = tmp_path / 'old'
    daily.prepare('2026-10-21', old_dir, old)
    before = (old_dir / 'scope.json').read_bytes()
    assert daily.prepare('2026-10-21', old_dir, cfg)['status'] == 'resume_existing'
    assert (old_dir / 'scope.json').read_bytes() == before
    new_dir = tmp_path / 'new'
    daily.prepare('2026-10-23', new_dir, cfg)
    assert daily.load(new_dir / 'scope.json')['content_review_policy'] == MALE_POLICY
    instruction = (new_dir / 'browser-task.txt').read_text()
    assert '仅排除真人口播' not in instruction
    assert '男性角色为主体' in instruction and '女性主体均排除' in instruction


@pytest.mark.parametrize('presence,subject,decision', [
    ('absent', 'male', 'keep'),
    ('absent', 'female', 'exclude'),
    ('uncertain', 'female', 'exclude'),
    ('present', 'male', 'exclude'),
    ('present', 'female', 'exclude'),
    ('present', None, 'exclude'),
])
def test_male_primary_combined_rule(presence, subject, decision):
    assert validate_review({'content_review_policy': MALE_POLICY}, {
        'review_policy': MALE_PRIMARY_NO_REAL_PERSON, 'decision': decision,
        'checks': {'real_person_presence': presence, 'primary_subject': subject},
    })


@pytest.mark.parametrize('presence,subject,decision', [
    ('absent', 'female', 'keep'), ('present', 'male', 'keep'),
    ('absent', 'male', 'exclude'), ('absent', 'mixed', 'keep'),
    ('absent', 'none', 'keep'), ('absent', 'uncertain', 'keep'),
    ('absent', None, 'keep'), ('uncertain', 'male', 'keep'),
    ('absent', 'mixed', 'exclude'),
])
def test_subject_must_be_reviewed_and_consistent(presence, subject, decision):
    with pytest.raises(ValueError):
        validate_review({'content_review_policy': MALE_POLICY}, {
            'review_policy': MALE_PRIMARY_NO_REAL_PERSON, 'decision': decision,
            'checks': {'real_person_presence': presence, 'primary_subject': subject},
        })


def test_no_real_person_only_review_cannot_certify_male_subject():
    with pytest.raises(ValueError, match='explicitly'):
        validate_review({'content_review_policy': MALE_POLICY}, {
            'review_policy': NO_REAL_PERSON, 'decision': 'keep',
            'checks': {'real_person_presence': 'absent'},
        })


def test_resuming_v1_preserves_its_pinned_policy(tmp_path):
    cfg = daily.load(daily.CONFIG)
    old = {**cfg, 'content_review_policy': POLICY}
    daily.prepare('2026-10-21', tmp_path, old)
    before = (tmp_path / 'scope.json').read_bytes()
    resumed = daily.prepare('2026-10-21', tmp_path, cfg)
    assert resumed['status'] == 'resume_existing' and resumed['review_policy'] == NO_REAL_PERSON
    assert (tmp_path / 'scope.json').read_bytes() == before


def test_unsupported_policy_is_rejected_before_scope_is_written(tmp_path):
    cfg = copy.deepcopy(daily.load(daily.CONFIG))
    cfg['content_review_policy']['version'] = 'unknown-policy'
    with pytest.raises(ValueError, match='Unsupported'):
        daily.prepare('2026-10-23', tmp_path, cfg)
    assert not (tmp_path / 'scope.json').exists()
