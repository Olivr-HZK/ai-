import copy
import pytest
from ua_workflows import social_chat_browser_daily as daily
from ua_workflows.social_chat_content_policy import PLUGIN_COMPANION, PLUGIN_FEEDBACK, policy_for, validate_review


def review(presence='absent', subject='male', decision='keep'):
    return {'review_policy':PLUGIN_FEEDBACK,'decision':decision,'checks':{
        'real_person_presence':presence,'primary_subject':subject,
        'male_character_display':'yes','female_oriented_appeal':'yes','real_person_talking_head':'absent'}}


@pytest.mark.parametrize('presence,subject,decision',[
    ('present','male','exclude'),('present','female','exclude'),
    ('absent','female','exclude'),('absent','male','keep'),
    ('uncertain','male','review'),('absent','mixed','review'),
])
def test_new_plugin_feedback_checks_people_and_primary_subject(presence,subject,decision):
    assert validate_review({'content_review_policy':policy_for(PLUGIN_FEEDBACK)},review(presence,subject,decision))


def test_no_speech_does_not_certify_no_real_people():
    r=review();r['checks'].pop('real_person_presence')
    with pytest.raises(ValueError,match='presence'):validate_review({'content_review_policy':policy_for(PLUGIN_FEEDBACK)},r)
    with pytest.raises(ValueError,match='disagree'):validate_review({'content_review_policy':policy_for(PLUGIN_FEEDBACK)},review('present','male','keep'))


def test_new_scopes_pin_feedback_but_existing_scopes_keep_their_policy(tmp_path):
    config=daily.load(daily.CONFIG);old=copy.deepcopy(config)
    old['plugin_handoff']['content_review_policy']=policy_for(PLUGIN_COMPANION)
    old_dir=tmp_path/'old';daily.prepare('2026-11-03',old_dir,old)
    before=(old_dir/'scope.json').read_bytes();daily.prepare('2026-11-03',old_dir,config)
    assert (old_dir/'scope.json').read_bytes()==before
    new_dir=tmp_path/'new';daily.prepare('2026-11-04',new_dir,config)
    assert daily.load(new_dir/'scope.json')['content_review_policy']==policy_for(PLUGIN_FEEDBACK)
    task=(new_dir/'handoff-task.md').read_text()
    assert '不以主持或口播为前提' in task and '不按女仆服' in task
    assert '非口播真人恋爱剧情可保留' not in task
