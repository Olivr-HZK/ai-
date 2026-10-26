import pytest

from ua_workflows.social_selection import selection_policy, selection_report


@pytest.mark.parametrize('total,selected,mode',[(0,0,'top_n'),(7,7,'top_n'),(99,10,'top_n'),(100,10,'fraction'),(101,11,'fraction')])
def test_policy_and_report_agree(total,selected,mode):
    policy=selection_policy(total)
    assert policy['selected']==selected and policy['selection_mode']==mode
    message=selection_report({'denominator':total,**policy})
    assert ('10篇' in message)==(total<100)
    assert ('10%' in message)==(total>=100)
    assert f'：**{selected} 篇**' in message


@pytest.mark.parametrize('bad',[-1,True,1.1,None])
def test_invalid_denominator_blocks(bad):
    with pytest.raises(ValueError):selection_policy(bad)


@pytest.mark.parametrize('total,expected',[(0,0),(1,1),(99,99),(100,100),(101,100),(1001,100)])
def test_fixed_top100_policy_and_report(total,expected):
    policy=selection_policy(total, 'likes_top100_v1')
    assert policy['selected']==expected and policy['top_n']==100
    message=selection_report({'denominator':total,**policy})
    assert '100篇' in message and '10%' not in message


def test_unknown_rule_blocks():
    with pytest.raises(ValueError,match='Unknown'):
        selection_policy(10,'unknown')
