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
