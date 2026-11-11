import pytest
from ua_workflows.guangdada_headless_ui import selected_presets,preset_scope_suffix


def test_default_range_stays_compatible_and_explicit_three_gets_its_own_scope():
    assert preset_scope_suffix(selected_presets())=='both'
    assert preset_scope_suffix(selected_presets(['tool2']))=='tool2'
    assert preset_scope_suffix(selected_presets(['game','tool','tool2']))=='game_tool_tool2'
    assert selected_presets(['tool2'])[0]['preset']=='afizzy竞品2'
    assert selected_presets(['tool2'])[0]['category']=='工具'

def test_daily_scope_has_four_distinct_tool_presets():
    keys = ['game', 'tool', 'tool2', 'tool3', 'tool4']
    scope = selected_presets(keys)
    assert preset_scope_suffix(scope) == 'game_tool_tool2_tool3_tool4'
    assert [(p['category'], p['preset']) for p in scope[1:]] == [
        ('工具', 'afizzy竞品'), ('工具', 'afizzy竞品2'),
        ('工具', 'afizzy竞品3'), ('工具', 'afizzy竞品4'),
    ]


@pytest.mark.parametrize('keys',[['unknown'],['tool2','tool2']])
def test_invalid_range_does_not_silently_expand(keys):
    with pytest.raises(ValueError):selected_presets(keys)
