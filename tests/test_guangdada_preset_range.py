import pytest
from ua_workflows.guangdada_headless_ui import selected_presets,preset_scope_suffix


def test_default_range_stays_compatible_and_explicit_three_gets_its_own_scope():
    assert preset_scope_suffix(selected_presets())=='both'
    assert preset_scope_suffix(selected_presets(['tool2']))=='tool2'
    assert preset_scope_suffix(selected_presets(['game','tool','tool2']))=='game_tool_tool2'
    assert selected_presets(['tool2'])[0]['preset']=='afizzy竞品2'
    assert selected_presets(['tool2'])[0]['category']=='工具'


@pytest.mark.parametrize('keys',[['unknown'],['tool2','tool2']])
def test_invalid_range_does_not_silently_expand(keys):
    with pytest.raises(ValueError):selected_presets(keys)
