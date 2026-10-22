"""Shared likes selection after each social platform's eligibility filters."""

RULE = 'likes_top10_below100_else_decile_v1'


def selection_policy(denominator):
    if type(denominator) is not int or denominator < 0:
        raise ValueError('Candidate count must be a nonnegative integer')
    small = denominator < 100
    return {'selection_rule': RULE, 'selection_mode': 'top_n' if small else 'fraction',
            'candidate_threshold': 100, 'top_n': 10 if small else None,
            'fraction': None if small else '0.10', 'rounding': None if small else 'ceil',
            'selected': min(denominator, 10) if small else (denominator + 9) // 10}


def selection_report(selection):
    if selection['denominator'] < 100:
        rule = '按点赞量合并取前 **10篇**（有效候选少于100篇，不足10篇全选）'
    else:
        rule = '按点赞量合并取前 **10%**，向上取整'
    return rule + f"：**{selection['selected']} 篇**"
