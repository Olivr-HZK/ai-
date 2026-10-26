"""Pinned ranking rules for new daily batches and historical resumes."""
from decimal import Decimal, InvalidOperation, ROUND_CEILING


def rule(value=None):
    # Scopes created before ranking was pinned used the global top decile.
    value = value or {'mode': 'fraction', 'fraction': '0.10', 'rounding': 'ceil'}
    mode = value.get('mode', 'fraction')
    if mode == 'top_n':
        n = value.get('count')
        if isinstance(n, bool) or not isinstance(n, int) or n <= 0:
            raise ValueError('Ranking count must be a positive integer')
        return {'mode': 'top_n', 'count': n}
    if mode != 'fraction':
        raise ValueError('Unsupported ranking mode')
    try:
        fraction = Decimal(str(value['fraction']))
    except (KeyError, InvalidOperation) as exc:
        raise ValueError('Invalid ranking fraction') from exc
    if not fraction.is_finite() or not 0 < fraction <= 1 or value.get('rounding', 'ceil') != 'ceil':
        raise ValueError('Invalid ranking fraction or rounding')
    return {'mode': 'fraction', 'fraction': str(fraction), 'rounding': 'ceil'}


def count(eligible, ranking=None):
    if isinstance(eligible, bool) or not isinstance(eligible, int) or eligible < 0:
        raise ValueError('Invalid eligible count')
    r = rule(ranking)
    return min(eligible, r['count']) if r['mode'] == 'top_n' else int(
        (Decimal(eligible) * Decimal(r['fraction'])).to_integral_value(rounding=ROUND_CEILING))


def label(ranking=None):
    r = rule(ranking)
    if r['mode'] == 'top_n':
        return f"前{r['count']}条（不足时取全部通过项）"
    return f"前{Decimal(r['fraction']) * 100:g}%，向上取整"
