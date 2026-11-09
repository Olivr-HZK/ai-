"""Explicit product identity and verified-empty history for visual templates."""
from collections import defaultdict
from pathlib import Path
import hashlib
import json

LEGACY_VERSION = 'previous-day-template-review-v1'
VERSION = 'previous-day-template-baseline-v2'
FEISHU_FALLBACK = 'feishu-original-attachments-v1'


def policy_for(version):
    base = {'version':version,'history_days':1,'identity_scope':'same_app_and_os',
            'method':'bound_visual_evidence','history_input':'all_confirmed_reviewed_templates',
            'duplicate':'exclude_current','uncertain':'not_admitted','missing_history':'pending_not_zero'}
    if version == VERSION:
        base.update(identity_scope='same_product_and_os',
                    verified_empty_history='new_baseline_then_intraday_template_dedupe',
                    history_error='pending_not_zero',
                    template_confirmation_independent_of_final_selection=True)
    elif version != LEGACY_VERSION:
        raise ValueError('Unsupported previous-day template policy')
    return base


def identity(app, os, mapping, app_id=''):
    if not app or os not in ('Android','iOS'):
        raise ValueError('Template product identity is incomplete')
    matches = set()
    for product in mapping.get('products',[]):
        entry = product['apps'].get(os,{})
        if app in entry.get('names',[]) or app_id and app_id in entry.get('app_ids',[]):
            if app_id and entry.get('app_ids') and app_id not in entry['app_ids']:
                raise ValueError('App ID conflicts with template product alias')
            matches.add(product['name'])
    if len(matches)>1:
        raise ValueError('Ambiguous template product identity')
    return (next(iter(matches)) if matches else app,os)


def confirmed_templates(records):
    # The previous-day comparison or final selection can still be pending; a
    # separately confirmed template structure remains useful on the next day.
    return [r for r in records if r.get('visually_inspected') is True and
            r.get('template_review',{}).get('relation') in ('distinct','same_template','related_not_duplicate')]


def load_confirmed_history(base):
    base=Path(base).resolve();pointer=base/'template-history-latest.json'
    if pointer.exists():
        meta=json.loads(pointer.read_text());path=(base/meta['path']).resolve()
        if not path.is_relative_to(base) or hashlib.sha256(path.read_bytes()).hexdigest()!=meta['sha256']:
            raise ValueError('Revised template history path/hash mismatch')
        payload=json.loads(path.read_text())
        if payload['original_review_sha256']!=hashlib.sha256((base/'review_observations.json').read_bytes()).hexdigest():
            raise ValueError('Revised template history original review changed')
        return confirmed_templates(payload['records'])
    payload=json.loads((base/'review_observations.json').read_text())
    return confirmed_templates(payload['items'])


def history_state(templates, *, source_state, identity_verified=True, unreviewed_candidates=False):
    if not identity_verified:
        return {'status':'pending','reason':'identity_unverified'}
    if source_state == 'feishu_partial' and templates:
        return {'status':'compare_previous_partial','reference_count':len(templates),
                'no_match_status':'pending'}
    if source_state not in ('verified','verified_no_prior_batch') or unreviewed_candidates:
        return {'status':'pending','reason':'history_unavailable_or_unreviewed'}
    if templates:
        return {'status':'compare_previous','reference_count':len(templates)}
    return {'status':'new_baseline','reference_count':0,
            'reason':'verified_no_previous_templates','next':'intraday_template_dedupe'}


def representatives(records):
    groups=defaultdict(list)
    for row in records:
        if not row.get('template_key') or row.get('estimate_number') is None:
            raise ValueError('Template identity or estimate is missing')
        groups[(row['product'],row['os'],row['template_key'])].append(row)
    selected,removed=[],{}
    for group in groups.values():
        group.sort(key=lambda r:(-r['estimate_number'],r['ad_id']))
        selected.append(group[0])
        for row in group[1:]:removed[row['id']]=group[0]['id']
    selected.sort(key=lambda r:(-r['estimate_number'],r['product'],r['os'],r['ad_id']))
    return selected,removed
