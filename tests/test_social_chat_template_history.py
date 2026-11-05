import copy
import pytest
from ua_workflows.social_chat_template_history import confirmed_templates, history_state, identity, policy_for, representatives, load_confirmed_history, VERSION, LEGACY_VERSION
from ua_workflows import social_chat_browser_daily as daily


def test_verified_empty_history_becomes_new_baseline_but_errors_do_not():
    assert history_state([],source_state='verified')['status']=='new_baseline'
    assert history_state([],source_state='verified_no_prior_batch')['status']=='new_baseline'
    assert history_state([],source_state='missing_file')['status']=='pending'
    assert history_state([],source_state='verified',identity_verified=False)['status']=='pending'
    assert history_state([],source_state='verified',unreviewed_candidates=True)['status']=='pending'


def test_confirmed_template_does_not_require_previous_dedupe_or_ingestion_success():
    rows=[{'id':'prior','decision':'keep','visually_inspected':True,
           'template_review':{'relation':'distinct'},'previous_day_template_review':{'relation':'pending'}}]
    assert confirmed_templates(rows)==rows
    assert history_state(confirmed_templates(rows),source_state='verified')['status']=='compare_previous'
    assert not confirmed_templates([{'visually_inspected':True,'template_review':{'relation':'uncertain'}}])


def test_product_template_representatives_are_stable_and_keep_oses_separate():
    rows=[{'id':'a','ad_id':'2','product':'Tizz','os':'Android','template_key':'jealous','estimate_number':4400},
          {'id':'b','ad_id':'1','product':'Tizz','os':'Android','template_key':'jealous','estimate_number':4400},
          {'id':'c','ad_id':'3','product':'Tizz','os':'iOS','template_key':'jealous','estimate_number':4300}]
    kept,removed=representatives(rows)
    assert {r['id'] for r in kept}=={'b','c'} and removed=={'a':'b'}


def test_alias_identity_is_explicit_and_conflicting_ids_are_blocked():
    mapping={'products':[{'name':'Tizz','apps':{'Android':{'names':['TizzChat','Tizz','pkg.tizz'],'app_ids':['pkg.tizz']}}}]}
    assert identity('TizzChat','Android',mapping)==identity('pkg.tizz','Android',mapping)==('Tizz','Android')
    assert identity('TizzChat','iOS',mapping)==('TizzChat','iOS')
    with pytest.raises(ValueError):identity('TizzChat','Android',mapping,'different.package')


def test_new_policy_and_identity_map_do_not_migrate_old_scopes(tmp_path):
    config=daily.load(daily.CONFIG);old=copy.deepcopy(config)
    old['plugin_handoff']['previous_day_template_dedupe']=policy_for(LEGACY_VERSION)
    d=tmp_path/'old';daily.prepare('2026-11-03',d,old);before=(d/'scope.json').read_bytes()
    daily.prepare('2026-11-03',d,config);assert (d/'scope.json').read_bytes()==before
    n=tmp_path/'new';daily.prepare('2026-11-04',n,config);scope=daily.load(n/'scope.json')
    assert scope['previous_day_template_dedupe']['version']==VERSION
    assert scope['template_identity_mapping']['products'][0]['name']=='Tizz'


def test_revised_history_is_preferred_and_bad_hash_is_not_zero_history(tmp_path):
    import json,hashlib
    original={'items':[]};(tmp_path/'review_observations.json').write_text(json.dumps(original))
    payload={'original_review_sha256':hashlib.sha256((tmp_path/'review_observations.json').read_bytes()).hexdigest(),
             'records':[{'id':'confirmed','visually_inspected':True,'template_review':{'relation':'distinct'},'previous_day_template_review':{'relation':'pending'}}]}
    revision=tmp_path/'revised.json';revision.write_text(json.dumps(payload))
    pointer={'path':'revised.json','sha256':hashlib.sha256(revision.read_bytes()).hexdigest()}
    (tmp_path/'template-history-latest.json').write_text(json.dumps(pointer))
    assert load_confirmed_history(tmp_path)[0]['id']=='confirmed'
    pointer['sha256']='bad';(tmp_path/'template-history-latest.json').write_text(json.dumps(pointer))
    with pytest.raises(ValueError,match='hash mismatch'):load_confirmed_history(tmp_path)
