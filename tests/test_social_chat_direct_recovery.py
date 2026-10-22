import hashlib,json
from pathlib import Path
import pytest
from PIL import Image
from ua_workflows import social_chat_daily_state as state
from ua_workflows import social_chat_direct_recovery as recovery

@pytest.fixture
def archive(tmp_path):
    task={'id':'ios-01','app_id':'123456789','app_name':'Observed App','platform':'iOS'}
    scope={'tasks':[task],'target_date':'2026-10-21'};fp=state.fingerprint(scope)
    root=tmp_path/'tasks/ios-01';(root/'evidence').mkdir(parents=True)
    Image.new('RGB',(8,8),'white').save(root/'evidence/direct-terminal.png')
    text=root/'evidence/direct-terminal.txt';text.write_text('综合: 123456789\n暂无数据')
    seal={**task,'target_date':scope['target_date'],'checked_cards':0,'target_date_items':0,'terminal_reason':'explicit_no_results','image':{'path':'tasks/ios-01/evidence/direct-terminal.png','sha256':hashlib.sha256((root/'evidence/direct-terminal.png').read_bytes()).hexdigest()}}
    (root/'direct-complete.json').write_text(json.dumps(seal))
    paths=[text,root/'evidence/direct-terminal.png',root/'direct-complete.json']
    req={'schema':'visible-direct-recovery/v1','scope_sha256':fp,'authorization':'Explicit user continuation','source_hashes':{str(p.relative_to(tmp_path)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}}
    (tmp_path/'historical-direct-recovery.json').write_text(json.dumps(req))
    return tmp_path,scope

def test_zero_requires_actual_query_no_data_and_image(archive):
    base,scope=archive
    cards,seals=recovery.collection(base,scope)
    assert not cards and len(seals)==1

@pytest.mark.parametrize('relative',['tasks/ios-01/evidence/direct-terminal.txt','tasks/ios-01/evidence/direct-terminal.png','tasks/ios-01/direct-complete.json'])
def test_rejects_changed_historical_sources(archive,relative):
    base,scope=archive;(base/relative).write_bytes(b'changed')
    with pytest.raises(ValueError,match='Historical evidence changed'):recovery.collection(base,scope)

def test_explicit_authorization_and_fingerprint_required(archive):
    base,scope=archive;p=base/'historical-direct-recovery.json';d=json.loads(p.read_text());d['authorization']='';p.write_text(json.dumps(d))
    with pytest.raises(ValueError,match='authorization'):recovery.collection(base,scope)

def test_recovery_graph_does_not_claim_old_app_steps(tmp_path):
    scope={'tasks':[]};(tmp_path/'scope.json').write_text(json.dumps(scope))
    doc={'cursor':0,'execution_plan':[{'state':'COLLECTION_RECOVERY_PENDING','task':None}]}
    assert state.current(tmp_path,doc)['state']=='COLLECTION_RECOVERY_PENDING'
    doc['cursor']=1
    assert state.current(tmp_path,doc)['state']=='COMPLETED'
