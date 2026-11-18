import hashlib
import json
from pathlib import Path

import pytest

from ua_workflows.social_chat_clip_embed import (DIMENSIONS, collect_inputs, embed_missing,
                                                 fingerprint_for, missing, provenance_from_file,
                                                 verify_model)


def vector(seed):
    return [float(seed)] + [0.0] * (DIMENSIONS - 1)


def fixture(tmp_path, count=3):
    paths = []
    records = []
    for n in range(count):
        path = tmp_path / f'proxy-{n}.png'
        path.write_bytes(f'isolated proxy {n}'.encode())
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        paths.append(path)
        records.append({'id': f'R{n}', 'proxy': {'path': str(path), 'sha256': digest}})
    index = tmp_path / 'index.json'
    index.write_text(json.dumps(records))
    return index, tmp_path / 'cache', paths


def fake_encode(calls):
    def encode(paths):
        calls.append(list(paths))
        return [vector(len(paths) + i) for i in range(len(paths))]
    return encode


def test_embed_missing_fills_cache_and_is_resumable(tmp_path):
    index, cache, _ = fixture(tmp_path)
    calls = []
    result = embed_missing(index, cache, 'pinned-model', fake_encode(calls))
    assert result['encoded'] == 3 and result['remaining'] == 0
    assert len(calls) == 1
    entries = sorted(cache.glob('*.json'))
    assert len(entries) == 3
    payload = json.loads(entries[0].read_text())
    assert payload['model_fingerprint'] == 'pinned-model' and len(payload['vector']) == DIMENSIONS
    again = embed_missing(index, cache, 'pinned-model', fake_encode(calls))
    assert again['encoded'] == 0 and again['pending_before'] == 0


def test_embed_limit_and_proxy_change_are_explicit(tmp_path):
    index, cache, paths = fixture(tmp_path)
    result = embed_missing(index, cache, 'pinned-model', fake_encode([]), batch_size=1, limit=2)
    assert result['encoded'] == 2 and result['remaining'] == 1
    paths[2].write_bytes(b'changed after indexing')
    with pytest.raises(ValueError, match='changed'):
        embed_missing(index, cache, 'pinned-model', fake_encode([]))


def test_provenance_pin_rejects_unverified_model(tmp_path):
    provenance = {'model': 'sentence-transformers/clip-ViT-B-32', 'revision': 'deadbeef',
                  'files_sha256': {'modules.json': '0' * 64}, 'libraries': {}}
    path = tmp_path / 'model-provenance.json'
    path.write_text(json.dumps(provenance))
    pinned = provenance_from_file(path)
    assert pinned['fingerprint'] == fingerprint_for(provenance)
    with pytest.raises(ValueError, match='snapshot missing'):
        verify_model(pinned, hf_cache=tmp_path / 'hub')


def test_embed_command_dry_run_reports_gap_without_model(tmp_path, capsys):
    from ua_workflows import afizzy_daily
    index, cache, _ = fixture(tmp_path, count=2)
    assert afizzy_daily.main(['embed', '--media-index', str(index),
                              '--embedding-cache', str(cache), '--dry-run']) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload['status'] == 'dry_run' and payload['pending'] == 2 and payload['unique'] == 2
