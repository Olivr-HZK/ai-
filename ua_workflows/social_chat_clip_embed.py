"""Stable local CLIP embedding for cover proxies.

Replaces the per-run ad-hoc ``embed.py`` scripts: the model provenance is pinned,
the cache directory is a parameter, and missing entries are filled incrementally so
an interrupted run can resume. No network, no credentials, no business writes.
"""
from __future__ import annotations

import hashlib
import json
from importlib.metadata import version
from pathlib import Path

DIMENSIONS = 512
DEFAULT_CACHE = Path('data') / 'social_chat_plugin_reprocess_v2' / 'embedding-cache'


def sha256_file(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def fingerprint_for(provenance):
    return hashlib.sha256(json.dumps(provenance, sort_keys=True).encode()).hexdigest()


def provenance_from_bundle(pack):
    """Read the pinned model provenance from a history vector pack."""
    import zipfile
    with zipfile.ZipFile(pack) as archive:
        model = json.loads(archive.read('model.json'))
    provenance = model['provenance']
    if fingerprint_for(provenance) != model['model_fingerprint']:
        raise ValueError('Model provenance fingerprint mismatch')
    return {'provenance': provenance, 'fingerprint': model['model_fingerprint'],
            'model': provenance.get('model', 'sentence-transformers/clip-ViT-B-32')}


def provenance_from_file(path):
    provenance = json.loads(Path(path).read_text())
    return {'provenance': provenance, 'fingerprint': fingerprint_for(provenance),
            'model': provenance.get('model', 'sentence-transformers/clip-ViT-B-32')}


def verify_model(pinned, *, hf_cache=None):
    """Verify the pinned snapshot files and library versions; return the snapshot path."""
    provenance = pinned['provenance']
    revision = provenance['revision']
    name = provenance.get('model', 'sentence-transformers/clip-ViT-B-32').replace('/', '--')
    root = Path(hf_cache or Path.home() / '.cache/huggingface/hub')
    snapshot = root / ('models--' + name) / 'snapshots' / revision
    if not snapshot.is_dir():
        raise ValueError('Pinned CLIP snapshot missing: ' + str(snapshot))
    for relative, expected in provenance['files_sha256'].items():
        path = snapshot / relative
        if not path.is_file() or sha256_file(path) != expected:
            raise ValueError('Pinned model file mismatch: ' + relative)
    for library, expected in provenance.get('libraries', {}).items():
        if version(library) != expected:
            raise ValueError('Pinned library version mismatch: ' + library)
    return snapshot


def encoder_for(snapshot, device=None):
    """Build the real encoder; imports torch lazily so offline tests never load it."""
    from PIL import Image
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(str(snapshot), device=device or 'mps', local_files_only=True)

    def encode(paths):
        images = []
        for path in paths:
            with Image.open(path) as image:
                images.append(image.convert('RGB'))
        return model.encode(images, batch_size=len(images), show_progress_bar=False,
                            convert_to_numpy=True)

    return encode


def collect_inputs(index):
    """Map proxy digest -> proxy path from a normalized media index, verifying bytes."""
    document = json.loads(Path(index).read_text())
    records = document if isinstance(document, list) else document['records']
    inputs = {}
    for row in records:
        proxy = row.get('proxy')
        if not isinstance(proxy, dict) or not proxy.get('path') or not proxy.get('sha256'):
            raise ValueError('Missing proxy identity: ' + str(row.get('id')))
        digest = proxy['sha256']
        if sha256_file(proxy['path']) != digest:
            raise ValueError('Proxy image changed: ' + str(row.get('id')))
        inputs.setdefault(digest, proxy['path'])
    return inputs


def cache_path(cache, digest):
    return Path(cache) / (digest + '.json')


def missing(inputs, cache):
    return [digest for digest in inputs if not cache_path(cache, digest).is_file()]


def write_entry(cache, digest, fingerprint, vector):
    values = [float(v) for v in vector]
    if len(values) != DIMENSIONS or not all(v == v and abs(v) != float('inf') for v in values):
        raise ValueError('Invalid embedding for image ' + digest)
    target = cache_path(cache, digest)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix('.tmp')
    temporary.write_text(json.dumps({'image_sha256': digest, 'model_fingerprint': fingerprint,
                                     'vector': values}, ensure_ascii=False))
    temporary.replace(target)


def embed_missing(index, cache, fingerprint, encode, *, batch_size=16, limit=None, progress=None):
    """Fill missing cache entries; existing entries are never rewritten."""
    inputs = collect_inputs(index)
    cache = Path(cache)
    cache.mkdir(parents=True, exist_ok=True)
    pending = missing(inputs, cache)
    if limit is not None and limit <= 0:
        raise ValueError('limit must be a positive integer')
    todo = pending if limit is None else pending[:limit]
    encoded = 0
    for start in range(0, len(todo), batch_size):
        batch = todo[start:start + batch_size]
        for digest in batch:
            if sha256_file(inputs[digest]) != digest:
                raise ValueError('Proxy image changed while embedding: ' + digest)
        vectors = encode([inputs[digest] for digest in batch])
        if len(vectors) != len(batch):
            raise ValueError('Encoder returned a different number of vectors')
        for digest, vector in zip(batch, vectors):
            write_entry(cache, digest, fingerprint, vector)
        encoded += len(batch)
        if progress:
            progress(encoded, len(todo))
    return {'unique': len(inputs), 'pending_before': len(pending), 'encoded': encoded,
            'remaining': len(pending) - encoded, 'cache': str(cache),
            'model_fingerprint': fingerprint}
