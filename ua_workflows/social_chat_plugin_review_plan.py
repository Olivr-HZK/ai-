"""Local dedupe-first queue. No content decisions, browser, LLM or cloud writes."""
from collections import Counter, defaultdict
from datetime import date, timedelta
import hashlib
import html
import json
import math
from pathlib import Path

LEGACY_POLICY = {
    'version': 'dedupe-first-joint-review-v1',
    'order': ['package_ledger', 'exclude_relaunch', 'intraday_dedupe', 'crossday_7d_dedupe',
              'product_joint_visual_review', 'template_representatives', 'rank_top100', 'package_and_sync'],
    'threshold': 0.9, 'history_days': 7,
    'dedupe_input': 'all_downloaded_non_relaunch_before_content_review',
    'visual_review': ['content', 'template', 'style'],
    'frame_fractions': [0.03, 0.15, 0.30, 0.50, 0.70, 0.90],
    'reuse_bound_evidence': True, 'template_uncertain': 'not_admitted',
}
POLICY = {
    **LEGACY_POLICY,
    'version': 'dedupe-first-joint-review-v2',
    'order': ['package_ledger', 'plugin_relaunch_labels', 'intraday_dedupe', 'crossday_7d_dedupe',
              'product_joint_visual_review', 'template_representatives', 'rank_all_qualified', 'package_and_sync'],
    'screening_passes': ['plugin_relaunch_labels', 'intraday_vectors', 'crossday_7d_vectors',
                         'joint_content_style_product_template'],
    'selection': 'all_qualified',
}


def validate_policy(policy):
    if policy != POLICY and policy != LEGACY_POLICY:
        raise ValueError('Unsupported pinned processing policy')
    return policy


def is_relaunch(row):
    """Only explicit plugin label data; never infer from copy or absent labels."""
    value = row.get('relaunch')
    if isinstance(value, dict):
        return value.get('value') is True
    return value is True or value in ('confirmed', '重投')


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def group(row):
    name = ' '.join(html.unescape(row.get('app') or '').split())
    os_name = row.get('os')
    if not name or name.startswith('未观察到') or os_name not in ('iOS', 'Android'):
        raise ValueError('Missing/ambiguous App identity: ' + row['id'])
    return os_name + '|' + name


def _components(rows, vectors, threshold):
    import numpy as np
    if not rows:
        return [], None
    matrix = np.asarray([vectors[r['id']] for r in rows], dtype=np.float64)
    norms = np.linalg.norm(matrix, axis=1)
    if np.any(norms == 0):
        raise ValueError('Zero embedding cannot be used for dedupe')
    matrix /= norms[:, None]
    similarity = matrix @ matrix.T
    edges = similarity >= threshold
    # Use the original scalar sum at the numerical threshold boundary.
    for i, j in zip(*np.where(np.abs(similarity - threshold) < 1e-10)):
        a, b = vectors[rows[i]['id']], vectors[rows[j]['id']]
        score = sum(x*y for x, y in zip(a, b)) / (sum(x*x for x in a)*sum(y*y for y in b))**0.5
        similarity[i, j] = score
        edges[i, j] = score >= threshold
    parent = list(range(len(rows)))
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]; x = parent[x]
        return x
    for i, j in zip(*np.where(np.triu(edges, 1))):
        parent[find(int(i))] = find(int(j))
    clusters = defaultdict(list)
    for i in range(len(rows)):
        clusters[find(i)].append(i)
    return list(clusters.values()), similarity


def plan(target, records, vectors, policy=None):
    """Inputs are ledger-checked local media; existing content decisions are ignored."""
    policy = validate_policy(policy or POLICY)
    today_date = date.fromisoformat(target)
    ids = [r['id'] for r in records]
    if len(ids) != len(set(ids)):
        raise ValueError('Duplicate normalized media IDs')
    today, history, skipped_history, coverage, relaunch_ids = [], [], [], set(), []
    target_media_input = 0
    for row in records:
        age = (today_date - date.fromisoformat(row['day'])).days
        if age < 0 or age > policy['history_days']:
            continue
        if row.get('kind') not in ('视频', '图片'):
            continue
        if age == 0:
            target_media_input += 1
        if is_relaunch(row):
            if age == 0: relaunch_ids.append(row['id'])
            continue
        try:
            identity = group(row)
        except ValueError:
            if age == 0:
                raise
            skipped_history.append(row['id']); continue
        value = row.get('value')
        if not isinstance(value, (float, int)) or isinstance(value, bool) or not math.isfinite(value) or value < 0:
            raise ValueError('Missing/invalid estimate: ' + row['id'])
        vec = vectors.get(row['id'])
        if not isinstance(vec, list) or len(vec) != 512 or not all(isinstance(x, (float, int)) and math.isfinite(x) for x in vec):
            raise ValueError('Missing/invalid embedding: ' + row['id'])
        (today if age == 0 else history).append(dict(row, group=identity))
        if age:
            coverage.add(row['day'])
    all_groups, hist_groups = defaultdict(list), defaultdict(list)
    for row in today: all_groups[row['group']].append(row)
    for row in history: hist_groups[row['group']].append(row)
    outcomes, survivors = {}, []
    for identity, rows in sorted(all_groups.items()):
        rows.sort(key=lambda r: (-r['value'], r['id']))
        components, similarity = _components(rows, vectors, policy['threshold'])
        reps = []
        for members in components:
            best = min(members, key=lambda i: (-rows[i]['value'], rows[i]['id']))
            reps.append(rows[best])
            for i in members:
                outcomes[rows[i]['id']] = {'status': 'review_pending' if i == best else 'intraday_removed',
                    'representative': rows[best]['id'], 'similarity_to_representative': float(similarity[i, best])}
        hist = sorted(hist_groups[identity], key=lambda r: (r['day'], r['id']))
        nodes = reps + hist
        components, similarity = _components(nodes, vectors, policy['threshold'])
        for members in components:
            historical = [i for i in members if i >= len(reps)]
            for i in members:
                if i >= len(reps): continue
                if historical:
                    match = max(historical, key=lambda j: similarity[i, j])
                    outcomes[nodes[i]['id']] = {'status': 'crossday_removed', 'historical_match': nodes[match]['id'],
                        'matched_date': nodes[match]['day'], 'similarity': float(similarity[i, match]),
                        'direct': bool(similarity[i, match] >= policy['threshold'])}
                else:
                    survivors.append(nodes[i])
    counts = dict(Counter(o['status'] for o in outcomes.values()))
    missing = [(today_date - timedelta(days=i)).isoformat() for i in range(1, 8)
               if (today_date - timedelta(days=i)).isoformat() not in coverage]
    return {'version': policy['version'], 'target_date': target, 'policy': dict(policy),
            'status': 'JOINT_REVIEW_PENDING', 'input_before_content_review': len(today), 'counts': counts,
            'target_media_before_relaunch': target_media_input, 'plugin_relaunch_excluded_ids': relaunch_ids,
            'history_rows': len(history), 'history_dates': sorted(coverage), 'missing_history_dates': sorted(missing),
            'unidentified_history_ids': skipped_history, 'decisions': outcomes,
            'review_queue': sorted(survivors, key=lambda r: (r['group'], -r['value'], r['id'])),
            'review_certified': False}


def prepare_queue(target, index, cache, output, policy=None, history_vector_pack=None):
    index, cache, output = Path(index), Path(cache), Path(output)
    document = json.loads(index.read_text())
    records = document if isinstance(document, list) else document['records']
    vectors, fingerprints, blobs = {}, set(), {}
    for row in records:
        age = (date.fromisoformat(target) - date.fromisoformat(row['day'])).days
        if not 0 <= age <= 7 or is_relaunch(row) or row.get('kind') not in ('视频', '图片'):
            continue
        try: group(row)
        except ValueError:
            if age == 0: raise
            continue
        proxy = row['proxy'];digest = proxy['sha256']
        if sha(proxy['path']) != digest:
            raise ValueError('Proxy image changed: ' + row['id'])
        if digest not in blobs:
            blobs[digest] = json.loads((cache / (digest + '.json')).read_text())
        blob = blobs[digest]
        if blob.get('image_sha256') != digest or not blob.get('model_fingerprint'):
            raise ValueError('Embedding lacks bound image/model identity: ' + row['id'])
        fingerprints.add(blob['model_fingerprint']);vectors[row['id']] = blob['vector']
    if len(fingerprints) != 1:
        raise ValueError('Dedupe requires one verified CLIP model fingerprint')
    history_receipt = None
    if history_vector_pack:
        from ua_workflows.social_chat_clip_vectors import read_bundle
        bundle = read_bundle(history_vector_pack, next(iter(fingerprints)))
        existing = {r['id']:r for r in records}
        for row in bundle['records']:
            age = (date.fromisoformat(target)-date.fromisoformat(row['day'])).days
            if not 1 <= age <= 7:
                continue
            prior = existing.get(row['id'])
            if prior is not None:
                if any(prior.get(k)!=row.get(k) for k in ('day','app','os')) or prior['proxy']['sha256']!=row['proxy']['sha256']:
                    raise ValueError('History record identity disagrees with local index: '+row['id'])
                continue
            records.append(row);vectors[row['id']]=bundle['vectors'][row['id']]
        history_receipt = {'sha256':sha(history_vector_pack),'source_records':bundle['manifest']['records'],
                           'source_vectors':bundle['manifest']['vectors'],'images_required':False}
    result = plan(target, records, vectors, policy)
    if history_receipt:
        result['history_vector_pack'] = history_receipt
    # Freeze only the survivors for one combined human review; no style/template
    # decisions are inferred from the proxy or inherited content decisions.
    for row in result['review_queue']:
        sources = row.get('sources') or []
        if not sources or any(sha(s['path']) != s['sha256'] for s in sources):
            raise ValueError('Media changed/missing: ' + row['id'])
        frames = row.get('frames') or []
        if row['kind'] == '视频' and frames:
            if [f['fraction'] for f in frames] != POLICY['frame_fractions'] or any(sha(f['path']) != f['sha256'] for f in frames):
                raise ValueError('Existing six-frame evidence invalid: ' + row['id'])
            manifest_path = row.get('manifest')
            if not manifest_path or sha(manifest_path) != row.get('manifest_sha256'):
                raise ValueError('Existing frame manifest missing/changed: ' + row['id'])
            manifest = json.loads(Path(manifest_path).read_text())
            if manifest['source']['sha256'] != sources[0]['sha256'] or \
                    [(f['fraction'], f['sha256']) for f in manifest['frames']] != [(f['fraction'], f['sha256']) for f in frames]:
                raise ValueError('Existing frames not bound to source/parameters: ' + row['id'])
            evidence_action = 'reuse_verified_six_frames'
        else:
            evidence_action = 'extract_one_six_frame_set' if row['kind'] == '视频' else 'view_all_original_images'
        row['joint_review'] = {'status': 'pending', 'evidence_action': evidence_action,
                               'content': None, 'template': None, 'style': None}
        for field in ('review', 'decision', 'review_reason', 'previous_top100'):
            row.pop(field, None)
    result['index_sha256'] = sha(index)
    result['model_fingerprint'] = next(iter(fingerprints))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    return {k: v for k, v in result.items() if k not in ('review_queue', 'decisions', 'policy')}
