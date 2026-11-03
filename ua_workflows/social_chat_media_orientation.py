"""Read original media geometry; gate Guangdada intake, ingestion and replication."""
from __future__ import annotations
import argparse
import hashlib
import json
import math
from fractions import Fraction
from pathlib import Path
import subprocess

POLICY = {'version': 'non-landscape-original-media-v1', 'landscape': 'exclude',
          'square': 'keep', 'unknown': 'pending',
          'checkpoints': ['before_cover_dedupe', 'before_ingestion', 'before_replication']}
IMAGES = {'.png', '.jpg', '.jpeg', '.webp', '.gif', '.avif', '.bmp', '.tif', '.tiff'}
VIDEOS = {'.mp4', '.mov', '.webm', '.mkv', '.m4v', '.avi'}


class OrientationError(ValueError):
    def __init__(self, code, evidence):
        self.code, self.evidence = code, evidence
        super().__init__(code)


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def inspect_media(path, expected_sha256=None):
    """Geometry only, not content certification or full video decoding."""
    path = Path(path)
    receipt = {'version': POLICY['version'], 'path': str(path.resolve()),
               'status': 'unknown', 'orientation': None}
    try:
        before = sha(path)
        receipt['source_sha256'] = before
        if expected_sha256 is not None and before != expected_sha256:
            raise ValueError('source_hash_mismatch')
        if path.suffix.lower() in IMAGES:
            from PIL import Image
            with Image.open(path) as image:
                w, h = image.size
                exif_orientation = image.getexif().get(274, 1)
                if exif_orientation not in range(1, 9):
                    raise ValueError('invalid_exif_orientation')
                receipt.update(kind='图片', encoded_width=w, encoded_height=h,
                               exif_orientation=exif_orientation, method='Pillow-original')
                if exif_orientation in (5, 6, 7, 8):
                    w, h = h, w
                image.load()
        elif path.suffix.lower() in VIDEOS:
            process = subprocess.run(['ffprobe', '-v', 'error', '-select_streams', 'v:0',
                '-show_entries', 'stream=width,height,codec_type,sample_aspect_ratio:stream_disposition=attached_pic:stream_tags=rotate:stream_side_data=rotation:format=format_name',
                '-of', 'json', str(path)], capture_output=True, text=True, timeout=30, check=True)
            document = json.loads(process.stdout)
            streams = document.get('streams', [])
            if len(streams) != 1 or streams[0].get('codec_type') != 'video':
                raise ValueError('missing_video_stream')
            if any(x in document.get('format', {}).get('format_name', '') for x in ('image2', '_pipe', 'gif')):
                raise ValueError('not_video_container')
            stream = streams[0];w, h = stream['width'], stream['height']
            if stream.get('disposition', {}).get('attached_pic'):
                raise ValueError('attached_cover_not_video')
            sar = stream.get('sample_aspect_ratio')
            ratio = Fraction(sar.replace(':', '/')) if sar and sar != 'N/A' else Fraction(1)
            if ratio <= 0:
                raise ValueError('invalid_sample_aspect_ratio')
            rotations = [float(x['rotation']) for x in stream.get('side_data_list', []) if 'rotation' in x]
            if 'rotate' in stream.get('tags', {}):
                rotations.append(float(stream['tags']['rotate']))
            normalized = []
            for angle in rotations or [0]:
                if not math.isfinite(angle) or abs(angle / 90 - round(angle / 90)) > 1e-5:
                    raise ValueError('unsupported_rotation')
                normalized.append(round(angle / 90) % 4)
            if len(set(normalized)) != 1:
                raise ValueError('conflicting_rotation')
            receipt.update(kind='视频', encoded_width=w, encoded_height=h,
                           sample_aspect_ratio=str(ratio), rotation_quarter_turns=normalized[0],
                           method='ffprobe-original')
            w, h = Fraction(w) * ratio, Fraction(h)
            if normalized[0] % 2:
                w, h = h, w
        else:
            raise ValueError('unsupported_media_type')
        w, h = float(w), float(h)
        if not all(math.isfinite(n) and n > 0 for n in (w, h)):
            raise ValueError('invalid_dimensions')
        if sha(path) != before:
            raise ValueError('source_changed_during_probe')
        receipt.update(width=w, height=h, orientation='landscape' if w > h else 'square' if w == h else 'portrait',
                       status='landscape' if w > h else 'eligible')
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, ArithmeticError, subprocess.SubprocessError) as error:
        receipt['reason'] = str(error) if isinstance(error, ValueError) else type(error).__name__
    return receipt


def require_non_landscape(path, expected_sha256=None):
    receipt = inspect_media(path, expected_sha256)
    if receipt['status'] != 'eligible':
        raise OrientationError('landscape_media' if receipt['status'] == 'landscape' else 'media_dimensions_unverified', receipt)
    return receipt


def filter_current(target, records):
    """All sources in a multi-image creative must qualify; preserve historical vectors."""
    from ua_workflows.social_chat_plugin_review_plan import is_relaunch
    admitted, decisions = [], {}
    for row in records:
        if row['day'] != target or row.get('kind') not in ('视频', '图片') or is_relaunch(row):
            admitted.append(row)
            continue
        sources = row.get('sources') or []
        evidence = [inspect_media(s.get('path', ''), s.get('sha256')) for s in sources]
        valid_identity = all(s.get('sha256') and e.get('kind') == row['kind'] for s,e in zip(sources,evidence))
        if any(e['status'] == 'landscape' for e in evidence):
            status = 'landscape_excluded'
        elif not evidence or not valid_identity or any(e['status'] != 'eligible' for e in evidence):
            status = 'dimensions_pending'
        else:
            status = 'eligible'
            admitted.append(dict(row, orientation_evidence=evidence))
        decisions[row['id']] = {'status': status, 'evidence': evidence}
    return admitted, decisions


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('paths', nargs='+');parser.add_argument('--output', required=True)
    parser.add_argument('--checkpoint', required=True, choices=POLICY['checkpoints'])
    args = parser.parse_args()
    evidence = [inspect_media(p) for p in args.paths]
    result = {'checkpoint': args.checkpoint, 'policy': POLICY, 'evidence': evidence,
              'allowed': bool(evidence) and all(e['status'] == 'eligible' for e in evidence)}
    Path(args.output).write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(json.dumps({'allowed': result['allowed'], 'receipt': args.output}))
    raise SystemExit(0 if result['allowed'] else 2)


if __name__ == '__main__':
    main()
