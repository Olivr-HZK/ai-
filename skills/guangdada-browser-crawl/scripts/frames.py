#!/usr/bin/env python3
"""Extract real local video frames with FFmpeg; produce a hashed audit manifest."""
import argparse
import hashlib
import json
import shutil
import struct
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

POSITIONS = [0.03, 0.15, 0.30, 0.50, 0.70, 0.90]


def sha256(path):
    with path.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest() if hasattr(hashlib, 'file_digest') else hashlib.sha256(f.read()).hexdigest()


def png_size(path):
    head = path.read_bytes()[:24]
    if len(head) != 24 or head[:8] != b'\x89PNG\r\n\x1a\n' or head[12:16] != b'IHDR':
        raise ValueError('抽帧不是有效 PNG：' + str(path))
    return struct.unpack('>II', head[16:24])


def extract(video, root, out, scope_sha256, ad_id):
    root, video, out = root.resolve(), video.resolve(), out.resolve()
    if not video.is_relative_to(root) or not out.is_relative_to(root):
        raise ValueError('视频和抽帧目录必须在 task-root 内')
    if not video.is_file() or video.stat().st_size == 0:
        raise ValueError('视频不存在或为空')
    ffmpeg, ffprobe = shutil.which('ffmpeg'), shutil.which('ffprobe')
    if not ffmpeg or not ffprobe:
        raise ValueError('抽帧必须安装 FFmpeg/ffprobe；不能跳过抽帧改填成功')
    if out.exists():
        raise ValueError('抽帧目录已存在；请核对原清单或使用新的输出目录，拒绝覆盖审计证据')
    info = json.loads(subprocess.run([ffprobe, '-v', 'error', '-protocol_whitelist', 'file,pipe', '-show_streams', '-show_format', '-of', 'json', str(video)], check=True, capture_output=True, text=True).stdout)
    stream = next(s for s in info['streams'] if s['codec_type'] == 'video')
    duration = float(stream.get('duration') or info['format']['duration'])
    if duration <= 0:
        raise ValueError('视频时长无效')
    out.mkdir(parents=True)
    source_hash = sha256(video)
    frames = []
    for i, fraction in enumerate(POSITIONS, 1):
        timestamp = min(duration*fraction, max(0, duration-0.01))
        path = out/f'frame-{i:02d}.png'
        subprocess.run([ffmpeg, '-v', 'error', '-nostdin', '-protocol_whitelist', 'file,pipe', '-ss', str(timestamp), '-i', str(video), '-frames:v', '1', '-vf', 'scale=640:640:force_original_aspect_ratio=decrease,setsar=1', str(path)], check=True, capture_output=True)
        width, height = png_size(path)
        frames.append({'path': str(path.relative_to(root)), 'sha256': sha256(path), 'width': width, 'height': height, 'fraction': fraction, 'requested_timestamp_seconds': timestamp})
    if sha256(video) != source_hash:
        raise ValueError('抽帧期间源视频发生变化，结果无效')
    manifest = {'producer': 'gdd-frames/v1', 'scope_sha256': scope_sha256, 'ad_id': ad_id, 'created_at': datetime.now(timezone.utc).isoformat(), 'source': {'path': str(video.relative_to(root)), 'sha256': source_hash, 'width': int(stream['width']), 'height': int(stream['height']), 'duration_seconds': duration, 'decode_verified': True}, 'sampling': {'fractions': POSITIONS, 'audio_reviewed': False, 'timing_note': 'FFmpeg seeks near requested timestamps; full audio/video review not implied'}, 'frames': frames}
    path = out/'manifest.json'
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2)+'\n')
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ['video', 'task-root', 'output-dir', 'scope-sha256', 'ad-id']:
        parser.add_argument('--'+key, required=True)
    args = parser.parse_args()
    try:
        data = extract(Path(args.video), Path(args.task_root), Path(args.output_dir), args.scope_sha256, args.ad_id)
        print(json.dumps({'manifest': str((Path(args.output_dir)/'manifest.json').resolve()), 'frames': len(data['frames']), 'source_sha256': data['source']['sha256']}, ensure_ascii=False))
        return 0
    except (OSError, ValueError, KeyError, StopIteration, subprocess.CalledProcessError) as exc:
        print(json.dumps({'error': str(exc)}, ensure_ascii=False));return 1


if __name__ == '__main__':
    sys.exit(main())
