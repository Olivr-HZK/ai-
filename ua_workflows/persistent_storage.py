"""Shadow-copy and verify explicitly selected state. Never stops or starts services."""
from __future__ import annotations
from contextlib import ExitStack
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, delete=False) as stream:
        os.chmod(stream.name, 0o600)
        json.dump(value, stream, indent=2)
        stream.flush(); os.fsync(stream.fileno())
    os.replace(stream.name, path)


def layout(config):
    root = Path(config['state_root']).expanduser()
    if not root.is_absolute():
        raise ValueError('State root must be absolute')
    entries = []
    targets = []
    for entry in config['entries']:
        source = Path(entry['source']).expanduser()
        rel = Path(entry['target'])
        if not source.is_absolute() or rel.is_absolute() or '..' in rel.parts or not rel.parts:
            raise ValueError('Unsafe migration path')
        target = root / rel
        if source.is_symlink() or not source.is_dir():
            raise ValueError('Source must be an existing real directory')
        if target.resolve().is_relative_to(source.resolve()) or source.resolve().is_relative_to(target.resolve()):
            raise ValueError('Source and target cannot overlap')
        if any(target == old or target.is_relative_to(old) or old.is_relative_to(target) for old in targets):
            raise ValueError('Overlapping target entries')
        targets.append(target)
        entries.append((source, target, set(entry.get('excluded_top_level', []))))
    return root, entries


def files(source, excluded):
    for directory, dirs, names in os.walk(source, followlinks=False):
        directory = Path(directory)
        if directory == source:
            dirs[:] = [d for d in dirs if d not in excluded]
            names = [n for n in names if n not in excluded]
        for name in list(dirs):
            if (directory / name).is_symlink():
                raise ValueError('Symlink directories need an explicit migration entry')
        for name in names:
            path = directory / name
            if path.is_symlink() or not path.is_file():
                raise ValueError('Special files need an explicit migration entry')
            yield path


def stage(config, receipt, *, clone=False):
    root, entries = layout(config)
    config_hash = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
    marker = root / '.afizzy-state.json'
    if root.exists() and any(root.iterdir()) and not marker.exists():
        raise ValueError('Nonempty destination is not owned by this migration')
    if marker.exists() and json.loads(marker.read_text())['config_sha256'] != config_hash:
        raise ValueError('Migration configuration changed')
    root.mkdir(parents=True, exist_ok=True)
    root.chmod(0o700)
    save(marker, {'version': 1, 'config_sha256': config_hash})
    results = []
    changed = []
    for source, target, excluded in entries:
        target.mkdir(parents=True, exist_ok=True)
        for path in files(source, excluded):
            relative = path.relative_to(source)
            dest = target / relative
            # Refuse a symlink planted anywhere in the target path.
            for parent in (dest, *dest.parents):
                if parent == root.parent:
                    break
                if parent.is_symlink():
                    raise ValueError('Destination symlink is unsafe')
            before = path.stat()
            fingerprint = (before.st_size, before.st_mtime_ns, before.st_ino)
            source_hash = digest(path)
            dest.parent.mkdir(parents=True, exist_ok=True)
            if not dest.exists() or digest(dest) != source_hash:
                fd, name = tempfile.mkstemp(dir=dest.parent, prefix='.state-copy-')
                os.close(fd); tmp = Path(name)
                try:
                    if clone:
                        # APFS copy-on-write, never a hard link to mutable source bytes.
                        tmp.unlink()
                        subprocess.run(['/bin/cp', '-c', str(path), str(tmp)], check=True, capture_output=True)
                    else:
                        shutil.copyfile(path, tmp)
                    tmp.chmod(before.st_mode & 0o777)
                    if digest(tmp) != source_hash:
                        raise ValueError('Copied bytes differ')
                    os.replace(tmp, dest)
                finally:
                    tmp.unlink(missing_ok=True)
            after = path.stat()
            if (after.st_size, after.st_mtime_ns, after.st_ino) != fingerprint or digest(path) != source_hash:
                changed.append(str(path))
            results.append({'source': str(path), 'target': str(dest), 'sha256': source_hash,
                            'size': before.st_size, 'mtime_ns': before.st_mtime_ns})
            if len(results) % 5000 == 0:
                save(receipt, {'status': 'copying', 'files': results, 'changed_sources': changed,
                               'config_sha256': config_hash})
                print(json.dumps({'copied_files': len(results)}), flush=True)
    result = {'status': 'shadow_verified' if not changed else 'source_changed_retry_after_quiescence',
              'files': results, 'changed_sources': changed, 'config_sha256': config_hash,
              'excluded': {str(s): sorted(e) for s, _, e in entries if e},
              'activated': False}
    save(receipt, result)
    return result


def verify(config, receipt):
    _, entries = layout(config)
    result = json.loads(Path(receipt).read_text())
    expected = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
    if result['status'] != 'shadow_verified' or result['config_sha256'] != expected:
        raise ValueError('No complete verified shadow for this configuration')
    current = {str(p) for source, _, excluded in entries for p in files(source, excluded)}
    if current != {r['source'] for r in result['files']}:
        raise ValueError('Source file set changed; refresh shadow first')
    for row in result['files']:
        if digest(row['source']) != row['sha256'] or digest(row['target']) != row['sha256']:
            raise ValueError('Source or shadow changed; refresh shadow first')
    return result


def activate(config, receipt, journal, *, quiesced=False):
    """Link only fully copied entries after the operator has stopped producers.

    Excluded live state (browser profiles, etc.) must be handled separately in
    the final window. An incomplete shadow can never be activated.
    """
    if not quiesced:
        raise ValueError('Activation requires the final quiesced migration window')
    _, entries = layout(config)
    active_sources = {Path(e['source']).expanduser() for e in config['entries'] if e.get('activate', True)}
    for source, target, excluded in entries:
        if source not in active_sources:
            continue
        for name in excluded:
            # Dedicated profiles/environments are renamed only after owners exit,
            # outside the live shadow copy. The old path must already be an alias.
            old, new = source / name, target / name
            if (not old.is_symlink() or not new.is_dir() or new.is_symlink()
                    or old.resolve() != new.resolve()):
                raise ValueError('Excluded state must be relocated before activation')
    if Path(journal).exists():
        raise ValueError('Existing activation journal requires explicit recovery')
    with ExitStack() as stack:
        for path in config.get('guard_locks', []):
            # Open the existing inode only; missing guards are not evidence of idle.
            handle = stack.enter_context(Path(path).open('r+'))
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        verify(config, receipt)
        rows = []
        for source, target, _ in entries:
            if source not in active_sources:
                continue
            backup = source.with_name(source.name + '.before-persistent')
            if backup.exists() or backup.is_symlink():
                raise ValueError('Backup path already exists')
            rows.append({'source': str(source), 'target': str(target), 'backup': str(backup), 'phase': 'pending'})
        state = {'status': 'intent', 'entries': rows}
        save(journal, state)
        try:
            for row in rows:
                source, target, backup = (Path(row[k]) for k in ('source', 'target', 'backup'))
                source.rename(backup)
                row['phase'] = 'source_backed_up'; save(journal, state)
                source.symlink_to(target, target_is_directory=True)
                row['phase'] = 'linked'; save(journal, state)
            state['status'] = 'activated'
        except BaseException:
            for row in reversed(rows):
                source, backup = Path(row['source']), Path(row['backup'])
                if backup.exists():
                    if source.is_symlink(): source.unlink()
                    if not source.exists(): backup.rename(source)
            state['status'] = 'rolled_back'
            save(journal, state)
            raise
        save(journal, state)
        return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['stage', 'verify', 'activate'])
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--clone', action='store_true', help='Require APFS COW copies; no hard links/fallback')
    parser.add_argument('--journal', type=Path)
    parser.add_argument('--quiesced', action='store_true')
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    if args.action == 'stage': result = stage(config, args.receipt, clone=args.clone)
    elif args.action == 'verify': result = verify(config, args.receipt)
    else:
        if not args.journal: parser.error('--journal required')
        result = activate(config, args.receipt, args.journal, quiesced=args.quiesced)
    print(json.dumps({'status': result['status'], 'files': len(result.get('files', []))}))


if __name__ == '__main__': main()
