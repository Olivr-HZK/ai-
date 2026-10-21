#!/usr/bin/env node
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const crypto = require('node:crypto');
const {spawnSync} = require('node:child_process');
const packageRoot = path.resolve(__dirname, '..');
const pkg = require('../package.json');
const skillName = 'guangdada-browser-crawl';
const skillFiles = [
  'SKILL.md', 'agents/openai.yaml', 'references/browser-check.md',
  'references/scope.example.json', 'references/state-contract.md',
  'scripts/browser_check.js', 'scripts/frames.py', 'scripts/state.py'
];
const sha256 = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
function exists(file) {
  try { fs.lstatSync(file); return true; } catch (e) { if (e.code === 'ENOENT') return false; throw e; }
}
function verifyBundle(root = packageRoot) {
  const source = path.join(root, 'skill');
  const manifest = JSON.parse(fs.readFileSync(path.join(source, 'bundle.json'), 'utf8'));
  if (manifest.package !== pkg.name || manifest.version !== pkg.version || manifest.skill !== skillName ||
      JSON.stringify(Object.keys(manifest.files).sort()) !== JSON.stringify([...skillFiles].sort())) {
    throw new Error('Invalid bundled skill manifest; rebuild or reinstall the npm package.');
  }
  for (const rel of skillFiles) {
    const file = path.join(source, rel);
    // Reject symlinks in every component of a bundled source path.
    let current = source;
    for (const part of rel.split('/')) {
      current = path.join(current, part);
      if (fs.lstatSync(current).isSymbolicLink()) throw new Error('Bundled symlink is not allowed: ' + rel);
    }
    if (!fs.statSync(file).isFile() || sha256(fs.readFileSync(file)) !== manifest.files[rel]) {
      throw new Error('Bundled file hash mismatch: ' + rel);
    }
  }
  return {source, manifest};
}
function parse(argv) {
  const options = {command: 'help', json: false, force: false, dryRun: false};
  let commandSeen = false;
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--help' || arg === '-h') {options.command = 'help'; commandSeen = true;}
    else if (arg === '--version' || arg === '-v') {options.command = 'version'; commandSeen = true;}
    else if (arg === '--json') options.json = true;
    else if (arg === '--force') options.force = true;
    else if (arg === '--dry-run') options.dryRun = true;
    else if (arg === '--skills-dir') {
      if (!argv[i + 1] || argv[i + 1].startsWith('--')) throw new Error('--skills-dir requires a directory');
      options.skillsDir = path.resolve(argv[++i]);
    } else if (['install', 'doctor', 'help'].includes(arg) && !commandSeen) {
      options.command = arg; commandSeen = true;
    } else throw new Error('Unknown argument: ' + arg);
  }
  if (options.command !== 'install' && (options.force || options.dryRun)) throw new Error('--force and --dry-run require install');
  options.skillsDir ||= path.join(process.env.CODEX_HOME || path.join(os.homedir(), '.codex'), 'skills');
  options.skillsDir = path.resolve(options.skillsDir);
  return options;
}
function sameInstalled(target, manifest) {
  try {
    return skillFiles.every(rel => {
      let current = target;
      for (const part of rel.split('/')) {
        current = path.join(current, part);
        if (fs.lstatSync(current).isSymbolicLink()) return false;
      }
      return fs.statSync(current).isFile() && sha256(fs.readFileSync(current)) === manifest.files[rel];
    });
  } catch { return false; }
}
function install(options) {
  const {source, manifest} = verifyBundle();
  const parent = options.skillsDir;
  const target = path.join(parent, skillName);
  const present = exists(target);
  if (present && (!fs.lstatSync(target).isDirectory() || fs.lstatSync(target).isSymbolicLink())) {
    throw new Error('Target must be a real directory, not a symlink or file: ' + target);
  }
  const result = {package: pkg.name, version: pkg.version, target};
  if (present && sameInstalled(target, manifest)) return {...result, status: 'up_to_date'};
  if (present && !options.force) throw new Error('Existing skill differs. Use install --force to replace it with a recoverable sibling backup: ' + target);
  if (options.dryRun) return {...result, status: 'dry_run', action: present ? 'backup_and_replace' : 'install'};
  fs.mkdirSync(parent, {recursive: true});
  const stage = fs.mkdtempSync(path.join(parent, '.gdd-install-'));
  let backup = null;
  try {
    for (const rel of skillFiles) {
      const dest = path.join(stage, rel);
      fs.mkdirSync(path.dirname(dest), {recursive: true});
      fs.copyFileSync(path.join(source, rel), dest, fs.constants.COPYFILE_EXCL);
      if (sha256(fs.readFileSync(dest)) !== manifest.files[rel]) throw new Error('Copy verification failed: ' + rel);
    }
    fs.writeFileSync(path.join(stage, '.gdd-skill-install.json'), JSON.stringify({
      ...manifest, installed_at: new Date().toISOString()
    }, null, 2) + '\n');
    if (present) {
      // Keep backups outside skills/, so Codex does not discover duplicate skills.
      const backupRoot = path.join(path.dirname(parent), 'skill-backups');
      fs.mkdirSync(backupRoot, {recursive: true});
      backup = path.join(backupRoot, skillName + '-' + new Date().toISOString().replace(/[:.]/g, '-') + '-' + crypto.randomBytes(4).toString('hex'));
      fs.renameSync(target, backup);
    } else if (exists(target)) throw new Error('Target appeared during install; preserving it.');
    try { fs.renameSync(stage, target); }
    catch (error) {
      if (backup && !exists(target)) fs.renameSync(backup, target);
      throw error;
    }
    return {...result, status: 'installed', backup};
  } finally { fs.rmSync(stage, {recursive: true, force: true}); }
}
function probe(command, args) {
  const r = spawnSync(command, args, {encoding: 'utf8', timeout: 5000, windowsHide: true});
  return r.status === 0 ? (r.stdout || r.stderr).trim().split(/\r?\n/)[0] : null;
}
function doctor(options) {
  verifyBundle();
  let python = null;
  for (const command of ['python3', 'python', ...(process.platform === 'win32' ? ['py'] : [])]) {
    const version = probe(command, ['-c', 'import sys; print(".".join(map(str,sys.version_info[:3])))']);
    const parts = version?.split('.').map(Number);
    if (parts && (parts[0] > 3 || parts[0] === 3 && parts[1] >= 10)) {python = {command, version}; break;}
  }
  const ffmpeg = probe('ffmpeg', ['-version']), ffprobe = probe('ffprobe', ['-version']);
  const target = path.join(options.skillsDir, skillName);
  const {manifest} = verifyBundle();
  return {package: pkg.name, version: pkg.version, status: python && ffmpeg && ffprobe ? 'dependencies_ready' : 'setup_needed',
    target, installed_matches: exists(target) && !fs.lstatSync(target).isSymbolicLink() && sameInstalled(target, manifest),
    dependencies: {python, ffmpeg, ffprobe},
    browser: 'not_checked: requires a Codex session with browser tools and the user’s Guangdada login'};
}
function main(argv) {
  const options = parse(argv);
  if (options.command === 'version') {console.log(pkg.version); return;}
  if (options.command === 'help') {
    console.log(`guangdada-browser-skill ${pkg.version}

Usage:
  guangdada-browser-skill install [--skills-dir DIR] [--dry-run] [--force] [--json]
  guangdada-browser-skill doctor [--skills-dir DIR] [--json]
  guangdada-browser-skill --version

Default: $CODEX_HOME/skills, or ~/.codex/skills.
--force preserves the existing skill under a sibling skill-backups directory.
No browser actions, credentials, paid calls or automatic dependency installation.
After installation, use $guangdada-browser-crawl in a Codex chat with browser tools.`);
    return;
  }
  const result = options.command === 'install' ? install(options) : doctor(options);
  if (options.json) console.log(JSON.stringify(result, null, 2));
  else {
    console.log(`${result.status}: ${result.target}`);
    if (result.backup) console.log('Previous skill backup: ' + result.backup);
    if (options.command === 'install') console.log('Run guangdada-browser-skill doctor to check Python 3.10+ and FFmpeg. Use $guangdada-browser-crawl in Codex.');
    else console.log(JSON.stringify(result, null, 2));
  }
}
if (require.main === module) {
  try { main(process.argv.slice(2)); }
  catch (error) {
    if (process.argv.includes('--json')) console.error(JSON.stringify({status: 'error', error: error.message}));
    else console.error('Error: ' + error.message);
    process.exitCode = 1;
  }
}
module.exports = {verifyBundle, parse, install, doctor};
