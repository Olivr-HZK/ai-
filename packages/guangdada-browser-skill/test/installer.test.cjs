'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const {spawnSync} = require('node:child_process');
const root = path.resolve(__dirname, '..');
const cli = path.join(root, 'bin/cli.cjs');
const skill = 'guangdada-browser-crawl';
function sandbox(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'gdd npm test '));
  t.after(() => fs.rmSync(dir, {recursive: true, force: true}));
  return dir;
}
function run(args, options = {}) {
  return spawnSync(process.execPath, [cli, ...args], {encoding: 'utf8', ...options});
}
function json(result) { assert.equal(result.status, 0, result.stderr); return JSON.parse(result.stdout); }
test('dry run does not create directories; install works outside the repo and is idempotent', t => {
  const dir = sandbox(t), codex = path.join(dir, 'custom codex');
  const opts = {cwd: dir, env: {...process.env, CODEX_HOME: codex}};
  assert.equal(json(run(['install', '--dry-run', '--json'], opts)).status, 'dry_run');
  assert.equal(fs.existsSync(codex), false);
  const installed = json(run(['install', '--json'], opts));
  assert.equal(installed.target, path.join(codex, 'skills', skill));
  assert.equal(installed.status, 'installed');
  assert.equal(fs.readFileSync(path.join(installed.target, 'SKILL.md'), 'utf8'), fs.readFileSync(path.join(root, 'skill/SKILL.md'), 'utf8'));
  const before = fs.statSync(path.join(installed.target, 'SKILL.md')).mtimeMs;
  assert.equal(json(run(['install', '--json'], opts)).status, 'up_to_date');
  assert.equal(fs.statSync(path.join(installed.target, 'SKILL.md')).mtimeMs, before);
});
test('existing changes are preserved; force keeps the whole prior directory outside skills', t => {
  const dir = sandbox(t), skills = path.join(dir, 'skills'), target = path.join(skills, skill);
  json(run(['install', '--skills-dir', skills, '--json']));
  fs.writeFileSync(path.join(target, 'SKILL.md'), 'local customization');
  fs.writeFileSync(path.join(target, 'extra.txt'), 'keep me');
  const blocked = run(['install', '--skills-dir', skills, '--json']);
  assert.equal(blocked.status, 1);
  assert.match(blocked.stderr, /Existing skill differs/);
  assert.equal(fs.readFileSync(path.join(target, 'SKILL.md'), 'utf8'), 'local customization');
  const result = json(run(['install', '--skills-dir', skills, '--force', '--json']));
  assert.equal(path.dirname(result.backup), path.join(dir, 'skill-backups'));
  assert.equal(fs.readFileSync(path.join(result.backup, 'SKILL.md'), 'utf8'), 'local customization');
  assert.equal(fs.readFileSync(path.join(result.backup, 'extra.txt'), 'utf8'), 'keep me');
  assert.match(fs.readFileSync(path.join(target, 'SKILL.md'), 'utf8'), /name: guangdada-browser-crawl/);
  assert.deepEqual(fs.readdirSync(skills), [skill]);
});
test('symlink targets and symlinked installed files are never overwritten implicitly', t => {
  const dir = sandbox(t), skills = path.join(dir, 'skills'), outside = path.join(dir, 'outside');
  fs.mkdirSync(skills); fs.mkdirSync(outside);
  fs.writeFileSync(path.join(outside, 'SKILL.md'), 'outside');
  fs.symlinkSync(outside, path.join(skills, skill), 'dir');
  assert.equal(run(['install', '--skills-dir', skills, '--force']).status, 1);
  assert.equal(fs.readFileSync(path.join(outside, 'SKILL.md'), 'utf8'), 'outside');
});
test('corrupt bundled payload fails before any destination write', t => {
  const dir = sandbox(t), copy = path.join(dir, 'package'), target = path.join(dir, 'skills');
  fs.mkdirSync(copy);
  for (const rel of ['package.json', 'bin', 'skill']) fs.cpSync(path.join(root, rel), path.join(copy, rel), {recursive: true});
  fs.appendFileSync(path.join(copy, 'skill/SKILL.md'), '\ncorrupt');
  const result = spawnSync(process.execPath, [path.join(copy, 'bin/cli.cjs'), 'install', '--skills-dir', target], {encoding: 'utf8'});
  assert.equal(result.status, 1); assert.match(result.stderr, /hash mismatch/);
  assert.equal(fs.existsSync(target), false);
});
test('failed replacement restores the prior skill and removes staging files', t => {
  const dir = sandbox(t), skills = path.join(dir, 'skills'), target = path.join(skills, skill);
  json(run(['install', '--skills-dir', skills, '--json']));
  fs.writeFileSync(path.join(target, 'SKILL.md'), 'restore this edit');
  const originalRename = fs.renameSync;
  try {
    fs.renameSync = (from, to) => {
      if (path.basename(from).startsWith('.gdd-install-') && to === target) throw new Error('simulated replacement failure');
      return originalRename(from, to);
    };
    assert.throws(() => require('../bin/cli.cjs').install({skillsDir: skills, force: true}), /simulated/);
  } finally { fs.renameSync = originalRename; }
  assert.equal(fs.readFileSync(path.join(target, 'SKILL.md'), 'utf8'), 'restore this edit');
  assert.deepEqual(fs.readdirSync(skills), [skill]);
});
test('doctor reports missing external dependencies without claiming browser readiness', t => {
  const dir = sandbox(t);
  const result = json(run(['doctor', '--skills-dir', path.join(dir, 'skills'), '--json'], {env: {...process.env, PATH: ''}}));
  assert.equal(result.status, 'setup_needed');
  assert.equal(result.dependencies.python, null);
  assert.equal(result.dependencies.ffmpeg, null);
  assert.match(result.browser, /^not_checked/);
  assert.equal(fs.existsSync(path.join(dir, 'skills')), false);
});
test('invalid options fail, while version and help do not require a browser', () => {
  assert.equal(run(['install', '--skills-dir']).status, 1);
  assert.equal(run(['doctor', '--force']).status, 1);
  assert.equal(run(['--unknown']).status, 1);
  assert.equal(run(['--version']).stdout.trim(), '1.0.0');
  assert.match(run([]).stdout, /Usage:/);
});
test('real npm tarball installs offline via npm exec, with a strict publication file list', t => {
  const dir = sandbox(t), npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';
  const opts = {encoding: 'utf8', cwd: root, timeout: 60000,
    env: {...process.env, npm_config_cache: path.join(dir, 'npm-cache'), npm_config_update_notifier: 'false'}};
  const plan = spawnSync(npm, ['pack', '--dry-run', '--json'], opts);
  assert.equal(plan.status, 0, plan.stderr);
  const listing = JSON.parse(plan.stdout.slice(plan.stdout.indexOf('[')))[0].files.map(f => f.path);
  assert.equal(listing.length, 13);
  assert.ok(listing.includes('skill/scripts/state.py'));
  assert.ok(listing.includes('skill/bundle.json'));
  assert.ok(listing.every(p => p === 'README.md' || p === 'package.json' || p === 'bin/cli.cjs' || p === 'scripts/build.cjs' || p.startsWith('skill/')));
  assert.ok(listing.every(p => !/\.env|auth|\.mp4|__pycache__|\.DS_Store/.test(p)));
  const pack = spawnSync(npm, ['pack', '--pack-destination', dir], opts);
  assert.equal(pack.status, 0, pack.stderr);
  const tgz = path.join(dir, 'guangdada-browser-skill-1.0.0.tgz');
  assert.ok(fs.statSync(tgz).size > 0);
  const skills = path.join(dir, 'standalone skills');
  const installed = spawnSync(npm, ['exec', '--offline', '--yes', '--package=' + tgz, '--',
    'guangdada-browser-skill', 'install', '--skills-dir', skills, '--json'], {...opts, cwd: dir});
  assert.equal(installed.status, 0, installed.stderr);
  assert.equal(JSON.parse(installed.stdout).status, 'installed');
  assert.ok(fs.existsSync(path.join(skills, skill, 'scripts/frames.py')));
});
