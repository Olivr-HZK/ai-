#!/usr/bin/env node
'use strict';
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const root = path.resolve(__dirname, '..');
const pkg = require('../package.json');
// An explicit list keeps project data, login state and generated caches out of npm.
const files = [
  'SKILL.md', 'agents/openai.yaml', 'references/browser-check.md',
  'references/scope.example.json', 'references/state-contract.md',
  'scripts/browser_check.js', 'scripts/frames.py', 'scripts/state.py'
];
const source = path.resolve(root, '../../skills/guangdada-browser-crawl');
const output = path.join(root, 'skill');
const hash = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
if (!fs.existsSync(path.join(source, 'SKILL.md'))) {
  // A packed package is self-contained and can be packed again without the repo.
  require('../bin/cli.cjs').verifyBundle(root);
  process.exit(0);
}
const text = fs.readFileSync(path.join(source, 'SKILL.md'), 'utf8');
if (!new RegExp('version: ["\']?' + pkg.version.replaceAll('.', '\\.') + '["\']?\\s').test(text)) {
  throw new Error('Skill metadata.version must match package.json version');
}
const stage = fs.mkdtempSync(path.join(root, '.skill-build-'));
try {
  const hashes = {};
  for (const rel of files) {
    const file = path.join(source, rel);
    if (!fs.lstatSync(file).isFile()) throw new Error('Expected regular file: ' + rel);
    const bytes = fs.readFileSync(file);
    hashes[rel] = hash(bytes);
    const dest = path.join(stage, rel);
    fs.mkdirSync(path.dirname(dest), {recursive: true});
    fs.writeFileSync(dest, bytes);
  }
  fs.writeFileSync(path.join(stage, 'bundle.json'), JSON.stringify({
    package: pkg.name, version: pkg.version, skill: 'guangdada-browser-crawl', files: hashes
  }, null, 2) + '\n');
  fs.rmSync(output, {recursive: true, force: true});
  fs.renameSync(stage, output);
  console.log(`Bundled ${files.length} skill files for ${pkg.name}@${pkg.version}`);
} finally {
  fs.rmSync(stage, {recursive: true, force: true});
}
