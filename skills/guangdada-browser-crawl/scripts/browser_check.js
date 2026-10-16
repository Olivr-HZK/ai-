/* Run this helper in the permitted browser-tool REPL, never inside page.evaluate.
 * click/read must use that tool's documented API and fresh visible DOM locators.
 * persist(path, bytesOrText) is the environment's permitted local-file bridge.
 * sha256(bytes) is a local hashing callback. No standalone browser driver here.
 */
async function gddCheckedClick({tab, scopeSha256, taskId, eventId, key, expected,
                                click, read, persist, sha256}) {
  for (const value of [click, read, persist, sha256]) {
    if (typeof value !== 'function') throw new Error('Missing browser/file/hash adapter');
  }
  if (!/^[a-z0-9-]+$/.test(eventId)) throw new Error('Use a unique lowercase eventId');
  const setLike = ['filter.channels', 'filter.regions', 'filter.languages'].includes(key);
  const normalize = value => {
    if (Array.isArray(value)) {
      const items = value.map(normalize);
      return setLike ? items.sort((a,b) => JSON.stringify(a) < JSON.stringify(b) ? -1 : JSON.stringify(a) > JSON.stringify(b) ? 1 : 0) : items;
    }
    if (value && typeof value === 'object') {
      return Object.fromEntries(Object.keys(value).sort().map(k => [k, normalize(value[k])]));
    }
    return value;
  };
  const equal = (a, b) => JSON.stringify(normalize(a)) === JSON.stringify(normalize(b));
  const base = 'evidence/clicks/' + eventId;
  const saveImage = async suffix => {
    const bytes = await tab.getScreenshot({emit: false});
    const raw = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
    const png = raw.length >= 8 && [137,80,78,71,13,10,26,10].every((b,i) => raw[i] === b);
    const jpeg = raw.length >= 3 && raw[0] === 255 && raw[1] === 216 && raw[2] === 255;
    if (!png && !jpeg) throw new Error('Unsupported screenshot format');
    const format = png ? 'png' : 'jpg';
    const path = base + '.' + suffix + '.' + format;
    await persist(path, bytes);
    return {path, sha256: await sha256(bytes), format};
  };
  const before = await read();
  const beforeImage = await saveImage('before');
  let after = null, error = null, status = 'failed';
  const performedAt = new Date().toISOString();
  try {
    await click(); // Execute the real browser-tool action; no caller-supplied success flag.
    // State observation also lets the browser tool settle; never blindly repeat click.
    await tab.playwright.domSnapshot();
    after = await read();
    status = equal(after, expected) ? 'passed' : 'mismatch';
  } catch (exc) {
    error = String(exc.message || exc);
  }
  const afterImage = await saveImage('after');
  const event = {producer: 'gdd-checked-click/v1', scope_sha256: scopeSha256,
    task_id: taskId, event_id: eventId, key, kind: 'click', performed_at: performedAt,
    status, expected, before, after, images: {before: beforeImage, after: afterImage}, error};
  await persist(base + '.json', JSON.stringify(event, null, 2));
  if (status !== 'passed') throw new Error('Click check failed; inspect ' + base + '.json');
  return {event_file: base + '.json', event};
}

// Allows offline testing only. Browser REPLs may paste the function above directly.
if (typeof module !== 'undefined') module.exports = {gddCheckedClick};
