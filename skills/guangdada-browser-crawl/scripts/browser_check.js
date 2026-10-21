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

// Already-correct base filters are observations, never fabricated clicks.
// A card's ad ID is only known after its actual detail opens; the intended
// App/platform is fixed before clicking and the observed ID is checked too.
async function gddCheckedObservation({tab, scopeSha256, taskId, eventId, key,
                                      expected, click, read, persist, sha256, card=false}) {
  if (!/^[a-z0-9-]+$/.test(eventId)) throw new Error('Use a unique lowercase eventId');
  if (card) {
    if (key !== 'card.open' || typeof click !== 'function' || !expected.app_id ||
        !['iOS','Android'].includes(expected.platform) ||
        Object.keys(expected).sort().join(',') !== 'app_id,platform') throw new Error('Missing intended card identity');
  } else if (!key.startsWith('filter.') || key === 'filter.sort' || click !== undefined) {
    throw new Error('Read-only checks only apply to base filters; App and sort require clicks');
  }
  for (const fn of [read,persist,sha256]) if (typeof fn !== 'function') throw new Error('Missing adapter');
  const base='evidence/clicks/'+eventId;
  const snap=async suffix=>{
    const bytes=await tab.getScreenshot({emit:false});
    const png=[137,80,78,71,13,10,26,10].every((b,i)=>bytes[i]===b);
    const jpeg=bytes[0]===255&&bytes[1]===216&&bytes[2]===255;
    if (!png&&!jpeg) throw new Error('Unsupported screenshot format');
    const format=png?'png':'jpg',path=base+'.'+suffix+'.'+format;
    await persist(path,bytes);return {path,format,sha256:await sha256(bytes)};
  };
  const setLike=['filter.channels','filter.regions','filter.languages'].includes(key);
  const normalize=v=>Array.isArray(v)?(setLike?v.map(normalize).sort((a,b)=>JSON.stringify(a).localeCompare(JSON.stringify(b))):v.map(normalize)):(v&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,normalize(v[k])])):v);
  const equal=(a,b)=>JSON.stringify(normalize(a))===JSON.stringify(normalize(b));
  const before=await read(),beforeImage=await snap('before'),performed_at=new Date().toISOString();
  let after=null,error=null,status='failed';
  try {
    if (card) await click();
    await tab.playwright.domSnapshot();after=await read();
    const valid=card ? after && Object.keys(after).sort().join(',')==='ad_id,app_id,platform' &&
      /^[a-f0-9]{32}$/.test(after.ad_id) && after.app_id===expected.app_id && after.platform===expected.platform :
      equal(before,expected)&&equal(after,expected);
    status=valid?'passed':'mismatch';
  } catch (exc) {error=String(exc.message||exc);}
  const afterImage=await snap('after');
  const event={producer:card?'gdd-checked-card/v1':'gdd-readonly-check/v1',scope_sha256:scopeSha256,
    task_id:taskId,event_id:eventId,key,kind:card?'click':'read',performed_at,status,expected,before,after,
    images:{before:beforeImage,after:afterImage},error};
  await persist(base+'.json',JSON.stringify(event,null,2));
  if(status!=='passed')throw new Error('Evidence check failed; inspect '+base+'.json');
  return {event_file:base+'.json',event};
}
async function gddReadOnlyCheck(args){return gddCheckedObservation({...args,card:false});}
async function gddCheckedCard(args){return gddCheckedObservation({...args,card:true});}
if (typeof module !== 'undefined') Object.assign(module.exports,{gddReadOnlyCheck,gddCheckedCard});

// The helper invokes the candidate locator's click directly; Enter is not an adapter.
async function gddCheckedAppCandidate({candidate, ...args}) {
  if (!candidate || typeof candidate.click !== 'function') throw new Error('A visible advertiser candidate locator is required');
  const persist = args.persist;
  const interaction = {action:'click',target:'advertiser_candidate'};
  const result = await gddCheckedClick({...args,key:'app.candidate',
    click:async()=>candidate.click(),
    persist:async(path,bytes)=>{
      if(path.endsWith('.json')){const event=JSON.parse(bytes);event.interaction=interaction;bytes=JSON.stringify(event,null,2);}
      return persist(path,bytes);
    }});
  result.event.interaction=interaction;return result;
}
if (typeof module !== 'undefined') Object.assign(module.exports,{gddCheckedAppCandidate});
