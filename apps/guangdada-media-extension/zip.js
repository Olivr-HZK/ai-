// ZIP STORE: media is already compressed; keep original bytes, UTF-8 paths, CRC32.
const table = Uint32Array.from({length: 256}, (_, n) => {
  for (let k = 0; k < 8; k++) n = (n & 1) ? 0xedb88320 ^ (n >>> 1) : n >>> 1;
  return n >>> 0;
});
export function crc32(bytes) {
  let crc = 0xffffffff;
  for (const byte of bytes) crc = table[(crc ^ byte) & 255] ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}
export function makeZip(entries) {
  if (entries.length > 65535) throw new Error('ZIP 文件数量超过上限');
  const parts = [], directory = [];
  let offset = 0, dirSize = 0;
  for (const {name, bytes} of entries) {
    if (name.startsWith('/') || name.split('/').includes('..')) throw new Error('无效的 ZIP 文件名');
    const path = new TextEncoder().encode(name);
    if (path.length > 65535 || bytes.length > 0xffffffff) throw new Error('ZIP 文件过大');
    const crc = crc32(bytes);
    const local = new Uint8Array(30 + path.length), l = new DataView(local.buffer);
    l.setUint32(0, 0x04034b50, true); l.setUint16(4, 20, true);
    l.setUint16(6, 0x800, true); l.setUint16(12, 33, true);
    l.setUint32(14, crc, true); l.setUint32(18, bytes.length, true); l.setUint32(22, bytes.length, true);
    l.setUint16(26, path.length, true); local.set(path, 30);
    const central = new Uint8Array(46 + path.length), c = new DataView(central.buffer);
    c.setUint32(0, 0x02014b50, true); c.setUint16(4, 20, true); c.setUint16(6, 20, true);
    c.setUint16(8, 0x800, true); c.setUint16(14, 33, true);
    c.setUint32(16, crc, true); c.setUint32(20, bytes.length, true); c.setUint32(24, bytes.length, true);
    c.setUint16(28, path.length, true); c.setUint32(42, offset, true); central.set(path, 46);
    parts.push(local, bytes); directory.push(central);
    offset += local.length + bytes.length; dirSize += central.length;
    if (offset + dirSize > 0xffffffff) throw new Error('ZIP 超过 4GB 上限');
  }
  const end = new Uint8Array(22), e = new DataView(end.buffer);
  e.setUint32(0, 0x06054b50, true); e.setUint16(8, entries.length, true); e.setUint16(10, entries.length, true);
  e.setUint32(12, dirSize, true); e.setUint32(16, offset, true);
  return new Blob([...parts, ...directory, end], {type: 'application/zip'});
}
