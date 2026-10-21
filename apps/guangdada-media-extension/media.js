export function allowedPage(value) {
  try {
    const u = new URL(value);
    return u.protocol === "https:" && (u.hostname === "guangdada.net" || u.hostname.endsWith(".guangdada.net"));
  } catch { return false; }
}

export function normalize(items) {
  const found = new Map();
  for (const item of items) {
    try {
      const u = new URL(item.url);
      if (!["http:", "https:"].includes(u.protocol) || u.username || u.password) continue;
      u.hash = "";
      const playable = item.kind === 'playable';
      const video = !playable && (item.kind === "video" || /\.(mp4|webm|mov|m4v)$/i.test(u.pathname));
      // Only a URL positively identified as video may use this legacy CDN alias.
      if (video) u.pathname = u.pathname.replace(/\.webp$/i, ".mp4");
      if (/\.(m3u8|mpd|ts)$/i.test(u.pathname)) continue;
      const url = u.href;
      const old = found.get(url);
      if (!old || (video && old.kind !== 'video')) found.set(url, {...item, url, kind: playable?'playable':video ? "video" : "image"});
    } catch { /* Ignore malformed page attributes. */ }
  }
  return [...found.values()];
}

export function fileName(item, index, folder) {
  const path = new URL(item.url).pathname;
  const ext = item.kind==='playable'?(path.match(/\.(html?)$/i)?.[1]?.toLowerCase() || 'html'):path.match(/\.(mp4|webm|mov|m4v|jpg|jpeg|png|gif|webp|avif|svg|bmp)$/i)?.[1]?.toLowerCase()
    || (item.kind === "video" ? "mp4" : "jpg");
  const base = path.split("/").pop().replace(/\.[^.]+$/, "").replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 70) || "media";
  const page = item.pageNumber ? `/第${String(item.pageNumber).padStart(4, '0')}页` : '';
  return `${folder}${page}/${item.kind}/${String(item.cardIndex || index + 1).padStart(4, "0")}_${base}.${ext}`;
}

export function pageLimit(value = 1) {
  const count = Number(value);
  if (!Number.isInteger(count) || count < 1 || count > 1000) throw new Error('采集页数须为 1-1000 的整数（包含当前页）。');
  return count;
}

export function mergeCreatives(items) {
  const seen = new Set();
  return normalize(items).filter(item => {
    if (!item.adId) return true;
    if (seen.has(item.adId)) return false;
    seen.add(item.adId); return true;
  });
}

export function parseSelection(text, count) {
  if (!Number.isInteger(count) || count < 1) throw new Error("未取得本页素材数量");
  const value = String(text ?? "").trim().replace(/[，、；;]/g, ",").replace(/[–—～~]/g, "-");
  if (!value) return Array.from({length: count}, (_, i) => i + 1);
  const selected = new Set();
  for (const token of value.split(/[,\s]+/).filter(Boolean)) {
    const match = /^(\d+)(?:-(\d+))?$/.exec(token);
    if (!match) throw new Error("编号格式不正确，请输入 1,3,4 或 1-4,7");
    const start = Number(match[1]), end = Number(match[2] ?? match[1]);
    if (start < 1 || end > count || start > end) throw new Error(`编号须在 1-${count} 内，范围从小到大填写`);
    for (let i = start; i <= end; i++) selected.add(i);
  }
  if (!selected.size) throw new Error("请填写有效编号，或清空以选择全部");
  return [...selected].sort((a, b) => a - b);
}
