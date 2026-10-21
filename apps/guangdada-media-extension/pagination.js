// Runs in MAIN world. Use the rendered pagination controls, never a private API.
export function pageControl(action = 'read', expected = null) {
  try {
  const visible = el => !!el && el.getClientRects().length > 0 && getComputedStyle(el).visibility !== 'hidden';
  const pagers = [...document.querySelectorAll('.ant-pagination')].filter(visible);
  if (pagers.length !== 1) throw new Error('未能唯一识别列表分页控件，请关闭详情弹窗并打开展示广告列表。');
  const pager = pagers[0];
  const current = pager.querySelector('.ant-pagination-item-active, [aria-current="page"]');
  const pageNumber = Number(current?.getAttribute('title') || current?.textContent.trim());
  if (!Number.isInteger(pageNumber) || pageNumber < 1) throw new Error('无法读取当前页码，已停止翻页。');
  const next = pager.querySelector('.ant-pagination-next, [title="下一页"], [aria-label="Next Page"], [aria-label="next page"]');
  if (!next) throw new Error('未识别到下一页控件，无法判断是否末页。');
  const disabled = el => !!el && (el.disabled || el.getAttribute('aria-disabled') === 'true' || el.classList.contains('ant-pagination-disabled'));
  const hasNext = !disabled(next) && !disabled(next.querySelector('button, a'));
  const cards = [...document.querySelectorAll('.shadow-common-light.bg-white')].filter(visible).sort((a,b) => {
    const ar=a.getBoundingClientRect(), br=b.getBoundingClientRect();
    return Math.abs(ar.top-br.top)<4 ? ar.left-br.left : ar.top-br.top;
  });
  const cardKeys = cards.map(card => {
    const key=Object.keys(card).find(k=>k.startsWith('__reactFiber$'));
    let fiber=key && card[key];
    for(let depth=0;fiber && depth<8;depth++,fiber=fiber.return) {
      if(typeof fiber.memoizedProps?.value?.ad_key === 'string') return fiber.memoizedProps.value.ad_key;
    }
    return null;
  });
  const loading = [...document.querySelectorAll('.ant-spin-spinning, [aria-busy="true"]')].some(visible);
  if (expected && (expected.pageNumber !== pageNumber || JSON.stringify(expected.cardKeys) !== JSON.stringify(cardKeys))) {
    throw new Error('页面素材或页码已变化，请重新开始翻页采集。');
  }
  if (action === 'warm') {
    if (loading) throw new Error('页面仍在加载，未滚动素材卡片。');
    const card = cards[expected?.cardIndex - 1];
    if (!card) throw new Error('待加载的素材卡片已变化。');
    card.scrollIntoView({block:'center',behavior:'instant'});
  }
  if (action === 'next') {
    if (loading) throw new Error('页面仍在加载，未点击下一页。');
    if (!hasNext) return {pageNumber, cardKeys, hasNext, loading, clicked:false};
    // Selection badges belong to the previous page; never carry them forward.
    document.querySelectorAll('[data-gdd-download-number], #gdd-download-selection-toolbar').forEach(el=>el.remove());
    (next.querySelector('button, a') || next).click();
  }
  return {pageNumber, cardKeys, hasNext, loading, clicked:action === 'next' && hasNext};
  } catch (error) { return {error:error.message}; }
}
