# 浏览器脚本点击与截图检查

## 必须使用的执行方式

读取 `../scripts/browser_check.js`，把 `gddCheckedClick` 函数定义放入**环境允许的浏览器工具 REPL**，再通过该函数执行操作。函数在工具侧运行，不能注入网页、放进 page.evaluate、用独立 Playwright 启动第二个浏览器或调用私有接口。

函数没有固定 CSS/坐标。按“分类→基础筛选→App→排序→点卡/翻页”的顺序执行；每次先观察当前 UI，按浏览器工具文档构造 click/read 回调；DOM evaluate 仅用于读取可见控件状态。先把该操作所需菜单打开、候选等可见目标定位好，再执行受检点击。

回调参数是本 helper 的契约，不是工具内置 API：

- `click()`：通过浏览器工具实际点击所需目标；不能仅返回 true。基础筛选已正确时使用 gddReadOnlyCheck，不点击展开控件；需要修改时才使用此点击函数；日志必须如实反映操作，不把无操作的 check() 宣称为真实点击。
- `read()`：从当前页面读取并归一实际值。禁止 `()=>expected`、直接复制 scope 或从旧记录返回值。复选框读 checked/aria-checked，日期读实际选中快捷项或起止输入值，App 读已选广告主和平台。
- `persist(relativePath, bytesOrText)`：由当前环境**允许的文件接口**实现，写到当前 task 目录，原子落盘且拒绝覆盖已有事件文件。使用唯一 eventId；失败时重新观察 UI 后产生新事件，不能覆盖失败日志。
- `sha256(bytes)`：由本地标准哈希能力实现，计算实际截图字节的 SHA-256。
- `tab`：当前浏览器工具标签对象；如接口名称不同，按当前环境文档适配截图和状态观察两处。工具无法保存截图或读取实际选值时，停在对应 PENDING，报告能力缺失，不能填写成功日志补过关。

函数实际执行 `read → 前截图 → click → DOM 观察 → read → 后截图 → 比较 → 保存日志`。读回不符或点击失败时保存失败日志并抛错，不自动重复点击。先检查页面上的加载、验证或遮挡，再决定如何修复。

示意调用（目标必须先在真实页面中观察到；persist/sha256 使用环境已提供或允许实现的本地回调）：

```javascript
await gddCheckedClick({
  tab, scopeSha256, taskId: 'tipsy-ios', eventId: 'dedupe-material-001',
  key: 'filter.dedupe', expected: '素材', persist, sha256,
  click: async () => tab.playwright.getByRole('radio', {name:'素材', exact:true}).click(),
  read: async () => tab.playwright.getByRole('radio', {name:'素材', exact:true})
    .evaluate(e => (e.checked === true || e.getAttribute('aria-checked') === 'true') ? '素材' : null)
});
```

这是某次页面已暴露 radio 的用法，不是永久选择器。日期“近1年”可规范成 scope 的“1年”，原截图保留真实 UI 标签。

## 日志与引用

每次产生：

```text
 evidence/clicks/<event-id>.before.png 或 .jpg
 evidence/clicks/<event-id>.after.png 或 .jpg
 evidence/clicks/<event-id>.json
```

日志包含 producer=gdd-checked-click/v1、scope_sha256、task_id、key、kind=click、执行时间、before、expected、after、status、错误及前后截图路径/哈希及实际格式。截图按原始字节检测 PNG/JPEG 后选择扩展名，不能假定工具一定返回 PNG。

对应 key 与 expected：

| 检查对象 | key | expected / 实际 after |
|---|---|---|
| 每个筛选字段 | filter.category / filter.channels / filter.regions / filter.dedupe / filter.date 等 | 与 scope.filters 对应字段完全相同 |
| App 选择 | app | app_name、platform、app_id、selected_app_ids、residual_keywords 五个字段组成的对象 |
| 打开一张卡片 | card.open | 当前详情的 ad_id、app_id、platform 对象 |
| 下一页 | page.open | {"page":实际页码} |

`filters.json.ui_checks` 按基础筛选字段（不含 sort）指向日志；sort.json.ui_check 独立引用 App 选择之后的排序日志；`app.json.ui_check` 和 `cards/*.json.ui_check` 指向各自日志。第2页及以后的 pages/*.json 也须 ui_check 指向 page.open 日志。

验证码恢复后重新产生各筛选和 App 日志；执行时间必须晚于 hold。把新日志路径写入 resume-verification.json，不能复用暂停前日志。

这些日志证明的是工具执行记录及其读回一致性，**不是不可伪造的浏览器取证或第三方签名**。agent 必须真实执行，不手工伪造 producer 或成功值；截图和页面语义仍需实际查看。

## 真实页面验收发现（2026-10-15）

- ant-radio-button 的内部 input 在本环境可能定位成功却无法点击；点击当前可见 label/文字，再读取 input.checked。不能因为 timeout 自动认定已选中，也不能盲目重复点击。
- 国家选中后可能移除 placeholder。读取之前必须确认目标唯一且存在，不能把“查不到控件”返回成 []。本次通过观察国家输入框所在结构和“国家/地区”已选标签定位，读取了真实 value。
- 国家弹层“全部”可能是展示所有候选的分类标签，不是清空条件；不限需关闭对应已选筛选标签，随后重新读回值为空。
- 多选渠道/国家/语言按集合比较，顺序无业务含义；脚本保留原始读回顺序，同时做集合等价比较。
- App 搜索输入值可能被浏览器工具脱敏，选中后 DOM 结构也会变化。使用可见完整名称、正确平台候选中的真实 App ID、唯一已选图标/平台徽标互相核对；probe 再从详情官方商店链接交叉验证真实 App ID。不能把搜索框里输入的文字当作广告主已选中的唯一证据。
- 多选视频类型与单选平台的选中标签不在同一 DOM 层级；应先观察当前结构再读取可见已选项，不复用固定 parent 层级。
- 隐藏的旧广告主关闭图标可能 force-click 后仍未移除；本次在截图和新鲜元素边界支持下用浏览器原生点击关闭，再确认已选徽标数量归零。不得强行继续带旧 App 的搜索。
- 点卡和翻页之后可能仍有加载占位。页码变更与详情加载分别验证；只有读取到真实详情字段/新卡片集合才记录对应成功。
- 窄侧栏可能遮挡桌面筛选控件。本次在证实操作受影响后临时使用桌面宽度完成测试，结束恢复默认；不要仅为截图美观调整窗口。
- 浏览器 REPL 若禁止 eval/new Function，应读取并直接粘贴 helper 函数定义，不用字符串动态执行。修复函数后要显式传入新版 checker 或重建外层包装函数，防止旧闭包仍调用旧版本。

这些是已观察到的交互问题及处理原则，不是永久定位器。验收只覆盖上述类别和代表值，不等于遍历所有渠道/国家组合或完成竞品全量采集。

- 本次浏览器实际返回 JPEG，已修正 helper 按文件签名命名 .jpg，校验器按实际图片字节接受 PNG/JPEG；视频抽帧仍严格要求 PNG。初期误标扩展名的证据不改写，保留原字节及哈希，后续按正确格式新建事件复测。

## 切换 App 的基础筛选复用

每个 App 仍重新检查分类和基础筛选，但正确时只读核对。gddReadOnlyCheck 支持所有非 sort 的 filter.*，要求 before、after 均等于 expected，并分别保存截图及哈希；渠道/地区/语言按集合比较。值不符时检查失败，按可见控件实际修改后重新检查。它拒绝 App、排序、点卡和翻页；这些步骤仍保存真实点击证据。每个 App 使用新的 task_id/event_id，检查时间晚于上一 App 封账，禁止复用旧日志。

## App候选必须真实点击（2026-10-21）

搜索类型为“广告主”，输入完整名或已核实App ID后，先记录app.query；使用gddCheckedAppCandidate({candidate:当前可见的正确候选locator,...})执行候选locator.click()，不能用press Enter回调。read必须从点击后的已选区域读回app_name/platform/app_id/selected_app_ids/residual_keywords/search_mode六项。回车可以辅助提交查询，但不能作为候选点击证据；先读候选身份，再点击，再核对已选身份。失败留在APP_PENDING，不把候选未加载误判零结果。app.json须保留query_ui_check与ui_check两个引用。
