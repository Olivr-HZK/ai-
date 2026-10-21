# 每日混合媒体统一状态机

本规则用于新prepare生成的每日任务（视频+图片），取代每日执行中的旧state.py状态推进；通用单次采集skill仍保留旧FSM。唯一权威状态是 `<run>/daily-machine/state.json`。scope固定目标日及21项App；不得编辑cursor、成功值或伪造证据。历史目录不自动迁移、重跑、写表或发群消息。

共21个命名状态：17个执行状态、COMPLETED、3个控制状态。PREPARE一次，以下7个App状态按scope顺序循环；21项全部封账后进入批次后处理。新21项任务共157个执行步骤，状态数量不等于步骤数量。

| 状态 | 动作 | 通过条件与下一状态 |
|---|---|---|
| PREPARE | 固定日期、名单、筛选、scope哈希，初始化 | prepare成功后自动校验scope/run哈希，进入START_NOTICE_PENDING |
| START_NOTICE_PENDING | 执行既有启动通知命令 | 正确群、@全体及真实发送回执核验；进入BROWSER_PENDING，同批不重复发送 |
| BROWSER_PENDING | 启动/确认防睡眠租约，核对登录 | browser.json记录scope哈希、authenticated、browser_session及截图哈希；进入首App CATEGORY_PENDING |
| CATEGORY_PENDING | 观察工具分类 | 已正确只读，不正确才修正；真实前后读回/截图通过后进入FILTERS_PENDING |
| FILTERS_PENDING | 一次观察基础筛选：工具、素材、目标日、图片&视频、类型空选、渠道/地区/语言/尺寸不限 | 正确只读；仅修正不一致项；进入APP_PENDING。不为检查空选打开下拉框 |
| APP_PENDING | 清理旧App/关键词，搜索类型选广告主，输入App ID并点击下方正确候选，再核对已选身份 | 真实app.query与其后的app.candidate点击日志、App ID/平台一致、唯一已选App及无残留；仅回车不通过；进入SORT_PENDING |
| SORT_PENDING | 本次搜索后重新点击最新创意 | 真实filter.sort日志晚于本次query和候选点击；进入PROBE_PENDING |
| PROBE_PENDING | 点第一张卡核对身份、详情日期 | 详情及card.identity证据有效；第一张直接计入采集，不重采。明确零结果凭完整零结果证据跳过点卡；进入COLLECTING |
| COLLECTING | 串行点卡和翻页，按详情日期范围起点判断目标日；重投照常采 | 保留连续位置、逐卡身份/截图、日期截止或明确零结果证据；执行单App混合校验通过后进入COLLECTION_SUMMARY_PENDING |
| COLLECTION_SUMMARY_PENDING | 验证本App盘点、截止证据并封账 | 校验器生成daily-machine/app-seals/<task>.json（原始清单哈希、数量、完成时刻）；下一App CATEGORY_PENDING，最后一项进入RELAUNCH_PENDING |
| RELAUNCH_PENDING | 重新校验21项并按详情可见重投标签划分 | 写核对统计，进入MEDIA_PENDING；不删除原始卡片 |
| MEDIA_PENDING | 非重投视频下载/解码/六帧；图片原图下载/解码/尺寸/哈希 | 复用独立混合校验；有实际有限重试证据才允许媒体缺失例外；进入REVIEW_PENDING |
| REVIEW_PENDING | 实际看视频六帧或全部原图，记录审核理由 | 混合校验器完整审核检查（不生成排名产物）；没有未解决内容复核；进入RANK_PENDING |
| RANK_PENDING | 执行mixed_window select | 完整重新校验并合并取10%向上取整，生成selection及validation；进入DELIVERY_PENDING |
| DELIVERY_PENDING | 执行mixed_delivery（不加--apply），制作本地预览及待写表包 | 文件/待写入包存在；仍需实际打开预览检查，不以文件存在宣称播放已验收；进入SYNC_PENDING |
| SYNC_PENDING | 执行mixed_delivery --apply | 既有同步器幂等写入、业务字段/附件token及大小远端读回通过；进入REPORT_PENDING |
| REPORT_PENDING | 执行既有完成日报命令 | 正确群、真实发送回执、统计及表链接核对；进入COMPLETED，同批不重复发送 |
| COMPLETED | 本批完成 | 不再采集/写表/发消息 |
| WAITING_HUMAN | 验证码/登录需要人工 | 中断当前attempt并停止租约，保存原因、原阶段；人工完成后resume，再start租约，重新只读检查登录/筛选/身份，恢复断点 |
| PAUSED | 用户要求暂停 | 同上停止租约；用户继续后resume，不改变日期和采集断点 |
| RETRY_REQUIRED | 工具/校验失败 | 保存真实错误、已耗时、证据；修复原因后显式resume，仅重试当前阶段，不从头跑 |

## 执行与耗时

使用项目命令 `.venv/bin/python -m ua_workflows.social_chat_daily_state <action> --run-dir <目录>`：

- `status`：只读返回当前状态、task、阶段耗时/次数/失败数、动作耗时/次数/错误数和人工等待时间；不要以高频轮询推动工作。
- 浏览器阶段先 `begin`，实际动作完成后 `complete --proof <相对run路径>`。校验失败自动记失败并保持原阶段，显示RETRY_REQUIRED。`fail --reason <真实错误>`用于工具失败。不能把尝试操作当完成。
- Python排名、交付、写表、启动/完成通知的CLI入口自动begin/complete或记录失败；不要在这些入口外再begin。`--test`通知不推进业务状态；内部纯证据复核不制造重复阶段计时。
- `hold --kind WAITING_HUMAN|PAUSED --reason ...`关闭当前attempt并保留原阶段；另调用browser_collection_runtime stop释放租约。`resume --reason <实际确认或修复说明>`后需重新begin，人工等待时间单独累计。App阶段从WAITING_HUMAN/PAUSED恢复必须带`--proof <恢复观察JSON>`，除FILTERS结构外同时记录authenticated=true、app_id、platform、selected_app_ids（唯一）、residual_keywords=[]，observed_at必须晚于hold；校验实际登录、筛选和身份后才解锁。工具卡死留下active_attempt时先hold，禁止静默覆盖旧attempt。
- 每阶段最多3次attempt（初次+2次重试），不自动重试外部写入。用尽后说明阻塞；确认根因解决后才可 `resume --extra-attempts 1 --reason ...` 为当前阶段增加预算，不扩大其他阶段预算。
- attempt记录started_at/ended_at、实际elapsed_seconds、次数、结果、脱敏错误和证据SHA256。执行者处理及工具往返计入阶段耗时，人工等待另记。尚未结束阶段显示实时经过时间，不伪造成已完成。

## 浏览器阶段证据

CATEGORY/FILTERS的proof是浏览器实际读回生成的JSON，不得只填期望值。包含scope_sha256、task_id、observed_at（带时区）、action、before、after、expected、images.before/after（path相对run及sha256）。每个阶段begin后生成新观察；校验器拒绝旧时间或哈希不符。

CATEGORY的expected为`{"category":"工具"}`。
FILTERS的expected为`{"category":"工具","dedupe":"素材","date":{"start":"目标日","end":"目标日"},"media":"图片&视频","selected_types":[],"channels":[],"regions":[],"languages":[],"size":""}`，目标日填scope的真实值。

before与expected相同必须action=readonly。不同才action=repair，先记录不一致，只操作变化项，然后after须等于expected。一个浏览器脚本观察全部基础条件，可同一张截图覆盖多个条件，无需每个空控件点开/截图。App选择和排序不适用此省略，仍需真实动作。

APP proof记录scope_sha256、observed_at、app_name（网站完整显示名；合法别名按通用skill身份映射）、app_id、platform、selected_app_ids（唯一）、residual_keywords=[]、search_mode=广告主、query_type=app_id、query_app_id及query_ui_check、ui_check两个路径（相对本task根目录）。query_ui_check指向本次app.query日志；ui_check指向搜索后通过gddCheckedAppCandidate实际点击候选产生的app.candidate日志，interaction必须为action=click/target=advertiser_candidate。observed_at须晚于候选点击，实际读回已选区域；不能用输入框内容冒充选中身份。通过后保存app-selection.json供最终混合校验复查。只有回车/搜索提交或未加载候选都不能完成APP_PENDING；必须等待正确候选并点击。PROBE proof指向第一张实际card JSON及其真实card.identity点击证据；零结果指向collection_complete.json且必须由混合校验验证为空。

COLLECTING结束需要原始collection_complete.json和全部原始证据；它在这里是待校验盘点，最终封账以SUMMARY生成的daily-machine/app-seals为准。状态机不伪造卡片或截图，不把未采完当零。

## 动作日志（每页批量写入，避免每卡额外CLI往返）

浏览器脚本给每个实际动作记录UUID、name、action（readonly/repair/search/sort/card/page/download/review）、page/card、started_at/ended_at、error（失败时原始消息）、evidence_paths（相对run）。readonly/repair必须同时有真实before/after/expected。观察时间必须在当前attempt内，不凭估计补写历史耗时。

每页结束将JSON数组通过stdin交给 `social_chat_daily_state operations --run-dir ...`；同一UUID拒绝重复，同阶段/task/name/page/card再次执行自动标记operation_attempt和retry。日志留在daily-machine/operations.json，status按动作统计；不得跳过出错动作只留成功项。非HTTP错误仅保存工具提供的信息，不编造调用栈。resume后的登录/筛选/身份复查也记动作，条件正确仍readonly。

顺序：先完成本页真实动作、批量登记动作，再complete阶段。失败时先批量记录已有动作再fail/hold。禁止把按钮点击成功作为业务数据已获取成功。未实际执行的新批不填耗时；历史不反推。
