# Afizzy 05:00 无头采集、筛选与原素材入表

## 2026-11-05 Mac mini 05:00任务执行绑定

保留原`automation-3`、北京时间05:00、ACTIVE及原关联聊天。完整日流程的代码目录统一为`/Users/ggbond/oliver/replica-listener`，持久目录为`/Users/ggbond/oliver/afizzy-state`。聊天仍关联旧项目不代表要执行旧代码；每次shell调用显式使用新代码目录为workdir，并设置：

```sh
cd /Users/ggbond/oliver/replica-listener
export UA_STATE_ROOT=/Users/ggbond/oliver/afizzy-state
export PATH=/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin
```

`.venv`及`.env`均通过本部署目录链接到持久目录的独立环境和配置；禁止回退旧`ai-`的解释器或凭据。`data/afizzy_daily`、原采集断点、封面缓存/向量、模板/语言审核、入表及两端日报回执均复用持久数据，不新建空历史。旧绝对证据路径只经兼容链接读取，不重写历史JSON、scope或发送回执。新批显式三个预设与`D_game_tool_tool2`，旧`D_both`仅按原规则恢复。

全部业务规则以本代码目录当前config/skill及既有scope为准；本次只迁执行绑定。日流程结束于原素材入表核验和唯一飞书/企业微信日报，不接管独立复刻监听。参见[日任务依赖与验收](../../../docs/afizzy-daily-entry.md)。


**2026-11-04新增预设（从11月5日执行起）**：每日固定游戏afizzy竞品游戏、工具afizzy竞品、工具afizzy竞品2三组。按config.daily_workflow.presets显式传`--presets game tool tool2`；新采集断点/输出范围为`D_game_tool_tool2`，summary.scope须精确等于三组才算范围完成。旧两组`D_both`及其已交付结果不迁移/重采；本次用户明确单独测试新预设的11月3日素材，固定`D_tool2`，只采新预设，后处理作为原日补充批次去重、入表和独立验收，不冒称重跑完整旧批。

2026-11-04用户明确选择“沿用原脚本，只加预设”：本次及后续每日任务沿早上已实站跑通的v0.2.1采集器，允许原脚本读取当前可见卡片绑定的React字段获取该卡广告ID、类型和原址；这项明确授权覆盖此前仅DOM/禁止该卡片绑定字段的限制，不扩展到私有接口、全局隐藏状态、共享Cookie、绕过验证或未显示数据。日期和App/平台/估值/重投继续取原脚本的可见证据，按原断点批量保存地址，关闭浏览器后三路静态下载。未验收DOM改造已隔离，不在正式批次反复试验；新增预设只做范围/名称配置，不擅自混入底层重构。

2026-11-03用户授权完整日链路，已于2026-11-04原09:00首次执行；同日用户将后续每日执行时间提前为北京时间05:00。复用本机automation-3，不补跑已完成批次；目标日D为原执行日R的前一天。此入口优先于旧“仅手动交包/暂停广大大”说明；手动ZIP和历史scope仍保留原规则。调度由本聊天Agent执行，实际看图与模板判断仍由Agent完成，不能用占位审核或仅启动worker代替。

## 固定目录、锁与范围

先读取 config/social_chat_browser_daily.json 的 daily_workflow、原目录和实际活跃PID。日账固定 data/afizzy_daily/D/，原执行日、目标日、Asia/Shanghai、配置与scope哈希落盘，恢复不漂移日期。保存collection/screening/original_sync各阶段状态及证据引用；已有成功阶段仅核验后继续下一步。完成旧批不重算、不重采、不重新生成或上传。跨阶段按广告ID、系统、源SHA256、原记录ID和任务ID绑定，保护手动增加、其他平台与其他目标日记录。

使用唯一日批锁；采集器自身 profile_lock 和复刻worker/表写锁继续生效。已运行的worker只核对PID、脚本、scope、进度，不再启动第二份。多个批次外部写入共享同表串行锁；既有独立复刻批次不属于本日任务范围。只读取本项目.env业务凭据，复刻CLI复用其已授权的私有会话，不输出密码、token或签名URL。

## 1. 完整采集

先读 docs/guangdada-direct.md。从2026-11-05执行起固定游戏→afizzy竞品游戏、工具→afizzy竞品、工具→afizzy竞品2三组全部卡片；预设后重新设置网站起止日期D、最新创意，保存真实筛选、连续页码/广告身份与日期截止证据。不传小样、--only或页数上限。正常登录最多一次，安全验证或账号冲突立即截图、保留断点、释放防睡眠，通知需要人工；不绕过验证或反复抢登录。使用独立Playwright和原验证读取器：可读当前可见卡片绑定React字段；不读全局隐藏状态，不使用扩展/IAB、旧VE、私有API或其他浏览器Cookie。

```sh
caffeinate -i -s .venv/bin/python -m ua_workflows.guangdada_direct run --date D --presets game tool tool2 --state-dir data/afizzy_daily/collector-state --output data/afizzy_daily/raw --credentials-env /Users/ggbond/oliver/afizzy-state/.env
```

D须替换为实际YYYY-MM-DD。新三组断点为 collector-state/runs/D_game_tool_tool2，输出 raw/D_game_tool_tool2；旧D_both保留原范围。先检查本scope的game/tool/tool2状态及summary：已采到真实末页/日期截止则复用，不因下载有缺失重采网页。暂停项使用同参数resume，不新建目录绕过断点；已有ZIP按SHA256复用。未知翻页先核对原意图，不盲目再点。下载按现有三路预算，只获取保存的原地址；不能无限重试不可解析外链或404。可复用严格同广告ID/系统/日期/原址且大小/哈希一致的真实原文件缓存，另存恢复证据和新交付版本，保留初次失败。

独立核验原报告、元数据、目标日期、每条原文件SHA256/大小、ZIP CRC与原字节；视频完整解码、图片解码，已有同源哈希一致的解码回执复用。Playable仅静态存档，绝不执行或进入筛选/复刻。summary.complete不等于完整下载：必须分别读取collection_complete、all_media_downloaded、full_day_complete及missing/failed。仅下载缺失允许对实际取得的媒体部分继续并披露；采集未到截止、身份/估值/日期或哈希证据缺失不能伪称完整或跳过。

## 2. 筛选与原素材入表

2026-11-04模板空历史修正优先：按config.plugin_handoff.previous_day_template_dedupe固定v2策略，新scope同时深复制template_identity_mapping。用social_chat_template_history.load_confirmed_history承接全部已确认结构（优先经SHA核验的template-history-latest.json），不因前日未入选、未入表或前日自己的跨日核验pending丢掉确认结构。前日来源/身份已核验且无可用模板时建立当天新基线，再同产品/系统日内模板去重，不整组待核；真实数据/身份异常不当零。显式Tizz别名归同产品，OS仍分开；其他名称不模糊合并。用户已授权本批31条模板待核按v2修订，原交付/入表证据保留。

筛选第一步先执行config.orientation_filter并在新scope深复制固定。原文件/日期/身份核验后、重投和CLIP之前，用ffprobe取得实际首视频流宽高、sample_aspect_ratio和旋转元数据，按实际显示方向判定；图片解码取得显示尺寸（含EXIF方向）。宽>高为横屏直接排除，正方形/竖屏保留；不按缩略图判断。多图逐原图排除横图、剩余原图继续；整条无剩余则排除。未知尺寸/旋转信息冲突记pending，不伪造0或放行。excluded-landscape.json逐文件保存广告ID、源SHA256/大小、原probe及哈希或图像尺寸证据、实际显示宽高、规则和排除原因，汇总排除条数/文件数；横屏不进入后续向量、六帧/主体/模板/语言审核或入表。此前原报告/文件保持，不重算旧scope。

沿SKILL.md及pre-review.md、content-review.md完成原筛选四道和前一天模板比对。独立采集ZIP含原下载报告和同源结构化game/tool/tool2.json：按实际页/位置/广告ID关联，App/系统/估值/重投取metadata实际观察；缺App ID留空，未知重投不造false。原报告不改写。保存原包交接清单及SHA256，标准化媒体索引；新筛选scope记录真实execution_origin=scheduled、collection_origin=guangdada-direct、源包与采集summary哈希，固定config中的processing_policy、ranking.mode=all、previous_day_template_dedupe和image_replication_after_ingestion。不得伪标为用户手动交包：现有manual-only prepare不是此入口，勿调用它来假造触发来源；按现有pre-review输入格式建立真实未审核scope与索引，再运行pre-review及后续流程，不能手填完成状态。

先尺寸筛选后再剔明确重投，剩余非重投已取得视频/图片先同App/系统CLIP0.9日内去重、前7自然日封面去重；历史使用同一已核实模型和指纹，缺日留账。只对剩余项实际查看六帧/全部原图，同次做男性有意义展示、女频伴侣/恋爱相关、排除真人主持式推广、风格与模板判断；存疑补证据，仍存疑不纳入。与D−1同App/系统完整已确认模板对照，确认重复剔当天项；缺历史记pending，不当零历史。保存当天全部已确认模板及剔除关联供次日使用，向量包不能代替模板证据。

所有合格模板代表按实际展示估值降序交付，不截100条/10%。入表前核验主要文案和完整音轨口播的单主语言，品牌/背景歌词不决定语言。按既有混合素材交付函数、唯一键和统一命名上传；目标固定既有Base Adz4bj9ECaCCJrsgkj7coheYn3c、表tblwhAqPfQoBXK3Q，原素材字段fldRHIjFcO，链接fldM60UZFW，主语言fldcaMX7YL。写前按ID核对真实schema，不创建旧列。串行写入并独立读回业务字段、token、名称、精确大小和本地SHA256；原素材入库核验后本日任务结束。

## 3. 原素材入表核验后结束

2026-11-03用户最新要求：后续不自动进入复刻。原素材入库独立核验后，交付精选ZIP、审核账与实际入表数量即可结束；新批固定automatic_replication=false，image_replication_after_ingestion不启用。视频和图片均只在用户另行明确要求后启动复刻及复刻附件回填，不自动prepare/run/resume复刻worker、不提交Logo/图片生成、不自动上传到复刻服务或写复刻附件。已存在的独立授权运行批次及其scope、任务ID和产物不修改、不停止，也不由此每日任务自动续办。

## 4. 飞书结束播报（2026-11-04用户启用）

**用户最新minimal-v4及双渠道要求优先**：飞书和企业微信对外只显示目标日总上新、去重后实际入表、每个竞品“上新N→入表M”（双端合并）和素材表入口。删除“各轮剔除”整个区块及未取得/Playable/存疑/待核的提示行；预设、口径、技术说明也不展示。详细筛选/失败/待核和部分完成状态仍完整保留在内部日账，不能因此伪造成功或丢弃证据。该样式覆盖下方compact-v3/clean-v2。

企业微信为用户2026-11-04明确授权的新通道，凭据只读项目`.env`的`AFIZZY_WECOM_WEBHOOK`，不将完整Webhook或key写入配置、自动化提示、报告、日志或Git。不得复用/改写其他工作流的通用WECOM变量。沿原05:00任务，入表核验后执行`ua_workflows.afizzy_daily_report send`会分别续办飞书和企业微信，不新建定时器；首报目标日2026-11-03。两端独立回执，一端失败不能触发另一端重发。仅发正式日报、不发测试或启动消息、不@全体。

企业微信只发一条markdown，内容绑定最新核验入表回执（有sync-latest则验证其SHA及独立读回），与飞书共享同一份统计；长度超限停止，不静默截断/拆分。发送前持`.afizzy-wecom-report.lock`并落盘payload哈希/脱敏目标指纹/send-intent；HTTP200且errcode=0才记`wecom-report-receipt.json.status=accepted_by_webhook`。Webhook不返回message_id、不提供同等消息读回，不能伪造；未知提交绝不自动重放，已有成功回执只核验，不重复POST。已发企业微信若数据或样式变化，不能擅自再发，需新的明确指令；飞书可按既有授权更新原卡片。调试/异常只保存脱敏响应和异常类型，不打印含key的URL或requests原异常字符串。

**最新精简样式compact-v3优先**：顶部只显示竞品上新总数与去重后实际入表总数；总览保留横屏、重投、封面日内/跨日、主体、模板前日/同日每轮剔除数，失败/存疑/待核和Playable用一行辅助文字单列。下方每个竞品只用一行“上新N → 入表M”，同竞品双端合并，不再逐竞品展开筛选/去重细项。不显示预设或数据口径说明，所有逐项证据保持内部留存。此条覆盖下文clean-v2的三项总览及竞品详细分段样式。

**2026-11-04格式优化**：对外卡片采用clean-v2，顶部三列突出上新/去重/入表，筛选数字双列排列，竞品按上新数降序独立分段，零值辅助项省略、每轮封面/模板去重数仍保留。仅展示日期、结果数字、失败/存疑/待核和素材表入口；不显示预设、数据口径解释、模型历史覆盖、内部技术标识或复刻说明。所有原口径、范围、哈希和失败证据继续内部保存。用户明确要求改样式时，可只更新已发送的原message_id卡片，保存前版、PATCH意图和成功回执，数据不重算、不另发消息。PATCH成功只记updated_by_api_ack，不冒称受限的消息独立读回已通过；完成旧日报再次运行仅核验原更新回执，不重复PATCH或POST。

沿同一05:00日任务执行，不新增定时器。原素材入表独立核验后，使用`daily_workflow.completion_broadcast`指向的既有`crawl_completion_report`霍振堃测试群，由广告之神应用发一条正式结束日报，再结束。本节优先于旧的“入表后立即结束/不发群”说明；复刻仍不启动。当前授权包含今天已完成的2026-11-03批次首报；其他已交付历史批次不补发、不重算。无启动通知、无@全体、无额外测试消息。播报只读统计和发送消息，不重新采集、下载、审核、上传附件、写业务表或启动复刻。

```sh
.venv/bin/python -m ua_workflows.afizzy_daily_report prepare --run-dir data/afizzy_daily/D
.venv/bin/python -m ua_workflows.afizzy_daily_report send --run-dir data/afizzy_daily/D
```

D替换为固定目标日。prepare从source-ledger、真实横屏/重投/CLIP/内容/模板逐项证据和selection派生互斥处理状态，核对原scope/selection哈希及独立入表回执，不把报告手填总数当证据。生成`report/payload.json`和逐创意状态。顶部列上新总数、实际取得/未取得、横屏/重投、封面日内/前7自然日、主体筛除、前日/同日模板去重、存疑及前日模板待核、Playable、实际入表数；下方逐竞品列相同明细，明细求和必须与总数一致。

上新单位为本次预设下可见且首次发现日=D的广告记录，包含下载失败/缺址及Playable；日期外截止证据不计上新。成功取得文件、失败、Playable、各轮剔除、存疑/待核及入表相互区分，不把未执行步骤记0。只能按真实结果说明范围完整或部分交付，不称全站全量。素材身份重复/日期未知/阶段证据不齐时停止准备并报告具体原因，不伪造日报完成。

`config/afizzy_report_products.json`的明确别名仅用于播报将同竞品iOS/Android合并为一段；不改变原App/系统去重范围、前日模板身份、表广告主或旧scope。未知名称保留原可见名，不模糊猜测合并；未观察到的产品不凭旧名单报告全站0。所有门槛保持原scope。

send先核对既有群ID/名称及group类型，持`.afizzy-report.lock`，先落盘确定性UUID和send-intent，再提交一次；先保存message_id，再独立GET核对群、内容、各数字及无mention。读回通过记`report-receipt.json.status=verified`。若仅因现有应用缺少`im:message:readonly/im:message`返回99991672，不扩大权限、不换身份，改用已保存的发送接口服务端规范化正文逐行核对同样内容、群和message_id，记`verified_by_send_response`、`readback_status=permission_denied`并如实说明“发送回执核验、独立读回受限”；不能称独立读回成功。已知该状态再次执行仅本地核验原发送回执，不反复请求被拒接口。其他未知提交/读回异常仍留断点，不重放POST；存在旧日报回执先核对，禁止双发。同批再次触发只核验原回执，不另发一版。发送卡片、原始响应/读回或权限错误及SHA256均留在原日目录，不输出凭据。

## 结束与恢复

每日任务完成以采集范围证据、筛选/前一天模板证据和原素材入库独立读回为准，不等待复刻。未进行复刻如实标明“未启动，等待用户指令”，不标为复刻失败。不能用ZIP存在或审核准备好当入表完成；失败或待审核沿原目录恢复，禁止重建scope绕过。无合格项以真实审核证据记不适用。

不发启动消息或@全体；仅按上述2026-11-04新授权向既有群发送唯一结束日报，不增加其他群消息。在当前聊天报告目标日、卡片/媒体/失败、各道筛选数量、原入表数、日报message_id、精选ZIP及证据路径；复刻仅标明未自动启动。用户要求每阶段在当前聊天先说明工具、输入范围、具体操作及验收依据，结束报实际数量/失败/待处理和证据；进入下一阶段前继续说明，不逐卡刷屏。无变化的重复核验或已完成且日报已核验批次不重复通知。小红书原00:00与抖音暂停状态不变，不补跑旧历史批次，也不自动接管其他聊天的357条旧复刻队列。
