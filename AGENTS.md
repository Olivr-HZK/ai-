# Agent 工作日志

## 2026-10-22 [广大大] 直采脚本与整App验收正式接入skill

- 新增skill内可复用`direct_session.mjs`及项目`social_chat_direct`验收器；新prepare固定`direct-browser-v1`，21项App由单个APP_COLLECTION_PENDING逐项验收，连同准备/通知/浏览器及原7项后处理共31个执行步骤，替代旧模式157步。历史scope保留旧图，不迁移或手改完成状态。
- 实际脚本组合只读筛选检查、必要修正、综合App ID真实Enter及最新创意；同页最多10张/45秒、逐卡真实点击及前后截图/哈希、原文身份与日期、分页连续性、截止/零结果/末页证据落盘。记录批次耗时、App验收耗时及状态机尝试/等待；验证码保存断点并hold/释放保护，人工恢复须重新核对查询、筛选及已保存广告ID边界。Heyyyy iOS游戏分类固定进scope。
- 新增`social_chat_direct_media prefetch/review-media`：单批文件锁、最多3路、跨启动保留有限失败预算及哈希缓存；只下载实际观察原地址，拒绝不完整YouTube链接。视频完整解码并真实生成六帧、图片完整解码；不自动生成内容审核决定。重投剔除、实际查看审核、视频图片合并前10%、原飞书附件幂等写入/读回、群日报及去重流程保留。
- 70项相关Python回归通过，覆盖21项完整隔离浏览器执行/真实本地FSM桥、验证码恢复及分页、证据篡改/漏卡拒绝、真实媒体解码/六帧生成、人工审核闸口、前10%及模拟飞书附件读回/群日报防重。14项Node检查通过；项目及已安装两份skill格式校验通过，5个安装文件逐字节同步。顺带修复交付HTML在排除数量为0时的KeyError。
- 本次未重新执行真实批次、未写远端表或发群；外部写入/消息为隔离模拟验证。已有批次临时direct-complete.json不自动充当新验收证据，未声称当前未审核素材已完成后处理。

### [广大大] Skill 默认采用浏览器直采与原素材并行下载

- 按用户要求，将本轮实际采集模式写入两份 skill：综合 App ID真实 Enter后重新点最新创意，组合只读筛选、同页最多10张连续点卡、详情就绪即落盘、按页登记；无需插件弹窗，不读取隐藏React状态或私有接口。清理旧广告主候选和全采完才开始下载的冲突描述。
- 新增 references/direct-collection.md：已观察原素材最多3路下载与后续串行点卡并行；去重、哈希复用、跨轮重试预算、YouTube原始外链识别、真实身份映射、加载完成再盘点、动作超时先读回实际结果、持久闭包显式传当前任务。验证码立即暂停，人工后核对筛选/页码/已存广告ID边界；区分采集、下载、人工等待与恢复耗时。
- 每日统一状态机保留，下载预取不提前完成MEDIA/REVIEW，原始下载清单不冒充审核/排名/入表或完成日报；prepare生成的后续提示同步并行预取规则。同步项目与已安装目录5份skill/引用文件并逐字节核对，未改定时任务、历史批次或发外部消息。
- 本地48项Python回归、13项Node浏览器批次/准备行为测试、两份skill格式校验通过；本次未重新登录、抓取、下载或写表，未发布npm或创建提交。

### [广大大] 综合App ID回车查询替代广告主候选

- 按2026-10-22用户最新要求，后续使用综合搜索→输入App ID→真实Enter提交→最新创意，不再选择广告主候选。更新配置、两份skill、点卡前合并脚本、prepare提示及实际每日07:00自动任务；逐卡实际App ID/平台核验保留，旧候选分支仅兼容历史证据。
- 综合提交校验要求真实回车事件和提交后唯一App ID关键词读回、无广告主/额外关键词；排序须晚于提交。48项Python回归及2项Node行为测试通过；未恢复暂停批次，未重新采集、写表或发群消息。

### [社交聊天竞品] 定时任务自动正常重登与验证码暂停

- 按用户授权更新每日07:00定时任务及两份浏览器skill：定时任务遇登录失效或其他设备登录强制退出，使用已有保存凭据或项目.env既有账号正常重登；手动任务先询问。任何验证码立即保存断点、停止浏览器操作并等待人工，不自动处理。
- prepare新增实际执行来源manual/scheduled，默认manual；恢复保留原来源，不用config默认scheduled推断授权。新增social_chat_login_policy本地证据日志与决策，单批最多2次正常登录尝试；验证码状态须实际人工确认及新页面证据才解除，成功后重新核对筛选、App身份和断点。
- 实际定时任务配置已通过应用工具更新并读回：ACTIVE、每天07:00不变。55项针对性Python测试及两份skill格式校验通过；本次未触发真实登录、采集、写表或群消息。

## 2026-10-22

### [提交检查] 社交日采集可靠性修复

- 此次提交仅包含小红书/抖音持久化异步搜索、媒体续传、电源预检、依赖的最新入选规则及对应测试/文档；广大大浏览器相关未提交改动与本地采集数据、媒体、凭据均不纳入。
- 提交前严格离线入口再次342项通过；真实测试边界如上，抖音空结果原因未确定，不宣称本次新素材完整链路通过。未推送远端。


### [小红书/抖音] 乙女向真实异步及跨进程恢复验收

- 按用户限定，各提交1次“乙女向”最近24小时异步搜索；小红书只取第一页、不跟随游标。获取job_id和落盘提交回执后主动退出提交进程，由新PID仅GET恢复；两平台job_id/回执SHA256均一致，已完成任务在禁止网络session下再次读取命中缓存。搜索POST共2次、付费详情0次。
- 小红书20条均在请求前24小时内，下载本次结果1条720×1280视频（1,203,236字节），64KB处主动停止后由新PID按相同实体续传，19段校验及FFmpeg完整解码通过。
- 抖音本次成功状态但返回0条，供应商标记refunded、净收费0；不能声称本次搜索媒体链路验收成功。仅使用既有“乙女向”历史样本的原图地址做真实下载，1440×1440、67,174字节，64KB断点跨进程续传及Pillow解码通过，明确不是本次搜索结果。两个媒体完成后均在禁止GET条件下读取缓存、哈希一致。
- 实际搜索净费用0.005556 USD；证据 `data/social_async_real_test_20261022_142950/acceptance.json` 及各平台search/media目录。未写表、未发群、未修改定时任务或重跑日批次。本次仅新增本地验收产物与日志，未改生产源码。

### [小红书/抖音] 日搜索持久化异步、媒体续传和电源预检

- 新增everyinfra_jobs/social_search：按页或关键词保存实际异步POST意图、job_id、状态、原始轮询和成功结果，fsync+原子JSON+文件锁防重复；提交不确定禁止重发，pending/重启只GET原job；未知旧版请求不自动重调。每日run和搜索脚本接入，恢复日期使用实际提交时刻，异步结果保留原结构并由排名器核验；小红书原有游标链、抖音limit50与最新入选数量规则保留。
- 新增resumable_media：两平台图片/视频64KB→8KB自适应范围下载、有限代理/直连切换、源及远端实体绑定、分段哈希、崩溃尾部清理、完整后原子落盘，原图/ffprobe/FFmpeg验证保留。抖音搜索/缓存详情优先尝试真实download_url/bitrates，避免盲目重复付费详情。旧无验证断点不混拼，源变化保留旧文件，重定向逐跳核验CDN，512MB限制。
- collection_runtime新增macOS接电及所属进程系统防睡眠断言预检，新付费请求前掉电即停，已有job可仅查询；不改系统电源配置。审核等待与异步等待、电源拦截、代理/传输错误分开记录。配置与两平台文档更新，未更改定时任务时间、重新抓取、写飞书或发群；未创建commit。
- 342项本地回归通过，最终在禁止外网连接的环境验证，含实际本地HTTP异步契约、重启/并发/不确定提交防重、两平台完整搜索证据、断点续传/远端变更/短读/范围错误/代理切换、掉电拦截及手动收录兼容。没有使用真实供应商密钥运行测试。
- 固定离线入口scripts/test_social_daily_offline.py仅允许连接本测试进程绑定的本地端口，阻断外网DNS/连接及本机现有代理，并在运行前自检；首轮发现旧用例只mock了requests.Session而未拦截requests.post，曾以假密钥尝试外连，已修正。最终342项通过该严格离线入口，未用真实密钥调用供应商，也未真实采集或改远端数据。

### [抖音/小红书] 小样本点赞前10篇

- 两平台共用social_selection入选数量规则：现有筛选和跨关键词去重后N<100取min(10,N)，N>=100取ceil(N/10)；同赞顺序及缺失数据/内容审核闸口保留。selection.json记录实际模式和规则版本，日报动态显示前10篇或前10%，小样本不再标10%。配置与文档同步，既有已完成批次不自动重算或重发。
- 两条原有00:00自动任务（24/24-2）已通过应用工具更新并读回，ACTIVE、运行时间和目标聊天保持原值，提示词同步新阈值规则，无重复任务。禁止外网连接的68项针对性回归通过，覆盖0/1/7/9/10/11/99/100/101/111篇、去重/内容筛选后分母、点赞排序、日报两分支和原安全闸口；未调用付费接口、下载、改表或发群。


## 2026-10-21 当前更新提交检查点

- 按用户要求提交当前累计源码、配置、测试和文档更新，涵盖三平台每日采集、链接补全、Chrome插件、skill/CLI分发、素材港及已有VE/Arrow2改动，并保留已有API示例文档删除。此次未开始架构重构、迁移目录或调整定时任务。
- 不纳入本地采集数据、媒体、报告产物、安装ZIP、数据库、凭据、浏览器临时文件和本地托管状态；这些文件仍留在本机。提交候选已做常见凭据模式及本地环境敏感值匹配检查，未发现匹配。
- 修正requirements.txt中yt-dlp版本与下一条说明注释粘连的格式，依赖语法验证通过。101个Python文件语法、候选JSON及git diff --check通过。
- 选定Python回归初次583通过、1失败（跨目录CLI检查因当前环境未安装包而找不到模块）；在临时目录离线构建安装后，CLI包44项全部通过，不修改正在使用的虚拟环境。插件/浏览器检查16项Node测试、skill安装器8项通过；未执行真实付费采集、写表或发消息，也未重新做浏览器实站验收。

## 2026-10-21

### [广大大] 插件说明顶部版本下载表

- 按用户最终要求，在原飞书说明最上方加入「版本号 / 更新描述 / 安装包」三列表格，倒序列出v1.0.0～v1.8.0共9版；原安装步骤、用户截图、常见问题、Playable详细说明及原附件保留，简介改为指向顶部版本表。未改插件功能、分享权限或发送通知。
- 核验6个真实历史安装包的manifest版本、ZIP CRC和SHA256：1.0.0、1.1.0、1.3.0、1.4.0、1.7.0、1.8.0；逐个上传至对应版本行。1.3.0从未覆盖的原1.3目录补打ZIP（15800字节）。1.2.0、1.5.0、1.6.0原目录已被新版覆盖，未找到历史原包，表格明确标为「历史原包未留存（原目录已更新）」，不把新版伪标为旧版。
- 本地核验索引 `data/guangdada-extension-release-index.json`；上传暂存 `data/guangdada-extension-release-upload/` 仅6个已核验ZIP。飞书显示保存后刷新，顶部表格9行、6个对应版本附件文件名及3处缺包说明均可见，原正文简介和安装截图仍在。

### [广大大] Chrome 插件 Playable 原HTML获取 v1.8.0

- 新增Playable类型与默认勾选项；按实际 `ads_type=7` 卡片读取 `resource_urls[].html_url`，兼容该卡片顶层 `html_url` 和明确HTML的 `cdn_url`。当前页、跨页点选、指定页数/采到末页均支持；去重/页码编号/单ZIP/512MB/120秒边界保留。文件保存为 `媒体/第XXXX页/playable/原编号_名称.html|htm`，不使用封面/视频/ZIP冒充试玩HTML。
- 新增 `playable.js`，仅静态检查原字节，不执行脚本、不访问依赖。拒绝JSON、非HTML/静态错误页、登录或授权页；MD报告逐项记录Playable类型、缺址、HTTP/登录/内容错误及成功项的外部引用提示。原HTML字节保留，外部JS/图像/音频/动态资源不打包，不保证离线或本地直接运行；未宣称完整离线归档，不支持原ZIP试玩包。
- 15项Node测试及4份隔离Chromium集成通过，覆盖图片/视频原回归、Playable实际弹窗、单页/跨页/自动末页、原始字节/CRC、html_url/cdn回退、登录/非HTML/403/404/缺址/类型筛选/跨页去重及不执行脚本/不抓外部依赖。真实CDN样本5500字节返回200，通过本地测试服务器原样供给扩展并核验ZIP字节；证据 `data/guangdada-playable-acceptance/acceptance.json` 和弹窗截图。不是用户登录实站验收；用户Chrome当前提示“其他设备登录，强制退出”，未重新登录或改其筛选/清单。
- 交付 `data/guangdada-media-extension-1.8.0/` 与安装ZIP `data/guangdada-media-extension-1.8.0.zip`（41444字节、19文件、CRC/manifest核验）；1.2.0/1.4.0/1.5.0/1.6.0/1.7.0旧目录19文件同步为1.8.0并逐字节核对。用户需安装/重新加载新版，未声称已重载其当前插件。
- 原飞书说明 `https://scnmrtumk0zm.feishu.cn/wiki/PZ3Uweg2kiJTHJkj3BHcjVBKnpb` 已发现用户自行精简并添加截图和1.7附件，故只追加第四节Playable说明与1.8安装包，更新标题/适用版本/简介为1.8；原三节、截图和旧附件保留，明确旧1.7附件不含新功能。重新加载后读回标题/版本/新章节和40.47KB新版附件，无分享权限变更或发送消息。

### [广大大] Chrome 插件 Markdown 下载报告 v1.7.0

- 已按用户要求通过当前已登录的Chrome飞书账号，在「我的文档库」新建《广大大媒体下载插件｜使用说明（v1.7.0）》：`https://scnmrtumk0zm.feishu.cn/wiki/PZ3Uweg2kiJTHJkj3BHcjVBKnpb`。涵盖安装更新、三种模式、跨页点选示例、指定页数/末页、授权进度、ZIP路径、MD报告和失败处理。云端保存后重新加载并读回章节/跨页步骤/报告说明，检查排版；未改变分享权限或发送通知。
- 按用户要求，ZIP 内只附一份 `下载报告.md`，替换原JSON报告。新增 `report.js`，在去重/类型过滤前保留所有选择，报告包含原页码/从左到右卡片编号、广告ID、类型、文件、结果、原因和建议；「失败及未完成内容」「全部选择」两表逐项对账，重复项说明合并目标及对应文件结果，类型未选不计为下载失败。
- 下载失败区分HTTP 401/403/404/410/429/5xx、网络未得到响应、响应后传输中断、120秒超时、512MB限制、HTML/JSON响应、空内容。403等仅陈述服务器证据及可能原因；试玩、HLS/DASH/流片段、blob等非HTTP地址明确不支持，缺失/无效原地址、未读取到卡片数据分别说明且不发请求，不把所有失败统称不支持。
- 后台持久保存逐项状态/错误代码/响应状态/字节数；弹窗「查看下载报告.md」打开最近一批纯文本Markdown报告，可按需单独保存。全失败或无可下载原址仍保存报告，不生成空ZIP；停止/翻页异常报告未执行和范围不完整，下一批会替换最近报告。原三并发、单ZIP、跨页选择及翻页功能保留，报告不含签名URL/凭据，Markdown单元格转义。
- 12项Node测试、三份隔离Chromium集成通过；13卡混合批次=成功1文件+请求失败6+重复1+缺址2+不支持3，实际覆盖403/404/429、HTML/空/网络失败与HLS/blob/无效URL，ZIP仅媒体+一份MD、CRC通过；全失败报告查看、全不支持弹窗按钮打开并实际保存MD验证通过，原视频完整解码/图片字节/60卡/自动翻页/跨页点选回归通过。
- 交付 `data/guangdada-media-extension-1.7.0/`，1.2.0/1.4.0/1.5.0/1.6.0旧安装目录18个文件同步并逐字节核对。用户Chrome管理页仍显示1.6.0；用户正在切换其他页面，本次未完成手动重载，需在扩展页点「重新加载」使1.7.0生效。未改动用户当前清单或筛选，未声称本次新报告已做实站下载验收；旧批次报告明确提示无法还原所有合并前选择。

### [广大大] Chrome 插件跨页点选与采到末页 v1.6.0

- 新增 `selection.js` / `selection-ui.js`：隔离世界点选界面通过扩展消息通知后台，后台独立读取当前卡片并校验页码/卡片身份，不接受页面提供的媒体URL。清单按标签页持久化、以广告ID关联；手动翻页自动显示勾选，回页和刷新恢复，重复素材同步选中，已选原地址按可见页更新但保留首次选择页码/编号。不同标签页隔离，关闭该标签页清理清单。
- 弹窗新增「跨页已选清单」及逐页编号/总数，支持继续点选后合并为一个ZIP；页面提供全选本页、清空本页、清空全部、收起点选。清空本页保留其他页，收起保留清单；保存中拦截翻页、过期清单版本/空选拒绝提交，避免默默下载整页。
- 自动翻页支持手动输入1～1000页（含当前页）或「一直采到末页」；后者无填写页数上限，仍以网站下一页明确禁用为结束证据。原512MB媒体/120秒单资源边界保留，未知分页/超时/跳页仍明确失败，不拆多ZIP。
- 修正首次站点授权可能关闭弹窗导致未启动的问题：先交后台持久化任务，再请求当前实际CDN权限；`permissions.onAdded` 和状态对账在授权后后台继续。点选器重载失效时自动退出，加载提示在页面恢复后清除，避免展示旧错误。
- 8项Node测试和三份隔离Chromium集成（单页原逻辑、直到末页/指定页数、跨页点选）通过。实际点击第1页1+2、第2页1后单ZIP仅2视频+1图，原字节/CRC/完整解码通过；覆盖异步加载、回页/刷新恢复、重复同步、标签页隔离、清空本页/全部/收起、保存中翻页、空清单和旧版本拦截。
- 已在用户Chrome实际重载1.6.0（扩展ID `edakfmeobpckdecabcindhomamhjgoan`），在独立验证标签页选第1页1、4和第2页1，插件重载后清单仍为两页3条；实际单ZIP 1763689字节、1视频+2图片成功3/3、失败0，视频完整解码/图片/CRC通过。证据 `data/guangdada-extension-cross-page-selection-real-acceptance.json`；ZIP `/Users/oliver/Downloads/广大大/广大大媒体_2026-10-21T05-47-52-259Z.zip`。自动采到末页已做隔离3页验收，未在用户默认100页结果上执行全量采集。
- 交付 `data/guangdada-media-extension-1.6.0/`；1.2.0、1.4.0、1.5.0旧交付/安装目录内容同步为1.6.0。未修改用户原来的Heyyyy试玩筛选标签页。

### [广大大] Chrome 插件自动翻页采集 v1.5.0

- 新增「采集范围：自动翻页（每页全部）」和 1～20 页上限（含当前页，默认3页）。通过当前列表 Ant 分页按钮逐页前进，核对页码递增、卡片身份、加载和媒体地址稳定性；对缺原址卡片滚动触发加载并合并页面快照。当前页点击挑选保留，翻页模式明确获取每页全部所选类型；末页提前停止，不回到第一页、不调用私有接口。
- `pagination.js` 与后台持久化采集状态新增页数进度、关闭弹窗后继续、停止、30秒下一页/60秒本页就绪超时、页码跳跃/整页重复/离开列表拦截。跨页按原URL及广告ID去重，ZIP按实际页码和原卡片编号归档；只调用一次Chrome下载，报告逐页对账。新CDN仅在实际出现时暂停等待点击授权，翻页未完成不交付不完整ZIP或允许重新打包伪装完整结果。
- 6项Node测试、单页浏览器回归及新增 `tests/pagination_smoke.py` 通过。真实扩展弹窗启动第4～6页异步采集、滚动后才提供的原地址、单ZIP原字节/CRC/视频解码、跨页去重、缺失报告、末页/页数上限、新来源授权暂停、取消、超时/跳页/未知分页和非法页数均覆盖。未声称自动化验证Service Worker真实休眠恢复。
- 用户Chrome当前插件已原位更新并实际重新加载至1.5.0。实站旧登录失效时采集正确停止、未出包；使用Chrome已保存登录恢复后，实际完成第1～2页120卡：45试玩等不支持、2视频无原址、14跨卡/页重复，最终47视频+12图片，成功59/59、失败0，单ZIP 156828920字节。全部47视频ffprobe及ffmpeg完整解码、12图片Pillow和ZIP CRC通过；证据 `data/guangdada-extension-pagination-real-acceptance.json`，产物 `/Users/oliver/Downloads/广大大/广大大媒体_2026-10-21T04-48-18-450Z.zip`。
- 交付 `data/guangdada-media-extension-1.5.0/`；当前安装目录 `data/guangdada-media-extension-1.4.0/` 和更早原安装目录 `data/guangdada-media-extension-1.2.0/` 同步为1.5.0。512MB批次上限、单资源120秒及浏览器退出后媒体不可续传边界保留。

### [小红书日采集] 媒体传输恢复

- `ua_workflows/xhs_daily.py`：针对代理连接异常、直连超时和响应中断，媒体 CDN GET 增加代理/直连各一次的有限重试；整图核对 Content-Length，分段长度不足触发传输重试，完整后才原子落盘。保留 `.part` 和已有媒体 SHA256 缓存；不重调搜索或付费详情。受信 CDN 按精确域名/子域检查。
- 实测个别视频 CDN 返回正确206头但响应体在约10KB后停滞，curl HTTP/1.1 与 HTTP/2 同样复现；连续8KB精确范围可用。大分段两条路径失败后回退8KB逐段校验，恢复全部视频；未改原URL。诊断证据保存在批次 `download-diagnosis.json`。
- `tests/test_xhs_daily.py`：20 项通过，新增代理失败回退、完整长度不足拒绝入库、分段不足后恢复且不拼入错误字节、大分段停滞后小分段拼接回归。小红书日流程、链接机器人、抖音及分享格式相关合计272项通过。
- 从 `data/xhs_daily/20261021`（报告归属 2026-10-20）已有28页、7词搜索断点恢复，原搜索请求/响应SHA256保持不变，恢复期间付费视频API调用0。全部535篇媒体补齐：272篇图文1516张图片、263条视频均通过图像工具实际查看并保存人物/男性主体证据；图片内容排除170篇，主体排除176篇，合格189篇，统一点赞前10%向上取整19篇。
- 19篇（17视频、2图文）已附媒体写入指定飞书表并读回核验，平台小红书/来源每日最新；广告之神向霍振堃测试群发送一次日报，回执 `om_x100b62b62ebccca8cfd908240a8184d`。本批 `daily-result.json` 与 `delivery-validation.json` 记录最终结果，旧阻塞状态已归档。

本文档记录所有 agent 对项目做出的代码变更与功能更新，供后续 agent 接手时快速了解项目现状。

## 2026-10-21

### [小红书/广大大] 每日采集防睡眠及异常日志

- 小红书日流程、独立逐页搜索、显式授权恢复及媒体审核准备接入 collection_phase；搜索每页每次请求、视频详情沿用单次HTTP诊断，超时不自动重试，已有明确未收费429有限重试不变。下载/审核准备/入表捕获的失败补充脱敏异常链文件路径，保护正常或异常退出自动释放。
- 广大大 Python 准备/校验/入表/通知阶段自动保护和记录异常；新增 `browser_collection_runtime` 本地 start/renew/status/stop/error 入口，独立浏览器步骤用15分钟可续期保护，重复start复用同一进程，过期自动释放、禁止直接renew失效保护。浏览器真实工具错误由执行者提交原始消息/阶段/App/页/卡/证据路径，缺堆栈不得编造。不会调用爬虫或私有API。
- 已更新项目与安装目录的 social-chat-browser-daily skill，以及新生成的 browser-task 提示；每页前和至少每5分钟续期，暂停/人工等待/结束时停止。仅修改仍运行自动任务的原项目，未迁移worktree或改变任务时间/抓取范围。未重新抓取、下载、写表或发群。
- 禁止外网连接的本地回归89项通过，覆盖小红书超时单次调用/详情意图防重调、广大大准备入口/失败留痕、真实macOS保护启动/复用/续期/到期/停止释放，以及原抖音和飞书模拟业务；项目与已安装skill校验通过。临时保护进程全部停止，未声称真实浏览器采集验收。

### [抖音] 采集防睡眠与请求异常诊断

- 新增 `ua_workflows/collection_runtime.py`：macOS `caffeinate -is -w PID`，请求前核验自身防空闲睡眠断言，正常/异常退出释放，保护失败禁止开始付费请求；不更改系统配置，不阻止锁屏/息屏。
- `douyin_daily run`、独立七词搜索与单次搜索/视频详情请求接入；保留原120秒读取超时、断点和禁止自动重试逻辑。请求诊断记录时间/耗时、代理候选、HTTP状态、请求ID及脱敏异常链堆栈，600权限；失败关键词保留原始目录。仅修改当前仍执行自动任务的原项目，未切换新worktree或改定时任务。
- `tests/test_collection_runtime.py` 与抖音/小红书日流程回归共38项通过，含本机真实断言创建/异常释放、保护失败阻止请求、模拟超时单次请求、异常链脱敏、非JSON响应和失败目录保留。本次不调用付费搜索/详情，不下载媒体、不写表或发群。

## 2026-10-20

### [广大大] Chrome 页面媒体下载插件

- v1.4.0：按用户要求新增页面点击选择。插件「在页面点选素材」为当前卡片按视觉从左至右、从上至下显示编号勾选按钮；阻止勾选事件触发详情跳转，提供全选/清空/退出，重新打开插件展示所选编号并只打包选中项。空选拒绝提交，提交核对当前卡片身份/排列，ZIP文件名前缀保留原页编号、报告记录选择范围；先筛选后去重，缺失统计限定选中范围。隔离Chromium实际点击1和3并验证ZIP仅两条素材、无卡片导航，覆盖全选/清空/退出、空选/页面变化拦截；原单ZIP/60卡范围/失败/取消回归及4项Node测试通过。已更新原安装文件夹 `data/guangdada-media-extension-1.2.0/` 内容为1.4.0，另交付 `data/guangdada-media-extension-1.4.0/`。用户当前Chrome已切换至登录窗口，未重载其插件或声称本次点选已做实站验收，需要用户在扩展页点重新加载。
- v1.3.0：用户反馈单页60素材却下载100+图片、没有视频；通过其 Chrome 实际已登录页面导出卡片结构确认当前站点为 React，根因是旧版全 DOM 图片扫描及误用 Vue 提取。改为当前 `.shadow-common-light.bg-white` 卡片各自 React `memoizedProps.value` 的 `ad_key/ads_type/resource_urls`，只取每卡一个主要原素材，不读取父列表、不扫图标/背景/视频封面、不用封面补缺失视频。首次授权缩为实际素材 CDN，不再一次请求所有 HTTP/HTTPS 网站。用户原安装目录 `data/guangdada-media-extension-1.2.0/` 已原位更新并通过 Chrome 重载为1.3.0；另提供 `data/guangdada-media-extension-1.3.0/`。真实页面60卡=52视频卡+6图片卡+2试玩；50视频URL经7重复合并为43视频，2视频无原址另记缺失；实际Chrome单ZIP下载成功49/49、161937724字节，包内43视频+6图片+报告，CRC通过。真实产物位于用户下载目录 `广大大/广大大媒体_2026-10-20T08-44-55-080Z.zip`。43个视频经ffprobe识别及ffmpeg完整解码全部通过、6张图片Pillow校验通过，证据 `data/guangdada-extension-real-acceptance.json`。隔离Chromium新增60卡/180 DOM图片只返回60原素材回归，并覆盖React绑定、父列表排除、缺失/试玩报告、单ZIP/重打包/全失败/取消；Node3项与语法检查通过。
- v1.2.0：按用户澄清，插件运行时只交付一个媒体 ZIP，插件安装仍用文件夹。新增 `offscreen.html/js` 和无第三方依赖 ZIP STORE/UTF-8/CRC32 打包器；三并发 fetch 后只调用一次 Chrome download，包内按图片/视频分目录并附不含签名链接的下载报告。首次按钮点击申请可选 HTTP/HTTPS 来源读取权限用于 CDN 内容获取；512MB 批次上限、120 秒单文件超时、部分失败报告、全失败不出包、停止及完整重新打包。状态区分媒体已获取和 ZIP 已保存，关闭弹窗继续，浏览器退出不支持续传。隔离 Chromium 实测单 ZIP 解包/CRC/中文路径/4 PNG 与3 MP4原字节及解码、404报告、重新打包、全失败/停止不产生额外ZIP通过；3项 Node测试通过。真实广大大页面、真实权限弹窗仍未验收。安装文件夹 `data/guangdada-media-extension-1.2.0/`。
- v1.1.0：针对用户反馈无视频，补充 MAIN world 只读读取 DOM 绑定 Vue 2 响应式 props/data、Vue 3 props/setupState 的视频字段，以及当前文档已请求的视频地址；不调用私有 API，不把普通封面 WebP 直接推测成视频。弹窗分列视频/图片数量，零视频明确提示。隔离 Chromium 模拟未播放卡片实测 3 个 MP4 下载及 ffmpeg 完整解码、4 个 PNG、404 错误统计通过；真实站点尚未验收。交付 `data/guangdada-media-extension-1.1.0.zip`。
- 新增 `apps/guangdada-media-extension/`，Manifest V3、activeTab 按点击授权；当前 guangdada.net 页面图片/视频/封面/背景/懒加载属性/开放 Shadow DOM 收集、URL 去重、可选 45 秒本页滚动与位置恢复，不翻页、不调用私有 API、不读取账号凭据。
- Chrome downloads 后台三并发队列，storage 持久化、下载事件和 alarm 对账，弹窗统计真实完成/失败，支持停止及失败重试。尚未加载地址的视频需要先播放/打开详情；blob/HLS/DASH 不支持，明确提示边界。
- `node --test apps/guangdada-media-extension/tests/*.test.js` 3 项通过；`.venv/bin/python apps/guangdada-media-extension/tests/browser_smoke.py` 隔离 Chromium 加载通过，验证 DOM/背景/Shadow/懒加载/视频地址、去重、4 个真实 PNG 下载字节及 404 失败统计。测试临时副本仅对模拟站点加 host permission 代替无头工具栏授权，正式包不带。真实广大大登录页面、工具栏 activeTab 授权及 worker 休眠恢复尚未实测。

### [素材港] AI 解析切换 Codex CLI / GPT-6 Luna medium

- 按用户明确要求，`codex_intake.py` 固定调用 `codex exec -m gpt-6-luna -c model_reasoning_effort="medium"`，替换无效的 OpenRouter 通道；不读取项目模型 API key，不更换用户指定模型。
- 使用独立临时目录、独立解析指令、stdin 输入、结构化输出、只读 sandbox、关闭 shell/插件/浏览器/hooks/多 agent 等工具、忽略用户配置、ephemeral 和进程组超时清理。结果要求完整回合且无意外工具调用，继续通过原 JSON Schema 与业务闸口。
- 本机 CLI 已登录 ChatGPT，真实解析 Tipsy 双端成功，完整名称、注册表 App ID、Facebook/TikTok、英/韩语、30天、9:16 及三项内容条件均正确。解析引擎随草稿/提交留痕；README 更新为 CLI 配置与账号额度边界。
- 浏览器已实际完成 CLI 调用、字段回填、人工改标题后提交、后台来源/模型读回及取消验收任务；队列为 0，未启动抓取。36 项隔离测试通过，截图 `data/creative_platform/ai-intake-codex-success.png`，当前本机服务仍运行。

### [素材港] 自然语言采集任务与 AI 字段映射

- 新增 `apps/creative-platform/task_intake.py` 和已登录接口 `/api/task-intake`，通过 OpenRouter JSON Schema 解析需求并回填原表单；App ID 仅取注册表，双端分别对应完整名称，歧义、冲突及当前不支持的条件提示补充。
- 原需求、AI 草稿和最终字段分别留痕；草稿限当前用户、24 小时有效，最终条件重新校验后入队。原文本不下发为 Agent 执行指令；解析不启动抓取。新增频率/并发/超时和鉴权失败处理，手动表单保留。
- 增加 AI 解析、草稿权限、HTTP 提交及业务回归测试；当前 30 项隔离测试通过。真实浏览器调用现有 OpenRouter key 返回 401 User not found，已请求管理员更新项目 `.env`，未声称真实模型回填成功；没有新增抓取任务。README 记录模型配置、数据流及验收阻塞。

### [素材港] Guru SSO 认证重构

- `apps/creative-platform/sso.py` / `server.py`：统一接入固定 Guru issuer 的授权码 + PKCE、state/浏览器绑定/nonce、RS256 JWKS 与 claims 校验；HTTP 本地密码、注册、邀请码入口关闭。仅按显式 `(issuer, subject)` 准入和映射管理员，保留原任务归属和全部素材。
- 本机客户端 `guru-creative-platform-local` 已通过 Guru SSO CLI 0.6.0 创建并回读，使用精确 127.0.0.1 回调；运行配置、加密会话密钥及迁移前 SQLite 备份仅保留在忽略的 `data/creative_platform/`。公网仍未接入；未来需独立 HTTPS web-confidential 客户端。
- `manage_access.py`、`sso.example.json`、`oidc-client.local.yaml`、`requirements.txt`、README 与 SSO_ACCEPTANCE：补齐准入维护、依赖、标准登出、恢复和部署边界。浏览器真实验证登录、视频解码、登出、再次登录、任务提交及取消；106 条素材维持 83/4/19。
- 21 项隔离自动化测试通过，覆盖完整 HTTP OIDC 流程、伪造/重放/过期/权限及业务回归。未测试公网或另一真实账号；未接入 Back-Channel Logout，其他应用退出不会实时撤销本站最多 8 小时的会话。未启动新的广大大抓取。

## 2026-10-19

### 2026-10-20 [广大大浏览器] 已核实 App ID 的网站显示名映射

- `skills/guangdada-browser-crawl/scripts/state.py` 允许在显式 scope App ID 完全一致、平台一致时，以 `canonical_app_name` 关联商店名与实际网站显示名；`app.json.app_name` 和点击日志仍保留实际观察名称，未放宽 ID/平台/截图检查。
- 新增身份映射测试覆盖匹配、错误 ID/平台/商店名、空显示名及无 scoped App ID 拒绝；用于 `io.whif` 的商店名与广大大显示名差异，不更改本批 scope 指纹。

### [社交聊天竞品] 小红书关键词接入登记

- 新增 `config/social_chat_xhs_daily.json`，独立登记乙女向、女性向、虚拟男友、猫箱、星野、破次元恋人、梦女向 7 个关键词及图片/视频范围；不使用广大大 App/双端竞品列表。
- 新增 `docs/social-chat-xhs-daily.md`，记录 API 合同、日期与排名口径待确认，跨关键词按笔记 ID 合并去重。当前 `enabled=false` / `pending_api_contract`，未调用供应商、未修改定时任务或远端表。
- 已验证配置 JSON 可解析及 7 个关键词唯一；供应商调用示例尚待用户提供，不猜测接口及展示估值映射。

## 2026-08-06

### [社交聊天竞品] 每广告主每日差异度优先 Top10

- `ua_workflows/video_enhancer/diversity_selection.py`（新增）：在封面日内/历史去重后，按同广告主执行每日 Top10 精选。封面 CLIP 差异度权重 90%，展示估值权重 10%；首条按对同组素材的平均距离选择，后续按与已选集合的最小距离贪心补齐，缺失向量的素材只作为曝光排序兜底。
- `ua_workflows/video_enhancer/pipeline.py`：新增 `--per-advertiser-daily-limit`，精选前的完整候选保留在 `creative_library` 供历史去重，日快照、分析和飞书只处理入选项；社交聊天工作流固定传 10，其他 VE 工作流默认 0、不受影响。
- 2026-08-04 / 2026-08-05 已从封面去重后的 225 / 302 条分别精选为 56 / 54 条；所有产品均不超过 10 条，Tipsy 两日均为 10 条。独立库 `data/afizzy_competitor_pipeline.db` 的 `daily_creative_insights` 已裁为 110 条，完整 527 条仍保留在 `creative_library`；操作前备份为 `data/local_db_backups/afizzy_competitor_pipeline.before_daily_top10_20260806.db`。
- 飞书写入包已生成到 `data/feishu_social_chat_daily_top10_pending/`，使用项目 `.env` 对应的 `ua-materials-env` bot 身份；目标应用当前缺 `base:field:create` / `base:record:read`（并需 `base:record:create`）权限，授权前未写入远端表。
- `tests/test_social_chat_diversity_selection.py` / `tests/test_social_chat_daily_workflow.py`：覆盖每广告主上限、差异度优先于曝光、缺失向量兜底和 wrapper 固定传 Top10。

## 2026-06-17

### [广大大] 轻量可安装爬取包入口

- 新增根目录 `pyproject.toml`，项目包名 `guangdada-crawl`，安装后提供 `guangdada` CLI；当前包只暴露爬取入口，不默认触发 VE / Arrow2 / 多维表同步 / LLM 分析 / 看板等业务下游。
- 新增 `guangdada_crawl/`：`jobs.py` 统一把每次爬取落到 `~/.guangdada-crawl/jobs/{job_id}/manifest.json|raw.json|summary.json|run.log`；`cli.py` 提供 `guangdada crawl chart`、`guangdada crawl advertiser`、`guangdada crawl ad` 和 `guangdada job show`；`adapters.py` 薄封装现有 `ua_workflows/shared/guangdada` 爬取内核。
- `guangdada_crawl/auth.py` / `cli.py`：新增 `guangdada auth status` 与 `guangdada auth login --headed`；登录态默认写到 `~/.guangdada-crawl/auth/state.json`，chart 的 `auth-state` 模式会使用该路径。`auth login --dry-run` 可用于同事 Codex 先检查路径与参数，不打开浏览器。
- `guangdada_crawl/doctor.py` / `cli.py`：新增 `guangdada --json doctor`，可从任意 cwd 输出包版本、home/jobs/auth 路径、凭据/登录态状态、Playwright/共享爬虫核心/MCP optional extra 可用性，以及是否已具备真实浏览器爬取条件；无登录态或未安装 MCP extra 时返回 `status=setup_needed` 但命令本身仍成功，适合同事 Codex 作为第一步环境诊断。
- `guangdada crawl ad` 已支持 `--search-flag` / `--app-type` / 从广大大 detail URL 自动解析 `id/type/search_flag`；有 `search_flag` 时会尝试用本机 auth state 拉 `detail-v2` 并写入 `raw.json.items[0].detail_v2`，没有时只写 detail URL seed 且 `summary.json.detail_v2_status=skipped_missing_search_flag`。
- `guangdada job list --limit N` / `guangdada job artifacts JOB_ID`：新增最近任务发现与 artifact 路径查询。`job list` 会返回最近 job 的 `kind/status/summary/raw_path/log_path`，`job artifacts` 会返回 `manifest/summary/raw/log` 的路径、存在性与大小，便于同事 Codex 不知道 job_id 时先发现再读取结果。
- 新增 `guangdada_crawl/mcp_tools.py` / `mcp_server.py` 与 optional extra `.[mcp]` 下的 `guangdada-mcp` 入口；MCP 层只注册 `auth_status`、`start_chart_crawl`、`start_advertiser_crawl`、`start_ad_crawl`、`list_recent_jobs`、`get_job_status`、`get_job_artifacts` 等工具，内部仍调用 `guangdada` CLI 并解析 JSON，不把 Playwright 爬取逻辑搬进 MCP，也不触发 VE/Arrow2/飞书等下游。
- `guangdada crawl advertiser` 新增 `--date-mode active|first-seen`，默认 `first-seen` 表示抓 UTC+8 目标日首次出现的素材；不要因 `resume_advertising_flag` / 重投标记过滤素材，只要 `first_seen` 命中目标日就保留。需要“某日仍在投放”时显式传 `--date-mode active`。MCP `start_advertiser_crawl` 同步支持 `date_mode`；wrapper 现在能从“进度日志 + 最后 JSON”的 stdout 中提取最后 JSON，避免真实爬取成功但 MCP 外层误报 failed。旧 job `20260617T082403Z_advertiser_95055161` 的 PixVerse 2026-06-17 first-seen 口径仍为 3 条。
- 新增完整 Codex plugin 结构：`.codex-plugin/plugin.json`、`.mcp.json`、`skills/guangdada-crawl/SKILL.md`。plugin manifest 暴露 repo-local skill 与 `guangdada-crawl` MCP server；`.mcp.json` 通过 `python3 -m guangdada_crawl.mcp_server` 启动 MCP，仍依赖同一个轻量 CLI/job artifact 边界。
- `guangdada_crawl/examples.py` / `cli.py`：新增 `guangdada --json examples`，机器可读返回 first steps、常用 crawl/job 命令、MCP tool 名称和安全边界，便于其他同事 Codex 在不知道用法时先自发现。
- 新增 `docs/guangdada-crawl-package.md` 和 `codex/skills/guangdada-crawl/SKILL.md`，给同事 Codex 使用时明确边界：默认只产出 raw/job artifact，不写 VE/Arrow2/飞书，多维表和验证码必须人工处理，不做破解。
- 新增 `tests/test_guangdada_crawl_package.py`，覆盖安装元数据、plugin manifest/MCP/skill、job artifact、CLI dry-run、auth status/login dry-run、doctor from non-repo cwd、examples、job show/list/artifacts、ad detail seed、ad detail-v2 fake-run、chart/advertiser adapter fake-run、MCP CLI wrapper 参数/JSON 解析/失败返回/doctor/examples，以及文档/skill 安全边界。已验证 `.venv/bin/python -m pip install -e . --no-deps`、`.venv/bin/guangdada --help`、`guangdada --json doctor` from `/tmp`、`guangdada --json examples`、auth dry-run、chart/ad dry-run、job list/artifacts、MCP wrapper chart dry-run、MCP wrapper doctor/examples、`.venv/bin/python -m compileall guangdada_crawl`、`.venv/bin/python /Users/oliver/.codex/skills/.system/plugin-creator/scripts/validate_plugin.py /Users/oliver/guru/ua素材` 与该测试文件通过。系统 `python3` 跑 validator 会因缺 `yaml` 失败，项目 `.venv` 可通过。

## 2026-06-15

### [VE] 3星及以上素材模板复刻点击触发入口

- `浩鹏评分` / `尉蓝评分` 已从 1星/2星/3星单选字段改为飞书 number rating 星级评分字段（star，1～5 颗星）；字段 ID 保持 `fldjNfzzoG` / `fldGfV16uF`，代码仍兼容旧文本值解析。
- `ua_workflows/video_enhancer/template_trigger.py` / `template_trigger_server.py`（新增）：3 星及以上素材可通过 HTTP 点击触发生成 `data/ve_template_copy_jobs/{job_id}.json`，任务中包含来源记录、图片/视频模板初判和 `aigc-template-copy` skill handoff 信息；按钮传 `record_id` 时走单条记录读取，低于 3 星记录返回 `not_top_rating`，避免误触发。
- `scripts/run_ve_template_trigger_server.py`（新增）：本机触发服务入口，默认读取 `VIDEO_ENHANCER_BITABLE_URL`，支持 `/trigger?record_id=...` 或 POST `/trigger`；可用 `VE_TEMPLATE_TRIGGER_TOKEN` 加简单 token 校验。
- `scripts/run_ve_template_trigger_links.py`（新增）：把目标日 3 星及以上素材的 `模板复刻触发链接` 写回主表，并把 `模板复刻状态` 置为 `待触发`；默认链接到本机触发服务，也可传公网/内网 `--trigger-url`。
- `ua_workflows/video_enhancer/template_copy_worker.py` / `scripts/run_ve_template_copy_worker.py`（新增）：本地 worker 可消费 `data/ve_template_copy_jobs/{job_id}.json`，下载/引用源素材、从 `data/template_model_refs/` 选默认模特图，生成 `codex_prompt.txt`、`params.image-editing.json`、`params.image-to-video.json`；默认 dry-run 不调用 Codex、不创建 Video Lab 任务，显式 `--execute-codex` 才拉起本地 Codex。
- `ua_workflows/video_enhancer/template_trigger_server.py`：新增 `--auto-prepare` / `VE_TEMPLATE_COPY_AUTO_PREPARE`，点击触发后可自动进入 worker 准备态；当前本机临时服务在 `screen` session `ve-template-trigger` 中运行，已开启 `--auto-prepare --auto-execute-codex`。Video Lab CLI 已安装并登录，`preset_image_editing` 与 `preset_image_to_video_single` payload dry-run 均通过。
- `ua_workflows/video_enhancer/template_copy_worker.py`：`--execute-codex` 现在会真正调用本地 `codex exec` 跑 `$aigc-template-copy`，解析最终消息里的 `VE_TEMPLATE_COPY_RESULT_JSON`，并写回 job 的 `completed` / `quality_failed` / `blocked` / `codex_failed` 状态；skill 只有输出 `status=completed` 且 `quality_verdict=passed` 才会被映射为多维表 `已完成`，低于 90 或质量不达标统一写 `失败`。2026-06-17 增加 `--codex-model` / `VE_TEMPLATE_COPY_CODEX_MODEL`，当前临时服务固定 `gpt-5.5`，用于规避默认模型 `Selected model is at capacity`；代码兼容把口语写法 `gpt5.5` 归一成 `gpt-5.5`。
- `ua_workflows/video_enhancer/template_trigger_server.py`：新增 `--auto-execute-codex` / `VE_TEMPLATE_COPY_AUTO_EXECUTE_CODEX`，点击触发后可从准备态继续进入本地 Codex skill 执行层，并根据结构化结果回写 `模板复刻状态`；测试可用 `VE_TEMPLATE_COPY_CODEX_BIN` 指向 fake codex，避免消耗 Video Lab credits。
- `ua_workflows/video_enhancer/template_trigger.py` / `template_trigger_server.py`：触发成功后会把源记录的 `模板复刻状态` 写为 `已提交` 并回填 `模板复刻任务ID`；worker 准备失败时写 `失败`。飞书 URL 字段写入必须使用 `{"text": "...", "link": "..."}` 结构，不能直接传裸 URL 字符串。
- `scripts/run_ve_template_trigger_links.py` / `ua_workflows/video_enhancer/template_trigger.py`：新增 `--include-all-records --link-only` 口径，可给目标日期所有记录预写 `模板复刻触发链接`，不修改状态/任务 ID；点击时服务端仍重新读取记录并校验 3 星及以上，低分或未评分会返回 `not_top_rating`。用于避免“打完星但行里没有链接”的体验问题。
- `ua_workflows/video_enhancer/template_trigger.py` / `template_trigger_server.py` / `scripts/upsert_ve_template_rating_link_workflow.py`：新增评分变更自动写链接链路。Base Workflow 使用 `SetRecordTrigger` 监听 `浩鹏评分` / `尉蓝评分 >= 3`，POST 到 `${PUBLIC_URL}/ensure-link`；服务端只给当前记录写 `模板复刻触发链接`，不会创建 job、不会调用 Codex 或 Video Lab。2026-06-17 因 Quick Tunnel 变更，旧 Workflow `wkf41pFBFURE4nX2` / `wkfeSi3DxnYRdjdD` 已禁用；当前备份表 `tblF7kz1DuncOpc5` 启用 `wkfg0v9Ry1jFKSAe`（浩鹏评分）与 `wkfGUdzKkgFThiUI`（尉蓝评分），指向 `data/ve_template_trigger_public_url.txt` 当前 URL。
- 已用备份表 `tblF7kz1DuncOpc5` / 视图 `vewd67ZK4J` 实测：给记录 `recvmEuOFWXMdQ` 临时写入 `浩鹏评分=3` 后，通过 Cloudflare 公网触发链接点击生成 job `2026-06-15_671ab51710dacabec2ba686f26b812e7_1b01f235`，本地状态为 `prepared`；随后真实调用 Video Lab 跑通 GPT Image 2 首帧图和 Vidu Q3 Turbo 图生视频，任务 ID 分别为 `a653fe5c-7dbc-4311-b150-4c04962751e7` / `8b1bb5bb-43b5-4527-b0ad-130377b61dfd`，实际消耗 47 credits。但该样本质量不达标：模特图是近景自拍，强行生成全身跳舞模板导致身份与姿态迁移不稳定；已把本地 `real_run_summary.json` 标为 `quality_verdict=failed`，并将表内状态回写为 `失败`。
- `scripts/build_ve_template_workflow_payload.py`（新增）：生成飞书 Base `ButtonTrigger -> HTTPClientAction` Workflow JSON；按钮 Workflow 需要一个飞书服务器能访问到的触发 URL，本机 `127.0.0.1` 仅适合浏览器直接点链接触发。
- `scripts/upsert_ve_template_button_workflow.py`（新增）：按当前 tunnel URL 创建或更新同名 Base Workflow，并可启用；临时 Cloudflare Quick Tunnel URL 改变时用它替换 Workflow 里的 `/trigger` 地址。
- `docs/ve-template-trigger-deployment.md`（新增）：记录本机临时 Quick Tunnel 部署、Workflow URL 替换命令，以及后续 Mac mini 常驻部署步骤。
- `ua_workflows/video_enhancer/sync.py`：主表字段定义补充「模板复刻状态」「模板复刻任务ID」「模板复刻触发链接」，用于后续把点击触发状态写回多维表。
- `tests/test_ve_template_copy_worker.py` / `tests/test_ve_template_trigger.py`（新增）：覆盖 3 星及以上记录触发落盘、低于 3 星拦截、Workflow payload 会 POST 当前记录 ID、fake codex 结构化成功/质量失败解析，以及服务端 `--auto-execute-codex` 状态回写。

## 2026-06-12

### [VE] 视频内容大模型去重与业务硬拦并入主流程

- `ua_workflows/video_enhancer/video_content_llm_filter.py`（新增）：把 6.11 看板验证过的「广大大视频内容/素材脚本分析」筛选逻辑抽成生产模块。按同广告主分组，只比较目标日前历史素材与目标日已处理素材，避免 6.8 与 6.9/6.10 这类未来日期互比；业务硬拦优先于去重，命中后写 `exclude_from_bitable` / `exclude_from_cluster` 与素材标签。
- `ua_workflows/video_enhancer/video_content_minimal_analysis.py`（新增）：用视频内容文本生成主表同步够用的极简字段，只给未被 LLM 去重/业务硬拦排除的素材调用文本模型生成 `analysis` 与 `effect_one_liner`；已排除素材写本地筛选摘要，不再浪费后续模型。
- `ua_workflows/video_enhancer/pipeline.py`：主流程在封面去重后补全视频内容，随后先执行 LLM 视频内容去重/业务硬拦，再把极简 analysis 注入 `existing_analysis`，默认跳过旧多模态分析队列（`VE_VIDEO_CONTENT_TEXT_ONLY_FLOW_ENABLED=1`）；如需回退旧多模态，可设 `VE_VIDEO_CONTENT_TEXT_ONLY_FLOW_ENABLED=0`。报告写入 `data/workflow_video_enhancer_{date}_llm_video_content_filter.json` 与 `data/workflow_video_enhancer_{date}_video_content_minimal_analysis.json`。
- `ua_workflows/video_enhancer/pipeline.py` / `scripts/cron_ai_video_enhancer_daily.sh`：浩鹏 TopN 二次筛选推送暂时默认关闭；cron 外层只调用主流程，不再额外追加 TopN，主流程也只有显式设置 `VE_HAOPENG_TOPN_ENABLED=1` 才会在同步后执行 TopN。
- `tests/test_ve_video_content_filtering.py` / `tests/test_cron_video_enhancer_daily.py`：新增回归测试覆盖历史只看目标日前、日内只看已处理目标日素材、业务硬拦优先级高于重复去重、极简字段只为保留素材生成，以及浩鹏 TopN 默认关闭。

## 2026-06-11

### [VE] 竞品自动更新、爬取去重与同步能力补记

- `ua_workflows/video_enhancer/competitor_list.py` / `scripts/run_ve_competitor_list_sync.py`：VE 竞品配置已支持从主多维表里的 `竞品list` 子表同步，默认读取字段 `广告主名` / `appid`，合并写回 `config/ai_product.json` 的 `video` / `photo` 分组；可用 `VE_COMPETITOR_LIST_TABLE_ID` / `VE_COMPETITOR_LIST_TABLE_NAME` / `VE_COMPETITOR_LIST_PRODUCT_FIELD` / `VE_COMPETITOR_LIST_APPID_FIELD` 覆盖，`--dry-run` 只读预览，`--strict` 可在同步失败时返回非 0。
- `ua_workflows/video_enhancer/pipeline.py` / `crawl.py`：VE 主流程启动时默认执行 `sync_competitor_config_if_enabled()`，成功后设置 `VE_COMPETITOR_LIST_ALREADY_SYNCED=1`，后续 standalone crawl 不重复同步；若 `VE_COMPETITOR_LIST_SYNC_ENABLED=0`、表为空、权限失败或未配置多维表链接，则沿用本地 `config/ai_product.json`，不阻塞日常抓取。
- `ua_workflows/video_enhancer/weekly_competitor_review.py` / `scripts/run_ve_weekly_competitor_review_server.sh`：周一竞品检查会基于上一周多维表/本地数据产出建议新增竞品、低量观察与建议移除老竞品，并推送飞书；2026-06-11 修复 server runner 在无额外参数时因空数组展开退出的问题，已补推 2026-06-01～2026-06-07 周报。周榜新竞品采集走 `ua_workflows/shared/guangdada/new_charts_ai_tools.py`，现已接入日更主爬取同款飞书安全验证人工闸口：检测到安全验证时发送飞书 IM，人工完成后点击「已完成」会重启周榜采集入口。
- `ua_workflows/video_enhancer/crawl.py`：VE 抓取默认用广大大 UI 日期范围 `target_date ~ target_date`，逐卡进入 detail-v2 并用本地 UTC+8 `first_seen == target_date` 二次校验；仍保留 `--no-ui-date-range` / `--napi-list` 作为调试回退。爬取阶段会做广告主匹配、非目标日剔除、重投剔除、重复 `ad_key` 去重，并把每产品点卡、页面卡、抓到、保留与剔除原因写入 raw / `crawl_product_retention_report`。
- `ua_workflows/shared/guangdada/search.py` / `interrupt_alerts.py`：VE 日更主爬取使用的 `run_batch` 已接入广大大安全验证人工闸口。登录后或主站稳定阶段检测到滑块/安全验证时，默认发送飞书 IM 交互卡片和帮助图，等待人工在页面完成验证后点击「已完成」，随后关闭当前浏览器并重启抓取入口；不自动破解验证码。默认等待 900 秒，默认重启 1 次，可用 `GUANGDADA_CAPTCHA_MANUAL_GATE`、`GUANGDADA_FEISHU_CONFIRM_TIMEOUT_SEC`、`GUANGDADA_HUMAN_CHECK_RESTART_LIMIT` 等环境变量调整。
- `ua_workflows/shared/media/resolve.py` / `ua_workflows/video_enhancer/sync.py`：远端同步进来的 `.webp` 视频 URL 已在共享媒体 helper 中统一归一为 `.mp4` 消费用 URL，覆盖 VE 的分析、入库、同步、飞书推送、复核看板与浩鹏 TopN 等下游；多维表同步侧的封面链接会把 `.image` 路径改为 `.png` 以改善飞书预览，封面附件上传先尝试原始 URL，失败时再尝试 `.png` 版本。
- `ua_workflows/video_enhancer/cover_dedupe.py` / `crawl_similarity.py` / `pipeline.py`：封面去重包含跨日 URL/ahash/素材指纹与 CLIP 视觉聚类。CLIP 默认阈值 `0.75`，历史参照默认 60 天，硬去重默认 7 天；7 天内历史命中剔除，7 天外历史命中保留当天最高展示估值代表并打「历史簇持续发力」类标签；同日相似封面只保留代表，剔除成员会计入代表的「日内相似素材数」。
- `ua_workflows/video_enhancer/pipeline.py` / `ua_workflows/shared/db/video_enhancer.py`：分析入队前还会做日内/跨日素材去重与历史分析复用，重复项复用 canonical 分析结果，不重复消耗 LLM；同步前会补跑成人/色情、人像照片特效硬拦、可选日内玩法重复、老玩法重复、embedding 高置信重复与已投放匹配等过滤/标记。
- `ua_workflows/video_enhancer/sync.py`：多维表同步前默认开启同玩法同模板去重（`BITABLE_TEMPLATE_DEDUP_ENABLED=1`），同产品/同玩法桶内按模板 exact 与封面 CLIP 相似合并，只保留展示估值/曝光/热度最高代表；模板文本相似默认关闭，仅设置 `BITABLE_TEMPLATE_DEDUP_TEXT_SIMILARITY_ENABLED=1` 时启用。同步报告会按产品记录硬排除、同模板合并、同玩法非代表、低采纳优先级跳过与最终写入数。
- `ua_workflows/video_enhancer/sync.py`：同步写主表时会读取真实字段类型并归一化值，尤其是「玩法」多选字段只写已有选项、不自动创建 AI 新建议；封面和视频会尽量以附件写入，同时保留链接字段；素材标签聚合风险、玩法判断、历史持续发力、优先级等信号。当前主流程默认发筛选漏斗报告，旧素材日报/企微/Google Sheet 与浩鹏 TopN 需要显式开关。

## 2026-06-02

### [VE] 素材留存维护 dry-run 与归档标记入口

- `ua_workflows/video_enhancer/retention.py`（新增）：提供低风险留存维护逻辑。多维表记录默认只评估归档候选；实际执行时只写「生命周期状态=已归档」「归档日期」「归档原因」，不调用飞书删除接口。归档候选默认要求超过 45 天，且没有 `待定` / `采纳` / `入素材库` 等保留信号，也没有 `历史簇持续发力` 标签。
- `scripts/run_ve_retention.py` / `scripts/cron_ve_retention_weekly.sh`（新增）：前者提供手工入口，后者提供每周 dry-run 定时入口；默认只写 `reports/ve_retention_{date}.json`，不改多维表、不删本地文件。实际写归档标记需显式传 `--apply-bitable`，实际清本地产物需显式传 `--apply-local`。
- 本地产物清理范围仅限 `data/` 与 `reports/` 下的 JSON / JSONL / Markdown / HTML / 图片等运行产物，以及 `data/remote_snapshots/ve/` 中超过保留份数的非 DB 快照产物；明确排除 SQLite 主库、远端 VE 快照 DB 与 `creative_library`，保留跨日去重、持续发力、回测和反馈训练所需历史。
- `.gitignore` / `docs/workflows.md` / `docs/cron-schedules.md` / `docs/setup-and-data.md`：补充 VE 留存维护说明、周一 dry-run cron 示例、执行开关与报告忽略规则。
- `tests/test_ve_retention.py`（新增）：覆盖多维表归档候选判定、待定/采纳/历史持续发力保护、本地文件保留规则、飞书 batch_update 归档字段 payload 与 dry-run 报告摘要。

### [VE/广大大] 新创意榜 AI 工具视频素材本地 raw 落盘

- `ua_workflows/shared/guangdada/new_charts_ai_tools.py`（新增）：采集广大大「新创意榜」工具类 AI 工具下 `AI图像生成` / `AI视频` 类目的视频素材，整理为 VE raw 兼容 JSON（`target_date` / `total` / `competitors` / `items` / `filter_report`），并额外输出字段完备性报告；当前只写本地 `data/guangdada_new_charts_ai_tools_{date}_raw.json` 与 `_completeness.json`，暂不写入多维表。
- `scripts/run_new_charts_ai_tools.py`（新增）：提供可复用入口，支持 `.env` 账号密码登录、保存/复用登录态、指定类目、每类目 TopN、headed 调试和本地输出前缀。
- `tests/test_new_charts_ai_tools.py`（新增）：覆盖类目别名解析、广大大 NAPI 创意列表抽取、视频链接归一、VE raw payload 构造与字段完备性统计。

### [VE] 浩鹏二次 AI 筛选 TopN 推送链路补齐

- `ua_workflows/video_enhancer/haopeng_ai_filter.py`（新增）：从 VE 主多维表读取目标日素材与 2026-05-25 起的浩鹏有效反馈，按产品调用 `qwen/qwen3.7-max` 做二次 AI 筛选，输出 `data/haopeng_topn_experiments/{date}_label_prior.json`；该步骤不写回多维表、不影响主流程拦截，只生产推送输入。
- `ua_workflows/video_enhancer/haopeng_topn_push.py`：未传 `--input-json` 时默认先生成当天二次 AI 筛选 JSON，再推送 TopN；保留 `--input-json` 与 `--use-latest-local` 兼容旧实验文件推送；新增 `--generate-only` 便于服务器只验证筛选产物。生产推送默认不展示「浩鹏接受情况」/ `actual_hp`，只有显式 `--include-backtest` 做复盘时才读取并展示。
- `ua_workflows/video_enhancer/pipeline.py`：VE 主流程默认不再发送旧「AI工具竞品日报」素材卡片，也不再默认触发企业微信或 Google Sheet；同步完成后保留 `haopeng_topn_push` 节点用于生成并推送「VE 浩鹏采纳预测 TopN」。TopN 未指定群时走 `VE_HAOPENG_TOPN_FEISHU_WEBHOOK`，没有专用值时复用筛选漏斗报告群 `VE_FLOW_REPORT_FEISHU_WEBHOOK`，最后才尝试 `FEISHU_UA_WEBHOOK`；不会兜底到通用测试群 `FEISHU_BOT_WEBHOOK`。2026-06-12 起该节点默认关闭，需 `VE_HAOPENG_TOPN_ENABLED=1` 才执行。
- `scripts/run_ve_haopeng_ai_filter.py`（新增）/ `scripts/run_ve_haopeng_topn_push.py`：前者只生成二次筛选 JSON，后者默认“生成 + 推送”。模型可用 `VE_HAOPENG_FILTER_MODEL` 覆盖，历史起点可用 `VE_HAOPENG_HISTORY_START_DATE` 覆盖。
- `ua_workflows/video_enhancer/haopeng_ai_filter.py` / `haopeng_topn_push.py`：当前固化为 2026-06-01 高命中口径：只用目标日前历史浩鹏反馈判断采纳偏好，目标日反馈只用于事后回测，进入模型前排除 `admob` / `youtube`，飞书卡片展示非排除渠道 Top10（不因 `hold` 强制少推）。
- `ua_workflows/video_enhancer/haopeng_topn_push.py`：TopN 卡片末尾追加“查看多维表格”按钮，链接到 `VIDEO_ENHANCER_BITABLE_URL`，便于当天查看浩鹏反馈；默认不展示回测字段，除非手动传 `--include-backtest`。
- `scripts/cron_ai_video_enhancer_daily.sh`：保持只执行 `run_video_enhancer.py`；浩鹏 TopN 只在主流程开关开启时发出，筛选漏斗报告仍由主流程发出，避免 cron 额外推测试群或重复推送。
- `.gitignore`：忽略 `data/haopeng_topn_experiments/`，避免每日二次筛选 JSON 进入版本管理。
- `tests/test_haopeng_ai_filter.py` / `tests/test_haopeng_topn_push.py`：覆盖多维表字段归一、二次筛选报告格式、排序与推送入口默认生成行为。

### [VE] Mac mini 生产库同步入口恢复

- `scripts/sync_ve_data_from_remote.sh`（新增）：恢复 VE-only 远端同步脚本，默认从 `ggbond@10.125.46.30:/Users/ggbond/oliver/ai-` 用远端 `sqlite3 .backup` 生成安全快照，再 `rsync` 到 `data/remote_snapshots/ve/`；同步后本地执行 `PRAGMA quick_check` 并输出 `daily_creative_insights` 的行数与最新 `target_date`。脚本显式排除 Arrow2 数据。
- `.gitignore`：重新忽略 `data/remote_snapshots/`，避免拉下来的生产快照进入版本管理。
- `tests/test_sync_ve_data_from_remote.py`：静态检查同步脚本存在、默认远端路径、VE DB 快照、完整性校验与 Arrow2 排除口径。

## 2026-05-22

### [VE] 多维表玩法字段收口为只同步正式标签

- `ua_workflows/video_enhancer/analyze.py` / `ua_workflows/video_enhancer/play_assets.py`：VE 灵感分析提示词改为只从多维表格「玩法」字段候选标签中选择；未命中时写 `unmatched_play`、玩法资产名称留空、玩法归类为「未命中」，不再要求模型生成 `new_play` 或自造新玩法名。
- `ua_workflows/video_enhancer/play_asset_report.py`：AI 明确给出 `unmatched_play` / 「未命中」时停止规则兜底匹配，保持玩法标签为空，避免后续自动补回不确定标签。
- `ua_workflows/video_enhancer/sync.py`：不删除多维表字段，但主表同步不再产出「玩法资产 / 玩法变种 / 玩法新旧 / 玩法资产ID / 玩法变种ID / 玩法判断来源 / 玩法判断理由 / 狭义新判断 / 狭义新理由」等内部列；仅继续写「玩法」「玩法指纹」「差异点」「模板指纹」等筛选需要字段，且「玩法」只有命中正式标签时才写。
- `tests/test_ve_template_normalization.py`：新增回归测试覆盖未命中玩法不产出正式标签、同步字段不包含冗余内部列、`unmatched_play` 不触发规则兜底补标。

### [VE] 筛选看板改为三层漏斗与多维表实数对账

- `ua_workflows/video_enhancer/review_dashboard.py`：筛选复核看板新增「三层筛选漏斗」，按爬取资格、封面/指纹去重、入表前业务筛选展示每层输入、输出和剔除原因；产品明细表同步展示抓到数、第一层后、指纹、跨日 CLIP、日内 CLIP、第二层后、业务硬拦、同模板、应入表与表内实际。
- `ua_workflows/video_enhancer/review_dashboard.py`：看板会用项目 `.env` 中的 `FEISHU_APP_ID` / `FEISHU_APP_SECRET` / `VIDEO_ENHANCER_BITABLE_URL` 只读扫描主表，并按 `抓取日期` 的 UTC+8 显示日期统计当日多维表实际记录数；失败时只在看板备注错误，不阻塞本地看板生成。
- `ua_workflows/video_enhancer/review_dashboard.py`：第三层新增两块明细，「业务硬拦（不进多维表）」和「同玩法同模板（不进多维表）」。同模板明细展示保留代表、封面相似度、模板相似度和原因，便于人工复核最终入表前剔除。
- 已刷新 `2026-05-21` 看板：本地应入多维表 32 条（40 条成功分析 - 6 条业务硬拦 - 2 条同玩法同模板），飞书主表实际 `抓取日期=2026-05-21` 为 0 条，说明当天主表同步尚未落表或同步中断。
- 根据人工查看习惯，顶部漏斗收窄为核心两块：「封面图去重」与「同玩法同模板去重」；第一层爬取资格不再作为主视线展示，只在备注中说明“封面前”已经过广告主、日期和重投过滤。
- 看板中「玩法资产库」改名为「多维表玩法标签库（字段：玩法）」，避免误读为旧 JSON 资产库；新增「筛选后素材 · 每条玩法标签（应入多维表）」表格，逐条展示最终应入表素材的多维表玩法标签、AI 建议玩法、产品、广告 ID、核心卖点、玩法指纹与模板指纹。`new_play` 未命中多维表标签时在「玩法标签」列统一显示「待沉淀」，AI 建议名单独放在「AI建议玩法」列。
- `ua_workflows/video_enhancer/analyze.py`：收紧 AI 新玩法建议名，禁止把「AI图片转视频」「图片转视频」「AI视频生成」「照片生成动态视频」等底层能力词自动当作新玩法沉淀；若模型只给出这类泛词，解析兜底会把玩法名和变种名改为「待人工归类」，并在玩法判断理由里保留原始建议，等待业务人员确认。
- `tests/test_ve_template_normalization.py`：新增泛玩法名清洗回归测试，覆盖「AI 图片转视频」被置为「待人工归类」、具体可复用模板名仍保留。
- `ua_workflows/video_enhancer/sync.py`：修复主表「玩法」字段已从文本变为多选后导致的同步失败。同步写记录前会读取真实字段类型；多选字段写成字符串数组，并且只保留飞书字段已有选项，AI 新建议/待沉淀玩法不会自动创建为正式「玩法」选项。`tests/test_ve_template_normalization.py` 新增多选字段值格式与未知选项过滤测试。
- 2026-05-21 VE 同步排查：同步命令目标确认仍是 `CivwbJ2HkazcKTsKnbGclA5RnWc / tblrZZvVuFcjL0kE / vewGH7cmSs`，不是写错表；原始失败为「玩法」多选字段值格式错误。后续一度出现 `403 Forbidden (91403)`，根因是外层进程环境里残留旧 `FEISHU_APP_ID`（末位 `8dcca`），`load_dotenv()` 默认不覆盖，导致没用项目 `.env` 中正确应用（末位 `c1cb3`）。已将 VE 关键入口改为 `load_dotenv(PROJECT_ROOT / ".env", override=True)`；正确 app 下假字段 `batch_create` 不再 403，而是返回预期字段不存在错误。
- 已用项目 `.env` 正确应用补跑 `2026-05-21` VE 主表同步，写入 32/32 条；多维表实际 `抓取日期=2026-05-21` 读数为 32 条（Pixverse 21、AI Mirror 6、DreamFace 2、Glam 1、Remini 1、GIO 1），并刷新 `reports/ve_filter_review_2026-05-21.html`。
- `ua_workflows/shared/config.py`：新增 `load_project_env()` / `project_env_values()`，用于统一让项目 `.env` 覆盖外层 shell/cron 残留环境变量；`ua_workflows/video_enhancer/pipeline.py` 的子进程环境也会用 `.env` 值覆盖，避免同步、看板、玩法标签读取吃到旧 app id。
- `ua_workflows/video_enhancer/cover_dedupe.py` / `ua_workflows/arrow2/cover_dedupe.py`：封面 CLIP 跨日历史改为双窗口。历史检索窗口默认 60 天（`COVER_STYLE_HISTORY_LOOKBACK_DAYS`，上限 60），硬去重窗口默认 7 天（`COVER_STYLE_HISTORY_HARD_DEDUPE_DAYS`）。7 天内命中历史仍按 `cover_style_cluster_vs_yesterday` 剔除今日素材；7 天外命中历史不直接丢弃，而是保留当天最高展示估值代表，并在报告 `history_refresh` 记录为 `cover_style_cluster_history_refresh`，用于表达「这个旧素材/旧模板又在发力」。
- 2026-05-21 人工复核素材 `300c5d6a2ecdc432aaa4ac4013eace49` 与 2026-05-08 历史素材 `8f2943975b7a180603b6ea249454cf6e` 封面相似度 `0.7554`，超过阈值 `0.75`，但旧 7 天历史检索窗口没加载 5/8。新逻辑会把它归为 13 天前历史簇的持续发力代表，而不是当作全新素材，也不是直接删掉。
- `ua_workflows/video_enhancer/sync.py` / `crawl_similarity.py` / `shared/db/video_enhancer.py`：多维表同步会给 7 天外历史簇代表写入「历史簇持续发力」「历史命中:日期」「历史间隔:N天」「历史封面相似度:X」标签；同一历史刷新簇里被日内合并的素材继续计入代表行「日内相似素材数」；持续发力日报信号也会读取 `history_refresh`。
- `tests/test_ve_template_normalization.py`：新增封面历史窗口默认 60 天、上限 60 天、7 天外历史刷新保留代表、7 天内历史硬去重、历史刷新标签写入的回归测试。

## 2026-05-21

### [VE] Retake 暂停抓取与人物照片特效硬拦截

- `config/ai_product.json`：从 VE 默认 `photo` 竞品列表移除 `Retake AI Face & Selfie Editor`，日常默认抓取暂不再包含 Retake；如需恢复可重新加回对应 appid。
- `ua_workflows/video_enhancer/content_filters.py`：新增“只保留用户上传人物照片加工特效”硬拦截；电商/商品广告、宠物/动物、房间装修、风景、食物、车辆、纯文字/Logo/海报等非人物照片加工素材会标记 `exclude_from_bitable` / `exclude_from_cluster`，标签为 `非人物照片加工特效`。
- `ua_workflows/video_enhancer/pipeline.py` / `sync.py` / `flow_report.py`：主流程与独立同步都会执行该硬拦截，并在同步/漏斗报告中记录 `ecommerce_effect`、`non_human_photo_effect`、`missing_human_photo_input` 等原因。
- `tests/test_ve_template_normalization.py`：新增回归测试，覆盖电商商品图、宠物跳舞、房间装修应拦截，自拍生成生日写真应保留。

### [VE/Arrow2] CLIP 封面去重默认阈值下调

- `ua_workflows/video_enhancer/cover_dedupe.py`：`COVER_VISUAL_DEDUP_THRESHOLD` 未在 `.env` 显式覆盖时，默认值从 `0.8` 下调到 `0.75`；VE 与复用该 helper 的 Arrow2 封面 CLIP 日内 / 跨日向量去重都会使用新默认阈值。若需临时回退，可在 `.env` 设置 `COVER_VISUAL_DEDUP_THRESHOLD=0.8`。
- 注意：历史已生成的 `*_cover_style_intraday.json` 与本地 HTML 看板仍记录当时运行的 `cosine_threshold`，不会因默认值变更自动改写；需要重跑对应日期流程才会按 `0.75` 重新筛。

### [VE] 同玩法同模板换人合并增强

- `ua_workflows/video_enhancer/sync.py`：主表同步前的同模板去重从 exact key 升级为同产品/同玩法桶内并查集聚类；同玩法优先用玩法资产 ID，模板 exact 命中继续保留，同时新增可选模板文本相似（阈值 `BITABLE_TEMPLATE_DEDUP_TEXT_SIMILARITY_THRESHOLD=0.78`）和默认封面 CLIP 相似（阈值 `BITABLE_TEMPLATE_DEDUP_CLIP_THRESHOLD=0.70`）两类边，专门补掉同一模板只换人种、性别或模特导致的漏筛。跳过明细会记录 `match_reason`、`match_ad_key`、模板相似度和封面相似度，链式聚类也能看到直接证据。
- `tests/test_ve_template_normalization.py`：新增同步前模板模糊合并回归测试，覆盖同一模板不同描述应合并、同玩法但不同模板应保留。
- 已对 `2026-05-20` 本地样本做只读 dry-run：成功分析候选 95 条；旧 exact 逻辑删除 1 条、剩 94 条；新逻辑删除 15 条、剩 80 条，额外多筛 14 条（候选池额外压缩 14.74%）。产物：`data/workflow_video_enhancer_2026-05-20_template_dedup_dry_run.json`、`reports/ve_template_dedup_dry_run_2026-05-20.html`。
- 根据人工复核结论，模板文本相似不再默认作为自动合并证据；仅当 `BITABLE_TEMPLATE_DEDUP_TEXT_SIMILARITY_ENABLED=1` 时才开启。默认自动合并保留 exact 模板和同玩法内封面 CLIP 相似。

### [VE] 玩法库切到多维表格「玩法」字段

- `ua_workflows/video_enhancer/bitable_play_labels.py`（新增）：从 `VE_PLAY_LABEL_BITABLE_URL` 或 `VIDEO_ENHANCER_BITABLE_URL` 指向的多维表读取「玩法」字段（可用 `VE_PLAY_LABEL_FIELD_NAME` 覆盖字段名），优先取单选/多选字段选项，取不到选项时扫描记录里的历史值；标签会转成旧代码兼容的 `play_<sha1>` 玩法 ID，并缓存到 `data/ve_bitable_play_labels.json`。
- `ua_workflows/video_enhancer/play_assets.py`：默认 `VE_PLAY_LIBRARY_SOURCE=bitable`，不再使用 `config/ve_play_assets.json` 作为主玩法库；如需临时回退旧 JSON，可设置 `VE_PLAY_LIBRARY_SOURCE=legacy_json`。
- `ua_workflows/video_enhancer/analyze.py`：分析 prompt 改为「多维表格玩法标签库候选清单」，要求 AI 直接从多维表格「玩法」标签中选择玩法；旧 `玩法资产名称` 字段现在语义上就是多维表格「玩法」标签，`玩法变种` 固定为基础变体/新玩法兼容值。
- `ua_workflows/video_enhancer/sync.py`：主表字段新增「玩法」，同步时写入 AI 选择的多维表格玩法标签；保留「玩法资产」等旧字段用于兼容历史看板/报表。
- 当前本地 `lark-cli` bot 读取表结构/记录缺少 `base:field:read` / `base:record:read` scope，user 身份也未授权；代码已支持读取，实际拉取标签需先给应用开通对应权限或提供 `VE_PLAY_LABELS` 临时标签列表。

## 2026-05-19

### [Arrow2] 最新素材切到 UI 指定日期抓取

- `ua_workflows/arrow2/crawl.py`：`latest_yesterday` 默认改为和 VE 一样先在广大大 UI 选择 `target_date ~ target_date`，再逐张点卡 detail-v2 并用本地 `first_seen == target_date` 校验；保留 `--no-ui-date-range` 作为旧 7 天池口径调试回退，raw 产物新增 `ui_date_range`。
- `ua_workflows/arrow2/crawl.py` / `ua_workflows/shared/guangdada/search.py`：搜索按钮点击超时时增加 force click / Enter 兜底；单产品爬取异常时会重登、重设筛选并重试一次，重试仍失败则记录空结果后继续后续产品，避免一个产品页面状态异常中断整天同步。
- 已用新 UI 日期口径补跑 `2026-05-17` 与 `2026-05-18` Arrow2 latest：分别同步 45 条、40 条到多维表；本地 `data/arrow2_pipeline.db` 对应日期分析完成数为 45/45、40/40。

### [VE] 全流程产品漏斗报告与低量告警

- `ua_workflows/video_enhancer/flow_report.py`（新增）：汇总当日 crawl、分析入队、分析结果、sync 与 acceptance 产物，生成 `data/workflow_video_enhancer_{date}_flow_report.json` 和 `reports/workflow_video_enhancer_{date}_flow_report.md`；默认读取 `VE_FLOW_REPORT_FEISHU_WEBHOOK` 推送飞书交互卡片。
- `ua_workflows/video_enhancer/pipeline.py`：分析入队阶段新增 `data/workflow_video_enhancer_{date}_analysis_queue_report.json`，按产品记录封面后素材数、分析前去重、历史缓存复用、准入失败原因和实际 LLM 入队数；流程成功结束和分析成功率过低提前退出时都会补跑全流程报告。
- `ua_workflows/video_enhancer/sync.py`：主表同步新增 `data/workflow_video_enhancer_{date}_sync_report.json`，按产品记录成功分析数、硬排除、同模板换人/性别合并、同玩法非代表、低采纳优先级跳过和最终主表写入数。
- 全流程报告按产品展示点卡、抓到、爬取保留、封面后、分析去重后、LLM 入队、可用分析、同步候选、主表写入和主要筛掉/跳过原因；若某产品「封面后保留」或「主表写入」低于近 5 天均值 50%（且历史均值不低于 3 条），飞书卡片会给出人工确认用的单产品重试命令。

### [VE] 相似素材数前移到爬取产物

- `ua_workflows/video_enhancer/crawl_similarity.py`（新增）：相似素材数从 raw/crawl 阶段开始沉淀。standalone crawl 会按同产品/appid 下的 exact `image_ahash_md5`、封面 URL、视频 URL 先写入 `crawl_similarity_count_by_ad` 与每条 item/creative 的 `crawl_similarity_count`。
- `ua_workflows/video_enhancer/pipeline.py`：封面 CLIP 日内聚类后调用 `merge_cover_similarity_counts`，把 `cover_style_cluster` 同日剔除成员计入代表素材的 raw 相似素材数；跨日旧封面不计入当天「日内相似素材数」。
- `ua_workflows/video_enhancer/sync.py`：多维表「日内相似素材数」优先读取 raw 中的 `crawl_similarity_count_by_ad`，再用旧的同步时兜底逻辑和同模板换人/性别合并补充；避免最后同步阶段才临时开始计算。
- `tests/test_ve_template_normalization.py`：新增 raw 阶段 exact 签名计数、封面 CLIP 日内簇合并计数回归测试。

## 2026-05-18

### [VE] 最新素材狭义新判断

- `ua_workflows/video_enhancer/analyze.py`：VE 结构化输出新增 `【模板指纹】`，要求模型描述具体分镜/布局/脚本骨架，并忽略同模板只换人种、肤色、性别或男女模特；`analysis` 兼容短摘要同步包含模板指纹。
- `ua_workflows/shared/db/video_enhancer.py` / `pipeline.py`：`daily_creative_insights` 与 `creative_library` 自动迁移新增 `template_fingerprint`，单条增量入库、素材库回写、历史缓存复用和日报候选读取都会保留该字段；主流程回写素材库时同步携带玩法资产判断与模板指纹。
- `ua_workflows/video_enhancer/play_asset_report.py`：日报新口径从“新玩法/新变种”收敛为“新玩法/老玩法新迭代/老玩法换皮”。稳定玩法未见过算新玩法；稳定玩法已见过但模板指纹首次出现算老玩法新迭代；稳定玩法与模板指纹均见过（包括同模板换人种/性别）算老玩法换皮，不进入推送代表。
- `ua_workflows/video_enhancer/sync.py` / `push_feishu.py` / `review_dashboard.py`：多维表新增「模板指纹」「狭义新判断」「狭义新理由」字段；日报标签和飞书卡片改为狭义新素材口径。未改动 Arrow2 定时或数据流。
- `ua_workflows/video_enhancer/pipeline.py`：移除日常主流程里的二次 `dom_enrich` 详情补全节点；VE 抓取已默认走 DOM 点卡 detail 主路径，raw 阶段已经拿到详情，`dom_enrich.py` 仅保留作手动排障工具。
- `ua_workflows/video_enhancer/content_filters.py` / `pipeline.py` / `sync.py`：移除「低采纳主题/家居装修」硬拦截，空房间、豪华装修、家居设计等不再仅因主题被写 `exclude_from_bitable`。
- `ua_workflows/video_enhancer/sync.py`：多维表「日内相似素材数」计数补入同日封面 CLIP 聚类排除成员；仅统计 `cover_style_cluster` 日内封面重复，不把跨日旧封面 `cover_style_cluster_vs_yesterday` 算入当天扎堆数。
- `ua_workflows/video_enhancer/sync.py`：主表同步新增默认开启的 `BITABLE_TEMPLATE_DEDUP_ENABLED` 模板级硬筛。同步前按同产品/appid、人口/性别归一后的玩法指纹、人口/性别归一后的模板指纹分组，只保留展示估值/曝光/热度最高代表；被同模板合并的素材仍会计入代表行「日内相似素材数」。已清理 2026-05-17 GIO 主表中 4 条同模板换人重复行，并回填 3 条代表素材的相似素材数。
- `ua_workflows/video_enhancer/crawl.py` / `pipeline.py`：新增每日产品级抓取保留漏斗。爬取 raw 的 `filter_report.per_product` 增加点卡详情数、页面卡片数、抓到素材数、广告主不匹配、非目标日、截断剔除等计数；主流程生成 `crawl_product_retention_report` 与 `data/workflow_video_enhancer_{date}_crawl_product_retention.json`，合并封面跨日/日内去重原因和最终保留数。
- `tests/test_ve_template_normalization.py`：新增狭义新回归测试，覆盖同模板人口属性替换不推送、老玩法新模板进入“老玩法新迭代”、全新稳定玩法进入“新玩法”。

## 2026-05-15

### [VE] 审核多维表反馈训练链路（独立于正常工作流）

- `ua_workflows/video_enhancer/feedback_training.py`（新增）：从 VE 审核多维表 `CivwbJ2HkazcKTsKnbGclA5RnWc / tblrZZvVuFcjL0kE / vewJtPixtM` 直接拉取字段，单独写入 `data/ve_feedback_training.db`。`接受情况` 中 `接受`/`采纳`/`入素材库=1`，`删除`/`不采纳=0`，`待定`/`重复抓取`/空值只做留存不进训练。
- 训练特征限定为素材本身：标题、正文、视频/封面链接、核心卖点、Hook、脚本/口播、玩法资产/变种、玩法指纹、差异点、风险等级、AI 分析和素材标签；产品、广告主、抓取日期、展示估值、人气、热度、投放地区等运营字段只写审计 JSON，不作为自变量。
- 新增产物：`data/ve_feedback_training_dataset_{date}.jsonl`、`data/models/ve_feedback_preference_nb_{date}.json`、`reports/ve_feedback_training_{date}.md`。当前 baseline 是无额外依赖的文本朴素贝叶斯，便于先积累反馈闭环；后续可替换为多模态/排序模型。
- `ua_workflows/video_enhancer/feedback_training.py`：新增 `--complete-profile` 完整度过滤。`core` 只用标题、视频/封面链接、核心卖点、Hook、脚本/口播、风险等级、AI 分析齐全的样本；`core_play` / `play` 额外要求玩法字段；`all` 要求所有训练字段齐全（当前历史数据因 `玩法判断理由` 为空会筛到 0 条）。
- `scripts/run_ve_feedback_training.py` / `scripts/cron_ve_feedback_training_daily.sh`（新增）：支持 `run`（拉取+入库+导出+训练）、`pull`、`train`、`export`；cron 建议每天 09:40 运行，日志 `logs/cron_ve_feedback_training.log`。该链路不触发广大大爬取、VE 分析、多维表主表同步或日报推送。
- `.gitignore`：忽略反馈训练本地产物（SQLite、JSONL、模型 JSON、日报 Markdown），避免每日训练输出进入版本管理。
- `docs/ve-feedback-training.md` / `docs/workflows.md` / `docs/cron-schedules.md` / `docs/README.md`：补充独立反馈训练链路说明、字段口径、产物和定时任务。

### [VE] 目标日最新创意滚动深度修复

- 飞书迭代日志人工复核确认：2026-05-14 PixVerse 若干图片素材的 exact `ad_key` 未进入本地 raw / `daily_creative_insights` / `creative_library`，不是后续封面阈值或玩法筛选误杀；本地 PixVerse 目标日素材时间从 `18:29` 跳到 `14:33`，漏掉了用户截图中的 `17:xx` 区间。
- `ua_workflows/shared/guangdada/search.py`：`run_batch` / `_collect_keyword_crawl_result` 新增可配置 `max_scroll_rounds` 与 `stop_scroll_if_oldest_first_seen_before_ymd`，复用已有 `_search_one_keyword` 的“滚到早于目标日即停”能力。
- `ua_workflows/video_enhancer/crawl.py`：VE 在 `--target-date` 模式下默认把最新创意滚动上限从固定 16 轮提高到 56 轮，可通过 `VIDEO_ENHANCER_MAX_SCROLL_ROUNDS` 覆盖。因广大大初始 NAPI 响应会混入很老的历史素材，`first_seen < target_date` 早停容易误触发并导致只抓首屏，现默认关闭该早停；如需临时打开可设 `VIDEO_ENHANCER_TARGET_DATE_EARLY_STOP_ENABLED=1`。原封面 CLIP 阈值仍保持默认 `0.8`。

### [VE] 同模板人种/性别差异不再拆新素材

- `ua_workflows/shared/db/video_enhancer.py`：玩法文本归一化新增人口属性抹平规则，黑人/白人/亚裔、男性/女性、男模/女模等同一模板换人差异会归为同一玩法文本，从而影响日内玩法重复、老玩法重复、日报新素材/新变种 key、玩法资产 fallback 匹配等后续判重入口；`男变女` / `女变男` 等性别转换机制会先规整为 `性别转换`，避免误删核心玩法。
- `ua_workflows/video_enhancer/analyze.py`：VE 分析 prompt 新增模板级去重规则，要求模型不要因为同一模板只替换人种、肤色或性别就输出新玩法/新变种，也不要把这些人口属性写进玩法指纹/玩法变种名。
- `tests/test_ve_template_normalization.py`：新增回归测试，覆盖“黑人女性 vs 白人男性同模板”为同一玩法，以及性别转换机制仍被保留。

### [VE] 视频分析结构化收敛与 analysis embedding 去重下线

- `ua_workflows/video_enhancer/analyze.py`：VE 多模态分析 prompt 取消原有 1～6 段正文分析，只要求模型输出 7 个固定结构化字段：`Hook解析`、`脚本口播`、`核心卖点`、`玩法指纹`、`差异点`、`风险标签`、`风险等级`。`analysis` 老字段不再承载长正文，仅由这些结构化字段拼出兼容短摘要，保证旧同步成功判定与多维表 `AI分析结果` 不为空。
- `ua_workflows/video_enhancer/pipeline.py` / `ua_workflows/shared/db/video_enhancer.py`：下线旧 `analysis_embedding` 语义跨日去重链路，不再创建、写入或读取 `creative_library.analysis_embedding`，也不再跑 `_apply_semantic_dedup`。后续玩法相似判断只保留 `play_fingerprint` / `effect_one_liner` 文本与 `effect_one_liner_embedding` 相关链路，避免正文随机性继续影响去重。
- `.gitignore`：补充忽略筛选看板 HTML、`reports/assets/` 封面缓存、验收报告、方向卡片 md 与本地抓取调试文件，避免自动产物继续进入 git status；已发现仓库中仍有旧的 tracked 报告/调试文件，后续可单独做一次 git rm 清理。
- 项目安全清理：删除已废弃且无代码引用的 tracked 调试/报告/旧配置产物，包括根目录抓包 HTML/PNG/JSON、`config/style_filters.json`、`config/twitter_input.*`、旧 `data/ua_suggestion_workflow_video_enhancer_*.md`、旧 `reports/workflow_video_enhancer_*_acceptance.md`。未删除当前筛选看板 HTML 与 `reports/assets/` 封面缓存，避免影响正在浏览的复核页面。

### [VE] 内部特效上线记录合并到玩法资产库

- `config/ve_play_assets.json`：通过 Chrome/Computer Use 从 Google Sheet `AI产品热点排期表 / 特效上线记录`（2025-11-17～2026-05-14，364 条）复制解析内部上线主题，并入 VE 玩法资产库。资产数从 25 扩到 27，子标签从约 100 扩到 144；新增稳定基类「照片生成直播间/主播 PK 场景」和「照片生成密集场景寻人/找自己」。
- `config/ve_play_assets.json`：把 Stadium/Stands Cam、Soccer Minis、Pet Dance、Warm Reunion/Alive Again/Still Here、Find Me/Where’s Me、Go Live/Live Queen/Live Battle、Chibi Emojis/Chibi Doll、Hairstyle Lookbook、Best Mom/Mother’s Day、Floral/Chinese/Graduation 等内部上线主题沉到现有玩法的 aliases、关键词、example_effects 和 subtags，作为新变种判断依据，而不是在代码里补产品特判。
- `ua_workflows/video_enhancer/play_asset_doc_sync.py` / `docs/ve-play-assets.md`：云文档渲染头部新增内部上线记录来源、记录数、日期范围和 Google Sheet 入口；本地文档补充该 Google Sheet 是玩法资产库的内部上线主题来源。
- 已验证 `config/ve_play_assets.json` JSON 合法，`match_play_asset` 可正确命中 Stadium Cam、Find Me/Where’s Me、Go Live/Live Battle、Pet Dance、Warm Reunion、Chibi Emojis 等代表样例；已渲染云文档 Markdown 预览 `/tmp/ve_play_assets_doc.md`。

### [VE] 视频分析阶段直接输出玩法资产判断

- `ua_workflows/video_enhancer/analyze.py` / `play_assets.py`：VE 视频/图片分析 prompt 现在会注入压缩后的玩法资产库候选清单，要求模型额外输出 `【玩法资产ID】`、`【玩法资产名称】`、`【玩法变种ID】`、`【玩法变种名称】`、`【玩法归类】`、`【玩法判断理由】`；其中已有玩法必须引用资产库 ID，不匹配时写 `new_play`，已有资产但新变种写 `new_variant`。分析子进程启动时会先尝试从飞书云文档拉取最新资产库，失败则使用本地 JSON。
- `ua_workflows/shared/db/video_enhancer.py`：`daily_creative_insights` 与 `creative_library` 自动迁移新增 AI 玩法判断字段（资产 ID/名称、变种 ID/名称、新旧、来源、理由），单条分析入库和素材库回写都会保留这些字段，历史缓存复用时也不丢失。
- `ua_workflows/video_enhancer/play_asset_report.py` / `sync.py`：玩法归类改为「AI 主判 + 规则兜底」。日报/同步优先使用分析阶段的 AI 资产判断；若 AI 缺失、ID 无效或不确定，再走关键词/别名/案例匹配。多维表新增「玩法判断来源」「玩法判断理由」字段，并在素材标签里写 `玩法判断:AI`，但「新玩法 / 新变种 / 已沉淀」仍会结合历史库重新计算，避免只相信单条素材自报。
- `README.md` / `docs/workflows.md` / `docs/setup-and-data.md` / `docs/ve-play-assets.md`：补充 VE 玩法资产库、AI 玩法判断、主表新增字段、内部上线记录来源和本地/云文档兜底关系，方便后续按文档接手维护。
- 已验证 prompt 中包含 `photoshoot_noir_vintage` 等变种 ID，解析器能正确抽取 AI 玩法字段，临时 SQLite upsert/readback 可保留 `play_asset_id`，`git diff --check` 通过。

## 2026-05-14

### [VE] 正式抓取口径切为 UI 指定日期 + 点卡校验

- `ua_workflows/video_enhancer/crawl.py` / `ua_workflows/video_enhancer/pipeline.py`：确认正式 VE 主流程调用 `--target-date` 时默认设置 UI 日期范围为 `target_date ~ target_date`，替代旧的「先选 7 天再本地筛」口径；`--no-ui-date-range` 仅保留作 A/B 调试回退。
- 对 PixVerse 2026-05-12 做三组对比：旧 `napi list + 7天` 仅 5 条，`7天 + 点卡 detail + first_seen` 39 条，`UI 指定日期 + 点卡 detail + first_seen` 64 条；因此正式口径采用最后一种。注意 UI 日期筛仍会夹带非目标日排序结果，必须保留本地 `first_seen == target_date` 校验和早停。
- `ua_workflows/shared/guangdada/search.py`：headed 检查 JSON 额外写入 `date_filtered_out`，用于人工对比被本地日期校验剔除的非目标日素材。
- `ua_workflows/shared/db/video_enhancer.py`：VE 数据库补充卡片 detail 可得到的投放地区字段；`daily_creative_insights` 与 `creative_library` 新增 `country_codes_json`、`geo_targeting_json` 并自动迁移。展示估值/人气/热度继续写入既有 `all_exposure_value` / `impression` / `heat` 数值列；投放地区同时保留规范化国家码与原始 geo payload，便于后续同步和对账。

### [VE] 2026-05-13 筛选复核看板刷新与 CLIP 日内展示修正

- `ua_workflows/video_enhancer/review_dashboard.py`：刷新 2026-05-13 复核看板；CLIP 指标改为「CLIP 封面命中」，并在统计备注中拆分跨日 / 日内，避免把 `cover_style_cluster` 日内簇误读成跨日命中。
- `ua_workflows/video_enhancer/review_dashboard.py`：修复统计值为 `0` 时被空字符串吞掉的问题；旧报告中缺失被剔除素材封面的 CLIP 日内簇，会用「同簇代表封面」兜底展示，避免页面出现 `无本地封面`。
- `ua_workflows/video_enhancer/review_dashboard.py`：一句话 / 玩法筛选板块前置到封面去重板块之前，每个玩法簇先展示封面卡片，再展示文字证据，避免复核时需要翻过大量 CLIP 簇才能看到一句话筛掉的素材。
- `ua_workflows/video_enhancer/review_dashboard.py`：一句话 / 玩法筛选板块改为「被剔除素材 vs 对比对象」成对封面展示；日内玩法、embedding、语义重复直接取命中 ad_key，老玩法仅有文案证据时会按 appid + 玩法指纹从本地库反查历史代表素材补封面。
- `ua_workflows/video_enhancer/review_dashboard.py`：一句话对比卡新增两张封面的 CLIP 相似度与阈值解释；看板新增「未筛掉 / 当前看板保留素材」与「日报推送筛选逻辑」板块，展示 5/13 当前保留 46 条、日报新素材推送段 5 条，以及 5 条 `exclude_from_cluster=1` 但未写 `exclude_from_bitable=1` 的口径差异素材。
- `ua_workflows/video_enhancer/analyze.py`：收紧 VE `玩法指纹` 生成原则，要求模型先归纳稳定的「输入对象 + 关键变换 + 输出大类」，把热点话题、运动项目、节日、发型、服装、名人同款、画面文案等易变元素放到「差异点」，减少靠不断补硬编码规则来合并玩法。
- `config/ve_play_assets.json` / `ua_workflows/video_enhancer/play_assets.py`：新增人工沉淀的 VE 玩法资产库与通用关键词匹配 helper，默认读取可版本化的 `config/ve_play_assets.json`（兼容旧 `data/ve_play_assets.json`）。当前版本基于本地库 2026-03-18～2026-05-13 的 526 条历史已分析素材沉淀，共 25 个资产，历史覆盖率 100%；其中 2026-05-05～2026-05-13 的 466 条五月已分析素材也 100% 覆盖。资产包含体育赛事现场转播、写真/杂志封面、双人融合、美颜前后对比、照片动态化、机甲奇幻、跳舞动画、模板换脸、老照片修复、游戏角色皮肤、路人消除、成人交友导流拦截等稳定玩法资产。
- `config/ve_play_assets.json` / `ua_workflows/video_enhancer/play_assets.py`：玩法资产新增 `subtags` 子标签机制，一个素材先命中稳定基类，再可多选变体标签；当前 25 个基类中 23 个已配置 100 个子标签，覆盖 F1/赛车、棒球、足球/世界杯、观众席/球迷、直播/转播包装、生日主题、杂志封面、红毯/奢华生活、名人宝丽来合影、病毒/热门 trend、换脸主角、机甲/战甲、恶魔/僵尸、老照片上色、文本提示词、成人导流风险 hook 等变体。2026-05-05～2026-05-13 五月素材中 400/466 条已命中至少一个子标签。
- `ua_workflows/video_enhancer/review_dashboard.py`：复核看板读取玩法资产库，统计「玩法资产命中 / 待沉淀」，并新增「玩法资产库（历史沉淀）」与「未筛掉素材 · 按玩法资产归类」板块，方便每天把新类目继续沉淀为资产，而不是在代码里继续补产品特判。
- `ua_workflows/video_enhancer/play_asset_report.py` / `push_feishu.py` / `push_multichannel.py`：新增「新玩法 / 新玩法变种」日报口径。日报先给当日候选素材命中玩法资产与子标签，再与历史全量素材比对：玩法基类历史没见过算「新玩法」，基类已见过但子标签组合没见过算「新玩法变种」；飞书/企微日报默认只推这两类代表素材，不再把持续发力或普通老玩法换素材混入推送。2026-05-13 当前口径为新玩法 0 个、新玩法变种 13 个、代表素材 13 条。
- `ua_workflows/video_enhancer/sync.py`：主表新增「玩法资产」「玩法变种」「玩法新旧」「玩法资产ID」「玩法变种ID」「玩法指纹」「差异点」字段，并把玩法资产/变种/新旧标签写入「素材标签」，方便在多维表格里按稳定玩法和 trend 变体筛选复核。
- `ua_workflows/video_enhancer/pipeline.py` / `sync.py`：主表同步默认放宽，日内玩法重复、老玩法重复、embedding 玩法重复不再默认作为 `exclude_from_bitable` 硬拦截；`BITABLE_SYNC_DAILY_PLAY_REPRESENTATIVES_ONLY` 与 `BITABLE_ACCEPTANCE_PRIORITY_SYNC_ENABLED` 默认改为关闭，主表尽可能同步更多成功分析素材。成人/色情、已投放等硬风险仍保留拦截；如需恢复旧的窄同步，可显式打开对应环境变量。
- `ua_workflows/video_enhancer/review_dashboard.py`：筛选复核看板的「日报推送筛选逻辑」切换到新玩法资产/变种口径，新增「日报新玩法 / 新变种推送段」和「日报新口径未推送素材」板块；未推送素材会标注「同资产变种非代表」或「已沉淀玩法/变种」，并展示封面、玩法资产、子标签、变种 ID，方便复核为什么没有进入每日推送。
- `ua_workflows/video_enhancer/play_asset_doc_sync.py` / `scripts/sync_ve_play_assets_doc.py` / `docs/ve-play-assets.md`：将飞书云文档 `https://www.feishu.cn/docx/HrxAdmiN6o7S4BxSNpXcT8h2n1n` 升级为「人可编辑、机器可解析」的玩法资产库源。文档中每个玩法块含 YAML 字段；项目默认尝试从云文档拉取并覆盖 `config/ve_play_assets.json`，失败则使用本地 JSON 兜底。支持 `pull-doc`、`push-doc`、`render`、`append-drafts --date YYYY-MM-DD`；5/13 当前无待沉淀草稿需要追加。
- `ua_workflows/video_enhancer/cover_dedupe.py`：未来 CLIP 日内剔除明细也会写入 `product`、`all_exposure_value`、`cover_url`，后续看板不再需要同簇代表图兜底。
- 已生成并在 in-app browser 打开 2026-05-13 看板：`reports/ve_cover_dedupe_clusters_2026-05-13.html` / `reports/ve_filter_review_2026-05-13.html`。当前统计：CLIP 50 条 / 30 簇（跨日 12、日内 38），指纹跨日 3 条 / 3 簇，一句话命中 8 个素材 / 8 簇 / 8 条证据，其中封面已覆盖 0 个，玩法兜底 8 个；保留素材 46 条均已命中 25 个玩法资产之一；浏览器验证无 `无本地封面` 占位。

## 2026-05-13

### [VE] 爬取主路径改为页面卡片逐张点击 detail-v2

- `ua_workflows/shared/guangdada/search.py`：新增通用 latest 点卡主路径 `_collect_keyword_crawl_result_latest_dom_detail`，流程为搜索/滚动后读取当前 DOM 卡片并逐张获取 `detail-v2`，再用 detail 覆盖 DOM 基础字段；若点卡结果为空才回退 napi 列表。`_click_cards_for_details` 的响应解析改为直接支持 detail-v2 返回的单 dict，并收窄监听日志，避免普通运行时刷屏。
- `ua_workflows/shared/guangdada/search.py`：`run_batch` 新增 `detail_click_primary` 与 `first_seen_target_ymd` 参数；VE 使用时会完整滚动加载页面卡片，并在点开的 detail 真正早于目标日期时早停（`VIDEO_ENHANCER_FIRST_SEEN_EARLY_STOP=0` 可关）。点卡默认翻 3 页以覆盖超过 70 条的竞品素材池，`VIDEO_ENHANCER_DOM_CLICK_MAX_PAGES` 可调页数，`VIDEO_ENHANCER_DOM_CLICK_MAX_CARDS` 可限每页点卡数。
- `ua_workflows/video_enhancer/crawl.py`：默认启用 `dom_detail_click` 模式，和 Arrow2 点击卡片抓 detail 的逻辑对齐；有 `--target-date` 时默认同步把 UI 日期设为 `target_date ~ target_date`，减少 7 天池子/虚拟列表加载不完整导致的漏抓。raw 输出新增 `crawl_mode` / `ui_date_range`。保留 `--no-ui-date-range` 对比旧 7 天口径，保留 `--napi-list` 作为旧 napi 列表口径的调试回退；最终过滤会丢弃无 `ad_key` 的 DOM-only 行，避免后续入库缺主键。
- `ua_workflows/shared/guangdada/search.py` / `scripts/test_video_enhancer_crawl.py`：新增 headed 人工验收辅助，`--all-products --pause-per-product` 会按全部 VE 产品逐个爬取，每个产品完成后在浏览器保持当前页面并打印 `source/all_creatives/detail_rows`、目标日 `first_seen` 命中数和日期分布摘要，等待回车再继续下一个产品。

### [VE] CLIP 封面跨日去重硬拦截

- `ua_workflows/video_enhancer/cover_dedupe.py`：修正 CLIP 封面向量跨日去重口径。过去实现虽然把近 7 日历史封面向量加入聚类，但同簇按展示估值最高者胜出，导致“今日估值更高”的跨日相似封面会被保留；现改为只要今日封面命中历史 CLIP 簇（同 appid、cosine ≥ `COVER_VISUAL_DEDUP_THRESHOLD`，默认 0.8），即按 `cover_style_cluster_vs_yesterday` 剔除今日素材，只有纯日内簇才按展示估值保留当日代表。
- `ua_workflows/shared/db/video_enhancer.py`：`load_cover_style_rows_for_dates_grouped_by_appid` 返回历史 `target_date`，供 CLIP 跨日命中证据记录 `matched_date` 与相似度。
- `ua_workflows/video_enhancer/review_dashboard.py`：新增本地 HTML 筛选复核看板，统一展示 CLIP 跨日封面、ahash/url 指纹跨日封面，以及「一句话 / 玩法筛选剔除」明细；同簇聚合展示，支持搜索，封面会缓存到 `reports/assets/ve_filter_review_日期/`，并兼容旧的 `ve_cover_dedupe_clusters_日期.html` 地址。
- `ua_workflows/video_enhancer/review_dashboard.py`：一句话 / 玩法筛选剔除板块现在也会在每个玩法簇内展示被筛掉素材的封面卡片，再展示文字证据明细，便于直接按图复核误杀。
- `ua_workflows/video_enhancer/review_dashboard.py`：一句话板块新增筛选归因拆分：按当前封面规则已会被 CLIP/指纹提前剔除的素材标为「封面已覆盖」，不再计作一句话筛选能力；剩余标为「玩法兜底」。同时展示粗粒度「玩法族」，降低自由一句话随机性对人工量化的影响。
- `ua_workflows/video_enhancer/pipeline.py`：主流程在分析筛选与 embedding 写库后自动刷新复核看板，默认写 `reports/ve_filter_review_日期.html`，同时覆盖 `reports/ve_cover_dedupe_clusters_日期.html`，便于日常直接打开固定地址复核。
- 已生成 2026-05-12 封面去重人工复核 HTML：`reports/ve_cover_dedupe_clusters_2026-05-12.html`，上半部分展示按新规则识别出的 CLIP 跨日命中（17 条 / 9 簇），下半部分展示原流程已剔除的 ahash/url 指纹跨日命中（20 条 / 18 簇）；封面缓存于 `reports/assets/ve_cover_dedupe_2026-05-12/`。
- 已刷新 2026-05-12 筛选复核 HTML：`reports/ve_filter_review_2026-05-12.html` 与 `reports/ve_cover_dedupe_clusters_2026-05-12.html`，当前统计为 CLIP 跨日 17 条 / 9 簇、指纹跨日 20 条 / 18 簇、一句话命中 33 个素材 / 26 簇 / 39 条证据，其中按当前封面规则已覆盖 12 个、真正玩法兜底 21 个；浏览器验证无「无本地封面」占位。
## 2026-05-11

### [VE] 核心卖点与玩法指纹拆分

- `ua_workflows/video_enhancer/analyze.py`：VE prompt 在「核心卖点」之外新增固定 `【玩法指纹】` 与 `【差异点】` 行；核心卖点继续面向人工阅读和推送展示，玩法指纹专门面向机器去重，要求写清输入对象、关键转换、输出形态和不可省略视觉元素，减少「AI修图/AI美颜」这类泛化描述对去重的干扰。
- `ua_workflows/shared/db/video_enhancer.py` / `pipeline.py`：`daily_creative_insights` 与 `creative_library` 自动迁移新增 `play_fingerprint`、`differentiator`；日内玩法、老玩法、embedding 硬拦截、embedding 候选和日报新玩法判断均优先使用 `play_fingerprint`，缺失时回退 `effect_one_liner`。
- `ua_workflows/shared/db/video_enhancer.py` / `push_feishu.py`：日报「新玩法」不再等于素材条数，也不做每产品固定数量截断；先排除 `exclude_from_bitable` 硬拦截素材，再把 `play_fingerprint` 折成粗粒度玩法族并结合文本/embedding 做聚类，且默认会参考全 VE 近 7 日历史玩法族（不只同 appid）判断是否“完全新玩法”，`new_effect_count` 统计聚类簇数，`new_material_count` 统计这些新玩法簇内素材数，推送展示每个簇的代表素材并标注同玩法素材数。可用 `DAILY_PLAY_CLUSTER_TEXT_THRESHOLD`、`DAILY_PLAY_CLUSTER_EMBEDDING_THRESHOLD`、`DAILY_PLAY_CLUSTER_EMBEDDING_ENABLED=0`、`DAILY_PLAY_GLOBAL_HISTORY_ENABLED=0` 调整。
- `ua_workflows/video_enhancer/sync.py`：主表创建新行时复用日报新玩法聚类结果，把 `日报:新玩法代表`、`日报:新玩法`、`玩法族:...`、`同玩法素材数:N`、`日报:老玩法换素材` 等写入现有「素材标签」字段；不需要更新历史行或新增字段权限。可用 `BITABLE_DAILY_PLAY_TAGS_ENABLED=0` 关闭，`BITABLE_DAILY_PLAY_TAG_LOOKBACK_DAYS` 调整历史窗口。
- `ua_workflows/video_enhancer/pipeline.py`：主流程口径收敛为「前一天全量抓取 → 分析/去重 → 同步去重后的主表素材 → 推送新玩法日报」；旧 UA 方向卡片生成失败不再阻塞主表同步和日报推送，仅跳过旧聚类表同步。
- `ua_workflows/video_enhancer/content_filters.py` / `sync.py`：根据 2026-05-09～2026-05-11 多维表人工反馈校准：将 dating / 免费聊天 / videochat / busty / 附近嫂子等成人交友导流素材纳入硬拦截；将空房间/豪华装修/家居设计等低采纳主题纳入硬拦截（`LOW_FIT_THEME_FILTER_ENABLED=0` 可关）；主表同步默认只写入同日新玩法簇代表，跳过 `日报:同玩法素材` 非代表行，减少同玩法重复抓取噪声。可用 `BITABLE_SYNC_DAILY_PLAY_REPRESENTATIVES_ONLY=0` 临时恢复全量同玩法同步。
- `ua_workflows/video_enhancer/sync.py`：新增采纳率优先同步评分（默认开启，`BITABLE_ACCEPTANCE_PRIORITY_SYNC_ENABLED=1`）：新玩法代表保留；老玩法换素材需命中高采纳主题（球赛抓拍、机甲科幻、手绘漫画、亲情合影、热门模板、人物形象替换、生日写真、明星合影红毯、剧情短片、年龄变化、求职商务照等）才进主表，并写入 `高采纳主题:...` 标签；默认阈值 `BITABLE_ACCEPTANCE_PRIORITY_MIN_SCORE=3`，偏“少而精”。
- 已清空本地 SQLite 中 2026-05-09、2026-05-10 VE 原有分析字段后重新跑分析与去重回写，生成校准报告：`data/ve_play_fingerprint_dedupe_2026-05-09_2026-05-10.json`、`reports/ve_play_fingerprint_dedupe_2026-05-09_2026-05-10.md`。

### [VE] 玩法 embedding 硬拦截与历史回填校准

- `ua_workflows/shared/db/video_enhancer.py`：新增 `creative_library.effect_one_liner_embedding` 与 `daily_creative_insights` 中的 embedding 重复证据 JSON 字段；新增 `apply_effect_embedding_duplicate_filter`，默认只对高置信玩法 embedding 重复做硬拦截（日内 0.95、跨日 0.96），并记录命中来源、相似度、阈值、匹配素材。
- `ua_workflows/shared/db/video_enhancer.py`：日内文本硬拦截默认阈值调到 0.94，老玩法文本阈值仍为 0.94；同义归一补充姿态/姿势、骑行/骑乘、美颜/修图、动画/视频，并保留少量明确签名（名人宝丽来合影、派对修图前后对比、母亲节手绘合影）。
- `ua_workflows/video_enhancer/pipeline.py` / `sync.py`：一键流程与独立同步都会在老玩法文本筛选后补跑 embedding 硬拦截，再做 `embedding重复候选` soft tag；新增 `EFFECT_EMBEDDING_DUP_FILTER_ENABLED=0` 可关闭。
- 已回填 2026-05-07～2026-05-10 历史 VE 分析 JSON 与本地 SQLite 证据字段，生成校准报告：`data/ve_effect_embedding_backfill_2026-05-07_2026-05-10.json`、`reports/ve_effect_embedding_backfill_2026-05-07_2026-05-10.md`。

## 2026-05-09

### [Arrow2] 企业微信周报：新玩法 / 新 Hook

- `ua_workflows/video_enhancer/analyze.py`：Arrow2 精简分析 prompt 新增 `【玩法描述】` / `【Hook描述】` 固定字段，解析结果写入 `play_one_liner` / `hook_one_liner`，并保留 `ad_one_liner` 供同步与周报使用。
- `ua_workflows/shared/db/arrow2.py`：`arrow2_daily_insights` 自动迁移新增 `play_one_liner`、`hook_one_liner` 两列；增量分析入库同步写入新字段。
- `ua_workflows/shared/push/wecom.py`：抽出企业微信 markdown 分段推送公共 helper，VE 多渠道推送改为复用该 helper。
- `ua_workflows/arrow2/push_weekly_wecom.py` / `scripts/run_arrow2_weekly_wecom.py`：新增 Arrow2 周报入口，默认统计昨日往前 7 天的「最新创意」，按同 `appid` 过去 28 天历史判断新玩法/新 Hook，支持 `--workflow`、`--lookback-days`、`--dry-run`、`--no-llm`，Webhook 读取 `ARROW2_WECOM_BOT_WEBHOOK` 或 `WECOM_BOT_WEBHOOK`。

### [VE] 同步前老玩法/成人风险拦截 + 单产品硬截断默认关闭

- `ua_workflows/video_enhancer/content_filters.py`：新增成人/色情风险文本拦截，综合 `analysis`、标题、正文、标签、`effect_one_liner` 等字段判断；命中后设置 `exclude_from_bitable` / `exclude_from_cluster`，避免仅因「核心卖点」没写出风险而漏筛。
- `ua_workflows/shared/db/video_enhancer.py`：新增 `apply_old_effect_bitable_filter` 与 `apply_intraday_effect_bitable_filter`；同步前按同 `appid` 近 7 日历史玩法标记「老玩法重复」（主表拦截阈值默认 `OLD_EFFECT_SIMILARITY_THRESHOLD=0.94`，比日报新玩法口径更保守），同日同批次相似玩法仅保留展示估值更高的代表素材并标记「日内玩法重复」（默认 `INTRADAY_EFFECT_SIMILARITY_THRESHOLD=0.94`）。`normalize_effect_one_liner` 补入少量同义词归一（如 名人/明星、合照/合影、宝丽来/拍立得、姿态/姿势、骑行/骑乘、修脸/修图/修复/美颜、动画/视频）。
- `ua_workflows/shared/db/video_enhancer.py`：新增 `effect_one_liner_embedding` 存储与 `apply_effect_embedding_duplicate_filter`，对高置信同义玩法做硬拦截并记录阈值证据（默认日内 `EFFECT_EMBEDDING_INTRADAY_HARD_THRESHOLD=0.95`，跨日 `EFFECT_EMBEDDING_CROSSDAY_HARD_THRESHOLD=0.96`）；`apply_embedding_duplicate_candidate_tags` 对未被硬拦截的素材做 soft candidate 标记，默认 `EMBEDDING_DUP_CANDIDATE_THRESHOLD=0.90`，只加 `embedding重复候选` 标签和 `embedding_duplicate_candidate` 详情。
- `ua_workflows/video_enhancer/pipeline.py` / `sync.py`：一键流程与单独同步均补跑成人风险、日内玩法、老玩法、embedding 硬拦截、embedding 候选、已投放等标记，保证独立同步不绕过筛选；可用 `INTRADAY_EFFECT_FILTER_ENABLED=0` / `OLD_EFFECT_BITABLE_FILTER_ENABLED=0` / `EFFECT_EMBEDDING_DUP_FILTER_ENABLED=0` / `EMBEDDING_DUP_CANDIDATE_ENABLED=0` 分别关闭；`sync.py` 同步主表时会把 analysis `material_tags` 合并进「素材标签」字段。
- `ua_workflows/video_enhancer/analyze.py`：复核并收紧 VE prompt：`核心卖点` 要求补充可区分场景/对象/风格/呈现方式，避免「AI修图/AI美颜」过泛导致误去重；新增固定 `【风险标签】` 行（成人色情/擦边露肤/版权名人/产品不适配/低质/无明显风险）和 `【风险等级】` 行（低/中/高），标签解析进 `material_tags` 供多维表「素材标签」复核，等级写入单独「风险等级」列，其中成人色情仍由内容过滤硬拦。
- `ua_workflows/video_enhancer/analyze.py` / `sync.py`：VE prompt 新增固定 `【Hook解析】` 与 `【脚本口播】` 输出，分析正文要求更侧重前 1~3 秒 Hook 与脚本/字幕/口播提炼，减少逐帧冗余；若广大大 raw/detail 后续带 `script` / `transcript` / `subtitle` / `ocr_text` 等字段，会一并喂给模型。同步主表自动新增并写入「Hook解析」「脚本/口播」「风险等级」三列。
- `ua_workflows/video_enhancer/crawl.py`：单产品 `>10` 只保留 Top10 的硬截断默认关闭，改为保留日期命中素材，后续交给封面去重/玩法筛选/同步前排除处理；如需临时恢复可设 `VIDEO_ENHANCER_PER_PRODUCT_TRUNCATE_ENABLED=1`。

### [VE] 新素材 / 新玩法 / 持续发力日报口径落地

- `ua_workflows/shared/db/video_enhancer.py`：新增 `load_daily_material_report` 公共聚合口径，仅用于 **Video Enhancer**：新素材要求 `creative_library.first_target_date = target_date` 且同 `appid` 下 `effect_one_liner` 过去 N 日（默认 7）无精确或相似命中；老玩法换素材仅进 `old_play_items` 复核，不计入新素材；持续发力聚合 VE 封面/URL/ahash/玩法跨日信号，并按产品限制展示条数，不读取 Arrow2 报告。
- `ua_workflows/video_enhancer/push_feishu.py` / `push_multichannel.py`：日报标题改为「新素材 / 新玩法 / 持续发力」三指标；老玩法换素材在标题中展示「未计入新素材」数量，不再混入新素材列表；持续发力小节开始展示跨日重复素材/玩法。
- `ua_workflows/video_enhancer/acceptance.py`：验收新增日报素材报告摘要，检查新素材数、`effect_one_liner` 覆盖率（`ACCEPTANCE_MIN_EFFECT_COVERAGE`，默认 0.9）和持续发力信号生成情况。

## 2026-05-08

### [Arrow2] 修复 pipeline 封面去重 import（cron Step3 崩溃）

- `ua_workflows/arrow2/pipeline.py`：旧名 `arrow2_cover_style_intraday` 已改为 `from ua_workflows.arrow2.cover_dedupe import apply_arrow2_cover_style_dedupe`，避免 `run_arrow2_latest.py` 在 `arrow2_creative_library` 写入后 `ModuleNotFoundError`。

## 2026-05-07

### [运维] macOS crontab 推荐入口（避开工位上其它 7–10 点定时任务）

- 新增可执行脚本：`scripts/cron_ai_video_enhancer_daily.sh`、`scripts/cron_ai_arrow2_latest_daily.sh`、`scripts/cron_ai_arrow2_exposure_wed_sat.sh`（内含 `TZ=Asia/Shanghai`、`PYTHONUNBUFFERED=1`，日志请由 crontab 重定向到 `logs/`）。
- 与用户本机 `crontab -l` 错峰：`05:20` 跑 VE、`11:10` 跑 Arrow2 昨日最新、`14:20` 仅在周三/六跑 Arrow2 展示估值（与每日两条**叠加**）。
- 三条均要求：分析 + 同步（VE 为 `run_video_enhancer.py` 默认全流程；Arrow2 脚本传 `--analyze`）。实际路径以本机仓库根目录为准。
- 已在当前登录用户的 `crontab` 写入标记块 `# BEGIN ai- ua_workflows` … `# END ai- ua_workflows`（若改仓库路径须同步改这三行）。
- **`docs/` 运维文档**：`cron-schedules.md`、`workflows.md`、`setup-and-data.md`、`docs/README.md` 索引；根 `README.md` 已增加「项目文档」表链出。

## 2026-05-06

### [项目结构] 三工作流包化重构

- 项目已重构为围绕三条生产工作流维护：**Video Enhancer**、**Arrow2 latest_yesterday**、**Arrow2 exposure_top10**。
- 核心代码迁入 `ua_workflows/`：
  - `ua_workflows/video_enhancer/`：VE 抓取、DOM 补全、分析、封面去重、同步、推送、验收。
  - `ua_workflows/arrow2/`：Arrow2 pipeline、detail-v2 爬虫、封面去重、飞书同步。
  - `ua_workflows/shared/`：广大大、LLM、媒体解析、SQLite、路径配置等共享基础设施。
- `scripts/` 只保留三个整合入口：
  - `scripts/run_video_enhancer.py`
  - `scripts/run_arrow2_latest.py`
  - `scripts/run_arrow2_exposure.py`
- 另保留三个爬取冒烟测试入口，只跑一个产品、只落 raw JSON、不入库/不同步，且支持 `--headed` / `--headless`：
  - `scripts/test_video_enhancer_crawl.py`
  - `scripts/test_arrow2_latest_crawl.py`
  - `scripts/test_arrow2_exposure_crawl.py`
- 旧 UA、hot rank/new rank、playable、调试探针、一次性 backfill/preview/test 脚本已归档到 `archive/removed_scripts_2026_05_06/`。
- 后续新增或修改功能时，优先在 `ua_workflows/` 内按工作流归属维护；`scripts/` 只放整合入口或无副作用的冒烟测试入口。

## 项目结构原则

本项目围绕**三条生产工作流**展开，所有变更记录和汇报均按工作流分别组织：


| 工作流                | 简称              | 核心脚本                             | 用途                              |
| ------------------ | --------------- | -------------------------------- | ------------------------------- |
| **Video Enhancer** | VE              | `scripts/run_video_enhancer.py`  | 广大大竞品素材抓取 + 灵感分析 + 日报推送         |
| **Arrow2 最新**      | Arrow2 latest   | `scripts/run_arrow2_latest.py`   | 广大大 Arrow2 竞品每日最新抓取 + 可选分析 + 同步 |
| **Arrow2 展示估值**    | Arrow2 exposure | `scripts/run_arrow2_exposure.py` | 广大大 Arrow2 竞品展示估值抓取 + 可选分析 + 同步 |


- **变更日志按工作流分组**：每条记录标注所属工作流（VE / Arrow2 latest / Arrow2 exposure）；若同时涉及多条，分别描述。
- **验收报告不追踪**：`reports/` 下的 `*_acceptance.md` 为工作流自动产物，不需要在变更日志中追踪或记录。
- **共享基础设施**（如 `llm_client.py`、`video_enhancer_pipeline_db.py`、`cover_style_intraday.py`）变更时注明影响的工作流。

---

## 2026-04-29

### [Arrow2] 周趋势推送 + 数据补齐

- `**scripts/arrow2_weekly_trend.py`**（新增）：从 `arrow2_daily_insights` 汇总指定日期范围内素材的一句话描述，让 LLM 生成趋势分析报告，分两条飞书卡片推送（新素材趋势 / 展示估值趋势）。支持 `--workflow` 指定只推某一条、`--dry-run` 只打印不推送。
- 补入库 2026-04-27 最新创意 41 条（此前中断后未入库），重新同步飞书多维表（122 条）。
- 运行 2026-04-28 每日最新全流程（38 条入库 + 同步飞书）。

---

## 2026-04-28

### [Arrow2] 爬虫统一替换 + 工作流对齐

详见「当前 Arrow2 工作流」章节。本次变更要点：

- 爬虫统一为 `test_arrow2_first_card_fields.py`（detail-v2 逐张点击 + appid 精确匹配 + 多重重试）
- ad_key 去重改为新数据覆盖旧数据
- 展示估值每产品限 20 条
- 修复 `sync_arrow2_to_bitable.py` 的 import 和附件上传
- 新增 `guangdada_detail_url.py`（SPA 链接构建）

---

## 2026-04-15

### [Arrow2] `first_seen` 早于目标日则提前停滚、停 DOM 点卡

- `**run_search_workflow.py`**：新增 `_oldest_first_seen_ymd_among_creatives`；`**_search_one_keyword`** 可选 `stop_scroll_if_oldest_first_seen_before_ymd`——napi 已合并行中最早 `first_seen`（北京日）严格早于该 ISO 日（与「仅该日 `first_seen`」目标一致）时不再向下滚。`**_click_cards_for_details**` 可选 `stop_after_detail_first_seen_before_ymd`：某次详情里 `first_seen` 早于该日则关弹层后不再点后续卡（新→旧）。`**run_arrow2_batch` → `_collect_keyword_crawl_result_arrow2_latest_dom`**：在 `filter_yesterday_only` 的 latest+DOM 路径上传入 `first_seen_ymd`。环境变量 `**ARROW2_FIRST_SEEN_EARLY_STOP=0`** 关闭上述早停（默认开）。
- `**run_search_workflow.py**`：`_merge_prefer_dom_detail` 改为按 **展示估值工作流入库口径** 合并：latest+DOM 时，`**impression` / `all_exposure_value` / `heat` / `days_count` / `new_week_exposure_value` 固定优先取同 `ad_key` 的 napi 行**（与 exposure_top10 直接用列表 creative 入库一致）；detail 继续用于补正文、素材链接、首末次时间等详情字段。`preview_img_url`、`platform`、`advertiser_name`、`resource_urls` 等基础字段若 detail 缺失仍会回填，避免性能指标被 detail 覆盖或丢失。
- `**test_arrow2_competitors.py`**：补传 `keyword_product` 到 `run_arrow2_batch`。此前终端 `print_arrow2_matched_creatives` 使用的 `all_creatives` 在 latest_yesterday 调试场景里只过了日期筛选、**未走 keyword→广告主匹配**，会短暂打印出 `Arrow Maze` / `Arrows Out` 等相邻广告主；而 raw 落盘前又在脚本里二次 `advertiser_matches_product`，导致“终端条数”和“raw 条数”不一致。现已统一为：**终端打印 / run 返回 / raw 落盘**都使用同一广告主过滤口径。
- `**config/arrow2_competitor.json` + `run_search_workflow.py`**：`latest_yesterday` 去掉 `max_creatives_per_keyword: 30`，改为**默认不按每词 30 条截断**；同时 `_collect_keyword_crawl_result_arrow2_latest_dom` 在未显式设置环境变量 `**ARROW2_DOM_CLICK_MAX_CARDS`** 时，默认把**当前 DOM 已加载的卡片全部点击**（而不是固定 100 张内或更小 cap）。若后续想限条，显式设该环境变量为正整数即可。
- `**run_search_workflow.py`**：按最新确认口径调整 latest+DOM 点卡停止条件。`_collect_keyword_crawl_result_arrow2_latest_dom` 不再把 napi 中“已出现早于目标日”作为**点卡阶段**的提前停止信号；改为：**先从页首逐张点击当前已加载卡片**，只有 `_click_cards_for_details` 在真正点开的某张详情里发现 `first_seen < target_date` 时，才停止后续点卡。并为单张 click 增加 **1 次重试**，降低“页面上有卡但本次没点开/没拦到 detail”导致的漏点。
- `**test_arrow2_competitors.py`**：新增 `**--pause-per-product`**，主测试脚本现已统一支持：单产品（`--products ...`）、全产品（`--all-products`）、逐词暂停（`--debug-step-products`）、逐产品暂停（`--pause-per-product`）。开启 `--pause-per-product` 时自动等同 `--debug`，并默认关闭流程末尾额外暂停。
- `**test_arrow2_yesterday_all_products_headed.py**`：不再维护独立实现，改为**兼容包装器**，内部直接转调：`test_arrow2_competitors.py --all-products --pull-only latest_yesterday --debug --pause-per-product`。后续单/全产品测试请优先使用主脚本。
- `**run_search_workflow.py`（再调整）**：根据用户最新要求，`latest_yesterday` 已重新切回**卡片点击主路径**：`run_arrow2_batch` 对 `latest_yesterday` 调 `_collect_keyword_crawl_result_arrow2_latest_dom`，搜索/滚动后对**当前页面 DOM 卡片逐张点击**拿 detail；最终 `all_creatives` 以 **DOM 基础卡片 + detail 覆盖结果** 为主，不再把 `napi creative_list` 作为 latest_yesterday 的最终主结果源。`napi` 仍用于列表加载/滚动与兜底回退。调试输出新增：广告主匹配后全部卡、按 `first_seen=target_date` 筛后卡，以及逐张点击日志（含 `UTC+8` 时间）。

---

## 文档与代码对齐说明（2026-04-13 修订）

以下历史段落曾描述**已变更**行为，以本说明为准：


| 主题                        | 原文档说法                                                                               | 当前代码事实                                                                                                                                                                                                                                                                                                                                                                                                             |
| ------------------------- | ----------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **灵感「套路」筛选**              | 同一次多模态输出 `flower_background` / `bw_blockbuster` 等，或 `config/style_filters.json` 配置化 | **已移除**。`analyze_video_from_raw_json.py` 仅输出纯文本灵感（或解析 JSON 中的 `analysis`）。`style_filter_match_summary` 等字段可仍为列占位，恒为空。**「我方已投」**由 `launched_effects_db.apply_launched_effects_filter`（主流程 Step 2.9）及 `sync_raw_analysis_to_bitable_and_push_card.py` 补标处理。                                                                                                                                                          |
| **一键主流程 Step 2c 多维去重进分析** | `get_deduped_items_for_analysis` → `*_dedup_report.json`，仅去重后子集进分析                  | `**workflow_video_enhancer_full_pipeline.py` 未调用**该函数。入库仍用全量 `items`；分析队列为 **准入 + 未命中历史成功缓存** 的 `pending_items`（见 `[step:analysis-queue]`）。`get_deduped_items_for_analysis` 仍保留在 `video_enhancer_pipeline_db.py`（供封面指纹等复用逻辑），**不等同于当前一键分析入队口径**。                                                                                                                                                                   |
| **文案去重**                  | `text_fingerprint` SHA1 相同归组                                                        | **已关闭**。`TEXT_FINGERPRINT_DEDUP_ENABLED` 默认 `0`；当前只保留封面图去重 + effect_one_liner 去重。                                                                                                                                                                                                                                                                                                                                  |
| **UA 建议**                 | `_build_single_ua_suggestion` → `ua_suggestion_single` + `ad_one_liner`             | **已移除**（2026-04-29）。VE 流程不再生成单条 UA 建议；DB 列 `insight_ua_suggestion` 保留但不再写入。                                                                                                                                                                                                                                                                                                                                        |
| **推送格式**                  | 方向卡片（`_render_card_markdown`）                                                       | **已替换为新日报格式**（2026-04-29）。标题「AI工具竞品日报 + 日期」；按产品分组；每条新素材用 effect_one_liner 做可点击链接。                                                                                                                                                                                                                                                                                                                                  |
| **定时任务**                  | 每天 10:30 crontab                                                                    | `**daily_video_enhancer_workflow.sh` / `daily_ua_job.sh` 注释为手动执行**；若本机仍挂 crontab 为旧配置，以实际 shell 为准。                                                                                                                                                                                                                                                                                                                |
| **OpenRouter 用量**         | 仅 shell 内 `curl`                                                                    | **主流程**在 `workflow_video_enhancer_full_pipeline.py` 内调用 `llm_client.print_openrouter_key_meter`（工作流开始/结束）；与 `.env` 中 `OPENROUTER_METER` 等一致。                                                                                                                                                                                                                                                                       |
| **特效库语义阈值**               | 文档某处写默认 0.80                                                                        | `**launched_effects_db.py` 中默认 `LAUNCHED_EFFECTS_MATCH_THRESHOLD` 为 0.65**（以代码与环境变量为准）。                                                                                                                                                                                                                                                                                                                            |
| **封面日内 / 跨日向量去重**         | 多模态抽封面 + 文本 LLM 聚类；跨日仅载入「昨日」`insight_cover_style` 与今日同 appid 比较                     | **CLIP**（`clip-ViT-B-32`）封面向量并查集，`cosine ≥ COVER_VISUAL_DEDUP_THRESHOLD`（默认 **0.75**），无 LLM。跨日参照为 `**target_date` 之前连续 N 个日历日**（默认 **7**，`COVER_STYLE_HISTORY_LOOKBACK_DAYS`，上限 60），`load_cover_style_rows_for_dates_grouped_by_appid` 批量读库；簇内历史胜出时 `reason` 仍为 `**cover_style_cluster_vs_yesterday`**（兼容旧筛选）。指纹层不变：`COVER_STYLE_CROSS_DAY_FINGERPRINT_ENABLED` + `crossday_filter_items_against_creative_library`。 |


---

## 2026-03-31

### [VE] `analyze_video_from_raw_json`：格式异常时多模态重试 3 次，纯文本 JSON 修复改默认关

- **新**：`VIDEO_ANALYSIS_MULTIMODAL_FORMAT_RETRIES`（默认 3，0=关）—— 解析/正文过短等触发 `_needs_json_or_format_repair` 时，**串行**再调多模态（视频/图），不叠加 `PARALLEL_SHARDS`；成功打标 `inspiration_enrichment=multimodal_format_retry`。
- **改**：`VIDEO_ANALYSIS_JSON_REPAIR` 默认由开改为**关**；仅多模态仍失败时可选开启。纯文本收束**去掉 16k 截断**（全量原文进兜底 prompt）。
- 模块头 docstring 已同步说明。

### [VE] 飞书主表 `视频` 附件 + 恢复 `launched_effects_db.py`（我方已投 + embedding）

- `**sync_raw_analysis_to_bitable_and_push_card.py`**：`FIELD_DEFS` 增「视频」(type=17)；对 `video_duration>0` 且可直链下载的 `pick_video_url` 拉流上传（`VIDEO_BITABLE_MAX_MB` / `VIDEO_BITABLE_UPLOAD`）；主表同步前对 `analysis["results"]` 调用 `apply_launched_effects_filter`（与全链路 Step 2.9 一致）。
- `**launched_effects_db.py`**：仓库中曾为空导致 Step 2.9 未生效；已补全：飞书已投放表拉取 + 本地缓存 + 关键词子串 + `**llm_client.call_embedding` 语义 cosine ≥ `LAUNCHED_EFFECTS_MATCH_THRESHOLD`（默认 0.65）**；无飞书时降级 `data/launched_effects_descriptions_only.json`。
- `**workflow_video_enhancer_acceptance.py`**：已补全实现；`run_acceptance_after_workflow` 汇总 raw/analysis/方向卡片/封面与已投放 step/推送表行数/历史截断量，写 `data/workflow_video_enhancer_{date}_acceptance.json` 与 `reports/…_acceptance.md`；环境变量与 AGENTS 中「工作流验收」说明一致。未增加 `daily_video_enhancer_acceptance` 表（文档曾提及，当前以文件落盘 + 既有 DB 查询为主）。

---

## 2026-04-20

### [Arrow2] `latest_yesterday`：滚动越过「昨日」边界 + 竞品配置

- `**run_search_workflow._search_one_keyword`**：可选 `scroll_until_older_than_date`（目标昨日 YYYY-MM-DD）。在「最新创意」下持续滚动直到底层 napi 合并结果里出现 first_seen 或 created_at（UTC+8）早于该日 的素材（认为已扫完目标日窗口），或仍受「连续 3 轮无新批次」与**约 48 轮**上限约束。之后 `filter_yesterday_only` 仍只保留 `first_seen=昨日`。
- `**run_arrow2_batch`**：对 `order_by=latest` 且 `filter_yesterday_only` 的 pull_spec，默认开启上述滚动（`scroll_until_past_target_date` 缺省为真）；显式设为 `false` 可恢复较短轮数 + idle 的旧停法。
- `**config/arrow2_competitor.json`**：移除 `com.arrow.out` 行；`latest_yesterday` 中说明 `scroll_until_past_target_date`。
- `**test_arrow2_competitors.py`**：未指定 `--products` 时默认只跑配置中**第一个**产品；`--all-products` 跑全部。
- **Arrow2 `order_by=latest`（含 latest_yesterday）**：与 `exposure` 一样改为 `**arrow2_build_result_from_dom_after_search`**，列表为 **DOM 卡片 + detail-v2**（`list_source=dom`）；`filter_yesterday_only` 时单词默认多取卡（`max_n` 默认 120、硬顶 200）再按 first_seen 筛昨日。旧无 `pull_specs` 矩阵里 `latest` 亦走 DOM。
- **DOM 广告主过滤**：`run_arrow2_batch` 增加 `keyword_product`（搜索框 key → 与 config `match` 一致的产品名）；`arrow2_build_result_from_dom_after_search` 在 detail-v2 后调用 `advertiser_matches_product` 剔除与目标产品不一致的卡。`test_arrow2_competitors` 自动传入各条目的 `product`。

---

## 2026-04-14

### [VE] 封面去重改为 CLIP 向量 + 7 日历史窗口

**动机**：去掉封面多模态与封面聚类 LLM，改为与主库一致的 **CLIP 封面向量**（`creative_library.cover_embedding`），阈值可调、成本更低。

**实现概要**（`scripts/cover_style_intraday.py`）：

- **指纹**（默认开，`COVER_STYLE_CROSS_DAY_FINGERPRINT_ENABLED`）：在算 CLIP 前仍调用 `crossday_filter_items_against_creative_library`，与文首「封面跨日指纹」一致。
- **向量**：优先读库 `load_cover_embedding_blob_map_by_ad_keys`；缺失则 `cover_embedding.compute_cover_embedding_vector_from_url` 仅算向量、不写库（当日首轮入库前新 `ad_key` 可能无行；后续 `run_cover_embedding_job` 等仍会补 `cover_embedding`）。
- **占位入库**：`insight_cover_style` 写入 CLIP 占位 JSON（如 `style_type`: 「CLIP视觉」），`upsert_single_cover_style_insight` 逐条更新。
- **聚类**：同 `appid` 内并查集，边条件 `cosine_similarity ≥` `**COVER_VISUAL_DEDUP_THRESHOLD`**（默认 0.75）；簇内保留 `all_exposure_value` 最大一条；若最优来自**历史窗口**则剔今日（`cover_style_cluster_vs_yesterday`），若最优为今日则日内互斥（`cover_style_cluster`）。报告 `cover_dedupe_mode`: `**clip_visual`**。
- **历史窗口**：`COVER_STYLE_CROSS_DAY_ENABLED` 开启时，加载 `**target_date` 前连续 N 日**（`**COVER_STYLE_HISTORY_LOOKBACK_DAYS`**，默认 **7**，即 T-1…T-7）内非空 `insight_cover_style`；`video_enhancer_pipeline_db.load_cover_style_rows_for_dates_grouped_by_appid` 合并多日，同一 `ad_key` 取 exposure 更高的一条。报告含 `cross_day_history_dates`、`cross_day_history_lookback_days`；`cross_day_prev_date` 仍为 **T-1**（兼容旧读者）；`per_appid` 增加 `history_ref_count`，并保留 `yesterday_ref_count` 与同长度（兼容）。

**环境变量**（`.env.example` 已列）：`COVER_VISUAL_DEDUP_THRESHOLD`、`COVER_STYLE_HISTORY_LOOKBACK_DAYS`；其余 `COVER_STYLE_INTRADAY_ENABLED`、`COVER_STYLE_CROSS_DAY_*`、`COVER_STYLE_WORKERS` 仍适用。

**联动**：`workflow_video_enhancer_full_pipeline.py` / `workflow_video_enhancer_steps.py` / `daily_video_enhancer_workflow.sh` / `run_cover_style_intraday.py` / `workflow_video_enhancer_acceptance.py` 中用户可见文案已由「多模态封面」改为 **CLIP 封面**。

**注意**：历史窗口内条目需库内已有 `**cover_embedding`** 才参与向量聚类；过去未跑封面或未回填向量时，参照会变弱。

---

## 2026-04-13

### [VE] 工作流验收：`workflow_video_enhancer_acceptance.py`

- **用途**：对指定日期的 Video Enhancer 产物做可重复检查（文件存在与 JSON 结构、本次新分析成功率阈值、封面/语义等异常提示、方向卡片字段与参考链接、推送表行数、`daily_video_enhancer_workflow_{date}.log` 中子进程错误线索、近 N 日截断后条数对比）。
- **输出**：`data/workflow_video_enhancer_{date}_acceptance.json`、`reports/workflow_video_enhancer_{date}_acceptance.md`。
- **SQLite**：`daily_video_enhancer_acceptance` 表（`init_db()` 自动补齐），`upsert_daily_video_enhancer_acceptance` 写入；另增 `query_filter_log_post_total`、`count_daily_ua_push_rows`、`load_filter_log_post_totals_lookback` 供验收与历史对比。
- **接入**：`workflow_video_enhancer_full_pipeline.py` 在「全流程正常结束」或「灵感分析失败提前 return」前调用 `run_acceptance_after_workflow`；`workflow_video_enhancer_steps.py` 的 `push_sync` 结束调用。
- **环境变量**：`ACCEPTANCE_ENABLED`（默认开启，设为 `0`/`false` 关闭）、`ACCEPTANCE_BLOCK_ON_FAIL`（硬失败时进程退出码 2）、`ACCEPTANCE_MIN_SUCCESS_RATE`、`ACCEPTANCE_COVER_REMOVAL_WARN`、`ACCEPTANCE_LOOKBACK_DAYS`、`ACCEPTANCE_LOW_VS_MEAN`、`ACCEPTANCE_HIGH_VS_MEAN`、`ACCEPTANCE_EXIT_ON_SOFT`、`ACCEPTANCE_STRICT`（详见脚本文件头注释）。
- **飞书通知**：配置 `ACCEPTANCE_FEISHU_WEBHOOK`（群机器人 webhook，可与业务 UA 卡片 webhook 分开）后，每次验收结束发送 **interactive 卡片**（紧凑摘要：状态/得分/问题/阶段一行摘要）；`ACCEPTANCE_FEISHU_ENABLED=0` 关闭；`ACCEPTANCE_FEISHU_STRICT=1` 时推送失败会抛错。CLI：`--feishu-webhook`、`--no-feishu`。

---

## 2026-04-09

### [VE] 逐条主表同步：`sync_raw_analysis_to_bitable_and_push_card.py`

- **我方已投放**：在 `main()` 读入 analysis 后**先**调用 `apply_launched_effects_filter`，单独跑同步脚本也会排除已投放命中（不再依赖必须先跑 `cluster_store`）。`apply_launched_effects_filter` 对历史 JSON 中已有 `launched_effect_match` / 「我方已投放」标签但未写 `exclude_from_bitable` 的行**补写**主表排除。
- **封面图附件（旧脚本当时口径）**：路径以 `.image` 结尾时，下载**优先**用 `.png` 替换后的 URL；`upload_image_as_attachment` 中飞书附件 `file_name` 对 `.image` 后缀**强制改为** `.png`，避免多维表里仍显示 `.image`。当前 VE 主同步媒体归一口径见 2026-06-11 条目。
- **视频（旧脚本当时口径）**：仅「视频链接」文本列，**无**视频附件列。当前 VE 主同步已支持视频附件，见 2026-06-11 条目。

---

## 2026-04-08

### [VE] `llm_client.call_vision` 文本降级时剥离媒体 URL

**问题**：部分图片素材在多模态与纯文本路径均被 OpenRouter 返回 403（TOS）；根因之一是视觉失败后 `call_text` 仍携带与请求相同的图片/视频直链，网关对同一 URL 再次拦截。

**改动**（`scripts/llm_client.py`）：新增 `_text_fallback_user_text`，在「全部视觉模型失败 → 纯文本降级」时，将 `user_text` 中的 `media_url` 替换为简短中文说明，避免重复提交直链；分析可仅依据标题、文案与指标推断。

---

## 2026-04-03

### [VE] 我方已投放特效库匹配 `scripts/launched_effects_db.py`（新增）

**背景**：我方有一个飞书多维表（194 条记录），记录了已上线到 Evoke/Kavi/Toki/Avatar 各产品的特效/主题。竞品抓取的素材中可能包含与我方已投放特效相同概念的创意——需要识别并标记。

**数据源**：飞书多维表 `JhMMbPlSUaE6G7siF0RcQn6jnlg` / `tblo36ykG6Pl2X04`，含字段：说明（特效名+描述）、类型、来源、已上线产品、日期。

**新增文件 `scripts/launched_effects_db.py`**：

- `sync_launched_effects()` — 通过 FEISHU_APP_ID/SECRET 拉取飞书多维表全量记录。
- **24h 本地缓存**：`data/launched_effects_cache.json`，过期自动重新拉取；网络异常时降级到过期缓存。
- `_extract_effect_names(desc)` — 从说明字段提取特效名关键词（英文名 + 中文名 + 【brackets】内名 + 列表名），当前提取 **481 个关键词（303 唯一）**。
- **双层匹配** `match_against_launched_effects(analysis, title, body)`：
  - **Layer 1 — 关键词硬匹配**：特效名（英文/中文）在竞品 title+body+analysis 中出现即命中（精度极高）。
  - **Layer 2 — 语义嵌入匹配**：对特效说明前 200 字做 embedding，与竞品分析文本 cosine similarity ≥ 0.65 即命中。默认使用**本地模型** `BAAI/bge-small-zh-v1.5`（中英双语、512 维、33M 参数、零 API 成本）。实测：纯中文"圣诞老人打视频电话"（无任何关键词命中）→ 语义层成功匹配到 Santa Call 特效（sim=0.72）。
- `apply_launched_effects_filter(combined_results)` — Pipeline 集成入口；命中素材设 `exclude_from_cluster=True` + 标签 `我方已投放: XXX特效`。

**Embedding 基础设施升级**（`scripts/llm_client.py`）：

- `call_embedding()` 改为**本地模型优先**策略：先尝试 `sentence-transformers` + `BAAI/bge-small-zh-v1.5`，不可用时降级到 API。
- 模型进程内单例加载（首次调用 ~10s，后续调用 <10ms）。
- 环境变量 `EMBEDDING_PROVIDER=api` 可强制走 API；`LOCAL_EMBEDDING_MODEL` 可切换本地模型。

**Pipeline 集成**：

- 全流程 `workflow_video_enhancer_full_pipeline.py` Step 2.9（语义去重之后、方向卡片之前）。
- 分步流程 `workflow_video_enhancer_steps.py` `step_cluster_store` 在生成方向卡片之前。
- 命中素材不进入方向卡片生成（`exclude_from_cluster`），但仍保留在 analysis JSON 和多维表中供查看。

**环境变量**：

- `LAUNCHED_EFFECTS_BITABLE_URL`（默认内置）、`LAUNCHED_EFFECTS_MATCH_THRESHOLD`（代码默认 **0.65**，可通过 `.env` 覆盖）、`LAUNCHED_EFFECTS_ENABLED`（默认开启）。

---

### [VE] 架构优化：五项改进（统一 LLM 层 / 配置化套路已废弃 / 语义嵌入 / 历史卡片 / 趋势信号）

#### 1) 统一 LLM 调用层 `scripts/llm_client.py`（新增）

**背景**：`analyze_video_from_raw_json.py`、`cover_style_intraday.py`、`generate_video_enhancer_ua_suggestions_from_analysis.py` 各自维护 ~80-200 行的 LLM 重试/降级逻辑，高度重复。

**变更**：

- 新增 `scripts/llm_client.py`，提供 `call_text()`、`call_vision()`、`call_embedding()` 三个统一接口。
- **Model fallback chain**：按 `vision_models` 列表依次尝试，全部失败自动降级到 `text_fallback_system` + 纯文本。
- **Circuit breaker**：某模型因区域限制返回 403 后，整个进程不再重试该模型。
- **Usage tracking**：所有调用的 token 用量集中在 `llm_client._usage_patch`，各脚本结束时 `flush_usage(target_date)` 统一写入 `ai_llm_usage_daily`。
- `analyze_video_from_raw_json.py` 中 `_call_llm_text` / `_call_llm_video` / `_call_llm_image` 精简为 3-5 行的 `llm_client` 委托，删除 ~200 行重复重试逻辑。
- `generate_video_enhancer_ua_suggestions_from_analysis.py` 中 `_call_llm` 精简为 1 行 `llm_client.call_text()`。
- `cover_style_intraday.py`：**（2026-04-14 起）** 封面步骤已改为 CLIP 向量聚类，**不再**经 `llm_client` 做多模态封面；见 **§2026-04-14**。

#### 2) 套路过滤配置化 `config/style_filters.json`（**已废弃**）

**历史（2026-04-03）**：曾新增 `config/style_filters.json`，并在 `analyze_video_from_raw_json.py` 中动态加载，与灵感分析同一次多模态输出套路 JSON。

**当前**：该能力**已从分析脚本移除**（见上文「文档与代码对齐说明」）。`config/style_filters.json` 若仍存在，仅为遗留文件，**分析流程不再读取**。若需「我方已投」类排除，**仅**依赖 `launched_effects_db` 与下游同步补标。

#### 3) 语义嵌入去重

**当前状态（2026-05-15）**：本节所述 `analysis_embedding` / `semantic_crossday_filter` 已下线，代码不再创建、写入或读取 `creative_library.analysis_embedding`；玩法相似判断改由 `play_fingerprint` / `effect_one_liner` 文本与 `effect_one_liner_embedding` 链路承担。以下内容仅作历史背景。

**背景**：现有跨日去重仅靠 ad_key / URL / ahash，无法捕获「同一创意思路、不同素材资产」的重复。

**变更**：

- `llm_client.py` 新增 `call_embedding()`（默认 `openai/text-embedding-3-small`）、`cosine_similarity()`、`embedding_to_bytes()` / `bytes_to_embedding()`。
- `video_enhancer_pipeline_db.py` 新增：
  - `creative_library.analysis_embedding` BLOB 列（`init_db()` 自动补齐）。
  - `upsert_analysis_embedding(ad_key, blob)` — 单条写入嵌入向量。
  - `load_embeddings_for_crossday(target_date, appid)` — 加载历史嵌入。
  - `semantic_crossday_filter(target_date, items)` — 同 appid 内对已有分析的素材做 cosine similarity ≥ 0.92 的语义去重。
- `workflow_video_enhancer_full_pipeline.py` Step 2.7：分析结果同步 creative_library 后，为每条有分析文本的素材计算嵌入并存储。
- `workflow_video_enhancer_steps.py` `step_analyze_store` 末尾同理。
- 环境变量 `EMBEDDING_MODEL`（可选）、`SEMANTIC_DEDUP_THRESHOLD`（默认 0.92）。

#### 4) 方向卡片注入历史上下文

**背景**：每天的方向卡片独立生成，不知道昨天/前天推了什么方向，可能重复建议。

**变更**：

- `video_enhancer_pipeline_db.py` 新增 `load_recent_direction_cards(target_date, n_days=3)` — 从 `daily_ua_push_content` 读取近 N 天方向卡片摘要。
- `generate_video_enhancer_ua_suggestions_from_analysis.py` `_build_prompt()` 新增 `target_date` 参数；若有历史卡片，在提示词中注入「近期已推送的方向卡片」段落，并要求：
  - 与历史重叠的标注「持续推荐」+ 今日新增洞察
  - 优先输出与历史不同的新方向

#### 6) Code Review 修复（4 项）

根据 `reports/claude_code_review_2026-04-03.md` 评审意见，修复以下问题：

**Fix 1 — `appearance_count` 同日重复累加（高优先级）**

- **原因**：`upsert_creative_library()` 的 UPDATE 无条件 `appearance_count + 1`，但全流程对同一 `raw_payload` 调用两次（Step 2b + Step 2.6），导致同日素材被多计一次。
- **修复**：查询 `last_target_date`，仅当 `last_target_date != target_date` 时递增。回归测试：同日二次写入 count=1，跨日写入 count=2。

**Fix 2 — canonical 代表切换条件写反（高优先级）**

- **原因**：原条件 `if canonical != ad_key and is_canonical == 0` 在旧代表仍最强时反而清零其 `is_canonical`，新代表更强时又不执行清零 → 组内可能 0 个或多个 `is_canonical=1`。
- **修复**：改为 `if canonical == ad_key and dedup_reason != "new"`（仅新素材接管代表时执行清零+更新全组 `canonical_ad_key`）。回归测试：组内始终恰好 1 个 `is_canonical=1`。

**Fix 3 — 一键流程漏传 `--cluster-url` / `--sync-target`（中优先级）**

- **原因**：分步 `push_sync` 传了 `--cluster-url` 和 `--sync-target`，一键流程没传，导致一键流程不同步聚类表。
- **修复**：在 `workflow_video_enhancer_full_pipeline.py` Step 4 的 `sync_cmd` 中补齐：有 `cluster_bitable_url` 时传 `--cluster-url` 和 `--sync-target both`，无时传 `--sync-target raw`。

**Fix 4 — `semantic_crossday_filter` 接入主链路（中优先级）**

> 2026-05-15 已下线旧 `analysis_embedding` 语义去重，保留此段仅作历史记录。

- **原因**：`semantic_crossday_filter()` 仅定义未调用，文档描述与实际行为不一致。
- **修复**：在全流程 Step 2.7（嵌入存储）之后新增 Step 2.8：对 `combined_results` 做语义比对，同 appid 内 cosine similarity ≥ 0.92 的素材设 `exclude_from_cluster=True`，下游 `generate_video_enhancer_ua_suggestions_from_analysis.py` 自动跳过（复用已有排除机制）。标记数和匹配详情会打印到终端并回写 analysis JSON。

#### 5) 趋势信号利用

**背景**：`creative_library` 已有 `first_target_date`/`appearance_count` 等生命周期数据，但未被利用。

**变更**：

- `video_enhancer_pipeline_db.py` 新增 `compute_trend_signals(target_date, lookback_days=14)` — 计算各产品本周 vs 上周新素材数、趋势方向（rising/declining/stable）。
- `generate_video_enhancer_ua_suggestions_from_analysis.py` `_build_prompt()` 注入趋势段落，要求方向卡片中引用「该类素材本周明显增多/减少」等趋势表述。

---

### [VE] 爬取与封面去重拆分（分步工作流）

**背景**：将「抓取」与「封面日内去重（现为 CLIP，见 **§2026-04-14**）」解耦，便于独立执行或重跑。

**变更**：

- `scripts/workflow_video_enhancer_steps.py`：`crawl_store` 增加 `**--crawl-only`** — 只跑 `test_video_enhancer_two_competitors_318.py` 写 `workflow_video_enhancer_{日期}_raw.json`，**不**做封面去重、**不**写库；终端提示下一步执行 `cover_store`。
- 新增子命令 `**cover_store`**：读已有 raw →（若 `COVER_STYLE_INTRADAY_ENABLED` 开启）`apply_intraday_cover_style_dedupe` → 写回 raw 与 `*_cover_style_intraday.json` → `**prune_daily_creative_insights_not_in_raw`**（删除当日库里已不在 raw 中的 `ad_key`，避免先全量入库再缩条后残留行）→ `upsert_daily_creative_insights` / `upsert_creative_library` / `upsert_daily_video_enhancer_filter_log`。
- `scripts/video_enhancer_pipeline_db.py`：新增 `**prune_daily_creative_insights_not_in_raw**`。
- `scripts/workflow_video_enhancer_full_pipeline.py`：增加 `**--skip-cover-dedupe**` — 一键流程中跳过封面聚类块，抓取后直接用全量 raw 继续（与关闭封面类似，无需改环境变量）。
- 默认 `**crawl_store` 不带 `--crawl-only**` 时行为与改前一致（抓取 + 封面 + 入库）。分步编号在文件头注释中已更新为含 `cover_store`。

### [VE] 封面去重：与「昨日」同 appid 参照（跨日）

**（2026-04-14 修订）** 下文「昨日」在**产品语义**上已扩展为 **过去 N 日历史窗口**（默认 7 日），实现为 CLIP 向量并查集而非 LLM；详见 **§2026-04-14** 与文首对齐表「封面日内 / 跨日向量去重」。

**背景**：封面聚类原先仅对比**当日**同产品；现增加**历史**已入库的 `insight_cover_style`（及库内 `cover_embedding`）作为参照，减少与近几日视觉重复的今日素材。

**逻辑**（`scripts/cover_style_intraday.py`）：

- 从 DB 读取 `**target_date` 之前连续 N 日**（默认 **7**）各 `appid` 下非空 `insight_cover_style`（`load_cover_style_rows_for_dates_grouped_by_appid`；单日查询仍可用 `load_cover_style_rows_for_date_grouped_by_appid`），与今日条目一并进入 **CLIP 余弦聚类**。
- 簇内保留 `**all_exposure_value` 最高**的一条（历史、今日同一比较口径）。若历史侧 `ad_key` 胜出，今日同簇素材剔除，`removed` 中 `**reason`** 仍为 `**cover_style_cluster_vs_yesterday**`；若今日胜出，逻辑为 `**cover_style_cluster**`。
- 报告字段：`cross_day_history_dates`、`cross_day_rows_loaded` 等；`per_appid` 含 `history_ref_count` / `yesterday_ref_count`。
- 环境变量 `**COVER_STYLE_CROSS_DAY_ENABLED**`（默认开启；`0`/`false`/`no`/`off` 关闭历史向量参照，仅日内 CLIP）；`**COVER_STYLE_HISTORY_LOOKBACK_DAYS**`（默认 7）。`.env.example` 已补充说明。

**监控关注点**：历史窗口依赖过去 N 日 `daily_creative_insights` 中已有封面占位且 `**creative_library.cover_embedding` 非空**；新日期或历史未跑封面时，该 appid 可能仅有今日数据，行为退化为日内逻辑。

### [VE] 封面跨日指纹（与 `creative_library` / 灵感分析 Step B 对齐）

**背景**：避免「主流程跨日已会剔除的素材」仍进入封面多模态；与 `get_deduped_items_for_analysis` 的 Step B 使用同一套比对逻辑。

**变更**：

- `scripts/video_enhancer_pipeline_db.py`：抽取 `**crossday_filter_items_against_creative_library(target_date, items)`**（同 appid 下对 `creative_library` 早于当日的记录比对 `ad_key` / 主媒体 URL / 封面 `image_ahash_md5` 汉明距离 ≤ 阈值）。`**get_deduped_items_for_analysis`** 的 Step B 改为调用该函数，避免重复实现。
- `scripts/cover_style_intraday.py`：在 `apply_intraday_cover_style_dedupe` 内、**CLIP 编码之前**默认执行上述过滤；命中则从本条列表剔除，不再进入后续封面向量步骤。报告含 `**cross_day_fingerprint_removed_count` / `cross_day_fingerprint_removed`**、`**input_count_before_cross_day_fingerprint`**；若过滤后无剩余素材，返回 `**empty_after_cross_day_fingerprint**`。
- 环境变量 `**COVER_STYLE_CROSS_DAY_FINGERPRINT_ENABLED**`（默认开启；`0`/`false`/`no`/`off` 关闭指纹层；**不**影响上文「历史 `insight_cover_style` + CLIP」跨日，后者仍由 `**COVER_STYLE_CROSS_DAY_ENABLED`** / `**COVER_STYLE_HISTORY_LOOKBACK_DAYS`** 控制）。
- `.env.example` 已补充 `COVER_STYLE_CROSS_DAY_FINGERPRINT_ENABLED` 说明。

**监控关注点**：指纹层依赖 `creative_library` 已有历史行（`first_target_date < 当日`）；冷启动或从未入库的竞品首日可能无命中，仅靠 **CLIP 历史窗口**参照兜底（见 **§2026-04-14**）。

---

## 2026-04-02

### [VE] OpenRouter 用量：工作流前后各查一次 Key

**变更**：主流程在 `workflow_video_enhancer_full_pipeline.py` 内调用 `**llm_client.print_openrouter_key_meter`**（工作流开始前 / 结束后各一次，需 `.env` 中 `OPENROUTER_API_KEY`）。默认关闭；设 `**OPENROUTER_METER=1`** 等开启（以 `llm_client` 内逻辑为准），输出经 `tee` 进入当日 `logs/daily_video_enhancer_workflow_${TARGET_DATE}.log`。独立脚本 `**scripts/openrouter_key_snapshot.sh**` 供单次手动对比（可能用 `curl`）。

### [VE] 终端耗时：灵感多模态 / 封面多模态

**变更**：`analyze_video_from_raw_json.py` 每条素材在 `_call_llm_video` / `_call_llm_image`（灵感分析）结束后打印 `灵感多模态耗时 X.Xs · [video|image]`。**（2026-04-14 起）** 封面步骤已改为 CLIP，**不再**打印「封面多模态 / 封面聚类 LLM」耗时；历史描述保留供对照。

---

### [VE] 封面风格日内：进度日志 + 逐条入库

**变更**：`scripts/cover_style_intraday.py` 每条封面处理前后打印进度，完成后调用 `video_enhancer_pipeline_db.upsert_single_cover_style_insight` 写入 `daily_creative_insights.insight_cover_style`（**2026-04-14 起** 为 CLIP 占位 JSON，非多模态描述）；`apply_intraday_cover_style_dedupe(..., crawl_date)` 第三参传入 `raw_payload["crawl_date"]`。`workflow_video_enhancer_full_pipeline.py` / `workflow_video_enhancer_steps.py` 已传参。

---

## 2026-03-31

### [VE] 灵感分析：花卉背景 / 黑白大片套路过滤（**已废弃**）

**历史逻辑**：曾与主灵感分析同一次多模态输出套路布尔字段；命中则打「我方已经投过」等。**该能力已移除**，详见文首「文档与代码对齐说明」。

**下游（仍适用）**：`sync_raw_analysis_to_bitable_and_push_card.py` 仍对 `exclude_from_bitable` 等字段做主表是否同步；`generate_video_enhancer_ua_suggestions_from_analysis.py` 仍对 `exclude_from_cluster` 做聚类排除（`cluster_excluded_count`）。**来源**以主流程 **Step 2.8 语义去重**、**Step 2.9 已投放特效库**、`material_tags` 补标为主，而非分析脚本内套路。

---

### [VE] 多模态封面日内去重默认开启

**变更**：`scripts/cover_style_intraday.py` 中 `is_cover_style_intraday_enabled()` 默认改为开启；通过 `COVER_STYLE_INTRADAY_ENABLED=0`（或 `false`/`no`/`off`）关闭。`scripts/daily_video_enhancer_workflow.sh` 中 `export COVER_STYLE_INTRADAY_ENABLED="${COVER_STYLE_INTRADAY_ENABLED:-1}"`，便于环境变量显式覆盖。`.env.example` 已补充说明。**（2026-04-14）** 实现已为 **CLIP 封面**，标题中「多模态」为历史命名。

---

## 2026-03-24

### [VE] 项目清理：废弃脚本归档到 `scripts/legacy/`

**操作**：将 50 个废弃/旧流程脚本从 `scripts/` 移入 `scripts/legacy/`，保留 31 个活跃脚本。

**保留在 `scripts/` 的活跃脚本分类：**


| 分类                 | 脚本                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Video Enhancer 主流程 | `workflow_video_enhancer_full_pipeline.py`, `workflow_video_enhancer_steps.py`, `test_video_enhancer_two_competitors_318.py`, `analyze_video_from_raw_json.py`, `generate_video_enhancer_ua_suggestions_from_analysis.py`, `sync_raw_analysis_to_bitable_and_push_card.py`, `push_video_enhancer_multichannel.py`, `push_video_enhancer_feishu_card_only.py`, `sync_video_enhancer_date_to_google_sheet.py`, `video_enhancer_pipeline_db.py` |
| 核心依赖               | `path_util.py`, `proxy_util.py`, `guangdada_login.py`, `run_search_workflow.py`, `workflow_guangdada_competitor_yesterday_creatives.py`, `guangdada_yesterday_creatives_db.py`                                                                                                                                                                                                                                                               |
| 定时任务               | `daily_ua_job.sh`, `daily_video_enhancer_workflow.sh`                                                                                                                                                                                                                                                                                                                                                                                        |
| 竞品热门榜流程            | `hot_rank_step1_crawl.py`, `hot_rank_step2_video_analysis.py`, `hot_rank_step3_cluster.py`, `hot_rank_step4_push_feishu.py`, `workflow_competitor_hot_rank.py`                                                                                                                                                                                                                                                                               |
| 其他次要流程             | `workflow_competitor_new_rank.py`, `compute_new_rank_diff.py`, `fetch_competitor_raw.py`, `fetch_competitor_new_creatives.py`, `fetch_dom_yesterday_creatives.py`, `sync_top3_competitor_by_heat_to_feishu.py`, `ua_crawl_db.py`, `competitor_hot_db.py`, `competitor_ua_db.py`                                                                                                                                                              |


**注意**：`workflow_guangdada_competitor_yesterday_creatives.py` 是主流程的核心依赖（被 `test_video_enhancer_two_competitors_318.py` import，提供 `_apply_relaunch_pipeline_tag`、`_creative_hits_target_date`、`advertiser_matches_product` 等工具函数）。

---

### [VE] 功能新增：图片素材灵感分析支持

**背景**：原工作流仅分析有视频 URL 的素材，纯图片素材（`video_duration=0`，`resource_urls=[]`，`preview_img_url` 有值，`video2pic=1`）完全被跳过。

**涉及文件**：

- `scripts/analyze_video_from_raw_json.py`
- `scripts/generate_video_enhancer_ua_suggestions_from_analysis.py`
- `scripts/sync_raw_analysis_to_bitable_and_push_card.py`

---

#### `scripts/analyze_video_from_raw_json.py`

**新增函数 `_pick_image_url(creative)`**

```python
def _pick_image_url(creative: Dict[str, Any]) -> str:
    """提取图片 URL：优先 resource_urls 中纯图片条目，其次 preview_img_url。"""
    for r in creative.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("image_url") and not r.get("video_url"):
            return str(r["image_url"])
    if creative.get("preview_img_url"):
        return str(creative["preview_img_url"])
    return ""
```

**新增函数 `_call_llm_image(user_content, image_url)`**

- 使用 `image_url` 类型的多模态消息调用视觉模型
- 优先复用 `OPENROUTER_VIDEO_MODEL`（Gemini 支持图片），兜底用 `OPENROUTER_MODEL`
- 无可用模型时回退纯文本分析

**新增函数 `_build_image_prompt(item, creative, image_url)`**

- 图片专属 prompt，分析维度：构图、视觉焦点、Before/After 对比、文字排版
- 第 2 项改为「视觉钩子（第一眼抓人的核心元素）」，区别于视频的「Hook（前几秒）」

`**_build_prompt` 重命名为 `_build_video_prompt**`（无逻辑改动）

`**main()` 逻辑变化**：

- 旧：无 `video_url` → `continue` 跳过
- 新：无 `video_url` → 尝试 `image_url`，两者都无才跳过
- 输出结果新增字段：`creative_type`（`"video"` / `"image"`）、`image_url`、`title`、`body`
- 输出汇总信息：`视频 N / 图片 N / 跳过 N`
- 输出 JSON 新增字段：`video_analyzed`、`image_analyzed`、`skipped`

---

#### `scripts/generate_video_enhancer_ua_suggestions_from_analysis.py`

`**_build_prompt()` 中 `source_by_adkey` 构建逻辑更新**：

- 新增 `creative_type` 字段传入 LLM
- 图片素材传 `image_url`，视频素材传 `video_url`
- 让 LLM 在生成方向卡片时能感知素材类型

---

#### `scripts/sync_raw_analysis_to_bitable_and_push_card.py`

`**adkey_to_video`（即 `video_by_ad`）映射构建逻辑更新**：

```python
# 旧：
v = str(it.get("video_url") or "")
# 新：
v = str(it.get("video_url") or "") or str(it.get("image_url") or "")
```

- 图片素材的 ad_key 也纳入 `adkey_to_video` 映射
- 确保 LLM 在方向卡片「参考链接」中引用图片 ad_key 时，飞书卡片能渲染出可点击链接

**多维表记录字段无变化**：`封面图链接` 字段已使用 `preview_img_url`，图片缩略图本已支持；`视频链接` 对图片素材保持为空，符合语义。

---

---

### [VE] 功能新增：素材主库 creative_library 与智能多维去重

**背景**：`daily_creative_insights` 是按日期快照存储的，同一条广告素材跨天重复出现时没有归一化处理，且无法判断"视觉相同但 ad_key 不同"的情况（如同图跨平台投放）。

**涉及文件**：

- `scripts/video_enhancer_pipeline_db.py`
- `scripts/workflow_video_enhancer_full_pipeline.py`

---

#### `scripts/video_enhancer_pipeline_db.py`

**新增表 `creative_library`**（跨天去重主库）：


| 字段                                        | 说明                                                       |
| ----------------------------------------- | -------------------------------------------------------- |
| `ad_key`                                  | 唯一标识，UNIQUE                                              |
| `dedup_group_id`                          | 去重组 ID，格式：`ahash_{hex8}` / `text_{fp8}` / `adkey_{key8}` |
| `canonical_ad_key`                        | 组内代表（热度/人气最高者）                                           |
| `is_canonical`                            | 1=本条是组代表                                                 |
| `image_ahash_md5`                         | 感知哈希（从 raw_json 提取，广大大预计算）                               |
| `text_fingerprint`                        | sha1(normalize(title+body))，用于文案去重                       |
| `creative_type`                           | `"video"` / `"image"`                                    |
| `best_heat/impression/all_exposure_value` | 历史最高热度指标（跨天取最大）                                          |
| `first_target_date` / `last_target_date`  | 首次/最近出现日期                                                |
| `appearance_count`                        | 跨天出现次数                                                   |
| `dedup_reason`                            | 去重原因：`new` / `ahash(dist=N)` / `text` / `exact`          |


**三个索引**：`dedup_group_id`、`image_ahash_md5`、`text_fingerprint`

**新增工具函数**：

- `_ahash_hamming(h1, h2)` — 计算两个 16 进制感知哈希的汉明距离
- `_text_fingerprint(title, body)` — 归一化文案后取 sha1

**新增核心函数 `upsert_creative_library(target_date, raw_payload, analysis_by_ad)`**：

去重优先级：

1. `ad_key` 完全匹配 → 更新热度/出现次数（exact）
2. `image_ahash_md5` 汉明距离 ≤ 8 → 视觉内容相同归组（ahash）
3. `text_fingerprint` 相同且非空 → 文案完全一致归组（text）
4. 无匹配 → 新建，自立为组代表（new）

常量：`AHASH_HAMMING_THRESHOLD = 8`，`AHASH_LOOKUP_LIMIT = 2000`

**新增查询函数 `query_dedup_summary(target_date=None)`**：

- 返回每个 dedup_group 的代表素材 + 组内素材数
- 可按 `target_date` 过滤，只看当日出现的去重组

---

#### `scripts/workflow_video_enhancer_full_pipeline.py`

**Step 2b**（紧接原始素材落库后）：

```python
n_lib, n_grouped = upsert_creative_library(target_date, raw_payload)
print(f"[DB] 素材主库 creative_library: 写入/更新 {n_lib} 条，发现重复归组 {n_grouped} 条。")
```

**Step 2.6**（分析结果写库后）：

```python
_, _ = upsert_creative_library(target_date, raw_payload, analysis_by_ad)
print(f"[DB] creative_library 分析结果已同步。")
```

---

### [VE] 功能新增：多维去重过滤（日内 + 跨日），只让新唯一素材进入分析

**背景**：之前所有抓取到的 23 条素材都会进入分析，其中有重复（日内同组 9 条，跨日重复待积累）。现在只让真正新的唯一素材进入分析/推送/多维表，但全量原始素材仍完整入库存档。

**涉及文件**：

- `scripts/video_enhancer_pipeline_db.py` — 新增 `get_deduped_items_for_analysis()`
- `scripts/workflow_video_enhancer_full_pipeline.py` — 主流程集成

---

#### `scripts/video_enhancer_pipeline_db.py`

**新增函数 `get_deduped_items_for_analysis(target_date, raw_payload)`**

返回 `(deduped_items, report)`，执行两步去重：

**Step A — 日内去重（四维 OR 逻辑，优先级从高到低）**：

- 维度0: `video_url` / `image_url` 完全匹配（URL 相同=绝对同文件）
- 维度1: `image_ahash_md5` 汉明距离 ≤ 8（封面视觉相近）
- 维度2: `text_fingerprint` sha1 相同（文案完全一致）
- 同组保留 `impression` 最高者为代表，其余标记为 `intraday_removed`

**Step B — 跨日去重**：

- 从 `creative_library` 加载 `first_target_date < target_date` 的历史记录
- 对 Step A 代表素材按同样四维逻辑与历史比对
- 命中历史的素材标记为 `crossday_removed`，不进入分析

**report 结构**：

```json
{
  "total_input": 23,
  "after_intraday": 14,
  "after_crossday": 14,
  "intraday_removed_count": 9,
  "crossday_removed_count": 0,
  "intraday_removed": [{"ad_key", "reason", "group_id"}],
  "crossday_removed": [{"ad_key", "reason", "matched_ad_key", "matched_date"}]
}
```

去重报告保存至 `data/{output_prefix}_dedup_report.json`。

---

#### `scripts/workflow_video_enhancer_full_pipeline.py`（与一键流程的关系）

**历史设计**：曾计划在 Step 2c 调用 `get_deduped_items_for_analysis`，仅让去重后子集进入分析并写 `*_dedup_report.json`。

**当前一键流程（`workflow_video_enhancer_full_pipeline.py`）**：**未接入**上述步骤。`creative_library` 仍会在 Step 2b 对全量 raw 做归组；**分析入队**为：全量 `items` 中，**未复用历史成功分析**且 **符合灵感准入** 的条目 → `*_raw_pending_analysis.json` → `analyze_video_from_raw_json.py`。`get_deduped_items_for_analysis` 函数仍存在于 `video_enhancer_pipeline_db.py`，供库内逻辑复用，**与当前一键分析入队无直接调用关系**。

**首次运行说明**（仍适用于 `creative_library`）：冷启动时跨日指纹等命中 0 条为正常现象。

---

### [VE] Bug 修复 + 历史数据回填

**Bug**：`upsert_creative_library` 末尾向内存 ahash 缓存追加了 `sqlite3.Row` 类对象（写错了），导致后续迭代时报 `TypeError: type 'sqlite3.Row' is not subscriptable`。

**修复**（`video_enhancer_pipeline_db.py`）：

1. `existing_ahash_rows` 初始化时将 sqlite3.Row 转为 dict（`{"ad_key", "image_ahash_md5", "dedup_group_id"}`）
2. 末尾追加新条目时直接 `append(dict)`，移除错误的 `sqlite3.Row` 占位行

**新增脚本 `scripts/backfill_creative_library.py`**：

- 从 `daily_creative_insights` 按 `target_date` 升序（从旧到新）逐日回填 `creative_library`
- 每日数据经过完整去重逻辑（url → ahash → text）再写入，确保 `first_target_date` 准确
- 支持 `--dry-run` 只统计不写库
- **回填结果**（2026-03-18 ~ 2026-03-24，88 条原始）：
  - `creative_library` 总记录：88 条
  - 唯一去重组：45 组
  - 去重原因分布：new=45 条、text=31 条、ahash(dist=0)=12 条
  - 跨天重复出现的素材：1 条（appearance_count > 1）

---

## 当前 Arrow2 工作流

### 两条工作流概览


|           | 每日最新 (`latest_yesterday`) | 展示估值 (`exposure_top10`) |
| --------- | ------------------------- | ----------------------- |
| **目的**    | 跟踪竞品每日新素材                 | 跟踪竞品高曝光素材               |
| **排序**    | 最新创意                      | 展示估值                    |
| **搜索天数**  | 7                         | 30                      |
| **广告主筛选** | appid 精确匹配                | appid 精确匹配              |
| **每产品条数** | 不限（first_seen=昨日）         | 最多 20 条                 |
| **建议频率**  | 每天                        | 每周 2-3 次                |


### 全流程（两条工作流共用）

```
workflow_arrow2_full_pipeline.py --pull-only <pull_id> [--analyze]
 │
 ├─ Step 1  爬取
 │     test_arrow2_first_card_fields.py --pull-only <pull_id> --all-products
 │       │  统一爬虫：detail-v2 逐张点击 + ad_key 去重
 │       │  广告主筛选：advertiser_id == appid 精确匹配
 │       │  重试：detail-v2 获取重试 3 次 / 搜索无卡片重试 2 次 / Top 创意下拉框重试 3 次
 │       │
 │       ├─ latest_yesterday：滚动到 first_seen < target_date 早停 + 仅保留 first_seen=昨日
 │       └─ exposure_top10：appid 匹配计数满 20 即停
 │
 ├─ Step 2  入库
 │     upsert_arrow2_creative_library_batch → arrow2_creative_library（跨日去重主库）
 │     dedupe_arrow2_raw_items_by_ad_key → ad_key 去重（新数据覆盖旧数据）
 │
 ├─ Step 3  封面 CLIP 去重
 │     apply_arrow2_cover_style_dedupe
 │       ├─ 跨日指纹过滤：ad_key / URL / preview_img_url / ahash 汉明 ≤ 8
 │       └─ CLIP 向量聚类：cosine ≥ 0.75，7 日历史窗口，保留 exposure 最高者
 │
 ├─ Step 4  灵感分析（需 --analyze，默认跳过）
 │     analyze_video_from_raw_json.py --arrow2
 │       └─ 逐条入库 arrow2_daily_insights（含 ad_one_liner / material_tags 等）
 │
 └─ Step 5  飞书多维表同步
       sync_arrow2_to_bitable.py（raw + analysis → 飞书记录 + 附件上传）
```

### 数据库表


| 表                         | 用途                                            | 唯一键                     |
| ------------------------- | --------------------------------------------- | ----------------------- |
| `arrow2_creative_library` | 跨日去重主库（ahash/text 归组 + 封面向量 + 分析结果）           | `ad_key`                |
| `arrow2_daily_insights`   | 日快照（含灵感分析 + ad_one_liner + crawl_workflow 标签） | `(target_date, ad_key)` |


### 周趋势推送

`scripts/arrow2_weekly_trend.py`：从 `arrow2_daily_insights` 汇总指定日期范围内素材的一句话描述，让 LLM 生成趋势分析报告，分两条飞书卡片推送（新素材趋势 / 展示估值趋势）。

```bash
# 两个都推
python scripts/arrow2_weekly_trend.py --start 2026-04-27 --end 2026-04-28

# 只推新素材 / 展示估值
python scripts/arrow2_weekly_trend.py --workflow 最新创意
python scripts/arrow2_weekly_trend.py --workflow 展示估值
```

### Cron 设计方案

```crontab
# Arrow2 每日最新 —— 每天北京时间 08:00（UTC 00:00）
0 0 * * * cd /Users/oliver/guru/ua素材 && PYTHONPATH=scripts PYTHONUNBUFFERED=1 .venv/bin/python3 scripts/workflow_arrow2_full_pipeline.py --pull-only latest_yesterday --analyze >> logs/arrow2_latest_yesterday_$(date +\%Y-\%m-\%d).log 2>&1

# Arrow2 展示估值 —— 每周二、四、六北京时间 08:00（UTC 00:00）
0 0 * * 2,4,6 cd /Users/oliver/guru/ua素材 && PYTHONPATH=scripts PYTHONUNBUFFERED=1 .venv/bin/python3 scripts/workflow_arrow2_full_pipeline.py --pull-only exposure_top10 --analyze >> logs/arrow2_exposure_top10_$(date +\%Y-\%m-\%d).log 2>&1

# Arrow2 周趋势 —— 每周五北京时间 18:00（UTC 10:00）
0 10 * * 5 cd /Users/oliver/guru/ua素材 && PYTHONPATH=scripts PYTHONUNBUFFERED=1 .venv/bin/python3 scripts/arrow2_weekly_trend.py >> logs/arrow2_weekly_trend_$(date +\%Y-\%m-\%d).log 2>&1
```

### 关键脚本


| 脚本                                 | 用途                      |
| ---------------------------------- | ----------------------- |
| `workflow_arrow2_full_pipeline.py` | 一键全流程（爬取→入库→封面去重→分析→同步） |
| `test_arrow2_first_card_fields.py` | 统一爬虫（detail-v2 逐张点击）    |
| `arrow2_pipeline_db.py`            | SQLite 表管理 + 去重 + 入库    |
| `arrow2_cover_style_intraday.py`   | 封面 CLIP 去重              |
| `sync_arrow2_to_bitable.py`        | 飞书多维表同步 + 附件上传          |
| `arrow2_weekly_trend.py`           | 周趋势 LLM 总结 + 飞书卡片推送     |
| `daily_arrow2_workflow.sh`         | 每日最新 shell 入口           |
| `arrow2_exposure_workflow.sh`      | 展示估值 shell 入口           |


---

## 2026-04-29（[VE] 工作流重构）

### 特效玩法 `effect_one_liner` + 新素材 / 持续发力 + 日报推送

**背景**：VE 工作流增加「特效玩法」一句话描述，用于去重和推送；取消 UA 建议和 `ad_one_liner`；推送格式从旧的方向卡片改为新日报格式。

#### 1) 特效玩法 `effect_one_liner`

- `**analyze_video_from_raw_json.py`**：VE prompt footer 新增 `【特效玩法】` 行（约 10~20 字概括核心特效/玩法/创意卖点，如「圣诞华服换脸」「AI 肌肉编辑」）。
- `_strip_arrow2_footer_lines` 返回值新增第 5 项 `effect_one_liner`；VE 路径解析该行并写入输出。
- `**video_enhancer_pipeline_db.py`**：`daily_creative_insights` 和 `creative_library` 均新增 `effect_one_liner TEXT` 列；`upsert` 时写入。
- `**sync_raw_analysis_to_bitable_and_push_card.py`**：`FIELD_DEFS` 新增「特效玩法」字段（type=1）；同步时从 analysis 取 `effect_one_liner` 写入。

#### 2) 去重逻辑精简

- **文案去重已关闭**：`TEXT_FINGERPRINT_DEDUP_ENABLED` 默认 `0`（关闭）；`_allow_text_only_dedup` 不再参与 `upsert_creative_library` 归组。
- **当前只保留两个去重维度**：
  - **封面图去重**：CLIP 向量 + 7 日历史窗口（`cover_style_intraday.py`）+ 跨日指纹（`crossday_filter_items_against_creative_library`）
  - **一句话玩法去重**：`effect_one_liner` 精确匹配 + 跨 7 天历史（`effect_based_crossday_dedup`）

#### 3) 新素材与持续发力

- **新素材**：`creative_library.first_target_date = target_date` 且 `effect_one_liner` 在同 `appid` 近 7 日无精确/相似命中；老玩法换素材不计入新素材，仅作为 `old_play_items` 复核。
- **持续发力**：基于去重流程中被去掉的素材，汇总三个来源的持续发力信号（`compute_sustained_effort_signals`）：
  - **来源 1：封面跨日去掉**（`cover_style_intraday.json` 的 `cross_day_fingerprint_removed` + CLIP `vs_yesterday`）→ 同画面/URL 跨天重复
  - **来源 2：ahash 去重组跨天**（`creative_library` 查询）→ 同一画面换了 ad_key
  - **来源 3：effect_one_liner 跨天**（`creative_library` 查询）→ 同一玩法换了不同画面
- **追溯机制**：每个持续发力信号追溯到被匹配的历史素材的 `effect_one_liner`、产品名、媒体链接（视频/图片 URL），提供具体描述而非仅 `ad_key`。

#### 4) 日报推送格式（替代旧方向卡片）

**标题**：`AI工具竞品日报 {日期}`

**格式**：按竞品分组，每条新素材用 `effect_one_liner` 做可点击链接跳转到视频/图片：

```
**2026-04-28** | 新素材 **29** 条

**Remini**  ·  AI照片增强/修复/滤镜
🎬 [AI 照片接吻特效 (AI Kiss Effect)](视频链接)
🎬 [AI 静态照片生成拥抱视频特效](视频链接)
...

**Glam AI**  ·  AI美颜/换装/照片编辑
🎬 [自拍生成 AI 雨夜时尚大片](视频链接)
🖼 [xxx特效](图片链接)
...
```

- 🎬 = 视频素材，🖼 = 图片素材
- 链接指向视频 URL 或图片 URL
- 持续发力部分暂不展示（数据已就绪，可随时开启）

**推送通道统一**：

- **飞书卡片**：`push_video_enhancer_feishu_card_only.py`（交互式卡片）
- **企业微信**：`push_video_enhancer_multichannel.py`（markdown 分段推送，与飞书内容一致）
- **Google Sheet**：`push_video_enhancer_multichannel.py`（数据行 + 方向卡片 JSON 同步，格式不变）

#### 5) 移除 UA 建议

- `**analyze_video_from_raw_json.py`**：删除 `_build_single_ua_suggestion` 函数及调用；移除 `ua_suggestion_single` 和 `ad_one_liner` 从 VE 输出。
- `**workflow_video_enhancer_full_pipeline.py`**：移除 `ua_suggestion_single` 和 `ad_one_liner` 在 `analysis_by_ad` 字典中的传递。
- `**video_enhancer_pipeline_db.py`**：移除 `ad_one_liner` 在 `upsert_daily_creative_insight` 中的读取和写入（DB 列保留）。

#### 6) 分析重试机制

- **单条重试**：`_analyze_one_item` 中 LLM 调用失败（403/超时/空分析/格式错误）时最多重试 3 次（`_vision_call_retry_max`，2 秒间隔）。
- **二轮重试**：初始分析全部完成后，对仍失败/为空的条目再做一轮统一重试。
- **成功率阈值**：若分析成功率 < 90%（`ANALYSIS_FAILURE_TOLERANCE`），工作流停止；≥ 90% 则继续后续步骤（卡片、推送等）。
- **Embedding 修复**：`_store_analysis_embeddings` 中 `break` 改为 `continue`，单条 embedding 失败不中断整个嵌入过程。

#### 7) Embedding 本地模型

- `**llm_client.call_embedding()`** 改为**本地模型优先**：先尝试 `sentence-transformers` + `BAAI/bge-small-zh-v1.5`（中英双语、512 维、零 API 成本），不可用时降级到 OpenRouter/OpenAI API。
- 环境变量 `EMBEDDING_PROVIDER=api` 可强制走 API；`LOCAL_EMBEDDING_MODEL` 可切换本地模型。

---

## 当前主工作流（Video Enhancer 日流程）

**触发方式**：`scripts/daily_video_enhancer_workflow.sh` 与 `daily_ua_job.sh` 注释均为 **手动执行**（默认 `TARGET_DATE` = 昨天；使用项目根 `.venv/bin/python3`）。若本机仍配置 crontab，以实际为准。

```
daily_ua_job.sh（可选）
 └→ daily_video_enhancer_workflow.sh   # PYTHONUNBUFFERED=1；COVER_STYLE_INTRADAY_ENABLED 等
      └→ workflow_video_enhancer_full_pipeline.py --date <TARGET_DATE>
           │
           ├─ Step 1  爬取
           │     test_video_enhancer_two_competitors_318.py → workflow_video_enhancer_{日期}_raw.json
           │
           ├─ Step 2  可选封面日内 CLIP 去重（COVER_STYLE_INTRADAY_ENABLED；--skip-cover-dedupe 跳过）
           │     apply_intraday_cover_style_dedupe → 写回 raw + *_cover_style_intraday.json
           │
           ├─ Step 3  灵感准入统计
           │
           ├─ Step 4  原始落库
           │     upsert_daily_creative_insights（仅 raw，无 analysis）
           │     upsert_creative_library（全量归组）
           │
           ├─ Step 5  分析入队
           │     全量 items → 复用历史成功分析跳过 → 准入 + 待分析 → *_raw_pending_analysis.json
           │
           ├─ Step 6  灵感分析：analyze_video_from_raw_json.py
           │     ├─ VE prompt 含【特效玩法】行 → 从 LLM 输出解析 effect_one_liner
           │     ├─ 单条重试 3 次（403/超时/空/格式错误）
           │     └─ 二轮统一重试（仍有失败条目时）
           │
           ├─ Step 7  后处理 + 回写 DB
           │     ├─ 已投放特效库：apply_launched_effects_filter → exclude_* / material_tags
           │     ├─ 回写 daily_creative_insights + creative_library（含 analysis + effect_one_liner）
           │     └─ 玩法嵌入：play_fingerprint / effect_one_liner → effect_one_liner_embedding
           │
           ├─ （若分析成功率 < 90%）提前 return；可跑验收（partial）
           │
           ├─ Step 8  方向卡片：generate_video_enhancer_ua_suggestions_from_analysis.py
           ├─ Step 9  飞书多维表同步：sync_raw_analysis_to_bitable_and_push_card.py（--no-card）
           │     └─ 含「特效玩法」字段
           ├─ Step 10  飞书日报卡片：push_video_enhancer_feishu_card_only.py
           │     └─ AI工具竞品日报 + 按产品分组 + effect_one_liner 可点击链接
           ├─ Step 11  推送表：upsert_daily_push_content（daily_ua_push_content）
           ├─ Step 12  企业微信 + Google Sheet：push_video_enhancer_multichannel.py
           │     └─ 企业微信推送与飞书卡片使用同一日报格式
           │
           ├─ OpenRouter 用量：print_openrouter_key_meter（工作流结束，若启用）
           └─ 验收：run_acceptance_after_workflow（workflow_video_enhancer_acceptance.py）
```

**分步等价**：`workflow_video_enhancer_steps.py`：`crawl_store` → `cover_store`（可选与爬取拆分）→ `analyze_store` → `cluster_store` → `push_sync`。

### 去重体系（当前）


| 维度          | 实现                                      | 用途           |
| ----------- | --------------------------------------- | ------------ |
| **封面图去重**   | CLIP 向量 + 7 日历史窗口 + 跨日指纹                | 同画面/同视觉创意    |
| **一句话玩法去重** | `effect_one_liner` 精确匹配 + 7 日历史         | 同一特效玩法换了不同画面 |
| ~~文案去重~~    | `TEXT_FINGERPRINT_DEDUP_ENABLED=0`（已关闭） | ~~文案完全一致~~   |


### 持续发力信号

基于去重流程中被去掉的素材，`compute_sustained_effort_signals` 汇总三个来源，每个信号追溯历史素材的 `effect_one_liner` + 产品名 + 媒体链接：


| 来源                  | 含义             | 追溯内容                                     |
| ------------------- | -------------- | ---------------------------------------- |
| 封面跨日去掉              | 同画面/URL 跨天重复出现 | matched 素材的 effect_one_liner + 视频/图片链接   |
| ahash 组跨天           | 同一画面换了 ad_key  | canonical 素材的 effect_one_liner + 视频/图片链接 |
| effect_one_liner 跨天 | 同一玩法换了不同画面     | 最高展示量素材的视频/图片链接                          |


### 新素材判定

`creative_library.first_target_date = target_date` 且 `effect_one_liner` 在同 `appid` 近 7 日无精确/相似命中；仅 ad_key 首次出现但玩法已出现过，不计入新素材。

### 飞书多维表字段（当前）


| 字段名    | 类型  | 说明                             |
| ------ | --- | ------------------------------ |
| 标题     | 文本  | 素材标题                           |
| 产品     | 文本  | 竞品产品名                          |
| 平台     | 文本  | 投放平台                           |
| 视频链接   | 文本  | 视频直链                           |
| 封面图链接  | 文本  | 封面图 URL                        |
| 封面图    | 附件  | 封面图文件                          |
| 视频附件   | 附件  | 视频文件                           |
| 特效玩法   | 文本  | effect_one_liner（一句话概括核心特效/玩法） |
| AI分析结果 | 文本  | 灵感分析全文                         |
| UA灵感借鉴 | 文本  | （历史兼容列，新流程不再写入）                |
| 抓取日期   | 日期  | target_date                    |
| 素材标签   | 文本  | pipeline_tags / material_tags  |


**代码量**：请以实际 `wc -l` 为准；工作流核心文件见 `workflow_video_enhancer_full_pipeline.py`、`video_enhancer_pipeline_db.py`、`llm_client.py`、`push_video_enhancer_feishu_card_only.py`。

## 数据结构备忘

### raw JSON 素材字段区分


| 字段                | 视频素材                             | 图片素材     |
| ----------------- | -------------------------------- | -------- |
| `video_duration`  | > 0                              | 0        |
| `resource_urls`   | `[{type, image_url, video_url}]` | `[]`     |
| `preview_img_url` | 视频封面缩略图                          | 图片本体 URL |
| `video2pic`       | 0                                | 1        |


### 分析结果 JSON 新增字段（2026-03-24 起，2026-04-29 更新）

- `creative_type`: `"video"` 或 `"image"`
- `image_url`: 图片素材的图片 URL（视频素材为空字符串）
- `title`: 素材标题
- `body`: 素材文案
- `effect_one_liner`: 特效玩法一句话（如「AI 照片接吻特效」「自拍生成 AI 雨夜时尚大片」），从【特效玩法】行提取
- `video_analyzed`: 视频分析数量
- `image_analyzed`: 图片分析数量
- `skipped`: 跳过数量（无视频也无图片）
- `style_filter_match_summary`: 可为空串（套路筛选已移除后的列兼容）

**已移除字段**（2026-04-29）：

- `ua_suggestion_single`: 单条 UA 建议（已移除，不再生成第二次 LLM 调用）
- `ad_one_liner`: 一句话说明（已从 VE 输出移除，Arrow2 路径保留）

主流程合并后的 `analysis` JSON 还可能含：`pipeline_items`、`exclude_from_cluster`（2.8）、`launched_effect_match`（2.9）、`semantic_dedup`_* 等，以当日产物为准。

## 2026-08-05

### [社交聊天 AI] 新增独立日工作流

- 新增 `config/social_chat_apps.json`，覆盖 Linky AI、zeta、LoveyDovey、PolyBuzz、Character AI、CHAI、Talkie、Tipsy 的 Android appid。
- `scripts/run_social_chat_daily.py` / `scripts/cron_social_chat_daily.sh` 复用 VE 主流程的广大大抓取、封面去重、分析和多维表同步，使用 `workflow_social_chat_{date}` 独立产物前缀。
- 爬取节点新增自定义配置文件/分类及 `--skip-advertiser-filter`，该工作流不按广告主名字筛选；默认 VE 行为保持不变。

## 2026-10-15

### [广大大] 浏览器逐卡采集 skill 与证据状态机

- 新增 `skills/guangdada-browser-crawl/`，仅通过环境浏览器工具进行全名广告主选择、分平台筛选、逐卡翻页和可见详情采集，不调用原爬虫 CLI/私有接口；包含完整性、假空结果、验证码人工处理、条件指纹与断点恢复约定。
- `scripts/state.py` 是纯 Python 标准库本地证据校验器，提供 init/check/hold/resume；核对分页覆盖、末页、广告主/平台、筛选内容、下载文件哈希和交付文件，不代替 UI/视觉判断；状态有 COMPLETED_WITH_REVIEW，不能把待复核混进保留项。
- 已安装到 `~/.codex/skills/guangdada-browser-crawl/`，分享包 `data/skill_exports/guangdada-browser-crawl.zip` 不含登录态或项目数据。
- `tests/test_guangdada_browser_skill_state.py` 17项离线测试、skill quick_validate 与分享包解压独立 init/check 冒烟通过；本次未执行新的真实广大大采集。

### [广大大浏览器 skill] 扩展筛选边界与强制脚本证据

- 支持展示广告 游戏/工具、渠道、国家、广告/素材，日期 7天/30天/90天/1年 及自定义范围；scope 改为 date 对象，card_count/count_basis 按当前模式核对。
- 新增 `scripts/browser_check.js`：在获准浏览器工具 REPL 执行真实点击，读回当前状态，保存点击前后 PNG/哈希及日志；App、每项筛选、逐卡与翻页均要求对应检查。恢复时拒绝复用暂停前日志。
- 新增 `scripts/frames.py`：使用本地 FFmpeg/ffprobe 真实抽取六处视频帧，生成源视频/截图哈希清单；新增 FRAMES_PENDING/FRAMES_VERIFIED，筛选必须实际查看并引用对应六帧，交付视频与抽帧源哈希一致。
- 28项 Python 测试（含真实 FFmpeg 抽帧）和4项 JS 点击助手模拟测试、skill quick_validate、分享包解压独立运行均通过；未执行新版完整真实站点验收。已同步本机安装副本及分享 ZIP。

### [广大大浏览器 skill] 固定逐 App 顺序和完成清单

- 将设置拆为 CATEGORY_VERIFIED -> BASE_FILTERS_VERIFIED -> APP_VERIFIED -> SORT_VERIFIED：先登录、游戏/工具、除排序外筛选，再选 App，之后才点击展示估值/热度；按点击时间检查顺序。
- 新增 `seal` 命令、`collection_complete.json`、总清单 `app_checklist.json` 和 next_collection_task；前一 App 未采集登记完成时，后续任务停在 PREVIOUS_APP_PENDING。采集完成与后续抽帧/审核/交付完成分开报告。
- 34项 Python 测试、4项点击助手模拟测试及分享包解压串行流程检查通过，已更新本机安装与 ZIP；未做新版完整真实站点验收。

### [广大大浏览器 skill] 2026-10-15 真实 UI 筛选验收

- 登录态被其他设备挤出后，使用现有配置重新登录；通过获准浏览器工具完成真实点击，不使用站点私有接口或独立爬虫。
- 已验证 游戏/工具、Facebook系/TikTok/AppLovin 单/多选及不限、日韩单/多选及清空、广告/素材、7/30/90天/1年与自定义日期、AppStore/GooglePlay、视频/9:16、双端 Tipsy 全名/真实商店ID、展示估值/热度及翻页。列表 iOS105 / Android200，第2页45张且下一页禁用；只做单卡和分页验收，未全量采集。
- 验收目录 `data/gdd_browser_ui_acceptance_20261015/1792052920763/`，report.html/summary.json 保留53次点击记录（39成功、14失败/不匹配），106张前后截图经哈希和实际格式/尺寸检查；另对真实 Android 单卡生成并查看六帧。
- 修复多选顺序等价比较、原生JPEG截图格式识别；明确 placeholder 消失、radio内层不可点击、国家“全部”非清空、App输入值脱敏、旧App关闭与加载暂态的实际处理。原失败证据不改写。
- 回归：36项Python测试、6项JS助手测试、skill校验及分享包独立初始化通过。更新安装副本和分享ZIP。


### [广大大浏览器 Skill] SeaHaze / Sea Soul 实际采集与渠道别名修正（2026-10-15）

- 使用完整 Android App 名、日韩、近30天、视频9:16、Facebook系/TikTok/AppLovin、文案语言不限，实际浏览器逐卡复点并保存前后截图；SeaHaze `ai.seaart.app.hot` 共100条2页，Sea Soul `ai.seaart.seasoul` 共36条1页，两项均由 state.py seal 完成原始采集登记。iOS 名称未确认，本轮未采集，不记零结果。
- 136条视频全部落盘，按 skill 抽取并查看每条六帧；SeaHaze 保留79/待复核17/排除4，Sea Soul 保留14/待复核9/排除13。属于画面初筛，未完整观看或审听；原始证据和可恢复状态在 `data/seahaze_seasoul_browser_20261015/run/`，交付预览在同级 `delivery/index.html`，保留与待复核分目录。
- `skills/guangdada-browser-crawl/scripts/state.py` 与已安装副本：真实 UI 的 `Facebook系` 现在与旧 `Facebook` 一样映射到精确 `FB News Feed`，仍拒绝 Instagram 等附带渠道；新增回归测试，37项通过，并更新分享ZIP。


## 2026-10-16

### [广大大浏览器 skill] v1.0.0 上线提交

- `skills/guangdada-browser-crawl/SKILL.md` 标记 `metadata.version=1.0.0`，发布范围为浏览器展示广告采集、逐项点击读回和截图证据、逐 App 完成清单、断点恢复及六帧视频初筛。
- 本版验证：37项 Python 状态机/真实 FFmpeg 抽帧测试、6项 JS 点击助手测试及 skill quick_validate；真实页面覆盖代表筛选值与双端 Tipsy 单卡/分页，Android SeaHaze100条和Sea Soul36条全量采集已验收。未穷举所有筛选组合、未完成新版 iOS 全量验收，内容仍为未完整审听的抽帧初筛。
- 本次提交仅包含该浏览器 skill、对应测试和相关日志；采集视频、登录态、运行证据和分享 ZIP 留在本地。


### [广大大浏览器 skill] npm 分发包（2026-10-16）

- 新增 `packages/guangdada-browser-skill/`，包名 `guangdada-browser-skill`、版本1.0.0，Node20+、零npm依赖，提供 install/doctor、CODEX_HOME或自定义skills目录、dry-run、相同内容免写、不同内容拒绝隐式覆盖、force可恢复备份及失败回滚；安装无postinstall、无网络请求、不启动浏览器或读取凭据。
- prepack从已提交skill按8文件白名单生成独立payload并校验版本与SHA256，npm tarball仅13文件；不含项目.env、登录态、视频和业务下游。构建产物skill目录和tgz不进Git。
- `test/installer.test.cjs` 8项测试通过，含真实npm pack与离线npm exec安装、目录含空格、备份/回滚、符号链接拒绝、损坏阻断与依赖缺失；本机doctor确认Python/FFmpeg可用，但不代表浏览器登录已检查。
- 本地包 `data/skill_exports/guangdada-browser-skill-1.0.0.tgz`，31169字节，SHA256 `91db52a7f1c042d8a8ae95e210139aa1e824282bc1e13a08f2407a5daca72a03`；registry查询该包返回404，本机npm whoami返回ENEEDAUTH，未发布。包保留UNLICENSED，公开发布前由权利人决定许可。


### [社交聊天竞品] 用户确认双端完整名称总名单（2026-10-16）

- 新增 `config/social_chat_browser_competitors.json`，逐字保存用户提供的iOS10项、Android11项完整App名，供浏览器采集选品与搜索使用；平台分任务，未提供的另一端名称不自行推断。名单不携带默认筛选条件，也不触发采集；现有生产日工作流配置保持原状。

### [社交聊天竞品] 手动浏览器日采集与全局前10%（2026-10-16）

- 用户确认新口径：北京时间昨天首次发现的视频，渠道/地区/语言/尺寸不限，仅排除真人口播；iOS10项和Android11项全部完成后合并按展示估值取前10%，数量向上取整。保持手动执行，不创建定时任务；不沿用旧工作流每广告主Top10/差异度精选或旧日韩/9:16/男性/女频条件。
- 新增 `config/social_chat_browser_daily.json`、`ua_workflows/social_chat_browser_daily.py`、`scripts/run_social_chat_browser_daily.py`，提供 prepare/select/sync/provision。prepare仅准备浏览器任务；真实采集仍由 `guangdada-browser-crawl` 执行，不调用广大大私有接口/项目爬虫。select强制完整21项清单、状态机REVIEW_COMPLETE、可见首次发现标签、六帧审核和有效估值；未解决的目标日内容或字段缺失阻塞全局排名。
- 通过项目.env的飞书应用新建 Base `Adz4bj9ECaCCJrsgkj7coheYn3c`，表 `tblwhAqPfQoBXK3Q`，25个业务字段已配置并读回校验，素材名称为主字段；仅清理该新建空表的默认字段和10条空白行，未插入测试素材。provision实际验证字段管理和记录读取，真实素材写入尚未端到端执行。
- sync以日期/平台/App ID/广告ID幂等写入，逐字段读回后才将同日旧落选记录改为当前入选=false，保留历史；不写旧VE表、不发群消息。new skill `skills/social-chat-browser-daily/SKILL.md` 已安装到本机同名skills目录，说明见 `docs/social-chat-browser-daily.md`。
- 已准备 `data/social_chat_browser_daily/2026-10-15/` 的21项任务，当前尚未真实采集；远端表0条素材。`tests/test_social_chat_browser_daily.py` 23项通过（全局取整、单位换算、日期语义、部分采集阻塞、待复核阻塞、幂等写入、同日退榜及写入读回失败保护），新skill quick_validate通过。

### [社交聊天竞品] 改为 App ID + 最新创意，重跑21项单卡测试（2026-10-16）

- 用户纠正采集顺序：日期设置后选择最新创意，每次App ID搜索并选中广告主后必须重新点击最新创意；全量拿齐后在本地按实际展示估值合并选前10%，不在网页按估值预筛。daily config、任务说明、两份skill与已安装副本已更新；默认新目录为 `data/social_chat_browser_daily_latest/{date}`，旧条件证据不混用。
- 浏览器状态机增加最新创意排序与 `query_type=app_id` / `query_app_id`，仍核对完整App身份、正确平台和唯一广告主；排序点击必须晚于该App的选中事件。新增对应回归，状态机与daily测试共61项通过。
- `config/social_chat_browser_app_ids.json` 保存21项平台映射：20项通过广大大候选ID/已选图标核对，其中18项还由单卡详情商店链接核对；Heyyyy iOS ID `6761748236` 由官方App Store确认，但广大大按完整名和ID均无可确认候选，不算零素材。
- 按最新条件重跑21项：18项单卡打开并身份校验通过，dotdotdot iOS与Talkie Lab iOS目标条件下无卡片，Heyyyy待确认。259条脚本点击检查通过，9条失败日志保留；20项已选App均有独立且晚于选中事件的最新创意点击，全部事件截图SHA256复核通过。证据与清单在 `data/social_chat_browser_probe_20261016/latest-restart/`；此前旧排序测试保留在父目录，不能算本轮通过证据。
- 此轮按用户要求仅做单卡交互测试：不翻页采全、不抽帧内容筛选、不计算/写入正式榜单。临时桌面视口用于解决窄栏候选点击遮挡，结束已恢复默认；广大大标签保留以便后续继续。

## 2026-10-19

### [广大大浏览器日采集] 首次发现日期口径修正

- 用户确认详情日期范围起点即首次发现；社交聊天技能、通用广大大技能及已安装副本同步更新，明确单独首次发现字段优先、否则采用详情范围起点。
- `social_chat_browser_daily.discovered()` 支持从真实详情范围提取日期，仍拒绝仅凭旧 first_seen 或列表日期；保留原始证据，新增范围/显式字段优先级回归测试。

### [广大大浏览器] 目标日窗口采集与只读检查

- 按用户新规则，国家/渠道不限时只读检查，有残留才清除；最新创意排序遇首张首次发现早于目标日的详情即停止，明确字段优先于详情范围起点。已同步两个 repo/installed skill。
- 2026-10-18 剩余18项原始窗口采集落盘 `data/social_chat_window_20261018`：101条目标日，79条保留、9条真人口播排除、13条YouTube外链无下载源待补全；已保存逐卡脚本点击/截图/哈希、日期截止证据及六帧审核。合并先前Heyyyy和Whispy iOS后107条，83保留/11排除/13待补全。
- 目标日清单用独立证据校验 `validation.json`，明确不是旧版全列表FSM通过；因13条媒体未补全，不计算全局Top10%、不写飞书。汇总交付 `delivery/index.html` / `全部素材清单.csv`，83条视频本地路径已核验，首条在浏览器实际播放成功。

### [YouTube 原始链接补全]

- 用户授权补全后，使用已安装 yt-dlp 仅下载已从广大大详情观察到的12个公开YouTube原始URL，不使用cookies或读取浏览器登录态。12条均成功下载，真实六帧审核保留11/排除1条真人口播；汇总为94保留、12排除、1待补全。
- 剩余 `cf861c6e02f2f08e0c16b6f2f325745b` 实为Admob，独立详情无原始链接、video src为空且下载禁用，证据保存在 `empty-original-link.txt/.jpg`。此前把13条全部称为YouTube外链不准确，已在交付中修正。

### [广大大浏览器 skill] YouTube 外链补全规则

- 用户要求将本轮成功流程加入 skill：详情已观察到的 YouTube 原始链接主动尝试 yt-dlp 单视频下载，记录来源、日志、大小和 SHA256，解码检查后继续真实六帧审核；不读取 cookies/共享登录态。
- 补全失败保留待处理原因与断点，不作内容排除或零估值处理；空原始链接/其他渠道不得误标 YouTube。明确媒体子步骤与原有状态机关卡的关系，同步通用/日采集两份项目 skill 与本机安装版。

### [社交聊天竞品] 2026-10-18 前10%导入飞书

- 用户明确“按前10%，解决不了的就不解决”，本批跳过1条无视频未审核Admob，在94条已审核可用视频中全平台合并按展示估值取10条；不将该例外改成日常默认规则。已写入原指定表并添加“视频附件”，10条记录25项业务字段和10个附件token/大小均API读回核验。
- 批次脚本 `data/social_chat_window_20261018/run_scripts/import_feishu_top10.py` 重新核验94条日期、六帧、源哈希与审核，保留独立窗口校验及用户例外，不冒充旧FSM通过。排名/结果分别存 `selection-with-media-waiver.json`、`feishu-sync-result.json`，预览和汇总已更新。
- 真实写表发现并修复 `social_chat_browser_daily.sync` 请求token格式：飞书拒绝UUIDv5，改为批内容摘要生成稳定UUIDv4格式；数值字段API返回十进制字符串，读回支持数值转换并仍拒绝不一致/非有限值/布尔值。回归测试25项通过。

### [社交聊天飞书表] 字段结构调整

- 用户要求删除“入选数量/有效候选总数/全局排名”；主字段改名appid并填实际App ID，App完整名称改名广告主。广告主/平台改为单选，投放渠道/投放地区/文案语言改为多选，已初始化真实选项并按原数据回填。10条记录及视频附件保留，API读回22项业务字段与附件一致；迁移前快照留在批次目录。
- 同步字段映射、读回校验、业务skill/本机副本与文档同步更新，避免下次复建删除列；测试覆盖选项值及主列。
- 授予霍振堃管理权限尚待账号标识：当前应用通讯录查询缺权限，现有协作者只有应用，已向用户请求飞书绑定邮箱或本应用open_id，未猜测账号或授权他人。

### [飞书协作者] 霍振堃管理权限已授予

- 用户确认添加官方API调试台可信回调后完成用户登录授权；使用广告之神（cli_a906892c9c8c1cb3）的user_access_token按“霍振堃”搜索，唯一匹配open_id `ou_e94416b8d17116481afde4e229147771`。
- 应用身份为目标Base `Adz4bj9ECaCCJrsgkj7coheYn3c`添加该openid的full_access/container权限，并GET协作者列表核验成功；搜索/授权结果保存在本批次目录，无群消息或邀请通知。临时令牌文件已删除、REPL变量已清空、调试台已关闭。

### [社交聊天竞品] 每日09:00自动采集与导入

- 用户授权后续每天早上执行，创建当前聊天heartbeat自动化，id=automation，ACTIVE，每天北京时间09:00。沿用21项竞品、昨天首次发现、最新创意截止、口播排除、YouTube补全、已审核候选前10%向上取整、视频附件和目标表新字段；缺失媒体有限重试后单列跳过，验证码/登录失效暂停。
- config.execution设scheduled并登记schedule；业务skill及本机副本同步自动执行授权与缺失媒体边界。不另建系统cron或新聊天，不扩展到群消息/其他表。

- 用户随后要求提前：同一自动化已更新为每天北京时间07:00，保持ACTIVE；config及skill同步，不新增重复任务。

### [飞书表] 来源字段

- 新增来源单选：广大大、facebookad libarary、小红书、抖音（按用户提供拼写）。现有本批10条回填广大大，后续浏览器日采集同步映射增加来源=广大大，区别于投放渠道；业务skill和本机副本同步。

### [广大大 skill] 2026-10-19 本地分享包

- 最新通用浏览器skill按8文件白名单重打npm tgz和直接安装ZIP，包含YouTube外链补全及目标日窗口规则。产物在data/skill_exports/2026-10-19，版本沿用1.0.0，以日期与SHA256区分，未发布registry；不包含业务日采集配置、账号、登录态或视频。
- npm安装器8项测试、skill quick_validate、ZIP逐文件一致性与tgz入口一致性检查通过；包内分别9项（含安装说明）和13项。

### [广大大 skill] 采集启动群通知与@所有人

- 用户授权每次实际采集开始发群通知，先到测试群“霍振堃”。通过广告之神搜索唯一群并GET核对，保存chat_id于业务config，群允许所有成员@。已发一条明确“不重新抓取”的测试通知，message_id=om_x100b626833f688a0cf9a62837ebce9f；应用缺im:message读取权限，未重发，改在本机飞书群界面确认发送者、正文及真实@所有人并保存截图。
- 新模块social_chat_crawl_notice提供 --run-dir / --test：稳定请求UUID+持久化回执去重、正式启动必须有目标日期、精确核对群名/chat_id、不擅自扩大接收人。发送API归一化回执可核验，不要求读取全部消息；异常状态保留message_id，禁止因回读失败重发。
- 通用skill新增按用户授权目标发送通知的规则，业务skill/已安装副本、07:00同一自动化prompt同步；通知仅启动一次，不扩展完成/失败通知。通用分享包不携带私人群ID或凭据。30项业务和通知测试、两个skill quick_validate通过；本地ZIP/npm包重新打包。

### [广大大日采集] 完成播报与视频+图片范围

- 用户新增完成日报，广告之神已向既有霍振堃测试群发送真实10-19执行/10-18首次发现的结果：21项App/平台、上新107、详情107、视频成功106、口播排除12、缺失1、审核94、前10%入表10，卡片附多维表格按钮。message_id=om_x100b6268a97410acce4fd7482c49186，官方发送回执已保存；没有再次@全体。
- 新增social_chat_crawl_report.py，发前核验候选/哈希与远端入选记录/附件，稳定UUID/本地回执避免重复。完成日报单独授权，仅同测试群；后续支持视频/图片分项及合计，明确监测范围而非平台全集。
- 用户随后要求每日获取视频和图片，config.media_types改为两类、移除仅视频筛选；prepare保存21项App/42项类型检查计划，生成指令同步最新日期截止/通知/审核规则。07:00同一自动化、通用/业务skill及本机副本同步：视频六帧、图片原图验证，合并取前10%，上传对应附件，日报分项统计。图片/混合采集尚未真实页面验收，旧视频FSM不能冒充支持图片，自动化明确要求执行时先补全验证独立混合证据/同步适配。
- 测试36项通过，两个skill校验通过，本地分享ZIP与npm包重打并核对哈希。

### [广大大混合类型] 默认空选纠正

- 用户指出类型空选即图片&视频，撤销之前分视频/图片两轮及42项计划。改为21项App/平台各采一次混合列表，逐卡识别类型；只读核对默认，有残留才清除。prepare计划、config、两份skill/本机副本及07:00自动化同步。
- 10-17批次按用户要求继续暂停，旧视频限定查询证据保留，hold记PAUSED_BY_USER，恢复前须清除残留并按新scope建批次或迁移；未继续浏览器操作或发新群消息。

### [小红书] EveryInfra 单关键词测试入口

- `scripts/test_xhs_keyword_search.py`：单页关键词搜索测试，默认乙女向/all/newest/any_time，保存真实请求与响应，检查笔记ID、URL、分页及媒体字段；无自动重试/翻页，不写飞书或发通知。
- 已按用户提供的官方文档配置 EveryInfra 端点与游标字段；本地暂缺 `EVERYINFRA_API_KEY`，真实调用尚未执行，配置保持禁用每日任务。

- 2026-10-19 已真实测试 EveryInfra「乙女向」第一页：20条笔记、14条视频均返回播放地址、20条返回图片字段，存在下一页；结果位于 `data/xhs_keyword_tests/20261019T173135_757379/`。仅搜索字段核验，未下载审核媒体、未翻页、未写飞书、未启用每日任务。

- 2026-10-19 18:09 实测「乙女向 / last_24_hours / newest」：18条（7视频、11图文），无下一页；全部互动数据导出至 `data/xhs_keyword_tests/20261019T180900_836415/互动数据.csv`。本次7条视频未返回播放URL，与上次不限日期测试不同；未自动补调收费详情或写表。

### [广大大混合日采集] 2026-10-17 完整批次与图片入表

- `ua_workflows/social_chat_mixed_window.py`：新增独立目标日窗口校验，核对21项混合列表 scope、App ID搜索后重新点击最新创意、真实身份与点击前后截图哈希、连续页/卡位、首张早于目标日的截止证据；分别验证所有原图格式/尺寸/解码/哈希与视频源/六帧，并绑定实际查看的审核看板和媒体清单哈希。不改写旧视频FSM为通过；缺估值、未审核、身份失败仍阻断，媒体失败例外不能隐藏已有媒体。
- `ua_workflows/social_chat_mixed_delivery.py`：图片与视频候选合并按展示估值取前10%向上取整；生成带视频封面、本地播放和原图的HTML，上传图片附件/视频附件，新增素材类型/图片链接字段，地区、渠道和语言拆为独立选项。重跑按唯一键更新、只退选同目标日旧入选，逐字段和附件token/大小读回；其他日期记录保留。
- `social_chat_crawl_report.py`：兼容明确的 unavailable 媒体失败记录与独立混合校验；只在真实写表读回后播报，分别报告视频/图片上新和成功数，试玩范围外单列。修正历史 Heyyyy 特殊路径只在该实际子目录存在时使用。
- 本轮 `data/social_chat_mixed_20261017_restart/` 已完成21/21：目标日1083张卡，包括765视频、317图片、1条HTML试玩（原先误判图片，已按实际iframe/截图修正并保留原始证据）。视频752成功、13条有限重试仍无法获取（8条YouTube、5条Admob无源）；317条图片的321张原图全部实际查看。752条视频全部实际查看六帧；45条真人产品口播排除，真人情景短剧和AI角色对话保留。有效1024，合并前10%103条（86视频、17图片）。
- 飞书正式写入103条，字段/附件/当前入选读回通过，保留10-18原10条，UI可见总计113条。广告之神完成播报已发送霍振堃测试群，message_id=`om_x100b626a95df14b4b070e3702c44579`；没有再次@全体。启动通知沿用本批已有回执，未重发。原图、视频、失败日志、独立清单、审核记录、选择结果、写表回执、播报回执和HTML均存本批次目录。
- 业务skill及本机副本更新为混合校验/交付模块入口；旧视频CLI不承担混合证据认证。`tests/test_social_chat_mixed_delivery.py` 新增混合合并取整、未审核/例外阻断、原图缺失/篡改、附件和字段读回验证；连同浏览器日采集/启动通知/完成日报41项测试通过，业务skill quick_validate通过。看板已真实验证视频播放与原图显示。

- 2026-10-19 18:18 已按官方 `xiaohongshu/video` 文档实测一条搜索返回的视频笔记（ID `6ad5eaf1000000002f039e2d`），付费 $0.005556，返回同一笔记ID及播放URL；实际下载4,019,606字节，ffprobe确认720×954、22.799秒，FFmpeg完整视频流解码通过。证据在 `data/xhs_keyword_tests/20261019T180900_836415/video_test_181859/`。

### [小红书] 7关键词最近24小时全分页搜索

- 新增 `scripts/run_xhs_keyword_search.py`，顺序采集配置中全部关键词，按真实游标翻至末页，逐页保存请求/响应/断点，跨关键词笔记ID去重及命中词合并，输出CSV；重复页或游标等异常标记未完成，不冒充末页。明确非计费429有限重试，其他不确定结果不盲目重发。
- 2026-10-19 18:35～18:37 实测7关键词全部末页，共25页/25次请求、491条命中、去重480篇（132视频/348图文），实际回执费用合计$0.138900。独立核对请求游标链、终止游标、数量、费用和每条发布时间均通过，日期异常0。产物 `data/xhs_keyword_runs/20261019T183543_693287/`，未下载媒体、未补调详情、未写飞书或更改定时任务。

### [小红书] 每日00:00点赞前10%媒体入表与广告之神日报

- 新增 `ua_workflows/xhs_daily.py`：重验7关键词真实搜索与游标/时间/合并证据，去重后点赞全局Top10%向上取整，同赞发布时间降序；缺点赞/时间/类型阻塞。下载仅入选笔记媒体，视频缺URL才用保留token的原文调用video，图文下载返回的全部图片，保留原字节及PNG，核验解码/尺寸/哈希。
- 幂等写指定飞书表：来源小红书、笔记/关键词/作者/赞藏评分享及图片视频附件，null不当0，不填伪造App/广告估值字段；媒体失败不补位，保留部分完成，远端读回核验字段与附件。已真实创建并核验新增字段，未立即写入本批记录或发群消息。
- 广告之神应用身份检查、霍振堃测试群身份检查、稳定UUID/消息回执去重，日报沿用广大大卡片版式，附表格按钮；不@全体、不发额外启动消息，不影响07:00广大大任务。
- 自动化 `24` 已ACTIVE，Asia/Shanghai每日00:00，首次2026-10-20 00:00；执行 `.venv/bin/python -m ua_workflows.xhs_daily run`。同日目录/文件锁固定日期，完整日报已完成重跑直接返回。
- `tests/test_xhs_daily.py` 6项通过：原始证据与合并排名、ceil/同赞时间排序、null点赞阻塞、未末页阻塞、越界日期阻塞、字段语义和附件读回及部分日报幂等。真实480篇搜索证据plan通过，选48篇；首次完整下载/写表/群播报尚待午夜执行。

### [抖音] EveryInfra 单关键词单页测试

- 新增 `scripts/test_douyin_keyword_search.py`，默认乙女向、one_day、newest、limit20；仅一次请求，无自动翻页/重试，保存请求、响应、互动CSV与日期核验，不下载或写表。
- 2026-10-19 18:48 实测返回17条，发布时间均落在最近24小时，实际计费$0.000556；响应均标记 image，17条同时有 video_url，不能仅据播放链接判断视频笔记。证据 `data/douyin_keyword_tests/20261019T184855_240303/`。

### [抖音] 搜索媒体字段异常实测

- 样本 `7698301790031103679` 搜索标记 `media_type=image`，`video_url` 实际GET返回 audio/mpeg、MP3、308579字节，ffprobe仅1条音频流，约19.252秒，完整解码通过；证据在单页测试目录 `media_probe/`。
- 同作品 `douyin/video` 详情补调返回空对象，费用0，未拿到图片。定位为供应商返回的video_url在此样本指向音频，不能把有URL视为成功获取视频；media_type是否错需原帖或原图证据，不据此断言。未改写原始数据、未上传音频冒充视频。

- 2026-10-19 18:54 换测搜索作品 `7698318125444909978` 的douyin/video仍空结果且费用0；随后用供应商真实搜索响应样例作品 `7635624004494435825` 调video成功，费用$0.000556，下载1,006,389字节MP4，H.264视频576×1024+AAC音频，8.1秒，完整解码通过。关键反证：详情仍返回media_type=image，实际确有视频流，证明供应商该字段至少在此样本错误。证据 `data/douyin_keyword_tests/documented_video_20261019T185449/`。后续必须独立保存reported_media_type与探测actual_media_type，不能依赖标签或URL存在性判断媒体成功。

### [抖音] 全关键词最近24小时数量与分页能力核验

- 新增 `scripts/test_douyin_all_keywords.py`，每关键词仅一次one_day/newest/limit50请求。实时catalog及实际7次响应均没有分页游标或has_more，不能声称翻到末页或全量页数。
- 2026-10-19 18:58批次：乙女向14、女性向2、虚拟男友18、猫箱16、星野13、破次元恋人1、梦女向0，共64条命中；梦女向空响应未收费，总费用$0.003336。证据和去重互动CSV在 `data/douyin_keyword_runs/20261019T185816_011515/`。未补调媒体、未写表或创建抖音定时任务。

### [抖音] 午夜点赞前10%媒体入表及群日报

- 新增 `ua_workflows/douyin_daily.py`、`config/social_chat_douyin_daily.json`、`docs/social-chat-douyin-daily.md`；7词one_day/newest/limit50搜索，原始证据核验、作品ID去重后按点赞取前10%向上取整，只下载入选项。抖音无翻页能力明确披露，缺赞/日期及新分页信号阻塞。
- 下载后按真实视频流或原图解码确认媒体，拒绝音频/封面冒充，缓存一次付费详情补全结果；失败不补位，保留分母。复用已存在小红书通用飞书辅助，不修改其00:00任务或广大大07:00任务。
- 飞书平台及来源均标注抖音，笔记互动/原文/真实附件幂等写入同一指定表；远端provision已核验平台单选含抖音，未即时写入素材或发群。日报按广大大样式，明确无分页范围和部分失败，发广告之神的霍振堃测试群，不@全体。
- 新增 `tests/test_douyin_daily.py`，与 `tests/test_xhs_daily.py` 共12项通过；真实63篇算出7篇入选的plan通过。`scripts/test_douyin_all_keywords.py` 增加--run-dir供每日落盘，已有证据不覆盖。
- 已创建ACTIVE本聊天自动化 `24-2`：每天北京时间00:00，首次2026-10-20 00:00。完整首批下载/上传/播报尚未运行，不以字段测试冒充日报完成。

### [抖音] 2026-10-20 午夜批次搜索异常阻塞

- 已执行自动化24-2对应 `douyin_daily run`，日目录 `data/douyin_daily/20261020/`。返回49条已知命中；星野11条中3条posted_at早于请求24h窗口（含10/3与10/18）；破次元恋人ReadTimeout、计费未知，未自动重发。其余词响应保存，梦女向空结果费用0。
- 校验按规则阻塞在排名前：未计算前10%、未下载媒体、未写飞书、未发送成功/部分完成群日报。证据与 `blocked-result.json` 已落盘，避免把失败词当0、把旧帖混入最近24h或重复计费。

### [抖音] 午夜批次授权恢复并完成入表/播报

- 用户2026-10-20授权旧帖直接剔除、破次元恋人额外重试一次。已单独重试成功1条，新增费用$0.000556；原超时计费仍未知，其余6词未重新搜索。保存原summary与重试授权/结果，不覆盖原始响应。
- `douyin_daily.select` 改为先剔除请求24h窗之前旧帖并落清单，日期缺失/未来、点赞缺失等仍阻塞；自动化24-2已同步此规则，不将一次重试授权扩展成不确定费用自动重试。
- 本批50条命中剔除3条旧帖，47篇合格、点赞前10%取5篇；均实际图文，共16张原图对应附件，0视频、0失败，已写指定飞书表，平台/来源抖音；再次独立核验业务字段、哈希、解码、附件token和精确大小均通过。广告之神已向霍振堃群发日报，message_id `om_x100b6258e1cfd884ce33a3904b2b270`，未重复@或发多条。
- 运行中补齐实际观察到的douyinpic.com/douyinmusicpromotion.com/amemv.com媒体域名，搜索音频/下载失败仍可复用缓存详情原图列表，避免误判为不可获取；无新搜索/已完成详情重调。自有旧worker在第五条尚无付费详情intent时因CDN GET等待终止并以修正版恢复，证据 `resume-download-fix.json`。
- `tests/test_douyin_daily.py` 补旧帖先于点赞剔除、未来时间拦截、缓存原图兜底；与xhs测试共15项通过。批次证据 `data/douyin_daily/20261020/completion-validation.json`，首批前后搜索快照差异已在群日报披露。

## 2026-10-20

### [小红书] 午夜批次断点恢复、全量媒体入表及群日报完成

- 用户明确授权重试星野第5页后，新增 `scripts/resume_xhs_keyword_search.py`，仅从原checkpoint续采，保留旧请求、汇总备份和授权记录；其他6关键词不重复请求。星野续至第12页末页，7关键词合计33页、651次笔记命中，跨关键词/分页去重607篇；原超时调用费用仍未知，回执已知费用不冒充全部费用。
- `ua_workflows/xhs_daily.py` 增强：视频CDN原URL按Content-Range分段下载并断点续传，核对每段/总大小；图片加Referer及User-Agent解决真实405；大于20MB的附件走飞书官方prepare/part/finish并记录分片校验/回执；重跑校验已匹配远端记录后不重复更新。未修改其他平台流程。
- 本批 `data/xhs_daily/20261020/` 报告归属2026-10-19，按各页请求时刻前24h（含续采，不声称严格自然日）。607篇点赞全局前10%向上取整61篇，21视频/40图文，最低111赞；全部213个媒体文件（21视频/192图片）下载、解码及SHA256核验通过。
- 飞书指定主表实际61篇小红书当前入选记录，业务字段、附件token/大小及入选集合读回通过，失败0；已有广大大/其他平台记录保留。首轮51成功/10失败证据在 `sync-attempt-1/`，修复后无失败。独立证据为 `delivery-validation.json`。
- 广告之神已向已核实霍振堃测试群发送唯一完成日报，message_id=`om_x100b6258ad118cb0ce80a054eb732ea`，未@全体；回执 `report-receipt.json`、`daily-result.json`、`execution-status.json` 已落盘，防止重复发送。每日00:00自动化24保持启用。
- `tests/test_xhs_daily.py` 9项通过，新增分段下载范围/尺寸/断点/来源头及大文件分片上传/回执复用测试。


## 2026-10-20

### [社交聊天竞品] 用户确认17号日期规则与采后重投剔除

- 用户明确确认17号采集口径正确。日采集只使用详情日期范围起点判断目标日与截止；独立首次发现旧日期仅保存，不参与停止判断。重投卡片也先逐卡保存，21项全采封账后按可见独立重投标签统一排除，剩余审核可用视频/图片合并取展示估值前10%。
- 同步项目与已安装两份业务/广大大skill、prepare任务说明、独立混合媒体筛选、日报重投分项及07:00现有自动任务。保留原始采集证据，不自动重写历史批次、不重发历史群通知。
- 回归覆盖旧独立首次发现日期不抢占范围起点、只见独立首次发现而无范围时阻塞、重投标签与标题子串区分、重投剔除后重新计算Top10%及日报统计对账。


## 2026-10-20

### [抖音] 多维表专属视图布局

- 调整 `tblwhAqPfQoBXK3Q / vewqmFbfSz` 抖音视图：正文、图片/视频附件、赞藏评分享、发布时间及命中关键词前置，超高行高用于预览；隐藏空标题及广告业务字段，缩窄不可隐藏的主字段 appid。
- 筛选为来源=抖音且当前入选=true，按目标日期降序、点赞数降序自动排序，已通过浏览器保存并用官方 API 核验筛选；当前显示 5 条记录。未修改帖子数据、其他视图或权限。
- 证据保存在 `data/view_adjustments/douyin_20261020.json` 与同名截图。

### [飞书] 小红书视图阅读布局调整

- 仅整理主表小红书视图 `vewTbxTE4Z` 的列顺序/显隐/列宽/行高/冻结及排序：标题、视频/图片附件、素材类型、命中关键词、赞藏评分享、发布时间、作者、原文、摘要、目标日期；原始appid索引列保留并压缩，标题冻结；高行高预览媒体，数字和类型列收窄。
- 隐藏25个广告专用、源媒体链接与内部同步字段，保留来源=小红书的原筛选；点赞降序/发布时间降序两级自动排序并保存。刷新后布局仍生效；实际视图61记录、字段名称/类型无变化。API前后快照与截图位于 `data/feishu_xhs_view_layout_20261020/`。未修改素材内容、字段业务类型或日采集排名规则。

## 2026-10-20

### [小红书] 人物图片审核前置到点赞精选

- `ua_workflows/xhs_daily.py`、`config/social_chat_xhs_daily.json`：新增 `person_or_digital_human_v1` 图片内容闸口。所有图文候选先下载/解码并生成联系表，Agent实际逐图看图后写每篇 `image-review.json`；逐图绑定来源列表、路径和SHA256，缺图、缺审、身份/哈希变更阻塞。人物为实质主体才保留；纯文字、小说页、聊天截图、小头像及非人物画面排除。多图只同步keep附件，整篇无合格图才退出排名。
- 按视频与审核合格图文合并候选计算点赞前10%，分别保留原始总量、内容剔除数、合格分母；`prepare-images` 提供6线程CDN下载及审核图集，不调用付费视频API。旧完成状态不能绕过新审核；报告回执仍幂等保留，不重复群发。
- 当前 `data/xhs_daily/20261020` 已实际看图416篇/1417张，保留446张、排除971张，235篇无合格图；原始607篇缩为372篇合格候选，精选38篇（21视频、17图文/30张图片附件）。飞书38篇字段/附件读回核验成功，旧23篇仅标记当前入选=false，不删除历史。小红书视图 `vewTbxTE4Z` 增加当前入选=true条件，API和浏览器均确认38条；广大大/抖音记录未变。
- 旧榜单/汇总/回执备份 `before-person-image-filter/`，视觉证据为 `selected-image-review/`、`pending-review-1/`、`pending-review-2/`；`delivery-validation.json` 保存新统计，`person-filter-result.jpg` 为飞书结果截图。
- `docs/social-chat-xhs-daily.md`、自动化24已同步新审核顺序，仍北京时间00:00，广大大07:00任务未改。`tests/test_xhs_daily.py` 12项通过，覆盖混合图文只留合格页、全图覆盖/缺审阻塞、图片哈希变更以及图片剔除后重新计算分母。

### [小红书] 男性主体筛选前置

- 用户进一步要求男频暂不收女性类素材。新增 `male_subject_v1`，配置 `audience_content_filter` 覆盖图片/视频，顺序为人物图片审核→男性主体审核→合并点赞前10%。女性主体、男女混合、无人画面排除，不确定停待审；保持7关键词，不用标题或关键词代替视觉判断。
- `ua_workflows/xhs_daily.py` 新增逐文件哈希绑定的男性主体审核、候选视频真实6帧准备命令 `prepare-audience`，`select` / `sync` 都执行闸口，缺审不会沿用旧38篇或写表。排名产物记录独立主体剔除数及审核摘要，原始总量不变。自动化24及说明已更新，午夜时间保持不变。
- 当前批次已实跑闸口，返回 `AUDIENCE_REVIEW_PENDING`；旧38篇仍是上一版人物图片规则结果，尚未重新完成全量男频视觉审核，未声称新规则通过、未修改远端或重发旧日报。
- `tests/test_xhs_daily.py` 已15项通过，新增男女混合/女性排除、缺审/不确定阻塞及排名前闸口用例。

### [广大大浏览器 Skill] 明确每 App 上限与真实只读/点卡证据

- `state.py` 支持 scope 显式 `collection_limit_per_app`，保留网站总数及实际采集数；只有达到上限且存在未采卡片/下一页证据时允许 `limit_reached` 封账，未配置上限仍要求真实末页。
- `browser_check.js` 新增 `gddReadOnlyCheck`（仅空选不限渠道/地区/语言）与 `gddCheckedCard`（点击前固定App身份、点击后核对详情广告ID）；校验器保留身份、截图哈希、时间、分页和唯一性约束，不手改FSM状态。
- 已同步安装 skill，`tests/test_guangdada_browser_skill_state.py` 41项通过，覆盖残留筛选/身份错配/上限冒充末页拦截。

### [小红书] 归属2026-10-19批次补做男频筛选

- 实际复核181篇图文的446张人物图片，以及191条视频的分布抽帧；女性主体、混合、无人画面和实际看过仍不能确认的素材暂不入选，缺审核证据仍阻塞。
- 607篇原始笔记筛为230篇合格候选，点赞前10%为23篇（8视频、15图文），飞书小红书视图23篇业务字段与附件读回核验通过；历史保留，旧群回执保留不重复播报。
- 修复音频/容器时长超过实际视频导致抽空帧，改用真实解码帧序号并记录时间与SHA256；新增稀疏视频回归，tests/test_xhs_daily.py 16项通过。更新自动任务24及流程文档，原始搜索不重发。

### [广告之神] 测试群@小红书链接手动收录

- 新增`ua_workflows/xhs_link_bot.py`、`config/feishu_xhs_link_bot.json`及操作文档，官方长连接接收消息，仅白名单霍振堃测试群、真人用户@本机器人触发；支持完整链接与校验跳转的xhslink。
- 手动收录不走日排名/男性主体筛选；note详情加实际媒体附件，独立`小红书|手动|笔记ID`幂等键，远端字段附件读回。消息持久队列、付费请求intent闸口、上传缓存和稳定回复UUID防重复；每日选择退榜逻辑限定日排名唯一键，不影响手动记录。
- 已真实测试note详情接口：ID/标题/发布时间及图片列表通过；单元/回归22项通过。部署本机screen常驻会话adgod-xhs-link（LaunchAgent因外置卷启动阻塞未启用）；群接收权限与版本发布需确认后完成，未声称群端全链路已上线。

- 后续用户确认授权后，已开通im:message.group_at_msg:readonly，长连接及im.message.receive_v1已配置，广告之神1.0.5于2026-10-20 15:07发布成功；screen服务存活。真人群消息完整链路待首个用户@链接触发验证，不能声称已完成该项实测。

- 首条用户测试群@链接已真实跑通：笔记6a98e2d8000000001001f37b，视频附件1个，飞书记录reczz28PZqspkb6U业务字段及附件读回通过，群回复发送API返回message_id。消息历史读取权限未开通，不为额外验证扩大权限。

### [多维表格] 平台与收录来源拆分

- 按用户要求迁移182条记录：平台单选固定广大大、facebook ad、小红书、抖音；113条iOS/Android改广大大，64条小红书、5条抖音补齐。来源固定每日最新、手动增加；181条自动采集、1条手动链接，全部远端读回核验通过。迁移前字段/记录/视图已备份到data/field-migration-20261020-*。
- 将原来源平台筛选迁移为平台字段，保留竞品/社媒创作者两个现有视图及原筛选范围；删除旧选项前完成行值迁移。
- xhs_daily、xhs_link_bot、douyin_daily、social_chat_browser_daily及群日报校验同步新口径；手动缓存期望行更新，机器人服务重启。自动任务24、24-2、automation保留原计划并更新字段规则。64项相关测试通过。


### [抖音] 群内@广告之神链接手动收录

- 扩展现有 xhs_link_bot 单一监听服务，支持抖音完整视频/图文及短分享链接，逐跳白名单和作品ID校验；仅原霍振堃测试群，最多5链接，小红书入口保留。
- 详情付费请求缓存并保留不确定结果闸口，复用 douyin_daily 媒体验证和飞书附件上传/读回；平台=抖音、来源=手动增加，独立手动唯一键，不参加每日筛选排名，不新增群权限。
- 18项相关测试通过；包含短链安全、请求缓存、抖音手动命名空间/重复入表和原小红书回归。尚待用户群内真实抖音链接端到端验证。

### [抖音] 官方分享短链修复

- 用户群内真实短链跳转 www.iesdouyin.com/share/video/ID，被旧白名单拒绝；此前未发生付费详情请求。加入精确官方分享域名及路径，仍逐跳校验，拒绝相似恶意域名。
- 增加分享跳转回归，19项测试通过；错误栈仅保存文件/函数/行号，不输出密钥或原始响应。群回复按消息与结果状态幂等，使失败恢复后可发成功回执。恢复原用户任务，沿用已有缓存。

- 同条作品详情成功后媒体经环境代理 ReadTimeout，直连成功；下载模块加入超时/连接失败后直连回退并新增回归。20项相关测试通过。实际作品7695307129971952939已解码、上传1个视频附件并读回，记录reczz28PaGnJDlE5；群成功回复回执已保存，详情仅调用一次，未补收费请求。

### [广告之神] 双用户任务隔离核查

- 核实当前为单worker串行消费，消息ID独立任务文件，回复接口绑定原始message_id，无聊天历史/LLM会话上下文。增加双用户隔离回归：A失败、B成功，验证各自状态/结果/回复目标/UUID独立；链接机器人11项测试通过。共享仅限平台+笔记ID素材缓存及公开素材去重，不提供群内成员间隐私隔离。

### [广告之神] 链接收录正式群白名单切换

- 用户将机器人加入正式群后，读取群列表并核验群名/ID，将唯一入口切换为afizzy小红书创作者挖人&视频下载（oc_ba246953f910d88ccf16585c6a4b97f8）。原霍振堃及AI推送测试群均通过忽略测试，日采集播报配置未变。重启链接机器人加载新配置，旧配置已备份。

### [广告之神] 接收事件审计补齐

- 用户指出未响应后，飞书事件日志核实17:54:47与17:55:57的im.message.receive_v1均投递SUCCESS。仅凭任务目录为空不能断言未收到@，旧版本不保存过滤信息，无法追溯具体过滤条件。
- enqueue新增仅元数据审计：消息ID、收到时间、白名单匹配、@身份匹配、消息类型、支持链接数及忽略/拒绝原因；不保存正文/链接token到审计，不扩大群权限。11项测试通过并重启服务。

### [广告之神] xhslink.cn分享域名兼容

- 用户真实链接暴露域名遗漏：新增xhslink.cn与www.xhslink.cn精确白名单，保留逐跳安全检查；真实链接已解析到笔记6acf2eb900000000140397fd并保留token。13项链接机器人测试通过。依据原群消息接收证据和用户提供链接恢复此前忽略任务，不伪造事件回执。

### [广告之神] 自由分享文案链接提取

- 提取逻辑独立为extract_links，支持中文文案紧邻链接、换行/中文标点、Markdown、富文本href、HTML &amp;、无协议白名单域名，查询token保持原样、同消息链接去重。保留群/@/域名/逐跳/每消息5条限制；拒绝伪装子域、用户名伪装、嵌入非可信URL、非法端口，非法链接不再使整条解析崩溃。
- 51项链接/XHS/抖音相关测试通过；重启正式群链接服务应用新提取器。

### [广告之神] 抖音modal_id链接与@尾缀

- 正式群18:13消息失败在本地链接解析，尚未调用详情API：搜索页使用modal_id作品ID，末尾Feishu @@_user_1被拼入URL。补充可信抖音域名modal_id数字ID提取及URL末尾@标记切分，保留域名/跳转白名单；增加回归并恢复该阻塞任务。

### [广告之神] 分享格式矩阵与真实收录回归

- 新增tests/test_social_share_formats.py，14类小红书/抖音链接×10类整段文案包装，另测富文本、HTTP安全升级HTTPS、无协议、混合多链接、去重/5条上限、攻击域名/端口/逐跳及无作品ID拒绝；全链路fixture逐格式校验API参数、媒体上传、飞书字段/附件和幂等。
- 修复HTTP分享提取、英文逗号/分号相连URL及URL:标签，维持群/@/域名白名单限制。相关268项测试通过。4条历史真实样本整段文案重放与远端字段附件读回通过，0新付费详情/0新表记录/0测试群消息；保存XML、真实验证JSON与范围报告。
- 重启正式群链接服务加载新提取器。明确fixture与真实验证边界，不宣称任意链接/供应商永远成功。


### [抖音] 2026-10-21 日采集手动记录保护

- 自动任务仍在原目录执行，迁移未切换。执行前发现同日旧入选取消逻辑仅限定平台和目标日，补充来源=每日最新及抖音|目标日期|前缀限制，保护手动增加记录；10项抖音回归通过。

### [抖音] 归属2026-10-20批次恢复完成

- 用户授权继续后仅对虚拟男友/星野各补采一次，分别1/8条；其余5词缓存复用。共47命中/47有效去重，0超时窗旧帖，统一点赞前10%入选5篇。各词快照跨午夜及上午，日报已披露各请求rolling24h范围，旧超时费用仍未知。
- 5篇全部实际媒体验证及远端业务字段、附件token/精确大小/SHA256通过：4视频+1图文，共12附件39332626字节。27MB视频触发单次附件上传1061002，改复用xhs_daily.upload_attachment分片/断点实现；28项相关测试通过，未重复下载/调用详情。
- 先发部分日报后恢复完成，原消息om_x100b62b5ad02f898cd192a04e17cab3已原位更新完成，不另发重复完成日报。completion-validation.json、resolved-result.json与回执均在data/douyin_daily/20261021。运行仍在原目录，迁移worktree尚未切换；本次修复未同步worktree。


### 2026-10-21 [社交聊天竞品] 10月20日混合媒体日采集完成

- `data/social_chat_mixed_20261020/`：21项双端App混合列表全部封账并经独立证据校验；目标日详情801条，其中视频664、图片133、范围外HTML试玩4。日期仅按可见详情范围起点，先全采后排除185条范围内重投。
- 剩余612条完成有限重试：视频482条、原图102条成功并实际查看六帧/原图，28条下载失败留痕；排除35条真人口播，审核可用549条。合并展示估值前10%向上取整55条（47视频、8图片）。审核记录绑定148张不可覆盖看板和媒体清单SHA256。
- 55条实际附件写入指定飞书表，业务字段、附件token/正大小及当前入选集合读回通过；新建55、更新/落选撤销0。广告之神向霍振堃测试群发送完成日报，回执 `om_x100b62b1656bd938cd32b1ffdbaf958`，未重复启动通知。
- `social_chat_mixed_delivery.py` 增加仅允许平台广大大/来源每日最新的自动记录更新与撤销，同唯一键手动/外部记录冲突阻断；日报按真实唯一键而非表内平台字段识别双端，远端集合仅检查自动来源。修正看板硬编码日期/试玩数。相关40项测试通过。


### 2026-10-21 [广大大skill] 切App基础筛选改为只读复核

- 通用state.py与browser_check.js允许分类、日期等全部非排序基础筛选使用真实只读证据，before/after均须匹配scope；不再要求对正确条件重复点击。保留每App新证据、前后原生截图/哈希和前App封账后的时间约束。App选择、排序、点卡/翻页不能使用只读证据。
- 同步项目与本机安装skill的脚本及状态/浏览器文档，保持每次App搜索后真实点击排序；增加只读分类/日期/非空渠道及残留、App/排序拒绝回归。未启动采集或修改历史批次。

### 2026-10-21 [广大大插件] 隔离字段采集原型与多维表只读核对

- 新增本地隔离测试副本 `data/guangdada-metadata-probe/extension/`，未改现有安装版或生产源。原型按每卡可见文字/商店链接补充日期范围、可见重投、估值原文/缩写精度、身份及缺失字段，ZIP增加字段JSON；未获取字段保持null，不以零或false代替。
- Node测试18项通过（新增3项元数据测试），覆盖缩写估值、无效日期、未知身份/重投、字段传递和报告不泄露签名URL。尚未加载该隔离版做广大大实站验收，不宣称已全字段可采。
- 只读调用当前目标飞书表，保存真实40字段schema及27项广大大字段类型核对至 `remote-schema.json` / `field-audit.json`，类型匹配。其余13项为笔记/互动字段，不要求广大大填充。未写表、未发群消息。

### 2026-10-21 [社交聊天日采集] 统一混合媒体状态机、阶段耗时与重试

- 新增 `ua_workflows/social_chat_daily_state.py`，新prepare以 `daily-machine/state.json` 为唯一每日状态，不再初始化旧视频FSM。21个命名状态（17执行、完成、人工等待/暂停/待重试），21项App共157步骤，按7个App阶段循环；单App及全局关卡复用mixed_window真实证据校验，不伪造旧FSM通过。
- 接入prepare、启动通知/排名/交付/实际写表/完成日报CLI入口；历史批次不静默迁移。状态越级拒绝；失败保留断点、错误及耗时，默认每阶段初次+2重试，增加预算仅作用于当前阶段。通知测试不推进状态，内部复核不重新点击UI。
- 分类/基础筛选已正确时强制readonly，不一致才repair；恢复App阶段需新登录、筛选和真实App身份读回。首卡probe复用，零结果需原混合校验。按页批量登记动作UUID、页/卡、真实时间、错误与证据，统计各阶段/动作耗时、失败和重试，等待时间单列；未实际执行不补造历史耗时。
- mixed_window增加单App验证及不落盘审核验证入口；summary阶段生成独立已校验封账。同步原远端附件/业务字段读回和消息回执规则保留。skill文档及daily状态参考同步至项目和已安装两项skill。
- 106项相关Python回归和浏览器JS只读检查通过；两项skill quick_validate通过。临时目录真实prepare/恢复测试确认21项、157步骤、日期不漂移、恢复不重放、提前sync被拦截。未启动实站采集、未写表/发群消息，首次线上新批仍需观察实际执行耗时。

### 2026-10-21 [广大大Skill] App选择强制候选点击与已选身份读回

- 通用 `app_click_check` 与每日 `APP_PENDING` 均要求广告主搜索模式、真实app.query及搜索后的app.candidate点击；候选日志必须标识click/advertiser_candidate，已选完整名称、App ID、平台、唯一已选及无残留关键词与实际读回一致。仅回车、仅查询/搜索按钮或错误候选不能完成。
- 新增 `gddCheckedAppCandidate`，直接调用可见候选locator.click()，不接受Enter回调代替；保存前后截图和候选点击证据。每日保存app-selection.json供混合完整性校验复查，排序必须晚于搜索及候选点击。保留历史无统一状态机批次的校验兼容，不改写历史证据。
- prepare执行说明、两项skill及参考文档已同步至项目与已安装目录。104项相关Python回归、浏览器JS候选点击/只读检查以及两项skill校验通过；新增用例覆盖仅Enter、仅查询、错误平台、多App/残留关键词拒绝和正确候选通过。未启动实站采集或外部写入。

### 2026-10-21 [广大大Skill] 浏览器内批量点卡与字段就绪判断

- 新增 `skills/guangdada-browser-crawl/scripts/browser_batch.js`：在授权浏览器REPL内通过当前可见UI适配器连续点卡、一次读齐字段、保存真实前后截图/哈希、card.identity日志及耐久断点。默认同页10张/45秒预算，正常路径不逐卡交回模型；首卡probe复用。无独立浏览器驱动、私有接口或固定sleep。
- 详情按实际身份、广告ID/详情URL、可见日期范围、展示估值、媒体类型/来源就绪即继续，不等视频缓冲或推荐内容。旧独立首次发现和重投不触发截止；首张详情范围起点早于目标日才截止。验证码、身份/列表变动、缺字段或保存/关闭失败停止，不自动重试点击；已保存卡片不因close失败重开。
- 逐卡操作/就绪耗时按页批量登记统一状态机；截止证据可引用原生PNG/JPEG路径，mixed_window兼容seal.screenshot_path，旧terminal.jpg仍支持。项目及已安装两项skill的执行说明已同步。
- 11项批量执行离线用例及原浏览器检查JS通过（12项Node结果）；50项相关Python回归及两项skill校验通过。覆盖混合类型、截止、验证码/身份错误、字段未就绪、旧详情残留、列表变化、关闭/保存失败、预算让出。未启动实站采集；当前站点locator/可见字段适配仍须在实际浏览器首卡核对，不宣称实站提速倍数或真实采集成功。
