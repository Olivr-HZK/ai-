# Replica Studio 接口 CLI

按用户提供的 `replica-studio-workbench.openapi.yaml` 封装。业务请求直接通过 HTTP 发送，不点击页面、不依赖先前浏览器 JavaScript 客户端。登录时打开 CLI 自己的专用 Chrome 会话，由用户完成文档规定的 OIDC；随后使用该会话的 APIRequestContext 发请求，不读取或导出 Cookie。默认服务固定为 `https://replica-studio.upupup.today`。

基础 CLI 负责 Replica 自动分析、智能Logo、可选翻译及已有结果下载。另有记录绑定的飞书批处理入口，用于此前173条视频及复刻附件回填；本次翻译封装不自动接入该批次或触发器。

## 命令与接口

| 命令 | HTTP 接口 | 行为 |
|---|---|---|
| `login` | `GET /api/auth/oidc/login`，`GET /api/auth/session` | 打开专用登录窗口，等待用户完成SSO，实际会话检查成功才返回已认证 |
| `session` | `GET /api/auth/session` | 验证会话并在内存保存 CSRF；输出不包含 Cookie、CSRF 或令牌 |
| `upload FILE...` | `POST /api/jobs` | 单线程连续提交全部文件；每文件独立请求ID、1个人物组，`character` 为空，`auto_analyze=true` |
| `upload-status REQUEST_ID` | `GET /api/source-uploads/{request_id}` | 只读核对原上传结果 |
| `status ID...` | `GET /api/jobs/{identifier}` | 多任务查询；支持等待分析、Logo或指定语言/内容的翻译完成 |
| `logo ID...` | `POST /api/jobs/{identifier}/actions/logo_replace` | 多选任务逐条提交 `logo_mode=smart`，不重复付费请求 |
| `translate ID...` | `POST /api/jobs/{identifier}/actions/translate` | 多任务/多语言翻译，支持文字、语音或仅字幕；先只读预检，意图落盘后提交 |
| `translate --source-video` | `PUT /api/jobs/{identifier}`，然后同上 | 仅显式指定时切换`video_route=source_video`，带最新revision，不启动额外生成 |
| `download ID...` | `POST /api/jobs/download/files`，再 GET 结果媒体 | 默认仍是原语言Logo结果；`--step translation --languages ...`下载翻译视频和字幕 |

智能换 Logo 前只读 `GET /api/settings` 取得工作区默认值；翻译前读取翻译服务/文字能力/已启用语言，不写回设置、不调整并发或模型。翻译写请求不带force_new，不改变remember_parts偏好。没有添加人物图、视频换人、独立重复分析、完整一键生成或额外拼接命令，HTTP 白名单继续拒绝这些动作。PUT只允许revision与source_video路线两个字段。

上传回执未知时，还应检查实际任务。SDK提供只读`list_jobs()`、`task_history(filter)`、`deleted_jobs()`和`deleted_projects()`，分别使用`GET /api/jobs`、`GET /api/tasks`（仅固定filter）、`GET /api/deleted-characters`、`GET /api/deleted-projects`。先按原文件名、日期/序号和上传时间窗反查候选；有候选时进一步核实原片身份后才可关联，不仅凭同名或序号猜测，不直接重传。历史接口返回条数可能低于全局counts.total，必须报告范围限制；没有查询结果不能证明从未上传或未计费。回收站仅查询，不开放恢复写接口。

## 入口

从项目根目录运行：

```sh
./scripts/replica-studio --help
```

也可用：

```sh
.venv/bin/python -m ua_workflows.replica_cli --help
```

`scripts/replica-studio` 使用项目已有虚拟环境，并保留调用方的工作目录，支持包含空格的文件路径。`pyproject.toml` 另登记了 `replica-studio` console entry；安装项目包后可直接使用该命令。

## 鉴权的实际范围

文档声明的是 **Cookie 会话鉴权**：`replica_session`。写请求还需要与服务地址一致的 `Origin` 和 `GET /api/auth/session` 返回的 `X-Replica-CSRF`。CLI 按此发送请求，没有发明 API Key、Bearer Token 或账号密码登录接口。

文档中的登录入口是 OIDC 浏览器跳转。CLI 使用自己的专用浏览器会话，由用户正常完成登录，state/nonce/PKCE与回调由站点处理。`login` 只有在会话接口返回真实用户及CSRF后才输出 `cli_authenticated=true`。它不复用或读取用户原 Chrome 配置，不输出授权码、授权地址参数、Cookie或CSRF。

专用浏览器由 `replica_auth_bridge.py` 管理；业务调用仅使用其 APIRequestContext 的HTTP接口，没有DOM脚本、页面fetch或业务按钮点击。CLI与登录进程通过当前用户专用的0600 Unix socket通信；代理固定Replica站点和必要接口白名单，禁止Cookie/Authorization头注入、不跟随API重定向、不自动重试付费请求。日志只记录方法、路径和HTTP状态。媒体按8MiB Range分段读取。

正常使用无需手动会话文件：

```sh
./scripts/replica-studio login --timeout 600
./scripts/replica-studio session
```

保留CLI专用登录窗口／进程即可复用会话；会话过期或关闭进程后，再运行login。`login --probe` 保留只读入口诊断能力，不打开窗口，也不会将入口303当作已经登录。

已有合法程序会话时，仍可显式使用私密会话文件，路径按下列顺序选取；没有文件时使用上述CLI自己的登录上下文：

1. 全局参数 `--session-file /absolute/private/session.json`。
2. 项目 `.env` 中的 `REPLICA_SESSION_FILE` 文件路径。
3. 项目 `data/replica-cli/session.json`，如果该文件存在。

可选会话配置格式为 `{"replica_session": "由已授权流程提供的有效会话值"}`。文件必须是仅当前用户可读写的普通文件，权限0600。CLI不从浏览器提取此值。没有有效程序会话或登录上下文时明确返回 `authentication_required`，不会上传或生成；不要在聊天或命令行参数中粘贴会话值。`data/replica-cli/`（包括专用登录配置、断点和媒体）已加入Git忽略规则。

2026-10-30 已通过用户在专用窗口完成SSO，CLI的login和session均实际返回已认证。随后实际完成单视频上传自动分析、智能Logo、下载闭环，任务 `d424384da84865bc8a832edd68b43e97`；9秒结果922427字节，SHA256与FFmpeg完整解码通过，抽查1秒帧顶部可见Afizzy。业务审计只有上传、智能Logo与下载计划各一次POST，没有人物图、换人或完整Pipeline。服务端对6.37秒附近转场有人工复查标记，按原响应留证，不冒称人工内容审核全通过。证据在 `data/replica-cli-live-test-20261030/verification.json`；这不是173条批量验收，也未写飞书。

## 使用顺序

下面的 `JOB_A`、`JOB_B` 使用上传命令真实返回的 `ids` 替换，不能使用文件序号猜测。

```sh
./scripts/replica-studio login
./scripts/replica-studio session
./scripts/replica-studio upload "/absolute/video-a.mp4" "/absolute/video-b.mp4"
./scripts/replica-studio status JOB_A JOB_B --wait-for analysis
./scripts/replica-studio logo JOB_A JOB_B
./scripts/replica-studio status JOB_A JOB_B --wait-for logo
./scripts/replica-studio download JOB_A JOB_B --output-dir "/absolute/results"
```

所有命令输出一份 JSON。`upload` 和 `logo` 返回已接受请求，不代表生成已经完成。上传与记录的对应关系保留在每项结果的 `path`、`request_id`、`ids` 中。

其他必要操作：

```sh
./scripts/replica-studio upload-status UPLOAD_REQUEST_ID
./scripts/replica-studio status JOB_A --analysis
./scripts/replica-studio status JOB_A --wait-for logo --timeout 7200 --interval 3.5
./scripts/replica-studio download JOB_A JOB_B --plan-only
```

全局参数置于命令之前，例如：

```sh
./scripts/replica-studio --session-file "/absolute/private/session.json" --checkpoint "/absolute/batch/state.json" upload "/absolute/video-a.mp4"
```

## 状态、断点和下载

- 默认断点为 `data/replica-cli/state.json`，绑定认证账号；进程间文件锁避免同一断点同时提交。源文件的路径、文件名、精确大小、SHA256及上传选项与上传标识一起保存，操作意图先落盘再 POST。
- 上传结果未知时保留原32位标识；后续只查原上传结果，不悄悄换标识重传。404不代表上传一定没有发生。
- Logo 的网络中断、5xx或缺失回执均保留待核对状态，并停止后续提交。重跑 `logo` 不会自动再次 POST；没有 `force_new` 或“强制重做”入口。
- 已有 Logo 结果跳过；分析未就绪、排队中、错误和 Playable 不会被提交到 Logo 生成。`status --wait-for analysis` 依据分析步骤状态；Logo 等待依据明确的 Logo 完成视图，不把通用 `status=completed` 当作所有步骤完成。智能 Logo 修改原片后可能使分析重新变成待生成，不因此重复付费分析。
- 下载只接受所选任务、固定站点的媒体路径，按任务ID建子目录。已有文件不覆盖。核对精确字节数、SHA256，并使用 `ffmpeg` 完整解码视频后才记成功；源文件和生成结果均不转码。已有且SHA256一致的下载可复用。
- Cookie、CSRF和授权头不会进入 JSON 输出或断点。基础CLI不写飞书或发送消息；下面的独立批处理只写指定复刻附件字段。

退出码：0成功；2部分失败或参数／响应问题；3缺少认证或需要文档规定的浏览器登录；4等待超时仍有未完成任务。超时不等于生成失败，也不会重新提交任务。

## 验证

```sh
.venv/bin/python -m unittest discover -s tests -p 'test_replica*.py' -v
```

37项测试执行实际SDK／CLI和登录HTTP桥模块，覆盖接口白名单、会话与CSRF、5xx不重放、跨账号断点拒绝、多文件／多任务映射、未知上传核对（含503保留未知状态）、媒体来源与长度校验、真实本地视频完整解码、下载复用、Range拼接、Cookie不导出、失败／待复查步骤不记成功及额外生成接口拒绝。另验证会话预检失败不产生上传意图或误判视频失败、已明确未发送的意图沿原ID进行首次实际提交；未知意图仍只读查询，404不当作从未提交的证据。离线HTTP适配器测试与上面的单视频实站验收分别留证。

## 可选翻译

本节按用户提供的OpenAPI及此前保存的前端检查规则封装，尚未实站执行翻译。新代码的离线验证与此前Logo实站验收分别记载。已有旧版CLI专用登录窗口时，先关闭该专用窗口，再运行`login`加载新HTTP桥；旧桥缺少翻译能力时会在翻译意图保存之前明确报`bridge_upgrade_required`，不误记未知付费请求。

建议翻译使用独立断点，下面JOB_A/JOB_B替换为真实任务ID：

```sh
./scripts/replica-studio --checkpoint data/replica-cli/translation-state.json translate JOB_A JOB_B --languages ja de --parts text voice --check-only
./scripts/replica-studio --checkpoint data/replica-cli/translation-state.json translate JOB_A JOB_B --languages ja de --parts text voice
./scripts/replica-studio status JOB_A JOB_B --wait-for translation --languages ja de --parts text voice --timeout 7200
./scripts/replica-studio --checkpoint data/replica-cli/translation-state.json download JOB_A JOB_B --step translation --languages ja de --output-dir /absolute/translated-results
```

- `--languages`必填，使用文档语言代码，如ja/de/en；`hun`保持原兼容代码，不能写hu。目标语言需在工作区已启用；不自动开通语言或服务。
- `--parts`默认voice，只配音；`--parts text voice`同时翻译画面文字和语音；`--parts subtitles`只生成字幕，不能混选。
- `--source-language en`可显式指定原语言，否则沿任务翻译视图/服务端分析结果识别。目标包含原语言时，普通翻译会排除原语言；仅字幕允许原语言。例如原语言字幕使用`--languages en --source-language en --parts subtitles`，不是`--languages source`。`source`仅用于下载。
- `--check-only`只读会话、设置和任务，返回输入路线/就绪性、已有语言状态和是否需要显式切换路线；不保存付费意图、不PUT、不POST翻译。
- 默认保留当前路线。已换Logo、分析视图为空、没有生成片段而`source_story.available=true`时，沿前端的原视频回退规则放行，不要求analysis=done或先换人。其他原视频场景可显式加`--source-video`，要求源剧情可用，然后只切路线。没有可用原视频或生成片段时明确拒绝，不代为分析、人物图、换人。
- 翻译POST与原账号/任务/语言/parts/原语言绑定，先原子保存意图；多选逐任务入队，未知回执停止后续。重跑只读取原任务；看到匹配内容的真实产物或更高revision的明确运行态才恢复已提交状态，否则保持未知，不重发。已完成内容直接复用；失败/丢失结果不自动重做。新增语言与尚未确认的旧请求重叠时先要求核对，不谎称新增语言已提交。
- 等待按每个目标语言、parts和实际video_file/srt_file判断；通用completed、其他语言完成或仅字幕完成不被当作所需翻译视频完成。仅字幕模式不会等待不存在的视频。
- 下载仍走逐文件计划，不调用拼接/export或一次性ZIP接口。视频按精确字节、SHA256及FFmpeg完整解码核验；字幕保留原UTF-8正文及换行并核对SHA256。按真实project_id建目录，字幕标记项目及所选任务集合，不猜测归属某条飞书记录。拒绝外站/未选任务/未选项目和路径穿越，不覆盖原文件；相同结果可复用。

翻译接入时SDK、HTTP桥和翻译测试共69项通过；其中27项覆盖真实请求参数/鉴权、旧桥识别、窄PUT、额外生成禁止、多语言与原语言字幕、Logo结果的pending分析回退、只读预检、未知回执不重放/只读找回、旧断点兼容、语言/内容正确等待、不继承服务端remember_parts写偏好的默认值，以及真实本地视频完整解码与UTF-8字幕保存/重复下载、媒体归属及路径校验。与原批处理35项合计104项离线测试通过。

```sh
.venv/bin/python -m unittest discover -s tests -p 'test_replica*.py'
```

## 飞书记录批处理

`ua_workflows.social_chat_replica` 固定原表及两个附件字段ID，按目标日期、视频类型和复刻附件为空建立不可变scope；每条保留原记录ID、原附件token、名称和精确字节数。业务仅上传自动分析、智能Logo、结果下载，以及官方飞书附件上传和原记录复刻附件写入。可选翻译仍独立使用，不自动加入本流程。

现在默认只有一种分阶段调度：

1. `upload_all`：单线程连续提交全部源文件，不等待前面任务生成完，也没有每轮5条或在途15条限制。每文件仍用独立的上传请求ID和任务映射；每份回执在下一文件提交前落盘。“全量提交”是完整上传阶段，不是把所有文件合成一个不可拆分的大请求。
2. `wait_all_analyses`：只读核对所有非异常任务的自动分析；全部就绪后写`analysis-gate.json`。不额外调用独立分析接口。
3. `submit_all_logos`：把全部合格任务逐ID连续排入智能Logo队列，无每轮10条限制；不等待上一条生成完才提交下一条。处理并发交给服务端，保持远端默认参数。
4. `wait_all_logos`：等待全部非异常Logo任务完成。单条完成不会提前开始下载、上传附件或写表。
5. `download_all`：下载全部合格结果，以精确字节、SHA256和FFmpeg完整解码验证，保存`logo-gate.json`和下载断点。
6. `backfill_all`：将已核验结果上传飞书并写回各自原行；最后远端读回核验。官方写表每包最多500条属于请求容量边界，不是客户端在途限制。

上传、原片获取、任务状态查询均不使用本地线程池，`upload --workers`已移除。`--scheduling batch`作为兼容选项保留，省略时同样走上述流程；`--scheduling pipeline`已移除，旧运行目录中的pipeline设置也不会恢复旧调度，不改写历史策略文件。

原片优先复用原记录断点绑定的`source_path/source_sha256`：只有文件存在、精确大小和已保存SHA256一致才复用，无需重新下载、完整解码或调用飞书取附件。没有可信缓存时，读取原行核对原附件身份，再下载这一份并紧接着上传Replica；不要求全批原片下载完才上传。输入仅做字节/哈希核验，不再预先完整解码；生成结果的完整解码保留。`download-sources`仅为可选的串行预取命令，常规流程无需执行。

以下使用新运行目录及示例日期，替换为实际授权范围；不会自动恢复此前173条旧批次：

```sh
.venv/bin/python -m ua_workflows.social_chat_replica prepare --date YYYY-MM-DD --expected-count N --run-dir data/replica-feishu-NEW
.venv/bin/python -m ua_workflows.social_chat_replica run --run-dir data/replica-feishu-NEW
.venv/bin/python -m ua_workflows.social_chat_replica status --run-dir data/replica-feishu-NEW
```

长批次可在`run`末尾加`--background`，日志为`worker.log`，进程及完成/待处理状态为`worker.json`，实时数量为`progress.json`。所有记录完成或明确异常后执行远端附件及其他字段核验，保存`verification.json`。`pause --run-dir ...`会在当前操作结束后暂停；上传时每文件之间检查暂停请求。`resume --run-dir ...`从同一scope和断点继续，不启动第二份worker；不同时启动多个处理进程。

已完成和已入队任务沿原ID继续，不重置断点。未知上传先核对原请求ID并按下节规则自动反查，未确认回执默认每60秒再查，避免每轮阻塞其他任务；404仍不证明未提交，不能据此重传。需要按任务列表/历史反查时参见前文只读接口说明。已明确中断的智能Logo沿站点原“继续”机制最多恢复一次；未知生成不得当作中断重放。异常仍单独保留，未确认项不会被报告为成功。

对`logo_unknown`可使用`reconcile-logos --run-dir ...`：只认证和GET原任务，有明确完成或接受证据才恢复状态，没有证据仍保持未知，不POST、不强制重做，也不改写活跃worker的CLI断点。响应保存在`logo-reconciliation/`。

结果写入前重读原行及原附件身份，已有复刻附件不覆盖；写后按token、名称、大小核对。写回结果未知时只读原记录核对，不盲目重放。源文件不转码，不增加人物图、换人、额外分析、完整一键生成或翻译，不改变服务端模型/并发和其他飞书字段。

本轮离线测试覆盖完整18条上传、全部分析门槛、全部Logo门槛、超过10条结果下载与回填、串行提交/查询、原片缓存命中/篡改拒绝、按需取片后立即上传、暂停、原请求ID不重放及旧断点复用。基础SDK/HTTP桥/翻译与记录回填回归分别执行；离线适配器不等同于新流程实站批量验收。本轮不重跑旧批次、不调用真实业务接口。

```sh
.venv/bin/python -m unittest discover -s tests -p 'test_replica*.py'
.venv/bin/python -m unittest discover -s tests -p test_social_chat_replica.py
```

## 异常恢复与超时告警

串行分阶段顺序保持不变；以下恢复不会自动重传原片、强制重新生成、转码或改变服务端设置。

- 飞书附件收到token后保持`feishu_uploaded`，先完成附件准备，再建立写表发送记录。一个附件上传网络中断会保留为`attachment_unknown`并告警，其余已确认token的结果继续交付。未知附件上传保留本地成品，不猜token、不自动重放媒体POST。
- `write-audit/*-dispatch.json`记录版本、批次、逐行token/名称/大小，以及`prepared`、`dispatching`、`accepted`或`unknown`。只有匹配的新版`prepared`记录能证明尚未派发，恢复后复用原token进行首次写入；达到`dispatching`边界后的操作即使目标字段暂空也只读核对，不重放。旧意图缺少此证据时保留未知。断点及发送记录同时同步文件内容和目录项。
- 写入后GET超时、代理连接中断或临时5xx在同一worker内等待恢复；重读原行发现匹配附件即可确认成功。GET鉴权刷新沿原规则，POST仍不自动重试。登录失效/账号改变需用户处理，会保存告警并停止新增业务动作。
- 上传回执未知时先查原请求ID，仍未知则自动查当前任务列表。同一轮共用列表快照；只有唯一同名候选在原提交时间窗内，且通过原视频精确大小与SHA256核对、未绑定其他记录，才能关联找回；这种关联保留`original_receipt_confirmed=false`，不伪称原上传回执已恢复。候选原片通过文档规定的媒体GET流式核哈希，不另存副本。多候选不猜测，缺少原提交时间的历史意图不自动绑定；服务端已转换原片导致哈希无法一致时也保留未知并告警。没有结果不能证明未上传或未计费。
- `logo_unknown`自动进入原任务只读核对。只有同Logo动作、更高revision及真实产物/明确运行证据才恢复下载或等待；旧revision、其他动作或没有证据均保持未知。主worker不重放生成；独立`reconcile-logos`入口继续保留，且不覆盖活跃worker的CLI断点。
- 未知回执在有限窗口内自动只读核对，不阻止已确认任务继续交付。窗口耗尽仍保持未知、标为需要处理，不改成成功或处理失败；重新启动不会重置这段计时。没有可查token的`attachment_unknown`直接要求处理，不空转等待。

可在`run`或`resume`覆盖超时参数，均为正的有限秒数，保存在运行目录`timeout-policy.json`供后台和重启复用：

```sh
.venv/bin/python -m ua_workflows.social_chat_replica run --run-dir data/replica-feishu-NEW --stall-timeout 900 --stage-timeout 7200 --unknown-timeout 900 --reconcile-interval 60
```

| 参数 | 默认 | 含义 |
|---|---:|---|
| `--stall-timeout` | 900秒 | 批次或在处理任务没有可见进展时告警 |
| `--stage-timeout` | 7200秒 | 整个阶段超过时限告警；继续等待已有服务端任务 |
| `--unknown-timeout` | 900秒 | 未知回执的自动只读核对窗口；耗尽后保留未知 |
| `--reconcile-interval` | 60秒 | 未知回执的再次核对间隔 |

告警写入`alerts.json`（当前告警）、`alerts.jsonl`（产生/解除事件）和stdout结构化事件；`health.json`保存阶段开始、最后进展和未知起始时间，`status`暴露`active_alerts`、`needs_attention`和`reconciliation_pending`。相同告警不重复刷屏，恢复进展后解除对应告警。成功提交、下载和附件交付逐项更新`progress.json`，不再等整批完成才刷新。重复查询时间、轮询错误和无实质变化的revision不算进展。

超时不会取消云端任务、跳过仍在运行的任务或自动重新POST生成；整批仍受最慢任务影响。告警在有界HTTP调用之间检查，进程彻底退出或主机休眠时需外部巡检发现；CLI本身不创建调度、发送群聊或邮件。122项原回归之外，新增离线故障测试覆盖发送边界、附件中断隔离、迟到回执、候选身份/歧义、自动Logo找回、跨重启计时及告警解除，均禁止网络连接；不构成新的线上故障演练。
