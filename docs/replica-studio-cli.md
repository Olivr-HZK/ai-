# Replica Studio 接口 CLI

业务接口按用户提供的 `replica-studio-workbench.openapi.yaml` 封装；设备授权另参考 [Replica 独立CLI v0.10.6 的公开契约](https://github.com/yongchangwang-gt/replica-studio-cli/blob/1b681360c803a8c055798e3b9d2164857d111e81/replica_cli.py)。默认使用设备授权令牌，经requests直接发送HTTP，不依赖浏览器常驻或本地登录桥。服务固定为 `https://replica-studio.upupup.today`；旧Cookie/专用Chrome通道保留为显式兼容方式。

基础 CLI 负责 Replica 自动分析、指定人物参考图上传、智能Logo、可选翻译及已有结果下载。另有记录绑定的飞书批处理入口，用于此前173条视频及复刻附件回填；本次翻译封装不自动接入该批次或触发器。

## 命令与接口

| 命令 | HTTP 接口 | 行为 |
|---|---|---|
| `login` | `POST /api/cli/login/start`、`POST /api/cli/login/poll`、`GET /api/cli/whoami` | 浏览器核对短码、批准本机；先私密保存已批准令牌，再核验账号 |
| `session` / `whoami` | `GET /api/cli/whoami` | 返回账号和公开权限信息，不输出原始令牌 |
| `logout` | 无业务HTTP | 仅删除本机令牌；不冒称服务端已撤销 |
| `login --legacy-browser` | 原OIDC/会话接口 | 显式使用旧专用Chrome登录方式 |
| `upload FILE...` | `POST /api/jobs` | 单线程连续提交全部文件；每文件独立请求ID、1个人物组，`character` 为空，`auto_analyze=true` |
| `upload-status REQUEST_ID` | `GET /api/source-uploads/{request_id}` | 只读核对原上传结果 |
| `status ID...` | `GET /api/jobs/{identifier}` | 多任务查询；支持等待分析、Logo或指定语言/内容的翻译完成 |
| `characters ID` | `GET /api/jobs/{identifier}` | 只读列出任务角色ID和当前人物参考图 |
| `portrait ID IMAGE` | `POST /api/jobs/{identifier}/portrait/{source_id}` | 上传你提供的图片并设为指定角色当前参考图；不调用生图或视频生成 |
| `logo ID...` | `POST /api/jobs/{identifier}/actions/logo_replace` | 多选任务逐条提交 `logo_mode=smart`，不重复付费请求 |
| `translate ID...` | `POST /api/jobs/{identifier}/actions/translate` | 多任务/多语言翻译，支持文字、语音或仅字幕；先只读预检，意图落盘后提交 |
| `translate --input original`（默认） | 同上，无PUT | 翻译完整当前原视频；`--source-video`为兼容别名，两个翻译前裁剪开关均false |
| `download ID...` | `POST /api/jobs/download/files`，再 GET 结果媒体 | 默认仍是原语言Logo结果；`--step translation --languages ...`下载翻译视频和字幕 |

智能换 Logo 前只读 `GET /api/settings` 取得工作区默认值；翻译前读取翻译服务/文字能力/已启用语言，不写回设置、不调整并发或模型。翻译写请求不带force_new，不改变remember_parts偏好。人物参考图上传独立开放；没有添加人物图生成、视频换人生成、独立重复分析、完整一键生成或额外拼接命令，HTTP 白名单继续拒绝这些生成动作。PUT只允许revision与source_video路线两个字段。

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

2026-11-04本项目按用户要求恢复**专用浏览器持久化会话**：`.env`设置`REPLICA_AUTH_MODE=browser`，主表监听配置显式绑定`auth_mode=browser`。`login`和后续API调用均遵循项目模式，可用`./scripts/replica-studio login`重开原专用浏览器；`session`核验账号。原任务断点保留，失败行不自动重跑。桥与专用Chrome需要保持运行；正常会话/SSO有效期仍由平台控制，到期须正常登录。原设备令牌保留但不自动回退使用。下述设备模式仍可通过`--auth-mode device`显式选择。

默认设备授权流程为：

1. `login`启动本机授权，显示批准地址和短码，并默认打开浏览器。
2. 用户在已登录Replica的浏览器中核对短码、选择所需权限并批准；CLI轮询原设备请求，不读取浏览器Cookie或自动批准。
3. 服务端确认approved并返回令牌后，立即以0600权限原子保存（与参考CLI相同）；再GET`/api/cli/whoami`核实账号并补充账号绑定。核验失败不丢弃已批准令牌，可通过`session`安全重读，无需再次申请设备码。只有whoami成功且业务断点账号匹配才能继续业务；不把批准本身称为登录或任务成功。JSON输出、业务断点和日志均不包含令牌或私密device_code。
4. 后续API和媒体下载使用`Authorization: Bearer ...`直接访问固定Replica站点，不混入旧Cookie通道的Origin、CSRF或账号头，不需要Chrome或Unix socket常驻。请求不跟随重定向，不读取.netrc/环境代理，不混用Cookie；付费POST不因网络错误自动重发。

```sh
./scripts/replica-studio login --label "素材工作流"
./scripts/replica-studio whoami
./scripts/replica-studio session
```

可加`login --no-browser`仅显示批准链接；`--timeout`限制等待时间。默认令牌路径与参考CLI兼容，为`~/.config/replica-studio/cli.json`，遵循`XDG_CONFIG_HOME`；也可用全局`--token-file /absolute/private/cli.json`或项目`.env`的`REPLICA_CLI_TOKEN_FILE`指定。文件必须是本人所有、仅本人可读写的普通文件，不接受符号链接或其他服务器的凭据。新登录保存账号绑定；参考CLI已有的同服务器令牌文件也可读取，实际身份仍由whoami核实。

设备令牌缺失、失效、撤销或权限不足时明确报错，**不会自动回退浏览器账号**。`logout`仅删除本机文件；服务端撤销需在Replica工作台`/cli`页面操作。不要把令牌文件加入版本库或贴到命令行参数中。

独立`logo`和`translate`付费命令在设备模式下要求`--yes`，并按服务端契约发送`X-Replica-Confirm: paid`。`translate --check-only`不要求确认；图片参考上传、状态读取和已有结果下载不携带生成确认。已明确授权的飞书`run/resume`批处理在固定scope内由SDK显式确认原智能Logo动作，不扩大接口白名单、不重放未知动作。

旧方式仅在显式选择时启用：

```sh
./scripts/replica-studio login --legacy-browser
./scripts/replica-studio --auth-mode browser session
```

也可显式提供全局`--session-file /absolute/private/session.json`，或在选择browser模式后沿用`.env`的`REPLICA_SESSION_FILE`。批处理SDK可通过项目`.env`的`REPLICA_AUTH_MODE=browser`选择专用浏览器通道；未配置模式的SDK默认device。Cookie写请求继续使用Origin、CSRF和账号绑定，不读取用户原Chrome资料。`login --probe`仅保留旧OIDC入口的只读诊断，不把入口303当作已登录。

旧桥仍使用APIRequestContext和0600 Unix socket，只允许必要接口；媒体按8MiB Range读取。新设备令牌直连采用原始流式GET，保留任务归属、精确大小/SHA256和解码校验；本次不改变服务端模型、并发或生成逻辑。

2026-10-30 已通过用户在专用窗口完成SSO，CLI的login和session均实际返回已认证。随后实际完成单视频上传自动分析、智能Logo、下载闭环，任务 `d424384da84865bc8a832edd68b43e97`；9秒结果922427字节，SHA256与FFmpeg完整解码通过，抽查1秒帧顶部可见Afizzy。业务审计只有上传、智能Logo与下载计划各一次POST，没有人物图、换人或完整Pipeline。服务端对6.37秒附近转场有人工复查标记，按原响应留证，不冒称人工内容审核全通过。证据在 `data/replica-cli-live-test-20261030/verification.json`；这不是173条批量验收，也未写飞书。

## 使用顺序

下面的 `JOB_A`、`JOB_B` 使用上传命令真实返回的 `ids` 替换，不能使用文件序号猜测。

```sh
./scripts/replica-studio login
./scripts/replica-studio session
./scripts/replica-studio upload "/absolute/video-a.mp4" "/absolute/video-b.mp4"
./scripts/replica-studio status JOB_A JOB_B --wait-for analysis
./scripts/replica-studio logo JOB_A JOB_B --yes
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

设备授权接入后共243项离线回归通过，其中新增35项覆盖批准/拒绝/过期、授权提示、工作区准备的有界等待、whoami核验与私密保存、0600/账号/站点绑定、撤销不降级、媒体Bearer直连、权限不足停止和付费动作确认；测试禁止真实网络连接。此前任务状态与业务断点保持，线上授权需用户正常批准，不把离线通过称为已登录。后续依据真实403问题补充到245项：验证Bearer与旧会话头分离、已批准令牌在核验失败后可只读恢复、错误详情不泄露凭据及错误账号不能打开原断点。

## 可选翻译

2026-11-04按用户截图与当前公开前端请求参数修正：完整当前原视频通过翻译POST的`input=original`指定，不能用任务`video_route=source_video`替代。旧参数实测产出5秒日语片段，完整Logo输入为28.4秒，整片验收失败；随后以新参数完成一次28.4秒日语文字＋配音实测，输入输出时长一致、完整解码及本机日语识别通过；平台仍有66条文字复查提示，技术完成不等于人工效果验收。browser桥需支持`translation_original_v1`，旧桥在意图前报`bridge_upgrade_required`，不会派发错误输入。

2026-11-10收回本地字幕试验，当前翻译入口统一为仅口播。以下命令使用独立断点，JOB_A/JOB_B替换为真实任务ID。

```sh
./scripts/replica-studio --checkpoint data/replica-cli/translation-state.json translate JOB_A JOB_B --languages ja ko --check-only
./scripts/replica-studio --checkpoint data/replica-cli/translation-state.json translate JOB_A JOB_B --languages ja ko --yes
./scripts/replica-studio status JOB_A JOB_B --wait-for translation --languages ja ko --timeout 7200
./scripts/replica-studio --checkpoint data/replica-cli/translation-state.json download JOB_A JOB_B --step translation --languages ja ko --output-dir /absolute/translated-results
```

- `--languages`必填，使用文档语言代码，如ja/de/en；`hun`保持原兼容代码，不能写hu。目标语言需在工作区已启用；不自动开通语言或服务。
- `translate`固定为`voice`，兼容显式`--parts voice`。`status`默认按`voice`等待，查询历史任务时可显式指定其原parts。
- `--source-language en`可显式指定原语言，否则沿任务翻译视图/服务端分析结果识别。口播翻译会排除目标中的原语言。`source`用于下载原语言产物。
- `--check-only`只读会话、设置和任务，返回输入路线/就绪性、已有语言状态和是否需要显式切换路线；不保存付费意图、不PUT、不POST翻译。
- 默认`--input original`直接翻译整条当前版本（含换Logo结果），显式提交`trim={remove_cta:false,remove_gameplay:false}`，翻译前不额外删除内容，不切换任务剧情路线。`--source-video`保留为完整原视频的兼容别名，同样不PUT；`--input swap`仅用于已生成的换人片段，不会自动生成片段。修改分析范围不是整片翻译的前置。
- 翻译POST与原账号/任务/语言/parts/原语言及input/trim绑定，先原子保存意图；多选逐任务入队，未知回执停止后续。重跑只读取原任务；看到匹配内容的真实产物或更高revision的明确运行态才恢复已提交状态，否则保持未知，不重发。已完成内容直接复用；失败/丢失结果不自动重做。新旧输入或内容参数不同、存在完成结果时不自动覆盖或新增付费请求。新增语言与尚未确认的旧请求重叠时先要求核对。
- 等待按每个目标语言、parts和实际video_file/srt_file判断；通用completed、其他语言完成或仅字幕完成不被当作所需翻译视频完成。仅字幕模式不会等待不存在的视频。
- 下载仍走逐文件计划，不调用拼接/export或一次性ZIP接口。视频按精确字节、SHA256及FFmpeg完整解码核验；字幕保留原UTF-8正文及换行并核对SHA256。按真实project_id建目录，字幕标记项目及所选任务集合，不猜测归属某条飞书记录。拒绝外站/未选任务/未选项目和路径穿越，不覆盖原文件；相同结果可复用。

Python入口位于`ua_workflows.replica_translation`。`translate_voice_many(api, ids, checkpoint, languages, source_language=None, *, check_only=False, input_mode='original')`统一固定口播参数，默认完整输入及两项trim=false，并复用原付费意图日记、账号核验和恢复机制。`translation_status_many`默认parts为voice，`download_translations`负责原接口结果下载及媒体校验。主表的新翻译复用同一提交入口。低层`translate_many`保留历史参数恢复能力，CLI新提交仅开放voice。

翻译接入时SDK、HTTP桥和翻译测试共69项通过；其中27项覆盖真实请求参数/鉴权、旧桥识别、窄PUT、额外生成禁止、多语言与原语言字幕、Logo结果的pending分析回退、只读预检、未知回执不重放/只读找回、旧断点兼容、语言/内容正确等待、不继承服务端remember_parts写偏好的默认值，以及真实本地视频完整解码与UTF-8字幕保存/重复下载、媒体归属及路径校验。与原批处理35项合计104项离线测试通过。

```sh
.venv/bin/python -m unittest discover -s tests -p 'test_replica*.py'
```

## 使用你提供的人物图替换参考图

`portrait`直接上传已有图片，multipart字段固定为`file`，只替换所选任务角色的参考图。原图字节不缩放、不转码；允许PNG/JPG/JPEG/WebP，最大30 MiB、短边至少320像素，以本地`ffprobe`核对实际格式及尺寸。网站后端会将图片设为当前参考并保留历史；既有生成结果可能按平台规则变为待更新，本命令不重新生成任何图片或视频。

```sh
# 查看角色映射，只读
./scripts/replica-studio characters TASK_ID

# 单角色任务自动选中唯一角色
./scripts/replica-studio portrait TASK_ID "/absolute/path/person.png"

# 多角色任务必须指定要替换谁；其他角色不上传
./scripts/replica-studio portrait TASK_ID "/absolute/path/person.png" --source-id ROLE_ID

# 仅核对图片及角色映射，不上传、不写断点
./scripts/replica-studio portrait TASK_ID "/absolute/path/person.png" --source-id ROLE_ID --check-only
```

实际上传要求自动分析已完成并识别到目标角色，任务当前没有其他生成在运行。同一张图片再次执行会读取当前参考图并比对精确大小与SHA256，确认后复用原上传，不重复POST。以后要换一张新图时显式加`--replace`，保存旧操作记录；多个角色可逐条调用并分别指定`source-id`。

上传前将任务、角色、原图片路径/大小/SHA256/尺寸和原revision写入`portraits`断点。超时、5xx、响应身份不符或回执落盘失败均保留未知；再次执行只查询原任务，并读取该角色当前图片核对原上传字节及更高revision，不重新POST。服务端若转换过图片导致字节不能匹配，会继续保留未知，`--replace`也不能绕过未知写入。核对的是当前参考图，不把它宣称为已恢复原HTTP回执。

设备授权直连原生支持人物参考图上传。仅旧browser模式需要登录桥具备`portrait_upload_v1`；旧桥缺能力时，关闭旧专用窗口后用`login --legacy-browser`重新启动。不会因此读取用户Chrome资料或导出Cookie。

这一步不会替换视频中的人物，也不修改文字、口播、Logo、字幕擦除规则或现有批处理范围。后续视频换人命令尚未开放；上传参考图成功不代表换人视频已生成。返回`video_generation_submitted=false`，回执未知退出码为2。

## 飞书记录批处理

`ua_workflows.social_chat_replica` 固定原表及两个附件字段ID，按目标日期、视频类型和复刻附件为空建立不可变scope；每条保留原记录ID、原附件token、名称和精确字节数。业务仅上传自动分析、智能Logo、结果下载，以及官方飞书附件上传和原记录复刻附件写入。可选翻译仍独立使用，不自动加入本流程。

新批次默认使用 `pipeline` 异步推进，每条视频独立经过以下步骤：

1. 逐文件上传，收到回执立即保存原请求ID和任务映射，然后继续上传下一份。没有每轮5条或在途15条限制。
2. 只读查询各任务状态；某条自动分析成功，就立即为这条提交智能Logo，不等待其他文件上传或分析完成。单条分析证据保存在`analysis-ready/<record_id>.json`，不宣称整批分析完成。
3. 某条Logo成功且有真实结果，就进入下载、校验和原记录附件回填，不等待其他Logo生成完。平台建议人工复查的提示保留；提示本身不把有结果的成功状态改为失败。
4. 明确分析或Logo处理失败（含中断）的记录保留异常、跳过，不自动续跑Logo。回执未知进入下节的只读核对；其他已确认记录继续推进，最后统一核验远端附件及字段。

调度由一个异步事件循环协调三个固定I/O通道：上传、状态查询与Logo提交、结果下载与回填。上传通道一次只传一个原片，交付通道一次只处理一个结果；慢上传或慢下载不会占住状态查询通道。各通道有独立HTTP客户端并核对同一账号，同条记录只允许一个通道修改，断点与告警使用锁保护。没有为每个视频创建本地生成线程，也不改变服务端生成并发、模型或默认参数。`upload --workers`仍不提供。

这样缩短的是不同视频之间的等待时间，通常能更早拿到首批结果；单条云端生成耗时、网络带宽和服务端排队仍决定实际速度，不承诺固定倍数加速。

`--scheduling batch`保留整批阶段等待兼容模式：全部上传→全部非异常项分析完成→逐ID排入全部Logo→全部Logo完成→下载全部结果→回填。已有运行目录若保存了`scheduling.json`则沿用其模式，不自行改写旧策略；无策略文件的新批默认`pipeline`。需要显式切换时使用`run/resume --scheduling pipeline`，应先让旧worker退出，再从原断点启动唯一worker。已有任务ID、结果和附件回执继续复用，不重做已提交动作。

原片优先复用原记录断点绑定的`source_path/source_sha256`：只有文件存在、精确大小和已保存SHA256一致才复用，无需重新下载、完整解码或调用飞书取附件。没有可信缓存时，读取原行核对原附件身份，再下载这一份并紧接着上传Replica；不要求全批原片下载完才上传。输入仅做字节/哈希核验，不再预先完整解码；生成结果的完整解码保留。`download-sources`仅为可选的串行预取命令，常规流程无需执行。

以下使用新运行目录及示例日期，替换为实际授权范围；不会自动恢复此前173条旧批次：

```sh
.venv/bin/python -m ua_workflows.social_chat_replica prepare --date YYYY-MM-DD --expected-count N --run-dir data/replica-feishu-NEW
.venv/bin/python -m ua_workflows.social_chat_replica run --run-dir data/replica-feishu-NEW
.venv/bin/python -m ua_workflows.social_chat_replica status --run-dir data/replica-feishu-NEW
```

长批次可在`run`末尾加`--background`，日志为`worker.log`，进程及完成/待处理状态为`worker.json`，实时数量为`progress.json`。所有记录完成或明确异常后执行远端附件及其他字段核验，保存`verification.json`。`pause --run-dir ...`会停止领取新操作，等待各通道已经发出的请求结束并保存回执后暂停。`resume --run-dir ...`从同一scope和断点继续，不启动第二份worker；不同时启动多个处理进程。

已完成和已入队任务沿原ID继续，不重置断点。未知上传先核对原请求ID并按下节规则自动反查，未确认回执默认每60秒再查，避免每轮阻塞其他任务；404仍不证明未提交，不能据此重传。需要按任务列表/历史反查时参见前文只读接口说明。智能Logo失败或中断后不再次提交；历史续跑意图仍只读核对，未确认项不会被报告为成功。

### 自动重试规则

本地不设置批次大小或在途数量门槛。100条视频会连续进行100次上传；查询和结果交付独立推进，服务端决定分析与Logo生成并发。

| 情况 | 处理 |
|---|---|
| 上传收到429，或503同时明确返回“工作区正在准备”和`Retry-After`（平台约定本次未处理） | 持久化`upload_retry`、原请求ID、拒绝证据和下一次时间；等待后沿同一ID重试 |
| 上传超时、断线、普通5xx或回执不完整 | 保持未知，只查原请求ID及任务证据，不因404换ID重传 |
| 上传400等永久拒绝 | 单条失败，继续其余素材 |
| 分析或Logo明确失败、中断或请求被拒绝 | 单条记录原因并跳过，不重新生成；429仍会让受影响的请求通道等待 |
| 下载网络中断、5xx、429或字节不完整 | 保留`logo_ready`及结果身份，延迟重试，不再因为失败三次就丢弃；不重新生成视频 |
| 下载404、明确无结果、媒体无法解码或本地磁盘错误 | 记录失败或待处理，避免无限下载同一无效媒体；其余记录继续 |
| 401、403或账号改变 | 保存断点、等待已派发请求收尾后停止新增业务，要求处理认证/权限 |
| 飞书写入结果未知 | 沿原附件token、原行和发送记录只读核对，不重放写入 |

临时错误采用2、4、8秒逐步增加、最多60秒的本地退避；服务端`Retry-After`更长时遵守其等待时间，支持秒数和HTTP日期。pipeline的`service-retry.json`保存各通道等待期限，上传与下载的逐项期限也保存在原断点，重启和其他通道的进展通知不能绕过等待。初始化服务错误的期限保存在`initialization-retry.json`；等待时仍可暂停。长时间没有进展继续触发现有超时告警，不将未知结果标成成功或失败。

每次上传实际派发前会清除上一次拒绝证明，先持久化新的意图。即使先收到明确拒绝、随后重试又丢失回执，也不能拿第一次的拒绝证明再次POST。下载清单虽然使用POST，但仅查询已有文件，网络错误按只读操作恢复。

对`logo_unknown`可使用`reconcile-logos --run-dir ...`：只认证和GET原任务，有明确完成或接受证据才恢复状态，没有证据仍保持未知，不POST、不强制重做，也不改写活跃worker的CLI断点。响应保存在`logo-reconciliation/`。

结果写入前重读原行及原附件身份，已有复刻附件不覆盖；写后按token、名称、大小核对。写回结果未知时只读原记录核对，不盲目重放。源文件不转码，不增加人物图、换人、额外分析、完整一键生成或翻译，不改变服务端模型/并发和其他飞书字段。

离线测试覆盖快任务在慢分析完成前先回填、慢上传/下载不阻塞其他阶段、上传和下载各一路、超过15条持续上传、活跃附件上传不被误判为未知、未知Logo只读找回、暂停等待在途回执及账号改变停止。整批兼容模式、原片缓存命中/篡改拒绝、按需取片后立即上传、原请求ID不重放及旧断点复用回归继续保留。基础SDK/HTTP桥/翻译与记录回填回归分别执行；离线适配器不等同于新流程实站批量验收。本轮不重跑旧批次、不调用真实业务接口。

```sh
.venv/bin/python -m unittest discover -s tests -p 'test_replica*.py'
.venv/bin/python -m unittest discover -s tests -p test_social_chat_replica.py
```

## 异常恢复与超时告警

每条记录内部仍按上传、分析、Logo、下载回填的顺序推进；以下恢复不会自动重传原片、强制重新生成、转码或改变服务端设置。

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
| `--stage-timeout` | 7200秒 | pipeline按每条任务当前在途阶段计时；batch按整批阶段计时，超时继续等待 |
| `--unknown-timeout` | 900秒 | 未知回执的自动只读核对窗口；耗尽后保留未知 |
| `--reconcile-interval` | 60秒 | 未知回执的再次核对间隔 |

告警写入`alerts.json`（当前告警）、`alerts.jsonl`（产生/解除事件）和stdout结构化事件；`health.json`保存阶段开始、最后进展和未知起始时间，`status`暴露`active_alerts`、`needs_attention`和`reconciliation_pending`。相同告警不重复刷屏，恢复进展后解除对应告警。成功提交、下载和附件交付逐项更新`progress.json`，不再等整批完成才刷新。重复查询时间、轮询错误和无实质变化的revision不算进展。

超时不会取消云端任务、跳过仍在运行的任务或自动重新POST生成；pipeline中其他完成项继续向前，整批最终结束仍要等待未完成项得到确认或进入获准的异常状态。告警在有界HTTP调用之间检查，进程彻底退出或主机休眠时需外部巡检发现；CLI本身不创建调度、发送群聊或邮件。122项原回归之外，新增离线故障测试覆盖发送边界、附件中断隔离、迟到回执、候选身份/歧义、自动Logo找回、跨重启计时及告警解除，均禁止网络连接；不构成新的线上故障演练。
