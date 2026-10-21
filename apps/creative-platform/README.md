# Guru 素材港

本机竞品素材工作台。现有素材库、人工复核、任务队列、断点及浏览器 Agent 交接保留；网站不自动驱动广大大、不调用私有接口。任务范围 v1 为视频。

## 启动

在项目根目录执行：

```sh
.venv/bin/python -m pip install -r apps/creative-platform/requirements.txt
.venv/bin/python apps/creative-platform/server.py --port 8877
```

打开 http://127.0.0.1:8877/ ，点击「使用 Guru SSO 登录」。服务启动必需有效的 `data/creative_platform/sso.json`，或用 `--sso-config /absolute/path/sso.json` 指定。配置缺失或错误不会回退密码登录。请使用项目虚拟环境，系统 Python 未必安装签名校验依赖。本机模式只允许绑定 127.0.0.1，暂未配置公网或可靠的开机自启。

## 认证结构

- `sso.py`：固定 Guru issuer、OIDC discovery、Authorization Code + PKCE S256、state / 浏览器绑定 / nonce、RS256 JWKS 签名与 issuer/audience/azp/exp/iat/at_hash 校验、账号映射和会话。
- `server.py`：登录及回调、登出、Cookie、同源检查、业务角色检查；前端不处理授权码或调用 token endpoint。
- `core.py`：原有业务模型与 SQLite。历史本地账号辅助方法保留作迁移兼容；HTTP 密码登录、注册、邀请码入口一律返回 410，业务授权只接受 SSO 会话。
- `manage_access.py`：本机操作员维护工作台准入清单，禁止按姓名或邮箱隐式关联账号。

认证流程：浏览器 `/auth/login` → Guru → `/auth/callback` → 服务端兑换并验签 → 轮换本地会话 → 原业务页面。300 秒登录事务在数据库中原子消费，拒绝重复参数、过期、重放和跨浏览器回调。回调后立即重定向清除 URL 中的 code；日志只记路径，不记录查询、Cookie、令牌或请求体。

浏览器通常只持有不透明随机会话 Cookie；服务端只存其 SHA256。HttpOnly / SameSite=Lax，HTTPS 配置自动启用 Secure，绝对有效期 8 小时。未请求 offline_access、不持久化 access/refresh token；用于标准 RP-Initiated Logout 的 ID token hint 以 AES-GCM 加密存储，关联会话哈希，密钥仅在本机 0600 文件。**登出时 hint 会按 OIDC 标准经浏览器交给固定 Guru end_session_endpoint**；它不会进入应用存储或日志。前端不保存 token 至 localStorage。

退出先撤销本机会话，再跳转 Guru 登出并校验一次性 logout state。即使 Guru 不可用，本地也已经撤销。Guru 登出不承诺同时退出 Google 等上游身份提供商；再次登录可能被上游 SSO 自动完成。当前未登记/接入 Back-Channel Logout；其他应用或 Guru 后台单独撤销登录不会实时撤销本站 Cookie，本站会话最长 8 小时。管理员收回准入后重启本站会立即拒绝该用户的所有会话。

## CLI 登记与配置

安装并登录用户指定的 CLI（CLI 会话由 macOS Keychain 管理，不能拿来当网站用户会话）：

```sh
npm install -g --ignore-scripts --prefer-online https://auth.upupup.today/cli-releases/download/latest/castbox-guru-sso-cli.tgz
guru-sso auth login
guru-sso --json auth status
```

已登记本机客户端 `guru-creative-platform-local`，声明见 `oidc-client.local.yaml`。使用 `guru-sso oidc-client export guru-creative-platform-local` 获取公开配置；真实回调与退出回调均已登记并回读。CLI 的 validate/plan/apply 必须按输出的当前 planId、resourceVersion 执行，不保存过期计划或盲重试结果未知的写操作。

本机准入配置模板为 `sso.example.json`。复制到忽略的运行目录后填写 CLI export 的 client_id、已核实的 Guru subject。`legacy_admin_subject` 仅用于明确把当前操作员的身份映射至原本地管理员，保留其任务所有权；不能对任意新用户启用。主体键始终是 `(issuer, subject)`，显示名和 email 不授予权限。首次启用迁移会清除旧会话和邀请码；不会修改素材、任务或证据文件。

```sh
.venv/bin/python apps/creative-platform/manage_access.py list
.venv/bin/python apps/creative-platform/manage_access.py allow <verified-subject-uuid>
.venv/bin/python apps/creative-platform/manage_access.py admin <verified-subject-uuid>
.venv/bin/python apps/creative-platform/manage_access.py revoke <verified-subject-uuid>
```

重启本机服务使变更生效。默认只准入已核实的当前操作员，没有开放「任意 Guru 用户自动加入」。成员可查看团队素材、创建任务、取消自己的任务；领取执行和执行指令仅管理员。服务端采集桥继续使用已有的独立 worker bearer credential，不能登录用户页面；机器凭据不通过 SSO 前端传递。

未来接公网时，必须另建 `web-confidential` 客户端，精确登记 HTTPS 回调和退出地址，再配置 `base_url`、`token_endpoint_auth_method=client_secret_basic` 及 `client_secret_file`。用 `guru-sso oidc-client secret export <name> --yes --output <owner-only-file>` 获取凭据，文件须 0600 且 clientId 匹配；不能将本机 public client 或临时 tunnel 地址当生产登记。反向代理必须保留原始 Host，后端继续仅监听本机，TLS 在可信入口终止；本服务不信任任意 X-Forwarded-* 来决定回调或安全 Cookie。生产配置和后台单点登出需在有正式 HTTPS 入口后另做真实验收。

## 验证与恢复

```sh
.venv/bin/python -m unittest discover -s apps/creative-platform/tests -v
node --check apps/creative-platform/public/app.js
```

测试使用临时库和本地签名提供方，覆盖实际 HTTP 登录→Cookie→回调→业务→登出、伪造 JWT、错误 issuer/audience/nonce、过期/重放、准入与权限、旧入口关闭及业务状态机；不会对广大大发起抓取。真实浏览器验收记录见 `SSO_ACCEPTANCE.md`。

运行数据、会话密钥和凭据仅在被 git 忽略的 `data/creative_platform/`，不可纳入代码或分享。此次迁移前完整 SQLite 快照为该目录的 `portal.before-sso.sqlite3`（私密）。回滚应先停服务并备份当前库，再由操作员决定恢复哪个快照；不要覆盖迁移后新任务。保持 SSO 的配置和密钥与数据库一并备份。不要重新启用密码 HTTP 入口作为故障兜底；修正配置或恢复服务即可。

标准参考：[OIDC Core](https://openid.net/specs/openid-connect-core-1_0.html)、[PKCE](https://www.rfc-editor.org/rfc/rfc7636)、[Native Loopback](https://www.rfc-editor.org/rfc/rfc8252)、[RP-Initiated Logout](https://openid.net/specs/openid-connect-rpinitiated-1_0.html)。

## 自然语言采集需求

「新建采集」顶部可直接输入需求，点击「AI 识别并填写」。AI 把竞品、平台、日期、分类、素材/广告、尺寸、渠道、地区、语言和内容条件映射至原表单；用户检查或修改后，点击创建才入队。解析请求不启动广大大抓取。

例如：`抓取Tipsy双端过去30天在Facebook和TikTok投放的9:16视频，英语和韩语，男性角色、女频，不要真人口播，地区不限。`

服务端 `task_intake.py` 通过本机 Codex CLI 解析，固定 `gpt-6-luna` + `model_reasoning_effort="medium"`，以 `--output-schema` 约束输出，再执行 JSON Schema 与业务校验；App ID 只来自本地已核实名单。同名双端不明确、条件矛盾、图片/首次发现/Top10%等当前平台尚不支持的要求会提示补充，不能静默丢弃。未知产品保留完整名称和明确平台，App ID 空置、采集前核实。

不再读取 OpenRouter / Google / OpenAI API key。安装并登录本机 Codex CLI 后，`codex login status` 应显示已登录；服务进程 PATH 必须能找到 `codex`。所有团队解析使用本机操作员已有的 Codex 账号及其额度，网站用户拿不到 CLI 登录凭据。CLI 调用采用参数数组、用户文字只走 stdin；每次在随机临时目录运行，不以项目目录为 cwd，不加载用户 config / 项目 AGENTS，使用独立解析系统指令。开启 read-only sandbox，关闭 shell、浏览器、插件、MCP 用户配置、hook、图片工具、多 agent 和其他非解析工具；`--ephemeral` 不保存对话，临时 schema / 结果 / 事件日志随请求清除。原需求及解析结果按业务草稿留存。任何意外工具事件或未完成回合均拒绝作为成功解析。

只发送需求文字、当天北京时间日期和公开竞品名称/平台注册表。SSO 身份、Cookie、API key、本地文件及素材数据不进入提示词；子进程环境只保留基础运行/代理与 CLI 登录定位变量，不继承项目 API key。CLI 返回的原始诊断不会下发前端或打印到项目服务日志。

需登录方可解析；每用户每分钟 5 次、全服务最多并发 2 次、单次 120 秒超时，文本上限 6000 字；超时终止整个解析进程组。解析失败不产生任务，可点击「改为手动填写」。草稿绑定创建用户、24 小时有效。提交后原需求、解析提案、默认值说明、模型/推理强度及最终表单分别留存，浏览器 Agent 只收到最终校验字段，原文不会作为执行指令。

2026-10-20 已通过 Codex CLI 实际解析 Tipsy 双端需求：映射 iOS `6572296119` / Android `com.tipsyturbo.app`，Facebook/TikTok、英语/韩语、30天、9:16 及三项内容条件。旧 OpenRouter 401 已由本机 CLI 通道替换，无需更新旧 API key。浏览器端及隔离回归测试记录见最新 AGENTS 工作日志。

官方用法：[Codex non-interactive / output schema](https://developers.openai.com/codex/noninteractive)、[配置参考](https://developers.openai.com/codex/config-reference)。
