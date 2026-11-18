# Afizzy 05:00完整日流程：入口与依赖

采集段是纯命令行：`scripts/afizzy_daily_0500.sh`（或等价launchd/cron）每天北京时间05:00执行`afizzy_daily daily`，只做采集、写心跳和阶段标记，不需要Codex在线。Codex只读`afizzy_daily check`/`watchdog`的结果，据此决定是否继续筛选、是否需要告警或补跑；筛选证据和入表仍按原规则由Agent完成。历史入口`automation-3`保留任务ID、关联聊天、ACTIVE状态和日期冻结，不创建第二份任务、不立即补跑。

| 阶段 | 代码与持久依赖 | 完成依据 |
|---|---|---|
| 调度及准备 | 代码`/Users/ggbond/oliver/replica-listener`；显式`UA_STATE_ROOT=/Users/ggbond/oliver/afizzy-state`；该代码目录`scripts/afizzy-python`、持久目录`.env` | 实际PROJECT_ROOT/STATE_ROOT/解释器和任务元数据读回匹配 |
| 全量采集与下载 | `ua_workflows.guangdada_direct`；game/tool/tool2；`data/afizzy_daily/collector-state`、`raw` | 原目标日、真实页/日期截止、原地址、成功/失败和媒体哈希，不用部分下载冒充完整 |
| 原素材首筛 | 当前config/skill的原尺寸和明确重投规则 | 原文件显示尺寸/源SHA绑定的排除与待核证据 |
| 封面去重 | 日内和前7自然日CLIP0.9；`data/social_chat_clip_vectors`及`social_chat_plugin_reprocess_v2/embedding-cache` | 同模型指纹、图像SHA、产品/系统身份和历史覆盖 |
| 联合审核 | 一套真实六帧/全部原图；主体、风格、D−1和同日模板；实际主要文案/完整音轨 | 原审核/模板/语言证据、用户确认和当前policy；存疑不伪造通过 |
| 交付及入表 | 全部合格项、既有表和原附件字段、原唯一键/上传缓存/读回回执 | 原素材字段、token、名称、精确大小等独立核验，保护历史及手动记录 |
| 正式日报 | `ua_workflows.afizzy_daily_report`、既有Feishu/WeCom配置；原`report-receipt`、`wecom-report-receipt`及send-intent | minimal-v4、两端各一次、未知提交不重放；已完成只核验 |
| 日任务结束 | 保存待手动复刻清单 | 不自动触发复刻、不暂停/重启/接管独立监听 |

所有相对路径均以新代码目录为workdir，兼容链接解析到同一持久目录。不要在旧`ai-`执行每日代码或读取旧环境；历史索引中的旧绝对素材路径仍只读有效，不能因此删除旧路径入口。

## 迁移验收

先备份原任务和原业务正文，核对采集读取器、配置、去重/模板、语言/入表及日报依赖；仅改执行位置、环境绑定和早期采集示例的已确认三预设参数。原业务正文其余内容逐字保留，当前config/skill和最新覆盖规则继续优先，旧scope不重算。

只读验收包括新环境CLI help、路径解析、旧/新日期目录同一位置、向量包身份、前日模板和审核资料可读、原日scope/入表/两端日报回执哈希不变，以及调度ID/时间/状态/关联聊天不变。真实网页登录、全日下载、视觉审核与入表/发消息只能在已授权日执行中实际验收；入口检查不冒称新一日执行成功。

迁移日任务不重启独立复刻监听，不将其他开发分支的未发布改动带入生产。

所有日任务Python命令通过代码目录下的`scripts/afizzy-python`执行：它固定cwd，使用该checkout的独立.venv，推导并导出UA_STATE_ROOT，拒绝与现有UA_STATE_ROOT不一致的调用。迁移不依赖关联聊天仍保留的旧工作目录。

## 运行卡点：心跳、阶段标记与向量补算（2026-11-18）

| 命令 | 作用 | 产物 |
|---|---|---|
| `afizzy_daily daily --date D --run-dir R` | 05:00采集入口：写心跳、跑crawl、记录成功/失败 | `R/heartbeat.json`、`R/execution-control.json` |
| `afizzy_daily heartbeat --date D --run-dir R --status started\|done\|failed [--stage S] [--note T]` | 手工补记心跳 | 同上 |
| `afizzy_daily stage --date D --run-dir R --stage S --status running\|done\|failed` | 记录单个阶段；prepare/embed/pre-review会自己写 | `execution-control.json` |
| `afizzy_daily check --date D --run-dir R [--max-age 120] [--notify]` | 判断missing/running/stalled/failed/done；停滞或失败退出码2 | stdout JSON |
| `afizzy_daily watchdog --date D --run-dir R [--max-age] [--notify] [--backfill]` | 同上并给出告警文案；`--backfill`仅在完全没启动时补跑采集 | stdout JSON |
| `afizzy_daily embed --media-index I --embedding-cache C --history-vector-pack P [--limit N] [--dry-run]` | 生成缺失的CLIP向量；已有缓存不重写，可分批续跑 | `C/<image-sha256>.json` |

告警只在项目`.env`配置了`AFIZZY_ALERT_WECOM_WEBHOOK`（企业微信机器人完整地址）时发送；未配置时`notify`返回`not_configured`，不发送、不报错。

`pre-review`遇到缺失/损坏/未绑定模型指纹的向量缓存时不再整批中止：该素材标记为`not_deduped`并照常进入人工审核队列，`plan.json`的`dedupe_unavailable`逐条记录原因。只有代理图字节被改动、或同一批出现多个模型指纹时才继续阻断。

历史向量包在当天没有任何可用向量时被显式跳过（`history_vector_pack.skipped=no_current_vectors`），不会让整批失败。

调度建议（本仓库只提供脚本，不改线上launchd）：

- 05:00：`scripts/afizzy_daily_0500.sh` —— 采集 + 心跳；失败退出码非0，便于launchd记录。
- 每30~60分钟：`scripts/afizzy_daily_watchdog.sh` —— 检查心跳与阶段标记，超过`AFIZZY_STALL_MINUTES`（默认120）无更新即退出码2；配置`AFIZZY_WATCHDOG_ARGS=--notify`后同时发企业微信告警。
- 漏跑的补跑由watchdog `--backfill`显式触发，只在完全没有启动痕迹时执行，不覆盖已开始的批次。

筛选阶段（joint-review/select/sync/report）由Agent执行时调用
`afizzy_daily stage --date D --run-dir R --stage joint-review --status running|done|failed` 记录进度，停止后watchdog才能发现。
