# Afizzy 05:00完整日流程：入口与依赖

当前入口是Mac mini现有Codex heartbeat `automation-3`（Afizzy竞品每日采集筛选入表），每天北京时间05:00。它由Agent按项目skill完成各阶段，不是Replica监听进程，也不是只有采集的cron脚本。入口迁移保留任务ID、关联聊天、ACTIVE状态、日期冻结和完整原业务指令，不创建第二份任务、不立即补跑。

| 阶段 | 代码与持久依赖 | 完成依据 |
|---|---|---|
| 调度及准备 | 代码`/Users/ggbond/oliver/replica-listener`；显式`UA_STATE_ROOT=/Users/ggbond/oliver/afizzy-state`；该代码目录`.venv/bin/python`、持久目录`.env` | 实际PROJECT_ROOT/STATE_ROOT/解释器和任务元数据读回匹配 |
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
