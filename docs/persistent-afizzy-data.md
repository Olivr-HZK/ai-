# Afizzy 持久数据与独立部署

代码保存在部署 checkout，历史和运行数据保存在独立的持久目录。当前改造只覆盖广大大/Afizzy；旧 VE、Arrow2、抖音、小红书服务保持原目录与调度，不据此删除旧项目。

建议 Mac mini 布局：

- `/Users/ggbond/oliver/replica-listener`：部署代码，固定已通过 CI 的 main 提交。
- `/Users/ggbond/oliver/afizzy-state/data`：Afizzy 原包、媒体、审核证据、CLIP 向量、模板历史、复刻断点和该监听独有会话。
- `afizzy-state/reports`：对应历史报告。
- `afizzy-state/.env`：在 Mini 内复制并核对的既有应用配置，权限0600，不输出内容。
- `afizzy-state/.venv`：按已安装版本清单独立创建，不复用旧 `.venv` 链接，不改变旧环境。

新部署进程显式设置 `UA_STATE_ROOT`。不设置时保留原路径语义；设置为空、相对路径、目录缺失或缺少 data/reports 时拒绝启动，避免误建空历史。shared.config 的 DATA_DIR/REPORTS_DIR、Replica 状态和专用认证桥、独立采集 CLI 默认路径随之指向持久目录。代码资源、skill 和业务配置仍来自代码 checkout。

仍存在使用 `ROOT/data` 的兼容代码，以及历史 JSON 内冻结的绝对证据路径。因此正式切换必须同时配置：部署 checkout 的 data/reports/.env/.venv 为持久目录兼容链接；旧项目选中的 Afizzy 子目录也保留指向对应持久数据的链接。不能删历史路径、批量重写 JSON 或重算审核/上传身份来掩盖路径变化。旧项目其他数据和认证桥隔离保留。

## 在线准备

1. 只读盘点原目录、数据库、空间、进程和调度。原抓取与监听保持运行。
2. 本地分支完成路径适配及离线测试，通过 GitHub CI 后合并 main。
3. 用 `ua_workflows.persistent_storage stage` 复制选定历史到影子目录。源目录不写入；APFS 使用 `--clone` 创建写时复制文件，绝不建立硬链接。对源/目标逐文件 SHA256 核对，源文件集合变化、复制期间变化、损坏或配置不一致均不能认证完成。
4. 不复制正在使用的专用浏览器 profile 或 Python 环境。配置 excluded_top_level 后只登记排除项，activate 会拒绝未处理排除项。不要拿普通文件复制冒充 SQLite 在线一致性备份；本批只选择 Afizzy 的文件历史，旧 VE/Arrow2 数据库留原位。
5. 在新持久目录创建独立 Python 环境、安装已冻结的依赖并验证。原环境不改动。凭据只在 Mini 上处理，不通过聊天/开发机传输。

通用入口（参数使用该机私有清单）：

```sh
python -m ua_workflows.persistent_storage stage --config CONFIG.json --receipt RECEIPT.json --clone
python -m ua_workflows.persistent_storage verify --config CONFIG.json --receipt RECEIPT.json
```

配置 entries 为显式 source绝对目录/target持久目录内相对路径；目标不得重叠或穿越、不得覆盖不归本次迁移管理的非空目录、不得跟随源/目标中的未知 symlink。阶段结果 shadow_verified 只代表影子副本，不代表已切换服务。

## 最后切换窗口

**所有重构、CI、影子准备完成前，不停止旧抓取或 Mini 监听。** 完成后才进入最短窗口：正常暂停相关生产者，确认无在途/未知任务并持有原 watcher/collector 锁；停止对应守护器防止自动重启，不操作无关服务。再次同步增量并 verify。

专用 profile 与嵌套 clip-env 只在所有权已确认、使用者退出后于同一台 Mini 原子重命名到目标，再把旧路径设为兼容链接；不导出 Cookie、不创建新认证/复制日常浏览器资料。excluded_top_level 只有已经完成上述迁移、且源链接精确指向真实目标目录时才允许 activate。

```sh
python -m ua_workflows.persistent_storage activate --config CONFIG.json --receipt RECEIPT.json --journal CUTOVER.json --quiesced
```

activate 验证实际文件和已有锁，不创建缺失 guard 充当空闲证据；先持久化意图，再将旧目录改名为 `.before-persistent` 备份并创建链接。失败在进程内恢复原目录，未知/崩溃留日志，禁止盲目重复 activate。`--quiesced` 不能代替运维侧对守护器、生产者和后台任务的检查。

服务使用跟踪于代码库的 scripts/replica_supervisor.py；不能再用 data/deployment/supervisor.py 的 __file__.resolve() 推导代码位置，因为 data 将是持久目录链接。最后部署已验证 main、核验代码目录链接和 UA_STATE_ROOT、原账号、30条原任务身份、失败不重跑、历史向量包可读及审核证据哈希；恢复唯一监听和原抓取入口，保存远端回执。先保留全部备份，确认运行稳定后再另行清理。

## 验证边界

测试覆盖默认兼容与明确路径、错误路径拒绝、影子源不变且无硬链接、损坏/新增文件/配置变化拦截、活动锁阻止切换、profile不被静默遗漏、链接失败回滚和旧路径可读。此模块不停止/启动进程、不登录或爬取、不调用付费生成、飞书写入或群消息。
