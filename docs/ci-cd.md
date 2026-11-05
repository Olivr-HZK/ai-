# 离线 CI 与 Mac mini 手动发布

本次实现三个阶段的工具：push/PR 离线 CI、指定 commit 的手动发布、健康检查失败自动回退。当前部署入口**仅用于 Replica 主表监听独立 checkout** `/Users/ggbond/oliver/replica-listener`；不适用于有未提交修改的旧 `ai-`、广大大采集 cron 或其他服务。

## 1. CI

`.github/workflows/offline-ci.yml` 在 GitHub 托管 Ubuntu 上执行固定测试白名单。只安装 `requirements-ci.txt` 的测试依赖（含固定向量运算所需 NumPy）和 FFmpeg；不安装完整 requirements、不下载 CLIP 模型或浏览器。CI 检查配置、预设、Skill、品牌参考哈希与 CLI 入口一致性，运行图片/视频断点、防重复派发、主表 Logo→翻译、横屏（含旋转和 SAR）、模板、日内/前七日封面去重、原素材附件交付的离线回归。封面测试使用固定向量，不加载模型。

Python audit guard 禁止网络、shell 和业务 CLI，禁 dotenv 读取；清除凭据与代理环境。原生子进程仅允许受限本地 Git 与强制 `file,pipe` 协议的 FFmpeg/ffprobe。GitHub 上另用 `unshare --net` 隔离整个进程树的网络。普通本机运行只有语言级保护，不是恶意代码安全沙箱。`pre-review` 的真实 CLI 子进程测试、会导入旧版 LLM 依赖树的聚类兼容性对照测试明确不在此门禁中；当前日内与七日聚类行为仍有直接回归。

```sh
.venv/bin/python -m pip install -r requirements-ci.txt
.venv/bin/python -B scripts/ci_offline.py
```

需要 Python 3.11+、支持 `-display_rotation` 的 FFmpeg 和 ffprobe。本机不要运行整个 `tests/` 作为发布验证；其中另有真实采集脚本。保护自测在普通 pytest 下会跳过，只有固定入口已安装 guard 时才尝试被拒绝的命令。

CI 不使用生产秘密、SSH 或自托管 runner。需要管理员在 GitHub 分支保护中把 `offline-contracts-and-tests` 设为必需检查；本次未改远端设置。工作流 `workflow_dispatch` 仅手动重跑 CI，不会部署。

## 2. Mac mini 发布准备

只在 Mini 独立目录操作；不得在开发机旧断点启动第二个监听。先正常暂停，确认 worker 退出、所有云端在途工作已妥善完成，且本地没有未知回执。正常暂停不会取消云端任务。工具检查 scope 中每一条的实际 checkpoint，缺记录、非终态、未知态或孤儿记录都会拒绝；失败项不会重做。

保留 `pause-requested`，按现有部署运维流程停止并卸载两个用户 LaunchAgent（`com.oliver.replica-watcher`、`com.oliver.replica-bridge`），确认其自有进程退出。不要重建认证、复制 Cookie、杀用户其他 Chrome。脚本只检查服务已停止，不替用户执行 `launchctl`；任何未知 `launchctl` 返回也会阻止发布。

先正常 fetch 到已通过 GitHub CI、已审查的完整 SHA；工具本身不 fetch、不验证 GitHub 检查回执、不自动安装依赖。生产 venv 需预装小型 CI 依赖和现有业务依赖。依赖变更需另行审查，本工具不修改共享 venv，因此代码回滚不会回滚依赖。

将示例 `config/replica_deploy.example.json` 复制到 Mini 的私有 `data/deployment/release-config.json`，填真实 `hostname`。runtime 必须为现有 supervisor 消费的 `ROOT/data/replica-main-table-worker`，python 必须为 `ROOT/.venv/bin/python`；现有独立依赖链接保留。回执目录必须在 runtime 之外。部署 checkout 必须 detached、tracked 文件干净，不清理任何未跟踪文件、不自动 stash。

## 3. 一键检查、切换与回滚

在 Mini 调用可信、已审查的发布器（首次可从开发交付包拷贝这一个脚本到部署目录之外）。以下为命令模板，替换 `FULL_CHECKED_SHA`：

```sh
.venv/bin/python /path/to/reviewed/replica_deploy.py \
  --config data/deployment/release-config.json --commit FULL_CHECKED_SHA --check-only

.venv/bin/python /path/to/reviewed/replica_deploy.py \
  --config data/deployment/release-config.json --commit FULL_CHECKED_SHA
```

工具取得同一 `watcher.lock`，检查暂停、宿主机、服务、逐条状态和干净代码；将目标 commit 的 tracked 源码导出到临时目录并用现有 venv 跑固定离线门禁。不会复制媒体、`.env`、浏览器目录或私有断点。候选必须包含 CI 入口。验收后持久记录意图和旧 SHA 的备份 Git ref，再原地 `checkout --detach SHA`。现有服务的绝对 ROOT 不变，后续正常启动会使用新源码，不依赖无消费者的 symlink。

切换后再跑离线门禁，并核对 runtime 全部文件 SHA 未变。失败则自动切回旧 SHA、再次检查；旧版尚无 CI 入口时只验证源码语法与 venv 依赖，回执明确 `legacy_source_syntax_and_dependencies_only`，不冒称旧版全回归通过。失败详情和有界离线测试输出写入回执。版本保留在 `refs/replica-releases/*`，不会自动清理。

成功状态为 **`code_deployed_not_resumed`**，自动回退为 `rolled_back_not_resumed`。两者均保持暂停，**不代表服务已恢复或业务端到端验收通过**。若回退健康也失败，返回 `rollback_failed_keep_paused`，需人工处理。进程在切换中崩溃时查看 `intent` 回执及 HEAD，不自动重放。

验收版本后，按既有 Mini 运维流程重新加载原 LaunchAgents、正常恢复同一会话并移除暂停标记，再核对新 worker 的真实命令、版本和 `watching_main_table`。该恢复会开放原表业务触发，未包含在这个代码发布工具中。不要使用业务 `plan` 作健康检查，它会访问飞书。

已上线版本需要手动回退时，保持相同暂停和服务停止条件，读取回执 `previous` 完整 SHA，再运行同一发布命令。该旧版本须有离线 CI 入口；首次部署前的旧版如无入口，只支持本次发布失败时的自动回退，人工恢复按原运维流程执行。

本次没有注册 runner、改 LaunchAgents/cron/调度、配置远程 CD 或执行真实部署。稳定后可单独接人工 `workflow_dispatch` 发布，届时仍需固定已检查 SHA、受保护环境和明确审批；PR 代码不能获得 Mini 执行权。

已知测试局限：既有 `test_unknown_logo_is_never_mistaken_for_translation_or_reposted` 曾偶发等待中间态超时；门禁保留该测试，失败即阻止发布，不自动重试或忽略。GitHub Linux 托管环境需在提交后实际验证，本次没有触发远端 CI。
