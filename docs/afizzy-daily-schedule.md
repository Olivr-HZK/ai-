# Afizzy 日常调度（Mac mini，2026-11-18 生效）

采集与筛选拆成两段：**采集是纯命令行 launchd 任务，不需要 Codex 在线**；Codex 只读采集状态，决定是否继续筛选。

| 任务 | 时间（Asia/Shanghai） | 入口 | 日志 |
|---|---|---|---|
| 采集 `com.afizzy.daily-0500` | 每天 05:00 | `scripts/afizzy_daily_0500.sh` → `afizzy_daily daily` | `afizzy-state/data/logs/afizzy-daily-0500.log` |
| 看门狗 `com.afizzy.daily-watchdog` | 每小时第 20 分 | `scripts/afizzy_daily_watchdog.sh` → `afizzy_daily watchdog --notify` | `afizzy-state/data/logs/afizzy-daily-watchdog.log` |
| 筛选 Agent `automation-3`（Codex heartbeat） | 每天 06:00 | 只读 `afizzy_daily check`，`done` 才继续 prepare/审核/入表/日报 | 原关联聊天 |

plist 模板在 `scripts/launchd/`，路径按 mini 实际部署写死（代码目录 `/Users/ggbond/oliver/replica-listener`，状态根 `/Users/ggbond/oliver/afizzy-state`）。

## 安装与验证

```bash
cp scripts/launchd/com.afizzy.daily-0500.plist scripts/launchd/com.afizzy.daily-watchdog.plist ~/Library/LaunchAgents/
launchctl bootout   gui/$(id -u)/com.afizzy.daily-0500   2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.afizzy.daily-0500.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.afizzy.daily-watchdog.plist
launchctl list | grep afizzy.daily
launchctl kickstart -k gui/$(id -u)/com.afizzy.daily-watchdog   # 只读检查，安全
```

看门狗退出码 0 = 正常/已完成，2 = `missing/stalled/failed`（launchd 日志可见，配置了告警 webhook 时同时发送）。

## 采集段的行为

- **环境和断点目录固定**：采集用本 checkout 的 `.venv`（不再回退到 `guangdada-direct-0.2.1` 的旧虚拟环境），断点目录默认 `afizzy-state/data/afizzy_daily/collector-state`（不再默认 `~/.local/share/gdd-direct`），输出默认 `afizzy-state/data/afizzy_daily/raw`。需要覆盖时显式传 `--crawler-python / --state-dir`；
- `afizzy_daily daily` 先写 `heartbeat.json`，跑采集期间**每 120 秒刷新心跳**（`AFIZZY_HEARTBEAT_SECONDS` 可调），避免长批次被看门狗误判停滞；
- 目标日 = 北京时间昨天，由脚本计算，不接受手工改；
- **部分媒体未取得不阻断后处理**：采集器在"网页范围完整、少量媒体失败"时退出码是 2，入口会读 `summary.json`：`complete=true` 且 `attention` 为空、`media_failed>0` → 记 `done_with_gaps`（心跳 note 带 `media_failed=N; continue_with_gaps`），06:00 的筛选照常继续并披露缺口；网页没采完、有 attention 或登录/断点异常仍记 `failed` 并停止。

## 筛选 Agent（automation-3）改动

- 时间 05:00 → **06:00**（给采集留出约 20 分钟）；
- 开头加了一段覆盖指令：**禁止**再调用 `crawl/resume-crawl/download-collected`、禁止打开浏览器采集、禁止重复下载；
- 第一步改为 `afizzy_daily check --date <目标日>` 并分流：
  - `done` → 继续 prepare、向量、pre-review、联合审核、入表、日报；
  - `running` 且心跳未超 120 分钟 → 每 10 分钟复检，最多等 60 分钟；
  - `missing/stalled/failed` → 原文报告并停止，不补采集、不改目标日。
- `model_prompt_sha256` 已按新 prompt 重算；原 prompt 的规则正文（五预设、模板/内容/语言、入表唯一键、两端日报、不自动复刻）保留在覆盖段之后。

## 回滚

```bash
launchctl bootout gui/$(id -u)/com.afizzy.daily-0500
launchctl bootout gui/$(id -u)/com.afizzy.daily-watchdog
cp ~/.codex/automations/automation-3/automation.toml.bak-20261118 ~/.codex/automations/automation-3/automation.toml
```

备份文件生成于 2026-11-18 改动前；回滚后 automation-3 恢复 05:00 且自行采集。

## 告警（飞书）

`watchdog --notify` 只在**缺跑 / 停滞 / 失败**时发送，`running`/`done` 不发，避免每小时刷屏。

通道在项目 `.env` 配置，按顺序取第一个可用的：

| 变量 | 说明 |
|---|---|
| `AFIZZY_ALERT_FEISHU_CHAT_ID` | 飞书群 `chat_id`（推荐，走应用机器人 `im/v1/messages` 纯文本） |
| `AFIZZY_ALERT_FEISHU_CHAT_NAME` | 可选，群名，仅用于回执记录 |
| `AFIZZY_ALERT_WECOM_WEBHOOK` | 备用：企业微信群机器人完整地址 |

都没配置时返回 `not_configured`，只写日志、不发送。日报使用的 `AFIZZY_WECOM_WEBHOOK` 和 `crawl_completion_report` 群不会被自动复用，需要显式配置才发。
