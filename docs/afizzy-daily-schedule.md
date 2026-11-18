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

- `afizzy_daily daily` 先写 `heartbeat.json`，再跑五预设采集，最后记 `crawl done/failed`；
- 采集失败时心跳和阶段都记 `failed`，不会留下"看起来还在跑"的假状态；
- 目标日 = 北京时间昨天，由脚本计算，不接受手工改；
- 采集有缺口照常继续（按既定口径），日账披露缺口。

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

## 告警

`watchdog --notify` 只在项目 `.env` 配置 `AFIZZY_ALERT_WECOM_WEBHOOK`（企业微信机器人完整地址）时发送；当前 mini 未配置，告警只写日志，不发送任何消息。日报使用的 `AFIZZY_WECOM_WEBHOOK` 不会被用于告警。
