# 定时任务（macOS crontab）

## 目标

- **每天**跑两条「昨日最新」类生产流：**Video Enhancer（VE）**、**Arrow2 latest_yesterday**，均含 **分析 + 飞书同步**。
- **每天**另跑一条独立反馈训练流：**VE 反馈训练**，只读审核多维表，不触发抓取或同步。
- **每周三、六**在以上两条之外 **叠加** 跑 **Arrow2 exposure_top10（展示估值）**，同样含分析与同步。
- 与用户本机其它 crontab（常见 7:00–10:00 密集段）**错峰**。

## 当前排期（北京时间）

脚本内固定 `TZ=Asia/Shanghai`，cron 行中的「时:分」即 **北京时间**，不依赖 Mac 系统偏好时区。

| 频率 | 触发时间 | Shell 入口 | 说明 |
|------|----------|------------|------|
| 每天 | **05:20** | `scripts/cron_ai_video_enhancer_daily.sh` | `run_video_enhancer.py`，默认全流程（抓取、封面、入库、分析、多维表与推送） |
| 每天 | **09:40** | `scripts/cron_ve_feedback_training_daily.sh` | 从 VE 审核多维表拉取反馈，独立落库、导出训练集并训练 baseline |
| 每天 | **11:10** | `scripts/cron_ai_arrow2_latest_daily.sh` | `run_arrow2_latest.py --analyze` |
| 周三、六 | **14:20** | `scripts/cron_ai_arrow2_exposure_wed_sat.sh` | `run_arrow2_exposure.py --analyze`，与当日两条叠加 |

## crontab 中的标记块

安装时在用户 crontab 末尾追加，便于辨认与整体删除：

```text
# BEGIN ai- ua_workflows (video_enhancer + arrow2)
# 05:20 VE 昨日全流程 | 09:40 VE 反馈训练 | 11:10 Arrow2 latest | 14:20 Wed,Sat exposure（与每日两条叠加）
20 5 * * * <REPO>/scripts/cron_ai_video_enhancer_daily.sh >> <REPO>/logs/cron_video_enhancer.log 2>&1
40 9 * * * <REPO>/scripts/cron_ve_feedback_training_daily.sh >> <REPO>/logs/cron_ve_feedback_training.log 2>&1
10 11 * * * <REPO>/scripts/cron_ai_arrow2_latest_daily.sh >> <REPO>/logs/cron_arrow2_latest.log 2>&1
20 14 * * 3,6 <REPO>/scripts/cron_ai_arrow2_exposure_wed_sat.sh >> <REPO>/logs/cron_arrow2_exposure.log 2>&1
# END ai- ua_workflows
```

将 `<REPO>` 替换为仓库根路径（例如 `/Users/you/ai-`）。若迁移仓库目录，必须同步修改这三行。

## 日志

| 日志文件 | 任务 |
|----------|------|
| `logs/cron_video_enhancer.log` | VE 日更 |
| `logs/cron_ve_feedback_training.log` | VE 反馈训练 |
| `logs/cron_arrow2_latest.log` | Arrow2 昨日最新 |
| `logs/cron_arrow2_exposure.log` | Arrow2 展示估值（周三六） |

目录 `logs/` 由脚本首次运行前 `mkdir -p`。

## Shell 脚本约定

- `PYTHONUNBUFFERED=1`，便于 cron 环境下日志及时落盘。
- `PATH` 含 `/opt/homebrew/bin`、 `/usr/local/bin`，与常见 Homebrew 布局一致。
- 使用 `.venv/bin/python`；若无虚拟环境则脚本以非零退出码失败。

## 注意事项

1. **睡眠**：合上笔记本或进入睡眠后，`cron` 可能跳过执行；长期无人值守建议接电并调整「防止自动睡眠」，或改用 `launchd` + 唤醒策略。
2. **并发**：VE 与 Arrow2 均未与同一 cron 分钟内并行，避免多套 Playwright 同时抢广大大会话；同一天内先后顺序为 05:20 → 11:10。
3. **凭证**：依赖项目根 `.env`（广大大、`VIDEO_ENHANCER_*`、飞书、OpenRouter 等）；cron 环境无交互，密钥必须事先配置完备。
4. **维护**：增减任务时编辑 `crontab -e`，保留或更新 `BEGIN/END` 块；或直接改对应 `scripts/cron_ai_*.sh` 内部命令（例如临时加 `--skip-sync`）。

## 手动试跑

```bash
/path/to/repo/scripts/cron_ai_video_enhancer_daily.sh
/path/to/repo/scripts/cron_ve_feedback_training_daily.sh
/path/to/repo/scripts/cron_ai_arrow2_latest_daily.sh
/path/to/repo/scripts/cron_ai_arrow2_exposure_wed_sat.sh
```
