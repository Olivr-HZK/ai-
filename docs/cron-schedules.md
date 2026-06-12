# 定时任务（macOS crontab）

## 目标

- **每天**跑两条「昨日最新」类生产流：**Video Enhancer（VE）**、**Arrow2 latest_yesterday**，均含 **分析 + 飞书同步**。
- **每天**另跑一条独立反馈训练流：**VE 反馈训练**，只读审核多维表，不触发抓取或同步。
- **每周一**跑一条 **VE 竞品周检查**，输出低量/低采纳老竞品检查与新竞品候选，并推送飞书。
- **每周三、六**在以上两条之外 **叠加** 跑 **Arrow2 exposure_top10（展示估值）**，同样含分析与同步。
- **每周一**跑一次 **VE 留存维护 dry-run**，只产出归档/清理候选报告，默认不改多维表、不删文件。
- 与用户本机其它 crontab（常见 7:00–10:00 密集段）**错峰**。

## 当前排期（北京时间）

脚本内固定 `TZ=Asia/Shanghai`，cron 行中的「时:分」即 **北京时间**，不依赖 Mac 系统偏好时区。

| 频率 | 触发时间 | Shell 入口 | 说明 |
|------|----------|------------|------|
| 每天 | **05:20** | `scripts/cron_ai_video_enhancer_daily.sh` | `run_video_enhancer.py`，启动时先从多维表 `竞品list` 同步正式竞品，随后跑默认全流程（抓取、封面、视频内容 LLM 筛选、极简分析、多维表与筛选漏斗报告）；浩鹏 TopN 默认关闭，需 `VE_HAOPENG_TOPN_ENABLED=1` |
| 每天 | **09:40** | `scripts/cron_ve_feedback_training_daily.sh` | 从 VE 审核多维表拉取反馈，独立落库、导出训练集并训练 baseline |
| 每天 | **11:10** | `scripts/cron_ai_arrow2_latest_daily.sh` | `run_arrow2_latest.py --analyze` |
| 周一 | **12:40** | `scripts/cron_ve_retention_weekly.sh` | VE 留存维护 dry-run，报告多维表归档候选与本地产物清理候选 |
| 周一 | **13:10** | `scripts/run_ve_weekly_competitor_review_server.sh` | VE 竞品周检查：老竞品低量/低采纳恶化检查 + AI 图像/视频周榜新竞品候选 |
| 周三、六 | **14:20** | `scripts/cron_ai_arrow2_exposure_wed_sat.sh` | `run_arrow2_exposure.py --analyze`，与当日两条叠加 |

## crontab 中的标记块

安装时在用户 crontab 末尾追加，便于辨认与整体删除：

```text
# BEGIN ai- ua_workflows (video_enhancer + arrow2)
# 05:20 VE 昨日全流程 | 09:40 VE 反馈训练 | 11:10 Arrow2 latest | 12:40 Mon retention dry-run | 周一13:10 VE竞品周检查 | 14:20 Wed,Sat exposure（与每日两条叠加）
20 5 * * * <REPO>/scripts/cron_ai_video_enhancer_daily.sh >> <REPO>/logs/cron_video_enhancer.log 2>&1
40 9 * * * <REPO>/scripts/cron_ve_feedback_training_daily.sh >> <REPO>/logs/cron_ve_feedback_training.log 2>&1
10 11 * * * <REPO>/scripts/cron_ai_arrow2_latest_daily.sh >> <REPO>/logs/cron_arrow2_latest.log 2>&1
40 12 * * 1 <REPO>/scripts/cron_ve_retention_weekly.sh >> <REPO>/logs/cron_ve_retention.log 2>&1
10 13 * * 1 <REPO>/scripts/run_ve_weekly_competitor_review_server.sh >> <REPO>/logs/cron_ve_weekly_competitor_review.log 2>&1
20 14 * * 3,6 <REPO>/scripts/cron_ai_arrow2_exposure_wed_sat.sh >> <REPO>/logs/cron_arrow2_exposure.log 2>&1
# END ai- ua_workflows
```

将 `<REPO>` 替换为仓库根路径（例如 `/Users/you/ai-`）。若迁移仓库目录，必须同步修改这六行。

## 日志

| 日志文件 | 任务 |
|----------|------|
| `logs/cron_video_enhancer.log` | VE 日更（浩鹏 TopN 默认关闭；打开后由主流程内部执行） |
| `logs/cron_ve_feedback_training.log` | VE 反馈训练 |
| `logs/cron_arrow2_latest.log` | Arrow2 昨日最新 |
| `logs/cron_ve_retention.log` | VE 留存维护 dry-run / 可选执行 |
| `logs/cron_ve_weekly_competitor_review.log` | VE 竞品周检查 |
| `logs/cron_arrow2_exposure.log` | Arrow2 展示估值（周三六） |

目录 `logs/` 由脚本首次运行前 `mkdir -p`。

## Shell 脚本约定

- `PYTHONUNBUFFERED=1`，便于 cron 环境下日志及时落盘。
- `PATH` 含 `/opt/homebrew/bin`、 `/usr/local/bin`，与常见 Homebrew 布局一致。
- 使用 `.venv/bin/python`；若无虚拟环境则脚本以非零退出码失败。

## 注意事项

1. **睡眠**：合上笔记本或进入睡眠后，`cron` 可能跳过执行；长期无人值守建议接电并调整「防止自动睡眠」，或改用 `launchd` + 唤醒策略。
2. **并发**：VE 与 Arrow2 均未与同一 cron 分钟内并行，避免多套 Playwright 同时抢广大大会话；同一天内先后顺序为 05:20 → 11:10 → 周一 12:40 → 周一 13:10 → 周三/六 14:20。
3. **凭证**：依赖项目根 `.env`（广大大、`VIDEO_ENHANCER_*`、飞书、OpenRouter 等）；cron 环境无交互，密钥必须事先配置完备。浩鹏 TopN 默认关闭；如需临时恢复，设置 `VE_HAOPENG_TOPN_ENABLED=1`，发送方式仍读取 `VE_HAOPENG_TOPN_SEND_MODE`、`FEISHU_DAILY_PUSH_CHAT_ID` 或专用 webhook。VE 留存维护默认 dry-run；如需实际写归档标记或删除本地产物，通过 `VE_RETENTION_EXTRA_ARGS="--apply-bitable --apply-local"` 显式打开。
4. **维护**：增减任务时编辑 `crontab -e`，保留或更新 `BEGIN/END` 块；或直接改对应 `scripts/cron_ai_*.sh` 内部命令（例如临时加 `--skip-sync`）。
5. **VE 竞品来源**：VE 日更启动时默认同步多维表 `竞品list` 到 `config/ai_product.json`；若设置 `VE_COMPETITOR_LIST_SYNC_ENABLED=0` 或同步失败，会沿用本地配置继续跑。周一竞品检查只负责“推荐新增/建议移除/低量观察”的报告和推送，不会自动改正式竞品表。
6. **人机验证**：VE 日更主爬取和周一竞品周检查的新创意榜采集都已接入广大大安全验证飞书人工闸口；检测到验证时会发飞书 IM 卡片，等待人工完成页面验证并点击「已完成」后重启对应抓取入口。这不是自动破解验证码。
7. **VE 推送默认口径**：VE 主流程不再默认发旧素材日报、企业微信、Google Sheet 或浩鹏 TopN。若临时恢复旧渠道，手动传 `--send-material-card`、`--send-wecom`、`--sync-sheet` 或设置 `VE_HAOPENG_TOPN_ENABLED=1`。

## 手动试跑

```bash
/path/to/repo/scripts/cron_ai_video_enhancer_daily.sh
/path/to/repo/scripts/cron_ve_feedback_training_daily.sh
/path/to/repo/scripts/cron_ai_arrow2_latest_daily.sh
/path/to/repo/scripts/cron_ve_retention_weekly.sh
/path/to/repo/scripts/run_ve_weekly_competitor_review_server.sh
/path/to/repo/scripts/cron_ai_arrow2_exposure_wed_sat.sh
```
