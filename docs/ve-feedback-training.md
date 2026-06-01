# VE 反馈训练链路

这条链路用于把审核多维表里的「接受情况」沉淀成训练数据，和正常 Video Enhancer 抓取/分析/推送主流程分开运行。

默认数据源：

https://scnmrtumk0zm.feishu.cn/base/CivwbJ2HkazcKTsKnbGclA5RnWc?table=tblrZZvVuFcjL0kE&view=vewJtPixtM

## 做什么

- 直接从多维表拉取字段，不依赖 `data/video_enhancer_pipeline.db`。
- 记录 `接受情况`：`接受` / `采纳` / `入素材库` 作为正样本，`删除` / `不采纳` 作为负样本，`待定` / `重复抓取` / 空值只记录不进入训练。
- 只把素材本身作为模型特征：标题、正文、核心卖点、Hook、脚本/口播、玩法资产/变种、玩法指纹、差异点、AI 分析、素材标签等。
- 产品、广告主、投放日期、展示估值、人气、热度、地区等运营字段只做审计留存，不进入训练特征。
- 产出独立 SQLite、JSONL 训练集、baseline 模型 JSON 和日报。

## 产物

| 路径 | 说明 |
|------|------|
| `data/ve_feedback_training.db` | 独立反馈库 |
| `data/ve_feedback_training_dataset_YYYY-MM-DD.jsonl` | 可训练样本 |
| `data/models/ve_feedback_preference_nb_YYYY-MM-DD.json` | 无额外依赖的朴素贝叶斯 baseline |
| `reports/ve_feedback_training_YYYY-MM-DD.md` | 每日拉取与训练摘要 |

## 环境变量

| 变量 | 说明 |
|------|------|
| `FEISHU_APP_ID` / `FEISHU_APP_SECRET` | 读取多维表必需 |
| `VE_FEEDBACK_BITABLE_URL` | 可覆盖默认反馈表 URL |

## 手动运行

```bash
.venv/bin/python scripts/run_ve_feedback_training.py run --date 2026-05-15
```

只拉取不训练：

```bash
.venv/bin/python scripts/run_ve_feedback_training.py pull
```

只用本地反馈库重新导出和训练：

```bash
.venv/bin/python scripts/run_ve_feedback_training.py train --date 2026-05-15
```

只用核心素材字段齐全的样本训练：

```bash
.venv/bin/python scripts/run_ve_feedback_training.py train --date 2026-05-15 --complete-profile core
```

完整度口径：

| profile | 说明 |
|---------|------|
| `any` | 默认，不过滤缺字段样本 |
| `core` | 标题、视频/封面链接、核心卖点、Hook、脚本/口播、风险等级、AI 分析齐全 |
| `core_play` | `core` 之外，玩法资产/变种/ID、玩法指纹、差异点也齐全 |
| `play` | 只要求玩法资产/变种/ID、玩法指纹、差异点齐全 |
| `all` | 所有训练字段齐全；当前历史表因 `玩法判断理由` 缺失，通常会筛到 0 条 |

## 定时任务

脚本入口：

```bash
scripts/cron_ve_feedback_training_daily.sh
```

建议 crontab：

```text
40 9 * * * <REPO>/scripts/cron_ve_feedback_training_daily.sh >> <REPO>/logs/cron_ve_feedback_training.log 2>&1
```

这条任务只读审核多维表并写独立反馈训练产物，不会触发广大大爬取、VE 分析、多维表主表同步或日报推送。
