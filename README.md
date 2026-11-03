# UA 素材 · 竞品采集与复刻

本项目旨在爬取竞品广告素材，完成去重、内容审核和风格打标，再进行素材复刻，**目前聚焦 Afizzy 项目**。

当前主流程为：**爬取 → 筛选 → 复刻**。

| 阶段 | 当前情况 | 后续方向 |
|------|----------|----------|
| 爬取 | 用户手动用广大大浏览器插件采集，将目标日期和原始 ZIP 交给 Agent | 实现无头浏览器定时稳定采集；目前只有隔离样本可行性验证，未完成生产无人值守验收 |
| 筛选 | 重投剔除、日内/跨日7天封面去重；按产品联合做主体审核、风格打标、日内及前一天模板去重；标一个主语言，全部合格项入表 | 完善历史封面向量、逐日模板证据库与复刻输入清单的衔接 |
| 复刻 | 视频复刻、图片复刻已完成；视频走 Replica Studio CLI（换 Logo、翻译），新批图片入库核验后由 Agent 使用内置图片生成工具替换 Afizzy 品牌并回填复刻附件 | 完成 Playable 复刻；视频侧后续实现换人 |

完整流程、阶段现状和后续验收目标见 **[Afizzy 竞品素材流程与路线图](docs/afizzy-material-workflow.md)**（更新于2026-11-03）。未来方向不表示已实现，也不自动启动采集或复刻任务。

## 既有 VE / Arrow2 工作流

仓库仍保留以下既有工作流，其入口及说明用于维护原业务，不作为 Afizzy 当前默认入口：

1. **Video Enhancer**：竞品素材抓取、结构化灵感分析、玩法资产判断、飞书多维表同步与浩鹏 TopN 推送。
2. **Arrow2 latest_yesterday**：Arrow2 竞品每日最新素材。
3. **Arrow2 exposure_top10**：Arrow2 竞品展示估值素材。

核心代码位于 `ua_workflows/`，浏览器插件位于 `apps/`，执行入口位于 `scripts/`。

## 项目文档

| 说明 | 路径 |
|------|------|
| **Afizzy 当前流程与路线图** | [docs/afizzy-material-workflow.md](docs/afizzy-material-workflow.md) |
| Replica Studio CLI | [docs/replica-studio-cli.md](docs/replica-studio-cli.md) |
| 图片品牌复刻 | [image-replication.md](skills/social-chat-browser-daily/references/image-replication.md) |
| 小红书当前流程 | [docs/social-chat-xhs-daily.md](docs/social-chat-xhs-daily.md) |
| 文档索引 | [docs/README.md](docs/README.md) |
| **定时任务（crontab）** | [docs/cron-schedules.md](docs/cron-schedules.md) |
| 三条工作流说明 | [docs/workflows.md](docs/workflows.md) |
| 环境、`data/`、换机迁库 | [docs/setup-and-data.md](docs/setup-and-data.md) |
| VE 玩法资产库联动 | [docs/ve-play-assets.md](docs/ve-play-assets.md) |

Agent 变更日志见根目录 [AGENTS.md](AGENTS.md)。

## 环境准备

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

## 既有 VE / Arrow2 目录与入口

```text
ua素材/
├── ua_workflows/
│   ├── video_enhancer/
│   ├── arrow2/
│   └── shared/
├── scripts/
│   ├── run_video_enhancer.py
│   ├── run_ve_haopeng_ai_filter.py
│   ├── run_ve_haopeng_topn_push.py
│   ├── run_arrow2_latest.py
│   ├── run_arrow2_exposure.py
│   ├── cron_ai_video_enhancer_daily.sh
│   ├── cron_ai_arrow2_latest_daily.sh
│   ├── cron_ai_arrow2_exposure_wed_sat.sh
│   ├── test_video_enhancer_crawl.py
│   ├── test_arrow2_latest_crawl.py
│   └── test_arrow2_exposure_crawl.py
├── docs/
├── config/
├── data/
├── reports/
└── archive/
```

## 既有 VE / Arrow2 运行方式

在项目根目录下执行：

```bash
.venv/bin/python scripts/run_video_enhancer.py --date 2026-05-05
.venv/bin/python scripts/run_ve_haopeng_topn_push.py --date 2026-05-05 --top-n 10
.venv/bin/python scripts/run_arrow2_latest.py --date 2026-05-05 --analyze
.venv/bin/python scripts/run_arrow2_exposure.py --date 2026-05-05 --analyze
```

浩鹏 TopN 默认会重新生成目标日二次 AI 筛选报告，并用目标日前历史反馈判断“浩鹏采纳偏好类型”；目标日当天反馈只用于事后回测，不传给模型。每日 cron 追加推送读取 `FEISHU_DAILY_PUSH_CHAT_ID` 走飞书 IM 卡片，卡片末尾会带 `VIDEO_ENHANCER_BITABLE_URL` 的多维表按钮。

不需要分析时可去掉 `--analyze`。Arrow2 可用 `--skip-sync` 跳过飞书同步。

## 既有 VE / Arrow2 爬取冒烟测试

三个测试脚本只验证爬取，一个产品，不入库、不同步：

```bash
.venv/bin/python scripts/test_video_enhancer_crawl.py --headless
.venv/bin/python scripts/test_arrow2_latest_crawl.py --headless
.venv/bin/python scripts/test_arrow2_exposure_crawl.py --headless
```

需要看浏览器时改用 `--headed`，需要指定产品时传 `--product`：

```bash
.venv/bin/python scripts/test_arrow2_latest_crawl.py --headed --product "Arrows - Puzzle Escape"
```

## 必要配置

- `config/ai_product.json`：Video Enhancer 竞品配置。
- `config/ve_play_assets.json`：Video Enhancer 玩法资产库本地兜底；默认会先尝试从飞书云文档同步最新版本。
- `config/arrow2_competitor.json`：Arrow2 竞品与 `pull_specs` 配置。
- `config/iso3166_alpha3_zh.json`：国家代码映射。
- `.env`：广大大登录、OpenRouter/OpenAI、Feishu、企业微信、Google Sheet 等密钥。

## 验证

```bash
source .venv/bin/activate
python -m compileall ua_workflows scripts
.venv/bin/python scripts/run_video_enhancer.py --help
.venv/bin/python scripts/run_arrow2_latest.py --help
.venv/bin/python scripts/run_arrow2_exposure.py --help
```

## 归档说明

旧 UA、hot rank/new rank、playable、调试探针、一次性 backfill/preview/test 脚本已归档到 `archive/removed_scripts_2026_05_06/`。VE/Arrow2 原业务沿各自入口维护；Afizzy 新批次按上方当前流程处理，不默认调用旧爬虫。
