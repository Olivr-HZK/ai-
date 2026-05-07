# 三条生产工作流

核心实现位于 **`ua_workflows/`**；**`scripts/`** 只做薄封装入口，便于 `python scripts/run_*.py` 与 cron 调用。

## 总览

| 工作流 | 入口脚本 | 包路径 | 用途（简述） |
|--------|----------|--------|----------------|
| Video Enhancer | `scripts/run_video_enhancer.py` | `ua_workflows/video_enhancer/` | 广大大 **工具** 垂类竞品：抓取 → 封面/入库/分析 → 飞书多维表与日报等多渠道推送 |
| Arrow2 每日最新 | `scripts/run_arrow2_latest.py` | `ua_workflows/arrow2/` | 广大大 **游戏** 垂类：`latest_yesterday`，detail-v2 逐卡点卡，竞品维度的「昨日首见」素材 |
| Arrow2 展示估值 | `scripts/run_arrow2_exposure.py` | `ua_workflows/arrow2/` | 同上入口库，`exposure_top10`，偏高高展示估值素材维度 |

Arrow2 的 `scripts/run_arrow2_latest.py` / `run_arrow2_exposure.py` 在启动时会向 `argv` 注入默认 `--pull-only`（`latest_yesterday` / `exposure_top10`），再调用 **`ua_workflows.arrow2.pipeline`**。

## Video Enhancer

- **流水线主逻辑**：`ua_workflows/video_enhancer/pipeline.py`
- **爬取**：`ua_workflows/video_enhancer/crawl.py`（`run_batch`：工具 Tab、7 天、素材、最新创意等，与 `_do_setup` 一致）
- **分析**：`ua_workflows/video_enhancer/analyze.py`
- **数据库**：默认 `data/video_enhancer_pipeline.db`（`ua_workflows.shared.db.video_enhancer`）
- **多维表**：需 `.env` 中 `VIDEO_ENHANCER_BITABLE_URL` 等；全流程默认 **开启** 同步，除非传入 `--no-bitable-sync`。

## Arrow2（latest 与 exposure 共用 crawl + pipeline）

- **爬虫**：`ua_workflows/arrow2/crawl.py`（游戏 Tab、配置文件中的渠道/国家、`pull_specs`）
- **流水线**：`ua_workflows/arrow2/pipeline.py`
- **`--analyze`**：默认关；cron 与各整合入口若要「分析 + 同步」需显式传入。
- **`--skip-sync`**：跳过飞书同步。
- **数据库**：默认 `data/arrow2_pipeline.db`，可用环境变量 **`ARROW2_SQLITE_PATH`** 覆盖（见 `ua_workflows.shared.db.arrow2`）。

latest 与 exposure **共用同一套** `ua_workflows.arrow2.pipeline`；区别由入口脚本预设的 **`--pull-only`**（`latest_yesterday` / `exposure_top10`）决定。

## 冒烟测试（不入库、不同步）

仅验证爬取与一个产品，`data/` 下写 smoke raw：

- `scripts/test_video_enhancer_crawl.py`
- `scripts/test_arrow2_latest_crawl.py`
- `scripts/test_arrow2_exposure_crawl.py`

支持 `--headed` / `--headless`、`--product`。

## 配置文件

| 路径 | 用途 |
|------|------|
| `config/ai_product.json` | VE 竞品（含 video/photo 分类等） |
| `config/arrow2_competitor.json` | Arrow2 产品与 `filters`、`pull_specs`、`search_tab` |
| `config/iso3166_alpha3_zh.json` | 国家代码等辅助映射 |

更多历史设计与字段说明见 [AGENTS.md](../AGENTS.md)。
