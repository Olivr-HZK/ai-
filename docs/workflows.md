# 三条生产工作流

核心实现位于 `**ua_workflows/**`；`**scripts/**` 只做薄封装入口，便于 `python scripts/run_*.py` 与 cron 调用。

## 总览


| 工作流            | 入口脚本                             | 包路径                            | 用途（简述）                                                        |
| -------------- | -------------------------------- | ------------------------------ | ------------------------------------------------------------- |
| Video Enhancer | `scripts/run_video_enhancer.py`  | `ua_workflows/video_enhancer/` | 广大大 **工具** 垂类竞品：抓取 → 封面/入库/分析 → 飞书多维表与日报等多渠道推送                |
| Arrow2 每日最新    | `scripts/run_arrow2_latest.py`   | `ua_workflows/arrow2/`         | 广大大 **游戏** 垂类：`latest_yesterday`，detail-v2 逐卡点卡，竞品维度的「昨日首见」素材 |
| Arrow2 展示估值    | `scripts/run_arrow2_exposure.py` | `ua_workflows/arrow2/`         | 同上入口库，`exposure_top10`，偏高高展示估值素材维度                            |


Arrow2 的 `scripts/run_arrow2_latest.py` / `run_arrow2_exposure.py` 在启动时会向 `argv` 注入默认 `--pull-only`（`latest_yesterday` / `exposure_top10`），再调用 `**ua_workflows.arrow2.pipeline`**。

## Video Enhancer

- **流水线主逻辑**：`ua_workflows/video_enhancer/pipeline.py`
- **爬取**：`ua_workflows/video_enhancer/crawl.py`（`run_batch`：工具 Tab、7 天、素材、最新创意等，与 `_do_setup` 一致）
- **分析**：`ua_workflows/video_enhancer/analyze.py`
- **数据库**：默认 `data/video_enhancer_pipeline.db`（`ua_workflows.shared.db.video_enhancer`）
- **多维表**：需 `.env` 中 `VIDEO_ENHANCER_BITABLE_URL` 等；全流程默认 **开启** 同步，除非传入 `--no-bitable-sync`。
- **抓取后保留**：默认不再按单产品 `>10` 做硬截断，避免好素材在封面去重前被提前丢弃；如需恢复旧口径可设 `VIDEO_ENHANCER_PER_PRODUCT_TRUNCATE_ENABLED=1`。
- **同步前排除/标记**：一键流程与独立同步都会补跑成人/色情风险拦截、日内玩法去重、同产品老玩法拦截（默认近 7 日）、embedding 重复候选与已投放匹配；硬拦截项不进主表和方向卡片，embedding 候选仅打 `embedding重复候选` 标签供后续校准。
- **日报素材口径**：仅 Video Enhancer 使用 `load_daily_material_report()` 统一输出「新素材 / 新玩法 / 持续发力」：
  - 新素材：`creative_library.first_target_date = target_date`
  - 新玩法：同 `appid` 下 `effect_one_liner` 过去 7 日无精确或相似命中
  - 持续发力：VE 封面/URL/ahash/玩法跨日信号，日报按产品展示 Top 条目，不混入 Arrow2

## Arrow2（latest 与 exposure 共用 crawl + pipeline）

- **爬虫**：`ua_workflows/arrow2/crawl.py`（游戏 Tab、配置文件中的渠道/国家、`pull_specs`）
- **流水线**：`ua_workflows/arrow2/pipeline.py`
- `**--analyze`**：默认关；cron 与各整合入口若要「分析 + 同步」需显式传入。
- `**--skip-sync`**：跳过飞书同步。
- **数据库**：默认 `data/arrow2_pipeline.db`，可用环境变量 `**ARROW2_SQLITE_PATH`** 覆盖（见 `ua_workflows.shared.db.arrow2`）。
- **周报企微推送**：`scripts/run_arrow2_weekly_wecom.py` 从 `arrow2_daily_insights` 汇总指定周的新玩法 / 新 Hook，默认统计昨日往前 7 天的「最新创意」，并与过去 28 天同 `appid` 历史做精确/相似去重。Webhook 读取 `ARROW2_WECOM_BOT_WEBHOOK`，未设置时降级 `WECOM_BOT_WEBHOOK`；可用 `--dry-run --no-llm` 仅预览。`play_one_liner` 缺失时会临时回退 `ad_one_liner`，`hook_one_liner` 仅统计新增分析后的显式 Hook 字段。

latest 与 exposure **共用同一套** `ua_workflows.arrow2.pipeline`；区别由入口脚本预设的 `**--pull-only`**（`latest_yesterday` / `exposure_top10`）决定。

## 冒烟测试（不入库、不同步）

仅验证爬取与一个产品，`data/` 下写 smoke raw：

- `scripts/test_video_enhancer_crawl.py`
- `scripts/test_arrow2_latest_crawl.py`
- `scripts/test_arrow2_exposure_crawl.py`

支持 `--headed` / `--headless`、`--product`。

## 配置文件


| 路径                              | 用途                                             |
| ------------------------------- | ---------------------------------------------- |
| `config/ai_product.json`        | VE 竞品（含 video/photo 分类等）                       |
| `config/arrow2_competitor.json` | Arrow2 产品与 `filters`、`pull_specs`、`search_tab` |
| `config/iso3166_alpha3_zh.json` | 国家代码等辅助映射                                      |


更多历史设计与字段说明见 [AGENTS.md](../AGENTS.md)。
