# Arrow2 展示估值工作流说明

面向 **广大大 Arrow2 竞品 Tab**：按配置拉取素材、落库、可选封面去重、同步飞书多维表。  
默认跑 **展示估值** 类拉取（`pull_spec.id = exposure_top10`），与「昨日最新」类（`latest_yesterday`）区分开。

## 推荐入口

| 方式 | 说明 |
|------|------|
| `./scripts/arrow2_exposure_workflow.sh` | 展示估值一站式（见脚本内注释） |
| `./scripts/daily_arrow2_workflow.sh` | `all` 与 `arrow2_exposure_workflow` 同源（**展示估值** `exposure_top10`）；另有 `latest_yesterday` 全流程；`crawl-only` **仅爬** 且默认 `latest_yesterday`，要展示估值时 `crawl-only exposure_top10` |
| `python scripts/workflow_arrow2_full_pipeline.py --pull-only exposure_top10` | Python 直接调用，参数最全 |

**DOM 地区探针（调试用）**：`run_arrow2_creatives_country_via_dom` 内若设 `ARROW2_DEBUG_DOM_PROBE_FIRST=N`（如 5），会**先于**多轮点卡、按当前列表**从上到下点 N 张卡**，拦截 `detail-v2` 后：终端打印「本批 `creatives` 前 N 条 + detail-v2 摘要」→ **暂停**等你在终端按 Enter（浏览器不关闭，便于对图）。`ARROW2_DEBUG_DOM_PROBE_ONLY=1` 时 Enter 后**不再**继续多轮/单条重试。CLI：`./scripts/arrow2_exposure_workflow.sh --debug --debug-dom-probe 5`；仅爬、不入库且走 **最新创意** 组：`./scripts/daily_arrow2_workflow.sh crawl-only --debug --debug-dom-probe 5`（不要加 `exposure_top10`，除非你刻意测展示估值：`crawl-only exposure_top10`）。`workflow_arrow2_full_pipeline.py` 同样支持 `--debug` / `--debug-dom-probe`。亦可只设环境变量 `ARROW2_DEBUG_DOM_PROBE_FIRST=5`。

业务日默认 **昨日（UTC+8）**；指定日期：`TARGET_DATE=YYYY-MM-DD ./scripts/arrow2_exposure_workflow.sh`。

## 配置：`config/arrow2_competitor.json`

| 字段 | 说明 |
|------|------|
| `search_tab` | `game` / `tool` / `playable` 等，决定进入游戏/工具/试玩广告 Tab |
| `products` | 竞品列表：`keyword`（搜索词）、`match`（广告主匹配）、`appid`（包名，搜索框优先） |
| `pull_specs` | 多轮拉取定义；**展示估值**为 `id: "exposure_top10"`：`day_span`（如 30 天）、`order_by: "exposure"`、`popularity_option_text`（如 Top10%）、可选 `max_creatives_per_keyword` |
| `filters` | `ad_channels`（常规视频/图片）、`ad_channels_playable`（试玩）、`countries`（ISO3，Top 创意里勾选） |
| `extra_keywords` | 额外搜索词（一般不填则仅用 `products`） |

爬取合并后 **全局按 `ad_key` 去重**（`scripts/test_arrow2_competitors.py`），同一素材多轮次只保留一行，`seen_in_runs` 记录出现上下文。

## 环境变量（`.env`）

与 **飞书 / 广大大登录** 强相关项（节选，完整见项目根 `.env.example`）：

| 变量 | 作用 |
|------|------|
| `FEISHU_APP_ID` / `FEISHU_APP_SECRET` | 飞书应用凭证，同步多维表必填 |
| `ARROW2_BITABLE_URL` | Arrow2 专用多维表完整 URL（含 `table=`） |
| `GUANGDADA_EMAIL` / `GUANGDADA_PASSWORD` | 广大大账号（爬取） |
| `ARROW2_SQLITE_PATH` | 默认 `data/arrow2_pipeline.db` |
| `ARROW2_OUTPUT_PREFIX` | 输出 JSON 前缀，默认 `workflow_arrow2_<日期>` |
| `ARROW2_WIPE_DB=1` | 与封装脚本联用：爬取前 **清空** Arrow2 两表，慎用 |
| `ARROW2_MAX_CREATIVES_PER_KEYWORD` | 每搜索词最大条数全局默认，可被 `pull_spec.max_creatives_per_keyword` 覆盖 |
| `ARROW2_ENRICH_DETAIL_COUNTRY` 等 | 爬取后 detail-v2 / DOM 补全国家地区，见 `.env.example` |
| （默认） | 在 `run_arrow2_batch` 中若**未**设置上述两变量，则默认 `COUNTRY=1`、`DOM=1`（点卡拦 detail-v2）。`.env` 中写 `=0` 会保留，不会被默认覆盖。 |
| `ARROW2_ENSURE_FIELDS=0` | 同步时不再自动建列，仅写已有列 |
| `ARROW2_BITABLE_URL` 缺省时 | `workflow_arrow2_full_pipeline.py` 内仍有一块默认表 URL，生产环境请在 `.env` 显式设置 |

## 产物路径

- Raw：`data/<output_prefix>_raw.json`
- 封面去重报告：`data/<output_prefix>_cover_style_intraday.json`（未 `--skip-cover` 时）
- 分析（仅 `--analyze`）：`data/video_analysis_<output_prefix>_raw.json`
- 占位分析（未 `--analyze`）：仍写上述路径中的占位 JSON，供仅 raw 同步

## 与「昨日最新」工作流的区别

| | 展示估值 `exposure_top10` | 昨日最新 `latest_yesterday` |
|--|---------------------------|-----------------------------|
| `pull_spec` | 通常 30 天 + 展示估值 + Top10% | 7 天 + 按「最新」排序 + 仅昨日 `first_seen`；**不**强选「Top创意」下拉里首项（避免 Top1% 等） |
| 入口 | `arrow2_exposure_workflow.sh` 或 `daily_arrow2_workflow.sh all` | `daily_arrow2_workflow.sh latest_yesterday` |

## 仅爬取、不同步入库/飞书

`./scripts/daily_arrow2_workflow.sh crawl-only`（不跑 `workflow_arrow2_full_pipeline`；**默认** `latest_yesterday` 那一组，见上表）。

## 多 pull 一次跑满

`python scripts/workflow_arrow2_full_pipeline.py --all-pull-specs` 会按配置执行全部 `pull_specs`（需与业务预期一致，可能重复条数被 `ad_key` 去重合并）。

## 单产品 / 测试库

- 限制产品：`--products com.xxx`（与 `config/arrow2_competitor.json` 中 `appid` 等匹配）
- 测试库：`workflow_arrow2_full_pipeline.py --test-db` 写入 `data/arrow2_pipeline_test.db`（或 `ARROW2_TEST_SQLITE_PATH`）
