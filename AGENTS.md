# Agent 工作日志

本文档记录所有 agent 对项目做出的代码变更与功能更新，供后续 agent 接手时快速了解项目现状。

---

## 文档与代码对齐说明（2026-04-13 修订）

以下历史段落曾描述**已变更**行为，以本说明为准：

| 主题 | 原文档说法 | 当前代码事实 |
|------|------------|----------------|
| **灵感「套路」筛选** | 同一次多模态输出 `flower_background` / `bw_blockbuster` 等，或 `config/style_filters.json` 配置化 | **已移除**。`analyze_video_from_raw_json.py` 仅输出纯文本灵感（或解析 JSON 中的 `analysis`）。`style_filter_match_summary` 等字段可仍为列占位，恒为空。**「我方已投」**由 `launched_effects_db.apply_launched_effects_filter`（主流程 Step 2.9）及 `sync_raw_analysis_to_bitable_and_push_card.py` 补标处理。 |
| **一键主流程 Step 2c 多维去重进分析** | `get_deduped_items_for_analysis` → `*_dedup_report.json`，仅去重后子集进分析 | **`workflow_video_enhancer_full_pipeline.py` 未调用**该函数。入库仍用全量 `items`；分析队列为 **准入 + 未命中历史成功缓存** 的 `pending_items`（见 `[step:analysis-queue]`）。`get_deduped_items_for_analysis` 仍保留在 `video_enhancer_pipeline_db.py`（供封面指纹等复用逻辑），**不等同于当前一键分析入队口径**。 |
| **定时任务** | 每天 10:30 crontab | **`daily_video_enhancer_workflow.sh` / `daily_ua_job.sh` 注释为手动执行**；若本机仍挂 crontab 为旧配置，以实际 shell 为准。 |
| **OpenRouter 用量** | 仅 shell 内 `curl` | **主流程**在 `workflow_video_enhancer_full_pipeline.py` 内调用 `llm_client.print_openrouter_key_meter`（工作流开始/结束）；与 `.env` 中 `OPENROUTER_METER` 等一致。 |
| **特效库语义阈值** | 文档某处写默认 0.80 | **`launched_effects_db.py` 中默认 `LAUNCHED_EFFECTS_MATCH_THRESHOLD` 为 0.65**（以代码与环境变量为准）。 |
| **封面日内 / 跨日向量去重** | 多模态抽封面 + 文本 LLM 聚类；跨日仅载入「昨日」`insight_cover_style` 与今日同 appid 比较 | **CLIP**（`clip-ViT-B-32`）封面向量并查集，`cosine ≥ COVER_VISUAL_DEDUP_THRESHOLD`（默认 **0.8**），无 LLM。跨日参照为 **`target_date` 之前连续 N 个日历日**（默认 **7**，`COVER_STYLE_HISTORY_LOOKBACK_DAYS`，上限 60），`load_cover_style_rows_for_dates_grouped_by_appid` 批量读库；簇内历史胜出时 `reason` 仍为 **`cover_style_cluster_vs_yesterday`**（兼容旧筛选）。指纹层不变：`COVER_STYLE_CROSS_DAY_FINGERPRINT_ENABLED` + `crossday_filter_items_against_creative_library`。 |

---

## 2026-03-31

### `analyze_video_from_raw_json`：格式异常时多模态重试 3 次，纯文本 JSON 修复改默认关

- **新**：`VIDEO_ANALYSIS_MULTIMODAL_FORMAT_RETRIES`（默认 3，0=关）—— 解析/正文过短等触发 `_needs_json_or_format_repair` 时，**串行**再调多模态（视频/图），不叠加 `PARALLEL_SHARDS`；成功打标 `inspiration_enrichment=multimodal_format_retry`。
- **改**：`VIDEO_ANALYSIS_JSON_REPAIR` 默认由开改为**关**；仅多模态仍失败时可选开启。纯文本收束**去掉 16k 截断**（全量原文进兜底 prompt）。
- 模块头 docstring 已同步说明。

### 飞书主表 `视频` 附件 + 恢复 `launched_effects_db.py`（我方已投 + embedding）

- **`sync_raw_analysis_to_bitable_and_push_card.py`**：`FIELD_DEFS` 增「视频」(type=17)；对 `video_duration>0` 且可直链下载的 `pick_video_url` 拉流上传（`VIDEO_BITABLE_MAX_MB` / `VIDEO_BITABLE_UPLOAD`）；主表同步前对 `analysis["results"]` 调用 `apply_launched_effects_filter`（与全链路 Step 2.9 一致）。
- **`launched_effects_db.py`**：仓库中曾为空导致 Step 2.9 未生效；已补全：飞书已投放表拉取 + 本地缓存 + **关键词子串** + **`llm_client.call_embedding` 语义 cosine ≥ `LAUNCHED_EFFECTS_MATCH_THRESHOLD`（默认 0.65）**；无飞书时降级 `data/launched_effects_descriptions_only.json`。
- **`workflow_video_enhancer_acceptance.py`**：已补全实现；`run_acceptance_after_workflow` 汇总 raw/analysis/方向卡片/封面与已投放 step/推送表行数/历史截断量，写 `data/workflow_video_enhancer_{date}_acceptance.json` 与 `reports/…_acceptance.md`；环境变量与 AGENTS 中「工作流验收」说明一致。未增加 `daily_video_enhancer_acceptance` 表（文档曾提及，当前以文件落盘 + 既有 DB 查询为主）。

---

## 2026-04-20

### Arrow2 `latest_yesterday`：滚动越过「昨日」边界 + 竞品配置

- **`run_search_workflow._search_one_keyword`**：可选 `scroll_until_older_than_date`（目标昨日 YYYY-MM-DD）。在「最新创意」下持续滚动直到底层 napi 合并结果里出现 **first_seen 或 created_at（UTC+8）早于该日** 的素材（认为已扫完目标日窗口），或仍受「连续 3 轮无新批次」与**约 48 轮**上限约束。之后 `filter_yesterday_only` 仍只保留 `first_seen=昨日`。
- **`run_arrow2_batch`**：对 `order_by=latest` 且 `filter_yesterday_only` 的 pull_spec，默认开启上述滚动（`scroll_until_past_target_date` 缺省为真）；显式设为 `false` 可恢复较短轮数 + idle 的旧停法。
- **`config/arrow2_competitor.json`**：移除 `com.arrow.out` 行；`latest_yesterday` 中说明 `scroll_until_past_target_date`。
- **`test_arrow2_competitors.py`**：未指定 `--products` 时默认只跑配置中**第一个**产品；`--all-products` 跑全部。
- **Arrow2 `order_by=latest`（含 latest_yesterday）**：与 `exposure` 一样改为 **`arrow2_build_result_from_dom_after_search`**，列表为 **DOM 卡片 + detail-v2**（`list_source=dom`）；`filter_yesterday_only` 时单词默认多取卡（`max_n` 默认 120、硬顶 200）再按 first_seen 筛昨日。旧无 `pull_specs` 矩阵里 `latest` 亦走 DOM。
- **DOM 广告主过滤**：`run_arrow2_batch` 增加 `keyword_product`（搜索框 key → 与 config `match` 一致的产品名）；`arrow2_build_result_from_dom_after_search` 在 detail-v2 后调用 `advertiser_matches_product` 剔除与目标产品不一致的卡。`test_arrow2_competitors` 自动传入各条目的 `product`。

---

## 2026-04-14

### 封面去重改为 CLIP 向量 + 7 日历史窗口

**动机**：去掉封面多模态与封面聚类 LLM，改为与主库一致的 **CLIP 封面向量**（`creative_library.cover_embedding`），阈值可调、成本更低。

**实现概要**（`scripts/cover_style_intraday.py`）：
- **指纹**（默认开，`COVER_STYLE_CROSS_DAY_FINGERPRINT_ENABLED`）：在算 CLIP 前仍调用 `crossday_filter_items_against_creative_library`，与文首「封面跨日指纹」一致。
- **向量**：优先读库 `load_cover_embedding_blob_map_by_ad_keys`；缺失则 `cover_embedding.compute_cover_embedding_vector_from_url` 仅算向量、不写库（当日首轮入库前新 `ad_key` 可能无行；后续 `run_cover_embedding_job` 等仍会补 `cover_embedding`）。
- **占位入库**：`insight_cover_style` 写入 CLIP 占位 JSON（如 `style_type`: 「CLIP视觉」），`upsert_single_cover_style_insight` 逐条更新。
- **聚类**：同 `appid` 内并查集，边条件 `cosine_similarity ≥` **`COVER_VISUAL_DEDUP_THRESHOLD`**（默认 **0.8**）；簇内保留 `all_exposure_value` 最大一条；若最优来自**历史窗口**则剔今日（`cover_style_cluster_vs_yesterday`），若最优为今日则日内互斥（`cover_style_cluster`）。报告 `cover_dedupe_mode`: **`clip_visual`**。
- **历史窗口**：`COVER_STYLE_CROSS_DAY_ENABLED` 开启时，加载 **`target_date` 前连续 N 日**（**`COVER_STYLE_HISTORY_LOOKBACK_DAYS`**，默认 **7**，即 T-1…T-7）内非空 `insight_cover_style`；`video_enhancer_pipeline_db.load_cover_style_rows_for_dates_grouped_by_appid` 合并多日，同一 `ad_key` 取 exposure 更高的一条。报告含 `cross_day_history_dates`、`cross_day_history_lookback_days`；`cross_day_prev_date` 仍为 **T-1**（兼容旧读者）；`per_appid` 增加 `history_ref_count`，并保留 `yesterday_ref_count` 与同长度（兼容）。

**环境变量**（`.env.example` 已列）：`COVER_VISUAL_DEDUP_THRESHOLD`、`COVER_STYLE_HISTORY_LOOKBACK_DAYS`；其余 `COVER_STYLE_INTRADAY_ENABLED`、`COVER_STYLE_CROSS_DAY_*`、`COVER_STYLE_WORKERS` 仍适用。

**联动**：`workflow_video_enhancer_full_pipeline.py` / `workflow_video_enhancer_steps.py` / `daily_video_enhancer_workflow.sh` / `run_cover_style_intraday.py` / `workflow_video_enhancer_acceptance.py` 中用户可见文案已由「多模态封面」改为 **CLIP 封面**。

**注意**：历史窗口内条目需库内已有 **`cover_embedding`** 才参与向量聚类；过去未跑封面或未回填向量时，参照会变弱。

---

## 2026-04-13

### 工作流验收：`workflow_video_enhancer_acceptance.py`

- **用途**：对指定日期的 Video Enhancer 产物做可重复检查（文件存在与 JSON 结构、本次新分析成功率阈值、封面/语义等异常提示、方向卡片字段与参考链接、推送表行数、`daily_video_enhancer_workflow_{date}.log` 中子进程错误线索、近 N 日截断后条数对比）。
- **输出**：`data/workflow_video_enhancer_{date}_acceptance.json`、`reports/workflow_video_enhancer_{date}_acceptance.md`。
- **SQLite**：`daily_video_enhancer_acceptance` 表（`init_db()` 自动补齐），`upsert_daily_video_enhancer_acceptance` 写入；另增 `query_filter_log_post_total`、`count_daily_ua_push_rows`、`load_filter_log_post_totals_lookback` 供验收与历史对比。
- **接入**：`workflow_video_enhancer_full_pipeline.py` 在「全流程正常结束」或「灵感分析失败提前 return」前调用 `run_acceptance_after_workflow`；`workflow_video_enhancer_steps.py` 的 `push_sync` 结束调用。
- **环境变量**：`ACCEPTANCE_ENABLED`（默认开启，设为 `0`/`false` 关闭）、`ACCEPTANCE_BLOCK_ON_FAIL`（硬失败时进程退出码 2）、`ACCEPTANCE_MIN_SUCCESS_RATE`、`ACCEPTANCE_COVER_REMOVAL_WARN`、`ACCEPTANCE_LOOKBACK_DAYS`、`ACCEPTANCE_LOW_VS_MEAN`、`ACCEPTANCE_HIGH_VS_MEAN`、`ACCEPTANCE_EXIT_ON_SOFT`、`ACCEPTANCE_STRICT`（详见脚本文件头注释）。
- **飞书通知**：配置 `ACCEPTANCE_FEISHU_WEBHOOK`（群机器人 webhook，可与业务 UA 卡片 webhook 分开）后，每次验收结束发送 **interactive 卡片**（紧凑摘要：状态/得分/问题/阶段一行摘要）；`ACCEPTANCE_FEISHU_ENABLED=0` 关闭；`ACCEPTANCE_FEISHU_STRICT=1` 时推送失败会抛错。CLI：`--feishu-webhook`、`--no-feishu`。

---

## 2026-04-09

### 逐条主表同步：`sync_raw_analysis_to_bitable_and_push_card.py`

- **我方已投放**：在 `main()` 读入 analysis 后**先**调用 `apply_launched_effects_filter`，单独跑同步脚本也会排除已投放命中（不再依赖必须先跑 `cluster_store`）。`apply_launched_effects_filter` 对历史 JSON 中已有 `launched_effect_match` / 「我方已投放」标签但未写 `exclude_from_bitable` 的行**补写**主表排除。
- **封面图附件**：路径以 `.image` 结尾时，下载**优先**用 `.png` 替换后的 URL；`upload_image_as_attachment` 中飞书附件 `file_name` 对 `.image` 后缀**强制改为** `.png`，避免多维表里仍显示 `.image`。
- **视频**：仅「视频链接」文本列，**无**视频附件列。

---

## 2026-04-08

### `llm_client.call_vision` 文本降级时剥离媒体 URL

**问题**：部分图片素材在多模态与纯文本路径均被 OpenRouter 返回 403（TOS）；根因之一是视觉失败后 `call_text` 仍携带与请求相同的图片/视频直链，网关对同一 URL 再次拦截。

**改动**（`scripts/llm_client.py`）：新增 `_text_fallback_user_text`，在「全部视觉模型失败 → 纯文本降级」时，将 `user_text` 中的 `media_url` 替换为简短中文说明，避免重复提交直链；分析可仅依据标题、文案与指标推断。

---

## 2026-04-03

### 我方已投放特效库匹配 `scripts/launched_effects_db.py`（新增）

**背景**：我方有一个飞书多维表（194 条记录），记录了已上线到 Evoke/Kavi/Toki/Avatar 各产品的特效/主题。竞品抓取的素材中可能包含与我方已投放特效相同概念的创意——需要识别并标记。

**数据源**：飞书多维表 `JhMMbPlSUaE6G7siF0RcQn6jnlg` / `tblo36ykG6Pl2X04`，含字段：说明（特效名+描述）、类型、来源、已上线产品、日期。

**新增文件 `scripts/launched_effects_db.py`**：
- `sync_launched_effects()` — 通过 FEISHU_APP_ID/SECRET 拉取飞书多维表全量记录。
- **24h 本地缓存**：`data/launched_effects_cache.json`，过期自动重新拉取；网络异常时降级到过期缓存。
- `_extract_effect_names(desc)` — 从说明字段提取特效名关键词（英文名 + 中文名 + 【brackets】内名 + 列表名），当前提取 **481 个关键词（303 唯一）**。
- **双层匹配** `match_against_launched_effects(analysis, title, body)`：
  - **Layer 1 — 关键词硬匹配**：特效名（英文/中文）在竞品 title+body+analysis 中出现即命中（精度极高）。
  - **Layer 2 — 语义嵌入匹配**：对特效说明前 200 字做 embedding，与竞品分析文本 cosine similarity ≥ 0.65 即命中。默认使用**本地模型** `BAAI/bge-small-zh-v1.5`（中英双语、512 维、33M 参数、零 API 成本）。实测：纯中文"圣诞老人打视频电话"（无任何关键词命中）→ 语义层成功匹配到 Santa Call 特效（sim=0.72）。
- `apply_launched_effects_filter(combined_results)` — Pipeline 集成入口；命中素材设 `exclude_from_cluster=True` + 标签 `我方已投放: XXX特效`。

**Embedding 基础设施升级**（`scripts/llm_client.py`）：
- `call_embedding()` 改为**本地模型优先**策略：先尝试 `sentence-transformers` + `BAAI/bge-small-zh-v1.5`，不可用时降级到 API。
- 模型进程内单例加载（首次调用 ~10s，后续调用 <10ms）。
- 环境变量 `EMBEDDING_PROVIDER=api` 可强制走 API；`LOCAL_EMBEDDING_MODEL` 可切换本地模型。

**Pipeline 集成**：
- 全流程 `workflow_video_enhancer_full_pipeline.py` Step 2.9（语义去重之后、方向卡片之前）。
- 分步流程 `workflow_video_enhancer_steps.py` `step_cluster_store` 在生成方向卡片之前。
- 命中素材不进入方向卡片生成（`exclude_from_cluster`），但仍保留在 analysis JSON 和多维表中供查看。

**环境变量**：
- `LAUNCHED_EFFECTS_BITABLE_URL`（默认内置）、`LAUNCHED_EFFECTS_MATCH_THRESHOLD`（代码默认 **0.65**，可通过 `.env` 覆盖）、`LAUNCHED_EFFECTS_ENABLED`（默认开启）。

---

### 架构优化：五项改进（统一 LLM 层 / 配置化套路已废弃 / 语义嵌入 / 历史卡片 / 趋势信号）

#### 1) 统一 LLM 调用层 `scripts/llm_client.py`（新增）

**背景**：`analyze_video_from_raw_json.py`、`cover_style_intraday.py`、`generate_video_enhancer_ua_suggestions_from_analysis.py` 各自维护 ~80-200 行的 LLM 重试/降级逻辑，高度重复。

**变更**：
- 新增 `scripts/llm_client.py`，提供 `call_text()`、`call_vision()`、`call_embedding()` 三个统一接口。
- **Model fallback chain**：按 `vision_models` 列表依次尝试，全部失败自动降级到 `text_fallback_system` + 纯文本。
- **Circuit breaker**：某模型因区域限制返回 403 后，整个进程不再重试该模型。
- **Usage tracking**：所有调用的 token 用量集中在 `llm_client._usage_patch`，各脚本结束时 `flush_usage(target_date)` 统一写入 `ai_llm_usage_daily`。
- `analyze_video_from_raw_json.py` 中 `_call_llm_text` / `_call_llm_video` / `_call_llm_image` 精简为 3-5 行的 `llm_client` 委托，删除 ~200 行重复重试逻辑。
- `generate_video_enhancer_ua_suggestions_from_analysis.py` 中 `_call_llm` 精简为 1 行 `llm_client.call_text()`。
- `cover_style_intraday.py`：**（2026-04-14 起）** 封面步骤已改为 CLIP 向量聚类，**不再**经 `llm_client` 做多模态封面；见 **§2026-04-14**。

#### 2) 套路过滤配置化 `config/style_filters.json`（**已废弃**）

**历史（2026-04-03）**：曾新增 `config/style_filters.json`，并在 `analyze_video_from_raw_json.py` 中动态加载，与灵感分析同一次多模态输出套路 JSON。

**当前**：该能力**已从分析脚本移除**（见上文「文档与代码对齐说明」）。`config/style_filters.json` 若仍存在，仅为遗留文件，**分析流程不再读取**。若需「我方已投」类排除，**仅**依赖 `launched_effects_db` 与下游同步补标。

#### 3) 语义嵌入去重

**背景**：现有跨日去重仅靠 ad_key / URL / ahash，无法捕获「同一创意思路、不同素材资产」的重复。

**变更**：
- `llm_client.py` 新增 `call_embedding()`（默认 `openai/text-embedding-3-small`）、`cosine_similarity()`、`embedding_to_bytes()` / `bytes_to_embedding()`。
- `video_enhancer_pipeline_db.py` 新增：
  - `creative_library.analysis_embedding` BLOB 列（`init_db()` 自动补齐）。
  - `upsert_analysis_embedding(ad_key, blob)` — 单条写入嵌入向量。
  - `load_embeddings_for_crossday(target_date, appid)` — 加载历史嵌入。
  - `semantic_crossday_filter(target_date, items)` — 同 appid 内对已有分析的素材做 cosine similarity ≥ 0.92 的语义去重。
- `workflow_video_enhancer_full_pipeline.py` Step 2.7：分析结果同步 creative_library 后，为每条有分析文本的素材计算嵌入并存储。
- `workflow_video_enhancer_steps.py` `step_analyze_store` 末尾同理。
- 环境变量 `EMBEDDING_MODEL`（可选）、`SEMANTIC_DEDUP_THRESHOLD`（默认 0.92）。

#### 4) 方向卡片注入历史上下文

**背景**：每天的方向卡片独立生成，不知道昨天/前天推了什么方向，可能重复建议。

**变更**：
- `video_enhancer_pipeline_db.py` 新增 `load_recent_direction_cards(target_date, n_days=3)` — 从 `daily_ua_push_content` 读取近 N 天方向卡片摘要。
- `generate_video_enhancer_ua_suggestions_from_analysis.py` `_build_prompt()` 新增 `target_date` 参数；若有历史卡片，在提示词中注入「近期已推送的方向卡片」段落，并要求：
  - 与历史重叠的标注「持续推荐」+ 今日新增洞察
  - 优先输出与历史不同的新方向

#### 6) Code Review 修复（4 项）

根据 `reports/claude_code_review_2026-04-03.md` 评审意见，修复以下问题：

**Fix 1 — `appearance_count` 同日重复累加（高优先级）**
- **原因**：`upsert_creative_library()` 的 UPDATE 无条件 `appearance_count + 1`，但全流程对同一 `raw_payload` 调用两次（Step 2b + Step 2.6），导致同日素材被多计一次。
- **修复**：查询 `last_target_date`，仅当 `last_target_date != target_date` 时递增。回归测试：同日二次写入 count=1，跨日写入 count=2。

**Fix 2 — canonical 代表切换条件写反（高优先级）**
- **原因**：原条件 `if canonical != ad_key and is_canonical == 0` 在旧代表仍最强时反而清零其 `is_canonical`，新代表更强时又不执行清零 → 组内可能 0 个或多个 `is_canonical=1`。
- **修复**：改为 `if canonical == ad_key and dedup_reason != "new"`（仅新素材接管代表时执行清零+更新全组 `canonical_ad_key`）。回归测试：组内始终恰好 1 个 `is_canonical=1`。

**Fix 3 — 一键流程漏传 `--cluster-url` / `--sync-target`（中优先级）**
- **原因**：分步 `push_sync` 传了 `--cluster-url` 和 `--sync-target`，一键流程没传，导致一键流程不同步聚类表。
- **修复**：在 `workflow_video_enhancer_full_pipeline.py` Step 4 的 `sync_cmd` 中补齐：有 `cluster_bitable_url` 时传 `--cluster-url` 和 `--sync-target both`，无时传 `--sync-target raw`。

**Fix 4 — `semantic_crossday_filter` 接入主链路（中优先级）**
- **原因**：`semantic_crossday_filter()` 仅定义未调用，文档描述与实际行为不一致。
- **修复**：在全流程 Step 2.7（嵌入存储）之后新增 Step 2.8：对 `combined_results` 做语义比对，同 appid 内 cosine similarity ≥ 0.92 的素材设 `exclude_from_cluster=True`，下游 `generate_video_enhancer_ua_suggestions_from_analysis.py` 自动跳过（复用已有排除机制）。标记数和匹配详情会打印到终端并回写 analysis JSON。

#### 5) 趋势信号利用

**背景**：`creative_library` 已有 `first_target_date`/`appearance_count` 等生命周期数据，但未被利用。

**变更**：
- `video_enhancer_pipeline_db.py` 新增 `compute_trend_signals(target_date, lookback_days=14)` — 计算各产品本周 vs 上周新素材数、趋势方向（rising/declining/stable）。
- `generate_video_enhancer_ua_suggestions_from_analysis.py` `_build_prompt()` 注入趋势段落，要求方向卡片中引用「该类素材本周明显增多/减少」等趋势表述。

---

### 爬取与封面去重拆分（分步工作流）

**背景**：将「抓取」与「封面日内去重（现为 CLIP，见 **§2026-04-14**）」解耦，便于独立执行或重跑。

**变更**：
- `scripts/workflow_video_enhancer_steps.py`：`crawl_store` 增加 **`--crawl-only`** — 只跑 `test_video_enhancer_two_competitors_318.py` 写 `workflow_video_enhancer_{日期}_raw.json`，**不**做封面去重、**不**写库；终端提示下一步执行 `cover_store`。
- 新增子命令 **`cover_store`**：读已有 raw →（若 `COVER_STYLE_INTRADAY_ENABLED` 开启）`apply_intraday_cover_style_dedupe` → 写回 raw 与 `*_cover_style_intraday.json` → **`prune_daily_creative_insights_not_in_raw`**（删除当日库里已不在 raw 中的 `ad_key`，避免先全量入库再缩条后残留行）→ `upsert_daily_creative_insights` / `upsert_creative_library` / `upsert_daily_video_enhancer_filter_log`。
- `scripts/video_enhancer_pipeline_db.py`：新增 **`prune_daily_creative_insights_not_in_raw`**。
- `scripts/workflow_video_enhancer_full_pipeline.py`：增加 **`--skip-cover-dedupe`** — 一键流程中跳过封面聚类块，抓取后直接用全量 raw 继续（与关闭封面类似，无需改环境变量）。
- 默认 **`crawl_store` 不带 `--crawl-only`** 时行为与改前一致（抓取 + 封面 + 入库）。分步编号在文件头注释中已更新为含 `cover_store`。

### 封面去重：与「昨日」同 appid 参照（跨日）

**（2026-04-14 修订）** 下文「昨日」在**产品语义**上已扩展为 **过去 N 日历史窗口**（默认 7 日），实现为 CLIP 向量并查集而非 LLM；详见 **§2026-04-14** 与文首对齐表「封面日内 / 跨日向量去重」。

**背景**：封面聚类原先仅对比**当日**同产品；现增加**历史**已入库的 `insight_cover_style`（及库内 `cover_embedding`）作为参照，减少与近几日视觉重复的今日素材。

**逻辑**（`scripts/cover_style_intraday.py`）：
- 从 DB 读取 **`target_date` 之前连续 N 日**（默认 **7**）各 `appid` 下非空 `insight_cover_style`（`load_cover_style_rows_for_dates_grouped_by_appid`；单日查询仍可用 `load_cover_style_rows_for_date_grouped_by_appid`），与今日条目一并进入 **CLIP 余弦聚类**。
- 簇内保留 **`all_exposure_value` 最高**的一条（历史、今日同一比较口径）。若历史侧 `ad_key` 胜出，今日同簇素材剔除，`removed` 中 **`reason`** 仍为 **`cover_style_cluster_vs_yesterday`**；若今日胜出，逻辑为 **`cover_style_cluster`**。
- 报告字段：`cross_day_history_dates`、`cross_day_rows_loaded` 等；`per_appid` 含 `history_ref_count` / `yesterday_ref_count`。
- 环境变量 **`COVER_STYLE_CROSS_DAY_ENABLED`**（默认开启；`0`/`false`/`no`/`off` 关闭历史向量参照，仅日内 CLIP）；**`COVER_STYLE_HISTORY_LOOKBACK_DAYS`**（默认 7）。`.env.example` 已补充说明。

**监控关注点**：历史窗口依赖过去 N 日 `daily_creative_insights` 中已有封面占位且 **`creative_library.cover_embedding` 非空**；新日期或历史未跑封面时，该 appid 可能仅有今日数据，行为退化为日内逻辑。

### 封面跨日指纹（与 `creative_library` / 灵感分析 Step B 对齐）

**背景**：避免「主流程跨日已会剔除的素材」仍进入封面多模态；与 `get_deduped_items_for_analysis` 的 Step B 使用同一套比对逻辑。

**变更**：
- `scripts/video_enhancer_pipeline_db.py`：抽取 **`crossday_filter_items_against_creative_library(target_date, items)`**（同 appid 下对 `creative_library` 早于当日的记录比对 `ad_key` / 主媒体 URL / 封面 `image_ahash_md5` 汉明距离 ≤ 阈值）。**`get_deduped_items_for_analysis`** 的 Step B 改为调用该函数，避免重复实现。
- `scripts/cover_style_intraday.py`：在 `apply_intraday_cover_style_dedupe` 内、**CLIP 编码之前**默认执行上述过滤；命中则从本条列表剔除，不再进入后续封面向量步骤。报告含 **`cross_day_fingerprint_removed_count` / `cross_day_fingerprint_removed`**、**`input_count_before_cross_day_fingerprint`**；若过滤后无剩余素材，返回 **`empty_after_cross_day_fingerprint`**。
- 环境变量 **`COVER_STYLE_CROSS_DAY_FINGERPRINT_ENABLED`**（默认开启；`0`/`false`/`no`/`off` 关闭指纹层；**不**影响上文「历史 `insight_cover_style` + CLIP」跨日，后者仍由 **`COVER_STYLE_CROSS_DAY_ENABLED`** / **`COVER_STYLE_HISTORY_LOOKBACK_DAYS`** 控制）。
- `.env.example` 已补充 `COVER_STYLE_CROSS_DAY_FINGERPRINT_ENABLED` 说明。

**监控关注点**：指纹层依赖 `creative_library` 已有历史行（`first_target_date < 当日`）；冷启动或从未入库的竞品首日可能无命中，仅靠 **CLIP 历史窗口**参照兜底（见 **§2026-04-14**）。

---

## 2026-04-02

### OpenRouter 用量：工作流前后各查一次 Key

**变更**：主流程在 `workflow_video_enhancer_full_pipeline.py` 内调用 **`llm_client.print_openrouter_key_meter`**（工作流开始前 / 结束后各一次，需 `.env` 中 `OPENROUTER_API_KEY`）。默认关闭；设 **`OPENROUTER_METER=1`** 等开启（以 `llm_client` 内逻辑为准），输出经 `tee` 进入当日 `logs/daily_video_enhancer_workflow_${TARGET_DATE}.log`。独立脚本 **`scripts/openrouter_key_snapshot.sh`** 供单次手动对比（可能用 `curl`）。

### 终端耗时：灵感多模态 / 封面多模态

**变更**：`analyze_video_from_raw_json.py` 每条素材在 `_call_llm_video` / `_call_llm_image`（灵感分析）结束后打印 `灵感多模态耗时 X.Xs · [video|image]`。**（2026-04-14 起）** 封面步骤已改为 CLIP，**不再**打印「封面多模态 / 封面聚类 LLM」耗时；历史描述保留供对照。

---

### 封面风格日内：进度日志 + 逐条入库

**变更**：`scripts/cover_style_intraday.py` 每条封面处理前后打印进度，完成后调用 `video_enhancer_pipeline_db.upsert_single_cover_style_insight` 写入 `daily_creative_insights.insight_cover_style`（**2026-04-14 起** 为 CLIP 占位 JSON，非多模态描述）；`apply_intraday_cover_style_dedupe(..., crawl_date)` 第三参传入 `raw_payload["crawl_date"]`。`workflow_video_enhancer_full_pipeline.py` / `workflow_video_enhancer_steps.py` 已传参。

---

## 2026-03-31

### 灵感分析：花卉背景 / 黑白大片套路过滤（**已废弃**）

**历史逻辑**：曾与主灵感分析同一次多模态输出套路布尔字段；命中则打「我方已经投过」等。**该能力已移除**，详见文首「文档与代码对齐说明」。

**下游（仍适用）**：`sync_raw_analysis_to_bitable_and_push_card.py` 仍对 `exclude_from_bitable` 等字段做主表是否同步；`generate_video_enhancer_ua_suggestions_from_analysis.py` 仍对 `exclude_from_cluster` 做聚类排除（`cluster_excluded_count`）。**来源**以主流程 **Step 2.8 语义去重**、**Step 2.9 已投放特效库**、`material_tags` 补标为主，而非分析脚本内套路。

---

### 多模态封面日内去重默认开启

**变更**：`scripts/cover_style_intraday.py` 中 `is_cover_style_intraday_enabled()` 默认改为开启；通过 `COVER_STYLE_INTRADAY_ENABLED=0`（或 `false`/`no`/`off`）关闭。`scripts/daily_video_enhancer_workflow.sh` 中 `export COVER_STYLE_INTRADAY_ENABLED="${COVER_STYLE_INTRADAY_ENABLED:-1}"`，便于环境变量显式覆盖。`.env.example` 已补充说明。**（2026-04-14）** 实现已为 **CLIP 封面**，标题中「多模态」为历史命名。

---

## 2026-03-24

### [1] 项目清理：废弃脚本归档到 `scripts/legacy/`

**操作**：将 50 个废弃/旧流程脚本从 `scripts/` 移入 `scripts/legacy/`，保留 31 个活跃脚本。

**保留在 `scripts/` 的活跃脚本分类：**

| 分类 | 脚本 |
|------|------|
| Video Enhancer 主流程 | `workflow_video_enhancer_full_pipeline.py`, `workflow_video_enhancer_steps.py`, `test_video_enhancer_two_competitors_318.py`, `analyze_video_from_raw_json.py`, `generate_video_enhancer_ua_suggestions_from_analysis.py`, `sync_raw_analysis_to_bitable_and_push_card.py`, `push_video_enhancer_multichannel.py`, `push_video_enhancer_feishu_card_only.py`, `sync_video_enhancer_date_to_google_sheet.py`, `video_enhancer_pipeline_db.py` |
| 核心依赖 | `path_util.py`, `proxy_util.py`, `guangdada_login.py`, `run_search_workflow.py`, `workflow_guangdada_competitor_yesterday_creatives.py`, `guangdada_yesterday_creatives_db.py` |
| 定时任务 | `daily_ua_job.sh`, `daily_video_enhancer_workflow.sh` |
| 竞品热门榜流程 | `hot_rank_step1_crawl.py`, `hot_rank_step2_video_analysis.py`, `hot_rank_step3_cluster.py`, `hot_rank_step4_push_feishu.py`, `workflow_competitor_hot_rank.py` |
| 其他次要流程 | `workflow_competitor_new_rank.py`, `compute_new_rank_diff.py`, `fetch_competitor_raw.py`, `fetch_competitor_new_creatives.py`, `fetch_dom_yesterday_creatives.py`, `sync_top3_competitor_by_heat_to_feishu.py`, `ua_crawl_db.py`, `competitor_hot_db.py`, `competitor_ua_db.py` |

**注意**：`workflow_guangdada_competitor_yesterday_creatives.py` 是主流程的核心依赖（被 `test_video_enhancer_two_competitors_318.py` import，提供 `_apply_relaunch_pipeline_tag`、`_creative_hits_target_date`、`advertiser_matches_product` 等工具函数）。

---

### [2] 功能新增：图片素材灵感分析支持

**背景**：原工作流仅分析有视频 URL 的素材，纯图片素材（`video_duration=0`，`resource_urls=[]`，`preview_img_url` 有值，`video2pic=1`）完全被跳过。

**涉及文件**：
- `scripts/analyze_video_from_raw_json.py`
- `scripts/generate_video_enhancer_ua_suggestions_from_analysis.py`
- `scripts/sync_raw_analysis_to_bitable_and_push_card.py`

---

#### `scripts/analyze_video_from_raw_json.py`

**新增函数 `_pick_image_url(creative)`**
```python
def _pick_image_url(creative: Dict[str, Any]) -> str:
    """提取图片 URL：优先 resource_urls 中纯图片条目，其次 preview_img_url。"""
    for r in creative.get("resource_urls") or []:
        if isinstance(r, dict) and r.get("image_url") and not r.get("video_url"):
            return str(r["image_url"])
    if creative.get("preview_img_url"):
        return str(creative["preview_img_url"])
    return ""
```

**新增函数 `_call_llm_image(user_content, image_url)`**
- 使用 `image_url` 类型的多模态消息调用视觉模型
- 优先复用 `OPENROUTER_VIDEO_MODEL`（Gemini 支持图片），兜底用 `OPENROUTER_MODEL`
- 无可用模型时回退纯文本分析

**新增函数 `_build_image_prompt(item, creative, image_url)`**
- 图片专属 prompt，分析维度：构图、视觉焦点、Before/After 对比、文字排版
- 第 2 项改为「视觉钩子（第一眼抓人的核心元素）」，区别于视频的「Hook（前几秒）」

**`_build_prompt` 重命名为 `_build_video_prompt`**（无逻辑改动）

**`main()` 逻辑变化**：
- 旧：无 `video_url` → `continue` 跳过
- 新：无 `video_url` → 尝试 `image_url`，两者都无才跳过
- 输出结果新增字段：`creative_type`（`"video"` / `"image"`）、`image_url`、`title`、`body`
- 输出汇总信息：`视频 N / 图片 N / 跳过 N`
- 输出 JSON 新增字段：`video_analyzed`、`image_analyzed`、`skipped`

---

#### `scripts/generate_video_enhancer_ua_suggestions_from_analysis.py`

**`_build_prompt()` 中 `source_by_adkey` 构建逻辑更新**：
- 新增 `creative_type` 字段传入 LLM
- 图片素材传 `image_url`，视频素材传 `video_url`
- 让 LLM 在生成方向卡片时能感知素材类型

---

#### `scripts/sync_raw_analysis_to_bitable_and_push_card.py`

**`adkey_to_video`（即 `video_by_ad`）映射构建逻辑更新**：
```python
# 旧：
v = str(it.get("video_url") or "")
# 新：
v = str(it.get("video_url") or "") or str(it.get("image_url") or "")
```
- 图片素材的 ad_key 也纳入 `adkey_to_video` 映射
- 确保 LLM 在方向卡片「参考链接」中引用图片 ad_key 时，飞书卡片能渲染出可点击链接

**多维表记录字段无变化**：`封面图链接` 字段已使用 `preview_img_url`，图片缩略图本已支持；`视频链接` 对图片素材保持为空，符合语义。

---

---

### [3] 功能新增：素材主库 creative_library 与智能多维去重

**背景**：`daily_creative_insights` 是按日期快照存储的，同一条广告素材跨天重复出现时没有归一化处理，且无法判断"视觉相同但 ad_key 不同"的情况（如同图跨平台投放）。

**涉及文件**：
- `scripts/video_enhancer_pipeline_db.py`
- `scripts/workflow_video_enhancer_full_pipeline.py`

---

#### `scripts/video_enhancer_pipeline_db.py`

**新增表 `creative_library`**（跨天去重主库）：

| 字段 | 说明 |
|------|------|
| `ad_key` | 唯一标识，UNIQUE |
| `dedup_group_id` | 去重组 ID，格式：`ahash_{hex8}` / `text_{fp8}` / `adkey_{key8}` |
| `canonical_ad_key` | 组内代表（热度/人气最高者）|
| `is_canonical` | 1=本条是组代表 |
| `image_ahash_md5` | 感知哈希（从 raw_json 提取，广大大预计算）|
| `text_fingerprint` | sha1(normalize(title+body))，用于文案去重 |
| `creative_type` | `"video"` / `"image"` |
| `best_heat/impression/all_exposure_value` | 历史最高热度指标（跨天取最大）|
| `first_target_date` / `last_target_date` | 首次/最近出现日期 |
| `appearance_count` | 跨天出现次数 |
| `dedup_reason` | 去重原因：`new` / `ahash(dist=N)` / `text` / `exact` |

**三个索引**：`dedup_group_id`、`image_ahash_md5`、`text_fingerprint`

**新增工具函数**：
- `_ahash_hamming(h1, h2)` — 计算两个 16 进制感知哈希的汉明距离
- `_text_fingerprint(title, body)` — 归一化文案后取 sha1

**新增核心函数 `upsert_creative_library(target_date, raw_payload, analysis_by_ad)`**：

去重优先级：
1. `ad_key` 完全匹配 → 更新热度/出现次数（exact）
2. `image_ahash_md5` 汉明距离 ≤ 8 → 视觉内容相同归组（ahash）
3. `text_fingerprint` 相同且非空 → 文案完全一致归组（text）
4. 无匹配 → 新建，自立为组代表（new）

常量：`AHASH_HAMMING_THRESHOLD = 8`，`AHASH_LOOKUP_LIMIT = 2000`

**新增查询函数 `query_dedup_summary(target_date=None)`**：
- 返回每个 dedup_group 的代表素材 + 组内素材数
- 可按 `target_date` 过滤，只看当日出现的去重组

---

#### `scripts/workflow_video_enhancer_full_pipeline.py`

**Step 2b**（紧接原始素材落库后）：
```python
n_lib, n_grouped = upsert_creative_library(target_date, raw_payload)
print(f"[DB] 素材主库 creative_library: 写入/更新 {n_lib} 条，发现重复归组 {n_grouped} 条。")
```

**Step 2.6**（分析结果写库后）：
```python
_, _ = upsert_creative_library(target_date, raw_payload, analysis_by_ad)
print(f"[DB] creative_library 分析结果已同步。")
```

---

### [4] 功能新增：多维去重过滤（日内 + 跨日），只让新唯一素材进入分析

**背景**：之前所有抓取到的 23 条素材都会进入分析，其中有重复（日内同组 9 条，跨日重复待积累）。现在只让真正新的唯一素材进入分析/推送/多维表，但全量原始素材仍完整入库存档。

**涉及文件**：
- `scripts/video_enhancer_pipeline_db.py` — 新增 `get_deduped_items_for_analysis()`
- `scripts/workflow_video_enhancer_full_pipeline.py` — 主流程集成

---

#### `scripts/video_enhancer_pipeline_db.py`

**新增函数 `get_deduped_items_for_analysis(target_date, raw_payload)`**

返回 `(deduped_items, report)`，执行两步去重：

**Step A — 日内去重（四维 OR 逻辑，优先级从高到低）**：
- 维度0: `video_url` / `image_url` 完全匹配（URL 相同=绝对同文件）
- 维度1: `image_ahash_md5` 汉明距离 ≤ 8（封面视觉相近）
- 维度2: `text_fingerprint` sha1 相同（文案完全一致）
- 同组保留 `impression` 最高者为代表，其余标记为 `intraday_removed`

**Step B — 跨日去重**：
- 从 `creative_library` 加载 `first_target_date < target_date` 的历史记录
- 对 Step A 代表素材按同样四维逻辑与历史比对
- 命中历史的素材标记为 `crossday_removed`，不进入分析

**report 结构**：
```json
{
  "total_input": 23,
  "after_intraday": 14,
  "after_crossday": 14,
  "intraday_removed_count": 9,
  "crossday_removed_count": 0,
  "intraday_removed": [{"ad_key", "reason", "group_id"}],
  "crossday_removed": [{"ad_key", "reason", "matched_ad_key", "matched_date"}]
}
```

去重报告保存至 `data/{output_prefix}_dedup_report.json`。

---

#### `scripts/workflow_video_enhancer_full_pipeline.py`（与一键流程的关系）

**历史设计**：曾计划在 Step 2c 调用 `get_deduped_items_for_analysis`，仅让去重后子集进入分析并写 `*_dedup_report.json`。

**当前一键流程（`workflow_video_enhancer_full_pipeline.py`）**：**未接入**上述步骤。`creative_library` 仍会在 Step 2b 对全量 raw 做归组；**分析入队**为：全量 `items` 中，**未复用历史成功分析**且 **符合灵感准入** 的条目 → `*_raw_pending_analysis.json` → `analyze_video_from_raw_json.py`。`get_deduped_items_for_analysis` 函数仍存在于 `video_enhancer_pipeline_db.py`，供库内逻辑复用，**与当前一键分析入队无直接调用关系**。

**首次运行说明**（仍适用于 `creative_library`）：冷启动时跨日指纹等命中 0 条为正常现象。

---

### [5] Bug 修复 + 历史数据回填

**Bug**：`upsert_creative_library` 末尾向内存 ahash 缓存追加了 `sqlite3.Row` 类对象（写错了），导致后续迭代时报 `TypeError: type 'sqlite3.Row' is not subscriptable`。

**修复**（`video_enhancer_pipeline_db.py`）：
1. `existing_ahash_rows` 初始化时将 sqlite3.Row 转为 dict（`{"ad_key", "image_ahash_md5", "dedup_group_id"}`）
2. 末尾追加新条目时直接 `append(dict)`，移除错误的 `sqlite3.Row` 占位行

**新增脚本 `scripts/backfill_creative_library.py`**：
- 从 `daily_creative_insights` 按 `target_date` 升序（从旧到新）逐日回填 `creative_library`
- 每日数据经过完整去重逻辑（url → ahash → text）再写入，确保 `first_target_date` 准确
- 支持 `--dry-run` 只统计不写库
- **回填结果**（2026-03-18 ~ 2026-03-24，88 条原始）：
  - `creative_library` 总记录：88 条
  - 唯一去重组：45 组
  - 去重原因分布：new=45 条、text=31 条、ahash(dist=0)=12 条
  - 跨天重复出现的素材：1 条（appearance_count > 1）

---

## 当前主工作流（Video Enhancer 日流程）

**触发方式**：`scripts/daily_video_enhancer_workflow.sh` 与 `daily_ua_job.sh` 注释均为 **手动执行**（默认 `TARGET_DATE` = 昨天；使用项目根 `.venv/bin/python3`）。若本机仍配置 crontab，以实际为准。

```
daily_ua_job.sh（可选）
  └→ daily_video_enhancer_workflow.sh   # PYTHONUNBUFFERED=1；COVER_STYLE_INTRADAY_ENABLED 等
       └→ workflow_video_enhancer_full_pipeline.py --date <TARGET_DATE>
            │
            ├─ Step 1  爬取
            │     test_video_enhancer_two_competitors_318.py → workflow_video_enhancer_{日期}_raw.json
            │
            ├─ Step 2  可选封面日内 CLIP 去重（COVER_STYLE_INTRADAY_ENABLED；--skip-cover-dedupe 跳过）
            │     apply_intraday_cover_style_dedupe → 写回 raw + *_cover_style_intraday.json
            │
            ├─ Step 2.0  可选 DOM 详情补全（--skip-dom-enrich 跳过）
            ├─ 灵感准入统计（merge_inspiration_filter_stats，raw 不删条）
            │
            ├─ Step 2a  原始落库：upsert_daily_creative_insights（仅 raw，无 analysis）
            ├─ Step 2b  素材主库：upsert_creative_library（全量归组）
            │
            ├─ Step 2c  分析入队（非 get_deduped_items_for_analysis）
            │     全量 items → 复用历史成功分析跳过 → 准入 + 待分析 → *_raw_pending_analysis.json
            │
            ├─ Step 3  灵感分析子进程：analyze_video_from_raw_json.py → 合并写入 analysis JSON
            │
            ├─ Step 2.8  语义去重：_apply_semantic_dedup → exclude_from_cluster（可回写 analysis JSON）
            ├─ Step 2.9  已投放特效库：apply_launched_effects_filter → exclude_* / material_tags / launched_effect_match
            │
            ├─ Step 2.5  回写 DB：upsert_daily_creative_insights（带 analysis）+ upsert_daily_video_enhancer_filter_log
            ├─ Step 2.6  creative_library 同步 analysis
            ├─ Step 2.7  语义嵌入：call_embedding → upsert_analysis_embedding（analysis_embedding）
            │
            ├─ （若本次存在分析失败）提前 return；可跑验收（partial）
            │
            ├─ Step 4  方向卡片：generate_video_enhancer_ua_suggestions_from_analysis.py
            ├─ Step 5  飞书多维表同步：sync_raw_analysis_to_bitable_and_push_card.py（--no-card）
            ├─ Step 6  飞书聊天卡片：push_video_enhancer_feishu_card_only.py
            ├─ Step 7  推送表：upsert_daily_push_content（daily_ua_push_content）
            ├─ Step 8  企业微信 + Google Sheet：push_video_enhancer_multichannel.py
            │
            ├─ OpenRouter 用量：print_openrouter_key_meter（工作流结束，若启用）
            └─ 验收：run_acceptance_after_workflow（workflow_video_enhancer_acceptance.py）
```

**分步等价**：`workflow_video_enhancer_steps.py`：`crawl_store` → `cover_store`（可选与爬取拆分）→ `analyze_store` → `cluster_store` → `push_sync`。

**代码量**：请以实际 `wc -l` 为准；工作流核心文件见 `workflow_video_enhancer_full_pipeline.py`、`video_enhancer_pipeline_db.py`、`llm_client.py`。

## 数据结构备忘

### raw JSON 素材字段区分
| 字段 | 视频素材 | 图片素材 |
|------|---------|---------|
| `video_duration` | > 0 | 0 |
| `resource_urls` | `[{type, image_url, video_url}]` | `[]` |
| `preview_img_url` | 视频封面缩略图 | 图片本体 URL |
| `video2pic` | 0 | 1 |

### 分析结果 JSON 新增字段（2026-03-24 起）
- `creative_type`: `"video"` 或 `"image"`
- `image_url`: 图片素材的图片 URL（视频素材为空字符串）
- `title`: 素材标题
- `body`: 素材文案
- `video_analyzed`: 视频分析数量
- `image_analyzed`: 图片分析数量
- `skipped`: 跳过数量（无视频也无图片）
- `style_filter_match_summary`: 可为空串（套路筛选已移除后的列兼容）

主流程合并后的 `analysis` JSON 还可能含：`pipeline_items`、`exclude_from_cluster`（2.8）、`launched_effect_match`（2.9）、`semantic_dedup_*` 等，以当日产物为准。
