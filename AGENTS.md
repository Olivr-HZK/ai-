# Agent 工作日志

本文档记录所有 agent 对项目做出的代码变更与功能更新，供后续 agent 接手时快速了解项目现状。

---

## 2026-04-03

### 架构优化：五项改进（统一 LLM 层 / 配置化套路 / 语义嵌入 / 历史卡片 / 趋势信号）

#### 1) 统一 LLM 调用层 `scripts/llm_client.py`（新增）

**背景**：`analyze_video_from_raw_json.py`、`cover_style_intraday.py`、`generate_video_enhancer_ua_suggestions_from_analysis.py` 各自维护 ~80-200 行的 LLM 重试/降级逻辑，高度重复。

**变更**：
- 新增 `scripts/llm_client.py`，提供 `call_text()`、`call_vision()`、`call_embedding()` 三个统一接口。
- **Model fallback chain**：按 `vision_models` 列表依次尝试，全部失败自动降级到 `text_fallback_system` + 纯文本。
- **Circuit breaker**：某模型因区域限制返回 403 后，整个进程不再重试该模型。
- **Usage tracking**：所有调用的 token 用量集中在 `llm_client._usage_patch`，各脚本结束时 `flush_usage(target_date)` 统一写入 `ai_llm_usage_daily`。
- `analyze_video_from_raw_json.py` 中 `_call_llm_text` / `_call_llm_video` / `_call_llm_image` 精简为 3-5 行的 `llm_client` 委托，删除 ~200 行重复重试逻辑。
- `generate_video_enhancer_ua_suggestions_from_analysis.py` 中 `_call_llm` 精简为 1 行 `llm_client.call_text()`。
- `cover_style_intraday.py` 无需改动（继续 import analyze 中的 wrapper）。

#### 2) 套路过滤配置化 `config/style_filters.json`（新增）

**背景**：「花卉背景」「黑白大片」两个过滤器硬编码在提示词和 JSON 解析中，新增过滤器需改多处代码。

**变更**：
- 新增 `config/style_filters.json`，每个条目含 `id`、`label`、`description`、`enabled`。
- `analyze_video_from_raw_json.py` 改为动态加载配置：`_load_style_filters()` → `_style_filter_ids()` → `_style_filter_prompt_section()` → `_json_output_constraint()`。
- 提示词中的套路描述列表、JSON 输出字段约束、system message 的输出约束、JSON 解析 `_parse_inspiration_json_response()` 全部改为按 filter_ids 动态生成。
- **新增过滤器仅需编辑 JSON 文件**，不改代码。配置不存在时回退到内置默认（向下兼容）。

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

**背景**：将「抓取」与「封面日内多模态 + 聚类去重」解耦，便于独立执行或重跑。

**变更**：
- `scripts/workflow_video_enhancer_steps.py`：`crawl_store` 增加 **`--crawl-only`** — 只跑 `test_video_enhancer_two_competitors_318.py` 写 `workflow_video_enhancer_{日期}_raw.json`，**不**做封面去重、**不**写库；终端提示下一步执行 `cover_store`。
- 新增子命令 **`cover_store`**：读已有 raw →（若 `COVER_STYLE_INTRADAY_ENABLED` 开启）`apply_intraday_cover_style_dedupe` → 写回 raw 与 `*_cover_style_intraday.json` → **`prune_daily_creative_insights_not_in_raw`**（删除当日库里已不在 raw 中的 `ad_key`，避免先全量入库再缩条后残留行）→ `upsert_daily_creative_insights` / `upsert_creative_library` / `upsert_daily_video_enhancer_filter_log`。
- `scripts/video_enhancer_pipeline_db.py`：新增 **`prune_daily_creative_insights_not_in_raw`**。
- `scripts/workflow_video_enhancer_full_pipeline.py`：增加 **`--skip-cover-dedupe`** — 一键流程中跳过封面聚类块，抓取后直接用全量 raw 继续（与关闭封面类似，无需改环境变量）。
- 默认 **`crawl_store` 不带 `--crawl-only`** 时行为与改前一致（抓取 + 封面 + 入库）。分步编号在文件头注释中已更新为含 `cover_store`。

### 封面去重：与「昨日」同 appid 参照（跨日）

**背景**：封面聚类原先仅对比**当日**同产品；现增加**昨日**已入库的 `insight_cover_style` 作为参照，减少与昨天视觉套路重复的今日素材。

**逻辑**（`scripts/cover_style_intraday.py`）：
- 从 DB 读取 **`target_date` 前一天**（日历减 1 天）各 `appid` 下非空 `insight_cover_style`（`video_enhancer_pipeline_db.load_cover_style_rows_for_date_grouped_by_appid`），与今日条目一并进入聚类提示词（行内带 `source: yesterday` / `today`）。
- 簇内保留 **`all_exposure_value` 最高**的一条（昨日、今日同一比较口径）。若昨日 `ad_key` 胜出，今日同簇素材剔除，`removed` 中 **`reason`** 为 **`cover_style_cluster_vs_yesterday`**；若今日胜出，逻辑同原 **`cover_style_cluster`**。
- 报告字段：`cross_day_prev_date`、`cross_day_rows_loaded`；每 app 增加 `yesterday_ref_count`。
- 环境变量 **`COVER_STYLE_CROSS_DAY_ENABLED`**（默认开启；`0`/`false`/`no`/`off` 关闭跨日，仅日内聚类）。`.env.example` 已补充说明。

**监控关注点**：跨日依赖前一日 `daily_creative_insights` 中已有封面风格数据；新日期或昨日未跑封面时，该 appid 仅有今日数据，行为退化为原日内逻辑。

### 封面跨日指纹（与 `creative_library` / 灵感分析 Step B 对齐）

**背景**：避免「主流程跨日已会剔除的素材」仍进入封面多模态；与 `get_deduped_items_for_analysis` 的 Step B 使用同一套比对逻辑。

**变更**：
- `scripts/video_enhancer_pipeline_db.py`：抽取 **`crossday_filter_items_against_creative_library(target_date, items)`**（同 appid 下对 `creative_library` 早于当日的记录比对 `ad_key` / 主媒体 URL / 封面 `image_ahash_md5` 汉明距离 ≤ 阈值）。**`get_deduped_items_for_analysis`** 的 Step B 改为调用该函数，避免重复实现。
- `scripts/cover_style_intraday.py`：在 `apply_intraday_cover_style_dedupe` 内、封面多模态**之前**默认执行上述过滤；命中则从本条列表剔除，**不再调用**封面视觉模型。报告含 **`cross_day_fingerprint_removed_count` / `cross_day_fingerprint_removed`**、**`input_count_before_cross_day_fingerprint`**；若过滤后无剩余素材，返回 **`empty_after_cross_day_fingerprint`**。
- 环境变量 **`COVER_STYLE_CROSS_DAY_FINGERPRINT_ENABLED`**（默认开启；`0`/`false`/`no`/`off` 关闭指纹层；**不**影响上文「昨日 insight_cover_style」的 LLM 跨日，后者仍由 **`COVER_STYLE_CROSS_DAY_ENABLED`** 控制）。
- `.env.example` 已补充 `COVER_STYLE_CROSS_DAY_FINGERPRINT_ENABLED` 说明。

**监控关注点**：指纹层依赖 `creative_library` 已有历史行（`first_target_date < 当日`）；冷启动或从未入库的竞品首日可能无命中，仅靠 LLM 昨日参照兜底。

---

## 2026-04-02

### OpenRouter 用量：工作流前后各查一次 Key

**变更**：`scripts/daily_video_enhancer_workflow.sh` 在 `workflow_video_enhancer_full_pipeline.py` 前后各执行一次 `curl -i https://openrouter.ai/api/v1/key -H "Authorization: Bearer $OPENROUTER_API_KEY"`（需 `.env` 中 `OPENROUTER_API_KEY`）。默认关闭；设 **`OPENROUTER_METER=1`**（可写入 `.env` 或命令行前缀）开启，输出经 `tee` 进入当日 `logs/daily_video_enhancer_workflow_${TARGET_DATE}.log`。独立脚本 **`scripts/openrouter_key_snapshot.sh`** 供单次手动对比。

### 终端耗时：灵感多模态 / 封面多模态

**变更**：`analyze_video_from_raw_json.py` 每条素材在 `_call_llm_video` / `_call_llm_image`（灵感分析）结束后打印 `灵感多模态耗时 X.Xs · [video|image]`。`cover_style_intraday.py` 每条封面 `_call_llm_image` 后打印 `封面多模态耗时`；同 appid 多封面聚类时额外打印 `封面聚类 LLM 耗时`（文本模型）。

---

### 封面风格日内：进度日志 + 逐条入库

**变更**：`scripts/cover_style_intraday.py` 每条封面多模态前后打印 `[cover-style] [i/total]`，完成后调用 `video_enhancer_pipeline_db.upsert_single_cover_style_insight` 写入 `daily_creative_insights.insight_cover_style`；`apply_intraday_cover_style_dedupe(..., crawl_date)` 第三参传入 `raw_payload["crawl_date"]`。`workflow_video_enhancer_full_pipeline.py` / `workflow_video_enhancer_steps.py` 已传参。

---

## 2026-03-31

### 灵感分析：花卉背景 / 黑白大片套路过滤

**逻辑**（`scripts/analyze_video_from_raw_json.py`）：与主灵感分析**同一次**多模态请求；提示词内要求仅输出 JSON，含 `analysis`（正文）与 `flower_background`、`bw_blockbuster`。视频依据**整支视频**、图片依据**整张图**判断套路。命中任一则 `material_tags=["我方已经投过"]`，`exclude_from_bitable` / `exclude_from_cluster`，并跳过单条 UA 建议。`VIDEO_ANALYSIS_STYLE_FILTER_DISABLED=1` 恢复纯文本、不做套路字段。

**下游**：`sync_raw_analysis_to_bitable_and_push_card.py` 主表跳过 `exclude_from_bitable`；`generate_video_enhancer_ua_suggestions_from_analysis.py` 聚类输入排除 `exclude_from_cluster`（`cluster_excluded_count`）。

---

### 多模态封面日内去重默认开启

**变更**：`scripts/cover_style_intraday.py` 中 `is_cover_style_intraday_enabled()` 默认改为开启；通过 `COVER_STYLE_INTRADAY_ENABLED=0`（或 `false`/`no`/`off`）关闭。`scripts/daily_video_enhancer_workflow.sh` 中 `export COVER_STYLE_INTRADAY_ENABLED="${COVER_STYLE_INTRADAY_ENABLED:-1}"`，便于定时任务显式覆盖。`.env.example` 已补充说明。

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

#### `scripts/workflow_video_enhancer_full_pipeline.py`

**Step 2c**（新增，在 2b creative_library 写库后）：
```
原始 N 条
→ 日内去重后 M 条（去除 X 条重复）
→ 跨日去重后 K 条（去除 Y 条历史素材）
```
- **全量原始素材**仍写入 `daily_creative_insights` 和 `creative_library`（完整存档）
- **去重后唯一素材**才进入分析（Step 3）、多维表同步（Step 5）、飞书卡片推送（Step 5）
- `combined_results` 改为基于 `deduped_items` 构建（原为 `raw_payload.get("items")`）
- `analysis_payload` 新增 `deduped_items` 字段记录去重后数量

**首次运行说明**：`creative_library` 为空时，跨日去重命中 0 条（正常现象），随日积累数据后跨日去重会逐步生效。

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

每天 10:30 由 crontab 自动触发：

```
daily_ua_job.sh
  └→ daily_video_enhancer_workflow.sh   # export PYTHONUNBUFFERED=1；可选 OPENROUTER_METER=1
       └→ workflow_video_enhancer_full_pipeline.py --date 昨天
            │
            ├─ Step 1  爬取
            │     scripts/test_video_enhancer_two_competitors_318.py
            │     → workflow_video_enhancer_{日期}_raw.json
            │
            ├─ Step 2  可选封面日内多模态去重（COVER_STYLE_INTRADAY_ENABLED；可用 --skip-cover-dedupe 跳过）
            │     cover_style_intraday.apply_intraday_cover_style_dedupe → 写回 raw + *_cover_style_intraday.json
            │
            ├─ Step 2a 原始落库
            │     upsert_daily_creative_insights（仅 raw，无 analysis）
            │
            ├─ Step 2b 素材主库
            │     upsert_creative_library（全量去重归组）
            │
            ├─ Step 2c 多维去重报告
            │     get_deduped_items_for_analysis → *_dedup_report.json；仅去重后进入分析
            │
            ├─ Step 2d 增量分析准备
            │     复用历史成功分析；pending → *_raw_pending_analysis.json
            │
            ├─ Step 3  灵感分析（子进程；多模态经 llm_client：熔断 + 模型链）
            │     scripts/analyze_video_from_raw_json.py
            │     → video_analysis_*_raw.json（合并历史+本次；含 deduped_items 等）
            │
            ├─ Step 2.5 / 2.6 回写 DB
            │     upsert_daily_creative_insights（带 analysis）+ upsert_daily_video_enhancer_filter_log
            │     upsert_creative_library（同步 analysis）
            │
            ├─ Step 2.7 语义嵌入
            │     llm_client.call_embedding → upsert_analysis_embedding（creative_library.analysis_embedding）
            │
            ├─ Step 2.8 语义去重（聚类输入）
            │     与历史嵌入比对 → exclude_from_cluster；写回 analysis JSON
            │
            ├─ （若本次存在分析失败）提前 return，不跑 UA/推送
            │
            ├─ Step 4  方向卡片（子进程；llm_client.call_text + 历史卡片/趋势等上下文）
            │     scripts/generate_video_enhancer_ua_suggestions_from_analysis.py
            │     → ua_suggestion_*.json / .md
            │
            ├─ Step 5  飞书多维表同步（子进程，--no-card，不写聊天卡片）
            │     scripts/sync_raw_analysis_to_bitable_and_push_card.py
            │     --sync-target both（有 VIDEO_ENHANCER_CLUSTER_BITABLE_URL）| raw（无聚类表）
            │     可选 --cluster-url
            │
            ├─ Step 6  飞书卡片推送（子进程）
            │     scripts/push_video_enhancer_feishu_card_only.py
            │
            ├─ Step 7  推送表入库
            │     upsert_daily_push_content（daily_ua_push_content）
            │
            └─ Step 8  企业微信 + Google Sheet（子进程）
                  scripts/push_video_enhancer_multichannel.py
```

**分步等价**：`workflow_video_enhancer_steps.py`：`crawl_store` → `cover_store`（可选与爬取拆分）→ `analyze_store` → `cluster_store` → `push_sync`。

**代码量（约数，2026-04）**：`scripts/**/*.py` 合计约 **2.47 万行**（`find scripts -name '*.py' | xargs wc -l`）。与工作流强相关的单文件示例：`workflow_video_enhancer_full_pipeline.py` ~616 行、`video_enhancer_pipeline_db.py` ~1800 行、`llm_client.py` ~320 行。

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
