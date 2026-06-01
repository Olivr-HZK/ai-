# Agent 工作日志

本文档记录所有 agent 对项目做出的代码变更与功能更新，供后续 agent 接手时快速了解项目现状。

## 2026-05-22

### [VE] 多维表玩法字段收口为只同步正式标签

- `ua_workflows/video_enhancer/analyze.py` / `ua_workflows/video_enhancer/play_assets.py`：VE 灵感分析提示词改为只从多维表格「玩法」字段候选标签中选择；未命中时写 `unmatched_play`、玩法资产名称留空、玩法归类为「未命中」，不再要求模型生成 `new_play` 或自造新玩法名。
- `ua_workflows/video_enhancer/play_asset_report.py`：AI 明确给出 `unmatched_play` / 「未命中」时停止规则兜底匹配，保持玩法标签为空，避免后续自动补回不确定标签。
- `ua_workflows/video_enhancer/sync.py`：不删除多维表字段，但主表同步不再产出「玩法资产 / 玩法变种 / 玩法新旧 / 玩法资产ID / 玩法变种ID / 玩法判断来源 / 玩法判断理由 / 狭义新判断 / 狭义新理由」等内部列；仅继续写「玩法」「玩法指纹」「差异点」「模板指纹」等筛选需要字段，且「玩法」只有命中正式标签时才写。
- `tests/test_ve_template_normalization.py`：新增回归测试覆盖未命中玩法不产出正式标签、同步字段不包含冗余内部列、`unmatched_play` 不触发规则兜底补标。

### [VE] 筛选看板改为三层漏斗与多维表实数对账

- `ua_workflows/video_enhancer/review_dashboard.py`：筛选复核看板新增「三层筛选漏斗」，按爬取资格、封面/指纹去重、入表前业务筛选展示每层输入、输出和剔除原因；产品明细表同步展示抓到数、第一层后、指纹、跨日 CLIP、日内 CLIP、第二层后、业务硬拦、同模板、应入表与表内实际。
- `ua_workflows/video_enhancer/review_dashboard.py`：看板会用项目 `.env` 中的 `FEISHU_APP_ID` / `FEISHU_APP_SECRET` / `VIDEO_ENHANCER_BITABLE_URL` 只读扫描主表，并按 `抓取日期` 的 UTC+8 显示日期统计当日多维表实际记录数；失败时只在看板备注错误，不阻塞本地看板生成。
- `ua_workflows/video_enhancer/review_dashboard.py`：第三层新增两块明细，「业务硬拦（不进多维表）」和「同玩法同模板（不进多维表）」。同模板明细展示保留代表、封面相似度、模板相似度和原因，便于人工复核最终入表前剔除。
- 已刷新 `2026-05-21` 看板：本地应入多维表 32 条（40 条成功分析 - 6 条业务硬拦 - 2 条同玩法同模板），飞书主表实际 `抓取日期=2026-05-21` 为 0 条，说明当天主表同步尚未落表或同步中断。
- 根据人工查看习惯，顶部漏斗收窄为核心两块：「封面图去重」与「同玩法同模板去重」；第一层爬取资格不再作为主视线展示，只在备注中说明“封面前”已经过广告主、日期和重投过滤。
- 看板中「玩法资产库」改名为「多维表玩法标签库（字段：玩法）」，避免误读为旧 JSON 资产库；新增「筛选后素材 · 每条玩法标签（应入多维表）」表格，逐条展示最终应入表素材的多维表玩法标签、AI 建议玩法、产品、广告 ID、核心卖点、玩法指纹与模板指纹。`new_play` 未命中多维表标签时在「玩法标签」列统一显示「待沉淀」，AI 建议名单独放在「AI建议玩法」列。
- `ua_workflows/video_enhancer/analyze.py`：收紧 AI 新玩法建议名，禁止把「AI图片转视频」「图片转视频」「AI视频生成」「照片生成动态视频」等底层能力词自动当作新玩法沉淀；若模型只给出这类泛词，解析兜底会把玩法名和变种名改为「待人工归类」，并在玩法判断理由里保留原始建议，等待业务人员确认。
- `tests/test_ve_template_normalization.py`：新增泛玩法名清洗回归测试，覆盖「AI 图片转视频」被置为「待人工归类」、具体可复用模板名仍保留。
- `ua_workflows/video_enhancer/sync.py`：修复主表「玩法」字段已从文本变为多选后导致的同步失败。同步写记录前会读取真实字段类型；多选字段写成字符串数组，并且只保留飞书字段已有选项，AI 新建议/待沉淀玩法不会自动创建为正式「玩法」选项。`tests/test_ve_template_normalization.py` 新增多选字段值格式与未知选项过滤测试。
- 2026-05-21 VE 同步排查：同步命令目标确认仍是 `CivwbJ2HkazcKTsKnbGclA5RnWc / tblrZZvVuFcjL0kE / vewGH7cmSs`，不是写错表；原始失败为「玩法」多选字段值格式错误。后续一度出现 `403 Forbidden (91403)`，根因是外层进程环境里残留旧 `FEISHU_APP_ID`（末位 `8dcca`），`load_dotenv()` 默认不覆盖，导致没用项目 `.env` 中正确应用（末位 `c1cb3`）。已将 VE 关键入口改为 `load_dotenv(PROJECT_ROOT / ".env", override=True)`；正确 app 下假字段 `batch_create` 不再 403，而是返回预期字段不存在错误。
- 已用项目 `.env` 正确应用补跑 `2026-05-21` VE 主表同步，写入 32/32 条；多维表实际 `抓取日期=2026-05-21` 读数为 32 条（Pixverse 21、AI Mirror 6、DreamFace 2、Glam 1、Remini 1、GIO 1），并刷新 `reports/ve_filter_review_2026-05-21.html`。
- `ua_workflows/shared/config.py`：新增 `load_project_env()` / `project_env_values()`，用于统一让项目 `.env` 覆盖外层 shell/cron 残留环境变量；`ua_workflows/video_enhancer/pipeline.py` 的子进程环境也会用 `.env` 值覆盖，避免同步、看板、玩法标签读取吃到旧 app id。
- `ua_workflows/video_enhancer/cover_dedupe.py` / `ua_workflows/arrow2/cover_dedupe.py`：封面 CLIP 跨日历史改为双窗口。历史检索窗口默认 60 天（`COVER_STYLE_HISTORY_LOOKBACK_DAYS`，上限 60），硬去重窗口默认 7 天（`COVER_STYLE_HISTORY_HARD_DEDUPE_DAYS`）。7 天内命中历史仍按 `cover_style_cluster_vs_yesterday` 剔除今日素材；7 天外命中历史不直接丢弃，而是保留当天最高展示估值代表，并在报告 `history_refresh` 记录为 `cover_style_cluster_history_refresh`，用于表达「这个旧素材/旧模板又在发力」。
- 2026-05-21 人工复核素材 `300c5d6a2ecdc432aaa4ac4013eace49` 与 2026-05-08 历史素材 `8f2943975b7a180603b6ea249454cf6e` 封面相似度 `0.7554`，超过阈值 `0.75`，但旧 7 天历史检索窗口没加载 5/8。新逻辑会把它归为 13 天前历史簇的持续发力代表，而不是当作全新素材，也不是直接删掉。
- `ua_workflows/video_enhancer/sync.py` / `crawl_similarity.py` / `shared/db/video_enhancer.py`：多维表同步会给 7 天外历史簇代表写入「历史簇持续发力」「历史命中:日期」「历史间隔:N天」「历史封面相似度:X」标签；同一历史刷新簇里被日内合并的素材继续计入代表行「日内相似素材数」；持续发力日报信号也会读取 `history_refresh`。
- `tests/test_ve_template_normalization.py`：新增封面历史窗口默认 60 天、上限 60 天、7 天外历史刷新保留代表、7 天内历史硬去重、历史刷新标签写入的回归测试。

## 2026-05-21

### [VE] Retake 暂停抓取与人物照片特效硬拦截

- `config/ai_product.json`：从 VE 默认 `photo` 竞品列表移除 `Retake AI Face & Selfie Editor`，日常默认抓取暂不再包含 Retake；如需恢复可重新加回对应 appid。
- `ua_workflows/video_enhancer/content_filters.py`：新增“只保留用户上传人物照片加工特效”硬拦截；电商/商品广告、宠物/动物、房间装修、风景、食物、车辆、纯文字/Logo/海报等非人物照片加工素材会标记 `exclude_from_bitable` / `exclude_from_cluster`，标签为 `非人物照片加工特效`。
- `ua_workflows/video_enhancer/pipeline.py` / `sync.py` / `flow_report.py`：主流程与独立同步都会执行该硬拦截，并在同步/漏斗报告中记录 `ecommerce_effect`、`non_human_photo_effect`、`missing_human_photo_input` 等原因。
- `tests/test_ve_template_normalization.py`：新增回归测试，覆盖电商商品图、宠物跳舞、房间装修应拦截，自拍生成生日写真应保留。

### [VE/Arrow2] CLIP 封面去重默认阈值下调

- `ua_workflows/video_enhancer/cover_dedupe.py`：`COVER_VISUAL_DEDUP_THRESHOLD` 未在 `.env` 显式覆盖时，默认值从 `0.8` 下调到 `0.75`；VE 与复用该 helper 的 Arrow2 封面 CLIP 日内 / 跨日向量去重都会使用新默认阈值。若需临时回退，可在 `.env` 设置 `COVER_VISUAL_DEDUP_THRESHOLD=0.8`。
- 注意：历史已生成的 `*_cover_style_intraday.json` 与本地 HTML 看板仍记录当时运行的 `cosine_threshold`，不会因默认值变更自动改写；需要重跑对应日期流程才会按 `0.75` 重新筛。

### [VE] 同玩法同模板换人合并增强

- `ua_workflows/video_enhancer/sync.py`：主表同步前的同模板去重从 exact key 升级为同产品/同玩法桶内并查集聚类；同玩法优先用玩法资产 ID，模板 exact 命中继续保留，同时新增可选模板文本相似（阈值 `BITABLE_TEMPLATE_DEDUP_TEXT_SIMILARITY_THRESHOLD=0.78`）和默认封面 CLIP 相似（阈值 `BITABLE_TEMPLATE_DEDUP_CLIP_THRESHOLD=0.70`）两类边，专门补掉同一模板只换人种、性别或模特导致的漏筛。跳过明细会记录 `match_reason`、`match_ad_key`、模板相似度和封面相似度，链式聚类也能看到直接证据。
- `tests/test_ve_template_normalization.py`：新增同步前模板模糊合并回归测试，覆盖同一模板不同描述应合并、同玩法但不同模板应保留。
- 已对 `2026-05-20` 本地样本做只读 dry-run：成功分析候选 95 条；旧 exact 逻辑删除 1 条、剩 94 条；新逻辑删除 15 条、剩 80 条，额外多筛 14 条（候选池额外压缩 14.74%）。产物：`data/workflow_video_enhancer_2026-05-20_template_dedup_dry_run.json`、`reports/ve_template_dedup_dry_run_2026-05-20.html`。
- 根据人工复核结论，模板文本相似不再默认作为自动合并证据；仅当 `BITABLE_TEMPLATE_DEDUP_TEXT_SIMILARITY_ENABLED=1` 时才开启。默认自动合并保留 exact 模板和同玩法内封面 CLIP 相似。

### [VE] 玩法库切到多维表格「玩法」字段

- `ua_workflows/video_enhancer/bitable_play_labels.py`（新增）：从 `VE_PLAY_LABEL_BITABLE_URL` 或 `VIDEO_ENHANCER_BITABLE_URL` 指向的多维表读取「玩法」字段（可用 `VE_PLAY_LABEL_FIELD_NAME` 覆盖字段名），优先取单选/多选字段选项，取不到选项时扫描记录里的历史值；标签会转成旧代码兼容的 `play_<sha1>` 玩法 ID，并缓存到 `data/ve_bitable_play_labels.json`。
- `ua_workflows/video_enhancer/play_assets.py`：默认 `VE_PLAY_LIBRARY_SOURCE=bitable`，不再使用 `config/ve_play_assets.json` 作为主玩法库；如需临时回退旧 JSON，可设置 `VE_PLAY_LIBRARY_SOURCE=legacy_json`。
- `ua_workflows/video_enhancer/analyze.py`：分析 prompt 改为「多维表格玩法标签库候选清单」，要求 AI 直接从多维表格「玩法」标签中选择玩法；旧 `玩法资产名称` 字段现在语义上就是多维表格「玩法」标签，`玩法变种` 固定为基础变体/新玩法兼容值。
- `ua_workflows/video_enhancer/sync.py`：主表字段新增「玩法」，同步时写入 AI 选择的多维表格玩法标签；保留「玩法资产」等旧字段用于兼容历史看板/报表。
- 当前本地 `lark-cli` bot 读取表结构/记录缺少 `base:field:read` / `base:record:read` scope，user 身份也未授权；代码已支持读取，实际拉取标签需先给应用开通对应权限或提供 `VE_PLAY_LABELS` 临时标签列表。

## 2026-05-19

### [Arrow2] 最新素材切到 UI 指定日期抓取

- `ua_workflows/arrow2/crawl.py`：`latest_yesterday` 默认改为和 VE 一样先在广大大 UI 选择 `target_date ~ target_date`，再逐张点卡 detail-v2 并用本地 `first_seen == target_date` 校验；保留 `--no-ui-date-range` 作为旧 7 天池口径调试回退，raw 产物新增 `ui_date_range`。
- `ua_workflows/arrow2/crawl.py` / `ua_workflows/shared/guangdada/search.py`：搜索按钮点击超时时增加 force click / Enter 兜底；单产品爬取异常时会重登、重设筛选并重试一次，重试仍失败则记录空结果后继续后续产品，避免一个产品页面状态异常中断整天同步。
- 已用新 UI 日期口径补跑 `2026-05-17` 与 `2026-05-18` Arrow2 latest：分别同步 45 条、40 条到多维表；本地 `data/arrow2_pipeline.db` 对应日期分析完成数为 45/45、40/40。

### [VE] 全流程产品漏斗报告与低量告警

- `ua_workflows/video_enhancer/flow_report.py`（新增）：汇总当日 crawl、分析入队、分析结果、sync 与 acceptance 产物，生成 `data/workflow_video_enhancer_{date}_flow_report.json` 和 `reports/workflow_video_enhancer_{date}_flow_report.md`；默认读取 `VE_FLOW_REPORT_FEISHU_WEBHOOK` 推送飞书交互卡片。
- `ua_workflows/video_enhancer/pipeline.py`：分析入队阶段新增 `data/workflow_video_enhancer_{date}_analysis_queue_report.json`，按产品记录封面后素材数、分析前去重、历史缓存复用、准入失败原因和实际 LLM 入队数；流程成功结束和分析成功率过低提前退出时都会补跑全流程报告。
- `ua_workflows/video_enhancer/sync.py`：主表同步新增 `data/workflow_video_enhancer_{date}_sync_report.json`，按产品记录成功分析数、硬排除、同模板换人/性别合并、同玩法非代表、低采纳优先级跳过和最终主表写入数。
- 全流程报告按产品展示点卡、抓到、爬取保留、封面后、分析去重后、LLM 入队、可用分析、同步候选、主表写入和主要筛掉/跳过原因；若某产品「封面后保留」或「主表写入」低于近 5 天均值 50%（且历史均值不低于 3 条），飞书卡片会给出人工确认用的单产品重试命令。

### [VE] 相似素材数前移到爬取产物

- `ua_workflows/video_enhancer/crawl_similarity.py`（新增）：相似素材数从 raw/crawl 阶段开始沉淀。standalone crawl 会按同产品/appid 下的 exact `image_ahash_md5`、封面 URL、视频 URL 先写入 `crawl_similarity_count_by_ad` 与每条 item/creative 的 `crawl_similarity_count`。
- `ua_workflows/video_enhancer/pipeline.py`：封面 CLIP 日内聚类后调用 `merge_cover_similarity_counts`，把 `cover_style_cluster` 同日剔除成员计入代表素材的 raw 相似素材数；跨日旧封面不计入当天「日内相似素材数」。
- `ua_workflows/video_enhancer/sync.py`：多维表「日内相似素材数」优先读取 raw 中的 `crawl_similarity_count_by_ad`，再用旧的同步时兜底逻辑和同模板换人/性别合并补充；避免最后同步阶段才临时开始计算。
- `tests/test_ve_template_normalization.py`：新增 raw 阶段 exact 签名计数、封面 CLIP 日内簇合并计数回归测试。

## 2026-05-18

### [VE] 最新素材狭义新判断

- `ua_workflows/video_enhancer/analyze.py`：VE 结构化输出新增 `【模板指纹】`，要求模型描述具体分镜/布局/脚本骨架，并忽略同模板只换人种、肤色、性别或男女模特；`analysis` 兼容短摘要同步包含模板指纹。
- `ua_workflows/shared/db/video_enhancer.py` / `pipeline.py`：`daily_creative_insights` 与 `creative_library` 自动迁移新增 `template_fingerprint`，单条增量入库、素材库回写、历史缓存复用和日报候选读取都会保留该字段；主流程回写素材库时同步携带玩法资产判断与模板指纹。
- `ua_workflows/video_enhancer/play_asset_report.py`：日报新口径从“新玩法/新变种”收敛为“新玩法/老玩法新迭代/老玩法换皮”。稳定玩法未见过算新玩法；稳定玩法已见过但模板指纹首次出现算老玩法新迭代；稳定玩法与模板指纹均见过（包括同模板换人种/性别）算老玩法换皮，不进入推送代表。
- `ua_workflows/video_enhancer/sync.py` / `push_feishu.py` / `review_dashboard.py`：多维表新增「模板指纹」「狭义新判断」「狭义新理由」字段；日报标签和飞书卡片改为狭义新素材口径。未改动 Arrow2 定时或数据流。
- `ua_workflows/video_enhancer/pipeline.py`：移除日常主流程里的二次 `dom_enrich` 详情补全节点；VE 抓取已默认走 DOM 点卡 detail 主路径，raw 阶段已经拿到详情，`dom_enrich.py` 仅保留作手动排障工具。
- `ua_workflows/video_enhancer/content_filters.py` / `pipeline.py` / `sync.py`：移除「低采纳主题/家居装修」硬拦截，空房间、豪华装修、家居设计等不再仅因主题被写 `exclude_from_bitable`。
- `ua_workflows/video_enhancer/sync.py`：多维表「日内相似素材数」计数补入同日封面 CLIP 聚类排除成员；仅统计 `cover_style_cluster` 日内封面重复，不把跨日旧封面 `cover_style_cluster_vs_yesterday` 算入当天扎堆数。
- `ua_workflows/video_enhancer/sync.py`：主表同步新增默认开启的 `BITABLE_TEMPLATE_DEDUP_ENABLED` 模板级硬筛。同步前按同产品/appid、人口/性别归一后的玩法指纹、人口/性别归一后的模板指纹分组，只保留展示估值/曝光/热度最高代表；被同模板合并的素材仍会计入代表行「日内相似素材数」。已清理 2026-05-17 GIO 主表中 4 条同模板换人重复行，并回填 3 条代表素材的相似素材数。
- `ua_workflows/video_enhancer/crawl.py` / `pipeline.py`：新增每日产品级抓取保留漏斗。爬取 raw 的 `filter_report.per_product` 增加点卡详情数、页面卡片数、抓到素材数、广告主不匹配、非目标日、截断剔除等计数；主流程生成 `crawl_product_retention_report` 与 `data/workflow_video_enhancer_{date}_crawl_product_retention.json`，合并封面跨日/日内去重原因和最终保留数。
- `tests/test_ve_template_normalization.py`：新增狭义新回归测试，覆盖同模板人口属性替换不推送、老玩法新模板进入“老玩法新迭代”、全新稳定玩法进入“新玩法”。

## 2026-05-15

### [VE] 审核多维表反馈训练链路（独立于正常工作流）

- `ua_workflows/video_enhancer/feedback_training.py`（新增）：从 VE 审核多维表 `CivwbJ2HkazcKTsKnbGclA5RnWc / tblrZZvVuFcjL0kE / vewJtPixtM` 直接拉取字段，单独写入 `data/ve_feedback_training.db`。`接受情况` 中 `接受`/`采纳`/`入素材库=1`，`删除`/`不采纳=0`，`待定`/`重复抓取`/空值只做留存不进训练。
- 训练特征限定为素材本身：标题、正文、视频/封面链接、核心卖点、Hook、脚本/口播、玩法资产/变种、玩法指纹、差异点、风险等级、AI 分析和素材标签；产品、广告主、抓取日期、展示估值、人气、热度、投放地区等运营字段只写审计 JSON，不作为自变量。
- 新增产物：`data/ve_feedback_training_dataset_{date}.jsonl`、`data/models/ve_feedback_preference_nb_{date}.json`、`reports/ve_feedback_training_{date}.md`。当前 baseline 是无额外依赖的文本朴素贝叶斯，便于先积累反馈闭环；后续可替换为多模态/排序模型。
- `ua_workflows/video_enhancer/feedback_training.py`：新增 `--complete-profile` 完整度过滤。`core` 只用标题、视频/封面链接、核心卖点、Hook、脚本/口播、风险等级、AI 分析齐全的样本；`core_play` / `play` 额外要求玩法字段；`all` 要求所有训练字段齐全（当前历史数据因 `玩法判断理由` 为空会筛到 0 条）。
- `scripts/run_ve_feedback_training.py` / `scripts/cron_ve_feedback_training_daily.sh`（新增）：支持 `run`（拉取+入库+导出+训练）、`pull`、`train`、`export`；cron 建议每天 09:40 运行，日志 `logs/cron_ve_feedback_training.log`。该链路不触发广大大爬取、VE 分析、多维表主表同步或日报推送。
- `.gitignore`：忽略反馈训练本地产物（SQLite、JSONL、模型 JSON、日报 Markdown），避免每日训练输出进入版本管理。
- `docs/ve-feedback-training.md` / `docs/workflows.md` / `docs/cron-schedules.md` / `docs/README.md`：补充独立反馈训练链路说明、字段口径、产物和定时任务。

### [VE] 目标日最新创意滚动深度修复

- 飞书迭代日志人工复核确认：2026-05-14 PixVerse 若干图片素材的 exact `ad_key` 未进入本地 raw / `daily_creative_insights` / `creative_library`，不是后续封面阈值或玩法筛选误杀；本地 PixVerse 目标日素材时间从 `18:29` 跳到 `14:33`，漏掉了用户截图中的 `17:xx` 区间。
- `ua_workflows/shared/guangdada/search.py`：`run_batch` / `_collect_keyword_crawl_result` 新增可配置 `max_scroll_rounds` 与 `stop_scroll_if_oldest_first_seen_before_ymd`，复用已有 `_search_one_keyword` 的“滚到早于目标日即停”能力。
- `ua_workflows/video_enhancer/crawl.py`：VE 在 `--target-date` 模式下默认把最新创意滚动上限从固定 16 轮提高到 56 轮，可通过 `VIDEO_ENHANCER_MAX_SCROLL_ROUNDS` 覆盖。因广大大初始 NAPI 响应会混入很老的历史素材，`first_seen < target_date` 早停容易误触发并导致只抓首屏，现默认关闭该早停；如需临时打开可设 `VIDEO_ENHANCER_TARGET_DATE_EARLY_STOP_ENABLED=1`。原封面 CLIP 阈值仍保持默认 `0.8`。

### [VE] 同模板人种/性别差异不再拆新素材

- `ua_workflows/shared/db/video_enhancer.py`：玩法文本归一化新增人口属性抹平规则，黑人/白人/亚裔、男性/女性、男模/女模等同一模板换人差异会归为同一玩法文本，从而影响日内玩法重复、老玩法重复、日报新素材/新变种 key、玩法资产 fallback 匹配等后续判重入口；`男变女` / `女变男` 等性别转换机制会先规整为 `性别转换`，避免误删核心玩法。
- `ua_workflows/video_enhancer/analyze.py`：VE 分析 prompt 新增模板级去重规则，要求模型不要因为同一模板只替换人种、肤色或性别就输出新玩法/新变种，也不要把这些人口属性写进玩法指纹/玩法变种名。
- `tests/test_ve_template_normalization.py`：新增回归测试，覆盖“黑人女性 vs 白人男性同模板”为同一玩法，以及性别转换机制仍被保留。

### [VE] 视频分析结构化收敛与 analysis embedding 去重下线

- `ua_workflows/video_enhancer/analyze.py`：VE 多模态分析 prompt 取消原有 1～6 段正文分析，只要求模型输出 7 个固定结构化字段：`Hook解析`、`脚本口播`、`核心卖点`、`玩法指纹`、`差异点`、`风险标签`、`风险等级`。`analysis` 老字段不再承载长正文，仅由这些结构化字段拼出兼容短摘要，保证旧同步成功判定与多维表 `AI分析结果` 不为空。
- `ua_workflows/video_enhancer/pipeline.py` / `ua_workflows/shared/db/video_enhancer.py`：下线旧 `analysis_embedding` 语义跨日去重链路，不再创建、写入或读取 `creative_library.analysis_embedding`，也不再跑 `_apply_semantic_dedup`。后续玩法相似判断只保留 `play_fingerprint` / `effect_one_liner` 文本与 `effect_one_liner_embedding` 相关链路，避免正文随机性继续影响去重。
- `.gitignore`：补充忽略筛选看板 HTML、`reports/assets/` 封面缓存、验收报告、方向卡片 md 与本地抓取调试文件，避免自动产物继续进入 git status；已发现仓库中仍有旧的 tracked 报告/调试文件，后续可单独做一次 git rm 清理。
- 项目安全清理：删除已废弃且无代码引用的 tracked 调试/报告/旧配置产物，包括根目录抓包 HTML/PNG/JSON、`config/style_filters.json`、`config/twitter_input.*`、旧 `data/ua_suggestion_workflow_video_enhancer_*.md`、旧 `reports/workflow_video_enhancer_*_acceptance.md`。未删除当前筛选看板 HTML 与 `reports/assets/` 封面缓存，避免影响正在浏览的复核页面。

### [VE] 内部特效上线记录合并到玩法资产库

- `config/ve_play_assets.json`：通过 Chrome/Computer Use 从 Google Sheet `AI产品热点排期表 / 特效上线记录`（2025-11-17～2026-05-14，364 条）复制解析内部上线主题，并入 VE 玩法资产库。资产数从 25 扩到 27，子标签从约 100 扩到 144；新增稳定基类「照片生成直播间/主播 PK 场景」和「照片生成密集场景寻人/找自己」。
- `config/ve_play_assets.json`：把 Stadium/Stands Cam、Soccer Minis、Pet Dance、Warm Reunion/Alive Again/Still Here、Find Me/Where’s Me、Go Live/Live Queen/Live Battle、Chibi Emojis/Chibi Doll、Hairstyle Lookbook、Best Mom/Mother’s Day、Floral/Chinese/Graduation 等内部上线主题沉到现有玩法的 aliases、关键词、example_effects 和 subtags，作为新变种判断依据，而不是在代码里补产品特判。
- `ua_workflows/video_enhancer/play_asset_doc_sync.py` / `docs/ve-play-assets.md`：云文档渲染头部新增内部上线记录来源、记录数、日期范围和 Google Sheet 入口；本地文档补充该 Google Sheet 是玩法资产库的内部上线主题来源。
- 已验证 `config/ve_play_assets.json` JSON 合法，`match_play_asset` 可正确命中 Stadium Cam、Find Me/Where’s Me、Go Live/Live Battle、Pet Dance、Warm Reunion、Chibi Emojis 等代表样例；已渲染云文档 Markdown 预览 `/tmp/ve_play_assets_doc.md`。

### [VE] 视频分析阶段直接输出玩法资产判断

- `ua_workflows/video_enhancer/analyze.py` / `play_assets.py`：VE 视频/图片分析 prompt 现在会注入压缩后的玩法资产库候选清单，要求模型额外输出 `【玩法资产ID】`、`【玩法资产名称】`、`【玩法变种ID】`、`【玩法变种名称】`、`【玩法归类】`、`【玩法判断理由】`；其中已有玩法必须引用资产库 ID，不匹配时写 `new_play`，已有资产但新变种写 `new_variant`。分析子进程启动时会先尝试从飞书云文档拉取最新资产库，失败则使用本地 JSON。
- `ua_workflows/shared/db/video_enhancer.py`：`daily_creative_insights` 与 `creative_library` 自动迁移新增 AI 玩法判断字段（资产 ID/名称、变种 ID/名称、新旧、来源、理由），单条分析入库和素材库回写都会保留这些字段，历史缓存复用时也不丢失。
- `ua_workflows/video_enhancer/play_asset_report.py` / `sync.py`：玩法归类改为「AI 主判 + 规则兜底」。日报/同步优先使用分析阶段的 AI 资产判断；若 AI 缺失、ID 无效或不确定，再走关键词/别名/案例匹配。多维表新增「玩法判断来源」「玩法判断理由」字段，并在素材标签里写 `玩法判断:AI`，但「新玩法 / 新变种 / 已沉淀」仍会结合历史库重新计算，避免只相信单条素材自报。
- `README.md` / `docs/workflows.md` / `docs/setup-and-data.md` / `docs/ve-play-assets.md`：补充 VE 玩法资产库、AI 玩法判断、主表新增字段、内部上线记录来源和本地/云文档兜底关系，方便后续按文档接手维护。
- 已验证 prompt 中包含 `photoshoot_noir_vintage` 等变种 ID，解析器能正确抽取 AI 玩法字段，临时 SQLite upsert/readback 可保留 `play_asset_id`，`git diff --check` 通过。

## 2026-05-14

### [VE] 正式抓取口径切为 UI 指定日期 + 点卡校验

- `ua_workflows/video_enhancer/crawl.py` / `ua_workflows/video_enhancer/pipeline.py`：确认正式 VE 主流程调用 `--target-date` 时默认设置 UI 日期范围为 `target_date ~ target_date`，替代旧的「先选 7 天再本地筛」口径；`--no-ui-date-range` 仅保留作 A/B 调试回退。
- 对 PixVerse 2026-05-12 做三组对比：旧 `napi list + 7天` 仅 5 条，`7天 + 点卡 detail + first_seen` 39 条，`UI 指定日期 + 点卡 detail + first_seen` 64 条；因此正式口径采用最后一种。注意 UI 日期筛仍会夹带非目标日排序结果，必须保留本地 `first_seen == target_date` 校验和早停。
- `ua_workflows/shared/guangdada/search.py`：headed 检查 JSON 额外写入 `date_filtered_out`，用于人工对比被本地日期校验剔除的非目标日素材。
- `ua_workflows/shared/db/video_enhancer.py`：VE 数据库补充卡片 detail 可得到的投放地区字段；`daily_creative_insights` 与 `creative_library` 新增 `country_codes_json`、`geo_targeting_json` 并自动迁移。展示估值/人气/热度继续写入既有 `all_exposure_value` / `impression` / `heat` 数值列；投放地区同时保留规范化国家码与原始 geo payload，便于后续同步和对账。

### [VE] 2026-05-13 筛选复核看板刷新与 CLIP 日内展示修正

- `ua_workflows/video_enhancer/review_dashboard.py`：刷新 2026-05-13 复核看板；CLIP 指标改为「CLIP 封面命中」，并在统计备注中拆分跨日 / 日内，避免把 `cover_style_cluster` 日内簇误读成跨日命中。
- `ua_workflows/video_enhancer/review_dashboard.py`：修复统计值为 `0` 时被空字符串吞掉的问题；旧报告中缺失被剔除素材封面的 CLIP 日内簇，会用「同簇代表封面」兜底展示，避免页面出现 `无本地封面`。
- `ua_workflows/video_enhancer/review_dashboard.py`：一句话 / 玩法筛选板块前置到封面去重板块之前，每个玩法簇先展示封面卡片，再展示文字证据，避免复核时需要翻过大量 CLIP 簇才能看到一句话筛掉的素材。
- `ua_workflows/video_enhancer/review_dashboard.py`：一句话 / 玩法筛选板块改为「被剔除素材 vs 对比对象」成对封面展示；日内玩法、embedding、语义重复直接取命中 ad_key，老玩法仅有文案证据时会按 appid + 玩法指纹从本地库反查历史代表素材补封面。
- `ua_workflows/video_enhancer/review_dashboard.py`：一句话对比卡新增两张封面的 CLIP 相似度与阈值解释；看板新增「未筛掉 / 当前看板保留素材」与「日报推送筛选逻辑」板块，展示 5/13 当前保留 46 条、日报新素材推送段 5 条，以及 5 条 `exclude_from_cluster=1` 但未写 `exclude_from_bitable=1` 的口径差异素材。
- `ua_workflows/video_enhancer/analyze.py`：收紧 VE `玩法指纹` 生成原则，要求模型先归纳稳定的「输入对象 + 关键变换 + 输出大类」，把热点话题、运动项目、节日、发型、服装、名人同款、画面文案等易变元素放到「差异点」，减少靠不断补硬编码规则来合并玩法。
- `config/ve_play_assets.json` / `ua_workflows/video_enhancer/play_assets.py`：新增人工沉淀的 VE 玩法资产库与通用关键词匹配 helper，默认读取可版本化的 `config/ve_play_assets.json`（兼容旧 `data/ve_play_assets.json`）。当前版本基于本地库 2026-03-18～2026-05-13 的 526 条历史已分析素材沉淀，共 25 个资产，历史覆盖率 100%；其中 2026-05-05～2026-05-13 的 466 条五月已分析素材也 100% 覆盖。资产包含体育赛事现场转播、写真/杂志封面、双人融合、美颜前后对比、照片动态化、机甲奇幻、跳舞动画、模板换脸、老照片修复、游戏角色皮肤、路人消除、成人交友导流拦截等稳定玩法资产。
- `config/ve_play_assets.json` / `ua_workflows/video_enhancer/play_assets.py`：玩法资产新增 `subtags` 子标签机制，一个素材先命中稳定基类，再可多选变体标签；当前 25 个基类中 23 个已配置 100 个子标签，覆盖 F1/赛车、棒球、足球/世界杯、观众席/球迷、直播/转播包装、生日主题、杂志封面、红毯/奢华生活、名人宝丽来合影、病毒/热门 trend、换脸主角、机甲/战甲、恶魔/僵尸、老照片上色、文本提示词、成人导流风险 hook 等变体。2026-05-05～2026-05-13 五月素材中 400/466 条已命中至少一个子标签。
- `ua_workflows/video_enhancer/review_dashboard.py`：复核看板读取玩法资产库，统计「玩法资产命中 / 待沉淀」，并新增「玩法资产库（历史沉淀）」与「未筛掉素材 · 按玩法资产归类」板块，方便每天把新类目继续沉淀为资产，而不是在代码里继续补产品特判。
- `ua_workflows/video_enhancer/play_asset_report.py` / `push_feishu.py` / `push_multichannel.py`：新增「新玩法 / 新玩法变种」日报口径。日报先给当日候选素材命中玩法资产与子标签，再与历史全量素材比对：玩法基类历史没见过算「新玩法」，基类已见过但子标签组合没见过算「新玩法变种」；飞书/企微日报默认只推这两类代表素材，不再把持续发力或普通老玩法换素材混入推送。2026-05-13 当前口径为新玩法 0 个、新玩法变种 13 个、代表素材 13 条。
- `ua_workflows/video_enhancer/sync.py`：主表新增「玩法资产」「玩法变种」「玩法新旧」「玩法资产ID」「玩法变种ID」「玩法指纹」「差异点」字段，并把玩法资产/变种/新旧标签写入「素材标签」，方便在多维表格里按稳定玩法和 trend 变体筛选复核。
- `ua_workflows/video_enhancer/pipeline.py` / `sync.py`：主表同步默认放宽，日内玩法重复、老玩法重复、embedding 玩法重复不再默认作为 `exclude_from_bitable` 硬拦截；`BITABLE_SYNC_DAILY_PLAY_REPRESENTATIVES_ONLY` 与 `BITABLE_ACCEPTANCE_PRIORITY_SYNC_ENABLED` 默认改为关闭，主表尽可能同步更多成功分析素材。成人/色情、已投放等硬风险仍保留拦截；如需恢复旧的窄同步，可显式打开对应环境变量。
- `ua_workflows/video_enhancer/review_dashboard.py`：筛选复核看板的「日报推送筛选逻辑」切换到新玩法资产/变种口径，新增「日报新玩法 / 新变种推送段」和「日报新口径未推送素材」板块；未推送素材会标注「同资产变种非代表」或「已沉淀玩法/变种」，并展示封面、玩法资产、子标签、变种 ID，方便复核为什么没有进入每日推送。
- `ua_workflows/video_enhancer/play_asset_doc_sync.py` / `scripts/sync_ve_play_assets_doc.py` / `docs/ve-play-assets.md`：将飞书云文档 `https://www.feishu.cn/docx/HrxAdmiN6o7S4BxSNpXcT8h2n1n` 升级为「人可编辑、机器可解析」的玩法资产库源。文档中每个玩法块含 YAML 字段；项目默认尝试从云文档拉取并覆盖 `config/ve_play_assets.json`，失败则使用本地 JSON 兜底。支持 `pull-doc`、`push-doc`、`render`、`append-drafts --date YYYY-MM-DD`；5/13 当前无待沉淀草稿需要追加。
- `ua_workflows/video_enhancer/cover_dedupe.py`：未来 CLIP 日内剔除明细也会写入 `product`、`all_exposure_value`、`cover_url`，后续看板不再需要同簇代表图兜底。
- 已生成并在 in-app browser 打开 2026-05-13 看板：`reports/ve_cover_dedupe_clusters_2026-05-13.html` / `reports/ve_filter_review_2026-05-13.html`。当前统计：CLIP 50 条 / 30 簇（跨日 12、日内 38），指纹跨日 3 条 / 3 簇，一句话命中 8 个素材 / 8 簇 / 8 条证据，其中封面已覆盖 0 个，玩法兜底 8 个；保留素材 46 条均已命中 25 个玩法资产之一；浏览器验证无 `无本地封面` 占位。

## 2026-05-13

### [VE] 爬取主路径改为页面卡片逐张点击 detail-v2

- `ua_workflows/shared/guangdada/search.py`：新增通用 latest 点卡主路径 `_collect_keyword_crawl_result_latest_dom_detail`，流程为搜索/滚动后读取当前 DOM 卡片并逐张获取 `detail-v2`，再用 detail 覆盖 DOM 基础字段；若点卡结果为空才回退 napi 列表。`_click_cards_for_details` 的响应解析改为直接支持 detail-v2 返回的单 dict，并收窄监听日志，避免普通运行时刷屏。
- `ua_workflows/shared/guangdada/search.py`：`run_batch` 新增 `detail_click_primary` 与 `first_seen_target_ymd` 参数；VE 使用时会完整滚动加载页面卡片，并在点开的 detail 真正早于目标日期时早停（`VIDEO_ENHANCER_FIRST_SEEN_EARLY_STOP=0` 可关）。点卡默认翻 3 页以覆盖超过 70 条的竞品素材池，`VIDEO_ENHANCER_DOM_CLICK_MAX_PAGES` 可调页数，`VIDEO_ENHANCER_DOM_CLICK_MAX_CARDS` 可限每页点卡数。
- `ua_workflows/video_enhancer/crawl.py`：默认启用 `dom_detail_click` 模式，和 Arrow2 点击卡片抓 detail 的逻辑对齐；有 `--target-date` 时默认同步把 UI 日期设为 `target_date ~ target_date`，减少 7 天池子/虚拟列表加载不完整导致的漏抓。raw 输出新增 `crawl_mode` / `ui_date_range`。保留 `--no-ui-date-range` 对比旧 7 天口径，保留 `--napi-list` 作为旧 napi 列表口径的调试回退；最终过滤会丢弃无 `ad_key` 的 DOM-only 行，避免后续入库缺主键。
- `ua_workflows/shared/guangdada/search.py` / `scripts/test_video_enhancer_crawl.py`：新增 headed 人工验收辅助，`--all-products --pause-per-product` 会按全部 VE 产品逐个爬取，每个产品完成后在浏览器保持当前页面并打印 `source/all_creatives/detail_rows`、目标日 `first_seen` 命中数和日期分布摘要，等待回车再继续下一个产品。

### [VE] CLIP 封面跨日去重硬拦截

- `ua_workflows/video_enhancer/cover_dedupe.py`：修正 CLIP 封面向量跨日去重口径。过去实现虽然把近 7 日历史封面向量加入聚类，但同簇按展示估值最高者胜出，导致“今日估值更高”的跨日相似封面会被保留；现改为只要今日封面命中历史 CLIP 簇（同 appid、cosine ≥ `COVER_VISUAL_DEDUP_THRESHOLD`，默认 0.8），即按 `cover_style_cluster_vs_yesterday` 剔除今日素材，只有纯日内簇才按展示估值保留当日代表。
- `ua_workflows/shared/db/video_enhancer.py`：`load_cover_style_rows_for_dates_grouped_by_appid` 返回历史 `target_date`，供 CLIP 跨日命中证据记录 `matched_date` 与相似度。
- `ua_workflows/video_enhancer/review_dashboard.py`：新增本地 HTML 筛选复核看板，统一展示 CLIP 跨日封面、ahash/url 指纹跨日封面，以及「一句话 / 玩法筛选剔除」明细；同簇聚合展示，支持搜索，封面会缓存到 `reports/assets/ve_filter_review_日期/`，并兼容旧的 `ve_cover_dedupe_clusters_日期.html` 地址。
- `ua_workflows/video_enhancer/review_dashboard.py`：一句话 / 玩法筛选剔除板块现在也会在每个玩法簇内展示被筛掉素材的封面卡片，再展示文字证据明细，便于直接按图复核误杀。
- `ua_workflows/video_enhancer/review_dashboard.py`：一句话板块新增筛选归因拆分：按当前封面规则已会被 CLIP/指纹提前剔除的素材标为「封面已覆盖」，不再计作一句话筛选能力；剩余标为「玩法兜底」。同时展示粗粒度「玩法族」，降低自由一句话随机性对人工量化的影响。
- `ua_workflows/video_enhancer/pipeline.py`：主流程在分析筛选与 embedding 写库后自动刷新复核看板，默认写 `reports/ve_filter_review_日期.html`，同时覆盖 `reports/ve_cover_dedupe_clusters_日期.html`，便于日常直接打开固定地址复核。
- 已生成 2026-05-12 封面去重人工复核 HTML：`reports/ve_cover_dedupe_clusters_2026-05-12.html`，上半部分展示按新规则识别出的 CLIP 跨日命中（17 条 / 9 簇），下半部分展示原流程已剔除的 ahash/url 指纹跨日命中（20 条 / 18 簇）；封面缓存于 `reports/assets/ve_cover_dedupe_2026-05-12/`。
- 已刷新 2026-05-12 筛选复核 HTML：`reports/ve_filter_review_2026-05-12.html` 与 `reports/ve_cover_dedupe_clusters_2026-05-12.html`，当前统计为 CLIP 跨日 17 条 / 9 簇、指纹跨日 20 条 / 18 簇、一句话命中 33 个素材 / 26 簇 / 39 条证据，其中按当前封面规则已覆盖 12 个、真正玩法兜底 21 个；浏览器验证无「无本地封面」占位。
## 2026-05-11

### [VE] 核心卖点与玩法指纹拆分

- `ua_workflows/video_enhancer/analyze.py`：VE prompt 在「核心卖点」之外新增固定 `【玩法指纹】` 与 `【差异点】` 行；核心卖点继续面向人工阅读和推送展示，玩法指纹专门面向机器去重，要求写清输入对象、关键转换、输出形态和不可省略视觉元素，减少「AI修图/AI美颜」这类泛化描述对去重的干扰。
- `ua_workflows/shared/db/video_enhancer.py` / `pipeline.py`：`daily_creative_insights` 与 `creative_library` 自动迁移新增 `play_fingerprint`、`differentiator`；日内玩法、老玩法、embedding 硬拦截、embedding 候选和日报新玩法判断均优先使用 `play_fingerprint`，缺失时回退 `effect_one_liner`。
- `ua_workflows/shared/db/video_enhancer.py` / `push_feishu.py`：日报「新玩法」不再等于素材条数，也不做每产品固定数量截断；先排除 `exclude_from_bitable` 硬拦截素材，再把 `play_fingerprint` 折成粗粒度玩法族并结合文本/embedding 做聚类，且默认会参考全 VE 近 7 日历史玩法族（不只同 appid）判断是否“完全新玩法”，`new_effect_count` 统计聚类簇数，`new_material_count` 统计这些新玩法簇内素材数，推送展示每个簇的代表素材并标注同玩法素材数。可用 `DAILY_PLAY_CLUSTER_TEXT_THRESHOLD`、`DAILY_PLAY_CLUSTER_EMBEDDING_THRESHOLD`、`DAILY_PLAY_CLUSTER_EMBEDDING_ENABLED=0`、`DAILY_PLAY_GLOBAL_HISTORY_ENABLED=0` 调整。
- `ua_workflows/video_enhancer/sync.py`：主表创建新行时复用日报新玩法聚类结果，把 `日报:新玩法代表`、`日报:新玩法`、`玩法族:...`、`同玩法素材数:N`、`日报:老玩法换素材` 等写入现有「素材标签」字段；不需要更新历史行或新增字段权限。可用 `BITABLE_DAILY_PLAY_TAGS_ENABLED=0` 关闭，`BITABLE_DAILY_PLAY_TAG_LOOKBACK_DAYS` 调整历史窗口。
- `ua_workflows/video_enhancer/pipeline.py`：主流程口径收敛为「前一天全量抓取 → 分析/去重 → 同步去重后的主表素材 → 推送新玩法日报」；旧 UA 方向卡片生成失败不再阻塞主表同步和日报推送，仅跳过旧聚类表同步。
- `ua_workflows/video_enhancer/content_filters.py` / `sync.py`：根据 2026-05-09～2026-05-11 多维表人工反馈校准：将 dating / 免费聊天 / videochat / busty / 附近嫂子等成人交友导流素材纳入硬拦截；将空房间/豪华装修/家居设计等低采纳主题纳入硬拦截（`LOW_FIT_THEME_FILTER_ENABLED=0` 可关）；主表同步默认只写入同日新玩法簇代表，跳过 `日报:同玩法素材` 非代表行，减少同玩法重复抓取噪声。可用 `BITABLE_SYNC_DAILY_PLAY_REPRESENTATIVES_ONLY=0` 临时恢复全量同玩法同步。
- `ua_workflows/video_enhancer/sync.py`：新增采纳率优先同步评分（默认开启，`BITABLE_ACCEPTANCE_PRIORITY_SYNC_ENABLED=1`）：新玩法代表保留；老玩法换素材需命中高采纳主题（球赛抓拍、机甲科幻、手绘漫画、亲情合影、热门模板、人物形象替换、生日写真、明星合影红毯、剧情短片、年龄变化、求职商务照等）才进主表，并写入 `高采纳主题:...` 标签；默认阈值 `BITABLE_ACCEPTANCE_PRIORITY_MIN_SCORE=3`，偏“少而精”。
- 已清空本地 SQLite 中 2026-05-09、2026-05-10 VE 原有分析字段后重新跑分析与去重回写，生成校准报告：`data/ve_play_fingerprint_dedupe_2026-05-09_2026-05-10.json`、`reports/ve_play_fingerprint_dedupe_2026-05-09_2026-05-10.md`。

### [VE] 玩法 embedding 硬拦截与历史回填校准

- `ua_workflows/shared/db/video_enhancer.py`：新增 `creative_library.effect_one_liner_embedding` 与 `daily_creative_insights` 中的 embedding 重复证据 JSON 字段；新增 `apply_effect_embedding_duplicate_filter`，默认只对高置信玩法 embedding 重复做硬拦截（日内 0.95、跨日 0.96），并记录命中来源、相似度、阈值、匹配素材。
- `ua_workflows/shared/db/video_enhancer.py`：日内文本硬拦截默认阈值调到 0.94，老玩法文本阈值仍为 0.94；同义归一补充姿态/姿势、骑行/骑乘、美颜/修图、动画/视频，并保留少量明确签名（名人宝丽来合影、派对修图前后对比、母亲节手绘合影）。
- `ua_workflows/video_enhancer/pipeline.py` / `sync.py`：一键流程与独立同步都会在老玩法文本筛选后补跑 embedding 硬拦截，再做 `embedding重复候选` soft tag；新增 `EFFECT_EMBEDDING_DUP_FILTER_ENABLED=0` 可关闭。
- 已回填 2026-05-07～2026-05-10 历史 VE 分析 JSON 与本地 SQLite 证据字段，生成校准报告：`data/ve_effect_embedding_backfill_2026-05-07_2026-05-10.json`、`reports/ve_effect_embedding_backfill_2026-05-07_2026-05-10.md`。

## 2026-05-09

### [Arrow2] 企业微信周报：新玩法 / 新 Hook

- `ua_workflows/video_enhancer/analyze.py`：Arrow2 精简分析 prompt 新增 `【玩法描述】` / `【Hook描述】` 固定字段，解析结果写入 `play_one_liner` / `hook_one_liner`，并保留 `ad_one_liner` 供同步与周报使用。
- `ua_workflows/shared/db/arrow2.py`：`arrow2_daily_insights` 自动迁移新增 `play_one_liner`、`hook_one_liner` 两列；增量分析入库同步写入新字段。
- `ua_workflows/shared/push/wecom.py`：抽出企业微信 markdown 分段推送公共 helper，VE 多渠道推送改为复用该 helper。
- `ua_workflows/arrow2/push_weekly_wecom.py` / `scripts/run_arrow2_weekly_wecom.py`：新增 Arrow2 周报入口，默认统计昨日往前 7 天的「最新创意」，按同 `appid` 过去 28 天历史判断新玩法/新 Hook，支持 `--workflow`、`--lookback-days`、`--dry-run`、`--no-llm`，Webhook 读取 `ARROW2_WECOM_BOT_WEBHOOK` 或 `WECOM_BOT_WEBHOOK`。

### [VE] 同步前老玩法/成人风险拦截 + 单产品硬截断默认关闭

- `ua_workflows/video_enhancer/content_filters.py`：新增成人/色情风险文本拦截，综合 `analysis`、标题、正文、标签、`effect_one_liner` 等字段判断；命中后设置 `exclude_from_bitable` / `exclude_from_cluster`，避免仅因「核心卖点」没写出风险而漏筛。
- `ua_workflows/shared/db/video_enhancer.py`：新增 `apply_old_effect_bitable_filter` 与 `apply_intraday_effect_bitable_filter`；同步前按同 `appid` 近 7 日历史玩法标记「老玩法重复」（主表拦截阈值默认 `OLD_EFFECT_SIMILARITY_THRESHOLD=0.94`，比日报新玩法口径更保守），同日同批次相似玩法仅保留展示估值更高的代表素材并标记「日内玩法重复」（默认 `INTRADAY_EFFECT_SIMILARITY_THRESHOLD=0.94`）。`normalize_effect_one_liner` 补入少量同义词归一（如 名人/明星、合照/合影、宝丽来/拍立得、姿态/姿势、骑行/骑乘、修脸/修图/修复/美颜、动画/视频）。
- `ua_workflows/shared/db/video_enhancer.py`：新增 `effect_one_liner_embedding` 存储与 `apply_effect_embedding_duplicate_filter`，对高置信同义玩法做硬拦截并记录阈值证据（默认日内 `EFFECT_EMBEDDING_INTRADAY_HARD_THRESHOLD=0.95`，跨日 `EFFECT_EMBEDDING_CROSSDAY_HARD_THRESHOLD=0.96`）；`apply_embedding_duplicate_candidate_tags` 对未被硬拦截的素材做 soft candidate 标记，默认 `EMBEDDING_DUP_CANDIDATE_THRESHOLD=0.90`，只加 `embedding重复候选` 标签和 `embedding_duplicate_candidate` 详情。
- `ua_workflows/video_enhancer/pipeline.py` / `sync.py`：一键流程与单独同步均补跑成人风险、日内玩法、老玩法、embedding 硬拦截、embedding 候选、已投放等标记，保证独立同步不绕过筛选；可用 `INTRADAY_EFFECT_FILTER_ENABLED=0` / `OLD_EFFECT_BITABLE_FILTER_ENABLED=0` / `EFFECT_EMBEDDING_DUP_FILTER_ENABLED=0` / `EMBEDDING_DUP_CANDIDATE_ENABLED=0` 分别关闭；`sync.py` 同步主表时会把 analysis `material_tags` 合并进「素材标签」字段。
- `ua_workflows/video_enhancer/analyze.py`：复核并收紧 VE prompt：`核心卖点` 要求补充可区分场景/对象/风格/呈现方式，避免「AI修图/AI美颜」过泛导致误去重；新增固定 `【风险标签】` 行（成人色情/擦边露肤/版权名人/产品不适配/低质/无明显风险）和 `【风险等级】` 行（低/中/高），标签解析进 `material_tags` 供多维表「素材标签」复核，等级写入单独「风险等级」列，其中成人色情仍由内容过滤硬拦。
- `ua_workflows/video_enhancer/analyze.py` / `sync.py`：VE prompt 新增固定 `【Hook解析】` 与 `【脚本口播】` 输出，分析正文要求更侧重前 1~3 秒 Hook 与脚本/字幕/口播提炼，减少逐帧冗余；若广大大 raw/detail 后续带 `script` / `transcript` / `subtitle` / `ocr_text` 等字段，会一并喂给模型。同步主表自动新增并写入「Hook解析」「脚本/口播」「风险等级」三列。
- `ua_workflows/video_enhancer/crawl.py`：单产品 `>10` 只保留 Top10 的硬截断默认关闭，改为保留日期命中素材，后续交给封面去重/玩法筛选/同步前排除处理；如需临时恢复可设 `VIDEO_ENHANCER_PER_PRODUCT_TRUNCATE_ENABLED=1`。

### [VE] 新素材 / 新玩法 / 持续发力日报口径落地

- `ua_workflows/shared/db/video_enhancer.py`：新增 `load_daily_material_report` 公共聚合口径，仅用于 **Video Enhancer**：新素材要求 `creative_library.first_target_date = target_date` 且同 `appid` 下 `effect_one_liner` 过去 N 日（默认 7）无精确或相似命中；老玩法换素材仅进 `old_play_items` 复核，不计入新素材；持续发力聚合 VE 封面/URL/ahash/玩法跨日信号，并按产品限制展示条数，不读取 Arrow2 报告。
- `ua_workflows/video_enhancer/push_feishu.py` / `push_multichannel.py`：日报标题改为「新素材 / 新玩法 / 持续发力」三指标；老玩法换素材在标题中展示「未计入新素材」数量，不再混入新素材列表；持续发力小节开始展示跨日重复素材/玩法。
- `ua_workflows/video_enhancer/acceptance.py`：验收新增日报素材报告摘要，检查新素材数、`effect_one_liner` 覆盖率（`ACCEPTANCE_MIN_EFFECT_COVERAGE`，默认 0.9）和持续发力信号生成情况。

## 2026-05-08

### [Arrow2] 修复 pipeline 封面去重 import（cron Step3 崩溃）

- `ua_workflows/arrow2/pipeline.py`：旧名 `arrow2_cover_style_intraday` 已改为 `from ua_workflows.arrow2.cover_dedupe import apply_arrow2_cover_style_dedupe`，避免 `run_arrow2_latest.py` 在 `arrow2_creative_library` 写入后 `ModuleNotFoundError`。

## 2026-05-07

### [运维] macOS crontab 推荐入口（避开工位上其它 7–10 点定时任务）

- 新增可执行脚本：`scripts/cron_ai_video_enhancer_daily.sh`、`scripts/cron_ai_arrow2_latest_daily.sh`、`scripts/cron_ai_arrow2_exposure_wed_sat.sh`（内含 `TZ=Asia/Shanghai`、`PYTHONUNBUFFERED=1`，日志请由 crontab 重定向到 `logs/`）。
- 与用户本机 `crontab -l` 错峰：`05:20` 跑 VE、`11:10` 跑 Arrow2 昨日最新、`14:20` 仅在周三/六跑 Arrow2 展示估值（与每日两条**叠加**）。
- 三条均要求：分析 + 同步（VE 为 `run_video_enhancer.py` 默认全流程；Arrow2 脚本传 `--analyze`）。实际路径以本机仓库根目录为准。
- 已在当前登录用户的 `crontab` 写入标记块 `# BEGIN ai- ua_workflows` … `# END ai- ua_workflows`（若改仓库路径须同步改这三行）。
- **`docs/` 运维文档**：`cron-schedules.md`、`workflows.md`、`setup-and-data.md`、`docs/README.md` 索引；根 `README.md` 已增加「项目文档」表链出。

## 2026-05-06

### [项目结构] 三工作流包化重构

- 项目已重构为围绕三条生产工作流维护：**Video Enhancer**、**Arrow2 latest_yesterday**、**Arrow2 exposure_top10**。
- 核心代码迁入 `ua_workflows/`：
  - `ua_workflows/video_enhancer/`：VE 抓取、DOM 补全、分析、封面去重、同步、推送、验收。
  - `ua_workflows/arrow2/`：Arrow2 pipeline、detail-v2 爬虫、封面去重、飞书同步。
  - `ua_workflows/shared/`：广大大、LLM、媒体解析、SQLite、路径配置等共享基础设施。
- `scripts/` 只保留三个整合入口：
  - `scripts/run_video_enhancer.py`
  - `scripts/run_arrow2_latest.py`
  - `scripts/run_arrow2_exposure.py`
- 另保留三个爬取冒烟测试入口，只跑一个产品、只落 raw JSON、不入库/不同步，且支持 `--headed` / `--headless`：
  - `scripts/test_video_enhancer_crawl.py`
  - `scripts/test_arrow2_latest_crawl.py`
  - `scripts/test_arrow2_exposure_crawl.py`
- 旧 UA、hot rank/new rank、playable、调试探针、一次性 backfill/preview/test 脚本已归档到 `archive/removed_scripts_2026_05_06/`。
- 后续新增或修改功能时，优先在 `ua_workflows/` 内按工作流归属维护；`scripts/` 只放整合入口或无副作用的冒烟测试入口。

## 项目结构原则

本项目围绕**三条生产工作流**展开，所有变更记录和汇报均按工作流分别组织：


| 工作流                | 简称              | 核心脚本                             | 用途                              |
| ------------------ | --------------- | -------------------------------- | ------------------------------- |
| **Video Enhancer** | VE              | `scripts/run_video_enhancer.py`  | 广大大竞品素材抓取 + 灵感分析 + 日报推送         |
| **Arrow2 最新**      | Arrow2 latest   | `scripts/run_arrow2_latest.py`   | 广大大 Arrow2 竞品每日最新抓取 + 可选分析 + 同步 |
| **Arrow2 展示估值**    | Arrow2 exposure | `scripts/run_arrow2_exposure.py` | 广大大 Arrow2 竞品展示估值抓取 + 可选分析 + 同步 |


- **变更日志按工作流分组**：每条记录标注所属工作流（VE / Arrow2 latest / Arrow2 exposure）；若同时涉及多条，分别描述。
- **验收报告不追踪**：`reports/` 下的 `*_acceptance.md` 为工作流自动产物，不需要在变更日志中追踪或记录。
- **共享基础设施**（如 `llm_client.py`、`video_enhancer_pipeline_db.py`、`cover_style_intraday.py`）变更时注明影响的工作流。

---

## 2026-04-29

### [Arrow2] 周趋势推送 + 数据补齐

- `**scripts/arrow2_weekly_trend.py`**（新增）：从 `arrow2_daily_insights` 汇总指定日期范围内素材的一句话描述，让 LLM 生成趋势分析报告，分两条飞书卡片推送（新素材趋势 / 展示估值趋势）。支持 `--workflow` 指定只推某一条、`--dry-run` 只打印不推送。
- 补入库 2026-04-27 最新创意 41 条（此前中断后未入库），重新同步飞书多维表（122 条）。
- 运行 2026-04-28 每日最新全流程（38 条入库 + 同步飞书）。

---

## 2026-04-28

### [Arrow2] 爬虫统一替换 + 工作流对齐

详见「当前 Arrow2 工作流」章节。本次变更要点：

- 爬虫统一为 `test_arrow2_first_card_fields.py`（detail-v2 逐张点击 + appid 精确匹配 + 多重重试）
- ad_key 去重改为新数据覆盖旧数据
- 展示估值每产品限 20 条
- 修复 `sync_arrow2_to_bitable.py` 的 import 和附件上传
- 新增 `guangdada_detail_url.py`（SPA 链接构建）

---

## 2026-04-15

### [Arrow2] `first_seen` 早于目标日则提前停滚、停 DOM 点卡

- `**run_search_workflow.py`**：新增 `_oldest_first_seen_ymd_among_creatives`；`**_search_one_keyword`** 可选 `stop_scroll_if_oldest_first_seen_before_ymd`——napi 已合并行中最早 `first_seen`（北京日）严格早于该 ISO 日（与「仅该日 `first_seen`」目标一致）时不再向下滚。`**_click_cards_for_details**` 可选 `stop_after_detail_first_seen_before_ymd`：某次详情里 `first_seen` 早于该日则关弹层后不再点后续卡（新→旧）。`**run_arrow2_batch` → `_collect_keyword_crawl_result_arrow2_latest_dom`**：在 `filter_yesterday_only` 的 latest+DOM 路径上传入 `first_seen_ymd`。环境变量 `**ARROW2_FIRST_SEEN_EARLY_STOP=0`** 关闭上述早停（默认开）。
- `**run_search_workflow.py**`：`_merge_prefer_dom_detail` 改为按 **展示估值工作流入库口径** 合并：latest+DOM 时，`**impression` / `all_exposure_value` / `heat` / `days_count` / `new_week_exposure_value` 固定优先取同 `ad_key` 的 napi 行**（与 exposure_top10 直接用列表 creative 入库一致）；detail 继续用于补正文、素材链接、首末次时间等详情字段。`preview_img_url`、`platform`、`advertiser_name`、`resource_urls` 等基础字段若 detail 缺失仍会回填，避免性能指标被 detail 覆盖或丢失。
- `**test_arrow2_competitors.py`**：补传 `keyword_product` 到 `run_arrow2_batch`。此前终端 `print_arrow2_matched_creatives` 使用的 `all_creatives` 在 latest_yesterday 调试场景里只过了日期筛选、**未走 keyword→广告主匹配**，会短暂打印出 `Arrow Maze` / `Arrows Out` 等相邻广告主；而 raw 落盘前又在脚本里二次 `advertiser_matches_product`，导致“终端条数”和“raw 条数”不一致。现已统一为：**终端打印 / run 返回 / raw 落盘**都使用同一广告主过滤口径。
- `**config/arrow2_competitor.json` + `run_search_workflow.py`**：`latest_yesterday` 去掉 `max_creatives_per_keyword: 30`，改为**默认不按每词 30 条截断**；同时 `_collect_keyword_crawl_result_arrow2_latest_dom` 在未显式设置环境变量 `**ARROW2_DOM_CLICK_MAX_CARDS`** 时，默认把**当前 DOM 已加载的卡片全部点击**（而不是固定 100 张内或更小 cap）。若后续想限条，显式设该环境变量为正整数即可。
- `**run_search_workflow.py`**：按最新确认口径调整 latest+DOM 点卡停止条件。`_collect_keyword_crawl_result_arrow2_latest_dom` 不再把 napi 中“已出现早于目标日”作为**点卡阶段**的提前停止信号；改为：**先从页首逐张点击当前已加载卡片**，只有 `_click_cards_for_details` 在真正点开的某张详情里发现 `first_seen < target_date` 时，才停止后续点卡。并为单张 click 增加 **1 次重试**，降低“页面上有卡但本次没点开/没拦到 detail”导致的漏点。
- `**test_arrow2_competitors.py`**：新增 `**--pause-per-product`**，主测试脚本现已统一支持：单产品（`--products ...`）、全产品（`--all-products`）、逐词暂停（`--debug-step-products`）、逐产品暂停（`--pause-per-product`）。开启 `--pause-per-product` 时自动等同 `--debug`，并默认关闭流程末尾额外暂停。
- `**test_arrow2_yesterday_all_products_headed.py**`：不再维护独立实现，改为**兼容包装器**，内部直接转调：`test_arrow2_competitors.py --all-products --pull-only latest_yesterday --debug --pause-per-product`。后续单/全产品测试请优先使用主脚本。
- `**run_search_workflow.py`（再调整）**：根据用户最新要求，`latest_yesterday` 已重新切回**卡片点击主路径**：`run_arrow2_batch` 对 `latest_yesterday` 调 `_collect_keyword_crawl_result_arrow2_latest_dom`，搜索/滚动后对**当前页面 DOM 卡片逐张点击**拿 detail；最终 `all_creatives` 以 **DOM 基础卡片 + detail 覆盖结果** 为主，不再把 `napi creative_list` 作为 latest_yesterday 的最终主结果源。`napi` 仍用于列表加载/滚动与兜底回退。调试输出新增：广告主匹配后全部卡、按 `first_seen=target_date` 筛后卡，以及逐张点击日志（含 `UTC+8` 时间）。

---

## 文档与代码对齐说明（2026-04-13 修订）

以下历史段落曾描述**已变更**行为，以本说明为准：


| 主题                        | 原文档说法                                                                               | 当前代码事实                                                                                                                                                                                                                                                                                                                                                                                                             |
| ------------------------- | ----------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **灵感「套路」筛选**              | 同一次多模态输出 `flower_background` / `bw_blockbuster` 等，或 `config/style_filters.json` 配置化 | **已移除**。`analyze_video_from_raw_json.py` 仅输出纯文本灵感（或解析 JSON 中的 `analysis`）。`style_filter_match_summary` 等字段可仍为列占位，恒为空。**「我方已投」**由 `launched_effects_db.apply_launched_effects_filter`（主流程 Step 2.9）及 `sync_raw_analysis_to_bitable_and_push_card.py` 补标处理。                                                                                                                                                          |
| **一键主流程 Step 2c 多维去重进分析** | `get_deduped_items_for_analysis` → `*_dedup_report.json`，仅去重后子集进分析                  | `**workflow_video_enhancer_full_pipeline.py` 未调用**该函数。入库仍用全量 `items`；分析队列为 **准入 + 未命中历史成功缓存** 的 `pending_items`（见 `[step:analysis-queue]`）。`get_deduped_items_for_analysis` 仍保留在 `video_enhancer_pipeline_db.py`（供封面指纹等复用逻辑），**不等同于当前一键分析入队口径**。                                                                                                                                                                   |
| **文案去重**                  | `text_fingerprint` SHA1 相同归组                                                        | **已关闭**。`TEXT_FINGERPRINT_DEDUP_ENABLED` 默认 `0`；当前只保留封面图去重 + effect_one_liner 去重。                                                                                                                                                                                                                                                                                                                                  |
| **UA 建议**                 | `_build_single_ua_suggestion` → `ua_suggestion_single` + `ad_one_liner`             | **已移除**（2026-04-29）。VE 流程不再生成单条 UA 建议；DB 列 `insight_ua_suggestion` 保留但不再写入。                                                                                                                                                                                                                                                                                                                                        |
| **推送格式**                  | 方向卡片（`_render_card_markdown`）                                                       | **已替换为新日报格式**（2026-04-29）。标题「AI工具竞品日报 + 日期」；按产品分组；每条新素材用 effect_one_liner 做可点击链接。                                                                                                                                                                                                                                                                                                                                  |
| **定时任务**                  | 每天 10:30 crontab                                                                    | `**daily_video_enhancer_workflow.sh` / `daily_ua_job.sh` 注释为手动执行**；若本机仍挂 crontab 为旧配置，以实际 shell 为准。                                                                                                                                                                                                                                                                                                                |
| **OpenRouter 用量**         | 仅 shell 内 `curl`                                                                    | **主流程**在 `workflow_video_enhancer_full_pipeline.py` 内调用 `llm_client.print_openrouter_key_meter`（工作流开始/结束）；与 `.env` 中 `OPENROUTER_METER` 等一致。                                                                                                                                                                                                                                                                       |
| **特效库语义阈值**               | 文档某处写默认 0.80                                                                        | `**launched_effects_db.py` 中默认 `LAUNCHED_EFFECTS_MATCH_THRESHOLD` 为 0.65**（以代码与环境变量为准）。                                                                                                                                                                                                                                                                                                                            |
| **封面日内 / 跨日向量去重**         | 多模态抽封面 + 文本 LLM 聚类；跨日仅载入「昨日」`insight_cover_style` 与今日同 appid 比较                     | **CLIP**（`clip-ViT-B-32`）封面向量并查集，`cosine ≥ COVER_VISUAL_DEDUP_THRESHOLD`（默认 **0.75**），无 LLM。跨日参照为 `**target_date` 之前连续 N 个日历日**（默认 **7**，`COVER_STYLE_HISTORY_LOOKBACK_DAYS`，上限 60），`load_cover_style_rows_for_dates_grouped_by_appid` 批量读库；簇内历史胜出时 `reason` 仍为 `**cover_style_cluster_vs_yesterday`**（兼容旧筛选）。指纹层不变：`COVER_STYLE_CROSS_DAY_FINGERPRINT_ENABLED` + `crossday_filter_items_against_creative_library`。 |


---

## 2026-03-31

### [VE] `analyze_video_from_raw_json`：格式异常时多模态重试 3 次，纯文本 JSON 修复改默认关

- **新**：`VIDEO_ANALYSIS_MULTIMODAL_FORMAT_RETRIES`（默认 3，0=关）—— 解析/正文过短等触发 `_needs_json_or_format_repair` 时，**串行**再调多模态（视频/图），不叠加 `PARALLEL_SHARDS`；成功打标 `inspiration_enrichment=multimodal_format_retry`。
- **改**：`VIDEO_ANALYSIS_JSON_REPAIR` 默认由开改为**关**；仅多模态仍失败时可选开启。纯文本收束**去掉 16k 截断**（全量原文进兜底 prompt）。
- 模块头 docstring 已同步说明。

### [VE] 飞书主表 `视频` 附件 + 恢复 `launched_effects_db.py`（我方已投 + embedding）

- `**sync_raw_analysis_to_bitable_and_push_card.py`**：`FIELD_DEFS` 增「视频」(type=17)；对 `video_duration>0` 且可直链下载的 `pick_video_url` 拉流上传（`VIDEO_BITABLE_MAX_MB` / `VIDEO_BITABLE_UPLOAD`）；主表同步前对 `analysis["results"]` 调用 `apply_launched_effects_filter`（与全链路 Step 2.9 一致）。
- `**launched_effects_db.py`**：仓库中曾为空导致 Step 2.9 未生效；已补全：飞书已投放表拉取 + 本地缓存 + 关键词子串 + `**llm_client.call_embedding` 语义 cosine ≥ `LAUNCHED_EFFECTS_MATCH_THRESHOLD`（默认 0.65）**；无飞书时降级 `data/launched_effects_descriptions_only.json`。
- `**workflow_video_enhancer_acceptance.py`**：已补全实现；`run_acceptance_after_workflow` 汇总 raw/analysis/方向卡片/封面与已投放 step/推送表行数/历史截断量，写 `data/workflow_video_enhancer_{date}_acceptance.json` 与 `reports/…_acceptance.md`；环境变量与 AGENTS 中「工作流验收」说明一致。未增加 `daily_video_enhancer_acceptance` 表（文档曾提及，当前以文件落盘 + 既有 DB 查询为主）。

---

## 2026-04-20

### [Arrow2] `latest_yesterday`：滚动越过「昨日」边界 + 竞品配置

- `**run_search_workflow._search_one_keyword`**：可选 `scroll_until_older_than_date`（目标昨日 YYYY-MM-DD）。在「最新创意」下持续滚动直到底层 napi 合并结果里出现 first_seen 或 created_at（UTC+8）早于该日 的素材（认为已扫完目标日窗口），或仍受「连续 3 轮无新批次」与**约 48 轮**上限约束。之后 `filter_yesterday_only` 仍只保留 `first_seen=昨日`。
- `**run_arrow2_batch`**：对 `order_by=latest` 且 `filter_yesterday_only` 的 pull_spec，默认开启上述滚动（`scroll_until_past_target_date` 缺省为真）；显式设为 `false` 可恢复较短轮数 + idle 的旧停法。
- `**config/arrow2_competitor.json`**：移除 `com.arrow.out` 行；`latest_yesterday` 中说明 `scroll_until_past_target_date`。
- `**test_arrow2_competitors.py`**：未指定 `--products` 时默认只跑配置中**第一个**产品；`--all-products` 跑全部。
- **Arrow2 `order_by=latest`（含 latest_yesterday）**：与 `exposure` 一样改为 `**arrow2_build_result_from_dom_after_search`**，列表为 **DOM 卡片 + detail-v2**（`list_source=dom`）；`filter_yesterday_only` 时单词默认多取卡（`max_n` 默认 120、硬顶 200）再按 first_seen 筛昨日。旧无 `pull_specs` 矩阵里 `latest` 亦走 DOM。
- **DOM 广告主过滤**：`run_arrow2_batch` 增加 `keyword_product`（搜索框 key → 与 config `match` 一致的产品名）；`arrow2_build_result_from_dom_after_search` 在 detail-v2 后调用 `advertiser_matches_product` 剔除与目标产品不一致的卡。`test_arrow2_competitors` 自动传入各条目的 `product`。

---

## 2026-04-14

### [VE] 封面去重改为 CLIP 向量 + 7 日历史窗口

**动机**：去掉封面多模态与封面聚类 LLM，改为与主库一致的 **CLIP 封面向量**（`creative_library.cover_embedding`），阈值可调、成本更低。

**实现概要**（`scripts/cover_style_intraday.py`）：

- **指纹**（默认开，`COVER_STYLE_CROSS_DAY_FINGERPRINT_ENABLED`）：在算 CLIP 前仍调用 `crossday_filter_items_against_creative_library`，与文首「封面跨日指纹」一致。
- **向量**：优先读库 `load_cover_embedding_blob_map_by_ad_keys`；缺失则 `cover_embedding.compute_cover_embedding_vector_from_url` 仅算向量、不写库（当日首轮入库前新 `ad_key` 可能无行；后续 `run_cover_embedding_job` 等仍会补 `cover_embedding`）。
- **占位入库**：`insight_cover_style` 写入 CLIP 占位 JSON（如 `style_type`: 「CLIP视觉」），`upsert_single_cover_style_insight` 逐条更新。
- **聚类**：同 `appid` 内并查集，边条件 `cosine_similarity ≥` `**COVER_VISUAL_DEDUP_THRESHOLD`**（默认 0.75）；簇内保留 `all_exposure_value` 最大一条；若最优来自**历史窗口**则剔今日（`cover_style_cluster_vs_yesterday`），若最优为今日则日内互斥（`cover_style_cluster`）。报告 `cover_dedupe_mode`: `**clip_visual`**。
- **历史窗口**：`COVER_STYLE_CROSS_DAY_ENABLED` 开启时，加载 `**target_date` 前连续 N 日**（`**COVER_STYLE_HISTORY_LOOKBACK_DAYS`**，默认 **7**，即 T-1…T-7）内非空 `insight_cover_style`；`video_enhancer_pipeline_db.load_cover_style_rows_for_dates_grouped_by_appid` 合并多日，同一 `ad_key` 取 exposure 更高的一条。报告含 `cross_day_history_dates`、`cross_day_history_lookback_days`；`cross_day_prev_date` 仍为 **T-1**（兼容旧读者）；`per_appid` 增加 `history_ref_count`，并保留 `yesterday_ref_count` 与同长度（兼容）。

**环境变量**（`.env.example` 已列）：`COVER_VISUAL_DEDUP_THRESHOLD`、`COVER_STYLE_HISTORY_LOOKBACK_DAYS`；其余 `COVER_STYLE_INTRADAY_ENABLED`、`COVER_STYLE_CROSS_DAY_*`、`COVER_STYLE_WORKERS` 仍适用。

**联动**：`workflow_video_enhancer_full_pipeline.py` / `workflow_video_enhancer_steps.py` / `daily_video_enhancer_workflow.sh` / `run_cover_style_intraday.py` / `workflow_video_enhancer_acceptance.py` 中用户可见文案已由「多模态封面」改为 **CLIP 封面**。

**注意**：历史窗口内条目需库内已有 `**cover_embedding`** 才参与向量聚类；过去未跑封面或未回填向量时，参照会变弱。

---

## 2026-04-13

### [VE] 工作流验收：`workflow_video_enhancer_acceptance.py`

- **用途**：对指定日期的 Video Enhancer 产物做可重复检查（文件存在与 JSON 结构、本次新分析成功率阈值、封面/语义等异常提示、方向卡片字段与参考链接、推送表行数、`daily_video_enhancer_workflow_{date}.log` 中子进程错误线索、近 N 日截断后条数对比）。
- **输出**：`data/workflow_video_enhancer_{date}_acceptance.json`、`reports/workflow_video_enhancer_{date}_acceptance.md`。
- **SQLite**：`daily_video_enhancer_acceptance` 表（`init_db()` 自动补齐），`upsert_daily_video_enhancer_acceptance` 写入；另增 `query_filter_log_post_total`、`count_daily_ua_push_rows`、`load_filter_log_post_totals_lookback` 供验收与历史对比。
- **接入**：`workflow_video_enhancer_full_pipeline.py` 在「全流程正常结束」或「灵感分析失败提前 return」前调用 `run_acceptance_after_workflow`；`workflow_video_enhancer_steps.py` 的 `push_sync` 结束调用。
- **环境变量**：`ACCEPTANCE_ENABLED`（默认开启，设为 `0`/`false` 关闭）、`ACCEPTANCE_BLOCK_ON_FAIL`（硬失败时进程退出码 2）、`ACCEPTANCE_MIN_SUCCESS_RATE`、`ACCEPTANCE_COVER_REMOVAL_WARN`、`ACCEPTANCE_LOOKBACK_DAYS`、`ACCEPTANCE_LOW_VS_MEAN`、`ACCEPTANCE_HIGH_VS_MEAN`、`ACCEPTANCE_EXIT_ON_SOFT`、`ACCEPTANCE_STRICT`（详见脚本文件头注释）。
- **飞书通知**：配置 `ACCEPTANCE_FEISHU_WEBHOOK`（群机器人 webhook，可与业务 UA 卡片 webhook 分开）后，每次验收结束发送 **interactive 卡片**（紧凑摘要：状态/得分/问题/阶段一行摘要）；`ACCEPTANCE_FEISHU_ENABLED=0` 关闭；`ACCEPTANCE_FEISHU_STRICT=1` 时推送失败会抛错。CLI：`--feishu-webhook`、`--no-feishu`。

---

## 2026-04-09

### [VE] 逐条主表同步：`sync_raw_analysis_to_bitable_and_push_card.py`

- **我方已投放**：在 `main()` 读入 analysis 后**先**调用 `apply_launched_effects_filter`，单独跑同步脚本也会排除已投放命中（不再依赖必须先跑 `cluster_store`）。`apply_launched_effects_filter` 对历史 JSON 中已有 `launched_effect_match` / 「我方已投放」标签但未写 `exclude_from_bitable` 的行**补写**主表排除。
- **封面图附件**：路径以 `.image` 结尾时，下载**优先**用 `.png` 替换后的 URL；`upload_image_as_attachment` 中飞书附件 `file_name` 对 `.image` 后缀**强制改为** `.png`，避免多维表里仍显示 `.image`。
- **视频**：仅「视频链接」文本列，**无**视频附件列。

---

## 2026-04-08

### [VE] `llm_client.call_vision` 文本降级时剥离媒体 URL

**问题**：部分图片素材在多模态与纯文本路径均被 OpenRouter 返回 403（TOS）；根因之一是视觉失败后 `call_text` 仍携带与请求相同的图片/视频直链，网关对同一 URL 再次拦截。

**改动**（`scripts/llm_client.py`）：新增 `_text_fallback_user_text`，在「全部视觉模型失败 → 纯文本降级」时，将 `user_text` 中的 `media_url` 替换为简短中文说明，避免重复提交直链；分析可仅依据标题、文案与指标推断。

---

## 2026-04-03

### [VE] 我方已投放特效库匹配 `scripts/launched_effects_db.py`（新增）

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

### [VE] 架构优化：五项改进（统一 LLM 层 / 配置化套路已废弃 / 语义嵌入 / 历史卡片 / 趋势信号）

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

**当前状态（2026-05-15）**：本节所述 `analysis_embedding` / `semantic_crossday_filter` 已下线，代码不再创建、写入或读取 `creative_library.analysis_embedding`；玩法相似判断改由 `play_fingerprint` / `effect_one_liner` 文本与 `effect_one_liner_embedding` 链路承担。以下内容仅作历史背景。

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

> 2026-05-15 已下线旧 `analysis_embedding` 语义去重，保留此段仅作历史记录。

- **原因**：`semantic_crossday_filter()` 仅定义未调用，文档描述与实际行为不一致。
- **修复**：在全流程 Step 2.7（嵌入存储）之后新增 Step 2.8：对 `combined_results` 做语义比对，同 appid 内 cosine similarity ≥ 0.92 的素材设 `exclude_from_cluster=True`，下游 `generate_video_enhancer_ua_suggestions_from_analysis.py` 自动跳过（复用已有排除机制）。标记数和匹配详情会打印到终端并回写 analysis JSON。

#### 5) 趋势信号利用

**背景**：`creative_library` 已有 `first_target_date`/`appearance_count` 等生命周期数据，但未被利用。

**变更**：

- `video_enhancer_pipeline_db.py` 新增 `compute_trend_signals(target_date, lookback_days=14)` — 计算各产品本周 vs 上周新素材数、趋势方向（rising/declining/stable）。
- `generate_video_enhancer_ua_suggestions_from_analysis.py` `_build_prompt()` 注入趋势段落，要求方向卡片中引用「该类素材本周明显增多/减少」等趋势表述。

---

### [VE] 爬取与封面去重拆分（分步工作流）

**背景**：将「抓取」与「封面日内去重（现为 CLIP，见 **§2026-04-14**）」解耦，便于独立执行或重跑。

**变更**：

- `scripts/workflow_video_enhancer_steps.py`：`crawl_store` 增加 `**--crawl-only`** — 只跑 `test_video_enhancer_two_competitors_318.py` 写 `workflow_video_enhancer_{日期}_raw.json`，**不**做封面去重、**不**写库；终端提示下一步执行 `cover_store`。
- 新增子命令 `**cover_store`**：读已有 raw →（若 `COVER_STYLE_INTRADAY_ENABLED` 开启）`apply_intraday_cover_style_dedupe` → 写回 raw 与 `*_cover_style_intraday.json` → `**prune_daily_creative_insights_not_in_raw`**（删除当日库里已不在 raw 中的 `ad_key`，避免先全量入库再缩条后残留行）→ `upsert_daily_creative_insights` / `upsert_creative_library` / `upsert_daily_video_enhancer_filter_log`。
- `scripts/video_enhancer_pipeline_db.py`：新增 `**prune_daily_creative_insights_not_in_raw**`。
- `scripts/workflow_video_enhancer_full_pipeline.py`：增加 `**--skip-cover-dedupe**` — 一键流程中跳过封面聚类块，抓取后直接用全量 raw 继续（与关闭封面类似，无需改环境变量）。
- 默认 `**crawl_store` 不带 `--crawl-only**` 时行为与改前一致（抓取 + 封面 + 入库）。分步编号在文件头注释中已更新为含 `cover_store`。

### [VE] 封面去重：与「昨日」同 appid 参照（跨日）

**（2026-04-14 修订）** 下文「昨日」在**产品语义**上已扩展为 **过去 N 日历史窗口**（默认 7 日），实现为 CLIP 向量并查集而非 LLM；详见 **§2026-04-14** 与文首对齐表「封面日内 / 跨日向量去重」。

**背景**：封面聚类原先仅对比**当日**同产品；现增加**历史**已入库的 `insight_cover_style`（及库内 `cover_embedding`）作为参照，减少与近几日视觉重复的今日素材。

**逻辑**（`scripts/cover_style_intraday.py`）：

- 从 DB 读取 `**target_date` 之前连续 N 日**（默认 **7**）各 `appid` 下非空 `insight_cover_style`（`load_cover_style_rows_for_dates_grouped_by_appid`；单日查询仍可用 `load_cover_style_rows_for_date_grouped_by_appid`），与今日条目一并进入 **CLIP 余弦聚类**。
- 簇内保留 `**all_exposure_value` 最高**的一条（历史、今日同一比较口径）。若历史侧 `ad_key` 胜出，今日同簇素材剔除，`removed` 中 `**reason`** 仍为 `**cover_style_cluster_vs_yesterday**`；若今日胜出，逻辑为 `**cover_style_cluster**`。
- 报告字段：`cross_day_history_dates`、`cross_day_rows_loaded` 等；`per_appid` 含 `history_ref_count` / `yesterday_ref_count`。
- 环境变量 `**COVER_STYLE_CROSS_DAY_ENABLED**`（默认开启；`0`/`false`/`no`/`off` 关闭历史向量参照，仅日内 CLIP）；`**COVER_STYLE_HISTORY_LOOKBACK_DAYS**`（默认 7）。`.env.example` 已补充说明。

**监控关注点**：历史窗口依赖过去 N 日 `daily_creative_insights` 中已有封面占位且 `**creative_library.cover_embedding` 非空**；新日期或历史未跑封面时，该 appid 可能仅有今日数据，行为退化为日内逻辑。

### [VE] 封面跨日指纹（与 `creative_library` / 灵感分析 Step B 对齐）

**背景**：避免「主流程跨日已会剔除的素材」仍进入封面多模态；与 `get_deduped_items_for_analysis` 的 Step B 使用同一套比对逻辑。

**变更**：

- `scripts/video_enhancer_pipeline_db.py`：抽取 `**crossday_filter_items_against_creative_library(target_date, items)`**（同 appid 下对 `creative_library` 早于当日的记录比对 `ad_key` / 主媒体 URL / 封面 `image_ahash_md5` 汉明距离 ≤ 阈值）。`**get_deduped_items_for_analysis`** 的 Step B 改为调用该函数，避免重复实现。
- `scripts/cover_style_intraday.py`：在 `apply_intraday_cover_style_dedupe` 内、**CLIP 编码之前**默认执行上述过滤；命中则从本条列表剔除，不再进入后续封面向量步骤。报告含 `**cross_day_fingerprint_removed_count` / `cross_day_fingerprint_removed`**、`**input_count_before_cross_day_fingerprint`**；若过滤后无剩余素材，返回 `**empty_after_cross_day_fingerprint**`。
- 环境变量 `**COVER_STYLE_CROSS_DAY_FINGERPRINT_ENABLED**`（默认开启；`0`/`false`/`no`/`off` 关闭指纹层；**不**影响上文「历史 `insight_cover_style` + CLIP」跨日，后者仍由 `**COVER_STYLE_CROSS_DAY_ENABLED`** / `**COVER_STYLE_HISTORY_LOOKBACK_DAYS`** 控制）。
- `.env.example` 已补充 `COVER_STYLE_CROSS_DAY_FINGERPRINT_ENABLED` 说明。

**监控关注点**：指纹层依赖 `creative_library` 已有历史行（`first_target_date < 当日`）；冷启动或从未入库的竞品首日可能无命中，仅靠 **CLIP 历史窗口**参照兜底（见 **§2026-04-14**）。

---

## 2026-04-02

### [VE] OpenRouter 用量：工作流前后各查一次 Key

**变更**：主流程在 `workflow_video_enhancer_full_pipeline.py` 内调用 `**llm_client.print_openrouter_key_meter`**（工作流开始前 / 结束后各一次，需 `.env` 中 `OPENROUTER_API_KEY`）。默认关闭；设 `**OPENROUTER_METER=1`** 等开启（以 `llm_client` 内逻辑为准），输出经 `tee` 进入当日 `logs/daily_video_enhancer_workflow_${TARGET_DATE}.log`。独立脚本 `**scripts/openrouter_key_snapshot.sh**` 供单次手动对比（可能用 `curl`）。

### [VE] 终端耗时：灵感多模态 / 封面多模态

**变更**：`analyze_video_from_raw_json.py` 每条素材在 `_call_llm_video` / `_call_llm_image`（灵感分析）结束后打印 `灵感多模态耗时 X.Xs · [video|image]`。**（2026-04-14 起）** 封面步骤已改为 CLIP，**不再**打印「封面多模态 / 封面聚类 LLM」耗时；历史描述保留供对照。

---

### [VE] 封面风格日内：进度日志 + 逐条入库

**变更**：`scripts/cover_style_intraday.py` 每条封面处理前后打印进度，完成后调用 `video_enhancer_pipeline_db.upsert_single_cover_style_insight` 写入 `daily_creative_insights.insight_cover_style`（**2026-04-14 起** 为 CLIP 占位 JSON，非多模态描述）；`apply_intraday_cover_style_dedupe(..., crawl_date)` 第三参传入 `raw_payload["crawl_date"]`。`workflow_video_enhancer_full_pipeline.py` / `workflow_video_enhancer_steps.py` 已传参。

---

## 2026-03-31

### [VE] 灵感分析：花卉背景 / 黑白大片套路过滤（**已废弃**）

**历史逻辑**：曾与主灵感分析同一次多模态输出套路布尔字段；命中则打「我方已经投过」等。**该能力已移除**，详见文首「文档与代码对齐说明」。

**下游（仍适用）**：`sync_raw_analysis_to_bitable_and_push_card.py` 仍对 `exclude_from_bitable` 等字段做主表是否同步；`generate_video_enhancer_ua_suggestions_from_analysis.py` 仍对 `exclude_from_cluster` 做聚类排除（`cluster_excluded_count`）。**来源**以主流程 **Step 2.8 语义去重**、**Step 2.9 已投放特效库**、`material_tags` 补标为主，而非分析脚本内套路。

---

### [VE] 多模态封面日内去重默认开启

**变更**：`scripts/cover_style_intraday.py` 中 `is_cover_style_intraday_enabled()` 默认改为开启；通过 `COVER_STYLE_INTRADAY_ENABLED=0`（或 `false`/`no`/`off`）关闭。`scripts/daily_video_enhancer_workflow.sh` 中 `export COVER_STYLE_INTRADAY_ENABLED="${COVER_STYLE_INTRADAY_ENABLED:-1}"`，便于环境变量显式覆盖。`.env.example` 已补充说明。**（2026-04-14）** 实现已为 **CLIP 封面**，标题中「多模态」为历史命名。

---

## 2026-03-24

### [VE] 项目清理：废弃脚本归档到 `scripts/legacy/`

**操作**：将 50 个废弃/旧流程脚本从 `scripts/` 移入 `scripts/legacy/`，保留 31 个活跃脚本。

**保留在 `scripts/` 的活跃脚本分类：**


| 分类                 | 脚本                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Video Enhancer 主流程 | `workflow_video_enhancer_full_pipeline.py`, `workflow_video_enhancer_steps.py`, `test_video_enhancer_two_competitors_318.py`, `analyze_video_from_raw_json.py`, `generate_video_enhancer_ua_suggestions_from_analysis.py`, `sync_raw_analysis_to_bitable_and_push_card.py`, `push_video_enhancer_multichannel.py`, `push_video_enhancer_feishu_card_only.py`, `sync_video_enhancer_date_to_google_sheet.py`, `video_enhancer_pipeline_db.py` |
| 核心依赖               | `path_util.py`, `proxy_util.py`, `guangdada_login.py`, `run_search_workflow.py`, `workflow_guangdada_competitor_yesterday_creatives.py`, `guangdada_yesterday_creatives_db.py`                                                                                                                                                                                                                                                               |
| 定时任务               | `daily_ua_job.sh`, `daily_video_enhancer_workflow.sh`                                                                                                                                                                                                                                                                                                                                                                                        |
| 竞品热门榜流程            | `hot_rank_step1_crawl.py`, `hot_rank_step2_video_analysis.py`, `hot_rank_step3_cluster.py`, `hot_rank_step4_push_feishu.py`, `workflow_competitor_hot_rank.py`                                                                                                                                                                                                                                                                               |
| 其他次要流程             | `workflow_competitor_new_rank.py`, `compute_new_rank_diff.py`, `fetch_competitor_raw.py`, `fetch_competitor_new_creatives.py`, `fetch_dom_yesterday_creatives.py`, `sync_top3_competitor_by_heat_to_feishu.py`, `ua_crawl_db.py`, `competitor_hot_db.py`, `competitor_ua_db.py`                                                                                                                                                              |


**注意**：`workflow_guangdada_competitor_yesterday_creatives.py` 是主流程的核心依赖（被 `test_video_enhancer_two_competitors_318.py` import，提供 `_apply_relaunch_pipeline_tag`、`_creative_hits_target_date`、`advertiser_matches_product` 等工具函数）。

---

### [VE] 功能新增：图片素材灵感分析支持

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

`**_build_prompt` 重命名为 `_build_video_prompt**`（无逻辑改动）

`**main()` 逻辑变化**：

- 旧：无 `video_url` → `continue` 跳过
- 新：无 `video_url` → 尝试 `image_url`，两者都无才跳过
- 输出结果新增字段：`creative_type`（`"video"` / `"image"`）、`image_url`、`title`、`body`
- 输出汇总信息：`视频 N / 图片 N / 跳过 N`
- 输出 JSON 新增字段：`video_analyzed`、`image_analyzed`、`skipped`

---

#### `scripts/generate_video_enhancer_ua_suggestions_from_analysis.py`

`**_build_prompt()` 中 `source_by_adkey` 构建逻辑更新**：

- 新增 `creative_type` 字段传入 LLM
- 图片素材传 `image_url`，视频素材传 `video_url`
- 让 LLM 在生成方向卡片时能感知素材类型

---

#### `scripts/sync_raw_analysis_to_bitable_and_push_card.py`

`**adkey_to_video`（即 `video_by_ad`）映射构建逻辑更新**：

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

### [VE] 功能新增：素材主库 creative_library 与智能多维去重

**背景**：`daily_creative_insights` 是按日期快照存储的，同一条广告素材跨天重复出现时没有归一化处理，且无法判断"视觉相同但 ad_key 不同"的情况（如同图跨平台投放）。

**涉及文件**：

- `scripts/video_enhancer_pipeline_db.py`
- `scripts/workflow_video_enhancer_full_pipeline.py`

---

#### `scripts/video_enhancer_pipeline_db.py`

**新增表 `creative_library`**（跨天去重主库）：


| 字段                                        | 说明                                                       |
| ----------------------------------------- | -------------------------------------------------------- |
| `ad_key`                                  | 唯一标识，UNIQUE                                              |
| `dedup_group_id`                          | 去重组 ID，格式：`ahash_{hex8}` / `text_{fp8}` / `adkey_{key8}` |
| `canonical_ad_key`                        | 组内代表（热度/人气最高者）                                           |
| `is_canonical`                            | 1=本条是组代表                                                 |
| `image_ahash_md5`                         | 感知哈希（从 raw_json 提取，广大大预计算）                               |
| `text_fingerprint`                        | sha1(normalize(title+body))，用于文案去重                       |
| `creative_type`                           | `"video"` / `"image"`                                    |
| `best_heat/impression/all_exposure_value` | 历史最高热度指标（跨天取最大）                                          |
| `first_target_date` / `last_target_date`  | 首次/最近出现日期                                                |
| `appearance_count`                        | 跨天出现次数                                                   |
| `dedup_reason`                            | 去重原因：`new` / `ahash(dist=N)` / `text` / `exact`          |


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

### [VE] 功能新增：多维去重过滤（日内 + 跨日），只让新唯一素材进入分析

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

### [VE] Bug 修复 + 历史数据回填

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

## 当前 Arrow2 工作流

### 两条工作流概览


|           | 每日最新 (`latest_yesterday`) | 展示估值 (`exposure_top10`) |
| --------- | ------------------------- | ----------------------- |
| **目的**    | 跟踪竞品每日新素材                 | 跟踪竞品高曝光素材               |
| **排序**    | 最新创意                      | 展示估值                    |
| **搜索天数**  | 7                         | 30                      |
| **广告主筛选** | appid 精确匹配                | appid 精确匹配              |
| **每产品条数** | 不限（first_seen=昨日）         | 最多 20 条                 |
| **建议频率**  | 每天                        | 每周 2-3 次                |


### 全流程（两条工作流共用）

```
workflow_arrow2_full_pipeline.py --pull-only <pull_id> [--analyze]
 │
 ├─ Step 1  爬取
 │     test_arrow2_first_card_fields.py --pull-only <pull_id> --all-products
 │       │  统一爬虫：detail-v2 逐张点击 + ad_key 去重
 │       │  广告主筛选：advertiser_id == appid 精确匹配
 │       │  重试：detail-v2 获取重试 3 次 / 搜索无卡片重试 2 次 / Top 创意下拉框重试 3 次
 │       │
 │       ├─ latest_yesterday：滚动到 first_seen < target_date 早停 + 仅保留 first_seen=昨日
 │       └─ exposure_top10：appid 匹配计数满 20 即停
 │
 ├─ Step 2  入库
 │     upsert_arrow2_creative_library_batch → arrow2_creative_library（跨日去重主库）
 │     dedupe_arrow2_raw_items_by_ad_key → ad_key 去重（新数据覆盖旧数据）
 │
 ├─ Step 3  封面 CLIP 去重
 │     apply_arrow2_cover_style_dedupe
 │       ├─ 跨日指纹过滤：ad_key / URL / preview_img_url / ahash 汉明 ≤ 8
 │       └─ CLIP 向量聚类：cosine ≥ 0.75，7 日历史窗口，保留 exposure 最高者
 │
 ├─ Step 4  灵感分析（需 --analyze，默认跳过）
 │     analyze_video_from_raw_json.py --arrow2
 │       └─ 逐条入库 arrow2_daily_insights（含 ad_one_liner / material_tags 等）
 │
 └─ Step 5  飞书多维表同步
       sync_arrow2_to_bitable.py（raw + analysis → 飞书记录 + 附件上传）
```

### 数据库表


| 表                         | 用途                                            | 唯一键                     |
| ------------------------- | --------------------------------------------- | ----------------------- |
| `arrow2_creative_library` | 跨日去重主库（ahash/text 归组 + 封面向量 + 分析结果）           | `ad_key`                |
| `arrow2_daily_insights`   | 日快照（含灵感分析 + ad_one_liner + crawl_workflow 标签） | `(target_date, ad_key)` |


### 周趋势推送

`scripts/arrow2_weekly_trend.py`：从 `arrow2_daily_insights` 汇总指定日期范围内素材的一句话描述，让 LLM 生成趋势分析报告，分两条飞书卡片推送（新素材趋势 / 展示估值趋势）。

```bash
# 两个都推
python scripts/arrow2_weekly_trend.py --start 2026-04-27 --end 2026-04-28

# 只推新素材 / 展示估值
python scripts/arrow2_weekly_trend.py --workflow 最新创意
python scripts/arrow2_weekly_trend.py --workflow 展示估值
```

### Cron 设计方案

```crontab
# Arrow2 每日最新 —— 每天北京时间 08:00（UTC 00:00）
0 0 * * * cd /Users/oliver/guru/ua素材 && PYTHONPATH=scripts PYTHONUNBUFFERED=1 .venv/bin/python3 scripts/workflow_arrow2_full_pipeline.py --pull-only latest_yesterday --analyze >> logs/arrow2_latest_yesterday_$(date +\%Y-\%m-\%d).log 2>&1

# Arrow2 展示估值 —— 每周二、四、六北京时间 08:00（UTC 00:00）
0 0 * * 2,4,6 cd /Users/oliver/guru/ua素材 && PYTHONPATH=scripts PYTHONUNBUFFERED=1 .venv/bin/python3 scripts/workflow_arrow2_full_pipeline.py --pull-only exposure_top10 --analyze >> logs/arrow2_exposure_top10_$(date +\%Y-\%m-\%d).log 2>&1

# Arrow2 周趋势 —— 每周五北京时间 18:00（UTC 10:00）
0 10 * * 5 cd /Users/oliver/guru/ua素材 && PYTHONPATH=scripts PYTHONUNBUFFERED=1 .venv/bin/python3 scripts/arrow2_weekly_trend.py >> logs/arrow2_weekly_trend_$(date +\%Y-\%m-\%d).log 2>&1
```

### 关键脚本


| 脚本                                 | 用途                      |
| ---------------------------------- | ----------------------- |
| `workflow_arrow2_full_pipeline.py` | 一键全流程（爬取→入库→封面去重→分析→同步） |
| `test_arrow2_first_card_fields.py` | 统一爬虫（detail-v2 逐张点击）    |
| `arrow2_pipeline_db.py`            | SQLite 表管理 + 去重 + 入库    |
| `arrow2_cover_style_intraday.py`   | 封面 CLIP 去重              |
| `sync_arrow2_to_bitable.py`        | 飞书多维表同步 + 附件上传          |
| `arrow2_weekly_trend.py`           | 周趋势 LLM 总结 + 飞书卡片推送     |
| `daily_arrow2_workflow.sh`         | 每日最新 shell 入口           |
| `arrow2_exposure_workflow.sh`      | 展示估值 shell 入口           |


---

## 2026-04-29（[VE] 工作流重构）

### 特效玩法 `effect_one_liner` + 新素材 / 持续发力 + 日报推送

**背景**：VE 工作流增加「特效玩法」一句话描述，用于去重和推送；取消 UA 建议和 `ad_one_liner`；推送格式从旧的方向卡片改为新日报格式。

#### 1) 特效玩法 `effect_one_liner`

- `**analyze_video_from_raw_json.py`**：VE prompt footer 新增 `【特效玩法】` 行（约 10~20 字概括核心特效/玩法/创意卖点，如「圣诞华服换脸」「AI 肌肉编辑」）。
- `_strip_arrow2_footer_lines` 返回值新增第 5 项 `effect_one_liner`；VE 路径解析该行并写入输出。
- `**video_enhancer_pipeline_db.py`**：`daily_creative_insights` 和 `creative_library` 均新增 `effect_one_liner TEXT` 列；`upsert` 时写入。
- `**sync_raw_analysis_to_bitable_and_push_card.py`**：`FIELD_DEFS` 新增「特效玩法」字段（type=1）；同步时从 analysis 取 `effect_one_liner` 写入。

#### 2) 去重逻辑精简

- **文案去重已关闭**：`TEXT_FINGERPRINT_DEDUP_ENABLED` 默认 `0`（关闭）；`_allow_text_only_dedup` 不再参与 `upsert_creative_library` 归组。
- **当前只保留两个去重维度**：
  - **封面图去重**：CLIP 向量 + 7 日历史窗口（`cover_style_intraday.py`）+ 跨日指纹（`crossday_filter_items_against_creative_library`）
  - **一句话玩法去重**：`effect_one_liner` 精确匹配 + 跨 7 天历史（`effect_based_crossday_dedup`）

#### 3) 新素材与持续发力

- **新素材**：`creative_library.first_target_date = target_date` 且 `effect_one_liner` 在同 `appid` 近 7 日无精确/相似命中；老玩法换素材不计入新素材，仅作为 `old_play_items` 复核。
- **持续发力**：基于去重流程中被去掉的素材，汇总三个来源的持续发力信号（`compute_sustained_effort_signals`）：
  - **来源 1：封面跨日去掉**（`cover_style_intraday.json` 的 `cross_day_fingerprint_removed` + CLIP `vs_yesterday`）→ 同画面/URL 跨天重复
  - **来源 2：ahash 去重组跨天**（`creative_library` 查询）→ 同一画面换了 ad_key
  - **来源 3：effect_one_liner 跨天**（`creative_library` 查询）→ 同一玩法换了不同画面
- **追溯机制**：每个持续发力信号追溯到被匹配的历史素材的 `effect_one_liner`、产品名、媒体链接（视频/图片 URL），提供具体描述而非仅 `ad_key`。

#### 4) 日报推送格式（替代旧方向卡片）

**标题**：`AI工具竞品日报 {日期}`

**格式**：按竞品分组，每条新素材用 `effect_one_liner` 做可点击链接跳转到视频/图片：

```
**2026-04-28** | 新素材 **29** 条

**Remini**  ·  AI照片增强/修复/滤镜
🎬 [AI 照片接吻特效 (AI Kiss Effect)](视频链接)
🎬 [AI 静态照片生成拥抱视频特效](视频链接)
...

**Glam AI**  ·  AI美颜/换装/照片编辑
🎬 [自拍生成 AI 雨夜时尚大片](视频链接)
🖼 [xxx特效](图片链接)
...
```

- 🎬 = 视频素材，🖼 = 图片素材
- 链接指向视频 URL 或图片 URL
- 持续发力部分暂不展示（数据已就绪，可随时开启）

**推送通道统一**：

- **飞书卡片**：`push_video_enhancer_feishu_card_only.py`（交互式卡片）
- **企业微信**：`push_video_enhancer_multichannel.py`（markdown 分段推送，与飞书内容一致）
- **Google Sheet**：`push_video_enhancer_multichannel.py`（数据行 + 方向卡片 JSON 同步，格式不变）

#### 5) 移除 UA 建议

- `**analyze_video_from_raw_json.py`**：删除 `_build_single_ua_suggestion` 函数及调用；移除 `ua_suggestion_single` 和 `ad_one_liner` 从 VE 输出。
- `**workflow_video_enhancer_full_pipeline.py`**：移除 `ua_suggestion_single` 和 `ad_one_liner` 在 `analysis_by_ad` 字典中的传递。
- `**video_enhancer_pipeline_db.py`**：移除 `ad_one_liner` 在 `upsert_daily_creative_insight` 中的读取和写入（DB 列保留）。

#### 6) 分析重试机制

- **单条重试**：`_analyze_one_item` 中 LLM 调用失败（403/超时/空分析/格式错误）时最多重试 3 次（`_vision_call_retry_max`，2 秒间隔）。
- **二轮重试**：初始分析全部完成后，对仍失败/为空的条目再做一轮统一重试。
- **成功率阈值**：若分析成功率 < 90%（`ANALYSIS_FAILURE_TOLERANCE`），工作流停止；≥ 90% 则继续后续步骤（卡片、推送等）。
- **Embedding 修复**：`_store_analysis_embeddings` 中 `break` 改为 `continue`，单条 embedding 失败不中断整个嵌入过程。

#### 7) Embedding 本地模型

- `**llm_client.call_embedding()`** 改为**本地模型优先**：先尝试 `sentence-transformers` + `BAAI/bge-small-zh-v1.5`（中英双语、512 维、零 API 成本），不可用时降级到 OpenRouter/OpenAI API。
- 环境变量 `EMBEDDING_PROVIDER=api` 可强制走 API；`LOCAL_EMBEDDING_MODEL` 可切换本地模型。

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
           ├─ Step 3  灵感准入统计
           │
           ├─ Step 4  原始落库
           │     upsert_daily_creative_insights（仅 raw，无 analysis）
           │     upsert_creative_library（全量归组）
           │
           ├─ Step 5  分析入队
           │     全量 items → 复用历史成功分析跳过 → 准入 + 待分析 → *_raw_pending_analysis.json
           │
           ├─ Step 6  灵感分析：analyze_video_from_raw_json.py
           │     ├─ VE prompt 含【特效玩法】行 → 从 LLM 输出解析 effect_one_liner
           │     ├─ 单条重试 3 次（403/超时/空/格式错误）
           │     └─ 二轮统一重试（仍有失败条目时）
           │
           ├─ Step 7  后处理 + 回写 DB
           │     ├─ 已投放特效库：apply_launched_effects_filter → exclude_* / material_tags
           │     ├─ 回写 daily_creative_insights + creative_library（含 analysis + effect_one_liner）
           │     └─ 玩法嵌入：play_fingerprint / effect_one_liner → effect_one_liner_embedding
           │
           ├─ （若分析成功率 < 90%）提前 return；可跑验收（partial）
           │
           ├─ Step 8  方向卡片：generate_video_enhancer_ua_suggestions_from_analysis.py
           ├─ Step 9  飞书多维表同步：sync_raw_analysis_to_bitable_and_push_card.py（--no-card）
           │     └─ 含「特效玩法」字段
           ├─ Step 10  飞书日报卡片：push_video_enhancer_feishu_card_only.py
           │     └─ AI工具竞品日报 + 按产品分组 + effect_one_liner 可点击链接
           ├─ Step 11  推送表：upsert_daily_push_content（daily_ua_push_content）
           ├─ Step 12  企业微信 + Google Sheet：push_video_enhancer_multichannel.py
           │     └─ 企业微信推送与飞书卡片使用同一日报格式
           │
           ├─ OpenRouter 用量：print_openrouter_key_meter（工作流结束，若启用）
           └─ 验收：run_acceptance_after_workflow（workflow_video_enhancer_acceptance.py）
```

**分步等价**：`workflow_video_enhancer_steps.py`：`crawl_store` → `cover_store`（可选与爬取拆分）→ `analyze_store` → `cluster_store` → `push_sync`。

### 去重体系（当前）


| 维度          | 实现                                      | 用途           |
| ----------- | --------------------------------------- | ------------ |
| **封面图去重**   | CLIP 向量 + 7 日历史窗口 + 跨日指纹                | 同画面/同视觉创意    |
| **一句话玩法去重** | `effect_one_liner` 精确匹配 + 7 日历史         | 同一特效玩法换了不同画面 |
| ~~文案去重~~    | `TEXT_FINGERPRINT_DEDUP_ENABLED=0`（已关闭） | ~~文案完全一致~~   |


### 持续发力信号

基于去重流程中被去掉的素材，`compute_sustained_effort_signals` 汇总三个来源，每个信号追溯历史素材的 `effect_one_liner` + 产品名 + 媒体链接：


| 来源                  | 含义             | 追溯内容                                     |
| ------------------- | -------------- | ---------------------------------------- |
| 封面跨日去掉              | 同画面/URL 跨天重复出现 | matched 素材的 effect_one_liner + 视频/图片链接   |
| ahash 组跨天           | 同一画面换了 ad_key  | canonical 素材的 effect_one_liner + 视频/图片链接 |
| effect_one_liner 跨天 | 同一玩法换了不同画面     | 最高展示量素材的视频/图片链接                          |


### 新素材判定

`creative_library.first_target_date = target_date` 且 `effect_one_liner` 在同 `appid` 近 7 日无精确/相似命中；仅 ad_key 首次出现但玩法已出现过，不计入新素材。

### 飞书多维表字段（当前）


| 字段名    | 类型  | 说明                             |
| ------ | --- | ------------------------------ |
| 标题     | 文本  | 素材标题                           |
| 产品     | 文本  | 竞品产品名                          |
| 平台     | 文本  | 投放平台                           |
| 视频链接   | 文本  | 视频直链                           |
| 封面图链接  | 文本  | 封面图 URL                        |
| 封面图    | 附件  | 封面图文件                          |
| 视频附件   | 附件  | 视频文件                           |
| 特效玩法   | 文本  | effect_one_liner（一句话概括核心特效/玩法） |
| AI分析结果 | 文本  | 灵感分析全文                         |
| UA灵感借鉴 | 文本  | （历史兼容列，新流程不再写入）                |
| 抓取日期   | 日期  | target_date                    |
| 素材标签   | 文本  | pipeline_tags / material_tags  |


**代码量**：请以实际 `wc -l` 为准；工作流核心文件见 `workflow_video_enhancer_full_pipeline.py`、`video_enhancer_pipeline_db.py`、`llm_client.py`、`push_video_enhancer_feishu_card_only.py`。

## 数据结构备忘

### raw JSON 素材字段区分


| 字段                | 视频素材                             | 图片素材     |
| ----------------- | -------------------------------- | -------- |
| `video_duration`  | > 0                              | 0        |
| `resource_urls`   | `[{type, image_url, video_url}]` | `[]`     |
| `preview_img_url` | 视频封面缩略图                          | 图片本体 URL |
| `video2pic`       | 0                                | 1        |


### 分析结果 JSON 新增字段（2026-03-24 起，2026-04-29 更新）

- `creative_type`: `"video"` 或 `"image"`
- `image_url`: 图片素材的图片 URL（视频素材为空字符串）
- `title`: 素材标题
- `body`: 素材文案
- `effect_one_liner`: 特效玩法一句话（如「AI 照片接吻特效」「自拍生成 AI 雨夜时尚大片」），从【特效玩法】行提取
- `video_analyzed`: 视频分析数量
- `image_analyzed`: 图片分析数量
- `skipped`: 跳过数量（无视频也无图片）
- `style_filter_match_summary`: 可为空串（套路筛选已移除后的列兼容）

**已移除字段**（2026-04-29）：

- `ua_suggestion_single`: 单条 UA 建议（已移除，不再生成第二次 LLM 调用）
- `ad_one_liner`: 一句话说明（已从 VE 输出移除，Arrow2 路径保留）

主流程合并后的 `analysis` JSON 还可能含：`pipeline_items`、`exclude_from_cluster`（2.8）、`launched_effect_match`（2.9）、`semantic_dedup`_* 等，以当日产物为准。
