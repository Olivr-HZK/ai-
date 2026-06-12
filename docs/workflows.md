# 三条生产工作流 + 独立反馈训练链路

核心实现位于 `**ua_workflows/**`；`**scripts/**` 只做薄封装入口，便于 `python scripts/run_*.py` 与 cron 调用。

## 总览


| 工作流            | 入口脚本                             | 包路径                            | 用途（简述）                                                        |
| -------------- | -------------------------------- | ------------------------------ | ------------------------------------------------------------- |
| Video Enhancer | `scripts/run_video_enhancer.py`  | `ua_workflows/video_enhancer/` | 广大大 **工具** 垂类竞品：抓取 → 封面/入库/分析 → 飞书多维表 → 浩鹏 TopN 推送                |
| VE 反馈训练        | `scripts/run_ve_feedback_training.py` | `ua_workflows/video_enhancer/feedback_training.py` | 从审核多维表直接拉取「接受情况」，独立落库、导出素材偏好训练集并训练 baseline |
| VE 浩鹏 TopN 二次筛选 | `scripts/run_ve_haopeng_topn_push.py` / `scripts/run_ve_haopeng_ai_filter.py` | `ua_workflows/video_enhancer/haopeng_ai_filter.py` / `haopeng_topn_push.py` | 从主多维表读取目标日素材与浩鹏历史反馈，二次 AI 筛选后推送 TopN |
| Arrow2 每日最新    | `scripts/run_arrow2_latest.py`   | `ua_workflows/arrow2/`         | 广大大 **游戏** 垂类：`latest_yesterday`，detail-v2 逐卡点卡，竞品维度的「昨日首见」素材 |
| Arrow2 展示估值    | `scripts/run_arrow2_exposure.py` | `ua_workflows/arrow2/`         | 同上入口库，`exposure_top10`，偏高高展示估值素材维度                            |


Arrow2 的 `scripts/run_arrow2_latest.py` / `run_arrow2_exposure.py` 在启动时会向 `argv` 注入默认 `--pull-only`（`latest_yesterday` / `exposure_top10`），再调用 `**ua_workflows.arrow2.pipeline`**。

## Video Enhancer

- **流水线主逻辑**：`ua_workflows/video_enhancer/pipeline.py`
- **爬取**：`ua_workflows/video_enhancer/crawl.py`（`run_batch`：工具 Tab、目标日最新创意、DOM 点卡 detail 主路径；主流程不再额外跑 `dom_enrich` 二次详情补全）
- **分析**：`ua_workflows/video_enhancer/analyze.py`
- **数据库**：默认 `data/video_enhancer_pipeline.db`（`ua_workflows.shared.db.video_enhancer`）
- **多维表**：需 `.env` 中 `VIDEO_ENHANCER_BITABLE_URL` 等；全流程默认 **开启** 同步，除非传入 `--no-bitable-sync`。
- **竞品自动更新**：主流程和 standalone crawl 启动时默认从主多维表的 `竞品list` 子表同步正式竞品到 `config/ai_product.json`，字段默认是 `广告主名` / `appid`；成功后本轮设置 `VE_COMPETITOR_LIST_ALREADY_SYNCED=1` 防止重复同步。`VE_COMPETITOR_LIST_SYNC_ENABLED=0` 可关闭；同步失败、表为空或权限不足时沿用本地配置，不阻塞抓取。也可用 `scripts/run_ve_competitor_list_sync.py --dry-run` 单独预览。
- **目标日抓取**：传 `--date` / `--target-date` 时默认先在广大大 UI 选择 `target_date ~ target_date`，再逐卡进入 detail-v2 拉详情，并用本地 UTC+8 `first_seen == target_date` 二次校验；`created_at` 与 `first_seen` 不同日会按重投剔除。保留 `--no-ui-date-range` 和 `--napi-list` 作为调试回退。
- **人机验证中断处理**：VE 日更主爬取走共享 `run_batch`，登录后或主站稳定阶段检测到广大大滑块/安全验证时，默认发送飞书 IM 交互卡片和帮助图，等待人工完成页面验证后点击「已完成」，然后关闭当前浏览器并重启抓取入口；这不是自动破解验证码。默认等待 `GUANGDADA_FEISHU_CONFIRM_TIMEOUT_SEC=900` 秒，默认重启 `GUANGDADA_HUMAN_CHECK_RESTART_LIMIT=1` 次；`GUANGDADA_CAPTCHA_MANUAL_GATE=0` 可关闭人工闸口回到旧关闭逻辑。
- **媒体 URL 格式归一**：视频 URL 统一通过 `ua_workflows.shared.media.resolve.normalize_video_url_for_consumption()` 消费，广大大 CDN 中路径以 `.webp` 结尾的视频会在分析、入库、同步、飞书推送、复核看板等下游自动改为 `.mp4` 形态；封面同步到多维表时，`封面图链接` 会把 `.image` 路径改为 `.png`，附件上传先试原始封面 URL，失败再试 `.png` 版本。
- **抓取后保留**：爬取阶段会做广告主匹配、非目标日剔除、重投剔除、重复 `ad_key` 去重；默认不再按单产品 `>10` 做硬截断，避免好素材在封面去重前被提前丢弃，如需恢复旧口径可设 `VIDEO_ENHANCER_PER_PRODUCT_TRUNCATE_ENABLED=1`。target-date 模式默认滚动上限为 56 轮，可用 `VIDEO_ENHANCER_MAX_SCROLL_ROUNDS` 调整；旧 `first_seen` 早停默认关闭，可用 `VIDEO_ENHANCER_TARGET_DATE_EARLY_STOP_ENABLED=1` 打开。
- **抓取保留漏斗**：每日全流程会写 `data/workflow_video_enhancer_{date}_crawl_product_retention.json`，并同步嵌入 raw 的 `crawl_product_retention_report`；按产品记录点卡详情数、页面卡片数、抓到素材数、爬取过滤后保留数、封面过滤后保留数，以及广告主不匹配、非目标日、重投、重复 ad_key、截断、跨日/日内封面重复等剔除原因。
- **封面去重**：封面阶段先用 creative_library 的 URL / ahash / 素材指纹做跨日去重，再用本地 CLIP cover embedding 做视觉聚类。默认阈值 `COVER_VISUAL_DEDUP_THRESHOLD=0.75`，历史参照窗口 `COVER_STYLE_HISTORY_LOOKBACK_DAYS=60`，硬去重窗口 `COVER_STYLE_HISTORY_HARD_DEDUPE_DAYS=7`；7 天内历史命中剔除，7 天外历史命中保留当天最高展示估值代表并记录 `history_refresh`，同日相似封面只保留代表。
- **分析入队去重**：封面后进入 LLM 前会做日内 / 跨日素材去重和历史分析复用；重复素材复用 canonical 分析结果，不重复消耗模型。入队报告写 `data/workflow_video_enhancer_{date}_analysis_queue_report.json`，记录每产品封面后素材数、分析前去重、历史缓存复用和实际 LLM 入队数。
- **全流程报告**：主流程结束或分析低成功率提前停止时，会写 `data/workflow_video_enhancer_{date}_flow_report.json` 与 `reports/workflow_video_enhancer_{date}_flow_report.md`，并默认通过 `VE_FLOW_REPORT_FEISHU_WEBHOOK` 发送飞书卡片；报告按产品串起点卡、抓到、爬取保留、封面后、分析去重、LLM 入队、可用分析、同步候选和主表写入，并汇总每一步筛掉/跳过原因。
- **验收告警**：全流程报告会把「封面后保留数」和「主表写入数」与近 5 天同产品历史均值比较；默认低于历史均值 50% 且历史均值不低于 3 条时触发飞书告警，并在卡片中给出单产品重试命令，由人工决定是否重跑。可用 `VE_FLOW_REPORT_LOOKBACK_DAYS`、`VE_FLOW_REPORT_LOW_RATIO`、`VE_FLOW_REPORT_MIN_BASELINE` 调整阈值，用 `VE_FLOW_REPORT_FEISHU_ENABLED=0` 临时关推送。
- **浩鹏 TopN 推送**：主流程默认不再发送旧「AI工具竞品日报」素材卡片，也不再默认触发企业微信或 Google Sheet；同步完成后改为调用 `ua_workflows.video_enhancer.haopeng_topn_push`，未传 `--topn-input-json` 时会先从主多维表生成目标日二次 AI 筛选 JSON，再推送 TopN。发送方式可用 `VE_HAOPENG_TOPN_SEND_MODE` / `--topn-send-mode` 指定；未指定时有 chat_id 走 IM，否则走 `VE_HAOPENG_TOPN_FEISHU_WEBHOOK`，没有专用值时复用筛选漏斗报告群 `VE_FLOW_REPORT_FEISHU_WEBHOOK`，最后才尝试 `FEISHU_UA_WEBHOOK`；不会兜底到通用测试群 `FEISHU_BOT_WEBHOOK`。可用 `--no-topn-push` 或 `VE_HAOPENG_TOPN_PUSH_ENABLED=0` 关闭，`--topn-input-json` 指定旧实验文件。
- **旧渠道回退**：旧素材日报仅在传 `--send-material-card` 或设置 `VIDEO_ENHANCER_MATERIAL_DAILY_CARD_ENABLED=1` 时发送；企业微信和 Google Sheet 仅在传 `--send-wecom` / `--sync-sheet` 时运行。
- **同步前排除/标记**：一键流程与独立同步都会补跑成人/色情风险拦截、用户上传人物照片特效硬拦截、可选日内玩法重复、可选同产品老玩法拦截、可选玩法 embedding 高置信重复、embedding 重复候选与已投放匹配；玩法去重优先使用 `play_fingerprint`，缺失时回退 `effect_one_liner`。硬拦截项不进主表和方向卡片，候选类信号只写 `素材标签` 供后续校准。
- **同步前模板去重**：主表同步默认开启 `BITABLE_TEMPLATE_DEDUP_ENABLED=1`，在同产品/同 appid、同玩法桶内合并同模板只换人物、人种、性别或模特的素材；默认使用模板 exact 与同玩法封面 CLIP 相似，阈值 `BITABLE_TEMPLATE_DEDUP_CLIP_THRESHOLD=0.70`。模板文本相似默认关闭，仅设置 `BITABLE_TEMPLATE_DEDUP_TEXT_SIMILARITY_ENABLED=1` 时按 `BITABLE_TEMPLATE_DEDUP_TEXT_SIMILARITY_THRESHOLD=0.78` 参与合并。
- **同步执行能力**：同步写入前会读取目标表真实字段类型并归一化值；「玩法」若是多选字段，会拆成字符串数组且只保留字段已有选项，不把 AI 新建议自动创建成正式标签。主表批量写入默认每批 200 条；封面图与视频会尽量上传为附件，同时保留已归一化后的链接字段。`--sync-target raw|cluster|both` 可选择只写素材主表、只写方向聚类表或两者都写；主流程默认用 `--no-card`，推送由漏斗报告和浩鹏 TopN 节点负责。
- **日报新玩法聚类**：日报层不会强制限制每产品数量；会先排除同步前硬拦截素材，再把 `play_fingerprint` 折成粗粒度玩法族，并结合文本/embedding 相似度在同产品内正常聚类。`new_material_count` 是严格新玩法簇内素材数，`new_effect_count` 是聚类后的新玩法簇数；该报告现在默认只作为本地/兼容口径，不再自动发旧素材日报。
- **玩法资产库**：`config/ve_play_assets.json` 是本地兜底，协作源是飞书云文档；分析启动时会先尝试拉取最新云文档，失败时继续用本地 JSON。资产库也吸收了内部 Google Sheet「AI产品热点排期表 / 特效上线记录」中的上线主题，用于 aliases、关键词、子标签和案例沉淀。维护方式见 [ve-play-assets.md](./ve-play-assets.md)。
- **分析字段**：VE 分析会输出并同步「核心卖点」「Hook解析」「脚本/口播」「风险等级」「素材标签」；同时在 analysis JSON/SQLite 中保留 `play_fingerprint`（玩法指纹）和 `differentiator`（差异点）供去重校准。Hook 侧重前 1~3 秒抓人机制；脚本/口播提炼旁白、字幕、画中文字或 CTA，便于后续文案借鉴；风险等级只显示低/中/高，具体原因仍保留在素材标签。
- **AI 玩法判断**：VE 分析 prompt 会注入压缩后的玩法资产候选清单，并要求模型输出 `玩法资产ID`、`玩法资产名称`、`玩法变种ID`、`玩法变种名称`、`玩法归类`、`玩法判断理由`。日报和多维表同步优先使用 AI 判断；当 ID 无效、缺失或不确定时，再回退到本地关键词/别名/案例规则匹配。
- **多维表字段**：主表当前实际同步核心素材字段、媒体链接/附件、核心卖点、Hook、脚本/口播、风险等级、AI 分析、抓取日期、广告 ID、接受情况、素材标签，以及「玩法」「玩法指纹」「差异点」「模板指纹」「日内相似素材数」等筛选字段；旧「玩法资产 / 玩法变种 / 玩法新旧 / 玩法判断」内部列仍在字段定义里保持兼容，但日常写入不再依赖这些列。AI 判断命中时，`素材标签` 追加 `玩法判断:AI`；「日内相似素材数」会合并 raw 阶段 exact 相似数、同日封面 CLIP 聚类成员与同步前同模板合并成员，`1` 表示当天无同类相似项。
- **日报素材口径**：仅 Video Enhancer 使用 `load_daily_material_report()` 统一输出「新素材 / 新玩法 / 持续发力」：
  - 新素材：`creative_library.first_target_date = target_date`，通过同步前硬拦截，且同 `appid` 下粗粒度玩法族过去 7 日无精确或相似命中；老玩法换素材不计入新素材
  - 新玩法：严格新素材按同产品粗粒度玩法族聚类后的簇数，不等于素材条数
  - 持续发力：VE 封面/URL/ahash/玩法跨日信号，日报按产品展示 Top 条目，不混入 Arrow2
  - 玩法资产/变种：优先沿用分析阶段 AI 判断，仍按资产 ID 与变种 key 对历史全量素材做比对；已有资产但变种 key 首次出现算「新玩法变种」。

## VE 反馈训练（独立链路）

- **入口**：`scripts/run_ve_feedback_training.py`
- **定时入口**：`scripts/cron_ve_feedback_training_daily.sh`
- **数据源**：默认读取审核多维表 `CivwbJ2HkazcKTsKnbGclA5RnWc / tblrZZvVuFcjL0kE / vewJtPixtM`，也可用 `VE_FEEDBACK_BITABLE_URL` 覆盖。
- **数据库**：独立使用 `data/ve_feedback_training.db`，不读写正常 VE 主库。
- **训练标签**：多维表 `接受情况` 中 `接受` / `采纳` / `入素材库=1`，`删除` / `不采纳=0`，`待定` / `重复抓取` / 空值只留存不训练。
- **特征口径**：只使用素材字段，例如标题、正文、核心卖点、Hook、脚本/口播、玩法资产/变种、玩法指纹、差异点、AI 分析和素材标签；产品、广告主、日期、热度、展示估值、地区等只进审计字段。
- **产物**：`data/ve_feedback_training_dataset_YYYY-MM-DD.jsonl`、`data/models/ve_feedback_preference_nb_YYYY-MM-DD.json`、`reports/ve_feedback_training_YYYY-MM-DD.md`。
- **完整样本训练**：`--complete-profile core` 可只用核心素材字段齐全的样本训练；`core_play` 会额外要求玩法资产/玩法指纹等字段齐全，历史数据当前负样本过少，仅适合观察覆盖率。

更多说明见 [ve-feedback-training.md](./ve-feedback-training.md)。

## VE 浩鹏 TopN 二次筛选（独立推送链路）

- **只生成筛选 JSON**：`scripts/run_ve_haopeng_ai_filter.py --date YYYY-MM-DD`
- **生成并推送**：`scripts/run_ve_haopeng_topn_push.py --date YYYY-MM-DD --top-n 10`
- **每日链路**：`scripts/cron_ai_video_enhancer_daily.sh` 只执行 `run_video_enhancer.py`；TopN 生成与推送由主流程内部完成，默认跟筛选漏斗报告共用 `VE_FLOW_REPORT_FEISHU_WEBHOOK` 所在群，避免 cron 额外追加测试群或重复推送。
- **数据源**：默认读取 `VIDEO_ENHANCER_BITABLE_URL` 指向的 VE 主表；目标日素材来自「抓取日期」，历史反馈默认从 `2026-05-25` 到目标日前一天，且只使用浩鹏 `采纳 / 入素材库 / 不采纳 / 重复抓取 / 删除 / 拒绝` 等有效反馈，`待定` 不作为正负样本。目标日当天的浩鹏反馈不会传给模型，只在报告落盘后用于人工回测。
- **筛选口径**：模型仍按“浩鹏会采纳 / 入素材库式有价值变体”的历史偏好判断；目标日候选在进入模型前排除 `admob` / `youtube` 渠道。飞书卡片从非排除渠道结果中展示 Top10，不因为模型标记 `hold` 就强制少推。
- **卡片入口**：飞书卡片默认隐藏回测字段；末尾会追加“查看多维表格”按钮，链接到 `VIDEO_ENHANCER_BITABLE_URL` 指向的主表，便于当天直接复核浩鹏反馈。
- **模型**：默认 `qwen/qwen3.7-max`；可用 `VE_HAOPENG_FILTER_MODEL` 或 `--model` 覆盖。
- **产物**：`data/haopeng_topn_experiments/{date}_label_prior.json`，字段兼容 TopN 飞书卡片渲染。该链路不写回多维表，也不改变 VE 主流程同步/拦截结果。
- **兼容旧实验文件**：推送脚本传 `--input-json path/to/file.json` 可直接推指定产物；传 `--use-latest-local` 可恢复读取最新本地 `*_label_prior.json` 的旧行为。

## VE 竞品周检查

- **入口**：`scripts/run_ve_weekly_competitor_review_server.sh`，cron 周一 13:10 默认调用。
- **新竞品候选来源**：脚本先尝试运行 `scripts/run_new_charts_ai_tools.py` 抓广大大 AI 图像 / AI 视频周榜，再把 raw 交给 `run_ve_weekly_competitor_review.py` 生成新竞品候选、低量观察与建议移除老竞品。
- **人机验证中断处理**：周榜采集走 `ua_workflows/shared/guangdada/new_charts_ai_tools.py`，已接入 VE 日更主爬取同款飞书确认闸口。登录后、主站稳定阶段或采集过程中检测到“安全验证 / 请完成验证 / 人机验证”时，会发送飞书 IM 卡片；人工完成页面验证并点击「已完成」后，脚本关闭当前浏览器并重启周榜采集入口，重启次数同样受 `GUANGDADA_HUMAN_CHECK_RESTART_LIMIT` 控制。

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
| `config/ve_play_assets.json`    | VE 玩法资产库本地兜底；协作源为飞书云文档，内部上线主题来自 Google Sheet |
| `config/arrow2_competitor.json` | Arrow2 产品与 `filters`、`pull_specs`、`search_tab` |
| `config/iso3166_alpha3_zh.json` | 国家代码等辅助映射                                      |


更多历史设计与字段说明见 [AGENTS.md](../AGENTS.md)。
