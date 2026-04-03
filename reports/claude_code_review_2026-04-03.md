# Claude 新改动代码评审

评审时间：2026-04-03

评审范围：`AGENTS.md` 中 2026-04-03 记录的这批改动，重点查看了 `llm_client`、`creative_library` 去重、封面去重流程、全流程/分步流程编排，以及多维表/飞书推送链路。

结论：本批代码语法层面可通过，但存在 4 个需要优先处理的问题，其中前 2 个属于高优先级逻辑错误，会直接影响库内状态与后续统计结果。

## 主要问题

### 1. `creative_library.appearance_count` 会被同一天重复累加

严重级别：高

问题说明：

- 全流程中，同一份 `raw_payload` 被写入 `creative_library` 两次。
- `upsert_creative_library()` 在命中已有 `ad_key` 时，直接执行 `appearance_count = appearance_count + 1`。
- 结果是一次正常工作流执行，就会把当天素材记成“又出现了一次”，导致生命周期、趋势、重复出现次数等统计失真。

影响：

- `appearance_count` 不再表示“跨天出现次数”，而更像“被流程写入过多少次”。
- `compute_trend_signals()`、后续看板、人工判断素材复现频率时都会被误导。

相关位置：

- `scripts/workflow_video_enhancer_full_pipeline.py`
- `scripts/video_enhancer_pipeline_db.py`

建议修复：

- 只在 `target_date` 真正变化时递增 `appearance_count`。
- 或拆分“首次写入原始素材”和“补写分析结果”两类更新逻辑，后者不要改出现次数。

### 2. 去重组代表更新条件写反，导致 canonical 状态可能错误

严重级别：高

问题说明：

- 新素材命中 ahash/text 去重组后，代码会比较新旧素材的 `impression`，并决定 `canonical` 应该是谁。
- 但后面的批量更新条件写成了 `if canonical != ad_key and is_canonical == 0`，这只会在“旧素材仍是代表”时执行。
- 真正需要切换组代表的情况是“新素材应成为代表”，也就是 `canonical == ad_key`。
- 这样会造成：
  - 新强素材插入后自己带着 `is_canonical=1`
  - 旧代表没有被清零
  - 组内可能同时存在多个 `is_canonical=1`
  - `canonical_ad_key` 也可能没有同步更新

影响：

- `creative_library` 的组代表信息不可信。
- 任何依赖代表素材的摘要、报表、对外展示都有可能拿错样本。

相关位置：

- `scripts/video_enhancer_pipeline_db.py`

建议修复：

- 当新条目的热度超过原代表时，显式清空旧代表的 `is_canonical`，并把整组 `canonical_ad_key` 更新为新 `ad_key`。
- 同时补一个回归检查，确保同一 `dedup_group_id` 最多只有一条 `is_canonical=1`。

### 3. 一键全流程漏传聚类表同步参数，导致主流程与分步流程行为不一致

严重级别：中

问题说明：

- 分步流程 `workflow_video_enhancer_steps.py push_sync` 已经把 `--sync-target` 和 `--cluster-url` 传给了 `sync_raw_analysis_to_bitable_and_push_card.py`。
- 但一键流程 `workflow_video_enhancer_full_pipeline.py` 中同步多维表时，没有继续传这两个参数。
- 下游脚本只有在拿到 `cluster_url` 时才会同步聚类表，因此一键流程实际上可能只同步主表，不同步聚类表。

影响：

- 同样的业务流程，用“一键跑”和“分步跑”会得到不同结果。
- 容易出现主表已更新、聚类表没更新，但卡片链接又指向聚类表的混乱状态。

相关位置：

- `scripts/workflow_video_enhancer_full_pipeline.py`
- `scripts/workflow_video_enhancer_steps.py`
- `scripts/sync_raw_analysis_to_bitable_and_push_card.py`

建议修复：

- 让一键流程补齐 `--cluster-url` 和需要的 `--sync-target` 参数。
- 保证一键流程与分步流程使用同一套同步入口参数。

### 4. “语义嵌入跨日去重”已实现但未真正接入主链路

严重级别：中

问题说明：

- `AGENTS.md` 中把“语义嵌入去重”描述为已完成的重要能力。
- 实际代码里，`semantic_crossday_filter()` 只定义在 `video_enhancer_pipeline_db.py` 中，没有被主流程调用。
- 当前真正用于“进入分析前去重”的仍然只有：
  - `ad_key`
  - 主媒体 URL
  - `image_ahash_md5`
- embedding 目前只是分析完成后写入库，为将来使用做准备，但还没有在主链路里生效。

影响：

- 文档描述与实际行为不一致。
- 用户会以为“同创意语义、不同素材资产”的重复已被拦截，实际上并没有。

相关位置：

- `scripts/video_enhancer_pipeline_db.py`
- `scripts/workflow_video_enhancer_full_pipeline.py`

建议修复：

- 如果要兑现该能力，应在合适阶段接入 `semantic_crossday_filter()`。
- 如果暂时只是“先存 embedding，后续再启用”，应修改 `AGENTS.md` 表述，避免误导。

## 补充说明

- 我已对关键脚本执行 `python3 -m py_compile`，语法检查通过。
- 当前风险主要不是“跑不起来”，而是“状态写错但不容易立刻发现”。
- 这批改动最需要补的不是大而全测试，而是围绕以下场景的回归校验：
  - 同一素材在同一天被二次写入时，`appearance_count` 不应增加
  - 同一去重组在新素材更强时，代表素材应正确切换
  - 一键流程与分步流程对聚类表同步结果应一致
  - 语义去重若未接入，应避免在文档中表述为“已生效”

## 建议修复顺序

1. 先修 `appearance_count` 重复累加问题。
2. 再修 `canonical` 代表切换逻辑。
3. 然后补齐一键流程的聚类表同步参数。
4. 最后决定“语义去重”是立即接主链路，还是先修改文档描述。