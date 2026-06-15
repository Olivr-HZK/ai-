# VE 评分反馈与 3 星模板识别设计

## 背景

当前 Video Enhancer 主表里已经有 `浩鹏接受情况`、`尉蓝接受情况` 和历史兼容字段 `接受情况`。这些字段用 `采纳`、`入素材库`、`不采纳`、`重复抓取`、`待定` 表达人工反馈。后续业务希望把“采纳/不采纳”升级成 1 到 3 颗星评分，并让 3 星素材自动进入图片/视频模板识别流程，沉淀给杨爽作为可复刻模板灵感。

这次先改采纳与后续模型参考路径，并为 `aigc-template-copy` Skill 集成预留清晰输入。第一版不直接把 Eagle、Video Lab 或 Skill 自动生成链路并入 VE 日更主流程。

## 目标

1. 新增评分字段，保留旧接受情况字段，避免打断历史数据、采纳率复盘、留存保护和旧脚本。
2. 让反馈训练、浩鹏 TopN、回测摘要优先使用评分，评分缺失时兼容旧接受情况。
3. 让 3 星素材自动产出模板识别任务 JSON 和 Markdown 报告，作为后续模板识别 Skill 的输入。
4. 明确 3 星只代表“值得模板识别”，不在第一版自动生成图片、视频或导入 Eagle。

## 非目标

1. 不删除或重命名 `接受情况`、`浩鹏接受情况`、`尉蓝接受情况`。
2. 不把历史多维表记录批量迁移成评分。
3. 不在 VE 日更默认调用 `aigc-template-copy`、Eagle 或 Video Lab。
4. 不把 3 星模板识别结果直接写回主表，第一版只落本地可审计产物。
5. 不重做主流程筛选、封面去重、视频内容去重或多维表同步策略。

## 字段设计

主表新增字段：

| 字段 | 类型建议 | 说明 |
| --- | --- | --- |
| `浩鹏评分` | 单选或数字 | 浩鹏对素材的 1 到 3 星评分 |
| `尉蓝评分` | 单选或数字 | 尉蓝对素材的 1 到 3 星评分 |

代码读取时兼容这些值：

- `1星`、`一星`、`1颗星`、`1`
- `2星`、`二星`、`2颗星`、`2`
- `3星`、`三星`、`3颗星`、`3`
- Feishu 数字字段中的 `1`、`2`、`3`

写新行时，VE 主表同步只确保字段存在，不主动写评分值。旧 `接受情况` 仍可写 `待定` 作为兼容默认值。

## 兼容映射

新增统一反馈解析层，所有后续链路通过它读取人工反馈。

读取优先级：

1. reviewer 对应评分字段，例如 `浩鹏评分`。
2. reviewer 对应旧状态字段，例如 `浩鹏接受情况`。
3. 历史兼容字段 `接受情况`。

旧状态兜底评分：

| 旧状态 | 评分 |
| --- | --- |
| `采纳` / `接受` | 3 |
| `入素材库` | 2 |
| `不采纳` / `删除` / `拒绝` / `重复抓取` | 1 |
| `待定` / 空 | 无训练标签 |

解析结果统一包含：

```json
{
  "rating": 3,
  "rating_label": "3星",
  "source_field": "浩鹏评分",
  "source_value": "3星",
  "legacy_status": "采纳"
}
```

如果只有旧字段，`source_field` 指向旧字段，`rating` 使用兼容映射。

## 反馈训练

`ua_workflows/video_enhancer/feedback_training.py` 从二分类标签扩展为评分样本。

样本导出新增字段：

- `rating`
- `rating_label`
- `rating_source_field`
- `rating_source_value`
- `legacy_accept_status`

训练兼容策略：

- JSONL 始终导出评分字段。
- 现有朴素贝叶斯 baseline 可继续把 `3星` 视为高意向、`1星` 视为低意向，`2星` 可作为中性样本留存或在二分类 baseline 中暂不参与训练。
- 报告中展示评分分布，而不是只展示接受/删除数量。
- 后续更强模型直接使用 1 到 3 分作为偏好强弱信号。

## 浩鹏 TopN

`ua_workflows/video_enhancer/haopeng_ai_filter.py` 改为评分先验。

输入历史样本：

- 只使用目标日前的历史记录。
- 优先读取 `浩鹏评分`。
- 无评分时兼容 `浩鹏接受情况` / `接受情况`。
- 只有 `rating` 为 1、2、3 的记录进入历史偏好。

Prompt 调整：

- 从“历史浩鹏有效反馈”改为“历史浩鹏评分偏好”。
- 明确 `3星=强烈值得复刻/制作`，`2星=有参考价值但优先级中等`，`1星=低价值或重复/不适配`。
- 要求模型优先推荐与历史 3 星相似但不重复、并具有新模板画面或新片段价值的素材。

报告保持兼容：

- 继续输出 `accept_score`，用于 TopN 排序和飞书卡片。
- 新增 `history_rating_counts`。
- 目标日当天评分只用于回测，不传给模型。

## TopN 推送与回测

`ua_workflows/video_enhancer/haopeng_topn_push.py` 的默认推送仍不展示当天实际反馈。

当显式开启回测时，摘要改为：

- `3星命中 N/TopN`
- `平均评分 X.XX`
- `评分分布：3星 A / 2星 B / 1星 C / 未评分 D`

如果目标日没有评分但有旧状态，使用旧状态映射兜底计算，并在摘要里标注来源为兼容旧字段。

## 3 星模板识别任务

新增模块：

```text
ua_workflows/video_enhancer/template_recognition.py
scripts/run_ve_template_recognition.py
```

输入：

- 默认读取 `VIDEO_ENHANCER_BITABLE_URL` 主表。
- 支持 `--date YYYY-MM-DD`。
- 支持 `--reviewer haopeng`，默认映射 `浩鹏评分`。
- 只选目标日评分为 3 的记录；若评分缺失但旧字段是 `采纳`，可通过兼容模式纳入。

输出：

```text
data/ve_template_recognition_tasks_YYYY-MM-DD.json
reports/ve_template_recognition_tasks_YYYY-MM-DD.md
```

任务 JSON 每条包含：

- `record_id`
- `ad_key`
- `product`
- `platform`
- `crawl_date`
- `rating`
- `rating_source_field`
- `video_url`
- `cover_url`
- `title`
- `core`
- `hook`
- `script_or_voiceover`
- `play_label`
- `template_fingerprint`
- `material_tags`
- `suggested_template_kind`
- `template_reason`
- `aigc_template_copy_input`

`suggested_template_kind` 初判规则：

- 有视频链接、视频时长大于 0，且脚本/口播或模板指纹显示动态动作、转场、表情、身体/镜头运动时，标为 `video_template_candidate`。
- 只有封面或更像静态海报、拼贴、照片效果、卡片包装时，标为 `image_template_candidate`。
- 无法判断时标为 `needs_manual_review`。

`aigc_template_copy_input` 只整理给后续 Skill 使用的来源信息，不直接执行生成。

## Skill 集成边界

已检查 `/Users/oliver/Downloads/aigc-template-copy-package/`，该 Skill 能从视频、参考图或 Eagle 日期文件夹逆推 AIGC 图片/视频模板，并可进一步走 Video Lab 生成和 Eagle 导入。

第一版集成边界：

- VE 只负责产出 3 星素材的模板识别任务。
- `aigc-template-copy` 仍由人工或后续独立脚本触发。
- 任务 JSON 中的 `aigc_template_copy_input` 保持稳定，后续可以接成自动调用入口。

后续自动集成需要单独确认：

- 3 星素材是否自动下载视频/封面到本地。
- 是否自动创建 Eagle 日期文件夹。
- 使用哪张模特图作为对应模特。
- 生成失败是否回写主表状态。
- 是否在 VE 日更 cron 中打开自动模板生成。

## 错误处理

- 评分字段不存在时，提示缺字段并继续按旧字段兼容读取。
- 评分值不合法时，不进入训练标签，并在报告里计入 `invalid_rating`。
- 3 星素材缺视频和封面时，不生成可执行任务，但报告记录为 `missing_media`。
- Feishu 读取失败时不写空报告，命令返回非 0。
- 模板任务生成不影响 VE 主流程同步和漏斗报告。

## 测试计划

新增或更新测试：

1. 评分字段优先于旧接受情况。
2. 旧字段在评分缺失时正确映射为 1 到 3 分。
3. 不合法评分不会进入训练标签。
4. 反馈训练 JSONL 包含 `rating` 和来源字段。
5. Haopeng TopN prompt 使用评分偏好，不再只使用采纳/不采纳文案。
6. TopN 回测摘要展示 3 星命中、平均评分和评分分布。
7. 3 星素材进入模板识别任务，1 星和 2 星不进入。
8. 模板识别任务能区分图片模板候选、视频模板候选和人工确认。

## 验收标准

1. 新行同步不会破坏旧 `接受情况` 字段。
2. 多维表已有 `浩鹏评分` 时，训练和 TopN 优先使用评分。
3. 没有评分但有旧 `浩鹏接受情况` 时，下游仍能正常工作。
4. 目标日 3 星素材能生成模板识别 JSON 和 Markdown 报告。
5. 默认日更不自动调用 `aigc-template-copy`、Eagle 或 Video Lab。
6. 相关单元测试通过。
