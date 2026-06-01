# AIGC Template Copy Skill 评测设计

## 目标

为 `aigc-template-copy` skill 做一个小型评测工作流，用真实的 Video Enhancer 竞品素材检查 skill 的判断和提示词质量。首轮运行从飞书多维表中选择最新 5 条 `浩鹏接受情况 = 采纳` 且有可用视频链接的素材。这个工作流只产出 prompt 评测资料；Video Lab 由用户手动执行。

## 不做什么

- 不自动生成视频。
- 不调用 Video Lab、Kling 3.0 或 Vidu Q3 Pro。
- 不把评测结果写回飞书多维表。
- 不自动修改 skill 文件。
- 不纳入 Arrow2 素材。

## 输入

- 项目现有环境变量中的飞书凭证。
- `VIDEO_ENHANCER_BITABLE_URL` 指向的 Video Enhancer 多维表。
- skill 压缩包 `/Users/oliver/Downloads/aigc-template-copy-skill.zip`，读取其中的 `aigc-template-copy/SKILL.md`。
- 尽量复用项目里已有的多维表访问和素材 URL 提取辅助函数。

## 样本选择

脚本必须读取 live 多维表 schema，不假设字段 ID。需要识别：

- 评审字段：`浩鹏接受情况`
- 采纳值：`采纳`
- 表中已有的日期或创建时间排序字段
- 可用素材字段，例如 `视频链接`、`视频`、`封面图链接`、标题、正文、核心卖点、玩法标签等

选择规则：

1. 过滤 `浩鹏接受情况 = 采纳` 的记录。
2. 按最可靠的最新时间字段倒序排序；如果找不到明确时间字段，再退回飞书记录顺序。
3. 只保留有可用视频 URL 的记录。
4. 持续扫描，直到收集满 5 条或表已扫完。
5. 报告中保留被跳过记录的原因。

## Prompt 工作流

每条入选素材构造一次模型请求。请求中需要带上 `aigc-template-copy` 的关键规则和当前素材的元信息，让模型输出结构化逆向结果：

- 模板模式：`图片复刻` 或 `视频模板`
- 片段判断：为什么选择该模式，以及排除了哪些 UI、水印、教程、广告、字幕或无关片段
- 图片 Prompt 或首帧图像 Prompt
- 仅当模式为 `视频模板` 时，输出视频动态 Prompt、动态时长和建议生成时长

Prompt 必须保留 skill 的核心分流规则：

- `图片复刻`：用于静态效果图、海报、前后对比图、APP 结果页、轮播图、静态蒙太奇
- `视频模板`：用于有明确动态价值的视频，例如人物动作、表情变化、手部/身体动作、镜头运动、转场或环境互动

## 输出产物

每次运行按日期输出到：

`data/aigc_template_copy_eval/YYYY-MM-DD/`

产物包括：

- `selected_materials.json`：入选的 5 条素材，以及跳过记录的原因
- `skill_eval_results.json`：每条素材的结构化模型输出
- `skill_eval_review.csv`：给用户手动填写 Video Lab 结果的评测表
- `skill_eval_report.md`：便于人工阅读的评测包，包含源素材链接和可复制 prompt
- 可选 `skill_eval_report.html`：如果 Markdown 不方便看素材链接，再生成轻量本地预览页

CSV 需要包含这些人工回填字段：

- `mode_correct`：模板模式是否判断正确
- `segment_correct`：是否抓准真正该复刻的片段
- `irrelevant_elements_included`：是否误写入 UI、广告层、教程、水印、字幕等无关元素
- `template_text_wrongly_removed`：是否把模板本身需要保留的文字或视觉元素误删
- `kling30_score`：Kling 3.0 手动评测分数
- `vidu_q3_pro_score`：Vidu Q3 Pro 手动评测分数
- `video_lab_satisfied`：Video Lab 结果是否满意
- `failure_reason`：失败原因
- `skill_rule_suggestion`：建议补进 skill 的规则

## Skill 优化闭环

首版脚本只准备评测资料。用户手动填回 Video Lab 结果后，后续可以再做一个汇总步骤，把失败案例归因成 skill 优化建议，按以下类型分组：

- 模板模式误判
- 片段选择错误或复刻区域错误
- 误写入 UI、广告、教程、字幕、水印等无关元素
- 错误删除了模板中有价值的文字或视觉元素
- 视频动态 Prompt 不够可执行
- 推荐时长不匹配

任何真实的 `SKILL.md` 修改，都必须在用户单独确认后再做。

## 错误处理

- 如果缺少多维表配置，直接失败，并说明缺少哪个环境变量。
- 如果 live 表中没有 `浩鹏接受情况` 字段，直接失败，并列出看起来像评审字段的可用字段。
- 如果不足 5 条采纳视频素材，仍为已找到的素材产出评测包，并明确说明缺口。
- 如果某条素材模型调用失败，保留该素材并标记错误状态，继续处理其他素材；除非所有调用都失败。
- 如果 skill zip 无法读取，在查询多维表或调用模型之前直接失败。

## 验证

实现时需要有聚焦测试覆盖：

- skill zip 读取
- 多维表 URL 解析
- `浩鹏接受情况 = 采纳` 过滤
- 视频 URL 提取和跳过原因
- Prompt 构造是否保留图片复刻 / 视频模板分流规则
- 输出文件结构

首轮真实运行的人工验收：

1. 确认脚本选择的是最新 5 条 `浩鹏接受情况 = 采纳` 的视频素材。
2. 确认每条报告都包含原视频链接和生成的 prompt。
3. 确认没有调用 Video Lab、没有写回飞书、没有修改 skill 文件。
