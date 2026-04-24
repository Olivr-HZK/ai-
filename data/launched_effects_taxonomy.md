# 特效说明原始文本分类（启发式）

来源：`data/launched_effects_descriptions_only.json`，共 **189** 条。

请按 **category_id** 在 `config/launched_effects_category_rules.json` 里补充每类抽词规则；
再运行 `scripts/extract_launched_effects_llm_keywords.py`。

## 各类数量

- **`doc_theme_or_internal_id`**（含主题id/内部id）: **55**
- **`batch_new_effects_list`**（批量列举特效）: **34**
- **`single_line_effect_name`**（单行特效名片）: **24**
- **`doc_with_link`**（含HTTP/飞书链）: **21**
- **`ops_changelog_block`**（【】工单块）: **20**
- **`few_lines_theme_desc`**（少行主题说明）: **8**
- **`ops_surface_meta`**（运维表面操作）: **7**
- **`product_line_batch_spec`**（产品行+批次规格）: **6**
- **`multi_product_mixed`**（多产品混排长文）: **6**
- **`single_cn_phrase`**（单行纯中文短语）: **4**
- **`tagged_short_line`**（带【】的短行）: **2**
- **`other_long_or_mixed`**（其他长文/混合）: **2**

## 每类各 3 条示例（index）

### doc_theme_or_internal_id — 含主题id/内部id
- #31 `Santa Call（圣诞老人打视频主题）↵主题id：fun_santa_call_g_251128↵上线新封面，替换掉以前的`
- #33 `Xmas Glam（圣诞华服换脸玩法）↵主题id：festival_xmas_glam_f_251202↵焦点图文案：Shine bright in your dream Christmas dress today.`
- #36 `Kavi↵【Merry Christmas】Sweet Santa（圣诞甜心）↵Toki↵【Merry Christmas】↵新主题，包含 6 个特效，放在 Hot 后面；均为50积分↵主题id：video_christmas_g_251204↵主题描述语：Embrace the wonder of this Chri`

### batch_new_effects_list — 批量列举特效
- #41 `Toki&Kavi↵1、【 Merry Christmas】上线 7 个新特效：Tree Doll、Snow Bear、Magic Cat、Xmas Poster、Instant Tree、Stocking Sweetie、Snow Globe , 积分不一样↵↵2、【 AI Heat】上线 2 个新特效：Ripped`
- #43 `【Merry Christmas】上线 6 个新特效：Tree Doll、Snow Bear、Magic Cat、Xmas Poster、Instant Tree、Stocking Sweetie、Snow Globe , Santa Hat , 点券不一样`
- #47 `【Dress Up】新增2个特效，London Gent、Bloom Dress↵【AI Heat】新增2个特效，Ripped Abs、Flex Bicep↵【Merry Christmas】新增2个特效：Tree Doll、Instant Tree↵均为240点券`

### single_line_effect_name — 单行特效名片
- #0 `Bottle Dweller（瓶中精灵）`
- #1 `Winter Collage（冬日拼图）新主题`
- #2 `Enchanted Campus（哈利波特魔法学院）新主题`

### doc_with_link — 含HTTP/飞书链
- #29 `Santa Call（圣诞老人打视频主题）↵Vidu Q2↵参考生图，1080p，尺寸：3:4↵主题id：fun_santa_call_g_251128↵Evoke 以滑动切换的形式展示，与 Avatar的素材分开打包↵↵具体信息填入Avatar内容文档中：↵https://oqpc5k0pr2b.feishu.cn/`
- #42 `【Random Things】怪奇物语图片主题（图片场景是随机生成）↵主题id：fun_random_things_f_251211↵焦点图文案:Welcome to the mysterious world of Stranger Things.↵Evoke以滑动切换形式展示，素材已经分开打包↵https://oqp`
- #46 `【Christmas Doll】圣诞娃娃图片主题↵主题id：fun_christmas_doll_f_251212↵焦点图文案:Welcome to the wonderful world of Christmas.↵Evoke以滑动切换形式展示，素材已经分开打包↵https://oqpc5k0pr2b.feishu.`

### ops_changelog_block — 【】工单块
- #37 `【特效优化】Vidu Q2 模型做了升级，能更好的生成性感动作。麻烦给 Evoke：AI Heat 12 个特效更新提示词，都统一为图生视频，5s，720p，210 点券，打开音效。`
- #39 `【特效优化】↵1.Merry Christmas：Bear Hug 提示词更新。↵2.Banana Pro：Snow global、Pop Me、Plush Me 提示词更新。`
- #40 `【新增特效】↵1.Merry Christmas：Santa's Hug、Santa's Gift，210点券↵2.AI Heat:Bed Vibes、Camera Kiss，210点券`

### few_lines_theme_desc — 少行主题说明
- #79 `【Kavi 新特效】↵下架 Photo Revival 当前所有特效，上线的6 个新特效`
- #87 `【Evoke 更新提示词】↵Hello 2026 主题 10 号特效 Confetti Pop 更新提示词`
- #89 `【Evoke 更新提示词】↵Movie Set 主题，1号、2号特效更新提示词`

### ops_surface_meta — 运维表面操作
- #32 `AI Love视频封面优化，替换掉以前的`
- #45 `【新特效】↵下架 AI Heat 序号 1～20 的20个特效，重新上线更新后的20个特效，模型配置、积分都有更新`
- #90 `【Evoke 更新 Qwen 模型】↵1. Love Duo 主题，序号 7、9、11、13 共 4 个特效，更新为 Qwen2.6 模型，同时更新提示词。其中，11 号特效去掉用户图裁剪↵2. Memory Color 主题，全部 6 个特效更新为 Qwen2.6 模型，同时更新提示词。`

### product_line_batch_spec — 产品行+批次规格
- #27 `Evoke：Scene Show 新增 4 个特效，使用 Vidu Q2 的参考生视频，5s，720p，尺寸 3:4，需要打开 BGM`
- #28 `Evoke：Merry Christmas 新增 4 个特效，使用 Vidu Q2 的参考生视频，5s，720p`
- #30 `Evoke Scene Show 新增 2 个特效：Light Flow、Big Boom，都打开 BGM`

### multi_product_mixed — 多产品混排长文
- #38 `Evoke↵【Merry Christmas】Christmas Card（圣诞贺卡），240点券        ↵【Comic Chaos】Shake Fever（摇动热潮）430 点券、Kitchen Shuffle（厨房企鹅舞）290 点券，均设定为 540p↵【Pet Part】Balloon Parade（游`
- #60 `【Kavi 新特效】↵1. 下降 AI Heat 已有的全部特效↵2. 上线新的 24 个特效`
- #63 `Kavi&Toki↵【Pet Party】上新两个特效：Pet Sketch（宠物线稿）、Reindeer Magic（驯鹿魔法）↵【Merry Christmas】上新一个特效：Xmas Globe（圣诞水晶球）`

### single_cn_phrase — 单行纯中文短语
- #18 `黑白线稿漫画`
- #19 `Q版漫画表情包`
- #20 `漫画背景`

### tagged_short_line — 带【】的短行
- #34 `【Merry Christmas】Christmas Card（圣诞贺卡）`
- #35 `【Pet Party】Balloon Parade（游行气球）`

### other_long_or_mixed — 其他长文/混合
- #56 `Evoke&Toki&Kavi↵更新【Santa Call】主题的4张内置图`
- #61 `Evoke&Toki&Kavi↵更新【Santa Call】主题的4张内置图以及视频封面`
