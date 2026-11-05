# 图片复刻完整提示词：产品名、图标、字标同时替换

规则版本：`afizzy-full-brand-replacement-v2`。本文件供用户明确触发图片复刻后使用，不构成自动启动、历史重算或生成服务授权。与 [image-replication.md](image-replication.md) 一起读取。

## 每张图片先建立替换清单

实际查看原图，并绑定原文件 SHA256、广告主、广告ID、record_id。逐处记录位置、类型、原文/原标识、目标文字/目标标识。同一品牌出现三次就记录三处，不能只记录主Logo。至少检查：主图标、产品字标、正文、对话/界面、下载与聊天按钮、页脚小字、重复小图标。广告主名称用于辅助识别，不意味着把画面中所有专名都替换。

英文大小写及画面中已确认的本地化品牌名均纳入，例如 Tipsy/TIPSY、zeta、Emochi/emochi、LoveyDovey/卿卿我我、SeaHaze。别名必须由实际原图或已确认产品信息支持，不猜未知别名。角色名、故事标题、创作者署名、App Store/Google Play标识及AI免责声明不属于竞品产品品牌。

为每项写可核对的完整改文，例如：

| 位置/类型 | 原文或标识 | 替换目标 |
| --- | --- | --- |
| 左上/主品牌 | Emochi字标及黄色脸形图标 | `afizzy`及认可AF图标 |
| 下方/下载文案 | `DOWNLOAD emochi NOW!` | `DOWNLOAD afizzy NOW!` |
| 正文 | `Your Pirate Captain on Tipsy.` | `Your Pirate Captain on afizzy.` |
| 按钮/日文 | `この続きを、zetaで。` | `この続きを、afizzyで。` |
| 按钮/日文 | `zetaで彼に話しかける` | `afizzyで彼に話しかける` |
| 页脚 | `Choose your ending on Emochi.` | `Choose your ending on afizzy.` |

这只是示例；每张只填写该原图真实存在的项目，不能把示例场景或文案移植到别的原图。正文/按钮中只替换品牌文字，不在句子中额外插入AF图标。

## 每次调用必须使用的完整通用提示词

下面通用提示词必须与本图逐处替换清单一起传入内置 `image_gen`。不得压缩为“换Logo”“replace logo”或仅传“保持原图不变”。先实际查看两张输入图；Image 1是原图，Image 2是 `assets/afizzy-approved-brand.png`，后者是带其他内容的认可样稿，只取其左上品牌标识。占位符必须填完，最终实际提示全文逐图保存。

```text
Use case: precise-object-edit — complete product-brand replacement.
Image 1 is the EDIT TARGET: the original advertisement to edit.
Image 2 is an APPROVED AFIZZY BRAND REFERENCE ONLY. Use ONLY its small top-left geometric pale AF app icon on the navy-to-magenta gradient square and the lowercase afizzy wordmark. Do not transfer the reference image's person, tennis court, background, headline, or layout.

Primary request: replace ALL visible occurrences of the source competitor's PRODUCT BRAND in Image 1 with afizzy, including its written product name as well as its app icons and wordmarks. This is a complete product-brand replacement, not an icon-only change. Replacing an icon while leaving the old written brand anywhere in the ad is an incomplete result.

Source product identity: {{exact_advertiser_name}}.
Confirmed brand spellings visible in THIS source: {{observed_brand_spellings}}.
Target brand text, spelled exactly: afizzy (a-f-i-z-z-y), always lowercase.

MANDATORY LOCATION-BY-LOCATION REPLACEMENTS:
{{all_observed_brand_regions_with_exact_before_and_after_text}}

Check every listed location, including the main brand header, repeated small app icons, body copy, in-image app/chat UI, download or chat CTA buttons, and footer text. Replace every listed competitor name even when it is embedded inside a sentence or a button. For inline copy and buttons, replace ONLY the brand substring with afizzy and retain the surrounding sentence, punctuation, language, font style, color and placement. Do not insert an app icon into prose. For app icons and brand lockups, use the approved AF geometry and afizzy wordmark, matching the original footprint and visual treatment. If the per-image instructions specify a monochrome brand treatment, reproduce the same approved AF geometry in that color rather than inventing a new symbol.

PROTECTED NON-BRAND CONTENT:
{{actual_character_names_story_titles_credits_and_other_protected_text}}
These are not product-brand mentions; do not replace them with afizzy. Preserve App Store and Google Play badges and existing creator credits and AI disclosures. Preserve all non-brand ad copy verbatim in its original language. Do not translate, rewrite, or add product claims.

Preserve the original characters, identities, faces, expressions, hair, anatomy, clothing, pose, props, scene, illustration/photographic style, lighting, palette, composition and crop as closely as possible. Keep edits confined to the actual brand regions. Do not redesign the ad or change its story. Match the original brand's typography scale, placement and legibility while using afizzy.

Source dimensions and aspect ratio: {{width}} x {{height}}, {{aspect_ratio}}. Preserve the full original composition and aspect ratio. Do not add decorative borders or padding, crop people or copy, or turn a thin banner into a square poster.

Before finishing, verify every mandatory replacement: no old product name, wordmark or app icon remains at any listed location. Keep non-brand names and text intact. Output ONE finished standalone advertisement, not a comparison sheet or collage.
```

原图确实没有可见产品品牌时，清单写“原图无可见产品品牌”，将上面“MANDATORY LOCATION-BY-LOCATION REPLACEMENTS”内容替换为以下本图指令，不虚构已移除品牌的证据：

```text
There is no visible competitor product branding in this source. Add exactly ONE small approved AF icon plus lowercase afizzy wordmark at {{observed_free_corner}}, without covering people or existing text. Do not replace any character name, story title, dialogue label, creator credit, or headline. Keep all existing text unchanged.
```

## 必须按实际原图追加的逐图约束

- Emochi多处品牌：主图标/字标、末尾 `DOWNLOAD emochi NOW!`、`Choose your ending on Emochi.` 各自检查；重复黄色脸形图标若确认为产品图标也要替换，功能图标不盲目替换。
- zeta日文图：顶部字标与按钮内的 `zeta` 分别列清单；手机界面中的微型产品名也要检查。韩文角色标签 `구남친`（前男友）、`두쫀쿠 왕자`及故事标题不是zeta品牌，保留原文；不能把它们改成afizzy。
- LoveyDovey图：已确认的 `LoveyDovey`/`卿卿我我`和品牌图标一并替换；`Dear.B Studio`创作者署名保留。原版粉色单色标识可用同AF几何的粉色版，不重画人物。
- Tipsy图：图标和 `Tipsy`字标同时替换；正文里的 `on Tipsy`也要改。聊天角色名 `William`等保持原名，不能当竞品名替换。
- 以上约束按实际出现选择，不用固定品牌列表替代逐图检查。未知语言小字或无法确认是否品牌的标识，先补看/核实，不直接验收。

## 验收与针对性修正

逐张保存：原图身份/哈希、完整实际提示、源品牌逐处清单、生成回执、输出哈希、实际查看证据，以及每处替换是否完成、保护文字是否保留。只有主图标换好不足以通过。

验收必须同时满足：所有品牌文字已按清单替换；所有产品图标/字标已替换；afizzy拼写正确；正文与CTA无残留竞品名；角色名/故事标题/署名/免责声明未误改；主体画风和构图可接受；最终尺寸/比例按原流程核验。OCR辅助查找，实际画面复核为准。未检查或难辨认的项目记待核，不能默认通过。任一漏改、误改或保护项失败，状态为质量待修正，不上传、不标记复刻完成。

非安全质量修正可使用以下模板，输入1为待修正成品，输入2为原图（需要时另附认可品牌参考）；先实际查看，填入真实位置和文字，保留前版与修正回执：

```text
Precise local correction of Image 1. Image 2 is the ORIGINAL advertisement for content verification only.
At {{exact_location}}, {{describe_observed_error}}.
Change ONLY this region: {{exact_before}} -> {{exact_after}}.
{{explicit_protected_names_and_text_in_this_region}}
Keep all correct afizzy branding elsewhere and all other scene details and non-brand text unchanged. Do not add any other brand lockup. Output one finished ad with the same framing and aspect ratio.
```

安全拒绝只保存真实错误并跳过，不尝试通过改写提示、改工具或裁图绕过。请求状态未知先核对回执/已有输出，不盲目重发。

## 来源与能力边界

模板整理自本聊天2026-10-29已执行的品牌替换通用提示、Emochi 080的两处替换要求、zeta 108/118按钮改文，以及103/112角色标签和故事标题的纠正经验。仓库保存可移植模板，不把本机绝对路径、付费回执或媒体批量提交Git。它规定Agent如何调用内置工具和检查结果，不是独立API生成器，也不保证模型只改局部像素；仍需逐图实际验收。视频的Logo-only管线不适用于本图片流程。
