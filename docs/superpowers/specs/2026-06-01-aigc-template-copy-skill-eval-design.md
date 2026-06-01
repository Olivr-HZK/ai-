# AIGC Template Copy Skill Evaluation Design

## Goal

Build a small evaluation workflow for the `aigc-template-copy` skill using real Video Enhancer competitor materials. The first run will select the latest five Feishu Bitable records where `浩鹏接受情况` is `采纳` and the material has a usable video URL. The workflow produces prompt-review artifacts only; the user will run Video Lab manually.

## Non-Goals

- Do not generate videos automatically.
- Do not call Video Lab, Kling 3.0, or Vidu Q3 Pro.
- Do not write results back to Feishu Bitable.
- Do not automatically edit the skill file.
- Do not include Arrow2 records.

## Inputs

- Feishu credentials from the existing project environment.
- The Video Enhancer Bitable URL from `VIDEO_ENHANCER_BITABLE_URL`.
- The packaged skill at `/Users/oliver/Downloads/aigc-template-copy-skill.zip`, specifically `aigc-template-copy/SKILL.md`.
- Existing local project helpers for Bitable access and media URL extraction where practical.

## Sample Selection

The script will inspect the live Bitable schema instead of assuming field IDs. It will look for:

- Reviewer field: `浩鹏接受情况`
- Accepted value: `采纳`
- Date or creation ordering fields already present in the table
- Media fields such as `视频链接`, `视频`, `封面图链接`, title/body/core selling point/play-label fields where available

Selection rules:

1. Filter records where `浩鹏接受情况 = 采纳`.
2. Sort by the freshest reliable date field available, falling back to Feishu record ordering if needed.
3. Keep only records with a usable video URL.
4. Continue scanning until five records are collected or the table is exhausted.
5. Preserve skipped-record reasons in the report.

## Prompting Workflow

For each selected material, the script will build one model request that embeds the relevant `aigc-template-copy` rules and the record metadata. The request asks the model to produce a structured reverse-analysis result:

- Template mode: `图片复刻` or `视频模板`
- Segment judgment: why this mode was chosen and which UI, watermark, tutorial, ad, caption, or irrelevant parts were excluded
- Image prompt or first-frame image prompt
- For video-template mode only: video motion prompt, dynamic duration, and recommended generation duration

The prompt must preserve the skill's core distinction:

- `图片复刻` for static effect images, posters, before/after images, app result pages, carousels, and static montages
- `视频模板` for clear dynamic value such as subject motion, expression changes, hand/body movement, camera movement, transitions, or environmental interaction

## Output Artifacts

For each run date, output to:

`data/aigc_template_copy_eval/YYYY-MM-DD/`

Artifacts:

- `selected_materials.json`: the five selected records plus skipped-record reasons
- `skill_eval_results.json`: structured model outputs for each material
- `skill_eval_review.csv`: manual review sheet for Video Lab results
- `skill_eval_report.md`: human-readable review packet with source links and copyable prompts
- Optional `skill_eval_report.html`: lightweight local preview if the Markdown report is not enough for video/link review

The CSV will include manual fields for:

- `mode_correct`
- `segment_correct`
- `irrelevant_elements_included`
- `template_text_wrongly_removed`
- `kling30_score`
- `vidu_q3_pro_score`
- `video_lab_satisfied`
- `failure_reason`
- `skill_rule_suggestion`

## Skill Optimization Loop

The first script run only prepares evaluation artifacts. After the user manually fills Video Lab results, a later pass can summarize failures into skill-change suggestions grouped by:

- Mode classification errors
- Wrong segment or wrong copied region
- Irrelevant UI/ad/tutorial/caption/watermark elements included
- Useful template text or visual elements removed incorrectly
- Weak or non-executable video motion prompts
- Duration mismatch

Any actual `SKILL.md` edits require a separate confirmation from the user.

## Error Handling

- If Bitable configuration is missing, fail with the exact missing environment variable.
- If the live table does not contain `浩鹏接受情况`, fail and print the available reviewer-like fields.
- If fewer than five accepted video records exist, produce artifacts for the records found and state the shortfall.
- If a model call fails for a record, keep that record in the results with an error status and continue unless all calls fail.
- If the skill zip cannot be read, fail before querying or model work.

## Verification

Implementation should include focused tests for:

- Skill zip loading
- Bitable URL parsing
- Accepted-record filtering
- Video URL extraction and skip reasons
- Prompt construction preserving image-vs-video mode rules
- Output artifact shape

Manual verification for the first real run:

1. Confirm the script selected five latest `浩鹏接受情况 = 采纳` video records.
2. Confirm each report item contains the original video link and generated prompt artifacts.
3. Confirm no Video Lab call, Feishu writeback, or skill-file edit occurred.
