# VE Workflow Traceability Runbook

This document is the handoff entrypoint for tracing, diagnosing, and restarting
the Video Enhancer (VE) production workflow. It is written so a future Codex
skill can start here before touching code or rerunning jobs.

## Scope And Rules

- Work from repo root: `/Users/oliver/guru/ua素材`.
- Use `.venv/bin/python`, not bare `python`.
- Treat `target_date` as the trace id. All daily artifacts use the prefix
  `workflow_video_enhancer_{target_date}`.
- Diagnose from artifacts first. Do not claim a stage succeeded because a
  command was requested or a script exists.
- Keep VE separate from Arrow2 unless the user explicitly widens scope.
- Be careful with writes. Reading JSON, SQLite, and reports is safe; syncing to
  Bitable or sending IM/webhook cards is a production side effect.
- The current checkout has no general `workflow_video_enhancer_steps.py`
  command. Use the stage commands below, or rerun the full workflow.

## Golden Path

Daily cron entry:

```bash
scripts/cron_ai_video_enhancer_daily.sh >> logs/cron_video_enhancer.log 2>&1
```

Manual full run for a known date:

```bash
.venv/bin/python scripts/run_video_enhancer.py --date YYYY-MM-DD
```

Manual full run for selected products:

```bash
.venv/bin/python scripts/run_video_enhancer.py \
  --date YYYY-MM-DD \
  --products "Product A,Product B"
```

Side-effect-reduced local run:

```bash
.venv/bin/python scripts/run_video_enhancer.py \
  --date YYYY-MM-DD \
  --no-bitable-sync \
  --no-card \
  --no-wecom \
  --no-sheet
```

Cron runs the full VE workflow first. If `FEISHU_DAILY_PUSH_CHAT_ID` is
configured, it then runs the Haopeng TopN IM push.

## First Triage Checklist

Set the date and inspect the known artifacts:

```bash
export TARGET_DATE=YYYY-MM-DD
PREFIX="workflow_video_enhancer_${TARGET_DATE}"

ls -lh data/${PREFIX}* \
  data/video_analysis_${PREFIX}_raw.json \
  reports/${PREFIX}* 2>/dev/null
```

Check the local VE database before reasoning about history:

```bash
sqlite3 data/video_enhancer_pipeline.db "
PRAGMA quick_check;
SELECT target_date, COUNT(*)
FROM daily_creative_insights
GROUP BY target_date
ORDER BY target_date DESC
LIMIT 10;
SELECT COUNT(*), MIN(first_target_date), MAX(first_target_date)
FROM creative_library;
"
```

Regenerate read-only reports if the stage artifacts already exist:

```bash
.venv/bin/python -m ua_workflows.video_enhancer.acceptance \
  --date "${TARGET_DATE}"

.venv/bin/python -m ua_workflows.video_enhancer.flow_report \
  --date "${TARGET_DATE}" \
  --no-send
```

Read the generated summaries:

- `data/${PREFIX}_acceptance.json`
- `data/${PREFIX}_flow_report.json`
- `reports/${PREFIX}_acceptance.md`
- `reports/${PREFIX}_flow_report.md`

## Stage Map

Each stage below lists what to trace, how to judge success, and how to restart.

### 0. Environment And Config

Inputs:

- `.env`
- `config/ai_product.json`
- `VIDEO_ENHANCER_BITABLE_URL`
- `FEISHU_APP_ID` / `FEISHU_APP_SECRET`
- Guangdada login state used by the shared crawl helpers

Trace commands:

```bash
git status --short --branch
test -x .venv/bin/python && .venv/bin/python --version
.venv/bin/python - <<'PY'
from ua_workflows.shared.config import load_project_env
load_project_env()
print("project env loaded")
PY
```

Failure signs:

- `.venv/bin/python` missing.
- Feishu credentials are absent or belong to the wrong app.
- Bitable URL is missing or does not include a table id.
- Worktree/branch is not the expected production branch for the run.

Restart action:

- Fix environment first. Do not rerun crawl or sync until credentials and
  branch/worktree are known.

### 1. Competitor List Sync

Main path:

- `scripts/run_video_enhancer.py` calls
  `sync_competitor_config_if_enabled()` before crawl.

Standalone dry run:

```bash
.venv/bin/python scripts/run_ve_competitor_list_sync.py --dry-run
```

Standalone apply:

```bash
.venv/bin/python scripts/run_ve_competitor_list_sync.py
```

Trace:

- Terminal/log lines beginning with `[competitor-list]`.
- `config/ai_product.json` if the sync applied.

Success:

- The log says it synced official competitors from Bitable, or it clearly says
  it fell back to local config.

Failure signs:

- Feishu permission errors.
- `竞品list` table not found.
- Empty product/appid rows.

Restart action:

- If only competitor sync failed and local config is acceptable, continue with
  the fallback.
- If the user expects Bitable to be source of truth, fix permissions/table name
  and rerun the full workflow or standalone sync before crawl.

### 2. Crawl

Main path:

```bash
.venv/bin/python -m ua_workflows.video_enhancer.crawl \
  --target-date "${TARGET_DATE}" \
  --output-prefix "${PREFIX}"
```

With selected products:

```bash
.venv/bin/python -m ua_workflows.video_enhancer.crawl \
  --target-date "${TARGET_DATE}" \
  --output-prefix "${PREFIX}" \
  --products "Product A,Product B"
```

Artifacts:

- `data/${PREFIX}_raw.json`
- `data/${PREFIX}_reduce.json`

Trace fields in raw:

- `target_date`
- `total`
- `items`
- `filter_report.per_product`
- `ui_date_range`

Quick count:

```bash
.venv/bin/python - <<'PY'
import json, os
date = os.environ["TARGET_DATE"]
p = f"data/workflow_video_enhancer_{date}_raw.json"
d = json.load(open(p, encoding="utf-8"))
print("target_date", d.get("target_date"))
print("items", len(d.get("items") or []))
for product, row in sorted(((d.get("filter_report") or {}).get("per_product") or {}).items()):
    print(product, row)
PY
```

Success:

- Raw exists.
- `items` is nonzero for expected active products.
- `filter_report.per_product` explains page cards, clicked details, captured
  materials, target-date hits, and crawl filters.

Failure signs:

- Raw missing.
- Zero or unexpectedly low `items`.
- Many `advertiser_excluded`, `date_filtered`, or `resume_excluded` counts.
- Login or captcha screenshots in `data/guangdada_*`.

Restart action:

- For one product, rerun crawl or full workflow with `--products`.
- If login/captcha blocked crawl, resolve login first, then rerun.
- If raw is bad, downstream artifacts from the same date are suspect.

### 3. Cover And Visual Dedupe

Main path:

- The full workflow reads `data/${PREFIX}_raw.json`, applies cover filtering,
  writes the filtered raw back to the same file, and writes separate reports.

Artifacts:

- `data/${PREFIX}_cover_style_intraday.json`
- `data/${PREFIX}_filter_step3_cover.json`
- `data/${PREFIX}_crawl_product_retention.json`

Trace fields:

- `input_count`
- `output_count`
- `removed_total`
- `cross_day_fingerprint_removed`
- `per_appid[].removed`
- `crawl_product_retention_report.summary`
- `crawl_product_retention_report.per_product`

Success:

- `filter_step3_cover.json` exists.
- The filtered raw item count matches the cover report output.
- Per-product retention explains how many stayed after crawl and cover.

Failure signs:

- Cover report missing after a full run.
- Very high cover removal rate without a known reason.
- `history_refresh` expected but absent for older resurfaced clusters.

Restart action:

- There is no standalone cover-only CLI in this checkout. Rerun the full
  workflow for the date, or rerun with `--skip-cover-dedupe` only when the user
  explicitly wants to bypass visual filtering for diagnosis.

Bypass command:

```bash
.venv/bin/python scripts/run_video_enhancer.py \
  --date "${TARGET_DATE}" \
  --skip-cover-dedupe \
  --no-bitable-sync \
  --no-card \
  --no-wecom \
  --no-sheet
```

### 4. Raw Database Storage

Main path:

- Full workflow upserts raw data into `daily_creative_insights`.
- Full workflow upserts material identity/history into `creative_library`.

Artifacts:

- `data/video_enhancer_pipeline.db`
- `data/cover_embedding_report_${TARGET_DATE}.json` when cover embeddings run.

Trace commands:

```bash
sqlite3 data/video_enhancer_pipeline.db "
SELECT target_date, COUNT(*)
FROM daily_creative_insights
WHERE target_date='${TARGET_DATE}';
SELECT COUNT(*)
FROM creative_library
WHERE first_target_date='${TARGET_DATE}';
"
```

Success:

- `daily_creative_insights` has rows for `target_date`.
- `creative_library` has new or updated rows.
- `PRAGMA quick_check` returns `ok`.

Failure signs:

- DB has no rows for the date after crawl succeeded.
- SQLite quick check fails.
- Raw exists but DB counts are stale.

Restart action:

- Rerun the full workflow for the date. If the raw is already known good and
  rerun risk is high, inspect `pipeline.py` before attempting a manual DB-only
  replay because there is no supported DB-only CLI.

### 5. Analysis Queue And Dedup Before LLM

Main path:

- The full workflow builds the LLM queue after raw storage and history dedupe.

Artifacts:

- `data/${PREFIX}_analysis_dedup_report.json`
- `data/${PREFIX}_analysis_queue_report.json`
- `data/${PREFIX}_raw_pending_analysis.json`

Trace fields:

- `total.raw_after_cover`
- `total.dedup_removed`
- `total.after_dedup`
- `total.cache_reused`
- `total.ineligible_total`
- `total.llm_queued`
- `per_product`

Success:

- Queue report exists.
- `llm_queued + cache_reused + ineligible_total + dedup_removed` explains the
  post-cover population.
- Pending raw contains only items that should newly call the LLM.

Failure signs:

- Queue report missing but cover artifacts exist.
- `llm_queued` is unexpectedly zero.
- Ineligible reasons spike for media URL or unsupported material issues.

Restart action:

- If this stage failed before analysis, rerun the full workflow.
- If most items were skipped because historical analysis was reused, verify
  this is expected before forcing a clean rerun.

### 6. LLM Analysis

Standalone analysis command:

```bash
.venv/bin/python -m ua_workflows.video_enhancer.analyze \
  --input "data/${PREFIX}_raw_pending_analysis.json" \
  --output "data/video_analysis_${PREFIX}_raw.json"
```

Artifacts:

- `data/video_analysis_${PREFIX}_raw.json`
- `data/${PREFIX}_analysis_failed.json`

Trace fields:

- `analyzed_items`
- `new_success`
- `new_failed`
- `reused_existing`
- `results[].analysis`
- `results[].play_fingerprint`
- `results[].template_fingerprint`
- `results[].material_tags`

Success:

- Analysis JSON exists.
- Failed count is zero or success rate is at least 90%.
- Structured fields are populated for successful items.

Failure signs:

- `[ERROR]` analysis rows.
- Empty `analysis`.
- LLM quota or media preprocessing errors.
- Success rate below 90%; full workflow should stop before sync/push.

Restart action:

- If only analysis failed and pending raw is valid, rerun standalone analysis.
- After standalone analysis, rerun full workflow if DB sync/filter/push stages
  must be rebuilt from the merged analysis output.
- Do not manually edit analysis JSON unless the user explicitly asks.

### 7. Content And Business Filters

Main path:

- Full workflow applies content filters and optional business filters after
  merging new and cached analysis.

Artifacts:

- `data/video_analysis_${PREFIX}_raw.json`
- `data/${PREFIX}_filter_step_launched_effects.json`
- `reports/ve_filter_review_${TARGET_DATE}.html`

Trace fields in analysis results:

- `exclude_from_bitable`
- `exclude_from_cluster`
- `adult_content_filter_match`
- `human_photo_effect_filter_match`
- `effect_embedding_duplicate_match`
- `embedding_duplicate_candidate`
- `launched_effect_match`
- `material_tags`

Success:

- Exclusions are explainable by explicit fields and tags.
- Review dashboard exists or failure is logged as non-blocking.

Failure signs:

- Unexpectedly high hard exclusions.
- All or most records excluded before sync.
- Review dashboard generation failed and no alternate report explains the
  filter result.

Restart action:

- Rerun full workflow after changing only environment switches when requested.
- Common diagnostic switches are `INTRADAY_EFFECT_FILTER_ENABLED`,
  `OLD_EFFECT_BITABLE_FILTER_ENABLED`, `EFFECT_EMBEDDING_DUP_FILTER_ENABLED`,
  `EMBEDDING_DUP_CANDIDATE_ENABLED`, and `LAUNCHED_EFFECTS_ENABLED`.

### 8. Bitable Sync

Main path:

```bash
.venv/bin/python -m ua_workflows.video_enhancer.sync \
  --url "$VIDEO_ENHANCER_BITABLE_URL" \
  --raw "data/${PREFIX}_raw.json" \
  --analysis "data/video_analysis_${PREFIX}_raw.json" \
  --suggestion-json "data/ua_suggestion_${PREFIX}.json" \
  --suggestion-md "data/ua_suggestion_${PREFIX}.md" \
  --sync-target raw \
  --no-card
```

Artifacts:

- `data/${PREFIX}_sync_report.json`
- Bitable records in `VIDEO_ENHANCER_BITABLE_URL`

Trace fields:

- `summary`
- `per_product[].after_hard_exclusion`
- `per_product[].after_template_dedup`
- `per_product[].synced_records`
- `per_product[].removed_reasons`

Success:

- Sync report exists.
- `synced_records` matches the expected main-table write count.
- Flow report's `synced_records` agrees with sync report.

Failure signs:

- Feishu 403/91403 permissions.
- Field conversion errors, especially select/multi-select payloads.
- Missing Bitable fields.
- Sync report missing after analysis succeeded.

Restart action:

- If raw and analysis are good, rerun the standalone sync command.
- Use `--sync-target raw` for main table only.
- Avoid `--sync-target both` unless the cluster table and suggestion card are
  known good.
- After rerun, regenerate acceptance and flow report with `--no-send`.

### 9. Daily Card, Multichannel Push, And Push DB

Main path:

- Full workflow runs `push_feishu`, writes `daily_ua_push_content`, and runs
  `push_multichannel`.

Standalone daily card:

```bash
.venv/bin/python -m ua_workflows.video_enhancer.push_feishu \
  --date "${TARGET_DATE}"
```

Standalone multichannel:

```bash
.venv/bin/python -m ua_workflows.video_enhancer.push_multichannel \
  --date "${TARGET_DATE}" \
  --raw "data/${PREFIX}_raw.json" \
  --suggestion-md "data/ua_suggestion_${PREFIX}.md" \
  --suggestion-json "data/ua_suggestion_${PREFIX}.json"
```

Trace:

- `data/ua_suggestion_${PREFIX}.json`
- `data/ua_suggestion_${PREFIX}.md`
- `daily_ua_push_content` table
- Terminal/log push response

Success:

- Card command succeeds or is intentionally skipped with `--no-card`.
- Push DB has expected rows when suggestions are valid.

Failure signs:

- Suggestion JSON missing or `skipped_llm=true`.
- Webhook/IM send failure.
- Empty direction cards when clusters are expected.

Restart action:

- If Bitable sync already succeeded, rerun only the failed push command.
- If suggestion generation failed, rerun the full workflow or inspect
  `ua_workflows.video_enhancer.suggestions` before sending cards.

### 10. Haopeng TopN

Cron behavior:

- `scripts/cron_ai_video_enhancer_daily.sh` runs TopN only after the main VE
  workflow finishes and `FEISHU_DAILY_PUSH_CHAT_ID` is configured.

Generate only:

```bash
.venv/bin/python scripts/run_ve_haopeng_topn_push.py \
  --date "${TARGET_DATE}" \
  --top-n 10 \
  --generate-only
```

Generate and send IM:

```bash
.venv/bin/python scripts/run_ve_haopeng_topn_push.py \
  --date "${TARGET_DATE}" \
  --top-n 10 \
  --send-mode im \
  --chat-id "$FEISHU_DAILY_PUSH_CHAT_ID"
```

Artifacts:

- `data/haopeng_topn_experiments/${TARGET_DATE}_label_prior.json`

Success:

- JSON has target-day candidates and historical feedback count.
- IM send prints a Feishu message id.

Failure signs:

- No target candidates from Bitable.
- No effective historical feedback.
- Missing `FEISHU_DAILY_PUSH_CHAT_ID`.
- Feishu IM permission failure.

Restart action:

- If the main table is already synced, rerun TopN only.
- Use `--generate-only` first when diagnosing model or Bitable read behavior.

### 11. Acceptance And Flow Report

Acceptance:

```bash
.venv/bin/python -m ua_workflows.video_enhancer.acceptance \
  --date "${TARGET_DATE}"
```

Flow report without sending:

```bash
.venv/bin/python -m ua_workflows.video_enhancer.flow_report \
  --date "${TARGET_DATE}" \
  --no-send
```

Artifacts:

- `data/${PREFIX}_acceptance.json`
- `reports/${PREFIX}_acceptance.md`
- `data/${PREFIX}_flow_report.json`
- `reports/${PREFIX}_flow_report.md`

Success:

- Acceptance status is `pass`.
- Flow report status is `ok` or an explainable `alert`.
- `source_files` in flow report point to the stage artifacts actually used.

Failure signs:

- Hard acceptance issues.
- Flow report missing stage sources.
- Product-level volume alert without an understood cause.

Restart action:

- Reports are read-only except optional notification. Regenerate them freely
  with `--no-send`.
- Use the failing stage in `acceptance.json` or `flow_report.json` as the
  restart boundary.

## Restart Decision Tree

1. If environment or credentials are wrong, fix them first.
2. If raw crawl is missing or clearly wrong, rerun crawl/full workflow.
3. If raw is good but cover/filter reports are missing, rerun full workflow.
4. If pending analysis exists and only LLM failed, rerun analysis; otherwise
   rerun full workflow.
5. If analysis is good but Bitable sync failed, rerun standalone sync.
6. If Bitable sync is good but push failed, rerun only the failed push.
7. If Haopeng TopN failed after main VE succeeded, rerun only TopN.
8. After any restart, regenerate acceptance and flow report with `--no-send`
   before reporting success.

## Common Failure Playbooks

### Crawl Volume Is Low

Check:

- `data/${PREFIX}_raw.json`
- `filter_report.per_product`
- `data/${PREFIX}_crawl_product_retention.json`
- `data/guangdada_*` screenshots

Likely causes:

- Guangdada login/captcha.
- UI date filter did not apply.
- Advertiser mismatch.
- Target date has genuinely low supply.
- Page scroll/detail click failed for one product.

Restart:

```bash
.venv/bin/python scripts/run_video_enhancer.py \
  --date "${TARGET_DATE}" \
  --products "Problem Product" \
  --no-bitable-sync \
  --no-card \
  --no-wecom \
  --no-sheet
```

### Cover Removal Is High

Check:

- `data/${PREFIX}_cover_style_intraday.json`
- `data/${PREFIX}_filter_step3_cover.json`
- `reports/ve_filter_review_${TARGET_DATE}.html`
- `data/${PREFIX}_acceptance.json`

Likely causes:

- True repeated templates.
- Threshold too aggressive for the day.
- Historical resurfacing should be `history_refresh`.
- Product had many near-identical covers.

Restart:

- First regenerate reports and inspect examples.
- Only bypass cover dedupe for diagnosis, not as default production behavior.

### Analysis Failed

Check:

- `data/${PREFIX}_raw_pending_analysis.json`
- `data/video_analysis_${PREFIX}_raw.json`
- `data/${PREFIX}_analysis_failed.json`
- OpenRouter/key meter logs in terminal output.

Likely causes:

- LLM quota.
- Media URL preprocessing.
- Unsupported material type.
- Provider timeout.

Restart:

```bash
.venv/bin/python -m ua_workflows.video_enhancer.analyze \
  --input "data/${PREFIX}_raw_pending_analysis.json" \
  --output "data/video_analysis_${PREFIX}_raw.json"
```

Then rerun downstream stages through the full workflow or standalone sync,
depending on where the original run stopped.

### Bitable Sync Failed

Check:

- Terminal/log error.
- `data/${PREFIX}_sync_report.json` if it exists.
- Feishu app id and permissions.
- Field types for select and multi-select fields.

Likely causes:

- Wrong app credentials in process environment.
- Missing Bitable fields.
- Multi-select payload shape mismatch.
- Expired or insufficient Feishu permissions.

Restart:

```bash
.venv/bin/python -m ua_workflows.video_enhancer.sync \
  --url "$VIDEO_ENHANCER_BITABLE_URL" \
  --raw "data/${PREFIX}_raw.json" \
  --analysis "data/video_analysis_${PREFIX}_raw.json" \
  --suggestion-json "data/ua_suggestion_${PREFIX}.json" \
  --suggestion-md "data/ua_suggestion_${PREFIX}.md" \
  --sync-target raw \
  --no-card
```

Then:

```bash
.venv/bin/python -m ua_workflows.video_enhancer.acceptance --date "${TARGET_DATE}"
.venv/bin/python -m ua_workflows.video_enhancer.flow_report --date "${TARGET_DATE}" --no-send
```

### Push Failed

Check:

- Whether Bitable sync already succeeded.
- `data/ua_suggestion_${PREFIX}.json`
- `data/ua_suggestion_${PREFIX}.md`
- Webhook or IM configuration.

Restart:

- Rerun only the failed push command. Do not resync Bitable unless sync was
  also bad.

### Remote Snapshot Sync Is Needed

VE-only sync from Mac mini:

```bash
scripts/sync_ve_data_from_remote.sh
```

Verification:

```bash
sqlite3 data/remote_snapshots/ve/data/video_enhancer_pipeline.db "
PRAGMA quick_check;
SELECT MAX(target_date) FROM daily_creative_insights;
"
```

Rules:

- Report SSH failure plainly.
- Do not imply a fresh remote pull unless the script actually ran.
- This sync explicitly excludes Arrow2.

## Skill Entry Contract

A future `ve-workflow-traceability` skill should start with this sequence:

1. Identify `target_date` and whether the user wants read-only diagnosis or a
   production restart.
2. Run the first triage checklist.
3. Read `acceptance.json` and `flow_report.json` if available; regenerate with
   `--no-send` if stage artifacts exist.
4. Pick the earliest failed or missing stage from the stage map.
5. Use the stage restart command with the smallest production side effect.
6. Regenerate acceptance and flow report.
7. Report in execution order: what was checked, what was rerun, what changed,
   and what remains uncertain.
