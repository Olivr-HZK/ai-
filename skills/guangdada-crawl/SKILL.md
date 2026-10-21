---
name: guangdada-crawl
description: Use when a user asks Codex to crawl Guangdada charts, advertiser/app creatives, or a specific Guangdada ad.
---

# Guangdada Crawl

Use the installable `guangdada` CLI for raw Guangdada crawling. This skill is only for crawl collection. Do not route VE sync, Arrow2 sync, LLM analysis, Bitable writes, dashboard generation, or push notifications through this skill.

If a teammate has installed the optional MCP surface with `python -m pip install -e ".[mcp]"`, they can run `guangdada-mcp`. Treat it as a thin wrapper over the same CLI and job files, not as a separate crawler implementation.

## Choose The Command

Before a teammate's first crawl, check local setup:

```bash
guangdada --json doctor
```

To let Codex discover copy-pasteable commands and boundaries:

```bash
guangdada --json examples
```

Then check local auth state:

```bash
guangdada auth status
```

If there is no auth state and the user wants to prepare login state:

```bash
guangdada auth login --headed
```

If the user asks for a chart or榜单:

```bash
guangdada crawl chart --chart new --category AI视频 --limit 50 --cover-dedupe
```

Use `--chart hot` for每周热门榜 and `--chart surge` for每周飙升榜.

If the user asks for a specific advertiser, product, app, or package:

```bash
guangdada crawl advertiser --keyword com.example.app --label "Example App" --date YYYY-MM-DD --cover-dedupe
```

Advertiser crawls start from Guangdada's default game tab. Do not pass a game switch. If the user asks for a tool/AI tool advertiser, explicitly switch to the tool tab:

```bash
guangdada crawl advertiser --keyword com.example.app --label "Example Tool" --date YYYY-MM-DD --is-tool --cover-dedupe
```

`--date` defaults to `--date-mode first-seen`, meaning creatives first seen on that date. Do not filter out resumed/re-run creatives if their `first_seen` matches the target date.

When `--date` is supplied, advertiser crawls fill Guangdada's custom date selector with that single UTC+8 date before searching. Without `--date`, they use the shortcut time buttons; default is `--time-window 7d`, with `7d / 30d / 90d / 1y` matching `7天 / 30天 / 90天 / 近1年`.

Before chart or advertiser crawls, the user must explicitly choose whether to run VE-style CLIP cover dedupe. Do not assume. Use `--cover-dedupe` only after the user confirms dedupe should run; use `--no-cover-dedupe` when the user wants untouched raw crawl rows. If dedupe runs, treat `raw.items` as the kept representatives and inspect `cover_style_intraday_report` for input/output counts, removed rows, and cluster details.

By default, chart, advertiser, and single-ad crawls try to download video creatives into the job directory and package successful downloads into `videos.zip`. Inspect `media_manifest.json` for per-item download status and use `raw.items[].local_video_path` when present. If the user only wants URL/raw artifacts or a lightweight dry run, pass `--no-download-videos`.

If the user asks for a wider Guangdada window such as 30 days:

```bash
guangdada crawl advertiser --keyword com.example.app --label "Example App" --time-window 30d --cover-dedupe
```

If the user asks for all creatives active on that date, pass:

```bash
guangdada crawl advertiser --keyword com.example.app --label "Example App" --date YYYY-MM-DD --date-mode active --cover-dedupe
```

If the user gives an ad key or Guangdada detail URL:

```bash
guangdada crawl ad --ad-key AD_KEY --type 2
```

If the user also has `search_flag` or the URL contains `search_flag`, fetch `detail-v2`:

```bash
guangdada crawl ad --ad-key AD_KEY --type 2 --app-type 2 --search-flag SEARCH_FLAG
guangdada crawl ad --url "https://guangdada.net/modules/creative/display-ads/detail?id=AD_KEY&type=2&search_flag=SEARCH_FLAG"
```

If `search_flag` is missing, report that the job only produced a detail URL seed and that `summary.json` should show `detail_v2_status=skipped_missing_search_flag`.

Inspect results with:

```bash
guangdada job list --limit 5
guangdada job show JOB_ID
guangdada job artifacts JOB_ID
```

If the user asks for the latest result and no job id is provided, run `guangdada job list --limit 5` first, then inspect the relevant job with `guangdada job artifacts JOB_ID` and `guangdada job show JOB_ID`.

MCP tool names mirror these commands:

- `doctor`
- `examples`
- `auth_status`
- `start_chart_crawl`
- `start_advertiser_crawl`
- `start_ad_crawl`
- `list_recent_jobs`
- `get_job_status`
- `get_job_artifacts`

## Safety Boundary

- 默认只产 raw/job artifact.
- 不要默认写入 VE.
- 不要默认写入 Arrow2.
- 不要默认写入 Feishu 多维表格.
- 不要触发 LLM 分析或同步流程.
- 不要破解验证码. When human verification appears, tell the user to complete it manually and rerun or resume the crawl.

## Expected Output

Every crawl command prints JSON with `job_id`, `status`, `manifest_path`, `summary_path`, `raw_path`, and `log_path`. Successful video downloads are job artifacts under `media/` and `videos.zip`; `media_manifest.json` records downloaded, skipped, and failed rows. Use those files as the source of truth instead of summarizing terminal logs from memory.

## Debugging

Use `--dry-run` before a first real crawl to verify parameters and artifact paths:

```bash
guangdada crawl advertiser --keyword com.example.app --label "Example App" --time-window 30d --cover-dedupe --dry-run
```

Use `--headed` when login, proxy, or human verification needs visual inspection.
