# Guangdada Crawl Package

`guangdada-crawl` is a lightweight installable wrapper around the shared Guangdada crawler core. It is meant for teammates and their Codex sessions to run local crawl jobs without depending on VE, Arrow2, Bitable sync, LLM analysis, review dashboards, or any downstream business workflow.

## Install

From this repository checkout:

```bash
python -m pip install -e .
python -m playwright install chromium
```

For a teammate who does not keep this repository locally, install from a Git URL or an internal package mirror once published:

```bash
python -m pip install "git+ssh://git.example/ua-materials.git#egg=guangdada-crawl"
python -m playwright install chromium
```

## Environment

Set Guangdada login credentials in the shell or `.env` used by the installed package:

```bash
export GUANGDADA_EMAIL="name@example.com"
export GUANGDADA_PASSWORD="..."
```

Optional proxy variables:

```bash
export PLAYWRIGHT_PROXY_SERVER="http://127.0.0.1:7890"
export PROXY_AUTO_LAUNCH_APP=1
export PROXY_APP_NAME="Clash Verge"
```

Jobs are written to `~/.guangdada-crawl/jobs/` by default. Override with:

```bash
export GUANGDADA_CRAWL_HOME="$HOME/.guangdada-crawl"
```

## Commands

Diagnose local setup from any working directory:

```bash
guangdada --json doctor
```

Print machine-readable examples and safety boundaries:

```bash
guangdada --json examples
```

Check local auth state and credentials:

```bash
guangdada auth status
```

Create a reusable browser auth state:

```bash
guangdada auth login --headed
```

Chart crawl:

```bash
guangdada crawl chart --chart new --category AI视频 --limit 50 --cover-dedupe
```

Advertiser or app keyword crawl:

```bash
guangdada crawl advertiser --keyword com.example.app --label "Example App" --date 2026-06-17 --cover-dedupe
```

Advertiser crawls start from Guangdada's default game tab. For tool or AI tool advertisers, explicitly switch to the tool tab:

```bash
guangdada crawl advertiser --keyword com.example.tool --label "Example Tool" --date 2026-06-17 --is-tool --cover-dedupe
```

When advertiser `--date` is supplied, the crawler fills Guangdada's custom date selector with that UTC+8 date before searching, then applies `--date-mode` as a detail-level verification. The default `first-seen` mode means creatives first seen on that date; resumed/re-run creatives are retained when their `first_seen` matches. Without `--date`, the crawler uses the `--time-window` shortcut buttons.

Chart and advertiser crawls require an explicit cover-dedupe choice. Use `--cover-dedupe` only after the user confirms they want the VE cover CLIP dedupe stage. `raw.items` then contains the kept representatives, while `cover_style_intraday_report` records input/output counts, removed rows, and cluster details. Use `--no-cover-dedupe` when the user wants untouched raw crawl rows:

```bash
guangdada crawl advertiser --keyword com.example.app --label "Example App" --date 2026-06-17 --no-cover-dedupe
```

When no exact target date is supplied, choose a wider Guangdada shortcut window:

```bash
guangdada crawl advertiser --keyword com.example.app --label "Example App" --time-window 30d --cover-dedupe
```

Supported shortcut windows are `7d / 30d / 90d / 1y`, corresponding to Guangdada's `7天 / 30天 / 90天 / 近1年` buttons.

To collect all creatives active on that date:

```bash
guangdada crawl advertiser --keyword com.example.app --label "Example App" --date 2026-06-17 --date-mode active --cover-dedupe
```

By default, chart, advertiser, and single-ad crawls try to download video creatives into the job directory and package successful downloads into `videos.zip`. If a video URL is unavailable or a download fails, the crawl still completes and records the per-item status in `media_manifest.json`. To keep a run URL-only and skip local video files:

```bash
guangdada crawl advertiser --keyword com.example.app --label "Example App" --date 2026-06-17 --cover-dedupe --no-download-videos
```

Single ad detail seed:

```bash
guangdada crawl ad --ad-key abc123 --type 2
```

Single ad `detail-v2` fetch when `search_flag` is known:

```bash
guangdada crawl ad --ad-key abc123 --type 2 --app-type 2 --search-flag 8
guangdada crawl ad --url "https://guangdada.net/modules/creative/display-ads/detail?id=abc123&type=2&search_flag=8"
```

If no `search_flag` is available, the command only writes a detail URL seed and `summary.json` records `detail_v2_status=skipped_missing_search_flag`.

Inspect a job:

```bash
guangdada job list --limit 5
guangdada job show 20260617T010203Z_chart_ab12cd34
guangdada job artifacts 20260617T010203Z_chart_ab12cd34
```

Dry run without opening a browser:

```bash
guangdada crawl chart --chart new --category AI视频 --limit 5 --cover-dedupe --dry-run
guangdada crawl advertiser --keyword com.example.app --label "Example App" --time-window 30d --cover-dedupe --dry-run
```

## MCP Wrapper

For teammates who want to expose the crawler to their Codex through MCP, start:

```bash
python -m pip install -e ".[mcp]"
guangdada-mcp
```

MCP 只是薄包装 around the same local CLI. Each MCP tool calls `guangdada`, parses the JSON stdout, and returns the same job/artifact payload. The crawler logic still lives in the CLI package, not inside the MCP server.

Exposed tools:

- `doctor`
- `examples`
- `auth_status`
- `start_chart_crawl`
- `start_advertiser_crawl`
- `start_ad_crawl`
- `list_recent_jobs`
- `get_job_status`
- `get_job_artifacts`

Use `dry_run=true` from MCP before the first real crawl to verify parameters and output paths.

## Job Artifacts

Each crawl creates:

```text
~/.guangdada-crawl/jobs/{job_id}/
├── manifest.json
├── raw.json
├── summary.json
├── media_manifest.json
├── videos.zip
├── media/
│   └── 001_ad-key.mp4
└── run.log
```

The package 默认只产出 raw/job artifact. Video files and `videos.zip` are local job artifacts only. It does not sync to VE, Arrow2, Feishu Bitable, Google Sheets, or any downstream analysis system.

When Codex does not know a job id, run `guangdada job list --limit 5` first. Use `guangdada job artifacts JOB_ID` to get machine-readable paths and `exists` flags for `manifest`, `summary`, `raw`, `log`, `media_manifest`, and `videos_zip`.

## Scope

Included:

- Guangdada login and Playwright crawl execution.
- Creative chart crawl for new, hot, and surge charts.
- Advertiser/app keyword latest-material crawl.
- Game-tab advertiser/app crawl by default; tool-tab crawl with explicit `--is-tool`.
- Explicit user-confirmed cover dedupe choice for chart and advertiser crawl outputs: `--cover-dedupe` or `--no-cover-dedupe`.
- Local video file artifact generation with `media_manifest.json`, per-item `local_video_path`, and `videos.zip`; can be disabled with `--no-download-videos`.
- Single ad detail URL seed generation.
- Structured job artifacts for Codex to inspect.

Excluded from this package boundary:

- VE main-table sync.
- Arrow2 workflows.
- LLM analysis.
- Filtering, dedupe, review dashboards, and push notifications.
- Automatic captcha solving.

When Guangdada shows human verification, do not bypass or crack it. Let the user finish verification in the browser or through the existing manual gate, then rerun or resume the crawl.
