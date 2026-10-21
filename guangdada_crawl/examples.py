"""Machine-readable usage examples for future Codex sessions."""

from __future__ import annotations

from typing import Any

from guangdada_crawl import __version__


def build_examples() -> dict[str, Any]:
    return {
        "command": "examples",
        "version": __version__,
        "first_steps": [
            "guangdada --json doctor",
            "guangdada auth status",
            "guangdada auth login --headed",
        ],
        "examples": [
            {
                "name": "chart_new_ai_video",
                "command": "guangdada crawl chart --chart new --category AI视频 --limit 50 --cover-dedupe",
                "purpose": "Crawl the Guangdada new creative chart for the AI video category.",
                "dry_run": "guangdada crawl chart --chart new --category AI视频 --limit 5 --cover-dedupe --dry-run",
            },
            {
                "name": "advertiser_by_appid",
                "command": 'guangdada crawl advertiser --keyword com.example.app --label "Example App" --date YYYY-MM-DD --cover-dedupe',
                "purpose": "Crawl game-tab creatives first seen on one date for one advertiser/app keyword, including resumed/re-run creatives. The caller must explicitly choose --cover-dedupe or --no-cover-dedupe.",
                "dry_run": 'guangdada crawl advertiser --keyword com.example.app --label "Example App" --date YYYY-MM-DD --cover-dedupe --dry-run',
            },
            {
                "name": "tool_advertiser_by_appid",
                "command": 'guangdada crawl advertiser --keyword com.example.app --label "Example Tool" --date YYYY-MM-DD --is-tool --cover-dedupe',
                "purpose": "Switch to Guangdada's tool tab before crawling a tool advertiser/app keyword. Game is the default tab and does not need an explicit switch.",
                "dry_run": 'guangdada crawl advertiser --keyword com.example.app --label "Example Tool" --date YYYY-MM-DD --is-tool --cover-dedupe --dry-run',
            },
            {
                "name": "advertiser_30d_window",
                "command": 'guangdada crawl advertiser --keyword com.example.app --label "Example App" --date YYYY-MM-DD --time-window 30d --cover-dedupe',
                "purpose": "Click Guangdada's shortcut time button before result filtering. Supported values are 7d, 30d, 90d, and 1y.",
                "dry_run": 'guangdada crawl advertiser --keyword com.example.app --label "Example App" --date YYYY-MM-DD --time-window 30d --cover-dedupe --dry-run',
            },
            {
                "name": "advertiser_without_cover_dedupe",
                "command": 'guangdada crawl advertiser --keyword com.example.app --label "Example App" --date YYYY-MM-DD --no-cover-dedupe',
                "purpose": "Skip VE-style CLIP cover dedupe when the user chooses untouched raw crawl rows.",
                "dry_run": 'guangdada crawl advertiser --keyword com.example.app --label "Example App" --date YYYY-MM-DD --no-cover-dedupe --dry-run',
            },
            {
                "name": "advertiser_without_video_download",
                "command": 'guangdada crawl advertiser --keyword com.example.app --label "Example App" --date YYYY-MM-DD --cover-dedupe --no-download-videos',
                "purpose": "Skip local video files and videos.zip when the user only needs URL/raw artifacts.",
                "dry_run": 'guangdada crawl advertiser --keyword com.example.app --label "Example App" --date YYYY-MM-DD --cover-dedupe --no-download-videos --dry-run',
            },
            {
                "name": "advertiser_active_by_appid",
                "command": 'guangdada crawl advertiser --keyword com.example.app --label "Example App" --date YYYY-MM-DD --date-mode active --cover-dedupe',
                "purpose": "Crawl creatives active on one date for one advertiser/app keyword.",
                "dry_run": 'guangdada crawl advertiser --keyword com.example.app --label "Example App" --date YYYY-MM-DD --date-mode active --cover-dedupe --dry-run',
            },
            {
                "name": "single_ad_seed",
                "command": "guangdada crawl ad --ad-key AD_KEY --type 2",
                "purpose": "Create a single ad detail URL seed when search_flag is unknown.",
            },
            {
                "name": "single_ad_detail_v2",
                "command": "guangdada crawl ad --ad-key AD_KEY --type 2 --app-type 2 --search-flag SEARCH_FLAG",
                "purpose": "Fetch detail-v2 for one ad when search_flag is known.",
            },
            {
                "name": "recent_jobs",
                "command": "guangdada job list --limit 5",
                "purpose": "Find recent job ids before inspecting artifacts.",
            },
            {
                "name": "job_artifacts",
                "command": "guangdada job artifacts JOB_ID",
                "purpose": "Return manifest, summary, raw, log, media_manifest.json, and videos.zip artifact paths with existence flags.",
            },
        ],
        "mcp_tools": [
            "doctor",
            "examples",
            "auth_status",
            "start_chart_crawl",
            "start_advertiser_crawl",
            "start_ad_crawl",
            "list_recent_jobs",
            "get_job_status",
            "get_job_artifacts",
        ],
        "safety_boundaries": [
            "默认只产出 raw/job artifact",
            "不要默认写入 VE",
            "不要默认写入 Arrow2",
            "不要默认写入 Feishu 多维表格",
            "不要触发 LLM 分析或同步流程",
            "不要破解验证码",
        ],
    }
