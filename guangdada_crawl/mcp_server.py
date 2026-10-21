"""MCP server entrypoint for the lightweight Guangdada crawler."""

from __future__ import annotations

from typing import Any

from guangdada_crawl import mcp_tools


def build_server() -> Any:
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:  # pragma: no cover - depends on optional runtime environment
        raise RuntimeError("Install the `mcp` package to run `guangdada-mcp`.") from exc

    server = FastMCP("guangdada-crawl")

    @server.tool()
    def doctor(home: str | None = None, jobs_dir: str | None = None) -> dict[str, Any]:
        return mcp_tools.doctor(home=home, jobs_dir=jobs_dir)

    @server.tool()
    def examples(home: str | None = None, jobs_dir: str | None = None) -> dict[str, Any]:
        return mcp_tools.examples(home=home, jobs_dir=jobs_dir)

    @server.tool()
    def auth_status(home: str | None = None, jobs_dir: str | None = None) -> dict[str, Any]:
        return mcp_tools.auth_status(home=home, jobs_dir=jobs_dir)

    @server.tool()
    def start_chart_crawl(
        chart: str = "new",
        categories: list[str] | str | None = None,
        limit: int = 100,
        date: str = "",
        category_mode: str = "",
        auth_mode: str = "",
        headed: bool = False,
        debug: bool = False,
        scroll_rounds: int | None = None,
        wait_after_filter_ms: int | None = None,
        cover_dedupe: bool | None = None,
        download_videos: bool | None = None,
        dry_run: bool = False,
        home: str | None = None,
        jobs_dir: str | None = None,
    ) -> dict[str, Any]:
        return mcp_tools.start_chart_crawl(
            chart=chart,
            categories=categories,
            limit=limit,
            date=date,
            category_mode=category_mode,
            auth_mode=auth_mode,
            headed=headed,
            debug=debug,
            scroll_rounds=scroll_rounds,
            wait_after_filter_ms=wait_after_filter_ms,
            cover_dedupe=cover_dedupe,
            download_videos=download_videos,
            dry_run=dry_run,
            home=home,
            jobs_dir=jobs_dir,
        )

    @server.tool()
    def start_advertiser_crawl(
        keyword: str,
        label: str = "",
        date: str = "",
        date_mode: str = "first-seen",
        time_window: str = "7d",
        order_by: str = "",
        headed: bool = False,
        is_tool: bool = False,
        napi_list: bool = False,
        no_detail_analysis: bool = False,
        max_scroll_rounds: int | None = None,
        cover_dedupe: bool | None = None,
        download_videos: bool | None = None,
        dry_run: bool = False,
        home: str | None = None,
        jobs_dir: str | None = None,
    ) -> dict[str, Any]:
        return mcp_tools.start_advertiser_crawl(
            keyword=keyword,
            label=label,
            date=date,
            date_mode=date_mode,
            time_window=time_window,
            order_by=order_by,
            headed=headed,
            is_tool=is_tool,
            napi_list=napi_list,
            no_detail_analysis=no_detail_analysis,
            max_scroll_rounds=max_scroll_rounds,
            cover_dedupe=cover_dedupe,
            download_videos=download_videos,
            dry_run=dry_run,
            home=home,
            jobs_dir=jobs_dir,
        )

    @server.tool()
    def start_ad_crawl(
        ad_key: str = "",
        url: str = "",
        type: int = 2,
        app_type: int | None = None,
        search_flag: int | None = None,
        created_at: int | None = None,
        no_fetch_detail: bool = False,
        download_videos: bool | None = None,
        headed: bool = False,
        dry_run: bool = False,
        home: str | None = None,
        jobs_dir: str | None = None,
    ) -> dict[str, Any]:
        return mcp_tools.start_ad_crawl(
            ad_key=ad_key,
            url=url,
            type=type,
            app_type=app_type,
            search_flag=search_flag,
            created_at=created_at,
            no_fetch_detail=no_fetch_detail,
            download_videos=download_videos,
            headed=headed,
            dry_run=dry_run,
            home=home,
            jobs_dir=jobs_dir,
        )

    @server.tool()
    def get_job_status(job_id: str, home: str | None = None, jobs_dir: str | None = None) -> dict[str, Any]:
        return mcp_tools.get_job_status(job_id, home=home, jobs_dir=jobs_dir)

    @server.tool()
    def get_job_artifacts(job_id: str, home: str | None = None, jobs_dir: str | None = None) -> dict[str, Any]:
        return mcp_tools.get_job_artifacts(job_id, home=home, jobs_dir=jobs_dir)

    @server.tool()
    def list_recent_jobs(limit: int = 20, home: str | None = None, jobs_dir: str | None = None) -> dict[str, Any]:
        return mcp_tools.list_recent_jobs(limit=limit, home=home, jobs_dir=jobs_dir)

    return server


def main() -> None:
    build_server().run()


if __name__ == "__main__":
    main()
