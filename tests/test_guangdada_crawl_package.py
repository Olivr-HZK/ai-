from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
import zipfile
import builtins
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class GuangdadaCrawlPackageTest(unittest.TestCase):
    def test_pyproject_declares_installable_cli(self) -> None:
        pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")

        self.assertIn('name = "guangdada-crawl"', pyproject)
        self.assertIn('guangdada = "guangdada_crawl.cli:main"', pyproject)
        self.assertIn('guangdada-mcp = "guangdada_crawl.mcp_server:main"', pyproject)
        self.assertIn("[project.optional-dependencies]", pyproject)
        self.assertIn("mcp = [", pyproject)
        self.assertIn('"mcp>=', pyproject)
        self.assertIn('"guangdada_crawl*"', pyproject)

    def test_plugin_manifest_declares_skill_and_mcp_server(self) -> None:
        manifest_path = ROOT / ".codex-plugin" / "plugin.json"
        mcp_path = ROOT / ".mcp.json"
        skill_path = ROOT / "skills" / "guangdada-crawl" / "SKILL.md"

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        mcp = json.loads(mcp_path.read_text(encoding="utf-8"))
        skill = skill_path.read_text(encoding="utf-8")

        self.assertEqual(manifest["name"], "guangdada-crawl")
        self.assertEqual(manifest["skills"], "./skills/")
        self.assertEqual(manifest["mcpServers"], "./.mcp.json")
        self.assertEqual(manifest["interface"]["displayName"], "Guangdada Crawl")
        self.assertIn("Guangdada chart", manifest["interface"]["defaultPrompt"][0])
        self.assertIn("guangdada-crawl", mcp["mcpServers"])
        self.assertEqual(mcp["mcpServers"]["guangdada-crawl"]["command"], "python3")
        self.assertEqual(mcp["mcpServers"]["guangdada-crawl"]["args"], ["-m", "guangdada_crawl.mcp_server"])
        self.assertIn("guangdada --json doctor", skill)
        self.assertIn("不要默认写入 VE", skill)

    def test_docs_and_codex_skill_describe_safe_crawl_boundary(self) -> None:
        doc = (ROOT / "docs" / "guangdada-crawl-package.md").read_text(encoding="utf-8")
        skill = (ROOT / "codex" / "skills" / "guangdada-crawl" / "SKILL.md").read_text(encoding="utf-8")

        self.assertIn("guangdada crawl chart", doc)
        self.assertIn("guangdada crawl advertiser", doc)
        self.assertIn("--time-window 30d", doc)
        self.assertIn("7d / 30d / 90d / 1y", doc)
        self.assertIn("fills Guangdada's custom date selector", doc)
        self.assertIn("guangdada --json doctor", doc)
        self.assertIn("guangdada --json examples", doc)
        self.assertIn("guangdada auth status", doc)
        self.assertIn("--search-flag", doc)
        self.assertIn("guangdada job list", doc)
        self.assertIn("guangdada job artifacts", doc)
        self.assertIn("guangdada-mcp", doc)
        self.assertIn("MCP 只是薄包装", doc)
        self.assertIn("skipped_missing_search_flag", doc)
        self.assertIn("media_manifest.json", doc)
        self.assertIn("videos.zip", doc)
        self.assertIn("--no-download-videos", doc)
        self.assertIn("默认只产出 raw/job artifact", doc)
        self.assertIn("guangdada --json doctor", skill)
        self.assertIn("guangdada --json examples", skill)
        self.assertIn("guangdada auth login", skill)
        self.assertIn("--time-window 30d", skill)
        self.assertIn("7d / 30d / 90d / 1y", skill)
        self.assertIn("fill Guangdada's custom date selector", skill)
        self.assertIn("default game tab", skill)
        self.assertIn("--is-tool", skill)
        self.assertIn("CLIP cover dedupe", skill)
        self.assertIn("must explicitly choose", skill)
        self.assertIn("--cover-dedupe", skill)
        self.assertIn("cover_style_intraday_report", skill)
        self.assertIn("--no-cover-dedupe", skill)
        self.assertIn("guangdada job list", skill)
        self.assertIn("guangdada job artifacts", skill)
        self.assertIn("guangdada-mcp", skill)
        self.assertIn("start_chart_crawl", skill)
        self.assertIn("--search-flag", skill)
        self.assertIn("media_manifest.json", skill)
        self.assertIn("videos.zip", skill)
        self.assertIn("--no-download-videos", skill)
        self.assertIn("不要破解验证码", skill)
        self.assertIn("不要默认写入 VE", skill)

    def test_mcp_tool_wrapper_builds_chart_cli_args(self) -> None:
        from guangdada_crawl.mcp_tools import build_cli_args

        args = build_cli_args(
            "start_chart_crawl",
            {
                "chart": "new",
                "categories": ["AI视频", "AI图像生成"],
                "limit": 5,
                "date": "2026-06-17",
                "dry_run": True,
            },
        )

        self.assertEqual(args[:3], ["crawl", "chart", "--chart"])
        self.assertIn("new", args)
        self.assertIn("--category", args)
        self.assertEqual(args.count("--category"), 2)
        self.assertIn("AI视频", args)
        self.assertIn("AI图像生成", args)
        self.assertIn("--limit", args)
        self.assertIn("5", args)
        self.assertIn("--date", args)
        self.assertIn("2026-06-17", args)
        self.assertIn("--dry-run", args)

    def test_mcp_tool_wrapper_builds_advertiser_date_mode_arg(self) -> None:
        from guangdada_crawl.mcp_tools import build_cli_args

        args = build_cli_args(
            "start_advertiser_crawl",
            {
                "keyword": "com.pixverseai.pixverse",
                "date": "2026-06-17",
                "date_mode": "first-seen",
            },
        )

        self.assertIn("--date-mode", args)
        self.assertIn("first-seen", args)

    def test_mcp_tool_wrapper_builds_advertiser_time_window_arg(self) -> None:
        from guangdada_crawl.mcp_tools import build_cli_args

        args = build_cli_args(
            "start_advertiser_crawl",
            {
                "keyword": "com.pixverseai.pixverse",
                "date": "2026-06-17",
                "time_window": "30d",
            },
        )

        self.assertIn("--time-window", args)
        self.assertIn("30d", args)

    def test_mcp_tool_wrapper_builds_cover_dedupe_skip_args(self) -> None:
        from guangdada_crawl.mcp_tools import build_cli_args

        chart_args = build_cli_args(
            "start_chart_crawl",
            {"chart": "new", "cover_dedupe": False},
        )
        advertiser_args = build_cli_args(
            "start_advertiser_crawl",
            {"keyword": "com.example.app", "cover_dedupe": False},
        )
        chart_dedupe_args = build_cli_args(
            "start_chart_crawl",
            {"chart": "new", "cover_dedupe": True},
        )

        self.assertIn("--no-cover-dedupe", chart_args)
        self.assertIn("--no-cover-dedupe", advertiser_args)
        self.assertIn("--cover-dedupe", chart_dedupe_args)

    def test_mcp_tool_wrapper_builds_download_video_skip_args(self) -> None:
        from guangdada_crawl.mcp_tools import build_cli_args

        chart_args = build_cli_args(
            "start_chart_crawl",
            {"chart": "new", "download_videos": False, "cover_dedupe": False},
        )
        advertiser_args = build_cli_args(
            "start_advertiser_crawl",
            {"keyword": "com.example.app", "download_videos": False, "cover_dedupe": False},
        )
        default_args = build_cli_args(
            "start_chart_crawl",
            {"chart": "new", "cover_dedupe": False},
        )

        self.assertIn("--no-download-videos", chart_args)
        self.assertIn("--no-download-videos", advertiser_args)
        self.assertNotIn("--no-download-videos", default_args)

    def test_mcp_tool_wrapper_omits_cover_dedupe_when_not_explicit(self) -> None:
        from guangdada_crawl.mcp_tools import build_cli_args

        args = build_cli_args("start_advertiser_crawl", {"keyword": "com.example.app"})

        self.assertNotIn("--cover-dedupe", args)
        self.assertNotIn("--no-cover-dedupe", args)

    def test_mcp_tool_wrapper_only_switches_tool_tab_when_explicit(self) -> None:
        from guangdada_crawl.mcp_tools import build_cli_args

        default_args = build_cli_args(
            "start_advertiser_crawl",
            {"keyword": "com.example.game"},
        )
        tool_args = build_cli_args(
            "start_advertiser_crawl",
            {"keyword": "com.example.tool", "is_tool": True},
        )

        self.assertNotIn("--is-tool", default_args)
        self.assertIn("--is-tool", tool_args)

    def test_mcp_tool_wrapper_runs_cli_and_parses_json(self) -> None:
        from guangdada_crawl.mcp_tools import start_chart_crawl

        calls: list[list[str]] = []

        def fake_runner(cmd, **kwargs):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, stdout='{"status":"dry_run","job_id":"job1"}', stderr="")

        payload = start_chart_crawl(
            chart="new",
            categories=["AI视频"],
            limit=3,
            dry_run=True,
            home="/tmp/gdd-home",
            runner=fake_runner,
        )

        self.assertEqual(payload["status"], "dry_run")
        self.assertEqual(payload["job_id"], "job1")
        self.assertEqual(calls[0][:3], [sys.executable, "-m", "guangdada_crawl.cli"])
        self.assertIn("--home", calls[0])

    def test_mcp_tool_wrapper_parses_json_after_progress_logs(self) -> None:
        from guangdada_crawl.mcp_tools import start_advertiser_crawl

        stdout = "\n".join(
            [
                "打开广大大搜索页...",
                "抓取完成，准备写入 job artifact",
                '{"status":"completed","job_id":"job2","summary_path":"/tmp/summary.json"}',
            ]
        )

        def fake_runner(cmd, **kwargs):
            return subprocess.CompletedProcess(cmd, 0, stdout=stdout, stderr="")

        payload = start_advertiser_crawl(keyword="pixverse", date="2026-06-17", runner=fake_runner)

        self.assertEqual(payload["status"], "completed")
        self.assertEqual(payload["job_id"], "job2")
        self.assertEqual(payload["summary_path"], "/tmp/summary.json")

    def test_cli_examples_returns_machine_readable_usage_contract(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "guangdada_crawl.cli",
                "--json",
                "examples",
            ],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["command"], "examples")
        self.assertIn("guangdada --json doctor", payload["first_steps"])
        commands = [item["command"] for item in payload["examples"]]
        self.assertIn("guangdada crawl chart --chart new --category AI视频 --limit 50 --cover-dedupe", commands)
        self.assertIn('guangdada crawl advertiser --keyword com.example.app --label "Example App" --date YYYY-MM-DD --cover-dedupe', commands)
        self.assertIn('guangdada crawl advertiser --keyword com.example.app --label "Example App" --date YYYY-MM-DD --time-window 30d --cover-dedupe', commands)
        self.assertIn('guangdada crawl advertiser --keyword com.example.app --label "Example App" --date YYYY-MM-DD --date-mode active --cover-dedupe', commands)
        self.assertIn('guangdada crawl advertiser --keyword com.example.app --label "Example App" --date YYYY-MM-DD --no-cover-dedupe', commands)
        self.assertIn("guangdada job list --limit 5", commands)
        self.assertIn("start_chart_crawl", payload["mcp_tools"])
        self.assertIn("examples", payload["mcp_tools"])
        self.assertIn("不要默认写入 VE", payload["safety_boundaries"])

    def test_mcp_tool_wrapper_supports_examples(self) -> None:
        from guangdada_crawl.mcp_tools import examples

        calls: list[list[str]] = []

        def fake_runner(cmd, **kwargs):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, stdout='{"command":"examples","examples":[]}', stderr="")

        payload = examples(runner=fake_runner)

        self.assertEqual(payload["command"], "examples")
        self.assertEqual(payload["examples"], [])
        self.assertIn("examples", calls[0])

    def test_mcp_tool_wrapper_returns_error_payload_on_cli_failure(self) -> None:
        from guangdada_crawl.mcp_tools import get_job_status

        def fake_runner(cmd, **kwargs):
            return subprocess.CompletedProcess(cmd, 1, stdout='{"status":"failed","error":"missing"}', stderr="")

        payload = get_job_status("nope", runner=fake_runner)

        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["error"], "missing")
        self.assertEqual(payload["returncode"], 1)

    def test_cli_doctor_reports_setup_without_requiring_repo_cwd(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as other_cwd:
            env = os.environ.copy()
            env.pop("GUANGDADA_EMAIL", None)
            env.pop("GUANGDADA_USERNAME", None)
            env.pop("GUANGDADA_PASSWORD", None)

            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "guangdada_crawl.cli",
                    "--json",
                    "--home",
                    tmp,
                    "doctor",
                ],
                cwd=other_cwd,
                text=True,
                capture_output=True,
                check=False,
                env=env,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["command"], "doctor")
        self.assertEqual(payload["status"], "setup_needed")
        self.assertEqual(payload["home"], tmp)
        self.assertTrue(payload["jobs_dir"].endswith("jobs"))
        self.assertFalse(payload["auth"]["has_credentials"])
        self.assertFalse(payload["auth"]["has_auth_state"])
        self.assertTrue(payload["checks"]["guangdada_crawl_importable"])
        self.assertTrue(payload["checks"]["core_search_importable"])
        self.assertIn("playwright_importable", payload["checks"])

    def test_mcp_tool_wrapper_supports_doctor(self) -> None:
        from guangdada_crawl.mcp_tools import doctor

        calls: list[list[str]] = []

        def fake_runner(cmd, **kwargs):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, stdout='{"command":"doctor","status":"ok"}', stderr="")

        payload = doctor(home="/tmp/gdd-home", runner=fake_runner)

        self.assertEqual(payload["command"], "doctor")
        self.assertEqual(payload["status"], "ok")
        self.assertIn("doctor", calls[0])
        self.assertIn("--home", calls[0])

    def test_job_store_writes_manifest_summary_and_raw(self) -> None:
        from guangdada_crawl.jobs import JobStore

        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            job = store.create_job(
                kind="chart",
                params={"chart": "new", "categories": ["AI视频"], "limit": 5},
                dry_run=True,
            )
            store.write_summary(job.job_id, {"total": 0, "status": "dry_run"})
            store.write_raw(job.job_id, {"items": []})
            store.finish_job(job.job_id, status="dry_run")

            manifest = json.loads((job.path / "manifest.json").read_text(encoding="utf-8"))
            summary = json.loads((job.path / "summary.json").read_text(encoding="utf-8"))
            raw = json.loads((job.path / "raw.json").read_text(encoding="utf-8"))

        self.assertEqual(manifest["kind"], "chart")
        self.assertEqual(manifest["status"], "dry_run")
        self.assertEqual(manifest["params"]["categories"], ["AI视频"])
        self.assertTrue(manifest["dry_run"])
        self.assertEqual(summary["total"], 0)
        self.assertEqual(raw["items"], [])

    def test_cli_chart_dry_run_creates_a_job_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "guangdada_crawl.cli",
                    "--jobs-dir",
                    tmp,
                    "crawl",
                    "chart",
                    "--chart",
                    "new",
                    "--category",
                    "AI视频",
                    "--limit",
                    "3",
                    "--no-cover-dedupe",
                    "--dry-run",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(result.stdout)
            manifest_path = Path(payload["manifest_path"])
            summary_path = Path(payload["summary_path"])
            self.assertTrue(manifest_path.exists())
            self.assertTrue(summary_path.exists())
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["status"], "dry_run")
        self.assertEqual(manifest["kind"], "chart")
        self.assertEqual(manifest["params"]["chart"], "new")
        self.assertEqual(manifest["params"]["categories"], ["AI视频"])
        self.assertIs(manifest["params"]["cover_dedupe"], False)

    def test_cli_chart_requires_explicit_cover_dedupe_choice(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "guangdada_crawl.cli",
                    "--jobs-dir",
                    tmp,
                    "crawl",
                    "chart",
                    "--chart",
                    "new",
                    "--dry-run",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--cover-dedupe", result.stderr)
        self.assertIn("--no-cover-dedupe", result.stderr)

    def test_cli_advertiser_dry_run_records_keyword_label_and_date(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "guangdada_crawl.cli",
                    "--jobs-dir",
                    tmp,
                    "crawl",
                    "advertiser",
                    "--keyword",
                    "com.example.app",
                    "--label",
                    "Example App",
                    "--date",
                    "2026-06-17",
                    "--time-window",
                    "90d",
                    "--cover-dedupe",
                    "--dry-run",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(result.stdout)
            manifest = json.loads(Path(payload["manifest_path"]).read_text(encoding="utf-8"))

        self.assertEqual(manifest["kind"], "advertiser")
        self.assertEqual(manifest["params"]["keyword"], "com.example.app")
        self.assertEqual(manifest["params"]["label"], "Example App")
        self.assertEqual(manifest["params"]["date"], "2026-06-17")
        self.assertEqual(manifest["params"]["date_mode"], "first-seen")
        self.assertEqual(manifest["params"]["time_window"], "90d")
        self.assertIs(manifest["params"]["is_tool"], False)
        self.assertIs(manifest["params"]["cover_dedupe"], True)

    def test_cli_advertiser_requires_explicit_cover_dedupe_choice(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "guangdada_crawl.cli",
                    "--jobs-dir",
                    tmp,
                    "crawl",
                    "advertiser",
                    "--keyword",
                    "com.example.app",
                    "--date",
                    "2026-06-17",
                    "--dry-run",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--cover-dedupe", result.stderr)
        self.assertIn("--no-cover-dedupe", result.stderr)

    def test_cli_advertiser_dry_run_records_explicit_tool_tab(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "guangdada_crawl.cli",
                    "--jobs-dir",
                    tmp,
                    "crawl",
                    "advertiser",
                    "--keyword",
                    "com.example.tool",
                    "--date",
                    "2026-06-17",
                    "--is-tool",
                    "--no-cover-dedupe",
                    "--dry-run",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(result.stdout)
            manifest = json.loads(Path(payload["manifest_path"]).read_text(encoding="utf-8"))

        self.assertIs(manifest["params"]["is_tool"], True)

    def test_cli_job_show_reads_existing_manifest(self) -> None:
        from guangdada_crawl.jobs import JobStore

        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            job = store.create_job(kind="ad", params={"ad_key": "abc"}, dry_run=True)
            store.write_summary(job.job_id, {"ad_key": "abc"})
            store.finish_job(job.job_id, status="dry_run")

            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "guangdada_crawl.cli",
                    "--jobs-dir",
                    tmp,
                    "job",
                    "show",
                    job.job_id,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["manifest"]["kind"], "ad")
        self.assertEqual(payload["manifest"]["status"], "dry_run")
        self.assertEqual(payload["summary"]["ad_key"], "abc")

    def test_cli_job_list_returns_recent_jobs_with_summaries(self) -> None:
        from guangdada_crawl.jobs import JobStore

        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            older = store.create_job(kind="chart", params={"chart": "new"}, dry_run=True)
            store.write_summary(older.job_id, {"total": 1})
            store.finish_job(older.job_id, status="dry_run")
            newer = store.create_job(kind="advertiser", params={"keyword": "com.example"}, dry_run=True)
            store.write_summary(newer.job_id, {"total": 2})
            store.finish_job(newer.job_id, status="dry_run")

            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "guangdada_crawl.cli",
                    "--jobs-dir",
                    tmp,
                    "job",
                    "list",
                    "--limit",
                    "1",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["jobs"][0]["job_id"], newer.job_id)
        self.assertEqual(payload["jobs"][0]["kind"], "advertiser")
        self.assertEqual(payload["jobs"][0]["summary"]["total"], 2)

    def test_cli_job_artifacts_reports_paths_and_existence(self) -> None:
        from guangdada_crawl.jobs import JobStore

        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            job = store.create_job(kind="chart", params={"chart": "new"}, dry_run=True)
            store.write_summary(job.job_id, {"total": 1})
            store.write_raw(job.job_id, {"items": [{"ad_key": "a"}]})
            (job.path / "media_manifest.json").write_text('{"videos":[]}', encoding="utf-8")
            (job.path / "videos.zip").write_bytes(b"zip")
            store.finish_job(job.job_id, status="dry_run")

            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "guangdada_crawl.cli",
                    "--jobs-dir",
                    tmp,
                    "job",
                    "artifacts",
                    job.job_id,
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["job_id"], job.job_id)
        self.assertTrue(payload["artifacts"]["manifest"]["exists"])
        self.assertTrue(payload["artifacts"]["summary"]["exists"])
        self.assertTrue(payload["artifacts"]["raw"]["exists"])
        self.assertTrue(payload["artifacts"]["log"]["exists"])
        self.assertTrue(payload["artifacts"]["media_manifest"]["exists"])
        self.assertTrue(payload["artifacts"]["videos_zip"]["exists"])
        self.assertTrue(payload["artifacts"]["raw"]["path"].endswith("raw.json"))
        self.assertTrue(payload["artifacts"]["videos_zip"]["path"].endswith("videos.zip"))

    def test_media_artifacts_download_videos_and_zip_job_files(self) -> None:
        from guangdada_crawl.jobs import JobStore
        from guangdada_crawl.media import materialize_video_artifacts

        payload = {
            "items": [
                {
                    "product": "Example App",
                    "creative": {
                        "ad_key": "ad-one",
                        "resource_urls": [{"video_url": "https://cdn.example.com/video-one.mp4"}],
                    },
                },
                {
                    "creative": {
                        "ad_key": "ad-two",
                        "cdn_url": ["https://cdn.example.com/video-two.mp4?token=abc"],
                    },
                },
                {"creative": {"ad_key": "image-only", "preview_img_url": "https://cdn.example.com/image.jpg"}},
            ]
        }
        downloaded: list[str] = []

        def fake_downloader(url: str, target: Path) -> int:
            downloaded.append(url)
            target.write_bytes((url + "\n").encode("utf-8"))
            return target.stat().st_size

        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            job = store.create_job(kind="advertiser", params={"keyword": "com.example.app"}, dry_run=False)
            result = materialize_video_artifacts(job, payload, downloader=fake_downloader)
            media_manifest = json.loads((job.path / "media_manifest.json").read_text(encoding="utf-8"))
            zip_path = job.path / "videos.zip"
            with zipfile.ZipFile(zip_path) as archive:
                names = archive.namelist()

        self.assertEqual(result["downloaded_video_count"], 2)
        self.assertEqual(result["video_download_failed_count"], 0)
        self.assertEqual(result["skipped_without_video_count"], 1)
        self.assertEqual(downloaded, ["https://cdn.example.com/video-one.mp4", "https://cdn.example.com/video-two.mp4?token=abc"])
        self.assertTrue(result["media_manifest_path"].endswith("media_manifest.json"))
        self.assertTrue(result["media_zip_path"].endswith("videos.zip"))
        self.assertEqual([entry["status"] for entry in media_manifest["videos"]], ["downloaded", "downloaded", "skipped_no_video_url"])
        self.assertTrue(media_manifest["videos"][0]["relative_path"].startswith("media/001_ad-one"))
        self.assertEqual(len(names), 2)
        self.assertTrue(all(name.startswith("media/") for name in names))

    def test_advertiser_adapter_materializes_video_artifacts_by_default(self) -> None:
        import asyncio

        from guangdada_crawl.adapters import run_advertiser_crawl
        from guangdada_crawl.jobs import JobStore

        async def fake_run_batch(**kwargs):
            return [
                {
                    "all_creatives": [
                        {
                            "ad_key": "keep",
                            "first_seen": 1781539200,
                            "resource_urls": [{"video_url": "https://cdn.example.com/keep.mp4"}],
                        }
                    ]
                }
            ]

        def fake_downloader(url: str, target: Path) -> int:
            target.write_bytes(b"video")
            return target.stat().st_size

        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            job = store.create_job(kind="advertiser", params={"keyword": "com.example.app"}, dry_run=False)
            with patch("ua_workflows.shared.guangdada.search.run_batch", fake_run_batch):
                with patch("guangdada_crawl.media.download_video", fake_downloader):
                    summary = asyncio.run(
                        run_advertiser_crawl(
                            job,
                            store,
                            {
                                "keyword": "com.example.app",
                                "label": "Example",
                                "date": "2026-06-16",
                                "cover_dedupe": False,
                            },
                        )
                    )
            raw = json.loads((job.path / "raw.json").read_text(encoding="utf-8"))
            media_manifest_exists = Path(summary["media_manifest_path"]).exists()
            media_zip_exists = Path(summary["media_zip_path"]).exists()
            local_video_exists = Path(raw["items"][0]["local_video_path"]).exists()

        self.assertEqual(summary["downloaded_video_count"], 1)
        self.assertEqual(summary["video_download_failed_count"], 0)
        self.assertTrue(media_manifest_exists)
        self.assertTrue(media_zip_exists)
        self.assertTrue(raw["items"][0]["local_video_path"].endswith(".mp4"))
        self.assertTrue(local_video_exists)

    def test_base_response_exposes_video_artifact_paths_from_summary(self) -> None:
        from guangdada_crawl.cli import _base_response
        from guangdada_crawl.jobs import JobStore

        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            job = store.create_job(kind="chart", params={"chart": "new"}, dry_run=False)
            store.write_summary(
                job.job_id,
                {
                    "status": "completed",
                    "media_manifest_path": str(job.path / "media_manifest.json"),
                    "media_zip_path": str(job.path / "videos.zip"),
                    "downloaded_video_count": 2,
                    "video_download_failed_count": 1,
                },
            )
            response = _base_response(store, job.job_id, "completed")

        self.assertEqual(response["media_manifest_path"], str(job.path / "media_manifest.json"))
        self.assertEqual(response["media_zip_path"], str(job.path / "videos.zip"))
        self.assertEqual(response["downloaded_video_count"], 2)
        self.assertEqual(response["video_download_failed_count"], 1)

    def test_chart_adapter_can_skip_video_downloads(self) -> None:
        import asyncio

        from guangdada_crawl.adapters import run_chart_crawl
        from guangdada_crawl.jobs import JobStore

        async def fake_run_async(args):
            raw_path = Path(args.output_prefix + "_raw.json")
            completeness_path = Path(args.output_prefix + "_completeness.json")
            raw_path.write_text("{}", encoding="utf-8")
            completeness_path.write_text('{"total": 1}', encoding="utf-8")
            return raw_path, completeness_path, {
                "total": 1,
                "items": [{"ad_key": "chart1", "creative": {"ad_key": "chart1", "video_url": "https://cdn.example.com/chart.mp4"}}],
            }

        params = {
            "date": "2026-06-17",
            "chart": "new",
            "categories": ["AI视频"],
            "limit": 1,
            "category_mode": "combined",
            "auth_mode": "env-login",
            "headed": False,
            "debug": False,
            "scroll_rounds": 1,
            "wait_after_filter_ms": 500,
            "cover_dedupe": False,
            "download_videos": False,
        }
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            job = store.create_job(kind="chart", params=params, dry_run=False)
            with patch("ua_workflows.shared.guangdada.new_charts_ai_tools.run_async", fake_run_async):
                summary = asyncio.run(run_chart_crawl(job, store, params))
            raw = json.loads((job.path / "raw.json").read_text(encoding="utf-8"))

        self.assertEqual(summary["downloaded_video_count"], 0)
        self.assertEqual(summary["media_artifacts_status"], "disabled_by_param")
        self.assertFalse((job.path / "videos.zip").exists())
        self.assertNotIn("local_video_path", raw["items"][0])

    def test_auth_status_reports_state_and_credential_presence(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = os.environ.copy()
            env["GUANGDADA_CRAWL_HOME"] = tmp
            env.pop("GUANGDADA_EMAIL", None)
            env.pop("GUANGDADA_USERNAME", None)
            env.pop("GUANGDADA_PASSWORD", None)

            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "guangdada_crawl.cli",
                    "--home",
                    tmp,
                    "auth",
                    "status",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
                env=env,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertFalse(payload["has_auth_state"])
        self.assertFalse(payload["has_credentials"])
        self.assertTrue(payload["auth_state_path"].endswith("auth/state.json"))

    def test_auth_login_dry_run_reports_without_creating_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "guangdada_crawl.cli",
                    "--home",
                    tmp,
                    "auth",
                    "login",
                    "--dry-run",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )
            state_path = Path(tmp) / "auth" / "state.json"

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["status"], "dry_run")
        self.assertEqual(payload["auth_state_path"], str(state_path))
        self.assertFalse(state_path.exists())

    def test_ad_adapter_writes_detail_seed_without_browser(self) -> None:
        import asyncio

        from guangdada_crawl.adapters import run_ad_crawl
        from guangdada_crawl.jobs import JobStore

        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            job = store.create_job(kind="ad", params={"url": "https://guangdada.net/x?id=ad123"}, dry_run=False)
            summary = asyncio.run(run_ad_crawl(job, store, {"url": "https://guangdada.net/x?id=ad123", "type": 2}))
            raw = json.loads((job.path / "raw.json").read_text(encoding="utf-8"))

        self.assertEqual(summary["ad_key"], "ad123")
        self.assertIn("id=ad123", summary["detail_url"])
        self.assertEqual(raw["items"][0]["ad_key"], "ad123")
        self.assertEqual(summary["detail_v2_status"], "skipped_missing_search_flag")

    def test_ad_adapter_fetches_detail_v2_when_search_flag_available(self) -> None:
        import asyncio

        from guangdada_crawl.adapters import run_ad_crawl
        from guangdada_crawl.jobs import JobStore

        async def fake_fetch(**kwargs):
            self.assertEqual(kwargs["ad_key"], "ad123")
            self.assertEqual(kwargs["search_flag"], 8)
            self.assertEqual(kwargs["app_type"], 2)
            return {"ok": True, "status": 200, "body": {"data": {"ad_key": "ad123", "title": "detail"}}}

        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            job = store.create_job(kind="ad", params={"ad_key": "ad123", "search_flag": 8}, dry_run=False)
            with patch("guangdada_crawl.adapters.fetch_detail_v2_payload", fake_fetch):
                summary = asyncio.run(
                    run_ad_crawl(
                        job,
                        store,
                        {"ad_key": "ad123", "search_flag": 8, "type": 2, "app_type": 2},
                    )
                )
            raw = json.loads((job.path / "raw.json").read_text(encoding="utf-8"))

        self.assertEqual(summary["detail_v2_status"], "fetched")
        self.assertIn("search_flag=8", summary["detail_url"])
        self.assertEqual(raw["items"][0]["detail_v2"]["body"]["data"]["title"], "detail")

    def test_cli_ad_dry_run_records_url_search_flag_and_app_type(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "guangdada_crawl.cli",
                    "--jobs-dir",
                    tmp,
                    "crawl",
                    "ad",
                    "--url",
                    "https://guangdada.net/modules/creative/display-ads/detail?id=ad123&type=2&search_flag=8",
                    "--app-type",
                    "2",
                    "--dry-run",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(result.stdout)
            manifest = json.loads(Path(payload["manifest_path"]).read_text(encoding="utf-8"))

        self.assertEqual(manifest["params"]["url"], "https://guangdada.net/modules/creative/display-ads/detail?id=ad123&type=2&search_flag=8")
        self.assertEqual(manifest["params"]["app_type"], 2)

    def test_advertiser_adapter_filters_first_seen_date_from_fake_run_batch(self) -> None:
        import asyncio

        from guangdada_crawl.adapters import run_advertiser_crawl
        from guangdada_crawl.jobs import JobStore

        async def fake_run_batch(**kwargs):
            self.assertEqual(kwargs["keywords"], ["com.example.app"])
            self.assertEqual(kwargs["first_seen_target_ymd"], "2026-06-16")
            self.assertEqual(kwargs["day_span"], "7")
            self.assertEqual(kwargs["date_range"], "2026-06-16")
            self.assertIs(kwargs["is_tool"], False)
            return [
                {
                    "all_creatives": [
                        {"ad_key": "keep", "first_seen": 1781539200},
                        {
                            "ad_key": "keep-resume",
                            "first_seen": 1781539200,
                            "created_at": 1781193600,
                            "resume_advertising_flag": True,
                        },
                        {"ad_key": "drop", "first_seen": 1781625600},
                    ]
                }
            ]

        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            job = store.create_job(kind="advertiser", params={"keyword": "com.example.app"}, dry_run=False)
            with patch("ua_workflows.shared.guangdada.search.run_batch", fake_run_batch):
                summary = asyncio.run(
                    run_advertiser_crawl(
                        job,
                        store,
                        {
                            "keyword": "com.example.app",
                            "label": "Example",
                            "date": "2026-06-16",
                            "cover_dedupe": False,
                        },
                    )
                )
            raw = json.loads((job.path / "raw.json").read_text(encoding="utf-8"))

        self.assertEqual(summary["total"], 2)
        self.assertEqual(summary["date_mode"], "first-seen")
        self.assertEqual([item["creative"]["ad_key"] for item in raw["items"]], ["keep", "keep-resume"])

    def test_advertiser_adapter_supports_active_date_and_excludes_neighbor_advertisers(self) -> None:
        import asyncio

        from guangdada_crawl.adapters import run_advertiser_crawl
        from guangdada_crawl.jobs import JobStore

        async def fake_run_batch(**kwargs):
            self.assertEqual(kwargs["keywords"], ["com.pixverseai.pixverse"])
            self.assertIsNone(kwargs["first_seen_target_ymd"])
            self.assertEqual(kwargs["day_span"], "30")
            self.assertEqual(kwargs["date_range"], "2026-06-17")
            return [
                {
                    "all_creatives": [
                        {
                            "ad_key": "new-active",
                            "advertiser_name": "PixVerse: AI Video Generator",
                            "first_seen": 1781676838,
                            "date_range_text": "2026-06-17~2026-06-17",
                        },
                        {
                            "ad_key": "old-active",
                            "advertiser_name": "PixVerse: AI Video Generator",
                            "first_seen": 1781593200,
                            "date_range_text": "2026-06-16~2026-06-17",
                        },
                        {
                            "ad_key": "neighbor",
                            "advertiser_name": "Gen AI Video Generator",
                            "first_seen": 1781593200,
                            "date_range_text": "2026-06-16~2026-06-17",
                        },
                        {
                            "ad_key": "not-active",
                            "advertiser_name": "PixVerse: AI Video Generator",
                            "first_seen": 1781452800,
                            "date_range_text": "2026-06-15~2026-06-16",
                        },
                    ]
                }
            ]

        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            job = store.create_job(kind="advertiser", params={"keyword": "com.pixverseai.pixverse"}, dry_run=False)
            with patch("ua_workflows.shared.guangdada.search.run_batch", fake_run_batch):
                summary = asyncio.run(
                    run_advertiser_crawl(
                        job,
                        store,
                        {
                            "keyword": "com.pixverseai.pixverse",
                            "label": "Pixverse:AI Video Generator",
                            "date": "2026-06-17",
                            "date_mode": "active",
                            "time_window": "30d",
                            "cover_dedupe": False,
                        },
                    )
                )
            raw = json.loads((job.path / "raw.json").read_text(encoding="utf-8"))

        self.assertEqual(summary["total"], 2)
        self.assertEqual(summary["date_mode"], "active")
        self.assertEqual(summary["time_window"], "30d")
        self.assertEqual([item["creative"]["ad_key"] for item in raw["items"]], ["new-active", "old-active"])

    def test_advertiser_adapter_maps_one_year_time_window_to_guangdada_day_span(self) -> None:
        import asyncio

        from guangdada_crawl.adapters import run_advertiser_crawl
        from guangdada_crawl.jobs import JobStore

        async def fake_run_batch(**kwargs):
            self.assertEqual(kwargs["day_span"], "365")
            self.assertEqual(kwargs["date_range"], "2026-06-16")
            return [{"all_creatives": [{"ad_key": "keep", "first_seen": 1781539200}]}]

        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            job = store.create_job(kind="advertiser", params={"keyword": "com.example.app"}, dry_run=False)
            with patch("ua_workflows.shared.guangdada.search.run_batch", fake_run_batch):
                summary = asyncio.run(
                    run_advertiser_crawl(
                        job,
                        store,
                        {
                            "keyword": "com.example.app",
                            "label": "Example",
                            "date": "2026-06-16",
                            "time_window": "1y",
                            "cover_dedupe": False,
                        },
                    )
                )

        self.assertEqual(summary["time_window"], "1y")

    def test_advertiser_adapter_applies_cover_dedupe_to_raw_items(self) -> None:
        import asyncio

        from guangdada_crawl.adapters import run_advertiser_crawl
        from guangdada_crawl.jobs import JobStore

        async def fake_run_batch(**kwargs):
            return [
                {
                    "all_creatives": [
                        {"ad_key": "keep", "first_seen": 1781539200, "preview_img_url": "https://example.com/a.jpg"},
                        {"ad_key": "drop", "first_seen": 1781539200, "preview_img_url": "https://example.com/b.jpg"},
                    ]
                }
            ]

        def fake_cover_dedupe(items, target_date, crawl_date=None):
            self.assertEqual(target_date, "2026-06-16")
            return items[:1], {
                "skipped": False,
                "input_count": 2,
                "output_count": 1,
                "removed_total": 1,
                "per_appid": [
                    {
                        "appid": "com.example.app",
                        "removed": [{"ad_key": "drop", "kept_ad_key": "keep", "reason": "cover_style_cluster"}],
                    }
                ],
            }

        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            job = store.create_job(kind="advertiser", params={"keyword": "com.example.app"}, dry_run=False)
            with patch("ua_workflows.shared.guangdada.search.run_batch", fake_run_batch):
                with patch("ua_workflows.video_enhancer.cover_dedupe.apply_intraday_cover_style_dedupe", fake_cover_dedupe):
                    summary = asyncio.run(
                        run_advertiser_crawl(
                            job,
                            store,
                            {
                                "keyword": "com.example.app",
                                "label": "Example",
                                "date": "2026-06-16",
                                "cover_dedupe": True,
                            },
                        )
                    )
            raw = json.loads((job.path / "raw.json").read_text(encoding="utf-8"))

        self.assertEqual(summary["total_before_cover_dedupe"], 2)
        self.assertEqual(summary["total"], 1)
        self.assertEqual(summary["cover_dedupe_removed_total"], 1)
        self.assertEqual(raw["total_before_cover_dedupe"], 2)
        self.assertEqual(raw["total"], 1)
        self.assertEqual(raw["items"][0]["creative"]["ad_key"], "keep")
        self.assertEqual(raw["cover_style_intraday_report"]["removed_total"], 1)

    def test_advertiser_adapter_can_skip_cover_dedupe(self) -> None:
        import asyncio

        from guangdada_crawl.adapters import run_advertiser_crawl
        from guangdada_crawl.jobs import JobStore

        async def fake_run_batch(**kwargs):
            return [{"all_creatives": [{"ad_key": "keep", "first_seen": 1781539200}]}]

        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            job = store.create_job(kind="advertiser", params={"keyword": "com.example.app"}, dry_run=False)
            with patch("ua_workflows.shared.guangdada.search.run_batch", fake_run_batch):
                with patch("ua_workflows.video_enhancer.cover_dedupe.apply_intraday_cover_style_dedupe") as dedupe:
                    summary = asyncio.run(
                        run_advertiser_crawl(
                            job,
                            store,
                            {
                                "keyword": "com.example.app",
                                "label": "Example",
                                "date": "2026-06-16",
                                "cover_dedupe": False,
                            },
                        )
                    )
            raw = json.loads((job.path / "raw.json").read_text(encoding="utf-8"))

        dedupe.assert_not_called()
        self.assertEqual(summary["total"], 1)
        self.assertEqual(raw["cover_style_intraday_report"]["skipped"], True)
        self.assertEqual(raw["cover_style_intraday_report"]["reason"], "disabled_by_param")

    def test_cover_dedupe_reports_unavailable_when_ve_dependency_is_absent(self) -> None:
        from guangdada_crawl.adapters import _apply_cover_dedupe_to_payload

        real_import = builtins.__import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "ua_workflows.video_enhancer.cover_dedupe":
                raise ModuleNotFoundError("No module named 'ua_workflows.video_enhancer'")
            return real_import(name, globals, locals, fromlist, level)

        payload = {"items": [{"creative": {"ad_key": "keep"}}], "total": 1}
        with patch("builtins.__import__", fake_import):
            deduped, report = _apply_cover_dedupe_to_payload(
                payload,
                target_date="2026-06-16",
                enabled=True,
            )

        self.assertEqual(deduped["total"], 1)
        self.assertTrue(report["skipped"])
        self.assertEqual(report["reason"], "clip_cover_dedupe_unavailable")
        self.assertIn("No module named", report["error"])

    def test_core_day_span_click_supports_one_year_shortcut(self) -> None:
        import asyncio

        from ua_workflows.shared.guangdada import search

        calls: list[list[str]] = []

        async def fake_click(page, keys, timeout=5000):
            calls.append(list(keys))
            return any("365" in key or "近1年" in key or "1年" in key for key in keys)

        with patch("ua_workflows.shared.guangdada.search._click", fake_click):
            ok = asyncio.run(search._click_day_span(object(), "1y", ""))

        self.assertTrue(ok)
        self.assertTrue(any(any("365" in key or "近1年" in key for key in keys) for keys in calls))

    def test_chart_adapter_copies_fake_payload_to_job(self) -> None:
        import asyncio

        from guangdada_crawl.adapters import run_chart_crawl
        from guangdada_crawl.jobs import JobStore

        async def fake_run_async(args):
            raw_path = Path(args.output_prefix + "_raw.json")
            completeness_path = Path(args.output_prefix + "_completeness.json")
            raw_path.write_text("{}", encoding="utf-8")
            completeness_path.write_text('{"total": 1}', encoding="utf-8")
            return raw_path, completeness_path, {"total": 1, "items": [{"ad_key": "chart1"}]}

        params = {
            "date": "2026-06-17",
            "chart": "new",
            "categories": ["AI视频"],
            "limit": 1,
            "category_mode": "combined",
            "auth_mode": "env-login",
            "headed": False,
            "debug": False,
            "scroll_rounds": 1,
            "wait_after_filter_ms": 500,
            "cover_dedupe": False,
        }
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            job = store.create_job(kind="chart", params=params, dry_run=False)
            with patch("ua_workflows.shared.guangdada.new_charts_ai_tools.run_async", fake_run_async):
                summary = asyncio.run(run_chart_crawl(job, store, params))
            raw = json.loads((job.path / "raw.json").read_text(encoding="utf-8"))
            completeness = json.loads((job.path / "completeness.json").read_text(encoding="utf-8"))

        self.assertEqual(summary["total"], 1)
        self.assertEqual(raw["items"][0]["ad_key"], "chart1")
        self.assertEqual(completeness["total"], 1)

    def test_chart_adapter_applies_cover_dedupe_to_payload_items(self) -> None:
        import asyncio

        from guangdada_crawl.adapters import run_chart_crawl
        from guangdada_crawl.jobs import JobStore

        async def fake_run_async(args):
            raw_path = Path(args.output_prefix + "_raw.json")
            completeness_path = Path(args.output_prefix + "_completeness.json")
            raw_path.write_text("{}", encoding="utf-8")
            completeness_path.write_text('{"total": 2}', encoding="utf-8")
            return raw_path, completeness_path, {
                "total": 2,
                "items": [
                    {"ad_key": "chart_keep", "creative": {"ad_key": "chart_keep"}},
                    {"ad_key": "chart_drop", "creative": {"ad_key": "chart_drop"}},
                ],
            }

        def fake_cover_dedupe(items, target_date, crawl_date=None):
            self.assertEqual(target_date, "2026-06-17")
            return items[:1], {"skipped": False, "input_count": 2, "output_count": 1, "removed_total": 1}

        params = {
            "date": "2026-06-17",
            "chart": "new",
            "categories": ["AI视频"],
            "limit": 2,
            "category_mode": "combined",
            "auth_mode": "env-login",
            "headed": False,
            "debug": False,
            "scroll_rounds": 1,
            "wait_after_filter_ms": 500,
            "cover_dedupe": True,
        }
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            job = store.create_job(kind="chart", params=params, dry_run=False)
            with patch("ua_workflows.shared.guangdada.new_charts_ai_tools.run_async", fake_run_async):
                with patch("ua_workflows.video_enhancer.cover_dedupe.apply_intraday_cover_style_dedupe", fake_cover_dedupe):
                    summary = asyncio.run(run_chart_crawl(job, store, params))
            raw = json.loads((job.path / "raw.json").read_text(encoding="utf-8"))

        self.assertEqual(summary["total_before_cover_dedupe"], 2)
        self.assertEqual(summary["total"], 1)
        self.assertEqual(summary["cover_dedupe_removed_total"], 1)
        self.assertEqual(raw["total_before_cover_dedupe"], 2)
        self.assertEqual(raw["total"], 1)
        self.assertEqual(raw["items"][0]["ad_key"], "chart_keep")
        self.assertEqual(raw["cover_style_intraday_report"]["removed_total"], 1)


if __name__ == "__main__":
    unittest.main()
