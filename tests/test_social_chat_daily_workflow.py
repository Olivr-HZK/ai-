from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))


class SocialChatDailyWorkflowTest(unittest.TestCase):
    def test_config_contains_requested_android_appids(self) -> None:
        data = json.loads((ROOT / "config" / "social_chat_apps.json").read_text(encoding="utf-8"))
        self.assertEqual(len(data["social_chat"]), 8)
        self.assertEqual(data["social_chat"]["PolyBuzz"], "ai.socialapps.speakmaster")
        self.assertEqual(data["social_chat"]["CHAI"], "com.Beauchamp.Messenger.external")

    def test_wrapper_builds_isolated_pipeline_command(self) -> None:
        import run_social_chat_daily as workflow

        with patch.dict(os.environ, {"SOCIAL_CHAT_BITABLE_URL": "https://example.invalid/base/x?table=tbl"}, clear=True), patch.object(
            workflow.subprocess, "run"
        ) as run:
            run.return_value.returncode = 0
            self.assertEqual(workflow.main.__module__, "run_social_chat_daily")
            with patch.object(workflow, "parse_args", return_value=type("Args", (), {"date": "2026-08-05", "bitable_url": "", "skip_cover_dedupe": False, "no_bitable_sync": False, "db_path": ""})()):
                self.assertEqual(workflow.main(), 0)
        command = run.call_args.args[0]
        self.assertIn("--workflow-name", command)
        self.assertIn("social_chat", command)
        self.assertIn("--per-advertiser-daily-limit", command)
        self.assertEqual(command[command.index("--per-advertiser-daily-limit") + 1], "10")
        self.assertIn("--db-path", command) if "--db-path" in command else None
        self.assertEqual(run.call_args.kwargs["env"]["VIDEO_ENHANCER_DB_PATH"], str(workflow.DEFAULT_DB))
        self.assertIn("--skip-advertiser-filter", command)
        self.assertIn("--crawl-categories", command)
        self.assertIn("social_chat", command)

    def test_crawl_loader_accepts_custom_category_and_advertiser_bypass(self) -> None:
        from ua_workflows.video_enhancer import crawl

        competitors = crawl._load_workflow_competitors(
            config_file=ROOT / "config" / "social_chat_apps.json",
            categories=("social_chat",),
        )
        self.assertEqual([item.appid for item in competitors], [
            "com.aigc.ushow.ichat",
            "com.scatterlab.messenger",
            "ai.tain.reelso",
            "ai.socialapps.speakmaster",
            "ai.character.app",
            "com.Beauchamp.Messenger.external",
            "com.weaver.app.prod",
            "com.tipsyturbo.app",
        ])


if __name__ == "__main__":
    unittest.main()
