#!/usr/bin/env python3
"""Static checks for the daily VE cron wrapper."""

from __future__ import annotations

import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "cron_ai_video_enhancer_daily.sh"
GITIGNORE = ROOT / ".gitignore"


class CronVideoEnhancerDailyTest(unittest.TestCase):
    def test_daily_cron_runs_main_workflow_only(self) -> None:
        text = SCRIPT.read_text(encoding="utf-8")

        self.assertIn('"$ROOT/scripts/run_video_enhancer.py"', text)
        self.assertNotIn('"$ROOT/scripts/run_ve_haopeng_topn_push.py"', text)
        self.assertNotIn("FEISHU_TEST_WEBHOOK", text)
        self.assertNotIn("FEISHU_DAILY_PUSH_CHAT_ID", text)
        self.assertNotIn('exec "$ROOT/.venv/bin/python" "$ROOT/scripts/run_video_enhancer.py"', text)

    def test_pipeline_topn_push_is_disabled_until_explicitly_enabled(self) -> None:
        from ua_workflows.video_enhancer import pipeline

        args = Namespace(
            no_topn_push=False,
            topn_input_json="",
            topn_chat_id="",
            topn_send_mode=None,
            topn_top_n=10,
        )

        with patch.dict("os.environ", {}, clear=True), patch.object(pipeline, "_run") as mocked_run:
            pipeline._run_haopeng_topn_push("python", args, "2026-06-11")
            mocked_run.assert_not_called()

        with patch.dict("os.environ", {"VE_HAOPENG_TOPN_ENABLED": "1"}, clear=True), patch.object(
            pipeline, "_run"
        ) as mocked_run:
            pipeline._run_haopeng_topn_push("python", args, "2026-06-11")
            mocked_run.assert_called_once()
            cmd = mocked_run.call_args.args[0]
            self.assertIn("ua_workflows.video_enhancer.haopeng_topn_push", cmd)
            self.assertIn("--date", cmd)

    def test_local_haopeng_experiment_outputs_are_ignored(self) -> None:
        text = GITIGNORE.read_text(encoding="utf-8")

        self.assertIn("data/haopeng_topn_experiments/", text)


if __name__ == "__main__":
    unittest.main()
