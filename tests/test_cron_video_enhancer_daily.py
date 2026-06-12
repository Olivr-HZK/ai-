#!/usr/bin/env python3
"""Static checks for the daily VE cron wrapper."""

from __future__ import annotations

import unittest
from pathlib import Path


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

    def test_local_haopeng_experiment_outputs_are_ignored(self) -> None:
        text = GITIGNORE.read_text(encoding="utf-8")

        self.assertIn("data/haopeng_topn_experiments/", text)


if __name__ == "__main__":
    unittest.main()
