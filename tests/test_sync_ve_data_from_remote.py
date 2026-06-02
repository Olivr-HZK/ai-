#!/usr/bin/env python3
"""Static checks for the VE-only remote data sync script."""

from __future__ import annotations

import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "sync_ve_data_from_remote.sh"


class SyncVeDataFromRemoteScriptTest(unittest.TestCase):
    def test_script_exists_and_is_ve_only_snapshot_sync(self) -> None:
        self.assertTrue(SCRIPT.exists())
        text = SCRIPT.read_text(encoding="utf-8")

        self.assertIn("ggbond@10.125.46.30", text)
        self.assertIn("/Users/ggbond/oliver/ai-", text)
        self.assertIn("data/remote_snapshots/ve", text)
        self.assertIn("video_enhancer_pipeline.db", text)
        self.assertIn(".backup", text)
        self.assertIn("PRAGMA quick_check", text)
        self.assertIn("daily_creative_insights", text)
        self.assertIn("--exclude 'arrow2_pipeline.db'", text)

    def test_script_has_valid_bash_syntax(self) -> None:
        subprocess.run(["bash", "-n", str(SCRIPT)], check=True, cwd=str(ROOT))


if __name__ == "__main__":
    unittest.main()
