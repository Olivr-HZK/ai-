from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ua_workflows.shared.db.arrow2 import (
    ARROW2_CRAWL_LABEL_EXPOSURE,
    ARROW2_CRAWL_LABEL_LATEST,
    init_db,
    prune_arrow2_daily_insights_not_in_raw,
)


class Arrow2WorkflowIsolationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_db = os.environ.get("ARROW2_SQLITE_PATH")
        self._tmp = tempfile.TemporaryDirectory()
        os.environ["ARROW2_SQLITE_PATH"] = str(Path(self._tmp.name) / "arrow2_test.db")
        init_db()

    def tearDown(self) -> None:
        if self._old_db is None:
            os.environ.pop("ARROW2_SQLITE_PATH", None)
        else:
            os.environ["ARROW2_SQLITE_PATH"] = self._old_db
        self._tmp.cleanup()

    def _insert_daily(self, ad_key: str, crawl_workflow: str) -> None:
        conn = sqlite3.connect(os.environ["ARROW2_SQLITE_PATH"])
        try:
            conn.execute(
                """
                INSERT INTO arrow2_daily_insights (
                  target_date, crawl_date, appid, ad_key, raw_json, crawl_workflow
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-06-05",
                    "2026-06-06",
                    "app",
                    ad_key,
                    json.dumps({"ad_key": ad_key}),
                    crawl_workflow,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def _daily_rows(self) -> list[tuple[str, str]]:
        conn = sqlite3.connect(os.environ["ARROW2_SQLITE_PATH"])
        try:
            return [
                (str(ad_key), str(workflow))
                for ad_key, workflow in conn.execute(
                    "SELECT ad_key, crawl_workflow FROM arrow2_daily_insights ORDER BY ad_key"
                ).fetchall()
            ]
        finally:
            conn.close()

    def test_exposure_prune_keeps_latest_rows_for_same_date(self) -> None:
        self._insert_daily("latest_keep", ARROW2_CRAWL_LABEL_LATEST)
        self._insert_daily("exposure_keep", ARROW2_CRAWL_LABEL_EXPOSURE)
        self._insert_daily("exposure_drop", ARROW2_CRAWL_LABEL_EXPOSURE)

        raw = {
            "items_deduped_by_ad_key": [
                {
                    "appid": "app",
                    "pull_id": "exposure_top10",
                    "creative": {"ad_key": "exposure_keep"},
                }
            ]
        }
        deleted = prune_arrow2_daily_insights_not_in_raw(
            "2026-06-05",
            raw,
            crawl_workflow=ARROW2_CRAWL_LABEL_EXPOSURE,
        )

        self.assertEqual(deleted, 1)
        self.assertEqual(
            self._daily_rows(),
            [
                ("exposure_keep", ARROW2_CRAWL_LABEL_EXPOSURE),
                ("latest_keep", ARROW2_CRAWL_LABEL_LATEST),
            ],
        )


if __name__ == "__main__":
    unittest.main()
