#!/usr/bin/env python3
"""Tests for VE retention and cleanup planning."""

from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class VeRetentionTest(unittest.TestCase):
    def test_archive_candidate_skips_pending_accepted_and_recent_history_refresh(self) -> None:
        from ua_workflows.video_enhancer.retention import (
            ArchivePolicy,
            classify_bitable_record,
        )

        policy = ArchivePolicy(reference_date=date(2026, 6, 5), archive_after_days=45)

        archived = classify_bitable_record(
            {
                "record_id": "rec_old",
                "fields": {
                    "广告ID": "ad_old",
                    "抓取日期": "2026-04-01",
                    "接受情况": "删除",
                    "展示估值": 10,
                    "素材标签": "普通老素材",
                },
            },
            policy,
        )
        pending = classify_bitable_record(
            {"record_id": "rec_pending", "fields": {"抓取日期": "2026-04-01", "接受情况": "待定"}},
            policy,
        )
        accepted = classify_bitable_record(
            {"record_id": "rec_ok", "fields": {"抓取日期": "2026-04-01", "浩鹏接受情况": "采纳"}},
            policy,
        )
        refresh = classify_bitable_record(
            {
                "record_id": "rec_refresh",
                "fields": {
                    "抓取日期": "2026-04-01",
                    "接受情况": "删除",
                    "素材标签": "历史簇持续发力; 老玩法新迭代",
                },
            },
            policy,
        )

        self.assertEqual(archived.action, "archive")
        self.assertEqual(archived.reason, "older_than_45_days")
        self.assertEqual(pending.action, "keep")
        self.assertEqual(pending.reason, "pending_feedback")
        self.assertEqual(accepted.action, "keep")
        self.assertEqual(accepted.reason, "accepted_feedback")
        self.assertEqual(refresh.action, "keep")
        self.assertEqual(refresh.reason, "history_refresh")

    def test_rejected_status_containing_accept_word_can_still_archive(self) -> None:
        from ua_workflows.video_enhancer.retention import ArchivePolicy, classify_bitable_record

        decision = classify_bitable_record(
            {
                "record_id": "rec_rejected",
                "fields": {
                    "广告ID": "ad_rejected",
                    "抓取日期": "2026-04-01",
                    "浩鹏接受情况": "不采纳",
                },
            },
            ArchivePolicy(reference_date=date(2026, 6, 5), archive_after_days=45),
        )

        self.assertEqual(decision.action, "archive")
        self.assertEqual(decision.reason, "older_than_45_days")

    def test_select_local_artifact_candidates_respects_keep_days_and_keep_latest_snapshots(self) -> None:
        from ua_workflows.video_enhancer.retention import (
            LocalRetentionPolicy,
            select_local_artifact_candidates,
        )

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            data_dir = root / "data"
            reports_dir = root / "reports"
            snapshots_dir = data_dir / "remote_snapshots" / "ve"
            reports_dir.mkdir(parents=True)
            snapshots_dir.mkdir(parents=True)

            old_json = data_dir / "workflow_video_enhancer_2026-04-01_raw.json"
            recent_json = data_dir / "workflow_video_enhancer_2026-06-01_raw.json"
            db_file = data_dir / "video_enhancer_pipeline.db"
            old_report = reports_dir / "workflow_video_enhancer_2026-04-01_flow_report.md"
            for path in (old_json, recent_json, db_file, old_report):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("x", encoding="utf-8")

            snapshot_db = snapshots_dir / "data" / "video_enhancer_pipeline.db"
            snapshot_db.parent.mkdir(parents=True)
            snapshot_db.write_text("db", encoding="utf-8")
            for day in ("2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04"):
                snap = snapshots_dir / "data" / f"workflow_video_enhancer_{day}_raw.json"
                snap.parent.mkdir(parents=True, exist_ok=True)
                snap.write_text(day, encoding="utf-8")

            now = datetime(2026, 6, 5, 12, 0, 0)
            _set_mtime(old_json, now - timedelta(days=40))
            _set_mtime(recent_json, now - timedelta(days=3))
            _set_mtime(db_file, now - timedelta(days=40))
            _set_mtime(old_report, now - timedelta(days=40))
            _set_mtime(snapshot_db, now - timedelta(days=40))
            for age_days, day in zip((4, 3, 2, 1), ("2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04")):
                _set_mtime(
                    snapshots_dir / "data" / f"workflow_video_enhancer_{day}_raw.json",
                    now - timedelta(days=age_days),
                )

            candidates = select_local_artifact_candidates(
                root,
                LocalRetentionPolicy(reference_now=now, artifact_keep_days=30, keep_remote_snapshots=2),
            )
            candidate_paths = {c.path.relative_to(root).as_posix() for c in candidates}

        self.assertIn("data/workflow_video_enhancer_2026-04-01_raw.json", candidate_paths)
        self.assertIn("reports/workflow_video_enhancer_2026-04-01_flow_report.md", candidate_paths)
        self.assertIn("data/remote_snapshots/ve/data/workflow_video_enhancer_2026-05-01_raw.json", candidate_paths)
        self.assertIn("data/remote_snapshots/ve/data/workflow_video_enhancer_2026-05-02_raw.json", candidate_paths)
        self.assertNotIn("data/workflow_video_enhancer_2026-06-01_raw.json", candidate_paths)
        self.assertNotIn("data/video_enhancer_pipeline.db", candidate_paths)
        self.assertNotIn("data/remote_snapshots/ve/data/video_enhancer_pipeline.db", candidate_paths)
        self.assertNotIn("data/remote_snapshots/ve/data/workflow_video_enhancer_2026-05-03_raw.json", candidate_paths)
        self.assertNotIn("data/remote_snapshots/ve/data/workflow_video_enhancer_2026-05-04_raw.json", candidate_paths)

    def test_update_bitable_records_patches_archive_fields_in_batches(self) -> None:
        from ua_workflows.video_enhancer.retention import ArchiveDecision, update_bitable_archive_fields

        calls: list[dict] = []

        class FakeResponse:
            ok = True
            text = "{}"

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {"code": 0, "data": {}}

        def fake_post(url, headers=None, json=None, timeout=None):
            calls.append({"url": url, "headers": headers, "json": json, "timeout": timeout})
            return FakeResponse()

        decisions = [
            ArchiveDecision(
                record_id=f"rec{i}",
                ad_key=f"ad{i}",
                crawl_date="2026-04-01",
                age_days=65,
                action="archive",
                reason="older_than_45_days",
                detail="超过 45 天且无保留信号",
            )
            for i in range(205)
        ]

        with patch("ua_workflows.video_enhancer.retention.requests.post", side_effect=fake_post):
            result = update_bitable_archive_fields(
                "token",
                "app",
                "tbl",
                decisions,
                archive_date="2026-06-05",
                batch_size=200,
            )

        self.assertEqual(result["updated"], 205)
        self.assertEqual(len(calls), 2)
        self.assertTrue(calls[0]["url"].endswith("/records/batch_update"))
        self.assertEqual(len(calls[0]["json"]["records"]), 200)
        first = calls[0]["json"]["records"][0]
        self.assertEqual(first["record_id"], "rec0")
        self.assertEqual(first["fields"]["生命周期状态"], "已归档")
        self.assertEqual(first["fields"]["归档日期"], "2026-06-05")
        self.assertEqual(first["fields"]["归档原因"], "超过 45 天且无保留信号")

    def test_ensure_bitable_archive_fields_creates_missing_text_fields(self) -> None:
        from ua_workflows.video_enhancer.retention import ensure_bitable_archive_fields

        calls: list[dict] = []

        class FakeGetResponse:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {
                    "code": 0,
                    "data": {
                        "items": [
                            {"field_name": "标题", "type": 1},
                            {"field_name": "生命周期状态", "type": 1},
                        ],
                        "has_more": False,
                    },
                }

        class FakePostResponse:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {"code": 0, "data": {}}

        def fake_post(url, headers=None, json=None, timeout=None):
            calls.append({"url": url, "json": json})
            return FakePostResponse()

        with (
            patch("ua_workflows.video_enhancer.retention.requests.get", return_value=FakeGetResponse()),
            patch("ua_workflows.video_enhancer.retention.requests.post", side_effect=fake_post),
        ):
            result = ensure_bitable_archive_fields("token", "app", "tbl")

        self.assertEqual(result["created"], 2)
        self.assertEqual([c["json"]["field_name"] for c in calls], ["归档日期", "归档原因"])
        self.assertEqual([c["json"]["type"] for c in calls], [1, 1])

    def test_write_report_keeps_dry_run_and_counts_sections(self) -> None:
        from ua_workflows.video_enhancer.retention import (
            ArchiveDecision,
            LocalArtifactCandidate,
            write_retention_report,
        )

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "ve_retention_2026-06-05.json"
            report = write_retention_report(
                out,
                bitable_decisions=[
                    ArchiveDecision("rec1", "ad1", "2026-04-01", 65, "archive", "older", "old"),
                    ArchiveDecision("rec2", "ad2", "2026-06-01", 4, "keep", "recent", "recent"),
                ],
                local_candidates=[
                    LocalArtifactCandidate(Path(td) / "data" / "old.json", "artifact_age", 1, 10)
                ],
                applied_bitable=False,
                applied_local=False,
            )
            saved = json.loads(out.read_text(encoding="utf-8"))

        self.assertEqual(report["summary"]["bitable_archive_candidates"], 1)
        self.assertEqual(report["summary"]["bitable_keep_records"], 1)
        self.assertEqual(report["summary"]["local_delete_candidates"], 1)
        self.assertFalse(saved["applied"]["bitable"])
        self.assertFalse(saved["applied"]["local"])


def _set_mtime(path: Path, dt: datetime) -> None:
    ts = time.mktime(dt.timetuple())
    path.touch()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.chmod(0o644)
    import os

    os.utime(path, (ts, ts))


if __name__ == "__main__":
    unittest.main()
