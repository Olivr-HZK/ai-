from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_crawl_report(path: Path, rows: list[dict]) -> None:
    _write_json(
        path,
        {
            "per_product": [
                {
                    "product": row["product"],
                    "kept_after_crawl_filter": row.get("kept", 0),
                }
                for row in rows
            ]
        },
    )


class WeeklyCompetitorReviewTest(unittest.TestCase):
    def test_old_competitor_pool_uses_first_day_and_classifies_low_volume(self) -> None:
        from ua_workflows.video_enhancer.weekly_competitor_review import build_weekly_review

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _write_crawl_report(
                data_dir / "workflow_video_enhancer_2026-05-25_crawl_product_retention.json",
                [
                    {"product": "OldZero", "kept": 0},
                    {"product": "OldLowHigh", "kept": 4},
                    {"product": "OldLowLow", "kept": 3},
                    {"product": "OldEnough", "kept": 7},
                ],
            )
            _write_crawl_report(
                data_dir / "workflow_video_enhancer_2026-05-26_crawl_product_retention.json",
                [
                    {"product": "OldZero", "kept": 0},
                    {"product": "OldLowHigh", "kept": 4},
                    {"product": "OldLowLow", "kept": 3},
                    {"product": "OldEnough", "kept": 8},
                    {"product": "NewMidweek", "kept": 1},
                ],
            )

            report = build_weekly_review(
                run_date=date(2026, 6, 1),
                data_dir=data_dir,
                feedback_rows=[
                    {"product": "OldLowHigh", "date": "2026-05-27", "actual_hp": "采纳"},
                    {"product": "OldLowLow", "date": "2026-05-27", "actual_hp": "不采纳"},
                    {"product": "OldLowLow", "date": "2026-05-28", "actual_hp": "删除"},
                ],
                current_competitors={"OldEnough", "NewMidweek"},
                chart_paths=[],
            )

        self.assertEqual(report["week_start"], "2026-05-25")
        self.assertEqual(report["week_end"], "2026-05-31")
        actions = {row["product"]: row["recommendation"] for row in report["existing_competitors"]}
        self.assertEqual(actions["OldZero"], "remove")
        self.assertEqual(actions["OldLowHigh"], "watch")
        self.assertEqual(actions["OldLowLow"], "remove")
        self.assertEqual(actions["OldEnough"], "keep")
        self.assertNotIn("NewMidweek", actions)

    def test_weekly_chart_candidates_are_advertisers_over_threshold(self) -> None:
        from ua_workflows.video_enhancer.weekly_competitor_review import build_weekly_review

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _write_crawl_report(
                data_dir / "workflow_video_enhancer_2026-05-25_crawl_product_retention.json",
                [{"product": "OldEnough", "kept": 12}],
            )
            chart_items = []
            for idx in range(6):
                chart_items.append({"creative": {"ad_key": f"t{idx}", "advertiser_name": "TeraBox"}})
            for idx in range(5):
                chart_items.append({"creative": {"ad_key": f"n{idx}", "advertiser_name": "NotEnough"}})
            for idx in range(7):
                chart_items.append({"creative": {"ad_key": f"d{idx}", "advertiser_name": "Dreamina AI: Image&Video Maker"}})
            chart_path = data_dir / "guangdada_new_charts_ai_tools_2026-06-02_raw.json"
            _write_json(chart_path, {"items": chart_items})

            report = build_weekly_review(
                run_date=date(2026, 6, 1),
                data_dir=data_dir,
                feedback_rows=[],
                current_competitors={"OldEnough", "Dreamina AI: Image&Video Maker"},
                chart_paths=[chart_path],
            )

        candidates = {row["advertiser"]: row for row in report["new_competitor_candidates"]}
        self.assertEqual(candidates["TeraBox"]["material_count"], 6)
        self.assertEqual(candidates["TeraBox"]["status"], "new_candidate")
        self.assertEqual(candidates["Dreamina AI: Image&Video Maker"]["status"], "already_in_competitor_list")
        self.assertNotIn("NotEnough", candidates)

    def test_feishu_card_uses_product_columns_and_short_rules(self) -> None:
        from ua_workflows.video_enhancer.weekly_competitor_review import build_feishu_card

        report = {
            "week_start": "2026-05-25",
            "week_end": "2026-05-31",
            "remove": [{"product": "Creati", "weekly_materials": 0, "adoption_rate": None}],
            "watch": [{"product": "DreamFace", "weekly_materials": 8, "adoption_rate": 0.5}],
            "new_competitor_candidates": [
                {"advertiser": "TeraBox", "material_count": 6, "status": "new_candidate"}
            ],
        }

        payload = build_feishu_card(report)
        text = json.dumps(payload, ensure_ascii=False)

        self.assertEqual(payload["msg_type"], "interactive")
        self.assertIn('"tag": "column_set"', text)
        self.assertIn("建议剔除：0条；或少于10条且浩鹏采纳低于50%。", text)
        self.assertIn("低量观察：少于10条，但浩鹏采纳达到50%，或反馈不够。", text)
        self.assertIn("新竞品候选：AI图像 / AI视频周榜里，同广告主 >5条。", text)
        self.assertIn("Creati", text)
        self.assertIn("DreamFace", text)
        self.assertIn("TeraBox", text)
        self.assertNotIn("口径", text)

    def test_chart_discovery_does_not_fallback_to_stale_raw(self) -> None:
        from ua_workflows.video_enhancer.weekly_competitor_review import build_weekly_review

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            _write_crawl_report(
                data_dir / "workflow_video_enhancer_2026-06-01_crawl_product_retention.json",
                [{"product": "OldEnough", "kept": 12}],
            )
            _write_json(
                data_dir / "guangdada_new_charts_ai_tools_2026-06-02_raw.json",
                {
                    "items": [
                        {"creative": {"ad_key": f"stale_{idx}", "advertiser_name": "OldChart"}}
                        for idx in range(9)
                    ]
                },
            )

            report = build_weekly_review(
                run_date=date(2026, 6, 8),
                data_dir=data_dir,
                feedback_rows=[],
                current_competitors={"OldEnough"},
            )

        self.assertEqual(report["week_start"], "2026-06-01")
        self.assertEqual(report["new_competitor_candidates"], [])


if __name__ == "__main__":
    unittest.main()
