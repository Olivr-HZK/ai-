from __future__ import annotations

import unittest


class VeFeedbackRatingTest(unittest.TestCase):
    def test_rating_field_takes_priority_over_legacy_status(self) -> None:
        from ua_workflows.video_enhancer.feedback_rating import resolve_feedback_rating

        result = resolve_feedback_rating(
            {
                "浩鹏评分": "2星",
                "浩鹏接受情况": "采纳",
                "接受情况": "删除",
            },
            reviewer="haopeng",
        )

        self.assertEqual(result.rating, 2)
        self.assertEqual(result.rating_label, "2星")
        self.assertEqual(result.source_field, "浩鹏评分")
        self.assertEqual(result.legacy_status, "采纳")

    def test_legacy_status_maps_to_rating_when_rating_missing(self) -> None:
        from ua_workflows.video_enhancer.feedback_rating import resolve_feedback_rating

        self.assertEqual(
            resolve_feedback_rating({"浩鹏接受情况": "入素材库"}, reviewer="haopeng").rating,
            3,
        )
        self.assertEqual(
            resolve_feedback_rating({"浩鹏接受情况": "重复抓取"}, reviewer="haopeng").rating,
            1,
        )
        self.assertIsNone(
            resolve_feedback_rating({"浩鹏接受情况": "待定"}, reviewer="haopeng").rating
        )

    def test_out_of_range_rating_is_reported_without_legacy_override(self) -> None:
        from ua_workflows.video_enhancer.feedback_rating import resolve_feedback_rating

        result = resolve_feedback_rating(
            {"浩鹏评分": "4星", "浩鹏接受情况": "采纳"},
            reviewer="haopeng",
        )

        self.assertIsNone(result.rating)
        self.assertTrue(result.invalid_rating)
        self.assertEqual(result.source_field, "浩鹏评分")

    def test_weilan_field_mapping(self) -> None:
        from ua_workflows.video_enhancer.feedback_rating import resolve_feedback_rating

        result = resolve_feedback_rating({"尉蓝评分": 3}, reviewer="weilan")

        self.assertEqual(result.rating, 3)
        self.assertEqual(result.rating_label, "3星")
        self.assertEqual(result.source_field, "尉蓝评分")

    def test_numeric_string_and_chinese_star_values(self) -> None:
        from ua_workflows.video_enhancer.feedback_rating import normalize_rating_value

        self.assertEqual(normalize_rating_value("三颗星"), 3)
        self.assertIsNone(normalize_rating_value("四星"))
        self.assertIsNone(normalize_rating_value("5 stars"))
        self.assertEqual(normalize_rating_value("2"), 2)
        self.assertEqual(normalize_rating_value(1), 1)
        self.assertIsNone(normalize_rating_value("六星"))


if __name__ == "__main__":
    unittest.main()
