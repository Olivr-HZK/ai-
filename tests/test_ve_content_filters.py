from __future__ import annotations

import unittest


class VeContentFiltersTest(unittest.TestCase):
    def test_adult_filter_blocks_ai_undress_filter_from_guangdada_content(self) -> None:
        from ua_workflows.video_enhancer.content_filters import apply_adult_content_filter

        rows = [
            {
                "ad_key": "ai_undress",
                "guangdada_video_content": "视频展示用户上传照片后使用 AI 脱衣滤镜生成换装前后对比效果。",
                "material_tags": [],
            }
        ]

        newly_marked, details = apply_adult_content_filter(rows)

        self.assertEqual(newly_marked, 1)
        self.assertIs(rows[0]["exclude_from_bitable"], True)
        self.assertIs(rows[0]["exclude_from_cluster"], True)
        self.assertEqual(rows[0]["risk_level"], "高风险")
        self.assertIn("色情/成人风险", rows[0]["material_tags"])
        self.assertEqual(details[0]["ad_key"], "ai_undress")

    def test_adult_filter_blocks_two_person_kissing_content(self) -> None:
        from ua_workflows.video_enhancer.content_filters import apply_adult_content_filter

        rows = [
            {
                "ad_key": "two_person_kiss",
                "guangdada_video_content": "视频开篇直接展示 AI 生成的男女拥抱亲吻动态视频，并对比原始静态照片。",
                "material_tags": [],
            }
        ]

        newly_marked, details = apply_adult_content_filter(rows)

        self.assertEqual(newly_marked, 1)
        self.assertIs(rows[0]["exclude_from_bitable"], True)
        self.assertIs(rows[0]["exclude_from_cluster"], True)
        self.assertEqual(rows[0]["risk_level"], "高风险")
        self.assertIn("色情/成人风险", rows[0]["material_tags"])
        self.assertEqual(details[0]["ad_key"], "two_person_kiss")

    def test_adult_filter_blocks_ai_undress_from_existing_video_analysis(self) -> None:
        from ua_workflows.video_enhancer.content_filters import apply_adult_content_filter

        rows = [
            {
                "ad_key": "analysis_ai_undress",
                "analysis": "【Hook解析】开头展示用户上传人像后使用 AI 脱衣滤镜生成前后对比。",
                "effect_one_liner": "AI 脱衣滤镜前后对比",
                "material_tags": [],
            }
        ]

        newly_marked, details = apply_adult_content_filter(rows)

        self.assertEqual(newly_marked, 1)
        self.assertIs(rows[0]["exclude_from_bitable"], True)
        self.assertIs(rows[0]["exclude_from_cluster"], True)
        self.assertEqual(rows[0]["risk_level"], "高风险")
        self.assertEqual(details[0]["ad_key"], "analysis_ai_undress")

    def test_adult_filter_blocks_two_person_kiss_from_existing_video_analysis(self) -> None:
        from ua_workflows.video_enhancer.content_filters import apply_adult_content_filter

        rows = [
            {
                "ad_key": "analysis_two_person_kiss",
                "analysis": "【Hook解析】双人亲吻动态视频与原始照片对比，突出亲密互动效果。",
                "template_fingerprint": "双人照片生成亲吻动态视频",
                "material_tags": [],
            }
        ]

        newly_marked, details = apply_adult_content_filter(rows)

        self.assertEqual(newly_marked, 1)
        self.assertIs(rows[0]["exclude_from_bitable"], True)
        self.assertIs(rows[0]["exclude_from_cluster"], True)
        self.assertEqual(rows[0]["risk_level"], "高风险")
        self.assertEqual(details[0]["ad_key"], "analysis_two_person_kiss")

    def test_adult_filter_reads_db_insight_analysis_directly(self) -> None:
        from ua_workflows.video_enhancer.content_filters import apply_adult_content_filter

        rows = [
            {
                "ad_key": "db_insight_kiss",
                "insight_analysis": "【模板指纹】双人照片生成亲吻动态视频，强调情侣接吻互动。",
                "material_tags": [],
            }
        ]

        newly_marked, details = apply_adult_content_filter(rows)

        self.assertEqual(newly_marked, 1)
        self.assertIs(rows[0]["exclude_from_bitable"], True)
        self.assertIs(rows[0]["exclude_from_cluster"], True)
        self.assertEqual(details[0]["ad_key"], "db_insight_kiss")


if __name__ == "__main__":
    unittest.main()
