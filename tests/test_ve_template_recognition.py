from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path


class VeTemplateRecognitionTest(unittest.TestCase):
    def test_build_tasks_selects_three_star_or_higher_rows_only(self) -> None:
        from ua_workflows.video_enhancer.template_recognition import build_template_tasks

        rows = [
            {
                "record_id": "rec3",
                "fields": {
                    "广告ID": "ad3",
                    "产品": "Glam AI",
                    "抓取日期": "2026-06-15",
                    "浩鹏评分": "3星",
                    "视频链接": "https://example.com/a.mp4",
                    "视频时长": 8,
                    "核心卖点": "自拍跳舞模板",
                    "脚本/口播": "人物转身并跟随镜头移动",
                },
            },
            {
                "record_id": "rec2",
                "fields": {
                    "广告ID": "ad2",
                    "抓取日期": "2026-06-15",
                    "浩鹏评分": "4星",
                    "封面图链接": "https://example.com/b.png",
                },
            },
            {
                "record_id": "rec_low",
                "fields": {
                    "广告ID": "ad_low",
                    "抓取日期": "2026-06-15",
                    "浩鹏评分": "2星",
                    "封面图链接": "https://example.com/low.png",
                },
            },
        ]

        tasks = build_template_tasks(rows, target_date="2026-06-15", reviewer="haopeng")

        self.assertEqual([task["ad_key"] for task in tasks], ["ad3", "ad2"])
        self.assertEqual(tasks[0]["rating"], 3)
        self.assertEqual(tasks[1]["rating"], 4)
        self.assertEqual(tasks[0]["suggested_template_kind"], "video_template_candidate")
        self.assertEqual(tasks[0]["aigc_template_copy_input"]["source_url"], "https://example.com/a.mp4")

    def test_build_tasks_can_include_legacy_accepted_rows(self) -> None:
        from ua_workflows.video_enhancer.template_recognition import build_template_tasks

        tasks = build_template_tasks(
            [
                {
                    "record_id": "rec_legacy",
                    "fields": {
                        "广告ID": "ad_legacy",
                        "抓取日期": "2026-06-15",
                        "浩鹏接受情况": "采纳",
                        "封面图链接": "https://example.com/cover.png",
                    },
                }
            ],
            target_date="2026-06-15",
            reviewer="haopeng",
            include_legacy=True,
        )

        self.assertEqual([task["ad_key"] for task in tasks], ["ad_legacy"])
        self.assertEqual(tasks[0]["suggested_template_kind"], "image_template_candidate")

    def test_write_artifacts_outputs_json_and_markdown(self) -> None:
        from ua_workflows.video_enhancer.template_recognition import write_artifacts

        with tempfile.TemporaryDirectory() as tmp:
            data_path, report_path = write_artifacts(
                [
                    {
                        "ad_key": "ad3",
                        "product": "Glam AI",
                        "rating": 5,
                        "suggested_template_kind": "image_template_candidate",
                        "aigc_template_copy_input": {"source_url": "https://example.com/cover.png"},
                    }
                ],
                target_date="2026-06-15",
                data_dir=Path(tmp),
                reports_dir=Path(tmp),
            )

            payload = json.loads(data_path.read_text(encoding="utf-8"))
            report = report_path.read_text(encoding="utf-8")

        self.assertEqual(payload["target_date"], "2026-06-15")
        self.assertEqual(payload["tasks"][0]["ad_key"], "ad3")
        self.assertIn("ad3", report)
        self.assertIn("image_template_candidate", report)


if __name__ == "__main__":
    unittest.main()
