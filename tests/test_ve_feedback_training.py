from __future__ import annotations

import tempfile
import unittest
from pathlib import Path


class VeFeedbackTrainingTest(unittest.TestCase):
    def test_normalize_record_uses_material_features_and_label(self) -> None:
        from ua_workflows.video_enhancer.feedback_training import normalize_record

        sample = normalize_record(
            {
                "record_id": "rec1",
                "fields": {
                    "广告ID": "ad_1",
                    "标题": [{"text": "Magic edit"}],
                    "产品": "Some Product",
                    "广告主": "Some Advertiser",
                    "核心卖点": "把普通照片变成杂志大片",
                    "玩法指纹": "单人照片转杂志封面",
                    "差异点": "黑白胶片风",
                    "接受情况": "接受",
                    "展示估值": 999,
                    "投放地区": "USA",
                },
            }
        )

        self.assertEqual(sample.record_id, "rec1")
        self.assertEqual(sample.ad_key, "ad_1")
        self.assertEqual(sample.label, 1)
        self.assertIn("Magic edit", sample.feature_text)
        self.assertIn("单人照片转杂志封面", sample.feature_text)
        self.assertNotIn("Some Product", sample.feature_text)
        self.assertNotIn("Some Advertiser", sample.feature_text)
        self.assertNotIn("999", sample.feature_text)

    def test_pull_storage_and_training_baseline(self) -> None:
        from ua_workflows.video_enhancer.feedback_training import (
            BitableRef,
            export_dataset,
            load_labeled_samples,
            normalize_record,
            train_baseline_model,
            upsert_samples,
        )

        records = []
        for idx in range(6):
            records.append(
                normalize_record(
                    {
                        "record_id": f"accept_{idx}",
                        "fields": {
                            "广告ID": f"ad_accept_{idx}",
                            "标题": "AI 写真大片",
                            "核心卖点": "照片变高质感写真",
                            "玩法指纹": "照片转杂志大片",
                            "接受情况": "接受",
                        },
                    }
                )
            )
            records.append(
                normalize_record(
                    {
                        "record_id": f"reject_{idx}",
                        "fields": {
                            "广告ID": f"ad_reject_{idx}",
                            "标题": "免费聊天交友",
                            "核心卖点": "附近聊天",
                            "玩法指纹": "成人交友导流",
                            "接受情况": "删除",
                        },
                    }
                )
            )

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "feedback.db"
            stats = upsert_samples(
                records,
                source=BitableRef(app_token="app", table_id="tbl", view_id="vew"),
                db_path=db_path,
            )
            self.assertEqual(stats["accepted"], 6)
            self.assertEqual(stats["rejected"], 6)

            labeled = load_labeled_samples(db_path)
            dataset_path = export_dataset(labeled, run_date="2026-05-15", output_path=Path(tmp) / "dataset.jsonl")
            model_path, metrics = train_baseline_model(
                labeled,
                run_date="2026-05-15",
                model_path=Path(tmp) / "model.json",
            )

            self.assertEqual(len(labeled), 12)
            self.assertTrue(dataset_path.exists())
            self.assertIsNotNone(model_path)
            self.assertEqual(metrics["status"], "trained")


if __name__ == "__main__":
    unittest.main()
