from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class VECompetitorListTest(unittest.TestCase):
    def test_normalize_competitor_records_keeps_valid_unique_rows(self) -> None:
        from ua_workflows.video_enhancer.competitor_list import normalize_competitor_records

        rows = normalize_competitor_records(
            [
                {"fields": {"广告主名": " Pixverse ", "appid": " com.pixverse "}},
                {"fields": {"广告主名": "Missing Appid", "appid": ""}},
                {"fields": {"广告主名": "Pixverse", "appid": "com.pixverse"}},
                {"广告主名": "DreamFace", "appid": "com.dream"},
            ]
        )

        self.assertEqual(
            [(row.product, row.appid) for row in rows],
            [("Pixverse", "com.pixverse"), ("DreamFace", "com.dream")],
        )

    def test_merge_competitor_rows_preserves_non_ve_and_known_photo_group(self) -> None:
        from ua_workflows.video_enhancer.competitor_list import (
            CompetitorListRow,
            merge_competitors_into_ai_product_config,
        )

        merged = merge_competitors_into_ai_product_config(
            {
                "video": {"Old Video": "old.video", "DreamFace": "com.dream"},
                "photo": {"Remini": "com.remini"},
                "seek": {"Nova": "com.nova"},
            },
            [
                CompetitorListRow(product="Remini", appid="com.remini"),
                CompetitorListRow(product="DreamFace", appid="com.dream"),
                CompetitorListRow(product="New App", appid="com.new"),
            ],
        )

        self.assertEqual(merged["photo"], {"Remini": "com.remini"})
        self.assertEqual(merged["video"], {"DreamFace": "com.dream", "New App": "com.new"})
        self.assertEqual(merged["seek"], {"Nova": "com.nova"})
        self.assertNotIn("Old Video", merged["video"])

    def test_sync_competitor_config_writes_bitable_rows_to_config(self) -> None:
        from ua_workflows.video_enhancer.competitor_list import (
            CompetitorListRow,
            sync_competitor_config_from_bitable,
        )

        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "ai_product.json"
            config_path.write_text(
                json.dumps(
                    {
                        "video": {"Old": "old.app"},
                        "photo": {"Remini": "com.remini"},
                        "seek": {"Nova": "com.nova"},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with patch(
                "ua_workflows.video_enhancer.competitor_list.load_competitors_from_bitable",
                return_value=[
                    CompetitorListRow(product="Remini", appid="com.remini"),
                    CompetitorListRow(product="Pixverse", appid="com.pixverse"),
                ],
            ):
                result = sync_competitor_config_from_bitable(
                    config_path=config_path,
                    bitable_url="https://example.feishu.cn/base/app?table=tbl",
                )

            saved = json.loads(config_path.read_text(encoding="utf-8"))
            self.assertTrue(result.ok)
            self.assertEqual(result.competitor_count, 2)
            self.assertEqual(saved["photo"], {"Remini": "com.remini"})
            self.assertEqual(saved["video"], {"Pixverse": "com.pixverse"})
            self.assertEqual(saved["seek"], {"Nova": "com.nova"})

    def test_sync_competitor_config_honors_env_field_overrides(self) -> None:
        from ua_workflows.video_enhancer.competitor_list import (
            CompetitorListRow,
            sync_competitor_config_from_bitable,
        )

        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "ai_product.json"
            config_path.write_text(json.dumps({"video": {}, "photo": {}}), encoding="utf-8")
            with patch.dict(
                "os.environ",
                {
                    "VE_COMPETITOR_LIST_TABLE_NAME": "正式竞品",
                    "VE_COMPETITOR_LIST_PRODUCT_FIELD": "产品名",
                    "VE_COMPETITOR_LIST_APPID_FIELD": "包名",
                },
                clear=False,
            ), patch(
                "ua_workflows.video_enhancer.competitor_list.load_competitors_from_bitable",
                return_value=[CompetitorListRow(product="Pixverse", appid="com.pixverse")],
            ) as mocked_load:
                result = sync_competitor_config_from_bitable(
                    config_path=config_path,
                    bitable_url="https://example.feishu.cn/base/app?table=tbl",
                )

            self.assertTrue(result.ok)
            mocked_load.assert_called_once()
            _, kwargs = mocked_load.call_args
            self.assertEqual(kwargs["table_name"], "正式竞品")
            self.assertEqual(kwargs["product_field"], "产品名")
            self.assertEqual(kwargs["appid_field"], "包名")

if __name__ == "__main__":
    unittest.main()
