#!/usr/bin/env python3
"""Tests for the VE core-play shadow report helpers."""

from __future__ import annotations

import unittest
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ve_core_play_shadow_report import (
    build_dedupe_prompt,
    build_json_repair_prompt,
    build_missing_label_prompt,
    ensure_play_labels,
    extract_play_label,
    load_local_fallbacks,
    run_dedupe,
    summarize_decision_metrics,
)


class VeCorePlayShadowReportTest(unittest.TestCase):
    def test_dedupe_prompt_keeps_block_conservative_without_numeric_threshold(self) -> None:
        prompt = build_dedupe_prompt(
            product="GIO",
            target_date="2026-05-27",
            history=[
                {
                    "hidx": 0,
                    "ad_key": "hist1",
                    "date": "2026-05-25",
                    "hp": "采纳",
                    "core": "自拍生成生日主题写真，呈现火焰蛋糕特效场景",
                }
            ],
            candidates=[
                {
                    "idx": 0,
                    "ad_key": "cur1",
                    "core": "自拍一键生成生日主题专业写真大片",
                }
            ],
        )

        self.assertIn("历史采纳/入素材库不是放行理由", prompt)
        self.assertIn("不要使用固定数值阈值", prompt)
        self.assertIn("block", prompt)
        self.assertIn("几乎同款", prompt)
        self.assertIn("同主题延展", prompt)
        self.assertIn("只允许从“核心卖点 core”提炼关键词", prompt)

    def test_metrics_treat_only_block_as_not_synced(self) -> None:
        metrics = summarize_decision_metrics(
            [
                {"actual_hp": "不采纳", "decision": "block"},
                {"actual_hp": "重复抓取", "decision": "allow"},
                {"actual_hp": "采纳", "decision": "block"},
                {"actual_hp": "入素材库", "decision": "maybe_block"},
            ]
        )

        self.assertEqual(metrics["decision_counts"], {"block": 2, "allow": 1, "maybe_block": 1})
        self.assertEqual(
            metrics["block_only"],
            {
                "TP": 1,
                "FP": 1,
                "FN": 1,
                "TN": 1,
                "precision": 0.5,
                "recall": 0.5,
                "false_block_rate_good": 0.5,
                "sync_rate": 0.5,
            },
        )

    def test_json_repair_prompt_requests_valid_json_only(self) -> None:
        prompt = build_json_repair_prompt("[{'bad': true}]")

        self.assertIn("修复为合法 JSON", prompt)
        self.assertIn("只输出修复后的 JSON", prompt)
        self.assertIn("[{'bad': true}]", prompt)

    def test_extract_play_label_prefers_play_field_then_material_tags(self) -> None:
        self.assertEqual(
            extract_play_label({"play_label": "机甲变身", "material_tags": "玩法资产:漫画风"}),
            "机甲变身",
        )
        self.assertEqual(
            extract_play_label({"material_tags": "标签:新玩法、玩法资产:微缩小人、玩法变种:基础变体"}),
            "微缩小人",
        )

    def test_missing_label_prompt_only_asks_for_core_based_labels(self) -> None:
        prompt = build_missing_label_prompt(
            product="AI Mirror",
            rows=[{"idx": 0, "ad_key": "a1", "core": "真人照片生成星座主题插画"}],
        )

        self.assertIn("只根据核心卖点 core", prompt)
        self.assertIn("玩法标签", prompt)
        self.assertIn("星座主题插画", prompt)

    def test_load_local_fallbacks_tolerates_old_db_without_play_asset_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "old.db"
            conn = sqlite3.connect(str(db_path))
            conn.execute(
                """
                CREATE TABLE daily_creative_insights (
                    ad_key TEXT,
                    product TEXT,
                    effect_one_liner TEXT,
                    video_url TEXT,
                    preview_img_url TEXT,
                    material_tags TEXT,
                    raw_json TEXT
                )
                """
            )
            conn.execute(
                """
                INSERT INTO daily_creative_insights
                VALUES ('a1', 'AI Mirror', '核心卖点', 'https://v', 'https://c', '玩法资产:漫画风', '{"title":"标题"}')
                """
            )
            conn.commit()
            conn.close()

            rows = load_local_fallbacks(db_path)

        self.assertEqual(rows["a1"]["play_asset_name"], "")
        self.assertEqual(rows["a1"]["play_fingerprint"], "")
        self.assertEqual(rows["a1"]["material_tags"], "玩法资产:漫画风")

    def test_ensure_play_labels_batches_missing_rows(self) -> None:
        rows = [
            {"ad_key": f"a{i}", "product": "Pixverse", "core": f"核心卖点{i}"}
            for i in range(5)
        ]

        def fake_call_json(prompt: str, *, model: str) -> list[dict[str, object]]:
            return [
                {"idx": idx, "ad_key": row["ad_key"], "play_label": f"标签{row['ad_key']}"}
                for idx, row in enumerate(
                    [
                        {"ad_key": token.split('"')[0]}
                        for token in prompt.split('"ad_key": "')[1:]
                    ]
                )
            ]

        with patch("ve_core_play_shadow_report.call_json", side_effect=fake_call_json) as call:
            ensure_play_labels(rows=rows, model="fake-model", workers=3, batch_size=2)

        self.assertEqual(call.call_count, 3)
        self.assertEqual([row["play_label"] for row in rows], ["标签a0", "标签a1", "标签a2", "标签a3", "标签a4"])

    def test_run_dedupe_accepts_workers_for_product_parallelism(self) -> None:
        rows = [
            {"date": "2026-05-26", "actual_hp": "采纳", "ad_key": "h1", "product": "A", "core": "玩法一", "play_label": "标签一"},
            {"date": "2026-05-26", "actual_hp": "采纳", "ad_key": "h2", "product": "B", "core": "玩法二", "play_label": "标签二"},
            {"date": "2026-05-27", "actual_hp": "采纳", "ad_key": "c1", "product": "A", "core": "玩法一延展", "play_label": "标签一"},
            {"date": "2026-05-27", "actual_hp": "采纳", "ad_key": "c2", "product": "B", "core": "玩法二延展", "play_label": "标签二"},
        ]

        def fake_call_json(prompt: str, *, model: str) -> list[dict[str, object]]:
            if "产品：A" in prompt:
                return [{"idx": 0, "ad_key": "c1", "decision": "maybe_block", "is_new_play": False}]
            return [{"idx": 0, "ad_key": "c2", "decision": "allow", "is_new_play": True}]

        with patch("ve_core_play_shadow_report.call_json", side_effect=fake_call_json) as call:
            results = run_dedupe(
                rows=rows,
                target_date="2026-05-27",
                lookback_days=7,
                model="fake-model",
                workers=2,
            )

        self.assertEqual(call.call_count, 2)
        self.assertEqual(
            {r["ad_key"]: r["decision"] for r in results},
            {"c1": "maybe_block", "c2": "allow"},
        )

    def test_run_dedupe_uses_all_same_product_history_before_play_label(self) -> None:
        rows = [
            {"date": "2026-05-26", "actual_hp": "采纳", "ad_key": "h1", "product": "A", "core": "机甲历史", "play_label": "机甲变身"},
            {"date": "2026-05-26", "actual_hp": "采纳", "ad_key": "h2", "product": "A", "core": "赛场历史", "play_label": "赛场转播"},
            {"date": "2026-05-27", "actual_hp": "采纳", "ad_key": "c1", "product": "A", "core": "赛场当前", "play_label": "全新AI标签"},
        ]

        captured_prompts: list[str] = []

        def fake_call_json(prompt: str, *, model: str) -> list[dict[str, object]]:
            captured_prompts.append(prompt)
            return [{"idx": 0, "ad_key": "c1", "decision": "block", "is_new_play": False}]

        with patch("ve_core_play_shadow_report.call_json", side_effect=fake_call_json):
            results = run_dedupe(
                rows=rows,
                target_date="2026-05-27",
                lookback_days=7,
                model="fake-model",
                workers=1,
            )

        self.assertEqual(results[0]["decision"], "block")
        self.assertEqual(results[0]["play_label"], "全新AI标签")
        self.assertIn("机甲历史", captured_prompts[0])
        self.assertIn("赛场历史", captured_prompts[0])


if __name__ == "__main__":
    unittest.main()
