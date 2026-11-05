#!/usr/bin/env python3
"""Tests for AI-first VE material dedupe report helpers."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ve_ai_dedupe_report import (
    build_ai_dedupe_prompt,
    infer_gameplay_name_from_core,
    normalize_dedupe_decisions,
    run_ai_dedupe,
    summarize_dedupe_metrics,
    top_visual_history_refs,
)


class VeAiDedupeReportTest(unittest.TestCase):
    def test_dedupe_prompt_asks_model_to_judge_duplicates_without_label_bucket(self) -> None:
        prompt = build_ai_dedupe_prompt(
            product="Pixverse",
            target_date="2026-05-28",
            history=[
                {
                    "hidx": 0,
                    "ad_key": "h1",
                    "hp": "采纳",
                    "core": "真人照片生成暗黑机甲变身视频",
                    "play_label": "机甲变身",
                    "cover_url": "https://cover/h1.jpg",
                }
            ],
            candidates=[
                {
                    "idx": 0,
                    "ad_key": "c1",
                    "core": "真人照片生成机甲战士前后对比视频",
                    "play_label": "手绘",
                    "cover_url": "https://cover/c1.jpg",
                    "visual_history_refs": [{"ad_key": "h1", "similarity": 0.91}],
                }
            ],
        )

        self.assertIn("不要按玩法标签硬分桶", prompt)
        self.assertIn("核心卖点", prompt)
        self.assertIn("封面视觉近似证据", prompt)
        self.assertIn("duplicate_drop", prompt)
        self.assertIn("iteration", prompt)
        self.assertNotIn("低价值素材", prompt)

    def test_normalize_dedupe_decisions_defaults_unknown_status_to_watch(self) -> None:
        rows = normalize_dedupe_decisions(
            product="AI Mirror",
            candidates=[{"ad_key": "a1", "core": "核心"}, {"ad_key": "a2", "core": "核心2"}],
            raw_decisions=[
                {"idx": 0, "status": "nonsense", "reason": "bad"},
                {"ad_key": "a2", "status": "new_play", "play_name": "星座画像"},
            ],
        )

        self.assertEqual(rows[0]["status"], "watch")
        self.assertEqual(rows[1]["status"], "new_play")
        self.assertEqual(rows[1]["play_name"], "星座画像")

    def test_existing_specific_play_label_forces_new_play_to_iteration(self) -> None:
        rows = normalize_dedupe_decisions(
            product="Pixverse",
            candidates=[
                {"ad_key": "a1", "core": "静态照片生成指尖微缩视频", "play_label": "微缩小人"},
                {"ad_key": "a2", "core": "真人照片生成星座3D海报", "play_label": ""},
                {"ad_key": "a3", "core": "婚礼手绘写真", "play_label": "手绘"},
            ],
            raw_decisions=[
                {"idx": 0, "status": "new_play", "reason": "模型误判为新玩法"},
                {"idx": 1, "status": "new_play", "reason": "无标签新玩法"},
                {"idx": 2, "status": "new_play", "reason": "泛标签可由AI细化"},
            ],
        )

        self.assertEqual(rows[0]["status"], "iteration")
        self.assertFalse(rows[0]["is_new_play"])
        self.assertIn("已有玩法标签", rows[0]["reason"])
        self.assertEqual(rows[1]["status"], "new_play")
        self.assertEqual(rows[2]["status"], "new_play")

    def test_metrics_compare_drop_statuses_against_haopeng_negative_feedback(self) -> None:
        metrics = summarize_dedupe_metrics(
            [
                {"actual_hp": "不采纳", "status": "duplicate_drop"},
                {"actual_hp": "重复抓取", "status": "new_play"},
                {"actual_hp": "采纳", "status": "duplicate_drop"},
                {"actual_hp": "入素材库", "status": "iteration"},
            ]
        )

        self.assertEqual(metrics["drop_vs_negative"]["TP"], 1)
        self.assertEqual(metrics["drop_vs_negative"]["FP"], 1)
        self.assertEqual(metrics["drop_vs_negative"]["FN"], 1)
        self.assertEqual(metrics["drop_vs_negative"]["TN"], 1)
        self.assertEqual(metrics["drop_vs_negative"]["precision"], 0.5)

    def test_top_visual_history_refs_sorts_same_product_similarity(self) -> None:
        candidate = {"product": "A", "ad_key": "c1"}
        history = [
            {"product": "A", "ad_key": "h1", "core": "one"},
            {"product": "A", "ad_key": "h2", "core": "two"},
            {"product": "B", "ad_key": "h3", "core": "other"},
        ]
        vectors = {
            "c1": [1.0, 0.0],
            "h1": [0.8, 0.6],
            "h2": [0.95, 0.312249],
            "h3": [1.0, 0.0],
        }

        refs = top_visual_history_refs(candidate, history, vectors, top_k=2)

        self.assertEqual([ref["ad_key"] for ref in refs], ["h2", "h1"])
        self.assertNotIn("h3", [ref["ad_key"] for ref in refs])

    def test_infer_gameplay_name_from_core_handles_no_history_new_material(self) -> None:
        self.assertEqual(
            infer_gameplay_name_from_core("静态黑白照片一键生成开口唱歌动态视频效果。"),
            "照片开口唱歌",
        )
        self.assertEqual(
            infer_gameplay_name_from_core("真人照片生成专属玩具手办包装特效"),
            "玩具手办包装",
        )

    def test_no_history_new_material_gets_play_name_from_core(self) -> None:
        rows = [
            {
                "date": "2026-05-28",
                "actual_hp": "",
                "ad_key": "c1",
                "product": "DreamFace",
                "core": "静态黑白照片一键生成开口唱歌动态视频效果。",
                "play_label": "",
            }
        ]

        result = run_ai_dedupe(
            rows=rows,
            target_date="2026-05-28",
            lookback_days=1,
            model="fake-model",
            workers=1,
        )

        self.assertEqual(result[0]["status"], "new_play")
        self.assertEqual(result[0]["play_name"], "照片开口唱歌")


if __name__ == "__main__":
    unittest.main()
