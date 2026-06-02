#!/usr/bin/env python3
"""Tests for Haopeng second-pass AI filtering."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


class HaopengAiFilterTest(unittest.TestCase):
    def test_normalize_bitable_record_extracts_target_and_feedback_fields(self) -> None:
        from ua_workflows.video_enhancer.haopeng_ai_filter import normalize_bitable_record

        row = normalize_bitable_record(
            {
                "record_id": "rec1",
                "fields": {
                    "广告ID": "ad_1",
                    "产品": "Glam AI",
                    "抓取日期": "2026-05-28",
                    "核心卖点": "自拍生成复古棒球球员卡",
                    "玩法": "球员卡",
                    "浩鹏接受情况": "采纳",
                    "视频链接": "https://example.com/v.mp4",
                    "封面图链接": "https://example.com/c.png",
                    "Hook解析": "先展示普通自拍，再切球员卡成片",
                },
            },
            reviewer_field="浩鹏接受情况",
        )

        self.assertEqual(row["ad_key"], "ad_1")
        self.assertEqual(row["date"], "2026-05-28")
        self.assertEqual(row["core"], "自拍生成复古棒球球员卡")
        self.assertEqual(row["play_label"], "球员卡")
        self.assertEqual(row["actual_hp"], "采纳")
        self.assertEqual(row["video_url"], "https://example.com/v.mp4")

    def test_build_report_scores_candidates_with_ai_and_sorts_topn_shape(self) -> None:
        from ua_workflows.video_enhancer.haopeng_ai_filter import build_report_from_rows

        rows = [
            {
                "ad_key": "h1",
                "product": "Glam AI",
                "date": "2026-05-27",
                "core": "自拍生成手绘拼贴",
                "play_label": "手绘拼贴",
                "actual_hp": "采纳",
            },
            {
                "ad_key": "h2",
                "product": "Glam AI",
                "date": "2026-05-27",
                "core": "附近聊天导流",
                "play_label": "",
                "actual_hp": "不采纳",
            },
            {
                "ad_key": "c1",
                "product": "Glam AI",
                "date": "2026-05-28",
                "core": "自拍生成复古棒球球员卡",
                "play_label": "球员卡",
                "video_url": "https://example.com/c1.mp4",
            },
            {
                "ad_key": "c2",
                "product": "Glam AI",
                "date": "2026-05-28",
                "core": "免费聊天交友",
                "play_label": "",
            },
        ]

        def fake_call_json(*, product, target_date, history, candidates, model):
            self.assertEqual(product, "Glam AI")
            self.assertEqual(target_date, "2026-05-28")
            self.assertEqual(len(history), 2)
            self.assertEqual(len(candidates), 2)
            self.assertEqual(model, "qwen/qwen3.7-max")
            return [
                {
                    "ad_key": "c2",
                    "accept_score": 20,
                    "confidence": "low",
                    "recommend": "hold",
                    "matched_play_label": "新玩法候选",
                    "play_name": "聊天导流",
                    "reason": "受众不符。",
                },
                {
                    "ad_key": "c1",
                    "accept_score": 88,
                    "confidence": "high",
                    "recommend": "push",
                    "matched_play_label": "球员卡",
                    "play_name": "复古球员卡",
                    "reason": "老玩法新视觉包装，付费价值明确。",
                },
            ]

        with patch("ua_workflows.video_enhancer.haopeng_ai_filter.call_ai_json", side_effect=fake_call_json):
            report = build_report_from_rows(
                rows,
                target_date="2026-05-28",
                model="qwen/qwen3.7-max",
                history_start_date="2026-05-25",
            )

        self.assertEqual(report["target_date"], "2026-05-28")
        self.assertEqual(report["name"], "label_prior")
        self.assertEqual(report["history_window"], "2026-05-25..2026-05-27")
        self.assertEqual([r["ad_key"] for r in report["results"]], ["c1", "c2"])
        self.assertEqual(report["results"][0]["accept_score"], 88)
        self.assertEqual(report["results"][0]["matched_play_label"], "球员卡")
        self.assertEqual(report["summary"]["top10"]["candidate_count"], 2)

    def test_write_report_uses_label_prior_experiment_directory(self) -> None:
        from ua_workflows.video_enhancer.haopeng_ai_filter import write_report

        with tempfile.TemporaryDirectory() as tmp:
            out = write_report(
                {"target_date": "2026-05-28", "results": []},
                output_dir=Path(tmp),
            )

            self.assertEqual(out.name, "2026-05-28_label_prior.json")
            self.assertTrue(out.exists())


if __name__ == "__main__":
    unittest.main()
