#!/usr/bin/env python3
"""Tests for Haopeng Top-N Feishu push preview."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from argparse import Namespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ua_workflows.video_enhancer.haopeng_topn_push import (
    build_card_payload,
    build_im_card,
    classify_play_kind,
    load_or_generate_report,
    merge_report_rows_from_source,
    render_topn_markdown,
)


class PushHaopengTopNToFeishuTest(unittest.TestCase):
    def test_classify_play_kind_uses_matched_history_label(self) -> None:
        self.assertEqual(
            classify_play_kind({"matched_play_label": "手绘", "play_label": "手绘"}),
            "老玩法新变体",
        )
        self.assertEqual(
            classify_play_kind({"matched_play_label": "新玩法候选", "play_label": "球员卡"}),
            "新玩法候选",
        )
        self.assertEqual(classify_play_kind({"matched_play_label": ""}), "新玩法候选")

    def test_render_topn_markdown_hides_backtest_fields_by_default(self) -> None:
        report = {
            "target_date": "2026-05-28",
            "history_window": "2026-05-25..2026-05-27",
            "model": "qwen/qwen3.7-max",
            "name": "label_prior",
            "summary": {
                "top10": {
                    "accepted": 7,
                    "accepted_or_library": 8,
                    "actual_counts": {"采纳": 7, "入素材库": 1, "重复抓取": 2},
                }
            },
            "results": [
                {
                    "ad_key": "a1",
                    "product": "AI Mirror: AI Photo & Video",
                    "core": "真人照片生成手绘生活拼贴画",
                    "accept_score": 85,
                    "confidence": "high",
                    "matched_play_label": "手绘",
                    "play_label": "手绘",
                    "reason": "手绘类正样本反馈极佳。",
                    "actual_hp": "采纳",
                    "video_url": "https://video.example/a1.mp4",
                },
                {
                    "ad_key": "a2",
                    "product": "Glam AI",
                    "core": "单人自拍生成复古棒球球员卡",
                    "accept_score": 80,
                    "confidence": "medium",
                    "matched_play_label": "新玩法候选",
                    "play_label": "球员卡",
                    "reason": "复古球员卡是高价值新方向。",
                    "actual_hp": "入素材库",
                },
            ],
        }

        md = render_topn_markdown(report, top_n=10)

        self.assertIn("2026-05-28", md)
        self.assertIn("2026-05-25..2026-05-27", md)
        self.assertIn("[真人照片生成手绘生活拼贴画](https://video.example/a1.mp4)", md)
        self.assertIn("老玩法新变体", md)
        self.assertNotIn("回测命中", md)
        self.assertNotIn("浩鹏实际", md)

        top1_md = render_topn_markdown(report, top_n=1)
        self.assertIn("真人照片生成手绘生活拼贴画", top1_md)
        self.assertNotIn("复古棒球", top1_md)

    def test_render_topn_markdown_can_include_backtest_for_review(self) -> None:
        report = {
            "target_date": "2026-05-28",
            "history_window": "2026-05-25..2026-05-27",
            "model": "qwen/qwen3.7-max",
            "name": "label_prior",
            "summary": {
                "top10": {
                    "accepted": 7,
                    "accepted_or_library": 8,
                    "actual_counts": {"采纳": 7, "入素材库": 1, "重复抓取": 2},
                }
            },
            "results": [
                {
                    "ad_key": "a1",
                    "product": "AI Mirror: AI Photo & Video",
                    "core": "真人照片生成手绘生活拼贴画",
                    "accept_score": 85,
                    "confidence": "high",
                    "matched_play_label": "手绘",
                    "play_label": "手绘",
                    "reason": "手绘类正样本反馈极佳。",
                    "actual_hp": "采纳",
                    "video_url": "https://video.example/a1.mp4",
                }
            ],
        }

        md = render_topn_markdown(report, top_n=10, include_backtest=True)

        self.assertIn("采纳+入素材库：8/10", md)
        self.assertIn("浩鹏实际：采纳", md)

    def test_build_card_payload_wraps_markdown_for_webhook(self) -> None:
        payload = build_card_payload("VE Top10", "**hello**")

        self.assertEqual(payload["msg_type"], "interactive")
        self.assertEqual(payload["card"]["header"]["title"]["content"], "VE Top10")
        self.assertEqual(payload["card"]["elements"][0]["content"], "**hello**")

    def test_build_im_card_returns_card_without_webhook_wrapper(self) -> None:
        card = build_im_card("VE Top10", "**hello**")

        self.assertNotIn("msg_type", card)
        self.assertEqual(card["header"]["title"]["content"], "VE Top10")
        self.assertEqual(card["elements"][0]["content"], "**hello**")

    def test_merge_report_rows_from_source_adds_missing_media_links(self) -> None:
        report = {
            "results": [
                {"ad_key": "a1", "core": "旧核心", "video_url": ""},
                {"ad_key": "a2", "core": "已有核心", "video_url": "https://keep.example/video.mp4"},
            ]
        }
        source_rows = [
            {
                "ad_key": "a1",
                "core": "新核心",
                "video_url": "https://video.example/a1.mp4",
                "cover_url": "https://cover.example/a1.jpg",
            },
            {"ad_key": "a2", "video_url": "https://replace.example/video.mp4"},
        ]

        merge_report_rows_from_source(report, source_rows)

        rows = report["results"]
        self.assertEqual(rows[0]["core"], "旧核心")
        self.assertEqual(rows[0]["video_url"], "https://video.example/a1.mp4")
        self.assertEqual(rows[0]["cover_url"], "https://cover.example/a1.jpg")
        self.assertEqual(rows[1]["video_url"], "https://keep.example/video.mp4")

    def test_load_or_generate_report_generates_when_input_json_absent(self) -> None:
        args = Namespace(
            input_json="",
            use_latest_local=False,
            bitable_url="https://example.feishu.cn/base/app?table=tbl",
            date="2026-05-28",
            model="qwen/qwen3.7-max",
            history_start_date="2026-05-25",
            reviewer_field="浩鹏接受情况",
        )
        generated = ({"target_date": "2026-05-28", "results": []}, Path("/tmp/2026-05-28_label_prior.json"))

        with patch(
            "ua_workflows.video_enhancer.haopeng_ai_filter.generate_report_from_bitable",
            return_value=generated,
        ) as gen:
            report, path = load_or_generate_report(args)

        self.assertEqual(report["target_date"], "2026-05-28")
        self.assertEqual(path.name, "2026-05-28_label_prior.json")
        gen.assert_called_once_with(
            bitable_url="https://example.feishu.cn/base/app?table=tbl",
            target_date="2026-05-28",
            model="qwen/qwen3.7-max",
            history_start_date="2026-05-25",
            reviewer_field="浩鹏接受情况",
        )


if __name__ == "__main__":
    unittest.main()
