#!/usr/bin/env python3
"""Tests for Haopeng Top-N Feishu push preview."""

from __future__ import annotations

import sys
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ua_workflows.video_enhancer.haopeng_topn_push import (
    build_card_payload,
    build_im_card,
    classify_play_kind,
    enrich_results_from_bitable,
    load_or_generate_report,
    merge_report_rows_from_source,
    render_topn_markdown,
)
from ua_workflows.video_enhancer import haopeng_topn_push as topn_mod


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

    def test_find_default_report_path_prefers_target_date(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            (tmp_path / "2026-05-27_label_prior.json").write_text("{}", encoding="utf-8")
            (tmp_path / "2026-05-28_label_prior.json").write_text("{}", encoding="utf-8")
            (tmp_path / "manual_2026-05-28_label_prior.json").write_text("{}", encoding="utf-8")

            old_dir = topn_mod.DEFAULT_EXPERIMENT_DIR
            topn_mod.DEFAULT_EXPERIMENT_DIR = tmp_path
            try:
                self.assertEqual(
                    topn_mod.find_default_report_path("2026-05-27").name,
                    "2026-05-27_label_prior.json",
                )
                self.assertEqual(
                    topn_mod.find_default_report_path("2026-05-28").name,
                    "manual_2026-05-28_label_prior.json",
                )
            finally:
                topn_mod.DEFAULT_EXPERIMENT_DIR = old_dir

    def test_resolve_webhook_does_not_fallback_to_generic_bot_webhook(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "FEISHU_BOT_WEBHOOK": "https://example.invalid/test-bot",
                "FEISHU_UA_WEBHOOK": "",
                "VE_FLOW_REPORT_FEISHU_WEBHOOK": "",
                "VE_HAOPENG_TOPN_FEISHU_WEBHOOK": "",
            },
            clear=False,
        ):
            self.assertEqual(topn_mod.resolve_webhook(), "")

        with patch.dict(
            "os.environ",
            {
                "FEISHU_BOT_WEBHOOK": "https://example.invalid/test-bot",
                "FEISHU_UA_WEBHOOK": "https://example.invalid/ua",
                "VE_FLOW_REPORT_FEISHU_WEBHOOK": "",
                "VE_HAOPENG_TOPN_FEISHU_WEBHOOK": "",
            },
            clear=False,
        ):
            self.assertEqual(topn_mod.resolve_webhook(), "https://example.invalid/ua")

    def test_resolve_webhook_prefers_topn_then_flow_report_group(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "FEISHU_BOT_WEBHOOK": "https://example.invalid/test-bot",
                "FEISHU_UA_WEBHOOK": "https://example.invalid/ua",
                "VE_FLOW_REPORT_FEISHU_WEBHOOK": "https://example.invalid/flow",
                "VE_HAOPENG_TOPN_FEISHU_WEBHOOK": "",
            },
            clear=False,
        ):
            self.assertEqual(topn_mod.resolve_webhook(), "https://example.invalid/flow")

        with patch.dict(
            "os.environ",
            {
                "FEISHU_BOT_WEBHOOK": "https://example.invalid/test-bot",
                "FEISHU_UA_WEBHOOK": "https://example.invalid/ua",
                "VE_FLOW_REPORT_FEISHU_WEBHOOK": "https://example.invalid/flow",
                "VE_HAOPENG_TOPN_FEISHU_WEBHOOK": "https://example.invalid/topn",
            },
            clear=False,
        ):
            self.assertEqual(topn_mod.resolve_webhook(), "https://example.invalid/topn")

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
                    "hook": "普通自拍一键变成手绘生活拼贴",
                    "risk_level": "低风险",
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
                    "hook": "自拍立刻生成复古球员卡",
                    "risk_level": "中风险",
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
        self.assertIn("**产品** AI Mirror", md)
        self.assertIn("**Hook** 普通自拍一键变成手绘生活拼贴", md)
        self.assertIn("**风险** 低风险", md)
        self.assertIn("不代表浩鹏已审核", md)
        self.assertNotIn("理由：", md)
        self.assertNotIn("ID：", md)
        self.assertNotIn("手绘类正样本反馈极佳", md)
        self.assertNotIn("`a1`", md)
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
        self.assertIn("**浩鹏实际** 采纳", md)
        self.assertNotIn("不代表浩鹏已审核", md)

    def test_render_topn_markdown_skips_admob_and_youtube_rows(self) -> None:
        report = {
            "target_date": "2026-05-28",
            "history_window": "2026-05-25..2026-05-27",
            "model": "qwen/qwen3.7-max",
            "name": "label_prior",
            "results": [
                {
                    "ad_key": "a_admob",
                    "product": "Glam AI",
                    "core": "AdMob 渠道素材",
                    "platform": "admob",
                    "accept_score": 99,
                    "video_url": "https://video.example/admob.mp4",
                },
                {
                    "ad_key": "a_youtube",
                    "product": "Glam AI",
                    "core": "YouTube 渠道素材",
                    "platform": "YouTube",
                    "accept_score": 98,
                    "video_url": "https://video.example/youtube.mp4",
                },
                {
                    "ad_key": "a_tiktok",
                    "product": "Glam AI",
                    "core": "TikTok 渠道素材",
                    "platform": "tiktok",
                    "accept_score": 80,
                    "video_url": "https://video.example/tiktok.mp4",
                },
                {
                    "ad_key": "a_facebook",
                    "product": "Glam AI",
                    "core": "Facebook 渠道素材",
                    "platform": "facebook",
                    "accept_score": 70,
                    "video_url": "https://video.example/facebook.mp4",
                },
            ],
        }

        md = render_topn_markdown(report, top_n=2)

        self.assertNotIn("AdMob 渠道素材", md)
        self.assertNotIn("YouTube 渠道素材", md)
        self.assertIn("TikTok 渠道素材", md)
        self.assertIn("Facebook 渠道素材", md)

    def test_render_topn_markdown_uses_top_n_non_platform_rows_even_when_some_are_hold(self) -> None:
        report = {
            "target_date": "2026-06-01",
            "history_window": "2026-05-25..2026-05-31",
            "model": "qwen/qwen3.7-max",
            "name": "label_prior",
            "results": [
                {
                    "ad_key": "a_push_1",
                    "product": "Glam AI",
                    "core": "新场景素材",
                    "platform": "facebook",
                    "recommend": "push",
                    "accept_score": 82,
                },
                {
                    "ad_key": "a_hold_duplicate",
                    "product": "Glam AI",
                    "core": "历史同款重复素材",
                    "platform": "tiktok",
                    "recommend": "hold",
                    "accept_score": 99,
                    "reason": "与历史采纳素材高度同款。",
                },
                {
                    "ad_key": "a_push_2",
                    "product": "Glam AI",
                    "core": "老玩法新表达素材",
                    "platform": "tiktok",
                    "recommend": "push",
                    "accept_score": 75,
                },
            ],
        }

        md = render_topn_markdown(report, top_n=10)

        self.assertIn("新场景素材", md)
        self.assertIn("历史同款重复素材", md)
        self.assertIn("老玩法新表达素材", md)
        self.assertIn("\n3. ", md)

    def test_build_card_payload_wraps_markdown_for_webhook(self) -> None:
        payload = build_card_payload("VE Top10", "**hello**")

        self.assertEqual(payload["msg_type"], "interactive")
        self.assertEqual(payload["card"]["header"]["title"]["content"], "VE Top10")
        self.assertEqual(payload["card"]["elements"][0]["content"], "**hello**")

    def test_build_card_payload_adds_bitable_button_when_url_is_provided(self) -> None:
        bitable_url = "https://example.feishu.cn/base/app123?table=tbl456"

        payload = build_card_payload("VE Top10", "**hello**", bitable_url=bitable_url)
        card = build_im_card("VE Top10", "**hello**", bitable_url=bitable_url)

        for elements in (payload["card"]["elements"], card["elements"]):
            button = elements[-1]["actions"][0]
            self.assertEqual(elements[-1]["tag"], "action")
            self.assertEqual(button["tag"], "button")
            self.assertEqual(button["text"]["content"], "查看多维表格")
            self.assertEqual(button["type"], "primary")
            self.assertEqual(button["multi_url"]["url"], bitable_url)

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

    def test_bitable_enrich_skips_reviewer_field_by_default(self) -> None:
        report = {"results": [{"ad_key": "a1", "video_url": ""}]}
        fake_records = [
            {
                "fields": {
                    "广告ID": "a1",
                    "视频链接": "https://video.example/a1.mp4",
                    "浩鹏接受情况": "采纳",
                }
            }
        ]

        with patch(
            "ua_workflows.video_enhancer.haopeng_topn_push.fetch_bitable_records",
            create=True,
            return_value=fake_records,
        ), patch(
            "ua_workflows.video_enhancer.feedback_training.fetch_bitable_records",
            return_value=fake_records,
        ):
            enrich_results_from_bitable(report, "https://example.invalid/base/xxx?table=tbl", "浩鹏接受情况")

        self.assertEqual(report["results"][0]["video_url"], "https://video.example/a1.mp4")
        self.assertNotIn("actual_hp", report["results"][0])


if __name__ == "__main__":
    unittest.main()
