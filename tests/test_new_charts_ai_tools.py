from __future__ import annotations

import asyncio
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class NewChartsAiToolsTest(unittest.TestCase):
    def test_aliases_resolve_to_live_category_labels(self) -> None:
        from ua_workflows.shared.guangdada.new_charts_ai_tools import (
            AiToolCategory,
            category_ui_label,
            resolve_ai_tool_category,
        )

        self.assertEqual(
            resolve_ai_tool_category("AI图像生成"),
            AiToolCategory(label="AI工具/AI图像", slug="ai_image"),
        )
        self.assertEqual(
            resolve_ai_tool_category("AI视频"),
            AiToolCategory(label="AI工具/AI视频生成", slug="ai_video_generation"),
        )
        self.assertEqual(category_ui_label(resolve_ai_tool_category("AI视频")), "AI视频生成")

    def test_extract_creative_lists_finds_nested_creative_list(self) -> None:
        from ua_workflows.shared.guangdada.new_charts_ai_tools import extract_creative_lists

        body = {"data": {"creative_list": [{"ad_key": "a1"}, {"creative_id": "c2"}]}}

        self.assertEqual(extract_creative_lists(body), [body["data"]["creative_list"]])

    def test_video_url_prefers_resource_video(self) -> None:
        from ua_workflows.shared.guangdada.new_charts_ai_tools import pick_video_url

        creative = {
            "video_url": "",
            "resource_urls": [
                {"image_url": "https://example.test/cover.jpg"},
                {"video_url": "https://example.test/video.mp4"},
            ],
        }

        self.assertEqual(pick_video_url(creative), "https://example.test/video.mp4")

    def test_build_ve_raw_payload_skips_non_video_rows(self) -> None:
        from ua_workflows.shared.guangdada.new_charts_ai_tools import (
            AiToolCategory,
            build_ve_raw_payload,
        )

        category = AiToolCategory(label="AI工具/AI视频生成", slug="ai_video_generation")
        payload = build_ve_raw_payload(
            target_date="2026-06-02",
            per_category_raw={
                category: [
                    {
                        "ad_key": "video_1",
                        "advertiser_name": "Example",
                        "title": "Video",
                        "first_seen": 1789689600,
                        "resource_urls": [{"video_url": "https://example.test/video.mp4"}],
                    },
                    {"ad_key": "image_only", "preview_img_url": "https://example.test/cover.jpg"},
                ],
            },
        )

        self.assertEqual(payload["target_date"], "2026-06-02")
        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["competitors"], ["AI工具/AI视频生成"])
        item = payload["items"][0]
        self.assertEqual(item["category"], "ai_tools_new_charts")
        self.assertEqual(item["product"], "AI工具/AI视频生成")
        self.assertEqual(item["appid"], "ai_video_generation")
        self.assertEqual(item["keyword"], "AI工具/AI视频生成")
        self.assertEqual(item["creative"]["ad_key"], "video_1")
        self.assertEqual(item["creative"]["video_url"], "https://example.test/video.mp4")
        self.assertEqual(item["creative"]["new_charts_rank"], 1)

    def test_completeness_report_counts_missing_fields(self) -> None:
        from ua_workflows.shared.guangdada.new_charts_ai_tools import build_completeness_report

        payload = {
            "items": [
                {
                    "creative": {
                        "ad_key": "a",
                        "advertiser_name": "adv",
                        "title": "",
                        "video_url": "https://example.test/a.mp4",
                    }
                },
                {"creative": {"ad_key": "", "video_url": ""}},
            ]
        }

        report = build_completeness_report(payload)

        self.assertEqual(report["total"], 2)
        self.assertEqual(report["field_counts"]["ad_key"], 1)
        self.assertEqual(report["field_counts"]["video_url"], 1)
        self.assertEqual(report["missing_counts"]["title"], 2)

    def test_security_interruption_routes_to_human_gate(self) -> None:
        from ua_workflows.shared.guangdada import new_charts_ai_tools as mod

        async def fake_page_text(page: object) -> str:
            self.assertEqual(page, "page")
            return "请完成验证"

        async def fake_human_gate(page: object) -> None:
            self.assertEqual(page, "page")
            raise mod.GuangdadaHumanVerificationConfirmed("confirmed")

        async def scenario() -> None:
            with patch.object(mod, "_page_text", fake_page_text), patch.object(
                mod,
                "_dismiss_login_security_modal_if_needed",
                fake_human_gate,
            ):
                with self.assertRaises(mod.GuangdadaHumanVerificationConfirmed):
                    await mod._raise_if_interrupted("page", step="打开新创意榜")

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
