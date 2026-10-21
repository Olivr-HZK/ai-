from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock


class CrawlSmokeCliTest(unittest.TestCase):
    def test_video_enhancer_args_target_one_product_without_db_steps(self) -> None:
        from ua_workflows.shared.crawl_smoke import build_video_enhancer_crawl_args

        args = build_video_enhancer_crawl_args(
            date="2026-05-05",
            product="Remini - AI Photo Enhancer",
            output_prefix="smoke_ve",
        )

        self.assertEqual(
            args,
            [
                "--target-date",
                "2026-05-05",
                "--products",
                "Remini - AI Photo Enhancer",
                "--output-prefix",
                "smoke_ve",
            ],
        )

    def test_arrow2_latest_args_use_crawl_only_pull_spec(self) -> None:
        from ua_workflows.shared.crawl_smoke import build_arrow2_crawl_args

        args = build_arrow2_crawl_args(
            date="2026-05-05",
            product="Arrows - Puzzle Escape",
            pull_only="latest_yesterday",
            output_prefix="smoke_arrow2_latest",
        )

        self.assertEqual(
            args,
            [
                "--date",
                "2026-05-05",
                "--products",
                "Arrows - Puzzle Escape",
                "--no-pause",
                "--pull-only",
                "latest_yesterday",
                "--output-prefix",
                "smoke_arrow2_latest",
            ],
        )

    def test_headed_sets_debug_and_headless_removes_debug(self) -> None:
        from ua_workflows.shared.crawl_smoke import apply_browser_mode

        headed_env = apply_browser_mode({"DEBUG": ""}, headed=True, headless=False)
        headless_env = apply_browser_mode({"DEBUG": "1"}, headed=False, headless=True)

        self.assertEqual(headed_env["DEBUG"], "1")
        self.assertNotIn("DEBUG", headless_env)

    def test_video_enhancer_all_products_smoke_args_are_direct(self) -> None:
        date = "2026-05-05"
        output_prefix = "smoke_all"
        args = ["--target-date", date, "--output-prefix", output_prefix, "--pause-per-product"]

        self.assertEqual(
            args,
            ["--target-date", "2026-05-05", "--output-prefix", "smoke_all", "--pause-per-product"],
        )

    def test_exposure_output_requires_successful_top10_dropdown(self) -> None:
        from ua_workflows.shared.crawl_smoke import exposure_attempt_succeeded

        self.assertTrue(exposure_attempt_succeeded("Top 创意下拉 ✓\n[raw] out.json"))
        self.assertFalse(exposure_attempt_succeeded("Top 创意下拉 ✗\n[raw] out.json"))
        self.assertFalse(exposure_attempt_succeeded("Top 创意下拉 ✓\nTraceback"))

    def test_video_enhancer_db_extracts_card_detail_geo(self) -> None:
        from ua_workflows.shared.db.video_enhancer import (
            _extract_country_codes_from_creative,
            _extract_geo_targeting_from_creative,
        )

        creative = {
            "countries": ["USA", "SGP"],
            "country_code": "JPN,KOR",
            "areas": [{"code": "GBR", "name": "United Kingdom"}],
            "heat": 12,
            "all_exposure_value": 34,
            "impression": 56,
        }

        self.assertEqual(
            _extract_country_codes_from_creative(creative),
            ["USA", "SGP", "JPN", "KOR"],
        )
        self.assertEqual(
            _extract_geo_targeting_from_creative(creative),
            {
                "countries": ["USA", "SGP"],
                "country_code": "JPN,KOR",
                "areas": [{"code": "GBR", "name": "United Kingdom"}],
            },
        )

    def test_dom_detail_merge_keeps_same_preview_distinct_ad_keys(self) -> None:
        from ua_workflows.shared.guangdada.search import _merge_dom_cards_with_details

        same_preview = "https://cdn.example/cover.jpg"
        dom_cards = [
            {"_result_page": 1, "_dom_idx": 0, "preview_img_url": same_preview},
            {"_result_page": 1, "_dom_idx": 1, "preview_img_url": same_preview},
        ]
        detail_rows = [
            {
                "_result_page": 1,
                "_dom_idx": 0,
                "ad_key": "ad_a",
                "preview_img_url": same_preview,
                "first_seen": 1,
            },
            {
                "_result_page": 1,
                "_dom_idx": 1,
                "ad_key": "ad_b",
                "preview_img_url": same_preview,
                "first_seen": 2,
            },
            {
                "_result_page": 1,
                "_dom_idx": 1,
                "ad_key": "ad_b",
                "preview_img_url": same_preview,
                "first_seen": 2,
            },
        ]

        merged = _merge_dom_cards_with_details(dom_cards, detail_rows)

        self.assertEqual([x.get("ad_key") for x in merged], ["ad_a", "ad_b"])
        self.assertTrue(all(x.get("_source") == "dom_detail" for x in merged))

    def test_normalize_detail_dom_analysis_extracts_script_and_tags(self) -> None:
        from ua_workflows.shared.guangdada.search import _normalize_detail_dom_analysis

        raw = {
            "text": """
创意分析
创意内容
视频类型
录屏演示（App UI为主）
场景
居家
车内
居家自拍
色彩调性
暖色系
明亮
素材脚本分析
视频内容：视频通过分屏对比展示真人照片与AI生成的卡通化小人形象，演示如何使用PixVerse应用将照片转换为流行趋势风格的动画角色，并展示App操作界面。
0:00
0:07 实拍+功能演示
0:10 界面/UI展示
0:18 实拍+功能演示
0:20
AI辅助功能
""",
        }

        analysis = _normalize_detail_dom_analysis(raw)

        self.assertEqual(analysis["source"], "guangdada_detail_dom")
        self.assertEqual(analysis["creative_analysis"]["视频类型"], ["录屏演示（App UI为主）"])
        self.assertEqual(analysis["creative_analysis"]["场景"], ["居家", "车内", "居家自拍"])
        self.assertEqual(analysis["creative_analysis"]["色彩调性"], ["暖色系", "明亮"])
        self.assertIn("分屏对比展示真人照片", analysis["script_analysis"]["video_content"])
        self.assertEqual(
            analysis["script_analysis"]["timeline"],
            [
                {"time": "0:07", "label": "实拍+功能演示"},
                {"time": "0:10", "label": "界面/UI展示"},
                {"time": "0:18", "label": "实拍+功能演示"},
            ],
        )

    def test_normalize_detail_dom_analysis_returns_empty_for_missing_sections(self) -> None:
        from ua_workflows.shared.guangdada.search import _normalize_detail_dom_analysis

        self.assertEqual(_normalize_detail_dom_analysis({"text": "只有普通弹窗"}), {})

    def test_normalize_detail_dom_analysis_extracts_split_timeline_labels(self) -> None:
        from ua_workflows.shared.guangdada.search import _normalize_detail_dom_analysis

        analysis = _normalize_detail_dom_analysis(
            {
                "text": """
创意分析
素材脚本分析
视频内容：展示产品操作界面。
0:00
0:07
实拍+功能演示
0:10
界面/UI展示
AI辅助功能
""",
            }
        )

        self.assertEqual(
            analysis["script_analysis"]["timeline"],
            [
                {"time": "0:07", "label": "实拍+功能演示"},
                {"time": "0:10", "label": "界面/UI展示"},
            ],
        )

    def test_normalize_detail_dom_analysis_keeps_collapsed_sections_separate(self) -> None:
        from ua_workflows.shared.guangdada.search import _normalize_detail_dom_analysis

        analysis = _normalize_detail_dom_analysis(
            {
                "text": (
                    "创意分析 创意内容 视频类型 录屏演示（App UI为主） 场景 居家 办公室 "
                    "视觉对象 真人用户 AI 生成角色 对象行为 操作 App 手持道具 "
                    "呈现方式 功能演示 分屏对比 屏幕操作 色彩调性 多彩/彩色 中等 "
                    "创意策略 目标受众 年轻 爱玩 自媒体创作者 核心功能 AI 生成 视频特效 "
                    "素材脚本分析 视频内容： 视频通过分屏对比展示普通实拍画面转换为电影级场景。 "
                    "hook：前后对比/结果反差 0:00 0:03 分屏剪辑 ending： 明确下载/安装引导 0:21"
                )
            }
        )

        self.assertEqual(analysis["creative_analysis"]["场景"], ["居家", "办公室"])
        self.assertEqual(analysis["creative_analysis"]["视觉对象"], ["真人用户", "AI 生成角色"])
        self.assertEqual(analysis["creative_analysis"]["呈现方式"], ["功能演示", "分屏对比", "屏幕操作"])
        self.assertEqual(analysis["creative_analysis"]["色彩调性"], ["多彩/彩色", "中等"])
        self.assertEqual(analysis["creative_analysis"]["目标受众"], ["年轻", "爱玩", "自媒体创作者"])
        self.assertIn("电影级场景", analysis["script_analysis"]["video_content"])
        self.assertNotIn("hook", analysis["script_analysis"]["video_content"].lower())
        self.assertEqual(analysis["script_analysis"]["hook"], "前后对比/结果反差")
        self.assertEqual(analysis["script_analysis"]["ending"], "明确下载/安装引导")

    def test_detail_analysis_wait_ms_is_configurable(self) -> None:
        from ua_workflows.shared.guangdada.search import _detail_analysis_wait_ms

        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(_detail_analysis_wait_ms(), 20000)
        with mock.patch.dict(os.environ, {"GUANGDADA_DETAIL_ANALYSIS_WAIT_MS": "12000"}):
            self.assertEqual(_detail_analysis_wait_ms(), 12000)
        with mock.patch.dict(os.environ, {"GUANGDADA_DETAIL_ANALYSIS_WAIT_MS": "bad"}):
            self.assertEqual(_detail_analysis_wait_ms(), 20000)

    def test_detail_analysis_has_script_requires_script_content_or_timeline(self) -> None:
        from ua_workflows.shared.guangdada.search import _detail_analysis_has_script

        self.assertFalse(_detail_analysis_has_script({"creative_analysis": {"视频类型": ["录屏"]}}))
        self.assertTrue(
            _detail_analysis_has_script(
                {"script_analysis": {"video_content": "展示产品操作界面。"}}
            )
        )
        self.assertTrue(
            _detail_analysis_has_script(
                {"script_analysis": {"timeline": [{"time": "0:07", "label": "功能演示"}]}}
            )
        )

    def test_attach_guangdada_video_content_keeps_only_video_content(self) -> None:
        from ua_workflows.shared.guangdada.search import _attach_guangdada_video_content

        creative = {
            "ad_key": "ad_001",
            "guangdada_detail_analysis": {"stale": "should be removed"},
        }

        changed = _attach_guangdada_video_content(
            creative,
            {
                "source": "guangdada_detail_dom",
                "creative_analysis": {"视频类型": ["录屏"]},
                "script_analysis": {
                    "video_content": "视频通过分屏展示 AI 生成效果。",
                    "hook": "前后对比",
                    "timeline": [{"time": "0:07", "label": "界面/UI展示"}],
                },
            },
        )

        self.assertTrue(changed)
        self.assertEqual(creative["guangdada_video_content"], "视频通过分屏展示 AI 生成效果。")
        self.assertNotIn("guangdada_detail_analysis", creative)

    def test_normalize_material_script_analysis_response(self) -> None:
        from ua_workflows.shared.guangdada.search import _normalize_material_script_analysis_response

        raw = {
            "data": {
                "video_hook_type": [{"cn_name": "对比展示", "en_name": "Comparison"}],
                "video_ending": [{"cn_name": "功能演示", "en_name": "Demo"}],
                "material_script_analysis": {
                    "video_summary": "视频通过分屏对比展示真人照片与AI生成的卡通化小人形象。",
                    "timeline": [
                        {"start": 0, "end": 7, "segment_type": "实拍+功能演示", "segment_type_en": "Live demo"},
                        {"start": 7, "end": 10, "segment_type": "界面/UI展示", "segment_type_en": "UI"},
                    ],
                },
            }
        }

        analysis = _normalize_material_script_analysis_response(raw)

        self.assertEqual(analysis["source"], "guangdada_material_script_analysis_api")
        self.assertEqual(analysis["creative_analysis"]["video_hook_type"], ["对比展示"])
        self.assertEqual(analysis["creative_analysis"]["video_ending"], ["功能演示"])
        self.assertIn("分屏对比展示", analysis["script_analysis"]["video_content"])
        self.assertEqual(
            analysis["script_analysis"]["timeline"],
            [
                {"start": 0.0, "end": 7.0, "time": "0:07", "label": "实拍+功能演示"},
                {"start": 7.0, "end": 10.0, "time": "0:10", "label": "界面/UI展示"},
            ],
        )

    def test_normalize_material_script_analysis_prefers_video_content_over_summary(self) -> None:
        from ua_workflows.shared.guangdada.search import _normalize_material_script_analysis_response

        analysis = _normalize_material_script_analysis_response(
            {
                "data": {
                    "material_script_analysis": {
                        "video_content": "原始视频内容字段。",
                        "video_summary": "摘要字段不应优先。",
                    }
                }
            }
        )

        self.assertEqual(analysis["script_analysis"]["video_content"], "原始视频内容字段。")

    def test_video_content_is_persisted_to_ve_db_tables(self) -> None:
        import ua_workflows.shared.db.video_enhancer as ve_db

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "video_enhancer_pipeline.db"
            raw_payload = {
                "items": [
                    {
                        "category": "video",
                        "product": "Pixverse",
                        "appid": "com.pixverseai.pixverse",
                        "creative": {
                            "ad_key": "ad_video_content_001",
                            "platform": "facebook",
                            "video_url": "https://cdn.example.com/a.mp4",
                            "preview_img_url": "https://cdn.example.com/a.jpg",
                            "first_seen": 1780902000,
                            "guangdada_video_content": "视频通过分屏展示真人照片与 AI 生成角色的对比效果。",
                        },
                    }
                ]
            }

            with mock.patch.object(ve_db, "DB_PATH", db_path):
                self.assertEqual(ve_db.upsert_daily_creative_insights("2026-06-11", raw_payload, {}), 1)
                self.assertEqual(ve_db.upsert_creative_library("2026-06-11", raw_payload, {})[0], 1)

                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                try:
                    daily = conn.execute(
                        "SELECT guangdada_video_content FROM daily_creative_insights WHERE ad_key = ?",
                        ("ad_video_content_001",),
                    ).fetchone()
                    library = conn.execute(
                        "SELECT guangdada_video_content FROM creative_library WHERE ad_key = ?",
                        ("ad_video_content_001",),
                    ).fetchone()
                finally:
                    conn.close()

            self.assertEqual(
                daily["guangdada_video_content"],
                "视频通过分屏展示真人照片与 AI 生成角色的对比效果。",
            )
            self.assertEqual(
                library["guangdada_video_content"],
                "视频通过分屏展示真人照片与 AI 生成角色的对比效果。",
            )


if __name__ == "__main__":
    unittest.main()
