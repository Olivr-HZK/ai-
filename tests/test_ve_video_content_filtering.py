from __future__ import annotations

import unittest
from unittest import mock


class VeVideoContentFilteringEvalTest(unittest.TestCase):
    def test_minimal_video_content_analysis_generates_only_for_kept_rows(self) -> None:
        from ua_workflows.video_enhancer.video_content_minimal_analysis import apply_minimal_analysis_to_results

        rows = [
            {
                "ad_key": "keep_a",
                "title": "红毯写真",
                "guangdada_video_content": "用户上传自拍生成红毯明星写真视频。",
                "analysis": "",
                "material_tags": [],
            },
            {
                "ad_key": "drop_b",
                "title": "商品广告",
                "guangdada_video_content": "展示商品购买流程。",
                "analysis": "",
                "exclude_from_bitable": True,
                "llm_video_content_filter_match": {
                    "final_decision": "业务硬拦",
                    "business_reason": "商品广告",
                },
                "material_tags": ["大模型业务硬拦"],
            },
        ]
        calls = []

        def fake_generate(row, **kwargs):
            calls.append(row["ad_key"])
            return {
                "analysis": "核心卖点：自拍生成红毯明星写真\n风险等级：低风险",
                "effect_one_liner": "自拍生成红毯明星写真",
                "risk_level": "低风险",
            }

        summary = apply_minimal_analysis_to_results(
            rows,
            model="fake-model",
            timeout=1,
            max_workers=1,
            generate_fn=fake_generate,
        )

        self.assertEqual(calls, ["keep_a"])
        self.assertEqual(summary["generated"], 1)
        self.assertEqual(rows[0]["effect_one_liner"], "自拍生成红毯明星写真")
        self.assertIn("核心卖点", rows[0]["analysis"])
        self.assertIn("LLM视频内容筛选", rows[1]["analysis"])
        self.assertEqual(rows[1]["effect_one_liner"], "")

    def test_seed_analysis_results_include_video_content_for_sync_safe_minimal_flow(self) -> None:
        from ua_workflows.video_enhancer.video_content_minimal_analysis import seed_analysis_results_from_raw

        payload = {
            "items": [
                {
                    "category": "photo",
                    "product": "PixVerse",
                    "appid": "app_1",
                    "creative": {
                        "ad_key": "ad_1",
                        "title": "素材标题",
                        "body": "素材正文",
                        "video_url": "https://example.test/a.mp4",
                        "preview_img_url": "https://example.test/a.jpg",
                        "guangdada_video_content": "用户上传自拍生成电影感视频。",
                    },
                }
            ]
        }

        rows = seed_analysis_results_from_raw(payload, target_date="2026-06-11")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ad_key"], "ad_1")
        self.assertEqual(rows[0]["product"], "PixVerse")
        self.assertEqual(rows[0]["guangdada_video_content"], "用户上传自拍生成电影感视频。")
        self.assertEqual(rows[0]["analysis"], "")

    def test_llm_video_content_filter_marks_history_duplicate_only_against_past_rows(self) -> None:
        from ua_workflows.video_enhancer.video_content_llm_filter import run_llm_video_content_filter

        rows = [
            {
                "target_date": "2026-06-09",
                "advertiser_id": "adv_1",
                "advertiser_name": "PixVerse",
                "ad_key": "hist_old",
                "video_content": "用户上传人像照片生成足球球场欢呼短片。",
            },
            {
                "target_date": "2026-06-12",
                "advertiser_id": "adv_1",
                "advertiser_name": "PixVerse",
                "ad_key": "future_row",
                "video_content": "用户上传照片生成同款未来日期短片。",
            },
            {
                "target_date": "2026-06-11",
                "advertiser_id": "adv_1",
                "advertiser_name": "PixVerse",
                "ad_key": "today_a",
                "video_content": "用户上传人像照片生成足球球场欢呼短片。",
            },
        ]

        def fake_runner(**kwargs):
            self.assertEqual([row["ad_key"] for row in kwargs["history_rows"]], ["hist_old"])
            return [
                {
                    "ad_key": "today_a",
                    "is_duplicate": True,
                    "duplicate_type": "history",
                    "match_ad_key": "future_row",
                }
            ]

        payload = run_llm_video_content_filter(
            rows=rows,
            target_date="2026-06-11",
            model="fake-model",
            timeout=1,
            chunk_size=3,
            max_workers=1,
            llm_runner=fake_runner,
        )

        record = payload["records"][0]
        self.assertEqual(record["final_decision"], "保留")
        self.assertFalse(record["is_duplicate"])
        self.assertEqual(record["match_ad_key"], "")

    def test_llm_video_content_filter_validates_intraday_duplicate_against_prior_rows(self) -> None:
        from ua_workflows.video_enhancer.video_content_llm_filter import run_llm_video_content_filter

        rows = [
            {
                "target_date": "2026-06-11",
                "advertiser_id": "adv_1",
                "advertiser_name": "PixVerse",
                "ad_key": "today_a",
                "all_exposure_value": 200,
                "video_content": "用户上传照片生成红毯写真。",
            },
            {
                "target_date": "2026-06-11",
                "advertiser_id": "adv_1",
                "advertiser_name": "PixVerse",
                "ad_key": "today_b",
                "all_exposure_value": 100,
                "video_content": "用户上传照片生成红毯写真。",
            },
        ]

        def fake_runner(**kwargs):
            if kwargs["target_rows"][0]["ad_key"] == "today_a":
                return [
                    {
                        "ad_key": "today_a",
                        "is_duplicate": True,
                        "duplicate_type": "intraday",
                        "match_ad_key": "today_b",
                    }
                ]
            return [
                {
                    "ad_key": "today_b",
                    "is_duplicate": True,
                    "duplicate_type": "intraday",
                    "match_ad_key": "today_a",
                    "duplicate_confidence": "high",
                }
            ]

        payload = run_llm_video_content_filter(
            rows=rows,
            target_date="2026-06-11",
            model="fake-model",
            timeout=1,
            chunk_size=1,
            max_workers=1,
            llm_runner=fake_runner,
        )

        by_key = {row["ad_key"]: row for row in payload["records"]}
        self.assertEqual(by_key["today_a"]["final_decision"], "保留")
        self.assertEqual(by_key["today_b"]["final_decision"], "日内重复")
        self.assertEqual(by_key["today_b"]["match_ad_key"], "today_a")

    def test_apply_llm_video_content_filter_marks_business_hard_block_before_duplicate(self) -> None:
        from ua_workflows.video_enhancer.video_content_llm_filter import apply_llm_filter_results

        analysis_results = [
            {"ad_key": "today_a", "analysis": "旧分析", "material_tags": ["已有标签"]},
        ]
        payload = {
            "records": [
                {
                    "ad_key": "today_a",
                    "final_decision": "业务硬拦",
                    "situations": ["业务硬拦", "历史重复"],
                    "business_hard_block": True,
                    "business_labels": ["ecommerce_effect"],
                    "business_reason": "展示商品售卖流程",
                    "is_duplicate": True,
                    "duplicate_type": "history",
                    "match_ad_key": "hist_a",
                    "match_date": "2026-06-09",
                    "duplicate_confidence": "high",
                    "duplicate_reason": "同款商品广告",
                }
            ]
        }

        summary = apply_llm_filter_results(analysis_results, payload)

        row = analysis_results[0]
        self.assertEqual(summary["applied"], 1)
        self.assertTrue(row["exclude_from_bitable"])
        self.assertTrue(row["exclude_from_cluster"])
        self.assertIn("大模型业务硬拦", row["material_tags"])
        self.assertIn("业务硬拦:ecommerce_effect", row["material_tags"])
        self.assertNotIn("大模型历史重复", row["material_tags"])
        self.assertEqual(row["llm_video_content_filter_match"]["final_decision"], "业务硬拦")

    def test_evaluate_filter_coverage_uses_or_union_for_adult_and_non_person(self) -> None:
        from scripts.evaluate_ve_video_content_filtering import evaluate_filter_coverage

        rows = [
            {
                "ad_key": "ad_1",
                "guangdada_video_content": "已有内容",
                "analysis": "纯广告内容",
            },
            {
                "ad_key": "ad_2",
                "guangdada_video_content": "已有内容",
                "analysis": "纯广告内容",
            },
            {
                "ad_key": "ad_3",
                "guangdada_video_content": "已有内容",
                "analysis": "纯广告内容",
            },
        ]

        with mock.patch(
            "scripts.evaluate_ve_video_content_filtering._run_filter",
            side_effect=[
                (
                    {"ad_1": {"ad_key": "ad_1", "pattern": "porn"}},
                    {
                        "ad_2": {"ad_key": "ad_2", "reason": "ecommerce_effect"},
                        "ad_3": {"ad_key": "ad_3", "reason": "missing_human_photo_input"},
                    },
                ),
                (
                    {"ad_2": {"ad_key": "ad_2", "pattern": "porn"}},
                    {"ad_2": {"ad_key": "ad_2", "reason": "ecommerce_effect"}},
                ),
            ],
        ):
            payload = evaluate_filter_coverage(rows, with_video_required=True)

        adult = payload["summary"]["adult"]
        non_person = payload["summary"]["non_person"]
        combo = payload["summary"]["adult_or_non_person"]

        self.assertEqual(adult["base_hit"], 1)
        self.assertEqual(adult["video_hit"], 1)
        self.assertEqual(adult["true_positive"], 0)
        self.assertEqual(non_person["base_hit"], 2)
        self.assertEqual(non_person["video_hit"], 1)
        self.assertEqual(non_person["true_positive"], 1)
        self.assertEqual(combo["base_hit"], 3)
        self.assertEqual(combo["video_hit"], 1)
        self.assertEqual(combo["true_positive"], 1)

    def test_evaluate_filter_coverage_with_video_required_filters_empty_video_rows(self) -> None:
        from scripts.evaluate_ve_video_content_filtering import evaluate_filter_coverage

        rows = [
            {"ad_key": "ad_1", "guangdada_video_content": "有文字"},
            {"ad_key": "ad_2", "guangdada_video_content": ""},
        ]

        with mock.patch(
            "scripts.evaluate_ve_video_content_filtering._run_filter",
            side_effect=[({}, {}), ({}, {})],
        ):
            payload = evaluate_filter_coverage(rows, with_video_required=True)

        self.assertEqual(payload["summary"]["evaluated_rows"], 1)
        self.assertEqual(payload["summary"]["total_rows"], 2)
        self.assertEqual(len(payload["row_details"]), 1)

    def test_inject_video_text_supports_analysis_only_and_full_concat(self) -> None:
        from scripts.evaluate_ve_video_content_filtering import _inject_video_text

        row = {
            "analysis": "原始分析文本",
            "title": "素材标题",
            "guangdada_video_content": "视频脚本文案",
        }

        injected = _inject_video_text(row, use_full_concat=False)
        self.assertEqual(injected["analysis"], "视频脚本文案")
        self.assertEqual(injected["title"], "素材标题")

        full = _inject_video_text(row, use_full_concat=True)
        self.assertEqual(full["analysis"], "视频脚本文案")
        self.assertEqual(full["title"], "素材标题\n视频脚本文案")

    def test_coerce_llm_result_accepts_json_block_and_fallback(self) -> None:
        from scripts.evaluate_ve_video_content_filtering import _coerce_llm_result

        with_block = (
            "```json\n"
            '{"is_hard_filtered": true, "labels": ["adult", "ecommerce_effect"], "reason": "敏感内容"}\n'
            "```"
        )
        parsed = _coerce_llm_result(with_block)
        self.assertTrue(parsed["is_hard_filtered"])
        self.assertEqual(parsed["labels"], ["adult", "ecommerce_effect"])
        self.assertEqual(parsed["reason"], "敏感内容")

        invalid = (
            "text before\n"
            "{\n"
            '  "is_hard_filtered": false,\n'
            '  "labels": "adult"\n'
            "}\n"
            "text after"
        )
        parsed_invalid = _coerce_llm_result(invalid)
        self.assertTrue(parsed_invalid["is_hard_filtered"])
        self.assertEqual(parsed_invalid["labels"], ["adult"])
        self.assertNotIn("parse_error", parsed_invalid)

    def test_coerce_llm_result_derives_labels_from_dimension_hits(self) -> None:
        from scripts.evaluate_ve_video_content_filtering import _coerce_llm_result

        raw = (
            '{"adult": {"hit": false}, '
            '"non_human_photo_effect": {"hit": true, "reason": "物体场景"}, '
            '"missing_human_photo_input": {"hit": true, "reason": "未体现上传人像"}, '
            '"is_hard_filtered": false, "labels": [], "reason": "分项命中"}'
        )

        parsed = _coerce_llm_result(raw)

        self.assertTrue(parsed["is_hard_filtered"])
        self.assertEqual(
            parsed["labels"],
            ["missing_human_photo_input", "non_human_photo_effect"],
        )

    def test_coerce_llm_result_derives_labels_from_clear_reason_when_labels_missing(self) -> None:
        from scripts.evaluate_ve_video_content_filtering import _coerce_llm_result

        raw = (
            '{"is_hard_filtered": false, "labels": [], '
            '"reason": "素材为非人物特效场景，且未体现用户上传人物照片进行加工"}'
        )

        parsed = _coerce_llm_result(raw)

        self.assertTrue(parsed["is_hard_filtered"])
        self.assertEqual(
            parsed["labels"],
            ["non_human_photo_effect", "missing_human_photo_input"],
        )
        self.assertEqual(
            parsed["reason_fallback_labels"],
            ["non_human_photo_effect", "missing_human_photo_input"],
        )

    def test_normalize_llm_labels_accepts_set_values(self) -> None:
        from scripts.evaluate_ve_video_content_filtering import _normalize_llm_labels

        labels = _normalize_llm_labels({"adult", "missing_human_photo_input"})

        self.assertEqual(set(labels), {"adult", "missing_human_photo_input"})

    def test_ensure_video_content_does_not_limit_guangdada_search_date_range(self) -> None:
        from scripts.evaluate_ve_video_content_filtering import _ensure_video_content_for_rows

        captured = {}

        async def fake_fetch(rows, **kwargs):
            captured.update(kwargs)
            return {"direct": {"requested": 1, "updated": 0}, "search": {"requested": 1, "updated": 0}}

        rows = [
            {
                "target_date": "2026-06-08",
                "ad_key": "ad_a",
                "appid": "app_a",
                "guangdada_video_content": "",
            }
        ]

        with mock.patch(
            "scripts.evaluate_ve_video_content_filtering.fetch_missing_video_content_by_adkeys",
            side_effect=fake_fetch,
        ):
            _ensure_video_content_for_rows(rows, retries=1)

        self.assertIsNone(captured.get("date_range"))
        self.assertIs(captured.get("skip_time_filter"), True)

    def test_evaluate_filter_coverage_llm_mode_counts_hits(self) -> None:
        from scripts.evaluate_ve_video_content_filtering import evaluate_filter_coverage

        rows = [
            {
                "ad_key": "ad_1",
                "guangdada_video_content": "有内容",
                "analysis": "纯广告内容",
            },
            {
                "ad_key": "ad_2",
                "guangdada_video_content": "有内容",
                "analysis": "纯广告内容",
            },
            {
                "ad_key": "ad_3",
                "guangdada_video_content": "有内容",
                "analysis": "纯广告内容",
            },
        ]

        with mock.patch(
            "scripts.evaluate_ve_video_content_filtering._run_filter",
            side_effect=[({}, {}), ({}, {})],
        ), mock.patch(
            "scripts.evaluate_ve_video_content_filtering._classify_with_llm",
            side_effect=[
                (
                    {"adult"},
                    {"ad_key": "ad_1", "labels": ["adult"], "reason": "成人"},
                ),
                (
                    {"ecommerce_effect"},
                    {"ad_key": "ad_2", "labels": ["ecommerce_effect"], "reason": "商品"},
                ),
                (
                    set(),
                    {"ad_key": "ad_3", "labels": [], "reason": ""},
                ),
            ],
        ):
            payload = evaluate_filter_coverage(rows, with_video_required=True, use_llm=True)

        self.assertEqual(payload["summary"]["video_filter_mode"], "llm")
        self.assertEqual(payload["summary"]["adult"]["base_hit"], 0)
        self.assertEqual(payload["summary"]["adult"]["video_hit"], 1)
        self.assertEqual(payload["summary"]["adult"]["true_positive"], 0)
        self.assertEqual(payload["summary"]["non_person"]["base_hit"], 0)
        self.assertEqual(payload["summary"]["non_person"]["video_hit"], 1)
        self.assertEqual(payload["summary"]["adult_or_non_person"]["video_hit"], 2)
        self.assertEqual(payload["summary"]["adult_or_non_person"]["true_positive"], 0)
