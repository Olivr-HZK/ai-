from __future__ import annotations

import os
import unittest
from unittest.mock import patch


class VeTemplateNormalizationTest(unittest.TestCase):
    def test_cover_history_lookback_defaults_to_sixty_days(self) -> None:
        from ua_workflows.video_enhancer.cover_dedupe import _cover_history_lookback_days

        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(_cover_history_lookback_days(), 60)

    def test_cover_history_lookback_is_capped_at_sixty_days(self) -> None:
        from ua_workflows.video_enhancer.cover_dedupe import _cover_history_lookback_days

        with patch.dict(os.environ, {"COVER_STYLE_HISTORY_LOOKBACK_DAYS": "120"}):
            self.assertEqual(_cover_history_lookback_days(), 60)

    def test_cover_history_stale_match_refreshes_current_representative(self) -> None:
        from ua_workflows.shared.llm.client import embedding_to_bytes
        from ua_workflows.video_enhancer.cover_dedupe import _cluster_clip_dedupe

        kept, removed, refresh = _cluster_clip_dedupe(
            threshold=0.75,
            today_rows=[
                {
                    "ad_key": "today_old_cluster",
                    "exposure": 100,
                    "product": "AI Mirror",
                    "cover_url": "https://example.com/today.jpg",
                    "vec": [1.0, 0.0],
                }
            ],
            history_hist=[
                {
                    "ad_key": "history_old_cluster",
                    "target_date": "2026-05-08",
                    "exposure": 50,
                }
            ],
            history_emb={"history_old_cluster": embedding_to_bytes([1.0, 0.0])},
            target_date="2026-05-21",
            hard_dedupe_days=7,
        )

        self.assertEqual(kept, {"today_old_cluster"})
        self.assertEqual(removed, [])
        self.assertEqual(refresh[0]["reason"], "cover_style_cluster_history_refresh")
        self.assertEqual(refresh[0]["matched_ad_key"], "history_old_cluster")
        self.assertEqual(refresh[0]["history_age_days"], 13)

    def test_cover_history_recent_match_still_hard_dedupes_today(self) -> None:
        from ua_workflows.shared.llm.client import embedding_to_bytes
        from ua_workflows.video_enhancer.cover_dedupe import _cluster_clip_dedupe

        kept, removed, refresh = _cluster_clip_dedupe(
            threshold=0.75,
            today_rows=[
                {
                    "ad_key": "today_recent_cluster",
                    "exposure": 100,
                    "product": "AI Mirror",
                    "cover_url": "https://example.com/today.jpg",
                    "vec": [1.0, 0.0],
                }
            ],
            history_hist=[
                {
                    "ad_key": "history_recent_cluster",
                    "target_date": "2026-05-20",
                    "exposure": 50,
                }
            ],
            history_emb={"history_recent_cluster": embedding_to_bytes([1.0, 0.0])},
            target_date="2026-05-21",
            hard_dedupe_days=7,
        )

        self.assertEqual(kept, set())
        self.assertEqual(refresh, [])
        self.assertEqual(removed[0]["reason"], "cover_style_cluster_vs_yesterday")
        self.assertEqual(removed[0]["history_age_days"], 1)

    def test_demographic_swaps_do_not_create_new_template_text(self) -> None:
        from ua_workflows.shared.db.video_enhancer import (
            _effect_text_similarity,
            normalize_effect_one_liner,
        )

        a = "黑人女性自拍生成球员卡片模板"
        b = "白人男性自拍生成球员卡片模板"

        self.assertEqual(normalize_effect_one_liner(a), normalize_effect_one_liner(b))
        self.assertEqual(_effect_text_similarity(a, b), 1.0)

    def test_gender_conversion_mechanism_is_preserved(self) -> None:
        from ua_workflows.shared.db.video_enhancer import normalize_effect_one_liner

        compact = normalize_effect_one_liner("真人照片男变女性别转换视频")

        self.assertIn("性别转换", compact)

    def test_asset_variant_keys_ignore_demographic_swaps(self) -> None:
        from ua_workflows.video_enhancer.play_asset_report import (
            _fallback_variant_key,
            _new_variant_key,
        )

        a = {
            "ad_key": "a",
            "play_fingerprint": "黑人女性自拍生成球员卡片模板",
            "effect_one_liner": "",
            "differentiator": "",
        }
        b = {
            "ad_key": "b",
            "play_fingerprint": "白人男性自拍生成球员卡片模板",
            "effect_one_liner": "",
            "differentiator": "",
        }

        self.assertEqual(_fallback_variant_key(a), _fallback_variant_key(b))
        self.assertEqual(
            _new_variant_key(a, "template_asset", "球员卡片"),
            _new_variant_key(b, "template_asset", "球员卡片"),
        )

    def test_narrow_novelty_treats_same_template_demographic_swap_as_reskin(self) -> None:
        from ua_workflows.video_enhancer.play_asset_report import _annotate_narrow_novelty

        history = [
            {
                "target_date": "2026-05-17",
                "play_asset_id": "sports_card",
                "template_fingerprint": "黑人女性自拍开场生成球员卡定格模板",
            }
        ]
        items = [
            {
                "ad_key": "today_a",
                "play_asset_id": "sports_card",
                "play_asset_is_new": False,
                "play_asset_variant_is_new": True,
                "template_fingerprint": "白人男性自拍开场生成球员卡定格模板",
            }
        ]

        _annotate_narrow_novelty(items, history, "2026-05-18")

        self.assertEqual(items[0]["narrow_novelty_label"], "老玩法换皮")
        self.assertFalse(items[0]["narrow_novelty_is_reportable"])

    def test_narrow_novelty_marks_new_template_under_old_play_as_iteration(self) -> None:
        from ua_workflows.video_enhancer.play_asset_report import _annotate_narrow_novelty

        history = [
            {
                "target_date": "2026-05-17",
                "play_asset_id": "photo_template",
                "template_fingerprint": "自拍开场生成球员卡定格模板",
            }
        ]
        items = [
            {
                "ad_key": "today_b",
                "play_asset_id": "photo_template",
                "play_asset_is_new": False,
                "play_asset_variant_is_new": True,
                "template_fingerprint": "自拍开场生成杂志封面翻页模板",
            }
        ]

        _annotate_narrow_novelty(items, history, "2026-05-18")

        self.assertEqual(items[0]["narrow_novelty_label"], "老玩法新迭代")
        self.assertTrue(items[0]["narrow_novelty_is_reportable"])

    def test_narrow_novelty_marks_unseen_stable_play_as_new_play(self) -> None:
        from ua_workflows.video_enhancer.play_asset_report import _annotate_narrow_novelty

        items = [
            {
                "ad_key": "today_c",
                "play_asset_id": "brand_new_play",
                "play_asset_is_new": True,
                "play_asset_variant_is_new": True,
                "template_fingerprint": "单人照片生成全新互动结局模板",
            }
        ]

        _annotate_narrow_novelty(items, [], "2026-05-18")

        self.assertEqual(items[0]["narrow_novelty_label"], "新玩法")
        self.assertTrue(items[0]["narrow_novelty_is_reportable"])

    def test_generic_new_play_name_is_sanitized(self) -> None:
        from ua_workflows.video_enhancer.analyze import _sanitize_new_play_name

        name, variant, reason = _sanitize_new_play_name(
            play_asset_id="new_play",
            play_asset_name="AI 图片转视频",
            play_asset_subtag_names="AI 视频生成",
            play_asset_novelty_label="新玩法",
            play_asset_reason="未命中已有标签",
        )

        self.assertEqual(name, "待人工归类")
        self.assertEqual(variant, "待人工归类")
        self.assertIn("原值：AI 图片转视频", reason)

    def test_specific_new_play_name_is_preserved(self) -> None:
        from ua_workflows.video_enhancer.analyze import _sanitize_new_play_name

        name, variant, reason = _sanitize_new_play_name(
            play_asset_id="new_play",
            play_asset_name="照片生成灾难电影场景",
            play_asset_subtag_names="照片生成灾难电影场景",
            play_asset_novelty_label="新玩法",
            play_asset_reason="未命中已有标签",
        )

        self.assertEqual(name, "照片生成灾难电影场景")
        self.assertEqual(variant, "照片生成灾难电影场景")
        self.assertEqual(reason, "未命中已有标签")

    def test_bitable_multi_select_field_value_is_list(self) -> None:
        from ua_workflows.video_enhancer.sync import _normalize_bitable_field_value

        field_info = {"field_name": "玩法", "type": 4}

        self.assertEqual(_normalize_bitable_field_value("玩法", "赛场转播", field_info), ["赛场转播"])
        self.assertEqual(
            _normalize_bitable_field_value("玩法", "赛场转播、机甲变身", field_info),
            ["赛场转播", "机甲变身"],
        )

    def test_bitable_multi_select_field_value_filters_unknown_options(self) -> None:
        from ua_workflows.video_enhancer.sync import _normalize_bitable_field_value

        field_info = {
            "field_name": "玩法",
            "type": 4,
            "property": {"options": [{"name": "赛场转播"}, {"name": "机甲变身"}]},
        }

        self.assertEqual(
            _normalize_bitable_field_value("玩法", "赛场转播、AI 场景角色生成", field_info),
            ["赛场转播"],
        )
        self.assertEqual(_normalize_bitable_field_value("玩法", "AI 场景角色生成", field_info), [])

    def test_daily_similarity_count_includes_same_day_cover_removed_members(self) -> None:
        from ua_workflows.video_enhancer.sync import build_daily_similarity_count_map

        items = [
            {
                "creative": {
                    "ad_key": "kept_a_full_key",
                    "appid": "app.ve.test",
                }
            },
            {
                "creative": {
                    "ad_key": "kept_b_full_key",
                    "appid": "app.ve.test",
                }
            },
        ]
        play_asset_by_ad = {
            "kept_a_full_key": {"play_asset_variant_key": "variant::same-template"},
            "kept_b_full_key": {"play_asset_variant_key": "variant::same-template"},
        }
        cover_report = {
            "per_appid": [
                {
                    "removed": [
                        {
                            "ad_key": "removed_same_day",
                            "reason": "cover_style_cluster",
                            "kept_ad_key": "kept_a_full_key",
                        },
                        {
                            "ad_key": "removed_cross_day",
                            "reason": "cover_style_cluster_vs_yesterday",
                            "kept_ad_key": "kept_a_full_key",
                        },
                        {
                            "ad_key": "removed_stale_same_day",
                            "reason": "cover_style_cluster_history_refresh",
                            "kept_ad_key": "kept_a_full_key",
                        },
                    ]
                }
            ]
        }

        counts = build_daily_similarity_count_map(
            items,
            play_asset_by_ad=play_asset_by_ad,
            effect_by_ad={},
            play_fingerprint_by_ad={},
            cover_intraday_report=cover_report,
        )

        self.assertEqual(counts["kept_a_full_key"], 4)
        self.assertEqual(counts["kept_b_full_key"], 4)

    def test_cover_history_refresh_tags_are_written_for_kept_representative(self) -> None:
        from ua_workflows.video_enhancer.sync import build_cover_history_refresh_tag_map

        tags = build_cover_history_refresh_tag_map(
            {
                "per_appid": [
                    {
                        "history_refresh": [
                            {
                                "ad_key": "today_kept",
                                "matched_ad_key": "history_old",
                                "matched_date": "2026-05-08",
                                "history_age_days": 13,
                                "similarity": 0.7554,
                            }
                        ]
                    }
                ]
            }
        )

        self.assertIn("历史簇持续发力", tags["today_kept"])
        self.assertIn("历史命中:2026-05-08", tags["today_kept"])
        self.assertIn("历史间隔:13天", tags["today_kept"])
        self.assertIn("历史封面相似度:0.76", tags["today_kept"])


    def test_bitable_template_dedup_fuzzy_merges_same_template_wording(self) -> None:
        from ua_workflows.video_enhancer.sync import apply_template_dedup_for_bitable

        old_clip = os.environ.get("BITABLE_TEMPLATE_DEDUP_CLIP_ENABLED")
        old_text = os.environ.get("BITABLE_TEMPLATE_DEDUP_TEXT_SIMILARITY_ENABLED")
        os.environ["BITABLE_TEMPLATE_DEDUP_CLIP_ENABLED"] = "0"
        os.environ["BITABLE_TEMPLATE_DEDUP_TEXT_SIMILARITY_ENABLED"] = "1"
        try:
            items = [
                {
                    "product": "Pixverse",
                    "creative": {
                        "ad_key": "high_exposure",
                        "appid": "app.ve.test",
                        "all_exposure_value": 100,
                    },
                },
                {
                    "product": "Pixverse",
                    "creative": {
                        "ad_key": "low_exposure",
                        "appid": "app.ve.test",
                        "all_exposure_value": 20,
                    },
                },
            ]
            play_asset_by_ad = {
                "high_exposure": {"play_asset_id": "photo_to_dance"},
                "low_exposure": {"play_asset_id": "photo_to_dance"},
            }
            template_by_ad = {
                "high_exposure": "主播前景讲解加爆款视频背景，穿插动物跳舞案例与界面演示，最后展示社交账号数据与下载引导",
                "low_exposure": "爆款视频网格主播讲解，穿插动物跳舞案例和界面演示，最后展示社交账号数据与下载引导",
            }

            filtered, skipped = apply_template_dedup_for_bitable(
                items,
                play_asset_by_ad=play_asset_by_ad,
                effect_by_ad={
                    "high_exposure": "静态照片生成跳舞动画视频",
                    "low_exposure": "静态照片生成跳舞动画视频",
                },
                play_fingerprint_by_ad={},
                template_fingerprint_by_ad=template_by_ad,
            )
        finally:
            if old_clip is None:
                os.environ.pop("BITABLE_TEMPLATE_DEDUP_CLIP_ENABLED", None)
            else:
                os.environ["BITABLE_TEMPLATE_DEDUP_CLIP_ENABLED"] = old_clip
            if old_text is None:
                os.environ.pop("BITABLE_TEMPLATE_DEDUP_TEXT_SIMILARITY_ENABLED", None)
            else:
                os.environ["BITABLE_TEMPLATE_DEDUP_TEXT_SIMILARITY_ENABLED"] = old_text

        self.assertEqual(len(filtered), 1)
        self.assertEqual((filtered[0]["creative"] or {})["ad_key"], "high_exposure")
        self.assertEqual(len(skipped), 1)
        self.assertEqual(skipped[0]["ad_key"], "low_exposure")
        self.assertEqual(skipped[0]["match_reason"], "template_fuzzy_text")

    def test_bitable_template_dedup_keeps_distinct_templates_under_same_play(self) -> None:
        from ua_workflows.video_enhancer.sync import apply_template_dedup_for_bitable

        old_clip = os.environ.get("BITABLE_TEMPLATE_DEDUP_CLIP_ENABLED")
        os.environ["BITABLE_TEMPLATE_DEDUP_CLIP_ENABLED"] = "0"
        try:
            items = [
                {
                    "product": "Pixverse",
                    "creative": {
                        "ad_key": "sports_card",
                        "appid": "app.ve.test",
                        "all_exposure_value": 100,
                    },
                },
                {
                    "product": "Pixverse",
                    "creative": {
                        "ad_key": "magazine_cover",
                        "appid": "app.ve.test",
                        "all_exposure_value": 90,
                    },
                },
            ]
            filtered, skipped = apply_template_dedup_for_bitable(
                items,
                play_asset_by_ad={
                    "sports_card": {"play_asset_id": "photo_template"},
                    "magazine_cover": {"play_asset_id": "photo_template"},
                },
                effect_by_ad={
                    "sports_card": "照片生成热门模板",
                    "magazine_cover": "照片生成热门模板",
                },
                play_fingerprint_by_ad={},
                template_fingerprint_by_ad={
                    "sports_card": "自拍开场生成球员卡定格模板",
                    "magazine_cover": "自拍开场生成杂志封面翻页模板",
                },
            )
        finally:
            if old_clip is None:
                os.environ.pop("BITABLE_TEMPLATE_DEDUP_CLIP_ENABLED", None)
            else:
                os.environ["BITABLE_TEMPLATE_DEDUP_CLIP_ENABLED"] = old_clip

        self.assertEqual(len(filtered), 2)
        self.assertEqual(skipped, [])

    def test_bitable_play_labels_convert_to_asset_catalog(self) -> None:
        from ua_workflows.video_enhancer.bitable_play_labels import labels_to_play_assets

        assets = labels_to_play_assets(["照片生成跳舞动画", "赛事现场转播"])

        self.assertEqual([asset["name"] for asset in assets], ["照片生成跳舞动画", "赛事现场转播"])
        self.assertTrue(assets[0]["asset_id"].startswith("play_"))
        self.assertEqual(assets[0]["source"], "bitable_play_label")

    def test_crawl_similarity_count_starts_from_raw_signatures(self) -> None:
        from ua_workflows.video_enhancer.crawl_similarity import annotate_crawl_similarity_counts

        raw_payload = {
            "items": [
                {
                    "appid": "app.ve.test",
                    "creative": {
                        "ad_key": "a1",
                        "image_ahash_md5": "same-cover",
                    },
                },
                {
                    "appid": "app.ve.test",
                    "creative": {
                        "ad_key": "a2",
                        "image_ahash_md5": "same-cover",
                    },
                },
                {
                    "appid": "app.ve.test",
                    "creative": {
                        "ad_key": "a3",
                        "image_ahash_md5": "other-cover",
                    },
                },
            ]
        }

        annotate_crawl_similarity_counts(raw_payload)

        self.assertEqual(raw_payload["items"][0]["crawl_similarity_count"], 2)
        self.assertEqual(raw_payload["items"][1]["creative"]["crawl_similarity_count"], 2)
        self.assertEqual(raw_payload["items"][2]["crawl_similarity_count"], 1)
        self.assertEqual(raw_payload["crawl_similarity_count_by_ad"]["a1"], 2)

    def test_crawl_similarity_count_merges_same_day_cover_clusters(self) -> None:
        from ua_workflows.video_enhancer.crawl_similarity import merge_cover_similarity_counts

        raw_payload = {
            "items": [
                {"appid": "app.ve.test", "creative": {"ad_key": "kept_a"}},
                {"appid": "app.ve.test", "creative": {"ad_key": "kept_b"}},
            ],
            "cover_style_intraday_report": {
                "per_appid": [
                    {
                        "removed": [
                            {
                                "ad_key": "removed_same_day",
                                "reason": "cover_style_cluster",
                                "kept_ad_key": "kept_a",
                            },
                            {
                                "ad_key": "removed_cross_day",
                                "reason": "cover_style_cluster_vs_yesterday",
                                "kept_ad_key": "kept_a",
                            },
                            {
                                "ad_key": "removed_stale_same_day",
                                "reason": "cover_style_cluster_history_refresh",
                                "kept_ad_key": "kept_a",
                            },
                        ]
                    }
                ]
            },
        }

        merge_cover_similarity_counts(raw_payload)

        self.assertEqual(raw_payload["crawl_similarity_count_by_ad"]["kept_a"], 3)
        self.assertEqual(raw_payload["crawl_similarity_count_by_ad"]["kept_b"], 1)

    def test_crawl_retention_report_counts_cover_and_crawl_removals(self) -> None:
        from ua_workflows.video_enhancer.pipeline import _build_crawl_product_retention_report

        raw_payload = {
            "target_date": "2026-05-18",
            "crawl_mode": "dom_detail_click",
            "items": [
                {
                    "product": "Pixverse",
                    "creative": {
                        "ad_key": "kept_a",
                        "video_url": "https://example.com/a.mp4",
                    },
                }
            ],
            "filter_report": {
                "per_product": {
                    "Pixverse": {
                        "clicked_detail_rows": 5,
                        "dom_cards": 6,
                        "captured": 6,
                        "date_hits": 4,
                        "after": 3,
                        "advertiser_excluded": 1,
                        "date_filtered": 2,
                        "resume_excluded": 1,
                        "duplicate_excluded": 0,
                        "truncated_excluded": 0,
                    }
                }
            },
            "cover_style_intraday_report": {
                "cross_day_fingerprint_removed": [
                    {"product": "Pixverse", "ad_key": "old_fp"}
                ],
                "per_appid": [
                    {
                        "product": "Pixverse",
                        "removed": [
                            {
                                "ad_key": "old_clip",
                                "reason": "cover_style_cluster_vs_yesterday",
                            },
                            {
                                "ad_key": "same_day_clip",
                                "reason": "cover_style_cluster",
                            },
                        ],
                    }
                ],
            },
        }

        report = _build_crawl_product_retention_report(raw_payload)
        row = report["per_product"][0]

        self.assertEqual(row["clicked_detail_rows"], 5)
        self.assertEqual(row["captured_materials"], 6)
        self.assertEqual(row["kept_after_crawl_filter"], 3)
        self.assertEqual(row["kept_after_cover_filter"], 1)
        self.assertEqual(row["removed_reasons"]["advertiser_mismatch"], 1)
        self.assertEqual(row["removed_reasons"]["non_target_date"], 2)
        self.assertEqual(row["removed_reasons"]["resume_advertising"], 1)
        self.assertEqual(row["removed_reasons"]["cover_crossday_fingerprint"], 1)
        self.assertEqual(row["removed_reasons"]["cover_clip_crossday"], 1)
        self.assertEqual(row["removed_reasons"]["cover_clip_intraday"], 1)

    def test_flow_report_flags_low_product_volume_with_retry_command(self) -> None:
        from ua_workflows.video_enhancer.flow_report import detect_product_volume_alerts

        alerts = detect_product_volume_alerts(
            [
                {
                    "product": "PixVerse",
                    "kept_after_cover_filter": 2,
                    "synced_records": 1,
                }
            ],
            {
                "PixVerse": {
                    "kept_after_cover_filter": [12, 10, 14],
                    "synced_records": [8, 9, 7],
                }
            },
            low_ratio=0.5,
            min_baseline=3,
            min_history_days=2,
            target_date="2026-05-18",
        )

        metrics = {a["metric"] for a in alerts}
        self.assertIn("kept_after_cover_filter", metrics)
        self.assertIn("synced_records", metrics)
        self.assertTrue(all("PixVerse" in a["retry_command"] for a in alerts))

    def test_human_photo_effect_filter_blocks_ecommerce_and_non_human_effects(self) -> None:
        from ua_workflows.video_enhancer.content_filters import apply_human_photo_effect_filter

        rows = [
            {
                "ad_key": "person_ok",
                "title": "Upload your selfie",
                "effect_one_liner": "自拍生成生日写真",
                "play_fingerprint": "单人照片生成生日主题写真",
                "template_fingerprint": "用户自拍开场生成生日写真模板",
                "analysis": "用户上传一张真人自拍照片，生成生日主题写真海报。",
                "material_tags": ["无明显风险"],
            },
            {
                "ad_key": "ecom_bad",
                "title": "Product photo ads",
                "effect_one_liner": "商品图生成动态广告视频",
                "play_fingerprint": "商品图片生成促销广告",
                "analysis": "上传商品图片，一键生成电商促销广告视频。",
                "material_tags": [],
            },
            {
                "ad_key": "pet_bad",
                "title": "Pet dance",
                "effect_one_liner": "宠物照片生成跳舞动画",
                "play_fingerprint": "宠物照片生成跳舞视频",
                "analysis": "用户上传宠物照片，让猫狗生成跳舞动画。",
                "material_tags": [],
            },
            {
                "ad_key": "room_bad",
                "title": "Room makeover",
                "effect_one_liner": "空房间生成装修效果图",
                "play_fingerprint": "房间照片生成家装效果",
                "analysis": "上传房间照片，生成室内装修设计效果图。",
                "material_tags": [],
            },
        ]

        newly_marked, details = apply_human_photo_effect_filter(rows)

        self.assertEqual(newly_marked, 3)
        self.assertFalse(rows[0].get("exclude_from_bitable"))
        for row in rows[1:]:
            self.assertTrue(row.get("exclude_from_bitable"))
            self.assertTrue(row.get("exclude_from_cluster"))
            self.assertIn("非人物照片加工特效", row.get("material_tags") or [])
        reasons = {detail["ad_key"]: detail["reason"] for detail in details}
        self.assertEqual(reasons["ecom_bad"], "ecommerce_effect")
        self.assertEqual(reasons["pet_bad"], "non_human_photo_effect")
        self.assertEqual(reasons["room_bad"], "non_human_photo_effect")

    def test_ve_prompt_leaves_unmatched_play_labels_blank(self) -> None:
        from ua_workflows.video_enhancer.analyze import _ve_fixed_footer

        footer = _ve_fixed_footer()

        self.assertIn("若没有任何标签能覆盖核心玩法，玩法资产ID写 unmatched_play", footer)
        self.assertIn("玩法资产名称留空", footer)
        self.assertNotIn("写 new_play", footer)

    def test_bitable_play_fields_do_not_emit_redundant_internal_columns(self) -> None:
        from ua_workflows.video_enhancer.sync import _build_bitable_play_fields

        fields = _build_bitable_play_fields(
            {
                "play_asset_id": "play_existing",
                "play_asset_name": "赛场转播",
                "play_asset_variant_name": "基础变体",
                "play_asset_novelty_label": "已沉淀玩法",
                "narrow_novelty_label": "老玩法换皮",
                "play_asset_variant_key": "play_existing::base",
                "play_asset_match_source": "ai",
                "play_asset_classification_reason": "命中多维表玩法标签",
                "template_fingerprint": "自拍开场生成球员卡定格模板",
            },
            play_fingerprint="自拍生成球员卡",
            differentiator="无",
            template_fingerprint="",
        )

        self.assertEqual(
            fields,
            {
                "玩法": "赛场转播",
                "玩法指纹": "自拍生成球员卡",
                "差异点": "无",
                "模板指纹": "自拍开场生成球员卡定格模板",
            },
        )

    def test_bitable_play_fields_leave_unmatched_play_blank(self) -> None:
        from ua_workflows.video_enhancer.sync import _build_bitable_play_fields

        fields = _build_bitable_play_fields(
            {
                "play_asset_id": "new_play",
                "play_asset_name": "AI 图片转视频",
                "template_fingerprint": "自拍开场生成动态视频模板",
            },
            play_fingerprint="自拍生成动态视频",
            differentiator="无",
            template_fingerprint="",
        )

        self.assertEqual(fields["玩法"], "")
        self.assertEqual(fields["玩法指纹"], "自拍生成动态视频")
        self.assertEqual(fields["模板指纹"], "自拍开场生成动态视频模板")

    def test_unmatched_play_choice_does_not_fallback_to_rule_label(self) -> None:
        from ua_workflows.video_enhancer.play_asset_report import annotate_items_with_play_assets

        items = [
            {
                "ad_key": "unmatched_a",
                "play_asset_id": "unmatched_play",
                "play_asset_name": "",
                "play_asset_novelty_label": "未命中",
                "effect_one_liner": "自拍生成球员卡",
                "play_fingerprint": "自拍生成球员卡",
            }
        ]
        assets = [
            {
                "asset_id": "sports_card",
                "name": "赛场转播",
                "include_keywords": ["球员卡"],
                "min_score": 1,
            }
        ]

        annotate_items_with_play_assets(items, assets=assets)

        self.assertEqual(items[0]["play_asset_id"], "")
        self.assertEqual(items[0]["play_asset_name"], "")
        self.assertEqual(items[0]["play_asset_variant_name"], "")


if __name__ == "__main__":
    unittest.main()
