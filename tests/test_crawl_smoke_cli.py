from __future__ import annotations

import unittest


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


if __name__ == "__main__":
    unittest.main()
