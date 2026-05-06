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

    def test_exposure_output_requires_successful_top10_dropdown(self) -> None:
        from ua_workflows.shared.crawl_smoke import exposure_attempt_succeeded

        self.assertTrue(exposure_attempt_succeeded("Top 创意下拉 ✓\n[raw] out.json"))
        self.assertFalse(exposure_attempt_succeeded("Top 创意下拉 ✗\n[raw] out.json"))
        self.assertFalse(exposure_attempt_succeeded("Top 创意下拉 ✓\nTraceback"))


if __name__ == "__main__":
    unittest.main()
