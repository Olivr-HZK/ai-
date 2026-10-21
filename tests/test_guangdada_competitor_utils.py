from __future__ import annotations

import unittest


class GuangdadaCompetitorUtilsTest(unittest.TestCase):
    def test_advertiser_match_allows_case_and_spacing_differences(self) -> None:
        from ua_workflows.shared.guangdada.competitor_utils import advertiser_matches_product

        self.assertTrue(
            advertiser_matches_product(
                "PixVerse: AI Video Generator",
                "Pixverse:AI Video Generator",
            )
        )
        self.assertTrue(
            advertiser_matches_product(
                "Glam AI: Video & Photo Editor",
                "Glam AI:Video & Photo Editor",
            )
        )

    def test_advertiser_match_allows_same_specific_brand_with_renamed_descriptor(self) -> None:
        from ua_workflows.shared.guangdada.competitor_utils import advertiser_matches_product

        self.assertTrue(
            advertiser_matches_product(
                "GIO: AI Headshot Generator",
                "GIO - AI Photoshoot Generator",
            )
        )

    def test_advertiser_match_rejects_generic_ai_video_generator_neighbors(self) -> None:
        from ua_workflows.shared.guangdada.competitor_utils import advertiser_matches_product

        product = "AI Video - AI Video Generator"

        self.assertTrue(advertiser_matches_product("AI Video - AI Video Generator", product))
        self.assertFalse(advertiser_matches_product("PixVerse: AI Video Generator", product))
        self.assertFalse(advertiser_matches_product("AI Catch: AI Video Generator", product))
        self.assertFalse(advertiser_matches_product("AI Video Generator - Toki", product))


if __name__ == "__main__":
    unittest.main()
