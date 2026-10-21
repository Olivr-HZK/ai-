from __future__ import annotations

import unittest


class SocialChatDiversitySelectionTest(unittest.TestCase):
    def test_caps_each_advertiser_at_ten(self) -> None:
        from ua_workflows.video_enhancer.diversity_selection import (
            select_diverse_top_per_advertiser,
        )

        items = []
        embeddings = {}
        for appid in ("app.a", "app.b"):
            for idx in range(12):
                ad_key = f"{appid}-{idx}"
                items.append(
                    {
                        "appid": appid,
                        "product": appid,
                        "creative": {
                            "ad_key": ad_key,
                            "all_exposure_value": 100 - idx,
                        },
                    }
                )
                embeddings[ad_key] = [float(idx + 1), float(12 - idx), 1.0]

        selected, report = select_diverse_top_per_advertiser(
            items,
            limit=10,
            embedding_by_ad_key=embeddings,
        )

        self.assertEqual(len(selected), 20)
        self.assertEqual(
            {row["appid"]: row["output_count"] for row in report["per_appid"]},
            {"app.a": 10, "app.b": 10},
        )
        self.assertTrue(all("daily_value_selection" in item for item in selected))

    def test_difference_dominates_exposure_when_selecting(self) -> None:
        from ua_workflows.video_enhancer.diversity_selection import (
            select_diverse_top_per_advertiser,
        )

        def item(ad_key: str, exposure: int) -> dict:
            return {
                "appid": "app.a",
                "product": "A",
                "creative": {
                    "ad_key": ad_key,
                    "all_exposure_value": exposure,
                },
            }

        items = [
            item("common-high", 100),
            item("common-high-variant", 10_000),
            item("different-low", 1),
        ]
        embeddings = {
            "common-high": [1.0, 0.0],
            "common-high-variant": [0.9999, 0.01],
            "different-low": [-1.0, 0.0],
        }

        selected, report = select_diverse_top_per_advertiser(
            items,
            limit=2,
            embedding_by_ad_key=embeddings,
        )

        selected_keys = {row["creative"]["ad_key"] for row in selected}
        self.assertIn("different-low", selected_keys)
        self.assertEqual(len(selected_keys & {"common-high", "common-high-variant"}), 1)
        self.assertEqual(report["selection_priority"], "cover_difference_then_exposure")

    def test_missing_embeddings_are_only_fallback_candidates(self) -> None:
        from ua_workflows.video_enhancer.diversity_selection import (
            select_diverse_top_per_advertiser,
        )

        items = [
            {
                "appid": "app.a",
                "product": "A",
                "creative": {"ad_key": "with-vector", "all_exposure_value": 1},
            },
            {
                "appid": "app.a",
                "product": "A",
                "creative": {"ad_key": "without-vector", "all_exposure_value": 9999},
            },
        ]

        selected, _ = select_diverse_top_per_advertiser(
            items,
            limit=1,
            embedding_by_ad_key={"with-vector": [1.0, 0.0]},
        )

        self.assertEqual(selected[0]["creative"]["ad_key"], "with-vector")


if __name__ == "__main__":
    unittest.main()
