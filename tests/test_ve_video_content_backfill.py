from __future__ import annotations

import json
import argparse
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock


class VeVideoContentBackfillTest(unittest.TestCase):
    def test_load_key_scope_preserves_date_specific_ad_keys(self) -> None:
        from ua_workflows.video_enhancer.video_content_backfill import load_key_scope

        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "keys.json"
            path.write_text(
                json.dumps(
                    {
                        "product": "AI Video - AI Video Generator",
                        "dates": {
                            "2026-06-08": ["ad_a", "ad_b", "ad_a"],
                            "2026-06-09": ["ad_c"],
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            scope = load_key_scope(path)

        self.assertEqual(scope.product, "AI Video - AI Video Generator")
        self.assertEqual(scope.dates, {"2026-06-08": ["ad_a", "ad_b"], "2026-06-09": ["ad_c"]})
        self.assertEqual(scope.ad_keys, ["ad_a", "ad_b", "ad_c"])

    def test_collect_raw_records_uses_only_material_video_content(self) -> None:
        from ua_workflows.video_enhancer.video_content_backfill import (
            KeyScope,
            collect_raw_records,
        )

        with tempfile.TemporaryDirectory() as td:
            raw_dir = Path(td)
            (raw_dir / "workflow_video_enhancer_2026-06-08_raw.json").write_text(
                json.dumps(
                    {
                        "items": [
                            {
                                "product": "AI Video - AI Video Generator",
                                "appid": "ai.video.generator",
                                "creative": {
                                    "ad_key": "ad_a",
                                    "preview_img_url": "https://example.com/a.jpg",
                                    "material_script_analysis": {
                                        "video_content": "A woman uploads a photo and the app creates a dance video.",
                                        "timeline": [{"segment_observations": "ignored"}],
                                    },
                                },
                            },
                            {
                                "product": "AI Video - AI Video Generator",
                                "appid": "ai.video.generator",
                                "creative": {
                                    "ad_key": "ad_b",
                                    "material_script_analysis": {
                                        "video_content": "",
                                        "timeline": [{"segment_observations": "do not use this as fallback"}],
                                    },
                                },
                            },
                        ]
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            records = collect_raw_records(
                KeyScope(
                    product="AI Video - AI Video Generator",
                    dates={"2026-06-08": ["ad_a", "ad_b"]},
                ),
                raw_dir,
            )

        by_key = {record["ad_key"]: record for record in records}
        self.assertEqual(by_key["ad_a"]["video_content"], "A woman uploads a photo and the app creates a dance video.")
        self.assertEqual(by_key["ad_a"]["content_source"], "raw_material_script_analysis")
        self.assertEqual(by_key["ad_b"]["video_content"], "")
        self.assertEqual(by_key["ad_b"]["content_source"], "")

    def test_similarity_payload_uses_top_three_per_record(self) -> None:
        from ua_workflows.video_enhancer.video_content_backfill import build_similarity_payload

        records = [
            {"target_date": "2026-06-08", "ad_key": "ad_a", "video_content": "alpha"},
            {"target_date": "2026-06-08", "ad_key": "ad_b", "video_content": "alpha near"},
            {"target_date": "2026-06-09", "ad_key": "ad_c", "video_content": "beta"},
            {"target_date": "2026-06-10", "ad_key": "ad_d", "video_content": ""},
        ]
        vectors = {
            "alpha": [1.0, 0.0],
            "alpha near": [0.9, 0.1],
            "beta": [0.0, 1.0],
        }

        payload = build_similarity_payload(records, embed_fn=lambda text: vectors[text], top_k=3, min_similarity=0.0)

        self.assertEqual(payload["summary"]["records"], 4)
        self.assertEqual(payload["summary"]["embedded_records"], 3)
        self.assertEqual(payload["summary"]["empty_video_content"], 1)
        top_by_key = {row["ad_key"]: row["top_matches"] for row in payload["records"]}
        self.assertEqual([m["ad_key"] for m in top_by_key["ad_a"]], ["ad_b"])
        self.assertEqual([m["ad_key"] for m in top_by_key["ad_c"]], ["ad_b", "ad_a"])
        self.assertGreater(top_by_key["ad_c"][0]["similarity"], top_by_key["ad_c"][1]["similarity"])

    def test_similarity_payload_never_matches_future_dates_for_a_record(self) -> None:
        from ua_workflows.video_enhancer.video_content_backfill import build_similarity_payload

        records = [
            {"target_date": "2026-06-08", "ad_key": "ad_8", "video_content": "day8"},
            {"target_date": "2026-06-09", "ad_key": "ad_9", "video_content": "day9"},
            {"target_date": "2026-06-10", "ad_key": "ad_10", "video_content": "day10"},
        ]
        vectors = {
            "day8": [1.0, 0.0],
            "day9": [0.9, 0.1],
            "day10": [0.8, 0.2],
        }

        payload = build_similarity_payload(records, embed_fn=lambda text: vectors[text], top_k=3, min_similarity=0.0)
        by_key = {row["ad_key"]: row for row in payload["records"]}

        self.assertEqual(by_key["ad_8"]["top_matches"], [])
        self.assertEqual([m["ad_key"] for m in by_key["ad_9"]["top_matches"]], ["ad_8"])
        self.assertEqual([m["ad_key"] for m in by_key["ad_10"]["top_matches"]], ["ad_9", "ad_8"])
        self.assertTrue(
            all(pair["left_date"] >= pair["right_date"] for pair in payload["pairs"]),
            payload["pairs"],
        )

    def test_parse_adkeys_supports_comma_and_newline_dedupe(self) -> None:
        from ua_workflows.video_enhancer.video_content_backfill import parse_adkeys

        got = parse_adkeys(["a,b", "b,c", "a  d", "，e", "f\ng"])
        self.assertEqual(got, ["a", "b", "c", "d", "e", "f", "g"])

    def test_load_backfill_fills_search_results_by_ad_key(self) -> None:
        from ua_workflows.video_enhancer.video_content_backfill import _creatives_from_search_results

        result = _creatives_from_search_results(
            [
                {
                    "keyword": "ad_a",
                    "all_creatives": [
                        {"ad_key": "ad_a", "guangdada_video_content": "A"},
                        {"ad_key": "ad_a", "guangdada_video_content": "A2"},
                        {"ad_key": "ad_b", "guangdada_video_content": "B"},
                    ],
                },
                {"keyword": "ad_b", "all_creatives": [{"ad_key": "ad_c", "guangdada_video_content": "C"}]},
            ]
        )

        self.assertEqual(result["ad_a"]["guangdada_video_content"], "A2")
        self.assertEqual(result["ad_b"]["guangdada_video_content"], "B")
        self.assertEqual(result["ad_c"]["guangdada_video_content"], "C")

    def test_apply_direct_analysis_updates_only_missing_records(self) -> None:
        from ua_workflows.video_enhancer.video_content_backfill import apply_direct_analysis_to_missing

        records = [
            {
                "ad_key": "ad_a",
                "video_content": "",
                "raw_item": {"creative": {"ad_key": "ad_a"}},
            },
            {
                "ad_key": "ad_b",
                "video_content": "already present",
                "raw_item": {"creative": {"ad_key": "ad_b"}},
            },
            {
                "ad_key": "ad_c",
                "video_content": "",
                "raw_item": {"creative": {"ad_key": "ad_c"}},
            },
        ]

        async def fake_fetch(creative):
            if creative["ad_key"] == "ad_a":
                return {"script_analysis": {"video_content": "direct video content"}}
            return {}

        summary = asyncio_run(apply_direct_analysis_to_missing(records, fake_fetch))

        self.assertEqual(summary, {"requested": 2, "updated": 1})
        self.assertEqual(records[0]["video_content"], "direct video content")
        self.assertEqual(records[0]["content_source"], "guangdada_direct_material_script_analysis")
        self.assertEqual(records[1]["video_content"], "already present")
        self.assertEqual(records[2]["video_content"], "")

    def test_build_llm_video_content_analysis_uses_guangdada_like_shape(self) -> None:
        from ua_workflows.video_enhancer.video_content_backfill import build_llm_video_content_analysis

        analysis = build_llm_video_content_analysis(
            """
            ```json
            {
              "video_content": "视频展示用户上传自拍后生成节日主题动态写真。",
              "timeline": [{"time": "0:03", "label": "生成写真成片"}]
            }
            ```
            """
        )

        self.assertEqual(analysis["source"], "llm_video_content_fallback")
        self.assertEqual(
            analysis["script_analysis"]["video_content"],
            "视频展示用户上传自拍后生成节日主题动态写真。",
        )
        self.assertEqual(analysis["script_analysis"]["timeline"], [{"time": "0:03", "label": "生成写真成片"}])

    def test_apply_llm_video_content_fallback_updates_only_missing_media_records(self) -> None:
        from ua_workflows.video_enhancer.video_content_backfill import apply_llm_video_content_fallback_to_missing

        records = [
            {
                "ad_key": "ad_a",
                "video_content": "",
                "raw_item": {
                    "product": "PixVerse",
                    "creative": {"ad_key": "ad_a", "video_url": "https://example.com/a.mp4"},
                },
            },
            {
                "ad_key": "ad_b",
                "video_content": "already present",
                "raw_item": {
                    "product": "PixVerse",
                    "creative": {"ad_key": "ad_b", "video_url": "https://example.com/b.mp4"},
                },
            },
            {
                "ad_key": "ad_c",
                "video_content": "",
                "raw_item": {"product": "PixVerse", "creative": {"ad_key": "ad_c"}},
            },
        ]

        def fake_generate(item, creative):
            self.assertEqual(item["product"], "PixVerse")
            self.assertEqual(creative["ad_key"], "ad_a")
            return {"script_analysis": {"video_content": "LLM 根据视频补出的内容"}}

        summary = apply_llm_video_content_fallback_to_missing(records, generate_fn=fake_generate)

        self.assertEqual(summary, {"requested": 1, "updated": 1, "skipped_no_media": 1, "failed": 0})
        self.assertEqual(records[0]["video_content"], "LLM 根据视频补出的内容")
        self.assertEqual(records[0]["content_source"], "llm_video_content_fallback")
        creative = records[0]["raw_item"]["creative"]
        self.assertEqual(creative["guangdada_video_content"], "LLM 根据视频补出的内容")
        self.assertEqual(
            creative["material_script_analysis"]["script_analysis"]["video_content"],
            "LLM 根据视频补出的内容",
        )
        self.assertEqual(records[1]["video_content"], "already present")
        self.assertEqual(records[2]["video_content"], "")

    def test_apply_video_content_records_back_to_raw_payload(self) -> None:
        from ua_workflows.video_enhancer.video_content_backfill import apply_video_content_records_to_raw_payload

        raw_payload = {
            "items": [
                {"creative": {"ad_key": "ad_a"}},
                {"creative": {"ad_key": "ad_b", "guangdada_video_content": ""}},
            ]
        }
        records = [
            {
                "ad_key": "ad_a",
                "video_content": "补回内容",
                "raw_item": {
                    "creative": {
                        "ad_key": "ad_a",
                        "guangdada_video_content": "补回内容",
                        "material_script_analysis": {
                            "source": "llm_video_content_fallback",
                            "script_analysis": {"video_content": "补回内容"},
                        },
                    }
                },
            }
        ]

        updated = apply_video_content_records_to_raw_payload(raw_payload, records)

        self.assertEqual(updated, 1)
        creative = raw_payload["items"][0]["creative"]
        self.assertEqual(creative["guangdada_video_content"], "补回内容")
        self.assertEqual(
            creative["material_script_analysis"]["script_analysis"]["video_content"],
            "补回内容",
        )

    def test_overlay_existing_video_content_supports_resume(self) -> None:
        from ua_workflows.video_enhancer.video_content_backfill import overlay_existing_video_content

        records = [
            {"target_date": "2026-06-08", "ad_key": "ad_a", "video_content": ""},
            {"target_date": "2026-06-08", "ad_key": "ad_b", "video_content": "raw content"},
        ]

        updated = overlay_existing_video_content(
            records,
            {("2026-06-08", "ad_a"): "db content", ("2026-06-08", "ad_b"): "db ignored"},
        )

        self.assertEqual(updated, 1)
        self.assertEqual(records[0]["video_content"], "db content")
        self.assertEqual(records[0]["content_source"], "db_existing_guangdada_video_content")
        self.assertEqual(records[1]["video_content"], "raw content")

    def test_upsert_video_content_records_updates_existing_rows_without_inserting_duplicates(self) -> None:
        from ua_workflows.video_enhancer import video_content_backfill

        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "ve.db"
            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                CREATE TABLE daily_creative_insights (
                    target_date TEXT,
                    product TEXT,
                    ad_key TEXT,
                    guangdada_video_content TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE creative_library (
                    ad_key TEXT PRIMARY KEY,
                    guangdada_video_content TEXT
                )
                """
            )
            conn.execute(
                "INSERT INTO daily_creative_insights VALUES (?, ?, ?, ?)",
                ("2026-06-02", "AI Video - AI Video Generator", "ad_a", ""),
            )
            conn.execute("INSERT INTO creative_library VALUES (?, ?)", ("ad_a", ""))
            conn.commit()
            conn.close()

            with mock.patch.object(video_content_backfill.ve_db, "DB_PATH", db_path), mock.patch.object(
                video_content_backfill.ve_db,
                "init_db",
            ):
                summary = video_content_backfill.upsert_video_content_records(
                    [
                        {
                            "target_date": "2026-06-02",
                            "ad_key": "ad_a",
                            "product": "",
                            "video_content": "回填视频内容",
                            "raw_item": {"creative": {"ad_key": "ad_a", "guangdada_video_content": "回填视频内容"}},
                        }
                    ]
                )

            conn = sqlite3.connect(db_path)
            rows = conn.execute(
                "SELECT product, guangdada_video_content FROM daily_creative_insights WHERE ad_key = ?",
                ("ad_a",),
            ).fetchall()
            library = conn.execute(
                "SELECT guangdada_video_content FROM creative_library WHERE ad_key = ?",
                ("ad_a",),
            ).fetchone()
            conn.close()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0], ("AI Video - AI Video Generator", "回填视频内容"))
        self.assertEqual(library[0], "回填视频内容")
        self.assertEqual(summary["daily_updated"], 1)
        self.assertEqual(summary["library_updated"], 1)

    def test_render_dashboard_keeps_visible_cover_next_to_video(self) -> None:
        from ua_workflows.video_enhancer.video_content_backfill import render_dashboard_html

        html = render_dashboard_html(
            {
                "summary": {},
                "pairs": [],
                "records": [
                    {
                        "target_date": "2026-06-08",
                        "ad_key": "ad_a",
                        "video_url": "https://sp2cdn-idea-global.zingfront.com/sp_opera/a.mp4",
                        "preview_img_url": "https://sp2cdn-idea-global.zingfront.com/sp_opera/a.jpg",
                        "video_content": "视频内容",
                        "top_matches": [],
                    }
                ],
            },
            title="test",
        )

        self.assertIn("<video", html)
        self.assertIn('class="cover-fallback"', html)
        self.assertIn("/__media_proxy?u=", html)

    def test_render_dashboard_shows_candidate_match_covers(self) -> None:
        from ua_workflows.video_enhancer.video_content_backfill import render_dashboard_html

        html = render_dashboard_html(
            {
                "summary": {},
                "pairs": [
                    {
                        "left_ad_key": "ad_a",
                        "right_ad_key": "ad_b",
                        "left_date": "2026-06-08",
                        "right_date": "2026-06-09",
                        "similarity": 0.91,
                        "cross_day": True,
                    }
                ],
                "records": [
                    {
                        "target_date": "2026-06-08",
                        "ad_key": "ad_a",
                        "preview_img_url": "https://sp2cdn-idea-global.zingfront.com/sp_opera/a.jpg",
                        "video_content": "视频 A",
                        "top_matches": [
                            {
                                "ad_key": "ad_b",
                                "target_date": "2026-06-09",
                                "similarity": 0.91,
                                "cross_day": True,
                            }
                        ],
                    },
                    {
                        "target_date": "2026-06-09",
                        "ad_key": "ad_b",
                        "preview_img_url": "https://sp2cdn-idea-global.zingfront.com/sp_opera/b.jpg",
                        "video_content": "视频 B",
                        "top_matches": [],
                    },
                ],
            },
            title="test",
        )

        self.assertIn('class="pair-cover"', html)
        self.assertIn('class="match-cover"', html)
        self.assertIn("a.jpg", html)
        self.assertIn("b.jpg", html)

    def test_adkey_backfill_target_date_does_not_limit_guangdada_search_date_range(self) -> None:
        from scripts import backfill_ve_video_content_by_adkeys

        captured = {}

        async def fake_fetch(records, **kwargs):
            captured.update(kwargs)
            return {"direct": {"requested": 1, "updated": 0}, "search": {"requested": 1, "updated": 0}}

        args = argparse.Namespace(
            adkey=["ad_a"],
            adkeys_file="",
            product="",
            target_date=["2026-06-08"],
            retries=1,
            debug=False,
            max_scroll_rounds=1,
            no_direct=False,
            skip_search=False,
            top_k=3,
            min_similarity=0.85,
            build_dashboard=False,
            output_json="/tmp/unused.json",
            backfill_json="/tmp/unused_backfill.json",
            report_html="/tmp/unused.html",
        )

        with mock.patch.object(backfill_ve_video_content_by_adkeys, "load_project_env"), mock.patch.object(
            backfill_ve_video_content_by_adkeys,
            "read_video_content_records_for_adkeys",
            return_value=[{"target_date": "2026-06-08", "ad_key": "ad_a", "video_content": "", "raw_item": {}}],
        ), mock.patch.object(
            backfill_ve_video_content_by_adkeys,
            "fetch_missing_video_content_by_adkeys",
            side_effect=fake_fetch,
        ), mock.patch.object(
            backfill_ve_video_content_by_adkeys,
            "upsert_video_content_records",
            return_value={"daily_upserted": 0, "library_upserted": 0, "library_grouped": 0},
        ), mock.patch.object(
            backfill_ve_video_content_by_adkeys,
            "write_backfill_report",
        ):
            backfill_ve_video_content_by_adkeys.run(args)

        self.assertIsNone(captured.get("date_range"))
        self.assertIs(captured.get("skip_time_filter"), True)

    def test_adkey_backfill_no_direct_still_uses_search_fallback(self) -> None:
        from scripts import backfill_ve_video_content_by_adkeys

        captured = {}

        async def fake_fetch(records, **kwargs):
            captured.update(kwargs)
            return {"direct": {"requested": 0, "updated": 0}, "search": {"requested": 1, "updated": 1}}

        args = argparse.Namespace(
            adkey=["ad_a"],
            adkeys_file="",
            product="",
            target_date=[],
            retries=1,
            debug=False,
            max_scroll_rounds=1,
            no_direct=True,
            skip_search=False,
            top_k=3,
            min_similarity=0.85,
            build_dashboard=False,
            output_json="/tmp/unused.json",
            backfill_json="/tmp/unused_backfill.json",
            report_html="/tmp/unused.html",
        )

        with mock.patch.object(backfill_ve_video_content_by_adkeys, "load_project_env"), mock.patch.object(
            backfill_ve_video_content_by_adkeys,
            "read_video_content_records_for_adkeys",
            return_value=[{"target_date": "2026-06-08", "ad_key": "ad_a", "video_content": "", "raw_item": {}}],
        ), mock.patch.object(
            backfill_ve_video_content_by_adkeys,
            "fetch_missing_video_content_by_adkeys",
            side_effect=fake_fetch,
        ), mock.patch.object(
            backfill_ve_video_content_by_adkeys,
            "upsert_video_content_records",
            return_value={"daily_upserted": 1, "library_upserted": 1, "library_grouped": 1},
        ), mock.patch.object(
            backfill_ve_video_content_by_adkeys,
            "write_backfill_report",
        ):
            backfill_ve_video_content_by_adkeys.run(args)

        self.assertIs(captured.get("use_direct"), False)
        self.assertIs(captured.get("setup_search"), True)

    def test_adkey_search_timeout_returns_none_instead_of_hanging_batch(self) -> None:
        from ua_workflows.video_enhancer.video_content_backfill import _collect_adkey_search_result_with_timeout

        async def slow_collect(*args, **kwargs):
            import asyncio

            await asyncio.sleep(0.05)
            return {"keyword": "ad_a"}

        result, timed_out = asyncio_run(
            _collect_adkey_search_result_with_timeout(
                slow_collect,
                None,
                "ad_a",
                [],
                {"enabled": False},
                max_scroll_rounds=1,
                debug=False,
                timeout_sec=0.001,
            )
        )

        self.assertIsNone(result)
        self.assertTrue(timed_out)


def asyncio_run(coro):
    import asyncio

    return asyncio.run(coro)


if __name__ == "__main__":
    unittest.main()
