#!/usr/bin/env python3
"""Tests for VE daily workflow node summaries in reports."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


class VeDailyWorkflowNodesTest(unittest.TestCase):
    def test_core_stage_report_compares_advertiser_counts_to_7_day_history(self) -> None:
        from ua_workflows.video_enhancer import daily_nodes

        def write_json(data_dir: Path, date: str, suffix: str, payload: dict) -> None:
            path = data_dir / f"workflow_video_enhancer_{date}_{suffix}.json"
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

        with tempfile.TemporaryDirectory() as td:
            data_dir = Path(td)
            reports_dir = data_dir / "reports"
            reports_dir.mkdir()
            for date, crawl, cover, video in [
                ("2026-06-21", 5, 4, 3),
                ("2026-06-22", 7, 5, 4),
                ("2026-06-23", 6, 5, 3),
            ]:
                write_json(
                    data_dir,
                    date,
                    "crawl_product_retention",
                    {
                        "per_product": [
                            {
                                "product": "PixVerse",
                                "kept_after_crawl_filter": crawl,
                                "kept_after_cover_filter": cover,
                            }
                        ]
                    },
                )
                write_json(
                    data_dir,
                    date,
                    "llm_video_content_filter",
                    {"summary": {"advertiser_counts": {"PixVerse": {"保留": video}}}},
                )
            write_json(
                data_dir,
                "2026-06-24",
                "crawl_product_retention",
                {
                    "per_product": [
                        {
                            "product": "PixVerse",
                            "kept_after_crawl_filter": 20,
                            "kept_after_cover_filter": 2,
                        }
                    ]
                },
            )
            write_json(
                data_dir,
                "2026-06-24",
                "llm_video_content_filter",
                {"summary": {"advertiser_counts": {"PixVerse": {"保留": 1}}}},
            )
            (reports_dir / "ve_filter_review_2026-06-24.html").write_text("<html></html>", encoding="utf-8")

            with patch.object(daily_nodes, "DATA_DIR", data_dir), patch.object(daily_nodes, "REPORTS_DIR", reports_dir):
                report = daily_nodes.build_core_stage_advertiser_report("2026-06-24")
                rendered = daily_nodes.render_core_stage_markdown(report)

        stages = {stage["id"]: stage for stage in report["stages"]}
        self.assertEqual(stages["crawl"]["advertisers"][0]["today"], 20)
        self.assertAlmostEqual(stages["crawl"]["advertisers"][0]["history_avg"], 6.0)
        self.assertEqual(stages["cover_dedupe"]["advertisers"][0]["today"], 2)
        self.assertEqual(stages["video_content_dedupe"]["advertisers"][0]["today"], 1)
        alert_keys = {(alert["stage_id"], alert["advertiser"]) for alert in report["alerts"]}
        self.assertIn(("crawl", "PixVerse"), alert_keys)
        self.assertIn(("cover_dedupe", "PixVerse"), alert_keys)
        self.assertIn(("video_content_dedupe", "PixVerse"), alert_keys)
        self.assertIn("## 三阶段广告主留存对比", rendered)
        self.assertIn("| 爬取 | PixVerse | 20 | 6.0 |", rendered)

    def test_node_report_marks_failed_nodes_and_renders_markdown(self) -> None:
        from ua_workflows.video_enhancer import daily_nodes

        with tempfile.TemporaryDirectory() as td:
            data_dir = Path(td)
            pre = "workflow_video_enhancer_2026-06-24"
            (data_dir / f"{pre}_raw.json").write_text(
                json.dumps(
                    {
                        "items": [{"creative": {"ad_key": "ad1"}}],
                        "filter_report": {"pre_truncation_total": 3, "post_truncation_total": 1},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            (data_dir / f"video_analysis_{pre}_raw.json").write_text(
                json.dumps(
                    {
                        "results": [{"ad_key": "ad1", "analysis": "[ERROR] timeout"}],
                        "new_success": 0,
                        "new_failed": 1,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with patch.object(daily_nodes, "DATA_DIR", data_dir):
                report = daily_nodes.build_daily_node_report("2026-06-24")
                rendered = daily_nodes.render_node_markdown(report)

        by_id = {node["id"]: node for node in report["nodes"]}
        self.assertEqual(by_id["crawl"]["status"], "ok")
        self.assertEqual(by_id["analysis"]["status"], "failed")
        self.assertIn("本轮新分析失败 1 条", by_id["analysis"]["message"])
        self.assertEqual(by_id["bitable_sync"]["status"], "failed")
        self.assertIn("## 每日流程节点运行情况", rendered)
        self.assertIn("| AI 灵感分析 | 失败 |", rendered)
        self.assertIn("| 多维表同步 | 失败 |", rendered)

    def test_acceptance_and_flow_report_include_node_summary(self) -> None:
        from ua_workflows.video_enhancer import acceptance, flow_report

        node_report = {
            "summary": {"failed": 1, "ok": 1, "skipped": 0, "pending": 0, "warn": 0},
            "nodes": [
                {
                    "id": "crawl",
                    "name": "广大大抓取与 raw 落盘",
                    "status": "ok",
                    "message": "raw 1 条",
                    "artifact": "/tmp/raw.json",
                    "restart": "重跑主流程",
                },
                {
                    "id": "bitable_sync",
                    "name": "多维表同步",
                    "status": "failed",
                    "message": "缺少 sync_report，无法确认写入",
                    "artifact": "/tmp/sync_report.json",
                    "restart": "重跑同步节点或主流程",
                },
            ],
        }
        stages = {"workflow_nodes": node_report}
        acceptance_md = acceptance._render_markdown(
            "2026-06-24",
            "fail",
            75,
            {"hard": 1, "soft": 0, "warn": 0},
            [{"severity": "hard", "code": "node_failed_bitable_sync", "message": "多维表同步失败"}],
            stages,
        )
        flow_md = flow_report.render_markdown(
            {
                "target_date": "2026-06-24",
                "status": "alert",
                "lookback_days": 5,
                "alerts": [],
                "totals": {},
                "per_product": [],
                "workflow_nodes": node_report,
            }
        )
        card = flow_report._build_feishu_card(
            {
                "target_date": "2026-06-24",
                "status": "alert",
                "alerts": [],
                "totals": {},
                "per_product": [],
                "workflow_nodes": node_report,
            }
        )
        card_text = json.dumps(card, ensure_ascii=False)

        self.assertIn("## 每日流程节点运行情况", acceptance_md)
        self.assertIn("| 多维表同步 | 失败 |", acceptance_md)
        self.assertIn("## 每日流程节点运行情况", flow_md)
        self.assertIn("| 多维表同步 | 失败 |", flow_md)
        self.assertIn("失败节点", card_text)
        self.assertIn("多维表同步", card_text)

    def test_flow_report_card_highlights_three_stage_alerts_with_dashboard(self) -> None:
        from ua_workflows.video_enhancer import flow_report

        stage_report = {
            "summary": {"alert_count": 1, "history_days": 7},
            "alerts": [
                {
                    "stage_id": "cover_dedupe",
                    "stage_name": "封面图去重",
                    "advertiser": "PixVerse",
                    "today": 2,
                    "history_avg": 8.0,
                    "ratio": 0.25,
                    "status": "偏低",
                    "dashboard": "/tmp/ve_filter_review_2026-06-24.html",
                }
            ],
            "stages": [
                {
                    "id": "cover_dedupe",
                    "name": "封面图去重",
                    "total_today": 2,
                    "total_history_avg": 8.0,
                    "alerts": [],
                    "advertisers": [
                        {
                            "advertiser": "PixVerse",
                            "today": 2,
                            "history_avg": 8.0,
                            "ratio": 0.25,
                            "status": "偏低",
                            "history_days": 3,
                        }
                    ],
                }
            ],
        }
        card = flow_report._build_feishu_card(
            {
                "target_date": "2026-06-24",
                "status": "alert",
                "alerts": [],
                "totals": {},
                "per_product": [],
                "core_stage_advertiser_report": stage_report,
            }
        )
        card_text = json.dumps(card, ensure_ascii=False)

        self.assertIn("VE 每日验收", card["card"]["header"]["title"]["content"])
        self.assertIn("三阶段广告主留存", card_text)
        self.assertIn("三阶段告警", card_text)
        self.assertIn("PixVerse", card_text)
        self.assertIn("ve_filter_review_2026-06-24.html", card_text)


if __name__ == "__main__":
    unittest.main()
