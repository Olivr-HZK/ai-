from __future__ import annotations

import sys
import unittest
from unittest import mock
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _item(
    *,
    ad_key: str,
    chart_type: str,
    chart_name: str,
    rank: int,
    title: str,
    advertiser: str = "Advertiser",
    video_url: str | None = None,
    top_label: str = "",
) -> dict:
    return {
        "category": "ai_tools_charts",
        "product": "AI工具/AI视频生成",
        "creative": {
            "ad_key": ad_key,
            "title": title,
            "advertiser_name": advertiser,
            "video_url": video_url or f"https://example.test/{ad_key}.mp4",
            "preview_img_url": f"https://example.test/{ad_key}.jpg",
            "market_chart_type": chart_type,
            "market_chart_name": chart_name,
            "market_chart_rank": rank,
            "top_label": top_label,
            "impression": 1234,
            "all_exposure_value": 5678,
            "heat": 99,
        },
    }


class MarketWeeklyChartsPushTest(unittest.TestCase):
    def test_merge_chart_items_keeps_multi_chart_hits(self) -> None:
        from ua_workflows.video_enhancer.market_weekly_charts_push import merge_chart_items

        rows = merge_chart_items(
            [
                _item(ad_key="a1", chart_type="new", chart_name="新创意榜", rank=4, title="A"),
                _item(ad_key="a1", chart_type="surge", chart_name="每周飙升榜", rank=8, title="A2"),
                _item(ad_key="b1", chart_type="hot", chart_name="每周热门榜", rank=2, title="B", top_label="Top1%"),
            ]
        )

        self.assertEqual(len(rows), 2)
        first = next(row for row in rows if row["ad_key"] == "a1")
        self.assertEqual(
            [(hit["chart_type"], hit["rank"]) for hit in first["chart_hits"]],
            [("new", 4), ("surge", 8)],
        )
        self.assertEqual(first["title"], "A")

    def test_rank_focus_rows_prioritizes_multi_chart_and_top1(self) -> None:
        from ua_workflows.video_enhancer.market_weekly_charts_push import (
            merge_chart_items,
            rank_focus_rows,
        )

        rows = merge_chart_items(
            [
                _item(ad_key="multi", chart_type="new", chart_name="新创意榜", rank=9, title="multi"),
                _item(ad_key="multi", chart_type="surge", chart_name="每周飙升榜", rank=5, title="multi"),
                _item(ad_key="top1", chart_type="hot", chart_name="每周热门榜", rank=1, title="top1", top_label="Top1%"),
                _item(ad_key="plain", chart_type="new", chart_name="新创意榜", rank=1, title="plain"),
            ]
        )

        ordered = rank_focus_rows(rows)

        self.assertEqual([row["ad_key"] for row in ordered], ["multi", "top1", "plain"])

    def test_render_markdown_uses_facts_without_ai_sections(self) -> None:
        from ua_workflows.video_enhancer.market_weekly_charts_push import (
            merge_chart_items,
            render_markdown,
        )

        rows = merge_chart_items(
            [
                _item(ad_key="a1", chart_type="new", chart_name="新创意榜", rank=4, title="A"),
                _item(ad_key="a1", chart_type="surge", chart_name="每周飙升榜", rank=8, title="A2"),
                _item(ad_key="b1", chart_type="hot", chart_name="每周热门榜", rank=2, title="B", top_label="Top1%"),
            ]
        )
        md = render_markdown(
            rows,
            week_start="2026-06-08",
            week_end="2026-06-14",
            source_counts={"new": 1, "hot": 1, "surge": 1},
            focus_top_n=5,
            per_chart_top_n=3,
        )

        self.assertIn("VE 大盘周榜｜2026-06-08 ~ 2026-06-14", md)
        self.assertIn("新创意榜 1 条｜热门榜 1 条｜飙升榜 1 条", md)
        self.assertIn("排名：新创意榜 #4 / 每周飙升榜 #8", md)
        self.assertIn("展示估值：5678", md)
        self.assertIn("人气：1234", md)
        self.assertIn("热度：99", md)
        self.assertNotIn("产品：", md)
        self.assertIn("Top标签：Top1%", md)
        self.assertNotIn("Hook", md)
        self.assertNotIn("复刻价值", md)

    def test_build_chart_card_uses_one_collapsible_table_per_chart(self) -> None:
        from ua_workflows.video_enhancer.market_weekly_charts_push import (
            build_chart_card_payload,
            merge_chart_items,
        )

        rows = merge_chart_items(
            [
                _item(ad_key="new2", chart_type="new", chart_name="新创意榜", rank=2, title="New 2"),
                _item(ad_key="new1", chart_type="new", chart_name="新创意榜", rank=1, title="New 1"),
                _item(ad_key="hot1", chart_type="hot", chart_name="每周热门榜", rank=1, title="Hot 1"),
                _item(ad_key="surge1", chart_type="surge", chart_name="每周飙升榜", rank=1, title="Surge 1"),
            ]
        )

        payload = build_chart_card_payload(
            "VE 大盘周榜",
            rows,
            week_start="2026-06-08",
            week_end="2026-06-14",
            source_counts={"new": 2, "hot": 1, "surge": 1},
            per_chart_top_n=10,
        )

        card = payload["card"]
        self.assertEqual(card["schema"], "2.0")
        elements = card["body"]["elements"]
        panels = [element for element in elements if element.get("tag") == "collapsible_panel"]
        self.assertEqual(len(panels), 4)
        chart_panels = panels[1:]
        self.assertEqual(
            [panel["header"]["title"]["content"] for panel in chart_panels],
            ["新创意榜 Top10 · 2 条", "热门榜 Top10 · 1 条", "飙升榜 Top10 · 1 条"],
        )
        for panel in chart_panels:
            self.assertEqual(panel["elements"][0]["tag"], "markdown")
            self.assertIn(
                "| 排名 | 素材 | 广告主 | 展示估值 | 人气 | 热度 | 榜单排名 |",
                panel["elements"][0]["content"],
            )
            self.assertNotIn("| 产品 |", panel["elements"][0]["content"])
            self.assertNotIn("| 指标 |", panel["elements"][0]["content"])
            self.assertNotIn("| 命中 |", panel["elements"][0]["content"])
        new_table = chart_panels[0]["elements"][0]["content"]
        self.assertLess(new_table.index("| #1 | [New 1]"), new_table.index("| #2 | [New 2]"))
        self.assertIn("| #1 | [New 1](https://example.test/new1.mp4) | Advertiser | 5678 | 1234 | 99 | 新创意榜 #1 |", new_table)

    def test_build_chart_card_restores_focus_panel_before_chart_panels(self) -> None:
        from ua_workflows.video_enhancer.market_weekly_charts_push import (
            build_chart_card_payload,
            merge_chart_items,
        )

        rows = merge_chart_items(
            [
                _item(ad_key="multi", chart_type="new", chart_name="新创意榜", rank=9, title="Multi"),
                _item(ad_key="multi", chart_type="surge", chart_name="每周飙升榜", rank=5, title="Multi"),
                _item(ad_key="top1", chart_type="hot", chart_name="每周热门榜", rank=1, title="Top 1", top_label="Top1%"),
                _item(ad_key="plain", chart_type="new", chart_name="新创意榜", rank=1, title="Plain"),
            ]
        )

        payload = build_chart_card_payload(
            "VE 大盘周榜",
            rows,
            week_start="2026-06-08",
            week_end="2026-06-14",
            source_counts={"new": 3, "hot": 1, "surge": 1},
            per_chart_top_n=10,
            focus_top_n=2,
        )

        panels = [element for element in payload["card"]["body"]["elements"] if element.get("tag") == "collapsible_panel"]
        self.assertEqual([panel["header"]["title"]["content"] for panel in panels], ["重点关注 Top2 · 2 条", "新创意榜 Top10 · 2 条", "热门榜 Top10 · 1 条", "飙升榜 Top10 · 1 条"])
        self.assertIs(panels[0]["expanded"], True)
        self.assertTrue(all(panel["expanded"] is False for panel in panels[1:]))
        focus_table = panels[0]["elements"][0]["content"]
        self.assertLess(focus_table.index("[Multi]"), focus_table.index("[Top 1]"))

    def test_resolve_webhook_prefers_bot_before_ua_fallback(self) -> None:
        from ua_workflows.video_enhancer.market_weekly_charts_push import resolve_webhook

        with mock.patch.dict(
            "os.environ",
            {
                "VE_MARKET_WEEKLY_CHARTS_FEISHU_WEBHOOK": "",
                "VE_FLOW_REPORT_FEISHU_WEBHOOK": "",
                "FEISHU_BOT_WEBHOOK": "https://bot.example.test",
                "FEISHU_UA_WEBHOOK": "https://ua.example.test",
            },
            clear=False,
        ):
            self.assertEqual(resolve_webhook(), "https://bot.example.test")


if __name__ == "__main__":
    unittest.main()
