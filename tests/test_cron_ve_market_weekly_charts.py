from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "cron_ve_market_weekly_charts.sh"


class CronVeMarketWeeklyChartsTest(unittest.TestCase):
    def test_weekly_market_cron_crawls_three_charts_then_pushes_once(self) -> None:
        text = SCRIPT.read_text(encoding="utf-8")

        self.assertIn("VE_MARKET_WEEKLY_CHARTS_WEEK_END", text)
        self.assertIn("timedelta(days=6)", text)
        self.assertIn('CHART_RAW_PATHS=()', text)
        self.assertIn('for chart_type in new hot surge; do', text)
        self.assertIn('--chart-type "$chart_type"', text)
        self.assertIn('guangdada_market_weekly_${chart_type}_${WEEK_START}_${WEEK_END}', text)
        self.assertIn('scripts/run_ve_market_weekly_charts_push.py', text)
        self.assertIn('--week-start "$WEEK_START"', text)
        self.assertIn('--week-end "$WEEK_END"', text)
        self.assertIn('VE_MARKET_WEEKLY_CHARTS_DRY_RUN', text)


if __name__ == "__main__":
    unittest.main()
