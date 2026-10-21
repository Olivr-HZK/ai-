from __future__ import annotations

import sys
import unittest
from contextlib import redirect_stdout
from datetime import date
from io import StringIO
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class NewCompetitorColdStartTest(unittest.TestCase):
    def test_default_four_day_window_plans_three_shallow_days_and_one_full_day(self) -> None:
        from ua_workflows.video_enhancer.new_competitor_cold_start import build_cold_start_plan

        plan = build_cold_start_plan(
            products="Dreamina AI: Image&Video Maker,Kling AI: AI Image&Video Maker",
            run_date=date(2026, 6, 5),
            history_days=4,
            python_bin=".venv/bin/python",
        )

        self.assertEqual(plan["run_date"], "2026-06-05")
        self.assertEqual([step["date"] for step in plan["steps"]], ["2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04"])
        self.assertEqual([step["mode"] for step in plan["steps"]], ["shallow", "shallow", "shallow", "full"])

        shallow_command = plan["steps"][0]["command"]
        self.assertIn("scripts/run_ve_new_competitor_cold_start.py", shallow_command)
        self.assertIn("--shallow-date", shallow_command)
        self.assertNotIn("scripts/run_video_enhancer.py", shallow_command)

        full_command = plan["steps"][-1]["command"]
        self.assertIn("scripts/run_video_enhancer.py", full_command)
        self.assertIn("--date", full_command)
        self.assertIn("2026-06-04", full_command)
        self.assertIn("--products", full_command)

    def test_plan_only_does_not_execute_commands(self) -> None:
        from ua_workflows.video_enhancer.new_competitor_cold_start import build_cold_start_plan, execute_plan

        calls: list[list[str]] = []
        plan = build_cold_start_plan(
            products="Dreamina AI: Image&Video Maker",
            run_date=date(2026, 6, 5),
            history_days=2,
            python_bin=".venv/bin/python",
        )

        with redirect_stdout(StringIO()):
            execute_plan(plan, execute=False, runner=lambda command: calls.append(command))

        self.assertEqual(calls, [])

    def test_execute_runs_shallow_callable_for_shallow_steps_and_runner_for_full_step(self) -> None:
        from ua_workflows.video_enhancer.new_competitor_cold_start import build_cold_start_plan, execute_plan

        calls: list[tuple[str, str]] = []
        full_commands: list[list[str]] = []
        plan = build_cold_start_plan(
            products="Dreamina AI: Image&Video Maker",
            run_date=date(2026, 6, 5),
            history_days=2,
            python_bin=".venv/bin/python",
        )

        with redirect_stdout(StringIO()):
            execute_plan(
                plan,
                execute=True,
                runner=lambda command: full_commands.append(command),
                shallow_runner=lambda target_date, products: calls.append((target_date, products)),
            )

        self.assertEqual(calls, [("2026-06-03", "Dreamina AI: Image&Video Maker")])
        self.assertEqual(len(full_commands), 1)
        self.assertIn("scripts/run_video_enhancer.py", full_commands[0])
        self.assertIn("2026-06-04", full_commands[0])


if __name__ == "__main__":
    unittest.main()
