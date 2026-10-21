import json, os, sys, unittest, subprocess
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from codex_intake import extract, DISABLED
from core import Problem


class CodexIntakeTests(unittest.TestCase):
    def run_fake(self, payload, events=None, returncode=0):
        captured = {}

        class Proc:
            pid = 12345

            def __init__(self, args, **kwargs):
                captured.update(args=args, options=kwargs)
                self.returncode = returncode

            def communicate(self, content, timeout):
                captured["input"] = content
                captured["timeout"] = timeout
                args = captured["args"]
                Path(args[args.index("--output-last-message") + 1]).write_text(
                    '{"ok":true}'
                )
                root = Path(captured["options"]["cwd"])
                (root / "events.jsonl").write_text(
                    "\n".join(
                        json.dumps(e) for e in (events or [{"type": "turn.completed"}])
                    )
                )

        with (
            patch("codex_intake.shutil.which", return_value="/safe/codex"),
            patch("codex_intake.subprocess.Popen", Proc),
            patch.dict(
                os.environ,
                {
                    "OPENAI_API_KEY": "must-not-pass",
                    "FEISHU_APP_SECRET": "must-not-pass",
                },
            ),
        ):
            value = extract({"type": "object"}, "extract only", payload)
        return value, captured

    def test_fixed_model_effort_and_stdin_no_shell_or_project_secrets(self):
        text = "Tipsy; $(touch /tmp/never) --model evil"
        value, c = self.run_fake({"request_text": text})
        a = c["args"]
        self.assertEqual(value, {"ok": True})
        self.assertEqual(a[a.index("--model") + 1], "gpt-6-luna")
        self.assertIn('model_reasoning_effort="medium"', a)
        self.assertIn("--ignore-user-config", a)
        self.assertIn("--ephemeral", a)
        self.assertEqual(a[a.index("--sandbox") + 1], "read-only")
        self.assertNotIn(text, a)
        self.assertEqual(json.loads(c["input"])["request_text"], text)
        for feature in DISABLED:
            self.assertIn(feature, a)
        self.assertNotIn("OPENAI_API_KEY", c["options"]["env"])
        self.assertNotIn("FEISHU_APP_SECRET", c["options"]["env"])
        self.assertNotIn("shell", c["options"])
        self.assertTrue(c["options"]["start_new_session"])

    def test_nonzero_exit_is_not_exposed_or_accepted(self):
        with self.assertRaises(Problem) as ctx:
            self.run_fake({}, returncode=1)
        self.assertEqual(ctx.exception.status, 503)

    def test_unexpected_tool_events_are_rejected(self):
        with self.assertRaises(Problem):
            self.run_fake(
                {},
                events=[
                    {"type": "item.completed", "item": {"type": "command_execution"}},
                    {"type": "turn.completed"},
                ],
            )

    def test_timeout_kills_process_group_and_is_not_accepted(self):
        class Proc:
            pid = 12345
            returncode = None
            calls = 0

            def communicate(self, *args, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    raise subprocess.TimeoutExpired("codex", 120)
                return None, None

        with (
            patch("codex_intake.shutil.which", return_value="/safe/codex"),
            patch("codex_intake.subprocess.Popen", return_value=Proc()),
            patch("codex_intake.os.killpg") as kill,
        ):
            with self.assertRaises(Problem) as ctx:
                extract({}, "extract", {})
        self.assertEqual(ctx.exception.status, 504)
        kill.assert_called_once()

    def test_missing_turn_completion_is_rejected(self):
        with self.assertRaises(Problem):
            self.run_fake(
                {},
                events=[
                    {
                        "type": "item.completed",
                        "item": {"type": "agent_message", "text": "{}"},
                    }
                ],
            )


if __name__ == "__main__":
    unittest.main()
