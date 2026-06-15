from __future__ import annotations

import unittest
from types import SimpleNamespace


class LlmClientTimeoutTests(unittest.TestCase):
    def test_call_text_forwards_timeout_to_chat_completion(self) -> None:
        from ua_workflows.shared.llm import client

        calls: list[dict] = []

        class FakeCompletions:
            def create(self, **kwargs):
                calls.append(kwargs)
                return SimpleNamespace(
                    choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))],
                    usage=None,
                )

        class FakeOpenAI:
            def __init__(self, **_kwargs):
                self.chat = SimpleNamespace(completions=FakeCompletions())

        original_openai = client.OpenAI
        original_or_key = client._or_key
        original_oa_key = client._oa_key
        original_accumulate = client._accumulate
        try:
            client.OpenAI = FakeOpenAI
            client._or_key = lambda: "openrouter-key"
            client._oa_key = lambda: ""
            client._accumulate = lambda *_args, **_kwargs: None

            result = client.call_text("system", "user", models=["test/model"], timeout=12.5)

            self.assertEqual(result, "ok")
            self.assertEqual(calls[0]["timeout"], 12.5)
        finally:
            client.OpenAI = original_openai
            client._or_key = original_or_key
            client._oa_key = original_oa_key
            client._accumulate = original_accumulate


if __name__ == "__main__":
    unittest.main()
