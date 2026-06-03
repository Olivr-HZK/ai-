from __future__ import annotations

import asyncio
import sys
import unittest
import urllib.request
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class GuangdadaFeishuCallbackTest(unittest.TestCase):
    def test_card_payload_contains_page_guide_image_and_confirm_button(self) -> None:
        from ua_workflows.shared.guangdada.interrupt_alerts import (
            build_security_verification_card_payload,
        )

        payload = build_security_verification_card_payload(
            page_url="https://www.guangdada.net/modules/creative/charts/new-charts",
            guide_image_path=Path("/tmp/guangdada_captcha_operation_guide.png"),
            confirm_url="http://127.0.0.1:39123/guangdada-human-check/confirm?token=abc",
            image_key="img_test_key",
            workflow="VE 新竞品测试",
            step="登录后安全验证",
            product="Dreamina AI: Image&Video Maker",
            target_date="2026-06-01",
            notify_all=True,
            login_account="ops@example.test",
            password_configured=True,
        )

        self.assertEqual(payload["msg_type"], "interactive")
        card = payload["card"]
        content = card["elements"][0]["content"]
        self.assertIn("<at id=all></at>", content)
        self.assertIn("广大大触发安全验证", content)
        self.assertIn("账号密码：请去飞书文档中查找", content)
        self.assertNotIn("ops@example.test", content)
        self.assertNotIn("项目 .env 已配置", content)
        self.assertNotIn("plain-password", content)
        self.assertNotIn("https://www.guangdada.net", content)
        self.assertNotIn("guangdada_captcha_operation_guide.png", content)
        self.assertNotIn("VE 新竞品测试", content)
        self.assertNotIn("登录后安全验证", content)
        self.assertNotIn("Dreamina AI: Image&Video Maker", content)
        self.assertNotIn("2026-06-01", content)
        self.assertEqual(card["elements"][1]["tag"], "img")
        actions = card["elements"][2]["actions"]
        self.assertEqual(actions[0]["text"]["content"], "打开广大大页面")
        self.assertEqual(actions[0]["multi_url"]["url"], "https://www.guangdada.net/modules/creative/charts/new-charts")
        self.assertEqual(actions[1]["text"]["content"], "已完成")
        self.assertEqual(actions[1]["multi_url"]["url"], "http://127.0.0.1:39123/guangdada-human-check/confirm?token=abc")

    def test_confirmation_waiter_receives_button_callback(self) -> None:
        from ua_workflows.shared.guangdada.interrupt_alerts import (
            start_feishu_confirmation_waiter,
        )

        async def scenario() -> None:
            waiter = await start_feishu_confirmation_waiter()
            try:
                wait_task = asyncio.create_task(waiter.wait(timeout_sec=3))
                body = await asyncio.to_thread(
                    lambda: urllib.request.urlopen(waiter.confirm_url, timeout=3).read().decode("utf-8")
                )
                self.assertIn("已收到", body)
                result = await wait_task
                self.assertTrue(result.confirmed)
                self.assertEqual(result.token, waiter.token)
            finally:
                await waiter.close()

        asyncio.run(scenario())

    def test_send_im_card_uses_project_app_message_api(self) -> None:
        from ua_workflows.shared.guangdada.interrupt_alerts import send_security_verification_im_card

        calls: list[dict[str, object]] = []

        class FakeResponse:
            status_code = 200

            def __init__(self, data: dict[str, object]) -> None:
                self._data = data
                self.text = str(data)

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, object]:
                return self._data

        def fake_post(url: str, **kwargs: object) -> FakeResponse:
            calls.append({"url": url, **kwargs})
            return FakeResponse({"code": 0, "data": {"message_id": "om_test"}})

        payload = {"card": {"elements": []}}
        with patch("ua_workflows.shared.guangdada.interrupt_alerts.requests.post", side_effect=fake_post):
            data = send_security_verification_im_card(
                receive_id="oc_test_chat",
                token="tenant_token",
                card_payload=payload,
            )

        self.assertEqual(data["code"], 0)
        self.assertEqual(calls[0]["url"], "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id")
        self.assertEqual(calls[0]["headers"]["Authorization"], "Bearer tenant_token")  # type: ignore[index]
        body = calls[0]["json"]  # type: ignore[index]
        self.assertEqual(body["receive_id"], "oc_test_chat")
        self.assertEqual(body["msg_type"], "interactive")
        self.assertIn("\"elements\": []", body["content"])

    def test_login_failure_checks_for_human_verification_before_failing(self) -> None:
        from ua_workflows.shared.guangdada.search import (
            GuangdadaHumanVerificationConfirmed,
            _login_or_handle_human_check,
        )

        async def fake_login(page: object, email: str, password: str) -> bool:
            self.assertEqual(page, "page")
            self.assertEqual(email, "email")
            self.assertEqual(password, "password")
            return False

        async def fake_dismiss(page: object) -> None:
            self.assertEqual(page, "page")
            raise GuangdadaHumanVerificationConfirmed("confirmed")

        async def scenario() -> None:
            with patch("ua_workflows.shared.guangdada.search.login", fake_login), patch(
                "ua_workflows.shared.guangdada.search._dismiss_login_security_modal_if_needed",
                fake_dismiss,
            ):
                with self.assertRaises(GuangdadaHumanVerificationConfirmed):
                    await _login_or_handle_human_check("page", "email", "password")

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
