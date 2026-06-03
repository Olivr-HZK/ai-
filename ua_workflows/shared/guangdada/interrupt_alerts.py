from __future__ import annotations

import asyncio
import json
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests

MODULE_DIR = Path(__file__).resolve().parent


DEFAULT_GUIDE_IMAGE_PATH = MODULE_DIR / "assets" / "guangdada_captcha_operation_guide.png"


@dataclass(frozen=True)
class FeishuConfirmationResult:
    confirmed: bool
    token: str


class FeishuConfirmationWaiter:
    def __init__(self, *, server: asyncio.AbstractServer, host: str, port: int, path: str, token: str) -> None:
        self._server = server
        self.host = host
        self.port = port
        self.path = path
        self.token = token
        self._event = asyncio.Event()
        self._result: FeishuConfirmationResult | None = None

    @property
    def confirm_url(self) -> str:
        return f"http://{self.host}:{self.port}{self.path}?token={self.token}"

    async def wait(self, *, timeout_sec: int) -> FeishuConfirmationResult:
        try:
            await asyncio.wait_for(self._event.wait(), timeout=max(1, timeout_sec))
        except TimeoutError:
            return FeishuConfirmationResult(confirmed=False, token=self.token)
        return self._result or FeishuConfirmationResult(confirmed=False, token=self.token)

    async def close(self) -> None:
        self._server.close()
        await self._server.wait_closed()

    def _confirm(self, token: str) -> None:
        self._result = FeishuConfirmationResult(confirmed=True, token=token)
        self._event.set()


async def start_feishu_confirmation_waiter(
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    path: str = "/guangdada-human-check/confirm",
    token: str | None = None,
) -> FeishuConfirmationWaiter:
    token = token or secrets.token_urlsafe(18)
    waiter_ref: dict[str, FeishuConfirmationWaiter] = {}

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            raw = await reader.readuntil(b"\n")
        except Exception:
            writer.close()
            await writer.wait_closed()
            return
        line = raw.decode("utf-8", errors="ignore").strip()
        parts = line.split()
        target = parts[1] if len(parts) >= 2 else "/"
        parsed = urlparse(target)
        received_token = (parse_qs(parsed.query).get("token") or [""])[0]
        ok = parsed.path == path and received_token == token
        if ok:
            waiter_ref["waiter"]._confirm(received_token)
        body = (
            "<html><head><meta charset='utf-8'></head><body>"
            "<h2>已收到，可以返回飞书。</h2>"
            "<p>系统会继续后续爬取。</p>"
            "</body></html>"
            if ok
            else "<html><head><meta charset='utf-8'></head><body><h2>确认链接无效</h2></body></html>"
        )
        status = "200 OK" if ok else "403 Forbidden"
        payload = body.encode("utf-8")
        writer.write(
            (
                f"HTTP/1.1 {status}\r\n"
                "Content-Type: text/html; charset=utf-8\r\n"
                f"Content-Length: {len(payload)}\r\n"
                "Connection: close\r\n"
                "\r\n"
            ).encode("utf-8")
            + payload
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle, host=host, port=port)
    sockets = server.sockets or []
    bound_port = int(sockets[0].getsockname()[1]) if sockets else port
    waiter = FeishuConfirmationWaiter(server=server, host=host, port=bound_port, path=path, token=token)
    waiter_ref["waiter"] = waiter
    return waiter


def build_security_verification_card_payload(
    *,
    page_url: str,
    guide_image_path: Path,
    confirm_url: str,
    image_key: str = "",
    workflow: str = "",
    step: str = "",
    product: str = "",
    target_date: str = "",
    notify_all: bool = False,
    login_account: str = "",
    password_configured: bool = False,
) -> dict[str, Any]:
    lines = [
        "<at id=all></at>" if notify_all else "",
        "**广大大触发安全验证，需要人工处理**",
        "",
        "请按帮助图完成页面滑块验证。完成后回到本卡片点击「已完成」，系统会继续后续爬取。",
        "- 账号密码：请去飞书文档中查找",
    ]

    elements: list[dict[str, Any]] = [{"tag": "markdown", "content": "\n".join(line for line in lines if line)[:12000]}]
    if image_key:
        elements.append(
            {
                "tag": "img",
                "img_key": image_key,
                "alt": {"tag": "plain_text", "content": "广大大安全验证操作指示图"},
            }
        )

    actions: list[dict[str, Any]] = []
    if page_url:
        actions.append(
            {
                "tag": "button",
                "text": {"tag": "plain_text", "content": "打开广大大页面"},
                "type": "default",
                "multi_url": {
                    "url": page_url,
                    "pc_url": page_url,
                    "ios_url": page_url,
                    "android_url": page_url,
                },
            }
        )
    actions.append(
        {
            "tag": "button",
            "text": {"tag": "plain_text", "content": "已完成"},
            "type": "primary",
            "multi_url": {
                "url": confirm_url,
                "pc_url": confirm_url,
                "ios_url": confirm_url,
                "android_url": confirm_url,
            },
        }
    )
    elements.append({"tag": "action", "actions": actions})

    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True, "enable_forward": True},
            "header": {
                "title": {"tag": "plain_text", "content": "广大大安全验证需要人工处理"},
                "template": "orange",
            },
            "elements": elements,
        },
    }


def send_security_verification_im_card(
    *,
    receive_id: str,
    token: str,
    card_payload: dict[str, Any],
    receive_id_type: str = "chat_id",
) -> dict[str, Any]:
    url = f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={receive_id_type}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    card = card_payload.get("card") if isinstance(card_payload, dict) else {}
    response = requests.post(
        url,
        headers=headers,
        json={
            "receive_id": receive_id,
            "msg_type": "interactive",
            "content": json.dumps(card or {}, ensure_ascii=False),
        },
        timeout=20,
    )
    response.raise_for_status()
    data = response.json()
    if data.get("code") != 0:
        raise RuntimeError(f"Feishu IM push failed: {data}")
    return data
