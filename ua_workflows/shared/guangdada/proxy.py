"""
本地代理辅助：可选在运行爬虫前自动启动 macOS 上的 Clash 类客户端，并把环境变量里的代理地址转成 Playwright 可用的配置。

说明：
- 不能替代 VPN/节点：仍需本机安装 Clash / Clash Verge 等，并已有可用配置。
- Chromium 默认不读取 HTTP_PROXY，必须在 launch 时传入 proxy。
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from urllib.parse import urlparse


def _truthy(val: str | None) -> bool:
    return str(val or "").strip().lower() in ("1", "true", "yes", "on")


def _parse_host_port_from_server_url(server_url: str) -> tuple[str, int]:
    """从 Playwright 的 server URL 解析出用于 TCP 探测的 host/port。"""
    u = urlparse(server_url.strip())
    host = u.hostname or "127.0.0.1"
    if u.port is not None:
        return host, int(u.port)
    # 本地 Clash 混合代理常见未写端口，默认 7890
    if u.scheme in ("http", "https"):
        return host, 7890
    if u.scheme in ("socks5", "socks4"):
        return host, 1080
    return host, 7890


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def ensure_local_proxy_listener(
    host: str,
    port: int,
    *,
    auto_launch: bool,
    app_name: str | None,
    wait_seconds: float,
    interval: float = 0.5,
) -> bool:
    """
    若 host:port 已可连，返回 True。
    若 auto_launch 且为 macOS，则执行 `open -a <app_name>` 后轮询直至超时。
    """
    if _port_open(host, port):
        return True

    if not auto_launch:
        return False

    name = (app_name or "").strip()
    if not name:
        print(
            "[proxy] 已开启 PROXY_AUTO_LAUNCH_APP，但未设置 PROXY_APP_NAME（例如 Clash Verge）",
            file=sys.stderr,
        )
        return False

    if sys.platform != "darwin":
        print(
            "[proxy] 自动启动客户端目前仅支持 macOS（open -a）。"
            "请手动打开代理软件或自行配置 systemd/快捷方式。",
            file=sys.stderr,
        )
        return False

    print(f"[proxy] 未检测到 {host}:{port}，尝试启动应用: {name}")
    subprocess.run(["open", "-a", name], check=False)

    deadline = time.monotonic() + max(0.0, wait_seconds)
    while time.monotonic() < deadline:
        if _port_open(host, port):
            print(f"[proxy] 已检测到 {host}:{port} 可用")
            return True
        time.sleep(interval)

    print(
        f"[proxy] 等待 {wait_seconds:.0f}s 后仍无法连接 {host}:{port}，"
        "请检查客户端端口是否与 PLAYWRIGHT_PROXY_SERVER / HTTP_PROXY 一致。",
        file=sys.stderr,
    )
    return False


def proxy_server_url_from_env() -> str | None:
    """
    优先 PLAYWRIGHT_PROXY_SERVER，其次 HTTPS_PROXY、HTTP_PROXY。
    返回可用于 Playwright 的 server 字符串，如 http://127.0.0.1:7890
    """
    for key in ("PLAYWRIGHT_PROXY_SERVER", "HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY"):
        raw = (os.getenv(key) or "").strip()
        if raw:
            return raw
    return None


def playwright_proxy_config_from_env() -> dict | None:
    """
    将环境变量中的代理 URL 转为 Playwright launch 的 proxy 参数。
    支持 http(s):// 与 socks5://；若带用户名密码会一并传入。
    """
    raw = proxy_server_url_from_env()
    if not raw:
        return None

    u = urlparse(raw)
    if not u.scheme or not u.hostname:
        print(f"[proxy] 无法解析代理地址: {raw!r}", file=sys.stderr)
        return None

    if u.scheme in ("socks5", "socks5h", "socks4"):
        default_port = 1080
    elif u.scheme in ("http", "https"):
        default_port = 7890
    else:
        default_port = 80
    port = u.port if u.port is not None else default_port
    scheme = u.scheme
    if scheme == "socks5h":
        scheme = "socks5"
    server = f"{scheme}://{u.hostname}:{port}"

    cfg: dict = {"server": server}
    if u.username is not None:
        cfg["username"] = u.username
        cfg["password"] = u.password or ""
    return cfg


def prepare_playwright_proxy_for_crawl() -> dict | None:
    """
    在启动 Chromium 前调用：
    - 若环境变量未配置任何代理 URL，返回 None（直连）。
    - 若已配置，则根据 PROXY_AUTO_LAUNCH_APP 尝试启动 macOS 应用并等待端口；
      端口仍不可用时返回 None 并打印告警（由调用方决定是否退出）。
    """
    cfg = playwright_proxy_config_from_env()
    if not cfg:
        return None

    server = cfg.get("server") or ""
    host, port = _parse_host_port_from_server_url(server)

    check_host = (os.getenv("PROXY_CHECK_HOST") or host).strip() or host
    check_port = int(os.getenv("PROXY_CHECK_PORT") or port)
    wait_seconds = float(os.getenv("PROXY_WAIT_SECONDS") or "45")

    auto = _truthy(os.getenv("PROXY_AUTO_LAUNCH_APP"))
    app_name = os.getenv("PROXY_APP_NAME", "").strip() or None

    ok = ensure_local_proxy_listener(
        check_host,
        check_port,
        auto_launch=auto,
        app_name=app_name,
        wait_seconds=wait_seconds,
    )
    if not ok:
        if _truthy(os.getenv("PROXY_STRICT")):
            print("[proxy] 已设置 PROXY_STRICT=1，代理不可用则退出。", file=sys.stderr)
            sys.exit(3)
        print(
            "[proxy] 警告：代理端口未就绪，仍将尝试启动浏览器（可能连接失败）。",
            file=sys.stderr,
        )

    print(f"[proxy] Playwright 将使用代理: {server}")
    return cfg
