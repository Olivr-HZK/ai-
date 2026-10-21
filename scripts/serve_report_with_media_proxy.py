#!/usr/bin/env python3
"""Serve local report files and proxy known media URLs for browser previews."""

from __future__ import annotations

import argparse
import http.server
import shutil
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


ALLOWED_MEDIA_HOSTS = {"sp2cdn-idea-global.zingfront.com"}


class ReportHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - stdlib method name
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/__media_proxy":
            self._proxy_media(parsed)
            return
        super().do_GET()

    def do_HEAD(self) -> None:  # noqa: N802 - stdlib method name
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/__media_proxy":
            self._proxy_media(parsed, head_only=True)
            return
        super().do_HEAD()

    def _proxy_media(
        self,
        parsed: urllib.parse.ParseResult,
        *,
        head_only: bool = False,
    ) -> None:
        query = urllib.parse.parse_qs(parsed.query)
        target = (query.get("u") or [""])[0]
        target_parsed = urllib.parse.urlparse(target)
        if (
            target_parsed.scheme not in {"http", "https"}
            or target_parsed.netloc not in ALLOWED_MEDIA_HOSTS
        ):
            self.send_error(403, "media host is not allowed")
            return

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": self.headers.get("Accept", "*/*"),
        }
        if self.headers.get("Range"):
            headers["Range"] = self.headers["Range"]

        request = urllib.request.Request(
            target,
            headers=headers,
            method="HEAD" if head_only else "GET",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                self.send_response(response.status)
                for key in (
                    "Content-Type",
                    "Content-Length",
                    "Content-Range",
                    "Accept-Ranges",
                    "Last-Modified",
                    "ETag",
                ):
                    value = response.headers.get(key)
                    if value:
                        self.send_header(key, value)
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                if not head_only:
                    shutil.copyfileobj(response, self.wfile)
        except urllib.error.HTTPError as exc:
            self.send_error(exc.code, exc.reason)
        except Exception as exc:  # pragma: no cover - runtime diagnostic path
            self.send_error(502, str(exc))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--directory", default="reports")
    args = parser.parse_args()

    directory = Path(args.directory).resolve()
    handler = lambda *handler_args, **handler_kwargs: ReportHandler(
        *handler_args,
        directory=str(directory),
        **handler_kwargs,
    )
    server = http.server.ThreadingHTTPServer((args.host, args.port), handler)
    print(f"Serving {directory} with media proxy at http://{args.host}:{args.port}/")
    server.serve_forever()


if __name__ == "__main__":
    main()
