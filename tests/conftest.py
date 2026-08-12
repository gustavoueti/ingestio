"""Shared fixtures."""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import pytest

PAYLOAD: list[dict[str, Any]] = [
    {"id": 1, "name": "ada"},
    {"id": 2, "name": "grace"},
]


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/boom":
            self.send_error(500, "boom")
            return
        body = json.dumps(PAYLOAD).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        """Silence the default stderr logging."""


@pytest.fixture
def payload() -> list[dict[str, Any]]:
    """The records served by the `api_url` fixture."""
    return [dict(record) for record in PAYLOAD]


@pytest.fixture
def api_url() -> Iterator[str]:
    """A local HTTP server serving `PAYLOAD`."""
    server = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = int(server.server_address[1])
    try:
        yield f"http://127.0.0.1:{port}/records"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
