"""Reader for public HTTP APIs.

Built on `urllib` so the package keeps no runtime dependencies. Endpoints requiring
authentication are not supported.
"""

from __future__ import annotations

import json
import urllib.request
from typing import Any
from urllib.parse import urlparse

__all__ = ["JsonApiReader"]

_ALLOWED_SCHEMES = frozenset({"http", "https"})


class JsonApiReader:
    """Fetches a JSON endpoint and returns the decoded payload.

    Args:
        url: Endpoint to read from. Only `http` and `https` are accepted.
        timeout: Seconds to wait before giving up.
        headers: Extra request headers. `Accept: application/json` is sent by default.
        encoding: Fallback used when the response declares no charset.
    """

    def __init__(
        self,
        url: str,
        *,
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
        encoding: str = "utf-8",
    ) -> None:
        scheme = urlparse(url).scheme.lower()
        if scheme not in _ALLOWED_SCHEMES:
            raise ValueError(
                f"unsupported URL scheme {scheme!r}: expected one of {sorted(_ALLOWED_SCHEMES)}"
            )
        self.url = url
        self.timeout = timeout
        self.headers = {"Accept": "application/json", **(headers or {})}
        self.encoding = encoding

    def read(self) -> Any:
        """Perform the request and decode the JSON body."""
        request = urllib.request.Request(self.url, headers=self.headers)
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            charset = response.headers.get_content_charset() or self.encoding
            return json.loads(response.read().decode(charset))

    def __repr__(self) -> str:
        return f"{type(self).__name__}(url={self.url!r})"
