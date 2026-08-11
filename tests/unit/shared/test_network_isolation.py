"""Default unit collection cannot silently contact external services."""

import httpx
import pytest

pytestmark = pytest.mark.unit


def test_unmocked_http_request_is_denied() -> None:
    """Covers: ENV-003 — unmocked outbound requests fail before network I/O."""
    with pytest.raises(RuntimeError, match="real network connection"):
        httpx.get("https://example.invalid")
