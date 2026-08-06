"""Root test configuration.

Provides:
- Marker registration (strict_markers enforced via pyproject.toml)
- Network-denial autouse fixture for unit tests: any unmocked outbound
  network call from a ``unit``-marked test raises immediately so live
  dependencies cannot slip in silently.
- Shared low-level helpers used across test tiers.
"""

from __future__ import annotations

import socket
import sys
from pathlib import Path
from typing import Iterator
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "src"
for _p in (_ROOT, _SRC):
    _s = str(_p)
    if _s not in sys.path:
        sys.path.insert(0, _s)


# ---------------------------------------------------------------------------
# Network-denial guard for unit tests
# ---------------------------------------------------------------------------
# Any test decorated with @pytest.mark.unit that tries to open a real socket
# will hit this guard and fail immediately with a clear message.
# Tests that legitimately need the network carry a different marker.

def _guard_socket_connect(self, address):  # noqa: ANN001
    raise RuntimeError(
        f"Unit test attempted a real network connection to {address!r}. "
        "Mock all HTTP clients, database sessions, and Redis clients before "
        "exercising application code under @pytest.mark.unit."
    )


@pytest.fixture(autouse=True)
def _deny_network_in_unit_tests(request: pytest.FixtureRequest) -> Iterator[None]:
    """Block outbound network access inside unit-marked tests."""
    if request.node.get_closest_marker("unit"):
        with patch.object(socket.socket, "connect", _guard_socket_connect):
            yield
    else:
        yield
