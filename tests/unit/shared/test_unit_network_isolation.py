"""Unit tests cannot reach the integration tiers' services on loopback."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
from pathlib import Path
from typing import Iterator

import pytest

from tests.conftest import _guard_socket_connect, integration_service_ports

pytestmark = pytest.mark.unit

REPOSITORY = Path(__file__).resolve().parents[3]


@pytest.fixture
def listener() -> Iterator[socket.socket]:
    """A loopback service that records whether anything connected to it."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen()
    server.setblocking(False)
    yield server
    server.close()


def _accepted_a_connection(server: socket.socket) -> bool:
    try:
        connection, _address = server.accept()
    except BlockingIOError:
        return False
    connection.close()
    return True


def _refuse_to_connect(sock, address):  # noqa: ANN001, ARG001
    pytest.fail(f"the guard let a unit test connect to {address!r}")


def test_the_configured_service_ports_are_the_integration_tiers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ENV-003 — the guard knows which loopback ports are services."""
    monkeypatch.setenv("TEST_POSTGRES_PORT", "5432")
    monkeypatch.setenv("TEST_REDIS_URL", "redis://127.0.0.1:6380/15")
    assert integration_service_ports() == {5432, 6380}

    monkeypatch.setenv("TEST_REDIS_URL", "redis://127.0.0.1/15")
    assert integration_service_ports() == {5432, 6379}

    monkeypatch.setenv("TEST_POSTGRES_PORT", "not-a-port")
    monkeypatch.delenv("TEST_REDIS_URL")
    assert integration_service_ports() == frozenset()


@pytest.mark.parametrize("host", ["127.0.0.1", "::1"])
def test_a_unit_test_cannot_connect_to_a_configured_service(host: str) -> None:
    """Covers: ENV-003 — loopback stays open, except to the tiers' services."""
    with pytest.raises(RuntimeError, match="attempted a real network connection"):
        _guard_socket_connect(
            _refuse_to_connect, None, (host, 55432), frozenset({55432})
        )

    opened: list[tuple] = []
    _guard_socket_connect(
        lambda sock, address: opened.append(address),
        None,
        (host, 50001),
        frozenset({55432}),
    )
    assert opened == [(host, 50001)]


def test_the_suite_opens_no_connection_when_the_services_are_configured(
    listener: socket.socket,
) -> None:
    """Covers: ENV-003 — the unit files that read the integration variables
    run against a live loopback listener named by them and never connect.

    The coverage job exports these variables for its whole run, with
    PostgreSQL and Redis listening, so a unit fixture that read them and
    connected would wait on a real service there (PERF-001).
    """
    port = listener.getsockname()[1]
    environment = {
        **os.environ,
        "RUN_INTEGRATION_TESTS": "1",
        "TEST_POSTGRES_HOST": "127.0.0.1",
        "TEST_POSTGRES_PORT": str(port),
        "TEST_POSTGRES_USER": "population_test",
        "TEST_POSTGRES_PASSWORD": "population_test",
        "TEST_POSTGRES_DATABASE": "population_etl_test",
        "TEST_REDIS_URL": f"redis://127.0.0.1:{port}/15",
    }
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/unit/shared/test_redis_test_config.py",
            "-q",
            "-p",
            "no:cacheprovider",
        ],
        cwd=REPOSITORY,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert not _accepted_a_connection(listener)
