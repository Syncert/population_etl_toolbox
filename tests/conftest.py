"""Shared collection isolation and deterministic unit-test boundaries."""

from __future__ import annotations

import os
import socket
import json
import tempfile
from pathlib import Path
from typing import Iterator

import pytest

_FIXTURES = Path(__file__).parent / "fixtures"

if os.environ.get("RUN_INTEGRATION_TESTS") == "1":
    os.environ.setdefault(
        "AIRFLOW_HOME",
        str(Path(tempfile.gettempdir()) / "population-etl-airflow-integration"),
    )
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")


@pytest.fixture
def source_fixture():
    """Load a small reviewed JSON fixture by source and filename."""

    def _load(source: str, filename: str):
        path = _FIXTURES / source / filename
        return json.loads(path.read_text(encoding="utf-8"))

    return _load


# Marker deselection happens after imports. Ignore incompatible tiers before
# collection unless their dedicated runner explicitly enables them.
collect_ignore: list[str] = []
for _directory, _flag in {
    "dags": "RUN_DAG_TESTS",
    "integration": "RUN_INTEGRATION_TESTS",
    "resilience": "RUN_INTEGRATION_TESTS",
    "external": "RUN_EXTERNAL_TESTS",
    "e2e": "RUN_E2E_TESTS",
    "performance": "RUN_PERFORMANCE_TESTS",
}.items():
    if os.environ.get(_flag) != "1":
        collect_ignore.append(_directory)


def _network_error(target: object) -> RuntimeError:
    return RuntimeError(
        f"Unit test attempted a real network connection to {target!r}. "
        "Mock the HTTP, database, or Redis boundary before exercising the code."
    )


def _guard_socket_connect(real_connect, sock, address):  # noqa: ANN001
    """Block external sockets while allowing Windows' private socketpair."""
    host = address[0] if isinstance(address, tuple) and address else None
    if host in {"127.0.0.1", "::1"}:
        return real_connect(sock, address)
    raise _network_error(address)


def _guard_requests_send(self, request, **kwargs):  # noqa: ANN001, ARG001
    raise _network_error(getattr(request, "url", request))


def _guard_httpx_send(real_send, client, request, *args, **kwargs):  # noqa: ANN001
    # Starlette TestClient uses an in-process ASGI transport and this reserved
    # host. It performs no outbound network I/O.
    if request.url.host == "testserver":
        return real_send(client, request, *args, **kwargs)
    raise _network_error(str(request.url))


async def _guard_httpx_async_send(
    real_send,
    client,
    request,
    *args,
    **kwargs,  # noqa: ANN001
):
    if request.url.host == "testserver":
        return await real_send(client, request, *args, **kwargs)
    raise _network_error(str(request.url))


def _guard_database_connect(*args, **kwargs):  # noqa: ANN002, ARG001
    raise _network_error("PostgreSQL")


async def _guard_redis_command(*args, **kwargs):  # noqa: ANN002, ARG001
    raise _network_error("Redis")


@pytest.fixture(autouse=True)
def _deny_network_in_unit_tests(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> Iterator[None]:
    """Block real source, PostgreSQL, and Redis boundaries in unit tests."""
    if not request.node.get_closest_marker("unit"):
        yield
        return

    import httpx
    import psycopg2
    import requests

    real_connect = socket.socket.connect
    real_httpx_send = httpx.Client.send
    real_httpx_async_send = httpx.AsyncClient.send

    monkeypatch.setattr(
        socket.socket,
        "connect",
        lambda sock, address: _guard_socket_connect(real_connect, sock, address),
    )
    monkeypatch.setattr(requests.Session, "send", _guard_requests_send)
    monkeypatch.setattr(
        httpx.Client,
        "send",
        lambda client, http_request, *args, **kwargs: _guard_httpx_send(
            real_httpx_send, client, http_request, *args, **kwargs
        ),
    )

    async def guarded_async_send(client, http_request, *args, **kwargs):  # noqa: ANN001
        return await _guard_httpx_async_send(
            real_httpx_async_send, client, http_request, *args, **kwargs
        )

    monkeypatch.setattr(httpx.AsyncClient, "send", guarded_async_send)
    monkeypatch.setattr(psycopg2, "connect", _guard_database_connect)

    try:
        from redis.asyncio import Redis
    except ModuleNotFoundError:
        pass
    else:
        monkeypatch.setattr(Redis, "execute_command", _guard_redis_command)

    yield
