"""Operational error redaction contracts shared by DAG ledger boundaries."""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.normalization import sanitize_error_message

pytestmark = pytest.mark.unit


def test_error_sanitizer_removes_connection_urls_and_named_secrets() -> None:
    """Covers: DAG-014, RES-002 — runtime failures retain no credential values."""
    message = sanitize_error_message(
        "postgresql://user:db-secret@db.example/test "
        "api_key=source-secret token: bearer-secret password hunter2"
    )

    assert "db-secret" not in message
    assert "source-secret" not in message
    assert "bearer-secret" not in message
    assert "hunter2" not in message
    assert "postgresql://" not in message
    assert "api_key=***" in message
    assert "token=***" in message
    assert "password=***" in message


def test_error_sanitizer_enforces_positive_bounded_output() -> None:
    """Covers: DAG-014 — persisted task error context is length bounded."""
    assert sanitize_error_message("x" * 50, limit=12) == "x" * 12
    with pytest.raises(ValueError, match="positive"):
        sanitize_error_message("failure", limit=0)
