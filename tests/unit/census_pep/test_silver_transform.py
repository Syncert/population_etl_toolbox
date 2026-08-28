"""Unit contracts for the capture-scoped Census PEP silver transform."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from data_ingestion_toolbox.census_pep.silver_pep.transform import (
    transform_pep_to_silver,
)

pytestmark = pytest.mark.unit


def _hook_with_cursor(*, rowcount: int = 0) -> tuple[MagicMock, MagicMock, MagicMock]:
    hook = MagicMock()
    connection = MagicMock()
    cursor = MagicMock()
    hook.get_conn.return_value.__enter__.return_value = connection
    connection.cursor.return_value.__enter__.return_value = cursor
    cursor.rowcount = rowcount
    return hook, connection, cursor


def test_transform_preserves_vintage_geo_resolution_and_source_value() -> None:
    """Covers: ETL-030 — the conformed SQL retains all PEP identity axes."""
    hook, connection, cursor = _hook_with_cursor(rowcount=17)

    assert transform_pep_to_silver(hook) == 17

    statements = "\n".join(call.args[0] for call in cursor.execute.call_args_list)
    assert "release_vintage" in statements
    assert "observation_year" in statements
    assert "geography_basis_date" in statements
    assert "value_source" in statements
    assert "silver_ref.dim_geo_entity" in statements
    assert "silver_ref.geography_resolution" in statements
    assert (
        "ON CONFLICT (capture_id, source_row_index, source_column_index)" in statements
    )
    connection.commit.assert_called_once_with()


def test_transform_rolls_back_as_one_transaction() -> None:
    """Covers: DB-005 — a conformance failure cannot partially publish facts."""
    hook, connection, cursor = _hook_with_cursor()
    cursor.execute.side_effect = [None, RuntimeError("database unavailable")]

    with pytest.raises(RuntimeError, match="database unavailable"):
        transform_pep_to_silver(hook)

    connection.rollback.assert_called_once_with()
    connection.commit.assert_not_called()
