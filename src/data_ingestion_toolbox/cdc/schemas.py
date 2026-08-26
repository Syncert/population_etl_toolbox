"""CDC CDI column schema and normalization helpers.

This module defines the authoritative column contract for the U.S. Chronic
Disease Indicators (CDI) product (Socrata ``$id``: ``hksd-2xuw``). All silver
and gold layers are built from these columns; any change to the source contract
must be reflected here first.

The columns below were verified against the live Socrata API. Socrata column
types (e.g. ``[number]``, ``[text]``, ``[date]``) are noted in the comments.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

from .registry import CDI_ASSET

# ---------------------------------------------------------------------------
# Authoritative CDI column contract (hksd-2xuw)
# ---------------------------------------------------------------------------

#: All columns returned by the Socrata API, in the order they appear in the
#: dataset schema. Socrata column types are noted in parentheses.
CDI_COLUMNS: tuple[str, ...] = CDI_ASSET.select_columns

#: Columns that hold numeric data values. Missing/invalid values in these
#: columns must NOT be silently converted to zero.
CDI_NUMERIC_COLUMNS: frozenset[str] = frozenset(
    column.name for column in CDI_ASSET.expected_columns if column.data_type == "number"
)

#: The primary value column.
CDI_PRIMARY_VALUE_COLUMN = "datavalue"

#: Columns that identify a unique CDI record (natural key).
CDI_NATURAL_KEY: tuple[str, ...] = CDI_ASSET.source_key


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------


def _is_missing(value: object) -> bool:
    """Return True if a Socrata API value represents missing/absent data."""
    if value is None:
        return True
    if isinstance(value, str):
        stripped = value.strip()
        return stripped == "" or stripped in {"null", "None", "NA", "N/A", "-", "."}
    return False


def normalize_cdi_record(record: dict[str, object]) -> dict[str, object]:
    """Normalize a raw Socrata CDI record for silver storage.

    - Numeric columns: convert to ``Decimal`` if present; set ``None`` if
      missing or unparseable (never silently zero).
    - Text columns: strip whitespace; set ``None`` for empty strings.
    - Unknown columns: pass through unchanged.

    Returns a new dict; the input is not mutated.
    """
    normalized: dict[str, object] = {}
    for column in CDI_COLUMNS:
        value = record.get(column)
        if column in CDI_NUMERIC_COLUMNS:
            if _is_missing(value):
                normalized[column] = None
            else:
                try:
                    normalized[column] = Decimal(str(value))
                except (InvalidOperation, ValueError):
                    # Non-numeric value in a numeric column: preserve as-is
                    # for quarantine/review rather than silently zeroing.
                    normalized[column] = str(value)
        else:
            if _is_missing(value):
                normalized[column] = None
            elif isinstance(value, str):
                normalized[column] = value.strip()
            else:
                normalized[column] = value
    # Pass through any columns not in the known contract (defensive).
    for key, value in record.items():
        if key not in CDI_COLUMNS:
            normalized[key] = value
    return normalized


def cdi_record_is_suppressed(record: dict[str, object]) -> bool:
    """Return True if the record's primary value is suppressed or missing.

    CDC suppresses values that fail the disclosure-avoidance threshold. In the
    Socrata API these arrive as ``None``/empty for ``datavalue``. The
    ``datavaluefootnote`` column may also carry a suppression indicator, but
    the authoritative signal is the absence of the value itself.
    """
    return _is_missing(record.get("datavalue"))


def cdi_record_has_ci(record: dict[str, object]) -> bool:
    """Return True if the record carries confidence interval limits."""
    return not _is_missing(record.get("lowconfidencelimit")) and not _is_missing(
        record.get("highconfidencelimit")
    )
