"""The per-metric time-aggregation registry keeps its contract.

Covers: ETL-073
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from data_ingestion_toolbox.semantics.time_aggregation import (
    REGISTRY_PATH,
    RegistryError,
    authorized_method,
    load_registry,
    parse_registry,
)

pytestmark = pytest.mark.unit

SERVED = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "semantics"
    / "served_subannual_metrics.json"
)
SCHEMA = REGISTRY_PATH.parent / "time_aggregation_method.schema.json"


def _entry(**overrides: object) -> dict:
    base = {
        "metric_code": "BLS:CUUR0000SA0",
        "method": "mean",
        "status": "approved",
        "owner": "data-eng",
        "reviewer": "Nick",
        "version": 1,
        "effective_date": "2026-10-07",
        "last_reviewed_date": "2026-10-07",
        "rationale": "BLS averages 12 monthly indexes.",
        "limitations": [],
        "citations": ["https://www.bls.gov/opub/hom/cpi/calculation.htm"],
    }
    base.update(overrides)
    return base


def test_every_served_subannual_metric_has_an_entry() -> None:
    """Covers: ETL-073 — no served BLS, FRED or FBI UCR sub-annual metric is left without a method."""
    served = set(json.loads(SERVED.read_text(encoding="utf-8"))["metric_codes"])
    registry = load_registry()
    assert served - set(registry) == set()
    assert len(served) >= 120


def test_the_registry_matches_its_schema_fields() -> None:
    """Covers: ETL-073 — every entry carries the fields the schema requires and no others."""
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    item = schema["properties"]["entries"]["items"]
    allowed, required = set(item["properties"]), set(item["required"])
    for entry in json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))["entries"]:
        assert required <= set(entry) <= allowed, entry["metric_code"]


def test_no_draft_authorizes_a_derived_value() -> None:
    """Covers: ETL-073 — a draft, a not_aggregable entry or a missing entry derives nothing."""
    registry = load_registry()
    authorized = [
        code for code in registry if authorized_method(code, registry) is not None
    ]
    assert authorized == [
        entry.metric_code
        for entry in registry.values()
        if entry.status == "approved" and entry.method != "not_aggregable"
    ]
    approved = parse_registry({"entries": [_entry()]})
    assert authorized_method("BLS:CUUR0000SA0", approved).method == "mean"
    draft = parse_registry({"entries": [_entry(status="draft")]})
    assert authorized_method("BLS:CUUR0000SA0", draft) is None
    assert authorized_method("BLS:NOT_REGISTERED", approved) is None
    refused = parse_registry({"entries": [_entry(method="not_aggregable")]})
    assert authorized_method("BLS:CUUR0000SA0", refused) is None


@pytest.mark.parametrize(
    ("entry", "message"),
    [
        (_entry(method="median"), "unknown method"),
        (_entry(method="recompute_ratio"), "numerator"),
        (_entry(numerator="BLS:A", denominator="BLS:B"), "numerator"),
        (_entry(reviewer="Nick (pending review)"), "without a reviewer"),
        (_entry(citations=[]), "cites nothing"),
    ],
)
def test_a_broken_entry_is_refused(entry: dict, message: str) -> None:
    """Covers: ETL-073 — unknown methods, half ratios, unreviewed approvals and uncited entries are refused."""
    with pytest.raises(RegistryError, match=message):
        parse_registry({"entries": [entry]})


def test_duplicates_are_refused() -> None:
    """Covers: ETL-073 — one entry per metric."""
    with pytest.raises(RegistryError, match="two entries"):
        parse_registry({"entries": [_entry(), _entry()]})
