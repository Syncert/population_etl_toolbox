"""Shared reviewed-fixture loading for the FBI UCR unit suite."""

from __future__ import annotations

import dataclasses
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

FIXTURE_ROOT = Path(__file__).resolve().parents[3] / "tests/fixtures/fbi_ucr"


def load_payload(name: str) -> Any:
    """Load one reviewed provider fixture with exact decimal numbers."""
    text = (FIXTURE_ROOT / f"{name}.json").read_text(encoding="utf-8")
    return json.loads(text, parse_float=Decimal)


def load_bytes(name: str) -> bytes:
    """Return the exact fixture bytes, as a capture would store them."""
    return (FIXTURE_ROOT / f"{name}.json").read_bytes()


@pytest.fixture
def fbi_payload():
    """Return the reviewed-fixture loader."""
    return load_payload


@pytest.fixture
def fbi_bytes():
    """Return the exact-fixture-bytes loader."""
    return load_bytes


@pytest.fixture
def agency_names() -> dict[str, str]:
    """Return the ORI-to-published-name map the reference slice supplies."""
    from data_ingestion_toolbox.fbi_ucr.silver_fbi.agency import parse_agency_directory

    result = parse_agency_directory(
        load_payload("agency_directory_WI"),
        state_code="WI",
        slice_key="agency_directory:WI",
    )
    return {record.ori: record.agency_name for record in result.agencies}


def observation_fixture(product: Any, subject: Any) -> str:
    """Return the reviewed fixture captured for one product subject."""
    code = product.offense_code
    if subject.subject_type == "national":
        return f"summarized_national_{code}"
    if subject.subject_type == "state":
        return f"summarized_state_{subject.subject_code}_{code}"
    return f"summarized_agency_{subject.subject_code}_{code}"


#: States with captured summarized fixtures for every registered offense.
#: Wisconsin carries the reviewed agency sample, Pennsylvania's participation
#: moves from 76% to 99% inside the fixture window, and the Virgin Islands is
#: the territory subject. Every other documented state is proved at the
#: registry level rather than by a fixture.
FIXTURE_STATES: tuple[str, ...] = ("PA", "VI", "WI")


def fixture_scoped(product: Any) -> Any:
    """Return a registered product narrowed to the states with fixtures."""
    return dataclasses.replace(
        product,
        state_scope=tuple(
            state for state in product.state_scope if state in FIXTURE_STATES
        ),
    )
