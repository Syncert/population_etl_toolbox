"""Isolated live contract checks for the elections providers.

The congress-legislators files parse with nothing quarantined and hold the
current Congress; the Wikidata governor query answers every state; each
registered MEDSL Dataverse dataset lists exactly one registered file, and
that file's header is a results file. None takes a credential.

These checks are also what confirms the Wikidata identifiers and MEDSL file
names the offline fixtures could only assume (the build container cannot
reach either service).

Covers: EXT-031
"""

from __future__ import annotations

import logging

import httpx
import pytest

from data_ingestion_toolbox.elections.client import (
    ElectionsFetchError,
    ElectionsPayloadError,
    data_request,
    fetch_file,
    governor_request,
    legislator_requests,
    listing_request,
    select_data_file,
)
from data_ingestion_toolbox.elections.config import ElectionsConfig
from data_ingestion_toolbox.elections.registry import (
    RESULT_STREAMS,
    STATE_FIPS,
    result_dataset,
)
from data_ingestion_toolbox.elections.silver_elections.parse import (
    parse_governors,
    parse_legislators,
)
from tests.support.external import classify_external_failure, observe_external_call

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)
# The county file's guestbook is answered from the operator's environment
# (ELECTIONS_GUESTBOOK_*), as the DAG answers it.
CONFIG = ElectionsConfig.from_environment().model_copy(
    update={"min_spacing_seconds": 1.0, "max_attempts": 2}
)


def test_the_legislator_files_parse_and_hold_the_current_congress() -> None:
    """Covers: EXT-031 — nothing quarantined; 100 senators and 435+ House seats serving."""
    current, historical = legislator_requests()
    fetched, _result = observe_external_call(
        "elections:legislators-current",
        lambda: fetch_file(current, config=CONFIG),
        logger=LOGGER,
    )
    terms, rejected = parse_legislators(fetched.raw_bytes)
    assert rejected == []
    serving = [t for t in terms if t.end_date is None or t.end_date.year >= 2027]
    assert len({t.state_code for t in serving if t.office == "us_senate"}) == 50
    assert (
        len({(t.state_code, t.district) for t in serving if t.office == "us_house"})
        >= 435
    )
    fetched, _result = observe_external_call(
        "elections:legislators-historical",
        lambda: fetch_file(historical, config=CONFIG),
        logger=LOGGER,
    )
    _terms, rejected = parse_legislators(fetched.raw_bytes)
    assert rejected == []


def test_the_governor_query_answers_every_state() -> None:
    """Covers: EXT-031 — a serving governor in each state, a registered party for most."""
    fetched, _result = observe_external_call(
        "elections:governors",
        lambda: fetch_file(governor_request(), config=CONFIG),
        logger=LOGGER,
    )
    terms, _rejected = parse_governors(fetched.raw_bytes)
    states = {t.state_code for t in terms}
    assert states >= set(STATE_FIPS) - {"DC"}
    major = [t for t in terms if t.party_code in ("REP", "DEM")]
    assert len(major) >= 0.9 * len(terms)


@pytest.mark.parametrize("stream", RESULT_STREAMS)
def test_each_medsl_dataset_lists_one_registered_results_file(stream: str) -> None:
    """Covers: EXT-031 — the registered name matches once; the file is a results file."""
    dataset = result_dataset(stream)
    listing, _result = observe_external_call(
        f"elections:{stream}:listing",
        lambda: fetch_file(listing_request(dataset), config=CONFIG),
        logger=LOGGER,
    )
    file_id, _name = select_data_file(dataset, listing.raw_bytes)
    # fetch_file checks the header names year and candidatevotes.
    observe_external_call(
        f"elections:{stream}:data",
        lambda: fetch_file(data_request(dataset, file_id), config=CONFIG),
        logger=LOGGER,
    )


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            ElectionsFetchError(
                "county_president:data", code="retry_exhausted", status=503
            ),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            ElectionsFetchError("governors", code="non_retryable_http", status=404),
            "contract-regression",
        ),
        (
            ElectionsPayloadError("house:listing", code="ambiguous_file"),
            "contract-regression",
        ),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-031 — an outage is not reported as a contract regression."""
    assert classify_external_failure(error) == expected
