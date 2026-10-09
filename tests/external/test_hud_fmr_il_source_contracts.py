"""Isolated live contract checks for the HUD User Data API reads.

Every state answers each registered FMR year with nothing quarantined and a
row for nearly every county; each registered read still serves the edition
it was checked to serve (Napa County's two-bedroom FMR); and a county's
income limits answer as registered. Needs ``HUD_USER_API_TOKEN``.

Covers: EXT-023
"""

from __future__ import annotations

import logging
import os
from decimal import Decimal

import httpx
import pytest

from data_ingestion_toolbox.hud_fmr_il.api import (
    HudApiClient,
    HudApiError,
    HudApiPayloadError,
    parse_fmr_state,
    parse_il_county,
    registered_reads,
    state_codes,
)
from data_ingestion_toolbox.hud_fmr_il.config import (
    API_TOKEN_ENVIRONMENT_VARIABLE,
    HudConfig,
)
from data_ingestion_toolbox.hud_fmr_il.registry import FMR
from tests.support.external import (
    classify_external_failure,
    observe_external_call,
    require_external_key,
)

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)

#: Values the registered edition of each read publishes, from HUD's own
#: workbooks: Napa County's two-bedroom FMR and Kent County's four-person
#: 50% limit.
KNOWN = {
    "api:fmr:fy2026": ("0605599999", "fmr_2br", Decimal("3315")),
    "api:fmr:fy2027": ("0605599999", "fmr_2br", Decimal("3375")),
    "api:il:fy2026": ("1000199999", "income_limit_50_4p", Decimal("53900")),
}


@pytest.fixture(scope="module")
def api() -> HudApiClient:
    token = require_external_key(
        API_TOKEN_ENVIRONMENT_VARIABLE, os.environ.get(API_TOKEN_ENVIRONMENT_VARIABLE)
    )
    client = HudApiClient(HudConfig(hud_user_api_token=token, max_attempts=2))
    yield client
    client.close()


@pytest.mark.parametrize(
    "read",
    [read for read in registered_reads() if read.dataset == FMR],
    ids=lambda read: read.key,
)
def test_every_state_answers_its_fmrs(api: HudApiClient, read) -> None:  # noqa: ANN001
    """Covers: EXT-023 — every state's FMRs parse; nearly every county is present."""
    states, _result = observe_external_call(
        "hud_fmr_il:fmr/listStates",
        lambda: state_codes(api.get("fmr/listStates").raw_bytes),
        logger=LOGGER,
    )
    counties = 0
    for state in states:
        parsed = parse_fmr_state(
            api.get(f"fmr/statedata/{state}?year={read.fiscal_year}").raw_bytes,
            read=read,
        )
        assert parsed.quarantined == (), state
        counties += parsed.county_row_count
    assert len(states) >= 56 and counties >= 3000


@pytest.mark.parametrize("read", registered_reads(), ids=lambda read: read.key)
def test_each_read_serves_its_registered_edition(api: HudApiClient, read) -> None:  # noqa: ANN001
    """Covers: EXT-023 — the API still serves the edition the registry names."""
    fips, measure, value = KNOWN[read.key]
    if read.dataset == FMR:
        raw = api.get(
            f"fmr/statedata/{'CA' if fips.startswith('06') else 'DE'}?year={read.fiscal_year}"
        ).raw_bytes
        parsed = parse_fmr_state(raw, read=read)
    else:
        parsed = parse_il_county(
            api.get(f"il/data/{fips}?year={read.fiscal_year}").raw_bytes,
            read=read,
            fips=fips,
        )
    assert parsed.quarantined == ()
    served = {(obs.fips_code, obs.measure): obs.value for obs in parsed.observations}
    assert served[(fips, measure)] == value


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            HudApiError("fmr/statedata/DE", code="retry_exhausted", status=503),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            HudApiError("fmr/statedata/DE", code="non_retryable_http", status=404),
            "contract-regression",
        ),
        (
            HudApiPayloadError("fmr/statedata/DE", code="not_json"),
            "contract-regression",
        ),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-023 — an outage is not reported as a contract regression."""
    assert classify_external_failure(error) == expected
