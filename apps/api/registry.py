"""The reviewed registry of source serving contracts.

Every relation the observation endpoints read, and every source-specific
difference in how a row is projected, is declared here once. Nothing else in the
API may name a serving relation: relation names reach SQL only from this module,
never from request text, which is what makes the string interpolation in the
query builders safe.

Before this registry the same four sources were spelled out in five places -- a
latest-table map, a history-table map, a schema map, a column map, and a chain of
``if source in {...}`` branches inside the select builder -- and each of them had
drifted to a slightly different idea of which sources existed. Adding a source
meant finding all five. Adding one now means adding one entry.

The registry is deliberately a reviewed constant rather than something discovered
from the database at request time. A relation name that a request could influence
is an injection surface, and a serving contract that appears without review is
exactly the "silently select whichever relation happens to exist" behaviour the
API development plan forbids.
"""

from __future__ import annotations

from dataclasses import dataclass

#: How a source spells the geography label it prefers. PEP publishes places, so
#: its rows carry a ``place_name`` the other sources do not have; selecting it
#: unconditionally would fail on relations where the column does not exist.
_PLACE_AWARE_GEO_NAME = "COALESCE(place_name, county_name, state_name, geo_id)"
_DEFAULT_GEO_NAME = "COALESCE(county_name, state_name, geo_id)"


@dataclass(frozen=True)
class ServingContract:
    """One source's published observation surface and its projection capabilities.

    The capability flags describe what the source's relations actually carry. A
    source without seasonal adjustment does not get an empty string or a zero in
    that field -- it gets a typed ``NULL``, so a consumer can tell "this source
    does not publish seasonal adjustment" apart from "this observation is not
    seasonally adjusted".
    """

    #: The glossary's ``source_code``, as published in
    #: ``gold_glossary.dim_metric_catalog``.
    source_code: str
    #: The URL segment the source-specific routes are mounted under.
    route_segment: str
    #: Human-readable name, used in generated documentation.
    display_name: str
    #: The OpenAPI tag the source's routes are grouped under. Usually the route
    #: segment, but PEP has always been tagged ``census-pep`` and renaming a tag
    #: reorganises published documentation for no gain.
    openapi_tag: str
    #: The source's gold schema.
    schema: str
    #: Newest values. Refreshed independently of the durable history.
    latest_relation: str
    #: Durable as-published history, and the fallback when ``latest_relation``
    #: is empty.
    history_relation: str
    #: True when the source publishes a seasonal adjustment status.
    publishes_seasonal_adjustment: bool = False
    #: True when the source publishes dataset/vintage identity and a margin of
    #: error -- the Census-family survey fields.
    publishes_vintage_and_error: bool = False
    #: True when the source publishes place-level geography names.
    publishes_place_names: bool = False

    @property
    def geo_name_expression(self) -> str:
        return (
            _PLACE_AWARE_GEO_NAME if self.publishes_place_names else _DEFAULT_GEO_NAME
        )


#: Every source with a provider-neutral observation surface, keyed by route
#: segment. FBI UCR is deliberately absent: it publishes agency-level facts with
#: a participation basis that this row shape cannot represent honestly, and
#: API-004 owns giving it a surface that can.
SERVING_CONTRACTS: dict[str, ServingContract] = {
    contract.route_segment: contract
    for contract in (
        ServingContract(
            source_code="BLS",
            route_segment="bls",
            openapi_tag="bls",
            display_name="Bureau of Labor Statistics",
            schema="gold_bls",
            latest_relation="gold_bls.mv_bls_latest",
            history_relation="gold_bls.rpt_bls_observations",
            publishes_seasonal_adjustment=True,
        ),
        ServingContract(
            source_code="CENSUS_ACS",
            route_segment="census",
            openapi_tag="census",
            display_name="Census American Community Survey",
            schema="gold_census",
            latest_relation="gold_census.mv_acs_latest",
            history_relation="gold_census.rpt_acs_observations",
            publishes_vintage_and_error=True,
        ),
        ServingContract(
            source_code="FRED",
            route_segment="fred",
            openapi_tag="fred",
            display_name="Federal Reserve Economic Data",
            schema="gold_fred",
            latest_relation="gold_fred.mv_fred_latest",
            history_relation="gold_fred.rpt_fred_observations",
            publishes_seasonal_adjustment=True,
        ),
        ServingContract(
            source_code="CENSUS_PEP",
            route_segment="pep",
            openapi_tag="census-pep",
            display_name="Census Population Estimates Program",
            schema="gold_pep",
            latest_relation="gold_pep.mv_pep_latest",
            history_relation="gold_pep.rpt_pep_observations",
            publishes_vintage_and_error=True,
            publishes_place_names=True,
        ),
    )
}

#: Every relation the observation endpoints may read, for the privilege and
#: allowlist assertions. A relation absent from this set must never appear in a
#: generated query.
ALLOWED_OBSERVATION_RELATIONS: frozenset[str] = frozenset(
    relation
    for contract in SERVING_CONTRACTS.values()
    for relation in (contract.latest_relation, contract.history_relation)
)


class UnknownServingContract(KeyError):
    """Raised when code asks for a source that the registry does not publish."""


def serving_contract(route_segment: str) -> ServingContract:
    """Return the reviewed contract for ``route_segment``.

    Raises rather than falling back. A missing contract means the registry and
    the routes disagree, and answering from whichever relation happens to look
    similar is how a response ends up describing a source it did not come from.
    """
    try:
        return SERVING_CONTRACTS[route_segment.lower()]
    except KeyError as error:
        raise UnknownServingContract(
            f"no reviewed serving contract for source route '{route_segment}'"
        ) from error


def registered_route_segments() -> tuple[str, ...]:
    """Route segments with a serving contract, in registration order."""
    return tuple(SERVING_CONTRACTS)
