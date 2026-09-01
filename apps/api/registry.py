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

from collections.abc import Callable
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


# ---------------------------------------------------------------------------
# Discovery capabilities (API-003)
# ---------------------------------------------------------------------------

#: Version-relative path prefixes of the provider-neutral analytical routes.
#: A source marked ``served_by_neutral_routes`` is answerable through these;
#: one that is not gets an honest ``false`` instead of a silent empty page.
NEUTRAL_OBSERVATION_PREFIXES: tuple[str, ...] = (
    "/observations/",
    "/comparison",
    "/distribution/",
)


@dataclass(frozen=True)
class SourceDiscovery:
    """What one completed source's API surface looks like to a discovering client.

    This is the reviewed answer to the API-001 coverage matrix: the catalog
    advertises seven sources while the neutral observation routes serve three,
    and a client had no way to learn which was which. Each entry names the
    route segment the source's own observation routes live under (``None`` when
    the source has no observation surface yet -- FBI UCR until API-004), states
    whether the neutral routes can answer for it, and lists the provider
    dataset/product identities its routes accept.

    ``served_by_neutral_routes`` describes ``gold.v_metric_latest_by_geo`` and
    ``gold.v_metric_timeseries_by_geo`` as they stand: a three-way union over
    Census ACS, BLS, and FRED. API-004 replaces the union with registry
    dispatch; when it does, these flags flip to ``True`` in the same reviewed
    change and the capability resource reports the new reach without a shape
    change.
    """

    #: The glossary's ``source_code``.
    source_code: str
    #: Human-readable name, aligned with what the source publishes.
    display_name: str
    #: URL segment of the source-specific routes, ``None`` when none exist.
    route_segment: str | None
    #: True when the neutral observation/comparison/distribution routes can
    #: answer for this source's metrics today.
    served_by_neutral_routes: bool
    #: Registered provider dataset/product identities the source-specific
    #: routes accept as filters. Empty when dataset identity is embedded in the
    #: metric itself rather than accepted as a separate filter.
    dataset_provider: Callable[[], tuple[str, ...]] = tuple

    def registered_datasets(self) -> tuple[str, ...]:
        """Read the registered dataset identities at call time.

        Reading through the source registries rather than copying their values
        keeps this declaration from drifting when a product is enabled or
        retired; it is the same pattern the CDC router already uses for its
        dataset filter validation.
        """
        return tuple(self.dataset_provider())


def _cdc_datasets() -> tuple[str, ...]:
    from data_ingestion_toolbox.cdc.registry import enabled_assets

    return tuple(asset.asset_id for asset in enabled_assets())


def _nass_datasets() -> tuple[str, ...]:
    from data_ingestion_toolbox.usda_nass.registry import enabled_products

    return tuple(product.product_id for product in enabled_products())


def _fbi_datasets() -> tuple[str, ...]:
    from data_ingestion_toolbox.fbi_ucr.registry import enabled_products

    return tuple(product.product_id for product in enabled_products())


#: Every completed source, keyed by glossary source code, in the order the
#: capability resource serves them. The four sources with a ServingContract
#: derive their identity from it so the two declarations cannot disagree.
SOURCE_DISCOVERY: dict[str, SourceDiscovery] = {
    entry.source_code: entry
    for entry in (
        SourceDiscovery(
            source_code=SERVING_CONTRACTS["bls"].source_code,
            display_name=SERVING_CONTRACTS["bls"].display_name,
            route_segment="bls",
            served_by_neutral_routes=True,
        ),
        SourceDiscovery(
            source_code=SERVING_CONTRACTS["census"].source_code,
            display_name=SERVING_CONTRACTS["census"].display_name,
            route_segment="census",
            served_by_neutral_routes=True,
        ),
        SourceDiscovery(
            source_code="CDC",
            display_name="Centers for Disease Control and Prevention",
            route_segment="cdc",
            served_by_neutral_routes=False,
            dataset_provider=_cdc_datasets,
        ),
        SourceDiscovery(
            source_code=SERVING_CONTRACTS["pep"].source_code,
            display_name=SERVING_CONTRACTS["pep"].display_name,
            route_segment="pep",
            served_by_neutral_routes=False,
        ),
        SourceDiscovery(
            source_code="FBI_UCR",
            display_name=(
                "Federal Bureau of Investigation Uniform Crime Reporting Program"
            ),
            route_segment=None,
            served_by_neutral_routes=False,
            dataset_provider=_fbi_datasets,
        ),
        SourceDiscovery(
            source_code=SERVING_CONTRACTS["fred"].source_code,
            display_name=SERVING_CONTRACTS["fred"].display_name,
            route_segment="fred",
            served_by_neutral_routes=True,
        ),
        SourceDiscovery(
            source_code="USDA_NASS",
            display_name="USDA National Agricultural Statistics Service",
            route_segment="usda-nass",
            served_by_neutral_routes=False,
            dataset_provider=_nass_datasets,
        ),
    )
}
