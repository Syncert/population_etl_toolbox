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


#: Every source served by the generated per-source routes, keyed by route
#: segment. FBI UCR is deliberately absent: it publishes agency-level facts with
#: a participation basis that this row shape cannot represent honestly. Its
#: observation surface is the registry-dispatched neutral resource
#: (``OBSERVATION_DISPATCH`` below), whose envelope carries the participation
#: and coverage semantics this shape cannot.
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


class UnknownObservationDispatch(KeyError):
    """Raised when no reviewed dispatch entry exists for a source code."""


@dataclass(frozen=True)
class ObservationDispatch:
    """How the provider-neutral observation resource reads one source.

    API-004's registry dispatch: a requested metric resolves to its owning
    source through ``gold_glossary.dim_metric``, and this entry declares which
    reviewed relations answer for that source, how the metric's identity binds
    to those relations, and how a row projects onto the neutral envelope
    without erasing the source's own semantics. Every SQL fragment below is a
    reviewed constant over the declared relations; request text never reaches
    an identifier.

    Metric identity uses exactly one of three declared strategies:

    - ``metric_code_column`` -- the serving relation carries the same composed
      metric code the glossary publishes (BLS, FRED), so the requested code
      binds directly.
    - ``lineage_key_column`` (+ ``lineage_key_prefix``) -- the serving relation
      keys rows by the publisher's lineage ``key``, optionally under a
      different composed prefix. Census ACS publishes glossary codes as
      ``CENSUS_ACS:<dataset>:<variable>`` while its serving relations spell the
      same identity ``ACS:<dataset>:<variable>``; Census PEP's serving revision
      relation carries the bare measure code. The lineage key, not string
      surgery on the request, is the published bridge.
    - ``identity_columns`` -- the source publishes discrete identity fields in
      ``physical_lineage`` (CDC, FBI UCR, USDA NASS) that match same-named
      relation columns.
    """

    #: The glossary's ``source_code``.
    source_code: str
    #: Newest published values under this source's own latest semantics.
    latest_relation: str
    #: Every published release -- the as-released/revision surface.
    released_relation: str
    #: The ``schema``/``relation`` the source's publisher declares in
    #: ``physical_lineage``; checked against the glossary row whenever lineage
    #: identity is read, so a publication/registry disagreement fails loudly
    #: instead of reading the wrong rows.
    lineage_schema: str = ""
    lineage_relation: str = ""
    # -- metric identity strategy (exactly one) ---------------------------
    metric_code_column: str | None = None
    lineage_key_column: str | None = None
    lineage_key_prefix: str = ""
    identity_columns: tuple[str, ...] = ()
    # -- projection onto the neutral envelope ------------------------------
    release_expression: str = ""
    #: Orders releases oldest-to-newest (``release_watermark::BIGINT`` for CDC,
    #: dates elsewhere). Never carries ``DESC``.
    release_order_expression: str = ""
    as_of_expression: str | None = None
    period_start_expression: str = ""
    period_end_expression: str = ""
    geo_id_expression: str = "geo_id"
    geo_level_expression: str = "geo_level"
    value_expression: str = "value::TEXT"
    #: ``None`` when the source publishes no value-status vocabulary; the
    #: envelope then carries ``null``, which is distinguishable from ``valid``.
    value_status_column: str | None = None
    unit_expression: str = "units"
    #: ``(name, expression)`` pairs served verbatim under ``dimensions``.
    dimension_expressions: tuple[tuple[str, str], ...] = ()
    #: Source-published uncertainty fields; ``None``-like when absent.
    uncertainty_expressions: tuple[tuple[str, str], ...] = ()
    #: Source-published reporting-coverage fields (FBI UCR participation).
    coverage_expressions: tuple[tuple[str, str], ...] = ()
    source_record_id_column: str | None = None
    capture_id_column: str | None = None
    # -- filters and ordering ----------------------------------------------
    #: ``(query parameter, SQL condition)`` pairs. A request using a parameter
    #: absent here is rejected with an explanation, never silently ignored.
    filter_conditions: tuple[tuple[str, str], ...] = ()
    latest_order: tuple[str, ...] = ()
    released_order: tuple[str, ...] = ()
    # -- aligned analysis (API-005) -----------------------------------------
    #: True when the comparison and distribution routes can answer for this
    #: source: its latest surface reduces to one newest value per geography
    #: without discarding a published dimension. False for the stratified
    #: sources, whose rows carry strata, domains, or subject grains that an
    #: aligned single-value analysis would silently collapse.
    analysis_ready: bool = False
    #: Why the analysis routes decline, for sources where they do. Served in
    #: the preflight explanation, never invented at request time.
    analysis_restriction: str | None = None
    #: The numeric expression the analysis routes compute over. Only read when
    #: ``analysis_ready``; the underlying column is numeric for every ready
    #: source, and derived arithmetic is explicitly API-derived rather than a
    #: provider fact.
    analysis_value_expression: str = "value::DOUBLE PRECISION"
    #: True when the latest relation carries the four geography attribution
    #: columns (``state_fips``, ``county_fips``, ``state_name``,
    #: ``county_name``); a relation without them serves typed NULLs.
    publishes_geo_attribution: bool = False

    def supported_filters(self) -> tuple[str, ...]:
        return tuple(sorted(param for param, _ in self.filter_conditions))


#: Year-window conditions shared by the union-family relations, which carry a
#: date-typed ``observation_date``.
_DATE_YEAR_FROM = "observation_date >= MAKE_DATE(:year_from, 1, 1)"
_DATE_YEAR_TO = "observation_date <= MAKE_DATE(:year_to, 12, 31)"
_UNION_PERIOD_START = "COALESCE(duration_start, observation_date)::TEXT"
_UNION_PERIOD_END = "COALESCE(duration_end, observation_date)::TEXT"
_GEO_ID_FILTER = ("geo_id", "geo_id = :geo_id")
_GEO_LEVEL_FILTER = ("geo_level", "UPPER(geo_level) = UPPER(:geo_level)")
_STATE_FIPS_FILTER = ("state_fips", "state_fips = :state_fips")
_COUNTY_FIPS_FILTER = ("county_fips", "county_fips = :county_fips")

#: One reviewed dispatch entry per completed source, keyed by glossary source
#: code. This is the registry dispatch API-001 recorded: the neutral
#: observation resource reaches every source through its own serving relations
#: instead of widening the three-source ``gold.*`` union into a lossy shape.
OBSERVATION_DISPATCH: dict[str, ObservationDispatch] = {
    entry.source_code: entry
    for entry in (
        ObservationDispatch(
            source_code="BLS",
            latest_relation="gold_bls.mv_bls_latest",
            released_relation="gold_bls.rpt_bls_observations",
            lineage_schema="gold_bls",
            lineage_relation="fact_bls_observation",
            metric_code_column="metric_code",
            release_expression="as_of_date::TEXT",
            release_order_expression="as_of_date",
            as_of_expression="as_of_date::TEXT",
            period_start_expression=_UNION_PERIOD_START,
            period_end_expression=_UNION_PERIOD_END,
            dimension_expressions=(
                ("series_id", "series_id"),
                ("seasonal_adjustment_status", "seasonal_adjustment_status"),
            ),
            filter_conditions=(
                _GEO_ID_FILTER,
                _GEO_LEVEL_FILTER,
                _STATE_FIPS_FILTER,
                _COUNTY_FIPS_FILTER,
                ("year_from", _DATE_YEAR_FROM),
                ("year_to", _DATE_YEAR_TO),
            ),
            latest_order=("geo_id", "observation_date", "series_id"),
            released_order=("observation_date", "geo_id", "as_of_date", "series_id"),
            analysis_ready=True,
            publishes_geo_attribution=True,
        ),
        ObservationDispatch(
            source_code="CENSUS_ACS",
            latest_relation="gold_census.mv_acs_latest",
            released_relation="gold_census.rpt_acs_observations",
            lineage_schema="gold_census",
            lineage_relation="fact_acs_observation",
            lineage_key_column="metric_code",
            lineage_key_prefix="ACS:",
            release_expression="vintage_year::TEXT",
            release_order_expression="vintage_year",
            as_of_expression="as_of_date::TEXT",
            period_start_expression=_UNION_PERIOD_START,
            period_end_expression=_UNION_PERIOD_END,
            dimension_expressions=(
                ("dataset_code", "dataset_code"),
                ("variable_code", "variable_code"),
            ),
            uncertainty_expressions=(
                ("margin_of_error", "margin_of_error::TEXT"),
                ("margin_of_error_pct", "margin_of_error_pct::TEXT"),
            ),
            filter_conditions=(
                _GEO_ID_FILTER,
                _GEO_LEVEL_FILTER,
                _STATE_FIPS_FILTER,
                _COUNTY_FIPS_FILTER,
                ("year_from", _DATE_YEAR_FROM),
                ("year_to", _DATE_YEAR_TO),
            ),
            latest_order=("geo_id", "observation_date", "variable_code"),
            released_order=(
                "observation_date",
                "geo_id",
                "vintage_year",
                "variable_code",
            ),
            analysis_ready=True,
            publishes_geo_attribution=True,
        ),
        ObservationDispatch(
            source_code="CDC",
            latest_relation="gold_cdc.latest_release_observation",
            released_relation="gold_cdc.health_observation",
            lineage_schema="gold_cdc",
            lineage_relation="health_observation",
            identity_columns=("asset_id", "measure_id", "value_type_id"),
            release_expression="release_watermark",
            release_order_expression="release_watermark::BIGINT",
            period_start_expression="period_start::TEXT",
            period_end_expression="period_end::TEXT",
            geo_level_expression="geo_type",
            value_status_column="value_status",
            unit_expression="unit",
            dimension_expressions=(
                ("asset_id", "asset_id"),
                ("dataset_title", "dataset_title"),
                ("measure_label", "measure_label"),
                ("value_type_label", "value_type_label"),
                ("topic", "topic"),
                ("stratum_id", "stratum_id"),
                ("strata", "strata"),
                ("adjustment_status", "adjustment_status"),
                ("estimate_method", "estimate_method"),
                ("population_basis", "population_basis"),
                ("total_population", "total_population::TEXT"),
                ("population_18_plus", "population_18_plus::TEXT"),
                ("footnote_code", "footnote_code"),
                ("footnote_text", "footnote_text"),
            ),
            uncertainty_expressions=(
                ("confidence_lower", "confidence_lower::TEXT"),
                ("confidence_upper", "confidence_upper::TEXT"),
            ),
            source_record_id_column="source_record_id",
            capture_id_column="capture_id",
            filter_conditions=(
                _GEO_ID_FILTER,
                ("geo_level", "UPPER(geo_type) = UPPER(:geo_level)"),
                ("stratum_id", "stratum_id = :stratum_id"),
                ("adjustment_status", "adjustment_status = :adjustment_status"),
                ("year_from", "period_end >= :year_from"),
                ("year_to", "period_start <= :year_to"),
            ),
            latest_order=(
                "geo_id",
                "period_start",
                "period_end",
                "stratum_id",
                "observation_sk",
            ),
            released_order=(
                "release_watermark::BIGINT DESC",
                "geo_id",
                "period_start",
                "period_end",
                "stratum_id",
                "observation_sk",
            ),
            analysis_restriction=(
                "CDC publishes stratified health observations (stratum, "
                "adjustment status, multi-year periods) that an aligned "
                "one-value-per-geography analysis would silently collapse; "
                "query /observations with stratum_id and adjustment_status "
                "filters instead"
            ),
        ),
        ObservationDispatch(
            source_code="CENSUS_PEP",
            latest_relation="gold_pep.population_estimate_latest",
            released_relation="gold_pep.population_estimate_revision",
            lineage_schema="gold_pep",
            lineage_relation="population_estimate_revision",
            lineage_key_column="metric_code",
            release_expression="pep_vintage::TEXT",
            release_order_expression="pep_vintage",
            period_start_expression="estimate_date::TEXT",
            period_end_expression="estimate_date::TEXT",
            geo_level_expression="geo_type",
            unit_expression="unit",
            dimension_expressions=(
                ("dataset_code", "dataset_code"),
                ("product_code", "product_code"),
                ("summary_level", "summary_level"),
                ("value_source", "value_source"),
            ),
            capture_id_column="capture_id",
            filter_conditions=(
                _GEO_ID_FILTER,
                ("geo_level", "UPPER(geo_type) = UPPER(:geo_level)"),
                ("year_from", "observation_year >= :year_from"),
                ("year_to", "observation_year <= :year_to"),
            ),
            latest_order=("geo_id", "estimate_date", "dataset_code"),
            released_order=(
                "estimate_date",
                "geo_id",
                "pep_vintage",
                "dataset_code",
            ),
            analysis_ready=True,
        ),
        ObservationDispatch(
            source_code="FBI_UCR",
            latest_relation="gold_fbi.latest_release_observation",
            released_relation="gold_fbi.crime_observation",
            lineage_schema="gold_fbi",
            lineage_relation="crime_observation",
            identity_columns=("product_id", "measure_id"),
            release_expression="release_key",
            release_order_expression="release_key",
            as_of_expression="refresh_date::TEXT",
            period_start_expression="period_start::TEXT",
            period_end_expression="period_end::TEXT",
            geo_level_expression="source_geo_level",
            value_status_column="value_status",
            unit_expression="unit",
            dimension_expressions=(
                ("product_id", "product_id"),
                ("offense_code", "offense_code"),
                ("offense_label", "offense_label"),
                ("ucr_program", "ucr_program"),
                ("measure_form", "measure_form"),
                ("counted_entity_basis", "counted_entity_basis"),
                ("subject_type", "subject_type"),
                ("subject_code", "subject_code"),
                ("subject_label", "subject_label"),
                ("period", "period"),
                ("max_data_month", "max_data_month"),
                ("geography_basis", "geography_basis"),
            ),
            coverage_expressions=(
                ("population", "population::TEXT"),
                ("participated_population", "participated_population::TEXT"),
                ("coverage_percent", "coverage_percent::TEXT"),
                ("coverage_basis", "coverage_basis"),
                ("participation_status", "participation_status"),
                ("population_denominator", "population_denominator::TEXT"),
            ),
            source_record_id_column="source_record_id",
            capture_id_column="capture_id",
            filter_conditions=(
                _GEO_ID_FILTER,
                ("subject_type", "subject_type = :subject_type"),
                ("subject_code", "subject_code = :subject_code"),
                ("year_from", "period_end >= MAKE_DATE(:year_from, 1, 1)"),
                ("year_to", "period_start <= MAKE_DATE(:year_to, 12, 31)"),
            ),
            latest_order=(
                "period_start",
                "subject_type",
                "subject_code",
                "observation_sk",
            ),
            released_order=(
                "release_key DESC",
                "period_start",
                "subject_type",
                "subject_code",
                "observation_sk",
            ),
            analysis_restriction=(
                "FBI UCR publishes agency-grain, participation-qualified "
                "monthly counts whose subjects are not canonical geographies; "
                "an aligned per-geography analysis would misstate coverage. "
                "Query /observations with subject_type and subject_code "
                "filters instead"
            ),
        ),
        ObservationDispatch(
            source_code="FRED",
            latest_relation="gold_fred.mv_fred_latest",
            released_relation="gold_fred.rpt_fred_observations",
            lineage_schema="gold_fred",
            lineage_relation="fact_fred_observation",
            metric_code_column="metric_code",
            release_expression="as_of_date::TEXT",
            release_order_expression="as_of_date",
            as_of_expression="as_of_date::TEXT",
            period_start_expression=_UNION_PERIOD_START,
            period_end_expression=_UNION_PERIOD_END,
            dimension_expressions=(
                ("series_id", "series_id"),
                ("seasonal_adjustment_status", "seasonal_adjustment_status"),
            ),
            filter_conditions=(
                _GEO_ID_FILTER,
                _GEO_LEVEL_FILTER,
                _STATE_FIPS_FILTER,
                _COUNTY_FIPS_FILTER,
                ("year_from", _DATE_YEAR_FROM),
                ("year_to", _DATE_YEAR_TO),
            ),
            latest_order=("geo_id", "observation_date", "series_id"),
            released_order=("observation_date", "geo_id", "as_of_date", "series_id"),
            analysis_ready=True,
            publishes_geo_attribution=True,
        ),
        ObservationDispatch(
            source_code="USDA_NASS",
            latest_relation="gold_nass.latest_release_observation",
            released_relation="gold_nass.crop_observation",
            lineage_schema="gold_nass",
            lineage_relation="crop_observation",
            identity_columns=(
                "product_id",
                "statistic_sk",
                "statisticcat_desc",
                "unit_desc",
            ),
            release_expression="release_watermark",
            release_order_expression="release_watermark",
            period_start_expression="year::TEXT",
            period_end_expression="year::TEXT",
            geo_level_expression="agg_level_desc",
            value_status_column="value_status",
            unit_expression="unit_desc",
            dimension_expressions=(
                ("product_id", "product_id"),
                ("short_desc", "short_desc"),
                ("commodity_desc", "commodity_desc"),
                ("class_desc", "class_desc"),
                ("prodn_practice_desc", "prodn_practice_desc"),
                ("util_practice_desc", "util_practice_desc"),
                ("domain_desc", "domain_desc"),
                ("domaincat_desc", "domaincat_desc"),
                ("freq_desc", "freq_desc"),
                ("reference_period_desc", "reference_period_desc"),
                ("week_ending", "week_ending::TEXT"),
                ("suppression_code", "suppression_code"),
                ("state_fips", "state_fips"),
                ("county_fips", "county_fips"),
            ),
            uncertainty_expressions=(
                ("cv_value", "cv_value::TEXT"),
                ("cv_status", "cv_status"),
                ("cv_symbol", "cv_symbol"),
            ),
            source_record_id_column="source_record_id",
            capture_id_column="capture_id",
            filter_conditions=(
                _GEO_ID_FILTER,
                ("geo_level", "UPPER(agg_level_desc) = UPPER(:geo_level)"),
                _STATE_FIPS_FILTER,
                _COUNTY_FIPS_FILTER,
                ("domain_desc", "domain_desc = :domain_desc"),
                ("domaincat_desc", "domaincat_desc = :domaincat_desc"),
                ("year_from", "year >= :year_from"),
                ("year_to", "year <= :year_to"),
            ),
            latest_order=("geo_id", "year", "domaincat_desc", "observation_sk"),
            released_order=(
                "release_watermark DESC",
                "geo_id",
                "year",
                "domaincat_desc",
                "observation_sk",
            ),
            analysis_restriction=(
                "USDA NASS publishes multi-dimensional crop statistics "
                "(domain, class, practice, reference period) that an aligned "
                "one-value-per-geography analysis would silently collapse; "
                "query /observations with domain and geography filters instead"
            ),
        ),
    )
}


def observation_dispatch(source_code: str) -> ObservationDispatch:
    """Return the reviewed dispatch entry for ``source_code``.

    Raises rather than guessing: a metric whose source has no reviewed entry
    is not servable through the neutral observation resource, and the caller
    owns saying so explicitly.
    """
    try:
        return OBSERVATION_DISPATCH[source_code]
    except KeyError as error:
        raise UnknownObservationDispatch(
            f"no reviewed observation dispatch for source '{source_code}'"
        ) from error


#: Every relation the observation endpoints may read, for the privilege and
#: allowlist assertions. A relation absent from this set must never appear in a
#: generated query.
ALLOWED_OBSERVATION_RELATIONS: frozenset[str] = frozenset(
    relation
    for contract in SERVING_CONTRACTS.values()
    for relation in (contract.latest_relation, contract.history_relation)
) | frozenset(
    relation
    for dispatch in OBSERVATION_DISPATCH.values()
    for relation in (dispatch.latest_relation, dispatch.released_relation)
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
# Discovery capabilities (API-003, extended by API-004)
# ---------------------------------------------------------------------------

#: Version-relative paths of the neutral routes that answer for every source
#: with a dispatch entry: the registry-dispatched observation resource and its
#: release listing (API-004).
DISPATCH_NEUTRAL_PATHS: tuple[str, ...] = (
    "/observations",
    "/observations/releases",
)

#: Version-relative paths of the aligned analysis routes (API-005). They
#: answer only for ``analysis_ready`` dispatch entries; a stratified source
#: gets a preflight explanation instead of a silently collapsed number.
ANALYSIS_NEUTRAL_PATHS: tuple[str, ...] = (
    "/comparison",
    "/comparison/preflight",
    "/distribution/bins",
)

#: Paths answered for an analysis-ready source that is not published into the
#: legacy cross-source union views (Census PEP).
DISPATCH_ANALYSIS_PATHS: tuple[str, ...] = (
    DISPATCH_NEUTRAL_PATHS + ANALYSIS_NEUTRAL_PATHS
)

#: Paths answered for the three sources also published into the legacy
#: cross-source ``gold.*`` union views, which still back the legacy
#: latest/timeseries pair until API-008 retires it.
UNION_NEUTRAL_PATHS: tuple[str, ...] = DISPATCH_ANALYSIS_PATHS + (
    "/observations/latest",
    "/observations/timeseries",
)


@dataclass(frozen=True)
class SourceDiscovery:
    """What one completed source's API surface looks like to a discovering client.

    This is the reviewed answer to the API-001 coverage matrix: the catalog
    advertised seven sources while the neutral routes served three, and a
    client had no way to learn which was which. Each entry names the route
    segment the source's own observation routes live under (``None`` when the
    source has none -- FBI UCR, whose observation surface is the neutral
    resource), lists the exact neutral paths that can answer for it, and lists
    the provider dataset/product identities its routes accept.

    API-004 closed the gap with registry dispatch: every source's metrics are
    servable through ``/observations`` and ``/observations/releases``, so
    ``served_by_neutral_routes`` is true for all seven. API-005 added the
    aligned analysis paths for every ``analysis_ready`` dispatch entry. The
    paths are declared exactly -- not by prefix -- because the legacy
    ``/observations/latest``/``timeseries`` pair still reads the three-source
    union views, and the analysis routes decline the stratified sources by
    declared policy; advertising either for a source it cannot answer would
    recreate the silent empty page this registry exists to prevent.
    """

    #: The glossary's ``source_code``.
    source_code: str
    #: Human-readable name, aligned with what the source publishes.
    display_name: str
    #: URL segment of the source-specific routes, ``None`` when none exist.
    route_segment: str | None
    #: Version-relative neutral route paths that can answer for this source.
    neutral_paths: tuple[str, ...]
    #: Registered provider dataset/product identities the source-specific
    #: routes accept as filters. Empty when dataset identity is embedded in the
    #: metric itself rather than accepted as a separate filter.
    dataset_provider: Callable[[], tuple[str, ...]] = tuple

    @property
    def served_by_neutral_routes(self) -> bool:
        """True when at least one neutral route answers for this source."""
        return bool(self.neutral_paths)

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
            neutral_paths=UNION_NEUTRAL_PATHS,
        ),
        SourceDiscovery(
            source_code=SERVING_CONTRACTS["census"].source_code,
            display_name=SERVING_CONTRACTS["census"].display_name,
            route_segment="census",
            neutral_paths=UNION_NEUTRAL_PATHS,
        ),
        SourceDiscovery(
            source_code="CDC",
            display_name="Centers for Disease Control and Prevention",
            route_segment="cdc",
            neutral_paths=DISPATCH_NEUTRAL_PATHS,
            dataset_provider=_cdc_datasets,
        ),
        SourceDiscovery(
            source_code=SERVING_CONTRACTS["pep"].source_code,
            display_name=SERVING_CONTRACTS["pep"].display_name,
            route_segment="pep",
            neutral_paths=DISPATCH_ANALYSIS_PATHS,
        ),
        SourceDiscovery(
            source_code="FBI_UCR",
            display_name=(
                "Federal Bureau of Investigation Uniform Crime Reporting Program"
            ),
            route_segment=None,
            neutral_paths=DISPATCH_NEUTRAL_PATHS,
            dataset_provider=_fbi_datasets,
        ),
        SourceDiscovery(
            source_code=SERVING_CONTRACTS["fred"].source_code,
            display_name=SERVING_CONTRACTS["fred"].display_name,
            route_segment="fred",
            neutral_paths=UNION_NEUTRAL_PATHS,
        ),
        SourceDiscovery(
            source_code="USDA_NASS",
            display_name="USDA National Agricultural Statistics Service",
            route_segment="usda-nass",
            neutral_paths=DISPATCH_NEUTRAL_PATHS,
            dataset_provider=_nass_datasets,
        ),
    )
}
