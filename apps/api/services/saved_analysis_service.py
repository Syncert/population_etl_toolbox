"""Saved analysis configuration storage and validation (ADR-0003, API-007).

Two disciplines carry this module:

**Ownership is enforced in SQL, never after the fact.** Every statement is
scoped by ``owner_user_id``, so another user's configuration is not filtered
out of a result — it is never selected. A configuration id belonging to
someone else answers 404, indistinguishable from one that never existed, so
ids cannot be enumerated across accounts.

**A stored configuration is validated against live contracts.** The same
capability registry and compatibility policy the live routes enforce validate
a document on write, so persistence cannot become a back door for a request
the API would refuse. On read the document is re-validated and the verdict
reported — never silently repaired, because the document is the user's
content, not the API's.

Validation reads the warehouse glossary through the read-only serving session;
storage reads and writes go through the separate application session. The two
never share a transaction.
"""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from apps.api.registry import (
    CONFIGURATION_DOCUMENT_FIELDS,
    CONFIGURATION_FILTER_PARAMETERS,
    CONFIGURATION_ROUTES,
    OBSERVATION_DISPATCH,
    closed_value_refusal,
    normalize_geo_level,
)
from apps.api.schemas.observations import OBSERVATION_FILTER_BOUNDS
from apps.api.schemas import (
    AnalysisDocument,
    ConfigurationValidation,
    SavedAnalysisConfiguration,
    SavedAnalysisListResponse,
    SavedAnalysisSummary,
)
from apps.api.services.compatibility import evaluate_comparison
from apps.api.services.metric_freshness import retirement_refusal
from apps.api.services.neutral_observations_service import resolve_metric

#: Document fields that belong to no single kind: the kind itself, the
#: per-source `filters` the capability contract governs, and the opaque
#: `visualization` the API stores verbatim and never reads.
_DOCUMENT_FIELDS_EVERY_KIND_CARRIES = frozenset({"kind", "filters", "visualization"})


class ConfigurationInvalid(ValueError):
    """A document the live contracts would refuse (HTTP 422)."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class ConfigurationNotFound(LookupError):
    """No configuration with that id is owned by the caller (HTTP 404)."""


class ConfigurationConflict(Exception):
    """The stored version differs from the version the caller read (HTTP 409)."""

    def __init__(self, current_version: int) -> None:
        super().__init__(f"configuration has moved to version {current_version}")
        self.current_version = current_version


class ConfigurationNameTaken(Exception):
    """The caller already owns a configuration with that name (HTTP 409)."""


# ---------------------------------------------------------------------------
# Validation against live capability and compatibility contracts
# ---------------------------------------------------------------------------


def _require_metric(warehouse: Session, metric_code: Optional[str], field: str):
    if not metric_code:
        raise ConfigurationInvalid(f"{field} is required for this configuration kind")
    metric = resolve_metric(warehouse, metric_code)
    if metric is None:
        raise ConfigurationInvalid(f"{field} '{metric_code}' is not a published metric")
    # Existence was the only thing read here, so a measure the warehouse had
    # retired -- row present, state `retired`, observations no longer served --
    # validated as current and replayed as an empty page (API-119).
    retired = retirement_refusal(field, metric_code, metric.get("freshness_state"))
    if retired is not None:
        raise ConfigurationInvalid(retired)
    return metric


def _require_declared_filters(metric, filters: dict[str, Any], *, kind: str) -> None:
    """Refuse a filter the kind's own route would not accept.

    Two things have to hold, and the accepted set used to be their *union*
    rather than their intersection (API-117).

    A filter the route has a parameter for is still refused when the source
    declares none: `/distribution/bins` takes `state_fips`, Census PEP
    declares no such filter, and a document carrying both stored clean and
    replayed as a 422. A filter the source declares is still refused when
    the route has no parameter for it: an ACS distribution filtered by
    `year_from` or `geo_id` stored clean and replayed as the
    strict-parameter refusal (API-093).
    """
    source_code = str(metric.get("source_code") or "")
    dispatch = OBSERVATION_DISPATCH.get(source_code)
    if dispatch is None:
        raise ConfigurationInvalid(
            f"source '{source_code}' is not served by the observation routes"
        )
    route = CONFIGURATION_ROUTES[kind]
    accepted_by_route = CONFIGURATION_FILTER_PARAMETERS[kind]
    if accepted_by_route is not None:
        beyond_route = sorted(set(filters) - set(accepted_by_route))
        if beyond_route:
            raise ConfigurationInvalid(
                f"filters not accepted by {route}: {', '.join(beyond_route)}; "
                f"it accepts: {', '.join(sorted(accepted_by_route))}"
            )
    declared = set(dispatch.supported_filters())
    if accepted_by_route is not None:
        declared &= set(accepted_by_route)
    unsupported = sorted(set(filters) - declared)
    if unsupported:
        raise ConfigurationInvalid(
            f"filters not supported for source '{source_code}': "
            f"{', '.join(unsupported)}; supported filters: "
            f"{', '.join(sorted(declared)) or 'none'}"
        )
    # The names were checked and the values were not, so a value the live
    # route refuses -- a 5,000-character `geo_id` against its declared 200 --
    # stored clean, reported valid, and failed only when its owner reopened
    # it. The bound is read from where the route reads it (API-091).
    for name in sorted(filters):
        bound = OBSERVATION_FILTER_BOUNDS.get(name)
        if bound is None:
            continue
        rejection = bound.rejection(filters[name])
        if rejection:
            raise ConfigurationInvalid(f"filter '{name}' {rejection}")
    # And a value inside the bound can still be outside the closed set the
    # route accepts: `geo_level: "NOPE"` is 200 characters short of the bound
    # and is not a grain. API-122 made the live routes refuse it, so such a
    # document -- stored clean and reported valid before that -- now replays
    # as a 422 its reader never saw when they saved it, which is the defect
    # API-117 named. The rule is `registry.closed_value_refusal`, the one the
    # request layer applies (API-123).
    for name in sorted(filters):
        refusal = closed_value_refusal(name, filters[name])
        if refusal is not None:
            raise ConfigurationInvalid(f"filter '{name}': {refusal}")
    return dispatch


def _require_fields_the_route_can_send(document: AnalysisDocument) -> None:
    """Refuse a value the document's own kind has nowhere to send.

    One model carries three kinds, and the three routes do not take the same
    parameters: `/distribution/bins` and `/comparison` accept neither a
    scope, a release, nor a reduction. A stored distribution pinned to a
    release is not a request the API would refuse -- it is worse, an intent
    the API accepts and then cannot honour, reopening as the latest
    publication with nothing saying the pin was dropped, and reporting
    `valid: true` every time it is read (API-112).

    A field left at its default is never a refusal: it changes no request, so
    a document written before this existed -- or one that spells
    ``scope: "latest"`` outright -- validates exactly as it did.
    """
    allowed = CONFIGURATION_DOCUMENT_FIELDS[document.kind]
    carried = sorted(
        name
        for name, field in type(document).model_fields.items()
        if name not in allowed
        and name not in _DOCUMENT_FIELDS_EVERY_KIND_CARRIES
        and getattr(document, name) != field.default
    )
    if carried:
        raise ConfigurationInvalid(
            f"a configuration of kind '{document.kind}' cannot carry "
            f"{', '.join(carried)}: {CONFIGURATION_ROUTES[document.kind]} has "
            f"no such parameter, so the value could not be replayed. This "
            f"kind carries: {', '.join(sorted(allowed))}"
        )


def _require_consistent_observation_read(read: Any, *, label: str = "") -> None:
    """The contradictions ``/observations`` itself refuses (API-066, API-081).

    Storage is not a back door for a request the API would refuse, and a
    stored contradiction would replay as a 422 the reader never saw when they
    saved it.

    Takes anything carrying the five observation-read fields, so an
    ``AnalysisDocument`` of kind ``observations`` and one series of a
    workbench are checked by this code rather than by two copies of it --
    which is the whole reason a ``SeriesDocument`` carries exactly the fields
    an observations document carries. ``label`` names the series in the
    refusal, because "series 3" is actionable where "a series" is not.
    """
    where = f"{label}: " if label else ""
    if read.release is not None and read.scope != "as_released":
        raise ConfigurationInvalid(
            f"{where}release can only be combined with scope=as_released"
        )
    if read.newest_per_geography and read.scope != "latest":
        raise ConfigurationInvalid(
            f"{where}newest_per_geography can only be combined with scope=latest"
        )
    if read.newest_release_per_period and read.scope != "as_released":
        raise ConfigurationInvalid(
            f"{where}newest_release_per_period can only be combined with "
            "scope=as_released"
        )
    if read.newest_release_per_period and read.release is not None:
        raise ConfigurationInvalid(
            f"{where}release and newest_release_per_period contradict each other"
        )
    if read.newest_per_geography and read.newest_release_per_period:
        raise ConfigurationInvalid(
            f"{where}newest_per_geography and newest_release_per_period cannot "
            "be combined"
        )


def _validate_workbench(
    warehouse: Session, document: AnalysisDocument
) -> frozenset[str]:
    """A stored composition, checked series by series (WB-6).

    Each series is validated exactly as an ``observations`` document is,
    through the same three functions, because a series *is* an observations
    request. A composite contract would have to be kept in step with the
    observations contract by hand, and the two would drift the first time a
    filter bound moved.

    Beyond the series, two things only a composition can get wrong:

    - **An alignment naming a grain a series does not publish.** A
      cross-sectional presentation reads every measure at one grain, so a
      stored grain only some of them publish reopens to a control with no
      option for it and a request the route answers empty. The check is the
      intersection the composing screen computes, made again here because
      storage must not be a back door for a value the screen refused.
    - **A top-level ``filters``.** A workbench's filters belong to its series;
      one at the top has nowhere to be replayed, which is API-112's defect a
      level up. Refused rather than ignored.
    """
    series = list(document.series or ())
    if not series:
        raise ConfigurationInvalid(
            "a workbench carries at least one series; series is required for "
            "this configuration kind"
        )
    if document.filters:
        raise ConfigurationInvalid(
            "a workbench carries its filters on each series, not at the top "
            f"level: {', '.join(sorted(document.filters))} has nowhere to be "
            "replayed"
        )
    if document.presentation is None:
        raise ConfigurationInvalid(
            "presentation is required for this configuration kind"
        )

    metrics = []
    for index, entry in enumerate(series, start=1):
        label = f"series {index}"
        metric = _require_metric(
            warehouse, entry.metric_code, f"{label} metric_code"
        )
        _require_declared_filters(metric, dict(entry.filters or {}), kind="workbench")
        _require_consistent_observation_read(entry, label=label)
        metrics.append(metric)

    alignment = document.alignment
    if alignment is not None:
        refusal = closed_value_refusal("geo_level", alignment.geo_level)
        if refusal is not None:
            raise ConfigurationInvalid(f"alignment geo_level: {refusal}")
        wanted = normalize_geo_level(alignment.geo_level)
        without_it = [
            str(metric.get("metric_code") or "")
            for metric in metrics
            # A measure declaring no grains does not narrow the offer --
            # unknown is not none, the rule the composing screen applies --
            # so it is not named here either.
            if metric.get("valid_geo_grains")
            and wanted
            not in {
                normalize_geo_level(str(grain))
                for grain in (metric.get("valid_geo_grains") or ())
                if grain
            }
        ]
        if without_it:
            raise ConfigurationInvalid(
                f"alignment geo_level '{wanted}' is not published by "
                f"{', '.join(without_it)}; a cross-sectional reading is "
                "answered at one grain, and nothing is rolled up to reach it"
            )
        if alignment.state_fips is not None:
            bound = OBSERVATION_FILTER_BOUNDS.get("state_fips")
            rejection = bound.rejection(alignment.state_fips) if bound else ""
            if rejection:
                raise ConfigurationInvalid(f"alignment state_fips {rejection}")

    return _owning_sources(*metrics)


def _owning_sources(*metrics) -> frozenset[str]:
    """The sources the resolved measures belong to, upper-cased.

    Returned by ``validate_document`` because it has already resolved every
    measure the document asks for, and the packet service needs exactly this
    to cross an envelope's stated sources against the query that read them
    (API-113). Resolving them a second time would double the lookups a
    twelve-block packet spends.
    """
    codes = set()
    for metric in metrics:
        code = str((metric or {}).get("source_code") or "").upper()
        if code:
            codes.add(code)
    return frozenset(codes)


def validate_document(warehouse: Session, document: AnalysisDocument) -> frozenset[str]:
    """Raise ``ConfigurationInvalid`` unless the live contracts accept it.

    Answers the sources the document's measures belong to, resolved on the
    way through.
    """
    filters = dict(document.filters or {})
    _require_fields_the_route_can_send(document)

    if document.kind == "observations":
        metric = _require_metric(warehouse, document.metric_code, "metric_code")
        _require_declared_filters(metric, filters, kind="observations")
        _require_consistent_observation_read(document)
        return _owning_sources(metric)

    if document.kind == "workbench":
        return _validate_workbench(warehouse, document)

    if document.kind == "distribution":
        metric = _require_metric(warehouse, document.metric_code, "metric_code")
        _require_declared_filters(metric, filters, kind="distribution")
        dispatch = OBSERVATION_DISPATCH[str(metric.get("source_code") or "")]
        refusal = dispatch.analysis_refusal()
        if refusal is not None:
            raise ConfigurationInvalid(refusal)
        return _owning_sources(metric)

    metric_a = _require_metric(warehouse, document.metric_code_a, "metric_code_a")
    metric_b = _require_metric(warehouse, document.metric_code_b, "metric_code_b")
    for metric in (metric_a, metric_b):
        _require_declared_filters(metric, filters, kind="comparison")
    decision = evaluate_comparison(metric_a, metric_b)
    if not decision.comparable:
        raise ConfigurationInvalid(decision.failure_summary())
    return _owning_sources(metric_a, metric_b)


def _validation_state(
    warehouse: Session, document: AnalysisDocument
) -> ConfigurationValidation:
    try:
        validate_document(warehouse, document)
    except ConfigurationInvalid as exc:
        return ConfigurationValidation(valid=False, reason=exc.detail)
    return ConfigurationValidation(valid=True)


# ---------------------------------------------------------------------------
# Owner-scoped storage
# ---------------------------------------------------------------------------

_INSERT = text(
    """
    INSERT INTO app_api.saved_analysis_configuration (
        owner_user_id, name, version, document
    ) VALUES (:owner_user_id, :name, 1, CAST(:document AS JSONB))
    RETURNING configuration_id, name, version, created_at, updated_at
    """
)

_SELECT_ONE = text(
    """
    SELECT configuration_id, name, version, document, created_at, updated_at
    FROM app_api.saved_analysis_configuration
    WHERE configuration_id = :configuration_id AND owner_user_id = :owner_user_id
    """
)

# The page and its total in one statement, so a concurrent create cannot
# land between them and be counted by one and not the other (API-103). The
# warehouse engine answers this with `REPEATABLE READ` (API-100); this engine
# carries the optimistic-concurrency `UPDATE`, which needs a stale version to
# match no row and answer 409 rather than raise a serialization failure, so
# the fix here is API-084's: one reading, not one snapshot.
#
# `counted` always yields exactly one row, so the LEFT JOIN reports the true
# total even when the page is empty -- an `offset` past the end must not tell
# a caller their stored work is gone.
_SELECT_PAGE = text(
    """
    WITH owned AS (
        SELECT configuration_id, name, version, document, created_at, updated_at
        FROM app_api.saved_analysis_configuration
        WHERE owner_user_id = :owner_user_id
    ),
    counted AS (SELECT COUNT(*) AS total FROM owned),
    page AS (
        SELECT * FROM owned
        ORDER BY name, configuration_id
        LIMIT :limit OFFSET :offset
    )
    SELECT counted.total,
           page.configuration_id, page.name, page.version, page.document,
           page.created_at, page.updated_at
    FROM counted LEFT JOIN page ON TRUE
    ORDER BY page.name, page.configuration_id
    """
)

_UPDATE = text(
    """
    UPDATE app_api.saved_analysis_configuration
    SET name = :name,
        document = CAST(:document AS JSONB),
        version = version + 1,
        updated_at = NOW()
    WHERE configuration_id = :configuration_id
      AND owner_user_id = :owner_user_id
      AND version = :expected_version
    RETURNING configuration_id, name, version, created_at, updated_at
    """
)

_CURRENT_VERSION = text(
    """
    SELECT version FROM app_api.saved_analysis_configuration
    WHERE configuration_id = :configuration_id AND owner_user_id = :owner_user_id
    """
)

_DELETE = text(
    """
    DELETE FROM app_api.saved_analysis_configuration
    WHERE configuration_id = :configuration_id AND owner_user_id = :owner_user_id
    RETURNING configuration_id
    """
)

_NAME_TAKEN = text(
    """
    SELECT 1 FROM app_api.saved_analysis_configuration
    WHERE owner_user_id = :owner_user_id AND name = :name
      AND configuration_id <> :configuration_id
    """
)


def _document_of(row) -> AnalysisDocument:
    stored = row["document"]
    return AnalysisDocument.model_validate(stored)


def _detail(row, validation: ConfigurationValidation) -> SavedAnalysisConfiguration:
    return SavedAnalysisConfiguration(
        configuration_id=int(row["configuration_id"]),
        name=str(row["name"]),
        version=int(row["version"]),
        document=_document_of(row),
        validation=validation,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def create_configuration(
    storage: Session,
    warehouse: Session,
    owner_user_id: int,
    name: str,
    document: AnalysisDocument,
) -> SavedAnalysisConfiguration:
    validate_document(warehouse, document)
    taken = storage.execute(
        _NAME_TAKEN,
        {"owner_user_id": owner_user_id, "name": name, "configuration_id": -1},
    ).first()
    if taken is not None:
        raise ConfigurationNameTaken(name)

    row = (
        storage.execute(
            _INSERT,
            {
                "owner_user_id": owner_user_id,
                "name": name,
                "document": document.model_dump_json(),
            },
        )
        .mappings()
        .one()
    )
    storage.commit()
    return SavedAnalysisConfiguration(
        configuration_id=int(row["configuration_id"]),
        name=str(row["name"]),
        version=int(row["version"]),
        document=document,
        validation=ConfigurationValidation(valid=True),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def get_configuration(
    storage: Session,
    warehouse: Session,
    owner_user_id: int,
    configuration_id: int,
) -> SavedAnalysisConfiguration:
    row = (
        storage.execute(
            _SELECT_ONE,
            {"configuration_id": configuration_id, "owner_user_id": owner_user_id},
        )
        .mappings()
        .first()
    )
    if row is None:
        raise ConfigurationNotFound(configuration_id)
    document = _document_of(row)
    return _detail(row, _validation_state(warehouse, document))


def list_configurations(
    storage: Session,
    owner_user_id: int,
    limit: int,
    offset: int,
) -> SavedAnalysisListResponse:
    rows = (
        storage.execute(
            _SELECT_PAGE,
            {"owner_user_id": owner_user_id, "limit": limit, "offset": offset},
        )
        .mappings()
        .all()
    )
    total = int(rows[0]["total"]) if rows else 0
    items = [
        SavedAnalysisSummary(
            configuration_id=int(row["configuration_id"]),
            name=str(row["name"]),
            kind=_document_of(row).kind,
            version=int(row["version"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
        for row in rows
        # The count's own row when the page is empty, carrying no record.
        if row["configuration_id"] is not None
    ]
    return SavedAnalysisListResponse(
        total=total, limit=limit, offset=offset, items=items
    )


def update_configuration(
    storage: Session,
    warehouse: Session,
    owner_user_id: int,
    configuration_id: int,
    name: str,
    document: AnalysisDocument,
    expected_version: int,
) -> SavedAnalysisConfiguration:
    validate_document(warehouse, document)
    taken = storage.execute(
        _NAME_TAKEN,
        {
            "owner_user_id": owner_user_id,
            "name": name,
            "configuration_id": configuration_id,
        },
    ).first()
    if taken is not None:
        raise ConfigurationNameTaken(name)

    row = (
        storage.execute(
            _UPDATE,
            {
                "configuration_id": configuration_id,
                "owner_user_id": owner_user_id,
                "name": name,
                "document": document.model_dump_json(),
                "expected_version": expected_version,
            },
        )
        .mappings()
        .first()
    )
    if row is None:
        storage.rollback()
        current = storage.execute(
            _CURRENT_VERSION,
            {"configuration_id": configuration_id, "owner_user_id": owner_user_id},
        ).scalar()
        if current is None:
            raise ConfigurationNotFound(configuration_id)
        raise ConfigurationConflict(int(current))

    storage.commit()
    return SavedAnalysisConfiguration(
        configuration_id=int(row["configuration_id"]),
        name=str(row["name"]),
        version=int(row["version"]),
        document=document,
        validation=ConfigurationValidation(valid=True),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def delete_configuration(
    storage: Session, owner_user_id: int, configuration_id: int
) -> None:
    row = storage.execute(
        _DELETE,
        {"configuration_id": configuration_id, "owner_user_id": owner_user_id},
    ).first()
    storage.commit()
    if row is None:
        raise ConfigurationNotFound(configuration_id)
