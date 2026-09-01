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

from apps.api.registry import OBSERVATION_DISPATCH
from apps.api.schemas import (
    AnalysisDocument,
    ConfigurationValidation,
    SavedAnalysisConfiguration,
    SavedAnalysisListResponse,
    SavedAnalysisSummary,
)
from apps.api.services.compatibility import evaluate_comparison
from apps.api.services.neutral_observations_service import resolve_metric

#: Filters every source accepts on the analysis routes, beyond its declared
#: per-source filter set.
_ANALYSIS_UNIVERSAL_FILTERS = frozenset({"geo_level", "state_fips"})


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
    return metric


def _require_declared_filters(metric, filters: dict[str, Any], allowed_extra) -> None:
    source_code = str(metric.get("source_code") or "")
    dispatch = OBSERVATION_DISPATCH.get(source_code)
    if dispatch is None:
        raise ConfigurationInvalid(
            f"source '{source_code}' is not served by the observation routes"
        )
    declared = set(dispatch.supported_filters()) | set(allowed_extra)
    unsupported = sorted(set(filters) - declared)
    if unsupported:
        raise ConfigurationInvalid(
            f"filters not supported for source '{source_code}': "
            f"{', '.join(unsupported)}; supported filters: "
            f"{', '.join(sorted(declared))}"
        )
    return dispatch


def validate_document(warehouse: Session, document: AnalysisDocument) -> None:
    """Raise ``ConfigurationInvalid`` unless the live contracts accept it."""
    filters = dict(document.filters or {})

    if document.kind == "observations":
        metric = _require_metric(warehouse, document.metric_code, "metric_code")
        _require_declared_filters(metric, filters, allowed_extra=())
        if document.release is not None and document.scope != "as_released":
            raise ConfigurationInvalid(
                "release can only be combined with scope=as_released"
            )
        return

    if document.kind == "distribution":
        metric = _require_metric(warehouse, document.metric_code, "metric_code")
        dispatch = _require_declared_filters(
            metric, filters, allowed_extra=_ANALYSIS_UNIVERSAL_FILTERS
        )
        if not dispatch.analysis_ready:
            raise ConfigurationInvalid(
                dispatch.analysis_restriction
                or f"source '{dispatch.source_code}' has no aligned analysis surface"
            )
        return

    metric_a = _require_metric(warehouse, document.metric_code_a, "metric_code_a")
    metric_b = _require_metric(warehouse, document.metric_code_b, "metric_code_b")
    for metric in (metric_a, metric_b):
        _require_declared_filters(
            metric, filters, allowed_extra=_ANALYSIS_UNIVERSAL_FILTERS
        )
    decision = evaluate_comparison(metric_a, metric_b)
    if not decision.comparable:
        raise ConfigurationInvalid(decision.failure_summary())


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

_SELECT_PAGE = text(
    """
    SELECT configuration_id, name, version, document, created_at, updated_at
    FROM app_api.saved_analysis_configuration
    WHERE owner_user_id = :owner_user_id
    ORDER BY name, configuration_id
    LIMIT :limit OFFSET :offset
    """
)

_COUNT = text(
    """
    SELECT COUNT(*) FROM app_api.saved_analysis_configuration
    WHERE owner_user_id = :owner_user_id
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
    total = int(storage.execute(_COUNT, {"owner_user_id": owner_user_id}).scalar() or 0)
    rows = (
        storage.execute(
            _SELECT_PAGE,
            {"owner_user_id": owner_user_id, "limit": limit, "offset": offset},
        )
        .mappings()
        .all()
    )
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
