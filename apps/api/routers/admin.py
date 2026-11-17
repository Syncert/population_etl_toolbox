"""The admin warehouse pages: what each source is, what it holds, every table.

Three reads behind ``/admin/warehouse``, each answered ``private, no-store``:

- ``/admin/warehouse/sources`` -- the declared source inventory joined to the
  live metric catalog (metric counts, grains, newest watermark).
- ``/admin/warehouse/coverage/{source_code}`` -- years held per grain,
  measured from the source's serving relations and kept until a new release.
- ``/admin/warehouse/schema`` -- every warehouse table and view, its keys, and
  the foreign keys and view dependencies between them, read from the Postgres
  catalog and grouped into one diagram per source.

Who may read them. A deployment names its admins in ``API_ADMIN_ACCOUNTS``
(operator labels or signed-in email addresses). With none named the whole
resource answers 404 before any credential is looked at, so a deployment that
has not turned it on does not advertise it. Otherwise a missing or bad token
is the usual 401, and an account not on the list is 403.
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from apps.api.auth import Account, get_app_session_dep, require_account
from apps.api.dependencies import db_service_unavailable, get_db_session_dep
from apps.api.failures import FRESH_SIGN_IN, NOT_FOUND
from apps.api.schemas.admin import (
    CatalogSummary,
    GrainMark,
    SourceCoverageResponse,
    WarehouseSchemaResponse,
    WarehouseSourceEntry,
    WarehouseSourcesResponse,
)
from apps.api.services.account_service import load_account
from apps.api.services.warehouse_atlas import warehouse_atlas
from apps.api.services.warehouse_coverage import source_coverage
from apps.api.services.warehouse_inventory import (
    GRAINS,
    SOURCE_BY_CODE,
    TOPICS,
    WAREHOUSE_SOURCES,
)
from data_ingestion_toolbox.config import get_settings

ADMIN_FORBIDDEN_DETAIL = "this account is not on the deployment's admin list"
_NOT_FOUND_DETAIL = "Not Found"
_PRIVATE_CACHE = "private, no-store"


def admin_enabled() -> None:
    """404 the whole resource when the deployment names no admin."""
    if not get_settings().api_admin_accounts:
        raise HTTPException(status_code=404, detail=_NOT_FOUND_DETAIL)


def require_admin(
    account: Account = Depends(require_account),
    storage: Session = Depends(get_app_session_dep),
) -> Account:
    """The caller, if their operator label or email is on the admin list."""
    allowed = get_settings().api_admin_accounts
    if account.display_label.strip().lower() in allowed:
        return account
    try:
        record = load_account(storage, user_account_id=account.user_account_id)
    except SQLAlchemyError as exc:
        storage.rollback()
        raise db_service_unavailable(exc) from exc
    email = (record.email or "").strip().lower() if record is not None else ""
    if email and email in allowed:
        return account
    raise HTTPException(status_code=403, detail=ADMIN_FORBIDDEN_DETAIL)


router = APIRouter(
    prefix="/admin/warehouse",
    tags=["admin"],
    # Router dependencies resolve before the route's own, so an unconfigured
    # deployment answers 404 without looking at the credential.
    dependencies=[Depends(admin_enabled)],
    responses={**NOT_FOUND, **FRESH_SIGN_IN},
)


def _private(response: Response) -> None:
    response.headers["cache-control"] = _PRIVATE_CACHE
    response.headers["x-robots-tag"] = "noindex, nofollow"


_CATALOG_SUMMARY = text(
    """
    SELECT catalog.source_code,
           COUNT(*) FILTER (WHERE catalog.freshness_state <> 'retired') AS metrics,
           COUNT(*) FILTER (WHERE catalog.freshness_state = 'current') AS current_metrics,
           COUNT(*) FILTER (WHERE catalog.freshness_state = 'stale') AS stale_metrics,
           ARRAY(
               SELECT DISTINCT UPPER(g)
               FROM gold_glossary.dim_metric_catalog AS inner_catalog,
                    UNNEST(inner_catalog.valid_geo_grains) AS g
               WHERE inner_catalog.source_code = catalog.source_code
                 AND inner_catalog.freshness_state <> 'retired'
               ORDER BY 1
           ) AS geo_grains,
           ARRAY(
               SELECT DISTINCT UPPER(g)
               FROM gold_glossary.dim_metric_catalog AS inner_catalog,
                    UNNEST(inner_catalog.valid_time_grains) AS g
               WHERE inner_catalog.source_code = catalog.source_code
                 AND inner_catalog.freshness_state <> 'retired'
               ORDER BY 1
           ) AS time_grains,
           MAX(catalog.source_watermark) AS newest_watermark,
           MAX(catalog.publication_time) AS newest_publication,
           MAX(catalog.harvested_at) AS last_harvested
    FROM gold_glossary.dim_metric_catalog AS catalog
    GROUP BY catalog.source_code
    """
)


def _iso(value: object) -> str | None:
    if value is None:
        return None
    return value.isoformat() if isinstance(value, datetime) else str(value)


@router.get(
    "/sources",
    response_model=WarehouseSourcesResponse,
    summary="Every warehouse source, joined to the live metric catalog",
)
def warehouse_sources(
    response: Response,
    _admin: Account = Depends(require_admin),
    db: Session = Depends(get_db_session_dep),
) -> WarehouseSourcesResponse:
    _private(response)
    try:
        rows = db.execute(_CATALOG_SUMMARY).mappings().all()
    except SQLAlchemyError as exc:
        db.rollback()
        raise db_service_unavailable(exc) from exc
    summary = {
        row["source_code"]: CatalogSummary(
            metrics=int(row["metrics"] or 0),
            current_metrics=int(row["current_metrics"] or 0),
            stale_metrics=int(row["stale_metrics"] or 0),
            geo_grains=list(row["geo_grains"] or []),
            time_grains=list(row["time_grains"] or []),
            newest_watermark=row["newest_watermark"],
            newest_publication=_iso(row["newest_publication"]),
            last_harvested=_iso(row["last_harvested"]),
        )
        for row in rows
    }
    return WarehouseSourcesResponse(
        grains=[list(g) for g in GRAINS],
        topics=list(TOPICS),
        sources=[
            WarehouseSourceEntry(
                code=s.code,
                name=s.name,
                publisher=s.publisher,
                topic=s.topic,
                module=s.module,
                dag=s.dag,
                cadence=s.cadence,
                release=s.release,
                grains=[GrainMark(grain=g, mark=m) for g, m in s.grains.items()],
                declared_years=[list(span) for span in s.years],
                years_note=s.years_note,
                datasets=list(s.datasets),
                measures=s.measures,
                raw=list(s.raw),
                control=list(s.control),
                silver=list(s.silver),
                gold=list(s.gold),
                credential=s.credential,
                catalog_source_code=s.catalog_source_code,
                catalog=summary.get(s.catalog_source_code or ""),
            )
            for s in WAREHOUSE_SOURCES
        ],
        catalog_read_at=datetime.now(timezone.utc).isoformat(),
    )


@router.get(
    "/coverage/{source_code}",
    response_model=SourceCoverageResponse,
    summary="Years held per grain for one source, measured from its serving relation",
)
def warehouse_coverage(
    source_code: str,
    response: Response,
    refresh: bool = Query(
        False,
        description="Measure now instead of reusing the measurement kept for this release.",
    ),
    _admin: Account = Depends(require_admin),
    db: Session = Depends(get_db_session_dep),
) -> SourceCoverageResponse:
    _private(response)
    code = source_code.strip().upper()
    if code not in SOURCE_BY_CODE:
        raise HTTPException(status_code=404, detail="unknown source")
    return SourceCoverageResponse(**source_coverage(db, code, refresh=refresh))


@router.get(
    "/schema",
    response_model=WarehouseSchemaResponse,
    response_model_by_alias=True,
    summary="Every warehouse table and view, keyed and grouped per source",
)
def warehouse_schema(
    response: Response,
    _admin: Account = Depends(require_admin),
    db: Session = Depends(get_db_session_dep),
) -> WarehouseSchemaResponse:
    _private(response)
    try:
        atlas = warehouse_atlas(db)
    except SQLAlchemyError as exc:
        db.rollback()
        raise db_service_unavailable(exc) from exc
    return WarehouseSchemaResponse.model_validate(atlas)
