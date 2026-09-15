import hashlib
import json
from contextlib import asynccontextmanager

from fastapi import APIRouter, Depends, FastAPI

from apps.api.database import DatabaseNotConfigured, dispose_engine
from apps.api.dependencies import (
    reject_undeclared_query_parameters,
    reject_values_outside_a_closed_set,
    serving_contract_unavailable,
)
from apps.api.failures import (
    EVERY_ROUTE_FAILURES,
    PRIVATE_STORE_FAILURES,
    WAREHOUSE_READ_FAILURES,
)
from apps.api.freshness import PublicationEpochProvider
from apps.api.middleware import (
    RedisResponseCacheMiddleware,
    RequestBodyLimitMiddleware,
    SecurityHeadersMiddleware,
    build_cache_targets,
)
from apps.api.ratelimit import RateLimitMiddleware
from apps.api.appdb import dispose_app_engine
from apps.api.routers import (
    catalog,
    cdc,
    comparison,
    distribution,
    evidence_packets,
    health,
    observations,
    saved_analysis,
    usda_nass,
)
from apps.api.routers.source_observations import SOURCE_ROUTERS
from apps.api.services.observations_service import ServingContractUnavailable
from apps.api.telemetry import RequestTelemetryMiddleware
from apps.api.versioning import VERSIONED_ROOT
from data_ingestion_toolbox.config import Settings, get_settings

#: Every public resource, in the order it appears in the generated documentation.
#: Routers declare version-relative prefixes so the same router object can serve
#: both the versioned surface and the legacy alias without a second definition
#: that could drift from it.
#:
#: The model-status router is gone (API-006): it probed three relations no
#: manifest asset creates and reported whichever happened to exist -- naming
#: them in the response body. Modelling surfaces are a plan non-goal; when one
#: is designed, it arrives as a declared contract, not a probe.
#: The public analytical reads. Every one is a bounded, provider-published GET
#: over the warehouse, so every one is cacheable -- and the cache targets are
#: built from these routers' own paths (API-076) rather than from a list of
#: path fragments that nothing checked against the served contract. The
#: fragment list this replaced missed 13 of them, including the neutral
#: ``/observations`` resource the consumer guide tells clients to prefer.
CACHEABLE_ROUTERS: tuple[APIRouter, ...] = (
    catalog.router,
    observations.router,
    distribution.router,
    comparison.router,
    # Per-source gold schema routers. The observation pairs are generated from
    # the serving registry; CDC and USDA NASS keep hand-written routers because
    # their source-explorer contracts are not the shared observation shape.
    *SOURCE_ROUTERS,
    cdc.router,
    usda_nass.router,
)

#: API-owned, user-scoped storage: saved analysis (ADR-0003) and evidence
#: packets (ADR-0004). Authenticated, answered ``private, no-store``, and
#: never publicly cached -- they are deliberately absent from
#: ``CACHEABLE_ROUTERS`` above, and API-063 and API-076 both hold them there.
PRIVATE_ROUTERS: tuple[APIRouter, ...] = (
    saved_analysis.router,
    evidence_packets.router,
)

#: The content report (API-137). A warehouse read, so it is deliberately not
#: in ``CACHEABLE_ROUTERS`` -- an observability resource must describe now,
#: not the last five minutes -- and deliberately its own router rather than a
#: route on ``health.router``, which the rate limiter exempts wholesale.
CONTENT_ROUTERS: tuple[APIRouter, ...] = (health.content_router,)

PUBLIC_ROUTERS: tuple[APIRouter, ...] = (
    # The versioned health resource. Never cached: a probe answer must
    # describe now, not the last five minutes.
    health.router,
    *CONTENT_ROUTERS,
    *CACHEABLE_ROUTERS,
    *PRIVATE_ROUTERS,
)

#: The exact paths the response cache may serve from, derived once from the
#: routers above.
PUBLIC_CACHE_TARGETS = build_cache_targets(CACHEABLE_ROUTERS)


def contract_fingerprint(application: FastAPI) -> str:
    """A short digest of the served contract, for cache-key versioning.

    Any change to the public surface -- an operation, a bound, a schema field
    -- rotates every cache key, so a body cached under the previous contract
    can never be served for the new one. Derived from the application itself
    rather than hand-bumped, because a namespace literal only protects the
    contract when someone remembers to edit it.
    """
    document = json.dumps(application.openapi(), sort_keys=True, default=str)
    return hashlib.sha256(document.encode("utf-8")).hexdigest()[:16]


@asynccontextmanager
async def _lifespan(application: FastAPI):
    yield
    # Graceful shutdown: return pooled connections before the process exits.
    # Uvicorn drains in-flight requests first (--timeout-graceful-shutdown in
    # the deployment); the cache middleware closes its Redis client on the
    # same lifespan signal.
    dispose_engine()
    dispose_app_engine()


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the production application with one explicit runtime configuration."""
    configured = settings or get_settings()
    application = FastAPI(
        title=configured.api_title,
        version=configured.api_version,
        description=configured.api_description,
        lifespan=_lifespan,
        # Applied to every route the application serves, including the health
        # resource and the private ones, and solved before any route's own
        # dependencies. A query parameter no route declares is a caller
        # mistake this API used to answer with a confident wrong page
        # (API-093); it declares no parameters of its own, so the published
        # contract is unchanged.
        dependencies=[
            Depends(reject_undeclared_query_parameters),
            Depends(reject_values_outside_a_closed_set),
        ],
        # And declared on every route for the same reason: that dependency
        # refuses an undeclared query parameter with a 422 before any route
        # runs, so every operation can answer one. The published document
        # declared only the statuses FastAPI generates, so a client built from
        # it typed `422.detail` as an array and had no branch for the failures
        # the guide promises (API-121).
        responses=EVERY_ROUTE_FAILURES,
    )

    @application.exception_handler(ServingContractUnavailable)
    async def _handle_missing_serving_contract(_request, exc):
        return serving_contract_unavailable(exc)

    @application.exception_handler(DatabaseNotConfigured)
    async def _handle_unconfigured_database(_request, exc):
        # Same sanitized 503 as any other unavailability: a caller cannot tell
        # a misconfiguration from an outage, and the readiness probe reports
        # an unservable process instead of raising.
        return serving_contract_unavailable(exc)

    # Routers first: the cache middleware's contract fingerprint is computed
    # from the served OpenAPI document, which must be complete when hashed.
    # Each router is mounted once, under the versioned prefix. API-008 retired
    # the unversioned aliases; /api/v1 is the whole public surface.
    for router in PUBLIC_ROUTERS:
        # What the shared middleware and dependencies behind each group can
        # answer. A route adds the failures only it can raise -- a 404 for an
        # identifier it resolves, a 409 for a name it holds unique, a 413 for
        # a body it parses -- beside its own declaration.
        if router in PRIVATE_ROUTERS:
            group = PRIVATE_STORE_FAILURES
        elif router is health.router:
            # The versioned health resource reads nothing and is exempt from
            # the rate limiter, so the application-wide 422 is all it can
            # answer. Its content sibling is not in this branch: that one
            # reads the warehouse and is metered, so it declares the 429 and
            # 503 every other warehouse read declares.
            group = EVERY_ROUTE_FAILURES
        else:
            group = WAREHOUSE_READ_FAILURES
        application.include_router(router, prefix=VERSIONED_ROOT, responses=group)

    application.include_router(health.probe_router)

    # Middleware executes outermost-last-added: telemetry wraps everything
    # (every response carries a request id and is logged, cached or not),
    # then security headers -- applied to cached bodies too -- then the cache,
    # and innermost the rate limiter, so a cache hit costs no budget and the
    # limits meter exactly the requests that reach the database.
    # Innermost of all: a body over the bound is refused before any router
    # parses it, is never a cacheable response, and still spends budget.
    application.add_middleware(
        RequestBodyLimitMiddleware,
        max_bytes=configured.api_max_request_body_bytes,
    )
    application.add_middleware(
        RateLimitMiddleware,
        catalog_per_minute=configured.api_rate_limit_catalog_per_minute,
        analysis_per_minute=configured.api_rate_limit_analysis_per_minute,
        trusted_proxies=configured.api_trusted_proxy_ips,
    )
    application.add_middleware(
        RedisResponseCacheMiddleware,
        redis_url=configured.redis_url,
        ttl_seconds=configured.api_cache_ttl_seconds,
        contract_fingerprint=contract_fingerprint(application),
        epoch_provider=PublicationEpochProvider(configured.api_cache_freshness_seconds),
        targets=PUBLIC_CACHE_TARGETS,
    )
    application.add_middleware(SecurityHeadersMiddleware)
    application.add_middleware(RequestTelemetryMiddleware)

    return application


app = create_app()
